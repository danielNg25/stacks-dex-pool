//! [`BlockWalkingEventSource`] — single-loop block-driven ingestion.
//!
//! One task. Each tick:
//!   1. `GET /v2/info` → `stacks_tip_height`
//!   2. For `block in (last_processed_block+1)..=min(tip, last+max_catchup)`:
//!      `GET /extended/v2/blocks/{block}/transactions` (paginated)
//!      For each tx → iterate events → if `emitter ∈ registered_set` push.
//!   3. Advance `registry.set_last_processed_block(end)`.
//!
//! Rate cost: 1 call to `/v2/info` + 1+ calls per new block. Constant in the
//! number of pools — that's the win over [`super::per_contract_source`].
//!
//! Cold start (`last_processed_block == 0`): we just snap to `tip` without
//! walking any blocks. Anything that happened before us is captured by the
//! per-pool bootstrap snapshot; the source picks up from `tip+1` onward.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use reqwest::Client;

use super::event_queue::EventSender;
use super::metrics::CollectorMetrics;
use super::source::EventSource;
use crate::pool::principal::Principal;
use crate::registry::PoolRegistry;
use crate::rpc::block_walker::{fetch_block_transactions, fetch_chain_tip, BlockTransaction};

/// Config knobs specific to block-walking ingestion.
#[derive(Debug, Clone)]
pub struct BlockWalkingConfig {
    /// How often to wake up and poll `/v2/info`. Stacks block time is ~10
    /// minutes, but Hiro indexes tip much sooner; 6-12s is a sensible range.
    pub poll_interval: Duration,
    /// Page size for `/extended/v2/blocks/<h>/transactions`. Hiro caps at 50.
    pub page_size: u32,
    /// Max blocks to walk in one tick. Bounds catch-up after a long downtime
    /// so we don't hammer Hiro on restart. 30 ≈ ~5 hours of Stacks blocks.
    pub max_blocks_per_tick: u64,
}

impl Default for BlockWalkingConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(8),
            page_size: 50,
            max_blocks_per_tick: 30,
        }
    }
}

/// Block-walker event source. Holds only static config + HTTP client; all
/// per-tick state (cursor, registered set) is read fresh from the registry
/// so dynamic pool changes are picked up automatically.
///
/// Two RPC URLs: `tip_base_url` for `/v2/info` (every node has this — use
/// the one that doesn't rate-limit you, typically Bitflow's), and
/// `events_base_url` for `/extended/v1/*` and `/extended/v2/*` (Hiro-only).
/// They can be the same string if your operator runs a node that mirrors
/// both surfaces.
pub struct BlockWalkingEventSource {
    /// URL for `/v2/info` (chain-tip reads).
    tip_base_url: String,
    /// URL for `/extended/v1/*` and `/extended/v2/*` (events, block-list, tx-detail).
    events_base_url: String,
    http: Arc<Client>,
    config: BlockWalkingConfig,
}

impl BlockWalkingEventSource {
    /// Construct with a single base URL used for both tip and events
    /// (back-compat with older callers — works when the node mirrors both
    /// `/v2/*` and `/extended/v*`).
    pub fn new(events_base_url: String, http: Arc<Client>, config: BlockWalkingConfig) -> Self {
        Self {
            tip_base_url: events_base_url.clone(),
            events_base_url,
            http,
            config,
        }
    }

    /// Construct with separate URLs. Use this when `/v2/info` should hit a
    /// different node than `/extended/v*` — e.g. tip via Bitflow's node
    /// (no rate limit on `/v2/info`) and events via Hiro public.
    pub fn with_separate_tip(
        events_base_url: String,
        tip_base_url: String,
        http: Arc<Client>,
        config: BlockWalkingConfig,
    ) -> Self {
        Self {
            tip_base_url,
            events_base_url,
            http,
            config,
        }
    }
}

#[async_trait]
impl EventSource for BlockWalkingEventSource {
    async fn run(
        self: Arc<Self>,
        registry: Arc<PoolRegistry>,
        sender: EventSender,
        metrics: Option<Arc<dyn CollectorMetrics>>,
        stop_flag: Arc<AtomicBool>,
    ) {
        log::info!(
            "collector: block-walking source — interval={:?} max_blocks_per_tick={}",
            self.config.poll_interval,
            self.config.max_blocks_per_tick,
        );

        let mut next_tick = Instant::now();
        while !stop_flag.load(Ordering::Relaxed) {
            // Sleep in 250ms slices so we shut down quickly even with long intervals.
            while Instant::now() < next_tick {
                if stop_flag.load(Ordering::Relaxed) {
                    log::info!("collector: block-walking source stopped");
                    return;
                }
                let remaining = next_tick.saturating_duration_since(Instant::now());
                tokio::time::sleep(Duration::from_millis(250).min(remaining)).await;
            }
            next_tick = Instant::now() + self.config.poll_interval;

            let tick_start = Instant::now();
            match self.tick(&registry, &sender, metrics.as_deref()).await {
                Ok(stats) => {
                    if let Some(m) = metrics.as_ref() {
                        // Record a "synthetic" poll-cycle row keyed by the source
                        // name so dashboards can plot block-walking throughput
                        // alongside per-contract poller throughput.
                        m.record_poll_cycle(
                            "block-walking",
                            stats.events_enqueued,
                            tick_start.elapsed(),
                        );
                    }
                }
                Err(e) => {
                    log::warn!("collector: block-walking tick error: {}", e);
                    if let Some(m) = metrics.as_ref() {
                        m.record_poll_error("block-walking");
                    }
                }
            }
        }
        log::info!("collector: block-walking source stopped");
    }

    fn name(&self) -> &'static str {
        "block-walking"
    }
}

#[derive(Debug, Default)]
struct TickStats {
    blocks_walked: u64,
    /// All txs across the walked blocks (relevant + unrelated).
    txs_seen: u32,
    /// Txs with at least one event emitted from a registered contract, OR
    /// whose contract-call entry point is a registered contract. These are
    /// the only txs the source attempted to ingest events from.
    txs_relevant: u32,
    /// Decoded events from a registered emitter (before queue dedup).
    /// Equals "what apply_event would see if nothing were a dup".
    events_relevant: u32,
    /// Events that survived dedup and were sent to the queue. The gap
    /// `events_relevant - events_enqueued` is dedup overlap (we just walked
    /// txs that an earlier tick / poller already enqueued).
    events_enqueued: u32,
}

impl BlockWalkingEventSource {
    async fn tick(
        &self,
        registry: &PoolRegistry,
        sender: &EventSender,
        _metrics: Option<&dyn CollectorMetrics>,
    ) -> anyhow::Result<TickStats> {
        let tip = fetch_chain_tip(&self.http, &self.tip_base_url).await?;
        let cursor = registry.last_processed_block();

        // Cold start: snap to tip. Pools were bootstrapped at some tip; events
        // before the bootstrap are already baked into the mirror.
        if cursor == 0 {
            registry.set_last_processed_block(tip);
            log::info!(
                "collector: block-walking — cold start, snapped to tip={}",
                tip
            );
            return Ok(TickStats::default());
        }
        if tip <= cursor {
            return Ok(TickStats::default());
        }

        let registered: HashSet<Principal> = registry.polled_contracts().into_iter().collect();
        if registered.is_empty() {
            // Nothing to filter against — advance the cursor anyway so we
            // don't blow up next tick.
            registry.set_last_processed_block(tip);
            return Ok(TickStats::default());
        }

        let end = (cursor + self.config.max_blocks_per_tick).min(tip);
        let mut stats = TickStats::default();

        for height in (cursor + 1)..=end {
            let txs = fetch_block_transactions(
                &self.http,
                &self.events_base_url,
                height,
                self.config.page_size,
            )
            .await?;
            if txs.is_empty() {
                continue;
            }
            stats.blocks_walked += 1;
            stats.txs_seen += txs.len() as u32;
            let (txs_relevant, events_relevant, enqueued) =
                self.ingest_block_txs(txs, &registered, sender).await;
            stats.txs_relevant += txs_relevant;
            stats.events_relevant += events_relevant;
            stats.events_enqueued += enqueued;
        }

        registry.set_last_processed_block(end);

        if stats.events_relevant > 0 || stats.blocks_walked > 0 {
            log::info!(
                "collector: block-walking — walked {}..={} (blocks={}, txs={} relevant={}, events_relevant={} enqueued={})",
                cursor + 1,
                end,
                stats.blocks_walked,
                stats.txs_seen,
                stats.txs_relevant,
                stats.events_relevant,
                stats.events_enqueued,
            );
        }

        Ok(stats)
    }

    /// Returns `(txs_relevant, events_relevant, events_enqueued)`. Caller
    /// folds the trio into `TickStats`.
    ///
    /// **Two-stage fetch**: Hiro's block-list endpoint returns `event_count`
    /// but leaves `events: []`. For any tx with `event_count > 0` we issue a
    /// follow-up `/extended/v1/tx/{tx_id}` call to get the actual event
    /// payloads. We do those follow-ups in parallel per block to keep
    /// latency bounded.
    async fn ingest_block_txs(
        &self,
        txs: Vec<BlockTransaction>,
        registered: &HashSet<Principal>,
        sender: &EventSender,
    ) -> (u32, u32, u32) {
        // Phase 1: identify txs that COULD be relevant. A tx is a candidate
        // if its `contract_call.contract_id` is registered (direct call to
        // our pool), OR if it has any events at all (router-mediated swaps
        // whose contract_call.contract_id is the router; the relevant emitter
        // hides in the events list).
        let mut candidates: Vec<(BlockTransaction, bool)> = Vec::new();
        for tx in txs {
            let touches_registered = tx
                .contract_call_target
                .as_ref()
                .map(|c| registered.contains(c))
                .unwrap_or(false);
            // Skip txs with no events AND no registered entry point —
            // nothing to learn from them.
            if tx.event_count == 0 && !touches_registered {
                continue;
            }
            // If the block-list already populated `events` for us (some
            // future Hiro version might), skip the follow-up fetch.
            candidates.push((tx, touches_registered));
        }

        // Phase 2 (async fan-out): fetch events for each candidate that
        // doesn't already have them inline.
        let fetches: Vec<_> = candidates
            .iter()
            .enumerate()
            .filter_map(|(idx, (tx, _))| {
                if !tx.events.is_empty() || tx.event_count == 0 {
                    None
                } else {
                    let http = self.http.clone();
                    let base = self.events_base_url.clone();
                    let tx_id = tx.tx_id.clone();
                    Some(async move {
                        let res =
                            crate::rpc::block_walker::fetch_tx_events(&http, &base, &tx_id).await;
                        (idx, res)
                    })
                }
            })
            .collect();
        let results = futures_util::future::join_all(fetches).await;
        for (idx, res) in results {
            match res {
                Ok(events) => {
                    candidates[idx].0.events = events;
                }
                Err(e) => {
                    log::warn!(
                        "block-walker: tx-detail fetch failed for {}: {} (events lost for this tx)",
                        candidates[idx].0.tx_id,
                        e
                    );
                }
            }
        }

        // Phase 3: filter + enqueue. Same logic as before, just over the
        // populated events.
        let mut txs_relevant = 0u32;
        let mut events_relevant = 0u32;
        let mut enqueued = 0u32;
        for (tx, touches_registered) in candidates {
            let mut this_tx_had_relevant_event = false;
            for event in tx.events {
                let from_registered_emitter = registered.contains(&event.emitter);
                if !touches_registered && !from_registered_emitter {
                    continue;
                }
                events_relevant += 1;
                this_tx_had_relevant_event = true;
                if sender.send_dedup(event).await {
                    enqueued += 1;
                }
            }
            if this_tx_had_relevant_event {
                txs_relevant += 1;
            }
        }
        (txs_relevant, events_relevant, enqueued)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::clarity::{cv_encode, ClarityValue};
    use crate::pool::event::StacksEvent;
    use std::collections::BTreeMap;

    fn make_event(emitter: &str, tx_id: &str, idx: u32, action: &str) -> StacksEvent {
        let mut data = BTreeMap::new();
        data.insert("x".to_string(), ClarityValue::Uint(1));
        StacksEvent {
            emitter: emitter.parse().unwrap(),
            tx_id: tx_id.to_string(),
            event_index: idx,
            action: action.to_string(),
            data,
        }
    }

    #[test]
    fn cv_encode_is_callable() {
        // Sanity that test deps resolve.
        let v = ClarityValue::Uint(1);
        assert!(!cv_encode(&v).is_empty());
    }

    #[tokio::test]
    async fn ingest_filters_by_registered_emitter() {
        let http = Arc::new(reqwest::Client::new());
        let src = BlockWalkingEventSource::new(
            "https://example.invalid".to_string(),
            http,
            BlockWalkingConfig::default(),
        );

        let pool_a: Principal = "SP000000000000000000002Q6VF78.pool-a".parse().unwrap();
        let pool_b: Principal = "SP000000000000000000002Q6VF78.pool-b".parse().unwrap();
        let stranger: Principal = "SP000000000000000000002Q6VF78.unknown".parse().unwrap();

        let mut registered = HashSet::new();
        registered.insert(pool_a.clone());

        let (sender, mut rx) = crate::collector::event_queue::build_queue(16);

        // Pre-populate `events` directly (skipping the two-stage fetch
        // entirely) by providing them inline AND setting `event_count`
        // to match. The phase-2 fetch path is a no-op when events are
        // already non-empty.
        let txs = vec![
            // Tx 1: invokes pool_b (not registered), emits from stranger — skip.
            BlockTransaction {
                tx_id: "0x01".into(),
                contract_call_target: Some(pool_b.clone()),
                event_count: 1,
                events: vec![make_event(
                    "SP000000000000000000002Q6VF78.unknown",
                    "0x01",
                    0,
                    "x",
                )],
            },
            // Tx 2: invokes pool_a (registered) — every event in it is in scope.
            BlockTransaction {
                tx_id: "0x02".into(),
                contract_call_target: Some(pool_a.clone()),
                event_count: 2,
                events: vec![
                    make_event("SP000000000000000000002Q6VF78.pool-a", "0x02", 0, "swap"),
                    make_event("SP000000000000000000002Q6VF78.core", "0x02", 1, "fee"),
                ],
            },
            // Tx 3: invokes unrelated, but a registered contract emits an event
            //       (the cross-DEX router scenario). MUST be picked up.
            BlockTransaction {
                tx_id: "0x03".into(),
                contract_call_target: Some(stranger.clone()),
                event_count: 1,
                events: vec![make_event(
                    "SP000000000000000000002Q6VF78.pool-a",
                    "0x03",
                    0,
                    "swap-x-for-y",
                )],
            },
        ];

        let (txs_relevant, events_relevant, enqueued) =
            src.ingest_block_txs(txs, &registered, &sender).await;
        // Tx 1: skipped entirely (no registered emitter, no registered entry).
        // Tx 2: 2 events (touches_registered=true via pool_a entry).
        // Tx 3: 1 event (emitter=pool_a; entry stranger is irrelevant).
        assert_eq!(txs_relevant, 2);
        assert_eq!(events_relevant, 3);
        assert_eq!(enqueued, 3);

        let mut got = Vec::new();
        while let Ok(Some(qe)) = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
            got.push(qe.event);
        }
        assert_eq!(got.len(), 3);
        let actions: Vec<_> = got.iter().map(|e| e.action.as_str()).collect();
        assert!(actions.contains(&"swap"));
        assert!(actions.contains(&"fee"));
        assert!(actions.contains(&"swap-x-for-y"));
    }
}
