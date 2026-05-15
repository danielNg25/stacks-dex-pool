//! `PoolRegistry` — shared in-memory pool state.
//!
//! Same shape as `evm-dex-pool::PoolRegistry`:
//!   - `DashMap<Principal, Arc<tokio::RwLock<Box<dyn PoolInterface>>>>` for
//!     shard-level locking (multiple readers/writers on different pools don't
//!     contend).
//!   - Per-contract event watermark (`Arc<DashMap<Principal, String>>` —
//!     contract → latest tx_id seen).
//!   - Atomic last-processed-block cursor.
//!   - Topic registry (set of `StacksTopic`s the collector should poll).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::RwLock;

use crate::pool::base::PoolInterface;
use crate::pool::event::StacksTopic;
use crate::pool::principal::Principal;

pub type PoolHandle = Arc<RwLock<Box<dyn PoolInterface + Send + Sync>>>;

/// In-memory registry of mirrored pools.
///
/// Pools are keyed by [`PoolInterface::id`] (a `String`) rather than by
/// `pool_contract()`. This matters for singleton-contract DEXes where many
/// pools share one address (Arkadiko swap-v2-1, Velar univ2-core, ALEX
/// amm-pool-v2-01) — keying by `Principal` would clobber every pair after
/// the first. Each variant picks an `id()` that's unique per pool:
/// - DLMM, Bitflow XYK: `pool_contract.to_string()` (already unique)
/// - Arkadiko: the LP token principal
/// - Velar: the LP token principal
/// - ALEX: `<amm-contract>#<pool-id>` (uint discriminator embedded)
pub struct PoolRegistry {
    by_id: Arc<DashMap<String, PoolHandle>>,
    /// Per-contract event watermark: contract → latest tx_id processed.
    /// Keyed by the emitter contract Principal — multiple pools sharing one
    /// singleton contract share one watermark, which is correct.
    watermarks: Arc<DashMap<Principal, String>>,
    /// Set of topics the collector should poll. Insertion-ordered via
    /// DashMap. The collector reads this on each tick.
    topics: Arc<DashMap<StacksTopic, ()>>,
    /// Most-recent Stacks block height we've processed events from. Updated
    /// by the collector after each successful tick.
    last_processed_block: AtomicU64,
}

impl Default for PoolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for PoolRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolRegistry")
            .field("pool_count", &self.by_id.len())
            .field("topic_count", &self.topics.len())
            .field("last_processed_block", &self.last_processed_block())
            .finish()
    }
}

impl PoolRegistry {
    pub fn new() -> Self {
        Self {
            by_id: Arc::new(DashMap::new()),
            watermarks: Arc::new(DashMap::new()),
            topics: Arc::new(DashMap::new()),
            last_processed_block: AtomicU64::new(0),
        }
    }

    /// Insert (or replace) a pool. The pool's [`PoolInterface::topics`] are
    /// added to the topic set; the pool itself is keyed by its `id()`.
    pub fn insert(&self, pool: Box<dyn PoolInterface + Send + Sync>) {
        for t in pool.topics() {
            self.topics.insert(t, ());
        }
        let id = pool.id();
        self.by_id.insert(id, Arc::new(RwLock::new(pool)));
    }

    /// Get a handle to the pool with the given `id()`. Use `.read().await`
    /// for quotes / `.write().await` for event application.
    pub fn get(&self, id: &str) -> Option<PoolHandle> {
        self.by_id.get(id).map(|h| h.clone())
    }

    /// Remove a pool by `id()`. Returns true if it existed.
    pub fn remove(&self, id: &str) -> bool {
        self.by_id.remove(id).is_some()
    }

    /// Iterate over all pools whose topic set includes events emitted from
    /// `contract` — typically a shared core (DLMM core, Velar univ2-core,
    /// ALEX amm-pool, Arkadiko swap-v2-1). Used by the collector to
    /// dispatch core-emitted events to every pool that subscribes.
    pub fn pools_subscribed_to(&self, contract: &Principal) -> Vec<PoolHandle> {
        let mut out = Vec::new();
        // No reverse index topic→pool yet; iterate pools. O(pools), fine
        // for our scale (low hundreds).
        for entry in self.by_id.iter() {
            let handle = entry.value().clone();
            // Use try_read so we never block here; if contended, include
            // the pool anyway — apply_event will harmlessly drop foreign
            // events.
            let matches = {
                match handle.try_read() {
                    Ok(g) => g.topics().iter().any(|t| &t.contract == contract),
                    Err(_) => true,
                }
            };
            if matches {
                out.push(handle);
            }
        }
        out
    }

    /// All pools, for iteration.
    pub fn iter(&self) -> impl Iterator<Item = PoolHandle> + '_ {
        self.by_id.iter().map(|e| e.value().clone())
    }

    /// Number of pools registered.
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    // ---- Event watermarks ----

    pub fn watermark(&self, contract: &Principal) -> Option<String> {
        self.watermarks.get(contract).map(|e| e.clone())
    }

    pub fn set_watermark(&self, contract: &Principal, tx_id: String) {
        self.watermarks.insert(contract.clone(), tx_id);
    }

    // ---- Topics ----

    pub fn topics(&self) -> Vec<StacksTopic> {
        self.topics.iter().map(|e| e.key().clone()).collect()
    }

    /// Distinct contracts the collector should poll (union of all topics).
    pub fn polled_contracts(&self) -> Vec<Principal> {
        let mut set = std::collections::HashSet::new();
        for t in self.topics.iter() {
            set.insert(t.key().contract.clone());
        }
        set.into_iter().collect()
    }

    // ---- Block cursor ----

    pub fn last_processed_block(&self) -> u64 {
        self.last_processed_block.load(Ordering::Relaxed)
    }

    pub fn set_last_processed_block(&self, b: u64) {
        self.last_processed_block.store(b, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dlmm::pool::{BinState, DLMMPool};
    use crate::dlmm::PRICE_SCALE_BPS;
    use std::collections::BTreeMap;

    fn make_pool(name: &str) -> Box<dyn PoolInterface + Send + Sync> {
        let pool_contract: Principal =
            format!("SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD.{}", name)
                .parse()
                .unwrap();
        let core: Principal = "SP1PFR4V08H1RAZXREBGFFQ59WB739XM8VVGTFSEA.dlmm-core-v-1-1"
            .parse()
            .unwrap();
        let stx: Principal = "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.token-stx-v-1-2"
            .parse()
            .unwrap();
        let usdcx: Principal = "SP466FNC0P7JWTNM2R9T199QRZN1MYEDTAR0KP27.usdcx"
            .parse()
            .unwrap();
        Box::new(DLMMPool {
            pool_contract,
            core_contract: core,
            x_token: stx,
            y_token: usdcx,
            x_decimals: 6,
            y_decimals: 6,
            bin_step: 10,
            initial_price: PRICE_SCALE_BPS,
            active_bin_id: 0,
            x_protocol_fee: 15,
            x_provider_fee: 15,
            y_protocol_fee: 15,
            y_provider_fee: 15,
            x_variable_fee: 0,
            y_variable_fee: 0,
            bins: BTreeMap::from([(0, BinState { x: 0, y: 0 })]),
            last_tx_id: None,
            last_event_at: None,
            factors: vec![PRICE_SCALE_BPS; 1001],
        })
    }

    #[test]
    fn insert_and_lookup() {
        let r = PoolRegistry::new();
        let p1 = make_pool("dlmm-pool-stx-usdcx-v-1-bps-10");
        let id = p1.id();
        r.insert(p1);
        assert_eq!(r.len(), 1);
        assert!(r.get(&id).is_some());
        // Negative lookup must miss without panicking.
        assert!(r.get("not-a-real-pool-id").is_none());
    }

    /// The cross-cutting fix: two pools sharing a singleton contract must
    /// both survive in the registry. DLMM is per-pool-contract so we
    /// simulate the singleton case at the trait level with two pools that
    /// happen to differ only by name (different DLMM pool contracts), and
    /// rely on `PoolInterface::id()` returning their distinct contract
    /// strings — same outcome as singletons-with-discriminator.
    #[test]
    fn two_pools_with_distinct_ids_both_register() {
        let r = PoolRegistry::new();
        r.insert(make_pool("dlmm-pool-stx-usdcx-v-1-bps-1"));
        r.insert(make_pool("dlmm-pool-stx-usdcx-v-1-bps-10"));
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn topics_dedup_across_pools_sharing_core() {
        let r = PoolRegistry::new();
        r.insert(make_pool("dlmm-pool-stx-usdcx-v-1-bps-1"));
        r.insert(make_pool("dlmm-pool-stx-usdcx-v-1-bps-10"));
        // Two pools, each registers 9 topics. The 2 pool-specific × 2 = 4
        // unique pool topics, plus 7 shared core topics = 11 total distinct.
        assert_eq!(r.topics().len(), 11);
        // Both pools subscribe to the core; iterating polled_contracts
        // gives 3 contracts (2 pools + 1 core).
        assert_eq!(r.polled_contracts().len(), 3);
    }

    #[test]
    fn watermark_set_get() {
        let r = PoolRegistry::new();
        let c: Principal = "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.foo"
            .parse()
            .unwrap();
        assert!(r.watermark(&c).is_none());
        r.set_watermark(&c, "0xabcd".to_string());
        assert_eq!(r.watermark(&c).as_deref(), Some("0xabcd"));
    }

    #[test]
    fn block_cursor() {
        let r = PoolRegistry::new();
        assert_eq!(r.last_processed_block(), 0);
        r.set_last_processed_block(7_932_500);
        assert_eq!(r.last_processed_block(), 7_932_500);
    }
}
