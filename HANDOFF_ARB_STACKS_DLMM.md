# Handoff — adding DLMM venue support to `arbitrage-rs`

Target audience: the Claude session that will integrate Bitflow DLMM as a
new `Venue` in `arbitrage-rs/crates/stacks/`. Read this before writing any
code. It captures the decisions and constraints from the
`stacks-dex-pools` build session and tells you what's already done vs what
you have to do.

---

## 0. Look at `evm-dex-pool` first

`stacks-dex-pools` is the Stacks peer of the `evm-dex-pool` crate
(`evm-dex-pool = { version = "1.0.0", features = ["collector"] }` in
`arbitrage-rs/Cargo.toml`). Same shape: `PoolInterface` trait,
`PoolRegistry`, event-driven collector, per-DEX modules. `arbitrage-rs`
already consumes `evm-dex-pool` from `crates/evm/` — read those files
first to see how a `Venue` wraps a foreign pool registry:

- `crates/evm/src/evm_pool.rs` — the `Venue` impl (parallel of the
  `DlmmVenue` you're about to write).
- `crates/evm/src/registry_manager.rs` — singleton registry + collector
  lifecycle (parallel of the `DlmmRuntime` you'll need).

Differences from the EVM side:
- Identity type is `Principal` instead of `Address`; amounts are `u128`
  instead of `U256`.
- Ingestion is REST polling (`/extended/v1/contract/<id>/events`) instead
  of WebSocket `eth_getLogs` — the library hides this behind `EventSource`.
- DLMM uses a *bin* state model (1001 entries), not reserves — bootstrap
  is correspondingly heavier.

If you find yourself thinking "how does arb-evm handle X" — the answer is
probably "the same way, just with Stacks types". Mirror that pattern.

---

## 1. The architecture choice that drove everything

`arbitrage-rs` today implements Stacks venues (ALEX, Velar, Arkadiko,
Bitflow XYK, Bitflow V1/V2 stableswap) with a **pull-on-timer** pattern:
each `Venue::refresh()` makes a single `call-read` RPC to fetch reserves +
fees, then quotes locally. That works for those DEXes because each pool's
state fits in one RPC response.

**DLMM cannot work that way.** A DLMM pool has up to 1001 bins of
concentrated liquidity. A quote of any meaningful size walks several bins
in sequence (single-bin → next-bin → next-bin until the input is consumed
or inventory is hit). Re-fetching 1001 bins on every scan tick is not
viable (the RPC budget alone is ~10s per pool with chunked multicall). So
DLMM must maintain a long-lived **event-driven mirror**: bootstrap once,
then apply on-chain events to keep state fresh as swaps land.

`stacks-dex-pools` exists for exactly this. It is the only Stacks DEX in
the workspace that *needs* it; everything else is fine with the
pull-on-timer that arb-stacks already has.

**Decision for this session: arb-stacks's existing V2/stableswap venues
stay exactly as they are. We are adding ONE new venue (DLMM) that consumes
the `stacks-dex-pools` mirror.**

---

## 2. What `stacks-dex-pools` gives you

A `Cargo` workspace-internal crate at `../stacks-dex-pools/` (relative to
`arbitrage-rs/`). It exposes, with the right feature flags:

- **`DLMMPool`** — pool struct holding bin map, active bin, fees,
  factors. Implements `PoolInterface` (the library's trait, not arb-core's).
- **`fetch_dlmm_pool(...)`** — one-shot bootstrap that reads `get-pool`,
  decimals, and 1001 bin balances. Returns a fully-populated `DLMMPool`.
- **`PoolRegistry`** — concurrent map of pools, keyed by `id()`. Lock-free
  read access via `DashMap`, per-pool `tokio::RwLock` for writes.
- **`start_collector(...)`** — spawns the per-contract event-poller tasks
  (one task per polled contract), starts the dispatcher, returns a
  `CollectorHandle` for lifecycle control.
- **`CollectorHandle::add_pool` / `remove_pool` / `stop`** — dynamic pool
  membership and graceful shutdown.

What it does NOT give you:
- A `Venue` impl. arb-core's `Venue` trait lives in `arbitrage-rs` and is
  not visible from `stacks-dex-pools`. **Your job to write the adapter.**
- Token decimals. The library has a `TokenInfo` trait you implement against
  arb-stacks's existing decimals cache (or a simple manual map).
- Aggregator fees. DLMM math returns the raw pool quote; the 10bp Bitflow
  aggregator fee is applied at the venue level if you're comparing against
  the Bitflow FE.

---

## 3. Cargo dependency

Add to `arbitrage-rs/crates/stacks/Cargo.toml`:

```toml
[dependencies]
stacks-dex-pools = { path = "../../../stacks-dex-pools", features = ["collector"] }
# `collector` implies `registry` + `rpc`. That's the minimum viable set for DLMM.
# Do NOT enable `non_dlmm` — that pulls in V2/stableswap mirror code we don't need
# (arb-stacks already has those venues via the pull-on-timer pattern).
# Do NOT enable `block_walking` — DLMM uses the per-contract event source by default.
```

That's it. One dependency, one feature.

---

## 4. Key types you'll use

```rust
use stacks_dex_pools::{
    // Bootstrap
    DLMMPool,
    dlmm::fetcher::{fetch_dlmm_pool, BootstrapMode},
    // Registry + collector lifecycle
    PoolRegistry,
    CollectorConfig,
    CollectorHandle,
    start_collector,
    // The library's pool trait (used to box pools into the registry)
    PoolInterface,
    // RPC client (used at bootstrap)
    StacksRpcClient,
    RpcConfig,
    // Token decimals adapter trait
    StacksTokenInfo,
    // Stacks-side identity type
    Principal,
};
```

`DLMMPool` fields you'll most likely read:

```rust
pub struct DLMMPool {
    pub pool_contract: Principal,
    pub core_contract: Principal,
    pub x_token: Principal,
    pub y_token: Principal,
    pub x_decimals: u8,
    pub y_decimals: u8,
    pub bin_step: u32,
    pub active_bin_id: i32,
    pub x_protocol_fee: u32,  // bps
    pub x_provider_fee: u32,  // bps
    pub y_protocol_fee: u32,
    pub y_provider_fee: u32,
    pub x_variable_fee: u32,
    pub y_variable_fee: u32,
    pub bins: BTreeMap<i32, BinState>,  // signed bin id (centered on 0) -> (x, y)
    pub last_tx_id: Option<String>,     // tx id of the most-recently-applied event
    pub last_event_at: Option<u64>,     // unix epoch seconds when last_tx_id was set
    // ... factors[] is internal math, not user-facing
}
```

`last_event_at` is the freshness signal — the collector stamps it on every successful `apply_event` (after passing the cross-pool filter and matching an indexed action). `None` until the first event lands. Read it from your `DlmmVenue` to render "fresh / stale" in the UI:

```rust
fn freshness_secs(pool: &DLMMPool, now: u64) -> Option<u64> {
    pool.last_event_at.map(|t| now.saturating_sub(t))
}
```

Cross-pool-filtered events, informational events, and unknown actions do NOT advance the watermark — only events that actually mutate (or knowingly acknowledge) this pool's state. So `now - last_event_at` rising past your DLMM block-cadence ceiling is a real signal that polling for this pool's emitter has stalled, not a false alarm from filter traffic.

The quote function you'll call:

```rust
impl DLMMPool {
    /// Returns (dy, last_bin_walked, hit_inventory_cap)
    pub fn quote_x_for_y(&self, x_amount: u128) -> (u128, Option<i32>, bool);
    pub fn quote_y_for_x(&self, y_amount: u128) -> (u128, Option<i32>, bool);
}
```

The `PoolInterface` trait also provides `calculate_output(token_in: &Principal, amount_in: u128) -> Result<u128>` — slightly higher-level wrapper that picks the direction based on which token you pass.

---

## 5. Integration outline — what your `DlmmVenue` looks like

Sketch (don't copy verbatim — adapt to whatever arb-stacks's idioms are):

```rust
// arbitrage-rs/crates/stacks/src/dlmm.rs

use std::sync::Arc;
use arb_core::venue::{EstimateResult, SwapDirection, Venue, VenueInfo, VenueType};
use stacks_dex_pools::{PoolRegistry, DLMMPool, PoolInterface};
use stacks_dex_pools::pool::principal::Principal;
use tokio::sync::RwLock;

pub struct DlmmVenue {
    /// Stable registry id of THIS pool (= `pool_contract.to_string()` for DLMM).
    pool_id: String,
    /// Shared across all DLMM venues — they all read state from this registry.
    registry: Arc<PoolRegistry>,
    /// arb-core metadata.
    info: VenueInfo,
    /// Pre-cached because arb-core asks for them on the hot path.
    decimals_a: u8,
    decimals_b: u8,
    /// `true` if the venue's `token_a` is the registry's `x_token`.
    /// Set at construction by comparing arb-stacks token strings to the
    /// pool's Principal-typed tokens.
    a_is_x: bool,
    /// Bitflow aggregator fee in bps. Apply ONLY if the venue is being
    /// quoted as an "aggregator path" — Bitflow's FE deducts 10bp on top
    /// of pool fees in displayed quotes.
    aggregator_fee_bps: u32,
}

#[async_trait::async_trait]
impl Venue for DlmmVenue {
    fn info(&self) -> &VenueInfo { &self.info }

    fn is_ready(&self) -> bool {
        // The registry has the pool iff bootstrap completed and the
        // collector hasn't removed it.
        let Some(handle) = self.registry.get(&self.pool_id) else { return false };
        // Cheap try_read; if contended, assume ready (the collector lock
        // means an event is being applied — pool state will be valid again
        // in microseconds).
        let Ok(g) = handle.try_read() else { return true };
        let Some(p) = g.as_any().downcast_ref::<DLMMPool>() else { return false };
        !p.bins.is_empty()
    }

    async fn refresh(&self) -> anyhow::Result<()> {
        // NO-OP. The collector keeps state fresh in the background.
        // arb-scanner's RefreshManager will call this periodically; that's fine.
        // If you want a "force re-bootstrap" you can implement it here later.
        Ok(())
    }

    fn estimate_receive(&self, direction: SwapDirection, amounts_in: &[f64]) -> EstimateResult {
        let Some(handle) = self.registry.get(&self.pool_id) else {
            return EstimateResult::default();
        };
        let g = match handle.try_read() {
            Ok(g) => g,
            Err(_) => return EstimateResult::default(),  // contention; scanner retries
        };
        let pool = g.as_any().downcast_ref::<DLMMPool>().expect("registry holds DLMMPool");

        let amounts_out = amounts_in.iter().map(|&amt_human| {
            // Decide direction in pool's (x, y) frame.
            // arb-core's SwapDirection ⨯ self.a_is_x → pool direction.
            let pool_x_for_y = matches!(
                (direction, self.a_is_x),
                (SwapDirection::Sell, true) | (SwapDirection::Buy, false)
            );
            let (dec_in, dec_out) = if pool_x_for_y {
                (pool.x_decimals, pool.y_decimals)
            } else {
                (pool.y_decimals, pool.x_decimals)
            };
            let amount_raw = (amt_human * 10f64.powi(dec_in as i32)) as u128;
            let (out_raw, _last_bin, _capped) = if pool_x_for_y {
                pool.quote_x_for_y(amount_raw)
            } else {
                pool.quote_y_for_x(amount_raw)
            };
            // Optionally apply the aggregator fee (10bp) for "what the FE shows".
            let out_after_agg = if self.aggregator_fee_bps == 0 {
                out_raw
            } else {
                out_raw.saturating_mul((10_000 - self.aggregator_fee_bps) as u128) / 10_000
            };
            out_after_agg as f64 / 10f64.powi(dec_out as i32)
        }).collect();

        EstimateResult {
            amounts_out,
            prices: Vec::new(),
            amounts_without_fee: Vec::new(),
        }
    }

    fn estimate_give(&self, _: SwapDirection, _amounts_out: &[f64]) -> EstimateResult {
        // DLMM doesn't have a closed-form inverse; if you need this for routing,
        // implement Newton-Raphson on top of quote_x_for_y. Otherwise return
        // default and document the limitation.
        EstimateResult::default()
    }

    // identity(), raw_reserves(), reserves() — fill in following the
    // existing alex.rs / velar.rs patterns. For raw_reserves you can sum
    // all bin x and y across `pool.bins` and return as f64 in human units.
}
```

---

## 6. Wiring it up — bootstrap + collector lifecycle

`DlmmVenue` instances all share ONE `PoolRegistry` and ONE collector. Make
this a runtime singleton (`OnceCell` or similar) so any DLMM pool you
configure goes through the same machinery.

Typical startup sequence (e.g. in arb-stacks's venue builder):

```rust
use std::sync::Arc;
use stacks_dex_pools::{
    PoolRegistry, CollectorConfig, CollectorHandle, start_collector,
    StacksRpcClient, RpcConfig, StacksTokenInfo, PoolInterface,
};
use stacks_dex_pools::dlmm::fetcher::{fetch_dlmm_pool, BootstrapMode};
use std::time::Duration;

pub struct DlmmRuntime {
    pub registry: Arc<PoolRegistry>,
    pub handle: CollectorHandle,
}

impl DlmmRuntime {
    pub async fn new(pools: &[DlmmPoolCfg]) -> anyhow::Result<Self> {
        // 1. Build the bootstrap RPC client. Bitflow's node has higher
        //    read budgets than Hiro public AND honours `?tip=` for
        //    consistent snapshots. Use it for call-reads.
        let bootstrap_rpc = Arc::new(StacksRpcClient::new(RpcConfig {
            base_url: "https://node.bitflowapis.finance".to_string(),
            max_retries: 5,
            ..Default::default()
        })?);
        let token_info = StacksTokenInfo::new(bootstrap_rpc.clone());

        // 2. Bootstrap all pools at the snapshot tip (single block).
        //    Pin to chain tip if you want a consistent cross-pool snapshot;
        //    pass None to take "current at each call" (slightly cheaper).
        let registry = Arc::new(PoolRegistry::new());
        for cfg in pools {
            let pool_contract: Principal = cfg.pool_contract.parse()?;
            let core_contract: Principal = cfg.core_contract.parse()?;
            let pool = fetch_dlmm_pool(
                bootstrap_rpc.clone(),
                &pool_contract,
                &core_contract,
                &token_info,
                BootstrapMode::Full,    // 1001 bins via chunked multicall, ~10s
                8,                       // parallelism for per-bin fallback (unused in Full)
                None,                    // tip=None ⇒ current chain state
            ).await?;
            // Seed last_processed_block so the collector doesn't replay
            // pre-bootstrap events. The collector uses per-contract tx_id
            // watermarks for the per-contract source; the registry's
            // block cursor is unused in that path but set it anyway.
            registry.insert(Box::new(pool));
        }

        // 3. Start the collector.
        //    Events go through Hiro's `/extended/v1/contract/<id>/events`
        //    endpoint — that's where the inline event payloads live.
        //    Sized for 9 polled contracts (8 pools + 1 shared core) at
        //    12s/cycle ≈ 45 req/min — just under Hiro's free-tier 50/min.
        let handle = start_collector(
            "https://api.mainnet.hiro.so".to_string(),
            registry.clone(),
            CollectorConfig {
                poll_interval: Duration::from_secs(12),
                ..Default::default()
            },
            None,  // metrics — wire one if arb-stacks has a metrics trait
        ).await?;

        Ok(Self { registry, handle })
    }
}
```

Constructing `DlmmVenue` then becomes:

```rust
let runtime = DlmmRuntime::new(&dlmm_pool_cfgs).await?;
// Stash `runtime.registry.clone()` and `runtime.handle` on whatever owns
// the venues. The handle's `stop().await` must be awaited at shutdown.

let venue_a = DlmmVenue::new(&runtime.registry, "SM1FK….dlmm-pool-stx-usdcx-v-1-bps-10", ...);
let venue_b = DlmmVenue::new(&runtime.registry, "SM1FK….dlmm-pool-stx-sbtc-v-1-bps-15", ...);
// All venues read from the same registry. The collector keeps every
// pool's state fresh in the background.
```

---

## 7. Token decimals — implementing the bootstrap-side adapter

`fetch_dlmm_pool` takes a `&dyn TokenInfo` reference. The library ships
`StacksTokenInfo` which lazily fetches decimals via `call-read` and caches
them. **Use that as-is unless you have a reason not to:**

```rust
let token_info = StacksTokenInfo::new(bootstrap_rpc.clone());
// Each call to token_info.decimals(&principal) hits cache; first miss does
// `call-read get-decimals` and stores the u8 in a DashMap.
```

If arb-stacks already has a decimals cache (`crates/stacks/src/decimals.rs`),
write a small adapter: wrap it in a type that implements the library's
`TokenInfo` trait by delegating to the existing cache. Don't duplicate.

---

## 8. Configuration — what arb-stacks's TOML / discovery needs to know

Per DLMM pool you want to mirror, you need:

- `pool_contract` — `SP1FK….dlmm-pool-<x>-<y>-v-1-bps-<N>`
- `core_contract` — `SP1PFR….dlmm-core-v-1-1` (shared by every DLMM pool today)
- `token_a` / `token_b` — arb-stacks's existing token strings (e.g. `"STX"`, `"SP....usdcx"`)
  — used for venue identity + direction selection
- `quote_is_a` / `base_asset` / `quote_asset` — same shape as the other Stacks venues
- `aggregator_fee_bps` — `10` if you want to mirror Bitflow FE quotes, else `0`

### Two ways to populate that list

**Hardcoded** — copy the 8 names from
`stacks-dex-pools/src/bin/test_all_dlmm.rs:KNOWN_POOLS`. Simple, but you'll
miss pools added after this doc was written.

**Auto-discovery (recommended)** — use the registry-walker that ships in
`stacks_dex_pools::dlmm::discovery`:

```rust
use stacks_dex_pools::dlmm::discover_dlmm_pools;

let pools = discover_dlmm_pools(
    rpc_client.clone(),
    &"SP1PFR4V08H1RAZXREBGFFQ59WB739XM8VVGTFSEA.dlmm-core-v-1-1".parse()?,
    8,        // parallelism for the per-id RPC fan-out
    None,     // tip — None = current chain state
).await?;

for listing in pools {
    if !listing.status { continue; }            // skip paused
    // listing.pool_contract → use with fetch_dlmm_pool
    // listing.name / .symbol → useful for VenueInfo display
    // listing.id              → opaque registry id, mostly informational
}
```

The walker calls `dlmm-core::get-last-pool-id()` followed by parallel
`get-pool-by-id(id)` for id ∈ 1..=last_id. Cost: `1 + N` RPCs where N is
the registered pool count (currently 8 → ~9 RPCs, sub-second on Bitflow's
node). Run once per discovery cycle. Live-verified 2026-05 — returns the
same 8 pools as the hardcoded set.

**This is what arb-stacks's `periodic_discovery` should call** for the
`bitflow_dlmm` include-dex path. Diff the result against the registry on
each tick and `CollectorHandle::add_pool` / `remove_pool` accordingly.

Token mapping (`pool_contract` → `token_a` / `token_b`) is NOT in the
listing — you get that from the subsequent `fetch_dlmm_pool` call (it
returns `x_token` / `y_token` as Principal values you can map back to
arb-stacks's token strings). So the typical flow is:

```
discover_dlmm_pools  ─▶ Vec<DlmmPoolListing>
                            │
                            ▼ (filter status, dedup vs registry)
                       fetch_dlmm_pool for each new entry
                            │
                            ▼
                       registry.insert + (collector picks up automatically)
```

---

## 9. RPC hosts you'll need

| URL | What for | Notes |
|---|---|---|
| `https://node.bitflowapis.finance` | Bootstrap `call-read` (1001-bin multicall) | Has the read budget DLMM needs; honours `?tip=` |
| `https://api.mainnet.hiro.so` | Events polling (`/extended/v1/contract/<id>/events`) | Bitflow's node mirrors this too, but Hiro is the canonical source |

You can use a single host for both if you have a self-hosted Stacks node
+ Hiro API layer. For "just make it work", these two URLs are the
defaults `test_all_dlmm` uses.

---

## 10. Lifecycle / shutdown

- The `CollectorHandle` owns the poller tasks. Calling `.stop().await` is
  graceful — it flips a stop_flag, drops the queue sender so the
  processor exits, and awaits every task.
- Per-pool dynamic add: `handle.add_pool(Box::new(pool))` — if the pool
  introduces a new polled contract, a new poller task is spawned
  automatically.
- Dynamic remove: `handle.remove_pool(&pool_id)` — drops from registry;
  poller stays running but its events get dropped at dispatch (cheaper
  than tearing down the poller). The pool id is `pool_contract.to_string()`
  for DLMM.

Wire `handle.stop().await` into arb-stacks's existing shutdown path.

---

## 11. Testing — how to verify your `DlmmVenue` works

In order, cheapest-to-most-thorough:

1. **Unit test** — construct a fixture `DLMMPool` (use the test factory at
   `stacks-dex-pools/src/registry.rs::tests::make_pool` for inspiration),
   wrap in a `PoolRegistry`, build a `DlmmVenue` around it, call
   `estimate_receive` with a known fixture amount, assert the result.
2. **Live single-pool smoke**:
   ```bash
   cargo run --bin test_all_dlmm --features collector --release -- \
       --pool dlmm-pool-stx-usdcx-v-1-bps-10 \
       --rpc-host https://node.bitflowapis.finance \
       --duration 60 --log-interval 15 --poll-interval 5
   ```
   Confirms bootstrap works against live mainnet and the collector
   applies events. Outputs per-pool quote with the 10bp post-aggregator
   column for direct UI comparison.
3. **Cross-venue scan** — once `DlmmVenue` is wired into arb-scanner,
   run a small scan that includes BOTH a DLMM venue and a non-DLMM venue
   for the same pair (e.g. STX/USDCx via DLMM AND via Velar). Compare
   the quotes — they should differ by pool-level price (different fees,
   different liquidity), but both should be within a few percent.
4. **Live reconcile** — `stacks-dex-pools/tests/dlmm_reconcile_live.rs` is
   `#[ignore]` by default. Run with `--ignored` against mainnet to
   verify the event-applicator catches up correctly over a multi-block
   replay. Don't ship if this fails.

---

## 12. Caveats and gotchas

1. **Per-pool events vs core events.** DLMM pools emit `update-bin-balances*`
   from their own contracts (per-pool scoped). The core
   (`dlmm-core-v-1-1`) emits swap and fee events for EVERY pool sharing
   that core. The library's `apply_event` already filters core events
   by `data.pool-contract` so only the right pool sees them — this
   filtering is the original "cross-pool contamination fix" that
   motivated this whole crate. Don't disable it.

2. **No `estimate_give` closed form.** DLMM's bin walk doesn't have a
   closed-form inverse. If multi-hop routing needs `estimate_give`, you
   have two options:
   - Newton-Raphson around `estimate_receive` (slow, bounded).
   - Punt and document that DLMM venues only work in `estimate_receive`
     direction. arb-scanner's existing code paths probably don't use
     `estimate_give` for Stacks anyway — check before investing in NR.

3. **The DLMM `core_contract` is shared.** Every DLMM pool today uses
   `SP1PFR4V08H1RAZXREBGFFQ59WB739XM8VVGTFSEA.dlmm-core-v-1-1`. The
   collector subscribes to it ONCE for all pools — the `topics` machinery
   dedups per-contract automatically.

4. **`refresh()` is a no-op** because the collector keeps state fresh in
   the background. arb-scanner's `RefreshManager` will still call it
   periodically; that's fine. **Do not** make `refresh()` do a
   re-bootstrap on every tick — that defeats the whole point of the
   event-driven mirror.

5. **Aggregator fee is venue-side.** The library returns raw pool quotes.
   The 10bp Bitflow aggregator fee only matters if you're comparing
   against Bitflow's FE UI. For "best execution" arb routing you want
   the raw pool quote, no 10bp deduction.

6. **State persistence.** The mirror lives in RAM only. On restart you
   re-bootstrap from scratch + the collector picks up from the events
   tip. ~10-30s per pool to bootstrap. If you need warm restarts later,
   `PoolRegistry` is serializable (uses `serde` on every pool type) so
   you could snapshot to disk.

7. **DLMM has no on-chain quoter.** There is no callable `get-dy` on
   any DLMM-deployer contract. Math runs locally on both sides (our
   mirror + Bitflow's FE). If you want to verify a specific quote
   against on-chain you have to do it through the math fixtures
   (`stacks-dex-pools/tests/dlmm_math_fixtures.rs`) or the live
   reconcile test, not via a single RPC call.

8. **Rate limits.** Hiro public free tier is ~50 req/min. The default
   `poll_interval = 12s` × 9 contracts = 45 req/min fits. If arb-stacks
   wants a faster cycle, get a paid Hiro key OR self-host a Stacks node.

---

## 13. Files in `stacks-dex-pools` worth reading before you start

- `README.md` — top-level overview, feature tiers, quick-start examples.
- `src/dlmm/pool.rs` — `DLMMPool` struct, `quote_x_for_y` / `quote_y_for_x`.
- `src/dlmm/fetcher.rs` — bootstrap path (`fetch_dlmm_pool`), three modes.
- `src/dlmm/events.rs` — the event-application logic + cross-pool filter.
- `src/collector/bootstrap.rs` — `start_collector` entry point.
- `src/bin/test_all_dlmm.rs` — full integration example: 8 pools, live
  mainnet, per-contract events, watermark seeding, loop with Δ-vs-baseline
  printing. Read this top-to-bottom before integrating — it covers every
  edge case you'll hit.
- `src/dlmm/discovery.rs` + `examples/discover_dlmm.rs` — registry-walker
  for auto-enumerating pools. `cargo run --example discover_dlmm --features rpc --release`
  prints the live pool set.
- `tests/dlmm_math_fixtures.rs` — byte-exact math fixtures pinned against
  the Python POC and the on-chain math.
- `tests/dlmm_event_apply.rs` — the cross-pool filter regression test.

---

## 14. What this session should produce

Concrete deliverables for the integration session:

1. New file `arbitrage-rs/crates/stacks/src/dlmm.rs` with `DlmmVenue` impl.
2. New file in the config crate to construct `DlmmVenue` from TOML — same
   shape as the existing `alex_*` / `velar_*` builders.
3. A `DlmmRuntime` singleton (or equivalent) that holds the registry +
   collector handle. Plumbed into the app's shutdown path.
4. Unit test(s) for the venue: byte-exact fixture against a known pool
   state, verify `estimate_receive` matches.
5. One live smoke test entry in `scan_configs/` for STX→USDCx DLMM that
   the maintainer can run by hand to verify end-to-end.
6. Updated `arbitrage-rs/crates/stacks/README.md` (or CLAUDE.md) noting
   that DLMM is now supported, what RPC hosts it needs, and the
   `stacks-dex-pools` Cargo dependency.

**Do NOT** migrate arb-stacks's existing V2/stableswap venues to consume
`stacks-dex-pools` in this session. That's deliberately a separate scope
(the existing venues work, and `stacks-dex-pools` doesn't ship the
non-DLMM math in the default feature set anyway).
