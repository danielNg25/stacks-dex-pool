# stacks-dex-pools

[![crates.io](https://img.shields.io/crates/v/stacks-dex-pools.svg)](https://crates.io/crates/stacks-dex-pools)
[![docs.rs](https://docs.rs/stacks-dex-pools/badge.svg)](https://docs.rs/stacks-dex-pools)
[![license](https://img.shields.io/crates/l/stacks-dex-pools.svg)](LICENSE)

Quote-ready in-memory mirror of Stacks DEX pool state. Bootstraps once
from on-chain `(read-only fn)` calls, then keeps itself fresh via an
event-poll loop so callers can quote any pool without an RPC round-trip
on the hot path.

Bitflow DLMM (HODLMM) is the first-class venue — the bin-based mirror is
the reason this library exists. ALEX, Velar, Arkadiko, Bitflow V2 XYK,
and Bitflow V1/V2 StableSwap are also implemented (behind the `non_dlmm`
feature) with byte-exact `get-dy` math.

## Why a mirror

DLMM pools have up to 1001 bins; quoting a single swap walks multiple
bins of `(x, y)` inventory. Refetching the full bin state on every quote
makes any meaningful arbitrage or pricing loop unworkable. This crate
solves that by:

- **Bootstrapping once** — fetch all 1001 bins via chunked multicall (~12 s/pool
  on `https://node.bitflowapis.finance`).
- **Mirroring events** — a per-contract event poller applies on-chain
  `update-bin-balances` / `swap-x-for-y` / fee changes to the local
  mirror as they land.
- **Quoting locally** — `pool.quote_x_for_y(amount)` is a pure-`u128`
  walk of the local `BTreeMap<bin_id, BinState>`. No I/O.

The V2-family / StableSwap variants don't need 1001 bins, but they fit
the same `PoolInterface` trait and share the same collector — useful
when an application tracks both DLMM and constant-product pools.

## Quick start

```toml
[dependencies]
stacks-dex-pools = { version = "0.1", features = ["collector"] }
```

Bootstrap one pool and quote it live:

```rust
use std::sync::Arc;
use stacks_dex_pools::dlmm::fetcher::{fetch_dlmm_pool, BootstrapMode};
use stacks_dex_pools::pool::principal::Principal;
use stacks_dex_pools::rpc::client::{RpcConfig, StacksRpcClient};
use stacks_dex_pools::token_info::StacksTokenInfo;

# async fn run() -> anyhow::Result<()> {
let client = Arc::new(StacksRpcClient::new(RpcConfig {
    base_url: "https://node.bitflowapis.finance".to_string(),
    ..Default::default()
})?);
let token_info = StacksTokenInfo::new(client.clone());

let pool_contract: Principal =
    "SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD.dlmm-pool-stx-usdcx-v-1-bps-10".parse()?;
let core_contract: Principal =
    "SP1PFR4V08H1RAZXREBGFFQ59WB739XM8VVGTFSEA.dlmm-core-v-1-1".parse()?;

let pool = fetch_dlmm_pool(
    client,
    &pool_contract,
    &core_contract,
    &token_info,
    BootstrapMode::default(), // Full: all 1001 bins, ~12 s
    8,                        // parallelism for per-bin fallback
    None,                     // tip = current
).await?;

// Quote: 100 STX in → ? USDCx out (both 6-decimal).
let (dy, _last_bin, _exhausted) = pool.quote_x_for_y(100_000_000);
println!("100 STX → {:.6} USDCx", dy as f64 / 1_000_000.0);
# Ok(()) }
```

For a full event-driven mirror across multiple pools, use the collector:

```rust,no_run
use std::sync::Arc;
use stacks_dex_pools::{
    pool::base::PoolInterface,
    registry::PoolRegistry,
    collector::{start_collector, CollectorConfig},
};

# async fn run(pool: impl PoolInterface + Send + Sync + 'static) -> anyhow::Result<()> {
let registry = Arc::new(PoolRegistry::new());
registry.insert(Box::new(pool));

let handle = start_collector(
    "https://api.mainnet.hiro.so".to_string(), // Hiro events host
    registry.clone(),
    CollectorConfig::default(),
    None, // optional metrics hook
).await?;

// Quote any registered pool by id without hitting the network.
// Events apply automatically in the background.

handle.stop().await;
# Ok(()) }
```

Runnable end-to-end demo (after `git clone`):

```bash
# Quote one DLMM pool, live:
cargo run --example quote_dlmm --features rpc -- 100

# Discover every live DLMM pool via the on-chain registry:
cargo run --example discover_dlmm --features rpc

# Full collector loop across all DLMM pools, logs Δ on each event:
cargo run --bin test_all_dlmm --features collector -- \
    --rpc-host https://node.bitflowapis.finance \
    --duration 300 --log-interval 15
```

## Feature flags

| Feature | Adds | When to enable |
|---|---|---|
| `default` | Pure pool math (`quote_x_for_y` etc.) + Clarity/c32 codec, no I/O | Embed quote math in another tool, run unit tests |
| `rpc` | HTTP client (reqwest), `?tip=` historical reads, decimals cache | One-shot quoting from chain |
| `registry` | DashMap-backed `PoolRegistry` with per-pool `tokio::RwLock` | Hold many pools in one process |
| `collector` | Per-contract event polling tasks, bounded dedup queue, dispatcher | Production: keep mirrors fresh |
| `non_dlmm` | ALEX / Velar / Arkadiko / Bitflow XYK + V1/V2 stable math | You quote non-DLMM Bitflow pools too |
| `block_walking` | Block-walking event source (single cursor across all pools) | Tracking many pools cheaply at the cost of one extra RPC per event-bearing tx |

Tiers are additive: `collector` implies `registry` + `rpc`.

## What's mirrored

| DEX family | Status | Feature | Source of truth |
|---|---|---|---|
| Bitflow DLMM | ✅ first-class, event-driven | always on | `dlmm-core-v-1-1` |
| Bitflow V2 XYK | ✅ full math + events | `non_dlmm` | `xyk-core-v-1-X` |
| Bitflow V2 StableSwap | ✅ full math + events | `non_dlmm` | `stableswap-core-v-1-X` |
| Bitflow V1 StableSwap | ✅ full math + events (dual ABI, dual variant) | `non_dlmm` | per-pool `get-pair-data` |
| ALEX | ✅ `gmmm-dy` math + events | `non_dlmm` | `amm-pool-v2-01` |
| Velar | ✅ V2 math + events | `non_dlmm` | `univ2-core` |
| Arkadiko | ✅ V2 math (hardcoded 30 bps) + events | `non_dlmm` | `arkadiko-swap-v2-1` |

Every pool type implements the same `PoolInterface` trait, so the
collector and registry treat them uniformly.

## The cross-pool filter (read this if you touch event handling)

Bitflow's DLMM has a **shared core contract** (`dlmm-core-v-1-1`) that
runs the swap math for every DLMM pool. When the core emits a
`swap-x-for-y` event, it emits it on the core's contract address —
every pool sharing that core sees the same event in its core-contract
event stream.

Without filtering, applying any swap event would mutate every mirrored
pool's `active_bin_id`. The filter at the top of
`dlmm::events::apply_event` drops any event whose `pool-contract`
data field doesn't match the pool's own contract. Pool-emitted events
(`update-bin-balances*`) lack this field — they're implicitly scoped
by the event-stream URL — and pass through.

The test
`tests/dlmm_event_apply.rs::swap_event_for_other_pool_is_filtered_out`
guards against regressions; it builds a swap event with someone else's
`pool-contract` and asserts the local pool's `active_bin_id` doesn't
move.

The same pattern applies to V2 XYK and V2 StableSwap (shared core
contracts) — see `v2::events` and `stableswap::events`.

## Discovery

`dlmm-core-v-1-1` exposes a built-in pool registry; the crate ships an
async walker that enumerates every live DLMM pool without hardcoding
addresses:

```rust,no_run
use std::sync::Arc;
use stacks_dex_pools::dlmm::discover_dlmm_pools;
use stacks_dex_pools::pool::principal::Principal;
use stacks_dex_pools::rpc::client::{RpcConfig, StacksRpcClient};

# async fn run() -> anyhow::Result<()> {
let client = Arc::new(StacksRpcClient::new(RpcConfig::default())?);
let core: Principal = "SP1PFR4V08H1RAZXREBGFFQ59WB739XM8VVGTFSEA.dlmm-core-v-1-1".parse()?;
let listings = discover_dlmm_pools(client, &core, 8, None).await?;
for l in listings {
    println!("{}  {}  status={}", l.id, l.pool_contract, l.status);
}
# Ok(()) }
```

## Tests

```bash
cargo test                              # offline default-feature tests
cargo test --features rpc               # + token_info / RPC tests
cargo test --features registry          # + registry tests
cargo test --all-features               # full offline suite
cargo test --all-features -- --ignored  # + live reconcile against mainnet
```

The live reconcile test snapshots a pool at `current_tip - lookback`,
replays events forward to tip, and asserts every quote-relevant field
matches a fresh fetch at tip. 100% match required — that's the
correctness gate the event-handling code is held to.

## Repository layout

```
src/
├── codec/        # Clarity value + c32 address codec (no deps, always on)
├── pool/         # Principal, PoolInterface, StacksEvent, StacksTopic
├── rpc/          # HTTP RPC client + events endpoint (`rpc`)
├── token_info.rs # SIP-010 decimals cache (`rpc`)
├── dlmm/         # Bitflow DLMM — full impl, always on
├── v2/           # ALEX / Velar / Arkadiko / Bitflow XYK (`non_dlmm`)
├── stableswap/   # Bitflow V1/V2 stableswap (`non_dlmm`)
├── registry.rs   # PoolRegistry (`registry`)
└── collector/    # Event poller + dispatcher (`collector`)

tests/
├── codec_roundtrip.rs       # Clarity encode/decode + real event shapes
├── dlmm_math_fixtures.rs    # Hand-computed swap math fixtures
├── dlmm_event_apply.rs      # Cross-pool filter correctness — critical
└── dlmm_reconcile_live.rs   # `#[ignore]` — tip-and-replay against mainnet
```

## Out of scope

- **Reorg handling.** Stacks reorgs are rare and shallow; on a detected
  reorg the caller should re-fetch affected pools rather than try to
  rewind events.
- **Multi-hop routing.** This library quotes a single pool. Composing
  pools into routes belongs upstream.
- **Trade execution.** Read-only mirror; no transaction construction.

## License

[MIT](LICENSE)
