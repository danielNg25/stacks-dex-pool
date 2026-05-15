# stacks-dex-pools

Reusable Rust pool-state mirror library for Stacks DEXes — peer of
[`evm-dex-pool`](https://github.com/danielng25/evm-dex-pool) in architecture,
adapted for Stacks specifics.

**Bitflow DLMM (HODLMM) is first-class.** Uniswap V2 family (ALEX / Velar /
Arkadiko / Bitflow XYK) and Bitflow V1/V2 StableSwap are stubbed — type
definitions present, math marked `TODO(stacks-port):` with pointers to the
reference Python POC at `../test/`.

## Why

`arbitrage-rs` already mirrors EVM pool state via `evm-dex-pool` so it can
quote without re-fetching on every scan. The Stacks side needs the same
shape so DLMM pools (1001 bins per pool — re-reading per quote isn't an
option) stay quote-ready, and so V2/StableSwap variants slot in later
without reworking the integration.

The library design follows three principles from the EVM version:

- **Pool math is event-driven, not poll-on-quote.** Quotes read from a local
  mirror; the collector keeps the mirror in sync.
- **Reads and writes are separated by a per-pool `tokio::RwLock`.** Quote
  callers and event applicators don't contend on the registry; they only
  contend on the specific pool they touch.
- **Per-protocol implementations behind a single `PoolInterface` trait.**
  The collector and registry don't know the difference between a DLMM bin
  pool and a Uniswap-V2 reserve pair.

## Layout

```
src/
├── codec/                    # Clarity value + c32 address codec (default feature, no deps)
├── pool/                      # Principal, PoolInterface, StacksEvent, StacksTopic
├── rpc/                       # HTTP RPC client + events endpoint (feature `rpc`)
├── token_info.rs              # TokenInfo trait + decimals cache (feature `rpc`)
├── dlmm/                      # Bitflow DLMM — full implementation
├── v2/                        # Stub for ALEX / Velar / Arkadiko / Bitflow XYK
├── stableswap/                # Stub for Bitflow V1/V2 stableswap
├── registry.rs                # PoolRegistry (DashMap + tokio::RwLock) (feature `registry`)
└── collector/                 # Event poll lifecycle (feature `collector`)

tests/
├── codec_roundtrip.rs         # Clarity encode/decode roundtrip + real event shapes
├── dlmm_math_fixtures.rs      # Hand-computed swap math fixtures
├── dlmm_event_apply.rs        # Cross-pool filter correctness (THE critical test)
└── dlmm_reconcile_live.rs     # `#[ignore]` — tip-and-replay against live Hiro

examples/
└── quote_dlmm.rs              # Bootstrap a pool and quote
```

## Feature tiers

Matches `evm-dex-pool` so consumers can drag in only what they need:

| Feature | Adds | Use case |
|---|---|---|
| `default` | Pool math + Clarity/c32 codec | Embed quote math in another tool |
| `rpc` | HTTP client (reqwest), `?tip=` historical reads, events fetch, decimals cache | One-shot quoting from chain |
| `registry` | DashMap-backed `PoolRegistry` with per-pool RwLock | Multi-pool in-memory state |
| `collector` | Per-contract event polling tasks, dedup queue, dispatcher | Production: live mirror |

Each tier is additive (`collector` implies `registry` + `rpc`).

## Quick start

### End-to-end collector test (bootstrap + run live):

```bash
# Single pool, fast feedback (~15s bootstrap + your --duration):
cargo run --bin test_all_dlmm --features collector -- \
    --pool dlmm-pool-stx-usdcx-v-1-bps-10 \
    --rpc-host https://node.bitflowapis.finance \
    --duration 120 --log-interval 10 --poll-interval 5

# All 8 pools (~75s bootstrap + your --duration):
cargo run --bin test_all_dlmm --features collector -- \
    --rpc-host https://node.bitflowapis.finance \
    --duration 300 --log-interval 15

# Bootstrap + one quote snapshot only, no live collector:
cargo run --bin test_all_dlmm --features collector -- \
    --pool dlmm-pool-stx-usdcx-v-1-bps-10 --duration 0
```

What it does:

1. **Bootstrap in FULL mode** — fetches all 1001 bins per pool via the
   chunked multicall helper (`dlmm-pool-multi-helper-v-1-1.get-bin-balances-multi`).
   ~11 chunked calls (chunks of 100) × ~1s each ≈ 10-14s per pool. Quotes are
   accurate at any size. The slower per-bin fallback (`BootstrapMode::FullPerBin`)
   remains available for environments where the helper is unreachable.
2. **Insert into a `PoolRegistry` and start the collector.** The collector
   polls every contract in the topic set (each pool's contract + the shared
   core engine) and applies events to the mirror.
3. **Loop: every `--log-interval` seconds, log per-pool status:**
   `active_bin`, `bin_count`, event watermark, and the current quote for a
   fixed input. If chain state drifts during the run (someone swaps, an LP
   adds/removes), the line will show `Δ active …→…` and `Δ quote …`.

Example output:

```
[bootstrap] STX→USDCx-10   active= -26 bin_step= 10bps non_empty_bins=1001 fees(x→y)= 30bps 50.9s
[baseline]
  STX→USDCx-10   active= -26 bins=1001 wm=(none)         |   100.0000 →      26.675322
[collector] starting — poll_interval=5s, events host = https://api.mainnet.hiro.so

[t+ 10s]
  STX→USDCx-10   active= -26 bins=1001 wm=0x0668566c70…  |   100.0000 →      26.675322

[t+ 20s]
  STX→USDCx-10   active= -27 bins=1001 wm=0x9a3def…       |   100.0000 →      26.671004  Δ active -26→-27 Δ quote -4318
```

When `Δ` arrows show up, that's the collector applying real on-chain events
to the local mirror. Use `--duration 600` or longer to give the active pool
time to actually trade.

### Quote one pool live (custom amounts):

```bash
cargo run --example quote_dlmm --features rpc -- 100
```

Output:

```
Bootstrapping SM1FKXGN….dlmm-pool-stx-usdcx-v-1-bps-10 (±10 bin window)…
  active bin -37, x_decimals=6, y_decimals=6, bins mirrored=15
  fees (x→y): protocol=15 provider=15 variable=0 (total 30 bps)

          STX in        USDCx out  eff price (USD/STX)
          ------       ---------   -------------------
      100.000000        27.012345          0.27012345
```

### Full event-driven mirror (all features):

```rust
use std::sync::Arc;
use stacks_dex_pools::{
    PoolRegistry, start_collector, CollectorConfig,
    dlmm::fetcher::{fetch_dlmm_pool, BootstrapMode},
    rpc::client::{StacksRpcClient, RpcConfig},
    token_info::StacksTokenInfo,
};

let client = Arc::new(StacksRpcClient::new(RpcConfig::default())?);
let token_info = StacksTokenInfo::new(client.clone());
let registry = Arc::new(PoolRegistry::new());

let pool = fetch_dlmm_pool(
    client.clone(),
    &"SM1FKXGN….dlmm-pool-stx-usdcx-v-1-bps-10".parse()?,
    &"SP1PFR4V….dlmm-core-v-1-1".parse()?,
    &token_info,
    BootstrapMode::default(),
    8,
    None,
).await?;
registry.insert(Box::new(pool));

let handle = start_collector(
    "https://api.mainnet.hiro.so".to_string(),
    registry.clone(),
    CollectorConfig::default(),
    None,
).await?;
// ... quote loop ...
handle.stop().await;
```

## Key differences from evm-dex-pool

| EVM | Stacks |
|---|---|
| `Address` (`[u8; 20]`) | `Principal` (Standard or Contract) |
| `U256` for amounts | `u128` (Clarity uint is 128-bit) |
| `FixedBytes<32>` topic (Keccak hash) | `StacksTopic { contract, action_name }` |
| WebSocket log subscription | REST polling (Stacks RPC has no push) |
| `eth_getLogs` filter by address+topic | URL is per-contract; filter after decode |
| `blockNumber` parameter | `?tip=<index_block_hash>` query string |
| Multicall3 for batched reads | Parallelize with `futures::stream::buffer_unordered` |
| Events carry `blockNumber` inline | Hiro returns only `(tx_id, event_index)` — block via `/extended/v1/tx/<tx_id>` |

## The cross-pool filter — read this before changing event handling

Bitflow's DLMM has a **shared core contract** (`dlmm-core-v-1-1`) that runs the
swap math for every DLMM pool. When the core emits a `swap-x-for-y` event, it
emits it on the core's contract address — every pool sharing that core sees
the same event in its core-contract event stream.

Without filtering, applying any swap event would mutate every mirrored pool's
`active_bin_id`. That bug was the original cause of "DLMM quotes don't match
the FE" in the Python POC; the fix is at the top of
[`dlmm::events::apply_event`](src/dlmm/events.rs): drop any event whose
`pool-contract` data field doesn't match the pool's own contract. Pool-emitted
events (`update-bin-balances*`) lack this field — they're implicitly scoped
by the event-stream URL — and pass through.

The test `tests/dlmm_event_apply.rs::swap_event_for_other_pool_is_filtered_out`
guards against regressions; it builds a swap event with someone else's
`pool-contract` and asserts our pool's `active_bin_id` doesn't move.

## What's NOT implemented

- **`shares` field on bins** — intentional. Quote math never reads it; LP
  ownership simulation is the only consumer, and we don't have one. See
  `NOTES_bitflow_dlmm.md §12` in the POC.
- **V2 family math** — stubs only. Reference impls live in the POC's
  `test/fetch_alex_pools.py`, `fetch_velar_pools.py`, `fetch_arkadiko_pools.py`,
  `fetch_bitflow_pools.py`.
- **StableSwap math** — stubs only. Reference: `test/fetch_bitflow_pools.py:103-190`
  (V2 Curve), `test/fetch_bitflow_v1_pools.py` (V1).
- **Reorg handling** — matches `evm-dex-pool`'s posture: caller's
  responsibility (detect, refetch).
- **Multi-hop routing** — that belongs in `arbitrage-rs::Route`, not here.

## Tests

```
cargo test                              # 22 unit + 27 integration (offline)
cargo test --features rpc               # + token_info::cache_lookup_short_circuits
cargo test --features registry          # + 4 registry tests
cargo test --all-features               # 49 total
cargo test --all-features -- --ignored  # + tests/dlmm_reconcile_live (live Hiro)
```

Live reconcile runs the Rust port of `test/verify_dlmm_events.py`: snapshot
at `current_tip - lookback`, replay events to tip, compare quote-relevant
fields. 100% match required.

## Pointers

- Python POC: `../test/` (full byte-exact reference)
- Architectural notes: `../test/NOTES_bitflow_dlmm.md` (12 sections, every gotcha)
- Handoff: `../test/HANDOFF_STACKS.md` (broader Stacks-side context)
