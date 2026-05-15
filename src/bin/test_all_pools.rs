//! Multi-DEX live smoke against block-walking event source.
//!
//! Bootstraps one pool per implemented variant (DLMM, Arkadiko, Velar, ALEX,
//! BitflowXyk) at a single block, registers them, starts the collector with
//! [`BlockWalkingEventSource`], and prints per-pool quotes every N seconds.
//!
//! Designed to be run alongside the corresponding DEX UIs for eyeball
//! comparison. Each loop iteration also re-prints the block-walking source's
//! stats so you can see how many txs actually touched our pools and how
//! many events made it through dedup → apply.
//!
//! ## Registry uniqueness
//! Pools are keyed by [`PoolInterface::id()`] in the registry, so multiple
//! pairs sharing a singleton contract (Arkadiko swap-v2-1, Velar univ2-core,
//! ALEX amm-pool-v2-01) all coexist. The bin can register N pools per
//! variant — feel free to add more rows to [`POOLS`].
//!
//! ## Usage
//! ```bash
//! cargo run --bin test_all_pools --features collector --release -- \
//!     --rpc-host https://node.bitflowapis.finance \
//!     --duration 300 --log-interval 30
//! ```

#![cfg(feature = "collector")]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use reqwest::Client;
use stacks_dex_pools::codec::c32::c32_encode_address;
use stacks_dex_pools::codec::clarity::{cv_principal, cv_uint};
use stacks_dex_pools::collector::{
    start_collector_with_source, CollectorConfig, EventSource, PerContractEventSource,
};
#[cfg(feature = "block_walking")]
use stacks_dex_pools::collector::{BlockWalkingConfig, BlockWalkingEventSource};
use stacks_dex_pools::dlmm::fetcher::{fetch_dlmm_pool, BootstrapMode};
use stacks_dex_pools::pool::base::PoolInterface;
use stacks_dex_pools::pool::principal::Principal;
use stacks_dex_pools::registry::PoolRegistry;
use stacks_dex_pools::rpc::client::{RpcConfig, StacksRpcClient};
use stacks_dex_pools::stableswap::fetcher::{
    fetch_bitflow_v1_stable_pool, fetch_bitflow_v2_stable_pool,
};
use stacks_dex_pools::stableswap::{
    BitflowStableSwapV1Pool, BitflowStableSwapV2Pool, MathVariant, Sig,
};
use stacks_dex_pools::token_info::StacksTokenInfo;
use stacks_dex_pools::v2::fetcher::{
    fetch_alex_pool, fetch_arkadiko_pool, fetch_bitflow_xyk_pool, fetch_velar_pool,
};
use stacks_dex_pools::v2::{AlexPool, BitflowXykPool};

/// Default events RPC. Bitflow's node mirrors BOTH `/extended/v1/*`
/// (per-contract polling, tx-detail follow-ups) and `/extended/v2/*`
/// (block-list) with the same payload shape as Hiro AND no public rate-limit
/// — verified 2026-05 by probing 10 quick `/v2/info`+`/extended/v1/tx`
/// requests with no 429s. Tip is in sync with Hiro to the block.
///
/// We used to default this to Hiro public, but the free-tier ~50 req/min cap
/// was incompatible with the block-walker's two-stage fetch under any real
/// chain activity. Switch to `--events-host https://api.mainnet.hiro.so`
/// only if you specifically need Hiro for redundancy.
const DEFAULT_EVENTS_HOST: &str = "https://node.bitflowapis.finance";

/// Default bootstrap (call-read) RPC. Bitflow's node:
///   - Has higher per-call read budgets than Hiro public (≥5MB vs ~500KB),
///     so DLMM's chunked multicall fits.
///   - Honours `?tip=<index_block_hash>`, so every pool's bootstrap is pinned
///     to the same chain state for a consistent snapshot.
///   - Hiro public satisfies neither, hence the split.
///
/// Override with `--rpc-host` if you have a self-hosted Stacks node.
const DEFAULT_BOOTSTRAP_HOST: &str = "https://node.bitflowapis.finance";

// ---- Singletons / shared addresses ---------------------------------------
const DLMM_POOL_DEPLOYER: &str = "SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD";
const DLMM_CORE: &str = "SP1PFR4V08H1RAZXREBGFFQ59WB739XM8VVGTFSEA.dlmm-core-v-1-1";
const ARKADIKO_SWAP: &str = "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.arkadiko-swap-v2-1";
const VELAR_CORE: &str = "SP1Y5YSTAHZ88XYK1VPDH24GY0HPX5J4JECTMY4A1.univ2-core";
const ALEX_AMM: &str = "SP102V8P0F7JX67ARQ77WEA3D3CFB5XW39REDT0AM.amm-pool-v2-01";

/// One configured pool to bootstrap + quote. Each variant has different
/// state shape, so the enum carries variant-specific config.
enum PoolCfg {
    Dlmm {
        label: &'static str,
        pool_name: &'static str,
        quote_amount_x: f64,
    },
    Arkadiko {
        label: &'static str,
        x_token: &'static str,
        y_token: &'static str,
        quote_amount_x: f64,
    },
    Velar {
        label: &'static str,
        x_token: &'static str,
        y_token: &'static str,
        quote_amount_x: f64,
    },
    Alex {
        label: &'static str,
        x_token: &'static str,
        y_token: &'static str,
        quote_amount_x: f64,
    },
    BitflowXyk {
        label: &'static str,
        pool: &'static str,
        x_token: &'static str,
        y_token: &'static str,
        quote_amount_x: f64,
    },
    BitflowV2Stable {
        label: &'static str,
        pool: &'static str,
        x_token: &'static str,
        y_token: &'static str,
        quote_amount_x: f64,
    },
    BitflowV1Stable {
        label: &'static str,
        pool: &'static str,
        lp_token: &'static str,
        x_token: &'static str,
        y_token: &'static str,
        /// `"stx-anchored"` (2-arg `get-pair-data`) or `"token-pair"` (3-arg).
        sig: &'static str,
        /// `"v1-bal-bug"` for `-v-1-{1,2,3}` pools, `"v1-fixed"` for `-v-1-4+`.
        variant: &'static str,
        quote_amount_x: f64,
    },
}

impl PoolCfg {
    fn label(&self) -> &'static str {
        match self {
            PoolCfg::Dlmm { label, .. }
            | PoolCfg::Arkadiko { label, .. }
            | PoolCfg::Velar { label, .. }
            | PoolCfg::Alex { label, .. }
            | PoolCfg::BitflowXyk { label, .. }
            | PoolCfg::BitflowV2Stable { label, .. }
            | PoolCfg::BitflowV1Stable { label, .. } => label,
        }
    }
    fn quote_amount_x(&self) -> f64 {
        match self {
            PoolCfg::Dlmm { quote_amount_x, .. }
            | PoolCfg::Arkadiko { quote_amount_x, .. }
            | PoolCfg::Velar { quote_amount_x, .. }
            | PoolCfg::Alex { quote_amount_x, .. }
            | PoolCfg::BitflowXyk { quote_amount_x, .. }
            | PoolCfg::BitflowV2Stable { quote_amount_x, .. }
            | PoolCfg::BitflowV1Stable { quote_amount_x, .. } => *quote_amount_x,
        }
    }
}

/// Aggregator fee Bitflow's frontend charges on top of pool fees, in bps.
/// Mirrors `test_all_dlmm`'s constant — surfaced for direct UI comparison
/// only on Bitflow products (DLMM, BitflowXyk, BitflowV2Stable). Non-Bitflow
/// rows leave the post-fee column blank.
const AGGREGATOR_FEE_BPS: u32 = 10;

/// Default pool set. Verified live 2026-05 by probing each DEX's events
/// endpoint for the underlying token principals; tweak this list when you
/// want to compare against a specific UI screenshot.
const POOLS: &[PoolCfg] = &[
    // ─────────────────── DLMM (Bitflow HODLMM, 1001-bin) ───────────────────
    PoolCfg::Dlmm {
        label: "DLMM stx-usdcx-1",
        pool_name: "dlmm-pool-stx-usdcx-v-1-bps-1",
        quote_amount_x: 100.0,
    },
    PoolCfg::Dlmm {
        label: "DLMM stx-usdcx-10",
        pool_name: "dlmm-pool-stx-usdcx-v-1-bps-10",
        quote_amount_x: 100.0,
    },
    PoolCfg::Dlmm {
        label: "DLMM sbtc-usdcx-10",
        pool_name: "dlmm-pool-sbtc-usdcx-v-1-bps-10",
        quote_amount_x: 0.01,
    },
    PoolCfg::Dlmm {
        label: "DLMM stx-sbtc-15",
        pool_name: "dlmm-pool-stx-sbtc-v-1-bps-15",
        quote_amount_x: 1000.0,
    },
    PoolCfg::Dlmm {
        label: "DLMM aeusdc-usdcx",
        pool_name: "dlmm-pool-aeusdc-usdcx-v-1-bps-1",
        quote_amount_x: 100.0,
    },
    PoolCfg::Dlmm {
        label: "DLMM usdh-usdcx",
        pool_name: "dlmm-pool-usdh-usdcx-v-1-bps-1",
        quote_amount_x: 100.0,
    },
    // ─────────────────────── Arkadiko (singleton swap-v2-1) ────────────────
    PoolCfg::Arkadiko {
        label: "Arkadiko wSTX-USDA",
        // Arkadiko's wSTX wrap (NOT the SIP-010 token-wstx — Arkadiko's own).
        x_token: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.wrapped-stx-token",
        y_token: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.usda-token",
        quote_amount_x: 100.0,
    },
    PoolCfg::Arkadiko {
        label: "Arkadiko wSTX-WELSH",
        x_token: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.wrapped-stx-token",
        y_token: "SP3NE50GEXFG9SZGTT51P40X2CKYSZ5CC4ZTZ7A2G.welshcorgicoin-token",
        quote_amount_x: 100.0,
    },
    // ──────────────────────── Velar (singleton univ2-core) ─────────────────
    PoolCfg::Velar {
        label: "Velar wSTX-aeUSDC",
        x_token: "SP1Y5YSTAHZ88XYK1VPDH24GY0HPX5J4JECTMY4A1.wstx",
        y_token: "SP3Y2ZSH8P7D50B0VBTSX11S7XSG24M1VB9YFQA4K.token-aeusdc",
        quote_amount_x: 100.0,
    },
    PoolCfg::Velar {
        label: "Velar stSTX-aeUSDC",
        x_token: "SP4SZE494VC2YC5JYG7AYFQ44F5Q4PYV7DVMDPBG.ststx-token",
        y_token: "SP3Y2ZSH8P7D50B0VBTSX11S7XSG24M1VB9YFQA4K.token-aeusdc",
        quote_amount_x: 100.0,
    },
    PoolCfg::Velar {
        label: "Velar wSTX-aBTC",
        x_token: "SP1Y5YSTAHZ88XYK1VPDH24GY0HPX5J4JECTMY4A1.wstx",
        y_token: "SP3K8BC0PPEVCV7NZ6QSRWPQ2JE9E5B6N3PA0KBR9.token-abtc",
        quote_amount_x: 100.0,
    },
    PoolCfg::Velar {
        label: "Velar wSTX-LEO",
        x_token: "SP1Y5YSTAHZ88XYK1VPDH24GY0HPX5J4JECTMY4A1.wstx",
        y_token: "SP1AY6K3PQV5MRT6R4S671NWW2FRVPKM0BR162CT6.leo-token",
        quote_amount_x: 100.0,
    },
    PoolCfg::Velar {
        label: "Velar aBTC-aeUSDC",
        x_token: "SP3K8BC0PPEVCV7NZ6QSRWPQ2JE9E5B6N3PA0KBR9.token-abtc",
        y_token: "SP3Y2ZSH8P7D50B0VBTSX11S7XSG24M1VB9YFQA4K.token-aeusdc",
        quote_amount_x: 0.01,
    },
    // ───────────────────── ALEX (singleton amm-pool-v2-01) ─────────────────
    // Only one ALEX pool wired: amm-pool-v2-01's swap event doesn't include
    // token addresses, so adding more pools requires a separate on-chain
    // probe of `(get-pool-details x y factor)` per candidate token-pair.
    // pool-id 13 = STX/ALEX, confirmed via live probe.
    PoolCfg::Alex {
        label: "ALEX STX-ALEX",
        x_token: "SP102V8P0F7JX67ARQ77WEA3D3CFB5XW39REDT0AM.token-wstx-v2",
        y_token: "SP102V8P0F7JX67ARQ77WEA3D3CFB5XW39REDT0AM.token-alex",
        quote_amount_x: 100.0,
    },
    // ─────────────────── Bitflow XYK (per-pool contract) ───────────────────
    // Token order MUST match on-chain — XYK pools commit ordering at creation
    // (no auto-flip like Arkadiko/ALEX).
    PoolCfg::BitflowXyk {
        label: "Bitflow sBTC-STX",
        pool: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.xyk-pool-sbtc-stx-v-1-1",
        x_token: "SM3VDXK3WZZSA84XXFKAFAF15NNZX32CTSG82JFQ4.sbtc-token",
        y_token: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.token-stx-v-1-2",
        quote_amount_x: 0.01,
    },
    PoolCfg::BitflowXyk {
        label: "Bitflow STX-aeUSDC",
        pool: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.xyk-pool-stx-aeusdc-v-1-2",
        x_token: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.token-stx-v-1-2",
        y_token: "SP3Y2ZSH8P7D50B0VBTSX11S7XSG24M1VB9YFQA4K.token-aeusdc",
        quote_amount_x: 100.0,
    },
    PoolCfg::BitflowXyk {
        label: "Bitflow WELSH-STX",
        pool: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.xyk-pool-welsh-stx-v-1-1",
        x_token: "SP3NE50GEXFG9SZGTT51P40X2CKYSZ5CC4ZTZ7A2G.welshcorgicoin-token",
        y_token: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.token-stx-v-1-2",
        quote_amount_x: 1000.0,
    },
    PoolCfg::BitflowXyk {
        label: "Bitflow LEO-STX",
        pool: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.xyk-pool-leo-stx-v-1-1",
        x_token: "SP1AY6K3PQV5MRT6R4S671NWW2FRVPKM0BR162CT6.leo-token",
        y_token: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.token-stx-v-1-2",
        quote_amount_x: 100.0,
    },
    PoolCfg::BitflowXyk {
        label: "Bitflow sBTC-DOG",
        pool: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.xyk-pool-sbtc-dog-v-1-1",
        x_token: "SM3VDXK3WZZSA84XXFKAFAF15NNZX32CTSG82JFQ4.sbtc-token",
        y_token: "SP14NS8MVBRHXMM96BQY0727AJ59SWPV7RMHC0NCG.pontis-bridge-DOG",
        quote_amount_x: 0.001,
    },
    // ─────────── Bitflow V2 StableSwap (per-pool, shared core) ─────────────
    // All three pools below share `stableswap-core-v-1-4` — coexisting in
    // one registry exercises the singleton-core multi-pool path.
    PoolCfg::BitflowV2Stable {
        label: "BFv2-stable STX-stSTX",
        pool: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.stableswap-pool-stx-ststx-v-1-4",
        x_token: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.token-stx-v-1-2",
        y_token: "SP4SZE494VC2YC5JYG7AYFQ44F5Q4PYV7DVMDPBG.ststx-token",
        quote_amount_x: 100.0,
    },
    PoolCfg::BitflowV2Stable {
        label: "BFv2-stable aeUSDC-USDCx",
        pool: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.stableswap-pool-aeusdc-usdcx-v-1-1",
        x_token: "SP3Y2ZSH8P7D50B0VBTSX11S7XSG24M1VB9YFQA4K.token-aeusdc",
        y_token: "SP120SBRBQJ00MCWS7TM5R8WJNTTKD5K0HFRC2CNE.usdcx",
        quote_amount_x: 100.0,
    },
    PoolCfg::BitflowV2Stable {
        label: "BFv2-stable USDh-USDCx",
        pool: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.stableswap-pool-usdh-usdcx-v-1-1",
        x_token: "SPN5AKG35QZSK2M8GAMR4AFX45659RJHDW353HSG.usdh-token-v1",
        y_token: "SP120SBRBQJ00MCWS7TM5R8WJNTTKD5K0HFRC2CNE.usdcx",
        quote_amount_x: 100.0,
    },
    // ──────── Bitflow V1 StableSwap (self-contained, older generation) ─────
    // V1 pools are observably DORMANT on mainnet — no swap events in recent
    // windows during the 2026-05 probe. We include one anyway to exercise
    // the V1 bootstrap path end-to-end (dual-ABI dispatch, dual-math-variant,
    // separate fee data-vars). Quote correctness depends on the V1 contract
    // still serving call-reads on `get-pair-data` + the fee getters; if any
    // returned `none` the bootstrap soft-fails and the bin continues.
    //
    // `sig="stx-anchored"` → 2-arg `get-pair-data(y_token, lp_token)`. We
    // pass Bitflow V2's STX wrap principal in `x_token` as a placeholder for
    // "native STX" (V1's `x` is implicit at the contract level — `x_token`
    // is metadata used only as the pool's `calculate_output` discriminator).
    // `variant="v1-bal-bug"` → `-v-1-2` reproduces the double-count `get-y`.
    PoolCfg::BitflowV1Stable {
        label: "BFv1-stable STX-stSTX",
        pool: "SPQC38PW542EQJ5M11CR25P7BS1CA6QT4TBXGB3M.stableswap-stx-ststx-v-1-2",
        lp_token: "SPQC38PW542EQJ5M11CR25P7BS1CA6QT4TBXGB3M.stx-ststx-lp-token-v-1-2",
        x_token: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.token-stx-v-1-2",
        y_token: "SP4SZE494VC2YC5JYG7AYFQ44F5Q4PYV7DVMDPBG.ststx-token",
        sig: "stx-anchored",
        variant: "v1-bal-bug",
        quote_amount_x: 100.0,
    },
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceKind {
    /// Only registrable when the `block_walking` feature is enabled.
    #[cfg(feature = "block_walking")]
    BlockWalking,
    PerContract,
}

struct Args {
    /// Bootstrap RPC (call-read). Defaults to Bitflow node — see
    /// [`DEFAULT_BOOTSTRAP_HOST`].
    rpc_host: String,
    /// Events RPC (Hiro `/extended/v1/*` + `/extended/v2/*`). Defaults to
    /// Hiro public — see [`DEFAULT_EVENTS_HOST`].
    events_host: String,
    duration_s: u64,
    log_interval_s: u64,
    poll_interval_s: u64,
    /// If true, bootstrap pools pinned to the snapshot tip via `?tip=<hash>`.
    /// OFF by default — the `index_block_hash` we read from `--events-host`
    /// (Hiro) often isn't yet visible on `--rpc-host` (Bitflow) due to per-
    /// node sync lag, so the tip-pinned call-read 404s. Without pinning, each
    /// pool's bootstrap hits the bootstrap-RPC's current tip; the resulting
    /// cross-pool snapshot can span a few blocks. Re-application of events
    /// the walker subsequently picks up is harmless — our `apply_event`
    /// handlers assign absolute reserves, not deltas, so idempotent.
    ///
    /// Set `--pin-tip` if you run both bootstrap + events through the same
    /// node (or two perfectly-synced nodes) and want strictly-atomic snapshots.
    pin_tip: bool,
    /// If Some, override the initial `last_processed_block` cursor so the
    /// block-walker replays from this block onwards. Useful for testing
    /// event-apply against a known swap window without waiting for fresh
    /// activity. Pass e.g. `--start-block 7940590` to walk from that block.
    start_block: Option<u64>,
    /// Event-ingestion strategy. `"block-walking"` (default) drives forward
    /// from a block cursor; `"per-contract"` polls Hiro's per-contract events
    /// endpoint for each emitter — fewer total RPCs at our pool count, and
    /// the endpoint returns events inline (no two-stage fetch needed), so
    /// it's the practical choice under Hiro's free-tier rate limit.
    source_kind: SourceKind,
}

fn parse_args() -> Args {
    let mut a = Args {
        rpc_host: DEFAULT_BOOTSTRAP_HOST.to_string(),
        events_host: DEFAULT_EVENTS_HOST.to_string(),
        duration_s: 0, // 0 = forever
        log_interval_s: 30,
        poll_interval_s: 8,
        pin_tip: false,
        start_block: None,
        // Default to PerContract. The block-walker is gated behind the
        // `block_walking` feature and not needed at this pool count anyway.
        source_kind: SourceKind::PerContract,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--rpc-host" => a.rpc_host = it.next().expect("--rpc-host needs value"),
            "--events-host" => a.events_host = it.next().expect("--events-host needs value"),
            "--duration" => {
                a.duration_s = it
                    .next()
                    .expect("--duration needs value")
                    .parse()
                    .expect("u64")
            }
            "--log-interval" => {
                a.log_interval_s = it
                    .next()
                    .expect("--log-interval needs value")
                    .parse()
                    .expect("u64")
            }
            "--poll-interval" => {
                a.poll_interval_s = it
                    .next()
                    .expect("--poll-interval needs value")
                    .parse()
                    .expect("u64")
            }
            "--no-pin-tip" => a.pin_tip = false,
            "--pin-tip" => a.pin_tip = true,
            "--start-block" => {
                a.start_block = Some(
                    it.next()
                        .expect("--start-block needs value")
                        .parse()
                        .expect("u64"),
                )
            }
            "--source" => {
                let v = it.next().expect("--source needs value");
                a.source_kind = match v.as_str() {
                    #[cfg(feature = "block_walking")]
                    "block-walking" => SourceKind::BlockWalking,
                    #[cfg(not(feature = "block_walking"))]
                    "block-walking" => {
                        eprintln!(
                            "(error) --source block-walking requires the `block_walking` cargo feature. Rebuild with --features block_walking."
                        );
                        std::process::exit(2);
                    }
                    "per-contract" => SourceKind::PerContract,
                    other => {
                        eprintln!(
                            "(error) --source must be \"block-walking\" or \"per-contract\" (got {:?})",
                            other
                        );
                        std::process::exit(2);
                    }
                };
            }
            "-h" | "--help" => {
                println!(
                    "test_all_pools — multi-DEX smoke test against block-walking event source.\n\
                     \n\
                     Two RPC hosts (each used for a different phase):\n\
                       --rpc-host <url>     Bootstrap (call-read).    Default: Bitflow node.\n\
                       --events-host <url>  Events (extended-v1/v2). Default: Hiro public.\n\
                     Bootstrap defaults to Bitflow because public Hiro has a 500KB read_length\n\
                     cap that DLMM exceeds, and Hiro public doesn't honour `?tip=`. Events MUST\n\
                     go through Hiro (or another extended-API mirror); Bitflow's node does not\n\
                     expose /extended/*.\n\
                     \n\
                     Other flags:\n\
                       --duration <secs>    0 = forever (default)\n\
                       --log-interval <s>   Per-pool quote log cadence (default 30)\n\
                       --poll-interval <s>  Block-walker tick cadence (default 8)\n\
                       --pin-tip            Pin every bootstrap call-read to the snapshot\n\
                                            `index_block_hash`. OFF by default because\n\
                                            cross-node tip-lag often 404s; idempotent event\n\
                                            re-application makes the slight cross-pool\n\
                                            snapshot drift harmless in practice.\n\
                     \n\
                     Pool set is hardcoded — edit the POOLS slice at the top of this file."
                );
                std::process::exit(0);
            }
            other => eprintln!("(warn) ignoring unknown arg {:?}", other),
        }
    }
    a
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_secs()
        .format_target(false)
        .init();

    let args = parse_args();
    let rpc_config = RpcConfig {
        base_url: args.rpc_host.clone(),
        max_retries: 5,
        ..Default::default()
    };
    let client = Arc::new(StacksRpcClient::new(rpc_config)?);
    let token_info = StacksTokenInfo::new(client.clone());
    let events_http = Client::builder().timeout(Duration::from_secs(20)).build()?;

    // ── Phase 1: snap chain tip for a consistent bootstrap ─────────────
    println!("[setup] fetching chain tip from Hiro…");
    let (tip_height, tip_hash) = fetch_chain_tip(&events_http, &args.events_host).await?;
    println!(
        "[setup] snapshot tip = block {} ({}…)",
        tip_height,
        &tip_hash[..16]
    );
    println!(
        "[setup] {} pool(s) to bootstrap | bootstrap-rpc={} | events-rpc={}",
        POOLS.len(),
        args.rpc_host,
        args.events_host,
    );

    // ── Phase 2: bootstrap each pool at the snapshot block ─────────────
    let registry = Arc::new(PoolRegistry::new());
    let mut rows: Vec<Row> = Vec::with_capacity(POOLS.len());

    let tip_for_calls = if args.pin_tip {
        Some(tip_hash.as_str())
    } else {
        None
    };
    for cfg in POOLS {
        let started = Instant::now();
        let result = bootstrap_one(&client, &token_info, cfg, tip_for_calls).await;
        match result {
            Ok((pool, summary)) => {
                let label = cfg.label();
                println!(
                    "[bootstrap] {:<22} {:.1}s | {}",
                    label,
                    started.elapsed().as_secs_f64(),
                    summary
                );
                rows.push(Row {
                    label,
                    quote_amount_x: cfg.quote_amount_x(),
                    pool_id: pool.id(),
                    is_bitflow: cfg.is_bitflow_aggregator(),
                    last_quote: None,
                });
                registry.insert(pool);
            }
            Err(e) => {
                eprintln!("[bootstrap] {:<22} ✗ {}", cfg.label(), e);
                // Soft-fail: keep going so other pools still run.
            }
        }
    }

    if rows.is_empty() {
        eprintln!("[setup] no pools bootstrapped — aborting");
        std::process::exit(2);
    }

    // Block-walking cursor. Default: skip to tip so we only see events that
    // happen AFTER the snapshot. With `--start-block N`: replay from N
    // forward (set cursor to N-1 so block N is the first walked). Useful for
    // regression-testing event-apply against a known-divergent window.
    let cursor_init = match args.start_block {
        Some(b) => b.saturating_sub(1),
        None => tip_height,
    };
    registry.set_last_processed_block(cursor_init);
    println!(
        "[setup] {} pool(s) registered, block cursor = {} (tip = {})",
        registry.len(),
        cursor_init,
        tip_height,
    );

    // ── Phase 3: start the collector with the chosen event source ──────
    let bw_http = Arc::new(events_http.clone());
    let source: Arc<dyn EventSource> = match args.source_kind {
        #[cfg(feature = "block_walking")]
        SourceKind::BlockWalking => {
            let bw_cfg = BlockWalkingConfig {
                poll_interval: Duration::from_secs(args.poll_interval_s),
                ..Default::default()
            };
            // Split tip vs events: `/v2/info` hits the bootstrap RPC (Bitflow
            // node, no /v2/info rate-limit), `/extended/v*` hits the events
            // RPC. Avoids burning Hiro's ~50 req/min budget on tip when the
            // events host is Hiro.
            Arc::new(BlockWalkingEventSource::with_separate_tip(
                args.events_host.to_string(),
                args.rpc_host.to_string(),
                bw_http,
                bw_cfg,
            ))
        }
        SourceKind::PerContract => {
            // Per-contract polling. One `/extended/v1/contract/<id>/events`
            // call per emitter contract per cycle. With ~6-9 emitters in
            // our pool set this fits in Hiro's free-tier 50 req/min budget
            // at any cycle ≥ 12s, AND the endpoint returns events inline
            // (no two-stage fetch needed). Trade-off vs block-walking: O(N)
            // polls per cycle vs O(1) for block-walking, so this doesn't
            // scale to hundreds of pools — but it's the right choice for
            // validation runs at the current pool count.
            let pc_cfg = CollectorConfig {
                poll_interval: Duration::from_secs(args.poll_interval_s),
                ..Default::default()
            };
            Arc::new(PerContractEventSource::new(
                args.events_host.to_string(),
                bw_http,
                Arc::new(pc_cfg),
            ))
        }
    };
    let handle = start_collector_with_source(
        args.events_host.to_string(),
        registry.clone(),
        CollectorConfig::default(),
        None,
        source,
    )
    .await?;

    // ── Phase 4: Ctrl-C handler + loop ─────────────────────────────────
    let shutdown = Arc::new(AtomicBool::new(false));
    install_ctrlc(shutdown.clone());
    let log_interval = Duration::from_secs(args.log_interval_s);
    let stop_at = if args.duration_s > 0 {
        Some(Instant::now() + Duration::from_secs(args.duration_s))
    } else {
        None
    };

    println!(
        "\n[collector] running — block-walk every {}s, log every {}s. Ctrl-C to stop.\n",
        args.poll_interval_s, args.log_interval_s
    );

    print_block(client.clone(), &registry, &mut rows, "baseline").await;

    let mut next_log = Instant::now() + log_interval;
    while !shutdown.load(Ordering::Relaxed) {
        if let Some(t) = stop_at {
            if Instant::now() >= t {
                break;
            }
        }
        if Instant::now() >= next_log {
            let tag = chrono::Local::now().format("%H:%M:%S").to_string();
            print_block(client.clone(), &registry, &mut rows, &tag).await;
            next_log = Instant::now() + log_interval;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    println!("\n[collector] shutdown — stopping…");
    handle.stop().await;
    println!("[done]");
    Ok(())
}

/// Tracked state per registered pool.
struct Row {
    label: &'static str,
    quote_amount_x: f64,
    /// The pool's stable id (registry key) — `PoolInterface::id()`.
    pool_id: String,
    /// `true` for Bitflow products (DLMM, XYK, V2 stableswap, V1 stableswap).
    /// Drives the post-10bp aggregator-fee column — non-Bitflow rows show `—`.
    is_bitflow: bool,
    last_quote: Option<u128>,
}

impl PoolCfg {
    /// Whether this variant is consumed through Bitflow's aggregator
    /// frontend, which deducts an additional 10 bps on top of pool fees.
    fn is_bitflow_aggregator(&self) -> bool {
        matches!(
            self,
            PoolCfg::Dlmm { .. }
                | PoolCfg::BitflowXyk { .. }
                | PoolCfg::BitflowV2Stable { .. }
                | PoolCfg::BitflowV1Stable { .. }
        )
    }
}

async fn bootstrap_one(
    client: &Arc<StacksRpcClient>,
    token_info: &StacksTokenInfo,
    cfg: &PoolCfg,
    tip: Option<&str>,
) -> Result<(Box<dyn PoolInterface + Send + Sync>, String)> {
    match cfg {
        PoolCfg::Dlmm { pool_name, .. } => {
            let pool_contract: Principal =
                format!("{}.{}", DLMM_POOL_DEPLOYER, pool_name).parse()?;
            let core_contract: Principal = DLMM_CORE.parse()?;
            let pool = fetch_dlmm_pool(
                client.clone(),
                &pool_contract,
                &core_contract,
                token_info,
                BootstrapMode::Full,
                8,
                tip,
            )
            .await?;
            let summary = format!(
                "active={:+} bins={} fee_x={}bps",
                pool.active_bin_id,
                pool.bins.len(),
                pool.x_fee_bps()
            );
            Ok((Box::new(pool), summary))
        }
        PoolCfg::Arkadiko {
            x_token, y_token, ..
        } => {
            let swap_contract: Principal = ARKADIKO_SWAP.parse()?;
            let x: Principal = x_token.parse()?;
            let y: Principal = y_token.parse()?;
            let pool = fetch_arkadiko_pool(client.clone(), &swap_contract, &x, &y, token_info, tip)
                .await?;
            let summary = format!(
                "lp={} x={} y={} enabled={}",
                short_contract(&pool.swap_token.to_string()),
                pool.balance_x,
                pool.balance_y,
                pool.enabled,
            );
            Ok((Box::new(pool), summary))
        }
        PoolCfg::Velar {
            x_token, y_token, ..
        } => {
            let core: Principal = VELAR_CORE.parse()?;
            let x: Principal = x_token.parse()?;
            let y: Principal = y_token.parse()?;
            // For Velar the `pool_contract` slot is logically the core
            // (univ2-core); registry uniqueness is by id() / lp_token.
            let pool =
                fetch_velar_pool(client.clone(), &core, &core, &x, &y, token_info, tip).await?;
            let summary = format!(
                "lp={} rx={} ry={} fee={}/{} flipped={}",
                short_contract(&pool.lp_token.to_string()),
                pool.reserve_x,
                pool.reserve_y,
                pool.fee_num,
                pool.fee_den,
                pool.flipped,
            );
            Ok((Box::new(pool), summary))
        }
        PoolCfg::Alex {
            x_token, y_token, ..
        } => {
            let amm: Principal = ALEX_AMM.parse()?;
            let x: Principal = x_token.parse()?;
            let y: Principal = y_token.parse()?;
            let pool = fetch_alex_pool(client.clone(), &amm, &x, &y, token_info, tip).await?;
            let summary = format!(
                "pool-id={} x={} y={} fee_x={}/1e8",
                pool.pool_id, pool.balance_x, pool.balance_y, pool.fee_rate_x,
            );
            Ok((Box::new(pool), summary))
        }
        PoolCfg::BitflowXyk {
            pool: pool_str,
            x_token,
            y_token,
            ..
        } => {
            let pool_contract: Principal = pool_str.parse()?;
            let x: Principal = x_token.parse()?;
            let y: Principal = y_token.parse()?;
            let pool =
                fetch_bitflow_xyk_pool(client.clone(), &pool_contract, &x, &y, token_info, tip)
                    .await?;
            let summary = format!(
                "x={} y={} fee(x→y)={}bps fee(y→x)={}bps status={}",
                pool.x_balance,
                pool.y_balance,
                pool.x_fee_bps(),
                pool.y_fee_bps(),
                pool.pool_status,
            );
            Ok((Box::new(pool), summary))
        }
        PoolCfg::BitflowV2Stable {
            pool: pool_str,
            x_token,
            y_token,
            ..
        } => {
            let pool_contract: Principal = pool_str.parse()?;
            let x: Principal = x_token.parse()?;
            let y: Principal = y_token.parse()?;
            let pool = fetch_bitflow_v2_stable_pool(
                client.clone(),
                &pool_contract,
                &x,
                &y,
                token_info,
                tip,
            )
            .await?;
            let summary = format!(
                "x={} y={} amp={} midpoint={}/{} fee(x→y)={}bps status={}",
                pool.x_balance,
                pool.y_balance,
                pool.amp,
                pool.midpoint_num,
                pool.midpoint_den,
                pool.x_fee_bps(),
                pool.pool_status,
            );
            Ok((Box::new(pool), summary))
        }
        PoolCfg::BitflowV1Stable {
            pool: pool_str,
            lp_token,
            x_token,
            y_token,
            sig,
            variant,
            ..
        } => {
            let pool_contract: Principal = pool_str.parse()?;
            let lp: Principal = lp_token.parse()?;
            let x: Principal = x_token.parse()?;
            let y: Principal = y_token.parse()?;
            let sig_enum = match *sig {
                "stx-anchored" => Sig::StxAnchored,
                "token-pair" => Sig::TokenPair,
                other => {
                    return Err(anyhow!(
                        "BitflowV1Stable sig must be \"stx-anchored\" or \"token-pair\", got {:?}",
                        other
                    ))
                }
            };
            let variant_enum = MathVariant::parse(variant)?;
            let pool = fetch_bitflow_v1_stable_pool(
                client.clone(),
                &pool_contract,
                &lp,
                &x,
                &y,
                sig_enum,
                variant_enum,
                token_info,
                tip,
            )
            .await?;
            let summary = format!(
                "x={} y={} amp={} buy={}bps sell={}bps {:?}/{:?} approval={}",
                pool.x_balance,
                pool.y_balance,
                pool.amp,
                pool.buy_fee_bps,
                pool.sell_fee_bps,
                pool.sig,
                pool.variant,
                pool.approval,
            );
            Ok((Box::new(pool), summary))
        }
    }
}

/// Per-pool quote with Δ-vs-baseline and a Bitflow-only post-10bp column.
/// Uses each pool's stored decimals so output rendering is identical across
/// DEX types.
///
/// Layout: a single `[{tag}]` line, then a header row whose prefix and column
/// widths match the data rows exactly, then one line per pool. The arrow `→`
/// occupies the same character cell as the matching `→` placeholder in the
/// header so numeric columns line up under their labels.
/// Per-row data computed for the table — split out so we can do the local
/// quote synchronously (cheap), then fire on-chain quote RPCs concurrently,
/// then format the row.
struct PrintRow {
    label: &'static str,
    pool_id_truncated: String,
    quote_amount_x: f64,
    /// Mirror's local quote in raw token-y units.
    local_raw: u128,
    /// On-chain `get-dy` (or equivalent) result for the same `dx`, raw units.
    /// `None` for pool types without an on-chain quoter (Velar, Arkadiko).
    /// `Err` for pools whose quoter call failed (logged inline).
    onchain_raw: Option<Result<u128>>,
    is_bitflow: bool,
    /// y-side decimals for human-readable formatting.
    y_decimals: u8,
    /// Δ vs previous local raw quote — for the trailing `Δ` annotation.
    local_delta: Option<i128>,
}

/// Per-pool quote with Δ-vs-baseline, post-10bp Bitflow aggregator column,
/// AND an `on-chain` column that calls each pool's on-chain `get-dy` (or
/// equivalent) in parallel for direct cross-check.
///
/// `Δ%` is signed `(onchain - local) / local * 100`. A persistent non-zero
/// Δ% on any pool means our mirror has drifted from the chain — almost
/// certainly an event-apply bug (or bootstrap snapshot lag).
async fn print_block(
    client: Arc<StacksRpcClient>,
    registry: &Arc<PoolRegistry>,
    rows: &mut [Row],
    tag: &str,
) {
    println!("[{}]", tag);
    println!(
        "  {:<24} {:>22}   {:>12}   {:>14}   {:>14}   {:>14}   {:>8}",
        "pool", "id (truncated)", "in", "raw out", "post-10bp", "on-chain", "Δ%"
    );

    // ── Phase 1 (sync): gather pool snapshots + compute local quotes ──
    let mut prepared: Vec<Option<(usize, u128, PrintRow)>> = Vec::with_capacity(rows.len());
    let mut onchain_calls: Vec<(usize, PoolOnchainCall, u128)> = Vec::new();

    for (idx, row) in rows.iter_mut().enumerate() {
        let Some(handle) = registry.get(&row.pool_id) else {
            println!("  {:<24} (not in registry?)", row.label);
            prepared.push(None);
            continue;
        };
        let g = match handle.try_read() {
            Ok(g) => g,
            Err(_) => {
                println!("  {:<24} (locked — try next tick)", row.label);
                prepared.push(None);
                continue;
            }
        };
        let (x_token, _y_token) = g.tokens();
        let (x_decimals, y_decimals) = pool_decimals(&**g);
        let amount_raw = (row.quote_amount_x * 10f64.powi(x_decimals as i32)) as u128;
        let local_raw = g.calculate_output(x_token, amount_raw).unwrap_or(0);
        let local_delta = row.last_quote.map(|prev| local_raw as i128 - prev as i128);
        row.last_quote = Some(local_raw);

        // Build on-chain call spec while we still hold the read lock.
        if let Some(call) = build_onchain_call(&**g) {
            onchain_calls.push((idx, call, amount_raw));
        }

        prepared.push(Some((
            idx,
            amount_raw,
            PrintRow {
                label: row.label,
                pool_id_truncated: truncate(&row.pool_id, 22),
                quote_amount_x: row.quote_amount_x,
                local_raw,
                onchain_raw: None,
                is_bitflow: row.is_bitflow,
                y_decimals,
                local_delta,
            },
        )));
        // Drop the registry lock before we make any RPC calls.
    }

    // ── Phase 2 (async): fan out on-chain quote RPCs in parallel ──
    let onchain_futures = onchain_calls
        .into_iter()
        .map(|(idx, call, dx)| {
            let client = client.clone();
            async move { (idx, run_onchain_call(client, call, dx).await) }
        })
        .collect::<Vec<_>>();
    let results: Vec<(usize, Result<u128>)> = futures_util::future::join_all(onchain_futures).await;
    let mut by_idx: std::collections::HashMap<usize, Result<u128>> = results.into_iter().collect();

    // ── Phase 3: stitch on-chain results onto prepared rows, print ──
    for slot in prepared.into_iter() {
        let Some((idx, _dx, mut pr)) = slot else {
            continue;
        };
        if let Some(r) = by_idx.remove(&idx) {
            pr.onchain_raw = Some(r);
        }
        print_one(&pr);
    }
    println!();
}

fn print_one(pr: &PrintRow) {
    let scale = 10f64.powi(pr.y_decimals as i32);
    let local_human = pr.local_raw as f64 / scale;
    let post_fee_col = if pr.is_bitflow {
        let post_fee = apply_aggregator_fee(pr.local_raw, AGGREGATOR_FEE_BPS);
        format!("{:>14.6}", post_fee as f64 / scale)
    } else {
        // Non-Bitflow pools aren't consumed through Bitflow's aggregator,
        // so the 10bp deduction is meaningless — placeholder dash.
        format!("{:>14}", "—")
    };
    let (onchain_col, delta_pct_col) = match &pr.onchain_raw {
        Some(Ok(oc_raw)) => {
            let oc_human = *oc_raw as f64 / scale;
            let pct = if pr.local_raw == 0 {
                f64::NAN
            } else {
                (*oc_raw as f64 - pr.local_raw as f64) / pr.local_raw as f64 * 100.0
            };
            let pct_str = if pct.is_nan() {
                format!("{:>8}", "—")
            } else {
                // Surface anything ≥ 0.01% (one bp) so genuine mismatches
                // pop out. Smaller deltas are Newton-Raphson rounding.
                format!("{:>+8.4}", pct)
            };
            (format!("{:>14.6}", oc_human), pct_str)
        }
        Some(Err(e)) => {
            // Log the underlying error once per row so we know which call
            // failed without spamming. Keep the column aligned.
            log::warn!("on-chain quote error for {}: {}", pr.label, e);
            (format!("{:>14}", "err"), format!("{:>8}", "—"))
        }
        None => (format!("{:>14}", "—"), format!("{:>8}", "—")),
    };
    let delta_annotation = match pr.local_delta {
        Some(0) | None => String::new(),
        Some(d) => format!("  Δ {:+}", d),
    };
    println!(
        "  {:<24} {:>22}   {:>12.4} → {:>14.6}   {}   {}   {}{}",
        pr.label,
        pr.pool_id_truncated,
        pr.quote_amount_x,
        local_human,
        post_fee_col,
        onchain_col,
        delta_pct_col,
        delta_annotation,
    );
}

/// Specification of an on-chain quote call. Captured while we hold the
/// registry's read lock, then executed in [`run_onchain_call`] without it.
///
/// DLMM is intentionally absent: there's no on-chain `get-dy` (verified by
/// reading the dlmm-pool / dlmm-core / dlmm-pool-multi-helper ABIs in
/// 2026-05). DLMM math runs client-side on both sides (our mirror + the
/// Bitflow FE), each over the same on-chain bin state. Mirror correctness
/// is verified by [`tests/dlmm_reconcile_live`] instead.
enum PoolOnchainCall {
    /// ALEX: `<amm_deployer>.<amm_name>::get-y-given-x(x, y, factor, dx_net)`.
    /// `get-y-given-x` is FEE-LESS on-chain, so we pre-deduct the input-side
    /// fee here (`fee = mul_up(dx, fee_rate_x)`) to compare apples-to-apples
    /// with our local `quote_x_for_y` (which folds the fee in internally).
    Alex {
        amm_deployer: String,
        amm_name: String,
        x_token: Principal,
        y_token: Principal,
        factor: u128,
        /// 8-dp fixed-point input-side fee rate (e.g. 0.005 = 500_000).
        fee_rate_x: u128,
    },
    /// Bitflow XYK / V2 Stable: `<core>::get-dy(pool, x-token, y-token, dx)`
    /// where the pool is passed as a trait reference (encoded as a contract
    /// principal).
    BitflowCoreGetDy {
        core_deployer: String,
        core_name: String,
        pool_contract: Principal,
        x_token: Principal,
        y_token: Principal,
    },
    /// Bitflow V1 Stable: dual ABI on the pool itself.
    BitflowV1StableStxAnchored {
        pool_deployer: String,
        pool_name: String,
        y_token: Principal,
        lp_token: Principal,
    },
    BitflowV1StableTokenPair {
        pool_deployer: String,
        pool_name: String,
        x_token: Principal,
        y_token: Principal,
        lp_token: Principal,
    },
}

fn build_onchain_call(pool: &dyn PoolInterface) -> Option<PoolOnchainCall> {
    // DLMM intentionally has no entry — multi-bin walk has no single-call
    // on-chain quoter. See `PoolOnchainCall`'s docstring.
    if let Some(p) = pool.as_any().downcast_ref::<AlexPool>() {
        let (d, n) = split_principal(&p.pool_contract).ok()?;
        return Some(PoolOnchainCall::Alex {
            amm_deployer: d,
            amm_name: n,
            x_token: p.x_token.clone(),
            y_token: p.y_token.clone(),
            factor: p.factor,
            fee_rate_x: p.fee_rate_x,
        });
    }
    if let Some(p) = pool.as_any().downcast_ref::<BitflowXykPool>() {
        let (d, n) = split_principal(&p.core_contract).ok()?;
        return Some(PoolOnchainCall::BitflowCoreGetDy {
            core_deployer: d,
            core_name: n,
            pool_contract: p.pool_contract.clone(),
            x_token: p.x_token.clone(),
            y_token: p.y_token.clone(),
        });
    }
    if let Some(p) = pool.as_any().downcast_ref::<BitflowStableSwapV2Pool>() {
        let (d, n) = split_principal(&p.core_contract).ok()?;
        return Some(PoolOnchainCall::BitflowCoreGetDy {
            core_deployer: d,
            core_name: n,
            pool_contract: p.pool_contract.clone(),
            x_token: p.x_token.clone(),
            y_token: p.y_token.clone(),
        });
    }
    if let Some(p) = pool.as_any().downcast_ref::<BitflowStableSwapV1Pool>() {
        let (d, n) = split_principal(&p.pool_contract).ok()?;
        return Some(match p.sig {
            Sig::StxAnchored => PoolOnchainCall::BitflowV1StableStxAnchored {
                pool_deployer: d,
                pool_name: n,
                y_token: p.y_token.clone(),
                lp_token: p.lp_token.clone(),
            },
            Sig::TokenPair => PoolOnchainCall::BitflowV1StableTokenPair {
                pool_deployer: d,
                pool_name: n,
                x_token: p.x_token.clone(),
                y_token: p.y_token.clone(),
                lp_token: p.lp_token.clone(),
            },
        });
    }
    // Velar + Arkadiko: no on-chain `get-dy`.
    None
}

async fn run_onchain_call(
    client: Arc<StacksRpcClient>,
    call: PoolOnchainCall,
    dx: u128,
) -> Result<u128> {
    match call {
        PoolOnchainCall::Alex {
            amm_deployer,
            amm_name,
            x_token,
            y_token,
            factor,
            fee_rate_x,
        } => {
            // ALEX's `get-y-given-x` is fee-less; the actual swap fn does
            // `dx_net = dx - mul_up(dx, fee_rate_x)` first. Mirror that here
            // so the on-chain answer matches our local fee-folded quote.
            let fee = stacks_dex_pools::v2::alex::mul_up(dx, fee_rate_x);
            let dx_net = dx.saturating_sub(fee);
            let res = client
                .call_read(
                    &amm_deployer,
                    &amm_name,
                    "get-y-given-x",
                    &[
                        cv_principal(&x_token),
                        cv_principal(&y_token),
                        cv_uint(factor),
                        cv_uint(dx_net),
                    ],
                    None,
                )
                .await?
                .unwrap_ok()?;
            res.as_uint()
        }
        PoolOnchainCall::BitflowCoreGetDy {
            core_deployer,
            core_name,
            pool_contract,
            x_token,
            y_token,
        } => {
            // The pool-trait argument encodes as a contract principal — the
            // chain resolves trait conformance against the pool's contract.
            let res = client
                .call_read(
                    &core_deployer,
                    &core_name,
                    "get-dy",
                    &[
                        cv_principal(&pool_contract),
                        cv_principal(&x_token),
                        cv_principal(&y_token),
                        cv_uint(dx),
                    ],
                    None,
                )
                .await?
                .unwrap_ok()?;
            res.as_uint()
        }
        PoolOnchainCall::BitflowV1StableStxAnchored {
            pool_deployer,
            pool_name,
            y_token,
            lp_token,
        } => {
            let res = client
                .call_read(
                    &pool_deployer,
                    &pool_name,
                    "get-dy",
                    &[cv_principal(&y_token), cv_principal(&lp_token), cv_uint(dx)],
                    None,
                )
                .await?
                .unwrap_ok()?;
            res.as_uint()
        }
        PoolOnchainCall::BitflowV1StableTokenPair {
            pool_deployer,
            pool_name,
            x_token,
            y_token,
            lp_token,
        } => {
            let res = client
                .call_read(
                    &pool_deployer,
                    &pool_name,
                    "get-dy",
                    &[
                        cv_principal(&x_token),
                        cv_principal(&y_token),
                        cv_principal(&lp_token),
                        cv_uint(dx),
                    ],
                    None,
                )
                .await?
                .unwrap_ok()?;
            res.as_uint()
        }
    }
}

/// Split a `Principal::Contract` into `(deployer_c32, contract_name)` strings.
fn split_principal(p: &Principal) -> Result<(String, String)> {
    match p {
        Principal::Contract {
            version,
            hash160,
            name,
        } => Ok((c32_encode_address(*version, hash160), name.clone())),
        Principal::Standard { .. } => {
            Err(anyhow!("expected contract principal, got standard: {}", p))
        }
    }
}

/// Pull `(x_decimals, y_decimals)` off any of the implemented pool types.
/// The PoolInterface trait doesn't expose decimals directly, so downcast.
fn pool_decimals(pool: &dyn PoolInterface) -> (u8, u8) {
    use stacks_dex_pools::dlmm::DLMMPool;
    use stacks_dex_pools::stableswap::{BitflowStableSwapV1Pool, BitflowStableSwapV2Pool};
    use stacks_dex_pools::v2::{AlexPool, ArkadikoPool, BitflowXykPool, VelarPool};
    if let Some(p) = pool.as_any().downcast_ref::<DLMMPool>() {
        return (p.x_decimals, p.y_decimals);
    }
    if let Some(p) = pool.as_any().downcast_ref::<ArkadikoPool>() {
        return (p.x_decimals, p.y_decimals);
    }
    if let Some(p) = pool.as_any().downcast_ref::<VelarPool>() {
        return (p.x_decimals, p.y_decimals);
    }
    if let Some(p) = pool.as_any().downcast_ref::<AlexPool>() {
        return (p.x_decimals, p.y_decimals);
    }
    if let Some(p) = pool.as_any().downcast_ref::<BitflowXykPool>() {
        return (p.x_decimals, p.y_decimals);
    }
    if let Some(p) = pool.as_any().downcast_ref::<BitflowStableSwapV2Pool>() {
        return (p.x_decimals, p.y_decimals);
    }
    if let Some(p) = pool.as_any().downcast_ref::<BitflowStableSwapV1Pool>() {
        return (p.x_decimals, p.y_decimals);
    }
    (6, 6) // sensible default
}

fn short_contract(id: &str) -> &str {
    id.split_once('.').map(|x| x.1).unwrap_or(id)
}

/// Truncate `s` to at most `max` chars, appending `…` if it was longer.
fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}

/// Bitflow's aggregator deducts `bps` basis points on top of the pool fee.
/// Returns `dy * (10000 - bps) / 10000`, with `bps == 0` short-circuited.
fn apply_aggregator_fee(dy: u128, bps: u32) -> u128 {
    if bps == 0 {
        dy
    } else {
        dy.saturating_mul((10_000 - bps) as u128) / 10_000
    }
}

fn install_ctrlc(shutdown: Arc<AtomicBool>) {
    let s = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("\n(received Ctrl-C, shutting down…)");
        s.store(true, Ordering::Relaxed);
    });
}

/// Fetch the chain tip's `(stacks_tip_height, index_block_hash)` from
/// `<events_host>/v2/info`. Retries on transient JSON parse failures.
async fn fetch_chain_tip(http: &Client, events_host: &str) -> Result<(u64, String)> {
    for attempt in 0..3 {
        let url = format!("{}/v2/info", events_host);
        let resp = http
            .get(&url)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(|e| anyhow!("/v2/info GET error: {}", e))?;
        let body = resp.text().await?;
        match serde_json::from_str::<serde_json::Value>(&body) {
            Ok(v) => {
                let h = v
                    .get("stacks_tip_height")
                    .and_then(|x| x.as_u64())
                    .ok_or_else(|| anyhow!("/v2/info missing stacks_tip_height"))?;
                let hash = v
                    .get("stacks_tip")
                    .and_then(|x| x.as_str())
                    .or_else(|| v.get("stacks_tip_hash").and_then(|x| x.as_str()))
                    .ok_or_else(|| anyhow!("/v2/info missing stacks_tip"))?
                    .to_string();
                let hash = hash.trim_start_matches("0x").to_string();
                return Ok((h, hash));
            }
            Err(e) if attempt < 2 => {
                let preview: String = body.chars().take(120).collect();
                eprintln!(
                    "(warn) /v2/info JSON parse failed: {} (body: {}…); retry…",
                    e, preview
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            Err(e) => return Err(anyhow!("/v2/info JSON parse error: {}", e)),
        }
    }
    Err(anyhow!("/v2/info failed after retries"))
}
