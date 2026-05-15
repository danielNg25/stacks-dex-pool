//! Live, long-running collector test against all 8 Bitflow DLMM pools.
//!
//! Designed to be run alongside Bitflow's UI: every 30s the binary prints a
//! quote per pool; you eyeball the UI for the same pair and confirm the
//! numbers match (within the 10bp aggregator fee — both raw and post-fee
//! columns are printed for direct comparison).
//!
//! Consistency model:
//!   1. Query Stacks for the current chain tip once at startup. All pools
//!      are bootstrapped with `?tip=<index_block_hash>` pinned to THAT block,
//!      so the snapshot is internally consistent — every pool reflects the
//!      same chain state.
//!   2. For each polled contract (pool + shared core), walk recent events
//!      backwards looking up tx block heights; set the watermark to the
//!      newest event AT OR BEFORE the snapshot block. The collector will
//!      only apply events STRICTLY NEWER than the snapshot from then on —
//!      no stale events corrupting fresh state.
//!   3. Run forever. Every 30s, print per-pool quote. SIGINT triggers
//!      graceful shutdown (stops collector, awaits tasks).
//!
//! Usage:
//!   cargo run --bin test_all_dlmm --features collector --release -- \
//!       --rpc-host https://node.bitflowapis.finance
//!
//!   # Single pool for fast feedback:
//!   cargo run --bin test_all_dlmm --features collector --release -- \
//!       --pool dlmm-pool-stx-usdcx-v-1-bps-10 \
//!       --rpc-host https://node.bitflowapis.finance \
//!       --log-interval 30 --poll-interval 5

#![cfg(feature = "collector")]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use reqwest::Client;
use stacks_dex_pools::collector::{start_collector, CollectorConfig};
#[cfg(feature = "block_walking")]
use stacks_dex_pools::collector::{
    start_collector_with_source, BlockWalkingConfig, BlockWalkingEventSource, EventSource,
};
use stacks_dex_pools::dlmm::fetcher::{fetch_dlmm_pool, BootstrapMode};
use stacks_dex_pools::dlmm::DLMMPool;
use stacks_dex_pools::pool::base::PoolInterface;
use stacks_dex_pools::pool::principal::Principal;
use stacks_dex_pools::registry::PoolRegistry;
use stacks_dex_pools::rpc::client::{RpcConfig, StacksRpcClient};
use stacks_dex_pools::rpc::events::{fetch_events_page, fetch_tx_block_height};
use stacks_dex_pools::token_info::StacksTokenInfo;

const POOL_DEPLOYER: &str = "SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD";
const CORE: &str = "SP1PFR4V08H1RAZXREBGFFQ59WB739XM8VVGTFSEA.dlmm-core-v-1-1";

/// Default events RPC. Hiro mirrors `/extended/v1/*` (per-contract polling)
/// and `/extended/v2/*` (block-walking) — Bitflow's node doesn't, so events
/// must go through Hiro (or another extended-API mirror).
const DEFAULT_EVENTS_HOST: &str = "https://api.mainnet.hiro.so";

/// Default bootstrap RPC. Bitflow's node has the read-budget headroom DLMM's
/// chunked multicall needs (Hiro public 404s on `?tip=` AND caps read_length
/// at 500KB). Override with `--rpc-host` for self-hosted nodes.
const DEFAULT_BOOTSTRAP_HOST: &str = "https://node.bitflowapis.finance";
/// Bitflow FE deducts a 10bp aggregator fee on top of pool fees in displayed
/// quotes. We print both raw pool quote and post-aggregator for UI comparison.
const AGGREGATOR_FEE_BPS: u32 = 10;

struct PoolUnderTest {
    pool_name: &'static str,
    label: &'static str,
    /// Single fixed amount used for the recurring quote during the live loop,
    /// in token-X human units. Picked to be representative of a typical trade
    /// for that pair.
    quote_amount: f64,
}

const KNOWN_POOLS: &[PoolUnderTest] = &[
    PoolUnderTest {
        pool_name: "dlmm-pool-stx-usdcx-v-1-bps-1",
        label: "STX→USDCx-1",
        quote_amount: 100.0,
    },
    PoolUnderTest {
        pool_name: "dlmm-pool-stx-usdcx-v-1-bps-4",
        label: "STX→USDCx-4",
        quote_amount: 100.0,
    },
    PoolUnderTest {
        pool_name: "dlmm-pool-stx-usdcx-v-1-bps-10",
        label: "STX→USDCx-10",
        quote_amount: 100.0,
    },
    PoolUnderTest {
        pool_name: "dlmm-pool-sbtc-usdcx-v-1-bps-1",
        label: "sBTC→USDCx-1",
        quote_amount: 0.01,
    },
    PoolUnderTest {
        pool_name: "dlmm-pool-sbtc-usdcx-v-1-bps-10",
        label: "sBTC→USDCx-10",
        quote_amount: 0.01,
    },
    PoolUnderTest {
        pool_name: "dlmm-pool-stx-sbtc-v-1-bps-15",
        label: "STX→sBTC-15",
        quote_amount: 1000.0,
    },
    PoolUnderTest {
        pool_name: "dlmm-pool-aeusdc-usdcx-v-1-bps-1",
        label: "aeUSDC→USDCx",
        quote_amount: 100.0,
    },
    PoolUnderTest {
        pool_name: "dlmm-pool-usdh-usdcx-v-1-bps-1",
        label: "USDh→USDCx",
        quote_amount: 100.0,
    },
];

struct CliArgs {
    /// Bootstrap RPC (call-read). Default: Bitflow node.
    rpc_host: String,
    /// Events RPC (extended-v1/v2). Default: Hiro public.
    events_host: String,
    only: Option<String>,
    log_interval_s: u64,
    poll_interval_s: u64,
    source: SourceKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceKind {
    PerContract,
    /// Only registrable when the `block_walking` feature is enabled.
    #[cfg(feature = "block_walking")]
    BlockWalking,
}

fn parse_args() -> CliArgs {
    // Default poll_interval is sized for the 8-pool sweep (9 polled contracts:
    // 8 pools + 1 shared core). Hiro's free tier rate-limits at ~50 req/min;
    // with 9 contracts polled, 12s/cycle ≈ 45 req/min — just under the
    // ceiling. Single-pool runs (2 contracts) can safely drop to 5s.
    let mut args = CliArgs {
        rpc_host: DEFAULT_BOOTSTRAP_HOST.to_string(),
        events_host: DEFAULT_EVENTS_HOST.to_string(),
        only: None,
        log_interval_s: 30,
        poll_interval_s: 12,
        source: SourceKind::PerContract,
    };
    let mut argv = std::env::args().skip(1);
    while let Some(a) = argv.next() {
        match a.as_str() {
            "--rpc-host" => args.rpc_host = argv.next().expect("--rpc-host needs a value"),
            "--events-host" => args.events_host = argv.next().expect("--events-host needs a value"),
            "--pool" => args.only = Some(argv.next().expect("--pool needs a value")),
            "--log-interval" => {
                args.log_interval_s = argv
                    .next()
                    .expect("--log-interval needs a value")
                    .parse()
                    .expect("log-interval must be u64")
            }
            "--poll-interval" => {
                args.poll_interval_s = argv
                    .next()
                    .expect("--poll-interval needs a value")
                    .parse()
                    .expect("poll-interval must be u64")
            }
            "--source" => {
                let v = argv.next().expect("--source needs a value");
                args.source = match v.as_str() {
                    "per-contract" => SourceKind::PerContract,
                    #[cfg(feature = "block_walking")]
                    "block-walking" => SourceKind::BlockWalking,
                    #[cfg(not(feature = "block_walking"))]
                    "block-walking" => {
                        eprintln!(
                            "(error) --source block-walking requires the `block_walking` cargo feature. Rebuild with --features block_walking."
                        );
                        std::process::exit(2);
                    }
                    other => {
                        eprintln!(
                            "(error) --source must be one of: per-contract | block-walking (got {:?})",
                            other
                        );
                        std::process::exit(2);
                    }
                };
            }
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => eprintln!("(warning) ignoring unknown arg {:?}", other),
        }
    }
    args
}

fn print_help() {
    println!(
        "test_all_dlmm — bootstrap all 8 Bitflow DLMM pools at a single block,\n\
         seed event watermarks at that block, run the collector forever, and\n\
         log per-pool quotes every 30s for live UI comparison.\n\
         \n\
         Flags:\n\
           --rpc-host <url>        Bootstrap RPC (call-read).\n\
                                   Default: https://node.bitflowapis.finance\n\
                                   (Bitflow's node — has the read budget DLMM needs\n\
                                    AND honours `?tip=` for consistent snapshots).\n\
           --events-host <url>     Events RPC (extended-v1/v2 + /v2/info).\n\
                                   Default: https://api.mainnet.hiro.so\n\
                                   (Bitflow's node doesn't mirror /extended/*.)\n\
           --pool <name>           Only probe this specific pool contract.\n\
           --log-interval <secs>   How often to print per-pool quote (default: 30).\n\
           --poll-interval <secs>  Collector's event-poll cadence (default: 12).\n\
                                   Drop to 5 for single-pool tests; raise to 20+\n\
                                   for multi-pool to stay under Hiro 50req/min.\n\
           --source <kind>         Event-ingestion strategy:\n\
                                     per-contract  (default) — one /events poller per contract.\n\
                                     block-walking — single /v2/info + /extended/v2/blocks walker.\n\
                                     Block-walking is constant-cost regardless of pool count\n\
                                     and skips watermark seeding (uses last_processed_block).\n\
         \n\
         Runs forever. Ctrl-C triggers graceful shutdown.\n\
         \n\
         Known pools:"
    );
    for p in KNOWN_POOLS {
        println!("   {:<40}  {}", p.pool_name, p.label);
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // Default to info-level logging so collector activity is visible. Override
    // with `RUST_LOG=debug` for poll-cycle detail, `RUST_LOG=warn` for quiet.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_secs()
        .format_target(false)
        .init();

    let args = parse_args();
    let core_contract: Principal = CORE.parse()?;

    let rpc_config = RpcConfig {
        base_url: args.rpc_host.clone(),
        max_retries: 5,
        ..Default::default()
    };
    let client = Arc::new(StacksRpcClient::new(rpc_config)?);
    let token_info = StacksTokenInfo::new(client.clone());
    // Separate HTTP client for /extended/v1/* — same retry/timeout config but
    // always pointed at Hiro.
    let events_http = Client::builder().timeout(Duration::from_secs(20)).build()?;

    let pools_to_probe: Vec<&PoolUnderTest> = match &args.only {
        Some(name) => KNOWN_POOLS
            .iter()
            .filter(|p| p.pool_name == *name)
            .collect(),
        None => KNOWN_POOLS.iter().collect(),
    };
    if pools_to_probe.is_empty() {
        eprintln!("no pools matched --pool filter; try --help");
        std::process::exit(2);
    }

    // ── Phase 1: snapshot the chain tip ──────────────────────────────────
    println!("[setup] fetching current chain tip from Hiro…");
    let (snapshot_height, snapshot_hash) = fetch_chain_tip(&events_http, &args.events_host).await?;
    println!(
        "[setup] snapshot tip = block {} (0x{}…)",
        snapshot_height,
        &snapshot_hash[..16]
    );
    println!(
        "[setup] bootstrap host = {} | events host = {}",
        args.rpc_host, args.events_host
    );
    println!(
        "[setup] {} pool(s) in FULL mode (chunked multicall) pinned to that block",
        pools_to_probe.len()
    );

    // ── Phase 2: bootstrap every pool at the same block ──────────────────
    let registry = Arc::new(PoolRegistry::new());
    let mut quote_amounts: Vec<f64> = Vec::new();
    let mut pool_labels: Vec<String> = Vec::new();
    let mut last_quote: Vec<Option<u128>> = Vec::new();
    let mut last_active: Vec<i32> = Vec::new();
    let mut pool_contracts: Vec<Principal> = Vec::new();

    for entry in &pools_to_probe {
        let pool_contract: Principal = format!("{}.{}", POOL_DEPLOYER, entry.pool_name).parse()?;
        let started = Instant::now();
        let pool = match fetch_dlmm_pool(
            client.clone(),
            &pool_contract,
            &core_contract,
            &token_info,
            BootstrapMode::Full,
            8,
            Some(&snapshot_hash),
        )
        .await
        {
            Ok(p) => p,
            Err(e) => {
                eprintln!("✗ {} — bootstrap failed: {}", entry.label, e);
                std::process::exit(3);
            }
        };
        let elapsed = started.elapsed();
        println!(
            "[bootstrap] {:<14} active={:+4} bin_step={:>3}bps non_empty={:>4} fees(x→y)={:>3}bps {:.1}s",
            entry.label,
            pool.active_bin_id,
            pool.bin_step,
            pool.bins.len(),
            pool.x_fee_bps(),
            elapsed.as_secs_f64(),
        );

        quote_amounts.push(entry.quote_amount);
        pool_labels.push(entry.label.to_string());
        last_quote.push(None);
        last_active.push(pool.active_bin_id);
        pool_contracts.push(pool_contract.clone());

        let boxed: Box<dyn PoolInterface + Send + Sync> = Box::new(pool);
        registry.insert(boxed);
    }

    registry.set_last_processed_block(snapshot_height);
    println!(
        "[bootstrap] {} pool(s) registered, all at block {}",
        registry.len(),
        snapshot_height
    );

    // ── Phase 3: seed event watermarks at the snapshot block ─────────────
    // Watermark seeding is only meaningful for the per-contract source;
    // block-walking uses `registry.last_processed_block()` (already set) as
    // its cursor. When the `block_walking` feature is disabled, this branch
    // is unconditionally true (`SourceKind` has only `PerContract`).
    let needs_watermark_seeding = {
        #[cfg(feature = "block_walking")]
        {
            args.source == SourceKind::PerContract
        }
        #[cfg(not(feature = "block_walking"))]
        {
            true
        }
    };
    if needs_watermark_seeding {
        // For each polled contract (pools + the shared core), walk events
        // newest-first looking up tx block_heights. Stop at the first event
        // whose block_height is <= snapshot_height; that event's tx_id becomes
        // the watermark. The collector will then only apply events STRICTLY
        // NEWER than this snapshot.
        let polled: Vec<Principal> = registry.polled_contracts();
        println!(
            "[watermarks] seeding {} contract watermark(s) at block {}…",
            polled.len(),
            snapshot_height
        );
        for contract in &polled {
            match find_watermark_at_block(
                &events_http,
                &args.events_host,
                &contract.to_string(),
                snapshot_height,
            )
            .await
            {
                Ok(Some(tx_id)) => {
                    let short = tx_id.chars().take(14).collect::<String>();
                    println!(
                        "[watermarks] {:<60}  → {}…",
                        truncate(&contract.to_string(), 60),
                        short
                    );
                    registry.set_watermark(contract, tx_id);
                }
                Ok(None) => {
                    println!(
                        "[watermarks] {:<60}  → (no events at/before snapshot — collector cold-start)",
                        truncate(&contract.to_string(), 60),
                    );
                }
                Err(e) => {
                    eprintln!(
                        "[watermarks] {:<60}  ✗ {} (collector will cold-start this stream)",
                        truncate(&contract.to_string(), 60),
                        e
                    );
                }
            }
        }
    } else {
        println!(
            "[watermarks] skipped — block-walking source uses last_processed_block={} cursor",
            snapshot_height
        );
    }

    // ── Phase 4: start the collector ─────────────────────────────────────
    println!(
        "\n[collector] starting — source={:?}, poll_interval={}s, log_interval={}s",
        args.source, args.poll_interval_s, args.log_interval_s
    );
    let handle = match args.source {
        SourceKind::PerContract => {
            let collector_config = CollectorConfig {
                poll_interval: Duration::from_secs(args.poll_interval_s),
                ..Default::default()
            };
            start_collector(
                args.events_host.clone(),
                registry.clone(),
                collector_config,
                None,
            )
            .await?
        }
        #[cfg(feature = "block_walking")]
        SourceKind::BlockWalking => {
            // Reuse the events HTTP client we already built above.
            let http = Arc::new(events_http.clone());
            let bw_cfg = BlockWalkingConfig {
                poll_interval: Duration::from_secs(args.poll_interval_s),
                ..Default::default()
            };
            // Tip via bootstrap RPC (no /v2/info rate-limit on Bitflow's
            // node), events via the events host (Bitflow mirrors /extended/*
            // too, so a single host can serve both).
            let source: Arc<dyn EventSource> =
                Arc::new(BlockWalkingEventSource::with_separate_tip(
                    args.events_host.clone(),
                    args.rpc_host.clone(),
                    http,
                    bw_cfg,
                ));
            // CollectorConfig.poll_interval is irrelevant here (the source has
            // its own); leave the rest at defaults.
            start_collector_with_source(
                args.events_host.clone(),
                registry.clone(),
                CollectorConfig::default(),
                None,
                source,
            )
            .await?
        }
    };

    // ── Phase 5: install Ctrl-C handler and loop forever ─────────────────
    let shutdown = Arc::new(AtomicBool::new(false));
    install_ctrlc(shutdown.clone());
    println!(
        "[collector] running. Ctrl-C to stop. Logging every {}s.\n",
        args.log_interval_s
    );

    // Baseline log so the user sees opening quotes before the first interval.
    print_quote_block(
        "baseline",
        &registry,
        &pool_contracts,
        &pool_labels,
        &quote_amounts,
        &mut last_quote,
        &mut last_active,
    );

    let log_interval = Duration::from_secs(args.log_interval_s);
    let mut next_log = Instant::now() + log_interval;
    while !shutdown.load(Ordering::Relaxed) {
        let now = Instant::now();
        if now >= next_log {
            let elapsed = next_log.saturating_duration_since(Instant::now()); // 0 once we're here
            let _ = elapsed;
            let tag = chrono::Local::now().format("%H:%M:%S").to_string();
            print_quote_block(
                &tag,
                &registry,
                &pool_contracts,
                &pool_labels,
                &quote_amounts,
                &mut last_quote,
                &mut last_active,
            );
            next_log = Instant::now() + log_interval;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    println!("\n[collector] shutdown requested, stopping…");
    handle.stop().await;
    println!("[done] graceful shutdown complete");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────

/// Get (stacks_tip_height, index_block_hash) for the current chain tip.
///
/// Retries on transient failures (Hiro occasionally returns a non-JSON 502
/// gateway page during cluster restarts).
async fn fetch_chain_tip(http: &Client, events_host: &str) -> Result<(u64, String)> {
    let info = fetch_json_with_retry(http, &format!("{}/v2/info", events_host), "/v2/info").await?;
    let height = info
        .get("stacks_tip_height")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("/v2/info missing stacks_tip_height"))?;
    let block = fetch_json_with_retry(
        http,
        &format!("{}/extended/v2/blocks/{}", events_host, height),
        "/extended/v2/blocks",
    )
    .await?;
    let hash = block
        .get("index_block_hash")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("block response missing index_block_hash"))?
        .strip_prefix("0x")
        .unwrap_or_default()
        .to_string();
    Ok((height, hash))
}

async fn fetch_json_with_retry(http: &Client, url: &str, label: &str) -> Result<serde_json::Value> {
    let mut attempt = 0;
    let max_attempts = 3;
    loop {
        attempt += 1;
        let resp = match http.get(url).send().await {
            Ok(r) => r,
            Err(e) if attempt < max_attempts => {
                log::warn!(
                    "{} attempt {}/{} failed: {}",
                    label,
                    attempt,
                    max_attempts,
                    e
                );
                tokio::time::sleep(Duration::from_secs(2 * attempt as u64)).await;
                continue;
            }
            Err(e) => return Err(anyhow!("{}: {}", label, e)),
        };
        let status = resp.status();
        let body = match resp.text().await {
            Ok(t) => t,
            Err(e) => return Err(anyhow!("{}: read body: {}", label, e)),
        };
        if !status.is_success() {
            if attempt < max_attempts {
                log::warn!(
                    "{} attempt {}/{} got HTTP {}: {}",
                    label,
                    attempt,
                    max_attempts,
                    status,
                    &body.chars().take(100).collect::<String>()
                );
                tokio::time::sleep(Duration::from_secs(2 * attempt as u64)).await;
                continue;
            }
            return Err(anyhow!(
                "{}: HTTP {} body={:?}",
                label,
                status,
                &body[..body.len().min(200)]
            ));
        }
        match serde_json::from_str(&body) {
            Ok(v) => return Ok(v),
            Err(e) if attempt < max_attempts => {
                log::warn!(
                    "{} attempt {}/{} JSON parse failed: {} body_preview={:?}",
                    label,
                    attempt,
                    max_attempts,
                    e,
                    &body[..body.len().min(120)]
                );
                tokio::time::sleep(Duration::from_secs(2 * attempt as u64)).await;
            }
            Err(e) => {
                return Err(anyhow!(
                    "{}: JSON parse failed after {} attempts: {} body_preview={:?}",
                    label,
                    max_attempts,
                    e,
                    &body[..body.len().min(200)]
                ));
            }
        }
    }
}

/// For `contract_id`, walk events newest-first looking up each tx's
/// `block_height` until we find one with `block_height <= snapshot_height`.
/// Return that tx_id. If no such event exists in our walk window, return None
/// (the collector will cold-start from there — same as before).
///
/// Bounded walk: up to 2 pages × 50 events = 100 events back. With 250ms
/// pacing between tx-block lookups to stay under Hiro's ~50req/min limit
/// (we issue 9 contracts × N lookups serially across all of them).
///
/// We dedupe by tx_id — a single tx often emits multiple events for one
/// contract (e.g. an `add-liquidity` tx emits one `update-bin-balances` per
/// bin it touched). Looking up the same tx N times is wasteful AND counts
/// against rate limit.
async fn find_watermark_at_block(
    http: &Client,
    base: &str,
    contract_id: &str,
    snapshot_height: u64,
) -> Result<Option<String>> {
    const MAX_PAGES: u32 = 2;
    const PACING: Duration = Duration::from_millis(250);
    let mut tx_height_cache: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();
    for page_idx in 0..MAX_PAGES {
        let page = fetch_events_page(http, base, contract_id, 50, page_idx * 50).await?;
        if page.is_empty() {
            return Ok(None);
        }
        for env in page {
            let bh = if let Some(&cached) = tx_height_cache.get(&env.tx_id) {
                cached
            } else {
                tokio::time::sleep(PACING).await;
                match fetch_tx_block_height(http, base, &env.tx_id).await {
                    Ok(h) => {
                        tx_height_cache.insert(env.tx_id.clone(), h);
                        h
                    }
                    Err(e) => {
                        log::warn!("tx-block lookup failed for {}: {}", env.tx_id, e);
                        continue;
                    }
                }
            };
            if bh <= snapshot_height {
                return Ok(Some(env.tx_id));
            }
        }
    }
    Ok(None)
}

/// Truncate a string for column-aligned display.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max - 1])
    }
}

/// Apply the Bitflow FE's 10bp aggregator fee on top of a raw pool quote.
fn apply_aggregator_fee(raw_dy: u128) -> u128 {
    raw_dy.saturating_mul(10_000 - AGGREGATOR_FEE_BPS as u128) / 10_000
}

#[allow(clippy::too_many_arguments)]
fn print_quote_block(
    tag: &str,
    registry: &Arc<PoolRegistry>,
    pool_contracts: &[Principal],
    pool_labels: &[String],
    quote_amounts: &[f64],
    last_quote: &mut [Option<u128>],
    last_active: &mut [i32],
) {
    println!(
        "\n[{}]  {:<14} {:>6} {:>5} {:>11}   {:>10} → {:>14}  {:>14}",
        tag, "pool", "active", "bins", "wm", "in", "raw out", "post-10bp"
    );
    for (i, contract) in pool_contracts.iter().enumerate() {
        // DLMM pools are per-pool-contract, so `id() == pool_contract.to_string()`.
        let Some(handle) = registry.get(&contract.to_string()) else {
            println!("  {:<14} (not in registry?!)", pool_labels[i]);
            continue;
        };
        let Ok(guard) = handle.try_read() else {
            println!("  {:<14} (locked)", pool_labels[i]);
            continue;
        };
        let Some(pool) = guard.as_any().downcast_ref::<DLMMPool>() else {
            continue;
        };
        let wm = registry
            .watermark(contract)
            .map(|w| w.chars().take(10).collect::<String>())
            .unwrap_or_else(|| "(none)".to_string());

        let amt_h = quote_amounts[i];
        let raw = (amt_h * 10u128.pow(pool.x_decimals as u32) as f64) as u128;
        let dy = pool.calculate_output(&pool.x_token, raw).unwrap_or(0);
        let fe_dy = apply_aggregator_fee(dy);
        let dy_h = dy as f64 / 10u128.pow(pool.y_decimals as u32) as f64;
        let fe_dy_h = fe_dy as f64 / 10u128.pow(pool.y_decimals as u32) as f64;

        let active_diff = if pool.active_bin_id != last_active[i] {
            format!(" Δ{:+}→{:+}", last_active[i], pool.active_bin_id)
        } else {
            String::new()
        };
        let quote_diff = match last_quote[i] {
            Some(prev) if prev != dy => {
                let delta = dy as i128 - prev as i128;
                format!(
                    " Δ{:+.6}",
                    delta as f64 / 10u128.pow(pool.y_decimals as u32) as f64
                )
            }
            _ => String::new(),
        };

        println!(
            "  {:<14} {:>+6} {:>5} {:<11}   {:>10.4} → {:>14.6}  {:>14.6}{}{}",
            pool_labels[i],
            pool.active_bin_id,
            pool.bins.len(),
            wm,
            amt_h,
            dy_h,
            fe_dy_h,
            active_diff,
            quote_diff,
        );

        last_quote[i] = Some(dy);
        last_active[i] = pool.active_bin_id;
    }
}

fn install_ctrlc(shutdown: Arc<AtomicBool>) {
    // Best-effort signal install. On platforms where ctrlc fails we just
    // run forever; the user can kill the process.
    let s = shutdown.clone();
    let _ = ctrlc_handler(move || {
        s.store(true, Ordering::Relaxed);
    });
}

// Minimal Ctrl-C installer. We avoid pulling in the `ctrlc` crate by using
// tokio's signal future directly — only requires the `signal` feature on
// tokio. If that's not enabled, fall through to a no-op (the user can SIGKILL).
fn ctrlc_handler(cb: impl FnOnce() + Send + 'static) -> Result<()> {
    // Run an async task that awaits ctrl_c and then runs the callback once.
    let cb_cell: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>> =
        std::sync::Mutex::new(Some(Box::new(cb)));
    let cb_cell = Arc::new(cb_cell);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            if let Some(f) = cb_cell.lock().unwrap().take() {
                f();
            }
        }
    });
    Ok(())
}
