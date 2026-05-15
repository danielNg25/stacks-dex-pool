//! Live tip-and-replay reconcile — Rust port of `test/verify_dlmm_events.py`.
//!
//! Marked `#[ignore]` because it hits live Hiro mainnet. Run with:
//!     cargo test --features collector --test dlmm_reconcile_live -- --ignored --nocapture
//!
//! Algorithm:
//!   1. Snapshot pool state at block (current_tip - lookback) via `?tip=` reads.
//!   2. Fetch events from the boundary to current from BOTH the pool's
//!      contract and the core's contract.
//!   3. Apply each stream in chronological order (idempotent state-setters,
//!      so cross-stream ordering doesn't matter).
//!   4. Fetch fresh current state.
//!   5. Compare quote-relevant fields (active_bin_id, fees, bin x/y) —
//!      shares is deliberately not compared.
//!
//! Passes when 100% of compared fields match.

#![cfg(feature = "collector")]

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use reqwest::Client;
use stacks_dex_pools::dlmm::events::apply_event;
use stacks_dex_pools::dlmm::fetcher::{fetch_dlmm_pool, BootstrapMode};
use stacks_dex_pools::pool::event::StacksEvent;
use stacks_dex_pools::pool::principal::Principal;
use stacks_dex_pools::rpc::client::{RpcConfig, StacksRpcClient};
use stacks_dex_pools::rpc::events::fetch_events_page;
use stacks_dex_pools::token_info::StacksTokenInfo;

const HIRO: &str = "https://api.mainnet.hiro.so";

#[tokio::test]
#[ignore]
async fn reconcile_bps10_stx_usdcx() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let lookback = std::env::var("LOOKBACK")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(20);
    let bin_window = std::env::var("BIN_WINDOW")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(5);

    println!(
        "Reconcile: lookback={} blocks, bin_window=±{}",
        lookback, bin_window
    );

    let pool_contract: Principal =
        "SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD.dlmm-pool-stx-usdcx-v-1-bps-10".parse()?;
    let core_contract: Principal =
        "SP1PFR4V08H1RAZXREBGFFQ59WB739XM8VVGTFSEA.dlmm-core-v-1-1".parse()?;

    let rpc_config = RpcConfig {
        base_url: HIRO.to_string(),
        max_retries: 5,
        ..Default::default()
    };
    let client = Arc::new(StacksRpcClient::new(rpc_config)?);
    let token_info = StacksTokenInfo::new(client.clone());
    let http = Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()?;

    // 1. Pick a past block N.
    let info: serde_json::Value = http
        .get(format!("{}/v2/info", HIRO))
        .send()
        .await?
        .json()
        .await?;
    let current_h = info
        .get("stacks_tip_height")
        .and_then(|v| v.as_u64())
        .expect("stacks_tip_height");
    let snapshot_h = current_h - lookback;
    let block_resp: serde_json::Value = http
        .get(format!("{}/extended/v2/blocks/{}", HIRO, snapshot_h))
        .send()
        .await?
        .json()
        .await?;
    let snapshot_tip = block_resp
        .get("index_block_hash")
        .and_then(|v| v.as_str())
        .expect("index_block_hash")
        .strip_prefix("0x")
        .unwrap()
        .to_string();
    println!(
        "current_tip = block {}, snapshot_tip = block {}",
        current_h, snapshot_h
    );

    // 2. Snapshot at tip.
    let snapshot = fetch_dlmm_pool(
        client.clone(),
        &pool_contract,
        &core_contract,
        &token_info,
        BootstrapMode::Window { radius: bin_window },
        4,
        Some(&snapshot_tip),
    )
    .await?;
    println!(
        "Snapshot: active={}, bins={}",
        snapshot.active_bin_id,
        snapshot.bins.len()
    );

    // 3. Fetch events from both streams. Pages of 50; stop on first event
    //    we've seen before (or after one page on cold start for a tight loop).
    let mut replayed = snapshot.clone();
    let pool_id = pool_contract.to_string();
    let core_id = core_contract.to_string();
    let mut pool_events = collect_recent_events(&http, HIRO, &pool_id, 4).await?;
    let mut core_events = collect_recent_events(&http, HIRO, &core_id, 6).await?;
    pool_events.reverse(); // chrono
    core_events.reverse();
    println!(
        "Events fetched: pool={}, core={}",
        pool_events.len(),
        core_events.len()
    );

    let mut applied_count = HashMap::<String, u32>::new();
    for ev in pool_events.iter().chain(core_events.iter()) {
        let before_active = replayed.active_bin_id;
        apply_event(&mut replayed, ev)?;
        if replayed.active_bin_id != before_active
            || ev.action == "update-bin-balances"
            || ev.action == "update-bin-balances-on-withdraw"
        {
            *applied_count.entry(ev.action.clone()).or_default() += 1;
        }
    }
    println!("Applied per action: {:?}", applied_count);

    // 4. Fetch fresh.
    let fresh = fetch_dlmm_pool(
        client,
        &pool_contract,
        &core_contract,
        &token_info,
        BootstrapMode::Window { radius: bin_window },
        4,
        None,
    )
    .await?;
    println!(
        "Fresh: active={}, bins={}",
        fresh.active_bin_id,
        fresh.bins.len()
    );

    // 5. Compare quote-relevant fields.
    let mut mismatches = 0u32;
    let mut total = 0u32;
    macro_rules! check {
        ($name:expr, $a:expr, $b:expr) => {
            total += 1;
            if $a != $b {
                mismatches += 1;
                println!("  MISMATCH {}: replayed={:?} current={:?}", $name, $a, $b);
            }
        };
    }
    check!("active_bin_id", replayed.active_bin_id, fresh.active_bin_id);
    check!(
        "x_protocol_fee",
        replayed.x_protocol_fee,
        fresh.x_protocol_fee
    );
    check!(
        "x_provider_fee",
        replayed.x_provider_fee,
        fresh.x_provider_fee
    );
    check!(
        "x_variable_fee",
        replayed.x_variable_fee,
        fresh.x_variable_fee
    );
    check!(
        "y_protocol_fee",
        replayed.y_protocol_fee,
        fresh.y_protocol_fee
    );
    check!(
        "y_provider_fee",
        replayed.y_provider_fee,
        fresh.y_provider_fee
    );
    check!(
        "y_variable_fee",
        replayed.y_variable_fee,
        fresh.y_variable_fee
    );

    // Window bin diff — only bins in fresh's window (where we have ground truth).
    for (bid, fresh_bin) in fresh.bins.iter() {
        total += 1;
        match replayed.bins.get(bid) {
            Some(r) if r.x == fresh_bin.x && r.y == fresh_bin.y => {}
            Some(r) => {
                mismatches += 1;
                println!(
                    "  MISMATCH bin {}: replayed=({},{}) current=({},{})",
                    bid, r.x, r.y, fresh_bin.x, fresh_bin.y
                );
            }
            None => {
                mismatches += 1;
                println!(
                    "  MISMATCH bin {}: replayed=(none) current=({},{})",
                    bid, fresh_bin.x, fresh_bin.y
                );
            }
        }
    }
    println!("Summary: {}/{} fields match", total - mismatches, total);
    assert_eq!(mismatches, 0, "reconcile mismatches");
    Ok(())
}

/// Fetch a few pages of events for a contract; decode envelopes; return the
/// decoded ones newest-first. Bounded by `max_pages` (50 events/page).
async fn collect_recent_events(
    http: &Client,
    base: &str,
    contract_id: &str,
    max_pages: u32,
) -> Result<Vec<StacksEvent>> {
    let mut out = Vec::new();
    for p in 0..max_pages {
        let page = fetch_events_page(http, base, contract_id, 50, p * 50).await?;
        if page.is_empty() {
            break;
        }
        for env in page {
            if let Some(decoded) = env.decoded {
                out.push(decoded);
            }
        }
    }
    Ok(out)
}
