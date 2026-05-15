//! Per-contract event poller. One task per polled contract.
//!
//! Each tick walks Hiro's events endpoint newest-first, paging until we hit
//! the watermark `tx_id` from the registry (or `cold_start_pages` on cold
//! start). Successfully fetched events are sent (deduped) to the queue and
//! the watermark is advanced to the newest event's tx_id.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use reqwest::Client;

use super::config::CollectorConfig;
use super::event_queue::EventSender;
use super::metrics::CollectorMetrics;
use crate::pool::principal::Principal;
use crate::registry::PoolRegistry;
use crate::rpc::events::fetch_events_page;

/// Run the poll loop for one contract until `stop_flag` is set.
#[allow(clippy::too_many_arguments)]
pub async fn poll_contract_loop(
    contract: Principal,
    events_base_url: Arc<String>,
    http: Arc<Client>,
    registry: Arc<PoolRegistry>,
    sender: EventSender,
    config: Arc<CollectorConfig>,
    metrics: Option<Arc<dyn CollectorMetrics>>,
    stop_flag: Arc<std::sync::atomic::AtomicBool>,
) {
    let contract_id = contract.to_string();
    log::info!(
        "collector: polling {} every {:?}",
        contract_id,
        config.poll_interval
    );

    let mut next_tick = Instant::now();
    while !stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
        // Sleep in 250ms slices so we shut down quickly even with long intervals.
        while Instant::now() < next_tick {
            if stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }
            let remaining = next_tick.saturating_duration_since(Instant::now());
            tokio::time::sleep(Duration::from_millis(250).min(remaining)).await;
        }
        next_tick = Instant::now() + config.poll_interval;

        let cycle_start = Instant::now();
        match poll_once(
            &contract,
            &contract_id,
            &events_base_url,
            &http,
            &registry,
            &sender,
            &config,
        )
        .await
        {
            Ok(applied) => {
                if let Some(m) = metrics.as_ref() {
                    m.record_poll_cycle(&contract_id, applied, cycle_start.elapsed());
                }
            }
            Err(e) => {
                log::warn!("collector: poll error for {}: {}", contract_id, e);
                if let Some(m) = metrics.as_ref() {
                    m.record_poll_error(&contract_id);
                }
            }
        }
    }
    log::info!("collector: poller for {} stopped", contract_id);
}

/// Single poll cycle. Walks pages newest→older until we hit the watermark,
/// `cold_start_pages` (on first run), or `max_events_per_cycle`. Returns
/// the count of events enqueued.
async fn poll_once(
    contract: &Principal,
    contract_id: &str,
    events_base_url: &str,
    http: &Client,
    registry: &PoolRegistry,
    sender: &EventSender,
    config: &CollectorConfig,
) -> Result<u32> {
    let watermark = registry.watermark(contract);
    let max_pages = if watermark.is_none() {
        config.cold_start_pages
    } else {
        // Bound walk by max_events_per_cycle.
        config.max_events_per_cycle.div_ceil(config.page_size)
    };

    let mut to_apply = Vec::new();
    let mut newest_seen: Option<String> = None;
    let mut reached_watermark = false;

    for page_idx in 0..max_pages {
        let offset = page_idx * config.page_size;
        let page =
            fetch_events_page(http, events_base_url, contract_id, config.page_size, offset).await?;
        if page.is_empty() {
            break;
        }
        if newest_seen.is_none() {
            newest_seen = Some(page[0].tx_id.clone());
        }
        let page_len = page.len();
        for env in page {
            if let Some(w) = watermark.as_ref() {
                if &env.tx_id == w {
                    reached_watermark = true;
                    break;
                }
            }
            to_apply.push(env);
        }
        if reached_watermark {
            break;
        }
        if (page_len as u32) < config.page_size {
            // Last page from Hiro.
            break;
        }
        if to_apply.len() as u32 >= config.max_events_per_cycle {
            break;
        }
    }

    // Apply in chronological order (we collected newest-first across pages,
    // so reverse).
    let mut enqueued = 0u32;
    for env in to_apply.into_iter().rev() {
        if let Some(decoded) = env.decoded {
            if sender.send_dedup(decoded).await {
                enqueued += 1;
            }
        }
    }

    if enqueued > 0 {
        log::debug!(
            "poller: {} → enqueued {} new event(s) (reached_watermark={})",
            short_contract(contract_id),
            enqueued,
            reached_watermark,
        );
    }

    if let Some(w) = newest_seen {
        registry.set_watermark(contract, w);
    }

    Ok(enqueued)
}

/// Strip the deployer prefix for log readability. `SM1FKXGN….dlmm-pool-stx-usdcx-v-1-bps-10` → `dlmm-pool-stx-usdcx-v-1-bps-10`.
fn short_contract(contract_id: &str) -> &str {
    contract_id
        .split_once('.')
        .map(|x| x.1)
        .unwrap_or(contract_id)
}
