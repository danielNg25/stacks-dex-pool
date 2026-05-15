//! [`PerContractEventSource`] — the original ingestion strategy.
//!
//! Spawns one poller task per distinct contract in the registry's topic set.
//! Each poller walks `/extended/v1/contract/<id>/events` newest-first until it
//! hits the per-contract `tx_id` watermark.
//!
//! Trade-offs:
//! - Pros: cheap to reason about, watermark per stream is precise, errors in
//!   one stream don't stall others.
//! - Cons: O(N) Hiro requests per cycle. Above ~10 contracts on Hiro's
//!   ~50 req/min free tier you'll start eating into the budget. Use
//!   [`super::block_walking_source::BlockWalkingEventSource`] instead.

use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use async_trait::async_trait;
use reqwest::Client;

use super::config::CollectorConfig;
use super::event_poller::poll_contract_loop;
use super::event_queue::EventSender;
use super::metrics::CollectorMetrics;
use super::source::EventSource;
use crate::pool::principal::Principal;
use crate::registry::PoolRegistry;

/// Per-contract poller fan-out. Captures the topic snapshot at `run()` time;
/// dynamic pool additions go through [`super::handle::CollectorHandle::add_pool`]
/// which spawns ad-hoc pollers outside this source.
pub struct PerContractEventSource {
    events_base_url: Arc<String>,
    http: Arc<Client>,
    config: Arc<CollectorConfig>,
}

impl PerContractEventSource {
    pub fn new(events_base_url: String, http: Arc<Client>, config: Arc<CollectorConfig>) -> Self {
        Self {
            events_base_url: Arc::new(events_base_url),
            http,
            config,
        }
    }

    pub fn events_base_url(&self) -> Arc<String> {
        self.events_base_url.clone()
    }

    pub fn http(&self) -> Arc<Client> {
        self.http.clone()
    }

    pub fn config(&self) -> Arc<CollectorConfig> {
        self.config.clone()
    }
}

#[async_trait]
impl EventSource for PerContractEventSource {
    async fn run(
        self: Arc<Self>,
        registry: Arc<PoolRegistry>,
        sender: EventSender,
        metrics: Option<Arc<dyn CollectorMetrics>>,
        stop_flag: Arc<AtomicBool>,
    ) {
        let contracts: HashSet<Principal> = registry.polled_contracts().into_iter().collect();
        log::info!(
            "collector: per-contract source — spawning {} pollers @ {:?}",
            contracts.len(),
            self.config.poll_interval
        );

        let mut handles = Vec::with_capacity(contracts.len());
        for contract in contracts {
            let h = tokio::spawn(poll_contract_loop(
                contract,
                self.events_base_url.clone(),
                self.http.clone(),
                registry.clone(),
                sender.clone(),
                self.config.clone(),
                metrics.clone(),
                stop_flag.clone(),
            ));
            handles.push(h);
        }

        // Await every poller. `poll_contract_loop` exits cleanly on stop_flag.
        for h in handles {
            let _ = h.await;
        }
        log::info!("collector: per-contract source stopped");
    }

    fn name(&self) -> &'static str {
        "per-contract"
    }
}
