//! `CollectorHandle` — controls the collector's lifecycle.
//!
//! Returned by [`crate::collector::start_collector`]. Provides:
//!   - `stop()` — graceful shutdown (sets the stop flag, awaits tasks).
//!   - `pool_count()` — current registered pool count (delegates to registry).
//!   - `add_pool()` / `remove_pool()` — dynamic mutation; if a new pool brings
//!     a new contract into the topic set, a new poller task is spawned.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use reqwest::Client;
use tokio::task::JoinHandle;

use super::config::CollectorConfig;
use super::event_poller::poll_contract_loop;
use super::event_queue::EventSender;
use super::metrics::CollectorMetrics;
use crate::pool::base::PoolInterface;
use crate::pool::principal::Principal;
use crate::registry::PoolRegistry;

/// Handle to a running collector. Drop without calling `stop()` is OK but
/// less clean — tasks will still terminate when the registry / sender are
/// dropped, but `stop().await` waits for them to finish first.
pub struct CollectorHandle {
    pub(crate) registry: Arc<PoolRegistry>,
    pub(crate) events_base_url: Arc<String>,
    pub(crate) http: Arc<Client>,
    pub(crate) sender: EventSender,
    pub(crate) config: Arc<CollectorConfig>,
    pub(crate) metrics: Option<Arc<dyn CollectorMetrics>>,
    pub(crate) stop_flag: Arc<AtomicBool>,
    pub(crate) tasks: Vec<JoinHandle<()>>,
    /// Contracts we've already spawned pollers for. New pools that subscribe
    /// to one of these don't trigger a new poller.
    pub(crate) polled: std::collections::HashSet<Principal>,
    /// If true, [`Self::add_pool`] spawns an ad-hoc per-contract poller for
    /// any new contracts the added pool introduces. Set by the legacy
    /// per-contract bootstrap path. With a block-walking source this should
    /// be false — the source re-reads `registry.polled_contracts()` each
    /// tick and picks up the new contract automatically.
    pub(crate) spawn_per_contract_on_add: bool,
}

impl CollectorHandle {
    pub fn pool_count(&self) -> usize {
        self.registry.len()
    }

    pub fn registry(&self) -> Arc<PoolRegistry> {
        self.registry.clone()
    }

    /// Add a pool to the registry. If running with the per-contract source,
    /// any new contracts the pool subscribes to get an ad-hoc poller spawned.
    /// With a block-walking source the registry update is enough — the source
    /// picks up the new contract on its next tick.
    pub async fn add_pool(&mut self, pool: Box<dyn PoolInterface + Send + Sync>) -> Result<()> {
        let new_contracts: Vec<Principal> = pool
            .topics()
            .into_iter()
            .map(|t| t.contract)
            .filter(|c| !self.polled.contains(c))
            .collect();
        self.registry.insert(pool);
        if self.spawn_per_contract_on_add {
            for c in new_contracts {
                self.spawn_poller(c);
            }
        } else {
            // Just track them so we don't try to re-spawn later if the
            // user flips the bool (we don't expose that, but keep state tidy).
            for c in new_contracts {
                self.polled.insert(c);
            }
        }
        Ok(())
    }

    /// Remove a pool by `PoolInterface::id()`. Does NOT stop pollers — even
    /// if a polled contract has no subscribers any more, killing/restarting
    /// pollers is expensive and unnecessary; idle events just get dropped at
    /// dispatch.
    pub fn remove_pool(&self, id: &str) -> bool {
        self.registry.remove(id)
    }

    fn spawn_poller(&mut self, contract: Principal) {
        let registry = self.registry.clone();
        let events_base_url = self.events_base_url.clone();
        let http = self.http.clone();
        let sender = self.sender.clone();
        let config = self.config.clone();
        let metrics = self.metrics.clone();
        let stop_flag = self.stop_flag.clone();
        self.polled.insert(contract.clone());
        let handle = tokio::spawn(poll_contract_loop(
            contract,
            events_base_url,
            http,
            registry,
            sender,
            config,
            metrics,
            stop_flag,
        ));
        self.tasks.push(handle);
    }

    /// Signal stop and await all tasks. Idempotent.
    pub async fn stop(mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        // Drop sender so the processor's recv returns None.
        drop(self.sender);
        let tasks = std::mem::take(&mut self.tasks);
        for h in tasks {
            let _ = h.await;
        }
        log::info!("collector: stopped");
    }
}
