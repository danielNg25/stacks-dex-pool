//! `start_collector` — top-level entry point.
//!
//! Builds a shared HTTP client, the event queue, spawns the event processor,
//! drives the selected [`EventSource`], and returns a [`CollectorHandle`] for
//! lifecycle control.
//!
//! Two flavors:
//!   - [`start_collector`] — back-compat shim. Constructs a
//!     [`PerContractEventSource`] from the supplied URL+config (matches the
//!     legacy behaviour of one Hiro events poller per contract).
//!   - [`start_collector_with_source`] — caller chooses the source
//!     (typically [`super::BlockWalkingEventSource`] for multi-DEX setups).

use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use anyhow::Result;
use reqwest::Client;

use super::config::CollectorConfig;
use super::event_processor::event_processor_loop;
use super::event_queue::build_queue;
use super::handle::CollectorHandle;
use super::metrics::CollectorMetrics;
use super::per_contract_source::PerContractEventSource;
use super::source::EventSource;
use crate::registry::PoolRegistry;

/// Back-compat: start the collector with the legacy per-contract source.
///
/// `events_base_url` is typically `https://api.mainnet.hiro.so` (Bitflow's
/// node doesn't mirror `/extended/v1/*`).
pub async fn start_collector(
    events_base_url: String,
    registry: Arc<PoolRegistry>,
    config: CollectorConfig,
    metrics: Option<Arc<dyn CollectorMetrics>>,
) -> Result<CollectorHandle> {
    let http = Arc::new(
        Client::builder()
            .timeout(std::time::Duration::from_secs(20))
            .build()?,
    );
    let config_arc = Arc::new(config);
    let source: Arc<dyn EventSource> = Arc::new(PerContractEventSource::new(
        events_base_url.clone(),
        http.clone(),
        config_arc.clone(),
    ));
    start_collector_with_source_inner(
        events_base_url,
        http,
        registry,
        config_arc,
        metrics,
        source,
        true, // spawn_per_contract_on_add — legacy add_pool behaviour
    )
    .await
}

/// Start the collector with a caller-supplied [`EventSource`]. The
/// supplied source is what drives event ingestion; the dedup queue,
/// event-processor, and dispatch path are wired the same way regardless.
///
/// `events_base_url` is kept on the handle for dynamic-add bookkeeping, but
/// the source itself is responsible for using it (or not).
pub async fn start_collector_with_source(
    events_base_url: String,
    registry: Arc<PoolRegistry>,
    config: CollectorConfig,
    metrics: Option<Arc<dyn CollectorMetrics>>,
    source: Arc<dyn EventSource>,
) -> Result<CollectorHandle> {
    let http = Arc::new(
        Client::builder()
            .timeout(std::time::Duration::from_secs(20))
            .build()?,
    );
    let config_arc = Arc::new(config);
    start_collector_with_source_inner(
        events_base_url,
        http,
        registry,
        config_arc,
        metrics,
        source,
        false, // explicit source path: block-walker (or any custom) handles dynamic pools itself
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn start_collector_with_source_inner(
    events_base_url: String,
    http: Arc<Client>,
    registry: Arc<PoolRegistry>,
    config: Arc<CollectorConfig>,
    metrics: Option<Arc<dyn CollectorMetrics>>,
    source: Arc<dyn EventSource>,
    spawn_per_contract_on_add: bool,
) -> Result<CollectorHandle> {
    let (sender, rx) = build_queue(config.queue_capacity);
    let stop_flag = Arc::new(AtomicBool::new(false));
    let events_base_url = Arc::new(events_base_url);

    // Event processor — single task draining the queue.
    let processor_handle = tokio::spawn(event_processor_loop(
        rx,
        registry.clone(),
        metrics.clone(),
        stop_flag.clone(),
    ));

    let source_name = source.name();
    let source_handle = tokio::spawn({
        let source = source.clone();
        let registry = registry.clone();
        let sender = sender.clone();
        let metrics = metrics.clone();
        let stop_flag = stop_flag.clone();
        async move { source.run(registry, sender, metrics, stop_flag).await }
    });

    let polled: HashSet<_> = registry.polled_contracts().into_iter().collect();
    let tasks = vec![processor_handle, source_handle];

    log::info!(
        "collector: started — source={} ({} contracts in topic set, queue cap {})",
        source_name,
        polled.len(),
        config.queue_capacity
    );

    Ok(CollectorHandle {
        registry,
        events_base_url,
        http,
        sender,
        config,
        metrics,
        stop_flag,
        tasks,
        polled,
        spawn_per_contract_on_add,
    })
}
