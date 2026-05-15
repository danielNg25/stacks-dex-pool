//! Event processor — drains the queue, dispatches each event to every pool
//! that subscribes to it (the cross-pool filter in `apply_event` sorts out
//! relevance).

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc::Receiver;

use super::event_queue::QueuedEvent;
use super::metrics::CollectorMetrics;
use crate::registry::PoolRegistry;

/// Drain the queue until the channel is closed, dispatching events to pools.
pub async fn event_processor_loop(
    mut rx: Receiver<QueuedEvent>,
    registry: Arc<PoolRegistry>,
    metrics: Option<Arc<dyn CollectorMetrics>>,
    stop_flag: Arc<std::sync::atomic::AtomicBool>,
) {
    log::info!("collector: event processor started");
    loop {
        if stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
        let qe = match tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv()).await
        {
            Ok(Some(qe)) => qe,
            Ok(None) => break,  // channel closed
            Err(_) => continue, // timeout — check stop_flag and loop
        };
        if let Err(e) = dispatch(&qe, &registry, metrics.as_deref()).await {
            log::warn!("collector: dispatch error: {}", e);
        }
    }
    log::info!("collector: event processor stopped");
}

async fn dispatch(
    qe: &QueuedEvent,
    registry: &PoolRegistry,
    metrics: Option<&dyn CollectorMetrics>,
) -> Result<()> {
    let event = &qe.event;
    let candidates = registry.pools_subscribed_to(&event.emitter);
    if candidates.is_empty() {
        if let Some(m) = metrics {
            m.record_event_dropped(&event.action, "no-subscribers");
        }
        return Ok(());
    }
    let mut applied = 0;
    for handle in candidates {
        let mut guard = handle.write().await;
        // The pool's apply_event handles the cross-pool filter and
        // unknown-action drop; we just feed it.
        if let Err(e) = guard.apply_event(event) {
            log::warn!(
                "apply_event failed on {} for action={}: {}",
                guard.pool_contract(),
                event.action,
                e
            );
            continue;
        }
        applied += 1;
    }
    if let Some(m) = metrics {
        if applied > 0 {
            m.record_event_applied(&event.action);
        } else {
            m.record_event_dropped(&event.action, "no-pool-matched");
        }
    }
    Ok(())
}
