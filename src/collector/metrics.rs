//! Collector metrics trait. Implementors can wire counters into Prometheus
//! / StatsD / structured logs. The collector calls these from the event-
//! processor task; impls should be cheap (atomic increments, log calls).

use std::time::Duration;

/// Implement to collect telemetry on the collector. All methods have no-op
/// default implementations so consumers can override only what they care about.
pub trait CollectorMetrics: Send + Sync {
    /// Called when an event is successfully applied to a pool.
    fn record_event_applied(&self, _action: &str) {}
    /// Called when an event is dropped (informational or unknown).
    fn record_event_dropped(&self, _action: &str, _reason: &'static str) {}
    /// Called once per per-contract poll cycle.
    fn record_poll_cycle(&self, _contract: &str, _events_fetched: u32, _duration: Duration) {}
    /// Called when the event queue is full and we drop the oldest event.
    fn record_queue_overflow(&self) {}
    /// Called when an RPC error occurs during polling (any HTTP/parse failure).
    fn record_poll_error(&self, _contract: &str) {}
}
