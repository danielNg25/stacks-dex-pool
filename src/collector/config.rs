//! Collector configuration.

use std::time::Duration;

/// Settings for [`crate::collector::start_collector`].
#[derive(Debug, Clone)]
pub struct CollectorConfig {
    /// How often each per-contract poller wakes up. Default 5s.
    /// Faster = fresher quotes but more RPC. Hiro's free tier is ~50 req/min,
    /// so for K polled contracts pick `poll_interval > K * 60s / 50`.
    pub poll_interval: Duration,
    /// Max events per page (Hiro's hard cap is 50).
    pub page_size: u32,
    /// Hard cap on events fetched per poll cycle per contract — prevents
    /// runaway when a stream has very high throughput.
    pub max_events_per_cycle: u32,
    /// Bounded mpsc capacity. Older events get evicted if the queue fills
    /// (the processor logs a warning).
    pub queue_capacity: usize,
    /// Initial walk-back distance on cold start (no watermark in registry).
    /// We fetch up to this many pages backward, set the watermark to the
    /// newest event we see, and trust the registry's snapshot for older
    /// state.
    pub cold_start_pages: u32,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            page_size: 50,
            max_events_per_cycle: 500,
            queue_capacity: 1000,
            cold_start_pages: 1,
        }
    }
}
