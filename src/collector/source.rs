//! `EventSource` — abstraction over event ingestion strategies.
//!
//! Two implementations live alongside this trait:
//!   - [`super::per_contract_source::PerContractEventSource`] — N pollers, one
//!     per registered contract. Simple, robust, but N × `/extended/v1/contract/.../events`
//!     calls per cycle. Fine for a handful of DLMM contracts under Hiro's
//!     ~50 req/min budget.
//!   - [`super::block_walking_source::BlockWalkingEventSource`] — single
//!     block-walker: `/v2/info` → `/extended/v2/blocks/{N}/transactions` →
//!     filter txs by `contract_call.contract_id ∈ registry`. Constant cost
//!     (~3 calls/cycle) regardless of registered pool count. The Stacks
//!     analog of EVM's `eth_getLogs` with a multi-address filter.
//!
//! Both feed the same dedup queue ([`super::event_queue::EventSender`]) and
//! the same `event_processor_loop`; only the ingestion path differs.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use async_trait::async_trait;

use super::event_queue::EventSender;
use super::metrics::CollectorMetrics;
use crate::registry::PoolRegistry;

/// An event-ingestion strategy. Implementors spawn / drive whatever tasks
/// they need (single loop, fan-out, etc.) and push decoded events into
/// `sender`. They must return when `stop_flag` flips to `true`.
#[async_trait]
pub trait EventSource: Send + Sync {
    /// Run the source until `stop_flag` is set. Should NOT return early on
    /// transient errors — handle them internally (retry, backoff, etc.).
    async fn run(
        self: Arc<Self>,
        registry: Arc<PoolRegistry>,
        sender: EventSender,
        metrics: Option<Arc<dyn CollectorMetrics>>,
        stop_flag: Arc<AtomicBool>,
    );

    /// Short human-readable name for logs ("per-contract", "block-walking").
    fn name(&self) -> &'static str;
}
