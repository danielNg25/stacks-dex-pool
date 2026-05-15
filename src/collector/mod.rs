//! Event-polling collector — keeps a [`crate::PoolRegistry`] fresh by
//! ingesting on-chain events from a chosen source and dispatching decoded
//! events to matching pools.
//!
//! Sources (see [`source::EventSource`]):
//!   - [`per_contract_source::PerContractEventSource`] — one
//!     `/extended/v1/contract/<id>/events` poller per registered contract.
//!     The endpoint returns events inline, so this source is one RPC per
//!     emitter per cycle. Default for `start_collector`.
//!   - [`block_walking_source::BlockWalkingEventSource`] (behind the
//!     `block_walking` feature) — one block-walker that fetches the
//!     `/extended/v2/blocks/{h}/transactions` list and a per-event-tx
//!     follow-up via `/extended/v1/tx/{tx_id}` (Hiro's block-list
//!     endpoint always returns `events: []`). O(active event-tx per
//!     block), not O(pools); off by default to keep the minimal build
//!     small.
//!
//! Both sources feed the same dedup queue and the same event-processor.
//! Mirrors `evm-dex-pool/src/collector` in shape (BlockSource → Queue →
//! Processor); the difference vs EVM is that Stacks RPC has no log
//! subscription, so all ingestion is REST polling.

#[cfg(feature = "block_walking")]
pub mod block_walking_source;
pub mod bootstrap;
pub mod config;
pub mod event_poller;
pub mod event_processor;
pub mod event_queue;
pub mod handle;
pub mod metrics;
pub mod per_contract_source;
pub mod source;

#[cfg(feature = "block_walking")]
pub use block_walking_source::{BlockWalkingConfig, BlockWalkingEventSource};
pub use bootstrap::{start_collector, start_collector_with_source};
pub use config::CollectorConfig;
pub use handle::CollectorHandle;
pub use metrics::CollectorMetrics;
pub use per_contract_source::PerContractEventSource;
pub use source::EventSource;
