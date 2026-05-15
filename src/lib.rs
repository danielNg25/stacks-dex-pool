//! `stacks-dex-pools` — Rust library for mirroring Stacks DEX pool state.
//!
//! Peer of `evm-dex-pool` in architecture, adapted for Stacks specifics:
//!   - HTTP RPC + Clarity codec (no WebSocket; Stacks RPC is request/response only).
//!   - `?tip=<index_block_hash>` for historical reads (analog of EVM `blockNumber`).
//!   - REST polling of `/extended/v1/contract/<id>/events` for event ingestion.
//!   - Bitflow DLMM is the first-class implementation. V2-family
//!     (ALEX/Velar/Arkadiko/Bitflow XYK) and StableSwap (Bitflow V1/V2) live
//!     behind the opt-in `non_dlmm` feature.
//!
//! ## Feature tiers
//!
//! - `default` — pool math + Clarity/c32 codec only. No async deps. Useful for
//!   embedding the math into a tool that does its own RPC.
//! - `rpc` — adds the HTTP client (`reqwest` + `tokio`) for call-read and the
//!   events endpoint. Includes 429-retry and the `?tip=` parameter.
//! - `registry` — `PoolRegistry` (DashMap + per-pool `tokio::RwLock` + atomic
//!   block cursor + per-contract event watermark). Lock-free reads via DashMap.
//! - `collector` — top-level lifecycle: spawn per-contract event-poller tasks
//!   that feed a bounded dedup queue, dispatch to registry pools with the
//!   cross-pool filter, advance watermarks. Mirrors the Python POC's
//!   `verify_dlmm_events.py` design but as a long-running service.
//! - `non_dlmm` (opt-in) — ALEX, Velar, Arkadiko, Bitflow XYK, Bitflow V1/V2
//!   StableSwap. Math + event handlers + bootstrap fetchers + STX-wrap helpers.
//!   Off by default so DLMM-only consumers (current `arb-stacks`) don't pull
//!   in unused code paths.
//! - `block_walking` (opt-in) — block-driven event source (alternative to the
//!   per-contract polling default). Off by default; only useful if you want to
//!   walk a chain block-range and don't mind the per-event-tx follow-up calls.
//!
//! ## Quick start
//!
//! Math-only (no features):
//! ```ignore
//! use stacks_dex_pools::dlmm::math::{single_bin_swap_x_for_y, bin_price};
//! let (dx, dy, used) = single_bin_swap_x_for_y(amount_x, bin_x, bin_y, price, 15, 15, 0);
//! ```
//!
//! Full collector (all features):
//! ```ignore
//! use stacks_dex_pools::{PoolRegistry, start_collector, CollectorConfig};
//! let registry = Arc::new(PoolRegistry::new());
//! let handle = start_collector(client, events_client, config, registry, None).await?;
//! ```

pub mod codec;
pub mod pool;

#[cfg(feature = "rpc")]
pub mod rpc;

#[cfg(feature = "rpc")]
pub mod token_info;

pub mod dlmm;

#[cfg(feature = "non_dlmm")]
pub mod stableswap;

#[cfg(feature = "non_dlmm")]
pub mod stx_wrap;

#[cfg(feature = "non_dlmm")]
pub mod v2;

#[cfg(feature = "registry")]
pub mod registry;

#[cfg(feature = "collector")]
pub mod collector;

// ---------------------------------------------------------------------------
// Public re-exports
// ---------------------------------------------------------------------------

pub use pool::base::{EventApplicable, PoolInterface, PoolType, PoolTypeTrait, TopicList};
pub use pool::event::{StacksEvent, StacksTopic};
pub use pool::principal::Principal;

pub use codec::clarity::{cv_decode, cv_encode, ClarityValue};

#[cfg(feature = "rpc")]
pub use rpc::client::{RpcConfig, StacksRpcClient};

#[cfg(feature = "rpc")]
pub use rpc::events::{fetch_events_page, fetch_tx_block_height, EventEnvelope};

#[cfg(feature = "rpc")]
pub use token_info::{StacksTokenInfo, TokenInfo};

#[cfg(feature = "registry")]
pub use registry::PoolRegistry;

#[cfg(feature = "collector")]
pub use collector::{
    start_collector, start_collector_with_source, CollectorConfig, CollectorHandle,
    CollectorMetrics, EventSource, PerContractEventSource,
};

#[cfg(all(feature = "collector", feature = "block_walking"))]
pub use collector::{BlockWalkingConfig, BlockWalkingEventSource};

// Tier 1 export — DLMM is first-class.
pub use dlmm::{BinState, DLMMPool, INDEXED_ACTIONS, KNOWN_INFORMATIONAL};
