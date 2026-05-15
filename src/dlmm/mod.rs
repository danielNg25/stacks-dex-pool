//! Bitflow DLMM (HODLMM) — the first-class pool implementation.
//!
//! Bin-based concentrated liquidity. Each pool has 1001 signed bins
//! `[-500, +500]`, each with a fixed price and (x, y) inventory. Swaps walk
//! bins outward from the active bin. The math is a direct port of the
//! Bitflow `dlmm-core-v-1-1` contract verified byte-exact in
//! `test/fetch_bitflow_dlmm.py` and `test/estimate_swap_dlmm.py`.
//!
//! Submodules:
//! - [`math`] — pure-int swap math + bin-price calc
//! - [`factor`] — `get-bin-factors-by-step` cache
//! - [`events`] — apply_event w/ cross-pool filter, INDEXED_ACTIONS set
//! - [`pool`] — `DLMMPool` struct + `PoolInterface` impl
//! - [`fetcher`] — RPC bootstrap (window / full modes), only with `rpc` feature
//! - [`discovery`] — walk `dlmm-core` registry to enumerate every live pool
//!   without hardcoding addresses, only with `rpc` feature

pub mod events;
pub mod factor;
pub mod math;
pub mod pool;

#[cfg(feature = "rpc")]
pub mod discovery;

#[cfg(feature = "rpc")]
pub mod fetcher;

#[cfg(feature = "rpc")]
pub mod multicall;

// Public re-exports.
pub use events::{INDEXED_ACTIONS, KNOWN_INFORMATIONAL};
pub use pool::{BinState, DLMMPool};

#[cfg(feature = "rpc")]
pub use discovery::{discover_dlmm_pools, DlmmPoolListing};

/// Maximum bin id (signed). The contract uses unsigned 0..=1000 with
/// `CENTER_BIN_ID = 500`; signed_id = unsigned - 500.
pub const MAX_BIN_ID: i32 = 500;
pub const MIN_BIN_ID: i32 = -500;
pub const CENTER_BIN_ID: i32 = 500;
pub const NUM_OF_BINS: usize = 1001;

/// Scale factor used by the contract for bin prices.
pub const PRICE_SCALE_BPS: u128 = 100_000_000;
/// Scale factor for fees (bps).
pub const FEE_SCALE_BPS: u32 = 10_000;
