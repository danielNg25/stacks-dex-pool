//! Uniswap V2-family pools on Stacks.
//!
//! Four variants, each its own module:
//!   - [`arkadiko`] — singleton `arkadiko-swap-v2-1`, hardcoded 30 bps,
//!     cross-pair filter via `swap-token`.
//!   - [`velar`] — singleton `univ2-core`, per-pool `(fee_num, fee_den)`
//!     from `lookup-pool`, cross-pool filter via `lp-token`; events use the
//!     `op`-style payload shape (decoder transparently handles it).
//!   - [`alex`] — singleton `amm-pool-v2-01`, constant-product only
//!     (factor=1e8), cross-pool filter via `pool-id`.
//!   - [`bitflow_xyk`] — per-pool contract (no singleton), per-direction
//!     protocol+provider bps via the pool's bound `core-address`.
//!
//! Shared math lives in [`math`]; per-variant event handling in [`events`];
//! per-variant bootstrap (RPC reads) in [`fetcher`] (rpc-gated).
//!
//! All variants implement [`crate::pool::base::PoolInterface`] and feed the
//! same registry / collector pipeline as DLMM.
//!
//! Replaces the previous stub (`pool.rs` with a single `StacksUniswapV2Pool`
//! enum). The stub had no consumers; this layout matches `evm-dex-pool`'s
//! one-module-per-DEX shape and keeps each variant's fields honest.

pub mod alex;
pub mod arkadiko;
pub mod bitflow_xyk;
pub mod events;
pub mod math;
pub mod velar;

#[cfg(feature = "rpc")]
pub mod fetcher;

pub use alex::AlexPool;
pub use arkadiko::ArkadikoPool;
pub use bitflow_xyk::BitflowXykPool;
pub use velar::VelarPool;
