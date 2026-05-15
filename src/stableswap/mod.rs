//! Bitflow StableSwap pools on Stacks.
//!
//! Two generations, each its own module:
//! - [`bitflow_v2`] — `stableswap-pool-*-*-v-1-N` bound to a shared
//!   `stableswap-core-v-1-N`. Per-pool fees, amp, midpoint multiplier;
//!   per-pool events (`update-pool-balances`) + core events (`set-midpoint`).
//! - [`bitflow_v1`] — older self-contained pools at a different deployer.
//!   Dual ABI (STX-anchored 2-arg vs token-pair 3-arg) and a dual math
//!   variant (`v1-bal-bug` reproduces a double-count bug byte-exact).
//!
//! Shared Newton-Raphson math + decimal scaling lives in [`curve`];
//! per-variant event handling in [`events`]; per-variant bootstrap in
//! [`fetcher`] (rpc-gated).
//!
//! Both variants implement [`crate::pool::base::PoolInterface`] and feed
//! the same registry / collector pipeline as DLMM and the V2 family.
//!
//! Replaces the previous stub (a single `BitflowStableSwapPool` enum); the
//! stub had no consumers and the per-generation split matches the actual
//! shape of the two pool families.

pub mod bitflow_v1;
pub mod bitflow_v2;
pub mod curve;
pub mod events;

#[cfg(feature = "rpc")]
pub mod fetcher;

pub use bitflow_v1::{BitflowStableSwapV1Pool, MathVariant, Sig};
pub use bitflow_v2::BitflowStableSwapV2Pool;
