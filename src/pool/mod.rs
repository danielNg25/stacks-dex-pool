//! Pool base types — Principal, PoolInterface trait, decoded StacksEvent.
//!
//! These compile under the default feature (no async deps). They define the
//! shape that every pool implementation (DLMM, V2, StableSwap) must conform
//! to, plus the event envelope the collector dispatches to `apply_event`.

pub mod base;
pub mod event;
pub mod principal;
