//! Core pool traits. Mirror of `evm-dex-pool/src/pool/base.rs` adapted for
//! Stacks: principals instead of addresses, u128 instead of U256, decoded
//! events instead of raw logs.

use std::any::Any;
use std::fmt::Debug;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::pool::event::{StacksEvent, StacksTopic};
use crate::pool::principal::Principal;

/// Pool type classification — matches `evm-dex-pool::PoolType` in shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PoolType {
    /// Bitflow DLMM / HODLMM — bin-based concentrated liquidity.
    BitflowDlmm,
    /// Uniswap V2-family on Stacks — ALEX / Velar / Arkadiko / Bitflow XYK.
    /// Constant-product with fees in bps.
    StacksUniswapV2,
    /// Bitflow V1/V2 StableSwap — Curve invariant + Newton-Raphson.
    BitflowStableSwap,
}

impl std::fmt::Display for PoolType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolType::BitflowDlmm => write!(f, "bitflow_dlmm"),
            PoolType::StacksUniswapV2 => write!(f, "stacks_uniswap_v2"),
            PoolType::BitflowStableSwap => write!(f, "bitflow_stableswap"),
        }
    }
}

/// All pools implement this marker so the collector + registry can dispatch
/// dynamically.
pub trait PoolTypeTrait: Send + Sync {
    fn pool_type(&self) -> PoolType;
}

/// Apply a decoded Stacks event to pool state. Pool impls put their cross-
/// pool / cross-emitter filter inside `apply_event`.
pub trait EventApplicable {
    /// Mutate `self` based on the event. Return `Ok(())` whether the event
    /// affected state or not — only return Err on truly unrecoverable decode
    /// errors. Pool impls SHOULD silently drop events that don't apply
    /// (foreign pool, unindexed action, etc.).
    fn apply_event(&mut self, event: &StacksEvent) -> Result<()>;
}

/// What topics a pool wants the collector to subscribe to. This is the way
/// each pool declares "I care about these (contract, action) combos."
///
/// For DLMM this returns:
///   - the pool's own contract × all `update-bin-balances*` actions
///   - the core contract × all swap/fee/status actions
pub trait TopicList {
    fn topics(&self) -> Vec<StacksTopic>;
}

/// Universal pool interface. Mirrors `evm-dex-pool::PoolInterface` in shape;
/// adapted types: Principal / u128 / StacksEvent.
pub trait PoolInterface: Debug + Send + Sync + PoolTypeTrait + EventApplicable + TopicList {
    /// Quote: given an input token + amount, return the output amount.
    fn calculate_output(&self, token_in: &Principal, amount_in: u128) -> Result<u128>;

    /// Reverse quote: given an output token + desired amount, return required
    /// input. May not be supported on all pool types; default impl errors.
    fn calculate_input(&self, _token_out: &Principal, _amount_out: u128) -> Result<u128> {
        Err(anyhow::anyhow!(
            "calculate_input not implemented for {}",
            self.pool_type()
        ))
    }

    /// Apply a swap to the pool state (used for speculative quoting on pending
    /// blocks; the collector applies events from real txs instead via
    /// `apply_event`). Optional — default no-op.
    fn apply_swap(
        &mut self,
        _token_in: &Principal,
        _amount_in: u128,
        _amount_out: u128,
    ) -> Result<()> {
        Ok(())
    }

    /// Stable identifier (typically `pool_contract.to_string()`).
    fn id(&self) -> String;

    /// The pool's contract principal (the contract that holds state).
    fn pool_contract(&self) -> &Principal;

    /// (x_token, y_token) — pool ordering, NOT base/quote.
    fn tokens(&self) -> (&Principal, &Principal);

    /// Total fee in basis points for the x→y direction at quote time.
    /// (Per-direction fees are pool-type-specific; this is the simple summary.)
    fn fee_bps(&self) -> u32;

    /// Is `token` either x or y of this pool?
    fn contains_token(&self, token: &Principal) -> bool {
        let (x, y) = self.tokens();
        token == x || token == y
    }

    /// Clone the pool into a Boxed trait object (registry uses this to hand
    /// out copies for speculative reads).
    fn clone_box(&self) -> Box<dyn PoolInterface + Send + Sync>;

    /// Pretty one-line summary for logging.
    fn log_summary(&self) -> String;

    /// Downcast helpers — let consumers reach into pool-type-specific fields.
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl dyn PoolInterface + Send + Sync {
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.as_any().downcast_ref::<T>()
    }
    pub fn downcast_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.as_any_mut().downcast_mut::<T>()
    }
}
