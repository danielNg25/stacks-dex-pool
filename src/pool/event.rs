//! Decoded Stacks events.
//!
//! Hiro's `/extended/v1/contract/<id>/events` endpoint returns events with a
//! Clarity-encoded `value.hex` payload. The collector decodes that payload to
//! a `ClarityValue::Tuple { action, data }` and wraps it in `StacksEvent`
//! before dispatching to pools.
//!
//! The `StacksTopic` is what the collector uses to subscribe — `(contract, action)`.
//! Mirror of EVM's `(address, topic-hash)` filter, but on Stacks topics are
//! action-name strings (since events are always `print`-topic in our case).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::codec::clarity::ClarityValue;
use crate::pool::principal::Principal;

/// One decoded event ready to be applied to a pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StacksEvent {
    /// The contract that emitted the event (pool contract for `update-bin-balances*`,
    /// core contract for `swap-*` and admin events).
    pub emitter: Principal,
    /// `tx_id` of the originating transaction. Used for dedup `(tx_id, event_index)`.
    pub tx_id: String,
    /// Per-tx event index. Increases monotonically within a transaction.
    pub event_index: u32,
    /// Action name from the payload's `action` field (e.g. `"swap-x-for-y"`).
    pub action: String,
    /// The `data` tuple from the payload, decoded.
    pub data: BTreeMap<String, ClarityValue>,
}

impl StacksEvent {
    /// Convenience: a uint field from `data`, or None.
    pub fn data_uint(&self, key: &str) -> Option<u128> {
        match self.data.get(key)? {
            ClarityValue::Uint(n) => Some(*n),
            _ => None,
        }
    }
    /// Convenience: an int field from `data`, or None.
    pub fn data_int(&self, key: &str) -> Option<i128> {
        match self.data.get(key)? {
            ClarityValue::Int(n) => Some(*n),
            _ => None,
        }
    }
    /// Convenience: a principal field from `data`, or None.
    pub fn data_principal(&self, key: &str) -> Option<&Principal> {
        match self.data.get(key)? {
            ClarityValue::Principal(p) => Some(p),
            _ => None,
        }
    }
}

/// Subscription key the collector uses to know what to poll.
///
/// `(contract, action)` — note that for cross-pool cores (DLMM's
/// `dlmm-core-v-1-1`) the same `(core, action)` topic will be dispatched to
/// every pool sharing that core; each pool's `apply_event` filters via
/// `data["pool-contract"]`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StacksTopic {
    pub contract: Principal,
    pub action: String,
}

impl StacksTopic {
    pub fn new(contract: Principal, action: impl Into<String>) -> Self {
        Self {
            contract,
            action: action.into(),
        }
    }
}
