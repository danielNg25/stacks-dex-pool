//! DLMM event-application logic with cross-pool filter.
//!
//! Three concerns:
//!   1. Action classification — `INDEXED_ACTIONS` (apply), `KNOWN_INFORMATIONAL`
//!      (silently drop). Anything else is unknown and the caller should log.
//!   2. Cross-pool filter — the core engine emits events for all DLMM pools.
//!      Every core-emitted payload carries `pool-contract`; we drop events
//!      whose pool-contract doesn't match `self`.
//!   3. Per-action mutation — bin balance updates, active-bin transitions,
//!      fee changes, status changes.
//!
//! Direct port of [`test/fetch_bitflow_dlmm.py:432-507`] including the fix
//! at lines 447-461. NOTE: `shares` is deliberately NOT tracked — see
//! [`NOTES_bitflow_dlmm.md §12`].

use anyhow::Result;

use super::pool::{BinState, DLMMPool};
use super::CENTER_BIN_ID;
use crate::pool::event::StacksEvent;

/// Action strings whose events we APPLY to mirror state.
pub const INDEXED_ACTIONS: &[&str] = &[
    // Pool-emitted (per-pool contract, scoped by event-stream contract):
    "update-bin-balances",
    "update-bin-balances-on-withdraw",
    // Core-emitted (shared engine — MUST be filtered by pool-contract):
    "swap-x-for-y",
    "swap-y-for-x",
    "set-x-fees",
    "set-y-fees",
    "set-variable-fees",
    "reset-variable-fees",
    "set-pool-status",
];

/// Action strings whose events we DROP because their effect is captured by an
/// `INDEXED_ACTIONS` event on the same tx (or because they don't affect
/// quote-relevant state).
pub const KNOWN_INFORMATIONAL: &[&str] = &[
    "add-liquidity",      // redundant — pool emits update-bin-balances
    "withdraw-liquidity", // redundant — pool emits update-bin-balances-on-withdraw
    "pool-transfer",      // LP token transfer between users
    "pool-burn",          // LP token burn (bin state delta arrives separately)
    "pool-mint",          // LP token mint   (bin state delta arrives separately)
];

/// Apply an event to a `DLMMPool`. Returns `Ok(())` whether the event was
/// state-changing or a no-op; only returns `Err` on genuine decode trouble.
///
/// Cross-pool filter at the top: if `data["pool-contract"]` is present and
/// doesn't match `pool.pool_contract`, return immediately. This handles the
/// case where the core engine emits events for OTHER pools — without this
/// filter every swap on any DLMM pool would corrupt our mirror.
///
/// On any successful apply (the action matched an indexed action AND the
/// payload had the fields needed to mutate state) this also stamps
/// `pool.last_tx_id` and `pool.last_event_at`. Consumers read these as the
/// "freshness" signal for the mirror — without this, a DLMM mirror that
/// hasn't seen an event since bootstrap looks indistinguishable from one
/// that's been silently failing for hours.
pub fn apply_event(pool: &mut DLMMPool, event: &StacksEvent) -> Result<()> {
    // 1. Cross-pool filter (the critical fix).
    if let Some(target) = event.data_principal("pool-contract") {
        if target != &pool.pool_contract {
            return Ok(());
        }
    }

    // Pool-name suffix only for log readability; full principal is on the event.
    let pool_label = pool
        .pool_contract
        .contract_name()
        .unwrap_or("?")
        .to_string();
    let tx_short: String = event.tx_id.chars().take(12).collect();

    let applied = match event.action.as_str() {
        "update-bin-balances" | "update-bin-balances-on-withdraw" => {
            if let Some(detail) = apply_update_bin_balances(pool, event) {
                log::debug!(
                    "apply [{}] {} bin {:+} ← (x={}, y={}) tx={}",
                    pool_label,
                    event.action,
                    detail.bin_signed,
                    detail.x,
                    detail.y,
                    tx_short
                );
                true
            } else {
                false
            }
        }
        "swap-x-for-y" | "swap-y-for-x" => {
            if let Some(new_active) = apply_swap(pool, event) {
                log::debug!(
                    "apply [{}] {} active → {:+} tx={}",
                    pool_label,
                    event.action,
                    new_active,
                    tx_short
                );
                true
            } else {
                false
            }
        }
        "set-x-fees" => {
            let mut changed = false;
            if let Some(p) = event.data_uint("protocol-fee") {
                pool.x_protocol_fee = p as u32;
                changed = true;
            }
            if let Some(p) = event.data_uint("provider-fee") {
                pool.x_provider_fee = p as u32;
                changed = true;
            }
            if changed {
                log::debug!(
                    "apply [{}] set-x-fees → protocol={} provider={} tx={}",
                    pool_label,
                    pool.x_protocol_fee,
                    pool.x_provider_fee,
                    tx_short
                );
            }
            changed
        }
        "set-y-fees" => {
            let mut changed = false;
            if let Some(p) = event.data_uint("protocol-fee") {
                pool.y_protocol_fee = p as u32;
                changed = true;
            }
            if let Some(p) = event.data_uint("provider-fee") {
                pool.y_provider_fee = p as u32;
                changed = true;
            }
            if changed {
                log::debug!(
                    "apply [{}] set-y-fees → protocol={} provider={} tx={}",
                    pool_label,
                    pool.y_protocol_fee,
                    pool.y_provider_fee,
                    tx_short
                );
            }
            changed
        }
        "set-variable-fees" => {
            let mut changed = false;
            if let Some(p) = event.data_uint("x-fee") {
                pool.x_variable_fee = p as u32;
                changed = true;
            }
            if let Some(p) = event.data_uint("y-fee") {
                pool.y_variable_fee = p as u32;
                changed = true;
            }
            if changed {
                log::debug!(
                    "apply [{}] set-variable-fees → x={} y={} tx={}",
                    pool_label,
                    pool.x_variable_fee,
                    pool.y_variable_fee,
                    tx_short
                );
            }
            changed
        }
        "reset-variable-fees" => {
            pool.x_variable_fee = 0;
            pool.y_variable_fee = 0;
            log::debug!(
                "apply [{}] reset-variable-fees → x=0 y=0 tx={}",
                pool_label,
                tx_short
            );
            true
        }
        "set-pool-status" => {
            // We don't currently mirror pool status — but a Rust adapter that
            // needs to honour pause should add a `pub status: bool` field on
            // `DLMMPool` and read `data["status"]` here. We still mark it
            // applied so the freshness timestamp advances.
            log::debug!(
                "apply [{}] set-pool-status (not mirrored) tx={}",
                pool_label,
                tx_short
            );
            true
        }
        // Anything else is informational or unknown — silently drop and do
        // not advance the freshness watermark.
        _ => false,
    };

    if applied {
        pool.last_tx_id = Some(event.tx_id.clone());
        pool.last_event_at = Some(now_epoch_secs());
    }
    Ok(())
}

/// Current Unix epoch in whole seconds. Used to stamp
/// [`DLMMPool::last_event_at`] on successful apply. Returns `0` if the system
/// clock is before the epoch (unreachable in practice; the conversion can't
/// panic).
fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

struct BinUpdateDetail {
    bin_signed: i32,
    x: u128,
    y: u128,
}

fn apply_update_bin_balances(pool: &mut DLMMPool, event: &StacksEvent) -> Option<BinUpdateDetail> {
    // bin-id in pool events is UNSIGNED (0..=1000). Convert to signed.
    let unsigned = event.data_uint("bin-id")?;
    let unsigned = i32::try_from(unsigned).ok()?;
    let signed = unsigned - CENTER_BIN_ID;

    let x = event.data_uint("x-balance")?;
    let y = event.data_uint("y-balance")?;
    pool.bins.insert(signed, BinState { x, y });
    Some(BinUpdateDetail {
        bin_signed: signed,
        x,
        y,
    })
}

fn apply_swap(pool: &mut DLMMPool, event: &StacksEvent) -> Option<i32> {
    // The core's swap events carry `updated-active-bin-id` as a signed Clarity
    // int. (Same field is also surfaced as `active-bin-id` on the same event;
    // we read the explicit "updated-" form to match the contract's intent.)
    let new_active = event
        .data_int("updated-active-bin-id")
        .or_else(|| event.data_int("active-bin-id"))?;
    let v = i32::try_from(new_active).ok()?;
    pool.active_bin_id = v;
    Some(v)
}
