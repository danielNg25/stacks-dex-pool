//! `DLMMPool` struct + PoolInterface impl.
//!
//! Per-bin state stored in a `BTreeMap<i32, BinState>` keyed by signed bin id.
//! Walking outward from the active bin is a sorted range scan — BTreeMap gives
//! `O(log N + K)` for that, way cheaper than HashMap iteration.
//!
//! NOTE: `BinState` deliberately omits `shares` (LP ownership). See plan
//! §"DLMM module" and `NOTES_bitflow_dlmm.md §12` — quote math doesn't use it,
//! and tracking it correctly would require promoting `add-liquidity` events
//! from informational to indexed (a 5-line addition future work).

use std::any::Any;
use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use super::math::{bin_price as bin_price_fn, single_bin_swap_x_for_y, single_bin_swap_y_for_x};
use super::{MAX_BIN_ID, MIN_BIN_ID};
use crate::pool::base::{EventApplicable, PoolInterface, PoolType, PoolTypeTrait, TopicList};
use crate::pool::event::{StacksEvent, StacksTopic};
use crate::pool::principal::Principal;

/// Per-bin state. `shares` deliberately not tracked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BinState {
    pub x: u128,
    pub y: u128,
}

/// Full mirrored state for one Bitflow DLMM pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DLMMPool {
    pub pool_contract: Principal,
    pub core_contract: Principal,
    pub x_token: Principal,
    pub y_token: Principal,
    pub x_decimals: u8,
    pub y_decimals: u8,
    pub bin_step: u32,
    pub initial_price: u128,
    pub active_bin_id: i32, // signed [-500, +500]
    pub x_protocol_fee: u32,
    pub x_provider_fee: u32,
    pub y_protocol_fee: u32,
    pub y_provider_fee: u32,
    pub x_variable_fee: u32,
    pub y_variable_fee: u32,
    /// Per-bin (x, y) state. Keys are signed bin ids; missing keys = empty bin.
    pub bins: BTreeMap<i32, BinState>,
    /// Most-recently-processed event for this pool's contract stream. Used as
    /// a watermark by the collector; advanced after each successful apply.
    pub last_tx_id: Option<String>,
    /// Unix epoch seconds when [`last_tx_id`](Self::last_tx_id) was set.
    /// `None` means no event has been applied since bootstrap. Consumers
    /// (notably arb-stacks's DLMM venue) render "fresh / stale" from
    /// `now - last_event_at`.
    #[serde(default)]
    pub last_event_at: Option<u64>,
    /// Cached factor table for this pool's `bin_step`. Populated by the
    /// fetcher; bin-price math reads from this.
    #[serde(default)]
    pub factors: Vec<u128>,
}

impl DLMMPool {
    /// Total fee in bps for the x→y direction at quote time.
    pub fn x_fee_bps(&self) -> u32 {
        self.x_protocol_fee + self.x_provider_fee + self.x_variable_fee
    }
    /// Total fee in bps for the y→x direction at quote time.
    pub fn y_fee_bps(&self) -> u32 {
        self.y_protocol_fee + self.y_provider_fee + self.y_variable_fee
    }

    /// Multi-bin walker for x→y. Starts at the active bin and walks DOWN,
    /// consuming inventory until the input is fully spent or we run out of
    /// mirrored bins.
    ///
    /// Returns `(total_dy, last_bin_walked, window_exhausted)`. `window_exhausted=true`
    /// means we ran out of mirrored bins before consuming all input — the
    /// quote is a lower bound; the caller can bootstrap a wider window if
    /// they need accuracy at this size.
    pub fn quote_x_for_y(&self, x_amount: u128) -> (u128, Option<i32>, bool) {
        let mut remaining = x_amount;
        let mut bin_id = self.active_bin_id;
        let min_in_window = self
            .bins
            .keys()
            .next()
            .copied()
            .unwrap_or(self.active_bin_id);
        let mut total_dy: u128 = 0;
        let mut last_bin: Option<i32> = None;

        while remaining > 0 && bin_id >= MIN_BIN_ID {
            let Some(b) = self.bins.get(&bin_id).copied() else {
                bin_id -= 1;
                if bin_id < min_in_window {
                    return (total_dy, last_bin, remaining > 0);
                }
                continue;
            };
            if b.x == 0 && b.y == 0 {
                bin_id -= 1;
                if bin_id < min_in_window {
                    return (total_dy, last_bin, remaining > 0);
                }
                continue;
            }
            let Ok(bp) = bin_price_fn(self.initial_price, &self.factors, bin_id) else {
                break;
            };
            let r = single_bin_swap_x_for_y(
                remaining,
                b.x,
                b.y,
                bp,
                self.x_protocol_fee,
                self.x_provider_fee,
                self.x_variable_fee,
            );
            if r.used == 0 {
                break;
            }
            total_dy = total_dy.saturating_add(r.dy);
            last_bin = Some(bin_id);
            remaining -= r.used;
            if r.dy >= b.y {
                bin_id -= 1;
            } else {
                break;
            }
        }
        (total_dy, last_bin, remaining > 0)
    }

    /// Multi-bin walker for y→x. Starts at active and walks UP.
    pub fn quote_y_for_x(&self, y_amount: u128) -> (u128, Option<i32>, bool) {
        let mut remaining = y_amount;
        let mut bin_id = self.active_bin_id;
        let max_in_window = self
            .bins
            .keys()
            .next_back()
            .copied()
            .unwrap_or(self.active_bin_id);
        let mut total_dx: u128 = 0;
        let mut last_bin: Option<i32> = None;

        while remaining > 0 && bin_id <= MAX_BIN_ID {
            let Some(b) = self.bins.get(&bin_id).copied() else {
                bin_id += 1;
                if bin_id > max_in_window {
                    return (total_dx, last_bin, remaining > 0);
                }
                continue;
            };
            if b.x == 0 && b.y == 0 {
                bin_id += 1;
                if bin_id > max_in_window {
                    return (total_dx, last_bin, remaining > 0);
                }
                continue;
            }
            let Ok(bp) = bin_price_fn(self.initial_price, &self.factors, bin_id) else {
                break;
            };
            let r = single_bin_swap_y_for_x(
                remaining,
                b.x,
                b.y,
                bp,
                self.y_protocol_fee,
                self.y_provider_fee,
                self.y_variable_fee,
            );
            if r.used == 0 {
                break;
            }
            // For y→x the `dy` field on SingleBinSwap holds the x-output.
            total_dx = total_dx.saturating_add(r.dy);
            last_bin = Some(bin_id);
            remaining -= r.used;
            if r.dy >= b.x {
                bin_id += 1;
            } else {
                break;
            }
        }
        (total_dx, last_bin, remaining > 0)
    }
}

// -- PoolInterface plumbing ------------------------------------------------

impl PoolTypeTrait for DLMMPool {
    fn pool_type(&self) -> PoolType {
        PoolType::BitflowDlmm
    }
}

impl EventApplicable for DLMMPool {
    fn apply_event(&mut self, event: &StacksEvent) -> Result<()> {
        super::events::apply_event(self, event)
    }
}

impl TopicList for DLMMPool {
    fn topics(&self) -> Vec<StacksTopic> {
        let mut out = Vec::with_capacity(8);
        // Pool-emitted topics.
        out.push(StacksTopic::new(
            self.pool_contract.clone(),
            "update-bin-balances",
        ));
        out.push(StacksTopic::new(
            self.pool_contract.clone(),
            "update-bin-balances-on-withdraw",
        ));
        // Core-emitted topics (filtered by data["pool-contract"] in apply_event).
        for action in [
            "swap-x-for-y",
            "swap-y-for-x",
            "set-x-fees",
            "set-y-fees",
            "set-variable-fees",
            "reset-variable-fees",
            "set-pool-status",
        ] {
            out.push(StacksTopic::new(self.core_contract.clone(), action));
        }
        out
    }
}

impl PoolInterface for DLMMPool {
    fn calculate_output(&self, token_in: &Principal, amount_in: u128) -> Result<u128> {
        if token_in == &self.x_token {
            let (dy, _, exhausted) = self.quote_x_for_y(amount_in);
            if exhausted {
                // Caller may want to know this — for now, return the partial
                // amount (matches the contract's behaviour: the actual swap
                // would also only consume what's in bins).
            }
            Ok(dy)
        } else if token_in == &self.y_token {
            let (dx, _, _) = self.quote_y_for_x(amount_in);
            Ok(dx)
        } else {
            Err(anyhow!(
                "token {} is not in pool {}",
                token_in,
                self.pool_contract
            ))
        }
    }

    fn id(&self) -> String {
        self.pool_contract.to_string()
    }

    fn pool_contract(&self) -> &Principal {
        &self.pool_contract
    }

    fn tokens(&self) -> (&Principal, &Principal) {
        (&self.x_token, &self.y_token)
    }

    fn fee_bps(&self) -> u32 {
        // Default summary = x→y direction. Callers needing the other
        // direction can downcast and read `y_fee_bps()`.
        self.x_fee_bps()
    }

    fn clone_box(&self) -> Box<dyn PoolInterface + Send + Sync> {
        Box::new(self.clone())
    }

    fn log_summary(&self) -> String {
        format!(
            "DLMM[{}] active_bin={} bins={} x_fee={}bps y_fee={}bps",
            self.pool_contract,
            self.active_bin_id,
            self.bins.len(),
            self.x_fee_bps(),
            self.y_fee_bps(),
        )
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dlmm::PRICE_SCALE_BPS;

    fn fake_pool() -> DLMMPool {
        DLMMPool {
            pool_contract:
                "SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD.dlmm-pool-stx-usdcx-v-1-bps-10"
                    .parse()
                    .unwrap(),
            core_contract: "SP1PFR4V08H1RAZXREBGFFQ59WB739XM8VVGTFSEA.dlmm-core-v-1-1"
                .parse()
                .unwrap(),
            x_token: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.token-stx-v-1-2"
                .parse()
                .unwrap(),
            y_token: "SP466FNC0P7JWTNM2R9T199QRZN1MYEDTAR0KP27.usdcx"
                .parse()
                .unwrap(),
            x_decimals: 6,
            y_decimals: 6,
            bin_step: 10,
            initial_price: PRICE_SCALE_BPS,
            active_bin_id: 0,
            x_protocol_fee: 15,
            x_provider_fee: 15,
            y_protocol_fee: 15,
            y_provider_fee: 15,
            x_variable_fee: 0,
            y_variable_fee: 0,
            bins: [
                (
                    0,
                    BinState {
                        x: 1_000_000,
                        y: 1_000_000,
                    },
                ),
                (-1, BinState { x: 0, y: 1_000_000 }),
                (-2, BinState { x: 0, y: 1_000_000 }),
                (1, BinState { x: 1_000_000, y: 0 }),
            ]
            .into_iter()
            .collect(),
            last_tx_id: None,
            last_event_at: None,
            factors: {
                // center = PRICE_SCALE_BPS, neighbors via 10bp steps
                let mut v = vec![0u128; 1001];
                v[500] = PRICE_SCALE_BPS;
                v[501] = PRICE_SCALE_BPS * 10_010 / 10_000;
                v[499] = PRICE_SCALE_BPS * 10_000 / 10_010;
                v[498] = v[499] * 10_000 / 10_010;
                v[497] = v[498] * 10_000 / 10_010;
                v
            },
        }
    }

    #[test]
    fn quote_x_for_y_smoke() {
        let p = fake_pool();
        let (dy, _last_bin, _exhausted) = p.quote_x_for_y(1000);
        assert!(dy > 0, "expected non-zero dy");
    }

    #[test]
    fn pool_interface_calculate_output_unknown_token() {
        let p = fake_pool();
        let other: Principal = "SP000000000000000000002Q6VF78".parse().unwrap();
        assert!(p.calculate_output(&other, 100).is_err());
    }

    #[test]
    fn topics_include_pool_and_core() {
        let p = fake_pool();
        let ts = p.topics();
        // 2 pool actions + 7 core actions.
        assert_eq!(ts.len(), 9);
        let pool_count = ts.iter().filter(|t| t.contract == p.pool_contract).count();
        let core_count = ts.iter().filter(|t| t.contract == p.core_contract).count();
        assert_eq!(pool_count, 2);
        assert_eq!(core_count, 7);
    }
}
