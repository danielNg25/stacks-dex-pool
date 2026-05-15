//! Bitflow V2 StableSwap — `stableswap-pool-*-*-v-1-N` pools bound to a
//! shared `stableswap-core-v-1-N` for swap math.
//!
//! Math is byte-exact with `stableswap-core-v-1-X.get-dy`:
//! 1. Scale reserves and `dx` up to `max(x_dp, y_dp)`.
//! 2. Charge `protocol + provider` BPS fees off the scaled dx.
//! 3. Apply pool's `(midpoint_num / midpoint_den)` to dx and x-balance —
//!    handles non-1:1 pegs (e.g. STX/stSTX where stSTX accrues yield).
//! 4. Newton-Raphson `get_y` (see [`crate::stableswap::curve`]).
//! 5. Scale `new_y` back down to y's native units.
//! 6. Output `dy = y_balance - new_y` (saturating).
//!
//! Reference: [arbitrage-rs/crates/stacks/src/bitflow_v2_stable.rs:241-270].
//!
//! ## Per-pool fields
//! Each pool stores its own amp, threshold, midpoint, and per-direction fees.
//! The core address is `core_contract` — read once at bootstrap from the
//! pool's `core-address` field (NEVER hardcoded: different pools may bind
//! different core versions).
//!
//! ## Events (probed live 2026-05)
//! Mirror pattern matches [`crate::v2::bitflow_xyk`]:
//!
//! - Pool's own contract emits `update-pool-balances` with post-swap
//!   `x-balance`, `y-balance`, and `d` (the new invariant).
//! - Core emits `swap-x-for-y` / `swap-y-for-x` / `set-midpoint` carrying
//!   `pool-contract` for cross-pool filter.
//!
//! For reserves the pool-emitted event is authoritative (no cross-pool filter
//! needed since per-pool emitter). For midpoint changes we subscribe to the
//! core's `set-midpoint` and filter by `pool-contract`.

use std::any::Any;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use super::curve::{get_y, scale_down, scale_up};
use crate::pool::base::{EventApplicable, PoolInterface, PoolType, PoolTypeTrait, TopicList};
use crate::pool::event::{StacksEvent, StacksTopic};
use crate::pool::principal::Principal;

const BPS: u128 = 10_000;

pub const BITFLOW_V2_STABLE_UPDATE_POOL_BALANCES: &str = "update-pool-balances";
pub const BITFLOW_V2_STABLE_SET_MIDPOINT: &str = "set-midpoint";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitflowStableSwapV2Pool {
    pub pool_contract: Principal,
    /// `core-address` read from `<pool>::get-pool`. Pools may bind different
    /// core versions; MUST be read per pool, not hardcoded.
    pub core_contract: Principal,
    pub x_token: Principal,
    pub y_token: Principal,
    pub x_decimals: u8,
    pub y_decimals: u8,
    pub x_balance: u128,
    pub y_balance: u128,
    pub x_protocol_fee_bps: u32,
    pub x_provider_fee_bps: u32,
    pub y_protocol_fee_bps: u32,
    pub y_provider_fee_bps: u32,
    /// Curve amplification coefficient (typical 25-100).
    pub amp: u128,
    /// Convergence threshold for Newton-Raphson (typically 2).
    pub threshold: u128,
    /// Midpoint multiplier `(num / den)` for non-1:1 pegs. e.g. STX/stSTX
    /// uses `1_000_000 / 1_172_209` to reflect stSTX's accrued yield. Pools
    /// on cores ≥ v-1-3 carry these fields; older cores treat as 1/1.
    pub midpoint_num: u128,
    pub midpoint_den: u128,
    /// `false` = pool administratively paused. Quotes return 0.
    pub pool_status: bool,
    pub last_tx_id: Option<String>,
}

/// Full Curve + midpoint + scaling quote. `protocol_fee` + `provider_fee` are bps.
#[allow(clippy::too_many_arguments)]
pub fn quote_amount_out(
    dx: u128,
    x_balance: u128,
    y_balance: u128,
    x_decimals: u8,
    y_decimals: u8,
    protocol_fee_bps: u32,
    provider_fee_bps: u32,
    midpoint_num: u128,
    midpoint_den: u128,
    amp: u128,
    threshold: u128,
) -> u128 {
    if dx == 0 || x_balance == 0 || y_balance == 0 || midpoint_den == 0 {
        return 0;
    }
    let (bx_s, by_s) = scale_up(x_balance, y_balance, x_decimals, y_decimals);
    let (dx_s, _) = scale_up(dx, 0, x_decimals, y_decimals);

    let fee_protocol = dx_s.saturating_mul(protocol_fee_bps as u128) / BPS;
    let fee_provider = dx_s.saturating_mul(provider_fee_bps as u128) / BPS;
    let dx_after_fees = dx_s.saturating_sub(fee_protocol + fee_provider);

    let dx_mid = dx_after_fees.saturating_mul(midpoint_num) / midpoint_den;
    let bx_mid = bx_s.saturating_mul(midpoint_num) / midpoint_den;

    let new_y_s = get_y(dx_mid, bx_mid, by_s, amp, threshold);
    let (_, new_y) = scale_down(0, new_y_s, x_decimals, y_decimals);
    y_balance.saturating_sub(new_y)
}

impl BitflowStableSwapV2Pool {
    pub fn quote_x_for_y(&self, dx: u128) -> u128 {
        if !self.pool_status {
            return 0;
        }
        quote_amount_out(
            dx,
            self.x_balance,
            self.y_balance,
            self.x_decimals,
            self.y_decimals,
            self.x_protocol_fee_bps,
            self.x_provider_fee_bps,
            self.midpoint_num,
            self.midpoint_den,
            self.amp,
            self.threshold,
        )
    }

    /// y → x quote: reuses [`quote_amount_out`] with the reserves swapped and
    /// the **inverse** midpoint applied (`den / num`). Reflects the on-chain
    /// behaviour where `get-dy` flips midpoint for the reverse direction.
    pub fn quote_y_for_x(&self, dy: u128) -> u128 {
        if !self.pool_status {
            return 0;
        }
        quote_amount_out(
            dy,
            self.y_balance,
            self.x_balance,
            self.y_decimals,
            self.x_decimals,
            self.y_protocol_fee_bps,
            self.y_provider_fee_bps,
            self.midpoint_den,
            self.midpoint_num,
            self.amp,
            self.threshold,
        )
    }

    pub fn x_fee_bps(&self) -> u32 {
        self.x_protocol_fee_bps + self.x_provider_fee_bps
    }
    pub fn y_fee_bps(&self) -> u32 {
        self.y_protocol_fee_bps + self.y_provider_fee_bps
    }
}

impl PoolTypeTrait for BitflowStableSwapV2Pool {
    fn pool_type(&self) -> PoolType {
        PoolType::BitflowStableSwap
    }
}

impl EventApplicable for BitflowStableSwapV2Pool {
    fn apply_event(&mut self, event: &StacksEvent) -> Result<()> {
        super::events::apply_bitflow_v2_stable_event(self, event);
        Ok(())
    }
}

impl TopicList for BitflowStableSwapV2Pool {
    fn topics(&self) -> Vec<StacksTopic> {
        vec![
            // Pool-emitted reserve update — primary source of truth.
            StacksTopic::new(
                self.pool_contract.clone(),
                BITFLOW_V2_STABLE_UPDATE_POOL_BALANCES,
            ),
            // Core-emitted midpoint change — filtered by pool-contract.
            StacksTopic::new(self.core_contract.clone(), BITFLOW_V2_STABLE_SET_MIDPOINT),
        ]
    }
}

impl PoolInterface for BitflowStableSwapV2Pool {
    fn calculate_output(&self, token_in: &Principal, amount_in: u128) -> Result<u128> {
        if token_in == &self.x_token {
            Ok(self.quote_x_for_y(amount_in))
        } else if token_in == &self.y_token {
            Ok(self.quote_y_for_x(amount_in))
        } else {
            Err(anyhow!(
                "BitflowStableSwapV2Pool: token_in {} matches neither x ({}) nor y ({})",
                token_in,
                self.x_token,
                self.y_token,
            ))
        }
    }
    fn id(&self) -> String {
        // Per-pool contract — naturally unique.
        self.pool_contract.to_string()
    }
    fn pool_contract(&self) -> &Principal {
        &self.pool_contract
    }
    fn tokens(&self) -> (&Principal, &Principal) {
        (&self.x_token, &self.y_token)
    }
    fn fee_bps(&self) -> u32 {
        self.x_fee_bps()
    }
    fn clone_box(&self) -> Box<dyn PoolInterface + Send + Sync> {
        Box::new(self.clone())
    }
    fn log_summary(&self) -> String {
        format!(
            "BitflowV2Stable[{}] x={} y={} amp={} midpoint={}/{} fees(x→y)={}bps status={}",
            self.pool_contract,
            self.x_balance,
            self.y_balance,
            self.amp,
            self.midpoint_num,
            self.midpoint_den,
            self.x_fee_bps(),
            self.pool_status,
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

    /// Byte-exact fixture #1: STX/stSTX with non-trivial midpoint.
    /// Pinned in arbitrage-rs/crates/stacks/src/bitflow_v2_stable.rs:541-558
    /// (5700.490075 STX → 4889.662235 stSTX).
    #[test]
    fn quote_stx_ststx_fixture_byte_exact() {
        let dy = quote_amount_out(
            5_700_490_075,
            1_140_098_015_048,
            1_730_142_819_600,
            6,
            6,
            4,
            6, // 4 + 6 = 10 bps total
            1_000_000,
            1_172_209,
            100,
            2,
        );
        assert_eq!(dy, 4_889_662_235);
    }

    /// Byte-exact fixture #2: sBTC/pBTC with identity midpoint.
    /// Pinned in arbitrage-rs/crates/stacks/src/bitflow_v2_stable.rs:560-565.
    #[test]
    fn quote_sbtc_pbtc_fixture_byte_exact() {
        let dy = quote_amount_out(842_271, 168_454_282, 192_971_760, 8, 8, 4, 6, 1, 1, 25, 2);
        assert_eq!(dy, 845_716);
    }

    fn make_pool() -> BitflowStableSwapV2Pool {
        BitflowStableSwapV2Pool {
            pool_contract:
                "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.stableswap-pool-stx-ststx-v-1-4"
                    .parse()
                    .unwrap(),
            core_contract: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.stableswap-core-v-1-4"
                .parse()
                .unwrap(),
            x_token: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.token-stx-v-1-2"
                .parse()
                .unwrap(),
            y_token: "SP4SZE494VC2YC5JYG7AYFQ44F5Q4PYV7DVMDPBG.ststx-token"
                .parse()
                .unwrap(),
            x_decimals: 6,
            y_decimals: 6,
            x_balance: 1_140_098_015_048,
            y_balance: 1_730_142_819_600,
            x_protocol_fee_bps: 4,
            x_provider_fee_bps: 6,
            y_protocol_fee_bps: 4,
            y_provider_fee_bps: 6,
            amp: 100,
            threshold: 2,
            midpoint_num: 1_000_000,
            midpoint_den: 1_172_209,
            pool_status: true,
            last_tx_id: None,
        }
    }

    #[test]
    fn quote_x_for_y_uses_fixture() {
        let p = make_pool();
        let dy = p.quote_x_for_y(5_700_490_075);
        assert_eq!(dy, 4_889_662_235);
    }

    #[test]
    fn calculate_output_routes_x_or_y() {
        let p = make_pool();
        // x → y
        let dy = p.calculate_output(&p.x_token, 5_700_490_075).unwrap();
        assert_eq!(dy, 4_889_662_235);
        // y → x — flip direction; result > 0 and < x_balance.
        let dx = p.calculate_output(&p.y_token, 1_000_000_000).unwrap();
        assert!(dx > 0);
        assert!(dx < p.x_balance);
    }

    #[test]
    fn paused_pool_returns_zero() {
        let mut p = make_pool();
        p.pool_status = false;
        assert_eq!(p.quote_x_for_y(5_700_490_075), 0);
        assert_eq!(p.quote_y_for_x(1_000_000_000), 0);
    }

    #[test]
    fn topics_pool_balances_plus_core_midpoint() {
        let p = make_pool();
        let ts = p.topics();
        assert_eq!(ts.len(), 2);
        assert!(ts
            .iter()
            .any(|t| t.contract == p.pool_contract
                && t.action == BITFLOW_V2_STABLE_UPDATE_POOL_BALANCES));
        assert!(ts
            .iter()
            .any(|t| t.contract == p.core_contract && t.action == BITFLOW_V2_STABLE_SET_MIDPOINT));
    }

    #[test]
    fn calculate_output_unknown_token_errors() {
        let p = make_pool();
        let stranger: Principal = "SP000000000000000000002Q6VF78.stranger".parse().unwrap();
        assert!(p.calculate_output(&stranger, 1).is_err());
    }
}
