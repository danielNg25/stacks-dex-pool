//! ALEX AMM v2 (`amm-pool-v2-01`) — constant-product with `factor` scaling.
//!
//! We support only constant-product pools (`factor == 1e8`). Pools with
//! non-CP factor are liquid-staking rebalancers (e.g. STX↔stSTX with
//! factor=0.05) and irrelevant for arbitrage paths; quoting on those errors.
//!
//! Math byte-exact with the on-chain `amm-pool-v2-01::get-y-given-x` helper.
//! Reference: [arbitrage-rs/crates/stacks/src/alex.rs:123-160].
//!
//! Fee handling: charged on input via Clarity's `mul-up` (ceiling div on
//! 8-dp fixed point). The fee rate itself is 8-dp fixed (e.g. 0.003 fee =
//! 300_000 in raw units).
//!
//! ## Events (probed live 2026-05)
//! ALEX's `amm-pool-v2-01` is **singleton**: one contract for every ALEX
//! pool. Each pool is identified by a numeric `pool-id` (uint), assigned at
//! creation, so the mirror MUST filter cross-pool by `data.pool-id == self.pool_id`.
//!
//! - Actions: `swap-x-for-y` | `swap-y-for-x`
//! - Reserves carried in `data.balance-x` / `data.balance-y` (post-swap)
//! - Top-level (outside `data`) also has the swap deltas — `dx`, `dy`, `fee`,
//!   `fee-rebate` — useful for sanity-checking but not needed for mirror.

use std::any::Any;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::pool::base::{EventApplicable, PoolInterface, PoolType, PoolTypeTrait, TopicList};
use crate::pool::event::{StacksEvent, StacksTopic};
use crate::pool::principal::Principal;

/// ALEX uses 8-dp fixed point for `factor` and `fee_rate`.
pub const ONE_E8: u128 = 100_000_000;
/// `factor = 1e8` denotes a constant-product pool.
pub const CONSTANT_PRODUCT_FACTOR: u128 = ONE_E8;

pub const ALEX_SWAP_X_FOR_Y: &str = "swap-x-for-y";
pub const ALEX_SWAP_Y_FOR_X: &str = "swap-y-for-x";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlexPool {
    /// `amm-pool-v2-01` — same singleton principal for every ALEX pool.
    pub pool_contract: Principal,
    /// On-chain numeric pool identifier. Required as the cross-pool filter
    /// for incoming events (`data.pool-id == self.pool_id`). Read at
    /// bootstrap from `amm-pool-v2-01::get-pool-details` (`pool-id` field)
    /// or assigned by the deployer.
    pub pool_id: u128,
    pub x_token: Principal,
    pub y_token: Principal,
    pub x_decimals: u8,
    pub y_decimals: u8,
    pub balance_x: u128,
    pub balance_y: u128,
    /// 8-dp fixed; we only support `CONSTANT_PRODUCT_FACTOR`.
    pub factor: u128,
    /// 8-dp fixed input-fee rate. e.g. 30bps = 300_000.
    pub fee_rate_x: u128,
    pub fee_rate_y: u128,
    pub last_tx_id: Option<String>,
}

/// Ceiling division `ceil(a*b / 1e8)` — mirrors Clarity's `mul-up` helper.
pub fn mul_up(a: u128, b: u128) -> u128 {
    let product = a.saturating_mul(b);
    if product == 0 {
        0
    } else {
        1 + (product - 1) / ONE_E8
    }
}

/// Fee-less GMMM quote for CP pools — `dy = dx * y / (x + dx)`.
pub fn gmmm_dy(x: u128, y: u128, factor: u128, dx: u128) -> Result<u128> {
    if factor != CONSTANT_PRODUCT_FACTOR {
        return Err(anyhow!(
            "AlexPool only supports constant-product pools (factor=1e8); got {}",
            factor
        ));
    }
    if dx == 0 || x == 0 || y == 0 {
        return Ok(0);
    }
    let num = dx.saturating_mul(y);
    Ok(num / x.saturating_add(dx))
}

/// Full ALEX swap quote: `fee = mul_up(dx, fee_rate); dy = gmmm(x, y, dx - fee)`.
pub fn quote_with_fee(x: u128, y: u128, factor: u128, dx: u128, fee_rate: u128) -> Result<u128> {
    let fee = mul_up(dx, fee_rate);
    if fee >= dx {
        return Ok(0);
    }
    gmmm_dy(x, y, factor, dx - fee)
}

impl AlexPool {
    pub fn quote_x_for_y(&self, dx: u128) -> u128 {
        quote_with_fee(
            self.balance_x,
            self.balance_y,
            self.factor,
            dx,
            self.fee_rate_x,
        )
        .unwrap_or(0)
    }
    pub fn quote_y_for_x(&self, dy: u128) -> u128 {
        quote_with_fee(
            self.balance_y,
            self.balance_x,
            self.factor,
            dy,
            self.fee_rate_y,
        )
        .unwrap_or(0)
    }

    /// Approximate bps form of the input-side x→y fee rate.
    pub fn fee_bps_effective_x(&self) -> u32 {
        // fee_rate is 8-dp fixed: bps ≈ rate * 10_000 / 1e8.
        (self.fee_rate_x.saturating_mul(10_000) / ONE_E8) as u32
    }
}

impl PoolTypeTrait for AlexPool {
    fn pool_type(&self) -> PoolType {
        PoolType::StacksUniswapV2
    }
}

impl EventApplicable for AlexPool {
    fn apply_event(&mut self, event: &StacksEvent) -> Result<()> {
        super::events::apply_alex_event(self, event);
        Ok(())
    }
}

impl TopicList for AlexPool {
    fn topics(&self) -> Vec<StacksTopic> {
        vec![
            StacksTopic::new(self.pool_contract.clone(), ALEX_SWAP_X_FOR_Y),
            StacksTopic::new(self.pool_contract.clone(), ALEX_SWAP_Y_FOR_X),
        ]
    }
}

impl PoolInterface for AlexPool {
    fn calculate_output(&self, token_in: &Principal, amount_in: u128) -> Result<u128> {
        if token_in == &self.x_token {
            Ok(self.quote_x_for_y(amount_in))
        } else if token_in == &self.y_token {
            Ok(self.quote_y_for_x(amount_in))
        } else {
            Err(anyhow!(
                "AlexPool: token_in {} matches neither x ({}) nor y ({})",
                token_in,
                self.x_token,
                self.y_token,
            ))
        }
    }
    fn id(&self) -> String {
        // `pool_contract` is the singleton; `pool_id` is the per-pool key.
        format!("{}#{}", self.pool_contract, self.pool_id)
    }
    fn pool_contract(&self) -> &Principal {
        &self.pool_contract
    }
    fn tokens(&self) -> (&Principal, &Principal) {
        (&self.x_token, &self.y_token)
    }
    fn fee_bps(&self) -> u32 {
        self.fee_bps_effective_x()
    }
    fn clone_box(&self) -> Box<dyn PoolInterface + Send + Sync> {
        Box::new(self.clone())
    }
    fn log_summary(&self) -> String {
        format!(
            "ALEX[{}] x={} y={} factor={} fee_x={}/1e8",
            self.pool_contract, self.balance_x, self.balance_y, self.factor, self.fee_rate_x,
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

    /// Byte-exact fixture from arbitrage-rs/crates/stacks/src/alex.rs:384-393
    /// (STX→ALEX, fee-less branch).
    #[test]
    fn gmmm_dy_stx_alex_fixture() {
        let dy = gmmm_dy(
            28_249_703_881_723,
            8_628_053_857_398_893,
            CONSTANT_PRODUCT_FACTOR,
            141_248_519_408,
        )
        .unwrap();
        assert_eq!(dy, 42_925_641_081_400);
    }

    /// Second fee-less fixture from arbitrage-rs/crates/stacks/src/alex.rs:397-406
    /// (STX→aBTC).
    #[test]
    fn gmmm_dy_stx_abtc_fixture() {
        let dy = gmmm_dy(
            406_253_836_405,
            1_344_229,
            CONSTANT_PRODUCT_FACTOR,
            2_031_269_182,
        )
        .unwrap();
        assert_eq!(dy, 6_687);
    }

    #[test]
    fn mul_up_ceiling_boundaries() {
        assert_eq!(mul_up(0, 500_000), 0);
        assert_eq!(mul_up(1, 0), 0);
        assert_eq!(mul_up(1, 500_000), 1);
        assert_eq!(mul_up(ONE_E8, 500_000), 500_000);
    }

    #[test]
    fn non_cp_factor_rejected() {
        let r = gmmm_dy(1_000_000, 1_000_000, 5_000_000, 100);
        assert!(r.is_err());
    }

    #[test]
    fn quote_with_fee_lowers_dy_vs_no_fee() {
        let with_fee = quote_with_fee(
            1_000_000_000,
            1_000_000_000,
            CONSTANT_PRODUCT_FACTOR,
            1_000_000,
            300_000, // 0.003 = 30 bps
        )
        .unwrap();
        let no_fee = gmmm_dy(
            1_000_000_000,
            1_000_000_000,
            CONSTANT_PRODUCT_FACTOR,
            1_000_000,
        )
        .unwrap();
        assert!(with_fee > 0);
        assert!(with_fee < no_fee);
    }
}
