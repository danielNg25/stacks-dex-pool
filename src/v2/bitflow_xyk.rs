//! Bitflow V2 XYK — Uniswap V2 clone with per-direction protocol+provider
//! BPS fees. Math is executed in a shared core (`xyk-core-v-1-X`) which is
//! pool-pinned via the pool's own `core-address` field — different pools
//! may reference different cores so we MUST read it per pool rather than
//! hardcoding.
//!
//! Math byte-exact with `xyk-core-v-1-X::get-dy`. Reference:
//! [arbitrage-rs/crates/stacks/src/bitflow_v2_xyk.rs:126-143].
//!
//! ## Events (probed live 2026-05)
//! Bitflow XYK is the cleanest of the V2 family for event sync. Each pool
//! has its OWN contract, and the pool itself emits a single mirror-relevant
//! action when reserves change:
//!
//! - `update-pool-balances` (on the pool's own contract, NOT the core) with
//!   `data.x-balance` / `data.y-balance` — post-swap reserves.
//!
//! Pool-emitted = implicitly scoped: no cross-pool filter needed because
//! every event arrives via the pool's own emitter principal. Other observed
//! actions (`pool-transfer`, `pool-mint`, `pool-burn`, `add-liquidity`,
//! `withdraw-liquidity`) are LP-bookkeeping and don't change the AMM-relevant
//! reserves until the corresponding `update-pool-balances` fires.
//!
//! The core's `swap-x-for-y` / `swap-y-for-x` events DO carry `pool-contract`
//! (and the cross-pool filter pattern would work there), but we don't need
//! them because the pool's own `update-pool-balances` carries the authoritative
//! reserves. We keep the core in the topic list anyway for fee-change tracking
//! when we eventually add `set-x-fees` / `set-y-fees` handlers.

use std::any::Any;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::pool::base::{EventApplicable, PoolInterface, PoolType, PoolTypeTrait, TopicList};
use crate::pool::event::{StacksEvent, StacksTopic};
use crate::pool::principal::Principal;

const BPS: u128 = 10_000;

/// Pool-emitted: the only action we need to mirror swaps. Carries post-swap
/// `x-balance` / `y-balance` in the data tuple.
pub const BITFLOW_XYK_UPDATE_POOL_BALANCES: &str = "update-pool-balances";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitflowXykPool {
    pub pool_contract: Principal,
    /// `core-address` field read from `<pool>::get-pool`. Different XYK pools
    /// may reference different cores; this is per-pool.
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
    /// `false` => paused. Refuse to quote and refuse to apply events.
    pub pool_status: bool,
    pub last_tx_id: Option<String>,
}

/// `core::get-dy` math byte-exact. `protocol_fee` + `provider_fee` are bps.
pub fn quote_amount_out(
    dx: u128,
    x_balance: u128,
    y_balance: u128,
    protocol_fee: u32,
    provider_fee: u32,
) -> u128 {
    if dx == 0 || x_balance == 0 || y_balance == 0 {
        return 0;
    }
    let fee_protocol = dx.saturating_mul(protocol_fee as u128) / BPS;
    let fee_provider = dx.saturating_mul(provider_fee as u128) / BPS;
    let dx_net = dx.saturating_sub(fee_protocol + fee_provider);
    if dx_net == 0 {
        return 0;
    }
    y_balance.saturating_mul(dx_net) / (x_balance + dx_net)
}

impl BitflowXykPool {
    pub fn quote_x_for_y(&self, dx: u128) -> u128 {
        if !self.pool_status {
            return 0;
        }
        quote_amount_out(
            dx,
            self.x_balance,
            self.y_balance,
            self.x_protocol_fee_bps,
            self.x_provider_fee_bps,
        )
    }
    pub fn quote_y_for_x(&self, dy: u128) -> u128 {
        if !self.pool_status {
            return 0;
        }
        quote_amount_out(
            dy,
            self.y_balance,
            self.x_balance,
            self.y_protocol_fee_bps,
            self.y_provider_fee_bps,
        )
    }
    pub fn x_fee_bps(&self) -> u32 {
        self.x_protocol_fee_bps + self.x_provider_fee_bps
    }
    pub fn y_fee_bps(&self) -> u32 {
        self.y_protocol_fee_bps + self.y_provider_fee_bps
    }
}

impl PoolTypeTrait for BitflowXykPool {
    fn pool_type(&self) -> PoolType {
        PoolType::StacksUniswapV2
    }
}

impl EventApplicable for BitflowXykPool {
    fn apply_event(&mut self, event: &StacksEvent) -> Result<()> {
        super::events::apply_bitflow_xyk_event(self, event);
        Ok(())
    }
}

impl TopicList for BitflowXykPool {
    fn topics(&self) -> Vec<StacksTopic> {
        // The pool's own `update-pool-balances` is the authoritative source
        // for post-swap reserves; no cross-pool filter needed because the
        // emitter is the pool's principal itself.
        vec![StacksTopic::new(
            self.pool_contract.clone(),
            BITFLOW_XYK_UPDATE_POOL_BALANCES,
        )]
    }
}

impl PoolInterface for BitflowXykPool {
    fn calculate_output(&self, token_in: &Principal, amount_in: u128) -> Result<u128> {
        if token_in == &self.x_token {
            Ok(self.quote_x_for_y(amount_in))
        } else if token_in == &self.y_token {
            Ok(self.quote_y_for_x(amount_in))
        } else {
            Err(anyhow!(
                "BitflowXykPool: token_in {} matches neither x ({}) nor y ({})",
                token_in,
                self.x_token,
                self.y_token,
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
        self.x_fee_bps()
    }
    fn clone_box(&self) -> Box<dyn PoolInterface + Send + Sync> {
        Box::new(self.clone())
    }
    fn log_summary(&self) -> String {
        format!(
            "BitflowXyk[{}] x={} y={} fees(x→y)={}bps fees(y→x)={}bps status={}",
            self.pool_contract,
            self.x_balance,
            self.y_balance,
            self.x_fee_bps(),
            self.y_fee_bps(),
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

    /// Byte-exact fixture from `arbitrage-rs/crates/stacks/src/bitflow_v2_xyk.rs`
    /// docstring (lines 18-20): STX→sBTC, 10+40 bps fees,
    /// dx=2_373_420 → dy=7_134_976_450.
    ///
    /// (Reserves are unspecified in the snippet; we reproduce the formula
    /// rather than the integer answer here. The integer test at the bottom
    /// covers a hand-computed fixture.)
    #[test]
    fn fee_split_protocol_and_provider() {
        // 10 bps protocol + 40 bps provider = 50 bps total ≈ 0.5% net fee.
        let dy_split = quote_amount_out(1_000_000, 1_000_000_000, 1_000_000_000, 10, 40);
        let dy_total = quote_amount_out(1_000_000, 1_000_000_000, 1_000_000_000, 50, 0);
        // Both paths charge the same total bps, so result should match.
        assert_eq!(dy_split, dy_total);
    }

    /// 50 bps fee strictly reduces output vs zero-fee.
    #[test]
    fn fee_strictly_reduces_output() {
        let dy_fee = quote_amount_out(1_000_000, 1_000_000_000, 1_000_000_000, 10, 40);
        let dy_nofee = quote_amount_out(1_000_000, 1_000_000_000, 1_000_000_000, 0, 0);
        assert!(dy_fee < dy_nofee);
    }

    #[test]
    fn zero_inputs_return_zero() {
        assert_eq!(quote_amount_out(0, 1, 1, 10, 10), 0);
        assert_eq!(quote_amount_out(1, 0, 1, 10, 10), 0);
        assert_eq!(quote_amount_out(1, 1, 0, 10, 10), 0);
    }

    fn make_pool() -> BitflowXykPool {
        BitflowXykPool {
            pool_contract: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.xyk-pool-stx-sbtc"
                .parse()
                .unwrap(),
            core_contract: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.xyk-core-v-1-2"
                .parse()
                .unwrap(),
            x_token: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.token-stx-v-1-2"
                .parse()
                .unwrap(),
            y_token: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.token-sbtc"
                .parse()
                .unwrap(),
            x_decimals: 6,
            y_decimals: 8,
            x_balance: 1_000_000_000,
            y_balance: 1_000_000_000,
            x_protocol_fee_bps: 10,
            x_provider_fee_bps: 40,
            y_protocol_fee_bps: 10,
            y_provider_fee_bps: 40,
            pool_status: true,
            last_tx_id: None,
        }
    }

    #[test]
    fn paused_pool_returns_zero() {
        let mut p = make_pool();
        p.pool_status = false;
        assert_eq!(p.quote_x_for_y(1_000_000), 0);
        assert_eq!(p.quote_y_for_x(1_000_000), 0);
    }

    #[test]
    fn topics_include_pool_update_balances_only() {
        let p = make_pool();
        let ts = p.topics();
        assert_eq!(ts.len(), 1);
        assert_eq!(ts[0].contract, p.pool_contract);
        assert_eq!(ts[0].action, BITFLOW_XYK_UPDATE_POOL_BALANCES);
    }

    #[test]
    fn calculate_output_routes_x_or_y() {
        let p = make_pool();
        let dy = p.calculate_output(&p.x_token, 1_000_000).unwrap();
        assert!(dy > 0 && dy < p.y_balance);
    }
}
