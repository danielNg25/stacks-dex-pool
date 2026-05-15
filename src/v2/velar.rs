//! Velar V1 — Uniswap V2 clone with per-pool `(fee_num, fee_den)` tuple.
//!
//! State comes from
//! `SP1Y5YSTAHZ88XYK1VPDH24GY0HPX5J4JECTMY4A1.univ2-core::lookup-pool`
//! which returns `(some (ok {pool: {reserve0, reserve1, swap-fee: {num, den}, …,
//! lp-token: principal}, flipped: bool}))`. The `flipped` flag tells us
//! whether on-chain ordering `(token0, token1)` matches the order we asked
//! about; bootstrap normalises so the resulting pool's `x_token` always
//! corresponds to `reserve_x`, and stores `flipped` so event reserves
//! (which always arrive as raw `reserve0`/`reserve1`) can be mapped through
//! the same lens.
//!
//! Math byte-exact with the on-chain `univ2-library::get-amount-out` helper.
//! Reference: [arbitrage-rs/crates/stacks/src/velar.rs:110-125].
//!
//! ## Events (probed live 2026-05)
//! Velar's `univ2-core` is **singleton** AND uses an unusual print-event
//! shape: the top-level tuple's discriminator field is `op` (not `action`)
//! and the payload is flat at top level rather than nested under `data`.
//! The decoder's `op` fallback handles this; downstream `event.action` will
//! be `"swap"`. The pool tuple — including the post-swap reserves — lives
//! inside `data.pool`.
//!
//! Cross-pool filter: match `data.pool.lp-token` against the bootstrap-
//! latched `lp_token`. Every Velar pool sharing `univ2-core` would otherwise
//! see every other pool's swap events.

use std::any::Any;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use super::math::quote_amount_out_with_fee_ratio;
use crate::pool::base::{EventApplicable, PoolInterface, PoolType, PoolTypeTrait, TopicList};
use crate::pool::event::{StacksEvent, StacksTopic};
use crate::pool::principal::Principal;

pub const CORE_CONTRACT_ID: &str = "SP1Y5YSTAHZ88XYK1VPDH24GY0HPX5J4JECTMY4A1.univ2-core";
pub const VELAR_SWAP_ACTION: &str = "swap";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VelarPool {
    /// LP token principal — Velar's stable per-pair identity. The `univ2-core`
    /// singleton emits for every pool, so `pool_contract` would clash across
    /// pools; we use `lp_token` as the registry key instead. Bootstrap reads
    /// it from `lookup-pool`'s `pool.lp-token` and events carry the same.
    pub pool_contract: Principal,
    /// `univ2-core` — emits swap events for every Velar pool. Cross-pool
    /// filter via `lp_token` is required.
    pub core_contract: Principal,
    /// LP token principal — used as cross-pool discriminator on incoming
    /// events (`data.pool.lp-token == self.lp_token`).
    pub lp_token: Principal,
    pub x_token: Principal,
    pub y_token: Principal,
    pub x_decimals: u8,
    pub y_decimals: u8,
    /// Reserves in pool-x order (after un-flipping if `flipped=true` was
    /// observed at bootstrap).
    pub reserve_x: u128,
    pub reserve_y: u128,
    /// `true` iff on-chain `(token0, token1)` is `(y_token, x_token)`.
    /// Stored so events (which carry raw `reserve0` / `reserve1`) can be
    /// mapped to `(reserve_x, reserve_y)` consistently with bootstrap.
    pub flipped: bool,
    pub fee_num: u128,
    pub fee_den: u128,
    pub last_tx_id: Option<String>,
}

impl VelarPool {
    pub fn quote_x_for_y(&self, dx: u128) -> u128 {
        quote_amount_out_with_fee_ratio(
            dx,
            self.reserve_x,
            self.reserve_y,
            self.fee_num,
            self.fee_den,
        )
    }
    pub fn quote_y_for_x(&self, dy: u128) -> u128 {
        quote_amount_out_with_fee_ratio(
            dy,
            self.reserve_y,
            self.reserve_x,
            self.fee_num,
            self.fee_den,
        )
    }

    /// Effective fee in bps for [`PoolInterface::fee_bps`]. We don't have an
    /// exact bps if the denominator isn't 10_000; we round to the nearest bp.
    pub fn fee_bps_effective(&self) -> u32 {
        if self.fee_den == 0 || self.fee_num >= self.fee_den {
            return 10_000;
        }
        let cut = self.fee_den - self.fee_num;
        // (cut / fee_den) * 10_000 — integer math, rounded.
        let bps_x_2 = cut.saturating_mul(20_000) / self.fee_den;
        // Half-up rounding.
        bps_x_2.div_ceil(2) as u32
    }
}

impl PoolTypeTrait for VelarPool {
    fn pool_type(&self) -> PoolType {
        PoolType::StacksUniswapV2
    }
}

impl EventApplicable for VelarPool {
    fn apply_event(&mut self, event: &StacksEvent) -> Result<()> {
        super::events::apply_velar_event(self, event);
        Ok(())
    }
}

impl TopicList for VelarPool {
    fn topics(&self) -> Vec<StacksTopic> {
        // Velar's only useful event stream is the univ2-core's. The pool's
        // own contract (the LP token) doesn't emit swap-relevant prints.
        vec![StacksTopic::new(
            self.core_contract.clone(),
            VELAR_SWAP_ACTION,
        )]
    }
}

impl PoolInterface for VelarPool {
    fn calculate_output(&self, token_in: &Principal, amount_in: u128) -> Result<u128> {
        if token_in == &self.x_token {
            Ok(self.quote_x_for_y(amount_in))
        } else if token_in == &self.y_token {
            Ok(self.quote_y_for_x(amount_in))
        } else {
            Err(anyhow!(
                "VelarPool: token_in {} matches neither x ({}) nor y ({})",
                token_in,
                self.x_token,
                self.y_token,
            ))
        }
    }
    fn id(&self) -> String {
        // `pool_contract` is the singleton univ2-core; `lp_token` is what's
        // unique per pair, so it's the right registry identifier.
        self.lp_token.to_string()
    }
    fn pool_contract(&self) -> &Principal {
        &self.pool_contract
    }
    fn tokens(&self) -> (&Principal, &Principal) {
        (&self.x_token, &self.y_token)
    }
    fn fee_bps(&self) -> u32 {
        self.fee_bps_effective()
    }
    fn clone_box(&self) -> Box<dyn PoolInterface + Send + Sync> {
        Box::new(self.clone())
    }
    fn log_summary(&self) -> String {
        format!(
            "Velar[{}] x={} y={} fee={}/{} (≈{}bps)",
            self.pool_contract,
            self.reserve_x,
            self.reserve_y,
            self.fee_num,
            self.fee_den,
            self.fee_bps_effective(),
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

    fn make_pool() -> VelarPool {
        // The principals here only need to round-trip through our c32
        // decoder; we reuse valid-checksum strings from other in-tree tests
        // rather than the (apparently mis-checksummed) literals in the
        // arb-stacks fixtures.
        VelarPool {
            pool_contract: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.univ2-core"
                .parse()
                .unwrap(),
            core_contract: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.univ2-core"
                .parse()
                .unwrap(),
            lp_token: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.leo-aeusdc"
                .parse()
                .unwrap(),
            x_token: "SP3K8BC0PPEVCV7NZ6QSRWPQ2JE9E5B6N3PA0KBR9.token-stx-v-2"
                .parse()
                .unwrap(),
            y_token: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.token-aeusdc"
                .parse()
                .unwrap(),
            x_decimals: 6,
            y_decimals: 6,
            reserve_x: 30_558_825_120,
            reserve_y: 8_092_994_292,
            flipped: false,
            fee_num: 997,
            fee_den: 1000,
            last_tx_id: None,
        }
    }

    /// Byte-exact fixture from `arbitrage-rs/crates/stacks/src/velar.rs:363-372`
    /// (STX→aeUSDC, 152.794125 STX → 40.143461 aeUSDC).
    #[test]
    fn stx_aeusdc_fixture_byte_exact() {
        let p = make_pool();
        let dy = p.quote_x_for_y(152_794_125);
        assert_eq!(dy, 40_143_461);
    }

    #[test]
    fn fee_bps_effective_3_for_997_over_1000() {
        let p = make_pool();
        // 0.003 = 30 bps.
        assert_eq!(p.fee_bps_effective(), 30);
    }

    #[test]
    fn topics_include_only_univ2_core() {
        let p = make_pool();
        let ts = p.topics();
        assert_eq!(ts.len(), 1);
        assert_eq!(ts[0].contract, p.core_contract);
        assert_eq!(ts[0].action, VELAR_SWAP_ACTION);
    }
}
