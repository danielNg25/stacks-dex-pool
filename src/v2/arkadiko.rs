//! Arkadiko `arkadiko-swap-v2-1` — Uniswap V2 clone with hardcoded 30 bps LP
//! fee. No on-chain `get-amount-out` helper, so the math is mirrored locally
//! from the contract source (and verified byte-exact against the Python POC
//! at `test/fetch_arkadiko_pools.py`).
//!
//! ## Trade-offs vs the other V2 variants
//! - Fee is a constant of the contract — no per-pool fee field to read or
//!   track via events. `[Self::fee_bps]` always returns 30.
//! - Pool state is fetched via `get-pair-details(token-x, token-y)` returning
//!   `(some {balance-x, balance-y, enabled, ...})`. Either token ordering
//!   may match the on-chain pair; bootstrap probes both and latches the
//!   working one (see [`super::fetcher::fetch_arkadiko_pool`]).
//! - `enabled = false` means the pool has been turned off; quotes return 0
//!   and the mirror refuses to apply incoming trades.
//!
//! ## Events (probed live 2026-05)
//! Arkadiko's `arkadiko-swap-v2-1` is **singleton**: a single contract
//! handles every Arkadiko pair, and all events fire on its principal. The
//! mirror MUST filter cross-pool via the `swap-token` field (the LP token
//! principal of THIS pair) — same shape as the DLMM core fix.
//!
//! - Action: `swap-x-for-y` | `swap-y-for-x`
//! - Filter: `data.swap-token == self.swap_token`
//! - Apply: read post-swap `balance-x`, `balance-y`, and `enabled` from data.

use std::any::Any;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use super::math::quote_amount_out_with_fee_ratio;
use crate::pool::base::{EventApplicable, PoolInterface, PoolType, PoolTypeTrait, TopicList};
use crate::pool::event::{StacksEvent, StacksTopic};
use crate::pool::principal::Principal;

/// Hardcoded 30 bps fee as a `(num, den)` ratio matching the Clarity source's
/// `997 * dx / 1000`. Constants ensure tests stay obviously correct.
pub const ARKADIKO_FEE_NUM: u128 = 997;
pub const ARKADIKO_FEE_DEN: u128 = 1000;
pub const ARKADIKO_FEE_BPS: u32 = 30;

/// Action strings emitted on swaps. Probed live: Arkadiko's
/// `arkadiko-swap-v2-1` fires `swap-x-for-y` or `swap-y-for-x` (per direction).
pub const ARKADIKO_SWAP_X_FOR_Y: &str = "swap-x-for-y";
pub const ARKADIKO_SWAP_Y_FOR_X: &str = "swap-y-for-x";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArkadikoPool {
    /// The Arkadiko swap contract — same principal for every Arkadiko pair
    /// (it's a singleton AMM). Events from this contract serve all pairs;
    /// the mirror filters cross-pair via `swap_token`.
    pub pool_contract: Principal,
    /// LP token principal for this pair — used both as the pool's stable
    /// identifier and as the cross-pool filter on incoming swap events
    /// (`data.swap-token == self.swap_token`).
    pub swap_token: Principal,
    pub x_token: Principal,
    pub y_token: Principal,
    pub x_decimals: u8,
    pub y_decimals: u8,
    pub balance_x: u128,
    pub balance_y: u128,
    /// `false` = pool has been administratively disabled. We return 0 from
    /// quotes and refuse to apply events while disabled.
    pub enabled: bool,
    /// Last applied tx_id (for diagnostics; dedup lives in the queue).
    pub last_tx_id: Option<String>,
}

impl ArkadikoPool {
    /// Quote x → y given a raw `dx` (already scaled to x's smallest unit).
    pub fn quote_x_for_y(&self, dx: u128) -> u128 {
        if !self.enabled {
            return 0;
        }
        quote_amount_out_with_fee_ratio(
            dx,
            self.balance_x,
            self.balance_y,
            ARKADIKO_FEE_NUM,
            ARKADIKO_FEE_DEN,
        )
    }

    /// Quote y → x given a raw `dy`. Constant-product is symmetric, so this
    /// is just `quote` with reserves swapped.
    pub fn quote_y_for_x(&self, dy: u128) -> u128 {
        if !self.enabled {
            return 0;
        }
        quote_amount_out_with_fee_ratio(
            dy,
            self.balance_y,
            self.balance_x,
            ARKADIKO_FEE_NUM,
            ARKADIKO_FEE_DEN,
        )
    }
}

impl PoolTypeTrait for ArkadikoPool {
    fn pool_type(&self) -> PoolType {
        PoolType::StacksUniswapV2
    }
}

impl EventApplicable for ArkadikoPool {
    fn apply_event(&mut self, event: &StacksEvent) -> Result<()> {
        super::events::apply_arkadiko_event(self, event);
        Ok(())
    }
}

impl TopicList for ArkadikoPool {
    fn topics(&self) -> Vec<StacksTopic> {
        // Both directions; events for every Arkadiko pair land on the same
        // singleton contract and `apply_event` filters by `swap_token`.
        vec![
            StacksTopic::new(self.pool_contract.clone(), ARKADIKO_SWAP_X_FOR_Y),
            StacksTopic::new(self.pool_contract.clone(), ARKADIKO_SWAP_Y_FOR_X),
        ]
    }
}

impl PoolInterface for ArkadikoPool {
    fn calculate_output(&self, token_in: &Principal, amount_in: u128) -> Result<u128> {
        if token_in == &self.x_token {
            Ok(self.quote_x_for_y(amount_in))
        } else if token_in == &self.y_token {
            Ok(self.quote_y_for_x(amount_in))
        } else {
            Err(anyhow!(
                "ArkadikoPool: token_in {} matches neither x ({}) nor y ({})",
                token_in,
                self.x_token,
                self.y_token,
            ))
        }
    }
    fn id(&self) -> String {
        // `pool_contract` is the singleton swap-v2-1 address — same for
        // every Arkadiko pair. Use `swap_token` (LP token) as the stable
        // identifier so each pair is distinct in the registry.
        self.swap_token.to_string()
    }
    fn pool_contract(&self) -> &Principal {
        &self.pool_contract
    }
    fn tokens(&self) -> (&Principal, &Principal) {
        (&self.x_token, &self.y_token)
    }
    fn fee_bps(&self) -> u32 {
        ARKADIKO_FEE_BPS
    }
    fn clone_box(&self) -> Box<dyn PoolInterface + Send + Sync> {
        Box::new(self.clone())
    }
    fn log_summary(&self) -> String {
        format!(
            "Arkadiko[{}] x={} y={} enabled={} fee={}bps",
            self.pool_contract, self.balance_x, self.balance_y, self.enabled, ARKADIKO_FEE_BPS,
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

    fn make_pool() -> ArkadikoPool {
        ArkadikoPool {
            pool_contract: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.arkadiko-swap-v2-1"
                .parse()
                .unwrap(),
            // Real wSTX-USDA LP token observed in the probe.
            swap_token: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.arkadiko-swap-token-wstx-usda"
                .parse()
                .unwrap(),
            x_token: "SP3K8BC0PPEVCV7NZ6QSRWPQ2JE9E5B6N3PA0KBR9.token-stx-v-2"
                .parse()
                .unwrap(),
            y_token: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.usda-token"
                .parse()
                .unwrap(),
            x_decimals: 6,
            y_decimals: 6,
            balance_x: 604_895_060_900,
            balance_y: 160_672_499_568,
            enabled: true,
            last_tx_id: None,
        }
    }

    /// Byte-exact arb-stacks fixture.
    #[test]
    fn fixture_quote_matches_arb_stacks() {
        let p = make_pool();
        let dy = p.quote_x_for_y(3_024_475_304);
        assert_eq!(dy, 796_979_467);
    }

    #[test]
    fn calculate_output_routes_x_or_y() {
        let p = make_pool();
        let dy = p.calculate_output(&p.x_token, 3_024_475_304).unwrap();
        assert_eq!(dy, 796_979_467);

        let dx = p.calculate_output(&p.y_token, 100_000_000).unwrap();
        // Sanity only — y→x should be > 0 and < y reserve.
        assert!(dx > 0);
        assert!(dx < p.balance_x);
    }

    #[test]
    fn calculate_output_unknown_token_errors() {
        let p = make_pool();
        let stranger: Principal = "SP000000000000000000002Q6VF78.stranger".parse().unwrap();
        assert!(p.calculate_output(&stranger, 1).is_err());
    }

    #[test]
    fn disabled_pool_returns_zero() {
        let mut p = make_pool();
        p.enabled = false;
        assert_eq!(p.quote_x_for_y(3_024_475_304), 0);
        assert_eq!(p.quote_y_for_x(100_000_000), 0);
    }

    #[test]
    fn topics_include_both_swap_actions_on_singleton_contract() {
        let p = make_pool();
        let ts = p.topics();
        assert_eq!(ts.len(), 2);
        for t in &ts {
            assert_eq!(t.contract, p.pool_contract);
        }
        let actions: Vec<&str> = ts.iter().map(|t| t.action.as_str()).collect();
        assert!(actions.contains(&ARKADIKO_SWAP_X_FOR_Y));
        assert!(actions.contains(&ARKADIKO_SWAP_Y_FOR_X));
    }

    #[test]
    fn fee_bps_is_thirty() {
        assert_eq!(ARKADIKO_FEE_BPS, 30);
        let p = make_pool();
        assert_eq!(p.fee_bps(), 30);
    }
}
