//! Bitflow V1 StableSwap — older generation, self-contained pools (no
//! `core` split), at a different deployer than V2.
//!
//! Two structural variations from V2 to model:
//!
//! 1. **Dual signature.** STX-anchored pools (`stableswap-stx-<x>-v-1-N`)
//!    have a 2-arg ABI (`y-token`, `lp-token`, `dx`); token-pair pools
//!    have a 3-arg ABI (`x-token`, `y-token`, `lp-token`, `dx`). The
//!    [`Sig`] enum picks which one the bootstrap uses.
//!
//! 2. **Dual math variant.** `-v-1-{1,2,3}` pools pass POST-swap `x_bal`
//!    into `get-y`, which internally adds `x_amount` AGAIN — a double-count
//!    bug we must reproduce byte-for-byte. `-v-1-4+` pools fixed this and
//!    pass pre-swap `x_bal`. The [`MathVariant`] enum dispatches.
//!
//! Fees on V1 are also dual:
//! - STX-anchored: `buy-fees` for x→y, `sell-fees` for y→x (each a
//!   3-way `{lps, stacking-dao, bitflow}` tuple). Direction-asymmetric.
//! - Token-pair: one `swap-fees` `{lps, protocol}` tuple, same both ways.
//!
//! Both collapse to a single `fee_total_bps` for [`get_dy`].
//!
//! Pool-disabled flag is `approval = false` (vs V2's `pool-status`).
//!
//! Reference: [arbitrage-rs/crates/stacks/src/bitflow_v1.rs:1-330].
//!
//! ## Events (probed live 2026-05)
//! V1 pools are observably dormant on mainnet — no print events in recent
//! windows. The mirror's [`super::events::apply_bitflow_v1_stable_event`] is
//! a documented no-op as a result. Callers wanting fresh V1 state should
//! periodically re-bootstrap rather than rely on event sync.

use std::any::Any;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use super::curve::{get_y, scale_down, scale_up};
use crate::pool::base::{EventApplicable, PoolInterface, PoolType, PoolTypeTrait, TopicList};
use crate::pool::event::{StacksEvent, StacksTopic};
use crate::pool::principal::Principal;

const BPS: u128 = 10_000;

/// Bitflow V1 mainnet deployer (every V1 stableswap pool lives here).
pub const V1_DEPLOYER: &str = "SPQC38PW542EQJ5M11CR25P7BS1CA6QT4TBXGB3M";

/// V1 pool emits nothing distinct from V2's pool. Listed for parity with
/// the topic-list pattern; the [`super::events`] handler is a no-op.
pub const BITFLOW_V1_STABLE_UPDATE_POOL_BALANCES: &str = "update-pool-balances";

/// `get-dy` signature shape — STX-anchored takes 2 args (y-token, lp-token),
/// token-pair takes 3 (x-token, y-token, lp-token).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Sig {
    /// STX is implicit; bootstrap calls `get-pair-data(y-token, lp-token)`.
    StxAnchored,
    /// Both tokens explicit; bootstrap calls `get-pair-data(x-token, y-token, lp-token)`.
    TokenPair,
}

/// Which `get-y` argument convention the pool's Clarity source uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MathVariant {
    /// `-v-1-{1,2,3}` pools. Pass post-swap `x_bal` to `get-y`; the contract
    /// adds `x_amount` again internally → double-count. Reproduce byte-exact.
    BalBug,
    /// `-v-1-4+` pools. Pass pre-swap `x_bal`; `get-y` adds `x_amount` once
    /// (the correct semantic, identical to V2).
    Fixed,
}

impl MathVariant {
    /// Parse the user-friendly variant name (case-insensitive on the alias set).
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "v1-bal-bug" | "bug" | "buggy" => Ok(MathVariant::BalBug),
            "v1-fixed" | "fixed" => Ok(MathVariant::Fixed),
            other => Err(anyhow!(
                "unknown Bitflow V1 math variant {:?}; expected \"v1-bal-bug\" or \"v1-fixed\"",
                other
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitflowStableSwapV1Pool {
    /// `<V1_DEPLOYER>.stableswap-...-v-1-N`. Self-contained — no core.
    pub pool_contract: Principal,
    /// LP token. Required as the third arg of `get-pair-data` on both ABIs.
    pub lp_token: Principal,
    pub x_token: Principal,
    pub y_token: Principal,
    pub x_decimals: u8,
    pub y_decimals: u8,
    pub x_balance: u128,
    pub y_balance: u128,
    pub amp: u128,
    pub threshold: u128,
    /// Combined x→y fee in bps. STX-anchored = `buy-fees` total; token-pair = `swap-fees` total.
    pub buy_fee_bps: u32,
    /// Combined y→x fee in bps. STX-anchored = `sell-fees` total; token-pair = same as buy.
    pub sell_fee_bps: u32,
    pub sig: Sig,
    pub variant: MathVariant,
    /// `false` = pool deprecated. Quotes return 0.
    pub approval: bool,
    pub last_tx_id: Option<String>,
}

/// Full V1 quote with fee + variant-conditional `get-y` shape.
///
/// **Contract quirk we mirror**: `fee_total = (x_amount * fee_bps) / BPS`
/// uses the UNSCALED `x_amount`, then is subtracted from the scaled `dx_s`.
/// For matched-decimal pools (all current V1 pools) these are identical;
/// for mismatched pools it's a bug we reproduce.
#[allow(clippy::too_many_arguments)]
pub fn get_dy(
    x_amount: u128,
    x_balance: u128,
    y_balance: u128,
    x_decimals: u8,
    y_decimals: u8,
    fee_total_bps: u32,
    amp: u128,
    threshold: u128,
    variant: MathVariant,
) -> u128 {
    if x_amount == 0 || x_balance == 0 || y_balance == 0 {
        return 0;
    }
    let (bx_s, by_s) = scale_up(x_balance, y_balance, x_decimals, y_decimals);
    let (dx_s, _) = scale_up(x_amount, 0, x_decimals, y_decimals);

    // Contract quirk: fee computed off UNSCALED x_amount, then subtracted from dx_s.
    let fee_total = x_amount.saturating_mul(fee_total_bps as u128) / BPS;
    let dx_net_scaled = dx_s.saturating_sub(fee_total);

    // BalBug: caller hands `get_y` a post-swap `x_bal` (= bx_s + dx_net),
    // which the function ADDS dx_net to AGAIN → double-count.
    // Fixed: caller hands `get_y` the pre-swap `x_bal` — correct semantic.
    let x_for_y = match variant {
        MathVariant::BalBug => bx_s.saturating_add(dx_net_scaled),
        MathVariant::Fixed => bx_s,
    };

    let new_y_scaled = get_y(dx_net_scaled, x_for_y, by_s, amp, threshold);
    let (_, new_y) = scale_down(0, new_y_scaled, x_decimals, y_decimals);
    y_balance.saturating_sub(new_y)
}

impl BitflowStableSwapV1Pool {
    pub fn quote_x_for_y(&self, dx: u128) -> u128 {
        if !self.approval {
            return 0;
        }
        get_dy(
            dx,
            self.x_balance,
            self.y_balance,
            self.x_decimals,
            self.y_decimals,
            self.buy_fee_bps,
            self.amp,
            self.threshold,
            self.variant,
        )
    }

    pub fn quote_y_for_x(&self, dy: u128) -> u128 {
        if !self.approval {
            return 0;
        }
        get_dy(
            dy,
            self.y_balance,
            self.x_balance,
            self.y_decimals,
            self.x_decimals,
            self.sell_fee_bps,
            self.amp,
            self.threshold,
            self.variant,
        )
    }
}

impl PoolTypeTrait for BitflowStableSwapV1Pool {
    fn pool_type(&self) -> PoolType {
        PoolType::BitflowStableSwap
    }
}

impl EventApplicable for BitflowStableSwapV1Pool {
    fn apply_event(&mut self, event: &StacksEvent) -> Result<()> {
        super::events::apply_bitflow_v1_stable_event(self, event);
        Ok(())
    }
}

impl TopicList for BitflowStableSwapV1Pool {
    fn topics(&self) -> Vec<StacksTopic> {
        // V1 pools are observably dormant on mainnet (no recent print
        // events). Subscribe to `update-pool-balances` on the pool's own
        // contract anyway, in case a future trade fires it — the
        // [`super::events::apply_bitflow_v1_stable_event`] handler is the
        // no-op extension point.
        vec![StacksTopic::new(
            self.pool_contract.clone(),
            BITFLOW_V1_STABLE_UPDATE_POOL_BALANCES,
        )]
    }
}

impl PoolInterface for BitflowStableSwapV1Pool {
    fn calculate_output(&self, token_in: &Principal, amount_in: u128) -> Result<u128> {
        if token_in == &self.x_token {
            Ok(self.quote_x_for_y(amount_in))
        } else if token_in == &self.y_token {
            Ok(self.quote_y_for_x(amount_in))
        } else {
            Err(anyhow!(
                "BitflowStableSwapV1Pool: token_in {} matches neither x ({}) nor y ({})",
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
        self.buy_fee_bps
    }
    fn clone_box(&self) -> Box<dyn PoolInterface + Send + Sync> {
        Box::new(self.clone())
    }
    fn log_summary(&self) -> String {
        format!(
            "BitflowV1Stable[{}] {:?}/{:?} x={} y={} amp={} buy={}bps sell={}bps approval={}",
            self.pool_contract,
            self.sig,
            self.variant,
            self.x_balance,
            self.y_balance,
            self.amp,
            self.buy_fee_bps,
            self.sell_fee_bps,
            self.approval,
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

    /// Byte-exact fixture from arbitrage-rs/crates/stacks/src/bitflow_v1.rs:589-603
    /// (v1-bal-bug variant — STX/stSTX shape).
    #[test]
    fn get_dy_bal_bug_fixture_byte_exact() {
        let dy = get_dy(
            5_000_000_000,
            1_000_000_000_000,
            1_171_000_000_000,
            6,
            6,
            5,
            100,
            2,
            MathVariant::BalBug,
        );
        assert_eq!(dy, 5_004_907_045);
    }

    /// Byte-exact fixture from arbitrage-rs/crates/stacks/src/bitflow_v1.rs:606-620
    /// (v1-fixed variant — USDA/aeUSDC shape).
    #[test]
    fn get_dy_fixed_fixture_byte_exact() {
        let dy = get_dy(
            50_000_000,
            10_000_000_000,
            9_950_000_000,
            6,
            6,
            6,
            100,
            2,
            MathVariant::Fixed,
        );
        assert_eq!(dy, 49_965_042);
    }

    /// The two math variants MUST give different outputs for the same inputs
    /// — otherwise we'd silently route a buggy pool through the fixed path
    /// (or vice versa) and over- or under-quote forever.
    #[test]
    fn variants_diverge() {
        let bug = get_dy(
            5_000_000_000,
            1_000_000_000_000,
            1_171_000_000_000,
            6,
            6,
            5,
            100,
            2,
            MathVariant::BalBug,
        );
        let fixed = get_dy(
            5_000_000_000,
            1_000_000_000_000,
            1_171_000_000_000,
            6,
            6,
            5,
            100,
            2,
            MathVariant::Fixed,
        );
        assert_ne!(bug, fixed);
        // BalBug gives smaller dy: get-y receives a larger x_balance, which
        // inflates new_y → less to remove from y_balance.
        assert!(bug < fixed);
    }

    #[test]
    fn math_variant_parse() {
        assert_eq!(
            MathVariant::parse("v1-bal-bug").unwrap(),
            MathVariant::BalBug
        );
        assert_eq!(MathVariant::parse("buggy").unwrap(), MathVariant::BalBug);
        assert_eq!(MathVariant::parse("v1-fixed").unwrap(), MathVariant::Fixed);
        assert_eq!(MathVariant::parse("fixed").unwrap(), MathVariant::Fixed);
        assert!(MathVariant::parse("unknown").is_err());
    }

    fn make_v1_fixed_pool() -> BitflowStableSwapV1Pool {
        BitflowStableSwapV1Pool {
            pool_contract: "SPQC38PW542EQJ5M11CR25P7BS1CA6QT4TBXGB3M.stableswap-usda-aeusdc-v-1-4"
                .parse()
                .unwrap(),
            lp_token: "SPQC38PW542EQJ5M11CR25P7BS1CA6QT4TBXGB3M.usda-aeusdc-lp-token-v-1-4"
                .parse()
                .unwrap(),
            x_token: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.usda-token"
                .parse()
                .unwrap(),
            y_token: "SP3Y2ZSH8P7D50B0VBTSX11S7XSG24M1VB9YFQA4K.token-aeusdc"
                .parse()
                .unwrap(),
            x_decimals: 6,
            y_decimals: 6,
            x_balance: 10_000_000_000,
            y_balance: 9_950_000_000,
            amp: 100,
            threshold: 2,
            buy_fee_bps: 6,
            sell_fee_bps: 6,
            sig: Sig::TokenPair,
            variant: MathVariant::Fixed,
            approval: true,
            last_tx_id: None,
        }
    }

    #[test]
    fn quote_x_for_y_uses_fixed_variant() {
        let p = make_v1_fixed_pool();
        let dy = p.quote_x_for_y(50_000_000);
        assert_eq!(dy, 49_965_042);
    }

    #[test]
    fn paused_v1_returns_zero() {
        let mut p = make_v1_fixed_pool();
        p.approval = false;
        assert_eq!(p.quote_x_for_y(50_000_000), 0);
        assert_eq!(p.quote_y_for_x(50_000_000), 0);
    }

    #[test]
    fn topics_include_pool_update_balances() {
        let p = make_v1_fixed_pool();
        let ts = p.topics();
        assert_eq!(ts.len(), 1);
        assert_eq!(ts[0].contract, p.pool_contract);
        assert_eq!(ts[0].action, BITFLOW_V1_STABLE_UPDATE_POOL_BALANCES);
    }
}
