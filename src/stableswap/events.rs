//! Bitflow StableSwap event-application logic.
//!
//! Two handlers, one per generation:
//!
//! - V2 ([`apply_bitflow_v2_stable_event`]) — pool-emitted `update-pool-balances`
//!   carries post-swap `x-balance` / `y-balance` (and `d`, the new invariant
//!   which we don't mirror). Core-emitted `set-midpoint` updates the
//!   midpoint multiplier and is filtered cross-pool by `pool-contract`.
//! - V1 ([`apply_bitflow_v1_stable_event`]) — V1 pools are observably dormant
//!   on live Hiro (no print events in recent windows during the 2026-05
//!   probe). Handler is a no-op; documented for the future.
//!
//! Both probed live 2026-05. V2 pattern matches [`crate::v2::bitflow_xyk`]
//! one-for-one — the pool-emitted update is the authoritative reserve source.

use crate::pool::event::StacksEvent;

use super::bitflow_v1::BitflowStableSwapV1Pool;
use super::bitflow_v2::{
    BitflowStableSwapV2Pool, BITFLOW_V2_STABLE_SET_MIDPOINT, BITFLOW_V2_STABLE_UPDATE_POOL_BALANCES,
};

pub const BITFLOW_V2_STABLE_INDEXED_ACTIONS: &[&str] = &[
    BITFLOW_V2_STABLE_UPDATE_POOL_BALANCES,
    BITFLOW_V2_STABLE_SET_MIDPOINT,
];

/// Apply a Bitflow V2 stableswap event. Returns `true` if state changed.
///
/// Two paths:
/// - `update-pool-balances` (pool-emitted): authoritative new reserves.
/// - `set-midpoint` (core-emitted): midpoint multiplier update. Cross-pool
///   filtered via `data.pool-contract == self.pool_contract`.
///
/// Anything else (`add-liquidity`, `withdraw-proportional-liquidity`, the
/// core's `swap-*` events) is informational and silently dropped.
pub fn apply_bitflow_v2_stable_event(
    pool: &mut BitflowStableSwapV2Pool,
    event: &StacksEvent,
) -> bool {
    match event.action.as_str() {
        BITFLOW_V2_STABLE_UPDATE_POOL_BALANCES => {
            // Pool-emitted: dispatcher already routed by emitter principal,
            // no cross-pool filter needed.
            let Some(bx) = event.data_uint("x-balance") else {
                return false;
            };
            let Some(by) = event.data_uint("y-balance") else {
                return false;
            };
            pool.x_balance = bx;
            pool.y_balance = by;
            pool.last_tx_id = Some(event.tx_id.clone());
            log::debug!(
                "apply [bitflow-v2-stable {}] update-pool-balances → x={} y={} tx={}…",
                short_contract(&pool.pool_contract.to_string()),
                bx,
                by,
                event.tx_id.chars().take(12).collect::<String>(),
            );
            true
        }
        BITFLOW_V2_STABLE_SET_MIDPOINT => {
            // Core-emitted: filter cross-pool via `pool-contract`.
            match event.data_principal("pool-contract") {
                Some(p) if p == &pool.pool_contract => {}
                _ => return false,
            }
            // The set-midpoint event carries the new `(num, den)` pair under
            // `midpoint-numerator` / `midpoint-denominator`. Probed live;
            // matches the field names used in swap events too.
            let Some(num) = event.data_uint("midpoint-numerator") else {
                return false;
            };
            let Some(den) = event.data_uint("midpoint-denominator") else {
                return false;
            };
            if den == 0 {
                // Refuse to apply a divide-by-zero midpoint.
                return false;
            }
            pool.midpoint_num = num;
            pool.midpoint_den = den;
            pool.last_tx_id = Some(event.tx_id.clone());
            log::debug!(
                "apply [bitflow-v2-stable {}] set-midpoint → {}/{} tx={}…",
                short_contract(&pool.pool_contract.to_string()),
                num,
                den,
                event.tx_id.chars().take(12).collect::<String>(),
            );
            true
        }
        _ => false,
    }
}

/// Apply a Bitflow V1 stableswap event. Currently a no-op — V1 pools are
/// observably dormant on mainnet and no event shape has been verified.
/// Re-bootstrap periodically if you need fresh V1 state. The framework is
/// here so a future probe + handler is localised to this function.
pub fn apply_bitflow_v1_stable_event(
    _pool: &mut BitflowStableSwapV1Pool,
    _event: &StacksEvent,
) -> bool {
    false
}

fn short_contract(contract_id: &str) -> &str {
    contract_id
        .split_once('.')
        .map(|x| x.1)
        .unwrap_or(contract_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::clarity::ClarityValue;
    use crate::pool::event::StacksEvent;
    use crate::pool::principal::Principal;
    use crate::stableswap::bitflow_v1::{MathVariant, Sig};
    use std::collections::BTreeMap;

    fn make_v2_pool() -> BitflowStableSwapV2Pool {
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
            x_balance: 1_000_000_000,
            y_balance: 1_000_000_000,
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

    fn evt(action: &str, fields: &[(&str, ClarityValue)]) -> StacksEvent {
        let emitter: Principal =
            "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.stableswap-pool-stx-ststx-v-1-4"
                .parse()
                .unwrap();
        let mut data = BTreeMap::new();
        for (k, v) in fields {
            data.insert((*k).to_string(), v.clone());
        }
        StacksEvent {
            emitter,
            tx_id: "0xstable".to_string(),
            event_index: 0,
            action: action.to_string(),
            data,
        }
    }

    #[test]
    fn v2_update_pool_balances_applies() {
        let mut p = make_v2_pool();
        let e = evt(
            "update-pool-balances",
            &[
                ("x-balance", ClarityValue::Uint(1_139_472_911_462)),
                ("y-balance", ClarityValue::Uint(1_730_388_983_171)),
                ("d", ClarityValue::Uint(2_701_150_674_087)),
            ],
        );
        assert!(apply_bitflow_v2_stable_event(&mut p, &e));
        assert_eq!(p.x_balance, 1_139_472_911_462);
        assert_eq!(p.y_balance, 1_730_388_983_171);
    }

    #[test]
    fn v2_set_midpoint_for_our_pool_applies() {
        let mut p = make_v2_pool();
        let e = evt(
            "set-midpoint",
            &[
                (
                    "pool-contract",
                    ClarityValue::Principal(p.pool_contract.clone()),
                ),
                ("midpoint-numerator", ClarityValue::Uint(1_000_000)),
                ("midpoint-denominator", ClarityValue::Uint(1_172_500)),
            ],
        );
        assert!(apply_bitflow_v2_stable_event(&mut p, &e));
        assert_eq!(p.midpoint_num, 1_000_000);
        assert_eq!(p.midpoint_den, 1_172_500);
    }

    #[test]
    fn v2_set_midpoint_for_other_pool_filtered() {
        let mut p = make_v2_pool();
        let other: Principal =
            "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.stableswap-pool-sbtc-pbtc-v-1-1"
                .parse()
                .unwrap();
        let e = evt(
            "set-midpoint",
            &[
                ("pool-contract", ClarityValue::Principal(other)),
                ("midpoint-numerator", ClarityValue::Uint(1)),
                ("midpoint-denominator", ClarityValue::Uint(1)),
            ],
        );
        assert!(!apply_bitflow_v2_stable_event(&mut p, &e));
        // State unchanged.
        assert_eq!(p.midpoint_num, 1_000_000);
        assert_eq!(p.midpoint_den, 1_172_209);
    }

    #[test]
    fn v2_set_midpoint_zero_denominator_dropped() {
        let mut p = make_v2_pool();
        let e = evt(
            "set-midpoint",
            &[
                (
                    "pool-contract",
                    ClarityValue::Principal(p.pool_contract.clone()),
                ),
                ("midpoint-numerator", ClarityValue::Uint(1)),
                ("midpoint-denominator", ClarityValue::Uint(0)),
            ],
        );
        assert!(!apply_bitflow_v2_stable_event(&mut p, &e));
    }

    #[test]
    fn v2_unknown_action_dropped() {
        let mut p = make_v2_pool();
        let e = evt("add-liquidity", &[("x-balance", ClarityValue::Uint(999))]);
        assert!(!apply_bitflow_v2_stable_event(&mut p, &e));
    }

    #[test]
    fn v2_missing_balance_field_dropped() {
        let mut p = make_v2_pool();
        let e = evt(
            "update-pool-balances",
            &[("x-balance", ClarityValue::Uint(1))],
        );
        // Missing y-balance → drop.
        assert!(!apply_bitflow_v2_stable_event(&mut p, &e));
    }

    #[test]
    fn v1_handler_is_noop() {
        // Construct a minimal V1 pool — the handler returns false regardless.
        let mut p = BitflowStableSwapV1Pool {
            pool_contract: "SPQC38PW542EQJ5M11CR25P7BS1CA6QT4TBXGB3M.stableswap-stx-ststx-v-1-2"
                .parse()
                .unwrap(),
            lp_token: "SPQC38PW542EQJ5M11CR25P7BS1CA6QT4TBXGB3M.stx-ststx-lp-token-v-1-2"
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
            x_balance: 1,
            y_balance: 1,
            amp: 100,
            threshold: 2,
            buy_fee_bps: 5,
            sell_fee_bps: 5,
            sig: Sig::StxAnchored,
            variant: MathVariant::BalBug,
            approval: true,
            last_tx_id: None,
        };
        let e = evt("anything", &[("x-balance", ClarityValue::Uint(2))]);
        assert!(!apply_bitflow_v1_stable_event(&mut p, &e));
    }
}
