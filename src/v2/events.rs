//! V2-family event-application logic.
//!
//! One `apply_<variant>_event` per pool variant. Each mirrors its DLMM peer
//! at [`crate::dlmm::events`]:
//!   1. Drop events whose action isn't in the variant's `INDEXED_ACTIONS`.
//!   2. Apply the cross-pool/cross-pair filter — three of the four V2 variants
//!      are singleton contracts so this is required:
//!        - Arkadiko: `data.swap-token` (LP token principal)
//!        - Velar:    `data.pool.lp-token` (LP token principal)
//!        - ALEX:     `data.pool-id` (uint)
//!        - BitflowXyk: not needed (per-pool contract; events are implicitly scoped)
//!   3. Apply per-action mutation (reserves, fees, status).
//!
//! All four handlers were verified against live Hiro events (probe 2026-05);
//! see each function's doc-comment for the observed shape.

use crate::pool::event::StacksEvent;

use super::alex::AlexPool;
use super::arkadiko::ArkadikoPool;
use super::bitflow_xyk::BitflowXykPool;
use super::velar::VelarPool;

/// Arkadiko: probed live 2026-05. The singleton `arkadiko-swap-v2-1` fires
/// two action strings, one per direction. Both carry the post-swap reserves
/// in `data.balance-x` / `data.balance-y` plus `data.swap-token` (the LP
/// token of the affected pair) — we filter by `swap-token` so an event from
/// pair A doesn't corrupt pair B.
pub const ARKADIKO_INDEXED_ACTIONS: &[&str] = &[
    crate::v2::arkadiko::ARKADIKO_SWAP_X_FOR_Y,
    crate::v2::arkadiko::ARKADIKO_SWAP_Y_FOR_X,
];

/// Apply a swap event to an [`ArkadikoPool`]. Returns `true` if state was
/// changed. Drops events that don't match an indexed action, that belong to
/// a different pair (`swap-token` discriminator), or whose payload is
/// missing the required reserve fields.
pub fn apply_arkadiko_event(pool: &mut ArkadikoPool, event: &StacksEvent) -> bool {
    if !ARKADIKO_INDEXED_ACTIONS.contains(&event.action.as_str()) {
        return false;
    }
    // Cross-pair filter — the singleton emits for every pair. Drop foreign.
    match event.data_principal("swap-token") {
        Some(t) if t == &pool.swap_token => {}
        Some(_) => return false,
        None => {
            // No discriminator on the event — can't safely apply.
            return false;
        }
    }
    let Some(bx) = event.data_uint("balance-x") else {
        return false;
    };
    let Some(by) = event.data_uint("balance-y") else {
        return false;
    };

    pool.balance_x = bx;
    pool.balance_y = by;
    // The payload also carries `enabled` — keep it in sync.
    if let Some(crate::codec::clarity::ClarityValue::Bool(b)) = event.data.get("enabled") {
        pool.enabled = *b;
    }
    pool.last_tx_id = Some(event.tx_id.clone());
    log::debug!(
        "apply [arkadiko {}] {} → x={} y={} tx={}…",
        short_contract(&pool.swap_token.to_string()),
        event.action,
        bx,
        by,
        event.tx_id.chars().take(12).collect::<String>(),
    );
    true
}

/// Velar: probed live 2026-05. The univ2-core uses `op` (not `action`) for
/// its discriminator — our decoder maps that back to `event.action` so we
/// can still match on `"swap"`. The payload is FLAT at the top level (not
/// nested under `data`), and the post-swap reserves live inside a nested
/// `pool` tuple as `reserve0` / `reserve1`. Cross-pool discriminator is
/// `pool.lp-token`.
pub const VELAR_INDEXED_ACTIONS: &[&str] = &[crate::v2::velar::VELAR_SWAP_ACTION];

pub fn apply_velar_event(pool: &mut VelarPool, event: &StacksEvent) -> bool {
    use crate::codec::clarity::ClarityValue;

    if !VELAR_INDEXED_ACTIONS.contains(&event.action.as_str()) {
        return false;
    }
    // The `pool` tuple is what carries lp-token + reserves.
    let Some(ClarityValue::Tuple(inner)) = event.data.get("pool") else {
        return false;
    };
    // Cross-pool filter — drop events from other Velar pools.
    let Some(ClarityValue::Principal(lp)) = inner.get("lp-token") else {
        return false;
    };
    if lp != &pool.lp_token {
        return false;
    }
    let Some(ClarityValue::Uint(r0)) = inner.get("reserve0") else {
        return false;
    };
    let Some(ClarityValue::Uint(r1)) = inner.get("reserve1") else {
        return false;
    };
    let (rx, ry) = if pool.flipped { (*r1, *r0) } else { (*r0, *r1) };
    pool.reserve_x = rx;
    pool.reserve_y = ry;
    pool.last_tx_id = Some(event.tx_id.clone());
    log::debug!(
        "apply [velar {}] swap → rx={} ry={} (flipped={}) tx={}…",
        short_contract(&pool.lp_token.to_string()),
        rx,
        ry,
        pool.flipped,
        event.tx_id.chars().take(12).collect::<String>(),
    );
    true
}

/// ALEX: probed live 2026-05. `amm-pool-v2-01` is singleton; `pool-id`
/// (uint) is the cross-pool discriminator. Post-swap reserves arrive as
/// `data.balance-x` / `data.balance-y`. Top-level `dx`/`dy`/`fee` are
/// informational; we don't mirror them.
pub const ALEX_INDEXED_ACTIONS: &[&str] = &[
    crate::v2::alex::ALEX_SWAP_X_FOR_Y,
    crate::v2::alex::ALEX_SWAP_Y_FOR_X,
];

pub fn apply_alex_event(pool: &mut AlexPool, event: &StacksEvent) -> bool {
    if !ALEX_INDEXED_ACTIONS.contains(&event.action.as_str()) {
        return false;
    }
    // Cross-pool filter — drop foreign pool-ids.
    match event.data_uint("pool-id") {
        Some(id) if id == pool.pool_id => {}
        _ => return false,
    }
    let Some(bx) = event.data_uint("balance-x") else {
        return false;
    };
    let Some(by) = event.data_uint("balance-y") else {
        return false;
    };
    pool.balance_x = bx;
    pool.balance_y = by;
    pool.last_tx_id = Some(event.tx_id.clone());
    log::debug!(
        "apply [alex pool-id={}] {} → x={} y={} tx={}…",
        pool.pool_id,
        event.action,
        bx,
        by,
        event.tx_id.chars().take(12).collect::<String>(),
    );
    true
}

/// Bitflow XYK: probed live 2026-05. The pool itself emits
/// `update-pool-balances` with `data.x-balance` / `data.y-balance` — the new
/// post-swap reserves. Pool-emitted means no cross-pool filter is needed
/// here; the dispatcher already routes by emitter principal.
pub const BITFLOW_XYK_INDEXED_ACTIONS: &[&str] =
    &[crate::v2::bitflow_xyk::BITFLOW_XYK_UPDATE_POOL_BALANCES];

pub fn apply_bitflow_xyk_event(pool: &mut BitflowXykPool, event: &StacksEvent) -> bool {
    if !BITFLOW_XYK_INDEXED_ACTIONS.contains(&event.action.as_str()) {
        return false;
    }
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
        "apply [bitflow-xyk {}] {} → x={} y={} tx={}…",
        short_contract(&pool.pool_contract.to_string()),
        event.action,
        bx,
        by,
        event.tx_id.chars().take(12).collect::<String>(),
    );
    true
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
    use std::collections::BTreeMap;

    const SWAP_TOKEN_STR: &str =
        "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.arkadiko-swap-token-wstx-usda";
    const FOREIGN_SWAP_TOKEN_STR: &str =
        "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.arkadiko-swap-token-xbtc-usda";

    fn make_pool() -> ArkadikoPool {
        ArkadikoPool {
            pool_contract: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.arkadiko-swap-v2-1"
                .parse()
                .unwrap(),
            swap_token: SWAP_TOKEN_STR.parse().unwrap(),
            x_token: "SP3K8BC0PPEVCV7NZ6QSRWPQ2JE9E5B6N3PA0KBR9.token-stx-v-2"
                .parse()
                .unwrap(),
            y_token: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.usda-token"
                .parse()
                .unwrap(),
            x_decimals: 6,
            y_decimals: 6,
            balance_x: 1_000_000,
            balance_y: 1_000_000,
            enabled: true,
            last_tx_id: None,
        }
    }

    fn swap_event(
        action: &str,
        bx: Option<u128>,
        by: Option<u128>,
        swap_token: Option<&str>,
        enabled: Option<bool>,
    ) -> StacksEvent {
        let emitter: Principal = "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.arkadiko-swap-v2-1"
            .parse()
            .unwrap();
        let mut data = BTreeMap::new();
        if let Some(v) = bx {
            data.insert("balance-x".to_string(), ClarityValue::Uint(v));
        }
        if let Some(v) = by {
            data.insert("balance-y".to_string(), ClarityValue::Uint(v));
        }
        if let Some(t) = swap_token {
            data.insert(
                "swap-token".to_string(),
                ClarityValue::Principal(t.parse().unwrap()),
            );
        }
        if let Some(b) = enabled {
            data.insert("enabled".to_string(), ClarityValue::Bool(b));
        }
        StacksEvent {
            emitter,
            tx_id: "0xabcd".to_string(),
            event_index: 0,
            action: action.to_string(),
            data,
        }
    }

    #[test]
    fn swap_x_for_y_updates_reserves() {
        let mut p = make_pool();
        let e = swap_event(
            "swap-x-for-y",
            Some(2_500_000),
            Some(800_000),
            Some(SWAP_TOKEN_STR),
            Some(true),
        );
        assert!(apply_arkadiko_event(&mut p, &e));
        assert_eq!(p.balance_x, 2_500_000);
        assert_eq!(p.balance_y, 800_000);
        assert_eq!(p.last_tx_id.as_deref(), Some("0xabcd"));
    }

    /// Cross-pair filter: an event from a different pair (different
    /// `swap-token`) must not mutate this pair's state.
    #[test]
    fn swap_for_other_pair_is_filtered() {
        let mut p = make_pool();
        let e = swap_event(
            "swap-x-for-y",
            Some(2_500_000),
            Some(800_000),
            Some(FOREIGN_SWAP_TOKEN_STR),
            Some(true),
        );
        assert!(!apply_arkadiko_event(&mut p, &e));
        assert_eq!(p.balance_x, 1_000_000);
        assert_eq!(p.balance_y, 1_000_000);
    }

    /// Events that don't carry a `swap-token` discriminator are dropped —
    /// we never apply un-attributable singleton-contract events.
    #[test]
    fn missing_swap_token_is_dropped() {
        let mut p = make_pool();
        let e = swap_event(
            "swap-x-for-y",
            Some(2_500_000),
            Some(800_000),
            None,
            Some(true),
        );
        assert!(!apply_arkadiko_event(&mut p, &e));
        assert_eq!(p.balance_x, 1_000_000);
    }

    #[test]
    fn unknown_action_is_dropped() {
        let mut p = make_pool();
        let e = swap_event(
            "totally-unknown-action",
            Some(2),
            Some(2),
            Some(SWAP_TOKEN_STR),
            Some(true),
        );
        assert!(!apply_arkadiko_event(&mut p, &e));
        assert_eq!(p.balance_x, 1_000_000);
    }

    #[test]
    fn missing_reserve_fields_drop_event() {
        let mut p = make_pool();
        let e = swap_event(
            "swap-x-for-y",
            None,
            Some(2),
            Some(SWAP_TOKEN_STR),
            Some(true),
        );
        assert!(!apply_arkadiko_event(&mut p, &e));
    }

    /// `enabled = false` in the event flips pool state — quotes drop to 0.
    #[test]
    fn enabled_false_disables_pool() {
        let mut p = make_pool();
        let e = swap_event(
            "swap-y-for-x",
            Some(p.balance_x),
            Some(p.balance_y),
            Some(SWAP_TOKEN_STR),
            Some(false),
        );
        assert!(apply_arkadiko_event(&mut p, &e));
        assert!(!p.enabled);
    }

    // ---------- Velar ----------

    const VELAR_LP_STR: &str = "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.leo-aeusdc";
    const VELAR_LP_OTHER_STR: &str = "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.velar-stx";

    fn make_velar_pool(flipped: bool) -> VelarPool {
        VelarPool {
            pool_contract: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.univ2-core"
                .parse()
                .unwrap(),
            core_contract: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.univ2-core"
                .parse()
                .unwrap(),
            lp_token: VELAR_LP_STR.parse().unwrap(),
            x_token: "SP3K8BC0PPEVCV7NZ6QSRWPQ2JE9E5B6N3PA0KBR9.token-stx-v-2"
                .parse()
                .unwrap(),
            y_token: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.token-aeusdc"
                .parse()
                .unwrap(),
            x_decimals: 6,
            y_decimals: 6,
            reserve_x: 1_000_000,
            reserve_y: 2_000_000,
            flipped,
            fee_num: 997,
            fee_den: 1000,
            last_tx_id: None,
        }
    }

    /// Build a Velar-shaped event — flat top-level fields including a
    /// nested `pool` tuple, decoded the way our `decode_print_payload` with
    /// the `op` fallback would produce it.
    fn velar_event(action: &str, lp: Option<&str>, r0: u128, r1: u128) -> StacksEvent {
        let emitter: Principal = "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.univ2-core"
            .parse()
            .unwrap();
        let mut inner = BTreeMap::new();
        if let Some(lp) = lp {
            inner.insert(
                "lp-token".to_string(),
                ClarityValue::Principal(lp.parse().unwrap()),
            );
        }
        inner.insert("reserve0".to_string(), ClarityValue::Uint(r0));
        inner.insert("reserve1".to_string(), ClarityValue::Uint(r1));
        let mut data = BTreeMap::new();
        data.insert("pool".to_string(), ClarityValue::Tuple(inner));
        // `op` is the top-level discriminator in real events, but by the
        // time we reach apply_event the decoder has copied it to
        // `event.action` (and the entire top-level into `event.data`).
        StacksEvent {
            emitter,
            tx_id: "0xfeed".to_string(),
            event_index: 0,
            action: action.to_string(),
            data,
        }
    }

    #[test]
    fn velar_swap_unflipped_writes_reserve0_into_x() {
        let mut p = make_velar_pool(false);
        let e = velar_event("swap", Some(VELAR_LP_STR), 9_999_999, 4_242_424);
        assert!(apply_velar_event(&mut p, &e));
        assert_eq!(p.reserve_x, 9_999_999);
        assert_eq!(p.reserve_y, 4_242_424);
    }

    #[test]
    fn velar_swap_flipped_swaps_reserve_mapping() {
        let mut p = make_velar_pool(true);
        let e = velar_event("swap", Some(VELAR_LP_STR), 9_999_999, 4_242_424);
        assert!(apply_velar_event(&mut p, &e));
        assert_eq!(p.reserve_x, 4_242_424);
        assert_eq!(p.reserve_y, 9_999_999);
    }

    #[test]
    fn velar_swap_for_other_pool_is_filtered() {
        let mut p = make_velar_pool(false);
        let e = velar_event("swap", Some(VELAR_LP_OTHER_STR), 9, 9);
        assert!(!apply_velar_event(&mut p, &e));
        assert_eq!(p.reserve_x, 1_000_000);
    }

    #[test]
    fn velar_swap_without_lp_token_is_dropped() {
        let mut p = make_velar_pool(false);
        let e = velar_event("swap", None, 9, 9);
        assert!(!apply_velar_event(&mut p, &e));
    }

    #[test]
    fn velar_non_swap_action_is_dropped() {
        let mut p = make_velar_pool(false);
        let e = velar_event("mint", Some(VELAR_LP_STR), 9, 9);
        assert!(!apply_velar_event(&mut p, &e));
    }

    // ---------- ALEX ----------

    fn make_alex_pool(pool_id: u128) -> AlexPool {
        AlexPool {
            pool_contract: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.amm-pool-v2-01"
                .parse()
                .unwrap(),
            pool_id,
            x_token: "SP3K8BC0PPEVCV7NZ6QSRWPQ2JE9E5B6N3PA0KBR9.token-stx-v-2"
                .parse()
                .unwrap(),
            y_token: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.token-alex"
                .parse()
                .unwrap(),
            x_decimals: 8,
            y_decimals: 8,
            balance_x: 1_000_000_000,
            balance_y: 1_000_000_000,
            factor: 100_000_000,
            fee_rate_x: 500_000,
            fee_rate_y: 500_000,
            last_tx_id: None,
        }
    }

    fn alex_event(
        action: &str,
        pool_id: Option<u128>,
        bx: Option<u128>,
        by: Option<u128>,
    ) -> StacksEvent {
        let emitter: Principal = "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.amm-pool-v2-01"
            .parse()
            .unwrap();
        let mut data = BTreeMap::new();
        if let Some(id) = pool_id {
            data.insert("pool-id".to_string(), ClarityValue::Uint(id));
        }
        if let Some(v) = bx {
            data.insert("balance-x".to_string(), ClarityValue::Uint(v));
        }
        if let Some(v) = by {
            data.insert("balance-y".to_string(), ClarityValue::Uint(v));
        }
        StacksEvent {
            emitter,
            tx_id: "0xbeef".to_string(),
            event_index: 0,
            action: action.to_string(),
            data,
        }
    }

    #[test]
    fn alex_swap_for_matching_pool_id_applies() {
        let mut p = make_alex_pool(13);
        let e = alex_event("swap-x-for-y", Some(13), Some(2_500_000), Some(800_000));
        assert!(apply_alex_event(&mut p, &e));
        assert_eq!(p.balance_x, 2_500_000);
        assert_eq!(p.balance_y, 800_000);
    }

    #[test]
    fn alex_swap_for_other_pool_id_is_filtered() {
        let mut p = make_alex_pool(13);
        let e = alex_event("swap-x-for-y", Some(99), Some(9), Some(9));
        assert!(!apply_alex_event(&mut p, &e));
        assert_eq!(p.balance_x, 1_000_000_000);
    }

    #[test]
    fn alex_missing_pool_id_is_dropped() {
        let mut p = make_alex_pool(13);
        let e = alex_event("swap-y-for-x", None, Some(9), Some(9));
        assert!(!apply_alex_event(&mut p, &e));
    }

    #[test]
    fn alex_non_swap_action_is_dropped() {
        let mut p = make_alex_pool(13);
        let e = alex_event("create-pool", Some(13), Some(9), Some(9));
        assert!(!apply_alex_event(&mut p, &e));
    }

    // ---------- Bitflow XYK ----------

    fn make_bitflow_xyk_pool() -> BitflowXykPool {
        BitflowXykPool {
            pool_contract: "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.xyk-pool-sbtc-stx-v-1-1"
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

    fn bitflow_xyk_event(action: &str, bx: Option<u128>, by: Option<u128>) -> StacksEvent {
        let emitter: Principal =
            "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.xyk-pool-sbtc-stx-v-1-1"
                .parse()
                .unwrap();
        let mut data = BTreeMap::new();
        if let Some(v) = bx {
            data.insert("x-balance".to_string(), ClarityValue::Uint(v));
        }
        if let Some(v) = by {
            data.insert("y-balance".to_string(), ClarityValue::Uint(v));
        }
        StacksEvent {
            emitter,
            tx_id: "0xcafe".to_string(),
            event_index: 0,
            action: action.to_string(),
            data,
        }
    }

    #[test]
    fn bitflow_xyk_update_pool_balances_applies() {
        let mut p = make_bitflow_xyk_pool();
        let e = bitflow_xyk_event(
            "update-pool-balances",
            Some(473_334_833),
            Some(1_448_324_236_238),
        );
        assert!(apply_bitflow_xyk_event(&mut p, &e));
        assert_eq!(p.x_balance, 473_334_833);
        assert_eq!(p.y_balance, 1_448_324_236_238);
    }

    #[test]
    fn bitflow_xyk_non_indexed_action_is_dropped() {
        let mut p = make_bitflow_xyk_pool();
        for action in &["pool-transfer", "pool-burn", "pool-mint", "swap-x-for-y"] {
            let e = bitflow_xyk_event(action, Some(1), Some(1));
            assert!(
                !apply_bitflow_xyk_event(&mut p, &e),
                "action {} should be dropped",
                action
            );
        }
        assert_eq!(p.x_balance, 1_000_000_000);
    }

    #[test]
    fn bitflow_xyk_missing_balance_field_drops() {
        let mut p = make_bitflow_xyk_pool();
        let e = bitflow_xyk_event("update-pool-balances", None, Some(1));
        assert!(!apply_bitflow_xyk_event(&mut p, &e));
    }
}
