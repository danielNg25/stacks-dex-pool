//! DLMM `apply_event` correctness — the critical correctness gate.
//!
//! This test exercises the cross-pool filter (the bug fix from the POC) by
//! constructing events for OTHER pools and confirming they do NOT mutate
//! our pool's state. Mirrors the manual investigation in the Python verifier.

use std::collections::BTreeMap;

use stacks_dex_pools::codec::clarity::ClarityValue;
use stacks_dex_pools::dlmm::events::apply_event;
use stacks_dex_pools::dlmm::{DLMMPool, PRICE_SCALE_BPS};
use stacks_dex_pools::pool::event::StacksEvent;
use stacks_dex_pools::pool::principal::Principal;

fn make_pool(name: &str) -> DLMMPool {
    let pool_contract: Principal = format!("SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD.{}", name)
        .parse()
        .unwrap();
    let core_contract: Principal = "SP1PFR4V08H1RAZXREBGFFQ59WB739XM8VVGTFSEA.dlmm-core-v-1-1"
        .parse()
        .unwrap();
    let stx: Principal = "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.token-stx-v-1-2"
        .parse()
        .unwrap();
    let usdcx: Principal = "SP466FNC0P7JWTNM2R9T199QRZN1MYEDTAR0KP27.usdcx"
        .parse()
        .unwrap();
    DLMMPool {
        pool_contract,
        core_contract,
        x_token: stx,
        y_token: usdcx,
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
        bins: BTreeMap::new(),
        last_tx_id: None,
        last_event_at: None,
        factors: vec![PRICE_SCALE_BPS; 1001],
    }
}

fn pool_principal(name: &str) -> Principal {
    format!("SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD.{}", name)
        .parse()
        .unwrap()
}

fn core_event(action: &str, data: BTreeMap<String, ClarityValue>) -> StacksEvent {
    StacksEvent {
        emitter: "SP1PFR4V08H1RAZXREBGFFQ59WB739XM8VVGTFSEA.dlmm-core-v-1-1"
            .parse()
            .unwrap(),
        tx_id: "0xabcdef".to_string(),
        event_index: 42,
        action: action.to_string(),
        data,
    }
}

fn pool_event(action: &str, data: BTreeMap<String, ClarityValue>, pool_name: &str) -> StacksEvent {
    StacksEvent {
        emitter: pool_principal(pool_name),
        tx_id: "0xabcdef".to_string(),
        event_index: 42,
        action: action.to_string(),
        data,
    }
}

#[test]
fn swap_event_for_other_pool_is_filtered_out() {
    let mut pool = make_pool("dlmm-pool-stx-usdcx-v-1-bps-10");
    let original_active = pool.active_bin_id;

    // Build a swap event whose pool-contract is OTHER pool — bps-15 STX/sBTC.
    // Without the cross-pool filter, this would set our active bin to 200.
    let mut data = BTreeMap::new();
    data.insert(
        "pool-contract".to_string(),
        ClarityValue::Principal(pool_principal("dlmm-pool-stx-sbtc-v-1-bps-15")),
    );
    data.insert("updated-active-bin-id".to_string(), ClarityValue::Int(200));
    let ev = core_event("swap-x-for-y", data);

    apply_event(&mut pool, &ev).unwrap();
    assert_eq!(
        pool.active_bin_id, original_active,
        "cross-pool filter failed: foreign swap moved our active bin"
    );
}

#[test]
fn swap_event_for_our_pool_does_apply() {
    let mut pool = make_pool("dlmm-pool-stx-usdcx-v-1-bps-10");

    let mut data = BTreeMap::new();
    data.insert(
        "pool-contract".to_string(),
        ClarityValue::Principal(pool.pool_contract.clone()),
    );
    data.insert("updated-active-bin-id".to_string(), ClarityValue::Int(-37));
    let ev = core_event("swap-x-for-y", data);

    apply_event(&mut pool, &ev).unwrap();
    assert_eq!(pool.active_bin_id, -37, "our swap should have moved active");
}

#[test]
fn update_bin_balances_pool_event_passes_through() {
    let mut pool = make_pool("dlmm-pool-stx-usdcx-v-1-bps-10");
    let pool_name = "dlmm-pool-stx-usdcx-v-1-bps-10";

    // Pool-emitted events have NO pool-contract field. They're scoped by the
    // event stream URL. Our cross-pool filter must let them through.
    let mut data = BTreeMap::new();
    data.insert("bin-id".to_string(), ClarityValue::Uint(460)); // unsigned 460 = signed -40
    data.insert("x-balance".to_string(), ClarityValue::Uint(1_000_000));
    data.insert("y-balance".to_string(), ClarityValue::Uint(2_000_000));
    let ev = pool_event("update-bin-balances", data, pool_name);

    apply_event(&mut pool, &ev).unwrap();
    let bin = pool.bins.get(&-40).expect("bin -40 should be inserted");
    assert_eq!(bin.x, 1_000_000);
    assert_eq!(bin.y, 2_000_000);
}

#[test]
fn unsigned_bin_id_converts_to_signed() {
    let mut pool = make_pool("dlmm-pool-stx-usdcx-v-1-bps-10");
    // unsigned 500 = signed 0 (center)
    let mut data = BTreeMap::new();
    data.insert("bin-id".to_string(), ClarityValue::Uint(500));
    data.insert("x-balance".to_string(), ClarityValue::Uint(100));
    data.insert("y-balance".to_string(), ClarityValue::Uint(200));
    apply_event(
        &mut pool,
        &pool_event(
            "update-bin-balances",
            data,
            "dlmm-pool-stx-usdcx-v-1-bps-10",
        ),
    )
    .unwrap();
    assert!(pool.bins.contains_key(&0));
}

#[test]
fn set_variable_fees_for_other_pool_filtered() {
    let mut pool = make_pool("dlmm-pool-stx-usdcx-v-1-bps-10");
    let original_var = pool.x_variable_fee;

    let mut data = BTreeMap::new();
    data.insert(
        "pool-contract".to_string(),
        ClarityValue::Principal(pool_principal("dlmm-pool-stx-sbtc-v-1-bps-15")),
    );
    data.insert("x-fee".to_string(), ClarityValue::Uint(50));
    data.insert("y-fee".to_string(), ClarityValue::Uint(50));
    apply_event(&mut pool, &core_event("set-variable-fees", data)).unwrap();
    assert_eq!(pool.x_variable_fee, original_var);
}

#[test]
fn reset_variable_fees_for_our_pool() {
    let mut pool = make_pool("dlmm-pool-stx-usdcx-v-1-bps-10");
    pool.x_variable_fee = 100;
    pool.y_variable_fee = 100;

    let mut data = BTreeMap::new();
    data.insert(
        "pool-contract".to_string(),
        ClarityValue::Principal(pool.pool_contract.clone()),
    );
    apply_event(&mut pool, &core_event("reset-variable-fees", data)).unwrap();
    assert_eq!(pool.x_variable_fee, 0);
    assert_eq!(pool.y_variable_fee, 0);
}

#[test]
fn unknown_action_is_silently_dropped() {
    let mut pool = make_pool("dlmm-pool-stx-usdcx-v-1-bps-10");
    let snapshot_active = pool.active_bin_id;
    let snapshot_var = pool.x_variable_fee;

    // pool-mint is in KNOWN_INFORMATIONAL — we drop it without error.
    let mut data = BTreeMap::new();
    data.insert("amount".to_string(), ClarityValue::Uint(12345));
    apply_event(
        &mut pool,
        &pool_event("pool-mint", data, "dlmm-pool-stx-usdcx-v-1-bps-10"),
    )
    .unwrap();
    assert_eq!(pool.active_bin_id, snapshot_active);
    assert_eq!(pool.x_variable_fee, snapshot_var);
}

/// Successful apply stamps `last_tx_id` AND `last_event_at`. Regression
/// guard for the bug where DLMM's `apply_event` forgot to write either —
/// V2/stableswap handlers did, DLMM didn't, so every DLMM pool reported
/// `last_tx_id = None` forever and the FE had nothing to render.
#[test]
fn apply_event_stamps_freshness_on_apply() {
    let mut pool = make_pool("dlmm-pool-stx-usdcx-v-1-bps-10");
    assert!(pool.last_tx_id.is_none());
    assert!(pool.last_event_at.is_none());
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let mut data = BTreeMap::new();
    data.insert("bin-id".to_string(), ClarityValue::Uint(500));
    data.insert("x-balance".to_string(), ClarityValue::Uint(1));
    data.insert("y-balance".to_string(), ClarityValue::Uint(2));
    let mut ev = pool_event(
        "update-bin-balances",
        data,
        "dlmm-pool-stx-usdcx-v-1-bps-10",
    );
    ev.tx_id = "0xdeadbeef".to_string();

    apply_event(&mut pool, &ev).unwrap();
    assert_eq!(pool.last_tx_id.as_deref(), Some("0xdeadbeef"));
    let stamped = pool.last_event_at.expect("last_event_at must be Some");
    // Within a 10s window of the wall clock — generous to avoid flakes on
    // slow CI machines.
    assert!(
        stamped >= before && stamped <= before + 10,
        "stamped {} not within [{}, {}+10]",
        stamped,
        before,
        before
    );
}

/// Cross-pool-filtered events MUST NOT advance the freshness watermark —
/// they aren't for us, so they say nothing about how fresh our mirror is.
#[test]
fn cross_pool_filtered_event_does_not_stamp_freshness() {
    let mut pool = make_pool("dlmm-pool-stx-usdcx-v-1-bps-10");

    let mut data = BTreeMap::new();
    data.insert(
        "pool-contract".to_string(),
        ClarityValue::Principal(pool_principal("dlmm-pool-stx-sbtc-v-1-bps-15")),
    );
    data.insert("updated-active-bin-id".to_string(), ClarityValue::Int(200));
    apply_event(&mut pool, &core_event("swap-x-for-y", data)).unwrap();
    assert!(pool.last_tx_id.is_none());
    assert!(pool.last_event_at.is_none());
}

/// Unknown / informational actions don't stamp — we haven't actually
/// applied state, so the mirror's freshness is unchanged.
#[test]
fn unknown_action_does_not_stamp_freshness() {
    let mut pool = make_pool("dlmm-pool-stx-usdcx-v-1-bps-10");
    let mut data = BTreeMap::new();
    data.insert("amount".to_string(), ClarityValue::Uint(12345));
    apply_event(
        &mut pool,
        &pool_event("pool-mint", data, "dlmm-pool-stx-usdcx-v-1-bps-10"),
    )
    .unwrap();
    assert!(pool.last_tx_id.is_none());
    assert!(pool.last_event_at.is_none());
}

#[test]
fn pool_event_without_pool_contract_field_applies() {
    // Sanity check: an event with NO pool-contract field is implicitly
    // scoped (it came from the pool's own event stream URL). The filter
    // must not drop it.
    let mut pool = make_pool("dlmm-pool-stx-usdcx-v-1-bps-10");

    let mut data = BTreeMap::new();
    data.insert("bin-id".to_string(), ClarityValue::Uint(500));
    data.insert("x-balance".to_string(), ClarityValue::Uint(7));
    data.insert("y-balance".to_string(), ClarityValue::Uint(11));
    apply_event(
        &mut pool,
        &pool_event(
            "update-bin-balances",
            data,
            "dlmm-pool-stx-usdcx-v-1-bps-10",
        ),
    )
    .unwrap();
    let bin = pool.bins.get(&0).unwrap();
    assert_eq!(bin.x, 7);
    assert_eq!(bin.y, 11);
}
