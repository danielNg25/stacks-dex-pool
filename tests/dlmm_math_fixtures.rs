//! DLMM math fixtures — deterministic, hand-computed inputs → outputs.
//!
//! These test the per-bin swap math in isolation (no RPC, no event sync).
//! Each fixture is computed by running the Clarity-equivalent operations by
//! hand so we can verify the Rust port matches exactly. Mirrors the Python
//! POC's reproduction at [`test/fetch_bitflow_dlmm.py:167-194`].

use stacks_dex_pools::dlmm::math::{
    bin_price, single_bin_swap_x_for_y, single_bin_swap_y_for_x, SingleBinSwap,
};
use stacks_dex_pools::dlmm::PRICE_SCALE_BPS;

/// Factor table for testing — center bin (id 0) at PRICE_SCALE_BPS = "1.0".
/// `factors[500] = PRICE_SCALE_BPS`; ±k step uses `(1 + 0.0001)^±k`.
fn unit_factors() -> Vec<u128> {
    let mut v = vec![0u128; 1001];
    v[500] = PRICE_SCALE_BPS;
    let mut up = PRICE_SCALE_BPS;
    let mut down = PRICE_SCALE_BPS;
    for k in 1..=10 {
        up = up * 10_001 / 10_000;
        down = down * 10_000 / 10_001;
        v[(500 + k) as usize] = up;
        v[(500 - k) as usize] = down;
    }
    v
}

#[test]
fn bin_price_center_is_initial() {
    let factors = unit_factors();
    let price = bin_price(PRICE_SCALE_BPS, &factors, 0).unwrap();
    assert_eq!(price, PRICE_SCALE_BPS);
}

#[test]
fn bin_price_signed_arithmetic() {
    let factors = unit_factors();
    // Bin +1 should be ~1bp above center.
    let p_plus = bin_price(PRICE_SCALE_BPS, &factors, 1).unwrap();
    assert!(p_plus > PRICE_SCALE_BPS);
    // Bin -1 should be slightly below.
    let p_minus = bin_price(PRICE_SCALE_BPS, &factors, -1).unwrap();
    assert!(p_minus < PRICE_SCALE_BPS);
}

#[test]
fn bin_price_out_of_range_errors() {
    let factors = unit_factors();
    assert!(bin_price(PRICE_SCALE_BPS, &factors, 501).is_err());
    assert!(bin_price(PRICE_SCALE_BPS, &factors, -501).is_err());
    // Range check: a factor list of length 1001 supports IDs -500..=500 exactly.
}

/// Zero-fee single bin: 1000 x at 1:1 price gets exactly 1000 y back, no
/// fees. Trivial but ensures the no-fee path doesn't drop anything.
#[test]
fn zero_fee_one_to_one() {
    let r = single_bin_swap_x_for_y(1000, 0, 1_000_000, PRICE_SCALE_BPS, 0, 0, 0);
    assert_eq!(
        r,
        SingleBinSwap {
            dx_after_fee: 1000,
            dy: 1000,
            used: 1000,
        }
    );
}

/// 30 bps split = 15 protocol + 15 provider. 10000 in → 30 fee → 9970 dx → 9970 dy at 1:1.
/// This is the live bps-10 STX/USDCx pool's fee structure (no variable fee).
#[test]
fn thirty_bps_one_to_one() {
    let r = single_bin_swap_x_for_y(10_000, 0, 1_000_000, PRICE_SCALE_BPS, 15, 15, 0);
    assert_eq!(
        r,
        SingleBinSwap {
            dx_after_fee: 9_970,
            dy: 9_970,
            used: 10_000,
        }
    );
}

/// Inventory cap: bin has only 100 y; arbitrary input is capped at the
/// amount that drains the bin (which is ~100 x with no fee at 1:1).
#[test]
fn inventory_cap() {
    let r = single_bin_swap_x_for_y(1_000_000, 0, 100, PRICE_SCALE_BPS, 0, 0, 0);
    // Contract: max_x = ceil(100 * 10^8 / 10^8) = 100, used = 100, dy = 100.
    assert_eq!(r.used, 100);
    assert_eq!(r.dy, 100);
}

/// Empty bin: no y inventory means no input can be consumed.
#[test]
fn empty_bin_zero_output() {
    let r = single_bin_swap_x_for_y(1000, 0, 0, PRICE_SCALE_BPS, 15, 15, 0);
    assert_eq!(r.used, 0);
    assert_eq!(r.dy, 0);
}

/// Bin price below 1 (we're "buying y at a discount"): bin_price = 0.5 = 5*10^7.
/// 1000 x in at no fee should yield 500 y (since dy = dx * price / scale).
#[test]
fn half_price_doubles_x_required() {
    let half = PRICE_SCALE_BPS / 2;
    let r = single_bin_swap_x_for_y(1000, 0, 1_000_000, half, 0, 0, 0);
    assert_eq!(r.dy, 500); // dx * half / scale
}

/// Variable fee field works: 15 + 15 + 30 = 60 bps total → 60 fee on 10k input.
#[test]
fn variable_fee_added() {
    let r = single_bin_swap_x_for_y(10_000, 0, 1_000_000, PRICE_SCALE_BPS, 15, 15, 30);
    // total_fee = 60, fees = 10000 * 60 / 10000 = 60, dx = 9940, dy = 9940.
    assert_eq!(r.dx_after_fee, 9_940);
    assert_eq!(r.dy, 9_940);
}

/// Symmetric y→x at the same price: 10000 y in → ~9970 x out at 30 bps.
#[test]
fn y_for_x_symmetric() {
    let r = single_bin_swap_y_for_x(10_000, 1_000_000, 0, PRICE_SCALE_BPS, 15, 15, 0);
    assert_eq!(r.used, 10_000);
    // dy field on SingleBinSwap holds the output side, which for y→x is x.
    assert_eq!(r.dy, 9_970);
}

/// At a +10bp bin (price slightly above 1), x→y requires fewer x for the
/// same y output (price favours holders of x in the +direction).
#[test]
fn higher_price_more_y_per_x() {
    let factors = unit_factors();
    let bp_center = bin_price(PRICE_SCALE_BPS, &factors, 0).unwrap();
    let bp_plus = bin_price(PRICE_SCALE_BPS, &factors, 5).unwrap();
    // bp_plus > bp_center; dy = dx * bp / SCALE → bigger bp = more dy.
    let r_center = single_bin_swap_x_for_y(1000, 0, 1_000_000, bp_center, 0, 0, 0);
    let r_plus = single_bin_swap_x_for_y(1000, 0, 1_000_000, bp_plus, 0, 0, 0);
    assert!(r_plus.dy >= r_center.dy);
}
