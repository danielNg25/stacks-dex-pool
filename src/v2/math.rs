//! Shared constant-product math for the V2 family.
//!
//! The Uniswap V2 formula with a bps fee:
//!
//! ```text
//! dx_after_fee = dx * (10_000 - fee_bps) / 10_000
//! dy           = reserve_out * dx_after_fee / (reserve_in + dx_after_fee)
//! ```
//!
//! Variants differ in *where* fee bps comes from and whether the formula is
//! direction-symmetric:
//!   - Arkadiko: hardcoded 30 bps, encoded inline (`997 * dx / 1000`).
//!   - Velar:    per-pool `(fee_num, fee_den)` tuple from `lookup-pool`.
//!   - ALEX:     constant-product + per-pool `factor` (see [`super::alex`]).
//!   - Bitflow XYK: per-direction bps (asymmetric protocol+provider fees).
//!
//! All math is `u128` with Clarity-style flooring (`/` truncates). Outputs
//! match the contracts byte-exact where an on-chain helper exists (Velar
//! `get-amount-out`, Bitflow `<core>::get-dy`); for Arkadiko and ALEX we
//! mirror the inline source.

/// Standard `dy = reserve_out * dx_after_fee / (reserve_in + dx_after_fee)`
/// with a fee expressed as `(num, den)` so callers can pass `(997, 1000)`
/// (hardcoded 30 bps), `(9970, 10_000)` (Velar standard), or any other
/// numerator/denominator pair.
///
/// Returns 0 for any degenerate input — zero reserves, zero dx, or fee
/// numerator >= denominator.
pub fn quote_amount_out_with_fee_ratio(
    dx: u128,
    reserve_in: u128,
    reserve_out: u128,
    fee_num: u128,
    fee_den: u128,
) -> u128 {
    if dx == 0 || reserve_in == 0 || reserve_out == 0 || fee_den == 0 || fee_num > fee_den {
        return 0;
    }
    let dx_after_fee = dx.saturating_mul(fee_num) / fee_den;
    if dx_after_fee == 0 {
        return 0;
    }
    reserve_out.saturating_mul(dx_after_fee) / (reserve_in.saturating_add(dx_after_fee))
}

/// Same shape, but with the fee expressed in basis points. Convenience over
/// [`quote_amount_out_with_fee_ratio`]: pass `30` for 0.30%, `25` for 0.25%.
pub fn quote_amount_out_with_fee_bps(
    dx: u128,
    reserve_in: u128,
    reserve_out: u128,
    fee_bps: u32,
) -> u128 {
    if fee_bps >= 10_000 {
        return 0;
    }
    let fee_num = (10_000 - fee_bps) as u128;
    quote_amount_out_with_fee_ratio(dx, reserve_in, reserve_out, fee_num, 10_000)
}

/// Sanity check: after applying a (dx, dy) trade, the constant-product
/// invariant must not shrink. Used in tests and never on the hot path.
pub fn invariant_non_decreasing(reserve_in: u128, reserve_out: u128, dx: u128, dy: u128) -> bool {
    let lhs = reserve_in
        .saturating_add(dx)
        .saturating_mul(reserve_out.saturating_sub(dy));
    let rhs = reserve_in.saturating_mul(reserve_out);
    lhs >= rhs
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Arkadiko fixture: STX/USDA-shaped pool. The (997, 1000) ratio is
    /// equivalent to 30 bps; we lock in the exact integer Python yields.
    /// Reference: arbitrage-rs/crates/stacks/src/arkadiko.rs:392-399 (test
    /// `quote_stx_usda_fixture`).
    #[test]
    fn arkadiko_fixture_byte_exact() {
        let dy = quote_amount_out_with_fee_ratio(
            3_024_475_304,
            604_895_060_900,
            160_672_499_568,
            997,
            1000,
        );
        assert_eq!(dy, 796_979_467);
    }

    #[test]
    fn thirty_bps_matches_997_over_1000() {
        let dy_bps = quote_amount_out_with_fee_bps(1_000_000, 1_000_000_000, 2_000_000_000, 30);
        let dy_ratio =
            quote_amount_out_with_fee_ratio(1_000_000, 1_000_000_000, 2_000_000_000, 997, 1000);
        // 997/1000 == 9970/10000 — same fee, same answer.
        assert_eq!(dy_bps, dy_ratio);
    }

    #[test]
    fn zero_inputs_return_zero() {
        assert_eq!(quote_amount_out_with_fee_bps(0, 100, 100, 30), 0);
        assert_eq!(quote_amount_out_with_fee_bps(100, 0, 100, 30), 0);
        assert_eq!(quote_amount_out_with_fee_bps(100, 100, 0, 30), 0);
    }

    #[test]
    fn fee_at_or_over_ten_thousand_bps_returns_zero() {
        assert_eq!(
            quote_amount_out_with_fee_bps(1_000, 1_000_000, 1_000_000, 10_000),
            0
        );
        assert_eq!(
            quote_amount_out_with_fee_bps(1_000, 1_000_000, 1_000_000, 15_000),
            0
        );
    }

    #[test]
    fn invariant_holds_for_normal_trade() {
        let bx = 1_000_000_000u128;
        let by = 2_000_000_000u128;
        let dx = 1_000_000u128;
        let dy = quote_amount_out_with_fee_bps(dx, bx, by, 30);
        assert!(invariant_non_decreasing(bx, by, dx, dy));
    }

    #[test]
    fn fee_strictly_reduces_dy_vs_no_fee() {
        let with_fee = quote_amount_out_with_fee_bps(100, 1_000_000, 1_000_000, 30);
        let no_fee = quote_amount_out_with_fee_bps(100, 1_000_000, 1_000_000, 0);
        assert!(with_fee < no_fee);
    }
}
