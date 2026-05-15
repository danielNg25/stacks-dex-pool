//! Curve invariant solver + decimal scaling — shared by Bitflow V1 and V2
//! stableswap variants.
//!
//! Math is a direct port of [arbitrage-rs/crates/stacks/src/bitflow_v2_stable.rs]
//! and [arbitrage-rs/crates/stacks/src/bitflow_v1.rs] which themselves mirror
//! the on-chain `stableswap-core-v-1-X.get-d` / `get-y` byte-exact. Two
//! Newton-Raphson loops:
//!
//! - [`get_d`] solves the invariant
//!   `A·n^n·sum + D = A·n^n·D + D^(n+1) / (n^n · prod)` for `D` given balances.
//! - [`get_y`] solves for the new `y` after adding `x_amount` to `x_bal`.
//!
//! All inputs/outputs are raw `u128`. The original Clarity contract uses
//! 128-bit math; `saturating_mul` here means future ultra-deep pools surface
//! as wrong-but-bounded outputs rather than UB. Byte-exact fixtures pinned
//! in the tests.
//!
//! ## V1 vs V2 callers
//! V2 (`bitflow_v2`) calls [`get_d`] / [`get_y`] directly with the standard
//! "pass pre-swap `x_bal`" convention. V1 has two pool generations:
//!
//! - V1 "fixed" (`-v-1-4+`): same as V2, calls [`get_y`] with pre-swap `x_bal`.
//! - V1 "bal-bug" (`-v-1-{1,2,3}`): the on-chain `get-dy` passes the
//!   POST-swap `x_bal` (= `x_bal + dx_net`) into `get-y`, which internally
//!   adds `x_amount` AGAIN. Net effect: `get-y` sees `x_bal_new = x_bal + 2*dx_net`.
//!   We reproduce that byte-exact by having the caller wrap as
//!   `get_y(dx_net, x_bal + dx_net, y_bal, amp, threshold)`.
//!
//! Both V1 generations have an existing live deployment we're matching;
//! see [`super::bitflow_v1::MathVariant`] for the dispatch.

/// Stableswap pools are 2-token; n=2 throughout.
pub const N_TOKENS: u128 = 2;

/// Newton-Raphson iteration cap. Matches the Clarity contract's bound.
/// Healthy 2-token pools converge in 4-7 iterations; this is a safety net.
pub const NEWTON_MAX_ITERS: u32 = 384;

/// Scale `(x, y)` up to `max(x_dp, y_dp)` precision so Curve math operates
/// on a single unit. Identity when decimals match (typical case).
pub fn scale_up(x: u128, y: u128, x_dp: u8, y_dp: u8) -> (u128, u128) {
    if x_dp == y_dp {
        return (x, y);
    }
    if x_dp > y_dp {
        let m = 10u128.saturating_pow((x_dp - y_dp) as u32);
        (x, y.saturating_mul(m))
    } else {
        let m = 10u128.saturating_pow((y_dp - x_dp) as u32);
        (x.saturating_mul(m), y)
    }
}

/// Inverse of [`scale_up`]. Returns native units from scaled values.
pub fn scale_down(x: u128, y: u128, x_dp: u8, y_dp: u8) -> (u128, u128) {
    if x_dp == y_dp {
        return (x, y);
    }
    if x_dp > y_dp {
        let m = 10u128.saturating_pow((x_dp - y_dp) as u32);
        (x, y / m)
    } else {
        let m = 10u128.saturating_pow((y_dp - x_dp) as u32);
        (x / m, y)
    }
}

/// Solve the Curve invariant for `D` given two balances and amplification.
///
/// `amp` is the pool's amplification coefficient (commonly 25-100). `threshold`
/// is the absolute convergence threshold (`u2` on every current Bitflow pool).
/// Returns 0 if the inputs are degenerate (zero reserves or zero amp).
pub fn get_d(x_bal: u128, y_bal: u128, amp: u128, threshold: u128) -> u128 {
    if x_bal == 0 || y_bal == 0 || amp == 0 {
        return 0;
    }
    let n = N_TOKENS;
    let an = amp.saturating_mul(n);
    let s = x_bal.saturating_add(y_bal);
    let mut d = s;
    for _ in 0..NEWTON_MAX_ITERS {
        let d_part_x = d.saturating_mul(d) / n.saturating_mul(x_bal);
        let d_part = d.saturating_mul(d_part_x) / n.saturating_mul(y_bal);
        let numer = (an
            .saturating_mul(s)
            .saturating_add(n.saturating_mul(d_part)))
        .saturating_mul(d);
        let denom = (an - 1)
            .saturating_mul(d)
            .saturating_add((n + 1).saturating_mul(d_part));
        if denom == 0 {
            return d;
        }
        let new_d = numer / denom;
        if new_d.abs_diff(d) <= threshold {
            return new_d;
        }
        d = new_d;
    }
    d
}

/// Solve for the new `y` after adding `x_amount` to `x_bal`. The function
/// internally computes `x_bal_new = x_bal + x_amount`. Returns 0 for
/// degenerate inputs.
///
/// Caller convention: pass pre-swap `x_bal`. (For Bitflow V1's "bal-bug"
/// pools, the caller passes post-swap `x_bal` to reproduce the contract's
/// double-count bug — see module doc.)
pub fn get_y(x_amount: u128, x_bal: u128, y_bal: u128, amp: u128, threshold: u128) -> u128 {
    if x_bal == 0 || y_bal == 0 || amp == 0 {
        return 0;
    }
    let n = N_TOKENS;
    let an = amp.saturating_mul(n);
    let updated_x = x_bal.saturating_add(x_amount);
    if updated_x == 0 {
        return 0;
    }
    let d = get_d(x_bal, y_bal, amp, threshold);
    let c_b = d.saturating_mul(d) / n.saturating_mul(updated_x);
    let c = c_b.saturating_mul(d) / an.saturating_mul(n);
    let b = updated_x.saturating_add(d / an);
    let mut y = d;
    for _ in 0..NEWTON_MAX_ITERS {
        let y_num = y.saturating_mul(y).saturating_add(c);
        let y_den_pos = n.saturating_mul(y).saturating_add(b);
        // The Clarity contract subtracts D after the +b; underflow on
        // degenerate inputs would `(err …)` on-chain — we return the
        // current iterate.
        if y_den_pos < d {
            return y;
        }
        let y_den = y_den_pos - d;
        if y_den == 0 {
            return y;
        }
        let new_y = y_num / y_den;
        if new_y.abs_diff(y) <= threshold {
            return new_y;
        }
        y = new_y;
    }
    y
}

#[cfg(test)]
mod tests {
    use super::*;

    /// For a balanced pool (x=y), Newton-Raphson on D collapses to D=2x.
    /// Pinned against the Python POC.
    #[test]
    fn get_d_balanced_pool_is_2x() {
        let d = get_d(1_000_000_000_000, 1_000_000_000_000, 100, 2);
        assert_eq!(d, 2_000_000_000_000);
    }

    /// Symmetric pool: y after adding dx to x_bal should keep D approximately
    /// constant. Verified against the Python POC.
    #[test]
    fn get_y_symmetric_pool_smoke() {
        // Balanced 1e12/1e12 pool; add 1e9 to x → new y should be ~999_000_999
        // (slightly less than 1e9 due to stableswap curvature on the symmetric
        // path). We only assert the order of magnitude here; byte-exact comes
        // from the V1/V2 wrappers.
        let new_y = get_y(1_000_000_000, 1_000_000_000_000, 1_000_000_000_000, 100, 2);
        assert!(new_y < 1_000_000_000_000);
        assert!(new_y > 998_000_000_000);
    }

    #[test]
    fn get_d_zero_inputs_return_zero() {
        assert_eq!(get_d(0, 1, 100, 2), 0);
        assert_eq!(get_d(1, 0, 100, 2), 0);
        assert_eq!(get_d(1, 1, 0, 2), 0);
    }

    #[test]
    fn get_y_zero_inputs_return_zero() {
        assert_eq!(get_y(100, 0, 1, 100, 2), 0);
        assert_eq!(get_y(100, 1, 0, 100, 2), 0);
        assert_eq!(get_y(100, 1, 1, 0, 2), 0);
    }

    #[test]
    fn scale_up_identity_when_decimals_match() {
        assert_eq!(scale_up(100, 200, 6, 6), (100, 200));
        assert_eq!(scale_up(123, 456, 8, 8), (123, 456));
    }

    #[test]
    fn scale_up_raises_smaller_side() {
        // y has fewer dp → scale y up.
        assert_eq!(scale_up(1_000_000, 100, 6, 4), (1_000_000, 10_000));
        // x has fewer dp → scale x up.
        assert_eq!(scale_up(100, 1_000_000, 4, 6), (10_000, 1_000_000));
    }

    #[test]
    fn scale_roundtrips() {
        let (x, y) = scale_up(1_000_000, 100, 6, 4);
        let (xx, yy) = scale_down(x, y, 6, 4);
        assert_eq!((xx, yy), (1_000_000, 100));
        let (x, y) = scale_up(100, 1_000_000, 4, 6);
        let (xx, yy) = scale_down(x, y, 4, 6);
        assert_eq!((xx, yy), (100, 1_000_000));
    }
}
