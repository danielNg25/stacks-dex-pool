//! DLMM swap math — single-bin and multi-bin walker.
//!
//! Pure-integer port of [`fetch_bitflow_dlmm.py:137-194`] (single bin) and
//! [`:361-405`] (walker). All arithmetic is `u128`; intermediate products
//! (`y_balance * PRICE_SCALE_BPS`) fit because Stacks token magnitudes are
//! bounded by Clarity `uint`'s 2^128 cap — and even the largest practical
//! pool we'd see has `y_balance < 2^70`, so `* 10^8` stays under 2^104.
//!
//! Matches the on-chain `dlmm-core-v-1-1.swap-x-for-y` byte-for-byte for the
//! single-bin step; the walker reproduces the loop in `swap-x-for-y` that
//! moves to the next bin when inventory exhausts.

use anyhow::{anyhow, Result};

use super::{CENTER_BIN_ID, FEE_SCALE_BPS, PRICE_SCALE_BPS};

/// Compute the price of a bin given the pool's initial price and bin step.
///
/// `signed_bin_id` is in `[-500, 500]`. The factor table is per-step:
/// `dlmm-core-v-1-1.get-bin-factors-by-step(bin_step)` returns 1001 uints.
/// `factors[unsigned_bin_id] = (1 + bin_step/10000)^signed_bin_id * PRICE_SCALE_BPS`
/// pre-computed off-chain.
///
/// Returns `(initial_price * factor) / PRICE_SCALE_BPS`. Floor division
/// matches Clarity `/`.
pub fn bin_price(initial_price: u128, factors: &[u128], signed_bin_id: i32) -> Result<u128> {
    let unsigned = signed_bin_id
        .checked_add(CENTER_BIN_ID)
        .ok_or_else(|| anyhow!("bin id overflow"))?;
    if unsigned < 0 || (unsigned as usize) >= factors.len() {
        return Err(anyhow!("bin_id {} out of factor range", signed_bin_id));
    }
    let factor = factors[unsigned as usize];
    // Multiply with checked overflow — factor and initial_price are both well
    // under 2^96 in practice but be explicit.
    Ok(initial_price
        .checked_mul(factor)
        .ok_or_else(|| anyhow!("bin_price overflow"))?
        / PRICE_SCALE_BPS)
}

/// Result of a single-bin x→y swap step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SingleBinSwap {
    /// Net x consumed by the swap (input minus fees).
    pub dx_after_fee: u128,
    /// y received.
    pub dy: u128,
    /// Total x consumed from input (including fees).
    pub used: u128,
}

/// Reproduction of `dlmm-core-v-1-1.swap-x-for-y` math for a SINGLE bin.
///
/// Port of [`test/fetch_bitflow_dlmm.py:167-194`]:
/// ```text
/// max_x_no_fee = ceil(y_balance * PRICE_SCALE / bin_price)
/// if total_fee > 0:
///     max_x = max_x_no_fee * FEE_SCALE / (FEE_SCALE - total_fee)
/// else:
///     max_x = max_x_no_fee
/// used = min(x_amount, max_x)
/// fees = used * total_fee / FEE_SCALE
/// dx = used - fees
/// dy = min(dx * bin_price / PRICE_SCALE, y_balance)
/// ```
///
/// `protocol_fee + provider_fee + variable_fee` is the total fee in bps.
/// Floor division at every step matches Clarity `/`.
pub fn single_bin_swap_x_for_y(
    x_amount: u128,
    _x_balance: u128, // unused in the contract path (kept for symmetry)
    y_balance: u128,
    bin_price: u128,
    protocol_fee: u32,
    provider_fee: u32,
    variable_fee: u32,
) -> SingleBinSwap {
    if bin_price == 0 {
        return SingleBinSwap {
            dx_after_fee: 0,
            dy: 0,
            used: 0,
        };
    }
    let total_fee = protocol_fee + provider_fee + variable_fee;
    // max-x-no-fee = ceil(y_balance * PRICE_SCALE / bin_price)
    let num = y_balance.saturating_mul(PRICE_SCALE_BPS);
    let max_x_no_fee = num.div_ceil(bin_price);
    let max_x = if total_fee > 0 {
        // max_x = max_x_no_fee * FEE_SCALE / (FEE_SCALE - total_fee), floor.
        // total_fee < FEE_SCALE in any sensible config; we don't guard but if
        // someone sets fee=10000 the pool is broken at the contract level.
        let denom = FEE_SCALE_BPS - total_fee;
        max_x_no_fee.saturating_mul(FEE_SCALE_BPS as u128) / denom as u128
    } else {
        max_x_no_fee
    };
    let used = x_amount.min(max_x);
    let fees = used.saturating_mul(total_fee as u128) / FEE_SCALE_BPS as u128;
    let dx = used - fees;
    let dy_uncapped = dx.saturating_mul(bin_price) / PRICE_SCALE_BPS;
    let dy = dy_uncapped.min(y_balance);
    SingleBinSwap {
        dx_after_fee: dx,
        dy,
        used,
    }
}

/// Symmetric helper for y→x. The on-chain `swap-y-for-x` has the mirror math.
/// Same structure with x and y swapped and walking UP bins instead of down.
pub fn single_bin_swap_y_for_x(
    y_amount: u128,
    x_balance: u128,
    _y_balance: u128,
    bin_price: u128,
    protocol_fee: u32,
    provider_fee: u32,
    variable_fee: u32,
) -> SingleBinSwap {
    if bin_price == 0 {
        return SingleBinSwap {
            dx_after_fee: 0,
            dy: 0,
            used: 0,
        };
    }
    let total_fee = protocol_fee + provider_fee + variable_fee;
    // max-y-no-fee = ceil(x_balance * bin_price / PRICE_SCALE)
    let num = x_balance.saturating_mul(bin_price);
    let max_y_no_fee = num.div_ceil(PRICE_SCALE_BPS);
    let max_y = if total_fee > 0 {
        let denom = FEE_SCALE_BPS - total_fee;
        max_y_no_fee.saturating_mul(FEE_SCALE_BPS as u128) / denom as u128
    } else {
        max_y_no_fee
    };
    let used = y_amount.min(max_y);
    let fees = used.saturating_mul(total_fee as u128) / FEE_SCALE_BPS as u128;
    let dy_after = used - fees;
    let dx_uncapped = dy_after.saturating_mul(PRICE_SCALE_BPS) / bin_price;
    let dx = dx_uncapped.min(x_balance);
    SingleBinSwap {
        // For y→x we still report dx as the "received-side" output. The
        // walker treats `dy` as "output side" regardless of direction.
        dx_after_fee: dy_after,
        dy: dx,
        used,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke test: zero input → zero output, no fees consumed.
    #[test]
    fn zero_input_zero_output() {
        let r = single_bin_swap_x_for_y(0, 0, 1_000_000_000, 100_000_000, 15, 15, 0);
        assert_eq!(r.used, 0);
        assert_eq!(r.dy, 0);
    }

    /// Zero y-balance → no inventory, max_x is 0, used is 0.
    #[test]
    fn empty_bin() {
        let r = single_bin_swap_x_for_y(1_000_000, 1_000_000, 0, 100_000_000, 15, 15, 0);
        assert_eq!(r.used, 0);
        assert_eq!(r.dy, 0);
    }

    /// Fee math sanity: 30bps total → ~0.3% loss to fees on consumed input.
    #[test]
    fn fees_reduce_dx() {
        // bin_price = 1.0 (= PRICE_SCALE_BPS), so 1 x ↔ 1 y at no fee.
        // Plenty of y inventory so we don't cap.
        let bp = PRICE_SCALE_BPS;
        let r = single_bin_swap_x_for_y(
            10_000, 0, 1_000_000, bp, 15, 15, 0, // 30 bps total
        );
        // used should equal input (plenty of headroom)
        assert_eq!(r.used, 10_000);
        // fees = 10_000 * 30 / 10000 = 30
        // dx = 9_970, dy = 9_970 (1:1 price)
        assert_eq!(r.dx_after_fee, 9_970);
        assert_eq!(r.dy, 9_970);
    }

    /// Inventory cap: when input exceeds what the bin can absorb, `used`
    /// is capped at `max_x` and `dy` equals `y_balance`.
    #[test]
    fn capped_by_inventory() {
        let bp = PRICE_SCALE_BPS;
        let y = 100u128;
        // Huge input, tiny bin.
        let r = single_bin_swap_x_for_y(1_000_000_000, 0, y, bp, 0, 0, 0);
        // No fees, max_x = ceil(100 * 10^8 / 10^8) = 100, used = 100
        assert_eq!(r.used, 100);
        assert_eq!(r.dy, 100);
    }
}
