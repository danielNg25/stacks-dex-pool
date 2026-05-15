//! Cache for `dlmm-core-v-1-1.get-bin-factors-by-step(bin_step)`.
//!
//! The contract pre-computes `(1 + bin_step/10000)^k * PRICE_SCALE` for
//! `k = -500..=500` per supported `bin_step` and stores them as a list. The
//! list is **immutable** per (contract, bin_step), so we cache forever after
//! first fetch.
//!
//! Without the `rpc` feature, only the in-memory cache type is exposed (call
//! [`FactorCache::insert`] manually, e.g. from a fixture for tests).

use std::collections::HashMap;
use std::sync::RwLock;

use once_cell::sync::Lazy;

/// Per-process cache: bin_step → 1001-element factor list.
static GLOBAL: Lazy<RwLock<HashMap<u32, Vec<u128>>>> = Lazy::new(|| RwLock::new(HashMap::new()));

/// Insert a factor list into the global cache. Idempotent.
pub fn insert(bin_step: u32, factors: Vec<u128>) {
    GLOBAL.write().unwrap().insert(bin_step, factors);
}

/// Fetch a cached factor list if present.
pub fn get(bin_step: u32) -> Option<Vec<u128>> {
    GLOBAL.read().unwrap().get(&bin_step).cloned()
}

/// RPC-fetcher convenience — fetch from chain if not cached, else return
/// cached. Only available with the `rpc` feature.
#[cfg(feature = "rpc")]
pub async fn get_or_fetch(
    client: &crate::rpc::client::StacksRpcClient,
    core: &crate::pool::principal::Principal,
    bin_step: u32,
) -> anyhow::Result<Vec<u128>> {
    if let Some(v) = get(bin_step) {
        return Ok(v);
    }
    // Call <core>.get-bin-factors-by-step(bin_step). Returns `(some (list ...))`.
    let (deployer, contract_name) = match core {
        crate::pool::principal::Principal::Contract {
            version,
            hash160,
            name,
        } => (
            crate::codec::c32::c32_encode_address(*version, hash160),
            name.clone(),
        ),
        _ => return Err(anyhow::anyhow!("core must be a contract principal")),
    };
    let args = vec![crate::codec::clarity::cv_uint(bin_step as u128)];
    let result = client
        .call_read(
            &deployer,
            &contract_name,
            "get-bin-factors-by-step",
            &args,
            None,
        )
        .await?;
    // result: (ok (some (list uint ...))) — we want the inner list.
    use crate::codec::clarity::ClarityValue;
    let inner = result.unwrap_ok()?;
    let list = match inner {
        ClarityValue::OptionalSome(b) => *b,
        ClarityValue::List(_) => inner, // some impls flatten the option
        other => {
            return Err(anyhow::anyhow!("expected (some list), got {:?}", other));
        }
    };
    let items = match list {
        ClarityValue::List(items) => items,
        other => return Err(anyhow::anyhow!("expected list, got {:?}", other)),
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(item.as_uint()?);
    }
    insert(bin_step, out.clone());
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dlmm::math::bin_price;
    use crate::dlmm::PRICE_SCALE_BPS;

    /// A 1bp factor table — center bin = 1.0, each step ±0.01% (multiplicative).
    /// Just enough samples around center to test the math; full chain table
    /// has 1001 entries.
    fn fake_bps1_factors() -> Vec<u128> {
        // For testing only — compute factors around center.
        // center (index 500, signed 0): PRICE_SCALE_BPS = 10^8
        // +1 (index 501, signed +1): 10^8 * 10001 / 10000 = 100_010_000
        // -1 (index 499, signed -1): 10^8 * 10000 / 10001 ≈ 99_990_001 (floor)
        let mut v = vec![0u128; 1001];
        v[500] = PRICE_SCALE_BPS;
        // For test purposes, fill ±5 around center with computed steps.
        let mut up = PRICE_SCALE_BPS;
        let mut down = PRICE_SCALE_BPS;
        for k in 1..=5 {
            up = up * 10_001 / 10_000;
            down = down * 10_000 / 10_001;
            v[(500 + k) as usize] = up;
            v[(500 - k) as usize] = down;
        }
        v
    }

    #[test]
    fn factor_cache_roundtrip() {
        let factors = fake_bps1_factors();
        insert(99_999, factors.clone());
        let got = get(99_999).unwrap();
        assert_eq!(got, factors);
    }

    #[test]
    fn bin_price_uses_factors() {
        let factors = fake_bps1_factors();
        let price = bin_price(PRICE_SCALE_BPS, &factors, 0).unwrap();
        assert_eq!(price, PRICE_SCALE_BPS); // center bin at initial price
        let price_plus = bin_price(PRICE_SCALE_BPS, &factors, 1).unwrap();
        assert_eq!(price_plus, 100_010_000); // up 1 bp
    }

    #[test]
    fn bin_price_out_of_range() {
        let factors = fake_bps1_factors();
        assert!(bin_price(PRICE_SCALE_BPS, &factors, 501).is_err());
        assert!(bin_price(PRICE_SCALE_BPS, &factors, -501).is_err());
    }
}
