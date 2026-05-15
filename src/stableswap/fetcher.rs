//! Bitflow StableSwap bootstrap — V2 + V1 (rpc-gated).
//!
//! V2 reads `<pool>::get-pool` which returns a tuple with reserves, fees,
//! `core-address`, `amplification-coefficient`, `convergence-threshold`,
//! `midpoint-primary-{numerator,denominator}`, and the token principals.
//! Pools may bind different `stableswap-core-v-1-N` versions; the core
//! address is read per-pool, never hardcoded.
//!
//! V1 takes the dual-ABI route (see [`super::bitflow_v1`]): bootstrap
//! dispatches on [`super::bitflow_v1::Sig`] to choose between the 2-arg
//! (STX-anchored) and 3-arg (token-pair) `get-pair-data`. Fees come from
//! separate data-vars: `buy-fees`/`sell-fees` (sig=2, direction-asymmetric)
//! or `swap-fees` (sig=3, symmetric).

use std::sync::Arc;

use anyhow::{anyhow, Result};

use super::bitflow_v1::{BitflowStableSwapV1Pool, MathVariant, Sig, V1_DEPLOYER};
use super::bitflow_v2::BitflowStableSwapV2Pool;
use crate::codec::c32::c32_encode_address;
use crate::codec::clarity::{cv_principal, ClarityValue};
use crate::pool::principal::Principal;
use crate::rpc::client::StacksRpcClient;
use crate::token_info::TokenInfo;

/// Bootstrap a Bitflow V2 stableswap pool from `<pool>::get-pool`.
///
/// Token ordering on V2 stableswap is committed at pool creation; this
/// function does NOT probe orderings. Pass `expected_x_token` /
/// `expected_y_token` to match the pool's on-chain `(x-token, y-token)` or
/// it bails with a clear error (same posture as `fetch_bitflow_xyk_pool`).
pub async fn fetch_bitflow_v2_stable_pool(
    client: Arc<StacksRpcClient>,
    pool_contract: &Principal,
    expected_x_token: &Principal,
    expected_y_token: &Principal,
    token_info: &dyn TokenInfo,
    tip: Option<&str>,
) -> Result<BitflowStableSwapV2Pool> {
    let (deployer, name) = principal_parts(pool_contract)?;
    let raw = client
        .call_read(&deployer, &name, "get-pool", &[], tip)
        .await?
        .unwrap_ok()?;

    let x_balance = raw.field("x-balance")?.as_uint()?;
    let y_balance = raw.field("y-balance")?.as_uint()?;
    let x_protocol_fee_bps = raw.field("x-protocol-fee")?.as_uint()? as u32;
    let x_provider_fee_bps = raw.field("x-provider-fee")?.as_uint()? as u32;
    let y_protocol_fee_bps = raw.field("y-protocol-fee")?.as_uint()? as u32;
    let y_provider_fee_bps = raw.field("y-provider-fee")?.as_uint()? as u32;
    let amp = raw.field("amplification-coefficient")?.as_uint()?;
    // `convergence-threshold` is universally `u2` in practice but admin-settable.
    let threshold = raw
        .field("convergence-threshold")
        .ok()
        .and_then(|v| v.as_uint().ok())
        .unwrap_or(2);
    // Midpoint fields exist only on cores ≥ v-1-3. Older cores (e.g.
    // `stableswap-core-v-1-2`, sBTC/pBTC pool) omit them — those pools are
    // 1:1 pegged, so midpoint is implicitly 1/1.
    let midpoint_num = raw
        .field("midpoint-primary-numerator")
        .ok()
        .and_then(|v| v.as_uint().ok())
        .unwrap_or(1);
    let midpoint_den = raw
        .field("midpoint-primary-denominator")
        .ok()
        .and_then(|v| v.as_uint().ok())
        .unwrap_or(1);
    let pool_status = raw
        .field("pool-status")
        .ok()
        .and_then(|v| v.as_bool().ok())
        .unwrap_or(true);
    let core_contract = raw.field("core-address")?.as_principal()?.clone();
    let on_chain_x = raw.field("x-token")?.as_principal()?.clone();
    let on_chain_y = raw.field("y-token")?.as_principal()?.clone();

    if &on_chain_x != expected_x_token || &on_chain_y != expected_y_token {
        return Err(anyhow!(
            "BitflowV2Stable pool {} tokens ({}, {}) don't match expected ({}, {})",
            pool_contract,
            on_chain_x,
            on_chain_y,
            expected_x_token,
            expected_y_token,
        ));
    }

    let (x_decimals, y_decimals) = tokio::try_join!(
        token_info.decimals(expected_x_token),
        token_info.decimals(expected_y_token),
    )?;

    Ok(BitflowStableSwapV2Pool {
        pool_contract: pool_contract.clone(),
        core_contract,
        x_token: expected_x_token.clone(),
        y_token: expected_y_token.clone(),
        x_decimals,
        y_decimals,
        x_balance,
        y_balance,
        x_protocol_fee_bps,
        x_provider_fee_bps,
        y_protocol_fee_bps,
        y_provider_fee_bps,
        amp,
        threshold,
        midpoint_num,
        midpoint_den,
        pool_status,
        last_tx_id: None,
    })
}

/// Bootstrap a Bitflow V1 stableswap pool. Dispatches the RPC shape on `sig`:
///   - [`Sig::StxAnchored`]: `get-pair-data(y_token, lp_token)`.
///   - [`Sig::TokenPair`]: `get-pair-data(x_token, y_token, lp_token)`.
///
/// On top of `get-pair-data` (which returns reserves + decimals + amp +
/// approval), this also reads the convergence-threshold and fee data-vars
/// (`buy-fees`/`sell-fees` for sig=2, `swap-fees` for sig=3).
///
/// `caller` MUST pass `expected_x_token` that matches the pool's on-chain
/// `x` (which for sig=2 is the implicit STX wrap principal).
#[allow(clippy::too_many_arguments)]
pub async fn fetch_bitflow_v1_stable_pool(
    client: Arc<StacksRpcClient>,
    pool_contract: &Principal,
    lp_token: &Principal,
    expected_x_token: &Principal,
    expected_y_token: &Principal,
    sig: Sig,
    variant: MathVariant,
    token_info: &dyn TokenInfo,
    tip: Option<&str>,
) -> Result<BitflowStableSwapV1Pool> {
    let (deployer, name) = principal_parts(pool_contract)?;
    if deployer != V1_DEPLOYER {
        log::warn!(
            "fetch_bitflow_v1_stable_pool: pool deployer {} != known V1 deployer {} — proceeding anyway",
            deployer,
            V1_DEPLOYER,
        );
    }

    // ---- 1) get-pair-data: reserves + decimals + amp + approval ----
    let args = match sig {
        Sig::StxAnchored => vec![cv_principal(expected_y_token), cv_principal(lp_token)],
        Sig::TokenPair => vec![
            cv_principal(expected_x_token),
            cv_principal(expected_y_token),
            cv_principal(lp_token),
        ],
    };
    let outer = client
        .call_read(&deployer, &name, "get-pair-data", &args, tip)
        .await?;

    // Some V1 deployments return `(some {...})` directly, others
    // `(ok (some {...}))`. Tolerate both.
    let inner = match outer {
        ClarityValue::OptionalSome(b) => *b,
        ClarityValue::OptionalNone => {
            return Err(anyhow!(
                "BitflowV1 get-pair-data returned none for {}",
                pool_contract
            ));
        }
        ClarityValue::ResponseOk(boxed) => match *boxed {
            ClarityValue::OptionalSome(b) => *b,
            ClarityValue::OptionalNone => {
                return Err(anyhow!(
                    "BitflowV1 get-pair-data (ok-wrapped) returned none for {}",
                    pool_contract
                ));
            }
            other => other,
        },
        other => {
            return Err(anyhow!(
                "BitflowV1 get-pair-data: expected optional<tuple>, got {:?}",
                other
            ));
        }
    };

    let x_balance = inner.field("balance-x")?.as_uint()?;
    let y_balance = inner.field("balance-y")?.as_uint()?;
    let x_decimals = inner.field("x-decimals")?.as_uint()? as u8;
    let y_decimals = inner.field("y-decimals")?.as_uint()? as u8;
    let amp = inner.field("amplification-coefficient")?.as_uint()?;
    let approval = inner
        .field("approval")
        .ok()
        .and_then(|v| v.as_bool().ok())
        .unwrap_or(true);

    // ---- 2) convergence-threshold (raw data-var) ----
    let threshold = client
        .data_var(&deployer, &name, "convergence-threshold")
        .await
        .ok()
        .and_then(|v| v.as_uint().ok())
        .unwrap_or(2);

    // ---- 3) fees (raw data-vars; shape depends on sig) ----
    // Bitflow V1 stores `buy-fees`/`sell-fees`/`swap-fees` as Clarity tuples
    // in data-vars (not as getter functions). The `/v2/data_var/...` endpoint
    // reads them directly — our earlier attempt at `(get-<var>)` failed with
    // `UndefinedFunction` because the V1 contract doesn't expose getters.
    let (buy_fee_bps, sell_fee_bps) = match sig {
        Sig::StxAnchored => {
            let buy = client.data_var(&deployer, &name, "buy-fees").await?;
            let sell = client.data_var(&deployer, &name, "sell-fees").await?;
            (sum_three_way(&buy)?, sum_three_way(&sell)?)
        }
        Sig::TokenPair => {
            let swap = client.data_var(&deployer, &name, "swap-fees").await?;
            let total = sum_two_way(&swap)?;
            (total, total)
        }
    };

    // Use on-chain decimals first; fall back to TokenInfo if the field is
    // bogus. (All current V1 pools report correct decimals so this is
    // defensive only.)
    let (x_decimals, y_decimals) = if x_decimals > 0 && y_decimals > 0 {
        (x_decimals, y_decimals)
    } else {
        tokio::try_join!(
            token_info.decimals(expected_x_token),
            token_info.decimals(expected_y_token),
        )?
    };

    Ok(BitflowStableSwapV1Pool {
        pool_contract: pool_contract.clone(),
        lp_token: lp_token.clone(),
        x_token: expected_x_token.clone(),
        y_token: expected_y_token.clone(),
        x_decimals,
        y_decimals,
        x_balance,
        y_balance,
        amp,
        threshold,
        buy_fee_bps,
        sell_fee_bps,
        sig,
        variant,
        approval,
        last_tx_id: None,
    })
}

// ---- helpers ----

/// Sum a `{lps: uX, stacking-dao: uY, bitflow: uZ}` fee tuple (sig=2 form).
/// All in basis-points (10_000 = 100%).
fn sum_three_way(v: &ClarityValue) -> Result<u32> {
    let total: u128 = ["lps", "stacking-dao", "bitflow"]
        .iter()
        .map(|k| v.field(k).and_then(|f| f.as_uint()).unwrap_or(0))
        .sum();
    if total > u32::MAX as u128 {
        return Err(anyhow!("3-way fee sum {} doesn't fit u32", total));
    }
    Ok(total as u32)
}

/// Sum a `{lps: uX, protocol: uY}` fee tuple (sig=3 form).
fn sum_two_way(v: &ClarityValue) -> Result<u32> {
    let total: u128 = ["lps", "protocol"]
        .iter()
        .map(|k| v.field(k).and_then(|f| f.as_uint()).unwrap_or(0))
        .sum();
    if total > u32::MAX as u128 {
        return Err(anyhow!("2-way fee sum {} doesn't fit u32", total));
    }
    Ok(total as u32)
}

fn principal_parts(p: &Principal) -> Result<(String, String)> {
    match p {
        Principal::Contract {
            version,
            hash160,
            name,
        } => Ok((c32_encode_address(*version, hash160), name.clone())),
        Principal::Standard { .. } => {
            Err(anyhow!("expected contract principal, got standard: {}", p))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::clarity::ClarityValue;
    use std::collections::BTreeMap;

    fn make_three_way_fee(lps: u128, stacking_dao: u128, bitflow: u128) -> ClarityValue {
        let mut m = BTreeMap::new();
        m.insert("lps".to_string(), ClarityValue::Uint(lps));
        m.insert("stacking-dao".to_string(), ClarityValue::Uint(stacking_dao));
        m.insert("bitflow".to_string(), ClarityValue::Uint(bitflow));
        ClarityValue::Tuple(m)
    }

    fn make_two_way_fee(lps: u128, protocol: u128) -> ClarityValue {
        let mut m = BTreeMap::new();
        m.insert("lps".to_string(), ClarityValue::Uint(lps));
        m.insert("protocol".to_string(), ClarityValue::Uint(protocol));
        ClarityValue::Tuple(m)
    }

    #[test]
    fn sum_three_way_sums_components() {
        // STX/stSTX-style: 1 + 2 + 2 = 5 bps.
        let v = make_three_way_fee(1, 2, 2);
        assert_eq!(sum_three_way(&v).unwrap(), 5);
    }

    #[test]
    fn sum_three_way_missing_keys_default_to_zero() {
        // Forward-compat: missing field shouldn't break parsing.
        let mut m = BTreeMap::new();
        m.insert("lps".to_string(), ClarityValue::Uint(3));
        let v = ClarityValue::Tuple(m);
        assert_eq!(sum_three_way(&v).unwrap(), 3);
    }

    #[test]
    fn sum_two_way_sums_components() {
        // USDA/aeUSDC-style: 4 + 2 = 6 bps.
        let v = make_two_way_fee(4, 2);
        assert_eq!(sum_two_way(&v).unwrap(), 6);
    }
}
