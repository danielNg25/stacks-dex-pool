//! V2-family bootstrap — one async fn per variant.
//!
//! Each variant has its own state-fetching shape so we don't try to fit all
//! four into one function. Shared helpers (principal split, response unwrap)
//! mirror [`crate::dlmm::fetcher`].
//!
//! All four variants implemented as of 2026-05.

use std::sync::Arc;

use anyhow::{anyhow, Result};

use super::alex::{AlexPool, CONSTANT_PRODUCT_FACTOR};
use super::arkadiko::ArkadikoPool;
use super::bitflow_xyk::BitflowXykPool;
use super::velar::VelarPool;
use crate::codec::c32::c32_encode_address;
use crate::codec::clarity::{cv_principal, cv_uint, ClarityValue};
use crate::pool::principal::Principal;
use crate::rpc::client::StacksRpcClient;
use crate::token_info::TokenInfo;

/// Bootstrap an Arkadiko pool by calling
/// `arkadiko-swap-v2-1.get-pair-details(token-x, token-y)`. The returned
/// optional tuple holds `balance-x`, `balance-y`, and `enabled`.
///
/// On-chain `(x, y)` may be flipped vs the caller's expectation. Pass
/// `expected_x_token` / `expected_y_token` in the order you want the
/// resulting pool to advertise; this function tries both orderings and
/// swaps the response if needed so the returned pool's `x_token == expected_x_token`.
pub async fn fetch_arkadiko_pool(
    client: Arc<StacksRpcClient>,
    swap_contract: &Principal, // SP....arkadiko-swap-v2-1
    expected_x_token: &Principal,
    expected_y_token: &Principal,
    token_info: &dyn TokenInfo,
    tip: Option<&str>,
) -> Result<ArkadikoPool> {
    let (deployer, name) = principal_parts(swap_contract)?;

    // Probe (x, y); if the contract returns `none`, swap and retry.
    let (raw, swapped) = match call_pair_details(
        &client,
        &deployer,
        &name,
        expected_x_token,
        expected_y_token,
        tip,
    )
    .await?
    {
        Some(t) => (t, false),
        None => {
            let alt = call_pair_details(
                &client,
                &deployer,
                &name,
                expected_y_token,
                expected_x_token,
                tip,
            )
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "Arkadiko get-pair-details returned none for both orderings of {} / {}",
                    expected_x_token,
                    expected_y_token,
                )
            })?;
            (alt, true)
        }
    };

    let raw_balance_x = raw.field("balance-x")?.as_uint()?;
    let raw_balance_y = raw.field("balance-y")?.as_uint()?;
    // `enabled` may be absent in some pool variants; default to true if so.
    let enabled = raw
        .field("enabled")
        .ok()
        .and_then(|v| v.as_bool().ok())
        .unwrap_or(true);
    // `swap-token` is the LP token principal — same one events carry, so we
    // can filter cross-pair using it on incoming swap events.
    let swap_token = raw
        .field("swap-token")
        .map_err(|e| anyhow!("Arkadiko get-pair-details missing `swap-token`: {}", e))?
        .as_principal()?
        .clone();

    // If the on-chain ordering was flipped relative to expected_(x,y), swap
    // the reserves so the pool's x_balance corresponds to expected_x_token.
    let (balance_x, balance_y) = if swapped {
        (raw_balance_y, raw_balance_x)
    } else {
        (raw_balance_x, raw_balance_y)
    };

    let (x_decimals, y_decimals) = tokio::try_join!(
        token_info.decimals(expected_x_token),
        token_info.decimals(expected_y_token),
    )?;

    Ok(ArkadikoPool {
        pool_contract: swap_contract.clone(),
        swap_token,
        x_token: expected_x_token.clone(),
        y_token: expected_y_token.clone(),
        x_decimals,
        y_decimals,
        balance_x,
        balance_y,
        enabled,
        last_tx_id: None,
    })
}

async fn call_pair_details(
    client: &StacksRpcClient,
    deployer: &str,
    name: &str,
    left: &Principal,
    right: &Principal,
    tip: Option<&str>,
) -> Result<Option<ClarityValue>> {
    let args = vec![cv_principal(left), cv_principal(right)];
    let res = client
        .call_read(deployer, name, "get-pair-details", &args, tip)
        .await?
        .unwrap_ok()?;
    match res {
        ClarityValue::OptionalNone => Ok(None),
        ClarityValue::OptionalSome(inner) => {
            // The tuple is inside the optional.
            Ok(Some(*inner))
        }
        ClarityValue::Tuple(_) => {
            // Some Clarity ABIs flatten the optional out; accept tuple too.
            Ok(Some(res))
        }
        other => Err(anyhow!(
            "get-pair-details: expected optional<tuple>, got {:?}",
            other
        )),
    }
}

/// Bootstrap a Velar pool by calling
/// `univ2-core::lookup-pool(token_a, token_b)`. Returns
/// `(ok (some {pool: {reserve0, reserve1, swap-fee: {num, den}, …}, flipped: bool}))`.
///
/// If `flipped == true`, on-chain `(token0, token1)` is reversed relative to
/// `(expected_x_token, expected_y_token)`. We swap reserves so the returned
/// pool's `reserve_x` corresponds to `expected_x_token`.
pub async fn fetch_velar_pool(
    client: Arc<StacksRpcClient>,
    pool_contract: &Principal,
    core_contract: &Principal,
    expected_x_token: &Principal,
    expected_y_token: &Principal,
    token_info: &dyn TokenInfo,
    tip: Option<&str>,
) -> Result<VelarPool> {
    let (core_deployer, core_name) = principal_parts(core_contract)?;
    let args = vec![
        cv_principal(expected_x_token),
        cv_principal(expected_y_token),
    ];
    // Velar's `univ2-core::lookup-pool` signature is `(optional ...)`, NOT
    // `(response ...)` — call_read returns the bare optional. (We had assumed
    // a response wrapper; verified against live mainnet 2026-05.)
    let outer = client
        .call_read(&core_deployer, &core_name, "lookup-pool", &args, tip)
        .await?;

    // Tolerate either shape: bare optional (current Velar) OR
    // `(ok (some ...))` (some older deployments observed in the POC).
    let inner = match outer {
        ClarityValue::OptionalSome(b) => *b,
        ClarityValue::OptionalNone => {
            return Err(anyhow!(
                "Velar lookup-pool returned none for {} / {}",
                expected_x_token,
                expected_y_token,
            ));
        }
        ClarityValue::ResponseOk(boxed) => match *boxed {
            ClarityValue::OptionalSome(b) => *b,
            ClarityValue::OptionalNone => {
                return Err(anyhow!(
                    "Velar lookup-pool (ok wrapped) returned none for {} / {}",
                    expected_x_token,
                    expected_y_token,
                ));
            }
            other => other,
        },
        other => {
            return Err(anyhow!(
                "Velar lookup-pool: expected optional<tuple>, got {:?}",
                other
            ));
        }
    };

    let flipped = inner.field("flipped")?.as_bool().unwrap_or(false);
    let pool = inner.field("pool")?;
    let reserve0 = pool.field("reserve0")?.as_uint()?;
    let reserve1 = pool.field("reserve1")?.as_uint()?;
    let fee = pool.field("swap-fee")?;
    let fee_num = fee.field("num")?.as_uint()?;
    let fee_den = fee.field("den")?.as_uint()?;
    let lp_token = pool
        .field("lp-token")
        .map_err(|e| anyhow!("Velar lookup-pool missing `pool.lp-token`: {}", e))?
        .as_principal()?
        .clone();

    let (reserve_x, reserve_y) = if flipped {
        (reserve1, reserve0)
    } else {
        (reserve0, reserve1)
    };

    let (x_decimals, y_decimals) = tokio::try_join!(
        token_info.decimals(expected_x_token),
        token_info.decimals(expected_y_token),
    )?;

    Ok(VelarPool {
        pool_contract: pool_contract.clone(),
        core_contract: core_contract.clone(),
        lp_token,
        x_token: expected_x_token.clone(),
        y_token: expected_y_token.clone(),
        x_decimals,
        y_decimals,
        reserve_x,
        reserve_y,
        flipped,
        fee_num,
        fee_den,
        last_tx_id: None,
    })
}

/// Bootstrap an ALEX pool by calling `amm-pool-v2-01::get-pool-details`,
/// which takes the (x_token, y_token, factor) and returns the full pool
/// state. We only support `factor = CONSTANT_PRODUCT_FACTOR` (1e8) — other
/// factors are liquid-staking rebalancers and quoting on them errors.
///
/// The on-chain `(x, y)` ordering may be reversed vs the caller's expectation;
/// we try both orderings and swap on success.
pub async fn fetch_alex_pool(
    client: Arc<StacksRpcClient>,
    amm_contract: &Principal, // SP102....amm-pool-v2-01
    expected_x_token: &Principal,
    expected_y_token: &Principal,
    token_info: &dyn TokenInfo,
    tip: Option<&str>,
) -> Result<AlexPool> {
    let (deployer, name) = principal_parts(amm_contract)?;

    let (raw, swapped) = match call_get_pool_details(
        &client,
        &deployer,
        &name,
        expected_x_token,
        expected_y_token,
        tip,
    )
    .await
    {
        Ok(t) => (t, false),
        Err(_) => {
            let alt = call_get_pool_details(
                &client,
                &deployer,
                &name,
                expected_y_token,
                expected_x_token,
                tip,
            )
            .await?;
            (alt, true)
        }
    };

    let raw_balance_x = raw.field("balance-x")?.as_uint()?;
    let raw_balance_y = raw.field("balance-y")?.as_uint()?;
    let fee_rate_x = raw.field("fee-rate-x")?.as_uint()?;
    let fee_rate_y = raw.field("fee-rate-y")?.as_uint()?;
    let pool_id = raw.field("pool-id")?.as_uint()?;

    let (balance_x, balance_y, fee_rate_x, fee_rate_y) = if swapped {
        (raw_balance_y, raw_balance_x, fee_rate_y, fee_rate_x)
    } else {
        (raw_balance_x, raw_balance_y, fee_rate_x, fee_rate_y)
    };

    let (x_decimals, y_decimals) = tokio::try_join!(
        token_info.decimals(expected_x_token),
        token_info.decimals(expected_y_token),
    )?;

    Ok(AlexPool {
        pool_contract: amm_contract.clone(),
        pool_id,
        x_token: expected_x_token.clone(),
        y_token: expected_y_token.clone(),
        x_decimals,
        y_decimals,
        balance_x,
        balance_y,
        factor: CONSTANT_PRODUCT_FACTOR,
        fee_rate_x,
        fee_rate_y,
        last_tx_id: None,
    })
}

async fn call_get_pool_details(
    client: &StacksRpcClient,
    deployer: &str,
    name: &str,
    left: &Principal,
    right: &Principal,
    tip: Option<&str>,
) -> Result<ClarityValue> {
    let args = vec![
        cv_principal(left),
        cv_principal(right),
        cv_uint(CONSTANT_PRODUCT_FACTOR),
    ];
    let res = client
        .call_read(deployer, name, "get-pool-details", &args, tip)
        .await?
        .unwrap_ok()?;
    // Some Clarity ABIs return optional-some(tuple); strip the optional if so.
    match res {
        ClarityValue::OptionalSome(b) => Ok(*b),
        ClarityValue::OptionalNone => Err(anyhow!("ALEX get-pool-details returned none")),
        other => Ok(other),
    }
}

/// Bootstrap a Bitflow V2 XYK pool by calling `<pool>::get-pool`. Returns the
/// authoritative state including `core-address` (which differs across pools
/// using different core versions), per-direction fees, and `pool-status`.
///
/// The on-chain token ordering is taken AS-IS — Bitflow XYK pools commit to a
/// specific `(x-token, y-token)` order at deployment, so this function does
/// NOT probe orderings (unlike Arkadiko/ALEX). Caller is responsible for
/// passing `expected_x_token` / `expected_y_token` that match the pool's
/// stored order, otherwise we bail with a clear error.
pub async fn fetch_bitflow_xyk_pool(
    client: Arc<StacksRpcClient>,
    pool_contract: &Principal,
    expected_x_token: &Principal,
    expected_y_token: &Principal,
    token_info: &dyn TokenInfo,
    tip: Option<&str>,
) -> Result<BitflowXykPool> {
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
            "BitflowXyk pool {} tokens ({}, {}) don't match expected ({}, {}); \
             pass them in the on-chain order — XYK pools don't auto-flip",
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

    Ok(BitflowXykPool {
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
        pool_status,
        last_tx_id: None,
    })
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
