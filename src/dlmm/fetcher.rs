//! DLMM bootstrap — fetch a pool's full state via Stacks RPC.
//!
//! Three modes:
//!   - [`BootstrapMode::Window`] — ±N bins around active. ~21 parallel
//!     per-bin RPC calls for N=10. Minimum RPC budget; quotes capped at
//!     window edge.
//!   - [`BootstrapMode::Full`] (default) — all 1001 bins via the chunked
//!     multicall helper. ~11 RPC calls (chunks of 100) × ~1s each ≈ 10-14s
//!     per pool. ~3-5× faster than per-bin and quotes are exact at any
//!     size.
//!   - [`BootstrapMode::FullPerBin`] — all 1001 bins via 1001 per-bin
//!     calls. Fallback for environments where the multicall helper is
//!     unreachable. ~45s per pool on Bitflow's node with parallelism=8.
//!
//! Port of [`test/fetch_bitflow_dlmm.py:fetch_pool`]. The factor table for the
//! pool's bin_step is cached lazily via [`crate::dlmm::factor::get_or_fetch`].

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use futures_util::stream::{self, StreamExt};

use super::pool::{BinState, DLMMPool};
use super::{CENTER_BIN_ID, MAX_BIN_ID, MIN_BIN_ID};
use crate::codec::c32::c32_encode_address;
use crate::codec::clarity::{cv_uint, ClarityValue};
use crate::pool::principal::Principal;
use crate::rpc::client::StacksRpcClient;
use crate::token_info::TokenInfo;

/// Which bins to fetch and how.
#[derive(Debug, Clone, Copy, Default)]
pub enum BootstrapMode {
    /// Fetch `2 * radius + 1` bins centered on the active bin (clamped to
    /// the [MIN_BIN_ID, MAX_BIN_ID] range). Per-bin calls in parallel.
    Window { radius: u32 },
    /// All 1001 bins via chunked multicall (~11 chunked calls × 1s each ≈
    /// 10-14s per pool). Default and fastest path. Required for production-
    /// grade quotes at any size. See [`super::multicall`] for the chunk-size
    /// rationale.
    #[default]
    Full,
    /// All 1001 bins via per-bin calls (fallback). Use when the multicall
    /// helper is unreachable or you want to test parity with the older path.
    /// Pair with high parallelism; expect ~45s per pool on Bitflow's node.
    FullPerBin,
}

/// Bootstrap a single DLMM pool from chain.
///
/// `pool_contract` is the pool contract principal. The function reads
/// `get-pool()` for config + active bin, looks up decimals for both tokens,
/// fetches the factor table for the pool's bin step (cached), then bulk-reads
/// `get-bin-balances(unsigned_bin_id)` for every bin in `mode`'s range with
/// configurable parallelism.
pub async fn fetch_dlmm_pool(
    client: Arc<StacksRpcClient>,
    pool_contract: &Principal,
    core_contract: &Principal,
    token_info: &dyn TokenInfo,
    mode: BootstrapMode,
    parallelism: usize,
    tip: Option<&str>,
) -> Result<DLMMPool> {
    let (pool_deployer, pool_name) = principal_parts(pool_contract)?;

    // 1. get-pool for config + active bin + fees.
    let pool_data = client
        .call_read(&pool_deployer, &pool_name, "get-pool", &[], tip)
        .await?
        .unwrap_ok()?;
    let p = match pool_data {
        ClarityValue::Tuple(t) => t,
        other => return Err(anyhow!("get-pool: expected tuple, got {:?}", other)),
    };

    fn read_uint(t: &BTreeMap<String, ClarityValue>, key: &str) -> Result<u128> {
        t.get(key)
            .ok_or_else(|| anyhow!("get-pool missing '{}'", key))?
            .as_uint()
    }
    fn read_int(t: &BTreeMap<String, ClarityValue>, key: &str) -> Result<i128> {
        t.get(key)
            .ok_or_else(|| anyhow!("get-pool missing '{}'", key))?
            .as_int()
    }
    fn read_principal(t: &BTreeMap<String, ClarityValue>, key: &str) -> Result<Principal> {
        match t.get(key) {
            Some(ClarityValue::Principal(p)) => Ok(p.clone()),
            other => Err(anyhow!("get-pool '{}' not a principal: {:?}", key, other)),
        }
    }

    let x_token = read_principal(&p, "x-token")?;
    let y_token = read_principal(&p, "y-token")?;
    let bin_step = u32::try_from(read_uint(&p, "bin-step")?)
        .map_err(|_| anyhow!("bin-step out of u32 range"))?;
    let initial_price = read_uint(&p, "initial-price")?;
    let active_bin_id = i32::try_from(read_int(&p, "active-bin-id")?)
        .map_err(|_| anyhow!("active-bin-id out of i32 range"))?;
    // Fees come either flat at the top level or nested in pool-fees — handle both.
    let read_fee = |key: &str| -> u32 {
        p.get(key)
            .and_then(|v| v.as_uint().ok())
            .or_else(|| {
                p.get("pool-fees").and_then(|v| match v {
                    ClarityValue::Tuple(inner) => inner.get(key)?.as_uint().ok(),
                    _ => None,
                })
            })
            .unwrap_or(0) as u32
    };
    let x_protocol_fee = read_fee("x-protocol-fee");
    let x_provider_fee = read_fee("x-provider-fee");
    let y_protocol_fee = read_fee("y-protocol-fee");
    let y_provider_fee = read_fee("y-provider-fee");
    let x_variable_fee = read_fee("x-variable-fee");
    let y_variable_fee = read_fee("y-variable-fee");

    // 2. Decimals + factor table in parallel.
    let x_decimals_fut = token_info.decimals(&x_token);
    let y_decimals_fut = token_info.decimals(&y_token);
    let factors_fut = super::factor::get_or_fetch(&client, core_contract, bin_step);
    let (x_decimals, y_decimals, factors) =
        tokio::try_join!(x_decimals_fut, y_decimals_fut, factors_fut)?;

    // 3. Fetch bin balances. `Full` takes the multicall fast path (one RPC);
    //    `Window` and `FullPerBin` take the per-bin path with parallelism.
    let bins: BTreeMap<i32, BinState> = match mode {
        BootstrapMode::Full => {
            use super::multicall::{fetch_bin_balances_chunked, MultiHelper};
            let ids: Vec<u128> = (0..1001u128).collect();
            let helper = MultiHelper::default();
            // chunk_size=None → DEFAULT_CHUNK_SIZE (100). ~11 sequential
            // chunked calls instead of 1001 per-bin calls.
            let raw =
                fetch_bin_balances_chunked(client.clone(), &helper, pool_contract, &ids, None, tip)
                    .await?;
            raw.into_iter()
                .filter(|b| b.x > 0 || b.y > 0)
                .map(|b| (b.bin_id_signed, BinState { x: b.x, y: b.y }))
                .collect()
        }
        BootstrapMode::Window { .. } | BootstrapMode::FullPerBin => {
            let (lo, hi) = match mode {
                BootstrapMode::Window { radius } => {
                    let r = radius as i32;
                    (
                        (active_bin_id - r).max(MIN_BIN_ID),
                        (active_bin_id + r).min(MAX_BIN_ID),
                    )
                }
                _ => (MIN_BIN_ID, MAX_BIN_ID),
            };
            stream::iter(lo..=hi)
                .map(|signed| {
                    let client = client.clone();
                    let pool_deployer = pool_deployer.clone();
                    let pool_name = pool_name.clone();
                    let tip = tip.map(str::to_string);
                    async move {
                        let unsigned = (signed + CENTER_BIN_ID) as u128;
                        let args = vec![cv_uint(unsigned)];
                        let result = client
                            .call_read(
                                &pool_deployer,
                                &pool_name,
                                "get-bin-balances",
                                &args,
                                tip.as_deref(),
                            )
                            .await
                            .and_then(|cv| cv.unwrap_ok());
                        let bin = result.and_then(|inner| {
                            let t = match inner {
                                ClarityValue::Tuple(t) => t,
                                other => {
                                    return Err(anyhow!(
                                        "get-bin-balances: expected tuple, got {:?}",
                                        other
                                    ))
                                }
                            };
                            let x = t
                                .get("x-balance")
                                .and_then(|v| v.as_uint().ok())
                                .unwrap_or(0);
                            let y = t
                                .get("y-balance")
                                .and_then(|v| v.as_uint().ok())
                                .unwrap_or(0);
                            Ok((x, y))
                        });
                        (signed, bin)
                    }
                })
                .buffer_unordered(parallelism.max(1))
                .filter_map(|(signed, res)| async move {
                    match res {
                        Ok((x, y)) if x > 0 || y > 0 => Some((signed, BinState { x, y })),
                        _ => None,
                    }
                })
                .collect()
                .await
        }
    };

    Ok(DLMMPool {
        pool_contract: pool_contract.clone(),
        core_contract: core_contract.clone(),
        x_token,
        y_token,
        x_decimals,
        y_decimals,
        bin_step,
        initial_price,
        active_bin_id,
        x_protocol_fee,
        x_provider_fee,
        y_protocol_fee,
        y_provider_fee,
        x_variable_fee,
        y_variable_fee,
        bins,
        last_tx_id: None,
        last_event_at: None,
        factors,
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
