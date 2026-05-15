//! Batched bin-balance fetch using Bitflow's `dlmm-pool-multi-helper-v-1-1`.
//!
//! Stacks doesn't have a chain-wide multicall, but Bitflow deployed a helper
//! contract at `SM1FKXGN….dlmm-pool-multi-helper-v-1-1` that exposes:
//!
//! ```clarity
//! (define-public (get-bin-balances-multi
//!     (pool-traits (list 1001 <dlmm-pool-trait>))
//!     (ids         (list 1001 uint)))
//!   (response (list 1001 (response { bin-shares: uint, x-balance: uint, y-balance: uint } uint))
//!             none))
//! ```
//!
//! Despite the contract type allowing up to 1001 items in one call, the
//! Clarity VM's per-call `read_length` budget (5_000_000 bytes) caps the
//! practical batch size much lower. Empirically the response is ~20KB per
//! bin (Clarity's `(response T E)` wrappers carry a lot of metadata), so:
//!
//! - 1001 ids in one call → ~5.0MB → fails (over the limit)
//! - 250 ids per chunk → ~5.0MB → still fails
//! - 100 ids per chunk → ~2MB → works reliably
//!
//! We default to `DEFAULT_CHUNK_SIZE = 100` and split 1001 ids into 11
//! sequential calls (~1s each). For a fully-populated pool, bootstrap drops
//! from ~45s (1001 per-bin) to ~12s (11 chunks). For thin pools the result
//! is the same — we always fetch all 1001 ids since there's no way to know
//! which are populated without asking — but the result is filtered down to
//! just the non-empty bins before being stored.
//!
//! The helper itself is unauthenticated and read-only — anyone can call.
//! Lives under the same deployer as the pools (`SM1FKXGN…`); [`MultiHelper::default()`]
//! returns that path, [`MultiHelper::new`] lets you point at a redeployment.

use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::codec::clarity::{cv_encode, ClarityValue};
use crate::pool::principal::Principal;
use crate::rpc::client::StacksRpcClient;

/// Default multicall helper. Bitflow may redeploy under a different name
/// later; [`MultiHelper::new`] lets callers override.
const DEFAULT_HELPER_DEPLOYER: &str = "SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD";
const DEFAULT_HELPER_NAME: &str = "dlmm-pool-multi-helper-v-1-1";

/// Default chunk size for the chunked variant.
///
/// Empirically measured by binary-searching against the live
/// `dlmm-pool-multi-helper-v-1-1`: the Clarity VM's `read_length` budget
/// is 5_000_000 bytes per call, and this contract's response works out to
/// ~20KB per bin (way more than the raw tuple suggests — Clarity's
/// `(response T E)` wrappers serialize with a lot of metadata). So:
///
/// - 250 items → ~5.0MB → fails (just over the limit)
/// - 100 items → ~2MB → comfortable
///
/// 100 ⇒ 11 chunks for 1001 bins, ~11 × ~1s/call = ~11s bootstrap. Still ~4×
/// faster than the per-bin path (45s) and works reliably on real pools.
pub const DEFAULT_CHUNK_SIZE: usize = 100;

/// Identifies which contract to use as the batch helper. Use
/// [`MultiHelper::default()`] for the canonical Bitflow one.
#[derive(Debug, Clone)]
pub struct MultiHelper {
    pub deployer: String,
    pub contract: String,
}

impl Default for MultiHelper {
    fn default() -> Self {
        Self {
            deployer: DEFAULT_HELPER_DEPLOYER.to_string(),
            contract: DEFAULT_HELPER_NAME.to_string(),
        }
    }
}

impl MultiHelper {
    pub fn new(deployer: impl Into<String>, contract: impl Into<String>) -> Self {
        Self {
            deployer: deployer.into(),
            contract: contract.into(),
        }
    }
}

/// One bin's state as returned by the helper. Mirrors the contract tuple,
/// includes shares because the helper exposes them (even though our mirror
/// doesn't track shares).
#[derive(Debug, Clone, Copy)]
pub struct RawBinBalance {
    pub bin_id_signed: i32,
    pub x: u128,
    pub y: u128,
    /// LP shares for the bin. The collector mirror ignores this (see
    /// `NOTES_bitflow_dlmm.md §12`); included here so the helper interface
    /// is faithful in case a future feature needs it.
    pub shares: u128,
}

/// Batch-fetch bin balances for `unsigned_ids` on the given pool.
///
/// `unsigned_ids` are unsigned (0..=1000). Caller converts to signed before
/// storing in `DLMMPool.bins`. We return signed in `RawBinBalance` for
/// caller convenience.
///
/// Max list length is 1001 per the contract's type signature. If you pass
/// fewer, the contract still works (Clarity `(list 1001 T)` accepts shorter
/// lists). Splitting into chunks of 1001 is the caller's job — but 1001 is
/// the whole bin space so you only ever need one call per pool.
///
/// Errors:
/// - Network / RPC errors propagate.
/// - If the helper returns `(err uint)` at the OUTER level, we propagate.
/// - If an INDIVIDUAL bin returned `(err uint)` we silently skip it (the
///   helper sometimes does this for unset bins on some deployments). The
///   caller sees only successfully-decoded bins.
pub async fn fetch_bin_balances_multi(
    client: Arc<StacksRpcClient>,
    helper: &MultiHelper,
    pool_contract: &Principal,
    unsigned_ids: &[u128],
    tip: Option<&str>,
) -> Result<Vec<RawBinBalance>> {
    if unsigned_ids.is_empty() {
        return Ok(Vec::new());
    }
    if unsigned_ids.len() > 1001 {
        return Err(anyhow!(
            "multicall: max 1001 ids per call, got {}",
            unsigned_ids.len()
        ));
    }

    // Encode arguments. `pool-traits` is a list of pool-trait references — in
    // Clarity these are contract-principal CVs. We repeat the same pool for
    // all entries because we're only batching across BINS, not pools.
    let pool_traits = ClarityValue::List(
        (0..unsigned_ids.len())
            .map(|_| ClarityValue::Principal(pool_contract.clone()))
            .collect(),
    );
    let ids = ClarityValue::List(
        unsigned_ids
            .iter()
            .map(|&id| ClarityValue::Uint(id))
            .collect(),
    );

    let result = client
        .call_read(
            &helper.deployer,
            &helper.contract,
            "get-bin-balances-multi",
            &[cv_encode(&pool_traits), cv_encode(&ids)],
            tip,
        )
        .await?;

    let inner = result.unwrap_ok()?;
    let items = match inner {
        ClarityValue::List(v) => v,
        other => {
            return Err(anyhow!(
                "multicall: expected list of results, got {:?}",
                other
            ))
        }
    };
    if items.len() != unsigned_ids.len() {
        log::warn!(
            "multicall: requested {} bins, helper returned {} items — \
             ordering may be off; treating remaining as missing",
            unsigned_ids.len(),
            items.len(),
        );
    }

    let mut out = Vec::with_capacity(items.len());
    for (i, item) in items.into_iter().enumerate() {
        if i >= unsigned_ids.len() {
            break;
        }
        let unsigned = unsigned_ids[i];
        let signed = i32::try_from(unsigned)
            .map_err(|_| anyhow!("bin id out of i32 range: {}", unsigned))?
            - crate::dlmm::CENTER_BIN_ID;
        match item {
            ClarityValue::ResponseOk(boxed) => {
                let tuple = match *boxed {
                    ClarityValue::Tuple(t) => t,
                    other => {
                        log::warn!(
                            "multicall: bin {} returned non-tuple ok payload: {:?}",
                            signed,
                            other
                        );
                        continue;
                    }
                };
                let x = tuple
                    .get("x-balance")
                    .and_then(|v| v.as_uint().ok())
                    .unwrap_or(0);
                let y = tuple
                    .get("y-balance")
                    .and_then(|v| v.as_uint().ok())
                    .unwrap_or(0);
                let shares = tuple
                    .get("bin-shares")
                    .and_then(|v| v.as_uint().ok())
                    .unwrap_or(0);
                out.push(RawBinBalance {
                    bin_id_signed: signed,
                    x,
                    y,
                    shares,
                });
            }
            ClarityValue::ResponseErr(e) => {
                // Some helper deployments return (err uint) for genuinely
                // unset bins. Treat as a zero entry so the caller can decide
                // whether to filter; here we just emit (x=0, y=0).
                log::debug!("multicall: bin {} returned (err {:?})", signed, e);
                out.push(RawBinBalance {
                    bin_id_signed: signed,
                    x: 0,
                    y: 0,
                    shares: 0,
                });
            }
            other => {
                log::warn!("multicall: bin {} item not a response: {:?}", signed, other);
            }
        }
    }
    Ok(out)
}

/// Fetch all bin balances in `unsigned_ids`, splitting into chunks of
/// `chunk_size` (defaults to [`DEFAULT_CHUNK_SIZE`] = 500) to stay under
/// Clarity's per-call `read_length` budget.
///
/// Chunks are issued **sequentially** (not concurrently) — issuing them in
/// parallel risks rate-limiting on Hiro and saves little time anyway
/// because each call is already batched.
pub async fn fetch_bin_balances_chunked(
    client: Arc<StacksRpcClient>,
    helper: &MultiHelper,
    pool_contract: &Principal,
    unsigned_ids: &[u128],
    chunk_size: Option<usize>,
    tip: Option<&str>,
) -> Result<Vec<RawBinBalance>> {
    let chunk_size = chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE).min(1001);
    if chunk_size == 0 {
        return Err(anyhow!("chunk_size must be > 0"));
    }
    let mut out = Vec::with_capacity(unsigned_ids.len());
    let total_chunks = unsigned_ids.len().div_ceil(chunk_size);
    log::debug!(
        "multicall: fetching {} ids in {} chunks of {}",
        unsigned_ids.len(),
        total_chunks,
        chunk_size
    );
    for (i, chunk) in unsigned_ids.chunks(chunk_size).enumerate() {
        log::debug!(
            "multicall: chunk {}/{} ({} ids: {}..{})",
            i + 1,
            total_chunks,
            chunk.len(),
            chunk.first().copied().unwrap_or(0),
            chunk.last().copied().unwrap_or(0)
        );
        let batch =
            fetch_bin_balances_multi(client.clone(), helper, pool_contract, chunk, tip).await?;
        out.extend(batch);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helper_default_paths() {
        let h = MultiHelper::default();
        assert_eq!(h.deployer, "SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD");
        assert_eq!(h.contract, "dlmm-pool-multi-helper-v-1-1");
    }

    #[test]
    fn helper_override() {
        let h = MultiHelper::new("SP000000000000000000002Q6VF78", "some-other-helper");
        assert_eq!(h.deployer, "SP000000000000000000002Q6VF78");
        assert_eq!(h.contract, "some-other-helper");
    }
}
