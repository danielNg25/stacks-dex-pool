//! DLMM pool discovery via the core contract's built-in pool registry.
//!
//! `dlmm-core-v-1-1` exposes two read-only functions that together let us
//! enumerate every pool ever registered:
//!
//! ```text
//! (get-last-pool-id) -> (response uint)
//!     ;; highest assigned pool id; currently u8 on mainnet (2026-05).
//!
//! (get-pool-by-id (id uint)) -> (response (optional
//!     { id: uint,
//!       name: (string-ascii 32),         ;; e.g. "STX-USDCx-LP"
//!       pool-contract: principal,        ;; the pool's address
//!       status: bool,                    ;; live / paused
//!       symbol: (string-ascii 32) }))    ;; e.g. "STX-USDCx-10"
//! ```
//!
//! Walking 1..=last_id and filtering `(some {…})` gives us the live set
//! without hardcoding pool names. Cost is `1 + last_id` RPCs — fan out
//! with [`futures_util::stream::buffer_unordered`] for sub-second wall time.
//!
//! Run once at startup. The returned listings then feed
//! [`super::fetcher::fetch_dlmm_pool`] which does the heavy
//! ~10-30 s bin-state bootstrap per pool.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use futures_util::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};

use crate::codec::c32::c32_encode_address;
use crate::codec::clarity::{cv_uint, ClarityValue};
use crate::pool::principal::Principal;
use crate::rpc::client::StacksRpcClient;

/// One entry from `dlmm-core-v-1-1`'s pool registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlmmPoolListing {
    pub id: u128,
    pub pool_contract: Principal,
    /// e.g. `"STX-USDCx-LP"` — set at pool deployment, primarily for LP
    /// token branding.
    pub name: String,
    /// e.g. `"STX-USDCx-10"` — set at pool deployment, intended as a UI
    /// ticker.
    pub symbol: String,
    /// `false` = the pool has been administratively paused. Callers usually
    /// want to skip these (they'll quote zero anyway).
    pub status: bool,
}

/// Enumerate every pool registered in `dlmm-core-v-1-1`'s registry.
///
/// Returns only entries where `get-pool-by-id` returned `(some …)`; `none`
/// results (removed or never-assigned ids — id=1 is currently `none` on
/// mainnet 2026-05) are silently skipped. Result is sorted by `id` so the
/// output is deterministic across runs even though the RPCs land out of
/// order.
///
/// `parallelism` controls the buffered concurrency of the per-id lookups.
/// 8 is a sensible default; higher only helps if your RPC node has lots
/// of headroom (Bitflow's node does; Hiro public 429s quickly).
/// `tip` optionally pins reads to a specific `index_block_hash` for a
/// consistent registry snapshot.
pub async fn discover_dlmm_pools(
    client: Arc<StacksRpcClient>,
    core_contract: &Principal,
    parallelism: usize,
    tip: Option<&str>,
) -> Result<Vec<DlmmPoolListing>> {
    let (deployer, name) = principal_parts(core_contract)?;

    // 1. last assigned pool id.
    let last_id = client
        .call_read(&deployer, &name, "get-last-pool-id", &[], tip)
        .await?
        .unwrap_ok()?
        .as_uint()?;
    if last_id == 0 {
        return Ok(Vec::new());
    }

    // 2. fan out get-pool-by-id(1..=last_id) with bounded concurrency.
    //    We capture all inputs by value because the futures may outlive
    //    the borrow on `tip`.
    let tip_owned: Option<String> = tip.map(str::to_string);
    let listings: Vec<Option<DlmmPoolListing>> = stream::iter(1u128..=last_id)
        .map(|id| {
            let client = client.clone();
            let deployer = deployer.clone();
            let name = name.clone();
            let tip = tip_owned.clone();
            async move {
                let res = client
                    .call_read(
                        &deployer,
                        &name,
                        "get-pool-by-id",
                        &[cv_uint(id)],
                        tip.as_deref(),
                    )
                    .await
                    .ok()?
                    .unwrap_ok()
                    .ok()?;
                decode_listing(res)
            }
        })
        .buffer_unordered(parallelism.max(1))
        .collect()
        .await;

    let mut out: Vec<DlmmPoolListing> = listings.into_iter().flatten().collect();
    out.sort_by_key(|l| l.id);
    Ok(out)
}

/// Decode a single `get-pool-by-id` response into [`DlmmPoolListing`].
/// Returns `None` for `(none)` or any malformed payload.
fn decode_listing(cv: ClarityValue) -> Option<DlmmPoolListing> {
    let inner = match cv {
        ClarityValue::OptionalSome(b) => *b,
        _ => return None,
    };
    let fields = match inner {
        ClarityValue::Tuple(t) => t,
        _ => return None,
    };
    let id = fields.get("id")?.as_uint().ok()?;
    let pool_contract = match fields.get("pool-contract")? {
        ClarityValue::Principal(p) => p.clone(),
        _ => return None,
    };
    let name = match fields.get("name")? {
        ClarityValue::StringAscii(s) | ClarityValue::StringUtf8(s) => s.clone(),
        _ => return None,
    };
    let symbol = match fields.get("symbol")? {
        ClarityValue::StringAscii(s) | ClarityValue::StringUtf8(s) => s.clone(),
        _ => return None,
    };
    let status = fields.get("status")?.as_bool().ok()?;
    Some(DlmmPoolListing {
        id,
        pool_contract,
        name,
        symbol,
        status,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// A full `(some {...})` payload decodes to the expected listing.
    #[test]
    fn decode_listing_full_some() {
        let mut t = BTreeMap::new();
        t.insert("id".into(), ClarityValue::Uint(13));
        t.insert(
            "name".into(),
            ClarityValue::StringAscii("STX-USDCx-LP".into()),
        );
        t.insert(
            "pool-contract".into(),
            ClarityValue::Principal(
                "SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD.dlmm-pool-stx-usdcx-v-1-bps-10"
                    .parse()
                    .unwrap(),
            ),
        );
        t.insert("status".into(), ClarityValue::Bool(true));
        t.insert(
            "symbol".into(),
            ClarityValue::StringAscii("STX-USDCx-10".into()),
        );
        let cv = ClarityValue::OptionalSome(Box::new(ClarityValue::Tuple(t)));
        let listing = decode_listing(cv).unwrap();
        assert_eq!(listing.id, 13);
        assert_eq!(listing.name, "STX-USDCx-LP");
        assert_eq!(listing.symbol, "STX-USDCx-10");
        assert!(listing.status);
        assert_eq!(
            listing.pool_contract.to_string(),
            "SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD.dlmm-pool-stx-usdcx-v-1-bps-10"
        );
    }

    /// `(none)` returns no listing — id was never assigned or pool was removed.
    #[test]
    fn decode_listing_none_skipped() {
        assert!(decode_listing(ClarityValue::OptionalNone).is_none());
    }

    /// Missing required fields → silently skip (don't crash on malformed
    /// registry entries).
    #[test]
    fn decode_listing_missing_fields_skipped() {
        let mut t = BTreeMap::new();
        t.insert("id".into(), ClarityValue::Uint(1));
        // missing pool-contract, name, symbol, status
        let cv = ClarityValue::OptionalSome(Box::new(ClarityValue::Tuple(t)));
        assert!(decode_listing(cv).is_none());
    }

    /// Paused pools come through with `status=false` so the caller can
    /// decide whether to skip them.
    #[test]
    fn decode_listing_paused_pool_status_false() {
        let mut t = BTreeMap::new();
        t.insert("id".into(), ClarityValue::Uint(2));
        t.insert("name".into(), ClarityValue::StringAscii("X-Y-LP".into()));
        t.insert(
            "pool-contract".into(),
            ClarityValue::Principal(
                "SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD.dlmm-pool-stx-usdcx-v-1-bps-1"
                    .parse()
                    .unwrap(),
            ),
        );
        t.insert("status".into(), ClarityValue::Bool(false));
        t.insert("symbol".into(), ClarityValue::StringAscii("X-Y-1".into()));
        let cv = ClarityValue::OptionalSome(Box::new(ClarityValue::Tuple(t)));
        let listing = decode_listing(cv).unwrap();
        assert!(!listing.status);
    }

    /// `string-utf8` field also decodes (some pool versions may use it).
    #[test]
    fn decode_listing_accepts_utf8_strings() {
        let mut t = BTreeMap::new();
        t.insert("id".into(), ClarityValue::Uint(3));
        t.insert("name".into(), ClarityValue::StringUtf8("X-Y-LP".into()));
        t.insert(
            "pool-contract".into(),
            ClarityValue::Principal(
                "SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD.dlmm-pool-stx-usdcx-v-1-bps-1"
                    .parse()
                    .unwrap(),
            ),
        );
        t.insert("status".into(), ClarityValue::Bool(true));
        t.insert("symbol".into(), ClarityValue::StringUtf8("X-Y-1".into()));
        let cv = ClarityValue::OptionalSome(Box::new(ClarityValue::Tuple(t)));
        let listing = decode_listing(cv).unwrap();
        assert_eq!(listing.name, "X-Y-LP");
    }
}
