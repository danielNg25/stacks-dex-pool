//! Hiro RPC helpers for the block-walking event source.
//!
//! Two endpoints:
//!   - `GET /v2/info` — returns chain-tip header. We read `stacks_tip_height`.
//!   - `GET /extended/v2/blocks/{height}/transactions?limit=&offset=` —
//!     full tx objects (with `events` inline). Paginated.
//!
//! The decoded `BlockTransaction` exposes only the fields we need: the
//! `contract_call.contract_id` and the per-tx `events` (Clarity print
//! payloads). Anything else from Hiro's schema is ignored.

use std::time::Duration;

use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::Deserialize;

use crate::codec::clarity::{cv_decode, ClarityValue};
use crate::pool::event::StacksEvent;
use crate::pool::principal::Principal;

/// Fetch the current chain tip's stacks-block height via `/v2/info`.
///
/// Hiro occasionally returns HTML (Cloudflare error pages) on 502/503 —
/// surface the first 200 chars of the body when JSON parse fails so the
/// caller can decide whether to retry.
pub async fn fetch_chain_tip(http: &Client, base_url: &str) -> Result<u64> {
    let url = format!("{}/v2/info", base_url.trim_end_matches('/'));
    let resp = http
        .get(&url)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|e| anyhow!("/v2/info GET error: {}", e))?;
    if resp.status().as_u16() == 429 {
        return Err(anyhow!("429 rate limited on /v2/info"));
    }
    if !resp.status().is_success() {
        return Err(anyhow!("/v2/info HTTP {}", resp.status()));
    }
    let body = resp.text().await?;
    let parsed: serde_json::Value = serde_json::from_str(&body).map_err(|e| {
        let preview: String = body.chars().take(200).collect();
        anyhow!(
            "/v2/info JSON parse error: {} (body preview: {})",
            e,
            preview
        )
    })?;
    parsed
        .get("stacks_tip_height")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("/v2/info missing stacks_tip_height: {}", parsed))
}

/// One transaction inside a block, with the bits we care about: which
/// contract was called, and the print-log events that fired.
///
/// IMPORTANT: Hiro's `/extended/v2/blocks/{h}/transactions` endpoint returns
/// `event_count` but **always returns `events: []`**. Events must be fetched
/// separately via [`fetch_tx_events`] for any tx with `event_count > 0`.
/// `events` on this struct is left empty by [`fetch_block_transactions`]
/// and filled in by the two-stage fetch in [`crate::collector::block_walking_source`].
#[derive(Debug, Clone)]
pub struct BlockTransaction {
    pub tx_id: String,
    /// `Some` only for `tx_type = "contract_call"`. We do not need anything
    /// else — STX transfers, coinbase, etc. emit nothing relevant.
    pub contract_call_target: Option<Principal>,
    /// Number of events on this tx (from `event_count` in the block-list
    /// response). Use this to decide whether to fire the second-stage
    /// `/extended/v1/tx/{tx_id}` call.
    pub event_count: u32,
    /// Decoded events. Empty until populated by [`fetch_tx_events`] — the
    /// block-list endpoint never includes the actual event payloads inline.
    pub events: Vec<StacksEvent>,
}

/// Fetch every transaction at `height`, paginating internally. Returns
/// chronological order (Hiro returns ascending by `tx_index` per page).
pub async fn fetch_block_transactions(
    http: &Client,
    base_url: &str,
    height: u64,
    page_size: u32,
) -> Result<Vec<BlockTransaction>> {
    let mut out = Vec::new();
    let mut offset: u32 = 0;
    let base = base_url.trim_end_matches('/');
    loop {
        let url = format!(
            "{}/extended/v2/blocks/{}/transactions?limit={}&offset={}",
            base, height, page_size, offset
        );
        let resp = http
            .get(&url)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| anyhow!("block-txs GET error (h={}): {}", height, e))?;
        if resp.status().as_u16() == 429 {
            return Err(anyhow!("429 rate limited on block-txs h={}", height));
        }
        if resp.status().as_u16() == 404 {
            // Block not yet indexed — treat as empty so caller can retry next tick.
            return Ok(Vec::new());
        }
        if !resp.status().is_success() {
            return Err(anyhow!("block-txs HTTP {} for h={}", resp.status(), height));
        }
        let body: TxPage = resp.json().await?;
        let total = body.total;
        let got = body.results.len() as u32;
        for raw in body.results {
            out.push(decode_tx(raw));
        }
        if got < page_size || (offset + got) >= total {
            break;
        }
        offset += got;
    }
    Ok(out)
}

// ----------------------- JSON shapes (subset of Hiro) -----------------------

#[derive(Debug, Deserialize)]
struct TxPage {
    #[serde(default)]
    total: u32,
    results: Vec<RawTx>,
}

#[derive(Debug, Deserialize)]
struct RawTx {
    tx_id: String,
    #[serde(default)]
    tx_type: String,
    #[serde(default)]
    contract_call: Option<RawContractCall>,
    /// Always empty in `/extended/v2/blocks/{h}/transactions` responses —
    /// kept for forward-compat with any future Hiro version that fills it.
    #[serde(default)]
    events: Vec<RawEvent>,
    #[serde(default)]
    event_count: u32,
}

#[derive(Debug, Deserialize)]
struct RawContractCall {
    contract_id: String,
}

#[derive(Debug, Deserialize)]
struct RawEvent {
    #[serde(default)]
    event_type: String,
    #[serde(default)]
    event_index: u32,
    #[serde(default)]
    contract_log: Option<RawContractLog>,
}

#[derive(Debug, Deserialize)]
struct RawContractLog {
    contract_id: String,
    topic: String,
    value: Option<RawLogValue>,
}

#[derive(Debug, Deserialize)]
struct RawLogValue {
    hex: Option<String>,
}

fn decode_tx(raw: RawTx) -> BlockTransaction {
    let tx_id = raw.tx_id;

    let contract_call_target = if raw.tx_type == "contract_call" {
        raw.contract_call
            .as_ref()
            .and_then(|c| c.contract_id.parse::<Principal>().ok())
    } else {
        None
    };

    let event_count = raw.event_count;
    let mut events = Vec::new();
    for ev in raw.events {
        if ev.event_type != "smart_contract_log" {
            continue;
        }
        let Some(cl) = ev.contract_log else { continue };
        if cl.topic != "print" {
            continue;
        }
        let Some(hex) = cl.value.and_then(|v| v.hex) else {
            continue;
        };
        let Ok(emitter) = cl.contract_id.parse::<Principal>() else {
            continue;
        };
        let Ok(bytes) = hex_decode(hex.strip_prefix("0x").unwrap_or(&hex)) else {
            continue;
        };
        let Ok((payload, _)) = cv_decode(&bytes, 0) else {
            continue;
        };
        if let Some(decoded) = decode_print_payload(emitter, &tx_id, ev.event_index, &payload) {
            events.push(decoded);
        }
    }

    BlockTransaction {
        tx_id,
        contract_call_target,
        event_count,
        events,
    }
}

/// Fetch the full event list for a single transaction via
/// `/extended/v1/tx/{tx_id}`. Required because the block-list endpoint
/// returns `event_count` but `events: []` — there's no way to get event
/// payloads from a block-level call.
///
/// Returns the decoded `StacksEvent`s; non-print and undecodable events are
/// dropped silently. Errors only on transport/JSON failure.
pub async fn fetch_tx_events(
    http: &Client,
    base_url: &str,
    tx_id: &str,
) -> Result<Vec<StacksEvent>> {
    let url = format!(
        "{}/extended/v1/tx/{}",
        base_url.trim_end_matches('/'),
        tx_id
    );
    let resp = http
        .get(&url)
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .map_err(|e| anyhow!("tx GET error for {}: {}", tx_id, e))?;
    if resp.status().as_u16() == 429 {
        return Err(anyhow!("429 rate limited on tx detail {}", tx_id));
    }
    if !resp.status().is_success() {
        return Err(anyhow!("tx GET HTTP {} for {}", resp.status(), tx_id));
    }
    // The /extended/v1/tx response carries the same `events` array shape
    // (filled in this time). We reuse `RawEvent` decoding.
    #[derive(Deserialize)]
    struct TxDetail {
        #[serde(default)]
        events: Vec<RawEvent>,
    }
    let body: TxDetail = resp.json().await?;
    let mut out = Vec::with_capacity(body.events.len());
    for ev in body.events {
        if ev.event_type != "smart_contract_log" {
            continue;
        }
        let Some(cl) = ev.contract_log else { continue };
        if cl.topic != "print" {
            continue;
        }
        let Some(hex) = cl.value.and_then(|v| v.hex) else {
            continue;
        };
        let Ok(emitter) = cl.contract_id.parse::<Principal>() else {
            continue;
        };
        let Ok(bytes) = hex_decode(hex.strip_prefix("0x").unwrap_or(&hex)) else {
            continue;
        };
        let Ok((payload, _)) = cv_decode(&bytes, 0) else {
            continue;
        };
        if let Some(decoded) = decode_print_payload(emitter, tx_id, ev.event_index, &payload) {
            out.push(decoded);
        }
    }
    Ok(out)
}

fn decode_print_payload(
    emitter: Principal,
    tx_id: &str,
    event_index: u32,
    payload: &ClarityValue,
) -> Option<StacksEvent> {
    let ClarityValue::Tuple(fields) = payload else {
        return None;
    };
    // Most DEX events key on `action` with payload under `data`; Velar's
    // univ2-core uses `op` with the payload flat at top level. Try both.
    let try_field = |key: &str| -> Option<String> {
        match fields.get(key)? {
            ClarityValue::StringAscii(s) | ClarityValue::StringUtf8(s) => Some(s.clone()),
            _ => None,
        }
    };
    let (action, used_op) = if let Some(s) = try_field("action") {
        (s, false)
    } else {
        (try_field("op")?, true)
    };
    let data = if used_op {
        fields.clone()
    } else {
        match fields.get("data") {
            Some(ClarityValue::Tuple(d)) => d.clone(),
            _ => Default::default(),
        }
    };
    Some(StacksEvent {
        emitter,
        tx_id: tx_id.to_string(),
        event_index,
        action,
        data,
    })
}

fn hex_decode(s: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return Err(anyhow!("odd-length hex string"));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        out.push(u8::from_str_radix(&s[i..i + 2], 16)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_print_event_from_tx_json() {
        // A tuple `{ action: "ping", data: { x: u42 } }` encoded as Clarity:
        //   0x0c tuple, 2 fields:
        //     "action"  (string-ascii, 4 bytes, "ping")  → 0x0d 00000004 70696e67
        //     "data"    (tuple, 1 field:
        //                "x" (uint 42) → 0x01 0...0 2a (16 bytes BE))
        //
        // We hand-build this so we don't depend on a live Hiro response.
        let raw = serde_json::json!({
            "total": 1,
            "results": [{
                "tx_id": "0xdeadbeef",
                "tx_type": "contract_call",
                "contract_call": {
                    "contract_id": "SP000000000000000000002Q6VF78.bns"
                },
                "events": [{
                    "event_type": "smart_contract_log",
                    "event_index": 7,
                    "contract_log": {
                        "contract_id": "SP000000000000000000002Q6VF78.bns",
                        "topic": "print",
                        "value": {
                            "hex": clarity_hex_tuple_action_ping_data_x42()
                        }
                    }
                }]
            }]
        });
        let page: TxPage = serde_json::from_value(raw).unwrap();
        assert_eq!(page.results.len(), 1);
        let tx = decode_tx(page.results.into_iter().next().unwrap());
        assert_eq!(tx.tx_id, "0xdeadbeef");
        assert!(tx.contract_call_target.is_some());
        assert_eq!(tx.events.len(), 1);
        let ev = &tx.events[0];
        assert_eq!(ev.event_index, 7);
        assert_eq!(ev.action, "ping");
        assert_eq!(ev.data_uint("x"), Some(42));
    }

    /// Build the Clarity hex for `{ action: "ping", data: { x: u42 } }` using
    /// the public encoder so the test stays robust to any internal change.
    fn clarity_hex_tuple_action_ping_data_x42() -> String {
        use crate::codec::clarity::{cv_encode, ClarityValue};
        use std::collections::BTreeMap;

        let mut data_fields = BTreeMap::new();
        data_fields.insert("x".to_string(), ClarityValue::Uint(42));

        let mut tuple_fields = BTreeMap::new();
        tuple_fields.insert(
            "action".to_string(),
            ClarityValue::StringAscii("ping".to_string()),
        );
        tuple_fields.insert("data".to_string(), ClarityValue::Tuple(data_fields));

        let payload = ClarityValue::Tuple(tuple_fields);
        let bytes = cv_encode(&payload);
        let mut hex = String::with_capacity(2 + bytes.len() * 2);
        hex.push_str("0x");
        for b in bytes {
            hex.push_str(&format!("{:02x}", b));
        }
        hex
    }
}
