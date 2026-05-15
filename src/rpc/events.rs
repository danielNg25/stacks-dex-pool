//! Hiro events endpoint client.
//!
//! `GET /extended/v1/contract/<id>/events?limit=50&offset=N` returns events
//! newest-first. Each event has `tx_id`, `event_index`, `event_type`, and
//! `contract_log.value.hex` (Clarity-encoded payload — we decode it). No
//! `block_height` is returned inline; for block ordering use
//! [`fetch_tx_block_height`].
//!
//! This module is Hiro-specific (Bitflow's node doesn't expose `/extended/v1/...`).

use std::time::Duration;

use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::Deserialize;

use crate::codec::clarity::{cv_decode, ClarityValue};
use crate::pool::event::StacksEvent;
use crate::pool::principal::Principal;

/// Default Hiro public endpoint.
pub const DEFAULT_EVENTS_URL: &str = "https://api.mainnet.hiro.so";

/// Wrapper around a Hiro event response. Includes the raw payload plus a
/// decoded `StacksEvent` if the payload is a print-event tuple with `action`
/// and `data` fields (which our pools all use).
#[derive(Debug, Clone)]
pub struct EventEnvelope {
    pub tx_id: String,
    pub event_index: u32,
    pub raw_payload: Option<ClarityValue>,
    /// `Some` iff `raw_payload` is a tuple with `action` + `data` fields.
    pub decoded: Option<StacksEvent>,
}

/// Fetch one page of events for `contract_id` (the `address.name` form).
///
/// Returns the decoded events newest-first. Caller is responsible for
/// reversing to apply chronologically.
pub async fn fetch_events_page(
    http: &Client,
    base_url: &str,
    contract_id: &str,
    limit: u32,
    offset: u32,
) -> Result<Vec<EventEnvelope>> {
    let url = format!(
        "{}/extended/v1/contract/{}/events?limit={}&offset={}",
        base_url, contract_id, limit, offset
    );
    let resp = http
        .get(&url)
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .map_err(|e| anyhow!("events GET error: {}", e))?;
    if resp.status().as_u16() == 429 {
        return Err(anyhow!("429 rate limited"));
    }
    if !resp.status().is_success() {
        return Err(anyhow!(
            "events GET HTTP {} for {}",
            resp.status(),
            contract_id
        ));
    }
    let body: PageResponse = resp.json().await?;
    let emitter: Principal = contract_id
        .parse()
        .map_err(|e| anyhow!("invalid emitter contract id {}: {}", contract_id, e))?;
    let mut out = Vec::with_capacity(body.results.len());
    for r in body.results {
        let env = parse_envelope(&emitter, r)?;
        out.push(env);
    }
    Ok(out)
}

/// Look up a tx's `block_height` via `/extended/v1/tx/<tx_id>`. Used by the
/// collector / verifier when it needs to know whether a given event is
/// inside/outside a target block window. Hiro doesn't include `block_height`
/// inline on events.
pub async fn fetch_tx_block_height(http: &Client, base_url: &str, tx_id: &str) -> Result<u64> {
    let url = format!("{}/extended/v1/tx/{}", base_url, tx_id);
    let resp = http
        .get(&url)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|e| anyhow!("tx GET error: {}", e))?;
    if resp.status().as_u16() == 429 {
        return Err(anyhow!("429 rate limited"));
    }
    if !resp.status().is_success() {
        return Err(anyhow!("tx GET HTTP {} for {}", resp.status(), tx_id));
    }
    let body: serde_json::Value = resp.json().await?;
    body.get("block_height")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("tx response missing block_height: {}", body))
}

// ------ JSON shapes ------

#[derive(Debug, Deserialize)]
struct PageResponse {
    results: Vec<RawEvent>,
    // limit / offset / total are also present but we don't use them.
}

#[derive(Debug, Deserialize)]
struct RawEvent {
    tx_id: String,
    event_index: u32,
    contract_log: Option<ContractLog>,
}

#[derive(Debug, Deserialize)]
struct ContractLog {
    topic: String,
    value: Option<LogValue>,
}

#[derive(Debug, Deserialize)]
struct LogValue {
    hex: Option<String>,
}

fn parse_envelope(emitter: &Principal, r: RawEvent) -> Result<EventEnvelope> {
    let tx_id = r.tx_id;
    let event_index = r.event_index;
    let Some(cl) = r.contract_log else {
        return Ok(EventEnvelope {
            tx_id,
            event_index,
            raw_payload: None,
            decoded: None,
        });
    };
    // We only care about `print` topic events; ignore other contract logs.
    if cl.topic != "print" {
        return Ok(EventEnvelope {
            tx_id,
            event_index,
            raw_payload: None,
            decoded: None,
        });
    }
    let Some(hex) = cl.value.and_then(|v| v.hex) else {
        return Ok(EventEnvelope {
            tx_id,
            event_index,
            raw_payload: None,
            decoded: None,
        });
    };
    let bytes = hex_decode(hex.strip_prefix("0x").unwrap_or(&hex))?;
    let (payload, _) = cv_decode(&bytes, 0)?;
    let decoded = decode_print_payload(emitter.clone(), &tx_id, event_index, &payload);
    Ok(EventEnvelope {
        tx_id,
        event_index,
        raw_payload: Some(payload),
        decoded,
    })
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
    // Action — most DEX events use `action`, but Velar (univ2-core) emits
    // events keyed by `op` instead with the whole payload at top level rather
    // than nested under `data`. Accept either.
    let (action, used_op) = read_action_or_op(fields)?;
    // Data — for `action`-style events the payload is nested under `data`;
    // for `op`-style (Velar) every top-level field IS the data.
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

/// Returns `(action, used_op)` where `used_op == true` means we read the
/// action from the `op` field (Velar) and the entire top-level tuple is the
/// payload; `false` means we read `action` and the data is nested under `data`.
fn read_action_or_op(
    fields: &std::collections::BTreeMap<String, ClarityValue>,
) -> Option<(String, bool)> {
    let try_field = |key: &str| -> Option<String> {
        match fields.get(key)? {
            ClarityValue::StringAscii(s) | ClarityValue::StringUtf8(s) => Some(s.clone()),
            _ => None,
        }
    };
    if let Some(s) = try_field("action") {
        Some((s, false))
    } else {
        try_field("op").map(|s| (s, true))
    }
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
