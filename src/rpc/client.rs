//! `StacksRpcClient` — POST `/v2/contracts/call-read/<addr>/<contract>/<fn>`.
//!
//! Mirrors `test/stacks_lib.py:232-264` with 429-retry. Adds an optional
//! `tip` parameter for historical reads (the `?tip=<index_block_hash>`
//! query-string trick — see `NOTES_bitflow_dlmm.md §10`).

use std::time::Duration;

use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;

use crate::codec::clarity::{cv_decode, ClarityValue};

/// Well-known anchor sender (the burn address). Fine for every read-only call.
pub const ANCHOR_SENDER: &str = "SP000000000000000000002Q6VF78";

/// Default RPC URL — Hiro public. Use the host that supports `?tip=`.
pub const DEFAULT_RPC_URL: &str = "https://api.mainnet.hiro.so";

/// Configuration for `StacksRpcClient`.
#[derive(Debug, Clone)]
pub struct RpcConfig {
    /// Base URL (e.g. `https://api.mainnet.hiro.so` or
    /// `https://node.bitflowapis.finance`).
    pub base_url: String,
    /// Per-request timeout.
    pub timeout: Duration,
    /// Number of retries on 429 (not counting the initial attempt).
    pub max_retries: u32,
    /// Base backoff per retry. Total wait grows linearly: `backoff * (attempt + 1)`.
    pub backoff: Duration,
    /// Sender principal for read-only calls.
    pub sender: String,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_RPC_URL.to_string(),
            timeout: Duration::from_secs(15),
            max_retries: 3,
            backoff: Duration::from_secs(10),
            sender: ANCHOR_SENDER.to_string(),
        }
    }
}

/// Stacks RPC client wrapping the `/v2/contracts/call-read/...` endpoint.
#[derive(Debug, Clone)]
pub struct StacksRpcClient {
    config: RpcConfig,
    http: Client,
}

impl StacksRpcClient {
    pub fn new(config: RpcConfig) -> Result<Self> {
        let http = Client::builder().timeout(config.timeout).build()?;
        Ok(Self { config, http })
    }

    pub fn config(&self) -> &RpcConfig {
        &self.config
    }

    /// POST to `/v2/contracts/call-read/<deployer>/<contract>/<fn>`.
    ///
    /// `args` are pre-encoded Clarity bytes — typically via
    /// [`crate::codec::clarity::cv_uint`] / `cv_int` / `cv_principal` / `cv_bool`.
    ///
    /// `tip` is an optional `index_block_hash` (with or without `0x` prefix)
    /// for a historical read. `None` = current chain tip.
    ///
    /// Returns the decoded Clarity value — typically a `ResponseOk` or
    /// `ResponseErr` (the function-level response); the caller does its own
    /// `.unwrap_ok()` on it.
    pub async fn call_read(
        &self,
        deployer: &str,
        contract_name: &str,
        function: &str,
        args: &[Vec<u8>],
        tip: Option<&str>,
    ) -> Result<ClarityValue> {
        let mut url = format!(
            "{}/v2/contracts/call-read/{}/{}/{}",
            self.config.base_url, deployer, contract_name, function
        );
        if let Some(t) = tip {
            let stripped = t.strip_prefix("0x").unwrap_or(t);
            url.push_str("?tip=");
            url.push_str(stripped);
        }

        let body = json!({
            "sender": self.config.sender,
            "arguments": args.iter().map(|a| format!("0x{}", hex(a))).collect::<Vec<_>>(),
        });

        let mut attempt: u32 = 0;
        loop {
            let resp = self
                .http
                .post(&url)
                .json(&body)
                .send()
                .await
                .map_err(|e| anyhow!("call_read send error: {}", e))?;
            let status = resp.status();
            if status.as_u16() == 429 && attempt < self.config.max_retries {
                let wait = self.config.backoff * (attempt + 1);
                log::warn!(
                    "429 on {}.{} (attempt {}/{}), backing off {:?}",
                    contract_name,
                    function,
                    attempt + 1,
                    self.config.max_retries,
                    wait,
                );
                tokio::time::sleep(wait).await;
                attempt += 1;
                continue;
            }
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(anyhow!("call_read HTTP {} for {}: {}", status, url, text));
            }
            let env: CallReadEnvelope = resp
                .json()
                .await
                .map_err(|e| anyhow!("call_read JSON parse: {}", e))?;
            if !env.okay {
                return Err(anyhow!("call_read returned okay=false: {:?}", env.cause));
            }
            let result = env
                .result
                .ok_or_else(|| anyhow!("call_read: no result field"))?;
            let bytes = hex_decode(result.strip_prefix("0x").unwrap_or(&result))?;
            let (value, _) = cv_decode(&bytes, 0)?;
            return Ok(value);
        }
    }
}

impl StacksRpcClient {
    /// Read a contract's data-var directly. Stacks exposes data-vars at
    /// `GET /v2/data_var/<addr>/<contract>/<var>` (yes, GET — POST returns
    /// 405) returning `{ data: "0x...", proof: "..." }` where `data` is the
    /// hex-encoded Clarity value. No `tip` support on this endpoint on
    /// public Hiro; if needed, query a dedicated node that mirrors it.
    ///
    /// Returns the decoded [`ClarityValue`]. Errors on HTTP non-success,
    /// missing `data` field, or hex/CV decode failure.
    pub async fn data_var(
        &self,
        deployer: &str,
        contract_name: &str,
        var: &str,
    ) -> Result<ClarityValue> {
        let url = format!(
            "{}/v2/data_var/{}/{}/{}",
            self.config.base_url, deployer, contract_name, var
        );
        let mut attempt: u32 = 0;
        loop {
            // Stacks's data-var endpoint is GET (no body); using POST returns
            // 405 "Method Not Allowed".
            let resp = self
                .http
                .get(&url)
                .send()
                .await
                .map_err(|e| anyhow!("data_var send error: {}", e))?;
            let status = resp.status();
            if status.as_u16() == 429 && attempt < self.config.max_retries {
                let wait = self.config.backoff * (attempt + 1);
                log::warn!(
                    "429 on data_var {}.{}.{} (attempt {}/{}), backing off {:?}",
                    contract_name,
                    var,
                    "",
                    attempt + 1,
                    self.config.max_retries,
                    wait,
                );
                tokio::time::sleep(wait).await;
                attempt += 1;
                continue;
            }
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(anyhow!("data_var HTTP {} for {}: {}", status, url, text));
            }
            let env: DataVarEnvelope = resp
                .json()
                .await
                .map_err(|e| anyhow!("data_var JSON parse: {}", e))?;
            let data = env
                .data
                .ok_or_else(|| anyhow!("data_var: response missing `data` field for {}", var))?;
            let bytes = hex_decode(data.strip_prefix("0x").unwrap_or(&data))?;
            let (value, _) = cv_decode(&bytes, 0)?;
            return Ok(value);
        }
    }
}

#[derive(Debug, Deserialize)]
struct CallReadEnvelope {
    okay: bool,
    result: Option<String>,
    cause: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct DataVarEnvelope {
    data: Option<String>,
}

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for &x in b {
        use std::fmt::Write;
        write!(s, "{:02x}", x).unwrap();
    }
    s
}

fn hex_decode(s: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return Err(anyhow!("odd-length hex string"));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        out.push(u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| anyhow!("hex parse: {}", e))?);
    }
    Ok(out)
}
