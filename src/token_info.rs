//! `TokenInfo` — async resolver for SIP-010 token decimals, with a per-process
//! cache. Mirrors `evm-dex-pool::token_info` in shape but adapted for Stacks
//! principals and the `(define-read-only (get-decimals))` SIP-010 convention.
//!
//! Decimals are static — fetch once, cache forever. The cache is sync (no
//! tokio Mutex needed; we use a single `std::sync::Mutex` and only hold the
//! lock briefly).

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use once_cell::sync::Lazy;

use crate::codec::c32::c32_encode_address;
use crate::codec::clarity::ClarityValue;
use crate::pool::principal::Principal;
use crate::rpc::client::StacksRpcClient;

/// Trait every consumer that wants to look up decimals should accept. Allows
/// substituting a fake cache in tests.
#[async_trait]
pub trait TokenInfo: Send + Sync {
    async fn decimals(&self, token: &Principal) -> Result<u8>;
}

/// Default implementation — calls `<token>.get-decimals()` on first lookup,
/// caches forever.
pub struct StacksTokenInfo {
    client: std::sync::Arc<StacksRpcClient>,
    cache: Lazy<Mutex<HashMap<String, u8>>>,
}

impl std::fmt::Debug for StacksTokenInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.cache.lock().map(|c| c.len()).unwrap_or(0);
        f.debug_struct("StacksTokenInfo")
            .field("cache_size", &n)
            .finish()
    }
}

impl StacksTokenInfo {
    pub fn new(client: std::sync::Arc<StacksRpcClient>) -> Self {
        Self {
            client,
            cache: Lazy::new(|| Mutex::new(HashMap::new())),
        }
    }

    /// Pre-seed the cache with a known decimals value. Useful when you have a
    /// hardcoded table (e.g., STX=6, sBTC=8) and want to avoid the round-trip.
    pub fn insert(&self, token: &Principal, decimals: u8) {
        if let Ok(mut c) = self.cache.lock() {
            c.insert(token.to_string(), decimals);
        }
    }

    fn get_cached(&self, token: &Principal) -> Option<u8> {
        self.cache.lock().ok()?.get(&token.to_string()).copied()
    }
}

#[async_trait]
impl TokenInfo for StacksTokenInfo {
    async fn decimals(&self, token: &Principal) -> Result<u8> {
        if let Some(d) = self.get_cached(token) {
            return Ok(d);
        }
        let (deployer, contract_name) = match token {
            Principal::Contract {
                version,
                hash160,
                name,
            } => (c32_encode_address(*version, hash160), name.clone()),
            _ => {
                return Err(anyhow!(
                    "expected SIP-010 contract principal, got {}",
                    token
                ));
            }
        };
        let result = self
            .client
            .call_read(&deployer, &contract_name, "get-decimals", &[], None)
            .await?;
        let inner = result.unwrap_ok()?;
        let d = match inner {
            ClarityValue::Uint(n) => {
                u8::try_from(n).map_err(|_| anyhow!("decimals value {} out of u8 range", n))?
            }
            other => return Err(anyhow!("expected uint from get-decimals, got {:?}", other)),
        };
        if let Ok(mut c) = self.cache.lock() {
            c.insert(token.to_string(), d);
        }
        Ok(d)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::client::RpcConfig;
    use std::sync::Arc;

    /// Cache pre-seeding works without any RPC call.
    #[tokio::test]
    async fn cache_lookup_short_circuits() {
        let client = Arc::new(StacksRpcClient::new(RpcConfig::default()).unwrap());
        let ti = StacksTokenInfo::new(client);
        let stx: Principal = "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.token-stx-v-1-2"
            .parse()
            .unwrap();
        ti.insert(&stx, 6);
        assert_eq!(ti.decimals(&stx).await.unwrap(), 6);
    }
}
