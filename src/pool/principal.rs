//! Stacks `Principal` — either a standard address (user) or a contract
//! principal (contract reference).
//!
//! Encoding matches Clarity CV tags 0x05 and 0x06. Display form is the
//! human-readable `SP...` (standard) or `SP....contract-name` (contract).

use std::fmt;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::codec::c32::{c32_encode_address, stacks_addr_decode};

/// A Stacks principal. Either a user address (Standard) or a contract reference
/// (Contract). Both carry the same 21-byte (version, hash160) base; contract
/// principals add an ASCII contract name (max 128 bytes per Clarity).
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Principal {
    Standard {
        version: u8,
        #[serde(with = "hex_bytes_20")]
        hash160: [u8; 20],
    },
    Contract {
        version: u8,
        #[serde(with = "hex_bytes_20")]
        hash160: [u8; 20],
        name: String,
    },
}

impl Principal {
    /// True iff this is a contract reference with `contract_name == name`.
    pub fn is_contract_named(&self, name: &str) -> bool {
        matches!(self, Principal::Contract { name: n, .. } if n == name)
    }

    /// Returns the contract name, or None for standard principals.
    pub fn contract_name(&self) -> Option<&str> {
        match self {
            Principal::Contract { name, .. } => Some(name.as_str()),
            Principal::Standard { .. } => None,
        }
    }

    /// Returns the underlying (version, hash160) regardless of variant.
    pub fn address(&self) -> (u8, &[u8; 20]) {
        match self {
            Principal::Standard { version, hash160 } => (*version, hash160),
            Principal::Contract {
                version, hash160, ..
            } => (*version, hash160),
        }
    }

    /// Construct a contract principal from an address string + contract name.
    /// Example: `Principal::contract("SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR", "xyk-core-v-1-2")`.
    pub fn contract(addr: &str, name: &str) -> Result<Self> {
        let (version, hash160) = stacks_addr_decode(addr)?;
        if name.len() > 128 {
            return Err(anyhow!("contract name too long: {} bytes", name.len()));
        }
        if !name.is_ascii() {
            return Err(anyhow!("contract name must be ASCII: {:?}", name));
        }
        Ok(Principal::Contract {
            version,
            hash160,
            name: name.to_string(),
        })
    }

    /// Construct a standard principal from an address string.
    pub fn standard(addr: &str) -> Result<Self> {
        let (version, hash160) = stacks_addr_decode(addr)?;
        Ok(Principal::Standard { version, hash160 })
    }
}

impl fmt::Display for Principal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (version, hash160) = self.address();
        let addr = c32_encode_address(version, hash160);
        match self {
            Principal::Standard { .. } => write!(f, "{}", addr),
            Principal::Contract { name, .. } => write!(f, "{}.{}", addr, name),
        }
    }
}

impl fmt::Debug for Principal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Use Display form in Debug too — much more readable in logs.
        write!(f, "{}", self)
    }
}

impl FromStr for Principal {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s.split_once('.') {
            Some((addr, name)) => Self::contract(addr, name),
            None => Self::standard(s),
        }
    }
}

// Serde helper to emit a 20-byte hash160 as hex for readability in JSON dumps.
mod hex_bytes_20 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8; 20], s: S) -> Result<S::Ok, S::Error> {
        let mut hex = String::with_capacity(40);
        for &b in bytes {
            use std::fmt::Write;
            write!(&mut hex, "{:02x}", b).unwrap();
        }
        s.serialize_str(&hex)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 20], D::Error> {
        let s = String::deserialize(d)?;
        if s.len() != 40 {
            return Err(serde::de::Error::custom("hash160 must be 40 hex chars"));
        }
        let mut out = [0u8; 20];
        for (i, b) in out.iter_mut().enumerate() {
            *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).map_err(serde::de::Error::custom)?;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_display_roundtrip() {
        let cases = [
            "SP000000000000000000002Q6VF78",
            "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.xyk-core-v-1-2",
            "SP1PFR4V08H1RAZXREBGFFQ59WB739XM8VVGTFSEA.dlmm-core-v-1-1",
        ];
        for s in cases {
            let p: Principal = s.parse().unwrap();
            assert_eq!(p.to_string(), s);
        }
    }

    #[test]
    fn is_contract_named() {
        let p: Principal =
            "SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD.dlmm-pool-stx-usdcx-v-1-bps-10"
                .parse()
                .unwrap();
        assert!(p.is_contract_named("dlmm-pool-stx-usdcx-v-1-bps-10"));
        assert!(!p.is_contract_named("dlmm-pool-stx-usdcx-v-1-bps-1"));
    }

    #[test]
    fn standard_no_contract_name() {
        let p: Principal = "SP000000000000000000002Q6VF78".parse().unwrap();
        assert_eq!(p.contract_name(), None);
        assert!(!p.is_contract_named("anything"));
    }
}
