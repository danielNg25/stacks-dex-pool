//! Clarity value encode/decode.
//!
//! Covers every tag the Stacks DEXes we touch can emit/return. Byte-for-byte
//! compatible with `test/stacks_lib.py:103-212` — any drift is a bug.
//!
//! Tags supported:
//! ```text
//!   0x00 int             0x01 uint            0x02 buffer
//!   0x03 bool-true       0x04 bool-false
//!   0x05 principal-std   0x06 principal-cont
//!   0x07 (ok …)          0x08 (err …)
//!   0x09 none            0x0a (some …)
//!   0x0b list            0x0c tuple
//!   0x0d string-ascii    0x0e string-utf8
//! ```

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::pool::principal::Principal;

/// A decoded Clarity value. Matches the shape Python's `cv_decode` returns,
/// adapted for Rust's type system.
///
/// Tuples become `BTreeMap<String, ClarityValue>` (ordered insertion via key,
/// matches Python's `dict` semantics for our case where field order doesn't
/// matter — only key-based access does).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ClarityValue {
    Int(i128),
    Uint(u128),
    Buffer(Vec<u8>),
    Bool(bool),
    Principal(Principal),
    /// `(ok inner)` — response-ok.
    ResponseOk(Box<ClarityValue>),
    /// `(err inner)` — response-err.
    ResponseErr(Box<ClarityValue>),
    /// `none`
    OptionalNone,
    /// `(some inner)`
    OptionalSome(Box<ClarityValue>),
    List(Vec<ClarityValue>),
    Tuple(BTreeMap<String, ClarityValue>),
    /// ASCII string.
    StringAscii(String),
    /// UTF-8 string.
    StringUtf8(String),
}

impl ClarityValue {
    /// Convenience: unwrap a response, returning the inner value or an error
    /// with the error payload as a debug string.
    pub fn unwrap_ok(self) -> Result<ClarityValue> {
        match self {
            ClarityValue::ResponseOk(v) => Ok(*v),
            ClarityValue::ResponseErr(e) => Err(anyhow!("response-err: {:?}", e)),
            other => Err(anyhow!("expected response, got {:?}", other)),
        }
    }

    /// Convenience: extract a tuple field by name. Errors if not a tuple or
    /// key is missing.
    pub fn field(&self, key: &str) -> Result<&ClarityValue> {
        match self {
            ClarityValue::Tuple(m) => m
                .get(key)
                .ok_or_else(|| anyhow!("tuple missing key '{}'", key)),
            other => Err(anyhow!(
                "expected tuple to extract '{}', got {:?}",
                key,
                other
            )),
        }
    }

    /// As-uint convenience.
    pub fn as_uint(&self) -> Result<u128> {
        match self {
            ClarityValue::Uint(n) => Ok(*n),
            other => Err(anyhow!("expected uint, got {:?}", other)),
        }
    }

    /// As-int convenience.
    pub fn as_int(&self) -> Result<i128> {
        match self {
            ClarityValue::Int(n) => Ok(*n),
            other => Err(anyhow!("expected int, got {:?}", other)),
        }
    }

    /// As-bool convenience.
    pub fn as_bool(&self) -> Result<bool> {
        match self {
            ClarityValue::Bool(b) => Ok(*b),
            other => Err(anyhow!("expected bool, got {:?}", other)),
        }
    }

    /// As-principal convenience.
    pub fn as_principal(&self) -> Result<&Principal> {
        match self {
            ClarityValue::Principal(p) => Ok(p),
            other => Err(anyhow!("expected principal, got {:?}", other)),
        }
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/// Encode a Clarity value to bytes. Inverse of [`cv_decode`].
pub fn cv_encode(v: &ClarityValue) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    cv_encode_into(v, &mut out);
    out
}

fn cv_encode_into(v: &ClarityValue, out: &mut Vec<u8>) {
    match v {
        ClarityValue::Int(n) => {
            out.push(0x00);
            out.extend_from_slice(&n.to_be_bytes());
        }
        ClarityValue::Uint(n) => {
            out.push(0x01);
            out.extend_from_slice(&n.to_be_bytes());
        }
        ClarityValue::Buffer(b) => {
            out.push(0x02);
            out.extend_from_slice(&(b.len() as u32).to_be_bytes());
            out.extend_from_slice(b);
        }
        ClarityValue::Bool(true) => out.push(0x03),
        ClarityValue::Bool(false) => out.push(0x04),
        ClarityValue::Principal(Principal::Standard { version, hash160 }) => {
            out.push(0x05);
            out.push(*version);
            out.extend_from_slice(hash160);
        }
        ClarityValue::Principal(Principal::Contract {
            version,
            hash160,
            name,
        }) => {
            out.push(0x06);
            out.push(*version);
            out.extend_from_slice(hash160);
            let nb = name.as_bytes();
            out.push(nb.len() as u8);
            out.extend_from_slice(nb);
        }
        ClarityValue::ResponseOk(inner) => {
            out.push(0x07);
            cv_encode_into(inner, out);
        }
        ClarityValue::ResponseErr(inner) => {
            out.push(0x08);
            cv_encode_into(inner, out);
        }
        ClarityValue::OptionalNone => out.push(0x09),
        ClarityValue::OptionalSome(inner) => {
            out.push(0x0a);
            cv_encode_into(inner, out);
        }
        ClarityValue::List(items) => {
            out.push(0x0b);
            out.extend_from_slice(&(items.len() as u32).to_be_bytes());
            for item in items {
                cv_encode_into(item, out);
            }
        }
        ClarityValue::Tuple(fields) => {
            out.push(0x0c);
            out.extend_from_slice(&(fields.len() as u32).to_be_bytes());
            for (name, value) in fields {
                let nb = name.as_bytes();
                out.push(nb.len() as u8);
                out.extend_from_slice(nb);
                cv_encode_into(value, out);
            }
        }
        ClarityValue::StringAscii(s) => {
            out.push(0x0d);
            let b = s.as_bytes();
            out.extend_from_slice(&(b.len() as u32).to_be_bytes());
            out.extend_from_slice(b);
        }
        ClarityValue::StringUtf8(s) => {
            out.push(0x0e);
            let b = s.as_bytes();
            out.extend_from_slice(&(b.len() as u32).to_be_bytes());
            out.extend_from_slice(b);
        }
    }
}

// ---------------------------------------------------------------------------
// Encoding helpers (request arguments)
// ---------------------------------------------------------------------------

/// Encode a uint as a Clarity value (`0x01 || 16-byte big-endian`).
pub fn cv_uint(n: u128) -> Vec<u8> {
    cv_encode(&ClarityValue::Uint(n))
}

/// Encode a signed int as a Clarity value (`0x00 || 16-byte big-endian two's complement`).
pub fn cv_int(n: i128) -> Vec<u8> {
    cv_encode(&ClarityValue::Int(n))
}

/// Encode a bool as a Clarity value.
pub fn cv_bool(b: bool) -> Vec<u8> {
    cv_encode(&ClarityValue::Bool(b))
}

/// Encode a principal as a Clarity value.
pub fn cv_principal(p: &Principal) -> Vec<u8> {
    cv_encode(&ClarityValue::Principal(p.clone()))
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Decode one Clarity value from `buf` starting at `off`. Returns the value
/// and the new offset (past the value's end).
pub fn cv_decode(buf: &[u8], off: usize) -> Result<(ClarityValue, usize)> {
    if off >= buf.len() {
        return Err(anyhow!("CV decode: unexpected EOF at offset {}", off));
    }
    let tag = buf[off];
    let off = off + 1;
    match tag {
        0x00 => {
            // int: 16-byte big-endian signed
            if off + 16 > buf.len() {
                return Err(anyhow!("CV int: short read"));
            }
            let mut a = [0u8; 16];
            a.copy_from_slice(&buf[off..off + 16]);
            Ok((ClarityValue::Int(i128::from_be_bytes(a)), off + 16))
        }
        0x01 => {
            // uint: 16-byte big-endian unsigned
            if off + 16 > buf.len() {
                return Err(anyhow!("CV uint: short read"));
            }
            let mut a = [0u8; 16];
            a.copy_from_slice(&buf[off..off + 16]);
            Ok((ClarityValue::Uint(u128::from_be_bytes(a)), off + 16))
        }
        0x02 => {
            // buffer: 4-byte BE length, then bytes
            if off + 4 > buf.len() {
                return Err(anyhow!("CV buffer: short header"));
            }
            let ln = u32::from_be_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            let start = off + 4;
            if start + ln > buf.len() {
                return Err(anyhow!("CV buffer: short body ({})", ln));
            }
            Ok((
                ClarityValue::Buffer(buf[start..start + ln].to_vec()),
                start + ln,
            ))
        }
        0x03 => Ok((ClarityValue::Bool(true), off)),
        0x04 => Ok((ClarityValue::Bool(false), off)),
        0x05 => {
            // principal standard: 1-byte version, 20-byte hash160
            if off + 21 > buf.len() {
                return Err(anyhow!("CV principal-std: short read"));
            }
            let version = buf[off];
            let mut hash160 = [0u8; 20];
            hash160.copy_from_slice(&buf[off + 1..off + 21]);
            Ok((
                ClarityValue::Principal(Principal::Standard { version, hash160 }),
                off + 21,
            ))
        }
        0x06 => {
            // principal contract: version, hash160, 1-byte name length, name bytes
            if off + 22 > buf.len() {
                return Err(anyhow!("CV principal-contract: short header"));
            }
            let version = buf[off];
            let mut hash160 = [0u8; 20];
            hash160.copy_from_slice(&buf[off + 1..off + 21]);
            let nl = buf[off + 21] as usize;
            let start = off + 22;
            if start + nl > buf.len() {
                return Err(anyhow!("CV principal-contract: short name"));
            }
            let name = std::str::from_utf8(&buf[start..start + nl])
                .map_err(|_| anyhow!("CV principal-contract: name not ASCII/UTF-8"))?
                .to_string();
            Ok((
                ClarityValue::Principal(Principal::Contract {
                    version,
                    hash160,
                    name,
                }),
                start + nl,
            ))
        }
        0x07 => {
            let (inner, n) = cv_decode(buf, off)?;
            Ok((ClarityValue::ResponseOk(Box::new(inner)), n))
        }
        0x08 => {
            let (inner, n) = cv_decode(buf, off)?;
            Ok((ClarityValue::ResponseErr(Box::new(inner)), n))
        }
        0x09 => Ok((ClarityValue::OptionalNone, off)),
        0x0a => {
            let (inner, n) = cv_decode(buf, off)?;
            Ok((ClarityValue::OptionalSome(Box::new(inner)), n))
        }
        0x0b => {
            if off + 4 > buf.len() {
                return Err(anyhow!("CV list: short header"));
            }
            let cnt = u32::from_be_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            let mut o = off + 4;
            let mut items = Vec::with_capacity(cnt);
            for _ in 0..cnt {
                let (v, n) = cv_decode(buf, o)?;
                items.push(v);
                o = n;
            }
            Ok((ClarityValue::List(items), o))
        }
        0x0c => {
            if off + 4 > buf.len() {
                return Err(anyhow!("CV tuple: short header"));
            }
            let cnt = u32::from_be_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            let mut o = off + 4;
            let mut map = BTreeMap::new();
            for _ in 0..cnt {
                if o >= buf.len() {
                    return Err(anyhow!("CV tuple: short field-name length"));
                }
                let nl = buf[o] as usize;
                o += 1;
                if o + nl > buf.len() {
                    return Err(anyhow!("CV tuple: short field name"));
                }
                let name = std::str::from_utf8(&buf[o..o + nl])
                    .map_err(|_| anyhow!("CV tuple: field name not UTF-8"))?
                    .to_string();
                o += nl;
                let (value, n) = cv_decode(buf, o)?;
                map.insert(name, value);
                o = n;
            }
            Ok((ClarityValue::Tuple(map), o))
        }
        0x0d => {
            if off + 4 > buf.len() {
                return Err(anyhow!("CV string-ascii: short header"));
            }
            let ln = u32::from_be_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            let start = off + 4;
            if start + ln > buf.len() {
                return Err(anyhow!("CV string-ascii: short body"));
            }
            let s = std::str::from_utf8(&buf[start..start + ln])
                .map_err(|_| anyhow!("CV string-ascii: not valid ASCII/UTF-8"))?
                .to_string();
            Ok((ClarityValue::StringAscii(s), start + ln))
        }
        0x0e => {
            if off + 4 > buf.len() {
                return Err(anyhow!("CV string-utf8: short header"));
            }
            let ln = u32::from_be_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            let start = off + 4;
            if start + ln > buf.len() {
                return Err(anyhow!("CV string-utf8: short body"));
            }
            let s = std::str::from_utf8(&buf[start..start + ln])
                .map_err(|_| anyhow!("CV string-utf8: not valid UTF-8"))?
                .to_string();
            Ok((ClarityValue::StringUtf8(s), start + ln))
        }
        other => Err(anyhow!(
            "unknown CV tag 0x{:02x} at offset {}",
            other,
            off - 1
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::c32::stacks_addr_decode;

    fn roundtrip(v: ClarityValue) {
        let enc = cv_encode(&v);
        let (dec, off) = cv_decode(&enc, 0).expect("decode");
        assert_eq!(off, enc.len(), "decoder didn't consume all bytes");
        assert_eq!(dec, v, "roundtrip mismatch");
    }

    #[test]
    fn primitives() {
        roundtrip(ClarityValue::Uint(0));
        roundtrip(ClarityValue::Uint(42));
        roundtrip(ClarityValue::Uint(u128::MAX));
        roundtrip(ClarityValue::Int(0));
        roundtrip(ClarityValue::Int(-1));
        roundtrip(ClarityValue::Int(i128::MAX));
        roundtrip(ClarityValue::Int(i128::MIN));
        roundtrip(ClarityValue::Bool(true));
        roundtrip(ClarityValue::Bool(false));
        roundtrip(ClarityValue::OptionalNone);
    }

    #[test]
    fn principals() {
        let (v, h) = stacks_addr_decode("SP000000000000000000002Q6VF78").unwrap();
        roundtrip(ClarityValue::Principal(Principal::Standard {
            version: v,
            hash160: h,
        }));
        roundtrip(ClarityValue::Principal(Principal::Contract {
            version: v,
            hash160: h,
            name: "some-pool-v-1-1".to_string(),
        }));
    }

    #[test]
    fn composites() {
        let v = ClarityValue::Tuple(
            [
                ("x-balance".to_string(), ClarityValue::Uint(1_000_000_000)),
                ("active-bin-id".to_string(), ClarityValue::Int(-40)),
                ("pool-status".to_string(), ClarityValue::Bool(true)),
            ]
            .into_iter()
            .collect(),
        );
        roundtrip(v);

        let r = ClarityValue::ResponseOk(Box::new(ClarityValue::Uint(100)));
        roundtrip(r);

        let list = ClarityValue::List(vec![
            ClarityValue::Uint(1),
            ClarityValue::Uint(2),
            ClarityValue::Uint(3),
        ]);
        roundtrip(list);

        roundtrip(ClarityValue::OptionalSome(Box::new(ClarityValue::Bool(
            true,
        ))));
    }

    #[test]
    fn known_bytes_uint() {
        // 0x01 || 16-byte BE of 100
        let enc = cv_uint(100);
        let expected = {
            let mut v = vec![0x01];
            v.extend_from_slice(&100u128.to_be_bytes());
            v
        };
        assert_eq!(enc, expected);
    }

    #[test]
    fn known_bytes_bool() {
        assert_eq!(cv_bool(true), vec![0x03]);
        assert_eq!(cv_bool(false), vec![0x04]);
    }

    #[test]
    fn helpers() {
        let t = ClarityValue::ResponseOk(Box::new(ClarityValue::Tuple(
            [("x".to_string(), ClarityValue::Uint(7))]
                .into_iter()
                .collect(),
        )));
        let inner = t.unwrap_ok().unwrap();
        let x = inner.field("x").unwrap();
        assert_eq!(x.as_uint().unwrap(), 7);
    }
}
