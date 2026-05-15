//! Stacks c32check address codec.
//!
//! Stacks mainnet addresses look like `SP102V8P0F7JX67ARQ77WEA3D3CFB5XW39REDT0AM`
//! (version 22 = mainnet user) or `SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR`
//! (version 20 = mainnet contract). Format:
//!
//! ```text
//! 'S' + c32(version_byte) + c32check(hash160 || sha256d(version || hash160)[:4])
//! ```
//!
//! The c32 alphabet (`0123456789ABCDEFGHJKMNPQRSTVWXYZ`) is Crockford's base32
//! but with leading zero BYTES encoded as leading "0" CHARS (so `0x00...` becomes
//! `S...0...rest`). Byte-for-byte port of `test/stacks_lib.py:53-95`.

use anyhow::{anyhow, Result};
use sha2::{Digest, Sha256};

const C32: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Decode a c32check-encoded body to exactly `expected_len` bytes.
///
/// Counts leading "0" chars, parses the remainder as base-32, converts to
/// bytes, then left-pads with that many zero bytes. Errors if the decoded
/// length exceeds `expected_len`.
pub fn c32check_decode(s: &str, expected_len: usize) -> Result<Vec<u8>> {
    let bytes = s.as_bytes();
    let mut nzero = 0;
    while nzero < bytes.len() && bytes[nzero] == b'0' {
        nzero += 1;
    }
    // Use a Vec<u8> as a big-endian accumulator. Each c32 digit shifts the
    // existing bytes left by 5 bits and ORs in the new digit.
    let mut acc: Vec<u8> = Vec::with_capacity(expected_len);
    for &ch in &bytes[nzero..] {
        let d = c32_index(ch).ok_or_else(|| anyhow!("invalid c32 char {:?}", ch as char))? as u32;
        // Shift acc left by 5 bits and add d.
        let mut carry: u32 = d;
        for byte in acc.iter_mut().rev() {
            let v = (*byte as u32) << 5 | carry;
            *byte = (v & 0xff) as u8;
            carry = v >> 8;
        }
        while carry > 0 {
            acc.insert(0, (carry & 0xff) as u8);
            carry >>= 8;
        }
    }
    let mut full = vec![0u8; nzero];
    full.extend_from_slice(&acc);
    if full.len() > expected_len {
        return Err(anyhow!(
            "c32 decoded {} bytes, expected {}",
            full.len(),
            expected_len
        ));
    }
    while full.len() < expected_len {
        full.insert(0, 0);
    }
    Ok(full)
}

/// Encode a (version, hash160) pair as a Stacks address string ('S' prefix).
pub fn c32_encode_address(version: u8, hash160: &[u8; 20]) -> String {
    // payload = hash160 || sha256d(version || hash160)[:4]
    let mut pre = vec![version];
    pre.extend_from_slice(hash160);
    let h1 = Sha256::digest(&pre);
    let h2 = Sha256::digest(h1);
    let mut payload = hash160.to_vec();
    payload.extend_from_slice(&h2[..4]);

    // Count leading zero BYTES (encoded as "0" chars).
    let nzero = payload.iter().take_while(|&&b| b == 0).count();

    // Convert payload (big-endian) to base-32 digits.
    let mut n: Vec<u8> = payload[nzero..].to_vec();
    let mut digits = Vec::<u8>::new();
    while n.len() > 1 || n.first().is_some_and(|&b| b != 0) {
        // Divide n by 32, big-endian.
        let mut rem: u32 = 0;
        let mut out = Vec::with_capacity(n.len());
        for &b in &n {
            let cur = (rem << 8) | b as u32;
            out.push((cur / 32) as u8);
            rem = cur % 32;
        }
        // Strip leading zeros from quotient.
        while out.len() > 1 && out[0] == 0 {
            out.remove(0);
        }
        n = out;
        digits.push(C32[rem as usize]);
    }
    digits.reverse();

    let mut s = String::with_capacity(2 + nzero + digits.len());
    s.push('S');
    s.push(C32[version as usize] as char);
    for _ in 0..nzero {
        s.push('0');
    }
    for d in digits {
        s.push(d as char);
    }
    s
}

/// Decode a Stacks address (e.g. "SP102V8...") to (version_byte, hash160).
/// Verifies the embedded checksum and errors on mismatch.
pub fn stacks_addr_decode(addr: &str) -> Result<(u8, [u8; 20])> {
    if !addr.starts_with('S') || addr.len() < 2 {
        return Err(anyhow!("not a Stacks address: {}", addr));
    }
    let body = &addr[1..];
    let version_char = body.as_bytes()[0];
    let version =
        c32_index(version_char).ok_or_else(|| anyhow!("invalid c32 version char: {addr}"))?;
    let payload = c32check_decode(&body[1..], 24)?;
    let mut hash160 = [0u8; 20];
    hash160.copy_from_slice(&payload[..20]);
    let checksum = &payload[20..24];

    let mut pre = vec![version];
    pre.extend_from_slice(&hash160);
    let h1 = Sha256::digest(&pre);
    let h2 = Sha256::digest(h1);
    if checksum != &h2[..4] {
        return Err(anyhow!("checksum mismatch for {addr}"));
    }
    Ok((version, hash160))
}

fn c32_index(ch: u8) -> Option<u8> {
    C32.iter().position(|&c| c == ch).map(|i| i as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Roundtrip a handful of real Stacks mainnet addresses observed in our POC.
    /// Source: principals stored in Bitflow / Velar / ALEX pool tuples.
    #[test]
    fn known_addresses_roundtrip() {
        let cases = [
            // (address, version, expected_hash160_hex)
            "SP000000000000000000002Q6VF78", // burn (anchor sender)
            "SP1Y5YSTAHZ88XYK1VPDH24GY0HPX5J4JECTMY4A1", // velar deployer (SP — v22)
            "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR", // bitflow v2 deployer (SM — v20)
            "SP102V8P0F7JX67ARQ77WEA3D3CFB5XW39REDT0AM", // alex deployer
            "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR", // arkadiko deployer
            "SPQC38PW542EQJ5M11CR25P7BS1CA6QT4TBXGB3M", // bitflow v1 deployer
            "SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD", // dlmm pool deployer
            "SP1PFR4V08H1RAZXREBGFFQ59WB739XM8VVGTFSEA", // dlmm core deployer
        ];
        for addr in cases {
            let (v, h) = stacks_addr_decode(addr).expect(addr);
            let re = c32_encode_address(v, &h);
            assert_eq!(re, addr, "roundtrip failed for {addr}");
        }
    }

    #[test]
    fn rejects_bad_checksum() {
        // Flip the last char to corrupt the checksum.
        let bad = "SP000000000000000000002Q6VF79";
        assert!(stacks_addr_decode(bad).is_err());
    }

    #[test]
    fn rejects_non_s_prefix() {
        assert!(stacks_addr_decode("XP000000000000000000002Q6VF78").is_err());
        assert!(stacks_addr_decode("").is_err());
    }
}
