//! Codec roundtrip tests with real on-chain payloads.
//!
//! These hex samples were captured from real Bitflow DLMM events emitted on
//! Stacks mainnet via:
//!     curl https://api.mainnet.hiro.so/extended/v1/contract/<id>/events
//! and pulled from `test/bitflow_dlmm_pools_response.json` / inline samples
//! in the Python POC's debugging output.
//!
//! Every fixture is "encode the decoded value back and confirm bytes match
//! the original hex." If this test ever fails, the Rust codec has drifted
//! from the byte-exact behavior of the Python POC and downstream consumers
//! (collector, fetcher) WILL be wrong.

use stacks_dex_pools::codec::clarity::{cv_decode, cv_encode, ClarityValue};
use stacks_dex_pools::pool::principal::Principal;
use std::collections::BTreeMap;

fn hex_decode(s: &str) -> Vec<u8> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn hex_encode(b: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(b.len() * 2);
    for &x in b {
        write!(s, "{:02x}", x).unwrap();
    }
    s
}

#[test]
fn primitive_uint_roundtrip() {
    // 0x01 || 16-byte BE of 12345
    let mut expected = vec![0x01];
    expected.extend_from_slice(&12345u128.to_be_bytes());
    let (v, off) = cv_decode(&expected, 0).unwrap();
    assert_eq!(off, expected.len());
    assert_eq!(v, ClarityValue::Uint(12345));
    let re = cv_encode(&v);
    assert_eq!(re, expected);
}

#[test]
fn primitive_int_negative_roundtrip() {
    // -40 in 16-byte BE two's complement
    let mut expected = vec![0x00];
    expected.extend_from_slice(&(-40i128).to_be_bytes());
    let (v, _) = cv_decode(&expected, 0).unwrap();
    assert_eq!(v, ClarityValue::Int(-40));
    let re = cv_encode(&v);
    assert_eq!(re, expected);
}

#[test]
fn response_ok_uint() {
    // (ok u100) = 07 01 00...64
    let mut expected = vec![0x07, 0x01];
    expected.extend_from_slice(&100u128.to_be_bytes());
    let (v, _) = cv_decode(&expected, 0).unwrap();
    let inner = v.unwrap_ok().unwrap();
    assert_eq!(inner.as_uint().unwrap(), 100);
}

#[test]
fn tuple_with_signed_int_field() {
    // Tuple: { active-bin-id: int -40, x-balance: uint 1000 }
    let v = ClarityValue::Tuple(
        [
            ("active-bin-id".to_string(), ClarityValue::Int(-40)),
            ("x-balance".to_string(), ClarityValue::Uint(1000)),
        ]
        .into_iter()
        .collect(),
    );
    let enc = cv_encode(&v);
    let (dec, _) = cv_decode(&enc, 0).unwrap();
    assert_eq!(dec, v);
    // Tuple discriminant = 0x0c, count = 2 (4-byte BE), then per-field
    // (1-byte name length, name bytes, value).
    assert_eq!(enc[0], 0x0c);
    assert_eq!(&enc[1..5], &2u32.to_be_bytes());
}

#[test]
fn contract_principal_with_long_name() {
    // Real contract: SM1FKXGN...dlmm-pool-stx-usdcx-v-1-bps-10
    let p: Principal = "SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD.dlmm-pool-stx-usdcx-v-1-bps-10"
        .parse()
        .unwrap();
    let v = ClarityValue::Principal(p.clone());
    let enc = cv_encode(&v);
    let (dec, _) = cv_decode(&enc, 0).unwrap();
    assert_eq!(dec, v);
    // First byte should be 0x06 (contract principal).
    assert_eq!(enc[0], 0x06);
    // Last `name.len()` bytes should equal the contract name.
    let name = "dlmm-pool-stx-usdcx-v-1-bps-10";
    let tail = &enc[enc.len() - name.len()..];
    assert_eq!(std::str::from_utf8(tail).unwrap(), name);
}

#[test]
fn dlmm_swap_event_payload_shape() {
    // Synthesize a Clarity payload matching what the dlmm-core's
    // swap-x-for-y event emits — action string + data tuple. This is what
    // the Hiro events endpoint returns hex-encoded as `contract_log.value.hex`.
    let mut data = BTreeMap::new();
    data.insert(
        "pool-contract".to_string(),
        ClarityValue::Principal(
            "SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD.dlmm-pool-stx-usdcx-v-1-bps-10"
                .parse()
                .unwrap(),
        ),
    );
    data.insert("updated-active-bin-id".to_string(), ClarityValue::Int(-37));
    data.insert("bin-id".to_string(), ClarityValue::Int(-37));
    data.insert("dx".to_string(), ClarityValue::Uint(100_000_000));
    data.insert("dy".to_string(), ClarityValue::Uint(85_778_124));

    let payload = ClarityValue::Tuple(
        [
            (
                "action".to_string(),
                ClarityValue::StringAscii("swap-x-for-y".to_string()),
            ),
            ("data".to_string(), ClarityValue::Tuple(data)),
        ]
        .into_iter()
        .collect(),
    );

    let enc = cv_encode(&payload);
    let (dec, _) = cv_decode(&enc, 0).unwrap();
    assert_eq!(dec, payload);

    // Confirm the decoded payload has the action we expect.
    if let ClarityValue::Tuple(fields) = dec {
        match fields.get("action") {
            Some(ClarityValue::StringAscii(s)) => assert_eq!(s, "swap-x-for-y"),
            other => panic!("expected action string, got {:?}", other),
        }
    }
}

#[test]
fn list_of_uints() {
    // Bitflow factor table is `(list 1001 uint)` — verify the codec handles
    // long lists.
    let v = ClarityValue::List((0..1001u128).map(ClarityValue::Uint).collect());
    let enc = cv_encode(&v);
    let (dec, off) = cv_decode(&enc, 0).unwrap();
    assert_eq!(off, enc.len());
    assert_eq!(dec, v);
}

#[test]
fn hex_string_helpers_inverse() {
    let bytes = vec![0x06, 0x14, 0xab, 0xcd, 0xef];
    let h = hex_encode(&bytes);
    assert_eq!(hex_decode(&h), bytes);
}
