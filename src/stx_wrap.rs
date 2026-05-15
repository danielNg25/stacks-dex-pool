//! STX wrap canonicalisation at the venue boundary.
//!
//! Stacks' native STX has no SIP-010 contract; Clarity references it via the
//! `stx-transfer?` builtin. Every DEX that needs STX in a pool wraps it
//! locally, and they all use DIFFERENT wrap contracts:
//!
//! | Venue                          | STX appears as                                            |
//! |--------------------------------|-----------------------------------------------------------|
//! | Bitflow V1 stableswap, DLMM    | native STX (no SIP-010 wrap)                              |
//! | Bitflow V2 XYK + StableSwap    | `SM1793…HCCR.token-stx-v-1-2` (6-dp)                      |
//! | ALEX                           | `SP102…0AM.token-wstx-v2` (8-dp internally)               |
//! | Velar                          | `SP1Y5Y…MY4A1.wstx` (6-dp)                                |
//! | Arkadiko                       | `SP2C2YFP…YZR.wrapped-stx-token` (6-dp)                   |
//!
//! Higher-level callers see STX as the canonical asset `"STX"` regardless
//! of venue. Each pool's bootstrap shim runs the input through the
//! matching `for_<dex>` function before constructing the contract-principal
//! Clarity value.
//!
//! Direct port of [arbitrage-rs/crates/stacks/src/stx_wrap.rs].

/// The Bitflow V2 STX wrap principal (XYK + StableSwap V2 pools).
pub const BITFLOW_V2_STX: &str = "SM1793C4R5PZ4NS4VQ4WMP7SKKYVH8JZEWSZ9HCCR.token-stx-v-1-2";

/// ALEX's STX wrap (v2-era pools).
pub const ALEX_STX_V2: &str = "SP102V8P0F7JX67ARQ77WEA3D3CFB5XW39REDT0AM.token-wstx-v2";

/// Legacy ALEX wstx (v1-era pools — kept for completeness).
pub const ALEX_STX_V1: &str = "SP102V8P0F7JX67ARQ77WEA3D3CFB5XW39REDT0AM.token-wstx";

/// Velar's STX wrap, deployed at Velar's own deployer.
pub const VELAR_STX: &str = "SP1Y5YSTAHZ88XYK1VPDH24GY0HPX5J4JECTMY4A1.wstx";

/// Arkadiko's STX wrap, lives at the Arkadiko deployer.
pub const ARKADIKO_STX: &str = "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.wrapped-stx-token";

/// `"STX"` (case-insensitive) → Bitflow V2 wrap; otherwise unchanged.
pub fn for_bitflow_v2(principal: &str) -> &str {
    if is_native_stx(principal) {
        BITFLOW_V2_STX
    } else {
        principal
    }
}

/// `"STX"` (case-insensitive) → ALEX v2 wrap; otherwise unchanged.
pub fn for_alex_v2(principal: &str) -> &str {
    if is_native_stx(principal) {
        ALEX_STX_V2
    } else {
        principal
    }
}

/// `"STX"` (case-insensitive) → Velar wrap; otherwise unchanged.
pub fn for_velar(principal: &str) -> &str {
    if is_native_stx(principal) {
        VELAR_STX
    } else {
        principal
    }
}

/// `"STX"` (case-insensitive) → Arkadiko wrap; otherwise unchanged.
pub fn for_arkadiko(principal: &str) -> &str {
    if is_native_stx(principal) {
        ARKADIKO_STX
    } else {
        principal
    }
}

/// Recognise the canonical native-STX sentinel. Case-insensitive so user
/// configs can write `"stx"`, `"STX"`, `"Stx"` interchangeably.
pub fn is_native_stx(principal: &str) -> bool {
    principal.eq_ignore_ascii_case("STX")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_stx_maps_per_venue() {
        assert_eq!(for_bitflow_v2("STX"), BITFLOW_V2_STX);
        assert_eq!(for_alex_v2("stx"), ALEX_STX_V2);
        assert_eq!(for_velar("STX"), VELAR_STX);
        assert_eq!(for_arkadiko("Stx"), ARKADIKO_STX);
    }

    #[test]
    fn non_stx_passes_through() {
        let aeusdc = "SP3Y2ZSH8P7D50B0VBTJ11QZ7GYJYNZCJ46Z58P4Y2.token-aeusdc";
        assert_eq!(for_bitflow_v2(aeusdc), aeusdc);
        assert_eq!(for_alex_v2(aeusdc), aeusdc);
        assert_eq!(for_velar(aeusdc), aeusdc);
        assert_eq!(for_arkadiko(aeusdc), aeusdc);
    }

    #[test]
    fn is_native_stx_case_insensitive() {
        assert!(is_native_stx("STX"));
        assert!(is_native_stx("stx"));
        assert!(is_native_stx("Stx"));
        assert!(!is_native_stx("STX.token"));
        assert!(!is_native_stx("SP1.foo"));
    }
}
