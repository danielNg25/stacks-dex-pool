//! Stacks codec — pure-function layer.
//!
//! Default feature, no async deps. The two pieces:
//!   - [`c32`] — Stacks address encoding (SP*/SM* ↔ (version, hash160))
//!   - [`clarity`] — Clarity value encode/decode for every tag we touch
//!
//! Mirrors [`test/stacks_lib.py`] in the POC, byte-for-byte compatible. Any
//! drift is a bug.

pub mod c32;
pub mod clarity;
