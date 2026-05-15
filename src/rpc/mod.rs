//! HTTP RPC clients for Stacks (gated by `rpc` feature).
//!
//! Three endpoint groups we use:
//!   - `/v2/contracts/call-read/<addr>/<contract>/<fn>` — Clarity call-read
//!     (supports `?tip=<index_block_hash>` for historical reads). Both Hiro
//!     and Bitflow's node implement this. See [`client`].
//!   - `/extended/v1/contract/<id>/events` — paginated event stream that
//!     returns event payloads inline. See [`events`].
//!   - `/v2/info` + `/extended/v2/blocks/{h}/transactions` + per-tx
//!     `/extended/v1/tx/{tx_id}` — the block-walking ingestion path. Behind
//!     the `block_walking` feature; see [`block_walker`].
//!
//! All wrapped with 429-retry. Hiro public's free-tier limit is ~50 req/min;
//! Bitflow's node has no public rate-limit and mirrors all the same surfaces.

#[cfg(feature = "block_walking")]
pub mod block_walker;
pub mod client;
pub mod events;
