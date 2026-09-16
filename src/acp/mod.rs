//! ACP surface (FORK_PLAN P1–P2): compat/installers plus the stdio server.
//!
//! This module ports the proven `muse-acp` adapter logic (`compat.rs`,
//! `zed.rs`, `acp.rs` session/update mapping) onto this bridge's conventions:
//! serde_json instead of hand-rolled JSON, structured outcomes instead of
//! `println!`, `tokio` throughout so concurrent ACP sessions multiplex on one
//! MSP core, and no global state. Layout:
//!
//! - [`compat`]: schema compatibility verdicts + Zed/JetBrains installers.
//! - [`server`]: stdio JSON-RPC framing, handshake, and method dispatch.
//! - [`sessions`]: ACP session ↔ persistent MSP session + per-prompt turns.
//! - [`errors`]: MSP failure → typed client error mapping (SPEC §6).

pub mod compat;
pub mod errors;
pub mod server;
pub mod sessions;
