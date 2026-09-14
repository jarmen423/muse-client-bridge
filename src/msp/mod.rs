//! MSP client over stdio: framing, child supervision, handshake, view fold.
//!
//! See SPEC §4 and AGENTS.md rules 1–13. [`proto`] owns bytes and envelopes;
//! later packets add `spawn` (child), `host` (commands), and `fold` (views).

pub mod fold;
pub mod host;
pub mod proto;
pub mod spawn;
