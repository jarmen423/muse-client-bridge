//! Muse Bridge: localhost OpenAI-compatible HTTP API over a `muse serve` child.
//!
//! See `SPEC.md` for the normative contract. Module layout follows `AGENTS.md`:
//! [`cli`] parses flags/env, `msp` speaks the Muse Session Protocol over
//! stdio, `translate`/`dispatch` map OpenAI requests onto MSP turns, and
//! `http` serves the public surface.

pub mod acp;
pub mod cli;
pub mod dispatch;
pub mod http;
pub mod msp;
pub mod support;
pub mod translate;
