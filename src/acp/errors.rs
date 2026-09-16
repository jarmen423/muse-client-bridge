//! ACP error mapping: MSP failures → typed client errors (FORK_PLAN P4).
//!
//! Branch on `error.data.kind`, never on message text (AGENTS rule 6): every
//! mapping below keys off the stable kind, and the kind travels verbatim in
//! the message so clients can branch without parsing. Host text rides along
//! only as a truncated excerpt (diagnostics, never a branch point), and the
//! stderr tail rides 500-class failures only (captured, never parsed —
//! AGENTS rule 13).
//!
//! Codes stay JSON-RPC conventional: `-32602` for caller-fixable rejections
//! (bad params, unknown sessions/selectors, unresolvable resume/fork ids),
//! `-32603` for host failures. The SPEC §6 guidance (login required, rate
//! limited, context window, …) is shared with the HTTP surface's
//! [`crate::http::error`] table; the two tables must agree on guidance text
//! per kind even though the envelopes differ.

use crate::msp::host::{ExhaustReason, SupervisorError};
use crate::msp::proto::ErrorObject;
use crate::msp::spawn::ExitClass;

/// Host message excerpt cap in error bodies (diagnostics, never branched on).
const MESSAGE_EXCERPT_CHARS: usize = 300;
/// stderr tail cap in 500-class failure bodies.
const TAIL_CHARS: usize = 2000;

/// A typed ACP reply error: JSON-RPC code + stable message (never host text
/// alone — the kind rides along so clients can branch without parsing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcpError {
    /// JSON-RPC error code (`-32602` invalid params, `-32603` internal).
    pub code: i64,
    /// Stable message (kinds included, host text quoted, never bare).
    pub message: String,
}

impl AcpError {
    /// `-32602` invalid params.
    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
        }
    }

    /// `-32603` internal error (host failures, setup failures).
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            code: -32603,
            message: message.into(),
        }
    }

    /// An MSP command failure (SPEC §6): `-32603` with the kind up front.
    /// Branches on `data.kind` only — the host message is excerpted, never
    /// matched.
    pub fn msp_command(method: &str, error: &ErrorObject) -> Self {
        let kind = error.kind().unwrap_or("unknown");
        Self::internal(format!(
            "{method} failed ({kind}): {}",
            msp_detail(error.kind(), &error.message, "")
        ))
    }

    /// A resume/fork host failure: `-32602` with the kind up front. The id
    /// being (re-)attached is usually stale/unknown — caller-fixable — so
    /// this reads as invalid params even though the failure surfaced on the
    /// MSP plane.
    pub fn msp_lookup(method: &str, error: &ErrorObject) -> Self {
        let kind = error.kind().unwrap_or("unknown");
        Self::invalid_params(format!(
            "{method} failed ({kind}): {}",
            msp_detail(error.kind(), &error.message, "")
        ))
    }

    /// A mid-turn failure (fold `Failed` terminal or driver-invented kind):
    /// `-32603` with the kind up front plus the stderr tail on 500-class
    /// failures. `cancelled` never reaches here (it settles a `stopReason`).
    pub fn turn_failed(kind: &str, message: &str, stderr_tail: &str) -> Self {
        Self::internal(format!(
            "turn failed ({kind}): {}",
            msp_detail(Some(kind), message, stderr_tail)
        ))
    }

    /// A supervisor outage (no live host and none coming): `-32603` with a
    /// stable message per outage class. Config exits read as login-required,
    /// matching SPEC §6.
    pub fn host_unavailable(error: SupervisorError) -> Self {
        Self::internal(match error {
            SupervisorError::Exhausted(reason) => match reason {
                ExhaustReason::NonRestartableExit {
                    exit: ExitClass::Config,
                } => "muse login required (serve exited 3: configuration/credentials missing)"
                    .to_string(),
                ExhaustReason::NonRestartableExit { exit } => format!(
                    "msp host unavailable (serve cannot start: {}); fix serve and restart the bridge",
                    exit.describe()
                ),
                ExhaustReason::EphemeralHost => {
                    "msp host unavailable (ephemeral session host died); restart the bridge"
                        .to_string()
                }
                ExhaustReason::BudgetSpent { .. } => {
                    "msp host unavailable (restarts exhausted); restart the bridge".to_string()
                }
            },
            SupervisorError::Timeout => "msp host restart storm; retry shortly".to_string(),
            SupervisorError::Shutdown => "bridge is shutting down".to_string(),
        })
    }
}

/// Shared SPEC §6 kind table for MSP command errors and turn-failure
/// terminals. Open kinds: unknown kinds (and a missing kind) fall through
/// to the generic 500-class arm — never matched exhaustively.
fn msp_detail(kind: Option<&str>, host_message: &str, stderr_tail: &str) -> String {
    let excerpt = truncate(host_message, MESSAGE_EXCERPT_CHARS);
    let with_host = |base: &str| {
        if excerpt.is_empty() {
            base.to_string()
        } else {
            format!("{base}: {excerpt}")
        }
    };
    let with_tail = |message: String| {
        let tail = truncate(stderr_tail, TAIL_CHARS);
        if tail.is_empty() {
            message
        } else {
            format!("{message}\nserve stderr:\n{tail}")
        }
    };
    match kind {
        // 401-class: stable, no host text, no tail (credentials-adjacent).
        Some("authRequired") => "muse login required (serve reported authRequired)".to_string(),
        // 429-class: the client may retry the prompt.
        Some("rateLimit" | "overloaded" | "backpressured") => {
            with_host("host rate limited the request; retry the prompt")
        }
        // Context 400-class: the caller shrinks the prompt.
        Some("contextLength" | "contextWindow" | "inputTooLarge" | "outputResultTooLarge") => {
            with_host("request exceeds the model's context window; shrink the prompt")
        }
        Some("frameTooLarge") => {
            "request exceeds the 10 MiB frame cap; shrink images/prompt".to_string()
        }
        // 500-class below: host excerpt plus the stderr tail when present.
        Some("commandRejected") => with_tail(with_host("host rejected the command")),
        Some("invalidParams") => with_tail(with_host("bridge built an invalid MSP command (bug)")),
        Some("methodNotFound") => with_tail(with_host("bridge called an unknown MSP method (bug)")),
        Some("modelError") => with_tail(with_host("model error")),
        // 503-class: stable; the retry replays from scratch.
        Some("hostDead") => {
            "msp host died mid-request; safe to retry (replays from scratch)".to_string()
        }
        Some("timeout") => with_host("serve command timed out"),
        // Local protocol violations (our bug or a malformed host frame).
        Some("notInitialized") => {
            "bridge sent a command before the handshake completed (bug)".to_string()
        }
        Some("protocolError") => with_host("malformed host response"),
        Some("commandIdConflict") => {
            "bridge reused a commandId with a different payload (bug)".to_string()
        }
        Some(kind) => with_tail(with_host(&format!("msp {kind}"))),
        None => with_tail(with_host("msp error")),
    }
}

/// Truncate to `max` chars (char-boundary safe).
fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    text.chars().take(max).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msp_error(kind: &str, message: &str) -> ErrorObject {
        ErrorObject {
            code: -32000,
            message: message.to_string(),
            data: Some(serde_json::json!({"kind": kind})),
        }
    }

    fn msp_command_error(kind: &str) -> AcpError {
        AcpError::msp_command("turn/start", &msp_error(kind, "host says no"))
    }

    #[test]
    fn kind_table_covers_the_spec_rows() {
        // Auth reads as login-required (stable, no host text).
        let error = msp_command_error("authRequired");
        assert_eq!(error.code, -32603);
        assert!(
            error.message.contains("muse login required"),
            "{}",
            error.message
        );
        assert!(error.message.contains("authRequired"), "{}", error.message);
        assert!(
            !error.message.contains("host says no"),
            "401-class stays stable: {}",
            error.message
        );
        // Rate kinds invite a retry.
        for kind in ["rateLimit", "overloaded", "backpressured"] {
            let error = msp_command_error(kind);
            assert_eq!(error.code, -32603, "{kind}");
            assert!(
                error.message.contains("rate limited"),
                "{kind}: {}",
                error.message
            );
            assert!(error.message.contains(kind), "{kind}: {}", error.message);
            assert!(
                error.message.contains("host says no"),
                "{kind}: {}",
                error.message
            );
        }
        // Context kinds name the remedy.
        for kind in [
            "contextLength",
            "contextWindow",
            "inputTooLarge",
            "outputResultTooLarge",
        ] {
            let error = msp_command_error(kind);
            assert_eq!(error.code, -32603, "{kind}");
            assert!(
                error.message.contains("context window"),
                "{kind}: {}",
                error.message
            );
        }
        assert!(
            msp_command_error("frameTooLarge")
                .message
                .contains("10 MiB")
        );
        assert!(
            msp_command_error("commandRejected")
                .message
                .contains("rejected")
        );
        assert!(msp_command_error("invalidParams").message.contains("(bug)"));
        assert!(
            msp_command_error("hostDead")
                .message
                .contains("safe to retry")
        );
        // Unknown kinds fall through to the generic 500-class arm.
        let error = msp_command_error("futureExplosion");
        assert_eq!(error.code, -32603);
        assert!(
            error.message.contains("msp futureExplosion"),
            "{}",
            error.message
        );
    }

    #[test]
    fn mapping_branches_on_kind_never_on_message_text() {
        // A scary message with an innocent kind stays on the kind's arm.
        let error = AcpError::msp_command(
            "session/start",
            &msp_error("internal", "overloaded authRequired contextLength boom"),
        );
        assert!(
            error.message.contains("msp internal"),
            "kind wins over message text: {}",
            error.message
        );
        assert!(!error.message.contains("rate limited"), "{}", error.message);
        // And a benign message with a loaded kind takes the kind's arm.
        let error = AcpError::msp_command("session/start", &msp_error("overloaded", "all is well"));
        assert!(error.message.contains("rate limited"), "{}", error.message);
        // A missing kind reads as a generic msp error, never a hang or panic.
        let kindless = ErrorObject {
            code: -32603,
            message: "no kind anywhere".to_string(),
            data: None,
        };
        let error = AcpError::msp_command("session/start", &kindless);
        assert!(error.message.contains("(unknown)"), "{}", error.message);
    }

    #[test]
    fn long_host_text_is_truncated_not_dropped() {
        let error = AcpError::msp_command("turn/start", &msp_error("modelError", &"x".repeat(600)));
        assert!(error.message.contains(&"x".repeat(MESSAGE_EXCERPT_CHARS)));
        assert!(
            !error
                .message
                .contains(&"x".repeat(MESSAGE_EXCERPT_CHARS + 1))
        );
    }

    #[test]
    fn turn_failures_carry_the_tail_only_on_500_class() {
        let failed = AcpError::turn_failed("modelError", "host blew up", "boom\ntrace");
        assert_eq!(failed.code, -32603);
        assert!(
            failed.message.contains("turn failed (modelError)"),
            "{}",
            failed.message
        );
        assert!(
            failed.message.contains("host blew up"),
            "{}",
            failed.message
        );
        assert!(failed.message.contains("boom"), "{}", failed.message);
        // 401-class stays stable (no host text, no tail).
        let failed = AcpError::turn_failed("authRequired", "host blew up", "boom");
        assert!(
            !failed.message.contains("host blew up"),
            "{}",
            failed.message
        );
        assert!(!failed.message.contains("boom"), "{}", failed.message);
        // Rate/context carry the excerpt but no tail.
        let failed = AcpError::turn_failed("overloaded", "slow down", "boom");
        assert!(failed.message.contains("slow down"), "{}", failed.message);
        assert!(!failed.message.contains("boom"), "{}", failed.message);
        // Driver-invented gap-budget failures read honest.
        let failed = AcpError::turn_failed("internal", "gap refill exceeded its page budget", "");
        assert!(failed.message.contains("page budget"), "{}", failed.message);
    }

    #[test]
    fn lookups_read_as_caller_errors_with_the_kind_verbatim() {
        let error = AcpError::msp_lookup("session/resume", &msp_error("internal", "gone"));
        assert_eq!(error.code, -32602);
        assert!(
            error.message.contains("session/resume failed (internal)"),
            "{}",
            error.message
        );
    }

    #[test]
    fn supervisor_outages_map_to_stable_guidance() {
        let config = AcpError::host_unavailable(SupervisorError::Exhausted(
            ExhaustReason::NonRestartableExit {
                exit: ExitClass::Config,
            },
        ));
        assert!(
            config.message.contains("muse login required"),
            "{}",
            config.message
        );

        let spent =
            AcpError::host_unavailable(SupervisorError::Exhausted(ExhaustReason::BudgetSpent {
                last_exit: None,
            }));
        assert!(
            spent.message.contains("restarts exhausted"),
            "{}",
            spent.message
        );

        let ephemeral =
            AcpError::host_unavailable(SupervisorError::Exhausted(ExhaustReason::EphemeralHost));
        assert!(
            ephemeral.message.contains("ephemeral"),
            "{}",
            ephemeral.message
        );

        let usage = AcpError::host_unavailable(SupervisorError::Exhausted(
            ExhaustReason::NonRestartableExit {
                exit: ExitClass::Usage,
            },
        ));
        assert!(
            usage.message.contains("bad serve arguments"),
            "{}",
            usage.message
        );

        let storm = AcpError::host_unavailable(SupervisorError::Timeout);
        assert!(storm.message.contains("restart storm"), "{}", storm.message);

        let down = AcpError::host_unavailable(SupervisorError::Shutdown);
        assert!(down.message.contains("shutting down"), "{}", down.message);
    }
}
