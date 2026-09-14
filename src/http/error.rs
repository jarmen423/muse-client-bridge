//! Failure → HTTP mapping (SPEC §6). Branch on `data.kind`, never on
//! message text. Localhost audience, same-user threat model: 500-class and
//! 429/context-400 bodies include a truncated host message excerpt plus the
//! stderr tail (when present); caller-error bodies stay stable.

use axum::Json;
use axum::http::StatusCode;
use axum::http::header::RETRY_AFTER;
use axum::response::{IntoResponse, Response};
use serde_json::Value;

use crate::dispatch::DispatchError;
use crate::msp::host::SupervisorError;
use crate::translate::TranslateError;

/// `Retry-After` for 429/503 (SPEC §6 fixes 503→5; 429 uses the same beat).
pub const RETRY_AFTER_SECS: u64 = 5;
/// Host message excerpt cap in bodies (diagnostics, never a branch point).
const MESSAGE_EXCERPT_CHARS: usize = 300;
/// stderr tail cap in 500 bodies.
const TAIL_CHARS: usize = 2000;

/// One API error: status + OpenAI envelope + optional `Retry-After`.
#[derive(Debug, Clone)]
pub struct ApiError {
    /// HTTP status.
    pub status: StatusCode,
    /// OpenAI error `type`.
    pub error_type: &'static str,
    /// Stable machine code (`context_length_exceeded`, `msp_<kind>`, …).
    pub code: String,
    /// Human message (stable prefix + optional host excerpt/tail).
    pub message: String,
    /// `Retry-After` seconds (429/503).
    pub retry_after_secs: Option<u64>,
}

impl ApiError {
    /// Build an error directly (handler-level rejections).
    pub fn new(
        status: StatusCode,
        error_type: &'static str,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status,
            error_type,
            code: code.into(),
            message: message.into(),
            retry_after_secs: None,
        }
    }

    /// JSON 404 for unknown routes.
    pub fn not_found() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "invalid_request_error",
            "unknown_route",
            "unknown route",
        )
    }

    /// With `Retry-After`.
    pub fn retry_after(mut self, secs: u64) -> Self {
        self.retry_after_secs = Some(secs);
        self
    }

    /// The `{"error": …}` envelope (shared by JSON and SSE error frames).
    pub fn body(&self) -> Value {
        serde_json::json!({
            "error": {
                "message": self.message,
                "type": self.error_type,
                "param": Value::Null,
                "code": self.code,
            }
        })
    }

    /// Full HTTP response (status + envelope + `Retry-After`).
    pub fn into_response(self) -> Response {
        // Server-side signal: 5xx loud, 4xx quiet (caller errors are routine).
        // Never logs headers; bodies are our stable text, never client data.
        if self.status.is_server_error() {
            tracing::warn!(status = %self.status, code = %self.code, message = %self.message, "request failed");
        } else {
            tracing::debug!(status = %self.status, code = %self.code, message = %self.message, "request rejected");
        }
        let mut response = (self.status, Json(self.body())).into_response();
        if let Some(secs) = self.retry_after_secs
            && let Ok(value) = secs.to_string().parse()
        {
            response.headers_mut().insert(RETRY_AFTER, value);
        }
        response
    }

    /// Map a dispatch failure (SPEC §6). `stderr_tail` rides 500-class
    /// bodies only (and only when non-empty).
    pub fn from_dispatch(error: &DispatchError, stderr_tail: &str) -> Self {
        match error {
            DispatchError::HostUnavailable(reason) => match reason {
                SupervisorError::Exhausted(_) if reason.is_config_exit() => Self::new(
                    StatusCode::UNAUTHORIZED,
                    "invalid_request_error",
                    "muse_login_required",
                    "muse login required (serve exited 3: configuration/credentials missing)",
                ),
                SupervisorError::Exhausted(_) => Self::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "server_error",
                    "host_unavailable",
                    "msp host unavailable (restarts exhausted); restart the bridge",
                )
                .retry_after(RETRY_AFTER_SECS),
                SupervisorError::Timeout => Self::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "server_error",
                    "host_unavailable",
                    "msp host restart storm; retry shortly",
                )
                .retry_after(RETRY_AFTER_SECS),
                SupervisorError::Shutdown => Self::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "server_error",
                    "bridge_shutting_down",
                    "bridge is shutting down",
                )
                .retry_after(RETRY_AFTER_SECS),
            },
            DispatchError::HostDead => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "server_error",
                "host_died",
                "msp host died mid-request; safe to retry (replays from scratch)",
            )
            .retry_after(RETRY_AFTER_SECS),
            DispatchError::Msp(error) => map_msp_kind(error.kind(), &error.message, stderr_tail),
            DispatchError::UnknownModel(model) => Self::new(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "unknown_model",
                format!("unknown model '{model}'"),
            ),
            DispatchError::ApprovalModeMismatch { requested, folded } => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "approval_mode_mismatch",
                format!("approval mode mismatch: requested {requested}, folded {folded}"),
            ),
            // The server's `retryable` flag is logged, not mapped: HTTP
            // clients retry by status (429/503/500), which the kind table
            // already selects.
            DispatchError::TurnFailed { kind, message, .. } => {
                map_msp_kind(Some(kind), message, stderr_tail)
            }
            DispatchError::Internal(detail) => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "bridge_bug",
                format!("bridge bug: {detail}"),
            ),
        }
    }

    /// Map a mid-turn failure terminal (streams render this as an error
    /// frame; collected turns go through [`DispatchError::TurnFailed`]).
    pub fn from_turn_failure(kind: &str, message: &str, stderr_tail: &str) -> Self {
        map_msp_kind(Some(kind), message, stderr_tail)
    }

    /// Map a translation failure (all ⇒ caller 400).
    pub fn from_translate(error: &TranslateError) -> Self {
        let (code, message) = match error {
            TranslateError::EmptyPrompt => (
                "empty_prompt",
                "no prompt text or images in request".to_string(),
            ),
            TranslateError::ImageTooLarge { .. } => ("image_too_large", error.to_string()),
            TranslateError::ImageFetch { .. } => ("image_unfetchable", error.to_string()),
            TranslateError::InvalidImageData(_) => ("invalid_image", error.to_string()),
        };
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            code,
            message,
        )
    }
}

/// Shared kind table for MSP command errors and turn-failure terminals
/// (SPEC §6). Open terminals: unknown kinds ⇒ 500.
fn map_msp_kind(kind: Option<&str>, host_message: &str, stderr_tail: &str) -> ApiError {
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
        Some("authRequired") => ApiError::new(
            StatusCode::UNAUTHORIZED,
            "invalid_request_error",
            "muse_login_required",
            "muse login required (serve reported authRequired)",
        ),
        Some("rateLimit" | "overloaded" | "backpressured") => ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "rate_limit_exceeded",
            with_host("msp host rate limited the request"),
        )
        .retry_after(RETRY_AFTER_SECS),
        Some("contextLength" | "contextWindow" | "inputTooLarge" | "outputResultTooLarge") => {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "context_length_exceeded",
                with_host("request exceeds the model's context window"),
            )
        }
        Some("frameTooLarge") => ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "request_too_large",
            "request exceeds the 10 MiB MSP frame cap; shrink images/prompt",
        ),
        Some("commandRejected") => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "msp_command_rejected",
            with_tail(with_host("msp host rejected the command")),
        ),
        Some("invalidParams") => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "msp_invalid_params",
            with_tail(with_host("bridge built an invalid MSP command (bug)")),
        ),
        Some("hostDead") => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "server_error",
            "host_died",
            "msp host died mid-request; safe to retry (replays from scratch)",
        )
        .retry_after(RETRY_AFTER_SECS),
        Some("modelError") => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "model_error",
            with_tail(with_host("model error")),
        ),
        Some(kind) => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            sanitize_code(&format!("msp_{kind}")),
            with_tail(with_host(&format!("msp {kind}"))),
        ),
        None => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "msp_error",
            with_tail(with_host("msp error")),
        ),
    }
}

/// Truncate to `max` chars (char-boundary safe).
fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    text.chars().take(max).collect()
}

/// Sanitize a dynamic code to `[a-z0-9_]` (truncate 40).
fn sanitize_code(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .take(40)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msp::host::ExhaustReason;
    use crate::msp::proto::ErrorObject;
    use crate::msp::spawn::ExitClass;

    fn msp(kind: &str) -> DispatchError {
        DispatchError::Msp(ErrorObject {
            code: -32000,
            message: "host says no".to_string(),
            data: Some(serde_json::json!({"kind": kind})),
        })
    }

    /// (status, type, code, retry-after?) for an MSP kind with empty tail.
    fn mapped(kind: &str) -> (StatusCode, &'static str, String, Option<u64>) {
        let error = ApiError::from_dispatch(&msp(kind), "");
        (
            error.status,
            error.error_type,
            error.code,
            error.retry_after_secs,
        )
    }

    #[test]
    fn kind_table_maps_status_type_code_and_retry_after() {
        assert_eq!(
            mapped("authRequired"),
            (
                StatusCode::UNAUTHORIZED,
                "invalid_request_error",
                "muse_login_required".into(),
                None
            )
        );
        for kind in ["rateLimit", "overloaded", "backpressured"] {
            assert_eq!(
                mapped(kind),
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    "rate_limit_error",
                    "rate_limit_exceeded".into(),
                    Some(5)
                ),
                "{kind}"
            );
        }
        for kind in [
            "contextLength",
            "contextWindow",
            "inputTooLarge",
            "outputResultTooLarge",
        ] {
            assert_eq!(
                mapped(kind),
                (
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    "context_length_exceeded".into(),
                    None
                ),
                "{kind}"
            );
        }
        assert_eq!(
            mapped("frameTooLarge"),
            (
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "request_too_large".into(),
                None
            )
        );
        assert_eq!(
            mapped("commandRejected").0,
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(mapped("invalidParams").0, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            mapped("hostDead"),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "server_error",
                "host_died".into(),
                Some(5)
            )
        );
        assert_eq!(mapped("modelError").2, "model_error");
        // Unknown kinds fail 500, never match exhaustively.
        let (status, _, code, _) = mapped("futureExplosion");
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(code, "msp_futureexplosion");
    }

    #[test]
    fn host_messages_and_tails_ride_500s_only() {
        let error = ApiError::from_dispatch(&msp("modelError"), "boom\ntrace");
        assert!(error.message.contains("host says no"), "{}", error.message);
        assert!(error.message.contains("boom"), "{}", error.message);
        // 401s stay stable (no host text, no tail).
        let error = ApiError::from_dispatch(&msp("authRequired"), "tail");
        assert!(!error.message.contains("host says no"), "{}", error.message);
        assert!(!error.message.contains("tail"), "{}", error.message);
        // Context 400s carry the host excerpt but no tail.
        let error = ApiError::from_dispatch(&msp("inputTooLarge"), "tail");
        assert!(error.message.contains("host says no"), "{}", error.message);
        assert!(!error.message.contains("tail"), "{}", error.message);
    }

    #[test]
    fn supervisor_and_dispatch_errors_map() {
        let config_exit = DispatchError::HostUnavailable(SupervisorError::Exhausted(
            ExhaustReason::NonRestartableExit {
                exit: ExitClass::Config,
            },
        ));
        let error = ApiError::from_dispatch(&config_exit, "");
        assert_eq!(
            (error.status, error.code.as_str()),
            (StatusCode::UNAUTHORIZED, "muse_login_required")
        );

        let spent = DispatchError::HostUnavailable(SupervisorError::Exhausted(
            ExhaustReason::BudgetSpent { last_exit: None },
        ));
        let error = ApiError::from_dispatch(&spent, "");
        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.retry_after_secs, Some(5));

        let error = ApiError::from_dispatch(&DispatchError::HostDead, "");
        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);

        let error = ApiError::from_dispatch(&DispatchError::UnknownModel("x".into()), "");
        assert_eq!(
            (error.status, error.code.as_str()),
            (StatusCode::BAD_REQUEST, "unknown_model")
        );
        assert!(error.message.contains("'x'"), "{}", error.message);

        let error = ApiError::from_dispatch(
            &DispatchError::TurnFailed {
                kind: "hostDead".into(),
                message: String::new(),
                retryable: true,
            },
            "",
        );
        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);

        let error = ApiError::from_dispatch(
            &DispatchError::TurnFailed {
                kind: "weird".into(),
                message: "m".into(),
                retryable: false,
            },
            "",
        );
        assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn translate_errors_are_caller_400s() {
        for (error, code) in [
            (TranslateError::EmptyPrompt, "empty_prompt"),
            (
                TranslateError::ImageTooLarge { bytes: None },
                "image_too_large",
            ),
            (
                TranslateError::ImageFetch {
                    host: None,
                    detail: "x".into(),
                },
                "image_unfetchable",
            ),
            (
                TranslateError::InvalidImageData("x".into()),
                "invalid_image",
            ),
        ] {
            let api = ApiError::from_translate(&error);
            assert_eq!(api.status, StatusCode::BAD_REQUEST);
            assert_eq!(api.error_type, "invalid_request_error");
            assert_eq!(api.code, code);
        }
    }

    #[test]
    fn envelope_shape_and_retry_header() {
        let error = ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "rate_limit_exceeded",
            "slow",
        )
        .retry_after(5);
        assert_eq!(
            error.body(),
            serde_json::json!({"error": {"message": "slow", "type": "rate_limit_error", "param": null, "code": "rate_limit_exceeded"}})
        );
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers().get("retry-after").unwrap(), "5");
    }
}
