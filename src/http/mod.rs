//! OpenAI-style HTTP surface: endpoints, SSE, errors (SPEC §3, §6).
//!
//! - [`router`] builds the axum app: chat, responses, models, healthz,
//!   JSON 404/405, the 426 websocket refusal, and the fingerprint-warn
//!   header.
//! - [`error`] maps every failure to the SPEC §6 table by kind (never by
//!   message text).
//! - [`sse`] frames bytes; [`chat`] and [`responses`] render turns.

pub mod chat;
pub mod error;
pub mod models;
pub mod responses;
pub mod sse;

use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};

use crate::dispatch::Dispatcher;

/// Shared handler state (cheap clone: dispatcher is `Arc`-backed).
#[derive(Clone)]
pub struct AppState {
    /// Turn dispatcher.
    pub dispatcher: Dispatcher,
}

/// Build the full HTTP app.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(chat::handler))
        .route("/v1/responses", post(responses::handler))
        .route("/v1/models", get(models::handler))
        .route("/healthz", get(healthz))
        .fallback(fallback_404)
        .method_not_allowed_fallback(fallback_405)
        .layer(middleware::from_fn(websocket_426))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            fingerprint_warn,
        ))
        .with_state(state)
}

/// `GET /healthz`: liveness + serving state (always 200; the body carries
/// readiness — `exhausted` means restart the bridge).
async fn healthz(State(state): State<AppState>) -> Response {
    let supervisor = state.dispatcher.supervisor();
    let status = supervisor.status().await;
    let (status_label, detail) = match &status {
        crate::msp::host::SupervisorStatus::Serving => ("ok", None),
        crate::msp::host::SupervisorStatus::Restarting => (
            "restarting",
            Some("host relaunch in flight; requests wait it out"),
        ),
        crate::msp::host::SupervisorStatus::Exhausted { reason } => (
            "exhausted",
            Some(match reason {
                crate::msp::host::ExhaustReason::NonRestartableExit { .. } => {
                    "host exited unrecoverably; fix the host and restart the bridge"
                }
                crate::msp::host::ExhaustReason::EphemeralHost => {
                    "host died on an ephemeral profile; restart the bridge"
                }
                crate::msp::host::ExhaustReason::BudgetSpent { .. } => {
                    "host restart budget spent; restart the bridge"
                }
            }),
        ),
        crate::msp::host::SupervisorStatus::Shutdown => ("shutdown", None),
    };
    let host = supervisor.current().await.handshake_info().map(|info| {
        serde_json::json!({
            "label": info.host_label(),
            "durability": info.durability,
            "compat": match info.compat {
                crate::msp::host::CompatStatus::Tested => "tested",
                crate::msp::host::CompatStatus::FingerprintMismatch => "fingerprint_mismatch",
            },
        })
    });
    let mut body = serde_json::json!({"status": status_label, "host": host});
    if let Some(detail) = detail {
        body["detail"] = detail.into();
    }
    (StatusCode::OK, axum::Json(body)).into_response()
}

/// Unknown route → JSON 404 (SPEC §6).
async fn fallback_404() -> Response {
    error::ApiError::not_found().into_response()
}

/// Wrong method → JSON 405 (same envelope as all errors).
async fn fallback_405() -> Response {
    error::ApiError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "invalid_request_error",
        "method_not_allowed",
        "method not allowed for this path",
    )
    .into_response()
}

/// Any `Upgrade: websocket` → `426 Upgrade Required` (SPEC §3.3). The WS
/// submodule is never implemented. No header values are logged.
async fn websocket_426(
    headers: HeaderMap,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let wants_websocket = headers
        .get(header::UPGRADE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("websocket"))
        });
    if wants_websocket {
        return error::ApiError::new(
            StatusCode::UPGRADE_REQUIRED,
            "invalid_request_error",
            "websocket_not_supported",
            "this endpoint serves plain HTTP only (SSE for streams); the websocket subprotocol is not supported",
        )
        .into_response();
    }
    next.run(request).await
}

/// Serve `app` until a shutdown signal, then drain in-flight requests for at
/// most `drain_budget` before returning.
///
/// The budget bounds the POST-signal drain only: it must never stop a
/// healthy server that has received no signal (a `timeout(budget, serve)`
/// around the whole serve future would kill the bridge `budget` after
/// startup, found live in P7 when a 68 s agent turn outlasted it).
pub async fn serve_with_drain(
    listener: tokio::net::TcpListener,
    app: Router,
    drain_budget: Duration,
) -> Result<(), String> {
    let graceful = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .into_future();
    tokio::pin!(graceful);
    tokio::select! {
        result = &mut graceful => {
            result.map_err(|e| format!("http server failed: {e}"))
        }
        _ = shutdown_signal() => {
            match tokio::time::timeout(drain_budget, graceful).await {
                Ok(result) => result.map_err(|e| format!("http server failed: {e}")),
                Err(_) => {
                    tracing::warn!("http drain budget exceeded; forcing host shutdown");
                    Ok(())
                }
            }
        }
    }
}

/// SIGTERM/SIGINT (unix) or Ctrl-C anywhere.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler installs")
            .recv()
            .await
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received; draining");
}

/// Response header naming live-vs-pin fingerprint drift (SPEC §4.1).
pub const FINGERPRINT_WARN_HEADER: &str = "x-msp-fingerprint-warn";

/// Attach [`FINGERPRINT_WARN_HEADER`] when the live host's fingerprint
/// differs from the validated pin.
async fn fingerprint_warn(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let mut response = next.run(request).await;
    let mismatch = state
        .dispatcher
        .supervisor()
        .current()
        .await
        .handshake_info()
        .is_some_and(|info| info.compat == crate::msp::host::CompatStatus::FingerprintMismatch);
    if mismatch
        && let Ok(value) = HeaderValue::from_str(
            "live host schema fingerprint differs from the validated pin; continuing (Developer Preview drift)",
        )
    {
        response
            .headers_mut()
            .insert(FINGERPRINT_WARN_HEADER, value);
    }
    response
}
