//! HTTP-request-equivalent turn orchestration: session → model → turn → fold.
//!
//! One MSP session per HTTP request (stateless v1): [`Dispatcher::run_turn`]
//! starts a session, optionally sets the model, starts one turn, and streams
//! folded [`OutputEvent`]s to the caller. P5 renders those events as SSE or
//! collected JSON; dispatch never shapes HTTP itself.
//!
//! Orchestration rules (SPEC §4.4–§5):
//!
//! - Adopt `sessionId` from the server-minted start result (we never send
//!   one, so it is not an echo); verify the approval mode against the
//!   request from the result AND the fold — a mismatch fails, never
//!   silently downgrades.
//! - `turnId` comes from the `turn/start` ack, never derived.
//! - Approvals fail closed: first non-approving choice, else `turn/cancel`.
//!   `userInput/request` ⇒ `userInput/cancel` (`headless-bridge`).
//! - `view/gap` (and broadcast lag) ⇒ page + idempotent refold before
//!   rendering current; live events buffer during the refill (splice-fill).
//! - Client disconnect ⇒ `turn/cancel` with the explicit turn id.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde_json::Value;
use tokio::sync::{broadcast, mpsc, watch};

use crate::msp::fold::{OutputEvent, TurnFold, TurnOutcome, Usage};
use crate::msp::host::{HostEvent, HostState, MspConnection, Supervisor, SupervisorError};
use crate::msp::proto::ErrorObject;
use crate::translate::TurnInput;

/// Driver→caller channel depth (512 folded events of backpressure before the
/// driver stalls; a stalled driver heals via lag catch-up paging).
const EVENT_CHANNEL_DEPTH: usize = 512;
/// Upper bound on gap-refill paging (50 × 100 events) before failing loudly.
const MAX_GAP_PAGES: u32 = 50;
/// `view/page` limit for refills.
const GAP_PAGE_LIMIT: u64 = 100;

/// Request-scoped turn parameters (post-translation).
#[derive(Debug, Clone)]
pub struct TurnRequest {
    /// Absolute workspace root for `session/start`.
    pub workspace_root: String,
    /// Requested approval mode (verified against the fold).
    pub approval_mode: String,
    /// Model id (`None`/empty ⇒ server default, no `setModel`).
    pub model: Option<String>,
    /// Translated input parts + display text + effort.
    pub input: TurnInput,
}

/// Setup/terminal failure of a dispatched turn (P5 maps to HTTP, SPEC §6).
#[derive(Debug)]
pub enum DispatchError {
    /// No live host and none coming (503 + `Retry-After: 5`, except config
    /// exits, which are 401).
    HostUnavailable(SupervisorError),
    /// The host died mid-setup (503 + `Retry-After: 5`).
    HostDead,
    /// An MSP command failed (mapped by `data.kind`, SPEC §6).
    Msp(ErrorObject),
    /// `session/setModel` rejected the name (`invalid_model` ⇒ caller 400
    /// `invalid_request_error`; verified live — unknown models do not pass
    /// through).
    UnknownModel(String),
    /// Folded approval mode ≠ requested mode (never silently downgrade).
    ApprovalModeMismatch {
        /// Requested mode.
        requested: String,
        /// Folded effective mode.
        folded: String,
    },
    /// Non-stream terminal failure (streams render `Terminal` directly).
    TurnFailed {
        /// Failure kind (SPEC §6 row selector).
        kind: String,
        /// Human text.
        message: String,
        /// Server resubmission judgment.
        retryable: bool,
    },
    /// Bridge bug (host answered outside the contract).
    Internal(String),
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DispatchError::HostUnavailable(e) => write!(f, "msp host unavailable: {e:?}"),
            DispatchError::HostDead => write!(f, "msp host died mid-request"),
            DispatchError::Msp(e) => write!(f, "msp command failed: {}", e.message),
            DispatchError::UnknownModel(model) => write!(f, "unknown model '{model}'"),
            DispatchError::ApprovalModeMismatch { requested, folded } => write!(
                f,
                "approval mode mismatch: requested {requested}, folded {folded}"
            ),
            DispatchError::TurnFailed { kind, message, .. } => {
                write!(f, "turn failed ({kind}): {message}")
            }
            DispatchError::Internal(detail) => write!(f, "bridge bug: {detail}"),
        }
    }
}

impl std::error::Error for DispatchError {}

/// A running turn: folded events plus cancellation.
///
/// Dropping the handle cancels the turn (client-disconnect safety); P5
/// should still call [`TurnHandle::cancel`] explicitly on disconnect so the
/// `turn/cancel` lands before the response task ends.
pub struct TurnHandle {
    /// Folded output events, ending in exactly one `Terminal` (unless the
    /// host died mid-setup, which returns `Err` from `run_turn` instead).
    /// `Gap` never surfaces: the driver pages + refolds internally.
    pub events: mpsc::Receiver<OutputEvent>,
    cancel_tx: watch::Sender<bool>,
}

impl TurnHandle {
    /// Cancel the turn (`turn/cancel` with the explicit turn id) and stop
    /// the driver. Idempotent.
    pub fn cancel(&self) {
        let _ = self.cancel_tx.send(true);
    }

    /// Collect a non-stream turn from the event channel.
    pub async fn collect(&mut self) -> CollectedTurn {
        let mut collected = CollectedTurn::default();
        while let Some(event) = self.events.recv().await {
            match event {
                OutputEvent::ContentDelta(text) => collected.text.push_str(&text),
                OutputEvent::ReasoningDelta { text, .. } => collected.reasoning.push_str(&text),
                OutputEvent::StatusLine(line) => collected.status_lines.push(line),
                OutputEvent::Gap { .. } => {} // never surfaces; defensive
                OutputEvent::Terminal(outcome) => {
                    collected.outcome = Some(outcome);
                    break;
                }
            }
        }
        collected
    }
}

impl Drop for TurnHandle {
    /// Dropping the handle cancels the turn (client-disconnect safety).
    fn drop(&mut self) {
        let _ = self.cancel_tx.send(true);
    }
}

impl TurnHandle {
    /// Test handle over a scripted event channel (unit tests only).
    #[cfg(test)]
    pub(crate) fn for_tests(events: mpsc::Receiver<OutputEvent>) -> Self {
        let (cancel_tx, _) = watch::channel(false);
        Self { events, cancel_tx }
    }
}

/// One collected (non-stream) turn.
#[derive(Debug, Clone, Default)]
pub struct CollectedTurn {
    /// Concatenated `ContentDelta`s.
    pub text: String,
    /// Concatenated `ReasoningDelta`s (in emission order).
    pub reasoning: String,
    /// `StatusLine`s in order.
    pub status_lines: Vec<String>,
    /// The settling outcome.
    pub outcome: Option<TurnOutcome>,
}

/// One catalog row for `GET /v1/models`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelEntry {
    /// MSP `modelId`.
    pub id: String,
    /// Presentation label.
    pub display_label: Option<String>,
    /// Unix seconds from `releaseDate` (`0` when absent/unparseable).
    pub created: i64,
    /// MSP `providerId`.
    pub owned_by: Option<String>,
}

/// A fetched model catalog.
#[derive(Debug, Clone)]
pub struct ModelCatalog {
    /// Rows, newest first (host order preserved).
    pub models: Vec<ModelEntry>,
    /// When it was fetched.
    pub fetched_at: Instant,
}

/// `model/list` cache with retain-last-good (muse-acp pattern): a failed
/// refresh serves the previous catalog instead of failing the picker.
#[derive(Debug, Clone, Default)]
pub struct ModelCache {
    cached: Arc<Mutex<Option<ModelCatalog>>>,
}

impl ModelCache {
    /// Empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Last good catalog, if any.
    pub fn get(&self) -> Option<ModelCatalog> {
        self.cached
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Store a freshly parsed catalog.
    pub fn set(&self, catalog: ModelCatalog) {
        *self.cached.lock().unwrap_or_else(|p| p.into_inner()) = Some(catalog);
    }
}

/// Parse a `model/list` result into a catalog (pure; unit-tested).
fn parse_catalog(result: &Value) -> Result<ModelCatalog, ()> {
    let models = result.get("models").and_then(Value::as_array).ok_or(())?;
    let mut entries = Vec::with_capacity(models.len());
    for model in models {
        let id = model.get("modelId").and_then(Value::as_str).ok_or(())?;
        entries.push(ModelEntry {
            id: id.to_string(),
            display_label: model
                .get("displayLabel")
                .and_then(Value::as_str)
                .map(str::to_string),
            created: model
                .get("releaseDate")
                .and_then(Value::as_str)
                .map(parse_release_date)
                .unwrap_or(0),
            owned_by: model
                .get("providerId")
                .and_then(Value::as_str)
                .map(str::to_string),
        });
    }
    Ok(ModelCatalog {
        models: entries,
        fetched_at: Instant::now(),
    })
}

/// Canonicalize a workspace root to an absolute path (startup-fatal on error).
fn validate_workspace_root(path: &Path) -> Result<String, String> {
    path.canonicalize()
        .map(|p| p.to_string_lossy().into_owned())
        .map_err(|e| format!("workspace root {} is unusable: {e}", path.display()))
}

/// Whether a mode names one of the four wire approval modes.
fn valid_approval_mode(mode: &str) -> bool {
    matches!(
        mode,
        "allowAll" | "promptUnmatched" | "onRequest" | "denyUnmatched"
    )
}

/// `YYYY-MM-DD` → unix seconds (`0` when unparseable).
fn parse_release_date(date: &str) -> i64 {
    let mut parts = date.split('-');
    let (Some(y), Some(m), Some(d)) = (parts.next(), parts.next(), parts.next()) else {
        return 0;
    };
    let (Ok(y), Ok(m), Ok(d)) = (y.parse::<i64>(), m.parse::<i64>(), d.parse::<i64>()) else {
        return 0;
    };
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return 0;
    }
    // days_from_civil (Howard Hinnant), shifted to unix epoch.
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146097 + doe - 719468) * 86400
}

/// Request dispatcher: a supervisor plus request-scoped defaults.
#[derive(Clone)]
pub struct Dispatcher {
    supervisor: Arc<Supervisor>,
    models: ModelCache,
    workspace_root: String,
    approval_mode: String,
}

impl Dispatcher {
    /// New dispatcher. The workspace root must exist (canonicalized to an
    /// absolute path for `session/start`); the approval mode must be one of
    /// the four wire modes. Both are startup-fatal when invalid.
    pub fn new(
        supervisor: Arc<Supervisor>,
        workspace_root: &Path,
        approval_mode: &str,
    ) -> Result<Self, String> {
        Self::new_with_cache(supervisor, workspace_root, approval_mode, ModelCache::new())
    }

    /// [`Dispatcher::new`] with an explicit cache (lets tests share one
    /// cache across dispatchers to prove retain-last-good).
    pub fn new_with_cache(
        supervisor: Arc<Supervisor>,
        workspace_root: &Path,
        approval_mode: &str,
        models: ModelCache,
    ) -> Result<Self, String> {
        let canonical = validate_workspace_root(workspace_root)?;
        if !valid_approval_mode(approval_mode) {
            return Err(format!(
                "approval mode must be allowAll|promptUnmatched|onRequest|denyUnmatched, got '{approval_mode}'"
            ));
        }
        Ok(Self {
            supervisor,
            models,
            workspace_root: canonical,
            approval_mode: approval_mode.to_string(),
        })
    }

    /// The model cache (shared across clones).
    pub fn model_cache(&self) -> &ModelCache {
        &self.models
    }

    /// The underlying supervisor (for `/healthz` and shutdown).
    pub fn supervisor(&self) -> &Arc<Supervisor> {
        &self.supervisor
    }

    /// Requested approval mode.
    pub fn approval_mode(&self) -> &str {
        &self.approval_mode
    }

    /// `GET /v1/models`: fresh `model/list`, falling back to the last good
    /// catalog when the host is unreachable.
    pub async fn models(&self) -> Result<ModelCatalog, DispatchError> {
        let conn = match self.supervisor.ready().await {
            Ok(conn) => conn,
            Err(error) => {
                if let Some(catalog) = self.models.get() {
                    tracing::warn!("serving stale model catalog (host unavailable)");
                    return Ok(catalog);
                }
                return Err(DispatchError::HostUnavailable(error));
            }
        };
        match conn
            .command_with_retry("model/list", &serde_json::json!({}))
            .await
        {
            Ok(result) => match parse_catalog(&result) {
                Ok(catalog) => {
                    self.models.set(catalog.clone());
                    Ok(catalog)
                }
                Err(()) => Err(DispatchError::Internal(
                    "model/list result had no models array".to_string(),
                )),
            },
            Err(error) => {
                if let Some(catalog) = self.models.get() {
                    tracing::warn!(
                        kind = error.kind(),
                        "model/list failed; serving stale catalog"
                    );
                    Ok(catalog)
                } else {
                    Err(map_setup_error(error))
                }
            }
        }
    }

    /// Run one request-equivalent turn: setup inline (errors ⇒ `Err`), then
    /// spawn the fold driver and return its handle.
    pub async fn run_turn(
        &self,
        model: Option<String>,
        input: TurnInput,
    ) -> Result<TurnHandle, DispatchError> {
        let request = TurnRequest {
            workspace_root: self.workspace_root.clone(),
            approval_mode: self.approval_mode.clone(),
            model,
            input,
        };
        let conn = self
            .supervisor
            .ready()
            .await
            .map_err(DispatchError::HostUnavailable)?;
        // Subscribe BEFORE any command: `session/started` can precede its ack.
        let events_rx = conn.subscribe();
        let state_rx = conn.watch_state();
        let setup = setup_turn(&conn, &request, events_rx).await?;
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_DEPTH);
        tokio::spawn(drive_turn(DriveContext {
            conn,
            session_id: setup.session_id,
            turn_id: setup.turn_id,
            events_rx: setup.events_rx,
            buffered: setup.buffered,
            state_rx,
            cancel_rx,
            event_tx,
        }));
        Ok(TurnHandle {
            events: event_rx,
            cancel_tx,
        })
    }
}

/// Setup outcome: adopted ids plus the event stream (with pre-turn buffer).
struct TurnSetup {
    session_id: String,
    turn_id: String,
    events_rx: broadcast::Receiver<HostEvent>,
    buffered: Vec<HostEvent>,
}

/// `session/start` → verify mode → optional `setModel` → `turn/start`.
async fn setup_turn(
    conn: &MspConnection,
    request: &TurnRequest,
    mut events_rx: broadcast::Receiver<HostEvent>,
) -> Result<TurnSetup, DispatchError> {
    // 1. One session per request (stateless v1).
    let start_params = serde_json::json!({
        "commandId": conn.mint_command_id(),
        "workspaceRoot": request.workspace_root,
        "approvalMode": request.approval_mode,
    });
    let start = conn
        .command_with_retry("session/start", &start_params)
        .await
        .map_err(map_setup_error)?;
    let session = start.get("session").ok_or_else(|| {
        DispatchError::Internal("session/start result has no session object".to_string())
    })?;
    let session_id = session
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            DispatchError::Internal("session/start result has no sessionId".to_string())
        })?
        .to_string();
    // Fail fast on an echoed mode mismatch (the fold re-verifies below).
    let echoed_mode = session
        .get("approvalMode")
        .and_then(|m| m.get("mode"))
        .and_then(Value::as_str)
        .unwrap_or("");
    if !echoed_mode.is_empty() && echoed_mode != request.approval_mode {
        return Err(DispatchError::ApprovalModeMismatch {
            requested: request.approval_mode.clone(),
            folded: echoed_mode.to_string(),
        });
    }

    // 2. Optional model selection (empty/absent ⇒ server default).
    let model = request.model.as_deref().map(str::trim).unwrap_or("");
    if !model.is_empty() {
        let set_params = serde_json::json!({
            "commandId": conn.mint_command_id(),
            "sessionId": session_id,
            "model": {"modelId": model},
        });
        if let Err(error) = conn
            .command_with_retry("session/setModel", &set_params)
            .await
        {
            // Verified live: unknown models reject `commandRejected` /
            // `invalid_model` — they do not pass through.
            let reason = error
                .data
                .as_ref()
                .and_then(|d| d.get("reason"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if error.kind() == Some("commandRejected") && reason == "invalid_model" {
                return Err(DispatchError::UnknownModel(model.to_string()));
            }
            return Err(map_setup_error(error));
        }
    }

    // 3. Submit the turn; the ack names the turn that will carry the input.
    let mut turn_params = serde_json::json!({
        "commandId": conn.mint_command_id(),
        "sessionId": session_id,
        "input": request.input.to_msp_json(),
        "displayText": request.input.display_text,
    });
    if let Some(effort) = &request.input.reasoning_effort {
        turn_params["reasoningEffort"] = Value::String(effort.clone());
    }
    let ack = conn
        .command_with_retry("turn/start", &turn_params)
        .await
        .map_err(map_setup_error)?;
    let turn_id = ack
        .get("turnId")
        .and_then(Value::as_str)
        .ok_or_else(|| DispatchError::Internal("turn/start ack has no turnId".to_string()))?
        .to_string();
    if ack.get("disposition").and_then(Value::as_str) != Some("started") {
        tracing::warn!(
            session_id,
            turn_id,
            disposition = ?ack.get("disposition"),
            "fresh-session turn/start did not start immediately"
        );
    }
    tracing::info!(
        session_id,
        turn_id,
        model = request.model.as_deref().unwrap_or("default"),
        parts = request.input.parts.len(),
        "turn submitted"
    );

    // Drain pre-turn buffered events (session/started, approvalModeChanged):
    // they feed the fold first, in order, once the driver starts.
    let mut buffered = Vec::new();
    loop {
        match events_rx.try_recv() {
            Ok(event) => buffered.push(event),
            Err(broadcast::error::TryRecvError::Empty) => break,
            Err(broadcast::error::TryRecvError::Closed) => {
                return Err(DispatchError::HostDead);
            }
            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                // Missed events before the turn even started: the driver
                // will catch up from genesis via paging.
                tracing::warn!(skipped, "event lag during turn setup; driver will page");
                break;
            }
        }
    }
    // Folded-mode verification from the buffer (fold truth, not the echo).
    for event in &buffered {
        if let HostEvent::Notification { method, params } = event {
            let folded = match method.as_str() {
                "session/started" => params
                    .get("session")
                    .and_then(|s| s.get("approvalMode"))
                    .and_then(|m| m.get("mode"))
                    .and_then(Value::as_str),
                "session/approvalModeChanged" => {
                    if str_field(params, "sessionId") == session_id {
                        params.get("mode").and_then(Value::as_str)
                    } else {
                        None
                    }
                }
                _ => None,
            };
            if let Some(mode) = folded
                && session_of(method, params) == session_id
                && mode != request.approval_mode
            {
                return Err(DispatchError::ApprovalModeMismatch {
                    requested: request.approval_mode.clone(),
                    folded: mode.to_string(),
                });
            }
        }
    }

    Ok(TurnSetup {
        session_id,
        turn_id,
        events_rx,
        buffered,
    })
}

/// Map a setup-phase command error: host-dead disconnects become 503s, the
/// rest flow to kind-based mapping.
fn map_setup_error(error: ErrorObject) -> DispatchError {
    if error.kind() == Some("hostDead") {
        DispatchError::HostDead
    } else {
        DispatchError::Msp(error)
    }
}

fn str_field<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or("")
}

/// Owning session of a notification (`session/started` nests it).
fn session_of(method: &str, params: &Value) -> String {
    if method == "session/started" {
        params
            .get("session")
            .and_then(|s| s.get("sessionId"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    } else {
        str_field(params, "sessionId").to_string()
    }
}

/// Driver context: everything the fold loop needs.
struct DriveContext {
    conn: Arc<MspConnection>,
    session_id: String,
    turn_id: String,
    events_rx: broadcast::Receiver<HostEvent>,
    buffered: Vec<HostEvent>,
    state_rx: watch::Receiver<HostState>,
    cancel_rx: watch::Receiver<bool>,
    event_tx: mpsc::Sender<OutputEvent>,
}

/// Fold driver: feed events through [`TurnFold`], forward outputs, handle
/// gaps (page + refold), auto-decide approvals/user-input, and cancel on
/// client disconnect. Ends after the first terminal (or on cancel/death).
///
/// Splice-fill needs no explicit live buffer: events arriving during a
/// refill await sit in broadcast (ordered) and feed after it; any overlap
/// with page events dedupes through the fold's idempotency sets.
async fn drive_turn(mut ctx: DriveContext) {
    let mut fold = TurnFold::new(&ctx.session_id, &ctx.turn_id);
    // Pre-turn buffer first, in order (session/started, mode changes). A
    // gap here refills inline: the queue is static, so remaining queued
    // events feed after the refill in correct splice order.
    for event in std::mem::take(&mut ctx.buffered) {
        match drive_event(&mut fold, &ctx, event).await {
            DriveFlow::Continue => {}
            DriveFlow::Terminal => return,
            DriveFlow::Gap { after, next } => {
                if refill_gap(
                    &ctx.conn,
                    &mut fold,
                    &ctx.session_id,
                    &after,
                    &next,
                    &ctx.event_tx,
                )
                .await
                .is_err()
                {
                    return;
                }
            }
        }
    }
    // Destructure for the select loop: the `wait_for`/`recv` futures borrow
    // mutably across branches, so the channels must be disjoint locals.
    let DriveContext {
        conn,
        session_id,
        turn_id,
        mut events_rx,
        buffered: _,
        mut state_rx,
        mut cancel_rx,
        event_tx,
    } = ctx;
    loop {
        // Nobody listening: cancel the turn instead of streaming to the void.
        if event_tx.is_closed() {
            cancel_turn(&conn, &session_id, &turn_id).await;
            return;
        }
        tokio::select! {
            // `changed()` (not `wait_for`): a `wait_for` arm output would
            // hold its `watch::Ref` (which is `!Send`) across the arm's
            // awaits. The scoped borrows below drop before any await.
            _ = cancel_rx.changed() => {
                // `changed()` resolved, so the borrow below cannot fail;
                // the `Ref` temporary drops before any await.
                let cancelled = *cancel_rx.borrow_and_update();
                if cancelled {
                    cancel_turn(&conn, &session_id, &turn_id).await;
                }
                return;
            }
            _ = state_rx.changed() => {
                let dead = matches!(*state_rx.borrow_and_update(), HostState::Dead { .. });
                if dead {
                    send_terminal(
                        &event_tx,
                        TurnOutcome::Failed {
                            kind: "hostDead".to_string(),
                            message: "msp host died mid-turn".to_string(),
                            retryable: true,
                            usage: Usage::default(),
                        },
                    )
                    .await;
                }
                return;
            }
            received = events_rx.recv() => {
                match received {
                    Ok(event) => match drive_event_parts(&mut fold, &conn, &session_id, &turn_id, &event_tx, event).await {
                        DriveFlow::Continue => {}
                        DriveFlow::Terminal => return,
                        DriveFlow::Gap { after, next } => {
                            if refill_gap(
                                &conn, &mut fold, &session_id, &after, &next, &event_tx,
                            )
                            .await
                            .is_err()
                            {
                                return;
                            }
                        }
                    },
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "driver lagged behind host events; paging to catch up");
                        let after = fold.cursor().unwrap_or("").to_string();
                        if catch_up(&conn, &mut fold, &session_id, &after, &event_tx).await.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        send_terminal(
                            &event_tx,
                            TurnOutcome::Failed {
                                kind: "hostDead".to_string(),
                                message: "msp host connection closed mid-turn".to_string(),
                                retryable: true,
                                usage: Usage::default(),
                            },
                        )
                        .await;
                        return;
                    }
                }
            }
        }
    }
}

/// Driver control flow after one event.
#[derive(Debug, PartialEq, Eq)]
enum DriveFlow {
    /// Keep folding.
    Continue,
    /// A terminal was forwarded; the driver must stop.
    Terminal,
    /// A gap opened; the driver must page `(after, next)` and splice.
    Gap {
        /// Exclusive lower bound.
        after: String,
        /// Exclusive upper bound.
        next: String,
    },
}

/// Feed one host event: notifications fold, server requests auto-decide.
/// Other sessions' events are ignored here (their driver owns them).
async fn drive_event(fold: &mut TurnFold, ctx: &DriveContext, event: HostEvent) -> DriveFlow {
    drive_event_parts(
        fold,
        &ctx.conn,
        &ctx.session_id,
        &ctx.turn_id,
        &ctx.event_tx,
        event,
    )
    .await
}

/// [`drive_event`] with destructured context (for the select loop, where the
/// channels are disjoint locals).
async fn drive_event_parts(
    fold: &mut TurnFold,
    conn: &Arc<MspConnection>,
    session_id: &str,
    turn_id: &str,
    event_tx: &mpsc::Sender<OutputEvent>,
    event: HostEvent,
) -> DriveFlow {
    match event {
        HostEvent::Notification { method, params } => {
            if session_of(&method, &params) != session_id {
                return DriveFlow::Continue;
            }
            let mut flow = DriveFlow::Continue;
            for output in fold.feed(&method, &params) {
                match output {
                    OutputEvent::Gap { after, next } => {
                        flow = DriveFlow::Gap { after, next };
                    }
                    OutputEvent::Terminal(_) => {
                        send(event_tx, output).await;
                        return DriveFlow::Terminal;
                    }
                    delta => {
                        send(event_tx, delta).await;
                    }
                }
            }
            flow
        }
        HostEvent::ServerRequest { method, params } => {
            if str_field(&params, "sessionId") != session_id {
                return DriveFlow::Continue;
            }
            match method.as_str() {
                "approval/request" => decide_approval(conn, session_id, turn_id, &params).await,
                "userInput/request" => cancel_user_input(conn, session_id, &params).await,
                _ => {}
            }
            DriveFlow::Continue
        }
    }
}

/// Forward one output event (a closed channel means the caller went away:
/// cancel the turn — nobody is listening).
async fn send(event_tx: &mpsc::Sender<OutputEvent>, event: OutputEvent) {
    if event_tx.send(event).await.is_err() {
        // Caller dropped the handle: its Drop cancels via the watch below.
        // (No-op here: the cancel watch fires independently.)
    }
}

async fn send_terminal(event_tx: &mpsc::Sender<OutputEvent>, outcome: TurnOutcome) {
    send(event_tx, OutputEvent::Terminal(outcome)).await;
}

/// Fail-closed approval auto-decision (AGENTS rule 11): the FIRST
/// non-approving choice, else `turn/cancel`. Loud in logs with the durable
/// id. Never synthesize approval.
async fn decide_approval(conn: &MspConnection, session_id: &str, turn_id: &str, params: &Value) {
    let approval_id = str_field(params, "approvalId");
    let choices = params
        .get("availableChoices")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let deny = choices.iter().find(|choice| {
        !choice
            .get("decision")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_lowercase()
            .starts_with("approv")
    });
    match deny.and_then(|c| c.get("choiceId")).and_then(Value::as_str) {
        Some(choice_id) => {
            let requirement = params
                .get("currentRequirementId")
                .cloned()
                .unwrap_or(Value::Null);
            let decide_params = serde_json::json!({
                "commandId": conn.mint_command_id(),
                "sessionId": session_id,
                "approvalId": approval_id,
                "requirementId": requirement,
                "choiceId": choice_id,
            });
            match conn
                .command_with_retry("approval/decide", &decide_params)
                .await
            {
                Ok(_) => tracing::warn!(
                    session_id,
                    approval_id,
                    choice_id,
                    tool = str_field(params, "toolName"),
                    "headless bridge auto-denied an approval (fail closed)"
                ),
                Err(error) => {
                    // Already-resolved means another leg won the race: fine.
                    // Anything else logs loudly; the turn continues and its
                    // own terminal resolves (a still-pending approval parks
                    // until client disconnect cancels — rare under
                    // denyUnmatched, and failing the whole turn on a
                    // slow/racy decide would be worse).
                    tracing::warn!(
                        session_id,
                        approval_id,
                        choice_id,
                        kind = error.kind(),
                        message = %error.message,
                        "approval auto-decision failed; turn continues"
                    );
                }
            }
        }
        None => {
            tracing::warn!(
                session_id,
                approval_id,
                tool = str_field(params, "toolName"),
                "approval offers no deny choice; cancelling the turn rather than approve"
            );
            cancel_turn(conn, session_id, turn_id).await;
        }
    }
}

/// `userInput/request` ⇒ auto-`userInput/cancel` (`headless-bridge`).
/// Prompts time out server-side anyway; the cancel just hurries the
/// model-visible cancellation instead of parking the turn.
async fn cancel_user_input(conn: &MspConnection, session_id: &str, params: &Value) {
    let user_input_id = str_field(params, "userInputId");
    let cancel_params = serde_json::json!({
        "commandId": conn.mint_command_id(),
        "sessionId": session_id,
        "userInputId": user_input_id,
        "reason": "headless-bridge",
    });
    match conn
        .command_with_retry("userInput/cancel", &cancel_params)
        .await
    {
        Ok(_) => tracing::info!(
            session_id,
            user_input_id,
            "headless bridge auto-cancelled a user-input prompt"
        ),
        Err(error) => tracing::warn!(
            session_id,
            user_input_id,
            kind = error.kind(),
            message = %error.message,
            "user-input auto-cancel failed; prompt will time out server-side"
        ),
    }
}

/// Best-effort `turn/cancel` with the EXPLICIT turn id (target-anchored, so
/// same-`commandId` retries are safe).
async fn cancel_turn(conn: &MspConnection, session_id: &str, turn_id: &str) {
    let params = serde_json::json!({
        "commandId": conn.mint_command_id(),
        "sessionId": session_id,
        "turnId": turn_id,
    });
    match conn.command_with_retry("turn/cancel", &params).await {
        Ok(_) => tracing::info!(session_id, turn_id, "turn cancelled"),
        Err(error) => tracing::warn!(
            session_id,
            turn_id,
            kind = error.kind(),
            message = %error.message,
            "turn/cancel failed (best effort)"
        ),
    }
}

/// Gap refill: page `(after, …)` forward and re-feed through the fold with
/// its `done` + `usage_seen` idempotency sets, then resume. Failures terminal
/// loudly (missing content must never present as complete).
async fn refill_gap(
    conn: &MspConnection,
    fold: &mut TurnFold,
    session_id: &str,
    after: &str,
    next: &str,
    event_tx: &mpsc::Sender<OutputEvent>,
) -> Result<(), ()> {
    tracing::warn!(session_id, after, next, "view/gap: paging to refill");
    page_and_feed(conn, fold, session_id, after, event_tx).await
}

/// Broadcast-lag catch-up: page forward from the last folded cursor.
async fn catch_up(
    conn: &MspConnection,
    fold: &mut TurnFold,
    session_id: &str,
    after: &str,
    event_tx: &mpsc::Sender<OutputEvent>,
) -> Result<(), ()> {
    page_and_feed(conn, fold, session_id, after, event_tx).await
}

/// Page forward from `after`, feeding every page element through the fold
/// (which forwards new outputs and dedupes overlap). Bounded; on failure or
/// overflow the turn fails loudly.
async fn page_and_feed(
    conn: &MspConnection,
    fold: &mut TurnFold,
    session_id: &str,
    after: &str,
    event_tx: &mpsc::Sender<OutputEvent>,
) -> Result<(), ()> {
    let mut cursor = after.to_string();
    for _ in 0..MAX_GAP_PAGES {
        // An empty anchor omits `cursor` (page from the beginning): an
        // explicit `""` would be a missingAnchor error, not genesis.
        let mut params = serde_json::json!({
            "sessionId": session_id,
            "direction": "forward",
            "limit": GAP_PAGE_LIMIT,
        });
        if !cursor.is_empty() {
            params["cursor"] = Value::String(cursor.clone());
        }
        let page = match conn.command_with_retry("view/page", &params).await {
            Ok(page) => page,
            Err(error) => {
                send_terminal(
                    event_tx,
                    TurnOutcome::Failed {
                        kind: error.kind().unwrap_or("internal").to_string(),
                        message: format!("gap refill paging failed: {}", error.message),
                        retryable: false,
                        usage: Usage::default(),
                    },
                )
                .await;
                return Err(());
            }
        };
        let events = page
            .get("events")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if events.is_empty() {
            return Ok(());
        }
        for event in &events {
            let method = str_field(event, "method");
            let params = event.get("params").unwrap_or(&Value::Null);
            for output in fold.feed(method, params) {
                match output {
                    // Nested gaps: keep paging (the outer loop continues
                    // from the latest cursor below).
                    OutputEvent::Gap { .. } => {}
                    OutputEvent::Terminal(_) => {
                        send(event_tx, output).await;
                        return Err(()); // terminal forwarded; driver must stop
                    }
                    delta => {
                        send(event_tx, delta).await;
                    }
                }
            }
        }
        match page.get("nextCursor").and_then(Value::as_str) {
            Some(next) => cursor = next.to_string(),
            None => return Ok(()),
        }
    }
    send_terminal(
        event_tx,
        TurnOutcome::Failed {
            kind: "internal".to_string(),
            message: "gap refill exceeded its page budget".to_string(),
            retryable: false,
            usage: Usage::default(),
        },
    )
    .await;
    Err(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    #[test]
    fn catalog_parses_rows_and_release_dates() {
        let result = json!({
            "models": [
                {"modelId": "m-new", "displayLabel": "New",
                 "releaseDate": "2026-09-02", "providerId": "meta"},
                {"modelId": "m-old", "releaseDate": null, "providerId": "meta"},
                {"modelId": "m-bogus", "releaseDate": "not-a-date"},
            ],
            "providerId": "meta",
        });
        let catalog = parse_catalog(&result).expect("parse");
        assert_eq!(catalog.models.len(), 3);
        assert_eq!(catalog.models[0].id, "m-new");
        assert_eq!(catalog.models[0].display_label.as_deref(), Some("New"));
        // 2026-09-02T00:00:00Z.
        assert_eq!(catalog.models[0].created, 1_788_307_200);
        assert_eq!(catalog.models[0].owned_by.as_deref(), Some("meta"));
        assert_eq!(catalog.models[1].created, 0);
        assert_eq!(catalog.models[2].created, 0);
        assert!(parse_catalog(&json!({})).is_err());
        assert!(parse_catalog(&json!({"models": [{"displayLabel": "x"}]})).is_err());
    }

    #[test]
    fn release_date_parses_ymd_only() {
        assert_eq!(parse_release_date("2026-09-02"), 1_788_307_200);
        assert_eq!(parse_release_date("1970-01-01"), 0);
        for bad in [
            "",
            "2026-13-01",
            "2026-00-10",
            "2026-09-32",
            "09/02/2026",
            "2026-09",
        ] {
            assert_eq!(parse_release_date(bad), 0, "{bad}");
        }
    }

    #[tokio::test]
    async fn collect_turn_assembles_text_reasoning_and_status() {
        let (tx, rx) = mpsc::channel(16);
        let (_cancel_tx, _cancel_rx) = watch::channel(false);
        let mut handle = TurnHandle {
            events: rx,
            cancel_tx: _cancel_tx,
        };
        tx.send(OutputEvent::ContentDelta("Hello".to_string()))
            .await
            .unwrap();
        tx.send(OutputEvent::ReasoningDelta {
            part: 0,
            text: "think".to_string(),
        })
        .await
        .unwrap();
        tx.send(OutputEvent::StatusLine("[tool: x] running".to_string()))
            .await
            .unwrap();
        tx.send(OutputEvent::ContentDelta(" world".to_string()))
            .await
            .unwrap();
        tx.send(OutputEvent::Terminal(TurnOutcome::Completed {
            usage: Usage {
                prompt_tokens: 5,
                completion_tokens: 2,
                total_tokens: 7,
            },
        }))
        .await
        .unwrap();
        drop(tx);
        let collected = handle.collect().await;
        assert_eq!(collected.text, "Hello world");
        assert_eq!(collected.reasoning, "think");
        assert_eq!(collected.status_lines, vec!["[tool: x] running"]);
        assert!(matches!(
            collected.outcome,
            Some(TurnOutcome::Completed { .. })
        ));
    }

    #[test]
    fn startup_validation_rejects_bad_roots_and_modes() {
        for mode in ["allowAll", "promptUnmatched", "onRequest", "denyUnmatched"] {
            assert!(valid_approval_mode(mode), "{mode}");
        }
        for mode in ["", "allow", "DENYUNMATCHED", "prompt"] {
            assert!(!valid_approval_mode(mode), "{mode}");
        }
        let canonical = validate_workspace_root(&PathBuf::from(".")).expect("cwd exists");
        assert!(PathBuf::from(&canonical).is_absolute(), "{canonical}");
        assert!(
            validate_workspace_root(&PathBuf::from("/nonexistent-root-xyz/muse-bridge")).is_err()
        );
    }
}
