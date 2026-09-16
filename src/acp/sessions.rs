//! ACP sessions: one persistent MSP session per ACP session, turns per prompt.
//!
//! FORK_PLAN P3. Each ACP `sessionId` maps to a server-minted MSP `sessionId`
//! (created via `session/start`, re-attached via `session/resume`, branched
//! via `session/fork`); each `session/prompt` submits one `turn/start` — the
//! host queues concurrent turns itself, so every admitted turn is tracked and
//! each completes its own prompt reply. The folded view streams back as ACP
//! `session/update` notifications until the turn settles.
//!
//! Implemented here: new/list/resume/load/fork/close, prompt (incl. the
//! `/compact` protocol command), steering, mode/model/effort selectors, gap
//! page+refold with lag catch-up, fail-closed approvals, auto-cancelled user
//! input, and SPEC §6 error mapping. There is deliberately no permission
//! forwarding or elicitation surface: like the headless HTTP bridge, ACP
//! fails closed on approvals (first non-approving choice, else cancel) and
//! auto-cancels user input, loudly logged with the durable id.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Value, json};
use tokio::sync::{Mutex, watch};

use crate::acp::errors::AcpError;
use crate::acp::server::{self, Outbound};
use crate::dispatch::Dispatcher;
use crate::msp::fold::{OutputEvent, TurnFold, TurnOutcome};
use crate::msp::host::{HostEvent, HostState, MspConnection};
use crate::translate::{InputPart, MAX_IMAGE_BYTES, TurnInput};

/// Default reasoning effort for new ACP sessions (matches `muse-acp`).
pub const DEFAULT_REASONING_EFFORT: &str = "medium";
/// Default MSP approval mode for new ACP sessions. The driver fails
/// closed on approvals (first non-approving choice, else turn-cancel), so
/// nothing auto-approves under this default.
pub const DEFAULT_APPROVAL_MODE: &str = "promptUnmatched";
/// Env override for the session approval mode (`MUSE_APPROVAL_MODE`, either
/// vocabulary: `ask|auto|deny` or a host mode). Invalid values fail
/// `session/new` loudly instead of falling back.
pub const APPROVAL_MODE_ENV: &str = "MUSE_APPROVAL_MODE";
/// Cap for inlining local `resource_link` file text into a prompt (1 MiB;
/// larger files degrade to a `[resource: …]` reference, never an error).
const RESOURCE_TEXT_CAP_BYTES: u64 = 1024 * 1024;
/// `userInput/cancel` reason: P4 has no elicitation surface, so user-input
/// prompts auto-cancel (fail closed), loudly logged with the durable id.
const USER_INPUT_CANCEL_REASON: &str = "acp-fail-closed";

/// One in-flight ACP prompt: the MSP turn plus its reply routing.
struct InFlight {
    /// MSP turn id (adopted from the `turn/start` ack, never derived).
    turn_id: String,
    /// ACP request id awaiting the `stopReason` reply (`None` for
    /// steering-started turns, which were already acked `{}`).
    req_id: Option<Value>,
    /// Cancel flag watched by the driver (set by `session/cancel`/`close`).
    cancel_tx: watch::Sender<bool>,
}

/// One ACP session: a persistent MSP session plus prompt state.
struct AcpSession {
    /// Server-minted MSP session id.
    msp_sid: String,
    /// Session working directory (`""` ⇒ provider mode, no `workspaceRoot`).
    cwd: String,
    /// Negotiated ACP protocol version (1 or 2: chunk/message shapes).
    ver: u8,
    /// Effective MSP approval mode, in our vocabulary (`ask|auto|deny`).
    mode: String,
    /// Last selected model id (`""` ⇒ server default).
    model_value: String,
    /// Reasoning effort sent with each prompt.
    reasoning_effort: String,
    /// The running turn, if any (adopted from acks and resume results).
    active_turn: Option<String>,
    /// Every admitted turn (the host queues concurrent turns itself; each
    /// completes its own prompt reply).
    in_flight: Vec<InFlight>,
}

struct StoreInner {
    sessions: HashMap<String, AcpSession>,
}

/// ACP session table: persistent MSP sessions driven through the shared
/// [`Dispatcher`]'s supervisor, replying over the server's outbound channel.
pub struct SessionStore {
    dispatcher: Dispatcher,
    outbound: Outbound,
    inner: Mutex<StoreInner>,
}

impl SessionStore {
    /// New store over a dispatcher (supervisor + model catalog) and the
    /// server's outbound frame channel.
    pub fn new(dispatcher: Dispatcher, outbound: Outbound) -> Self {
        Self {
            dispatcher,
            outbound,
            inner: Mutex::new(StoreInner {
                sessions: HashMap::new(),
            }),
        }
    }

    /// `session/new`: start one persistent MSP session and register it.
    /// Returns the ACP result object (`sessionId`, `_meta`, `configOptions`,
    /// plus legacy `modes` on v1).
    pub async fn create_session(&self, ver: u8, params: &Value) -> Result<Value, AcpError> {
        let cwd = params
            .get("cwd")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        let workspace_root = if cwd.is_empty() {
            None
        } else {
            let path = std::path::Path::new(&cwd);
            if !path.is_dir() {
                return Err(AcpError::invalid_params(format!(
                    "session/new cwd is not a directory: {cwd}"
                )));
            }
            Some(path.canonicalize().map_err(|e| {
                AcpError::invalid_params(format!("session/new cwd is unusable ({cwd}): {e}"))
            })?)
        };
        let approval_mode = resolve_startup_mode()?;

        let conn = self.ready_conn().await?;
        let start_params = start_params(&conn, &approval_mode, workspace_root.as_deref());
        let start = conn
            .command_with_retry("session/start", &start_params)
            .await
            .map_err(|e| AcpError::msp_command("session/start", &e))?;
        let session = start.get("session").ok_or_else(|| {
            AcpError::internal("session/start result has no session object".to_string())
        })?;
        let msp_sid = session
            .get("sessionId")
            .and_then(Value::as_str)
            .ok_or_else(|| AcpError::internal("session/start result has no sessionId".to_string()))?
            .to_string();
        // Adopt the echoed mode for display; a mismatch warns loudly (never
        // silently downgrades) but the session stays usable.
        let echoed_mode = session
            .get("approvalMode")
            .and_then(|m| m.get("mode"))
            .and_then(Value::as_str)
            .unwrap_or("");
        if !echoed_mode.is_empty() && echoed_mode != approval_mode {
            tracing::warn!(
                msp_sid,
                requested = approval_mode,
                folded = echoed_mode,
                "session started with a different approval mode than requested"
            );
        }
        let mode = if echoed_mode.is_empty() {
            approval_mode.clone()
        } else {
            echoed_mode.to_string()
        };
        let model = session_model(session);

        // One catalog fetch for the model selector; a failed fetch degrades
        // to an empty selector (the retain-last-good cache covers flaps).
        let models = self.selector_models("session/new").await;

        let acp_sid = format!("acp-{}", uuid::Uuid::new_v4().simple());
        let mode_vocab = mode_from_msp(&mode).to_string();
        let active_turn = start
            .get("session")
            .and_then(|s| s.get("activeTurnId"))
            .and_then(Value::as_str)
            .map(str::to_string);
        {
            let mut inner = self.inner.lock().await;
            inner.sessions.insert(
                acp_sid.clone(),
                AcpSession {
                    msp_sid: msp_sid.clone(),
                    cwd,
                    ver,
                    mode: mode_vocab.clone(),
                    model_value: model.clone(),
                    reasoning_effort: DEFAULT_REASONING_EFFORT.to_string(),
                    active_turn,
                    in_flight: Vec::new(),
                },
            );
        }
        tracing::info!(acp_sid, msp_sid, mode, "acp session created");
        let mut result = json!({
            "sessionId": acp_sid,
            "_meta": {"mspSessionId": msp_sid},
            "configOptions": config_options(ver, &mode_vocab, &model, DEFAULT_REASONING_EFFORT, &models),
        });
        if ver != 2 {
            result["modes"] = session_modes(&mode_vocab);
        }
        // Advertise slash commands like `muse-acp` (best effort: a closed
        // channel means the writer is gone and the process is exiting).
        let _ = self
            .outbound
            .send(available_commands_frame(&acp_sid, ver))
            .await;
        Ok(result)
    }

    /// `session/prompt`: submit one turn and stream it. Admission errors
    /// reply inline; an admitted v1 prompt replies when its turn settles,
    /// while v2 acks `{}` immediately (the terminal arrives as a state
    /// update). The `/compact` protocol command settles inline, never as a
    /// turn.
    pub async fn prompt(self: &Arc<Self>, req_id: Value, params: &Value) {
        let acp_sid = params
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        match self.prompt_admit(&acp_sid, &req_id, params).await {
            Ok(PromptAdmit::Compact) => {}
            Ok(PromptAdmit::Driver(admitted)) => {
                let store = Arc::clone(self);
                tokio::spawn(async move {
                    drive_prompt(store, admitted).await;
                });
            }
            Err(error) => {
                self.send(server::error_frame(&req_id, error.code, &error.message))
                    .await;
            }
        }
    }

    /// Admit a prompt: validate, run `/compact` inline or `turn/start` and
    /// register in-flight. Every admitted turn is tracked (the host queues
    /// concurrent turns itself); each completes its own prompt reply.
    async fn prompt_admit(
        &self,
        acp_sid: &str,
        req_id: &Value,
        params: &Value,
    ) -> Result<PromptAdmit, AcpError> {
        if acp_sid.is_empty() {
            return Err(AcpError::invalid_params(
                "session/prompt requires params.sessionId".to_string(),
            ));
        }
        let (msp_sid, cwd, ver, effort) = {
            let inner = self.inner.lock().await;
            let session = inner
                .sessions
                .get(acp_sid)
                .ok_or_else(|| AcpError::invalid_params("unknown sessionId".to_string()))?;
            (
                session.msp_sid.clone(),
                session.cwd.clone(),
                session.ver,
                session.reasoning_effort.clone(),
            )
        };
        let (input, echo) = extract_prompt(params.get("prompt"), &cwd)?;
        // `/compact` is a protocol command, not a prompt: run
        // `session/compact` and settle immediately.
        if is_compact_command(&echo) {
            self.run_compact(acp_sid, &msp_sid, ver, req_id).await?;
            return Ok(PromptAdmit::Compact);
        }
        let conn = self.ready_conn().await?;
        // Subscribe BEFORE `turn/start`: view events can precede the ack.
        let events_rx = conn.subscribe();
        let state_rx = conn.watch_state();
        let mut turn_params = json!({
            "commandId": conn.mint_command_id(),
            "sessionId": msp_sid,
            "input": input.to_msp_json(),
            "displayText": display_text(&input),
        });
        turn_params["reasoningEffort"] = Value::String(effort);
        let ack = conn
            .command_with_retry("turn/start", &turn_params)
            .await
            .map_err(|e| AcpError::msp_command("turn/start", &e))?;
        let turn_id = ack
            .get("turnId")
            .and_then(Value::as_str)
            .ok_or_else(|| AcpError::internal("turn/start ack has no turnId".to_string()))?
            .to_string();
        // A missing disposition reads as started; anything else (queued,
        // steered, future values) is tracked identically — the turn's own
        // terminal settles it.
        let disposition = ack.get("disposition").and_then(Value::as_str);
        let started = disposition.is_none_or(|d| d == "started");
        if !started {
            tracing::info!(
                acp_sid,
                turn_id,
                disposition,
                "turn/start did not start immediately; tracking to its terminal"
            );
        }
        let (cancel_tx, cancel_rx) = watch::channel(false);
        {
            let mut inner = self.inner.lock().await;
            if let Some(session) = inner.sessions.get_mut(acp_sid) {
                session.in_flight.push(InFlight {
                    turn_id: turn_id.clone(),
                    req_id: Some(req_id.clone()),
                    cancel_tx,
                });
                if started {
                    session.active_turn = Some(turn_id.clone());
                }
            }
        }
        tracing::info!(
            acp_sid,
            turn_id,
            parts = input.parts.len(),
            "acp prompt submitted"
        );
        if ver == 2 {
            // Accepted: empty response, then the user-message echo (v2
            // MUST), then running (sent by the driver on start).
            self.send(server::result_frame(req_id, json!({}))).await;
            self.send(user_message_frame(acp_sid, &echo)).await;
        } else {
            // v1 echoes user content as chunks under one message id.
            let msg_id = format!("msg-{}", uuid::Uuid::new_v4().simple());
            if let Value::Array(blocks) = &echo {
                for block in blocks {
                    self.send(user_message_chunk_frame(acp_sid, &msg_id, block))
                        .await;
                }
            }
        }
        Ok(PromptAdmit::Driver(PromptDriver {
            acp_sid: acp_sid.to_string(),
            msp_sid,
            turn_id,
            msg_id: format!("msg-{}", uuid::Uuid::new_v4().simple()),
            req_id: Some(req_id.clone()),
            ver,
            conn,
            events_rx,
            state_rx,
            cancel_rx,
        }))
    }

    /// Run the `/compact` protocol command: `session/compact` (a `noop`
    /// status is success, logged with its reason), the user echo, and an
    /// immediate `end_turn` settlement — never a turn.
    async fn run_compact(
        &self,
        acp_sid: &str,
        msp_sid: &str,
        ver: u8,
        req_id: &Value,
    ) -> Result<(), AcpError> {
        let conn = self.ready_conn().await?;
        let params = json!({
            "commandId": conn.mint_command_id(),
            "sessionId": msp_sid,
        });
        let ack = conn
            .command_with_retry("session/compact", &params)
            .await
            .map_err(|e| AcpError::msp_command("session/compact", &e))?;
        if ack.get("status").and_then(Value::as_str) == Some("noop") {
            // Hoisted: `tracing::Value` shadows `serde_json::Value` inside
            // the macro expansion.
            let reason = ack.get("reason").and_then(Value::as_str).unwrap_or("?");
            tracing::info!(acp_sid, reason, "compact noop");
        }
        if ver == 2 {
            self.send(server::result_frame(req_id, json!({}))).await;
            let echo = Value::Array(vec![json!({"type": "text", "text": "/compact"})]);
            self.send(user_message_frame(acp_sid, &echo)).await;
            self.send(state_update_frame(acp_sid, "idle", Some("end_turn")))
                .await;
        } else {
            let msg_id = format!("msg-{}", uuid::Uuid::new_v4().simple());
            let block = json!({"type": "text", "text": "/compact"});
            self.send(user_message_chunk_frame(acp_sid, &msg_id, &block))
                .await;
            self.send(server::result_frame(
                req_id,
                json!({"stopReason": "end_turn"}),
            ))
            .await;
        }
        tracing::info!(acp_sid, "compact command settled");
        Ok(())
    }

    /// `session/cancel` (a notification): `session/cancel` stops all
    /// session work — best-effort `turn/cancel` for every in-flight turn
    /// plus each driver flag so prompts settle `cancelled` promptly.
    /// Idempotent.
    pub async fn cancel(&self, params: &Value) {
        let acp_sid = params
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        if acp_sid.is_empty() {
            return; // notification: nothing to acknowledge
        }
        let turns = {
            let inner = self.inner.lock().await;
            inner
                .sessions
                .get(acp_sid)
                .map(|s| {
                    let msp = s.msp_sid.clone();
                    s.in_flight
                        .iter()
                        .map(|f| (msp.clone(), f.turn_id.clone(), f.cancel_tx.clone()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };
        if turns.is_empty() {
            tracing::debug!(acp_sid, "session/cancel with no in-flight prompt");
            return;
        }
        let conn = self.ready_conn().await.ok();
        for (msp_sid, turn_id, cancel_tx) in turns {
            if let Some(conn) = &conn {
                cancel_turn(conn, &msp_sid, &turn_id).await;
            } else {
                tracing::warn!(acp_sid, turn_id, "session/cancel could not reach the host");
            }
            let _ = cancel_tx.send(true);
        }
    }

    /// `session/close`: stop session work, resolve every in-flight prompt
    /// as `cancelled`, drop local state. Returns `{}` (or unknown-session).
    pub async fn close(&self, params: &Value) -> Result<Value, AcpError> {
        let acp_sid = params
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        if acp_sid.is_empty() {
            return Err(AcpError::invalid_params(
                "session/close requires params.sessionId".to_string(),
            ));
        }
        let removed = self.inner.lock().await.sessions.remove(acp_sid);
        let Some(session) = removed else {
            return Err(AcpError::invalid_params("unknown sessionId".to_string()));
        };
        if !session.in_flight.is_empty() {
            let conn = self.ready_conn().await.ok();
            for in_flight in &session.in_flight {
                if let Some(conn) = &conn {
                    cancel_turn(conn, &session.msp_sid, &in_flight.turn_id).await;
                }
                let _ = in_flight.cancel_tx.send(true);
                // v1 resolves each prompt reply; v2 was already acked `{}`,
                // so one terminal state covers every prompt.
                if session.ver != 2
                    && let Some(req_id) = &in_flight.req_id
                {
                    self.send(server::result_frame(
                        req_id,
                        json!({"stopReason": "cancelled"}),
                    ))
                    .await;
                }
            }
            if session.ver == 2 {
                self.send(state_update_frame(acp_sid, "idle", Some("cancelled")))
                    .await;
            }
        }
        tracing::info!(acp_sid, "acp session closed");
        Ok(json!({}))
    }

    /// Clear one in-flight turn once its driver settles it, and report how
    /// many remain (v2 stays `running` until the last one settles).
    async fn clear_turn(&self, acp_sid: &str, turn_id: &str) -> usize {
        let mut inner = self.inner.lock().await;
        let Some(session) = inner.sessions.get_mut(acp_sid) else {
            return 0;
        };
        if let Some(pos) = session.in_flight.iter().position(|f| f.turn_id == turn_id) {
            session.in_flight.remove(pos);
        }
        if session.active_turn.as_deref() == Some(turn_id) {
            session.active_turn = None;
        }
        session.in_flight.len()
    }

    /// `session/list`: sessions are durable in the host, so list them
    /// there — adapter-live sessions first (under their ACP id), then host
    /// entries (under their MSP id, resumable via `session/resume`). A host
    /// listing hiccup never hides live sessions: degrade to owned-only.
    pub async fn list(&self, params: &Value) -> Result<Value, AcpError> {
        let filter_root = params
            .get("cwd")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let owned: Vec<(String, String, String)> = {
            let inner = self.inner.lock().await;
            // (ACP id, MSP id, cwd), sorted for a stable listing.
            let mut owned: Vec<_> = inner
                .sessions
                .iter()
                .map(|(acp, s)| (acp.clone(), s.msp_sid.clone(), s.cwd.clone()))
                .collect();
            owned.sort();
            owned
        };
        let entries: Vec<Value> = owned
            .iter()
            .map(|(acp, msp, cwd)| {
                json!({"sessionId": acp, "cwd": cwd, "_meta": {"mspSessionId": msp}})
            })
            .collect();
        let conn = self.ready_conn().await?;
        let mut list_params = json!({
            "commandId": conn.mint_command_id(),
            "limit": 200,
        });
        if !filter_root.is_empty() {
            list_params["workspaceRoot"] = Value::String(filter_root);
        }
        match conn.command_with_retry("session/list", &list_params).await {
            Ok(listed) => {
                let owned_msp: std::collections::HashSet<&str> =
                    owned.iter().map(|(_, m, _)| m.as_str()).collect();
                let mut entries = entries;
                if let Some(items) = listed.get("sessions").and_then(Value::as_array) {
                    for item in items {
                        let msp_id = item.get("sessionId").and_then(Value::as_str).unwrap_or("");
                        if msp_id.is_empty() || owned_msp.contains(msp_id) {
                            continue; // already listed under its ACP id
                        }
                        let mut entry = json!({
                            "sessionId": msp_id,
                            "cwd": item.get("workspaceRoot").and_then(Value::as_str).unwrap_or(""),
                        });
                        if let Some(updated) = item.get("updatedAt").and_then(Value::as_str) {
                            entry["updatedAt"] = Value::String(updated.to_string());
                        }
                        entries.push(entry);
                    }
                }
                Ok(json!({"sessions": entries}))
            }
            Err(error) => {
                tracing::warn!(
                    kind = error.kind(),
                    message = %error.message,
                    "session/list failed; returning adapter-live sessions only"
                );
                Ok(json!({"sessions": entries}))
            }
        }
    }

    /// `session/resume` / `session/load`: re-attach a durable host session.
    /// Unknown ACP ids resolve like the reference: a known session first,
    /// then `_meta.mspSessionId`, then the id itself (durable host ids are
    /// stable across adapter restarts). `session/load` always replays
    /// history; v2 `session/resume` replays only with `replayFrom`; v1
    /// `session/resume` reconnects silently.
    pub async fn resume_or_load(
        &self,
        method: &str,
        ver: u8,
        params: &Value,
    ) -> Result<Value, AcpError> {
        validate_opt_cwd(params)?;
        ignore_client_mcp_servers(params);
        reject_additional_dirs(params)?;
        let resume_cwd = params
            .get("cwd")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let sid = params
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if sid.is_empty() {
            return Err(AcpError::invalid_params(
                "session resume requires params.sessionId".to_string(),
            ));
        }
        let msp_sid = {
            let inner = self.inner.lock().await;
            inner
                .sessions
                .get(&sid)
                .map(|s| s.msp_sid.clone())
                .or_else(|| {
                    params
                        .get("_meta")
                        .and_then(|m| m.get("mspSessionId"))
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                })
                .unwrap_or_else(|| sid.clone())
        };
        let conn = self.ready_conn().await?;
        // Ask for inline history explicitly; the host may still downgrade
        // (`history.mode` reports what was actually served).
        let resume_params = json!({
            "commandId": conn.mint_command_id(),
            "sessionId": msp_sid,
            "history": "inline",
        });
        let resumed = conn
            .command_with_retry("session/resume", &resume_params)
            .await
            .map_err(|e| AcpError::msp_lookup("session/resume", &e))?;
        if let Some(pending) = resumed.get("pendingRequests").and_then(Value::as_array) {
            // Pending questions/approvals survive reconnects; the host
            // re-issues their requests, which the drivers pick up. Log them
            // so a stuck-looking turn is diagnosable.
            for request in pending {
                tracing::info!(acp_sid = sid, pending = %request, "resume: pending request");
            }
        }
        if let Some(history) = resumed.get("history") {
            let mode = history.get("mode").and_then(Value::as_str).unwrap_or("?");
            if mode != "inline" {
                tracing::info!(
                    acp_sid = sid,
                    mode,
                    "resume: history downgraded; replay may be partial"
                );
            }
        }
        let session = resumed.get("session").unwrap_or(&Value::Null);
        let real_msp = session
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or(&msp_sid)
            .to_string();
        let real_model = session_model(session);
        let restored_cwd = if resume_cwd.is_empty() {
            session
                .get("workspaceRoot")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        } else {
            resume_cwd
        };
        let replay = method == "session/load"
            || (method == "session/resume" && ver == 2 && params.get("replayFrom").is_some());
        let (mode_vocab, model_value, effort) = {
            let mut inner = self.inner.lock().await;
            let entry = inner
                .sessions
                .entry(sid.clone())
                .or_insert_with(|| AcpSession {
                    msp_sid: real_msp.clone(),
                    cwd: restored_cwd.clone(),
                    ver,
                    mode: "ask".to_string(),
                    model_value: String::new(),
                    reasoning_effort: DEFAULT_REASONING_EFFORT.to_string(),
                    active_turn: None,
                    in_flight: Vec::new(),
                });
            entry.msp_sid = real_msp.clone();
            entry.ver = ver;
            if !restored_cwd.is_empty() {
                entry.cwd = restored_cwd;
            }
            if !real_model.is_empty() {
                entry.model_value = real_model;
            }
            entry.active_turn = session
                .get("activeTurnId")
                .and_then(Value::as_str)
                .map(str::to_string);
            // Refresh the mode selector from the folded host mode so
            // resumed clients are not stuck stale.
            if let Some(folded) = session
                .get("approvalMode")
                .and_then(|m| m.get("mode"))
                .and_then(Value::as_str)
            {
                entry.mode = mode_from_msp(folded).to_string();
            }
            (
                entry.mode.clone(),
                entry.model_value.clone(),
                entry.reasoning_effort.clone(),
            )
        };
        if replay {
            for frame in replay_history(&sid, ver, &resumed) {
                self.send(frame).await;
            }
        }
        tracing::info!(acp_sid = sid, msp_sid = real_msp, "acp session resumed");
        let models = self.selector_models("session/resume").await;
        let mut result = json!({
            "sessionId": sid,
            "_meta": {"mspSessionId": real_msp},
            "configOptions": config_options(ver, &mode_vocab, &model_value, &effort, &models),
        });
        if ver != 2 {
            result["modes"] = session_modes(&mode_vocab);
        }
        let _ = self
            .outbound
            .send(available_commands_frame(&sid, ver))
            .await;
        Ok(result)
    }

    /// `session/fork`: branch a session's history into a new session. An
    /// absent cut point forks all completed turns; a JetBrains AIR fork
    /// point resolves to `cutPoint.lastTurnId` via `session/read`.
    pub async fn fork(&self, ver: u8, params: &Value) -> Result<Value, AcpError> {
        let src_sid = params
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if src_sid.is_empty() {
            return Err(AcpError::invalid_params(
                "session/fork requires params.sessionId".to_string(),
            ));
        }
        validate_opt_cwd(params)?;
        ignore_client_mcp_servers(params);
        reject_additional_dirs(params)?;
        let fork_cwd = params
            .get("cwd")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        // Resolve the source MSP session: known ACP session first, then
        // preserved metadata (same rule as resume).
        let msp_sid = {
            let inner = self.inner.lock().await;
            inner
                .sessions
                .get(&src_sid)
                .map(|s| s.msp_sid.clone())
                .or_else(|| {
                    params
                        .get("_meta")
                        .and_then(|m| m.get("mspSessionId"))
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                })
                .unwrap_or_else(|| src_sid.clone())
        };
        let conn = self.ready_conn().await?;
        let cut_point = resolve_fork_cut_point(&conn, &msp_sid, params).await?;
        let mut fork_params = json!({
            "commandId": conn.mint_command_id(),
            "sessionId": msp_sid,
        });
        if let Some(last_turn) = &cut_point {
            fork_params["cutPoint"] = json!({"lastTurnId": last_turn});
        }
        let forked = conn
            .command_with_retry("session/fork", &fork_params)
            .await
            .map_err(|e| AcpError::msp_lookup("session/fork", &e))?;
        let new_session = forked.get("session").unwrap_or(&Value::Null);
        let new_msp = new_session
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if new_msp.is_empty() {
            return Err(AcpError::internal(
                "session/fork returned no sessionId".to_string(),
            ));
        }
        let new_model = session_model(new_session);
        let restored_cwd = if fork_cwd.is_empty() {
            new_session
                .get("workspaceRoot")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        } else {
            fork_cwd
        };
        let mut mode_vocab = "ask".to_string();
        if let Some(folded) = new_session
            .get("approvalMode")
            .and_then(|m| m.get("mode"))
            .and_then(Value::as_str)
        {
            mode_vocab = mode_from_msp(folded).to_string();
        }
        let acp_sid = format!("acp-{}", uuid::Uuid::new_v4().simple());
        {
            let mut inner = self.inner.lock().await;
            inner.sessions.insert(
                acp_sid.clone(),
                AcpSession {
                    msp_sid: new_msp.clone(),
                    cwd: restored_cwd,
                    ver,
                    mode: mode_vocab.clone(),
                    model_value: new_model.clone(),
                    reasoning_effort: DEFAULT_REASONING_EFFORT.to_string(),
                    active_turn: new_session
                        .get("activeTurnId")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    in_flight: Vec::new(),
                },
            );
        }
        tracing::info!(acp_sid, msp_sid = new_msp, "acp session forked");
        let models = self.selector_models("session/fork").await;
        let mut result = json!({
            "sessionId": acp_sid,
            "_meta": {"mspSessionId": new_msp},
            "configOptions": config_options(ver, &mode_vocab, &new_model, DEFAULT_REASONING_EFFORT, &models),
        });
        if ver != 2 {
            result["modes"] = session_modes(&mode_vocab);
        }
        let _ = self
            .outbound
            .send(available_commands_frame(&acp_sid, ver))
            .await;
        Ok(result)
    }

    /// `session/set_config_option`: approval posture, model, or per-turn
    /// reasoning effort. Replies with the full updated option set, never
    /// just the delta.
    pub async fn set_config_option(&self, params: &Value) -> Result<Value, AcpError> {
        let sid = params
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let key = params
            .get("configId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let value = params
            .get("value")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let (msp_sid, ver) = {
            let inner = self.inner.lock().await;
            match inner.sessions.get(&sid) {
                Some(s) => (s.msp_sid.clone(), s.ver),
                None => {
                    return Err(AcpError::invalid_params("unknown sessionId".to_string()));
                }
            }
        };
        let conn = self.ready_conn().await?;
        let ack = match key.as_str() {
            "mode" => {
                let Some(host_mode) = resolve_mode(&value) else {
                    return Err(AcpError::invalid_params(
                        "mode must be ask|auto|deny".to_string(),
                    ));
                };
                let set_params = json!({
                    "commandId": conn.mint_command_id(),
                    "sessionId": msp_sid,
                    "mode": host_mode,
                });
                Some(
                    conn.command_with_retry("session/setApprovalMode", &set_params)
                        .await
                        .map_err(|e| AcpError::msp_command("session/setApprovalMode", &e))?,
                )
            }
            "model" => {
                let set_params = json!({
                    "commandId": conn.mint_command_id(),
                    "sessionId": msp_sid,
                    "model": {"modelId": value},
                });
                Some(
                    conn.command_with_retry("session/setModel", &set_params)
                        .await
                        .map_err(|e| AcpError::msp_command("session/setModel", &e))?,
                )
            }
            "reasoning_effort" => {
                if !is_reasoning_effort(&value) {
                    return Err(AcpError::invalid_params(
                        "reasoning_effort must be none|minimal|low|medium|high|xhigh|max|ultra"
                            .to_string(),
                    ));
                }
                // Adapter-local: rides every subsequent prompt/steer as the
                // turn-level effort (a turn-level effort overrides the
                // host's standing default for that turn only).
                None
            }
            _ => {
                return Err(AcpError::invalid_params(
                    "unknown configId (want mode|model|reasoning_effort)".to_string(),
                ));
            }
        };
        // The host echoes the folded mode; prefer it over the request so a
        // downgraded apply cannot desync selectors.
        let folded = ack
            .as_ref()
            .and_then(|r| r.get("effectiveMode"))
            .and_then(|e| e.get("mode"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let (mode_vocab, model_value, effort) = {
            let mut inner = self.inner.lock().await;
            let Some(session) = inner.sessions.get_mut(&sid) else {
                return Err(AcpError::invalid_params("unknown sessionId".to_string()));
            };
            match key.as_str() {
                "mode" => {
                    let host_mode = folded
                        .as_deref()
                        .or_else(|| resolve_mode(&value))
                        .unwrap_or("promptUnmatched");
                    session.mode = mode_from_msp(host_mode).to_string();
                }
                "model" => session.model_value.clone_from(&value),
                "reasoning_effort" => session.reasoning_effort.clone_from(&value),
                _ => unreachable!("configId validated above"),
            }
            (
                session.mode.clone(),
                session.model_value.clone(),
                session.reasoning_effort.clone(),
            )
        };
        let options = config_options(
            ver,
            &mode_vocab,
            &model_value,
            &effort,
            &self.selector_models("session/set_config_option").await,
        );
        Ok(json!({"configOptions": options}))
    }

    /// `session/set_mode`: the v1 operating-mode switch (`modeId` or legacy
    /// `mode`), same ask|auto|deny vocabulary.
    pub async fn set_mode(&self, params: &Value) -> Result<Value, AcpError> {
        let sid = params
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let value = params
            .get("modeId")
            .or_else(|| params.get("mode"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let msp_sid = {
            let inner = self.inner.lock().await;
            match inner.sessions.get(&sid) {
                Some(s) => s.msp_sid.clone(),
                None => {
                    return Err(AcpError::invalid_params("unknown sessionId".to_string()));
                }
            }
        };
        let Some(host_mode) = resolve_mode(&value) else {
            return Err(AcpError::invalid_params(
                "mode must be ask|auto|deny".to_string(),
            ));
        };
        let conn = self.ready_conn().await?;
        let set_params = json!({
            "commandId": conn.mint_command_id(),
            "sessionId": msp_sid,
            "mode": host_mode,
        });
        conn.command_with_retry("session/setApprovalMode", &set_params)
            .await
            .map_err(|e| AcpError::msp_command("session/setApprovalMode", &e))?;
        let mut inner = self.inner.lock().await;
        if let Some(session) = inner.sessions.get_mut(&sid) {
            session.mode = mode_from_msp(host_mode).to_string();
        }
        Ok(json!({"mode": value}))
    }

    /// `session/set_model`: the model-picker gesture.
    pub async fn set_model(&self, params: &Value) -> Result<Value, AcpError> {
        let sid = params
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let value = params
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if value.is_empty() {
            return Err(AcpError::invalid_params(
                "session/set_model requires params.model".to_string(),
            ));
        }
        let msp_sid = {
            let inner = self.inner.lock().await;
            match inner.sessions.get(&sid) {
                Some(s) => s.msp_sid.clone(),
                None => {
                    return Err(AcpError::invalid_params("unknown sessionId".to_string()));
                }
            }
        };
        let conn = self.ready_conn().await?;
        let set_params = json!({
            "commandId": conn.mint_command_id(),
            "sessionId": msp_sid,
            "model": {"modelId": value},
        });
        conn.command_with_retry("session/setModel", &set_params)
            .await
            .map_err(|e| AcpError::msp_command("session/setModel", &e))?;
        let mut inner = self.inner.lock().await;
        if let Some(session) = inner.sessions.get_mut(&sid) {
            session.model_value.clone_from(&value);
        }
        Ok(json!({"model": value}))
    }

    /// `_session/steering` (ACP v2 only): inject input into the running
    /// turn (`turn/steer` with the expected id), or start one when idle
    /// (`turn/start` with `ifBusy: "steer"`).
    pub async fn steering(
        self: &Arc<Self>,
        ver: u8,
        req_id: Value,
        params: &Value,
    ) -> Result<(), AcpError> {
        if ver != 2 {
            self.send(server::error_frame(
                &req_id,
                -32601,
                "steering requires ACP v2",
            ))
            .await;
            return Ok(());
        }
        let prompt_required = steering_prompt_required(params)?;
        let sid = params
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let (msp_sid, cwd, effort, active_turn) = {
            let inner = self.inner.lock().await;
            match inner.sessions.get(&sid) {
                Some(s) => (
                    s.msp_sid.clone(),
                    s.cwd.clone(),
                    s.reasoning_effort.clone(),
                    s.active_turn.clone(),
                ),
                None => {
                    self.send(server::error_frame(&req_id, -32602, "unknown sessionId"))
                        .await;
                    return Ok(());
                }
            }
        };
        let (input, echo) = match extract_prompt(params.get("prompt"), &cwd) {
            Ok((input, echo)) if !input.parts.is_empty() => (input, echo),
            Ok(_) => {
                self.send(server::error_frame(
                    &req_id,
                    -32602,
                    "steering requires content",
                ))
                .await;
                return Ok(());
            }
            Err(error) => {
                self.send(server::error_frame(&req_id, error.code, &error.message))
                    .await;
                return Ok(());
            }
        };
        if active_turn.is_none() && prompt_required {
            self.send(server::result_frame(
                &req_id,
                json!({"outcome": "promptRequired", "reason": "noRunningTurn"}),
            ))
            .await;
            return Ok(());
        }
        let conn = match self.ready_conn().await {
            Ok(conn) => conn,
            Err(error) => {
                self.send(server::error_frame(&req_id, error.code, &error.message))
                    .await;
                return Ok(());
            }
        };
        let events_rx = conn.subscribe();
        let state_rx = conn.watch_state();
        let ack = if let Some(expected) = active_turn.as_deref() {
            let steer_params = json!({
                "commandId": conn.mint_command_id(),
                "sessionId": msp_sid,
                "expectedTurnId": expected,
                "input": input.to_msp_json(),
                "reasoningEffort": effort,
            });
            conn.command_with_retry("turn/steer", &steer_params).await
        } else {
            let start_params = json!({
                "commandId": conn.mint_command_id(),
                "sessionId": msp_sid,
                "input": input.to_msp_json(),
                "ifBusy": "steer",
                "reasoningEffort": effort,
            });
            conn.command_with_retry("turn/start", &start_params).await
        };
        let ack = match ack {
            Ok(ack) => ack,
            Err(error) => {
                let failure = AcpError::msp_command("steering", &error);
                self.send(server::error_frame(&req_id, failure.code, &failure.message))
                    .await;
                return Ok(());
            }
        };
        let turn_id = ack
            .get("turnId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if turn_id.is_empty() {
            self.send(server::error_frame(
                &req_id,
                -32603,
                "steering returned no turnId",
            ))
            .await;
            return Ok(());
        }
        let (outcome, started_new) = if let Some(expected) = active_turn.as_deref() {
            if turn_id != expected {
                self.send(server::error_frame(
                    &req_id,
                    -32603,
                    "turn/steer returned a different turnId",
                ))
                .await;
                return Ok(());
            }
            ("injected", false)
        } else {
            match ack.get("disposition").and_then(Value::as_str) {
                Some("started") => ("startedNewTurn", true),
                Some("steered") => ("injected", false),
                Some(other) => {
                    self.send(server::error_frame(
                        &req_id,
                        -32603,
                        &format!("unexpected steering disposition '{other}'"),
                    ))
                    .await;
                    return Ok(());
                }
                None => {
                    self.send(server::error_frame(
                        &req_id,
                        -32603,
                        "steering turn/start returned no disposition",
                    ))
                    .await;
                    return Ok(());
                }
            }
        };
        if started_new {
            let (cancel_tx, cancel_rx) = watch::channel(false);
            {
                let mut inner = self.inner.lock().await;
                if let Some(session) = inner.sessions.get_mut(&sid) {
                    session.active_turn = Some(turn_id.clone());
                    session.in_flight.push(InFlight {
                        turn_id: turn_id.clone(),
                        req_id: None,
                        cancel_tx,
                    });
                }
            }
            let store = Arc::clone(self);
            let driver = PromptDriver {
                acp_sid: sid.clone(),
                msp_sid,
                turn_id: turn_id.clone(),
                msg_id: format!("msg-{}", uuid::Uuid::new_v4().simple()),
                req_id: None,
                ver,
                conn,
                events_rx,
                state_rx,
                cancel_rx,
            };
            tokio::spawn(async move { drive_prompt(store, driver).await });
        }
        // Acknowledge the extension before emitting the synthetic echo.
        self.send(server::result_frame(&req_id, json!({"outcome": outcome})))
            .await;
        self.send(user_message_frame(&sid, &echo)).await;
        self.send(state_update_frame(&sid, "running", None)).await;
        Ok(())
    }

    /// One catalog fetch for the model selector: a failed fetch degrades
    /// to an empty selector (the retain-last-good cache covers flaps).
    async fn selector_models(&self, method: &str) -> Vec<(String, String)> {
        match self.dispatcher.models().await {
            Ok(catalog) => catalog
                .models
                .iter()
                .map(|m| {
                    (
                        m.id.clone(),
                        m.display_label.clone().unwrap_or_else(|| m.id.clone()),
                    )
                })
                .collect(),
            Err(e) => {
                tracing::warn!("model/list failed for {method}; empty model selector: {e}");
                Vec::new()
            }
        }
    }

    /// A live MSP connection, or a typed ACP error (bridge 503-equivalent).
    async fn ready_conn(&self) -> Result<Arc<MspConnection>, AcpError> {
        self.dispatcher
            .supervisor()
            .ready()
            .await
            .map_err(AcpError::host_unavailable)
    }

    /// Send one outbound frame (dropped only when the writer is gone).
    async fn send(&self, frame: Value) {
        if self.outbound.send(frame).await.is_err() {
            tracing::debug!("outbound closed; dropping an ACP frame");
        }
    }
}

/// Prompt admission outcome: `/compact` settles inline, anything else
/// spawns a [`drive_prompt`] task.
enum PromptAdmit {
    /// The `/compact` protocol command ran and settled inline.
    Compact,
    /// A turn was admitted; drive it to its terminal.
    Driver(PromptDriver),
}

/// An admitted prompt: everything the fold driver needs.
struct PromptDriver {
    acp_sid: String,
    msp_sid: String,
    turn_id: String,
    /// One ACP message id per prompt (v2 chunks carry it; v1 omits it).
    msg_id: String,
    /// ACP request id awaiting the `stopReason` reply (`None` for
    /// steering-started turns: v1 has no reply to send, v2 rides the
    /// shared state updates).
    req_id: Option<Value>,
    ver: u8,
    conn: Arc<MspConnection>,
    events_rx: tokio::sync::broadcast::Receiver<HostEvent>,
    state_rx: watch::Receiver<HostState>,
    cancel_rx: watch::Receiver<bool>,
}

/// Upper bound on gap-refill paging (50 × 100 events) before failing loudly.
const MAX_GAP_PAGES: u32 = 50;
/// `view/page` limit for refills.
const GAP_PAGE_LIMIT: u64 = 100;

/// Fold driver: feed host events through [`TurnFold`], forward ACP updates,
/// page + refold across gaps (and broadcast lag), fail closed on approvals,
/// auto-cancel user input, and settle the prompt reply exactly once.
async fn drive_prompt(store: Arc<SessionStore>, mut ctx: PromptDriver) {
    if ctx.ver == 2 {
        store
            .send(state_update_frame(&ctx.acp_sid, "running", None))
            .await;
    }
    let mut fold = TurnFold::new(&ctx.msp_sid, &ctx.turn_id);
    loop {
        tokio::select! {
            _ = ctx.cancel_rx.changed() => {
                if *ctx.cancel_rx.borrow_and_update() {
                    settle_cancelled(&store, &mut ctx).await;
                    return;
                }
            }
            _ = ctx.state_rx.changed() => {
                if matches!(*ctx.state_rx.borrow_and_update(), HostState::Dead { .. }) {
                    settle_failed(
                        &store,
                        &mut ctx,
                        "hostDead",
                        "msp host died mid-turn",
                    )
                    .await;
                    return;
                }
            }
            received = ctx.events_rx.recv() => {
                match received {
                    Ok(HostEvent::Notification { method, params }) => {
                        if session_of(&method, &params) != ctx.msp_sid {
                            continue;
                        }
                        let mut gap: Option<(String, String)> = None;
                        let mut terminal: Option<TurnOutcome> = None;
                        for output in fold.feed(&method, &params) {
                            match output {
                                OutputEvent::Gap { after, next } => {
                                    gap = Some((after, next));
                                }
                                OutputEvent::Terminal(outcome) => {
                                    terminal = Some(outcome);
                                }
                                OutputEvent::ContentDelta(text) => {
                                    let frame = message_chunk_frame(
                                        &ctx.acp_sid,
                                        ctx.ver,
                                        &ctx.msg_id,
                                        &text,
                                    );
                                    store.send(frame).await;
                                }
                                OutputEvent::ReasoningDelta { text, .. } => {
                                    let frame = thought_chunk_frame(
                                        &ctx.acp_sid,
                                        ctx.ver,
                                        &ctx.msg_id,
                                        &text,
                                    );
                                    store.send(frame).await;
                                }
                                OutputEvent::StatusLine(line) => {
                                    tracing::debug!(
                                        acp_sid = ctx.acp_sid,
                                        turn_id = ctx.turn_id,
                                        line,
                                        "fold status line (no ACP surface in P3)"
                                    );
                                }
                            }
                        }
                        if let Some((after, next)) = gap
                            && refill_gap(&store, &mut ctx, &mut fold, &after, &next).await.is_err()
                        {
                            return;
                        }
                        if fold.is_settled() {
                            debug_assert!(
                                terminal.is_some(),
                                "a settled fold always emitted its terminal"
                            );
                        }
                        if let Some(outcome) = terminal {
                            settle_outcome(&store, &mut ctx, &outcome).await;
                            return;
                        }
                    }
                    Ok(HostEvent::ServerRequest { method, params }) => {
                        if params
                            .get("sessionId")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            != ctx.msp_sid
                        {
                            continue;
                        }
                        match method.as_str() {
                            // Fail closed like the HTTP surface (no
                            // permission/elicitation surface in P4).
                            "approval/request" => {
                                decide_approval(
                                    &ctx.conn,
                                    &ctx.msp_sid,
                                    &ctx.turn_id,
                                    &params,
                                )
                                .await;
                            }
                            "userInput/request" => {
                                cancel_user_input(&ctx.conn, &ctx.msp_sid, &params).await;
                            }
                            _ => {}
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            acp_sid = ctx.acp_sid,
                            turn_id = ctx.turn_id,
                            skipped,
                            "ACP driver lagged behind host events; paging to catch up"
                        );
                        let after = fold.cursor().unwrap_or("").to_string();
                        if page_and_feed(&store, &mut ctx, &mut fold, &after).await.is_err() {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        settle_failed(
                            &store,
                            &mut ctx,
                            "hostDead",
                            "msp host connection closed mid-turn",
                        )
                        .await;
                        return;
                    }
                }
            }
        }
    }
}

/// Gap refill: page `(after, …)` forward and re-feed through the fold with
/// its `done` + `usage_seen` idempotency sets, then resume. Failures settle
/// loudly (missing content must never present as complete). `Err` means the
/// driver must stop (a terminal was forwarded or the prompt failed).
async fn refill_gap(
    store: &SessionStore,
    ctx: &mut PromptDriver,
    fold: &mut TurnFold,
    after: &str,
    next: &str,
) -> Result<(), ()> {
    tracing::warn!(
        acp_sid = ctx.acp_sid,
        turn_id = ctx.turn_id,
        after,
        next,
        "view/gap: paging to refill"
    );
    page_and_feed(store, ctx, fold, after).await
}

/// Page forward from `after`, feeding every page element through the fold
/// (new outputs forward as ACP updates; overlap dedupes). Bounded; on
/// failure or overflow the prompt fails loudly. `Err` stops the driver.
async fn page_and_feed(
    store: &SessionStore,
    ctx: &mut PromptDriver,
    fold: &mut TurnFold,
    after: &str,
) -> Result<(), ()> {
    let mut cursor = after.to_string();
    for _ in 0..MAX_GAP_PAGES {
        // An empty anchor omits `cursor` (page from the beginning): an
        // explicit `\"\"` would be a missingAnchor error, not genesis.
        let mut params = json!({
            "commandId": ctx.conn.mint_command_id(),
            "sessionId": ctx.msp_sid,
            "direction": "forward",
            "limit": GAP_PAGE_LIMIT,
        });
        if !cursor.is_empty() {
            params["cursor"] = Value::String(cursor.clone());
        }
        let page = match ctx.conn.command_with_retry("view/page", &params).await {
            Ok(page) => page,
            Err(error) => {
                let kind = error.kind().unwrap_or("internal").to_string();
                let message = format!("gap refill paging failed: {}", error.message);
                settle_failed(store, ctx, &kind, &message).await;
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
            let method = event.get("method").and_then(Value::as_str).unwrap_or("");
            let params = event.get("params").unwrap_or(&Value::Null);
            for output in fold.feed(method, params) {
                match output {
                    // Nested gaps: keep paging (the outer loop continues
                    // from the latest cursor below).
                    OutputEvent::Gap { .. } => {}
                    OutputEvent::Terminal(outcome) => {
                        settle_outcome(store, ctx, &outcome).await;
                        return Err(()); // terminal forwarded; driver must stop
                    }
                    OutputEvent::ContentDelta(text) => {
                        let frame = message_chunk_frame(&ctx.acp_sid, ctx.ver, &ctx.msg_id, &text);
                        store.send(frame).await;
                    }
                    OutputEvent::ReasoningDelta { text, .. } => {
                        let frame = thought_chunk_frame(&ctx.acp_sid, ctx.ver, &ctx.msg_id, &text);
                        store.send(frame).await;
                    }
                    OutputEvent::StatusLine(_) => {}
                }
            }
        }
        match page.get("nextCursor").and_then(Value::as_str) {
            Some(next) => cursor = next.to_string(),
            None => return Ok(()),
        }
    }
    settle_failed(
        store,
        ctx,
        "internal",
        "gap refill exceeded its page budget",
    )
    .await;
    Err(())
}

/// Settle a prompt from its fold outcome: `completed`/`cancelled` get a
/// `stopReason` result, anything else a typed `-32603` (never a fake stop).
/// v2 reports `idle` only when no session work remains; otherwise it
/// re-asserts `running` so queued work isn't misreported.
async fn settle_outcome(store: &SessionStore, ctx: &mut PromptDriver, outcome: &TurnOutcome) {
    let rest = store.clear_turn(&ctx.acp_sid, &ctx.turn_id).await;
    match outcome {
        TurnOutcome::Completed { .. } => {
            tracing::info!(
                acp_sid = ctx.acp_sid,
                turn_id = ctx.turn_id,
                "acp prompt completed"
            );
            if ctx.ver == 2 {
                if rest == 0 {
                    store
                        .send(state_update_frame(&ctx.acp_sid, "idle", Some("end_turn")))
                        .await;
                } else {
                    store
                        .send(state_update_frame(&ctx.acp_sid, "running", None))
                        .await;
                }
            } else if let Some(req_id) = &ctx.req_id {
                store
                    .send(server::result_frame(
                        req_id,
                        json!({"stopReason": "end_turn"}),
                    ))
                    .await;
            }
        }
        TurnOutcome::Cancelled { .. } => settle_cancelled_rest(store, ctx, rest).await,
        TurnOutcome::Failed { kind, message, .. } => {
            settle_failed_rest(store, ctx, rest, kind, message).await;
        }
    }
}

/// Settle a prompt as client-cancelled.
async fn settle_cancelled(store: &SessionStore, ctx: &mut PromptDriver) {
    let rest = store.clear_turn(&ctx.acp_sid, &ctx.turn_id).await;
    settle_cancelled_rest(store, ctx, rest).await;
}

/// [`settle_cancelled`] with a known remaining count (settling from a fold
/// outcome clears first, so the count is threaded through).
async fn settle_cancelled_rest(store: &SessionStore, ctx: &PromptDriver, rest: usize) {
    tracing::info!(
        acp_sid = ctx.acp_sid,
        turn_id = ctx.turn_id,
        "acp prompt cancelled"
    );
    if ctx.ver == 2 {
        if rest == 0 {
            store
                .send(state_update_frame(&ctx.acp_sid, "idle", Some("cancelled")))
                .await;
        } else {
            store
                .send(state_update_frame(&ctx.acp_sid, "running", None))
                .await;
        }
    } else if let Some(req_id) = &ctx.req_id {
        store
            .send(server::result_frame(
                req_id,
                json!({"stopReason": "cancelled"}),
            ))
            .await;
    }
}

/// Settle a prompt as failed: v2 reports `idle/_failed`, v1 a typed
/// `-32603` carrying the kind verbatim.
async fn settle_failed(store: &SessionStore, ctx: &mut PromptDriver, kind: &str, message: &str) {
    let rest = store.clear_turn(&ctx.acp_sid, &ctx.turn_id).await;
    settle_failed_rest(store, ctx, rest, kind, message).await;
}

/// [`settle_failed`] with a known remaining count.
async fn settle_failed_rest(
    store: &SessionStore,
    ctx: &PromptDriver,
    rest: usize,
    kind: &str,
    message: &str,
) {
    tracing::warn!(
        acp_sid = ctx.acp_sid,
        turn_id = ctx.turn_id,
        kind,
        "acp prompt failed"
    );
    if ctx.ver == 2 {
        if rest == 0 {
            store
                .send(state_update_frame(&ctx.acp_sid, "idle", Some("_failed")))
                .await;
        } else {
            store
                .send(state_update_frame(&ctx.acp_sid, "running", None))
                .await;
        }
    } else if let Some(req_id) = &ctx.req_id {
        // SPEC §6 mapping with the kind verbatim; the stderr tail rides
        // 500-class failures (captured, never parsed).
        let tail = ctx.conn.stderr_tail().text();
        let error = AcpError::turn_failed(kind, message, &tail);
        store
            .send(server::error_frame(req_id, error.code, &error.message))
            .await;
    }
}

/// Whether an MSP choice decision approves (case-insensitive `approv*`,
/// matching the reference `fallback_deny`). The decision vocabulary is
/// open: anything not starting with "approv" counts as non-approving.
fn is_approving_decision(decision: &str) -> bool {
    decision.to_lowercase().starts_with("approv")
}

/// Fail-closed fallback choice (reference `fallback_deny`): the FIRST
/// non-approving choice id, skipping choices without an id. `None` when
/// every choice approves (or none is decidable) — the caller must fail
/// closed (cancel the turn), never synthesize approval.
fn fallback_deny_choice(choices: &[Value]) -> Option<&str> {
    choices.iter().find_map(|choice| {
        let decision = choice.get("decision").and_then(Value::as_str).unwrap_or("");
        if is_approving_decision(decision) {
            return None;
        }
        choice
            .get("choiceId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
    })
}

/// Fail-closed approval auto-decision (AGENTS rule 11): the FIRST
/// non-approving choice, else `turn/cancel`. Loud in logs with the durable
/// id. Never synthesize approval.
async fn decide_approval(conn: &MspConnection, session_id: &str, turn_id: &str, params: &Value) {
    let approval_id = params
        .get("approvalId")
        .and_then(Value::as_str)
        .unwrap_or("");
    // Hoisted out of the `tracing` field position below (`tracing::Value`
    // shadows `serde_json::Value` inside the macro expansion).
    let tool_name = params
        .get("toolName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    if approval_id.is_empty() {
        tracing::warn!(
            session_id,
            tool = tool_name,
            "approval request without an approvalId; cancelling the turn rather than decide blind"
        );
        cancel_turn(conn, session_id, turn_id).await;
        return;
    }
    let choices = params
        .get("availableChoices")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    match fallback_deny_choice(&choices) {
        Some(choice_id) => {
            let requirement = params
                .get("currentRequirementId")
                .cloned()
                .unwrap_or(Value::Null);
            let decide_params = json!({
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
                    tool = tool_name,
                    "auto-denied an approval (fail closed; no permission surface)"
                ),
                Err(error) => tracing::warn!(
                    session_id,
                    approval_id,
                    choice_id,
                    kind = error.kind(),
                    message = %error.message,
                    "approval auto-decision failed; turn continues"
                ),
            }
        }
        None => {
            tracing::warn!(
                session_id,
                approval_id,
                tool = tool_name,
                "approval offers no deny choice; cancelling the turn rather than approve"
            );
            cancel_turn(conn, session_id, turn_id).await;
        }
    }
}

/// `userInput/request` ⇒ auto-`userInput/cancel` (fail closed: P4 has no
/// elicitation surface). Prompts time out server-side anyway; the cancel
/// hurries the model-visible cancellation instead of parking the turn.
async fn cancel_user_input(conn: &MspConnection, session_id: &str, params: &Value) {
    let user_input_id = params
        .get("userInputId")
        .and_then(Value::as_str)
        .unwrap_or("");
    let cancel_params = json!({
        "commandId": conn.mint_command_id(),
        "sessionId": session_id,
        "userInputId": user_input_id,
        "reason": USER_INPUT_CANCEL_REASON,
    });
    match conn
        .command_with_retry("userInput/cancel", &cancel_params)
        .await
    {
        Ok(_) => tracing::info!(
            session_id,
            user_input_id,
            "auto-cancelled a user-input prompt (fail closed)"
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

/// Best-effort `turn/cancel` with the EXPLICIT turn id.
///
/// `session/cancel` stops all session work with plain-lane cancellation
/// (reference parity — the host reclaims queued turns itself, and the
/// fold settles `turn/unqueued`/`turn/retracted`). `turn/interrupt` (the
/// priority-lane stop gesture) and `turn/unqueue` (queued-submit reclaim)
/// ride the generic MSP command plane with the same `commandId`/retry
/// discipline; no ACP v1/v2 method needs their distinct semantics, so no
/// ACP path sends them today.
async fn cancel_turn(conn: &MspConnection, session_id: &str, turn_id: &str) {
    let params = json!({
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
        params
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    }
}

/// `session/start` params: `commandId` + `approvalMode`, plus
/// `workspaceRoot` only when the ACP session has a cwd (provider mode omits
/// the key — verified live).
fn start_params(
    conn: &MspConnection,
    approval_mode: &str,
    workspace_root: Option<&std::path::Path>,
) -> Value {
    let mut params = json!({
        "commandId": conn.mint_command_id(),
        "approvalMode": approval_mode,
    });
    if let Some(root) = workspace_root {
        params["workspaceRoot"] = Value::String(root.to_string_lossy().into_owned());
    }
    params
}

/// Resolve the startup approval mode: `MUSE_APPROVAL_MODE` in either
/// vocabulary, else [`DEFAULT_APPROVAL_MODE`]. Invalid values fail loudly.
fn resolve_startup_mode() -> Result<String, AcpError> {
    let raw = std::env::var(APPROVAL_MODE_ENV).unwrap_or_default();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(DEFAULT_APPROVAL_MODE.to_string());
    }
    resolve_mode(trimmed).map(str::to_string).ok_or_else(|| {
        AcpError::invalid_params(format!(
            "{APPROVAL_MODE_ENV} must be ask|auto|deny or a host mode, got '{trimmed}'"
        ))
    })
}

/// Best-effort model id from a `session/start` session object (top-level
/// `modelId`, else nested `model.modelId`, else `""`).
fn session_model(session: &Value) -> String {
    session
        .get("modelId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .or_else(|| {
            session
                .get("model")
                .and_then(|m| m.get("modelId"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
        })
        .unwrap_or("")
        .to_string()
}

/// Transcript label for the turn: `acp` plus the first text line (capped;
/// never model-visible, never logged above `debug` by callers).
fn display_text(input: &TurnInput) -> String {
    let first = input.parts.iter().find_map(|part| match part {
        InputPart::Text(text) => {
            let line = text.lines().next().unwrap_or("").trim();
            (!line.is_empty()).then(|| line.to_string())
        }
        InputPart::Image { .. } => None,
    });
    match first {
        Some(line) => format!("acp {}", line.chars().take(120).collect::<String>()),
        None => "acp prompt".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Mode vocabulary (ported from `muse-acp` `acp.rs`)
// ---------------------------------------------------------------------------

/// Our mode vocabulary → MSP `ApprovalMode`.
pub fn mode_to_msp(mode: &str) -> Option<&'static str> {
    match mode {
        "ask" => Some("promptUnmatched"),
        "auto" => Some("allowAll"),
        "deny" => Some("denyUnmatched"),
        _ => None,
    }
}

/// MSP `ApprovalMode` → our mode vocabulary.
pub fn mode_from_msp(mode: &str) -> &'static str {
    match mode {
        "allowAll" => "auto",
        "denyUnmatched" => "deny",
        _ => "ask",
    }
}

/// Resolve a configured mode in either vocabulary to the host enum.
pub fn resolve_mode(value: &str) -> Option<&'static str> {
    if let Some(mode) = mode_to_msp(value) {
        return Some(mode);
    }
    match value {
        "allowAll" | "promptUnmatched" | "onRequest" | "denyUnmatched" => Some(match value {
            "allowAll" => "allowAll",
            "promptUnmatched" => "promptUnmatched",
            "onRequest" => "onRequest",
            _ => "denyUnmatched",
        }),
        _ => None,
    }
}

/// Whether a value names a reasoning tier (the 8-tier vocabulary incl. `max`
/// as an alias; `translate` maps it — this is the ACP selector's guard).
pub fn is_reasoning_effort(value: &str) -> bool {
    matches!(
        value,
        "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max" | "ultra"
    )
}

// ---------------------------------------------------------------------------
// ACP shapes (ported from `muse-acp` `acp.rs`, via serde_json)
// ---------------------------------------------------------------------------

/// `configOptions`: mode, model, and reasoning selectors. ACP v1 calls the
/// selector key `id`; v2 renamed it to `configId` (the setter still uses
/// `configId` in both versions).
pub fn config_options(
    ver: u8,
    current_mode: &str,
    current_model: &str,
    reasoning_effort: &str,
    models: &[(String, String)],
) -> Value {
    let id_key = if ver == 1 { "id" } else { "configId" };
    let model_opts: Vec<Value> = models
        .iter()
        .map(|(id, label)| json!({"value": id, "name": label}))
        .collect();
    json!([
        {
            id_key: "mode",
            "name": "Session Mode",
            "description": "How the agent handles tool approvals",
            "category": "mode",
            "type": "select",
            "currentValue": current_mode,
            "options": [
                {"value": "ask", "name": "Ask", "description": "Request permission for unmatched tools"},
                {"value": "auto", "name": "Auto", "description": "Allow all tools without asking"},
                {"value": "deny", "name": "Deny", "description": "Deny unmatched tools"},
            ],
        },
        {
            id_key: "model",
            "name": "Model",
            "category": "model",
            "type": "select",
            "currentValue": current_model,
            "options": model_opts,
        },
        {
            id_key: "reasoning_effort",
            "name": "Reasoning Effort",
            "description": "Reasoning effort sent with each prompt and steering message",
            "category": "thought_level",
            "type": "select",
            "currentValue": reasoning_effort,
            "options": [
                {"value": "none", "name": "None"},
                {"value": "minimal", "name": "Minimal"},
                {"value": "low", "name": "Low"},
                {"value": "medium", "name": "Medium"},
                {"value": "high", "name": "High"},
                {"value": "xhigh", "name": "Extra High"},
                {"value": "ultra", "name": "Ultra"},
            ],
        },
    ])
}

/// Legacy v1 mode state for clients which predate `configOptions`.
pub fn session_modes(current_mode: &str) -> Value {
    json!({
        "currentModeId": current_mode,
        "availableModes": [
            {"id": "ask", "name": "Ask", "description": "Request permission for unmatched tools"},
            {"id": "auto", "name": "Auto", "description": "Allow all tools without asking"},
            {"id": "deny", "name": "Deny", "description": "Deny unmatched tools"},
        ],
    })
}

/// The Muse skills useful from an editor session (v1 `input` is bare-hint,
/// v2 wraps it as a typed object).
fn available_commands(ver: u8) -> Value {
    let input = |hint: &str| {
        if ver == 1 {
            json!({"hint": hint})
        } else {
            json!({"type": "text", "hint": hint})
        }
    };
    let commands: &[(&str, &str, Option<&str>)] = &[
        (
            "skill",
            "Invoke a Muse skill",
            Some("skill id and optional prompt"),
        ),
        (
            "plan",
            "Create a grounded plan and stop for approval",
            Some("what to plan"),
        ),
        ("compact", "Compact the session context", None),
        (
            "doctor",
            "Diagnose a Muse runtime or session issue",
            Some("symptom or session"),
        ),
        (
            "create-skill",
            "Create a Muse skill",
            Some("what the skill should do"),
        ),
        (
            "create-plugin",
            "Create a Muse plugin",
            Some("what the plugin should do"),
        ),
        (
            "import",
            "Import another agent's session",
            Some("transcript, path, or session id"),
        ),
    ];
    Value::Array(
        commands
            .iter()
            .map(|(name, description, hint)| {
                let mut item = json!({"name": name, "description": description});
                if let Some(hint) = hint {
                    item["input"] = input(hint);
                }
                item
            })
            .collect(),
    )
}

/// `available_commands_update` notification frame.
fn available_commands_frame(acp_sid: &str, ver: u8) -> Value {
    session_update_frame(
        acp_sid,
        json!({
            "sessionUpdate": "available_commands_update",
            "availableCommands": available_commands(ver),
        }),
    )
}

/// One `session/update` notification frame.
fn session_update_frame(acp_sid: &str, update: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {"sessionId": acp_sid, "update": update},
    })
}

/// v2 `state_update` (`state` + optional `stopReason`).
fn state_update_frame(acp_sid: &str, state: &str, stop: Option<&str>) -> Value {
    let mut update = json!({"sessionUpdate": "state_update", "state": state});
    if let Some(stop) = stop {
        update["stopReason"] = Value::String(stop.to_string());
    }
    session_update_frame(acp_sid, update)
}

/// `agent_message_chunk` for one content delta. v2 carries the turn's
/// `messageId`; v1 omits it (verified against `muse-acp` `fold.rs`).
fn message_chunk_frame(acp_sid: &str, ver: u8, msg_id: &str, text: &str) -> Value {
    let mut update = json!({
        "sessionUpdate": "agent_message_chunk",
        "content": {"type": "text", "text": text},
    });
    if ver == 2 {
        update["messageId"] = Value::String(msg_id.to_string());
    }
    session_update_frame(acp_sid, update)
}

/// `agent_thought_chunk` for one reasoning delta (same version split).
fn thought_chunk_frame(acp_sid: &str, ver: u8, msg_id: &str, text: &str) -> Value {
    let mut update = json!({
        "sessionUpdate": "agent_thought_chunk",
        "content": {"type": "text", "text": text},
    });
    if ver == 2 {
        update["messageId"] = Value::String(msg_id.to_string());
    }
    session_update_frame(acp_sid, update)
}

/// A v2 `user_message` upsert for accepted prompt content (v2 MUST echo).
fn user_message_frame(acp_sid: &str, content: &Value) -> Value {
    let msg_id = format!("msg-{}", uuid::Uuid::new_v4().simple());
    session_update_frame(
        acp_sid,
        json!({
            "sessionUpdate": "user_message",
            "messageId": msg_id,
            "content": content,
        }),
    )
}

/// A v1 `user_message_chunk` echoing one accepted prompt block. Unlike
/// agent chunks, user echoes carry a `messageId` on both versions.
fn user_message_chunk_frame(acp_sid: &str, msg_id: &str, block: &Value) -> Value {
    session_update_frame(
        acp_sid,
        json!({
            "sessionUpdate": "user_message_chunk",
            "messageId": msg_id,
            "content": block,
        }),
    )
}

// ---------------------------------------------------------------------------
// Resume/load/fork/steer helpers
// ---------------------------------------------------------------------------

/// `cwd` is optional when resuming/loading/forking — but when one is
/// supplied it must be absolute (same rule as `session/new`).
fn validate_opt_cwd(params: &Value) -> Result<(), AcpError> {
    if let Some(cwd) = params.get("cwd") {
        let valid = cwd
            .as_str()
            .is_some_and(|cwd| !cwd.is_empty() && std::path::Path::new(cwd).is_absolute());
        if !valid {
            return Err(AcpError::invalid_params(
                "params.cwd must be an absolute path".to_string(),
            ));
        }
    }
    Ok(())
}

/// Client-provided MCP servers cannot be forwarded (Muse owns its tool
/// runtime): tolerate and ignore them instead of aborting the session.
fn ignore_client_mcp_servers(params: &Value) {
    if params
        .get("mcpServers")
        .and_then(Value::as_array)
        .is_some_and(|servers| !servers.is_empty())
    {
        tracing::info!("ignoring client-provided MCP servers (not supported by the Muse host)");
    }
}

/// Additional workspace roots are not supported: fail loudly, never half-apply.
fn reject_additional_dirs(params: &Value) -> Result<(), AcpError> {
    if params
        .get("additionalDirectories")
        .and_then(Value::as_array)
        .is_some_and(|dirs| !dirs.is_empty())
    {
        return Err(AcpError::invalid_params(
            "additional directories are not supported".to_string(),
        ));
    }
    Ok(())
}

/// History replay for `session/load` (always) and v2 `session/resume` with
/// `replayFrom`: user/agent messages replay as message updates/chunks,
/// tool calls replay as completed tool updates (history carries args but
/// no output text). Unknown shapes resume without replay (logged), never
/// fail.
fn replay_history(acp_sid: &str, ver: u8, resume_res: &Value) -> Vec<Value> {
    let items = match resume_res
        .get("history")
        .and_then(|h| h.get("items"))
        .and_then(Value::as_array)
    {
        Some(items) => items.clone(),
        None => {
            tracing::info!(
                acp_sid,
                "resume: unrecognized history shape; resumed without replay"
            );
            return Vec::new();
        }
    };
    let mut out = Vec::new();
    for item in &items {
        let kind = item.get("kind").and_then(Value::as_str).unwrap_or("");
        match kind {
            "toolCall" => {
                let name = item.get("tool").and_then(Value::as_str).unwrap_or("tool");
                let status = item
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("completed");
                let title = item
                    .get("fallbackText")
                    .and_then(Value::as_str)
                    .filter(|t| !t.is_empty())
                    .unwrap_or(name);
                let tool_call_id = item.get("itemId").and_then(Value::as_str).unwrap_or("");
                out.push(session_update_frame(
                    acp_sid,
                    json!({
                        "sessionUpdate": "tool_call",
                        "toolCallId": tool_call_id,
                        "status": status,
                        "title": title,
                        "kind": name,
                        "content": [],
                    }),
                ));
            }
            "userMessage" | "agentMessage" => {
                let text = item.get("text").and_then(Value::as_str).unwrap_or("");
                if text.is_empty() {
                    continue;
                }
                let msg_id = format!("msg-{}", uuid::Uuid::new_v4().simple());
                // v1 replays stream chunks; v2 replays full message upserts.
                let update = if ver == 2 {
                    let update_kind = if kind == "userMessage" {
                        "user_message"
                    } else {
                        "agent_message"
                    };
                    json!({
                        "sessionUpdate": update_kind,
                        "messageId": msg_id,
                        "content": [{"type": "text", "text": text}],
                    })
                } else {
                    let update_kind = if kind == "userMessage" {
                        "user_message_chunk"
                    } else {
                        "agent_message_chunk"
                    };
                    json!({
                        "sessionUpdate": update_kind,
                        "messageId": msg_id,
                        "content": {"type": "text", "text": text},
                    })
                };
                out.push(session_update_frame(acp_sid, update));
            }
            _ => {}
        }
    }
    out
}

/// Whether steering must refuse to start a turn when idle:
/// `_meta.steering.idleBehavior == "promptRequired"`. A missing `_meta`
/// (or null) means steering may start one.
fn steering_prompt_required(params: &Value) -> Result<bool, AcpError> {
    let Some(meta) = params.get("_meta") else {
        return Ok(false);
    };
    if meta.is_null() {
        return Ok(false);
    }
    if !meta.is_object() {
        return Err(AcpError::invalid_params(
            "steering _meta must be an object".to_string(),
        ));
    }
    let Some(steering) = meta.get("steering") else {
        return Ok(false);
    };
    if !steering.is_object() {
        return Err(AcpError::invalid_params(
            "steering _meta.steering must be an object".to_string(),
        ));
    }
    match steering.get("idleBehavior") {
        None => Ok(false),
        Some(Value::Null) => Ok(false),
        Some(Value::String(value)) if value == "promptRequired" => Ok(true),
        Some(Value::String(_)) => Err(AcpError::invalid_params(
            "unsupported steering idleBehavior".to_string(),
        )),
        Some(_) => Err(AcpError::invalid_params(
            "steering idleBehavior must be a string".to_string(),
        )),
    }
}

/// Resolve an ACP fork point (`_meta.jetbrains.air.forkPoint`) to an MSP
/// `cutPoint.lastTurnId`. `Ok(None)` means "all completed turns".
///
/// `messageId` mode finds the item in `session/read` history and takes its
/// turn (`userShell` items carry `turnId: null` and cannot bound a turn).
/// Fingerprint mode matches `agentMessage` text hashes and takes the
/// 1-based `messageOccurrence` among duplicates. An unresolved point fails
/// closed rather than silently forking the whole history.
async fn resolve_fork_cut_point(
    conn: &MspConnection,
    msp_sid: &str,
    params: &Value,
) -> Result<Option<String>, AcpError> {
    let fork_point = params
        .get("_meta")
        .and_then(|m| m.get("jetbrains"))
        .and_then(|j| j.get("air"))
        .and_then(|a| a.get("forkPoint"));
    let Some(fork_point) = fork_point else {
        return Ok(None);
    };
    let message_id = fork_point
        .get("messageId")
        .and_then(Value::as_str)
        .unwrap_or("");
    let fingerprint = fork_point
        .get("messageFingerprint")
        .and_then(Value::as_str)
        .unwrap_or("");
    if message_id.is_empty() && fingerprint.is_empty() {
        return Err(AcpError::invalid_params(
            "fork point needs a messageId or messageFingerprint".to_string(),
        ));
    }
    if !fingerprint.is_empty() && !is_well_formed_fingerprint(fingerprint) {
        return Err(AcpError::invalid_params(format!(
            "fork point fingerprint must be sha256:<64 hex chars>: {fingerprint}"
        )));
    }
    let read_params = json!({"sessionId": msp_sid, "excludeItems": false});
    let read = conn
        .command_with_retry("session/read", &read_params)
        .await
        .map_err(|e| AcpError::msp_command("session/read", &e))
        .map_err(|e| {
            AcpError::invalid_params(format!("fork point history read failed: {}", e.message))
        })?;
    let items = read
        .get("history")
        .and_then(|h| h.get("items"))
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| {
            AcpError::invalid_params("fork point history read returned no items".to_string())
        })?;
    if !message_id.is_empty() {
        let turn = items.iter().find_map(|item| {
            if item.get("itemId").and_then(Value::as_str) == Some(message_id) {
                Some(item.get("turnId").cloned().unwrap_or(Value::Null))
            } else {
                None
            }
        });
        return match turn {
            Some(Value::String(turn)) if !turn.is_empty() => Ok(Some(turn)),
            // userShell items carry turnId: null; they cannot bound a turn.
            Some(_) => Err(AcpError::invalid_params(format!(
                "fork point message {message_id} is not turn-scoped"
            ))),
            None => Err(AcpError::invalid_params(format!(
                "fork point message {message_id} not found in session history"
            ))),
        };
    }
    // Fingerprint mode (AIR): match agent-authored message text, then pick
    // the 1-based occurrence among duplicates. Only agentMessage items count.
    let occurrence = fork_point
        .get("messageOccurrence")
        .and_then(Value::as_u64)
        .filter(|v| *v >= 1)
        .unwrap_or(1) as usize;
    let matches: Vec<&str> = items
        .iter()
        .filter(|item| item.get("kind").and_then(Value::as_str) == Some("agentMessage"))
        .filter(|item| {
            item.get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| air_fingerprint(text) == fingerprint)
        })
        .filter_map(|item| item.get("turnId").and_then(Value::as_str))
        .filter(|turn| !turn.is_empty())
        .collect();
    match matches.get(occurrence - 1) {
        Some(turn) => Ok(Some(turn.to_string())),
        None if matches.is_empty() => Err(AcpError::invalid_params(
            "fork point fingerprint matched no agent message in session history".to_string(),
        )),
        None => Err(AcpError::invalid_params(format!(
            "fork point occurrence {occurrence} exceeds the {} matching message(s)",
            matches.len()
        ))),
    }
}

/// `sha256:<64 hex chars>` well-formedness (the AIR fingerprint shape).
fn is_well_formed_fingerprint(fingerprint: &str) -> bool {
    fingerprint.starts_with("sha256:") && {
        let hex = &fingerprint["sha256:".len()..];
        hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit())
    }
}

/// The AIR message fingerprint: `sha256:` + lowercase hex SHA-256 over the
/// agent-authored text bytes.
fn air_fingerprint(text: &str) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, text.as_bytes());
    let mut hex = String::with_capacity(2 * ring::digest::SHA256.output_len());
    for byte in digest.as_ref() {
        use std::fmt::Write as _;
        write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
    }
    format!("sha256:{hex}")
}

// ---------------------------------------------------------------------------
// Prompt extraction: ACP content blocks → MSP turn input
// ---------------------------------------------------------------------------

/// Build MSP turn input from an ACP `prompt` value: a content-block array
/// (bare strings accepted as text). Text blocks merge into runs; images ride
/// inline; embedded resource text is inlined; local `resource_link` files are
/// read (same machine) with a size cap. Audio has no host surface
/// (`TurnInputPartType` is closed: text|image) and is rejected.
///
/// Returns the host input plus the accepted prompt re-serialized as ACP
/// content for the user-message echo (client text verbatim — slash
/// normalization applies to host input only).
pub fn extract_prompt(prompt: Option<&Value>, cwd: &str) -> Result<(TurnInput, Value), AcpError> {
    let invalid = |message: String| AcpError::invalid_params(message);
    let blocks = match prompt {
        Some(Value::Array(blocks)) => blocks.clone(),
        Some(Value::String(text)) => vec![Value::String(text.clone())],
        _ => {
            return Err(invalid(
                "session/prompt requires a prompt array".to_string(),
            ));
        }
    };
    let mut parts: Vec<InputPart> = Vec::new();
    let mut texts: Vec<String> = Vec::new();
    let mut content: Vec<Value> = Vec::new();
    let flush_text = |texts: &mut Vec<String>, parts: &mut Vec<InputPart>| {
        if texts.is_empty() {
            return;
        }
        let text = normalize_muse_slash_command(&texts.join("\n"));
        // Merge with a preceding text run (text runs are contiguous).
        match parts.last_mut() {
            Some(InputPart::Text(run)) => {
                run.push('\n');
                run.push_str(&text);
            }
            _ => parts.push(InputPart::Text(text)),
        }
        texts.clear();
    };
    for block in &blocks {
        match block {
            Value::String(text) => {
                texts.push(text.clone());
                content.push(json!({"type": "text", "text": text}));
            }
            Value::Object(_) => {
                let kind = block.get("type").and_then(Value::as_str).unwrap_or("");
                match kind {
                    "text" => {
                        if let Some(text) = block.get("text").and_then(Value::as_str) {
                            texts.push(text.to_string());
                            content.push(json!({"type": "text", "text": text}));
                        }
                    }
                    "resource" => {
                        let resource = block.get("resource").unwrap_or(&Value::Null);
                        let uri = resource.get("uri").and_then(Value::as_str).unwrap_or("");
                        let mime = resource
                            .get("mimeType")
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        if let Some(text) = resource.get("text").and_then(Value::as_str) {
                            texts.push(text.to_string());
                            content.push(block.clone());
                        } else if let Some(blob) = resource.get("blob").and_then(Value::as_str) {
                            if !mime.starts_with("image/") {
                                return Err(invalid(
                                    "embedded non-image resource blobs are not supported; send text"
                                        .to_string(),
                                ));
                            }
                            flush_text(&mut texts, &mut parts);
                            parts.push(decode_image_part(blob, mime)?);
                            content.push(block.clone());
                        } else if !uri.is_empty() {
                            texts.push(format!("[resource: {uri}]"));
                            content.push(block.clone());
                        } else {
                            return Err(invalid(
                                "resource block needs resource.text, resource.blob, or resource.uri"
                                    .to_string(),
                            ));
                        }
                    }
                    "resource_link" => {
                        let uri = block.get("uri").and_then(Value::as_str).unwrap_or("");
                        let name = block.get("name").and_then(Value::as_str).unwrap_or(uri);
                        let mime = block.get("mimeType").and_then(Value::as_str).unwrap_or("");
                        if uri.is_empty() {
                            return Err(invalid("resource_link block needs uri".to_string()));
                        }
                        match local_file_text(uri, cwd) {
                            Some(text)
                                if mime.starts_with("text/")
                                    || mime.is_empty()
                                    || looks_textual(uri) =>
                            {
                                texts.push(format!("[{name} {uri}]\n{text}"));
                            }
                            _ => texts.push(format!("[resource: {name} ({uri})]")),
                        }
                        content.push(block.clone());
                    }
                    "image" => {
                        flush_text(&mut texts, &mut parts);
                        if let Some(data) = block.get("data").and_then(Value::as_str) {
                            let mime = block
                                .get("mimeType")
                                .and_then(Value::as_str)
                                .unwrap_or("image/png");
                            parts.push(decode_image_part(data, mime)?);
                            content.push(block.clone());
                        } else if let Some(uri) = block.get("uri").and_then(Value::as_str) {
                            parts.push(read_image_part(uri, cwd)?);
                            content.push(block.clone());
                        } else {
                            return Err(invalid("image block needs data or uri".to_string()));
                        }
                    }
                    "audio" => {
                        return Err(invalid(
                            "audio blocks are not supported: the host input type is closed (text|image)"
                                .to_string(),
                        ));
                    }
                    _ => {
                        return Err(invalid(format!("unsupported content block type '{kind}'")));
                    }
                }
            }
            _ => {
                return Err(invalid(
                    "prompt blocks must be objects or strings".to_string(),
                ));
            }
        }
    }
    flush_text(&mut texts, &mut parts);
    if parts.is_empty() {
        return Err(invalid("session/prompt requires content".to_string()));
    }
    Ok((
        TurnInput {
            parts,
            display_text: "acp prompt".to_string(),
            reasoning_effort: None,
        },
        Value::Array(content),
    ))
}

/// Whether the accepted echo content is the `/compact` protocol command: a
/// single text block whose trimmed text is exactly `/compact`.
fn is_compact_command(echo: &Value) -> bool {
    match echo {
        Value::Array(blocks) if blocks.len() == 1 => {
            let only = &blocks[0];
            only.get("type").and_then(Value::as_str) == Some("text")
                && only
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim()
                    == "/compact"
        }
        _ => false,
    }
}

/// Validate an inline base64 image part (mime + size + decodability — the
/// host rejects invalid base64 with `invalidParams`, so fail at admission).
fn decode_image_part(data: &str, mime: &str) -> Result<InputPart, AcpError> {
    if !mime.starts_with("image/") {
        return Err(AcpError::invalid_params(format!(
            "image block mimeType must be image/*, got '{mime}'"
        )));
    }
    let bytes = decode_b64(data).ok_or_else(|| {
        AcpError::invalid_params("image block data is not valid base64".to_string())
    })?;
    if bytes.len() as u64 > MAX_IMAGE_BYTES {
        return Err(AcpError::invalid_params(format!(
            "image is {} bytes, over the {MAX_IMAGE_BYTES}-byte cap",
            bytes.len()
        )));
    }
    Ok(InputPart::Image {
        base64: data.to_string(),
        media_type: mime.to_string(),
    })
}

/// Read a local image (`file://` URI or `/` path; same machine) into a
/// validated image part.
fn read_image_part(uri: &str, cwd: &str) -> Result<InputPart, AcpError> {
    let path = file_uri_path(uri, cwd).ok_or_else(|| {
        AcpError::invalid_params(format!("image uri is not a readable local file: {uri}"))
    })?;
    let bytes = std::fs::read(&path).map_err(|e| {
        AcpError::invalid_params(format!("cannot read image file {}: {e}", path.display()))
    })?;
    if bytes.len() as u64 > MAX_IMAGE_BYTES {
        return Err(AcpError::invalid_params(format!(
            "image is {} bytes, over the {MAX_IMAGE_BYTES}-byte cap",
            bytes.len()
        )));
    }
    Ok(InputPart::Image {
        base64: base64_encode(&bytes),
        media_type: mime_for(&path).to_string(),
    })
}

/// Strict base64 decode (no whitespace tolerance: reject, don't scrub).
fn decode_b64(data: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(data.as_bytes())
        .ok()
}

/// Standard base64 encode.
fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Read a local text file for a `resource_link` block: `file://` URIs and
/// `/`-rooted paths only, UTF-8, capped at [`RESOURCE_TEXT_CAP_BYTES`].
/// Anything unreadable/oversize/non-text returns `None` (the caller degrades
/// to a reference line, never an error).
fn local_file_text(uri: &str, cwd: &str) -> Option<String> {
    let path = file_uri_path(uri, cwd)?;
    let bytes = std::fs::read(path).ok()?;
    if bytes.len() as u64 > RESOURCE_TEXT_CAP_BYTES {
        return None;
    }
    String::from_utf8(bytes).ok()
}

/// Decode a `file://` URI (or accept a `/`-rooted path) to a local path.
/// Rejects hosts, non-file schemes, and bad escapes. Relative paths resolve
/// against the session `cwd`.
fn file_uri_path(uri: &str, cwd: &str) -> Option<std::path::PathBuf> {
    if let Some(rest) = uri.strip_prefix("file://") {
        if rest.starts_with('/') {
            percent_decode(rest).map(std::path::PathBuf::from)
        } else {
            // `file://host/...` or `file://relative`: hosts rejected, and
            // bare `file://` + relative is ambiguous — resolve the tail
            // against the cwd only when it carries no host-looking head.
            let (head, tail) = rest.split_once('/')?;
            if head.is_empty() || head == "localhost" {
                percent_decode(&format!("/{tail}")).map(std::path::PathBuf::from)
            } else if rest.contains('/') {
                None // a host-looking head: reject
            } else {
                percent_decode(rest).map(|p| std::path::Path::new(cwd).join(p))
            }
        }
    } else if let Some(rest) = uri.strip_prefix('/') {
        percent_decode(&format!("/{rest}")).map(std::path::PathBuf::from)
    } else if has_uri_scheme(uri) {
        None // non-file scheme: reject, never resolve against the cwd
    } else {
        percent_decode(uri).map(|p| std::path::Path::new(cwd).join(p))
    }
}

/// Whether a string leads with a URI scheme (`scheme:` per RFC 3986).
fn has_uri_scheme(text: &str) -> bool {
    let mut chars = text.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    for c in chars {
        if c == ':' {
            return true;
        }
        if !(c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.')) {
            return false;
        }
    }
    false
}

/// `%XX` decode; `None` on any bad escape (reject, don't scrub).
fn percent_decode(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return None;
            }
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Extension-sniffed image mime (matches `muse-acp`; non-images fall back
/// to `application/octet-stream` and fail mime validation upstream).
fn mime_for(path: &std::path::Path) -> &'static str {
    let name = path.to_string_lossy().to_lowercase();
    if name.ends_with(".png") {
        "image/png"
    } else if name.ends_with(".jpg") || name.ends_with(".jpeg") {
        "image/jpeg"
    } else if name.ends_with(".gif") {
        "image/gif"
    } else if name.ends_with(".webp") {
        "image/webp"
    } else {
        "application/octet-stream"
    }
}

/// Whether a URI looks textual by extension (for `resource_link` inlining).
fn looks_textual(uri: &str) -> bool {
    const TEXTUAL: &[&str] = &[
        ".txt",
        ".md",
        ".markdown",
        ".rst",
        ".json",
        ".jsonc",
        ".yaml",
        ".yml",
        ".toml",
        ".xml",
        ".html",
        ".htm",
        ".css",
        ".js",
        ".ts",
        ".tsx",
        ".jsx",
        ".rs",
        ".py",
        ".go",
        ".java",
        ".c",
        ".h",
        ".cpp",
        ".hpp",
        ".sh",
        ".log",
        ".diff",
        ".patch",
    ];
    let lower = uri.to_lowercase();
    let path = lower.split(['?', '#']).next().unwrap_or(&lower);
    TEXTUAL.iter().any(|ext| path.ends_with(ext))
}

/// Short editor commands map to Muse's stable skill invocation syntax.
/// Clients use a leading space to escape execution; only a slash in byte
/// position zero normalizes. (Ported from `muse-acp`.)
fn normalize_muse_slash_command(text: &str) -> String {
    if !text.starts_with('/') {
        return text.to_string();
    }
    let mut words = text.splitn(2, char::is_whitespace);
    let command = words.next().unwrap_or_default();
    let argument = words.next().unwrap_or_default().trim();
    match command {
        "/plan" | "/doctor" | "/create-skill" | "/create-plugin" | "/import" => {
            let skill = command.trim_start_matches('/');
            if argument.is_empty() {
                format!("/skill {skill}")
            } else {
                format!("/skill {skill} {argument}")
            }
        }
        _ => text.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_vocabularies_round_trip() {
        assert_eq!(mode_to_msp("ask"), Some("promptUnmatched"));
        assert_eq!(mode_to_msp("auto"), Some("allowAll"));
        assert_eq!(mode_to_msp("deny"), Some("denyUnmatched"));
        assert_eq!(mode_to_msp("bogus"), None);
        assert_eq!(mode_from_msp("allowAll"), "auto");
        assert_eq!(mode_from_msp("denyUnmatched"), "deny");
        assert_eq!(mode_from_msp("promptUnmatched"), "ask");
        assert_eq!(mode_from_msp("onRequest"), "ask");
        assert_eq!(mode_from_msp("future-mode"), "ask");
        assert_eq!(resolve_mode("ask"), Some("promptUnmatched"));
        assert_eq!(resolve_mode("onRequest"), Some("onRequest"));
        assert_eq!(resolve_mode("bogus"), None);
    }

    #[test]
    fn fallback_deny_picks_the_first_non_approving_choice() {
        let choices = |decisions: &[(&str, &str)]| -> Vec<Value> {
            decisions
                .iter()
                .map(|(id, decision)| json!({"choiceId": id, "decision": decision}))
                .collect()
        };
        // Approving decisions (any case, any `approv*` suffix) are skipped.
        let mixed = choices(&[
            ("c-always", "approveAlways"),
            ("c-allow", "APPROVE"),
            ("c-deny", "deny"),
            ("c-later", "deny"),
        ]);
        assert_eq!(fallback_deny_choice(&mixed), Some("c-deny"));
        // An open decision vocabulary counts as non-approving (fail closed).
        let future = choices(&[("c-future", "quarantine")]);
        assert_eq!(fallback_deny_choice(&future), Some("c-future"));
        // Choices without an id are skipped, not decided blind.
        let unlisted = choices(&[("", "deny"), ("c-real", "deny")]);
        assert_eq!(fallback_deny_choice(&unlisted), Some("c-real"));
        // All-approve (or empty) fails closed upstream: no choice returned.
        let all_approve = choices(&[("c-a", "approve"), ("c-b", "approveForSession")]);
        assert_eq!(fallback_deny_choice(&all_approve), None);
        assert_eq!(fallback_deny_choice(&[]), None);
        assert!(!is_approving_decision("deny"));
        assert!(is_approving_decision("ApproveOnce"));
    }

    #[test]
    fn user_input_cancel_reason_is_stable() {
        assert_eq!(USER_INPUT_CANCEL_REASON, "acp-fail-closed");
    }

    #[test]
    fn reasoning_tiers_cover_the_8_tier_vocab() {
        for tier in [
            "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
        ] {
            assert!(is_reasoning_effort(tier), "{tier}");
        }
        assert!(!is_reasoning_effort("extreme"));
        assert!(!is_reasoning_effort(""));
    }

    #[test]
    fn config_selectors_use_the_versioned_key() {
        let models = vec![("m1".to_string(), "M One".to_string())];
        for (ver, key) in [(1u8, "id"), (2u8, "configId")] {
            let options = config_options(ver, "ask", "m1", "medium", &models);
            let items = options.as_array().expect("selector array");
            assert_eq!(items.len(), 3);
            assert_eq!(items[0][key], Value::String("mode".to_string()));
            assert_eq!(items[1][key], Value::String("model".to_string()));
            assert_eq!(items[2][key], Value::String("reasoning_effort".to_string()));
            assert!(items[0].get("currentValue").is_some());
        }
        // Empty catalog degrades to an empty model selector, not an error.
        let options = config_options(1, "ask", "m1", "medium", &[]);
        assert_eq!(options[1]["options"], Value::Array(Vec::new()));
    }

    #[test]
    fn legacy_modes_carry_the_current_mode() {
        let modes = session_modes("deny");
        assert_eq!(modes["currentModeId"], Value::String("deny".to_string()));
        assert_eq!(modes["availableModes"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn available_commands_advertise_seven_skills() {
        for ver in [1u8, 2u8] {
            let commands = available_commands(ver);
            let items = commands.as_array().expect("command array");
            assert_eq!(items.len(), 7);
            assert_eq!(items[0]["name"], Value::String("skill".to_string()));
        }
        // v1 input is a bare hint; v2 wraps it as a typed object.
        assert_eq!(
            available_commands(1)[0]["input"],
            json!({"hint": "skill id and optional prompt"})
        );
        assert_eq!(
            available_commands(2)[0]["input"],
            json!({"type": "text", "hint": "skill id and optional prompt"})
        );
    }

    #[test]
    fn chunks_carry_message_id_only_on_v2() {
        let v1 = message_chunk_frame("s", 1, "m", "hi");
        assert_eq!(
            v1["params"]["update"],
            json!({"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "hi"}})
        );
        let v2 = message_chunk_frame("s", 2, "m", "hi");
        assert_eq!(
            v2["params"]["update"]["messageId"],
            Value::String("m".to_string())
        );
        let v1t = thought_chunk_frame("s", 1, "m", "hmm");
        assert!(v1t["params"]["update"].get("messageId").is_none());
        let v2t = thought_chunk_frame("s", 2, "m", "hmm");
        assert_eq!(
            v2t["params"]["update"]["messageId"],
            Value::String("m".to_string())
        );
        assert_eq!(v2["params"]["sessionId"], Value::String("s".to_string()));
        assert_eq!(v2["method"], Value::String("session/update".to_string()));
    }

    #[test]
    fn text_blocks_merge_and_slash_commands_normalize() {
        let prompt = json!([
            {"type": "text", "text": "/plan the cache"},
            "second line",
        ]);
        let (input, echo) = extract_prompt(Some(&prompt), "/tmp").expect("text prompt");
        assert_eq!(input.parts.len(), 1);
        assert_eq!(
            input.parts[0],
            InputPart::Text("/skill plan the cache\nsecond line".to_string())
        );
        // The echo keeps client text verbatim; normalization is host-only.
        assert_eq!(
            echo,
            json!([
                {"type": "text", "text": "/plan the cache"},
                {"type": "text", "text": "second line"},
            ])
        );
        // A leading space escapes normalization.
        let prompt = json!([{"type": "text", "text": " /plan the cache"}]);
        let (input, _) = extract_prompt(Some(&prompt), "/tmp").expect("escaped prompt");
        assert_eq!(
            input.parts[0],
            InputPart::Text(" /plan the cache".to_string())
        );
    }

    #[test]
    fn prompt_rejects_empty_audio_and_unknown_blocks() {
        assert!(extract_prompt(Some(&json!([])), "/tmp").is_err());
        assert!(extract_prompt(None, "/tmp").is_err());
        assert!(extract_prompt(Some(&json!({})), "/tmp").is_err());
        let err = extract_prompt(Some(&json!([{"type": "audio"}])), "/tmp").expect_err("audio");
        assert_eq!(err.code, -32602);
        let err = extract_prompt(Some(&json!([{"type": "video"}])), "/tmp").expect_err("video");
        assert!(err.message.contains("video"));
        let err = extract_prompt(Some(&json!([[1]])), "/tmp").expect_err("nested");
        assert_eq!(err.code, -32602);
    }

    #[test]
    fn inline_image_parts_validate_mime_and_base64() {
        let prompt = json!([{"type": "image", "data": "aGk=", "mimeType": "image/png"}]);
        let (input, echo) = extract_prompt(Some(&prompt), "/tmp").expect("image prompt");
        assert_eq!(echo, prompt, "non-text blocks echo verbatim");
        assert_eq!(
            input.parts[0],
            InputPart::Image {
                base64: "aGk=".to_string(),
                media_type: "image/png".to_string(),
            }
        );
        let bad = json!([{"type": "image", "data": "!!!", "mimeType": "image/png"}]);
        assert!(extract_prompt(Some(&bad), "/tmp").is_err());
        let bad_mime = json!([{"type": "image", "data": "aGk=", "mimeType": "text/plain"}]);
        assert!(extract_prompt(Some(&bad_mime), "/tmp").is_err());
        let no_source = json!([{"type": "image"}]);
        assert!(extract_prompt(Some(&no_source), "/tmp").is_err());
    }

    #[test]
    fn resource_blocks_inline_text_and_reference_uris() {
        let prompt = json!([
            {"type": "resource", "resource": {"uri": "mcp://x", "text": "inline body"}},
            {"type": "resource", "resource": {"uri": "mcp://y"}},
        ]);
        let (input, _) = extract_prompt(Some(&prompt), "/tmp").expect("resources");
        assert_eq!(
            input.parts[0],
            InputPart::Text("inline body\n[resource: mcp://y]".to_string())
        );
        let bare = json!([{"type": "resource", "resource": {}}]);
        assert!(extract_prompt(Some(&bare), "/tmp").is_err());
        let non_image = json!([
            {"type": "resource", "resource": {"mimeType": "application/pdf", "blob": "aGk="}},
        ]);
        assert!(extract_prompt(Some(&non_image), "/tmp").is_err());
    }

    #[test]
    fn resource_links_inline_local_files_and_degrade_gracefully() {
        let dir = std::env::temp_dir().join(format!(
            "muse-bridge-acp-link-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).expect("tmpdir");
        let file = dir.join("note.md");
        std::fs::write(&file, "file body").expect("seed");
        let uri = format!("file://{}", file.display());
        let prompt = json!([
            {"type": "resource_link", "uri": uri, "name": "note", "mimeType": "text/markdown"},
            {"type": "resource_link", "uri": "file:///does/not/exist.txt", "name": "missing"},
        ]);
        let (input, _) = extract_prompt(Some(&prompt), "/tmp").expect("links");
        let InputPart::Text(text) = &input.parts[0] else {
            panic!("links fold to text");
        };
        assert!(text.contains("file body"), "{text}");
        assert!(
            text.contains("[resource: missing (file:///does/not/exist.txt)]"),
            "{text}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn file_uri_paths_reject_hosts_and_bad_escapes() {
        assert_eq!(
            file_uri_path("file:///a/b.txt", "/cwd"),
            Some(std::path::PathBuf::from("/a/b.txt"))
        );
        assert_eq!(
            file_uri_path("file://localhost/a.txt", "/cwd"),
            Some(std::path::PathBuf::from("/a.txt"))
        );
        assert!(file_uri_path("file://evil/a.txt", "/cwd").is_none());
        assert!(file_uri_path("https://x/a.txt", "/cwd").is_none());
        assert!(file_uri_path("file:///a/%zz.txt", "/cwd").is_none());
        assert_eq!(
            file_uri_path("rel/note.md", "/cwd"),
            Some(std::path::PathBuf::from("/cwd/rel/note.md"))
        );
    }

    #[test]
    fn compact_detection_needs_one_bare_slash_command() {
        assert!(is_compact_command(
            &json!([{"type": "text", "text": "/compact"}])
        ));
        assert!(is_compact_command(
            &json!([{"type": "text", "text": "  /compact  "}])
        ));
        assert!(!is_compact_command(
            &json!([{"type": "text", "text": "/compact now"}])
        ));
        assert!(!is_compact_command(
            &json!([{"type": "text", "text": "/plan"}])
        ));
        assert!(!is_compact_command(&json!([
            {"type": "text", "text": "/compact"},
            {"type": "text", "text": "more"},
        ])));
        assert!(!is_compact_command(
            &json!([{"type": "image", "data": "x"}])
        ));
        assert!(!is_compact_command(&json!([])));
    }

    #[test]
    fn steering_gate_reads_the_idle_behavior() {
        assert!(!steering_prompt_required(&json!({})).unwrap());
        assert!(!steering_prompt_required(&json!({"_meta": null})).unwrap());
        assert!(!steering_prompt_required(&json!({"_meta": {}})).unwrap());
        assert!(
            !steering_prompt_required(&json!({"_meta": {"steering": {}}})).unwrap(),
            "absent idleBehavior may start a turn"
        );
        assert!(
            steering_prompt_required(
                &json!({"_meta": {"steering": {"idleBehavior": "promptRequired"}}})
            )
            .unwrap()
        );
        for bad in [
            json!({"_meta": "nope"}),
            json!({"_meta": {"steering": "nope"}}),
            json!({"_meta": {"steering": {"idleBehavior": "queue"}}}),
            json!({"_meta": {"steering": {"idleBehavior": 7}}}),
        ] {
            assert_eq!(steering_prompt_required(&bad).unwrap_err().code, -32602);
        }
    }

    #[test]
    fn air_fingerprints_match_the_reference_vectors() {
        // `sha256:` + hex over the raw text bytes (muse-acp `sha256::hex`).
        assert_eq!(
            air_fingerprint("abc"),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert!(is_well_formed_fingerprint(&air_fingerprint("hello")));
        assert!(!is_well_formed_fingerprint(
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        ));
        assert!(!is_well_formed_fingerprint("sha256:xyz"));
        assert!(!is_well_formed_fingerprint(""));
    }

    #[test]
    fn resume_validation_rejects_bad_roots_and_extra_dirs() {
        assert!(validate_opt_cwd(&json!({})).is_ok());
        assert!(validate_opt_cwd(&json!({"cwd": "/tmp/x"})).is_ok());
        assert!(validate_opt_cwd(&json!({"cwd": "relative"})).is_err());
        assert!(validate_opt_cwd(&json!({"cwd": ""})).is_err());
        assert!(reject_additional_dirs(&json!({})).is_ok());
        assert!(reject_additional_dirs(&json!({"additionalDirectories": []})).is_ok());
        assert!(reject_additional_dirs(&json!({"additionalDirectories": ["/x"]})).is_err());
    }

    #[test]
    fn history_replay_renders_messages_per_version() {
        let resumed = json!({
            "history": {"mode": "inline", "items": [
                {"itemId": "u1", "kind": "userMessage", "text": "q"},
                {"itemId": "a1", "kind": "agentMessage", "text": "a"},
                {"itemId": "t1", "kind": "toolCall", "tool": "shell",
                 "status": "completed", "fallbackText": "ran it"},
                {"itemId": "x1", "kind": "workflow", "text": "noise"},
            ]},
        });
        let v1 = replay_history("s", 1, &resumed);
        assert_eq!(v1.len(), 3);
        assert_eq!(
            v1[0]["params"]["update"]["sessionUpdate"],
            json!("user_message_chunk")
        );
        assert_eq!(
            v1[1]["params"]["update"]["sessionUpdate"],
            json!("agent_message_chunk")
        );
        assert!(v1[0]["params"]["update"].get("messageId").is_some());
        let v2 = replay_history("s", 2, &resumed);
        assert_eq!(v2.len(), 3);
        assert_eq!(
            v2[0]["params"]["update"]["sessionUpdate"],
            json!("user_message")
        );
        assert_eq!(
            v2[1]["params"]["update"]["sessionUpdate"],
            json!("agent_message")
        );
        assert_eq!(
            v2[2]["params"]["update"]["sessionUpdate"],
            json!("tool_call")
        );
        // Unknown shapes resume without replay, never fail.
        assert!(replay_history("s", 1, &json!({})).is_empty());
        assert!(replay_history("s", 1, &json!({"history": {"mode": "none"}})).is_empty());
    }

    #[test]
    fn user_echo_frames_carry_message_ids_on_both_versions() {
        let content = json!([{"type": "text", "text": "hi"}]);
        let full = user_message_frame("s", &content);
        assert_eq!(
            full["params"]["update"]["sessionUpdate"],
            json!("user_message")
        );
        assert!(full["params"]["update"].get("messageId").is_some());
        assert_eq!(full["params"]["update"]["content"], content);
        let chunk = user_message_chunk_frame("s", "m", &content[0]);
        assert_eq!(
            chunk["params"]["update"],
            json!({
                "sessionUpdate": "user_message_chunk",
                "messageId": "m",
                "content": {"type": "text", "text": "hi"},
            })
        );
    }
}
