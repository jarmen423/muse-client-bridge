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
//! protocol commands `/compact` `/help` `/status` `/usage` `/name` `/model`
//! `/exit` `/effort` `/recap` `/stop` `/goal` `/tasks` `/subagents`
//! `/workflows`), steering,
//! mode/model/effort selectors, gap
//! page+refold with lag catch-up, permission dialogs
//! (`session/request_permission`), user questions (`elicitation/create`
//! when the client advertises `elicitation.form`), dynamic skill commands
//! (`muse skills list`), and SPEC §6 error mapping. Approvals and user
//! input still fail closed without a client surface: a permission with no
//! deny choice cancels the turn, and unadvertised user input auto-cancels —
//! both loudly logged with the durable id.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{Value, json};
use tokio::sync::{Mutex, watch};

use crate::acp::errors::AcpError;
use crate::acp::server::{self, ClientRequests, Outbound};
use crate::acp::skills::{self, SkillEntry};
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
/// vocabulary: `ask|auto|yolo|deny` or a host mode). Invalid values fail
/// `session/new` loudly instead of falling back.
pub const APPROVAL_MODE_ENV: &str = "MUSE_APPROVAL_MODE";
/// Cap for inlining local `resource_link` file text into a prompt (1 MiB;
/// larger files degrade to a `[resource: …]` reference, never an error).
const RESOURCE_TEXT_CAP_BYTES: u64 = 1024 * 1024;
/// `userInput/cancel` reason when the client advertised no elicitation
/// surface (fail closed), loudly logged with the durable id.
const USER_INPUT_CANCEL_REASON: &str = "acp-fail-closed";
/// `userInput/cancel` reason when an elicitation was declined/dismissed.
const ELICITATION_DISMISSED_REASON: &str = "acp-elicitation-dismissed";
/// `userInput/cancel` reason when the elicitation reply errored out.
const ELICITATION_FAILED_REASON: &str = "acp-elicitation-failed";
/// `userInput/cancel` reason in yolo mode (the user opted out of
/// questions; the host decides how to proceed without answers).
const YOLO_NO_QUESTIONS_REASON: &str = "acp-yolo-no-questions";

/// One in-flight ACP prompt: the MSP turn plus its reply routing.
struct InFlight {
    /// MSP turn id (adopted from the `turn/start` ack, never derived).
    turn_id: String,
    /// ACP request id awaiting the `stopReason` reply (`None` for
    /// steering-started turns, which were already acked `{}`).
    req_id: Option<Value>,
    /// Cancel flag watched by the driver (set by `session/cancel`/`close`/`/stop`).
    cancel_tx: watch::Sender<bool>,
}

/// An MSP approval presented to the client as `session/request_permission`
/// and awaiting the reply (ported from `muse-acp` `PendingPerm`).
struct PendingPerm {
    /// ACP request id awaiting the client reply.
    req_id: Value,
    /// MSP `approvalId` (the decide target).
    approval_id: String,
    /// `currentRequirementId` verbatim (the multi-stage race guard).
    requirement: Value,
    /// The turn a fail-closed settle cancels.
    turn_id: String,
    /// `(choiceId, decision)` in host order; first reject-ish is the deny
    /// fallback.
    choices: Vec<(String, String)>,
}

/// One elicitation question's label mapping (ported from `muse-acp`).
struct UiQuestion {
    /// Host question id (answers key on it).
    qid: String,
    /// Original host labels (sent back in `userInput/answer`).
    labels: Vec<String>,
    /// Deduped display labels (shown to the client in the enum).
    display: Vec<String>,
}

/// An MSP `userInput` prompt presented as `elicitation/create` and awaiting
/// the reply (ported from `muse-acp` `PendingUi`).
struct PendingUi {
    /// ACP request id awaiting the client reply.
    req_id: Value,
    /// MSP `userInputId` (the answer/cancel target).
    user_input_id: String,
    /// Question label mappings, in host order.
    questions: Vec<UiQuestion>,
}

/// Session facts folded from session notifications (latest-wins; every
/// field stays `None` until a fact lands). Feeds the `/status`, `/usage`,
/// and `/recap` cards plus `plan`/`session_info_update`/`usage_update`
/// bridges.
#[derive(Default)]
struct SessionFacts {
    /// `session/nameChanged` / `session/rename` canonical name.
    name: Option<String>,
    /// `session/goalChanged` goal block (raw; `Null` is an explicit clear).
    goal: Option<Value>,
    /// `session/branchChanged` as the namespaced `{branch, vcs,
    /// workspaceRoot}` observation (`branch: null` = detached).
    branch: Option<Value>,
    /// `session/todoListChanged` items (raw list, replace wholesale).
    todos: Option<Vec<Value>>,
    /// `session/tokenUsage` cumulative totals (replace wholesale).
    cum_prompt: Option<u64>,
    /// Output tokens, session total.
    cum_output: Option<u64>,
    /// Counted-once total, session total.
    cum_total: Option<u64>,
    /// `session/contextUsage` occupancy.
    usage_used: Option<u64>,
    /// `session/contextUsage` window.
    usage_size: Option<u64>,
    /// `session/contextUsage` pressure.
    usage_pressure: Option<String>,
}

/// One observed model-spawned child (a `subagent` run or `workflow`
/// launch), folded from view items by the session observer. Retention is
/// session-scoped — children outlive the turns that spawned them, and the
/// observer (not the per-turn fold) owns it, so completions that land
/// while idle are kept too. Feeds the `/subagents` and `/workflows` cards
/// plus the live `tool_call` frames.
#[derive(Clone)]
struct ChildRecord {
    /// The view item kind, verbatim (`subagent` or `workflow`).
    kind: String,
    /// Display title (objective / script identity / fallback).
    title: String,
    /// MSP item `status` verbatim (`""` until the first status lands).
    status: String,
    /// Second line: control/child state plus the terminal message.
    detail: String,
    /// Highest folded `revision` (replace-iff-higher, like the turn fold).
    rev: u64,
    /// A terminal status (or `item/completed`) has been folded.
    terminal: bool,
    /// The item's durable `subagentId` (`""` when the host never sent
    /// one): the control target for the `subagent/*` verbs.
    subagent_id: String,
    /// The retained `result` envelope summary (`subagent` only).
    result_summary: String,
    /// The retained `result` envelope text (`subagent` only).
    result_text: String,
}

/// One ACP session: a persistent MSP session plus prompt state.
struct AcpSession {
    /// Server-minted MSP session id.
    msp_sid: String,
    /// Session working directory (`""` ⇒ provider mode, no `workspaceRoot`).
    cwd: String,
    /// Negotiated ACP protocol version (1 or 2: chunk/message shapes).
    ver: u8,
    /// Effective MSP approval mode, in our vocabulary (`ask|auto|yolo|deny`).
    mode: String,
    /// Last selected model id (`""` ⇒ server default).
    model_value: String,
    /// Reasoning effort sent with each prompt (the per-turn override the
    /// selector owns; `session/setReasoningEffort` keeps the host default
    /// in step so resumed sessions inherit it).
    reasoning_effort: String,
    /// The running turn, if any (adopted from acks and resume results).
    active_turn: Option<String>,
    /// Every admitted turn (the host queues concurrent turns itself; each
    /// completes its own prompt reply).
    in_flight: Vec<InFlight>,
    /// Registry skills for this workspace (slash aliases + palette).
    skills: Vec<SkillEntry>,
    /// The permission currently displayed to the client, if any.
    pending_perm: Option<PendingPerm>,
    /// Approvals queued behind the displayed one (raw request params;
    /// drained one at a time — ACP shows one permission per session).
    perm_queue: Vec<Value>,
    /// Elicitations awaiting client replies.
    pending_ui: Vec<PendingUi>,
    /// User-input ids already presented or auto-cancelled (resume reissues
    /// and the request/notification pair cannot replay a settled prompt).
    ui_seen: HashSet<String>,
    /// Folded session facts (name/goal/branch/todos/usage).
    facts: SessionFacts,
    /// Observed subagent/workflow children by view `itemId`.
    children: HashMap<String, ChildRecord>,
}

struct StoreInner {
    sessions: HashMap<String, AcpSession>,
}

/// ACP session table: persistent MSP sessions driven through the shared
/// [`Dispatcher`]'s supervisor, replying over the server's outbound channel.
pub struct SessionStore {
    dispatcher: Dispatcher,
    outbound: Outbound,
    /// Server→client request plane (`session/request_permission`,
    /// `elicitation/create`).
    client: Arc<ClientRequests>,
    /// Whether the client advertised `elicitation.form` at `initialize`
    /// (gates the userInput bridge; unset ⇒ auto-cancel).
    elicitation_form: AtomicBool,
    inner: Mutex<StoreInner>,
}

impl SessionStore {
    /// New store over a dispatcher (supervisor + model catalog) and the
    /// server's outbound frame channel.
    pub fn new(dispatcher: Dispatcher, outbound: Outbound) -> Self {
        Self {
            dispatcher,
            client: ClientRequests::new(outbound.clone()),
            elicitation_form: AtomicBool::new(false),
            outbound,
            inner: Mutex::new(StoreInner {
                sessions: HashMap::new(),
            }),
        }
    }

    /// Record the client's `elicitation.form` advertisement (initialize).
    pub fn set_elicitation_form(&self, supported: bool) {
        self.elicitation_form.store(supported, Ordering::SeqCst);
    }

    /// Route a no-method client frame to a pending server→client request.
    pub fn resolve_client_reply(&self, frame: &Value) -> bool {
        self.client.resolve(frame)
    }

    /// `session/new`: start one persistent MSP session and register it.
    /// Returns the ACP result object (`sessionId`, `_meta`, `configOptions`,
    /// plus legacy `modes` on v1).
    pub async fn create_session(
        self: &Arc<Self>,
        ver: u8,
        params: &Value,
    ) -> Result<Value, AcpError> {
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
        let (approval_mode, startup_label) = resolve_startup_mode()?;

        let conn = self.ready_conn().await?;
        let mut start_params = start_params(&conn, &approval_mode, workspace_root.as_deref());
        if let Some(config) = forward_client_mcp_servers(params) {
            // Ungranted hosts reject `config.mcpServers` at construction
            // (`capabilityRequired`); drop the servers loudly instead of
            // failing a session the client could otherwise use.
            if conn
                .handshake_info()
                .is_some_and(|info| info.supports_session_mcp())
            {
                start_params["config"] = config;
            } else {
                tracing::warn!(
                    "host did not grant sessionMcp; dropping client MCP servers for this session"
                );
            }
        }
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
        // Dynamic slash commands from the workspace's skill registry;
        // a listing failure degrades to the static command set.
        let skills = self
            .fetch_skills(
                &workspace_root
                    .as_deref()
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default(),
            )
            .await;

        let acp_sid = format!("acp-{}", uuid::Uuid::new_v4().simple());
        // Yolo survives only when the host actually folded allowAll; a
        // downgraded echo falls back to the folded label, loudly (above).
        let mode_vocab = if startup_label == "yolo" && mode == "allowAll" {
            "yolo".to_string()
        } else {
            mode_from_msp(&mode).to_string()
        };
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
                    skills: skills.clone(),
                    pending_perm: None,
                    perm_queue: Vec::new(),
                    pending_ui: Vec::new(),
                    ui_seen: HashSet::new(),
                    facts: SessionFacts {
                        name: session
                            .get("name")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        branch: session.get("branch").map(|b| json!({"branch": b.clone()})),
                        ..SessionFacts::default()
                    },
                    children: HashMap::new(),
                },
            );
        }
        self.spawn_observer(&acp_sid);
        tracing::info!(acp_sid, msp_sid, mode, "acp session created");
        // Selectors ride `configOptions` (Zed negotiates v1 and renders
        // one button per option); `modes` rides along on v1 for clients
        // on the ModeSelector path (Zed itself prefers configOptions and
        // ignores it — verified against Zed 1.19.2 `config_state`).
        let mut result = json!({
            "sessionId": acp_sid,
            "_meta": {"mspSessionId": msp_sid},
            "configOptions": config_options(ver, &mode_vocab, &model, DEFAULT_REASONING_EFFORT, &models),
        });
        if ver != 2 {
            result["modes"] = session_modes(&mode_vocab);
        }
        // Slash-command advertisement goes out after the result frame (see
        // `advertise_commands`): the client only routes `session/update`
        // once the response arrives.
        Ok(result)
    }

    /// Advertise slash commands for a session AFTER its result frame is on
    /// the wire. Zed registers update routing only once the `session/new`
    /// (or resume/load/fork) response arrives, so an update sent before
    /// the result is dropped and `/` stays empty (verified against Zed
    /// 1.19.2 `agent_servers`; both references send result first).
    /// Best effort: a closed channel means the writer is gone and the
    /// process is exiting.
    pub async fn advertise_commands(&self, acp_sid: &str) {
        let (ver, skills) = {
            let inner = self.inner.lock().await;
            match inner.sessions.get(acp_sid) {
                Some(s) => (s.ver, s.skills.clone()),
                None => return,
            }
        };
        let _ = self
            .outbound
            .send(available_commands_frame(acp_sid, ver, &skills))
            .await;
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
            Ok(PromptAdmit::Settled) => {}
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

    /// Admit a prompt: validate, run protocol commands inline or
    /// `turn/start` and register in-flight. Every admitted turn is tracked
    /// (the host queues concurrent turns itself); each completes its own
    /// prompt reply.
    async fn prompt_admit(
        self: &Arc<Self>,
        acp_sid: &str,
        req_id: &Value,
        params: &Value,
    ) -> Result<PromptAdmit, AcpError> {
        if acp_sid.is_empty() {
            return Err(AcpError::invalid_params(
                "session/prompt requires params.sessionId".to_string(),
            ));
        }
        let (msp_sid, cwd, ver, effort, skills) = {
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
                session.skills.clone(),
            )
        };
        let (input, echo) = extract_prompt(params.get("prompt"), &cwd, &skills)?;
        // Protocol commands run inline and settle immediately — never a
        // turn (the host sees nothing).
        if let Some((command, arg)) = protocol_command(&echo) {
            self.run_protocol_command(acp_sid, &msp_sid, ver, req_id, command, &arg)
                .await?;
            return Ok(PromptAdmit::Settled);
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

    /// Run one protocol command: produce the card (host calls where the
    /// command maps to MSP), then echo + card + `end_turn` — never a turn.
    /// `/exit` additionally closes the session after settling. Read cards
    /// degrade to an error line; mutating commands propagate host errors.
    async fn run_protocol_command(
        self: &Arc<Self>,
        acp_sid: &str,
        msp_sid: &str,
        ver: u8,
        req_id: &Value,
        command: ProtocolCommand,
        arg: &str,
    ) -> Result<(), AcpError> {
        let echo_text = format!("/{}{}", command.name(), format_args_for(arg));
        let card = match command {
            ProtocolCommand::Compact => self.run_compact(acp_sid, msp_sid).await?,
            ProtocolCommand::Help => self.help_card(acp_sid).await,
            ProtocolCommand::Status => self.status_card(acp_sid, msp_sid).await,
            ProtocolCommand::Usage => self.usage_card(acp_sid).await,
            ProtocolCommand::Name => self.run_name(acp_sid, msp_sid, arg).await?,
            ProtocolCommand::Models => self.run_models(acp_sid, msp_sid, arg).await?,
            ProtocolCommand::Exit => "Session closed.".to_string(),
            ProtocolCommand::Effort => self.run_effort(acp_sid, msp_sid, arg).await?,
            ProtocolCommand::Recap => self.recap_card(acp_sid, msp_sid).await,
            ProtocolCommand::Stop => self.run_stop(acp_sid).await,
            ProtocolCommand::Goal => self.goal_card(acp_sid).await,
            ProtocolCommand::Tasks => self.tasks_card(acp_sid).await,
            ProtocolCommand::Subagents => self.run_subagents(acp_sid, msp_sid, arg).await?,
            ProtocolCommand::Workflows => self.run_workflows(acp_sid, arg).await,
        };
        self.settle_command(acp_sid, ver, req_id, &echo_text, &card)
            .await;
        if command == ProtocolCommand::Exit {
            tracing::info!(acp_sid, "/exit closed the session");
            let _ = self.close(&json!({"sessionId": acp_sid})).await;
        }
        Ok(())
    }

    /// Echo the command + emit the card + settle `end_turn` (the v1/v2
    /// framing differs, the sequence does not).
    async fn settle_command(
        &self,
        acp_sid: &str,
        ver: u8,
        req_id: &Value,
        echo_text: &str,
        card: &str,
    ) {
        let echo = Value::Array(vec![json!({"type": "text", "text": echo_text})]);
        if ver == 2 {
            self.send(server::result_frame(req_id, json!({}))).await;
            self.send(user_message_frame(acp_sid, &echo)).await;
            if !card.is_empty() {
                let msg_id = format!("msg-{}", uuid::Uuid::new_v4().simple());
                self.send(message_chunk_frame(acp_sid, ver, &msg_id, card))
                    .await;
            }
            self.send(state_update_frame(acp_sid, "idle", Some("end_turn")))
                .await;
        } else {
            let msg_id = format!("msg-{}", uuid::Uuid::new_v4().simple());
            if let Value::Array(blocks) = &echo {
                for block in blocks {
                    self.send(user_message_chunk_frame(acp_sid, &msg_id, block))
                        .await;
                }
            }
            if !card.is_empty() {
                self.send(message_chunk_frame(acp_sid, ver, &msg_id, card))
                    .await;
            }
            self.send(server::result_frame(
                req_id,
                json!({"stopReason": "end_turn"}),
            ))
            .await;
        }
    }

    /// `/compact`: `session/compact` (a `noop` status is success, logged
    /// with its reason); the card says what happened.
    async fn run_compact(&self, acp_sid: &str, msp_sid: &str) -> Result<String, AcpError> {
        let conn = self.ready_conn().await?;
        let params = json!({
            "commandId": conn.mint_command_id(),
            "sessionId": msp_sid,
        });
        let ack = conn
            .command_with_retry("session/compact", &params)
            .await
            .map_err(|e| AcpError::msp_command("session/compact", &e))?;
        let card = if ack.get("status").and_then(Value::as_str) == Some("noop") {
            // Hoisted: `tracing::Value` shadows `serde_json::Value` inside
            // the macro expansion.
            let reason = ack.get("reason").and_then(Value::as_str).unwrap_or("?");
            tracing::info!(acp_sid, reason, "compact noop");
            format!("Nothing to compact ({reason}).")
        } else {
            "Compacting the session context.".to_string()
        };
        tracing::info!(acp_sid, "compact command settled");
        Ok(card)
    }

    /// `/help`: the advertised command list plus the client gestures.
    async fn help_card(&self, acp_sid: &str) -> String {
        let skills = {
            let inner = self.inner.lock().await;
            inner
                .sessions
                .get(acp_sid)
                .map(|s| s.skills.clone())
                .unwrap_or_default()
        };
        let mut out = String::from("**Commands**\n\n");
        for (name, description, hint) in PROTOCOL_COMMANDS {
            out.push_str(&format!("- `/{name}` — {description}\n"));
            let _ = hint;
        }
        out.push_str("- `/skill <id> [prompt]` — Invoke a Muse skill\n");
        for skill in &skills {
            if skill.id.is_empty() {
                continue;
            }
            let desc = if skill.description.is_empty() {
                "Muse skill"
            } else {
                skill.description.as_str()
            };
            out.push_str(&format!("- `/{}` — {desc}\n", skill.id));
        }
        out.push_str(
            "\n**Selectors** — Session Mode (ask/auto/deny), Model, and Reasoning Effort \
             ride the client's configOptions pickers. Prompts sent while a turn runs \
             queue; v2 clients can also steer the running turn.\n",
        );
        out
    }

    /// `/status`: session/read for metadata plus folded facts and host
    /// handshake details. Degrades to an error card on failure.
    async fn status_card(&self, acp_sid: &str, msp_sid: &str) -> String {
        let conn = match self.ready_conn().await {
            Ok(conn) => conn,
            Err(error) => return format!("Status unavailable: {}", error.message),
        };
        let read = conn
            .command_with_retry(
                "session/read",
                &json!({
                    "commandId": conn.mint_command_id(),
                    "sessionId": msp_sid,
                }),
            )
            .await;
        let (mode, model, effort, cwd, in_flight, facts) = {
            let inner = self.inner.lock().await;
            match inner.sessions.get(acp_sid) {
                Some(s) => (
                    s.mode.clone(),
                    s.model_value.clone(),
                    s.reasoning_effort.clone(),
                    s.cwd.clone(),
                    s.in_flight.len(),
                    clone_facts(&s.facts),
                ),
                None => return "Status unavailable: unknown session.".to_string(),
            }
        };
        let mut out = String::from("**Status**\n\n");
        match read {
            Ok(result) => {
                let session = result.get("session").unwrap_or(&Value::Null);
                let name = session
                    .get("name")
                    .and_then(Value::as_str)
                    .or(facts.name.as_deref())
                    .unwrap_or("untitled");
                let turns = session
                    .get("turnCount")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let status = session.get("status").and_then(Value::as_str).unwrap_or("?");
                let updated = session
                    .get("updatedAt")
                    .and_then(Value::as_str)
                    .unwrap_or("?");
                out.push_str(&format!(
                    "- Session: {name} (`{msp_sid}`) · {status} · {turns} turns · updated {updated}\n"
                ));
                if let Some(branch) = session.get("branch").and_then(Value::as_str) {
                    out.push_str(&format!("- Branch: {branch}\n"));
                }
                if let Some(path) = session.get("path").and_then(Value::as_str)
                    && !path.is_empty()
                {
                    out.push_str(&format!("- Log: {path}\n"));
                }
                let pending = result
                    .get("pendingRequests")
                    .and_then(Value::as_array)
                    .map(|p| p.len())
                    .unwrap_or(0);
                if pending > 0 {
                    out.push_str(&format!("- Pending host requests: {pending}\n"));
                }
            }
            Err(error) => {
                out.push_str(&format!(
                    "- Session `{msp_sid}` (read failed: {})\n",
                    error.message
                ));
            }
        }
        out.push_str(&format!(
            "- Model: {} · Effort: {effort}\n",
            display_model(&model)
        ));
        out.push_str(&format!("- Approval mode: {mode}\n"));
        if !cwd.is_empty() {
            out.push_str(&format!("- Workspace: {cwd}\n"));
        }
        if let Some(info) = conn.handshake_info() {
            out.push_str(&format!(
                "- Host: {} · schema v{} · {}\n",
                info.host_label(),
                info.schema_version
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "?".to_string()),
                info.durability.as_deref().unwrap_or("durable?"),
            ));
        }
        if in_flight > 0 {
            out.push_str(&format!("- In-flight turns: {in_flight}\n"));
        }
        if let Some(goal) = facts.goal.as_ref().and_then(goal_summary) {
            out.push_str(&format!("- Goal: {goal}\n"));
        }
        if let Some(branch) = facts
            .branch
            .as_ref()
            .and_then(|b| b.get("branch"))
            .and_then(Value::as_str)
        {
            out.push_str(&format!("- Branch: {branch}\n"));
        }
        append_usage_lines(&mut out, &facts);
        out
    }

    /// `/usage`: the folded token/context facts card.
    async fn usage_card(&self, acp_sid: &str) -> String {
        let facts = {
            let inner = self.inner.lock().await;
            match inner.sessions.get(acp_sid) {
                Some(s) => clone_facts(&s.facts),
                None => return "Usage unavailable: unknown session.".to_string(),
            }
        };
        let mut out = String::from("**Usage**\n\n");
        if facts.cum_total.is_none() && facts.usage_used.is_none() {
            out.push_str("No usage reported yet — send a prompt first.\n");
            return out;
        }
        append_usage_lines(&mut out, &facts);
        out
    }

    /// `/stop`: best-effort `turn/cancel` for every in-flight turn —
    /// the same settle as `session/cancel`, as an inline card. The
    /// command itself never starts a turn, so it cannot cancel itself.
    async fn run_stop(&self, acp_sid: &str) -> String {
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
            return "No in-flight turn to stop.".to_string();
        }
        let stopped = turns.len();
        self.cancel_turns(acp_sid, turns).await;
        tracing::info!(acp_sid, stopped, "/stop cancelled in-flight turns");
        if stopped == 1 {
            "Stopped 1 turn.".to_string()
        } else {
            format!("Stopped {stopped} turns.")
        }
    }

    /// `/goal`: the folded `session/goalChanged` block, read-only.
    async fn goal_card(&self, acp_sid: &str) -> String {
        let goal = {
            let inner = self.inner.lock().await;
            match inner.sessions.get(acp_sid) {
                Some(s) => s.facts.goal.clone(),
                None => return "Goal unavailable: unknown session.".to_string(),
            }
        };
        render_goal_card(&goal)
    }

    /// `/tasks`: the folded `session/todoListChanged` list, read-only.
    async fn tasks_card(&self, acp_sid: &str) -> String {
        let todos = {
            let inner = self.inner.lock().await;
            match inner.sessions.get(acp_sid) {
                Some(s) => s.facts.todos.clone(),
                None => return "Tasks unavailable: unknown session.".to_string(),
            }
        };
        render_tasks_card(&todos)
    }

    /// `/subagents`: retained model-spawned subagents, read-only. The
    /// card always closes with the verb list — the palette entry can't
    /// name them all, and undiscoverable verbs are dead verbs.
    async fn subagents_card(&self, acp_sid: &str) -> String {
        let children = {
            let inner = self.inner.lock().await;
            match inner.sessions.get(acp_sid) {
                Some(s) => s.children.clone(),
                None => return "Subagents unavailable: unknown session.".to_string(),
            }
        };
        let mut card = render_children_card(
            "Subagents",
            "subagent",
            &children,
            "No subagents observed yet this session.",
        );
        card.push_str(
            "\nVerbs: stop, interrupt, close, resume, reopen, message <text>, followup <text>, \
             result — `/subagents <verb> [target]`, or pick from the selector.\n",
        );
        card
    }

    /// Pick one retained child via elicitation over `options` (all
    /// subagents, or an ambiguous subset). Returns the picked item id
    /// plus the body: the args text when given, else the form's text
    /// field when the verb needs one. `None` on no-surface/dismiss.
    async fn pick_subagent(
        &self,
        acp_sid: &str,
        verb: &SubagentVerb,
        options: &[(String, String)],
        body_arg: &str,
    ) -> Option<(String, String)> {
        let message = format!("Select a subagent to {}", verb.name);
        if verb.needs_body && body_arg.is_empty() {
            self.elicit_select_with_text(acp_sid, &message, options, "text")
                .await
        } else {
            let id = self.elicit_choice(acp_sid, &message, options).await?;
            Some((id, body_arg.to_string()))
        }
    }

    /// `/subagents [verb] [target] [text]`: bare lists the retained
    /// children; otherwise resolve the target (picker fallback when the
    /// client has a form surface) and run one control verb. User errors
    /// (unknown verb/target, missing body) return cards; host failures
    /// propagate like the other mutating commands. Every verb reports
    /// admission — the outcome lands in the child's block (even
    /// `result`, whose content rides the view stream).
    async fn run_subagents(
        &self,
        acp_sid: &str,
        msp_sid: &str,
        arg: &str,
    ) -> Result<String, AcpError> {
        let arg = arg.trim();
        if arg.is_empty() {
            return Ok(self.subagents_card(acp_sid).await);
        }
        let (verb_word, rest) = match arg.split_once(char::is_whitespace) {
            Some((v, r)) => (v, r.trim()),
            None => (arg, ""),
        };
        let Some(verb) = parse_subagent_verb(verb_word) else {
            return Ok(subagents_usage());
        };
        let children = {
            let inner = self.inner.lock().await;
            match inner.sessions.get(acp_sid) {
                Some(s) => s.children.clone(),
                None => return Ok("Subagents unavailable: unknown session.".to_string()),
            }
        };
        if !children.values().any(|c| c.kind == "subagent") {
            return Ok(self.subagents_card(acp_sid).await);
        }
        let (target_word, body_arg) = match rest.split_once(char::is_whitespace) {
            Some((t, b)) => (t, b.trim()),
            None => (rest, ""),
        };
        // Resolve the target: the picker covers nothing-typed and
        // ambiguity; a specific miss stays an error card (never a guess,
        // and never dropping typed body text into a picker).
        let subagent_rows: Vec<(&str, &ChildRecord)> = children
            .iter()
            .filter(|(_, c)| c.kind == "subagent")
            .map(|(id, c)| (id.as_str(), c))
            .collect();
        let (item_id, body) = if target_word.is_empty() {
            let options = subagent_options(&subagent_rows);
            match self.pick_subagent(acp_sid, verb, &options, body_arg).await {
                Some(picked) => picked,
                None => {
                    let mut card = self.subagents_card(acp_sid).await;
                    card.push_str(&format!(
                        "\nPick a target: `/subagents {} <id>`.",
                        verb.name
                    ));
                    return Ok(card);
                }
            }
        } else {
            match resolve_subagent_target(&children, target_word) {
                ChildTarget::One(id) => (id, body_arg.to_string()),
                ChildTarget::None => {
                    return Ok(format!(
                        "No subagent matches '{target_word}'.\n\n{}",
                        self.subagents_card(acp_sid).await
                    ));
                }
                ChildTarget::WorkflowOnly => {
                    return Ok(format!(
                        "'{target_word}' matches a workflow run — workflows have no control \
                         methods (MSP exposes none), so verbs are subagent-only."
                    ));
                }
                ChildTarget::Many(cands) => {
                    let rows: Vec<(&str, &ChildRecord)> =
                        cands.iter().map(|(id, c)| (id.as_str(), c)).collect();
                    let options = subagent_options(&rows);
                    match self.pick_subagent(acp_sid, verb, &options, body_arg).await {
                        Some(picked) => picked,
                        None => {
                            let titles: Vec<String> = cands
                                .iter()
                                .map(|(_, c)| format!("- {}", c.title))
                                .collect();
                            return Ok(format!(
                                "'{target_word}' matches several subagents — be more specific:\n{}",
                                titles.join("\n")
                            ));
                        }
                    }
                }
            }
        };
        // Re-read retention: the picker round-trip may have folded fresher
        // state (notably a result that landed while choosing).
        let record = {
            let inner = self.inner.lock().await;
            inner
                .sessions
                .get(acp_sid)
                .and_then(|s| s.children.get(&item_id))
                .cloned()
        };
        let Some(record) = record else {
            return Ok(self.subagents_card(acp_sid).await);
        };
        // `result` renders retained truth; consume (`readResult` is
        // state-changing) only when nothing is kept.
        if verb.method == "subagent/readResult"
            && (!record.result_summary.is_empty() || !record.result_text.is_empty())
        {
            return Ok(render_result_card(&record.title, &record));
        }
        // A missing body gets one text prompt when the client has a
        // form surface; otherwise (or on empty/dismissed) usage — never
        // a host round-trip the server would reject.
        let mut body = body.trim().to_string();
        if verb.needs_body && body.is_empty() {
            let prompt = format!("Message text for '{}' ({})", record.title, verb.name);
            match self.elicit_text(acp_sid, &prompt, "text").await {
                Some(text) if !text.trim().is_empty() => {
                    body = text.trim().to_string();
                }
                _ => {
                    return Ok(format!(
                        "`/subagents {} <target> <text>` needs message text.\n",
                        verb.name
                    ));
                }
            }
        }
        if record.subagent_id.is_empty() {
            return Ok(format!(
                "'{}' has no durable subagent id — the host never reported one, so it cannot \
                 be controlled.",
                record.title
            ));
        }
        let conn = self.ready_conn().await?;
        let mut params = json!({
            "commandId": conn.mint_command_id(),
            "sessionId": msp_sid,
            "subagentId": record.subagent_id,
        });
        if verb.needs_body {
            params["body"] = Value::String(body);
        } else if verb.takes_reason {
            params["reason"] = Value::String(format!(
                "/subagents {} '{}'",
                verb.name,
                truncate(&record.title, 80)
            ));
        }
        let ack = conn
            .command_with_retry(verb.method, &params)
            .await
            .map_err(|e| AcpError::msp_command(verb.method, &e))?;
        let status = ack
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("accepted");
        Ok(format!(
            "{} for '{}' ({status}) — the outcome lands in its block.",
            verb.ack, record.title
        ))
    }

    /// `/workflows`: retained workflow runs, read-only.
    async fn workflows_card(&self, acp_sid: &str) -> String {
        let children = {
            let inner = self.inner.lock().await;
            match inner.sessions.get(acp_sid) {
                Some(s) => s.children.clone(),
                None => return "Workflows unavailable: unknown session.".to_string(),
            }
        };
        render_children_card(
            "Workflows",
            "workflow",
            &children,
            "No workflow runs observed yet this session.",
        )
    }

    /// `/workflows [anything]`: the card; arguments are refused loudly —
    /// runs are display-only (MSP exposes no workflow methods).
    async fn run_workflows(&self, acp_sid: &str, arg: &str) -> String {
        let mut card = self.workflows_card(acp_sid).await;
        if !arg.trim().is_empty() {
            card.push_str(
                "\nWorkflow runs take no arguments — they are display-only (MSP exposes no \
                 workflow control methods).\n",
            );
        }
        card
    }

    /// `/name [name]`: `session/rename` on an argument, or the current name.
    async fn run_name(&self, acp_sid: &str, msp_sid: &str, arg: &str) -> Result<String, AcpError> {
        if arg.is_empty() {
            let known = {
                let inner = self.inner.lock().await;
                inner
                    .sessions
                    .get(acp_sid)
                    .and_then(|s| s.facts.name.clone())
            };
            return Ok(match known {
                Some(name) => format!("Session name: {name}"),
                None => "Session name: untitled — `/name <name>` sets it.".to_string(),
            });
        }
        let conn = self.ready_conn().await?;
        let ack = conn
            .command_with_retry(
                "session/rename",
                &json!({
                    "commandId": conn.mint_command_id(),
                    "sessionId": msp_sid,
                    "name": arg,
                }),
            )
            .await
            .map_err(|e| AcpError::msp_command("session/rename", &e))?;
        // The canonical name arrives in the result or via nameChanged.
        let settled = ack.get("name").and_then(Value::as_str).unwrap_or(arg);
        {
            let mut inner = self.inner.lock().await;
            if let Some(s) = inner.sessions.get_mut(acp_sid) {
                s.facts.name = Some(settled.to_string());
            }
        }
        Ok(format!("Session renamed to {settled}."))
    }

    /// Send one `elicitation/create` form and await the accepted
    /// `content` object. `None` when the client has no form surface, the
    /// request errors, or the user declines/dismisses/cancels — callers
    /// fall back to their card. Shared mechanics for the command
    /// pickers; commands run on spawned handler tasks, so awaiting the
    /// reply cannot wedge the read loop (no turn runs, so no state
    /// updates are sent).
    async fn elicit_form_raw(
        &self,
        acp_sid: &str,
        message: &str,
        properties: Value,
        required: &[&str],
    ) -> Option<Value> {
        if !self.elicitation_form.load(Ordering::SeqCst) {
            return None;
        }
        let params = json!({
            "sessionId": acp_sid,
            "mode": "form",
            "message": message,
            "requestedSchema": {
                "type": "object",
                "properties": properties,
                "required": required,
            },
        });
        let (_req_id, rx) = self
            .client
            .request("elic", params, "elicitation/create")
            .await;
        let reply = rx.await.ok()?;
        if reply.get("error").is_some() {
            return None;
        }
        let accepted = reply
            .get("result")
            .and_then(|r| r.get("action"))
            .and_then(Value::as_str)
            == Some("accept");
        if !accepted {
            tracing::info!(
                acp_sid,
                "command elicitation dismissed; falling back to the card"
            );
            return None;
        }
        reply.get("result").and_then(|r| r.get("content")).cloned()
    }

    /// Offer the client a single-select elicitation form and await the
    /// choice. `options` are (value, label) pairs; the form shows labels
    /// and the reply maps back by position (labels dedupe like user
    /// questions). Same fallback contract as [`elic_form_raw`], plus
    /// `None` on empty options or answers outside the list.
    async fn elicit_choice(
        &self,
        acp_sid: &str,
        message: &str,
        options: &[(String, String)],
    ) -> Option<String> {
        if options.is_empty() {
            return None;
        }
        let display = dedupe_labels(options);
        let content = self
            .elicit_form_raw(
                acp_sid,
                message,
                json!({"choice": {"type": "string", "enum": display}}),
                &["choice"],
            )
            .await?;
        let picked = content.get("choice").and_then(Value::as_str)?;
        // Map back by position; answers outside the list never apply.
        map_position(&display, picked).and_then(|i| options.get(i).map(|(v, _)| v.clone()))
    }

    /// Offer a select + free-text elicitation form (a child picker plus a
    /// body field for the `message`/`followup` verbs) and await both
    /// answers. Returns the mapped value and the body text, or `None`
    /// under the same contract as [`elicit_choice`].
    async fn elicit_select_with_text(
        &self,
        acp_sid: &str,
        message: &str,
        options: &[(String, String)],
        text_field: &str,
    ) -> Option<(String, String)> {
        if options.is_empty() {
            return None;
        }
        let display = dedupe_labels(options);
        let mut properties = serde_json::Map::new();
        properties.insert(
            "choice".to_string(),
            json!({"type": "string", "enum": display}),
        );
        properties.insert(text_field.to_string(), json!({"type": "string"}));
        let content = self
            .elicit_form_raw(
                acp_sid,
                message,
                Value::Object(properties),
                &["choice", text_field],
            )
            .await?;
        let picked = content.get("choice").and_then(Value::as_str)?;
        let body = content
            .get(text_field)
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        map_position(&display, picked)
            .and_then(|i| options.get(i).map(|(v, _)| v.clone()))
            .map(|value| (value, body))
    }

    /// Offer a single free-text elicitation form and await the answer.
    /// `None` under the same contract as [`elicit_choice`].
    async fn elicit_text(&self, acp_sid: &str, message: &str, text_field: &str) -> Option<String> {
        let mut properties = serde_json::Map::new();
        properties.insert(text_field.to_string(), json!({"type": "string"}));
        let content = self
            .elicit_form_raw(acp_sid, message, Value::Object(properties), &[text_field])
            .await?;
        Some(
            content
                .get(text_field)
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        )
    }

    /// `/models` (or `/model <id>`): list the catalog, or switch models.
    async fn run_models(
        &self,
        acp_sid: &str,
        msp_sid: &str,
        arg: &str,
    ) -> Result<String, AcpError> {
        if !arg.is_empty() {
            return self.run_model_select(acp_sid, msp_sid, arg).await;
        }
        let catalog = self
            .dispatcher
            .models()
            .await
            .map_err(|e| AcpError::internal(format!("model/list failed: {e}")))?;
        // Bare `/models` offers the picker form when the client can show
        // one; anything else falls through to the list card.
        if !catalog.models.is_empty() {
            let options: Vec<(String, String)> = catalog
                .models
                .iter()
                .map(|m| {
                    (
                        m.id.clone(),
                        m.display_label.clone().unwrap_or_else(|| m.id.clone()),
                    )
                })
                .collect();
            if let Some(id) = self
                .elicit_choice(acp_sid, "Select a model", &options)
                .await
            {
                return self.run_model_select(acp_sid, msp_sid, &id).await;
            }
        }
        let current = {
            let inner = self.inner.lock().await;
            inner
                .sessions
                .get(acp_sid)
                .map(|s| s.model_value.clone())
                .unwrap_or_default()
        };
        let mut out = String::from("**Models**\n\n");
        if catalog.models.is_empty() {
            out.push_str("No models reported by the host.\n");
        }
        for model in &catalog.models {
            let marker = if model.id == current {
                " (current)"
            } else {
                ""
            };
            let label = model.display_label.as_deref().unwrap_or(&model.id);
            let provider = model
                .owned_by
                .as_deref()
                .map(|p| format!(" · {p}"))
                .unwrap_or_default();
            out.push_str(&format!("- `{}` — {label}{provider}{marker}\n", model.id));
        }
        out.push_str("\nSwitch with `/model <id>` or the client's Model selector.\n");
        Ok(out)
    }

    /// `/model <id>`: `session/setModel` (durable for subsequent calls).
    async fn run_model_select(
        &self,
        acp_sid: &str,
        msp_sid: &str,
        arg: &str,
    ) -> Result<String, AcpError> {
        let conn = self.ready_conn().await?;
        conn.command_with_retry(
            "session/setModel",
            &json!({
                "commandId": conn.mint_command_id(),
                "sessionId": msp_sid,
                "model": {"modelId": arg},
            }),
        )
        .await
        .map_err(|e| AcpError::msp_command("session/setModel", &e))?;
        {
            let mut inner = self.inner.lock().await;
            if let Some(s) = inner.sessions.get_mut(acp_sid) {
                s.model_value = arg.to_string();
            }
        }
        Ok(format!("Model set to {arg}."))
    }

    /// `/effort [tier]`: `session/setReasoningEffort` (durable default)
    /// plus the per-turn override the selector owns, or the current tier.
    async fn run_effort(
        &self,
        acp_sid: &str,
        msp_sid: &str,
        arg: &str,
    ) -> Result<String, AcpError> {
        // Bare or invalid `/effort` offers the picker form when the
        // client can show one; the elicited tier is always valid.
        if arg.is_empty() || !is_reasoning_effort(arg) {
            let options: Vec<(String, String)> = EFFORT_TIERS
                .iter()
                .map(|t| (t.to_string(), t.to_string()))
                .collect();
            if let Some(tier) = self
                .elicit_choice(acp_sid, "Select reasoning effort", &options)
                .await
            {
                return self.run_effort_set(acp_sid, msp_sid, &tier).await;
            }
        }
        if arg.is_empty() {
            let effort = {
                let inner = self.inner.lock().await;
                inner
                    .sessions
                    .get(acp_sid)
                    .map(|s| s.reasoning_effort.clone())
                    .unwrap_or_else(|| DEFAULT_REASONING_EFFORT.to_string())
            };
            return Ok(format!(
                "Reasoning effort: {effort} — `/effort <{}>` sets it.",
                EFFORT_TIERS.join("|")
            ));
        }
        if !is_reasoning_effort(arg) {
            return Ok(format!(
                "Unknown effort '{arg}' — pick one of: {}.",
                EFFORT_TIERS.join(", ")
            ));
        }
        self.run_effort_set(acp_sid, msp_sid, arg).await
    }

    /// `/effort <tier>` set path (the tier is already validated).
    async fn run_effort_set(
        &self,
        acp_sid: &str,
        msp_sid: &str,
        arg: &str,
    ) -> Result<String, AcpError> {
        let conn = self.ready_conn().await?;
        conn.command_with_retry(
            "session/setReasoningEffort",
            &json!({
                "commandId": conn.mint_command_id(),
                "sessionId": msp_sid,
                "reasoningEffort": arg,
            }),
        )
        .await
        .map_err(|e| AcpError::msp_command("session/setReasoningEffort", &e))?;
        {
            let mut inner = self.inner.lock().await;
            if let Some(s) = inner.sessions.get_mut(acp_sid) {
                s.reasoning_effort = arg.to_string();
            }
        }
        Ok(format!("Reasoning effort set to {arg}."))
    }

    /// `/recap`: the folded session history distilled to a card — the last
    /// user prompt, the last agent answer, and the queue depth.
    async fn recap_card(&self, acp_sid: &str, msp_sid: &str) -> String {
        let conn = match self.ready_conn().await {
            Ok(conn) => conn,
            Err(error) => return format!("Recap unavailable: {}", error.message),
        };
        let result = match conn
            .command_with_retry(
                "session/read",
                &json!({
                    "commandId": conn.mint_command_id(),
                    "sessionId": msp_sid,
                    "excludeItems": false,
                }),
            )
            .await
        {
            Ok(result) => result,
            Err(error) => return format!("Recap unavailable: {}", error.message),
        };
        let (facts, in_flight) = {
            let inner = self.inner.lock().await;
            match inner.sessions.get(acp_sid) {
                Some(s) => (clone_facts(&s.facts), s.in_flight.len()),
                None => (SessionFacts::default(), 0),
            }
        };
        let session = result.get("session").unwrap_or(&Value::Null);
        let mut out = String::from("**Recap**\n\n");
        let name = session
            .get("name")
            .and_then(Value::as_str)
            .or(facts.name.as_deref())
            .unwrap_or("untitled");
        let turns = session
            .get("turnCount")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        out.push_str(&format!("- {name} · {turns} completed turns"));
        if in_flight > 0 {
            out.push_str(&format!(" · {in_flight} in flight"));
        }
        out.push('\n');
        if let Some(goal) = facts.goal.as_ref().and_then(goal_summary) {
            out.push_str(&format!("- Goal: {goal}\n"));
        }
        let items = result
            .get("history")
            .and_then(|h| h.get("items"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let last_of = |kind: &str| -> Option<String> {
            items
                .iter()
                .rev()
                .find(|i| i.get("kind").and_then(Value::as_str) == Some(kind))
                .and_then(|i| {
                    i.get("text")
                        .or_else(|| i.get("fallbackText"))
                        .and_then(Value::as_str)
                })
                .map(|t| truncate(&t.replace('\n', " "), 200))
        };
        if let Some(prompt) = last_of("userMessage") {
            out.push_str(&format!("- Last prompt: {prompt}\n"));
        }
        if let Some(answer) = last_of("agentMessage") {
            out.push_str(&format!("- Last answer: {answer}\n"));
        }
        if let Some(todos) = &facts.todos {
            let open = todos
                .iter()
                .filter(|t| t.get("status").and_then(Value::as_str) != Some("completed"))
                .count();
            if !todos.is_empty() {
                out.push_str(&format!("- Tasks: {open} open / {} total\n", todos.len()));
            }
        }
        if items.is_empty() {
            out.push_str("- No history on record yet.\n");
        }
        out
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
        self.cancel_turns(acp_sid, turns).await;
    }

    /// Best-effort `turn/cancel` for collected in-flight turns plus the
    /// local driver poke each one waits on. Shared by `session/cancel`
    /// and the `/stop` protocol command; never fails.
    async fn cancel_turns(&self, acp_sid: &str, turns: Vec<(String, String, watch::Sender<bool>)>) {
        let conn = self.ready_conn().await.ok();
        for (msp_sid, turn_id, cancel_tx) in turns {
            if let Some(conn) = &conn {
                cancel_turn(conn, &msp_sid, &turn_id).await;
            } else {
                tracing::warn!(acp_sid, turn_id, "cancel could not reach the host");
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
        // Retire dialog waiters: dropping each oneshot resolves its
        // completer task with `Err`, which fails closed and exits.
        if let Some(perm) = &session.pending_perm {
            self.client.cancel(&perm.req_id);
        }
        for ui in &session.pending_ui {
            self.client.cancel(&ui.req_id);
        }
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
        let owned: Vec<(String, String, String, Option<String>)> = {
            let inner = self.inner.lock().await;
            // (ACP id, MSP id, cwd, folded name), sorted for a stable listing.
            let mut owned: Vec<_> = inner
                .sessions
                .iter()
                .map(|(acp, s)| {
                    (
                        acp.clone(),
                        s.msp_sid.clone(),
                        s.cwd.clone(),
                        s.facts.name.clone(),
                    )
                })
                .collect();
            owned.sort();
            owned
        };
        let conn = self.ready_conn().await?;
        let mut list_params = json!({
            "commandId": conn.mint_command_id(),
            "limit": 200,
        });
        if !filter_root.is_empty() {
            list_params["workspaceRoot"] = Value::String(filter_root);
        }
        // Host metadata enriches owned rows (last activity) and supplies
        // importable foreign rows (past TUI/CLI sessions).
        let mut host_meta: std::collections::HashMap<String, HostRowMeta> =
            std::collections::HashMap::new();
        let mut host_items: Vec<Value> = Vec::new();
        match conn.command_with_retry("session/list", &list_params).await {
            Ok(listed) => {
                if let Some(items) = listed.get("sessions").and_then(Value::as_array) {
                    for item in items {
                        let msp_id = item.get("sessionId").and_then(Value::as_str).unwrap_or("");
                        if msp_id.is_empty() {
                            continue;
                        }
                        let meta = HostRowMeta {
                            name: host_string(item, "name"),
                            title: host_string(item, "title"),
                            prompt: host_string(item, "firstUserPrompt"),
                            updated: host_string(item, "updatedAt"),
                        };
                        host_meta.insert(msp_id.to_string(), meta);
                        host_items.push(item.clone());
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    kind = error.kind(),
                    message = %error.message,
                    "session/list failed; returning adapter-live sessions only"
                );
            }
        }
        let owned_msp: std::collections::HashSet<&str> =
            owned.iter().map(|(_, m, _, _)| m.as_str()).collect();
        let mut entries: Vec<Value> = owned
            .iter()
            .map(|(acp, msp, cwd, name)| {
                let mut entry = json!({
                    "sessionId": acp,
                    "cwd": cwd,
                    "_meta": {"mspSessionId": msp},
                });
                // Title: folded name, host name, derived host title, then
                // the first-prompt preview (capped — it can be long);
                // updatedAt from the host row. Absent fields stay absent.
                let meta = host_meta.get(msp.as_str()).cloned().unwrap_or_default();
                if let Some(title) = name
                    .clone()
                    .filter(|n| !n.is_empty())
                    .or(meta.name)
                    .or(meta.title)
                    .or_else(|| meta.prompt.map(|p| truncate(&p, 80)))
                {
                    entry["title"] = Value::String(title);
                }
                if let Some(updated) = meta.updated {
                    entry["updatedAt"] = Value::String(updated);
                }
                entry
            })
            .collect();
        for item in &host_items {
            let msp_id = item.get("sessionId").and_then(Value::as_str).unwrap_or("");
            if msp_id.is_empty() || owned_msp.contains(msp_id) {
                continue; // already listed under its ACP id
            }
            // `cwd` is required and must be absolute; a rootless host row
            // cannot be represented, so skip it (debug, not warn: provider
            // mode legitimately adopts a null root).
            let Some(root) = item
                .get("workspaceRoot")
                .and_then(Value::as_str)
                .filter(|r| r.starts_with('/'))
            else {
                tracing::debug!(
                    session_id = msp_id,
                    "session/list skips a rootless host row"
                );
                continue;
            };
            let mut entry = json!({"sessionId": msp_id, "cwd": root});
            if let Some(title) = host_string(item, "name")
                .or_else(|| host_string(item, "title"))
                .or_else(|| host_string(item, "firstUserPrompt").map(|p| truncate(&p, 80)))
            {
                entry["title"] = Value::String(title);
            }
            if let Some(updated) = item
                .get("updatedAt")
                .and_then(Value::as_str)
                .filter(|u| !u.is_empty())
            {
                entry["updatedAt"] = Value::String(updated.to_string());
            }
            entries.push(entry);
        }
        Ok(json!({"sessions": entries}))
    }

    /// `session/resume` / `session/load`: re-attach a durable host session.
    /// Unknown ACP ids resolve like the reference: a known session first,
    /// then `_meta.mspSessionId`, then the id itself (durable host ids are
    /// stable across adapter restarts). `session/load` always replays
    /// history; v2 `session/resume` replays only with `replayFrom`; v1
    /// `session/resume` reconnects silently.
    pub async fn resume_or_load(
        self: &Arc<Self>,
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
        // Re-resuming a known session keeps its registry; a fresh entry
        // needs the workspace's skills + a spawned observer.
        let (is_new, skills) = {
            let known = self.inner.lock().await.sessions.contains_key(&sid);
            if known {
                (false, Vec::new())
            } else {
                (true, self.fetch_skills(&restored_cwd).await)
            }
        };
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
                    skills: skills.clone(),
                    pending_perm: None,
                    perm_queue: Vec::new(),
                    pending_ui: Vec::new(),
                    ui_seen: HashSet::new(),
                    facts: SessionFacts::default(),
                    children: HashMap::new(),
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
                entry.mode = adopt_mode_label(&entry.mode, folded);
            }
            // Seed facts from the folded session object so /status and
            // /usage read real values before the first live event lands.
            if let Some(name) = session.get("name").and_then(Value::as_str) {
                entry.facts.name = Some(name.to_string());
            }
            if let Some(branch) = session.get("branch") {
                entry.facts.branch = Some(json!({"branch": branch.clone()}));
            }
            // Seed child retention from history (whether or not this
            // method replays frames) so /subagents and /workflows read
            // resumed children.
            seed_children(&mut entry.children, &resumed);
            (
                entry.mode.clone(),
                entry.model_value.clone(),
                entry.reasoning_effort.clone(),
            )
        };
        if is_new {
            self.spawn_observer(&sid);
        }
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
        // Slash commands advertise after the result frame (see
        // `advertise_commands`).
        // A resumed session may hold approvals/prompts issued before the
        // attach: pull `approval/listPending` and present each unknown one
        // (the observer's live stream may have delivered them already —
        // dedupe by id resolves pull-vs-reissue races to one dialog).
        self.reconcile_pending(&sid).await;
        Ok(result)
    }

    /// `session/fork`: branch a session's history into a new session. An
    /// absent cut point forks all completed turns; a JetBrains AIR fork
    /// point resolves to `cutPoint.lastTurnId` via `session/read`.
    pub async fn fork(self: &Arc<Self>, ver: u8, params: &Value) -> Result<Value, AcpError> {
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
        let skills = self.fetch_skills(&restored_cwd).await;
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
                    skills: skills.clone(),
                    pending_perm: None,
                    perm_queue: Vec::new(),
                    pending_ui: Vec::new(),
                    ui_seen: HashSet::new(),
                    facts: SessionFacts {
                        name: new_session
                            .get("name")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        branch: new_session
                            .get("branch")
                            .map(|b| json!({"branch": b.clone()})),
                        ..SessionFacts::default()
                    },
                    children: HashMap::new(),
                },
            );
        }
        self.spawn_observer(&acp_sid);
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
        // Slash commands advertise after the result frame (see
        // `advertise_commands`).
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
                        "mode must be ask|auto|yolo|deny".to_string(),
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
                    // Yolo survives only on a true allowAll fold; a
                    // downgraded apply falls back to the folded label.
                    if value == "yolo" && host_mode == "allowAll" {
                        session.mode = "yolo".to_string();
                    } else {
                        session.mode = mode_from_msp(host_mode).to_string();
                    }
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

    /// `session/set_mode`: the operating-mode switch (`modeId` or legacy
    /// `mode`), same ask|auto|yolo|deny vocabulary.
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
                "mode must be ask|auto|yolo|deny".to_string(),
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
            // Yolo is bridge-local (allowAll + no questions): keep the
            // label — the host only knows allowAll.
            if value == "yolo" {
                session.mode = "yolo".to_string();
            } else {
                session.mode = mode_from_msp(host_mode).to_string();
            }
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
        let (msp_sid, cwd, effort, active_turn, skills) = {
            let inner = self.inner.lock().await;
            match inner.sessions.get(&sid) {
                Some(s) => (
                    s.msp_sid.clone(),
                    s.cwd.clone(),
                    s.reasoning_effort.clone(),
                    s.active_turn.clone(),
                    s.skills.clone(),
                ),
                None => {
                    self.send(server::error_frame(&req_id, -32602, "unknown sessionId"))
                        .await;
                    return Ok(());
                }
            }
        };
        let (input, echo) = match extract_prompt(params.get("prompt"), &cwd, &skills) {
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

    /// The workspace's skill registry (`muse skills list --json` beside the
    /// supervised host — same binary and posture). Failure ⇒ empty (static
    /// fallback set).
    async fn fetch_skills(&self, cwd: &str) -> Vec<SkillEntry> {
        skills::list_skills(self.dispatcher.supervisor().config(), cwd).await
    }

    /// Spawn the per-session observer: folds session notifications into
    /// [`SessionFacts`], bridges `plan`/`session_info_update`/`usage_update`,
    /// and owns the approval/user-input dialogs. Exits when the session is
    /// removed or the supervisor is exhausted; resubscribes across host
    /// restarts.
    fn spawn_observer(self: &Arc<Self>, acp_sid: &str) {
        let store = Arc::clone(self);
        let acp_sid = acp_sid.to_string();
        tokio::spawn(async move { observe_session(store, acp_sid).await });
    }

    /// One host event for one session: fold facts under the lock, then send
    /// derived frames and run dialog work off-lock. `false` retires the
    /// observer (the session is gone).
    async fn observe_event(self: &Arc<Self>, acp_sid: &str, method: &str, params: &Value) -> bool {
        let msp_sid = session_of(method, params);
        enum Pending {
            Approval(Value),
            UserInput(Value),
        }
        let mut frames: Vec<Value> = Vec::new();
        let mut pending: Option<Pending> = None;
        {
            let mut inner = self.inner.lock().await;
            let Some(session) = inner.sessions.get_mut(acp_sid) else {
                return false;
            };
            if msp_sid.is_empty() || session.msp_sid != msp_sid {
                return true; // another session's event; keep watching
            }
            match method {
                "session/nameChanged" => {
                    session.facts.name = params
                        .get("name")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                }
                "session/goalChanged" => {
                    // Key present ⇒ replace (explicit `null` clears).
                    if let Some(goal) = params.get("goal") {
                        session.facts.goal = Some(goal.clone());
                    }
                }
                "session/branchChanged" => {
                    session.facts.branch = Some(json!({
                        "branch": params.get("branch").cloned().unwrap_or(Value::Null),
                        "vcs": params.get("vcs").cloned().unwrap_or(Value::Null),
                        "workspaceRoot": params.get("workspaceRoot").cloned().unwrap_or(Value::Null),
                    }));
                }
                "session/todoListChanged" => {
                    if let Some(items) = params.get("items").and_then(Value::as_array) {
                        session.facts.todos = Some(items.clone());
                    }
                }
                "session/contextUsage" => {
                    // Replace wholesale: an absent `windowTokens` means the
                    // basis has no limit — drop the stale size rather than
                    // re-emit it.
                    session.facts.usage_used = params.get("usedTokens").and_then(Value::as_u64);
                    session.facts.usage_size = params.get("windowTokens").and_then(Value::as_u64);
                    session.facts.usage_pressure = params
                        .get("pressure")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                }
                "session/tokenUsage" => {
                    if let Some(c) = params.get("cumulative") {
                        session.facts.cum_prompt = c.get("promptTokens").and_then(Value::as_u64);
                        session.facts.cum_output = c.get("outputTokens").and_then(Value::as_u64);
                        session.facts.cum_total = c.get("totalTokens").and_then(Value::as_u64);
                    }
                }
                "session/modelChanged" => {
                    if let Some(model) = params.get("modelId").and_then(Value::as_str)
                        && !model.is_empty()
                    {
                        session.model_value = model.to_string();
                    }
                }
                "session/approvalModeChanged" => {
                    if let Some(mode) = params.get("mode").and_then(Value::as_str)
                        && !mode.is_empty()
                    {
                        session.mode = mode_from_msp(mode).to_string();
                    }
                }
                "session/reasoningEffortChanged" => {
                    if let Some(tier) = params.get("reasoningEffort").and_then(Value::as_str)
                        && is_reasoning_effort(tier)
                    {
                        session.reasoning_effort = tier.to_string();
                    }
                }
                "turn/started" => {
                    if let Some(turn_id) = params.get("turnId").and_then(Value::as_str)
                        && !turn_id.is_empty()
                    {
                        session.active_turn = Some(turn_id.to_string());
                    }
                }
                _ => {}
            }
            match method {
                "session/todoListChanged" => {
                    if let Some(frame) = plan_frame(acp_sid, session.facts.todos.as_deref()) {
                        frames.push(frame);
                    }
                }
                "session/goalChanged" | "session/branchChanged" => {
                    if let Some(frame) = session_meta_frame(acp_sid, &session.facts) {
                        frames.push(frame);
                    }
                }
                "session/contextUsage" => {
                    // The fresh pressure rides this frame (upstream parity).
                    let pressure = session.facts.usage_pressure.clone();
                    if let Some(frame) = usage_frame(acp_sid, &session.facts, pressure.as_deref()) {
                        frames.push(frame);
                    }
                }
                "session/tokenUsage" => {
                    if let Some(frame) = usage_frame(acp_sid, &session.facts, None) {
                        frames.push(frame);
                    }
                }
                // Authoritative outcome: if session work continues,
                // re-assert running (a resolved approval unblocks the turn).
                "approval/resolved" | "approval/updated" => {
                    if session.ver == 2 && !session.in_flight.is_empty() {
                        frames.push(state_update_frame(acp_sid, "running", None));
                    }
                }
                // Model-spawned children: retain every revision, render
                // only transitions (first sighting, status/detail moves,
                // terminals) as `tool_call` blocks — the TUI's child
                // blocks, in chat. Session-scoped, so idle completions
                // land too; the rev guard makes redelivery converge.
                "item/started" | "item/updated" | "item/completed" => {
                    let item = params.get("item").cloned().unwrap_or(Value::Null);
                    let item_id = item
                        .get("itemId")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    if let Some((record, announce)) =
                        fold_child_item(session.children.get(&item_id), method, &item)
                    {
                        let frame = announce
                            .then(|| child_tool_frame(acp_sid, &item_id, &record, session.ver));
                        session.children.insert(item_id, record);
                        frames.extend(frame);
                    }
                }
                // Both legs of one pending request: the request and its
                // `*ed` notification dedupe by id inside the handlers.
                "approval/request" | "approval/requested" => {
                    pending = Some(Pending::Approval(params.clone()));
                }
                "userInput/request" | "userInput/requested" => {
                    pending = Some(Pending::UserInput(params.clone()));
                }
                _ => {}
            }
        }
        for frame in frames {
            self.send(frame).await;
        }
        match pending {
            Some(Pending::Approval(p)) => self.open_approval(acp_sid, &p).await,
            Some(Pending::UserInput(p)) => self.route_user_input(acp_sid, &p).await,
            None => {}
        }
        true
    }

    /// The turn an approval blocks: `params.turnId` when the host names it,
    /// else the session's in-flight turn (the fake and some hosts omit the
    /// field — the approval still belongs to the running turn).
    async fn approval_turn_id(&self, acp_sid: &str, params: &Value) -> String {
        if let Some(id) = params.get("turnId").and_then(Value::as_str)
            && !id.is_empty()
        {
            return id.to_string();
        }
        let inner = self.inner.lock().await;
        inner
            .sessions
            .get(acp_sid)
            .and_then(|s| s.in_flight.last().map(|f| f.turn_id.clone()))
            .unwrap_or_default()
    }

    /// Present an MSP approval to the client as `session/request_permission`
    /// (ported from `muse-acp` `open_approval`). One dialog per session at a
    /// time: a second approval queues behind the displayed one and ids
    /// dedupe across the request/notification pair.
    // Boxed so the future is an opaque `Send`: `complete_permission` re-enters
    // this through `pop_queued_approval`, and an unboxed coroutine cycle would
    // make the whole chain unresolvable for the `tokio::spawn` bound.
    fn open_approval<'a>(
        self: &'a Arc<Self>,
        acp_sid: &'a str,
        params: &'a Value,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let approval_id = params
                .get("approvalId")
                .and_then(Value::as_str)
                .unwrap_or("");
            let tool_name = params
                .get("toolName")
                .and_then(Value::as_str)
                .unwrap_or("Muse action")
                .to_string();
            if approval_id.is_empty() {
                // Fail closed like the HTTP surface: decide blind is never an
                // option, so the turn cannot run on an undecidable approval.
                let turn_id = self.approval_turn_id(acp_sid, params).await;
                tracing::warn!(
                    acp_sid,
                    tool = tool_name,
                    "approval request without an approvalId; cancelling the turn"
                );
                if !turn_id.is_empty()
                    && let Ok(conn) = self.ready_conn().await
                {
                    let msp_sid = {
                        let from_params = session_of("approval/request", params);
                        if !from_params.is_empty() {
                            from_params
                        } else {
                            let inner = self.inner.lock().await;
                            inner
                                .sessions
                                .get(acp_sid)
                                .map(|s| s.msp_sid.clone())
                                .unwrap_or_default()
                        }
                    };
                    cancel_turn(&conn, &msp_sid, &turn_id).await;
                }
                return;
            }
            {
                let mut inner = self.inner.lock().await;
                let Some(session) = inner.sessions.get_mut(acp_sid) else {
                    return;
                };
                if session
                    .pending_perm
                    .as_ref()
                    .is_some_and(|p| p.approval_id == approval_id)
                {
                    return; // the other leg of this same request
                }
                if session.pending_perm.is_some() {
                    if session
                        .perm_queue
                        .iter()
                        .any(|p| p.get("approvalId").and_then(Value::as_str) == Some(approval_id))
                    {
                        return;
                    }
                    session.perm_queue.push(params.clone());
                    tracing::info!(
                        acp_sid,
                        approval_id,
                        "approval queued behind the displayed permission"
                    );
                    return;
                }
            }
            let (options, choices) = perm_options(params);
            if choices.is_empty() {
                tracing::warn!(
                    acp_sid,
                    approval_id,
                    "approval has no decidable choices; left for the host"
                );
                return;
            }
            let requirement = params
                .get("currentRequirementId")
                .cloned()
                .unwrap_or(Value::Null);
            let turn_id = self.approval_turn_id(acp_sid, params).await;
            let subject = params.get("subject").cloned().unwrap_or(Value::Null);
            let tool_call_id = params
                .get("toolCallId")
                .and_then(Value::as_str)
                .unwrap_or("?")
                .to_string();
            let (title, kind) = approval_presentation(&subject, &tool_name);
            let raw_input = if subject.is_object() {
                subject.clone()
            } else {
                Value::Null
            };
            let ver = {
                let mut inner = self.inner.lock().await;
                let Some(session) = inner.sessions.get_mut(acp_sid) else {
                    return;
                };
                session.pending_perm = Some(PendingPerm {
                    req_id: Value::Null,
                    approval_id: approval_id.to_string(),
                    requirement,
                    turn_id,
                    choices,
                });
                session.ver
            };
            if ver == 2 {
                self.send(state_update_frame(acp_sid, "requires_action", None))
                    .await;
            }
            let mut tool_call = json!({
                "toolCallId": tool_call_id,
                "title": title,
                "kind": kind,
                "status": "pending",
            });
            if !raw_input.is_null() {
                tool_call["rawInput"] = raw_input;
            }
            let req_params = if ver == 2 {
                json!({
                    "sessionId": acp_sid,
                    "title": title,
                    "subject": {"type": "tool_call", "toolCall": tool_call},
                    "options": options,
                })
            } else {
                json!({
                    "sessionId": acp_sid,
                    "toolCall": tool_call,
                    "options": options,
                })
            };
            let (req_id, rx) = self
                .client
                .request("perm", req_params, "session/request_permission")
                .await;
            {
                let mut inner = self.inner.lock().await;
                match inner
                    .sessions
                    .get_mut(acp_sid)
                    .and_then(|s| s.pending_perm.as_mut())
                {
                    Some(p) if p.approval_id == approval_id => p.req_id = req_id.clone(),
                    // Closed/replaced mid-send: drop the waiter so its completer
                    // exits instead of parking on a session that is gone.
                    _ => {
                        self.client.cancel(&req_id);
                        return;
                    }
                }
            }
            let store = Arc::clone(self);
            let sid = acp_sid.to_string();
            tokio::spawn(async move {
                store
                    .complete_permission(&sid, &req_id, rx.await.ok())
                    .await;
            });
        })
    }

    /// The client reply to `session/request_permission` (correlated by the
    /// waiter, not an id scan). Fail closed (ported from `muse-acp`
    /// `complete_permission`): only an explicit approving selection may
    /// approve; errors/dismissals decide the first non-approving choice,
    /// and a list without one cancels the turn rather than approve.
    async fn complete_permission(
        self: &Arc<Self>,
        acp_sid: &str,
        req_id: &Value,
        reply: Option<Value>,
    ) {
        let (msp_sid, ver, pending) = {
            let mut inner = self.inner.lock().await;
            let Some(session) = inner.sessions.get_mut(acp_sid) else {
                return;
            };
            let Some(p) = session.pending_perm.take() else {
                return; // late/duplicate reply after a settle
            };
            (session.msp_sid.clone(), session.ver, p)
        };
        let _ = req_id;
        enum Verdict {
            Approve(String),
            Deny(String),
            FailClosed,
        }
        let is_approving = |cid: &str| {
            pending
                .choices
                .iter()
                .find(|(id, _)| id == cid)
                .map(|(_, d)| is_approving_decision(d))
                .unwrap_or(false)
        };
        let fallback = || fallback_deny(&pending.choices);
        let verdict = match reply {
            None => {
                tracing::warn!(acp_sid, "client reply lost; failing closed");
                fallback().map_or(Verdict::FailClosed, Verdict::Deny)
            }
            Some(frame) if frame.get("error").is_some() => {
                tracing::warn!(
                    acp_sid,
                    "session/request_permission errored at the client; failing closed"
                );
                fallback().map_or(Verdict::FailClosed, Verdict::Deny)
            }
            Some(frame) => {
                let outcome = frame.get("result").and_then(|r| r.get("outcome"));
                match outcome
                    .and_then(|o| o.get("outcome"))
                    .and_then(Value::as_str)
                {
                    Some("selected") => match outcome
                        .and_then(|o| o.get("optionId"))
                        .and_then(Value::as_str)
                    {
                        Some(cid) if is_approving(cid) => Verdict::Approve(cid.to_string()),
                        Some(cid) => Verdict::Deny(cid.to_string()),
                        None => fallback().map_or(Verdict::FailClosed, Verdict::Deny),
                    },
                    _ => fallback().map_or(Verdict::FailClosed, Verdict::Deny),
                }
            }
        };
        let choice = match verdict {
            Verdict::Approve(c) | Verdict::Deny(c) => c,
            Verdict::FailClosed => {
                tracing::warn!(
                    acp_sid,
                    approval_id = pending.approval_id,
                    "no deny choice available; cancelling the turn rather than approve"
                );
                if let Ok(conn) = self.ready_conn().await {
                    cancel_turn(&conn, &msp_sid, &pending.turn_id).await;
                }
                self.pop_queued_approval(acp_sid).await;
                return;
            }
        };
        let conn = match self.ready_conn().await {
            Ok(conn) => conn,
            Err(error) => {
                tracing::warn!(acp_sid, message = %error.message, "approval/decide unreachable");
                self.pop_queued_approval(acp_sid).await;
                return;
            }
        };
        let decide_params = json!({
            "commandId": conn.mint_command_id(),
            "sessionId": msp_sid,
            "approvalId": pending.approval_id,
            "requirementId": pending.requirement,
            "choiceId": choice,
        });
        match conn
            .command_with_retry("approval/decide", &decide_params)
            .await
        {
            Ok(result) => {
                // Admission is not the outcome: `terminal: false` means
                // further requirements remain pending, so the dialog's
                // successor keeps the requires_action posture.
                let terminal = result
                    .get("terminal")
                    .and_then(Value::as_bool)
                    .unwrap_or(true);
                if ver == 2 && terminal {
                    let busy = {
                        let inner = self.inner.lock().await;
                        inner
                            .sessions
                            .get(acp_sid)
                            .map(|s| !s.in_flight.is_empty())
                            .unwrap_or(false)
                    };
                    if busy {
                        self.send(state_update_frame(acp_sid, "running", None))
                            .await;
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    acp_sid,
                    approval_id = pending.approval_id,
                    kind = error.kind(),
                    message = %error.message,
                    "approval/decide failed"
                );
            }
        }
        // Decided or not, the displayed permission is settled from the
        // client's perspective; show the next queued approval.
        self.pop_queued_approval(acp_sid).await;
    }

    /// Display the next queued approval for a session, if any.
    async fn pop_queued_approval(self: &Arc<Self>, acp_sid: &str) {
        let next = {
            let mut inner = self.inner.lock().await;
            inner.sessions.get_mut(acp_sid).and_then(|s| {
                if s.pending_perm.is_some() || s.perm_queue.is_empty() {
                    None
                } else {
                    Some(s.perm_queue.remove(0))
                }
            })
        };
        if let Some(params) = next {
            self.open_approval(acp_sid, &params).await;
        }
    }

    /// Route an MSP `userInput` prompt (request or notification leg) to the
    /// elicitation bridge, or auto-cancel it when the client advertised no
    /// `elicitation.form`. `ui_seen` + `pending_ui` dedupe the pair and any
    /// reissue.
    async fn route_user_input(self: &Arc<Self>, acp_sid: &str, params: &Value) {
        let user_input_id = params
            .get("userInputId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if user_input_id.is_empty() {
            return;
        }
        let yolo = {
            let mut inner = self.inner.lock().await;
            let Some(session) = inner.sessions.get_mut(acp_sid) else {
                return;
            };
            if session
                .pending_ui
                .iter()
                .any(|p| p.user_input_id == user_input_id)
                || session.ui_seen.contains(&user_input_id)
            {
                return; // already presented or settled by us
            }
            session.ui_seen.insert(user_input_id.clone());
            session.mode == "yolo"
        };
        // Yolo never interrupts: questions decline loudly with the durable
        // id and the host proceeds without answers. (Host *approvals* are
        // untouched — yolo must never synthesize an approval; under
        // allowAll the host simply sends none.)
        if yolo {
            tracing::info!(
                acp_sid,
                user_input_id,
                "yolo mode declines a user-input prompt (no questions)"
            );
            self.cancel_user_input(acp_sid, params, YOLO_NO_QUESTIONS_REASON)
                .await;
            return;
        }
        if self.elicitation_form.load(Ordering::SeqCst)
            && self.bridge_user_input(acp_sid, params).await
        {
            return;
        }
        tracing::info!(
            acp_sid,
            user_input_id,
            "userInput not bridged (no elicitation.form); auto-cancelling"
        );
        self.cancel_user_input(acp_sid, params, USER_INPUT_CANCEL_REASON)
            .await;
    }

    /// Bridge an MSP `userInput` prompt to `elicitation/create` (form mode).
    /// Ported from `muse-acp` `bridge_user_input`: one schema property per
    /// question (`q{i}`), deduped display labels mapping back to originals
    /// by position. `false` = nothing bridgeable (caller falls back).
    async fn bridge_user_input(self: &Arc<Self>, acp_sid: &str, params: &Value) -> bool {
        let user_input_id = params
            .get("userInputId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let tool_call = params
            .get("toolCallId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let questions = match params.get("questions").and_then(Value::as_array) {
            Some(q) if !q.is_empty() => q.clone(),
            _ => return false,
        };
        let mut props = serde_json::Map::new();
        let mut required = Vec::new();
        let mut msg = Vec::new();
        let mut ui_qs = Vec::new();
        for (i, q) in questions.iter().enumerate() {
            let qid = q.get("id").and_then(Value::as_str).unwrap_or("");
            if qid.is_empty() {
                continue;
            }
            let header = q.get("header").and_then(Value::as_str).unwrap_or("");
            let text = q.get("question").and_then(Value::as_str).unwrap_or("");
            // Dedupe display labels (duplicate enum values confuse clients);
            // answers map back to originals by position.
            let mut labels = Vec::new();
            let mut display = Vec::new();
            if let Some(options) = q.get("options").and_then(Value::as_array) {
                for option in options {
                    if let Some(label) = option.get("label").and_then(Value::as_str) {
                        let mut name = label.to_string();
                        let mut n = 2;
                        while display.iter().any(|e: &String| e == &name) {
                            name = format!("{label} ({n})");
                            n += 1;
                        }
                        labels.push(label.to_string());
                        display.push(name);
                    }
                }
            }
            let single = q
                .get("selection")
                .and_then(|s| s.get("mode"))
                .and_then(Value::as_str)
                .unwrap_or("single")
                == "single";
            let min = q
                .get("selection")
                .and_then(|s| s.get("minSelections"))
                .and_then(Value::as_u64)
                .unwrap_or(1);
            let max = q
                .get("selection")
                .and_then(|s| s.get("maxSelections"))
                .and_then(Value::as_u64);
            let key = format!("q{i}");
            if labels.is_empty() {
                // Free-text question: no options, plain string answer.
                props.insert(key.clone(), json!({"type": "string"}));
            } else if single {
                props.insert(key.clone(), json!({"type": "string", "enum": display}));
            } else {
                let mut schema = json!({
                    "type": "array",
                    "items": {"type": "string", "enum": display},
                    "minItems": min,
                });
                if let Some(m) = max {
                    schema["maxItems"] = json!(m);
                }
                props.insert(key.clone(), schema);
            }
            if single || min > 0 {
                required.push(key);
            }
            msg.push(format!("{header}: {text}"));
            ui_qs.push(UiQuestion {
                qid: qid.to_string(),
                labels,
                display,
            });
        }
        if ui_qs.is_empty() {
            return false;
        }
        let mut elic_params = json!({
            "sessionId": acp_sid,
            "mode": "form",
            "message": msg.join("\n"),
            "requestedSchema": {
                "type": "object",
                "properties": Value::Object(props),
                "required": required,
            },
        });
        if !tool_call.is_empty() {
            elic_params["toolCallId"] = Value::String(tool_call);
        }
        let ver = {
            let mut inner = self.inner.lock().await;
            let Some(session) = inner.sessions.get_mut(acp_sid) else {
                return false;
            };
            session.pending_ui.push(PendingUi {
                req_id: Value::Null,
                user_input_id: user_input_id.clone(),
                questions: ui_qs,
            });
            session.ver
        };
        if ver == 2 {
            self.send(state_update_frame(acp_sid, "requires_action", None))
                .await;
        }
        let (req_id, rx) = self
            .client
            .request("elic", elic_params, "elicitation/create")
            .await;
        {
            let mut inner = self.inner.lock().await;
            match inner.sessions.get_mut(acp_sid).and_then(|s| {
                s.pending_ui
                    .iter_mut()
                    .find(|p| p.user_input_id == user_input_id)
            }) {
                Some(p) => p.req_id = req_id.clone(),
                None => {
                    self.client.cancel(&req_id);
                    return true; // session went; still counts as handled
                }
            }
        }
        tracing::info!(acp_sid, user_input_id, "bridged userInput to elicitation");
        let store = Arc::clone(self);
        let sid = acp_sid.to_string();
        tokio::spawn(async move {
            store
                .complete_elicitation(&sid, &req_id, rx.await.ok())
                .await;
        });
        true
    }

    /// The client reply to `elicitation/create`: `accept` + content maps
    /// back to `userInput/answer`; anything else cancels the prompt.
    /// (Ported from `muse-acp` `complete_elicitation`.)
    async fn complete_elicitation(
        self: &Arc<Self>,
        acp_sid: &str,
        req_id: &Value,
        reply: Option<Value>,
    ) {
        let (msp_sid, ver, pending) = {
            let mut inner = self.inner.lock().await;
            let Some(session) = inner.sessions.get_mut(acp_sid) else {
                return;
            };
            let Some(idx) = session.pending_ui.iter().position(|p| &p.req_id == req_id) else {
                return; // not ours (late duplicate); ignore
            };
            let p = session.pending_ui.remove(idx);
            (session.msp_sid.clone(), session.ver, p)
        };
        // accept + content -> answers; anything else -> cancel.
        let mut answers: Option<Vec<Value>> = None;
        if let Some(frame) = &reply
            && frame.get("error").is_none()
            && frame
                .get("result")
                .and_then(|r| r.get("action"))
                .and_then(Value::as_str)
                == Some("accept")
        {
            let content = frame
                .get("result")
                .and_then(|r| r.get("content"))
                .cloned()
                .unwrap_or(Value::Null);
            let mut parts = Vec::new();
            for (i, q) in pending.questions.iter().enumerate() {
                let key = format!("q{i}");
                match content.get(key.as_str()) {
                    Some(Value::String(v)) => match ui_original(q, v.as_str()) {
                        Some(orig) => {
                            parts.push(json!({"questionId": q.qid, "selectedLabel": orig}))
                        }
                        None => parts.push(json!({"questionId": q.qid, "freeText": v})),
                    },
                    Some(Value::Array(vs)) => {
                        let mut matched = Vec::new();
                        let mut free = Vec::new();
                        for v in vs {
                            match v.as_str().and_then(|s| ui_original(q, s)) {
                                Some(orig) => matched.push(Value::String(orig)),
                                None => {
                                    if let Some(s) = v.as_str() {
                                        free.push(s.to_string());
                                    }
                                }
                            }
                        }
                        let mut answer = json!({"questionId": q.qid});
                        if !matched.is_empty() {
                            answer["selectedLabels"] = Value::Array(matched);
                        }
                        if !free.is_empty() {
                            answer["freeText"] = Value::String(free.join(", "));
                        }
                        parts.push(answer);
                    }
                    _ => {}
                }
            }
            answers = Some(parts);
        }
        let conn = match self.ready_conn().await {
            Ok(conn) => conn,
            Err(error) => {
                tracing::warn!(acp_sid, message = %error.message, "userInput settle unreachable");
                return;
            }
        };
        match answers {
            Some(answers) => {
                let params = json!({
                    "commandId": conn.mint_command_id(),
                    "sessionId": msp_sid,
                    "userInputId": pending.user_input_id,
                    "answers": answers,
                });
                match conn.command_with_retry("userInput/answer", &params).await {
                    Ok(_) => {
                        if ver == 2 {
                            let busy = {
                                let inner = self.inner.lock().await;
                                inner
                                    .sessions
                                    .get(acp_sid)
                                    .map(|s| !s.in_flight.is_empty())
                                    .unwrap_or(false)
                            };
                            if busy {
                                self.send(state_update_frame(acp_sid, "running", None))
                                    .await;
                            }
                        }
                    }
                    Err(error) => tracing::warn!(
                        acp_sid,
                        kind = error.kind(),
                        message = %error.message,
                        "userInput/answer failed"
                    ),
                }
            }
            None => {
                let reason = match &reply {
                    Some(frame) if frame.get("error").is_some() => ELICITATION_FAILED_REASON,
                    None => ELICITATION_FAILED_REASON,
                    _ => ELICITATION_DISMISSED_REASON,
                };
                let params = json!({
                    "commandId": conn.mint_command_id(),
                    "sessionId": msp_sid,
                    "userInputId": pending.user_input_id,
                    "reason": reason,
                });
                match conn.command_with_retry("userInput/cancel", &params).await {
                    Ok(_) => tracing::info!(
                        acp_sid,
                        "elicitation declined/cancelled/failed; question cancelled"
                    ),
                    Err(error) => tracing::warn!(
                        acp_sid,
                        kind = error.kind(),
                        message = %error.message,
                        "userInput/cancel failed"
                    ),
                }
            }
        }
    }

    /// `userInput/cancel` on the bridge's own initiative (no client surface
    /// or an unbridgeable prompt). Loud with the durable id.
    async fn cancel_user_input(&self, acp_sid: &str, params: &Value, reason: &str) {
        let user_input_id = params
            .get("userInputId")
            .and_then(Value::as_str)
            .unwrap_or("");
        let msp_sid = {
            let from_params = session_of("userInput/request", params);
            if !from_params.is_empty() {
                from_params
            } else {
                let inner = self.inner.lock().await;
                inner
                    .sessions
                    .get(acp_sid)
                    .map(|s| s.msp_sid.clone())
                    .unwrap_or_default()
            }
        };
        let Ok(conn) = self.ready_conn().await else {
            tracing::warn!(
                acp_sid,
                user_input_id,
                "userInput cancel unreachable; prompt will time out server-side"
            );
            return;
        };
        let cancel_params = json!({
            "commandId": conn.mint_command_id(),
            "sessionId": msp_sid,
            "userInputId": user_input_id,
            "reason": reason,
        });
        match conn
            .command_with_retry("userInput/cancel", &cancel_params)
            .await
        {
            Ok(_) => tracing::info!(
                acp_sid,
                user_input_id,
                "auto-cancelled a user-input prompt (fail closed)"
            ),
            Err(error) => tracing::warn!(
                acp_sid,
                user_input_id,
                kind = error.kind(),
                message = %error.message,
                "user-input auto-cancel failed; prompt will time out server-side"
            ),
        }
    }

    /// `approval/listPending` after a resume: present every pending
    /// approval/user-input the live stream has not already shown (dedupe by
    /// id resolves pull-vs-reissue races to one dialog). A log-fold read —
    /// no `commandId`.
    async fn reconcile_pending(self: &Arc<Self>, acp_sid: &str) {
        let msp_sid = {
            let inner = self.inner.lock().await;
            match inner.sessions.get(acp_sid) {
                Some(s) => s.msp_sid.clone(),
                None => return,
            }
        };
        let Ok(conn) = self.ready_conn().await else {
            return;
        };
        let listed = match conn
            .command_with_retry("approval/listPending", &json!({"sessionId": msp_sid}))
            .await
        {
            Ok(listed) => listed,
            Err(error) => {
                tracing::warn!(
                    acp_sid,
                    kind = error.kind(),
                    message = %error.message,
                    "approval/listPending reconciliation failed"
                );
                return;
            }
        };
        let mut n = 0usize;
        if let Some(approvals) = listed.get("approvals").and_then(Value::as_array) {
            for approval in approvals.clone() {
                self.open_approval(acp_sid, &approval).await;
                n += 1;
            }
        }
        if let Some(inputs) = listed.get("userInputs").and_then(Value::as_array) {
            for input in inputs.clone() {
                self.route_user_input(acp_sid, &input).await;
                n += 1;
            }
        }
        if n > 0 {
            tracing::info!(acp_sid, presented = n, "pending requests reconciled");
        }
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

/// Prompt admission outcome: protocol commands settle inline, anything
/// else spawns a [`drive_prompt`] task.
enum PromptAdmit {
    /// A protocol command (`/compact`, `/help`, …) ran and settled inline.
    Settled,
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
                            // The session observer owns dialogs; drivers
                            // delegate too so an observer that died (e.g.
                            // during a host outage) can't strand a turn —
                            // both paths dedupe by request id.
                            "approval/request" => {
                                let store = Arc::clone(&store);
                                let sid = ctx.acp_sid.clone();
                                tokio::spawn(async move {
                                    store.open_approval(&sid, &params).await;
                                });
                            }
                            "userInput/request" => {
                                let store = Arc::clone(&store);
                                let sid = ctx.acp_sid.clone();
                                tokio::spawn(async move {
                                    store.route_user_input(&sid, &params).await;
                                });
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

/// Per-session host-event observer: folds session notifications into
/// facts, bridges plan/meta/usage updates, and owns approval/user-input
/// dialogs via [`SessionStore::observe_event`]. Resubscribes across host
/// restarts (a restart swaps the broadcast); retires when the session is
/// removed or the supervisor is exhausted.
async fn observe_session(store: Arc<SessionStore>, acp_sid: String) {
    let mut rx = match store.ready_conn().await {
        Ok(conn) => conn.subscribe(),
        Err(error) => {
            tracing::warn!(acp_sid, message = %error.message, "observer could not subscribe; session has no event feed");
            return;
        }
    };
    loop {
        match rx.recv().await {
            Ok(HostEvent::Notification { method, params })
            | Ok(HostEvent::ServerRequest { method, params }) => {
                if !store.observe_event(&acp_sid, &method, &params).await {
                    return;
                }
            }
            // Facts are latest-wins replace-wholesale snapshots: a skipped
            // batch loses at most one redundant emission.
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(acp_sid, skipped, "observer lagged behind host events");
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                // This connection died; resubscribe on the restart.
                match store.ready_conn().await {
                    Ok(conn) => rx = conn.subscribe(),
                    Err(error) => {
                        tracing::warn!(acp_sid, message = %error.message, "observer retiring (host unavailable)");
                        return;
                    }
                }
            }
        }
    }
}

/// Deny-safe fallback on `(choiceId, decision)` pairs (ported from
/// `muse-acp` `fallback_deny`): the first non-approving decision, else
/// `None` — the caller cancels the turn rather than approve.
fn fallback_deny(choices: &[(String, String)]) -> Option<String> {
    choices
        .iter()
        .find(|(_, d)| !is_approving_decision(d))
        .map(|(id, _)| id.clone())
}

/// ACP permission `options` from MSP `availableChoices`; returns the
/// options array plus `(choiceId, decision)` for reply mapping. (Ported
/// from `muse-acp` `perm_options`.)
fn perm_options(params: &Value) -> (Value, Vec<(String, String)>) {
    let mut options = Vec::new();
    let mut choices = Vec::new();
    if let Some(items) = params.get("availableChoices").and_then(Value::as_array) {
        for choice in items {
            let id = choice.get("choiceId").and_then(Value::as_str).unwrap_or("");
            if id.is_empty() {
                continue;
            }
            let label = choice.get("label").and_then(Value::as_str).unwrap_or(id);
            let decision = choice.get("decision").and_then(Value::as_str).unwrap_or("");
            let scope = choice
                .get("scope")
                .and_then(Value::as_str)
                .unwrap_or("once");
            options.push(json!({
                "optionId": id,
                "name": label,
                "kind": perm_kind(decision, scope),
            }));
            choices.push((id.to_string(), decision.to_string()));
        }
    }
    (Value::Array(options), choices)
}

/// MSP `(decision, scope)` → ACP permission-option kind (ported from
/// `muse-acp`): approving decisions are `allow_*`, the rest `reject_*`;
/// `session`/`localPersistent` scope widens to `*_always`.
fn perm_kind(decision: &str, scope: &str) -> &'static str {
    let approved = is_approving_decision(decision);
    let always =
        scope.eq_ignore_ascii_case("session") || scope.eq_ignore_ascii_case("localpersistent");
    match (approved, always) {
        (true, true) => "allow_always",
        (true, false) => "allow_once",
        (false, true) => "reject_always",
        (false, false) => "reject_once",
    }
}

/// `(title, kind)` for a permission's tool-call card from the approval
/// `subject` (ported from `muse-acp` `open_approval`'s title/kind arms).
fn approval_presentation(subject: &Value, tool_name: &str) -> (String, &'static str) {
    let subject_kind = subject.get("kind").and_then(Value::as_str).unwrap_or("");
    let command = subject.get("command").and_then(Value::as_str);
    let path = subject.get("path").and_then(Value::as_str);
    let target = subject.get("target").and_then(Value::as_str);
    let access = subject.get("access").and_then(Value::as_str);
    let description = subject.get("description").and_then(Value::as_str);
    let title = match subject_kind {
        "shell" | "process" => command.or(description).unwrap_or(tool_name).to_string(),
        "fileAccess" => match (access, path) {
            (Some(access), Some(path)) => format!("{access} {path}"),
            (None, Some(path)) => path.to_string(),
            (Some(access), None) => access.to_string(),
            (None, None) => description.unwrap_or(tool_name).to_string(),
        },
        "network" => target.or(description).unwrap_or(tool_name).to_string(),
        _ => command
            .or(path)
            .or(target)
            .or(access)
            .or(description)
            .unwrap_or(tool_name)
            .to_string(),
    };
    let kind = match subject_kind {
        "shell" | "process" => "execute",
        "fileAccess" => match access.unwrap_or("").to_ascii_lowercase().as_str() {
            "read" | "list" | "stat" => "read",
            "search" => "search",
            "write" | "create" | "append" | "edit" | "modify" => "edit",
            "delete" | "remove" => "delete",
            "move" | "rename" => "move",
            _ => "other",
        },
        "network" => "fetch",
        _ => "other",
    };
    (title, kind)
}

/// Match a client-returned display label back to the original host label
/// (ported from `muse-acp` `ui_original`).
fn ui_original(q: &UiQuestion, shown: &str) -> Option<String> {
    q.display
        .iter()
        .position(|d| d == shown)
        .and_then(|i| q.labels.get(i))
        .cloned()
}

/// `plan` update from the folded todo list (replace-wholesale; an empty
/// list is a cleared plan, not a no-op). `None` items ⇒ no frame (a
/// malformed list carries no authoritative fact).
fn plan_frame(acp_sid: &str, items: Option<&[Value]>) -> Option<Value> {
    let items = items?;
    let entries: Vec<Value> = items.iter().filter_map(todo_entry).collect();
    Some(session_update_frame(
        acp_sid,
        json!({"sessionUpdate": "plan", "entries": entries}),
    ))
}

/// One MSP `TodoItem` → an ACP plan entry (`inProgress`/`completed` map;
/// every open value stays `pending` — an unknown state never presents as
/// finished work).
fn todo_entry(item: &Value) -> Option<Value> {
    let text = item.get("text").and_then(Value::as_str)?;
    if text.trim().is_empty() {
        return None;
    }
    let status = match item.get("status").and_then(Value::as_str).unwrap_or("") {
        "inProgress" => "in_progress",
        "completed" => "completed",
        _ => "pending",
    };
    Some(json!({"content": text, "priority": "medium", "status": status}))
}

/// One-line goal text: the `Goal.objective` (legacy
/// `summary`/`title`/`text` fallbacks, then compact JSON for
/// unrecognized shapes). `None` when no goal is set or it cleared.
fn goal_summary(goal: &Value) -> Option<String> {
    if goal.is_null() {
        return None;
    }
    if let Some(text) = goal.as_str().filter(|t| !t.trim().is_empty()) {
        return Some(text.to_string());
    }
    goal.get("objective")
        .or_else(|| goal.get("summary"))
        .or_else(|| goal.get("title"))
        .or_else(|| goal.get("text"))
        .and_then(Value::as_str)
        .filter(|t| !t.trim().is_empty())
        .map(str::to_string)
        .or_else(|| Some(goal.to_string()))
}

/// `/goal` card body off a folded goal fact (pure; unit-tested).
fn render_goal_card(goal: &Option<Value>) -> String {
    let mut out = String::from("**Goal**\n\n");
    let Some(block) = goal else {
        out.push_str("No goal has been set for this session.\n");
        return out;
    };
    if block.is_null() {
        out.push_str("The session goal was cleared.\n");
        return out;
    }
    let Some(summary) = goal_summary(block) else {
        out.push_str("No goal has been set for this session.\n");
        return out;
    };
    out.push_str(&format!("- Objective: {summary}\n"));
    if let Some(status) = block
        .get("status")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        out.push_str(&format!("- Status: {status}"));
        if let Some(pct) = block.get("percentComplete").and_then(Value::as_f64) {
            out.push_str(&format!(" · {pct}%"));
        }
        out.push('\n');
    }
    for (key, label) in [("currentWork", "Current"), ("nextWork", "Next")] {
        if let Some(work) = block
            .get(key)
            .and_then(Value::as_str)
            .filter(|w| !w.trim().is_empty())
        {
            out.push_str(&format!("- {label}: {work}\n"));
        }
    }
    out
}

/// `/tasks` card body off folded todos (pure; unit-tested).
fn render_tasks_card(todos: &Option<Vec<Value>>) -> String {
    let mut out = String::from("**Tasks**\n\n");
    let Some(items) = todos else {
        out.push_str("No task list has been reported yet.\n");
        return out;
    };
    let mut shown = 0;
    for item in items {
        let text = item
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if text.is_empty() {
            continue;
        }
        let status = item.get("status").and_then(Value::as_str).unwrap_or("");
        // Unknown states stay open — never present as finished work.
        let mark = match status {
            "completed" => "x",
            "inProgress" => "~",
            "cancelled" => "-",
            _ => " ",
        };
        // The running item reads better in its present-tense form.
        let label = if status == "inProgress" {
            item.get("activeForm")
                .and_then(Value::as_str)
                .filter(|f| !f.trim().is_empty())
                .unwrap_or(text)
        } else {
            text
        };
        out.push_str(&format!("- [{mark}] {label}\n"));
        shown += 1;
    }
    if shown == 0 {
        out.push_str("The task list is empty.\n");
    }
    out
}

/// `session_info_update` carrying the goal (provider-neutral) and the
/// namespaced branch observation (`_meta.muse.branch`). `None` when no
/// meta fact has ever landed (ported from `muse-acp` `send_session_meta`).
fn session_meta_frame(acp_sid: &str, facts: &SessionFacts) -> Option<Value> {
    let mut meta = serde_json::Map::new();
    if let Some(goal) = &facts.goal {
        meta.insert("goal".to_string(), goal.clone());
    }
    if let Some(branch) = &facts.branch {
        meta.insert("muse".to_string(), json!({"branch": branch}));
    }
    if meta.is_empty() {
        return None;
    }
    Some(session_update_frame(
        acp_sid,
        json!({"sessionUpdate": "session_info_update", "_meta": Value::Object(meta)}),
    ))
}

/// `usage_update` (`{used, size}` + counted-once cumulative totals under
/// `_meta.museCumulative`; `musePressure` rides context-usage events).
/// Emits only when both `used` and `size` are known (ported from
/// `muse-acp` `send_usage`, minus the priced-cost leg).
fn usage_frame(acp_sid: &str, facts: &SessionFacts, pressure: Option<&str>) -> Option<Value> {
    let (used, size) = (facts.usage_used, facts.usage_size);
    let (Some(used), Some(size)) = (used, size) else {
        return None;
    };
    let mut meta = json!({
        "museCumulative": {
            "promptTokens": facts.cum_prompt,
            "outputTokens": facts.cum_output,
            "totalTokens": facts.cum_total,
        }
    });
    if let Some(p) = pressure {
        meta["musePressure"] = Value::String(p.to_string());
    }
    Some(session_update_frame(
        acp_sid,
        json!({
            "sessionUpdate": "usage_update",
            "used": used,
            "size": size,
            "_meta": meta,
        }),
    ))
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
/// vocabulary, else [`DEFAULT_APPROVAL_MODE`]. Returns the host mode plus
/// the selector label (`yolo` survives as a label; every other value maps
/// through [`mode_from_msp`]). Invalid values fail loudly.
fn resolve_startup_mode() -> Result<(String, String), AcpError> {
    let raw = std::env::var(APPROVAL_MODE_ENV).unwrap_or_default();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        let host = DEFAULT_APPROVAL_MODE.to_string();
        let label = mode_from_msp(&host).to_string();
        return Ok((host, label));
    }
    let host = resolve_mode(trimmed).map(str::to_string).ok_or_else(|| {
        AcpError::invalid_params(format!(
            "{APPROVAL_MODE_ENV} must be ask|auto|yolo|deny or a host mode, got '{trimmed}'"
        ))
    })?;
    let label = if trimmed == "yolo" {
        "yolo".to_string()
    } else {
        mode_from_msp(&host).to_string()
    };
    Ok((host, label))
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

/// Our mode vocabulary → MSP `ApprovalMode`. `yolo` is bridge-local
/// (host `allowAll` plus auto-declined user questions); the host never
/// reports it back, so adoption compares host modes, not labels.
pub fn mode_to_msp(mode: &str) -> Option<&'static str> {
    match mode {
        "ask" => Some("promptUnmatched"),
        "auto" => Some("allowAll"),
        "yolo" => Some("allowAll"),
        "deny" => Some("denyUnmatched"),
        _ => None,
    }
}

/// Adopt a folded host mode as the session label. Host modes compare,
/// not labels: yolo IS allowAll host-side, so a yolo session resuming
/// onto allowAll keeps its label instead of degrading to auto.
fn adopt_mode_label(current: &str, folded_host: &str) -> String {
    if mode_to_msp(current).is_some_and(|h| h == folded_host) {
        current.to_string()
    } else {
        mode_from_msp(folded_host).to_string()
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

/// The eight reasoning tiers in CLI order (`/effort` help + validation).
const EFFORT_TIERS: &[&str] = &[
    "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
];

// ---------------------------------------------------------------------------
// Protocol commands: slash words the bridge answers itself (P7/P8). They
// never reach `turn/start`; each settles its prompt inline.
// ---------------------------------------------------------------------------

/// A parsed protocol command (single text block starting `/name [arg]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProtocolCommand {
    Compact,
    Help,
    Status,
    Usage,
    Name,
    Models,
    Exit,
    Effort,
    Recap,
    Stop,
    Goal,
    Tasks,
    Subagents,
    Workflows,
}

impl ProtocolCommand {
    /// Canonical slash name (echoes in the user-message replay).
    fn name(&self) -> &'static str {
        match self {
            ProtocolCommand::Compact => "compact",
            ProtocolCommand::Help => "help",
            ProtocolCommand::Status => "status",
            ProtocolCommand::Usage => "usage",
            ProtocolCommand::Name => "name",
            ProtocolCommand::Models => "models",
            ProtocolCommand::Exit => "exit",
            ProtocolCommand::Effort => "effort",
            ProtocolCommand::Recap => "recap",
            ProtocolCommand::Stop => "stop",
            ProtocolCommand::Goal => "goal",
            ProtocolCommand::Tasks => "tasks",
            ProtocolCommand::Subagents => "subagents",
            ProtocolCommand::Workflows => "workflows",
        }
    }
}

/// `(name, description, hint)` rows for the `/help` card.
const PROTOCOL_COMMANDS: &[(&str, &str, Option<&str>)] = &[
    ("compact", "Compact the session context", None),
    ("help", "Show available commands and gestures", None),
    ("status", "Show session, model, and host status", None),
    ("usage", "Show token and context-window usage", None),
    (
        "name",
        "Show or set the durable session name",
        Some("new name"),
    ),
    (
        "models",
        "List models; `/model <id>` switches",
        Some("model id"),
    ),
    ("effort", "Show or set the reasoning effort", Some("tier")),
    ("recap", "Show a recap of recent session activity", None),
    ("stop", "Stop the in-flight turn", None),
    ("goal", "Show the session goal", None),
    ("tasks", "Show the session task list", None),
    (
        "subagents",
        "Show model-spawned subagents; verbs control them",
        Some("verb target"),
    ),
    ("workflows", "Show workflow runs", None),
    ("exit", "Close this session", None),
];

/// Parse the accepted echo into a protocol command: a single text block
/// whose trimmed text is `/<name>` (optionally followed by an argument).
/// Anything else (multi-block, attached resources, unknown names) is a
/// prompt, not a command.
fn protocol_command(echo: &Value) -> Option<(ProtocolCommand, String)> {
    let text = match echo {
        Value::Array(blocks) if blocks.len() == 1 => {
            let only = &blocks[0];
            if only.get("type").and_then(Value::as_str) != Some("text") {
                return None;
            }
            only.get("text").and_then(Value::as_str)?.trim()
        }
        _ => return None,
    };
    let rest = text.strip_prefix('/')?;
    let (name, arg) = match rest.split_once(char::is_whitespace) {
        Some((n, a)) => (n, a.trim()),
        None => (rest, ""),
    };
    let command = match name {
        "compact" => ProtocolCommand::Compact,
        "help" | "commands" => ProtocolCommand::Help,
        "status" => ProtocolCommand::Status,
        "usage" | "context" => ProtocolCommand::Usage,
        "name" | "rename" => ProtocolCommand::Name,
        "models" | "model" => ProtocolCommand::Models,
        "exit" | "quit" => ProtocolCommand::Exit,
        "effort" | "reasoning-effort" => ProtocolCommand::Effort,
        "recap" => ProtocolCommand::Recap,
        "stop" => ProtocolCommand::Stop,
        "goal" => ProtocolCommand::Goal,
        "tasks" => ProtocolCommand::Tasks,
        "subagents" | "subagent" => ProtocolCommand::Subagents,
        "workflows" | "workflow" => ProtocolCommand::Workflows,
        _ => return None,
    };
    Some((command, arg.to_string()))
}

/// `" <arg>"` for the command echo (empty when no argument was given).
fn format_args_for(arg: &str) -> String {
    if arg.is_empty() {
        String::new()
    } else {
        format!(" {arg}")
    }
}

/// Clone a session's folded facts (cards render off-lock).
fn clone_facts(facts: &SessionFacts) -> SessionFacts {
    SessionFacts {
        name: facts.name.clone(),
        goal: facts.goal.clone(),
        branch: facts.branch.clone(),
        todos: facts.todos.clone(),
        cum_prompt: facts.cum_prompt,
        cum_output: facts.cum_output,
        cum_total: facts.cum_total,
        usage_used: facts.usage_used,
        usage_size: facts.usage_size,
        usage_pressure: facts.usage_pressure.clone(),
    }
}

/// `model` or `host default` for cards (empty means the server picks).
fn display_model(model: &str) -> &str {
    if model.is_empty() {
        "host default"
    } else {
        model
    }
}

/// `k`-abbreviated thousands for card numbers (12,345 stays readable).
fn num(n: u64) -> String {
    if n >= 10_000 {
        format!("{:.1}k", n as f64 / 1000.0)
    } else {
        n.to_string()
    }
}

/// Shared usage lines for `/status` and `/usage`.
fn append_usage_lines(out: &mut String, facts: &SessionFacts) {
    match (facts.usage_used, facts.usage_size) {
        (Some(used), Some(size)) => {
            let pct = (used * 100).checked_div(size).unwrap_or(0);
            let pressure = facts.usage_pressure.as_deref().unwrap_or("?");
            out.push_str(&format!(
                "- Context: {} / {} tokens ({pct}%, {pressure})\n",
                num(used),
                num(size)
            ));
        }
        (Some(used), None) => {
            let pressure = facts.usage_pressure.as_deref().unwrap_or("?");
            out.push_str(&format!("- Context: {} tokens ({pressure})\n", num(used)));
        }
        _ => {}
    }
    if let Some(total) = facts.cum_total {
        let prompt = facts.cum_prompt.unwrap_or(0);
        let output = facts.cum_output.unwrap_or(0);
        out.push_str(&format!(
            "- Tokens: {} total ({} prompt · {output} output)\n",
            num(total),
            num(prompt)
        ));
    }
}

/// One-line card snippet, char-capped.
fn truncate(text: &str, cap: usize) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= cap {
        return trimmed.to_string();
    }
    let mut out: String = trimmed.chars().take(cap).collect();
    out.push('…');
    out
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
                {"value": "yolo", "name": "Yolo", "description": "Allow all tools and skip questions"},
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
                {"value": "max", "name": "Max"},
                {"value": "ultra", "name": "Ultra"},
            ],
        },
    ])
}

/// `SessionModeState` for session responses (both versions): the
/// ask|auto|yolo|deny switch for ModeSelector-path clients (Zed itself
/// negotiates v1 and renders the `configOptions` buttons instead).
pub fn session_modes(current_mode: &str) -> Value {
    json!({
        "currentModeId": current_mode,
        "availableModes": [
            {"id": "ask", "name": "Ask", "description": "Request permission for unmatched tools"},
            {"id": "auto", "name": "Auto", "description": "Allow all tools without asking"},
            {"id": "yolo", "name": "Yolo", "description": "Allow all tools and skip questions"},
            {"id": "deny", "name": "Deny", "description": "Deny unmatched tools"},
        ],
    })
}

/// The slash commands a session advertises: the protocol commands the
/// bridge answers itself, `/skill`, then one entry per workspace skill
/// (P7 — `muse skills list` at session create). When the registry listing
/// failed or is empty the static fallback set keeps the known commands.
/// (v1 `input` is bare-hint, v2 wraps it as a typed object.)
fn available_commands(ver: u8, skills: &[SkillEntry]) -> Value {
    let input = |hint: &str| {
        if ver == 1 {
            json!({"hint": hint})
        } else {
            json!({"type": "text", "hint": hint})
        }
    };
    let mut commands: Vec<Value> = PROTOCOL_COMMANDS
        .iter()
        .map(|(name, description, hint)| {
            let mut item = json!({"name": name, "description": description});
            if let Some(hint) = hint {
                item["input"] = input(hint);
            }
            item
        })
        .collect();
    commands.push(json!({
        "name": "skill",
        "description": "Invoke a Muse skill",
        "input": input("skill id and optional prompt"),
    }));
    let mut names: HashSet<&str> = PROTOCOL_COMMANDS.iter().map(|(n, _, _)| *n).collect();
    names.insert("skill");
    // Dynamic skills: `/<id>` normalizes to `/skill <id>` on the way out.
    // A registry miss degrades to the curated fallback so the palette is
    // never empty.
    if skills.is_empty() {
        for (name, description, hint) in [
            (
                "plan",
                "Create a grounded plan and stop for approval",
                Some("what to plan"),
            ),
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
        ] {
            if !names.insert(name) {
                continue;
            }
            let mut item = json!({"name": name, "description": description});
            if let Some(hint) = hint {
                item["input"] = input(hint);
            }
            commands.push(item);
        }
    } else {
        for skill in skills {
            if skill.id.is_empty() || !names.insert(skill.id.as_str()) {
                continue;
            }
            commands.push(json!({
                "name": skill.id,
                "description": if skill.description.is_empty() {
                    "Muse skill"
                } else {
                    skill.description.as_str()
                },
            }));
        }
    }
    Value::Array(commands)
}

/// `available_commands_update` notification frame.
fn available_commands_frame(acp_sid: &str, ver: u8, skills: &[SkillEntry]) -> Value {
    session_update_frame(
        acp_sid,
        json!({
            "sessionUpdate": "available_commands_update",
            "availableCommands": available_commands(ver, skills),
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

/// Map client-provided ACP `mcpServers` (`session/new` only) onto the MSP
/// `config.mcpServers` record (`session/start` construction). ACP stdio
/// entries (`name` + `command`, the Hermes/`muse-acp` shape) become
/// `{transport: "stdio", command, args?, env?}`; URL entries become
/// `{transport: "streamableHttp", url, headers?}`. Env/headers accept the
/// ACP `[{name, value}]` rows or a plain object map. `None` when nothing
/// is forwardable (absent, empty, or every entry skipped).
///
/// Fail-soft per entry, never per session: clients attach these on every
/// `session/new`, so an unmappable entry warns loudly and is skipped
/// instead of bricking session creation. `mode` stays unset (schema
/// default `required`); observed live, a broken server does NOT fail
/// construction — the session starts and the breakage surfaces
/// host-side, outside anything the wire reports back.
fn forward_client_mcp_servers(params: &Value) -> Option<Value> {
    let entries = params.get("mcpServers")?.as_array()?;
    let mut record = serde_json::Map::new();
    for entry in entries {
        let name = entry.get("name").and_then(Value::as_str).unwrap_or("");
        if name.trim().is_empty() {
            tracing::warn!(
                "skipping client MCP server without a name (cannot key config.mcpServers)"
            );
            continue;
        }
        if let Some(command) = entry
            .get("command")
            .and_then(Value::as_str)
            .filter(|c| !c.is_empty())
        {
            let mut server = json!({"transport": "stdio", "command": command});
            if let Some(args) = entry.get("args").and_then(Value::as_array) {
                let kept: Vec<Value> = args.iter().filter(|a| a.is_string()).cloned().collect();
                server["args"] = Value::Array(kept);
            }
            if let Some(env) = string_record(entry.get("env")) {
                server["env"] = Value::Object(env);
            }
            if record.insert(name.to_string(), server).is_some() {
                tracing::warn!(name, "duplicate client MCP server name; last entry wins");
            }
            continue;
        }
        if let Some(url) = entry
            .get("url")
            .and_then(Value::as_str)
            .filter(|u| !u.is_empty())
        {
            let mut server = json!({"transport": "streamableHttp", "url": url});
            if let Some(headers) = string_record(entry.get("headers")) {
                server["headers"] = Value::Object(headers);
            }
            if record.insert(name.to_string(), server).is_some() {
                tracing::warn!(name, "duplicate client MCP server name; last entry wins");
            }
            continue;
        }
        tracing::warn!(
            name,
            "skipping client MCP server with neither command nor url (no MSP transport arm)"
        );
    }
    if record.is_empty() {
        return None;
    }
    let names: Vec<&str> = record.keys().map(String::as_str).collect();
    tracing::info!(
        names = names.join(","),
        "forwarding client MCP servers to session construction"
    );
    Some(json!({"mcpServers": Value::Object(record)}))
}

/// ACP `[{name, value}]` rows (or a plain object map) → an MSP string
/// record. `None` when absent or nothing usable survives.
fn string_record(value: Option<&Value>) -> Option<serde_json::Map<String, Value>> {
    let value = value?;
    let mut record = serde_json::Map::new();
    if let Some(obj) = value.as_object() {
        for (key, val) in obj {
            if let Some(text) = val.as_str() {
                record.insert(key.clone(), Value::String(text.to_string()));
            }
        }
    } else {
        let rows = value.as_array()?;
        for row in rows {
            let name = row.get("name").and_then(Value::as_str).unwrap_or("");
            // Empty values are legal env (set-but-empty); empty names are not.
            let val = row.get("value").and_then(Value::as_str).unwrap_or("");
            if name.is_empty() {
                continue;
            }
            record.insert(name.to_string(), Value::String(val.to_string()));
        }
    }
    (!record.is_empty()).then_some(record)
}

/// `session/new` forwards client MCP servers at construction; resume, load,
/// and fork re-attach an existing session, so there is nothing to attach
/// to — tolerate and ignore them instead of aborting the session.
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

/// Display metadata kept per host `session/list` row (titles + activity
/// for owned rows; the full foreign entry otherwise).
#[derive(Clone, Default)]
struct HostRowMeta {
    /// Durable session name.
    name: Option<String>,
    /// Derived display title.
    title: Option<String>,
    /// First-user-prompt preview (title fallback, capped at render).
    prompt: Option<String>,
    /// Last activity (RFC3339 verbatim).
    updated: Option<String>,
}

/// Non-blank string field of a host row (`None` when missing/blank/null).
fn host_string(item: &Value, key: &str) -> Option<String> {
    item.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string)
}

/// Deduplicated display labels for an elicitation select, positionally
/// aligned with `options` (shared by the single and select+text forms).
fn dedupe_labels(options: &[(String, String)]) -> Vec<String> {
    let mut display = Vec::with_capacity(options.len());
    for (_, label) in options {
        let mut name = label.clone();
        let mut n = 2;
        while display.iter().any(|e: &String| e == &name) {
            name = format!("{label} ({n})");
            n += 1;
        }
        display.push(name);
    }
    display
}

/// Position of a picked label in the deduped display list (`None` when
/// the answer is outside the list — such answers never apply).
fn map_position(display: &[String], picked: &str) -> Option<usize> {
    display.iter().position(|d| d == picked)
}

/// Whether an MSP item status is terminal: anything other than
/// `inProgress` (the schema's rule — unknown values are
/// terminal-unknown, never silently open). An absent/empty status stays
/// open: `item/started` carries no status and must not read as done.
fn child_status_terminal(status: &str) -> bool {
    !status.is_empty() && status != "inProgress"
}

/// MSP item status → ACP `ToolCallStatus`. The left side is an open
/// vocabulary: unknown strings fail (visible, never a silent pass). v1
/// has no `cancelled`, so cancellations fail there and cancel on v2.
fn child_tool_status(status: &str, ver: u8) -> &'static str {
    match status {
        "" | "inProgress" => "in_progress",
        "completed" => "completed",
        "cancelled" if ver == 2 => "cancelled",
        "cancelled" => "failed",
        _ => "failed",
    }
}

/// Short display suffix for a child id (ids are ASCII; first 8 read fine).
fn child_short_id(item_id: &str) -> &str {
    item_id.get(..8.min(item_id.len())).unwrap_or(item_id)
}

/// Card/picker mark for a retained child status: unknown (terminal-unknown
/// per the schema) is a loud failure, never done and never hidden.
fn child_mark(status: &str) -> char {
    match status {
        "completed" => 'x',
        "inProgress" | "" => '~',
        "cancelled" => '-',
        _ => '!',
    }
}

/// Non-blank string field of a view item.
fn child_field<'a>(item: &'a Value, key: &str) -> Option<&'a str> {
    item.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
}

/// Display title for a `subagent`/`workflow` item: the objective or
/// script identity, else the generic fallback, else an anonymous label
/// (a child is always rendered — never dropped for lack of a name).
fn child_title(kind: &str, item: &Value, item_id: &str) -> String {
    if kind == "subagent" {
        if let Some(objective) = child_field(item, "objective") {
            return objective.trim().to_string();
        }
    } else if let Some(script) = child_field(item, "scriptId") {
        return script.trim().to_string();
    } else if let Some(entry) = child_field(item, "entryId") {
        return entry.trim().to_string();
    }
    if let Some(fallback) = child_field(item, "fallbackText") {
        return fallback.trim().to_string();
    }
    format!("{kind} {}", child_short_id(item_id))
}

/// Second-line detail: control/child state, plus the terminal message.
/// Subagent durations render in seconds; workflow children count by
/// reported terminals (the lifecycle status is durable vocabulary we
/// relay, never interpret).
fn child_detail(kind: &str, item: &Value) -> String {
    let mut parts = Vec::new();
    if kind == "subagent" {
        if let Some(control) = child_field(item, "controlStatus") {
            parts.push(control.trim().to_string());
        }
        if let Some(ms) = item.get("durationMs").and_then(Value::as_u64) {
            parts.push(if ms >= 1000 {
                format!("{:.1}s", ms as f64 / 1000.0)
            } else {
                format!("{ms}ms")
            });
        }
        if let Some(reason) = child_field(item, "failureReason") {
            parts.push(reason.trim().to_string());
        }
        // A ready result surfaces in the line (capped — the full text
        // rides the `/subagents result` card).
        if let Some(summary) = item
            .get("result")
            .and_then(|r| r.get("summary"))
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
        {
            parts.push(format!("result: {}", truncate(summary, 120)));
        }
    } else {
        let children = item
            .get("children")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if !children.is_empty() {
            let done = children
                .iter()
                .filter(|c| child_field(c, "terminal").is_some())
                .count();
            parts.push(format!("{done}/{} children", children.len()));
        }
        if let Some(trigger) = child_field(item, "triggerSource") {
            parts.push(trigger.trim().to_string());
        }
        if let Some(message) = child_field(item, "message") {
            parts.push(message.trim().to_string());
        }
    }
    parts.join(" · ")
}

/// Fold one `subagent`/`workflow` view item: `None` when the event is
/// for another kind, the item has no id, or the revision is stale
/// (replace-iff-higher, like the turn fold); else the new record plus
/// whether a visible transition occurred (first sighting, or a title /
/// status / detail / terminal change). The caller retains the record
/// and emits the `tool_call` frame only on transitions, so redelivery
/// and gap-fill overlap converge without double-rendering.
fn fold_child_item(
    prev: Option<&ChildRecord>,
    method: &str,
    item: &Value,
) -> Option<(ChildRecord, bool)> {
    let kind = item.get("kind").and_then(Value::as_str).unwrap_or("");
    if kind != "subagent" && kind != "workflow" {
        return None;
    }
    let item_id = item.get("itemId").and_then(Value::as_str).unwrap_or("");
    if item_id.is_empty() {
        return None;
    }
    let rev = item
        .get("revision")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        .max(1);
    if let Some(p) = prev
        && rev <= p.rev
    {
        return None;
    }
    let status = child_field(item, "status").unwrap_or("").to_string();
    // No delta stream depends on children staying open, so unlike the
    // turn fold (where `item/started` never settles) a terminal status
    // settles on any event; `item/completed` settles regardless.
    let terminal = method == "item/completed" || child_status_terminal(&status);
    let result = item.get("result").unwrap_or(&Value::Null);
    let record = ChildRecord {
        kind: kind.to_string(),
        title: child_title(kind, item, item_id),
        status,
        detail: child_detail(kind, item),
        rev,
        terminal,
        subagent_id: child_field(item, "subagentId").unwrap_or("").to_string(),
        result_summary: result
            .get("summary")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        result_text: result
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
    };
    let announce = match prev {
        None => true,
        Some(p) => {
            p.title != record.title
                || p.status != record.status
                || p.detail != record.detail
                || p.terminal != record.terminal
                || p.subagent_id != record.subagent_id
                || p.result_summary != record.result_summary
                || p.result_text != record.result_text
        }
    };
    Some((record, announce))
}

/// `tool_call` upsert for a retained child — the TUI shows each child as
/// its own block; so do we. Content rides terminal frames only (patch
/// semantics: omitted fields leave the client's block unchanged).
fn child_tool_frame(acp_sid: &str, item_id: &str, record: &ChildRecord, ver: u8) -> Value {
    let mut update = json!({
        "sessionUpdate": "tool_call",
        "toolCallId": item_id,
        "title": record.title,
        "kind": "execute",
        "status": child_tool_status(&record.status, ver),
    });
    if record.terminal && !record.detail.is_empty() {
        update["content"] = json!([{"type": "text", "text": record.detail}]);
    }
    session_update_frame(acp_sid, update)
}

/// Seed child retention from resumed history (no frames — the replay
/// already rendered them). Snapshots settle by status alone, so a child
/// that is still running resumes open.
fn seed_children(children: &mut HashMap<String, ChildRecord>, resume_res: &Value) {
    let Some(items) = resume_res
        .get("history")
        .and_then(|h| h.get("items"))
        .and_then(Value::as_array)
    else {
        return;
    };
    for item in items {
        let item_id = item.get("itemId").and_then(Value::as_str).unwrap_or("");
        if item_id.is_empty() {
            continue;
        }
        if let Some((record, _)) = fold_child_item(children.get(item_id), "item/updated", item) {
            children.insert(item_id.to_string(), record);
        }
    }
}

/// `/subagents` + `/workflows` card body off retained children (pure;
/// unit-tested). Sorted by title for stable output. Unknown statuses
/// are terminal-unknown per the schema: loud failure marks, never
/// presented as done and never hidden.
fn render_children_card(
    heading: &str,
    kind: &str,
    children: &HashMap<String, ChildRecord>,
    empty: &str,
) -> String {
    let mut out = format!("**{heading}**\n\n");
    let mut rows: Vec<&ChildRecord> = children.values().filter(|c| c.kind == kind).collect();
    if rows.is_empty() {
        out.push_str(empty);
        out.push('\n');
        return out;
    }
    rows.sort_by(|a, b| a.title.cmp(&b.title));
    for child in rows {
        let mark = child_mark(&child.status);
        out.push_str(&format!("- [{mark}] {}", child.title));
        if !child.detail.is_empty() {
            out.push_str(&format!(" ({})", child.detail));
        }
        out.push('\n');
    }
    out
}

/// One `/subagents` verb: the MSP method plus its arg shape. All eight
/// control methods ack admission-only (`CommandAcceptedResult`) — even
/// `readResult`, whose content rides the view stream — so every verb
/// reports admission and the outcome lands in the child's block.
struct SubagentVerb {
    /// Canonical name (echoes in usage + ack cards).
    name: &'static str,
    /// MSP method.
    method: &'static str,
    /// Aliases accepted at parse.
    aliases: &'static [&'static str],
    /// Whether the verb needs a body (`message`, `followup`).
    needs_body: bool,
    /// Whether the verb takes a durable reason (`stop`, `interrupt`, `close`).
    takes_reason: bool,
    /// Ack lead word (`Stop requested`, `Note queued`, …).
    ack: &'static str,
}

/// The eight `subagent/*` control verbs in help order.
const SUBAGENT_VERBS: &[SubagentVerb] = &[
    SubagentVerb {
        name: "stop",
        method: "subagent/stop",
        aliases: &["kill", "halt"],
        needs_body: false,
        takes_reason: true,
        ack: "Stop requested",
    },
    SubagentVerb {
        name: "interrupt",
        method: "subagent/interrupt",
        aliases: &["yield", "pause"],
        needs_body: false,
        takes_reason: true,
        ack: "Interrupt requested",
    },
    SubagentVerb {
        name: "close",
        method: "subagent/close",
        aliases: &[],
        needs_body: false,
        takes_reason: true,
        ack: "Close requested",
    },
    SubagentVerb {
        name: "resume",
        method: "subagent/resume",
        aliases: &[],
        needs_body: false,
        takes_reason: false,
        ack: "Resume requested",
    },
    SubagentVerb {
        name: "reopen",
        method: "subagent/reopen",
        aliases: &[],
        needs_body: false,
        takes_reason: false,
        ack: "Reopen requested",
    },
    SubagentVerb {
        name: "message",
        method: "subagent/sendMessage",
        aliases: &["msg", "note", "tell", "say"],
        needs_body: true,
        takes_reason: false,
        ack: "Note queued",
    },
    SubagentVerb {
        name: "followup",
        method: "subagent/followupTask",
        aliases: &["follow-up", "task"],
        needs_body: true,
        takes_reason: false,
        ack: "Follow-up queued",
    },
    SubagentVerb {
        name: "result",
        method: "subagent/readResult",
        aliases: &["read"],
        needs_body: false,
        takes_reason: false,
        ack: "Result requested",
    },
];

/// Parse a verb word (canonical or alias, ASCII-insensitive).
fn parse_subagent_verb(word: &str) -> Option<&'static SubagentVerb> {
    let folded = word.to_ascii_lowercase();
    SUBAGENT_VERBS
        .iter()
        .find(|v| v.name == folded || v.aliases.iter().any(|a| *a == folded))
}

/// Target resolution over retained children (verbs are subagent-only).
enum ChildTarget {
    /// Exactly one prefix match: the view `itemId` (the record is
    /// re-read after any picker round-trip, so resolution keeps the id).
    One(String),
    /// No subagent matches (and no workflow matches either).
    None,
    /// Several subagent matches: disambiguate (cloned rows for options).
    Many(Vec<(String, ChildRecord)>),
    /// Only workflow runs match: verbs are subagent-only.
    WorkflowOnly,
}

/// Resolve a target word: ASCII-insensitive prefix match over the view
/// `itemId` and the durable `subagentId` (an empty durable id never
/// matches). Sorted by title for stable picker order.
fn resolve_subagent_target(children: &HashMap<String, ChildRecord>, target: &str) -> ChildTarget {
    // Empty never resolves (every id is a prefix hit): the caller picks.
    if target.is_empty() {
        return ChildTarget::None;
    }
    let want = target.to_ascii_lowercase();
    let mut hits: Vec<(String, ChildRecord)> = Vec::new();
    let mut workflows = false;
    for (id, child) in children {
        let id_hit = id.to_ascii_lowercase().starts_with(&want);
        let sub_hit = !child.subagent_id.is_empty()
            && child.subagent_id.to_ascii_lowercase().starts_with(&want);
        if !id_hit && !sub_hit {
            continue;
        }
        if child.kind == "subagent" {
            hits.push((id.clone(), child.clone()));
        } else {
            workflows = true;
        }
    }
    hits.sort_by(|a, b| a.1.title.cmp(&b.1.title));
    if hits.len() == 1 {
        ChildTarget::One(hits.swap_remove(0).0)
    } else if !hits.is_empty() {
        ChildTarget::Many(hits)
    } else if workflows {
        ChildTarget::WorkflowOnly
    } else {
        ChildTarget::None
    }
}

/// Elicitation options over subagent rows: `(itemId, "[mark] title (short id)")`.
fn subagent_options(rows: &[(&str, &ChildRecord)]) -> Vec<(String, String)> {
    let mut options: Vec<(String, String)> = rows
        .iter()
        .map(|(id, child)| {
            (
                id.to_string(),
                format!(
                    "[{}] {} ({})",
                    child_mark(&child.status),
                    child.title,
                    child_short_id(id)
                ),
            )
        })
        .collect();
    options.sort_by(|a, b| a.1.cmp(&b.1));
    options
}

/// `/subagents` usage card (unknown verbs + arg-shape errors).
fn subagents_usage() -> String {
    String::from(
        "**Subagents verbs**\n\n\
         /subagents — list retained subagents\n\
         /subagents <verb> [target] [text]\n\n\
         Verbs: stop, interrupt, close, resume, reopen, message <text>, followup <text>, result\n\
         Target: an id prefix, or pick from the selector when one is offered.\n",
    )
}

/// `/subagents result` card off a retained result envelope (pure;
/// unit-tested). Text caps at 2k chars — the envelope allows 32 KiB.
fn render_result_card(title: &str, record: &ChildRecord) -> String {
    let mut out = format!("**Result: {title}**\n\n");
    if !record.result_summary.is_empty() {
        out.push_str(&record.result_summary);
        out.push('\n');
    }
    if !record.result_text.is_empty() {
        if !record.result_summary.is_empty() {
            out.push('\n');
        }
        out.push_str(&truncate(&record.result_text, 2000));
        out.push('\n');
    }
    if record.result_summary.is_empty() && record.result_text.is_empty() {
        out.push_str("No result retained yet.\n");
    }
    out
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
            // Resumed children replay as their own blocks (status mapped
            // to the ACP vocabulary); retention seeding happens at the
            // `resume_or_load` call site.
            "subagent" | "workflow" => {
                let item_id = item.get("itemId").and_then(Value::as_str).unwrap_or("");
                if item_id.is_empty() {
                    continue;
                }
                if let Some((record, _)) = fold_child_item(None, "item/updated", item) {
                    out.push(child_tool_frame(acp_sid, item_id, &record, ver));
                }
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
/// normalization applies to host input only). `skills` is the session's
/// registry so `/<id>` normalizes to `/skill <id>` (P7).
pub fn extract_prompt(
    prompt: Option<&Value>,
    cwd: &str,
    skills: &[SkillEntry],
) -> Result<(TurnInput, Value), AcpError> {
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
        let text = normalize_muse_slash_command(&texts.join("\n"), skills);
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
/// position zero normalizes. The static list covers a missing registry;
/// a listed workspace skill id also normalizes (P7). (Ported and extended
/// from `muse-acp`.)
fn normalize_muse_slash_command(text: &str, skills: &[SkillEntry]) -> String {
    if !text.starts_with('/') {
        return text.to_string();
    }
    let mut words = text.splitn(2, char::is_whitespace);
    let command = words.next().unwrap_or_default();
    let argument = words.next().unwrap_or_default().trim();
    let id = command.trim_start_matches('/');
    let is_skill = matches!(
        command,
        "/plan" | "/doctor" | "/create-skill" | "/create-plugin" | "/import"
    ) || skills.iter().any(|s| s.id == id);
    if !is_skill {
        return text.to_string();
    }
    if argument.is_empty() {
        format!("/skill {id}")
    } else {
        format!("/skill {id} {argument}")
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
        let choices = |decisions: &[(&str, &str)]| -> Vec<(String, String)> {
            decisions
                .iter()
                .map(|(id, decision)| (id.to_string(), decision.to_string()))
                .collect()
        };
        // Approving decisions (any case, any `approv*` suffix) are skipped.
        let mixed = choices(&[
            ("c-always", "approveAlways"),
            ("c-allow", "APPROVE"),
            ("c-deny", "deny"),
            ("c-later", "deny"),
        ]);
        assert_eq!(fallback_deny(&mixed).as_deref(), Some("c-deny"));
        // An open decision vocabulary counts as non-approving (fail closed).
        let future = choices(&[("c-future", "quarantine")]);
        assert_eq!(fallback_deny(&future).as_deref(), Some("c-future"));
        // All-approve (or empty) fails closed upstream: no choice returned.
        let all_approve = choices(&[("c-a", "approve"), ("c-b", "approveForSession")]);
        assert_eq!(fallback_deny(&all_approve), None);
        assert_eq!(fallback_deny(&[]), None);
        // `perm_options` drops id-less choices before this runs, so the deny
        // fallback can never decide a choice it cannot name.
        let (options, mapped) = perm_options(&json!({
            "availableChoices": [{"decision": "deny"}, {"choiceId": "c-real", "decision": "deny"}]
        }));
        assert_eq!(options.as_array().unwrap().len(), 1);
        assert_eq!(fallback_deny(&mapped).as_deref(), Some("c-real"));
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
            let modes: Vec<&str> = items[0]["options"]
                .as_array()
                .expect("mode options")
                .iter()
                .map(|o| o["value"].as_str().expect("mode value"))
                .collect();
            assert_eq!(modes, vec!["ask", "auto", "yolo", "deny"]);
        }
        // Empty catalog degrades to an empty model selector, not an error.
        let options = config_options(1, "ask", "m1", "medium", &[]);
        assert_eq!(options[1]["options"], Value::Array(Vec::new()));
    }

    #[test]
    fn effort_picker_advertises_all_8_tiers_in_cli_order() {
        let models = vec![("m1".to_string(), "M One".to_string())];
        let want = [
            "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
        ];
        for ver in [1u8, 2u8] {
            let options = config_options(ver, "ask", "m1", "medium", &models);
            let tiers: Vec<&str> = options[2]["options"]
                .as_array()
                .expect("effort options")
                .iter()
                .map(|o| o["value"].as_str().expect("tier value"))
                .collect();
            assert_eq!(tiers, want, "v{ver} effort picker");
        }
    }

    #[test]
    fn session_modes_carry_the_current_mode() {
        let modes = session_modes("deny");
        assert_eq!(modes["currentModeId"], Value::String("deny".to_string()));
        let ids: Vec<&str> = modes["availableModes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["ask", "auto", "yolo", "deny"]);
    }

    #[test]
    fn yolo_maps_to_allow_all_but_is_never_reported() {
        assert_eq!(mode_to_msp("yolo"), Some("allowAll"));
        assert_eq!(resolve_mode("yolo"), Some("allowAll"));
        // The host cannot express yolo; folded allowAll reads back auto.
        assert_eq!(mode_from_msp("allowAll"), "auto");
        assert_eq!(resolve_mode("bogus"), None);
    }

    #[test]
    fn startup_mode_preserves_the_yolo_label() {
        // resolve_startup_mode reads process env; yolo survives only as a
        // label over a true allowAll fold (see create_session).
        assert_eq!(mode_to_msp("yolo"), Some("allowAll"));
        assert_eq!(adopt_mode_label("yolo", "allowAll"), "yolo");
        assert_eq!(adopt_mode_label("yolo", "denyUnmatched"), "deny");
        assert_eq!(adopt_mode_label("auto", "allowAll"), "auto");
        assert_eq!(adopt_mode_label("ask", "allowAll"), "auto");
        assert_eq!(adopt_mode_label("deny", "promptUnmatched"), "ask");
    }

    #[test]
    fn available_commands_list_protocol_skills_and_fallback() {
        let names = |commands: &Value| -> Vec<String> {
            commands
                .as_array()
                .expect("command array")
                .iter()
                .filter_map(|c| c["name"].as_str().map(str::to_string))
                .collect()
        };
        for ver in [1u8, 2u8] {
            let empty = available_commands(ver, &[]);
            let names_empty = names(&empty);
            // Fourteen protocol commands, the `skill` verb, then the curated
            // fallback row when the registry misses.
            assert_eq!(names_empty.len(), PROTOCOL_COMMANDS.len() + 1 + 5);
            assert_eq!(names_empty[0], "compact");
            assert!(names_empty.contains(&"skill".to_string()));
            assert!(names_empty.contains(&"plan".to_string()));
            // A populated registry replaces the fallback rows.
            let skills = [
                SkillEntry {
                    id: "every-skill".to_string(),
                    description: "Browse skills".to_string(),
                },
                SkillEntry {
                    id: "compact".to_string(),
                    description: "shadowed by the protocol command".to_string(),
                },
                SkillEntry {
                    id: "every-skill".to_string(),
                    description: "duplicate".to_string(),
                },
            ];
            let dynamic = available_commands(ver, &skills);
            let names_dyn = names(&dynamic);
            assert_eq!(names_dyn.len(), PROTOCOL_COMMANDS.len() + 1 + 1);
            assert!(names_dyn.contains(&"every-skill".to_string()));
            assert!(!names_dyn.contains(&"plan".to_string()));
            assert_eq!(
                names_dyn.iter().filter(|n| *n == "compact").count(),
                1,
                "skill id shadowed by a protocol command is not duplicated"
            );
        }
        // v1 input is a bare hint; v2 wraps it as a typed object.
        let v1 = available_commands(1, &[]);
        let v2 = available_commands(2, &[]);
        let skill_at = |commands: &Value| -> usize {
            commands
                .as_array()
                .unwrap()
                .iter()
                .position(|c| c["name"] == "skill")
                .expect("skill row")
        };
        assert_eq!(
            v1[skill_at(&v1)]["input"],
            json!({"hint": "skill id and optional prompt"})
        );
        assert_eq!(
            v2[skill_at(&v2)]["input"],
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
        let (input, echo) = extract_prompt(Some(&prompt), "/tmp", &[]).expect("text prompt");
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
        let (input, _) = extract_prompt(Some(&prompt), "/tmp", &[]).expect("escaped prompt");
        assert_eq!(
            input.parts[0],
            InputPart::Text(" /plan the cache".to_string())
        );
    }

    #[test]
    fn prompt_rejects_empty_audio_and_unknown_blocks() {
        assert!(extract_prompt(Some(&json!([])), "/tmp", &[]).is_err());
        assert!(extract_prompt(None, "/tmp", &[]).is_err());
        assert!(extract_prompt(Some(&json!({})), "/tmp", &[]).is_err());
        let err =
            extract_prompt(Some(&json!([{"type": "audio"}])), "/tmp", &[]).expect_err("audio");
        assert_eq!(err.code, -32602);
        let err =
            extract_prompt(Some(&json!([{"type": "video"}])), "/tmp", &[]).expect_err("video");
        assert!(err.message.contains("video"));
        let err = extract_prompt(Some(&json!([[1]])), "/tmp", &[]).expect_err("nested");
        assert_eq!(err.code, -32602);
    }

    #[test]
    fn inline_image_parts_validate_mime_and_base64() {
        let prompt = json!([{"type": "image", "data": "aGk=", "mimeType": "image/png"}]);
        let (input, echo) = extract_prompt(Some(&prompt), "/tmp", &[]).expect("image prompt");
        assert_eq!(echo, prompt, "non-text blocks echo verbatim");
        assert_eq!(
            input.parts[0],
            InputPart::Image {
                base64: "aGk=".to_string(),
                media_type: "image/png".to_string(),
            }
        );
        let bad = json!([{"type": "image", "data": "!!!", "mimeType": "image/png"}]);
        assert!(extract_prompt(Some(&bad), "/tmp", &[]).is_err());
        let bad_mime = json!([{"type": "image", "data": "aGk=", "mimeType": "text/plain"}]);
        assert!(extract_prompt(Some(&bad_mime), "/tmp", &[]).is_err());
        let no_source = json!([{"type": "image"}]);
        assert!(extract_prompt(Some(&no_source), "/tmp", &[]).is_err());
    }

    #[test]
    fn resource_blocks_inline_text_and_reference_uris() {
        let prompt = json!([
            {"type": "resource", "resource": {"uri": "mcp://x", "text": "inline body"}},
            {"type": "resource", "resource": {"uri": "mcp://y"}},
        ]);
        let (input, _) = extract_prompt(Some(&prompt), "/tmp", &[]).expect("resources");
        assert_eq!(
            input.parts[0],
            InputPart::Text("inline body\n[resource: mcp://y]".to_string())
        );
        let bare = json!([{"type": "resource", "resource": {}}]);
        assert!(extract_prompt(Some(&bare), "/tmp", &[]).is_err());
        let non_image = json!([
            {"type": "resource", "resource": {"mimeType": "application/pdf", "blob": "aGk="}},
        ]);
        assert!(extract_prompt(Some(&non_image), "/tmp", &[]).is_err());
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
        let (input, _) = extract_prompt(Some(&prompt), "/tmp", &[]).expect("links");
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
    fn protocol_command_needs_one_bare_slash_command() {
        let parsed = |echo: &Value| protocol_command(echo).map(|(c, _)| c.name());
        assert_eq!(
            parsed(&json!([{"type": "text", "text": "/compact"}])),
            Some("compact")
        );
        assert_eq!(
            parsed(&json!([{"type": "text", "text": "  /compact  "}])),
            Some("compact")
        );
        // Trailing text is the argument, not a different command.
        assert_eq!(
            parsed(&json!([{"type": "text", "text": "/compact now"}])),
            Some("compact")
        );
        assert_eq!(parsed(&json!([{"type": "text", "text": "/plan"}])), None);
        assert_eq!(
            parsed(&json!([
                {"type": "text", "text": "/compact"},
                {"type": "text", "text": "more"},
            ])),
            None
        );
        assert_eq!(parsed(&json!([{"type": "image", "data": "x"}])), None);
        assert_eq!(parsed(&json!([])), None);
        let (cmd, arg) =
            protocol_command(&json!([{"type": "text", "text": "/name New Session"}])).unwrap();
        assert_eq!(cmd.name(), "name");
        assert_eq!(arg, "New Session");
        for slash in ["/stop", "/goal", "/tasks", "/subagents", "/workflows"] {
            assert_eq!(
                parsed(&json!([{"type": "text", "text": slash}])),
                Some(slash.trim_start_matches('/')),
                "{slash} parses"
            );
        }
        // The unmappable engine surfaces stay prompts, not commands.
        for slash in ["/deep-research", "/side", "/mcp"] {
            assert_eq!(
                parsed(&json!([{"type": "text", "text": slash}])),
                None,
                "{slash} is not a protocol command"
            );
        }
    }

    #[test]
    fn mcp_forwarding_maps_stdio_and_http() {
        let config = forward_client_mcp_servers(&json!({
            "mcpServers": [
                {"name": "files", "command": "mcp-files", "args": ["--root", 7],
                 "env": [{"name": "HOME", "value": "/r"}, {"name": "", "value": "x"}]},
                {"name": "web", "url": "https://mcp.example/s",
                 "headers": {"Authorization": "Bearer t", "n": 1}},
            ]
        }))
        .expect("forwardable entries");
        assert_eq!(
            config,
            json!({"mcpServers": {
                "files": {"transport": "stdio", "command": "mcp-files",
                          "args": ["--root"], "env": {"HOME": "/r"}},
                "web": {"transport": "streamableHttp", "url": "https://mcp.example/s",
                        "headers": {"Authorization": "Bearer t"}},
            }})
        );
    }

    #[test]
    fn mcp_forwarding_skips_soft_and_omits_config() {
        assert_eq!(forward_client_mcp_servers(&json!({})), None);
        assert_eq!(forward_client_mcp_servers(&json!({"mcpServers": []})), None);
        // Nameless, transportless, and junk entries skip; the survivor
        // still forwards and the session is never at risk.
        let config = forward_client_mcp_servers(&json!({"mcpServers": [
            {"command": "noname"},
            {"name": "sse-only"},
            {"name": 7, "command": "junk"},
            {"name": "ok", "command": "yes"},
        ]}))
        .expect("survivor forwards");
        assert_eq!(
            config,
            json!({"mcpServers": {"ok": {"transport": "stdio", "command": "yes"}}})
        );
        assert_eq!(
            forward_client_mcp_servers(&json!({"mcpServers": [{"name": "nope"}]})),
            None,
            "all skipped ⇒ no config key at all"
        );
    }

    #[test]
    fn goal_summary_reads_objective_first() {
        assert_eq!(
            goal_summary(&json!({"objective": "Ship it", "status": "active"})).as_deref(),
            Some("Ship it")
        );
        assert_eq!(goal_summary(&Value::Null), None);
        assert_eq!(
            goal_summary(&json!({"summary": "legacy"})).as_deref(),
            Some("legacy")
        );
        assert_eq!(
            goal_summary(&json!({"weird": 1})).as_deref(),
            Some(r#"{"weird":1}"#),
            "unrecognized shapes render raw, never blank"
        );
    }

    #[test]
    fn goal_card_renders_states() {
        let full = render_goal_card(&Some(json!({
            "objective": "Ship it",
            "status": "active",
            "percentComplete": 50,
            "currentWork": "tests",
            "nextWork": "docs",
        })));
        for want in [
            "**Goal**",
            "- Objective: Ship it",
            "- Status: active · 50%",
            "- Current: tests",
            "- Next: docs",
        ] {
            assert!(full.contains(want), "card has {want}: {full}");
        }
        let bare = render_goal_card(&Some(json!({"objective": "Ship it"})));
        assert!(bare.contains("- Objective: Ship it"), "{bare}");
        assert!(!bare.contains("Status:"), "no status line: {bare}");
        assert!(
            render_goal_card(&None).contains("No goal has been set"),
            "unset state"
        );
        assert!(
            render_goal_card(&Some(Value::Null)).contains("was cleared"),
            "cleared state"
        );
    }

    #[test]
    fn tasks_card_renders_marks_and_active_form() {
        let card = render_tasks_card(&Some(vec![
            json!({"text": "done", "status": "completed"}),
            json!({"text": "Write tests", "status": "inProgress", "activeForm": "Writing tests"}),
            json!({"text": "todo", "status": "pending"}),
            json!({"text": "dropped", "status": "cancelled"}),
            json!({"text": "mystery", "status": "zonked"}),
            json!({"text": "  ", "status": "pending"}),
        ]));
        for want in [
            "**Tasks**",
            "- [x] done",
            "- [~] Writing tests",
            "- [ ] todo",
            "- [-] dropped",
            "- [ ] mystery",
        ] {
            assert!(card.contains(want), "card has {want}: {card}");
        }
        assert!(
            render_tasks_card(&None).contains("No task list has been reported"),
            "unreported state"
        );
        assert!(
            render_tasks_card(&Some(vec![])).contains("empty"),
            "empty state"
        );
    }

    #[test]
    fn child_status_maps_to_the_acp_vocabulary() {
        // Open MSP vocabulary on the left: unknown strings fail loudly,
        // never complete silently. v1 has no `cancelled`.
        for (status, ver, want) in [
            ("", 1, "in_progress"),
            ("inProgress", 1, "in_progress"),
            ("inProgress", 2, "in_progress"),
            ("completed", 1, "completed"),
            ("completed", 2, "completed"),
            ("cancelled", 1, "failed"),
            ("cancelled", 2, "cancelled"),
            ("failed", 1, "failed"),
            ("rejected", 2, "failed"),
            ("timedOut", 1, "failed"),
            ("zonked", 1, "failed"),
            ("zonked", 2, "failed"),
        ] {
            assert_eq!(child_tool_status(status, ver), want, "{status}/v{ver}");
        }
        assert!(!child_status_terminal(""));
        assert!(!child_status_terminal("inProgress"));
        for terminal in [
            "completed",
            "failed",
            "cancelled",
            "rejected",
            "timedOut",
            "zonked",
        ] {
            assert!(child_status_terminal(terminal), "{terminal} is terminal");
        }
    }

    #[test]
    fn child_titles_prefer_identity_then_fallback() {
        let title = |item: Value| {
            child_title(
                item.get("kind").and_then(Value::as_str).unwrap(),
                &item,
                item.get("itemId").and_then(Value::as_str).unwrap(),
            )
        };
        assert_eq!(
            title(json!({"kind": "subagent", "itemId": "a1", "objective": " Explore X "})),
            "Explore X"
        );
        assert_eq!(
            title(
                json!({"kind": "workflow", "itemId": "b2", "scriptId": "research", "entryId": "e"})
            ),
            "research"
        );
        assert_eq!(
            title(json!({"kind": "workflow", "itemId": "b2", "entryId": "entry-7"})),
            "entry-7"
        );
        assert_eq!(
            title(json!({"kind": "subagent", "itemId": "c3", "fallbackText": "a child"})),
            "a child"
        );
        // Anonymous children still render — never dropped for no name.
        assert_eq!(
            title(json!({"kind": "subagent", "itemId": "abcdef123456"})),
            "subagent abcdef12"
        );
    }

    #[test]
    fn child_details_carry_control_and_terminal_state() {
        let detail =
            |item: &Value| child_detail(item.get("kind").and_then(Value::as_str).unwrap(), item);
        assert_eq!(
            detail(&json!({
                "kind": "subagent",
                "controlStatus": "running",
                "durationMs": 12_500,
            })),
            "running · 12.5s"
        );
        assert_eq!(
            detail(&json!({"kind": "subagent", "durationMs": 300})),
            "300ms"
        );
        assert_eq!(
            detail(&json!({
                "kind": "subagent",
                "controlStatus": "closed",
                "failureReason": "boom",
            })),
            "closed · boom"
        );
        assert_eq!(
            detail(&json!({
                "kind": "workflow",
                "triggerSource": "modelProposal",
                "children": [
                    {"childId": "a", "attempt": 1, "status": "done", "terminal": "completed"},
                    {"childId": "b", "attempt": 1, "status": "running"},
                ],
            })),
            "1/2 children · modelProposal"
        );
        assert_eq!(
            detail(&json!({"kind": "workflow", "message": "all green"})),
            "all green"
        );
        assert!(detail(&json!({"kind": "subagent"})).is_empty());
    }

    #[test]
    fn child_fold_announces_transitions_and_holds_stale_revs() {
        let item = |rev: u64, status: &str| {
            json!({
                "kind": "subagent",
                "itemId": "child-1",
                "revision": rev,
                "status": status,
                "objective": "Explore X",
                "controlStatus": "running",
            })
        };
        // First sighting announces.
        let (first, announce) = fold_child_item(None, "item/started", &item(1, "")).unwrap();
        assert!(announce);
        assert!(!first.terminal);
        assert_eq!(first.title, "Explore X");
        // Stale revisions hold (redelivery converges silently).
        assert!(fold_child_item(Some(&first), "item/updated", &item(1, "")).is_none());
        // Same content at a higher rev retains without re-announcing.
        let (same, announce) = fold_child_item(Some(&first), "item/updated", &item(2, "")).unwrap();
        assert!(!announce);
        // A status move announces.
        let (running, announce) =
            fold_child_item(Some(&same), "item/updated", &item(3, "inProgress")).unwrap();
        assert!(announce);
        assert!(!running.terminal);
        // Terminal announces and sticks.
        let (done, announce) =
            fold_child_item(Some(&running), "item/completed", &item(4, "completed")).unwrap();
        assert!(announce);
        assert!(done.terminal);
        // A terminal status settles on any event (no delta stream to protect).
        let (failed, _) = fold_child_item(
            None,
            "item/started",
            &json!({
                "kind": "subagent",
                "itemId": "child-2",
                "revision": 1,
                "status": "failed",
                "objective": "Doomed",
            }),
        )
        .unwrap();
        assert!(failed.terminal);
        // Other kinds, missing ids, and missing revisions never fold.
        assert!(
            fold_child_item(
                None,
                "item/started",
                &json!({"kind": "toolCall", "itemId": "t"})
            )
            .is_none()
        );
        assert!(fold_child_item(None, "item/started", &json!({"kind": "subagent"})).is_none());
    }

    #[test]
    fn child_frames_carry_blocks_and_terminal_content() {
        let (spawn, _) = fold_child_item(
            None,
            "item/started",
            &json!({
                "kind": "subagent",
                "itemId": "child-1",
                "revision": 1,
                "objective": "Explore X",
            }),
        )
        .unwrap();
        let frame = child_tool_frame("s", "child-1", &spawn, 1);
        let update = &frame["params"]["update"];
        assert_eq!(update["sessionUpdate"], json!("tool_call"));
        assert_eq!(update["toolCallId"], json!("child-1"));
        assert_eq!(update["title"], json!("Explore X"));
        assert_eq!(update["status"], json!("in_progress"));
        assert!(update.get("content").is_none(), "no content while open");
        let (done, _) = fold_child_item(
            Some(&spawn),
            "item/completed",
            &json!({
                "kind": "subagent",
                "itemId": "child-1",
                "revision": 2,
                "status": "failed",
                "objective": "Explore X",
                "controlStatus": "closed",
                "failureReason": "boom",
            }),
        )
        .unwrap();
        let terminal = child_tool_frame("s", "child-1", &done, 1)["params"]["update"].clone();
        assert_eq!(terminal["status"], json!("failed"));
        assert_eq!(
            terminal["content"],
            json!([{"type": "text", "text": "closed · boom"}])
        );
        let (cancelled, _) = fold_child_item(
            None,
            "item/completed",
            &json!({
                "kind": "workflow",
                "itemId": "wf-1",
                "revision": 1,
                "status": "cancelled",
                "scriptId": "research",
            }),
        )
        .unwrap();
        assert_eq!(
            child_tool_frame("s", "wf-1", &cancelled, 1)["params"]["update"]["status"],
            json!("failed"),
            "v1 has no cancelled"
        );
        assert_eq!(
            child_tool_frame("s", "wf-1", &cancelled, 2)["params"]["update"]["status"],
            json!("cancelled")
        );
    }

    #[test]
    fn children_cards_filter_sort_and_mark() {
        let mut children = HashMap::new();
        for (id, item) in [
            (
                "z-done",
                json!({"kind": "subagent", "itemId": "z-done", "revision": 2,
                       "status": "completed", "objective": "Zebra"}),
            ),
            (
                "a-run",
                json!({"kind": "subagent", "itemId": "a-run", "revision": 1,
                       "status": "inProgress", "objective": "Apple",
                       "controlStatus": "running"}),
            ),
            (
                "m-weird",
                json!({"kind": "subagent", "itemId": "m-weird", "revision": 1,
                       "status": "zonked", "objective": "Mango"}),
            ),
            (
                "w-flow",
                json!({"kind": "workflow", "itemId": "w-flow", "revision": 1,
                       "status": "inProgress", "scriptId": "research"}),
            ),
        ] {
            let (record, _) = fold_child_item(None, "item/updated", &item).unwrap();
            children.insert(id.to_string(), record);
        }
        let card = render_children_card(
            "Subagents",
            "subagent",
            &children,
            "No subagents observed yet this session.",
        );
        for want in [
            "**Subagents**",
            "- [~] Apple (running)",
            "- [!] Mango",
            "- [x] Zebra",
        ] {
            assert!(card.contains(want), "card has {want}: {card}");
        }
        assert!(!card.contains("research"), "workflows stay out: {card}");
        assert!(
            card.find("Apple").unwrap() < card.find("Mango").unwrap(),
            "sorted by title: {card}"
        );
        let flows = render_children_card(
            "Workflows",
            "workflow",
            &children,
            "No workflow runs observed yet this session.",
        );
        assert!(flows.contains("- [~] research"), "{flows}");
        assert!(!flows.contains("Apple"), "{flows}");
        assert!(
            render_children_card("Subagents", "subagent", &HashMap::new(), "empty-mark")
                .contains("empty-mark")
        );
    }

    #[test]
    fn seed_children_keeps_resumed_history_and_running_open() {
        let mut children = HashMap::new();
        seed_children(
            &mut children,
            &json!({"history": {"items": [
                {"kind": "subagent", "itemId": "old", "revision": 3,
                 "status": "completed", "objective": "Old work"},
                {"kind": "workflow", "itemId": "live", "revision": 1,
                 "status": "inProgress", "scriptId": "research"},
                {"kind": "toolCall", "itemId": "t", "revision": 1},
                {"kind": "subagent", "revision": 1, "objective": "no id"},
            ]}}),
        );
        assert_eq!(children.len(), 2);
        assert!(children["old"].terminal);
        assert!(!children["live"].terminal);
        assert_eq!(children["live"].title, "research");
        // Unknown shapes seed nothing, never fail.
        seed_children(&mut children, &json!({}));
        seed_children(&mut children, &json!({"history": {"mode": "none"}}));
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn replay_history_renders_resumed_children_as_blocks() {
        let frames = replay_history(
            "s",
            1,
            &json!({"history": {"items": [
                {"kind": "subagent", "itemId": "child-1", "revision": 2,
                 "status": "failed", "objective": "Explore X",
                 "controlStatus": "closed", "failureReason": "boom"},
            ]}}),
        );
        assert_eq!(frames.len(), 1);
        let update = &frames[0]["params"]["update"];
        assert_eq!(update["sessionUpdate"], json!("tool_call"));
        assert_eq!(update["toolCallId"], json!("child-1"));
        assert_eq!(update["status"], json!("failed"));
        assert_eq!(
            update["content"],
            json!([{"type": "text", "text": "closed · boom"}])
        );
    }

    #[test]
    fn subagent_verbs_parse_with_aliases() {
        for (word, method, needs_body) in [
            ("stop", "subagent/stop", false),
            ("KILL", "subagent/stop", false),
            ("halt", "subagent/stop", false),
            ("interrupt", "subagent/interrupt", false),
            ("yield", "subagent/interrupt", false),
            ("pause", "subagent/interrupt", false),
            ("close", "subagent/close", false),
            ("resume", "subagent/resume", false),
            ("reopen", "subagent/reopen", false),
            ("message", "subagent/sendMessage", true),
            ("msg", "subagent/sendMessage", true),
            ("note", "subagent/sendMessage", true),
            ("tell", "subagent/sendMessage", true),
            ("followup", "subagent/followupTask", true),
            ("follow-up", "subagent/followupTask", true),
            ("task", "subagent/followupTask", true),
            ("result", "subagent/readResult", false),
            ("read", "subagent/readResult", false),
        ] {
            let verb = parse_subagent_verb(word).unwrap_or_else(|| panic!("{word} parses"));
            assert_eq!(verb.method, method, "{word}");
            assert_eq!(verb.needs_body, needs_body, "{word}");
        }
        assert_eq!(SUBAGENT_VERBS.len(), 8);
        assert!(parse_subagent_verb("explode").is_none());
        assert!(parse_subagent_verb("").is_none());
    }

    #[test]
    fn subagent_targets_resolve_prefixes() {
        let mut children = HashMap::new();
        for (id, item) in [
            (
                "child-1",
                json!({"kind": "subagent", "itemId": "child-1", "revision": 1,
                       "status": "inProgress", "objective": "Explore",
                       "subagentId": "sa-1111"}),
            ),
            (
                "child-2",
                json!({"kind": "subagent", "itemId": "child-2", "revision": 1,
                       "status": "inProgress", "objective": "Build",
                       "subagentId": "sa-2222"}),
            ),
            (
                "wf-1",
                json!({"kind": "workflow", "itemId": "wf-1", "revision": 1,
                       "status": "inProgress", "scriptId": "research"}),
            ),
        ] {
            let (record, _) = fold_child_item(None, "item/updated", &item).unwrap();
            children.insert(id.to_string(), record);
        }
        // Exact + prefix + durable-id + case-insensitive ("child-"
        // hits both children, so it resolves under Many below).
        for target in ["child-1", "sa-1111", "sa-11", "SA-11", "Child-1"] {
            assert!(
                matches!(resolve_subagent_target(&children, target), ChildTarget::One(id) if id == "child-1"),
                "{target} resolves to child-1"
            );
        }
        // Ambiguity sorts by title for stable picker order.
        match resolve_subagent_target(&children, "child") {
            ChildTarget::Many(rows) => {
                let titles: Vec<&str> = rows.iter().map(|(_, c)| c.title.as_str()).collect();
                assert_eq!(titles, vec!["Build", "Explore"]);
            }
            _ => panic!("expected Many for the shared prefix"),
        }
        assert!(matches!(
            resolve_subagent_target(&children, "wf"),
            ChildTarget::WorkflowOnly
        ));
        assert!(matches!(
            resolve_subagent_target(&children, "zzz"),
            ChildTarget::None
        ));
        assert!(matches!(
            resolve_subagent_target(&children, ""),
            ChildTarget::None
        ));
    }

    #[test]
    fn subagent_options_label_rows() {
        let mut children = HashMap::new();
        for (id, item) in [
            (
                "child-1",
                json!({"kind": "subagent", "itemId": "child-1", "revision": 1,
                       "status": "inProgress", "objective": "Explore"}),
            ),
            (
                "child-2",
                json!({"kind": "subagent", "itemId": "child-2", "revision": 1,
                       "status": "failed", "objective": "Build"}),
            ),
        ] {
            let (record, _) = fold_child_item(None, "item/updated", &item).unwrap();
            children.insert(id.to_string(), record);
        }
        let rows: Vec<(&str, &ChildRecord)> =
            children.iter().map(|(id, c)| (id.as_str(), c)).collect();
        assert_eq!(
            subagent_options(&rows),
            vec![
                ("child-2".to_string(), "[!] Build (child-2)".to_string()),
                ("child-1".to_string(), "[~] Explore (child-1)".to_string()),
            ]
        );
    }

    #[test]
    fn result_card_renders_and_caps() {
        let record = |summary: &str, text: &str| ChildRecord {
            kind: "subagent".to_string(),
            title: "Explore".to_string(),
            status: "completed".to_string(),
            detail: String::new(),
            rev: 2,
            terminal: true,
            subagent_id: "sa-1".to_string(),
            result_summary: summary.to_string(),
            result_text: text.to_string(),
        };
        let card = render_result_card("Explore", &record("short summary", "line one\nline two"));
        for want in ["**Result: Explore**", "short summary", "line one\nline two"] {
            assert!(card.contains(want), "{want}: {card}");
        }
        let long = "x".repeat(3000);
        let capped = render_result_card("Explore", &record("", &long));
        assert!(capped.contains('…'), "long text caps: {capped}");
        assert!(!capped.contains(&long), "full text never inlines");
        assert!(render_result_card("Explore", &record("", "")).contains("No result retained yet."));
    }

    #[test]
    fn fold_retains_durable_id_and_result() {
        let (first, _) = fold_child_item(
            None,
            "item/started",
            &json!({
                "kind": "subagent", "itemId": "child-1", "revision": 1,
                "objective": "Explore", "subagentId": "sa-1",
            }),
        )
        .unwrap();
        assert_eq!(first.subagent_id, "sa-1");
        assert!(first.result_summary.is_empty());
        // A result landing announces (new detail + new fields).
        let (done, announce) = fold_child_item(
            Some(&first),
            "item/completed",
            &json!({
                "kind": "subagent", "itemId": "child-1", "revision": 2,
                "status": "completed", "objective": "Explore", "subagentId": "sa-1",
                "result": {"summary": "found it", "text": "the thing"},
            }),
        )
        .unwrap();
        assert!(announce);
        assert_eq!(done.result_summary, "found it");
        assert_eq!(done.result_text, "the thing");
        assert!(done.detail.contains("result: found it"), "{}", done.detail);
    }

    #[test]
    fn dedupe_labels_number_repeats_and_map_rejects_outsiders() {
        let options = vec![
            ("a".to_string(), "Same".to_string()),
            ("b".to_string(), "Same".to_string()),
            ("c".to_string(), "Other".to_string()),
        ];
        let display = dedupe_labels(&options);
        assert_eq!(display, vec!["Same", "Same (2)", "Other"]);
        assert_eq!(map_position(&display, "Same (2)"), Some(1));
        assert_eq!(map_position(&display, "Missing"), None);
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
                {"itemId": "x1", "kind": "reminderChild", "text": "noise"},
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
