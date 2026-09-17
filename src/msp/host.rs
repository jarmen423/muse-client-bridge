//! Supervised MSP host connection: handshake, commands, retry, restart.
//!
//! One [`MspConnection`] owns a `muse serve` child's stdio: a writer, a
//! `pending` correlation map, a UUIDv7 minter, and a broadcast fan-out of
//! host events. A dedicated reader task drains stdout from birth (before the
//! handshake), so a slow consumer wedges only itself, never the host.
//!
//! [`Supervisor`] keeps one live connection across host deaths: restart ≤3
//! with 250/500/1000 ms backoff when the profile is durable (or absent),
//! fail closed on ephemeral profiles and never-self-healing exits.
//!
//! Ack ≠ outcome everywhere: every command's truth arrives on the view
//! stream. This module delivers acks and routes events; [`crate::msp::fold`]
//! (P3) folds them into outcomes.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::io::BufReader;
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::{broadcast, oneshot, watch};

use super::proto::{
    ErrorObject, FRAME_LIMIT_BYTES, IdMinter, IncomingFrame, Pending, RpcId, classify_frame,
    method_not_found_frame, next_frame, notification_frame, ok_response_frame, request_frame,
    write_frame,
};
use super::spawn::{ChildHandle, ChildSpec, ExitClass, StderrTail, spawn_child};

/// Envelope schema version this bridge understands (`schema.version`).
pub const SUPPORTED_SCHEMA_VERSION: u64 = 1;
/// Stable-surface fingerprint of the validated host (muse 1.2.1, local
/// `muse schema` export). A mismatch warns, never fails (SPEC §4.1).
pub const PINNED_FINGERPRINT: &str =
    "sha256:c7ff6c5d1e89cd42f803aea1f05b8e72082f2099685802473eb726903484713b";

/// Maximum `command()` attempts per logical command: 1 send + up to 2
/// same-`commandId` retries on backpressure kinds.
pub const MAX_COMMAND_ATTEMPTS: u32 = 3;
/// Restart budget: cumulative relaunch attempts per bridge process.
pub const MAX_RESTARTS: u32 = 3;
/// Relaunch backoff per consumed attempt (SPEC §4.4).
pub const RESTART_BACKOFF_MS: [u64; 3] = [250, 500, 1000];
/// Broadcast fan-out depth for host events. A lagging receiver gets `Lagged`
/// and must resync via `view/page` (P4); the host never waits for us.
const EVENT_CHANNEL_DEPTH: usize = 1024;
/// Client-side `commandId` registry cap (bounded: retries land within
/// seconds; conflicts are immediate bugs, so eviction only blinds ancient
/// history).
const COMMAND_ID_REGISTRY_CAP: usize = 4096;
/// How long [`Supervisor::ready`] waits out a restart storm before 503.
const READY_DEADLINE: Duration = Duration::from_secs(30);

/// Host launch configuration (flags + env, resolved once at startup).
#[derive(Debug, Clone)]
pub struct HostConfig {
    /// Binary path or bare name (`MUSE_CLI` or `muse`).
    pub bin: String,
    /// Subcommand, normally `Some("serve")` (`None` only drives test fakes).
    pub subcommand: Option<String>,
    /// Extra serve args (`MUSE_SERVE_ARGS`, whitespace-split, + passthroughs).
    pub serve_args: Vec<String>,
    /// Working directory (bridge `--workspace-root`).
    pub cwd: PathBuf,
    /// Extra child env (tests use this for fake-host knobs; production
    /// inherits the bridge env untouched).
    pub extra_env: Vec<(String, String)>,
    /// `MUSE_COMMAND_TIMEOUT_MS` override (milliseconds, when positive).
    pub timeout_override_ms: Option<u64>,
    /// Retry backoff base in milliseconds (default 200; tests shrink it).
    pub retry_base_delay_ms: u64,
}

impl HostConfig {
    /// Resolve from flags + process env (production path).
    pub fn from_env(cwd: PathBuf, trust_workspace: bool) -> Self {
        let bin = std::env::var("MUSE_CLI").unwrap_or_else(|_| "muse".to_string());
        let mut serve_args =
            split_serve_args(&std::env::var("MUSE_SERVE_ARGS").unwrap_or_default());
        if trust_workspace && !serve_args.iter().any(|a| a == "--trust-workspace") {
            serve_args.push("--trust-workspace".to_string());
        }
        Self {
            bin,
            subcommand: Some("serve".to_string()),
            serve_args,
            cwd,
            extra_env: Vec::new(),
            timeout_override_ms: parse_timeout_override(
                std::env::var("MUSE_COMMAND_TIMEOUT_MS").ok().as_deref(),
            ),
            retry_base_delay_ms: 200,
        }
    }

    fn child_spec(&self) -> ChildSpec {
        ChildSpec {
            bin: self.bin.clone(),
            subcommand: self.subcommand.clone(),
            args: self.serve_args.clone(),
            cwd: self.cwd.clone(),
            extra_env: self.extra_env.clone(),
        }
    }
}

/// Compatibility verdict from the handshake (SPEC §4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatStatus {
    /// Fingerprint matches the validated pin.
    Tested,
    /// Fingerprint differs: warn loudly, continue (Developer Preview drift).
    FingerprintMismatch,
}

/// Host facts captured at handshake, for routing and diagnostics.
#[derive(Debug, Clone)]
pub struct HandshakeInfo {
    /// `serverInfo.name`.
    pub server_name: String,
    /// `serverInfo.version`.
    pub server_version: String,
    /// `schema.version` (must be 1; anything else is fatal before this).
    pub schema_version: Option<u64>,
    /// `schema.fingerprint` (`None` when the host omits it).
    pub fingerprint: Option<String>,
    /// Pin verdict (warn-only on mismatch).
    pub compat: CompatStatus,
    /// `sessionDurability`: absent means durable; unknown values carry no
    /// recovery guarantee and fail closed like ephemeral.
    pub durability: Option<String>,
    /// `grantedCapabilities`: the negotiated grant subset, fixed for the
    /// connection lifetime. Unknown entries never appear (the host simply
    /// withholds them); an absent member reads as no grants.
    pub granted_capabilities: Vec<String>,
}

impl HandshakeInfo {
    /// Whether a dead host of this profile may be restarted. Only `durable`
    /// (including the absent-means-durable arm) carries the recovery
    /// guarantee; `ephemeral` and unknown values fail closed.
    pub fn restartable(&self) -> bool {
        matches!(self.durability.as_deref(), None | Some("durable"))
    }

    /// `name/version` label for logs.
    pub fn host_label(&self) -> String {
        format!("{}/{}", self.server_name, self.server_version)
    }

    /// Whether the host granted the `userShell` capability at `initialize`
    /// (gates `session/userShell`; enforced host-side, recorded here for
    /// diagnostics and honest ACP behavior).
    pub fn supports_user_shell(&self) -> bool {
        self.granted_capabilities
            .iter()
            .any(|cap| cap == "userShell")
    }
}

/// Liveness of one connection (reader-owned, watched by waiters).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostState {
    /// Reader draining; commands may flow.
    Alive,
    /// Reader saw EOF/error: no command on this connection will complete.
    Dead {
        /// Stable local reason (never host text).
        message: String,
    },
}

/// One fanned-out host event (ack-plane responses never appear here: they
/// resolve `pending` directly in the reader).
#[derive(Debug, Clone)]
pub enum HostEvent {
    /// Server→client notification (view events, gap, settlements, …).
    Notification {
        /// Slash-namespaced method.
        method: String,
        /// Raw params (`Null` when omitted).
        params: Value,
    },
    /// Server-initiated request, already acked `{}`. Only methods accepted
    /// by [`known_server_request`] are forwarded; the payload still needs
    /// handling (the ack is a presentation receipt, not a decision).
    ServerRequest {
        /// `approval/request` or `userInput/request`.
        method: String,
        /// Raw params (`Null` when omitted).
        params: Value,
    },
}

/// Validated outcome of one command's ack: object result or typed error.
pub type CommandOutcome = Result<Value, ErrorObject>;

/// One in-flight command: the ack waiter plus its method (for diagnostics
/// when the response itself is malformed).
struct Waiter {
    tx: oneshot::Sender<CommandOutcome>,
    method: String,
}

/// Server-initiated request methods this bridge handles. Everything else
/// gets typed `methodNotFound`, never a synthetic `{}`.
pub fn known_server_request(method: &str) -> bool {
    matches!(method, "approval/request" | "userInput/request")
}

/// Default admission-ack budget for unclassified commands.
const DEFAULT_TIMEOUT_MS: u64 = 60_000;

/// Method-aware admission-ack budgets (SPEC §4.2). Acks are admission-only:
/// a turn may run for minutes after `turn/start` accepts.
fn method_timeout_ms(method: &str) -> u64 {
    match method {
        // Handshake: bounded startup, room for a cold binary start.
        "initialize" => 30_000,
        // Lifecycle/history work can page and replay large views.
        "session/start" | "session/resume" | "session/fork" | "session/read"
        | "session/compact" | "view/page" | "item/readOutput" => 180_000,
        // Cheap queries.
        "model/list" | "session/list" | "view/subscribe" | "view/unsubscribe" => 30_000,
        // Control-plane decisions should be fast but not flaky.
        "approval/decide" | "userInput/answer" | "userInput/cancel" | "userInput/clarify" => 30_000,
        // Turn control (start/steer/interrupt/cancel/unqueue), model/effort
        // selection, and rename/userShell ride the 60 s default.
        _ => DEFAULT_TIMEOUT_MS,
    }
}

/// Resolve a command timeout from the env override and the method table.
/// Invalid/non-positive overrides fall back to the table (never disable).
fn command_timeout(override_ms: Option<u64>, method: &str) -> Duration {
    if let Some(ms) = override_ms
        && ms > 0
    {
        return Duration::from_millis(ms);
    }
    Duration::from_millis(method_timeout_ms(method))
}

/// Whether an ack error is retryable under the same `commandId` (SPEC §4.2):
/// `overloaded` and `backpressured` ONLY, and never when the host explicitly
/// marks this error non-retryable.
fn is_backpressure(error: &ErrorObject) -> bool {
    if error.retryable() == Some(false) {
        return false;
    }
    matches!(error.kind(), Some("overloaded" | "backpressured"))
}

/// Backoff cap for retry `attempt` (0-based): `base * 2^attempt`, 30 s max.
fn backoff_cap_ms(attempt: u32, base_ms: u64) -> u64 {
    base_ms.saturating_mul(1 << attempt.min(16)).min(30_000)
}

/// Jittered backoff delay: uniform in `[0, cap]` ("full jitter").
fn backoff_delay_ms(attempt: u32, base_ms: u64) -> u64 {
    let cap = backoff_cap_ms(attempt, base_ms);
    if cap == 0 {
        return 0;
    }
    let bytes = uuid::Uuid::new_v4().into_bytes();
    let rand = u64::from_be_bytes(bytes[0..8].try_into().expect("8 bytes"));
    rand % (cap + 1)
}

/// Client-side `commandId` registry: retries reuse the id with the IDENTICAL
/// payload; reusing one id with a different payload is a client bug.
/// Returns `Err(())` on conflict. Bounded (see [`COMMAND_ID_REGISTRY_CAP`]).
fn remember_command_id(
    registry: &mut HashMap<String, u64>,
    command_id: &str,
    method: &str,
    params: &Value,
) -> Result<(), ()> {
    let mut hasher = DefaultHasher::new();
    method.hash(&mut hasher);
    // `Value` serializes deterministically (sorted keys).
    serde_json::to_string(params)
        .unwrap_or_default()
        .hash(&mut hasher);
    let hash = hasher.finish();
    match registry.get(command_id) {
        Some(previous) if *previous != hash => Err(()),
        Some(_) => Ok(()),
        None => {
            if registry.len() >= COMMAND_ID_REGISTRY_CAP
                && let Some(victim) = registry.keys().next().cloned()
            {
                registry.remove(&victim);
            }
            registry.insert(command_id.to_string(), hash);
            Ok(())
        }
    }
}

/// One live MSP connection: writer, correlation, minter, event fan-out.
///
/// Obtain via [`launch`] (handshake included) or [`MspConnection::spawn`]
/// (handshake-less; commands other than `initialize` are rejected until
/// [`MspConnection::handshake`] completes).
pub struct MspConnection {
    writer: tokio::sync::Mutex<Option<ChildStdin>>,
    pending: Mutex<Pending<Waiter>>,
    next_id: AtomicU64,
    minter: IdMinter,
    used_command_ids: Mutex<HashMap<String, u64>>,
    handshook: AtomicBool,
    handshake: Mutex<Option<HandshakeInfo>>,
    events: broadcast::Sender<HostEvent>,
    state_tx: watch::Sender<HostState>,
    state_rx: watch::Receiver<HostState>,
    stderr: Arc<StderrTail>,
    timeout_override_ms: Option<u64>,
    retry_base_delay_ms: u64,
}

impl MspConnection {
    fn new(
        stdin: ChildStdin,
        stderr: Arc<StderrTail>,
        timeout_override_ms: Option<u64>,
        retry_base_delay_ms: u64,
    ) -> Arc<Self> {
        let (events, _) = broadcast::channel(EVENT_CHANNEL_DEPTH);
        let (state_tx, state_rx) = watch::channel(HostState::Alive);
        Arc::new(Self {
            writer: tokio::sync::Mutex::new(Some(stdin)),
            pending: Mutex::new(Pending::new()),
            next_id: AtomicU64::new(1),
            minter: IdMinter::new(),
            used_command_ids: Mutex::new(HashMap::new()),
            handshook: AtomicBool::new(false),
            handshake: Mutex::new(None),
            events,
            state_tx,
            state_rx,
            stderr,
            timeout_override_ms,
            retry_base_delay_ms,
        })
    }

    /// Spawn the child and start the reader WITHOUT handshaking. The reader
    /// drains from birth; handshake-less commands are rejected locally.
    /// Prefer [`launch`]; this exists for stepwise control and tests.
    pub async fn spawn(config: &HostConfig) -> Result<SpawnedHost, String> {
        let spawned = spawn_child(&config.child_spec())?;
        let stderr = spawned.handle.stderr.clone();
        let conn = Self::new(
            spawned.stdin,
            stderr,
            config.timeout_override_ms,
            config.retry_base_delay_ms,
        );
        tokio::spawn(reader_loop(conn.clone(), spawned.stdout));
        Ok(SpawnedHost {
            conn,
            child: spawned.handle,
        })
    }

    /// Run the handshake: `initialize` → validate → `initialized`.
    /// Fatal on `schema.version != 1`; fingerprint mismatch warns only.
    pub async fn handshake(&self) -> Result<HandshakeInfo, LaunchError> {
        let params = serde_json::json!({
            "clientInfo": {
                "name": "muse_bridge",
                "version": env!("CARGO_PKG_VERSION"),
            },
            "capabilities": {
                "requestedCapabilities": ["userShell"],
            },
        });
        let result = self
            .command("initialize", &params)
            .await
            .map_err(LaunchError::Handshake)?;
        let info = classify_handshake(&result)?;
        tracing::info!(
            host = %info.host_label(),
            schema_version = ?info.schema_version,
            fingerprint = %info.fingerprint.as_deref().unwrap_or("absent"),
            compat = ?info.compat,
            durability = %info.durability.as_deref().unwrap_or("durable(absent)"),
            granted = ?info.granted_capabilities,
            "msp handshake complete"
        );
        if info.compat == CompatStatus::FingerprintMismatch {
            tracing::warn!(
                live = %info.fingerprint.as_deref().unwrap_or("absent"),
                pinned = PINNED_FINGERPRINT,
                "schema fingerprint differs from the validated pin; continuing (Developer Preview drift)"
            );
        }
        *self.handshake.lock().unwrap_or_else(|p| p.into_inner()) = Some(info.clone());
        self.handshook.store(true, Ordering::SeqCst);
        self.notify("initialized", None)
            .await
            .map_err(LaunchError::HandshakeNotify)?;
        Ok(info)
    }

    /// Mint one UUIDv7 `commandId` from this connection's minter. Fresh id
    /// per LOGICAL command; retries reuse it (AGENTS rules 2–3).
    pub fn mint_command_id(&self) -> String {
        self.minter.mint()
    }

    /// Send one command; resolve its ACK (object result or typed error).
    /// The ack is admission-only — outcomes arrive on the view stream.
    pub async fn command(&self, method: &str, params: &Value) -> Result<Value, ErrorObject> {
        if method != "initialize" && !self.handshook.load(Ordering::SeqCst) {
            return Err(ErrorObject::local(
                -32600,
                format!("command {method} sent before the handshake completed"),
                "notInitialized",
            ));
        }
        if let Some(command_id) = params.get("commandId").and_then(Value::as_str) {
            let mut registry = self
                .used_command_ids
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if remember_command_id(&mut registry, command_id, method, params).is_err() {
                return Err(ErrorObject::local(
                    -32603,
                    format!("BUG: commandId {command_id} reused with a different payload"),
                    "commandIdConflict",
                ));
            }
        }
        let id = RpcId::Int(self.next_id.fetch_add(1, Ordering::SeqCst) as i64);
        let timeout = command_timeout(self.timeout_override_ms, method);
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                &id,
                Waiter {
                    tx,
                    method: method.to_string(),
                },
            );
        if let Err(e) = self.write_frame(&request_frame(&id, method, params)).await {
            self.pending
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&id);
            return Err(map_write_error(method, e));
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => Err(ErrorObject::local(
                -32603,
                "serve host closed the connection",
                "hostDead",
            )),
            Err(_) => {
                self.pending
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(&id);
                Err(ErrorObject::local(
                    -32603,
                    format!(
                        "serve command timed out after {}ms method={method} id={}{}",
                        timeout.as_millis(),
                        id.to_value(),
                        session_suffix(params),
                    ),
                    "timeout",
                ))
            }
        }
    }

    /// [`MspConnection::command`] with the specified retry: ONLY `overloaded`/
    /// `backpressured` retry, same `commandId` (identical `params`), jittered
    /// backoff, [`MAX_COMMAND_ATTEMPTS`] total attempts.
    pub async fn command_with_retry(
        &self,
        method: &str,
        params: &Value,
    ) -> Result<Value, ErrorObject> {
        let mut retries: u32 = 0;
        loop {
            match self.command(method, params).await {
                Err(error) if is_backpressure(&error) && retries + 1 < MAX_COMMAND_ATTEMPTS => {
                    let delay = backoff_delay_ms(retries, self.retry_base_delay_ms);
                    tracing::warn!(
                        method,
                        kind = error.kind(),
                        retry = retries + 1,
                        max_attempts = MAX_COMMAND_ATTEMPTS,
                        delay_ms = delay,
                        "host backpressure; retrying with the same commandId"
                    );
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    retries += 1;
                }
                outcome => return outcome,
            }
        }
    }

    /// Client→server notification (no id, no response).
    pub async fn notify(&self, method: &str, params: Option<&Value>) -> Result<(), ErrorObject> {
        if method != "initialized" && !self.handshook.load(Ordering::SeqCst) {
            return Err(ErrorObject::local(
                -32600,
                format!("notification {method} sent before the handshake completed"),
                "notInitialized",
            ));
        }
        self.write_frame(&notification_frame(method, params))
            .await
            .map_err(|e| map_write_error(method, e))
    }

    /// Subscribe to fanned-out host events. Subscribe BEFORE the command
    /// whose events matter: `session/started` can precede its ack.
    pub fn subscribe(&self) -> broadcast::Receiver<HostEvent> {
        self.events.subscribe()
    }

    /// Watch this connection's liveness (reader-owned).
    pub fn watch_state(&self) -> watch::Receiver<HostState> {
        self.state_rx.clone()
    }

    /// Whether the reader is still draining (cheap; for fast paths).
    pub fn is_alive(&self) -> bool {
        matches!(*self.state_rx.borrow(), HostState::Alive)
    }

    /// Handshake facts (`None` until [`MspConnection::handshake`] succeeds).
    pub fn handshake_info(&self) -> Option<HandshakeInfo> {
        self.handshake
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Bounded stderr tail (for failure detail and `--support`).
    pub fn stderr_tail(&self) -> Arc<StderrTail> {
        self.stderr.clone()
    }

    /// Take the stdin pipe (shutdown path: dropping it sends EOF).
    pub fn take_stdin(&self) -> Option<ChildStdin> {
        self.writer.try_lock().ok()?.take()
    }

    async fn write_frame(&self, frame: &Value) -> Result<(), WriteError> {
        let mut guard = self.writer.lock().await;
        let Some(stdin) = guard.as_mut() else {
            return Err(WriteError::StdinGone);
        };
        write_frame(stdin, frame).await.map_err(WriteError::Frame)
    }
}

/// Reader task: drain stdout from birth, route every frame, mark death.
///
/// Owns nothing but the pipe and an `Arc` to the connection: responses
/// resolve `pending`, server requests are acked + fanned out, notifications
/// are fanned out, and EOF/error fails every waiter and flips [`HostState`].
async fn reader_loop(conn: Arc<MspConnection>, stdout: ChildStdout) {
    let mut reader = BufReader::new(stdout);
    let death = loop {
        match next_frame(&mut reader).await {
            Ok(None) => break "host stdout closed (EOF)".to_string(),
            Ok(Some(frame)) => route_frame(&conn, &frame).await,
            Err(e) => match e {
                // Inbound oversize is warn-and-skip inside `next_frame`;
                // this arm is statically unreachable.
                super::proto::FrameError::Oversize { .. } => continue,
                super::proto::FrameError::Io(io) => {
                    break format!("host stdout read error: {io}");
                }
            },
        }
    };
    tracing::warn!(message = %death, "msp host connection closed");
    let _ = conn.state_tx.send(HostState::Dead { message: death });
    conn.pending
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clear();
}

async fn route_frame(conn: &MspConnection, frame: &Value) {
    match classify_frame(frame) {
        IncomingFrame::Response { id, body } => {
            let waiter = conn
                .pending
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&id);
            match waiter {
                Some(waiter) => {
                    let _ = waiter.tx.send(body);
                }
                None => {
                    tracing::warn!(id = %id.to_value(), "response for unknown request id");
                }
            }
        }
        IncomingFrame::Invalid { reason, id } => {
            tracing::warn!(reason, id = ?id.as_ref().map(RpcId::to_value), "invalid inbound frame");
            // Fail that waiter now with a local protocol error instead of
            // hanging it until timeout. The connection survives.
            if let Some(id) = id
                && let Some(waiter) = conn
                    .pending
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(&id)
            {
                let _ = waiter.tx.send(Err(ErrorObject::local(
                    -32603,
                    format!("malformed response to {}: {reason}", waiter.method),
                    "protocolError",
                )));
            }
        }
        IncomingFrame::Notification { method, params } => {
            tracing::debug!(
                method,
                session = params
                    .get("sessionId")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
                "host notification"
            );
            let _ = conn.events.send(HostEvent::Notification { method, params });
        }
        IncomingFrame::ServerRequest { id, method, params } => {
            if known_server_request(&method) {
                if let Err(e) = conn.write_frame(&ok_response_frame(&id)).await {
                    tracing::warn!(method, error = %e.describe(), "failed to ack server request");
                }
                let _ = conn
                    .events
                    .send(HostEvent::ServerRequest { method, params });
            } else {
                tracing::warn!(
                    method,
                    "unknown server-initiated method; replying methodNotFound"
                );
                if let Err(e) = conn
                    .write_frame(&method_not_found_frame(&id, &method))
                    .await
                {
                    tracing::warn!(method, error = %e.describe(), "failed to reply methodNotFound");
                }
            }
        }
    }
}

/// Writer failure (stdin gone or frame refused).
enum WriteError {
    StdinGone,
    Frame(super::proto::FrameError),
}

impl WriteError {
    fn describe(&self) -> String {
        match self {
            WriteError::StdinGone => "host stdin closed".to_string(),
            WriteError::Frame(e) => e.to_string(),
        }
    }
}

/// Map a failed send to a local ack error. Any stdin write failure means a
/// dead/dying host. An oversize refusal is OUR request too big for the wire:
/// kind `frameTooLarge` (caller 400), deliberately distinct from the host's
/// `inputTooLarge` (context 400, SPEC §6 row 3).
fn map_write_error(method: &str, error: WriteError) -> ErrorObject {
    match error {
        WriteError::Frame(super::proto::FrameError::Oversize { len }) => ErrorObject {
            code: -32002,
            message: format!(
                "refusing to send {method}: frame is {len} bytes, over the {FRAME_LIMIT_BYTES}-byte cap"
            ),
            data: Some(
                serde_json::json!({"kind": "frameTooLarge", "limitBytes": FRAME_LIMIT_BYTES}),
            ),
        },
        WriteError::StdinGone | WriteError::Frame(_) => ErrorObject::local(
            -32603,
            format!("serve write failed for {method}: host unreachable"),
            "hostDead",
        ),
    }
}

/// `" session=<id>"` timeout diagnostics when params carry a session.
fn session_suffix(params: &Value) -> String {
    params
        .get("sessionId")
        .and_then(Value::as_str)
        .map(|s| format!(" session={s}"))
        .unwrap_or_default()
}

/// Validate an `initialize` result into handshake facts.
/// `schema.version != 1` is fatal; fingerprint mismatch is warn-only.
fn classify_handshake(result: &Value) -> Result<HandshakeInfo, LaunchError> {
    let schema = result.get("schema");
    let schema_version = schema
        .and_then(|s| s.get("version"))
        .and_then(Value::as_u64);
    let fingerprint = schema
        .and_then(|s| s.get("fingerprint"))
        .and_then(Value::as_str)
        .map(str::to_string);
    if schema_version != Some(SUPPORTED_SCHEMA_VERSION) {
        return Err(LaunchError::IncompatibleSchema {
            version: schema_version,
            fingerprint: fingerprint.unwrap_or_else(|| "absent".to_string()),
        });
    }
    let compat = match fingerprint.as_deref() {
        Some(fp) if fp == PINNED_FINGERPRINT => CompatStatus::Tested,
        _ => CompatStatus::FingerprintMismatch,
    };
    let server = result.get("serverInfo");
    let granted_capabilities = result
        .get("grantedCapabilities")
        .and_then(Value::as_array)
        .map(|grants| {
            grants
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    Ok(HandshakeInfo {
        server_name: server
            .and_then(|s| s.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        server_version: server
            .and_then(|s| s.get("version"))
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        schema_version,
        fingerprint,
        compat,
        durability: result
            .get("sessionDurability")
            .and_then(Value::as_str)
            .map(str::to_string),
        granted_capabilities,
    })
}

/// A spawned + draining host (handshake state depends on the constructor).
pub struct SpawnedHost {
    /// The MSP connection (reader draining from birth).
    pub conn: Arc<MspConnection>,
    /// The child (reap/kill/shutdown + stderr tail).
    pub child: ChildHandle,
}

/// Launch failure.
#[derive(Debug)]
pub enum LaunchError {
    /// `muse serve` would not spawn (actionable text).
    Spawn(String),
    /// `initialize` failed (timeout, host error, connection died).
    Handshake(ErrorObject),
    /// `schema.version != 1`: this bridge cannot drive this host.
    IncompatibleSchema {
        /// Reported version (`None` when absent).
        version: Option<u64>,
        /// Reported fingerprint (or `"absent"`).
        fingerprint: String,
    },
    /// `initialized` notify could not be sent.
    HandshakeNotify(ErrorObject),
}

impl std::fmt::Display for LaunchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LaunchError::Spawn(e) => write!(f, "{e}"),
            LaunchError::Handshake(e) => write!(f, "serve initialize failed: {}", e.message),
            LaunchError::IncompatibleSchema {
                version,
                fingerprint,
            } => write!(
                f,
                "incompatible host schema: version={} fingerprint={fingerprint}; upgrade muse-bridge",
                version.map(|v| v.to_string()).unwrap_or("absent".into()),
            ),
            LaunchError::HandshakeNotify(e) => {
                write!(f, "serve initialized notify failed: {}", e.message)
            }
        }
    }
}

impl std::error::Error for LaunchError {}

/// Spawn the host and complete the handshake. On failure the child is killed
/// and reaped (no orphans); the error names the remedy.
pub async fn launch(config: &HostConfig) -> Result<SpawnedHost, LaunchError> {
    let spawned = MspConnection::spawn(config)
        .await
        .map_err(LaunchError::Spawn)?;
    match spawned.conn.handshake().await {
        Ok(_) => Ok(spawned),
        Err(e) => {
            spawned.child.kill_and_reap().await;
            Err(e)
        }
    }
}

/// Why the supervisor stopped serving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExhaustReason {
    /// The host exited 2/3/5: restarting identically cannot help.
    NonRestartableExit {
        /// The exit classification.
        exit: ExitClass,
    },
    /// Ephemeral (or unknown) durability: nothing survives the host.
    EphemeralHost,
    /// Restart budget spent.
    BudgetSpent {
        /// Last classified exit (`None` when a relaunch itself failed).
        last_exit: Option<ExitClass>,
    },
}

/// Supervisor serving state (for `/healthz` and request routing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorStatus {
    /// A live connection is serving.
    Serving,
    /// Between death and relaunch (backoff in flight).
    Restarting,
    /// No live host and no restart coming.
    Exhausted {
        /// Why serving stopped.
        reason: ExhaustReason,
    },
    /// [`Supervisor::shutdown`] ran.
    Shutdown,
}

/// [`Supervisor::ready`] failure (dispatch maps to HTTP, SPEC §6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorError {
    /// Budget spent / fail-closed (503 + `Retry-After: 5`), EXCEPT config
    /// exits, which are 401 (`muse login` required).
    Exhausted(ExhaustReason),
    /// Bridge is shutting down.
    Shutdown,
    /// A restart storm outlasted [`READY_DEADLINE`] (503 + `Retry-After: 5`).
    Timeout,
}

impl SupervisorError {
    /// Config exits (code 3) surface as 401, not 503 (SPEC §6).
    pub fn is_config_exit(&self) -> bool {
        matches!(
            self,
            SupervisorError::Exhausted(ExhaustReason::NonRestartableExit {
                exit: ExitClass::Config
            })
        )
    }
}

struct SupervisorShared {
    config: HostConfig,
    inner: tokio::sync::RwLock<SupervisorInner>,
}

struct SupervisorInner {
    conn: Arc<MspConnection>,
    child: ChildHandle,
    status: SupervisorStatus,
    restarts_used: u32,
}

/// Keeps one live MSP connection across host deaths (SPEC §4.4).
///
/// In-flight requests on a dead connection fail (v1 has no resume: the
/// client's retry replays from scratch); new requests block in [`ready`](Supervisor::ready)
/// until the relaunch lands or the budget is spent.
pub struct Supervisor {
    shared: Arc<SupervisorShared>,
    monitor: tokio::task::JoinHandle<()>,
}

impl Supervisor {
    /// Launch the initial host and start the monitor. An initial failure is
    /// fatal (no restart budget applies before the first handshake).
    pub async fn launch(config: HostConfig) -> Result<Arc<Self>, LaunchError> {
        let spawned = launch(&config).await?;
        let shared = Arc::new(SupervisorShared {
            config,
            inner: tokio::sync::RwLock::new(SupervisorInner {
                conn: spawned.conn,
                child: spawned.child,
                status: SupervisorStatus::Serving,
                restarts_used: 0,
            }),
        });
        let monitor = tokio::spawn(monitor_loop(shared.clone()));
        Ok(Arc::new(Self { shared, monitor }))
    }

    /// Current serving state.
    pub async fn status(&self) -> SupervisorStatus {
        self.shared.inner.read().await.status.clone()
    }

    /// The launch configuration (bin, serve args, env) — the skills
    /// subprocess and diagnostics need the same binary/posture the host
    /// runs with.
    pub fn config(&self) -> &HostConfig {
        &self.shared.config
    }

    /// Latest connection (may be dead; check [`MspConnection::is_alive`]).
    /// Prefer [`ready`](Supervisor::ready) for request paths.
    pub async fn current(&self) -> Arc<MspConnection> {
        self.shared.inner.read().await.conn.clone()
    }

    /// A live connection, waiting out restarts. Fails when the supervisor
    /// is exhausted, shut down, or a restart storm outlasts the deadline.
    pub async fn ready(&self) -> Result<Arc<MspConnection>, SupervisorError> {
        let deadline = tokio::time::Instant::now() + READY_DEADLINE;
        loop {
            {
                let inner = self.shared.inner.read().await;
                match &inner.status {
                    SupervisorStatus::Serving if inner.conn.is_alive() => {
                        return Ok(inner.conn.clone());
                    }
                    SupervisorStatus::Exhausted { reason } => {
                        return Err(SupervisorError::Exhausted(reason.clone()));
                    }
                    SupervisorStatus::Shutdown => return Err(SupervisorError::Shutdown),
                    // Serving-a-dead-conn (monitor hasn't flipped yet) or
                    // Restarting: wait below.
                    _ => {}
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(SupervisorError::Timeout);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Graceful shutdown: stop the monitor, EOF the host, reap it.
    pub async fn shutdown(&self) -> ExitClass {
        self.monitor.abort();
        let mut inner = self.shared.inner.write().await;
        inner.status = SupervisorStatus::Shutdown;
        let stdin = inner.conn.take_stdin();
        let class = inner.child.shutdown(stdin).await;
        tracing::info!(exit = ?class, "host shut down");
        class
    }
}

/// Monitor task: wait for each connection's death, then restart or give up.
/// Ends on exhaustion (loudly) or supervisor shutdown (aborted).
async fn monitor_loop(shared: Arc<SupervisorShared>) {
    loop {
        let conn = shared.inner.read().await.conn.clone();
        let mut state = conn.watch_state();
        // Wait until THIS connection dies (a replaced connection is always
        // already dead, so a stale iteration falls straight through to the
        // budget check below — each iteration consumes budget or gives up).
        if state
            .wait_for(|s| matches!(s, HostState::Dead { .. }))
            .await
            .is_err()
        {
            return; // state sender gone (supervisor dropped): nothing to watch
        }
        // Reap the child. `None` means stdout EOF'd while the process
        // lingers: kill the wedge and classify the reap.
        let exit = {
            let inner = shared.inner.read().await;
            match inner.child.try_reap().await {
                Some(class) => class,
                None => {
                    tracing::warn!("host stdout EOF'd but the process lingers; killing");
                    inner.child.kill_and_reap().await
                }
            }
        };
        let restartable = conn
            .handshake_info()
            .map(|h| h.restartable())
            .unwrap_or(true); // durability absent ⇒ durable read
        let restarts_used = shared.inner.read().await.restarts_used;

        if !restartable {
            tracing::error!("host died on an ephemeral profile; failing closed (no restart)");
            set_exhausted(&shared, ExhaustReason::EphemeralHost).await;
            return;
        }
        if !exit.restartable() {
            tracing::error!(
                exit = ?exit,
                remedy = exit.describe(),
                "host exited unrecoverably; not restarting"
            );
            set_exhausted(&shared, ExhaustReason::NonRestartableExit { exit }).await;
            return;
        }
        if restarts_used >= MAX_RESTARTS {
            tracing::error!(
                restarts_used,
                last_exit = ?exit,
                "host restart budget spent; failing requests until the bridge restarts"
            );
            set_exhausted(
                &shared,
                ExhaustReason::BudgetSpent {
                    last_exit: Some(exit),
                },
            )
            .await;
            return;
        }
        let delay = RESTART_BACKOFF_MS[restarts_used as usize % RESTART_BACKOFF_MS.len()];
        {
            let mut inner = shared.inner.write().await;
            inner.status = SupervisorStatus::Restarting;
        }
        tracing::warn!(
            exit = ?exit,
            restarts_used,
            delay_ms = delay,
            "host died; relaunching"
        );
        tokio::time::sleep(Duration::from_millis(delay)).await;
        let config = shared.config.clone();
        match launch(&config).await {
            Ok(spawned) => {
                let mut inner = shared.inner.write().await;
                inner.conn = spawned.conn;
                inner.child = spawned.child;
                inner.status = SupervisorStatus::Serving;
                inner.restarts_used += 1;
                tracing::info!(restarts_used = inner.restarts_used, "host relaunched");
            }
            Err(e) => {
                let mut inner = shared.inner.write().await;
                inner.restarts_used += 1;
                tracing::error!(
                    error = %e,
                    restarts_used = inner.restarts_used,
                    "host relaunch failed"
                );
                if inner.restarts_used >= MAX_RESTARTS {
                    inner.status = SupervisorStatus::Exhausted {
                        reason: ExhaustReason::BudgetSpent { last_exit: None },
                    };
                    return;
                }
                // Loop: the dead conn falls through to another attempt.
            }
        }
    }
}

async fn set_exhausted(shared: &Arc<SupervisorShared>, reason: ExhaustReason) {
    shared.inner.write().await.status = SupervisorStatus::Exhausted { reason };
}

/// Split `MUSE_SERVE_ARGS` on whitespace (empty/missing → no args).
fn split_serve_args(raw: &str) -> Vec<String> {
    raw.split_whitespace().map(str::to_string).collect()
}

/// Parse `MUSE_COMMAND_TIMEOUT_MS`: positive integers only; anything else
/// (missing, garbage, zero, negative) falls back to the method table.
fn parse_timeout_override(raw: Option<&str>) -> Option<u64> {
    raw.and_then(|r| r.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_deliberately_handled_server_requests_are_forwarded() {
        for method in ["approval/request", "userInput/request"] {
            assert!(known_server_request(method), "{method} must be known");
        }
        for method in ["future/request", "approval/decide", "userInput/answer", ""] {
            assert!(
                !known_server_request(method),
                "{method:?} must get methodNotFound, not a synthetic result"
            );
        }
    }

    #[test]
    fn command_timeouts_are_method_aware() {
        let t = |m: &str| command_timeout(None, m);
        assert_eq!(t("initialize"), Duration::from_millis(30_000));
        assert_eq!(t("session/start"), Duration::from_millis(180_000));
        assert_eq!(t("session/resume"), Duration::from_millis(180_000));
        assert_eq!(t("session/fork"), Duration::from_millis(180_000));
        assert_eq!(t("session/read"), Duration::from_millis(180_000));
        assert_eq!(t("session/compact"), Duration::from_millis(180_000));
        assert_eq!(t("view/page"), Duration::from_millis(180_000));
        assert_eq!(t("item/readOutput"), Duration::from_millis(180_000));
        assert_eq!(t("model/list"), Duration::from_millis(30_000));
        assert_eq!(t("session/list"), Duration::from_millis(30_000));
        assert_eq!(t("view/subscribe"), Duration::from_millis(30_000));
        assert_eq!(t("view/unsubscribe"), Duration::from_millis(30_000));
        assert_eq!(t("approval/decide"), Duration::from_millis(30_000));
        assert_eq!(t("userInput/cancel"), Duration::from_millis(30_000));
        assert_eq!(t("turn/start"), Duration::from_millis(60_000));
        assert_eq!(t("turn/steer"), Duration::from_millis(60_000));
        assert_eq!(t("turn/interrupt"), Duration::from_millis(60_000));
        assert_eq!(t("turn/cancel"), Duration::from_millis(60_000));
        assert_eq!(t("turn/unqueue"), Duration::from_millis(60_000));
        assert_eq!(t("session/setModel"), Duration::from_millis(60_000));
        assert_eq!(t("session/rename"), Duration::from_millis(60_000));
        assert_eq!(
            t("session/setReasoningEffort"),
            Duration::from_millis(60_000)
        );
        assert_eq!(t("session/userShell"), Duration::from_millis(60_000));
        assert_eq!(t("future/method"), Duration::from_millis(60_000));
    }

    #[test]
    fn handshake_classification_records_grants_and_gates_schema() {
        let granted = serde_json::json!({
            "serverInfo": {"name": "muse-session-server", "version": "1.2.1"},
            "schema": {"version": 1, "fingerprint": PINNED_FINGERPRINT},
            "sessionDurability": "durable",
            "grantedCapabilities": ["userShell"],
        });
        let info = classify_handshake(&granted).expect("valid handshake");
        assert_eq!(info.compat, CompatStatus::Tested);
        assert!(info.supports_user_shell());
        assert!(info.restartable());
        // Absent grants read as none; the gate stays fail-closed.
        let bare = serde_json::json!({
            "serverInfo": {"name": "muse-session-server", "version": "1.2.1"},
            "schema": {"version": 1, "fingerprint": "sha256:other"},
        });
        let info = classify_handshake(&bare).expect("valid handshake");
        assert_eq!(info.compat, CompatStatus::FingerprintMismatch);
        assert!(!info.supports_user_shell());
        assert!(info.restartable(), "absent durability means durable");
        // schema.version != 1 is fatal.
        let bad = serde_json::json!({"schema": {"version": 2}});
        assert!(matches!(
            classify_handshake(&bad),
            Err(LaunchError::IncompatibleSchema { .. })
        ));
    }

    #[test]
    fn timeout_override_parses_positive_ints_only() {
        assert_eq!(parse_timeout_override(Some("250")), Some(250));
        assert_eq!(parse_timeout_override(Some(" 1000 ")), Some(1000));
        for raw in [
            None,
            Some(""),
            Some("bogus"),
            Some("0"),
            Some("-5"),
            Some("1.5"),
        ] {
            assert_eq!(parse_timeout_override(raw), None, "{raw:?}");
        }
        assert_eq!(
            command_timeout(Some(250), "session/resume"),
            Duration::from_millis(250)
        );
        // A `Some(0)` that slips past parsing still falls back to the table.
        assert_eq!(
            command_timeout(Some(0), "initialize"),
            Duration::from_millis(30_000)
        );
    }

    #[test]
    fn serve_args_split_on_whitespace() {
        assert!(split_serve_args("").is_empty());
        assert_eq!(split_serve_args("  --a  --b=c "), vec!["--a", "--b=c"]);
    }

    #[test]
    fn only_backpressure_kinds_retry_and_explicit_opt_out_wins() {
        let err = |kind: &str, retryable: Option<bool>| {
            let mut data = serde_json::json!({"kind": kind});
            if let Some(r) = retryable {
                data["retryable"] = r.into();
            }
            ErrorObject {
                code: -32001,
                message: "x".into(),
                data: Some(data),
            }
        };
        assert!(is_backpressure(&err("overloaded", None)));
        assert!(is_backpressure(&err("backpressured", None)));
        assert!(is_backpressure(&err("overloaded", Some(true))));
        assert!(!is_backpressure(&err("overloaded", Some(false))));
        for kind in [
            "inputTooLarge",
            "internal",
            "invalidParams",
            "commandRejected",
            "notFound",
        ] {
            assert!(!is_backpressure(&err(kind, None)), "{kind}");
            assert!(!is_backpressure(&err(kind, Some(true))), "{kind}");
        }
        // No data at all: not retryable.
        assert!(!is_backpressure(&ErrorObject {
            code: -32603,
            message: "x".into(),
            data: None,
        }));
    }

    #[test]
    fn backoff_caps_double_and_jitter_stays_inside() {
        assert_eq!(backoff_cap_ms(0, 200), 200);
        assert_eq!(backoff_cap_ms(1, 200), 400);
        assert_eq!(backoff_cap_ms(2, 200), 800);
        assert_eq!(backoff_cap_ms(100, 200), 30_000);
        for _ in 0..500 {
            let d = backoff_delay_ms(1, 200);
            assert!(d <= 400, "{d}");
        }
        assert_eq!(backoff_delay_ms(0, 0), 0);
    }

    #[test]
    fn command_id_registry_allows_identical_reuse_and_refuses_conflicts() {
        let mut registry = HashMap::new();
        let params = serde_json::json!({"commandId": "c1", "n": 1});
        assert!(remember_command_id(&mut registry, "c1", "turn/start", &params).is_ok());
        // Identical retry: fine.
        assert!(remember_command_id(&mut registry, "c1", "turn/start", &params).is_ok());
        // Same id, different params: client bug.
        let other = serde_json::json!({"commandId": "c1", "n": 2});
        assert!(remember_command_id(&mut registry, "c1", "turn/start", &other).is_err());
        // Same id, different method: also a conflict.
        assert!(remember_command_id(&mut registry, "c1", "turn/cancel", &params).is_err());
    }

    #[test]
    fn command_id_registry_is_bounded() {
        let mut registry = HashMap::new();
        for i in 0..COMMAND_ID_REGISTRY_CAP + 100 {
            let params = serde_json::json!({"n": i});
            remember_command_id(&mut registry, &format!("id-{i}"), "turn/start", &params)
                .expect("distinct ids never conflict");
        }
        assert_eq!(registry.len(), COMMAND_ID_REGISTRY_CAP);
    }

    #[test]
    fn handshake_classification_fatal_only_on_schema_version() {
        let base = serde_json::json!({
            "schema": {"version": 1, "fingerprint": PINNED_FINGERPRINT},
            "serverInfo": {"name": "muse", "version": "1.2.1"},
            "sessionDurability": "durable",
        });
        let info = classify_handshake(&base).expect("valid handshake");
        assert_eq!(info.compat, CompatStatus::Tested);
        assert_eq!(info.server_name, "muse");
        assert!(info.restartable());

        // Fingerprint drift warns and continues.
        let mut drifted = base.clone();
        drifted["schema"]["fingerprint"] = "sha256:deadbeef".into();
        let info = classify_handshake(&drifted).expect("drift must not fail");
        assert_eq!(info.compat, CompatStatus::FingerprintMismatch);

        // Absent durability reads as durable.
        let mut absent = base.clone();
        absent.as_object_mut().unwrap().remove("sessionDurability");
        assert!(classify_handshake(&absent).expect("ok").restartable());

        // Unknown durability values fail closed.
        for durability in ["ephemeral", "future-profile"] {
            let mut m = base.clone();
            m["sessionDurability"] = durability.into();
            assert!(
                !classify_handshake(&m).expect("ok").restartable(),
                "{durability}"
            );
        }

        // Schema version is the hard gate (wrong or absent = fatal).
        for version in [serde_json::json!(2), serde_json::json!("1"), Value::Null] {
            let mut m = base.clone();
            m["schema"]["version"] = version;
            assert!(
                matches!(
                    classify_handshake(&m),
                    Err(LaunchError::IncompatibleSchema { .. })
                ),
                "{}",
                m["schema"]["version"]
            );
        }
        let mut missing = base.clone();
        missing.as_object_mut().unwrap().remove("schema");
        assert!(matches!(
            classify_handshake(&missing),
            Err(LaunchError::IncompatibleSchema { .. })
        ));
    }

    #[test]
    fn timeout_diagnostics_carry_the_session_when_present() {
        assert_eq!(
            session_suffix(&serde_json::json!({"commandId": "c", "sessionId": "s-1"})),
            " session=s-1"
        );
        assert_eq!(session_suffix(&serde_json::json!({"commandId": "c"})), "");
    }

    #[test]
    fn supervisor_errors_flag_config_exits_for_401() {
        let config = SupervisorError::Exhausted(ExhaustReason::NonRestartableExit {
            exit: ExitClass::Config,
        });
        assert!(config.is_config_exit());
        for other in [
            SupervisorError::Exhausted(ExhaustReason::NonRestartableExit {
                exit: ExitClass::Usage,
            }),
            SupervisorError::Exhausted(ExhaustReason::EphemeralHost),
            SupervisorError::Exhausted(ExhaustReason::BudgetSpent { last_exit: None }),
            SupervisorError::Shutdown,
            SupervisorError::Timeout,
        ] {
            assert!(!other.is_config_exit(), "{other:?}");
        }
    }
}
