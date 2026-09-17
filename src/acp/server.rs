//! ACP stdio server: JSON-RPC framing, handshake, and method dispatch.
//!
//! FORK_PLAN P4. One ACP client per process over stdin/stdout (NDJSON,
//! CRLF-tolerant, 10 MiB cap — the same [`proto`] framing the MSP side
//! uses). The read loop never blocks on a request: every id-bearing frame
//! is handled on a spawned task, so `session/cancel` lands while a prompt
//! is streaming. Replies and `session/update` notifications multiplex over
//! one outbound channel drained by a single writer task, so frames never
//! interleave.
//!
//! Implemented methods: `initialize`, `session/new`, `session/list`,
//! `session/resume`, `session/load`, `session/fork`, `session/prompt`
//! (incl. `/compact`), `_session/steering` (v2), `session/set_config_option`,
//! `session/set_mode`, `session/set_model`, `session/cancel`
//! (notification), `session/close`, `shutdown`/`exit`. Everything else
//! answers `-32601` (requests) or is ignored with a `debug!`
//! (notifications and client replies to server requests) per AGENTS rule 7.
//! Failures map through [`crate::acp::errors`] (SPEC §6, kind-verbatim).
//!
//! [`proto`]: crate::msp::proto

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncWrite};
use tokio::sync::{Mutex, mpsc, oneshot, watch};

use crate::acp::sessions::SessionStore;
use crate::dispatch::Dispatcher;
use crate::msp::proto::{self, FrameError};

/// Agent name advertised in `initialize` results (and the `[[bin]]` name).
pub const AGENT_NAME: &str = "muse-acp-bridge";
/// Highest ACP protocol version spoken (1 and 2, negotiated down).
pub const MAX_PROTOCOL_VERSION: u8 = 2;
/// Outbound channel depth (512 frames of backpressure before senders stall;
/// a stalled sender heals when the client drains — stdout is never severed).
const OUTBOUND_DEPTH: usize = 512;

/// Outbound frames toward the ACP client (replies + notifications), drained
/// in send order by the single writer task.
pub type Outbound = mpsc::Sender<Value>;

/// Server→client request correlation for `session/request_permission` and
/// `elicitation/create`: mints request ids, registers a oneshot waiter per
/// request, and resolves waiters when no-method reply frames arrive.
/// Waiters are user-paced (no timeout — a parked dialog outlives any one
/// turn) and resolve `Err` when the writer is gone.
pub struct ClientRequests {
    outbound: Outbound,
    next_id: AtomicU64,
    pending: std::sync::Mutex<HashMap<String, oneshot::Sender<Value>>>,
}

impl ClientRequests {
    pub fn new(outbound: Outbound) -> Arc<Self> {
        Arc::new(Self {
            outbound,
            next_id: AtomicU64::new(0),
            pending: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Send `<prefix>-<n>` request; the receiver yields the client's reply
    /// frame (result or error both arrive; `Err` means the writer is gone).
    /// A failed send retracts the waiter so the caller resolves `Err`
    /// instead of parking forever.
    pub async fn request(
        &self,
        prefix: &str,
        params: Value,
        method: &str,
    ) -> (Value, oneshot::Receiver<Value>) {
        let id = json!(format!(
            "{prefix}-{}",
            self.next_id.fetch_add(1, Ordering::SeqCst) + 1
        ));
        let key = serde_json::to_string(&id).unwrap_or_default();
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key.clone(), tx);
        let frame = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        if self.outbound.send(frame).await.is_err() {
            self.pending
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&key);
        }
        (id, rx)
    }

    /// Drop a waiter without a reply (session closed mid-dialog): the
    /// receiver resolves `Err` so its completer exits instead of parking.
    pub fn cancel(&self, id: &Value) {
        let key = serde_json::to_string(id).unwrap_or_default();
        self.pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&key);
    }

    /// Route a no-method client frame to a waiting request. Returns true
    /// when a pending request claimed it.
    pub fn resolve(&self, frame: &Value) -> bool {
        let Some(id) = frame.get("id") else {
            return false;
        };
        let key = serde_json::to_string(id).unwrap_or_default();
        match self
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&key)
        {
            Some(tx) => {
                let _ = tx.send(frame.clone());
                true
            }
            None => false,
        }
    }
}

/// Negotiate the ACP protocol version: the client's `protocolVersion`
/// clamped to [`MAX_PROTOCOL_VERSION`] (missing/unparseable ⇒ 1).
pub fn negotiate_version(requested: Option<&Value>) -> u8 {
    let v = requested.and_then(Value::as_u64).unwrap_or(1);
    v.clamp(1, u64::from(MAX_PROTOCOL_VERSION)) as u8
}

/// `initialize` result for the negotiated version. Capabilities advertise
/// only what is implemented (honest minimal caps): text+image prompts with
/// embedded-context inlining, list/resume/load/fork/close, mode/model/effort
/// selectors, and v2 steering. Auth: none (the bridge handles no
/// credentials). Subagents stay unadvertised (not implemented).
pub fn initialize_result(ver: u8) -> Value {
    let version = env!("CARGO_PKG_VERSION");
    if ver == 2 {
        json!({
            "protocolVersion": 2,
            "capabilities": {
                "session": {
                    "prompt": {"image": {}, "embeddedContext": {}},
                    "fork": {},
                },
            },
            "info": {"name": AGENT_NAME, "title": "Muse ACP Bridge", "version": version},
            "authMethods": [],
            "_meta": {"steering": {"supported": true}},
        })
    } else {
        json!({
            "protocolVersion": 1,
            "agentCapabilities": {
                "promptCapabilities": {"text": true, "image": true, "audio": false, "embeddedContext": true},
                "mcpCapabilities": {"http": false, "sse": false},
                "loadSession": true,
                "sessionCapabilities": {"list": {}, "resume": {}, "close": {}, "fork": {}},
            },
            "agentInfo": {"name": AGENT_NAME, "title": "Muse ACP Bridge", "version": version},
        })
    }
}

/// One JSON-RPC 2.0 result frame (the id echoes verbatim).
pub fn result_frame(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

/// One JSON-RPC 2.0 error frame (the id echoes verbatim, `null` included).
pub fn error_frame(id: &Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

/// Serve one ACP client: read frames until EOF, `shutdown`, or I/O failure.
/// Returns when the client disconnects (clean EOF) or the writer fails.
pub async fn serve<R, W>(reader: R, writer: W, dispatcher: Dispatcher)
where
    R: AsyncBufRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_DEPTH);
    let store = Arc::new(SessionStore::new(dispatcher, outbound_tx.clone()));
    // Negotiated version for this client (single-client process; defaults
    // to 1 until `initialize` lands).
    let version = Arc::new(Mutex::new(1u8));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let writer_task = tokio::spawn(drain_writer(writer, outbound_rx));
    read_loop(
        reader,
        store,
        version,
        outbound_tx,
        shutdown_tx,
        shutdown_rx,
    )
    .await;
    writer_task.abort();
}

/// Read loop: one spawned task per id-bearing frame; notifications inline.
async fn read_loop<R>(
    mut reader: R,
    store: Arc<SessionStore>,
    version: Arc<Mutex<u8>>,
    outbound: Outbound,
    shutdown_tx: watch::Sender<bool>,
    mut shutdown_rx: watch::Receiver<bool>,
) where
    R: AsyncBufRead + Unpin,
{
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow_and_update() {
                    tracing::info!("acp shutdown requested");
                    return;
                }
            }
            frame = proto::next_frame(&mut reader) => {
                match frame {
                    Ok(Some(frame)) => {
                        dispatch_frame(frame, &store, &version, &outbound, &shutdown_tx).await;
                    }
                    Ok(None) => {
                        tracing::info!("acp client disconnected (EOF)");
                        return;
                    }
                    Err(FrameError::Io(e)) => {
                        tracing::warn!("acp stdio error: {e}");
                        return;
                    }
                    Err(FrameError::Oversize { .. }) => {
                        // Inbound oversize lines are warn-and-skipped inside
                        // `next_frame`; this arm is the outbound refusal,
                        // unreachable on the read path. Survive regardless.
                        tracing::warn!("acp read path hit an oversize frame");
                    }
                }
            }
        }
    }
}

/// Writer task: serialize outbound frames to stdout, one JSON object + `\n`
/// each. Ends when the channel closes (server shutdown) or stdout fails.
async fn drain_writer<W>(mut writer: W, mut outbound: mpsc::Receiver<Value>)
where
    W: AsyncWrite + Unpin + Send,
{
    while let Some(frame) = outbound.recv().await {
        if let Err(e) = proto::write_frame(&mut writer, &frame).await {
            tracing::warn!("acp stdout write failed, ending session: {e}");
            return;
        }
    }
}

/// Route one inbound frame: requests (id present and non-null) spawn a
/// handler task so the loop stays responsive; notifications run inline.
async fn dispatch_frame(
    frame: Value,
    store: &Arc<SessionStore>,
    version: &Arc<Mutex<u8>>,
    outbound: &Outbound,
    shutdown_tx: &watch::Sender<bool>,
) {
    let method = frame
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_string);
    let id = frame.get("id").cloned().unwrap_or(Value::Null);
    let is_request = !id.is_null();
    let Some(method) = method else {
        // No method: a client reply — maybe to our session/request_permission
        // or elicitation/create. Correlated by id; unclaimed replies and
        // garbage log-and-survive (rule 7), never answered.
        if !store.resolve_client_reply(&frame) {
            tracing::debug!("ignoring ACP frame without a method");
        }
        return;
    };
    if !is_request {
        handle_notification(&method, &frame, store).await;
        return;
    }
    // Requests run on spawned tasks: `session/prompt` admission awaits a
    // `turn/start` ack, and the loop must keep serving `session/cancel`.
    let store = Arc::clone(store);
    let version = Arc::clone(version);
    let outbound = outbound.clone();
    let shutdown_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        handle_request(
            &method,
            &id,
            &frame,
            &store,
            &version,
            &outbound,
            &shutdown_tx,
        )
        .await;
    });
}

/// Handle one id-bearing request; exactly one reply per call, except
/// `session/prompt` (the driver replies on settle) and `shutdown`.
#[allow(clippy::too_many_arguments)]
async fn handle_request(
    method: &str,
    id: &Value,
    frame: &Value,
    store: &Arc<SessionStore>,
    version: &Arc<Mutex<u8>>,
    outbound: &Outbound,
    shutdown_tx: &watch::Sender<bool>,
) {
    let params = frame.get("params").unwrap_or(&Value::Null);
    match method {
        "initialize" => {
            let ver = negotiate_version(params.get("protocolVersion"));
            *version.lock().await = ver;
            // Both ACP versions can advertise the form elicitation
            // extension; it gates the userInput → elicitation bridge.
            let caps = params.get(if ver == 1 {
                "clientCapabilities"
            } else {
                "capabilities"
            });
            let elicit_form = caps
                .and_then(|c| c.get("elicitation"))
                .and_then(|e| e.get("form"))
                .is_some_and(Value::is_object);
            store.set_elicitation_form(elicit_form);
            tracing::info!(
                protocol_version = ver,
                elicitation_form = elicit_form,
                "acp client initialized"
            );
            send(outbound, result_frame(id, initialize_result(ver))).await;
        }
        "session/new" => {
            let ver = *version.lock().await;
            match store.create_session(ver, params).await {
                Ok(result) => send(outbound, result_frame(id, result)).await,
                Err(error) => send(outbound, error_frame(id, error.code, &error.message)).await,
            }
        }
        "session/list" => match store.list(params).await {
            Ok(result) => send(outbound, result_frame(id, result)).await,
            Err(error) => send(outbound, error_frame(id, error.code, &error.message)).await,
        },
        "session/resume" | "session/load" => {
            let ver = *version.lock().await;
            match store.resume_or_load(method, ver, params).await {
                Ok(result) => send(outbound, result_frame(id, result)).await,
                Err(error) => send(outbound, error_frame(id, error.code, &error.message)).await,
            }
        }
        "session/fork" => {
            let ver = *version.lock().await;
            match store.fork(ver, params).await {
                Ok(result) => send(outbound, result_frame(id, result)).await,
                Err(error) => send(outbound, error_frame(id, error.code, &error.message)).await,
            }
        }
        // The driver owns the settle, but admission replies inline (`{}` on
        // v2, errors on both): `prompt` sends its own frames.
        "session/prompt" => store.prompt(id.clone(), params).await,
        // Steering sends its own reply (outcome or typed error).
        "_session/steering" => {
            let ver = *version.lock().await;
            if let Err(error) = store.steering(ver, id.clone(), params).await {
                send(outbound, error_frame(id, error.code, &error.message)).await;
            }
        }
        "session/set_config_option" => match store.set_config_option(params).await {
            Ok(result) => send(outbound, result_frame(id, result)).await,
            Err(error) => send(outbound, error_frame(id, error.code, &error.message)).await,
        },
        "session/set_mode" => match store.set_mode(params).await {
            Ok(result) => send(outbound, result_frame(id, result)).await,
            Err(error) => send(outbound, error_frame(id, error.code, &error.message)).await,
        },
        "session/set_model" => match store.set_model(params).await {
            Ok(result) => send(outbound, result_frame(id, result)).await,
            Err(error) => send(outbound, error_frame(id, error.code, &error.message)).await,
        },
        "session/close" => match store.close(params).await {
            Ok(result) => send(outbound, result_frame(id, result)).await,
            Err(error) => send(outbound, error_frame(id, error.code, &error.message)).await,
        },
        // `session/cancel` is specified as a notification, but answer `{}` when
        // a client sends it as a request rather than hanging the caller.
        "session/cancel" => {
            store.cancel(params).await;
            send(outbound, result_frame(id, json!({}))).await;
        }
        "shutdown" => {
            send(outbound, result_frame(id, json!({}))).await;
            let _ = shutdown_tx.send(true);
        }
        // `exit`: like `shutdown` but without a reply.
        "exit" => {
            let _ = shutdown_tx.send(true);
        }
        "authenticate" | "auth/login" | "auth/logout" | "logout" => {
            tracing::debug!(method, "acp auth method: the bridge handles no credentials");
            send(
                outbound,
                error_frame(
                    id,
                    -32601,
                    &format!("{method} is not supported: no auth surface"),
                ),
            )
            .await;
        }
        _ => {
            tracing::debug!(method, "unknown ACP method");
            send(
                outbound,
                error_frame(id, -32601, &format!("method not found: {method}")),
            )
            .await;
        }
    }
}

/// Handle one client notification (never answered).
async fn handle_notification(method: &str, frame: &Value, store: &Arc<SessionStore>) {
    let params = frame.get("params").unwrap_or(&Value::Null);
    match method {
        "session/cancel" => store.cancel(params).await,
        // `initialized`, `session/started`-style client chatter, and anything
        // future: log and survive (rule 7 — the connection outlives any one
        // frame, and unknown notifications must never wedge the loop).
        _ => tracing::debug!(method, "ignoring ACP notification"),
    }
}

/// Send one outbound frame (dropped only when the writer is gone).
async fn send(outbound: &Outbound, frame: Value) {
    if outbound.send(frame).await.is_err() {
        tracing::debug!("outbound closed; dropping an ACP reply");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_negotiation_clamps_to_spoken() {
        assert_eq!(negotiate_version(None), 1);
        assert_eq!(negotiate_version(Some(&json!(1))), 1);
        assert_eq!(negotiate_version(Some(&json!(2))), 2);
        assert_eq!(negotiate_version(Some(&json!(99))), 2);
        assert_eq!(negotiate_version(Some(&json!(0))), 1);
        assert_eq!(negotiate_version(Some(&json!("2"))), 1);
    }

    #[test]
    fn initialize_results_are_valid_handshakes() {
        let v1 = initialize_result(1);
        assert_eq!(v1["protocolVersion"], json!(1));
        assert_eq!(v1["agentInfo"]["name"], json!(AGENT_NAME));
        assert_eq!(v1["agentCapabilities"]["loadSession"], json!(true));
        let caps = &v1["agentCapabilities"]["sessionCapabilities"];
        for cap in ["list", "resume", "close", "fork"] {
            assert!(caps[cap].is_object(), "v1 advertises {cap}");
        }
        assert!(caps.get("subagents").is_none(), "no unimplemented caps");
        let v2 = initialize_result(2);
        assert_eq!(v2["protocolVersion"], json!(2));
        assert_eq!(v2["info"]["name"], json!(AGENT_NAME));
        assert_eq!(v2["authMethods"], json!([]));
        assert!(v2["capabilities"]["session"]["fork"].is_object());
        assert_eq!(
            v2["_meta"]["steering"]["supported"],
            json!(true),
            "v2 advertises steering"
        );
    }

    #[test]
    fn reply_frames_echo_the_id_verbatim() {
        let id = json!("req-7");
        let result = result_frame(&id, json!({"stopReason": "end_turn"}));
        assert_eq!(result["jsonrpc"], json!("2.0"));
        assert_eq!(result["id"], id);
        assert_eq!(result["result"]["stopReason"], json!("end_turn"));
        let error = error_frame(&json!(3), -32601, "method not found: nope");
        assert_eq!(error["id"], json!(3));
        assert_eq!(error["error"]["code"], json!(-32601));
    }
}
