//! ACP server tests against `fake_serve.py`.
//!
//! No login needed. Each test spawns the fake with per-test env knobs and
//! drives the real [`serve`] loop over in-memory duplex pipes: initialize →
//! session/new → session/prompt → chunks + `stopReason`, plus the error,
//! approval, retry, and tolerance arms.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use muse_bridge::acp::server::serve;
use muse_bridge::acp::sessions::{mode_from_msp, resolve_mode};
use muse_bridge::dispatch::Dispatcher;
use muse_bridge::msp::host::{HostConfig, Supervisor};
use muse_bridge::msp::proto::{next_frame, write_frame};
use serde_json::{Value, json};
use tokio::io::{AsyncWriteExt, BufReader};

fn fake_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake_serve.py")
}

fn test_config(env: HashMap<&str, String>) -> HostConfig {
    HostConfig {
        bin: "python3".to_string(),
        subcommand: None,
        serve_args: vec![fake_path().to_string_lossy().into_owned()],
        cwd: std::env::temp_dir(),
        extra_env: env.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        timeout_override_ms: None,
        retry_base_delay_ms: 10,
    }
}

/// `fake_muse.sh` as the bin: `skills list --json` answers a canned
/// registry while serve execs `fake_serve.py` — exercises the dynamic
/// skill list end to end.
fn muse_config(env: HashMap<&str, String>) -> HostConfig {
    HostConfig {
        bin: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/fake_muse.sh")
            .to_string_lossy()
            .into_owned(),
        subcommand: None,
        serve_args: vec![],
        cwd: std::env::temp_dir(),
        extra_env: env.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        timeout_override_ms: None,
        retry_base_delay_ms: 10,
    }
}

/// One connected ACP test client: frames in, frames out, server on a task.
struct AcpClient {
    /// Client→server writer (drop to send EOF).
    tx: tokio::io::DuplexStream,
    /// Server→client reader.
    rx: BufReader<tokio::io::DuplexStream>,
    server: tokio::task::JoinHandle<()>,
    supervisor: Arc<Supervisor>,
}

impl AcpClient {
    async fn connect(env: HashMap<&str, String>) -> Self {
        Self::connect_with(test_config(env)).await
    }

    async fn connect_muse(env: HashMap<&str, String>) -> Self {
        Self::connect_with(muse_config(env)).await
    }

    async fn connect_with(config: HostConfig) -> Self {
        let supervisor = Supervisor::launch(config)
            .await
            .expect("fake host must launch");
        let dispatcher =
            Dispatcher::new(supervisor.clone(), None, "denyUnmatched").expect("dispatcher setup");
        let (client_tx, server_rx) = tokio::io::duplex(256 * 1024);
        let (server_tx, client_rx) = tokio::io::duplex(256 * 1024);
        let server =
            tokio::spawn(
                async move { serve(BufReader::new(server_rx), server_tx, dispatcher).await },
            );
        Self {
            tx: client_tx,
            rx: BufReader::new(client_rx),
            server,
            supervisor,
        }
    }

    async fn send(&mut self, frame: &Value) {
        write_frame(&mut self.tx, frame)
            .await
            .expect("client write");
    }

    async fn recv(&mut self) -> Value {
        tokio::time::timeout(Duration::from_secs(10), next_frame(&mut self.rx))
            .await
            .expect("client read timed out")
            .expect("client read")
            .expect("server closed the stream")
    }

    /// Read frames until `want` returns true, returning all frames seen
    /// (notifications interleave with replies; order is not contractual).
    async fn recv_until(&mut self, want: impl Fn(&Value) -> bool) -> Vec<Value> {
        let mut seen = Vec::new();
        loop {
            let frame = self.recv().await;
            let done = want(&frame);
            seen.push(frame);
            if done {
                return seen;
            }
        }
    }

    /// Like `recv_until`, but declines every `session/request_permission`
    /// it sees (fail-closed path exercises the bridge's deny fallback).
    async fn recv_until_answering(&mut self, want: impl Fn(&Value) -> bool) -> Vec<Value> {
        let mut seen = Vec::new();
        loop {
            let frame = self.recv().await;
            if frame["method"].as_str() == Some("session/request_permission") {
                self.send(&json!({"jsonrpc": "2.0", "id": frame["id"].clone(),
                    "result": {"outcome": {"outcome": "cancelled"}}}))
                    .await;
                seen.push(frame);
                continue;
            }
            let done = want(&frame);
            seen.push(frame);
            if done {
                return seen;
            }
        }
    }

    async fn shutdown(self) {
        drop(self.tx);
        tokio::time::timeout(Duration::from_secs(10), self.server)
            .await
            .expect("server shutdown timed out")
            .expect("server task");
        self.supervisor.shutdown().await;
    }

    /// Read one frame, returning `None` on a quiet timeout (for settle-once
    /// assertions: nothing else may arrive after the terminal).
    async fn try_recv(&mut self, timeout: Duration) -> Option<Value> {
        tokio::time::timeout(timeout, next_frame(&mut self.rx))
            .await
            .ok()?
            .ok()?
    }
}

/// Poll a fake log file until it contains `want` (panics past `timeout`).
async fn poll_log_contains(path: &std::path::Path, want: &str, timeout: Duration) {
    let start = std::time::Instant::now();
    loop {
        let text = std::fs::read_to_string(path).unwrap_or_default();
        if text.contains(want) {
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "fake log never contained {want:?}: {text}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Read `FAKE_INPUT` JSON lines (one `{"method","params"}` object each).
fn read_input_lines(path: &std::path::Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("input line is JSON"))
        .collect()
}

/// AIR `sha256:<hex>` fingerprint over agent text (mirrors the bridge).
fn sha256_fingerprint(text: &str) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, text.as_bytes());
    let mut hex = String::with_capacity(64);
    for byte in digest.as_ref() {
        use std::fmt::Write as _;
        write!(hex, "{byte:02x}").expect("hex into String");
    }
    format!("sha256:{hex}")
}

/// Expected session/start approval mode: `MUSE_APPROVAL_MODE` (either
/// vocabulary) when the process env sets it, else the skeleton default.
fn expected_mode() -> String {
    let raw = std::env::var("MUSE_APPROVAL_MODE").unwrap_or_default();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        "promptUnmatched".to_string()
    } else {
        resolve_mode(trimmed)
            .unwrap_or("promptUnmatched")
            .to_string()
    }
}

fn unique_dir(test: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "muse-bridge-acp-{test}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("tmpdir");
    dir
}

#[tokio::test]
async fn acp_v1_round_trip_streams_chunks_and_stop_reason() {
    let log = unique_dir("v1").join("fake.log");
    let cwd = unique_dir("v1cwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;

    // initialize → v1 handshake naming this agent.
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1, "clientCapabilities": {}}}))
        .await;
    let init = client.recv().await;
    assert_eq!(init["id"], json!(1));
    assert_eq!(init["result"]["protocolVersion"], json!(1));
    assert_eq!(
        init["result"]["agentInfo"]["name"],
        json!("muse-acp-bridge")
    );

    // session/new → ACP id + modes; the fake echoes denyUnmatched,
    // which the bridge adopts (warn-loudly, never fail).
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let new_result = frames.last().expect("session/new reply");
    assert_eq!(new_result["id"], json!(2));
    let acp_sid = new_result["result"]["sessionId"]
        .as_str()
        .expect("acp session id")
        .to_string();
    assert!(acp_sid.starts_with("acp-"), "{acp_sid}");
    assert_eq!(
        new_result["result"]["configOptions"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        new_result["result"]["modes"]["currentModeId"],
        json!("deny"),
        "fake echoes denyUnmatched; the bridge adopts it"
    );
    assert!(
        new_result["result"]["configOptions"][1]["options"]
            .as_array()
            .unwrap()
            .iter()
            .any(|o| o["value"] == json!("fake-a")),
        "model selector carries the fake catalog"
    );
    // The advertisement immediately follows the result (update routing
    // only exists once the response arrives).
    let advertised = drain_advertisement(&mut client).await;
    assert!(
        advertised["params"]["update"]["availableCommands"]
            .as_array()
            .is_some_and(|c| !c.is_empty()),
        "available_commands_update is advertised"
    );
    // The START reached the host with the resolved posture + workspace root.
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        log_text.contains("session/start cmd=")
            && log_text.contains(&format!("mode={}", expected_mode())),
        "session/start carries the resolved posture: {log_text}"
    );
    assert!(
        log_text.contains(&format!("ws={}", cwd.to_string_lossy())),
        "workspace root travels to session/start: {log_text}"
    );

    // session/prompt → chunked text, then `stopReason: end_turn`.
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid,
                "prompt": [{"type": "text", "text": "say hi"}]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    let reply = frames.last().expect("prompt reply");
    assert_eq!(reply["result"], json!({"stopReason": "end_turn"}));
    let chunks: Vec<String> = frames
        .iter()
        .filter(|f| {
            f.get("method") == Some(&json!("session/update"))
                && f["params"]["update"]["sessionUpdate"] == json!("agent_message_chunk")
        })
        .filter_map(|f| {
            f["params"]["update"]["content"]["text"]
                .as_str()
                .map(str::to_string)
        })
        .collect();
    assert!(
        !chunks.is_empty(),
        "turn-happy streams at least one chunk: {frames:?}"
    );
    assert_eq!(chunks.concat(), "Hello, world");
    // v1 agent chunks carry no messageId (user echoes do, on both versions).
    assert!(
        frames
            .iter()
            .filter(|f| f["params"]["update"]["sessionUpdate"] == json!("agent_message_chunk"))
            .all(|f| f["params"]["update"].get("messageId").is_none()),
        "v1 agent chunks omit messageId"
    );
    assert!(
        frames.iter().any(|f| f["params"]["update"]["sessionUpdate"]
            == json!("user_message_chunk")
            && f["params"]["update"]["content"]["text"] == json!("say hi")),
        "v1 echoes user content as chunks: {frames:?}"
    );

    // Unknown methods fail typed; notifications stay silent.
    client
        .send(&json!({"jsonrpc": "2.0", "id": 4, "method": "frobnicate", "params": {}}))
        .await;
    let unknown = client.recv().await;
    assert_eq!(unknown["id"], json!(4));
    assert_eq!(unknown["error"]["code"], json!(-32601));
    client
        .send(&json!({"jsonrpc": "2.0", "method": "session/cancel",
            "params": {"sessionId": acp_sid}}))
        .await;
    // No reply to a notification: the next request's reply proves silence
    // (anything else in between would be an unexpected frame).
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 5, "method": "session/close",
            "params": {"sessionId": acp_sid}}),
        )
        .await;
    let closed = client.recv().await;
    assert_eq!(closed, json!({"jsonrpc": "2.0", "id": 5, "result": {}}));

    client.shutdown().await;
}

#[tokio::test]
async fn acp_v2_chunks_carry_message_ids_and_state_updates() {
    let mut client =
        AcpClient::connect(HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())])).await;
    let cwd = unique_dir("v2cwd");

    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 99, "capabilities": {}}}))
        .await;
    let init = client.recv().await;
    assert_eq!(
        init["result"]["protocolVersion"],
        json!(2),
        "versions above 2 negotiate down"
    );

    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        frames.last().unwrap()["result"].get("modes").is_none(),
        "v2 omits the legacy modes block"
    );
    assert_eq!(
        frames.last().unwrap()["result"]["configOptions"]
            .as_array()
            .unwrap()
            .len(),
        3,
        "v2 carries the selectors"
    );
    drain_advertisement(&mut client).await;

    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["say hi"]}}),
        )
        .await;
    // v2 prompt completion arrives as a state_update (idle/end_turn), not a
    // request reply — wait for that terminal state.
    let frames = client
        .recv_until(|f| {
            f.get("method") == Some(&json!("session/update"))
                && f["params"]["update"]["sessionUpdate"] == json!("state_update")
                && f["params"]["update"]["state"] == json!("idle")
        })
        .await;
    let terminal = frames.last().unwrap();
    assert_eq!(
        terminal["params"]["update"]["stopReason"],
        json!("end_turn")
    );
    assert!(
        frames.iter().any(
            |f| f["params"]["update"]["sessionUpdate"] == json!("state_update")
                && f["params"]["update"]["state"] == json!("running")
        ),
        "v2 announces running before idle"
    );
    let msg_ids: Vec<&str> = frames
        .iter()
        .filter(|f| f["params"]["update"]["sessionUpdate"] == json!("agent_message_chunk"))
        .filter_map(|f| f["params"]["update"]["messageId"].as_str())
        .collect();
    assert!(!msg_ids.is_empty(), "v2 chunks carry messageId");
    assert!(
        msg_ids.iter().all(|id| *id == msg_ids[0]),
        "one message id per prompt turn"
    );

    client.shutdown().await;
}

#[tokio::test]
async fn acp_prompt_rejects_unknown_sessions_and_empty_content() {
    let mut client = AcpClient::connect(HashMap::new()).await;

    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;

    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 2, "method": "session/prompt",
            "params": {"sessionId": "acp-nope", "prompt": ["hi"]}}),
        )
        .await;
    let unknown = client.recv().await;
    assert_eq!(unknown["error"]["code"], json!(-32602));

    // A real session still rejects an empty prompt at admission (no turn).
    let cwd = unique_dir("errcwd");
    client
        .send(&json!({"jsonrpc": "2.0", "id": 3, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    drain_advertisement(&mut client).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 4, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": []}}),
        )
        .await;
    let empty = client.recv().await;
    assert_eq!(empty["id"], json!(4));
    assert_eq!(empty["error"]["code"], json!(-32602));

    // P3 implements resume: re-attaching the live session succeeds.
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 5, "method": "session/resume",
            "params": {"sessionId": acp_sid}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(5))).await;
    let resumed = frames.last().expect("resume reply");
    assert_eq!(resumed["result"]["sessionId"], json!(acp_sid));

    client.shutdown().await;
}

#[tokio::test]
async fn acp_handshake_requests_usershell_capability() {
    let log = unique_dir("caps").join("fake.log");
    let env = HashMap::from([
        ("FAKE_LOG", log.to_string_lossy().into_owned()),
        ("FAKE_GRANT_USERSHELL", "1".to_string()),
    ]);
    let client = AcpClient::connect(env).await;
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        log_text.contains("initialize cmd=- caps=userShell,sessionMcp"),
        "initialize requests userShell + sessionMcp: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_session_new_drops_mcp_servers_without_the_grant() {
    // Ungranted hosts reject config.mcpServers at construction
    // (capabilityRequired, verified live): the bridge drops the servers
    // loudly and the session still succeeds.
    let input = unique_dir("mcpdrop").join("fake.input");
    let cwd = unique_dir("mcpdropcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "serve".to_string())]);
    env.insert("FAKE_INPUT", input.to_string_lossy().into_owned());
    env.insert("FAKE_WITHHOLD_SESSIONMCP", "1".to_string());
    let mut client = AcpClient::connect(env).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy(),
                "mcpServers": [{"name": "x", "command": "y"}]}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    assert!(
        frames.last().unwrap().get("result").is_some(),
        "the session survives the dropped servers: {frames:?}"
    );
    let lines = read_input_lines(&input);
    let started: Vec<&Value> = lines
        .iter()
        .filter(|l| l["method"] == json!("session/start"))
        .collect();
    assert_eq!(started.len(), 1, "one session/start: {lines:?}");
    assert!(
        started[0]["params"].get("config").is_none(),
        "no config without the grant: {}",
        started[0]
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_v2_prompt_acks_empty_and_echoes_user_message() {
    let mut client =
        AcpClient::connect(HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())])).await;
    let cwd = unique_dir("v2echo");
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 2}}))
        .await;
    let init = client.recv().await;
    assert_eq!(
        init["result"]["_meta"]["steering"]["supported"],
        json!(true),
        "v2 advertises steering"
    );
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": [{"type": "text", "text": "say hi"}]}}),
        )
        .await;
    // Accepted: the `{}` reply arrives before the terminal state.
    let frames = client
        .recv_until(|f| {
            f.get("method") == Some(&json!("session/update"))
                && f["params"]["update"]["sessionUpdate"] == json!("state_update")
                && f["params"]["update"]["state"] == json!("idle")
        })
        .await;
    let accept = frames
        .iter()
        .find(|f| f.get("id") == Some(&json!(3)))
        .expect("v2 prompt is acked");
    assert_eq!(accept["result"], json!({}));
    assert!(
        frames.iter().any(
            |f| f["params"]["update"]["sessionUpdate"] == json!("user_message")
                && f["params"]["update"]["content"] == json!([{"type": "text", "text": "say hi"}])
        ),
        "v2 echoes the user message: {frames:?}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_session_list_merges_owned_and_host() {
    let log = unique_dir("list").join("fake.log");
    let cwd = unique_dir("listcwd");
    let mut env = HashMap::new();
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    drain_advertisement(&mut client).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 3, "method": "session/list", "params": {}}))
        .await;
    let listed = client.recv().await;
    let sessions = listed["result"]["sessions"].as_array().unwrap();
    assert_eq!(sessions[0]["sessionId"], json!(acp_sid), "owned first");
    assert!(sessions[0]["_meta"]["mspSessionId"].is_string());
    // Import metadata: owned rows inherit the host row's activity, and
    // host-only rows carry everything Zed's importer shows.
    assert_eq!(
        sessions[0]["updatedAt"],
        json!("2026-09-14T00:00:00Z"),
        "owned rows inherit host activity: {sessions:?}"
    );
    assert_eq!(
        sessions[0]["title"],
        json!("fake session"),
        "owned rows inherit the host name: {sessions:?}"
    );
    let foreign = sessions
        .iter()
        .find(|s| s["sessionId"] == json!("fake-sess-host-only"))
        .expect("host-only sessions surface under their MSP id");
    assert_eq!(foreign["cwd"], json!("/tmp/fake-ws"));
    assert_eq!(foreign["updatedAt"], json!("2026-09-15T00:00:00Z"));
    assert_eq!(foreign["title"], json!("fake session"));
    client.shutdown().await;
}

#[tokio::test]
async fn acp_session_load_replays_history() {
    let mut env = HashMap::from([("FAKE_SCENARIO", "serve".to_string())]);
    env.insert("FAKE_HISTORY", "1".to_string());
    let mut client = AcpClient::connect(env).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;
    // Load by durable host id directly (no adapter session yet).
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/load",
            "params": {"sessionId": "fake-sess-1"}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let loaded = frames.last().expect("load reply");
    assert_eq!(loaded["result"]["sessionId"], json!("fake-sess-1"));
    assert_eq!(
        loaded["result"]["_meta"]["mspSessionId"],
        json!("fake-sess-1")
    );
    let updates: Vec<&str> = frames
        .iter()
        .filter_map(|f| f["params"]["update"]["sessionUpdate"].as_str())
        .collect();
    assert!(
        updates.contains(&"user_message_chunk") && updates.contains(&"agent_message_chunk"),
        "load replays history as chunks: {updates:?}"
    );
    // v1 resume reconnects the same session silently (no replay).
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/resume",
            "params": {"sessionId": "fake-sess-1"}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert!(
        !frames
            .iter()
            .any(|f| f["params"]["update"]["sessionUpdate"] == json!("user_message_chunk")),
        "v1 resume reconnects silently"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_selectors_round_trip_mode_model_effort() {
    let mut client = AcpClient::connect(HashMap::new()).await;
    let cwd = unique_dir("selcwd");
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    drain_advertisement(&mut client).await;
    // Mode selector: the fake echoes the folded mode.
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/set_config_option",
            "params": {"sessionId": acp_sid, "configId": "mode", "value": "auto"}}),
        )
        .await;
    let mode = client.recv().await;
    assert_eq!(
        mode["result"]["configOptions"][0]["currentValue"],
        json!("auto")
    );
    // Model + effort selectors.
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 4, "method": "session/set_config_option",
            "params": {"sessionId": acp_sid, "configId": "model", "value": "fake-b"}}),
        )
        .await;
    let model = client.recv().await;
    assert_eq!(
        model["result"]["configOptions"][1]["currentValue"],
        json!("fake-b")
    );
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 5, "method": "session/set_config_option",
            "params": {"sessionId": acp_sid, "configId": "reasoning_effort", "value": "ultra"}}),
        )
        .await;
    let effort = client.recv().await;
    assert_eq!(
        effort["result"]["configOptions"][2]["currentValue"],
        json!("ultra")
    );
    // Legacy setters agree.
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 6, "method": "session/set_mode",
            "params": {"sessionId": acp_sid, "modeId": "deny"}}),
        )
        .await;
    assert_eq!(client.recv().await["result"], json!({"mode": "deny"}));
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 7, "method": "session/set_model",
            "params": {"sessionId": acp_sid, "model": "fake-a"}}),
        )
        .await;
    assert_eq!(client.recv().await["result"], json!({"model": "fake-a"}));
    // Unknown selectors and values fail typed.
    for (id, method, params) in [
        (
            8,
            "session/set_config_option",
            json!({"sessionId": acp_sid, "configId": "nope", "value": "x"}),
        ),
        (
            9,
            "session/set_config_option",
            json!({"sessionId": acp_sid, "configId": "mode", "value": "turbo"}),
        ),
        (
            10,
            "session/set_config_option",
            json!({"sessionId": acp_sid, "configId": "reasoning_effort", "value": "extreme"}),
        ),
        (11, "session/set_model", json!({"sessionId": acp_sid})),
        (
            12,
            "session/set_model",
            json!({"sessionId": "acp-nope", "model": "x"}),
        ),
    ] {
        client
            .send(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await;
        let err = client.recv().await;
        assert_eq!(err["error"]["code"], json!(-32602), "{method} {params}");
    }
    client.shutdown().await;
}

#[tokio::test]
async fn acp_fork_branches_history_with_cut_points() {
    let log = unique_dir("fork").join("fake.log");
    let mut env = HashMap::from([("FAKE_SCENARIO", "serve".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    env.insert("FAKE_HISTORY", "1".to_string());
    let mut client = AcpClient::connect(env).await;
    let cwd = unique_dir("forkcwd");
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    // Plain fork: all completed turns, new ids on both planes.
    client
        .send(&json!({"jsonrpc": "2.0", "id": 3, "method": "session/fork",
            "params": {"sessionId": acp_sid}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    let forked = frames.last().expect("fork reply");
    let fork_sid = forked["result"]["sessionId"].as_str().unwrap().to_string();
    assert_ne!(fork_sid, acp_sid);
    assert_eq!(
        forked["result"]["_meta"]["mspSessionId"],
        json!("fake-sess-fork")
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(log_text.contains("cut=<all>"), "no cut point: {log_text}");
    // messageId cut point resolves to the item's turn.
    client
        .send(&json!({"jsonrpc": "2.0", "id": 4, "method": "session/fork",
            "params": {"sessionId": acp_sid,
                "_meta": {"jetbrains": {"air": {"forkPoint": {"messageId": "h-agent-1"}}}}}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(4))).await;
    assert!(frames.last().unwrap().get("result").is_some(), "{frames:?}");
    drain_advertisement(&mut client).await;
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        log_text.contains("cut=h-turn-1"),
        "cut point travels: {log_text}"
    );
    // Unknown messages fail closed, never fork the whole history silently.
    client
        .send(&json!({"jsonrpc": "2.0", "id": 5, "method": "session/fork",
            "params": {"sessionId": acp_sid,
                "_meta": {"jetbrains": {"air": {"forkPoint": {"messageId": "h-nope"}}}}}}))
        .await;
    let err = client.recv().await;
    assert_eq!(err["error"]["code"], json!(-32602));
    client.shutdown().await;
}

#[tokio::test]
async fn acp_compact_command_settles_inline_without_a_turn() {
    let log = unique_dir("compact").join("fake.log");
    let mut env = HashMap::from([("FAKE_SCENARIO", "serve".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let cwd = unique_dir("compactcwd");
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": [{"type": "text", "text": "  /compact  "}]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    assert!(
        frames
            .iter()
            .any(|f| f["params"]["update"]["sessionUpdate"] == json!("user_message_chunk")),
        "compact echoes the command"
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        log_text.contains("session/compact"),
        "compact runs: {log_text}"
    );
    assert!(
        !log_text.contains("turn/start"),
        "no turn starts: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_steering_starts_injects_or_defers() {
    let mut client =
        AcpClient::connect(HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())])).await;
    let cwd = unique_dir("steercwd");
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    drain_advertisement(&mut client).await;
    // Steering is v2-only.
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "_session/steering",
            "params": {"sessionId": acp_sid, "prompt": ["nudge"]}}),
        )
        .await;
    let err = client.recv().await;
    assert_eq!(err["error"]["code"], json!(-32601));
    client.shutdown().await;

    // v2 idle: steering starts a turn (or defers when promptRequired).
    let mut client =
        AcpClient::connect(HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())])).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 2}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    drain_advertisement(&mut client).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "_session/steering",
            "params": {"sessionId": acp_sid, "prompt": ["nudge"],
                "_meta": {"steering": {"idleBehavior": "promptRequired"}}}}),
        )
        .await;
    let deferred = client.recv().await;
    assert_eq!(
        deferred["result"],
        json!({"outcome": "promptRequired", "reason": "noRunningTurn"})
    );
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 4, "method": "_session/steering",
            "params": {"sessionId": acp_sid, "prompt": ["nudge"]}}),
        )
        .await;
    let frames = client
        .recv_until(|f| {
            f.get("method") == Some(&json!("session/update"))
                && f["params"]["update"]["sessionUpdate"] == json!("state_update")
                && f["params"]["update"]["state"] == json!("idle")
        })
        .await;
    let steer = frames
        .iter()
        .find(|f| f.get("id") == Some(&json!(4)))
        .expect("steering reply");
    assert_eq!(steer["result"], json!({"outcome": "startedNewTurn"}));
    assert_eq!(
        frames.last().unwrap()["params"]["update"]["stopReason"],
        json!("end_turn"),
        "the steered turn runs to its terminal"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_steering_injects_into_a_running_turn() {
    let mut client = AcpClient::connect(HashMap::from([(
        "FAKE_SCENARIO",
        "turn-tool-slow".to_string(),
    )]))
    .await;
    let cwd = unique_dir("steerbusycwd");
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 2}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    // Start a slow turn; the `{}` ack proves admission (active turn set).
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["slow work"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(frames.last().unwrap()["result"], json!({}));
    // Steering injects into the running turn.
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 4, "method": "_session/steering",
            "params": {"sessionId": acp_sid, "prompt": ["actually, do this"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(4))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"outcome": "injected"})
    );
    // Cancelling settles the turn (and only then the session idles).
    client
        .send(&json!({"jsonrpc": "2.0", "method": "session/cancel",
            "params": {"sessionId": acp_sid}}))
        .await;
    let frames = client
        .recv_until(|f| {
            f.get("method") == Some(&json!("session/update"))
                && f["params"]["update"]["sessionUpdate"] == json!("state_update")
                && f["params"]["update"]["state"] == json!("idle")
        })
        .await;
    assert_eq!(
        frames.last().unwrap()["params"]["update"]["stopReason"],
        json!("cancelled")
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_gap_refill_streams_through_the_turn() {
    let mut client =
        AcpClient::connect(HashMap::from([("FAKE_SCENARIO", "turn-gap".to_string())])).await;
    let cwd = unique_dir("gapcwd");
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["gappy"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    let chunks: String = frames
        .iter()
        .filter(|f| {
            f.get("method") == Some(&json!("session/update"))
                && f["params"]["update"]["sessionUpdate"] == json!("agent_message_chunk")
        })
        .filter_map(|f| {
            f["params"]["update"]["content"]["text"]
                .as_str()
                .map(str::to_string)
        })
        .collect();
    assert_eq!(
        chunks, "Hello, world",
        "gap refill converges without resend"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_concurrent_prompts_each_settle() {
    let mut client =
        AcpClient::connect(HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())])).await;
    let cwd = unique_dir("multicwd");
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    // Two prompts back-to-back: the host queues; each completes its own reply.
    for id in [3, 4] {
        client
            .send(
                &json!({"jsonrpc": "2.0", "id": id, "method": "session/prompt",
                "params": {"sessionId": acp_sid, "prompt": ["hi"]}}),
            )
            .await;
    }
    let mut settled = 0;
    for _ in 0..50 {
        let frame = client.recv().await;
        if frame.get("id") == Some(&json!(3)) || frame.get("id") == Some(&json!(4)) {
            assert_eq!(
                frame["result"],
                json!({"stopReason": "end_turn"}),
                "{frame}"
            );
            settled += 1;
            if settled == 2 {
                break;
            }
        }
    }
    assert_eq!(settled, 2, "both prompts settle");
    client.shutdown().await;
}

/// Consume the `available_commands_update` trailing a session/new (or
/// resume/load/fork) result, asserting it arrives immediately after.
/// Returns the frame for content assertions.
async fn drain_advertisement(client: &mut AcpClient) -> Value {
    let update = client.recv().await;
    assert_eq!(
        update["params"]["update"]["sessionUpdate"],
        json!("available_commands_update"),
        "advertisement trails the result: {update}"
    );
    update
}

/// Initialize (v2) + open a session; returns the ACP session id.
async fn v2_session(client: &mut AcpClient, cwd: &std::path::Path) -> String {
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 2}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    drain_advertisement(client).await;
    sid
}

/// Initialize (v1) + open a session; returns the ACP session id.
async fn v1_session(client: &mut AcpClient, cwd: &std::path::Path) -> String {
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    drain_advertisement(client).await;
    sid
}

#[tokio::test]
async fn acp_approval_declined_dialog_denies_first_non_approving_choice() {
    let log = unique_dir("p4appr").join("fake.log");
    let cwd = unique_dir("p4apprcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-approval".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;

    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["do the thing"]}}),
        )
        .await;
    let mut frames = Vec::new();
    let mut perm_reqs = 0;
    loop {
        let frame = client.recv().await;
        if frame["method"].as_str() == Some("session/request_permission") {
            perm_reqs += 1;
            let options = frame["params"]["options"].as_array().expect("options");
            assert!(
                options.iter().any(|o| o["optionId"] == "c-deny"),
                "the deny choice is an option: {frame:?}"
            );
            // Dismiss the dialog: fail closed to the first non-approving
            // choice rather than park the turn.
            client
                .send(&json!({"jsonrpc": "2.0", "id": frame["id"].clone(),
                    "result": {"outcome": {"outcome": "cancelled"}}}))
                .await;
            continue;
        }
        let done = frame.get("id") == Some(&json!(3));
        frames.push(frame);
        if done {
            break;
        }
    }
    assert_eq!(perm_reqs, 1, "one permission dialog for the request");
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"}),
        "the turn continues after the auto-deny: {frames:?}"
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        log_text.contains("approval/decide")
            && log_text.contains("approval=ap-1")
            && log_text.contains("choice=c-deny"),
        "first non-approving choice decided with the durable id: {log_text}"
    );
    assert!(
        log_text.contains("<response> id=9100 result"),
        "the server request gets its {{}} presentation receipt: {log_text}"
    );
    assert!(
        !log_text.contains("choice=c-allow"),
        "never synthesizes approval: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_approval_with_no_deny_choice_cancels_the_turn() {
    let log = unique_dir("p4allappr").join("fake.log");
    let cwd = unique_dir("p4allapprcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-approval-all-approve".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;

    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["do the thing"]}}),
        )
        .await;
    let frames = client
        .recv_until_answering(|f| f.get("id") == Some(&json!(3)))
        .await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "cancelled"}),
        "no deny choice cancels rather than approves: {frames:?}"
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(log_text.contains("turn/cancel"), "{log_text}");
    assert!(
        !log_text.contains("approval/decide"),
        "an all-approve menu is never decided: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_user_input_auto_cancels_fail_closed() {
    let log = unique_dir("p4ui").join("fake.log");
    let cwd = unique_dir("p4uicwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-userinput".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;

    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["ask me later"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"}),
        "the turn continues after the auto-cancel: {frames:?}"
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        log_text.contains("userInput/cancel")
            && log_text.contains("ui=ui-1")
            && log_text.contains("reason=acp-fail-closed"),
        "stable fail-closed cancel reason with the durable id: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_turn_failure_maps_kind_not_message() {
    let cwd = unique_dir("p4failcwd");
    let mut client = AcpClient::connect(HashMap::from([(
        "FAKE_SCENARIO",
        "turn-failed".to_string(),
    )]))
    .await;
    let acp_sid = v1_session(&mut client, &cwd).await;

    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["hi"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    let reply = frames.last().expect("prompt reply");
    assert_eq!(reply["error"]["code"], json!(-32603));
    let message = reply["error"]["message"].as_str().unwrap_or("");
    assert!(
        message.contains("authRequired") && message.contains("muse login required"),
        "SPEC §6 auth row, kind verbatim: {message}"
    );
    assert!(
        !message.contains("fake login required"),
        "401-class bodies stay stable (no host text): {message}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_backpressure_retries_the_same_command_id() {
    let log = unique_dir("p4retry").join("fake.log");
    let cwd = unique_dir("p4retrycwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    env.insert("FAKE_OVERLOADED", "turn/start:2".to_string());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;

    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["hi"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"}),
        "backpressure retries still complete the turn"
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    let starts: Vec<&str> = log_text
        .lines()
        .filter(|l| l.starts_with("turn/start cmd="))
        .collect();
    assert_eq!(starts.len(), 3, "two retries then success: {log_text}");
    let ids: Vec<&str> = starts
        .iter()
        .filter_map(|l| l.split_whitespace().find(|t| t.starts_with("cmd=")))
        .collect();
    assert!(
        ids.iter().all(|id| *id == ids[0]),
        "retries reuse the commandId: {ids:?}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_tolerates_unknown_notifications_kinds_and_garbage_lines() {
    let cwd = unique_dir("p4noisecwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())]);
    env.insert("FAKE_NOISE", "1".to_string());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;

    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["hi"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    let chunks: String = frames
        .iter()
        .filter(|f| {
            f.get("method") == Some(&json!("session/update"))
                && f["params"]["update"]["sessionUpdate"] == json!("agent_message_chunk")
        })
        .filter_map(|f| {
            f["params"]["update"]["content"]["text"]
                .as_str()
                .map(str::to_string)
        })
        .collect();
    assert_eq!(chunks, "Hello, world", "noise never corrupts the turn");
    client.shutdown().await;
}

#[tokio::test]
async fn acp_shutdown_ends_the_loop() {
    let mut client = AcpClient::connect(HashMap::new()).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "shutdown"}))
        .await;
    let reply = client.recv().await;
    assert_eq!(reply, json!({"jsonrpc": "2.0", "id": 1, "result": {}}));
    // The server task ends on its own after shutdown (no EOF needed).
    tokio::time::timeout(Duration::from_secs(10), &mut client.server)
        .await
        .expect("shutdown must end the loop")
        .expect("server task");
    client.tx.shutdown().await.ok();
    client.supervisor.shutdown().await;
}

// ---------------------------------------------------------------------------
// P5 ports: settle rules, cancel/close, admission, selectors, fork points,
// host death, and honest capability surface.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn acp_cancel_mid_tool_sends_explicit_turn_cancel() {
    let log = unique_dir("canceltool").join("fake.log");
    let cwd = unique_dir("canceltoolcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-tool-slow".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;

    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["slow work"]}}),
        )
        .await;
    // The turn is admitted once `turn/start` lands (no client-visible
    // frames precede the cancel: tool legs render as status lines).
    poll_log_contains(&log, "turn/start cmd=", Duration::from_secs(10)).await;
    client
        .send(&json!({"jsonrpc": "2.0", "method": "session/cancel",
            "params": {"sessionId": acp_sid}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "cancelled"})
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    let start = log_text
        .lines()
        .find(|l| l.starts_with("turn/start cmd="))
        .expect("turn/start logged");
    let cmd = start
        .split_whitespace()
        .find(|t| t.starts_with("cmd="))
        .expect("commandId logged");
    let turn = cmd.trim_start_matches("cmd=");
    assert!(
        log_text.contains("turn/cancel cmd=") && log_text.contains(&format!("turn={turn}")),
        "cancel carries the EXPLICIT turn id: {log_text}"
    );
    assert!(
        !log_text.contains("turn/interrupt") && !log_text.contains("turn/unqueue"),
        "plain-lane cancel only: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_close_cancels_in_flight_turn() {
    let log = unique_dir("closecancel").join("fake.log");
    let cwd = unique_dir("closecancelcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-tool-slow".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;

    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["slow work"]}}),
        )
        .await;
    poll_log_contains(&log, "turn/start cmd=", Duration::from_secs(10)).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 4, "method": "session/close",
            "params": {"sessionId": acp_sid}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(4))).await;
    assert_eq!(frames.last().unwrap()["result"], json!({}));
    let prompt_reply = frames
        .iter()
        .find(|f| f.get("id") == Some(&json!(3)))
        .expect("in-flight prompt resolves on close");
    assert_eq!(
        prompt_reply["result"],
        json!({"stopReason": "cancelled"}),
        "{frames:?}"
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(log_text.contains("turn/cancel"), "{log_text}");
    client.shutdown().await;
}

#[tokio::test]
async fn acp_unqueued_turn_settles_prompt_cancelled() {
    let cwd = unique_dir("unqueuedcwd");
    let mut client = AcpClient::connect(HashMap::from([(
        "FAKE_SCENARIO",
        "turn-unqueued".to_string(),
    )]))
    .await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["reclaim me"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "cancelled"})
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_retracted_turn_settles_prompt_cancelled() {
    let cwd = unique_dir("retractedcwd");
    let mut client = AcpClient::connect(HashMap::from([(
        "FAKE_SCENARIO",
        "turn-retracted".to_string(),
    )]))
    .await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["retract me"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "cancelled"})
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_retract_then_completed_settles_exactly_once() {
    let cwd = unique_dir("retractoncecwd");
    let mut client = AcpClient::connect(HashMap::from([(
        "FAKE_SCENARIO",
        "turn-retract-then-completed".to_string(),
    )]))
    .await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["settle once"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "cancelled"}),
        "the retract wins; the late terminal is dropped"
    );
    assert!(
        client.try_recv(Duration::from_millis(500)).await.is_none(),
        "no second reply after the settle"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_retry_scheduled_never_settles_then_completes() {
    let cwd = unique_dir("retrythencwd");
    let mut client = AcpClient::connect(HashMap::from([(
        "FAKE_SCENARIO",
        "turn-retry-then-completed".to_string(),
    )]))
    .await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["flaky turn"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"}),
        "the later completion settles (the retry is non-terminal)"
    );
    let chunks: String = frames
        .iter()
        .filter(|f| {
            f.get("method") == Some(&json!("session/update"))
                && f["params"]["update"]["sessionUpdate"] == json!("agent_message_chunk")
        })
        .filter_map(|f| {
            f["params"]["update"]["content"]["text"]
                .as_str()
                .map(str::to_string)
        })
        .collect();
    assert_eq!(chunks, "recovered");
    client.shutdown().await;
}

#[tokio::test]
async fn acp_queued_turns_settle_together() {
    let cwd = unique_dir("queuedcwd");
    let mut client = AcpClient::connect(HashMap::from([(
        "FAKE_SCENARIO",
        "turn-queued".to_string(),
    )]))
    .await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    // The first turn holds; the second `turn/start` completes both.
    for id in [3, 4] {
        client
            .send(
                &json!({"jsonrpc": "2.0", "id": id, "method": "session/prompt",
                "params": {"sessionId": acp_sid, "prompt": ["hi"]}}),
            )
            .await;
    }
    let mut settled = 0;
    for _ in 0..50 {
        let frame = client.recv().await;
        if frame.get("id") == Some(&json!(3)) || frame.get("id") == Some(&json!(4)) {
            assert_eq!(
                frame["result"],
                json!({"stopReason": "end_turn"}),
                "{frame}"
            );
            settled += 1;
            if settled == 2 {
                break;
            }
        }
    }
    assert_eq!(settled, 2, "both queued prompts settle");
    client.shutdown().await;
}

#[tokio::test]
async fn acp_reasoning_streams_thought_chunks() {
    let cwd = unique_dir("thoughtcwd");
    let mut client = AcpClient::connect(HashMap::from([(
        "FAKE_SCENARIO",
        "turn-reasoning".to_string(),
    )]))
    .await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["think aloud"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    let thoughts: String = frames
        .iter()
        .filter(|f| {
            f.get("method") == Some(&json!("session/update"))
                && f["params"]["update"]["sessionUpdate"] == json!("agent_thought_chunk")
        })
        .filter_map(|f| {
            f["params"]["update"]["content"]["text"]
                .as_str()
                .map(str::to_string)
        })
        .collect();
    // The completed summary restates the streamed deltas: no double-emit.
    assert_eq!(thoughts, "Considering the schemaThen testing");
    client.shutdown().await;
}

#[tokio::test]
async fn acp_reasoning_quiet_still_emits_summary_once() {
    let cwd = unique_dir("quietthoughtcwd");
    let mut client = AcpClient::connect(HashMap::from([(
        "FAKE_SCENARIO",
        "turn-reasoning-quiet".to_string(),
    )]))
    .await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["think quietly"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    let thoughts: Vec<String> = frames
        .iter()
        .filter(|f| {
            f.get("method") == Some(&json!("session/update"))
                && f["params"]["update"]["sessionUpdate"] == json!("agent_thought_chunk")
        })
        .filter_map(|f| {
            f["params"]["update"]["content"]["text"]
                .as_str()
                .map(str::to_string)
        })
        .collect();
    assert_eq!(thoughts, vec!["Committed thought".to_string()]);
    client.shutdown().await;
}

#[tokio::test]
async fn acp_tool_calls_complete_without_live_tool_updates() {
    // Adapted port: the reference bridges tool completions to `tool_call`
    // updates with result text, but this fold renders tools as status lines
    // (reference parity) and the ACP driver has no live tool surface — the
    // turn completes with agent text only. History replay DOES emit
    // `tool_call` (see the replay golden); live tool bridging is unresolved.
    let cwd = unique_dir("toolbridgecwd");
    let mut client =
        AcpClient::connect(HashMap::from([("FAKE_SCENARIO", "turn-tool".to_string())])).await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["run the tool"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    assert!(
        !frames.iter().any(|f| matches!(
            f["params"]["update"]["sessionUpdate"].as_str(),
            Some("tool_call" | "tool_call_update")
        )),
        "no live tool updates today: {frames:?}"
    );
    let chunks: String = frames
        .iter()
        .filter(|f| f["params"]["update"]["sessionUpdate"] == json!("agent_message_chunk"))
        .filter_map(|f| {
            f["params"]["update"]["content"]["text"]
                .as_str()
                .map(str::to_string)
        })
        .collect();
    assert_eq!(chunks, "wrapped");
    client.shutdown().await;
}

#[tokio::test]
async fn acp_approval_without_id_cancels_the_turn() {
    let log = unique_dir("noidappr").join("fake.log");
    let cwd = unique_dir("noidapprcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-approval-no-id".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["do the thing"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "cancelled"}),
        "an id-less approval cancels rather than deciding blind"
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(log_text.contains("turn/cancel"), "{log_text}");
    assert!(
        !log_text.contains("approval/decide"),
        "never decide without the durable id: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_malformed_acp_line_is_survived() {
    // Adapted port: garbage lines are warn-and-skipped (no rejection reply
    // — there is no id to correlate), and the session continues.
    let mut client = AcpClient::connect(HashMap::new()).await;
    let cwd = unique_dir("malformedcwd");
    client
        .tx
        .write_all(b"{oops this is not json\n")
        .await
        .expect("raw write");
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let init = client.recv().await;
    assert_eq!(init["id"], json!(1), "garbage yields no frame: {init}");
    assert_eq!(init["result"]["protocolVersion"], json!(1));
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    assert!(frames.last().unwrap().get("result").is_some());
    client.shutdown().await;
}

#[tokio::test]
async fn acp_invalid_session_roots_rejected() {
    let mut client = AcpClient::connect(HashMap::new()).await;
    let cwd = unique_dir("rootsok");
    let file = cwd.join("not-a-dir.txt");
    std::fs::write(&file, "x").expect("seed file");
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;
    for (id, params) in [
        (2, json!({"cwd": "relative/path"})),
        (3, json!({"cwd": "/nonexistent-bridge-dir-9f3c"})),
        (4, json!({"cwd": file.to_string_lossy()})),
    ] {
        client
            .send(&json!({"jsonrpc": "2.0", "id": id, "method": "session/new", "params": params}))
            .await;
        let err = client.recv().await;
        assert_eq!(err["error"]["code"], json!(-32602), "{params}");
    }
    // Resume admits an omitted cwd, but a supplied one must be absolute.
    client
        .send(&json!({"jsonrpc": "2.0", "id": 5, "method": "session/load",
            "params": {"sessionId": "fake-sess-1", "cwd": "relative/path"}}))
        .await;
    let err = client.recv().await;
    assert_eq!(err["error"]["code"], json!(-32602));
    client.shutdown().await;
}

#[tokio::test]
async fn acp_load_rejects_additional_dirs_and_tolerates_mcp() {
    let mut env = HashMap::from([("FAKE_SCENARIO", "serve".to_string())]);
    env.insert("FAKE_HISTORY", "1".to_string());
    let mut client = AcpClient::connect(env).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/load",
            "params": {"sessionId": "fake-sess-1",
                "additionalDirectories": ["/tmp/elsewhere"]}}))
        .await;
    let err = client.recv().await;
    assert_eq!(err["error"]["code"], json!(-32602));
    assert!(
        err["error"]["message"]
            .as_str()
            .unwrap_or("")
            .contains("additional directories")
    );
    // Client MCP servers cannot be forwarded: tolerated, never fatal.
    client
        .send(&json!({"jsonrpc": "2.0", "id": 3, "method": "session/load",
            "params": {"sessionId": "fake-sess-1",
                "mcpServers": [{"name": "x", "command": "y"}]}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert!(frames.last().unwrap().get("result").is_some());
    client.shutdown().await;
}

#[tokio::test]
async fn acp_approval_mode_mismatch_adopts_loudly() {
    // Adapted port: the reference fails `session/new` on a mode mismatch,
    // but the bridge adopts the echoed mode (warn-loudly, never fail) and
    // the session stays usable.
    let startup = expected_mode();
    let fake_mode = if startup == "denyUnmatched" {
        "allowAll"
    } else {
        "denyUnmatched"
    };
    let mut client =
        AcpClient::connect(HashMap::from([("FAKE_MODE", fake_mode.to_string())])).await;
    let cwd = unique_dir("mismatchcwd");
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let created = frames.last().expect("session/new reply");
    assert!(
        created.get("result").is_some(),
        "mismatch never fails: {created}"
    );
    let adopted = mode_from_msp(fake_mode);
    assert_eq!(created["result"]["modes"]["currentModeId"], json!(adopted));
    assert_eq!(
        created["result"]["configOptions"][0]["currentValue"],
        json!(adopted)
    );
    let acp_sid = created["result"]["sessionId"].as_str().unwrap();
    drain_advertisement(&mut client).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/close",
            "params": {"sessionId": acp_sid}}),
        )
        .await;
    assert_eq!(client.recv().await["result"], json!({}));
    client.shutdown().await;
}

#[tokio::test]
async fn acp_resource_link_inlines_workspace_text() {
    let dir = unique_dir("reslink");
    let input = dir.join("fake.input");
    let cwd = unique_dir("reslinkcwd");
    std::fs::write(cwd.join("notes.txt"), "the spices must flow").expect("seed note");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())]);
    env.insert("FAKE_INPUT", input.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    let uri = format!("file://{}/notes.txt", cwd.to_string_lossy());
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": [
                {"type": "resource_link", "uri": uri, "name": "notes.txt"}]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    assert!(
        frames.iter().any(|f| f["params"]["update"]
            == json!({"sessionUpdate": "user_message_chunk",
                "messageId": f["params"]["update"]["messageId"],
                "content": {"type": "resource_link", "uri": uri, "name": "notes.txt"}})),
        "the echo carries the link verbatim: {frames:?}"
    );
    let lines = read_input_lines(&input);
    let started: Vec<&Value> = lines
        .iter()
        .filter(|l| l["method"] == json!("turn/start"))
        .collect();
    assert_eq!(started.len(), 1);
    assert!(
        started[0].to_string().contains("the spices must flow"),
        "file text reaches the host: {}",
        started[0]
    );
    // An unreadable file degrades to a reference line, never an error.
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 4, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": [
                {"type": "resource_link", "uri": "file:///nonexistent-bridge-9f3c.txt",
                    "name": "missing.txt"}]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(4))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    let lines = read_input_lines(&input);
    assert!(
        lines
            .iter()
            .filter(|l| l["method"] == json!("turn/start"))
            .nth(1)
            .is_some_and(|l| l.to_string().contains("[resource:")),
        "missing file degrades to a reference: {lines:?}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_embedded_blob_audio_and_unknown_blocks_rejected() {
    let log = unique_dir("badblocks").join("fake.log");
    let cwd = unique_dir("badblockscwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    for (id, block, want) in [
        (
            3,
            json!({"type": "resource",
                "resource": {"blob": "aGk=", "mimeType": "text/plain"}}),
            "non-image",
        ),
        (4, json!({"type": "audio", "data": "aGk="}), "audio"),
        (5, json!({"type": "telepathy", "text": "hi"}), "unsupported"),
        (6, json!({"type": "resource", "resource": {}}), "needs"),
    ] {
        client
            .send(
                &json!({"jsonrpc": "2.0", "id": id, "method": "session/prompt",
                "params": {"sessionId": acp_sid, "prompt": [block]}}),
            )
            .await;
        let err = client.recv().await;
        assert_eq!(err["error"]["code"], json!(-32602), "{want}");
        assert!(
            err["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains(want),
            "{err}"
        );
    }
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        !log_text.contains("turn/start"),
        "rejected prompts never start a turn: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_slash_aliases_rewrite_to_skill_grammar() {
    let dir = unique_dir("slashalias");
    let input = dir.join("fake.input");
    let cwd = unique_dir("slashaliascwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())]);
    env.insert("FAKE_INPUT", input.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid,
                "prompt": [{"type": "text", "text": "/plan the cache"}]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    assert!(
        frames.iter().any(|f| f["params"]["update"]["sessionUpdate"]
            == json!("user_message_chunk")
            && f["params"]["update"]["content"]["text"] == json!("/plan the cache")),
        "the echo keeps client text verbatim: {frames:?}"
    );
    let lines = read_input_lines(&input);
    assert!(
        lines.iter().any(|l| l["method"] == json!("turn/start")
            && l.to_string().contains("/skill plan the cache")),
        "host input uses the skill grammar: {lines:?}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_leading_space_escapes_slash_command() {
    let dir = unique_dir("slashescape");
    let input = dir.join("fake.input");
    let cwd = unique_dir("slashescapecwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())]);
    env.insert("FAKE_INPUT", input.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid,
                "prompt": [{"type": "text", "text": " /plan the cache"}]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    let lines = read_input_lines(&input);
    let started: Vec<&Value> = lines
        .iter()
        .filter(|l| l["method"] == json!("turn/start"))
        .collect();
    assert_eq!(started.len(), 1);
    assert!(
        started[0].to_string().contains(" /plan the cache")
            && !started[0].to_string().contains("/skill"),
        "leading space escapes the rewrite: {}",
        started[0]
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_compact_in_running_text_is_a_prompt() {
    let log = unique_dir("compacttext").join("fake.log");
    let cwd = unique_dir("compacttextcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid,
                "prompt": [{"type": "text", "text": "please /compact the history"}]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        log_text.contains("turn/start"),
        "running text starts a turn: {log_text}"
    );
    assert!(
        !log_text.contains("session/compact"),
        "no protocol compact runs: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_reasoning_effort_sent_to_msp() {
    let dir = unique_dir("effortmsp");
    let input = dir.join("fake.input");
    let cwd = unique_dir("effortmspcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())]);
    env.insert("FAKE_INPUT", input.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["first"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 4, "method": "session/set_config_option",
            "params": {"sessionId": acp_sid,
                "configId": "reasoning_effort", "value": "ultra"}}),
        )
        .await;
    assert_eq!(
        client.recv().await["result"]["configOptions"][2]["currentValue"],
        json!("ultra")
    );
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 5, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["second"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(5))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    let started: Vec<Value> = read_input_lines(&input)
        .into_iter()
        .filter(|l| l["method"] == json!("turn/start"))
        .collect();
    assert_eq!(started.len(), 2);
    assert!(
        started[0]
            .to_string()
            .contains(r#""reasoningEffort":"medium""#),
        "default effort travels: {}",
        started[0]
    );
    assert!(
        started[1]
            .to_string()
            .contains(r#""reasoningEffort":"ultra""#),
        "selected effort travels: {}",
        started[1]
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_fork_fingerprint_resolves_occurrence_and_rejects() {
    let log = unique_dir("forkfp").join("fake.log");
    let cwd = unique_dir("forkfpcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "serve".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    env.insert("FAKE_HISTORY", "1".to_string());
    env.insert("FAKE_HISTORY_FORK", "1".to_string());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    let fingerprint = sha256_fingerprint("fork here");
    // Second occurrence of the duplicated text resolves to turn-2.
    client
        .send(&json!({"jsonrpc": "2.0", "id": 3, "method": "session/fork",
            "params": {"sessionId": acp_sid, "_meta": {"jetbrains": {"air": {
                "forkPoint": {"messageFingerprint": fingerprint,
                    "messageOccurrence": 2}}}}}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert!(frames.last().unwrap().get("result").is_some(), "{frames:?}");
    drain_advertisement(&mut client).await;
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(log_text.contains("cut=turn-2"), "{log_text}");
    // Occurrence past the duplicates, unmatched, and malformed points fail.
    for (id, point, want) in [
        (
            4,
            json!({"messageFingerprint": fingerprint, "messageOccurrence": 3}),
            "exceeds",
        ),
        (
            5,
            json!({"messageFingerprint": sha256_fingerprint("nothing matches this")}),
            "matched no agent message",
        ),
        (
            6,
            json!({"messageFingerprint": "sha256:zzz"}),
            "must be sha256",
        ),
        (7, json!({"messageId": "shell-no-turn"}), "not turn-scoped"),
    ] {
        client
            .send(
                &json!({"jsonrpc": "2.0", "id": id, "method": "session/fork",
                "params": {"sessionId": acp_sid,
                    "_meta": {"jetbrains": {"air": {"forkPoint": point}}}}}),
            )
            .await;
        let err = client.recv().await;
        assert_eq!(err["error"]["code"], json!(-32602), "{want}");
        assert!(
            err["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains(want),
            "{err}"
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn acp_v2_failed_terminal_yields_failed_stop() {
    let cwd = unique_dir("v2failcwd");
    let mut client = AcpClient::connect(HashMap::from([(
        "FAKE_SCENARIO",
        "turn-failed".to_string(),
    )]))
    .await;
    let acp_sid = v2_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["hi"]}}),
        )
        .await;
    let frames = client
        .recv_until(|f| {
            f.get("method") == Some(&json!("session/update"))
                && f["params"]["update"]["sessionUpdate"] == json!("state_update")
                && f["params"]["update"]["state"] == json!("idle")
        })
        .await;
    assert_eq!(
        frames.last().unwrap()["params"]["update"]["stopReason"],
        json!("_failed")
    );
    // The prompt was already acked `{}`: exactly one id-bearing frame.
    let replies: Vec<&Value> = frames
        .iter()
        .filter(|f| f.get("id") == Some(&json!(3)))
        .collect();
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0]["result"], json!({}));
    client.shutdown().await;
}

#[tokio::test]
async fn acp_host_death_fails_turn_and_restart_serves_new_session() {
    let dir = unique_dir("hostdeath");
    let launch_log = dir.join("fake.launch");
    let marker = dir.join("restart.marker");
    let cwd = unique_dir("hostdeathcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())]);
    env.insert("FAKE_CRASH_AFTER_TURN_START", "1".to_string());
    env.insert("FAKE_RESTART_MARKER", marker.to_string_lossy().into_owned());
    env.insert("FAKE_LAUNCH_LOG", launch_log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["hi"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    let reply = frames.last().expect("prompt reply");
    assert_eq!(reply["error"]["code"], json!(-32603));
    assert!(
        reply["error"]["message"]
            .as_str()
            .unwrap_or("")
            .contains("hostDead"),
        "mid-turn death reads as hostDead: {reply}"
    );
    // The supervisor restarted the host: a fresh session works end to end.
    client
        .send(&json!({"jsonrpc": "2.0", "id": 4, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(4))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 5, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["after the crash"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(5))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"}),
        "the replacement host serves turns"
    );
    let launches = std::fs::read_to_string(&launch_log).unwrap_or_default();
    assert_eq!(
        launches.lines().count(),
        2,
        "exactly one restart: {launches}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_client_disconnect_ends_promptly_with_turn_in_flight() {
    let log = unique_dir("disconnect").join("fake.log");
    let cwd = unique_dir("disconnectcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-tool-slow".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["slow work"]}}),
        )
        .await;
    poll_log_contains(&log, "turn/start cmd=", Duration::from_secs(10)).await;
    // EOF with a turn parked: the read loop ends promptly (the prompt
    // driver is detached and settles on its own; nothing blocks the exit).
    client.tx.shutdown().await.expect("eof");
    tokio::time::timeout(Duration::from_secs(5), &mut client.server)
        .await
        .expect("disconnect must end the loop promptly")
        .expect("server task");
    client.supervisor.shutdown().await;
}

#[tokio::test]
async fn acp_caps_omit_subagents_and_air() {
    let mut client = AcpClient::connect(HashMap::new()).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let v1 = client.recv().await;
    let caps = v1["result"]["agentCapabilities"].to_string().to_lowercase();
    assert!(
        !caps.contains("subagent") && !caps.contains("asynctask") && !caps.contains("air"),
        "v1 advertises only implemented caps: {caps}"
    );
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 2, "method": "_session/asyncTask",
            "params": {}}),
        )
        .await;
    assert_eq!(client.recv().await["error"]["code"], json!(-32601));
    client.shutdown().await;

    let mut client = AcpClient::connect(HashMap::new()).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 2}}))
        .await;
    let v2 = client.recv().await;
    let caps = v2["result"]["capabilities"].to_string().to_lowercase();
    assert!(
        !caps.contains("subagent") && !caps.contains("asynctask"),
        "v2 advertises only implemented caps: {caps}"
    );
    assert_eq!(
        v2["result"]["_meta"]["steering"]["supported"],
        json!(true),
        "steering stays advertised"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_dynamic_skills_advertise_and_normalize() {
    let input = unique_dir("skilldyn").join("fake.input");
    let cwd = unique_dir("skilldyncwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())]);
    env.insert("FAKE_INPUT", input.to_string_lossy().into_owned());
    let mut client = AcpClient::connect_muse(env).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    let advertised = drain_advertisement(&mut client).await;
    let commands = advertised["params"]["update"]["availableCommands"]
        .as_array()
        .expect("available_commands_update frame");
    let names: Vec<&str> = commands.iter().filter_map(|c| c["name"].as_str()).collect();
    for want in ["fake-skill", "other-skill", "skill", "help", "status"] {
        assert!(names.contains(&want), "advertised: {names:?}");
    }
    assert!(
        !names.contains(&"off-skill"),
        "disabled activations are filtered: {names:?}"
    );
    assert!(
        !names.contains(&"plan"),
        "a live registry replaces the fallback rows: {names:?}"
    );

    // `/<id>` normalizes to `/skill <id>` on the way to the host.
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["/fake-skill run it"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    let lines = read_input_lines(&input);
    let started: Vec<&Value> = lines
        .iter()
        .filter(|l| l["method"] == json!("turn/start"))
        .collect();
    assert_eq!(started.len(), 1);
    assert!(
        started[0].to_string().contains("/skill fake-skill run it"),
        "skill id normalizes to the /skill verb: {}",
        started[0]
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_help_status_and_usage_cards() {
    let input = unique_dir("cards").join("fake.input");
    let log = unique_dir("cards").join("fake.log");
    let cwd = unique_dir("cardscwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "serve".to_string())]);
    env.insert("FAKE_INPUT", input.to_string_lossy().into_owned());
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    env.insert("FAKE_HISTORY", "1".to_string());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;

    for (id, slash, want) in [
        (3, "/help", "**Commands**"),
        (4, "/status", "**Status**"),
        (5, "/usage", "**Usage**"),
        (6, "/recap", "seeded"),
    ] {
        client
            .send(
                &json!({"jsonrpc": "2.0", "id": id, "method": "session/prompt",
                "params": {"sessionId": acp_sid, "prompt": [slash]}}),
            )
            .await;
        let frames = client.recv_until(|f| f.get("id") == Some(&json!(id))).await;
        assert_eq!(
            frames.last().unwrap()["result"],
            json!({"stopReason": "end_turn"}),
            "{slash} settles end_turn: {frames:?}"
        );
        assert!(
            frames
                .iter()
                .any(|f| f["params"]["update"]["sessionUpdate"] == "user_message_chunk"),
            "{slash} echoes the command: {frames:?}"
        );
        let card = frames
            .iter()
            .filter(|f| f["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
            .filter_map(|f| f["params"]["update"]["content"]["text"].as_str())
            .collect::<String>();
        assert!(card.contains(want), "{slash} card has {want}: {card}");
    }
    // Protocol commands never reach the host as turns.
    let lines = read_input_lines(&input);
    assert!(
        !lines.iter().any(|l| l["method"] == json!("turn/start")),
        "cards settle locally: {lines:?}"
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        log_text.contains("session/read"),
        "status/recap read the host: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_name_models_and_effort_call_the_host() {
    let log = unique_dir("cmds").join("fake.log");
    let input = unique_dir("cmds").join("fake.input");
    let cwd = unique_dir("cmdscwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "serve".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    env.insert("FAKE_INPUT", input.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;

    let card_of = |frames: &[Value]| -> String {
        frames
            .iter()
            .filter(|f| f["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
            .filter_map(|f| f["params"]["update"]["content"]["text"].as_str())
            .collect()
    };
    for (id, slash, want) in [
        (
            3,
            "/name bridge session",
            "Session renamed to bridge session.",
        ),
        (4, "/models", "fake-a"),
        (5, "/model fake-a", "Model set to fake-a."),
        (6, "/effort high", "Reasoning effort set to high."),
        (7, "/effort", "Reasoning effort: high"),
        (8, "/effort bogus", "Unknown effort"),
    ] {
        client
            .send(
                &json!({"jsonrpc": "2.0", "id": id, "method": "session/prompt",
                "params": {"sessionId": acp_sid, "prompt": [slash]}}),
            )
            .await;
        let frames = client.recv_until(|f| f.get("id") == Some(&json!(id))).await;
        assert_eq!(
            frames.last().unwrap()["result"],
            json!({"stopReason": "end_turn"}),
            "{slash} settles end_turn: {frames:?}"
        );
        let card = card_of(&frames);
        assert!(card.contains(want), "{slash} card has {want}: {card}");
    }
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    for want in [
        "session/rename",
        "name=bridge session",
        "session/setModel",
        "model=fake-a",
        "session/setReasoningEffort",
        "effort=high",
    ] {
        assert!(log_text.contains(want), "host saw {want}: {log_text}");
    }
    let lines = read_input_lines(&input);
    assert!(
        !lines.iter().any(|l| l["method"] == json!("turn/start")),
        "host-backed commands still settle locally: {lines:?}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_exit_closes_the_session() {
    let cwd = unique_dir("exitcwd");
    let mut client = AcpClient::connect(HashMap::new()).await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["/exit"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    // The session is gone: a follow-up prompt fails rather than turning.
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 4, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["hello?"]}}),
        )
        .await;
    let err = client.recv().await;
    assert!(
        err.get("error").is_some(),
        "prompt after /exit is a caller error: {err}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_approval_selected_option_decides_it() {
    let log = unique_dir("selappr").join("fake.log");
    let cwd = unique_dir("selapprcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-approval".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["do the thing"]}}),
        )
        .await;
    let mut frames = Vec::new();
    loop {
        let frame = client.recv().await;
        if frame["method"].as_str() == Some("session/request_permission") {
            assert_eq!(
                frame["params"]["toolCall"]["kind"].as_str(),
                Some("execute"),
                "shell subject maps to the execute kind: {frame:?}"
            );
            client
                .send(&json!({"jsonrpc": "2.0", "id": frame["id"].clone(),
                    "result": {"outcome": {"outcome": "selected", "optionId": "c-allow"}}}))
                .await;
            continue;
        }
        let done = frame.get("id") == Some(&json!(3));
        frames.push(frame);
        if done {
            break;
        }
    }
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        log_text.contains("approval/decide")
            && log_text.contains("approval=ap-1")
            && log_text.contains("choice=c-allow"),
        "the selected option decides verbatim: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_user_input_bridges_to_elicitation() {
    let log = unique_dir("uiform").join("fake.log");
    let cwd = unique_dir("uiformcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-userinput-form".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1,
                "clientCapabilities": {"elicitation": {"form": {}}}}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["ask me later"]}}),
        )
        .await;
    let mut frames = Vec::new();
    let mut elicitations = 0;
    loop {
        let frame = client.recv().await;
        if frame["method"].as_str() == Some("elicitation/create") {
            elicitations += 1;
            assert_eq!(frame["params"]["mode"].as_str(), Some("form"));
            // Display labels dedupe: [red, red, blue] → [red, red (2), blue].
            let labels = frame["params"]["requestedSchema"]["properties"]["q0"]["enum"]
                .as_array()
                .expect("enum labels");
            assert_eq!(
                labels,
                &vec![json!("red"), json!("red (2)"), json!("blue")],
                "duplicate display labels dedupe: {frame:?}"
            );
            // "red (2)" maps back to the host's second "red" label.
            client
                .send(&json!({"jsonrpc": "2.0", "id": frame["id"].clone(),
                    "result": {"action": "accept", "content": {"q0": "red (2)"}}}))
                .await;
            continue;
        }
        let done = frame.get("id") == Some(&json!(3));
        frames.push(frame);
        if done {
            break;
        }
    }
    assert_eq!(elicitations, 1);
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        log_text.contains("userInput/answer") && log_text.contains("selectedLabel"),
        "the form answer lands as selectedLabel: {log_text}"
    );
    assert!(
        !log_text.contains("userInput/cancel"),
        "accepted elicitations never cancel: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_user_input_decline_cancels_fail_closed() {
    let log = unique_dir("uidecline").join("fake.log");
    let cwd = unique_dir("uideclinecwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-userinput-form".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1,
                "clientCapabilities": {"elicitation": {"form": {}}}}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["ask me later"]}}),
        )
        .await;
    let mut frames = Vec::new();
    loop {
        let frame = client.recv().await;
        if frame["method"].as_str() == Some("elicitation/create") {
            client
                .send(&json!({"jsonrpc": "2.0", "id": frame["id"].clone(),
                    "result": {"action": "decline"}}))
                .await;
            continue;
        }
        let done = frame.get("id") == Some(&json!(3));
        frames.push(frame);
        if done {
            break;
        }
    }
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        log_text.contains("userInput/cancel"),
        "a declined form cancels the prompt: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_session_new_forwards_mcp_servers() {
    let input = unique_dir("mcpnew").join("fake.input");
    let log = unique_dir("mcpnew").join("fake.log");
    let cwd = unique_dir("mcpnewcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "serve".to_string())]);
    env.insert("FAKE_INPUT", input.to_string_lossy().into_owned());
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
        "params": {"cwd": cwd.to_string_lossy(), "mcpServers": [
            {"name": "files", "command": "mcp-files", "args": ["--root"],
             "env": [{"name": "HOME", "value": "/r"}]},
            {"name": "web", "url": "https://mcp.example/s",
             "headers": {"Authorization": "Bearer t"}},
            {"name": "broken"},
        ]}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    assert!(
        frames.last().unwrap().get("result").is_some(),
        "unmappable entries never brick the session: {frames:?}"
    );
    let lines = read_input_lines(&input);
    let started: Vec<&Value> = lines
        .iter()
        .filter(|l| l["method"] == json!("session/start"))
        .collect();
    assert_eq!(started.len(), 1, "one session/start: {lines:?}");
    assert_eq!(
        started[0]["params"]["config"],
        json!({"mcpServers": {
            "files": {"transport": "stdio", "command": "mcp-files",
                      "args": ["--root"], "env": {"HOME": "/r"}},
            "web": {"transport": "streamableHttp", "url": "https://mcp.example/s",
                    "headers": {"Authorization": "Bearer t"}},
        }}),
        "config.mcpServers forwarded: {}",
        started[0]
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        log_text.contains("mcp=files,web"),
        "start line names the forwarded servers: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_stop_cancels_the_in_flight_turn() {
    let log = unique_dir("stopcmd").join("fake.log");
    let cwd = unique_dir("stopcmdcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-tool-slow".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;

    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["slow work"]}}),
        )
        .await;
    poll_log_contains(&log, "turn/start cmd=", Duration::from_secs(10)).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 4, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["/stop"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(4))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"}),
        "/stop settles inline: {frames:?}"
    );
    let card: String = frames
        .iter()
        .filter(|f| f["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
        .filter_map(|f| f["params"]["update"]["content"]["text"].as_str())
        .collect();
    assert!(card.contains("Stopped 1 turn"), "/stop card: {card}");
    // The slow prompt settles cancelled, exactly like session/cancel.
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "cancelled"})
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    let start = log_text
        .lines()
        .find(|l| l.starts_with("turn/start cmd="))
        .expect("turn/start logged");
    let cmd = start
        .split_whitespace()
        .find(|t| t.starts_with("cmd="))
        .expect("commandId logged");
    let turn = cmd.trim_start_matches("cmd=");
    assert!(
        log_text.contains("turn/cancel cmd=") && log_text.contains(&format!("turn={turn}")),
        "/stop cancels the EXPLICIT in-flight turn: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_stop_idle_reports_nothing_to_stop() {
    let log = unique_dir("stopidle").join("fake.log");
    let cwd = unique_dir("stopidlecwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "serve".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;

    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["/stop"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"}),
        "/stop settles inline: {frames:?}"
    );
    let card: String = frames
        .iter()
        .filter(|f| f["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
        .filter_map(|f| f["params"]["update"]["content"]["text"].as_str())
        .collect();
    assert!(
        card.contains("No in-flight turn to stop"),
        "/stop idle card: {card}"
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        !log_text.contains("turn/cancel"),
        "idle /stop sends no cancel: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_goal_and_tasks_cards_render_folded_facts() {
    let cwd = unique_dir("goalcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "serve".to_string())]);
    env.insert("FAKE_GOAL", "1".to_string());
    env.insert("FAKE_TODOS", "1".to_string());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;

    let card_of = |frames: &[Value]| -> String {
        frames
            .iter()
            .filter(|f| f["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
            .filter_map(|f| f["params"]["update"]["content"]["text"].as_str())
            .collect()
    };
    // /status triggers session/read; the fake emits goalChanged +
    // todoListChanged after the result, which the observer folds.
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["/status"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    // The fold lands asynchronously; poll the pure-fold cards.
    let mut next_id = 4;
    let mut poll_card = async |slash: &str, want: &str| -> String {
        let mut card = String::new();
        for _ in 0..50 {
            client
                .send(
                    &json!({"jsonrpc": "2.0", "id": next_id, "method": "session/prompt",
                    "params": {"sessionId": acp_sid, "prompt": [slash]}}),
                )
                .await;
            let frames = client
                .recv_until(|f| f.get("id") == Some(&json!(next_id)))
                .await;
            next_id += 1;
            assert_eq!(
                frames.last().unwrap()["result"],
                json!({"stopReason": "end_turn"}),
                "{slash} settles inline"
            );
            card = card_of(&frames);
            if card.contains(want) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        card
    };
    let goal = poll_card("/goal", "Ship the fake feature").await;
    for want in [
        "**Goal**",
        "- Objective: Ship the fake feature",
        "- Status: active · 40%",
        "- Current: fake tests",
        "- Next: fake docs",
    ] {
        assert!(goal.contains(want), "/goal card has {want}: {goal}");
    }
    let tasks = poll_card("/tasks", "Writing fake tests").await;
    for want in [
        "**Tasks**",
        "- [x] fake done",
        "- [~] Writing fake tests",
        "- [ ] fake todo",
    ] {
        assert!(tasks.contains(want), "/tasks card has {want}: {tasks}");
    }
    // A second /status now carries the goal line (objective, not echo).
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": next_id, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["/status"]}}),
        )
        .await;
    let frames = client
        .recv_until(|f| f.get("id") == Some(&json!(next_id)))
        .await;
    let status = card_of(&frames);
    assert!(
        status.contains("- Goal: Ship the fake feature"),
        "status goal line: {status}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_model_children_stream_as_tool_blocks_and_cards() {
    // A turn that spawns a subagent + workflow: the observer retains both,
    // streams each as its own `tool_call` block (TUI-style), and the
    // /subagents + /workflows cards read the retention.
    let cwd = unique_dir("childrencwd");
    let mut client = AcpClient::connect(HashMap::from([(
        "FAKE_SCENARIO",
        "turn-children".to_string(),
    )]))
    .await;
    let acp_sid = v1_session(&mut client, &cwd).await;

    let card_of = |frames: &[Value]| -> String {
        frames
            .iter()
            .filter(|f| f["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
            .filter_map(|f| f["params"]["update"]["content"]["text"].as_str())
            .collect()
    };
    let tool_blocks = |frames: &[Value]| -> Vec<(String, String, String)> {
        frames
            .iter()
            .filter(|f| f["params"]["update"]["sessionUpdate"] == "tool_call")
            .map(|f| {
                let update = &f["params"]["update"];
                (
                    update["toolCallId"].as_str().unwrap_or("").to_string(),
                    update["title"].as_str().unwrap_or("").to_string(),
                    update["status"].as_str().unwrap_or("").to_string(),
                )
            })
            .collect()
    };

    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": [{"type": "text", "text": "go"}]}}),
        )
        .await;
    let mut seen = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        seen.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    // The observer folds asynchronously; poll the cards (collecting every
    // frame) until retention lands.
    let mut next_id = 4;
    let mut poll_card = async |slash: &str, want: &str| -> String {
        let mut card = String::new();
        for _ in 0..50 {
            client
                .send(
                    &json!({"jsonrpc": "2.0", "id": next_id, "method": "session/prompt",
                    "params": {"sessionId": acp_sid, "prompt": [slash]}}),
                )
                .await;
            let frames = client
                .recv_until(|f| f.get("id") == Some(&json!(next_id)))
                .await;
            next_id += 1;
            assert_eq!(
                frames.last().unwrap()["result"],
                json!({"stopReason": "end_turn"}),
                "{slash} settles inline"
            );
            card = card_of(&frames);
            seen.extend(frames);
            if card.contains(want) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        card
    };
    let subagents = poll_card("/subagents", "Explore the schema").await;
    for want in ["**Subagents**", "- [x] Explore the schema"] {
        assert!(
            subagents.contains(want),
            "/subagents has {want}: {subagents}"
        );
    }
    let workflows = poll_card("/workflows", "research").await;
    for want in ["**Workflows**", "- [x] research (2/2 children"] {
        assert!(
            workflows.contains(want),
            "/workflows has {want}: {workflows}"
        );
    }
    // Both children streamed as their own blocks with mapped statuses.
    let blocks = tool_blocks(&seen);
    for (id, title) in [("child-1", "Explore the schema"), ("wf-1", "research")] {
        for status in ["in_progress", "completed"] {
            assert!(
                blocks
                    .iter()
                    .any(|(i, t, s)| i == id && t == title && s == status),
                "{id}/{title} streams {status}: {blocks:?}"
            );
        }
    }
    client.shutdown().await;
}

#[tokio::test]
async fn acp_subagent_verbs_execute_and_report() {
    // Verbs resolve targets, call the `subagent/*` methods with the durable
    // id, and report admission; user errors return cards, never host calls.
    let log = unique_dir("verbs").join("fake.log");
    let cwd = unique_dir("verbscwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-children".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;

    let card_of = |frames: &[Value]| -> String {
        frames
            .iter()
            .filter(|f| f["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
            .filter_map(|f| f["params"]["update"]["content"]["text"].as_str())
            .collect()
    };
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": [{"type": "text", "text": "go"}]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    // The observer folds asynchronously; wait for retention first so every
    // verb below resolves deterministically (no elicitation surface here).
    let mut next_id = 4;
    let mut run_slash = async |slash: &str| -> String {
        client
            .send(
                &json!({"jsonrpc": "2.0", "id": next_id, "method": "session/prompt",
                "params": {"sessionId": acp_sid, "prompt": [slash]}}),
            )
            .await;
        let frames = client
            .recv_until(|f| f.get("id") == Some(&json!(next_id)))
            .await;
        next_id += 1;
        assert_eq!(
            frames.last().unwrap()["result"],
            json!({"stopReason": "end_turn"}),
            "{slash} settles inline"
        );
        card_of(&frames)
    };
    let mut card = String::new();
    for _ in 0..50 {
        card = run_slash("/subagents").await;
        if card.contains("Explore the schema") && card.contains("Write the migration") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        card.contains("Explore the schema") && card.contains("Write the migration"),
        "retention lands: {card}"
    );
    let log_text = || std::fs::read_to_string(&log).unwrap_or_default();

    let stop = run_slash("/subagents stop child-1").await;
    assert!(
        stop.contains("Stop requested for 'Explore the schema' (accepted)"),
        "{stop}"
    );
    assert!(
        log_text().contains("subagent/stop") && log_text().contains("sub=sa-1"),
        "stop targets the durable id: {}",
        log_text()
    );
    // Aliases + durable-id prefixes resolve the same row.
    let kill = run_slash("/subagents kill sa-2").await;
    assert!(
        kill.contains("Stop requested for 'Write the migration'"),
        "{kill}"
    );
    let note = run_slash("/subagents message child-2 hurry up").await;
    assert!(
        note.contains("Note queued for 'Write the migration' (accepted)"),
        "{note}"
    );
    assert!(
        log_text().contains("subagent/sendMessage") && log_text().contains("sub=sa-2"),
        "message targets the durable id: {}",
        log_text()
    );
    // Retained results render without consuming (no host call).
    let result = run_slash("/subagents result child-1").await;
    for want in [
        "**Result: Explore the schema**",
        "schema mapped",
        "tables: users, orders",
    ] {
        assert!(result.contains(want), "{want}: {result}");
    }
    assert!(
        !log_text().contains("subagent/readResult"),
        "retained results render locally: {}",
        log_text()
    );
    // Nothing retained: consume, and the content streams into the block.
    let pending = run_slash("/subagents result child-2").await;
    assert!(
        pending.contains("Result requested for 'Write the migration' (accepted)"),
        "{pending}"
    );
    assert!(
        log_text().contains("subagent/readResult") && log_text().contains("sub=sa-2"),
        "missing results consume: {}",
        log_text()
    );
    // User errors stay cards (unknown verb/target, workflow target, body).
    let usage = run_slash("/subagents frobnicate").await;
    assert!(usage.contains("Subagents verbs"), "{usage}");
    let miss = run_slash("/subagents stop zzz").await;
    assert!(miss.contains("No subagent matches 'zzz'"), "{miss}");
    let flow = run_slash("/subagents stop wf-1").await;
    assert!(flow.contains("workflows have no control methods"), "{flow}");
    let bodyless = run_slash("/subagents message child-2").await;
    assert!(bodyless.contains("needs message text"), "{bodyless}");
    let workflows = run_slash("/workflows foo").await;
    assert!(workflows.contains("display-only"), "{workflows}");
    client.shutdown().await;
}

#[tokio::test]
async fn acp_subagent_verbs_offer_pickers() {
    // With a form surface, missing targets (and bodies) pop elicitation
    // selects instead of failing: select-only, and select+text in one form.
    let log = unique_dir("verbpick").join("fake.log");
    let cwd = unique_dir("verbpickcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-children".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1,
                "clientCapabilities": {"elicitation": {"form": {}}}}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    drain_advertisement(&mut client).await;
    let card_of = |frames: &[Value]| -> String {
        frames
            .iter()
            .filter(|f| f["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
            .filter_map(|f| f["params"]["update"]["content"]["text"].as_str())
            .collect()
    };
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": [{"type": "text", "text": "go"}]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert_eq!(
        frames.last().unwrap()["result"],
        json!({"stopReason": "end_turn"})
    );
    // Wait for retention (bare cards never elicit).
    let mut next_id = 4;
    for _ in 0..50 {
        client
            .send(
                &json!({"jsonrpc": "2.0", "id": next_id, "method": "session/prompt",
                "params": {"sessionId": acp_sid, "prompt": ["/subagents"]}}),
            )
            .await;
        let frames = client
            .recv_until(|f| f.get("id") == Some(&json!(next_id)))
            .await;
        next_id += 1;
        if card_of(&frames).contains("Write the migration") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Run one verb, answering every elicitation with `answer`, and return
    // the card plus the elicitation params seen.
    let mut run_verb = async |slash: &str, answer: Value| -> (String, Vec<Value>) {
        client
            .send(
                &json!({"jsonrpc": "2.0", "id": next_id, "method": "session/prompt",
                "params": {"sessionId": acp_sid, "prompt": [slash]}}),
            )
            .await;
        let mut frames = Vec::new();
        let mut elicited = Vec::new();
        loop {
            let frame = client.recv().await;
            if frame["method"].as_str() == Some("elicitation/create") {
                elicited.push(frame["params"].clone());
                client
                    .send(&json!({"jsonrpc": "2.0", "id": frame["id"].clone(),
                        "result": {"action": "accept", "content": answer}}))
                    .await;
                continue;
            }
            let done = frame.get("id") == Some(&json!(next_id));
            frames.push(frame);
            if done {
                break;
            }
        }
        next_id += 1;
        assert_eq!(
            frames.last().unwrap()["result"],
            json!({"stopReason": "end_turn"}),
            "{slash} settles inline"
        );
        (card_of(&frames), elicited)
    };
    // Target-less `stop`: a select over both children with status marks.
    let (card, elicited) = run_verb(
        "/subagents stop",
        json!({"choice": "[~] Write the migration (child-2)"}),
    )
    .await;
    assert_eq!(elicited.len(), 1);
    let labels = elicited[0]["requestedSchema"]["properties"]["choice"]["enum"]
        .as_array()
        .expect("choice enum");
    assert!(
        labels.contains(&json!("[x] Explore the schema (child-1)"))
            && labels.contains(&json!("[~] Write the migration (child-2)")),
        "picker lists both children: {labels:?}"
    );
    assert!(
        card.contains("Stop requested for 'Write the migration' (accepted)"),
        "{card}"
    );
    // Target-less `message`: select + text in a single form.
    let (card, elicited) = run_verb(
        "/subagents message",
        json!({"choice": "[x] Explore the schema (child-1)", "text": "well done"}),
    )
    .await;
    assert_eq!(elicited.len(), 1);
    let props = &elicited[0]["requestedSchema"]["properties"];
    assert!(props.get("choice").is_some() && props.get("text").is_some());
    assert_eq!(props["text"]["type"], json!("string"));
    assert!(
        card.contains("Note queued for 'Explore the schema' (accepted)"),
        "{card}"
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        log_text.contains("subagent/stop") && log_text.contains("sub=sa-2"),
        "picked stop lands: {log_text}"
    );
    assert!(
        log_text.contains("subagent/sendMessage") && log_text.contains("sub=sa-1"),
        "picked message lands: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_set_mode_switches_approval_posture() {
    // `session/set_mode` drives the switch and fails closed on unknown
    // modes/sessions (clients revert optimistic updates on error).
    let log = unique_dir("setmode").join("fake.log");
    let cwd = unique_dir("setmodecwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "serve".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    for (id, mode) in [(3, "auto"), (4, "deny"), (5, "ask")] {
        client
            .send(
                &json!({"jsonrpc": "2.0", "id": id, "method": "session/set_mode",
                "params": {"sessionId": acp_sid, "modeId": mode}}),
            )
            .await;
        let reply = client.recv().await;
        assert!(reply.get("result").is_some(), "{mode} switches: {reply}");
    }
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 6, "method": "session/set_mode",
            "params": {"sessionId": acp_sid, "modeId": "frenzied"}}),
        )
        .await;
    assert_eq!(client.recv().await["error"]["code"], json!(-32602));
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 7, "method": "session/set_mode",
            "params": {"sessionId": "acp-nope", "modeId": "auto"}}),
        )
        .await;
    assert_eq!(client.recv().await["error"]["code"], json!(-32602));
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    for want in [
        "session/setApprovalMode",
        "mode=allowAll",
        "mode=denyUnmatched",
        "mode=promptUnmatched",
    ] {
        assert!(log_text.contains(want), "host saw {want}: {log_text}");
    }
    client.shutdown().await;
}

#[tokio::test]
async fn acp_yolo_declines_questions_without_interrupting() {
    // Yolo rides host allowAll and auto-declines user questions — even
    // when the client advertised a form surface. The turn still settles
    // end_turn; the host proceeds without answers.
    let log = unique_dir("yoloq").join("fake.log");
    let cwd = unique_dir("yoloqcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "turn-userinput-form".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1,
                "clientCapabilities": {"elicitation": {"form": {}}}}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    drain_advertisement(&mut client).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/set_mode",
            "params": {"sessionId": acp_sid, "modeId": "yolo"}}),
        )
        .await;
    assert!(client.recv().await.get("result").is_some(), "yolo switches");
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 4, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["ask me later"]}}),
        )
        .await;
    let mut saw_form = false;
    loop {
        let frame = client.recv().await;
        if frame["method"].as_str() == Some("elicitation/create") {
            saw_form = true;
        }
        if frame.get("id") == Some(&json!(4)) {
            assert_eq!(
                frame["result"],
                json!({"stopReason": "end_turn"}),
                "the turn survives unanswered questions"
            );
            break;
        }
    }
    assert!(!saw_form, "yolo never surfaces questions");
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        log_text.contains("userInput/cancel"),
        "yolo declines at the host: {log_text}"
    );
    assert!(
        log_text.contains("mode=allowAll"),
        "yolo rides allowAll: {log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_yolo_label_survives_matching_resume() {
    // The host only knows allowAll; resume must not degrade the yolo
    // label when the folded posture matches.
    let cwd = unique_dir("yoloresumecwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "serve".to_string())]);
    env.insert("FAKE_MODE", "allowAll".to_string());
    let mut client = AcpClient::connect(env).await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/set_mode",
            "params": {"sessionId": acp_sid, "modeId": "yolo"}}),
        )
        .await;
    assert!(client.recv().await.get("result").is_some());
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 4, "method": "session/resume",
            "params": {"sessionId": acp_sid}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(4))).await;
    let resumed = frames.last().expect("resume reply");
    assert_eq!(
        resumed["result"]["modes"]["currentModeId"],
        json!("yolo"),
        "resume keeps yolo: {resumed}"
    );
    assert_eq!(
        resumed["result"]["configOptions"][0]["currentValue"],
        json!("yolo")
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_model_and_effort_commands_offer_picker_forms() {
    // Bare `/models` and invalid `/effort` pop single-select
    // elicitation forms when the client has a form surface.
    let log = unique_dir("pickform").join("fake.log");
    let cwd = unique_dir("pickformcwd");
    let mut env = HashMap::from([("FAKE_SCENARIO", "serve".to_string())]);
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1,
                "clientCapabilities": {"elicitation": {"form": {}}}}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let acp_sid = frames.last().unwrap()["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    drain_advertisement(&mut client).await;

    // Bare `/models` → form offering the catalog → accept switches.
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["/models"]}}),
        )
        .await;
    let mut card = String::new();
    loop {
        let frame = client.recv().await;
        if frame["method"].as_str() == Some("elicitation/create") {
            let options = frame["params"]["requestedSchema"]["properties"]["choice"]["enum"]
                .as_array()
                .expect("choice enum");
            assert!(
                options.iter().any(|o| o == "fake-a"),
                "form offers the catalog: {frame:?}"
            );
            client
                .send(&json!({"jsonrpc": "2.0", "id": frame["id"].clone(),
                    "result": {"action": "accept",
                        "content": {"choice": "fake-a"}}}))
                .await;
            continue;
        }
        if frame.get("id") == Some(&json!(3)) {
            assert_eq!(frame["result"], json!({"stopReason": "end_turn"}));
            break;
        }
        if frame["params"]["update"]["sessionUpdate"] == "agent_message_chunk" {
            card.push_str(
                frame["params"]["update"]["content"]["text"]
                    .as_str()
                    .unwrap_or(""),
            );
        }
    }
    assert!(card.contains("Model set to fake-a"), "switch card: {card}");

    // Invalid `/effort` → form offering the tiers → accept switches.
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 4, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["/effort bogus"]}}),
        )
        .await;
    let mut card = String::new();
    loop {
        let frame = client.recv().await;
        if frame["method"].as_str() == Some("elicitation/create") {
            let options = frame["params"]["requestedSchema"]["properties"]["choice"]["enum"]
                .as_array()
                .expect("choice enum");
            assert_eq!(options.len(), 8, "form offers all tiers");
            client
                .send(&json!({"jsonrpc": "2.0", "id": frame["id"].clone(),
                    "result": {"action": "accept",
                        "content": {"choice": "ultra"}}}))
                .await;
            continue;
        }
        if frame.get("id") == Some(&json!(4)) {
            assert_eq!(frame["result"], json!({"stopReason": "end_turn"}));
            break;
        }
        if frame["params"]["update"]["sessionUpdate"] == "agent_message_chunk" {
            card.push_str(
                frame["params"]["update"]["content"]["text"]
                    .as_str()
                    .unwrap_or(""),
            );
        }
    }
    assert!(
        card.contains("Reasoning effort set to ultra"),
        "effort card: {card}"
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(log_text.contains("session/setModel"), "{log_text}");
    assert!(
        log_text.contains("session/setReasoningEffort"),
        "{log_text}"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn acp_model_and_effort_commands_fall_back_to_cards() {
    // Without a form surface, bare `/models` and `/effort` render
    // their cards (no memorization required either way).
    let cwd = unique_dir("pickcardcwd");
    let mut client =
        AcpClient::connect(HashMap::from([("FAKE_SCENARIO", "serve".to_string())])).await;
    let acp_sid = v1_session(&mut client, &cwd).await;
    let card_of = |frames: &[Value]| -> String {
        frames
            .iter()
            .filter(|f| f["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
            .filter_map(|f| f["params"]["update"]["content"]["text"].as_str())
            .collect()
    };
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["/models"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    let card = card_of(&frames);
    assert!(card.contains("**Models**"), "{card}");
    assert!(card.contains("fake-a"), "{card}");
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 4, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["/effort"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(4))).await;
    let card = card_of(&frames);
    assert!(card.contains("Reasoning effort:"), "{card}");
    client.shutdown().await;
}
