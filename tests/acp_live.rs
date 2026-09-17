//! Live ACP proof: one file-edit task against real `muse serve`.
//!
//! Env-gated: returns early unless `MUSE_LIVE_TESTS=1`, so plain `cargo
//! test` (and CI without credentials) stays green. Live runs need `muse
//! login` and spend subscription usage (one minimal turn).
//!
//! The session runs in `auto` approval mode (`MUSE_APPROVAL_MODE=auto`):
//! the bridge is fail-closed (no permission surface), so an edit turn in
//! a restrictive mode would auto-deny instead of proving the round trip.
//! The workspace is a fresh temp dir, bounding what the turn can touch.

use std::sync::Arc;
use std::time::Duration;

use muse_bridge::acp::server::serve;
use muse_bridge::dispatch::Dispatcher;
use muse_bridge::msp::host::{HostConfig, Supervisor};
use muse_bridge::msp::proto::{next_frame, write_frame};
use serde_json::{Value, json};
use tokio::io::BufReader;

fn live_enabled() -> bool {
    std::env::var("MUSE_LIVE_TESTS").as_deref() == Ok("1")
}

/// Generous per-frame budget: live turns stream for minutes.
const LIVE_FRAME_TIMEOUT: Duration = Duration::from_secs(300);

struct LiveAcp {
    tx: tokio::io::DuplexStream,
    rx: BufReader<tokio::io::DuplexStream>,
    server: tokio::task::JoinHandle<()>,
    supervisor: Arc<Supervisor>,
}

impl LiveAcp {
    async fn connect() -> Self {
        let supervisor = Supervisor::launch(HostConfig::from_env(std::env::temp_dir(), false))
            .await
            .expect("live launch");
        let dispatcher =
            Dispatcher::new(supervisor.clone(), None, "allowAll").expect("dispatcher setup");
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
        tokio::time::timeout(LIVE_FRAME_TIMEOUT, next_frame(&mut self.rx))
            .await
            .expect("live frame timed out")
            .expect("client read")
            .expect("server closed the stream")
    }

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

    async fn shutdown(self) {
        drop(self.tx);
        tokio::time::timeout(Duration::from_secs(30), self.server)
            .await
            .expect("server shutdown timed out")
            .expect("server task");
        self.supervisor.shutdown().await;
    }
}

#[tokio::test]
#[allow(unsafe_code, reason = "process-env mode for the live-only binary")]
async fn live_acp_file_edit_task() {
    if !live_enabled() {
        eprintln!("skipped: set MUSE_LIVE_TESTS=1 for live-host tests");
        return;
    }
    // SAFETY: this file's tests all require the same mode, and the live
    // suite runs explicitly — no hermetic test shares this process.
    unsafe {
        std::env::set_var("MUSE_APPROVAL_MODE", "auto");
    }

    let workspace = std::env::temp_dir().join(format!(
        "muse-bridge-live-acp-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&workspace).expect("live workspace");
    let marker = "hello from the acp live test";

    let mut client = LiveAcp::connect().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1, "clientCapabilities": {}}}))
        .await;
    let init = client.recv().await;
    assert_eq!(init["result"]["protocolVersion"], json!(1));

    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": workspace.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let created = frames.last().expect("session/new reply");
    let acp_sid = created["result"]["sessionId"]
        .as_str()
        .expect("acp session id")
        .to_string();
    eprintln!("live acp session: {acp_sid}");

    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": [{
                "type": "text",
                "text": format!(
                    "Create a file named hello-acp.txt in the workspace root \
                     containing exactly this single line: {marker}. \
                     Do nothing else.")}]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    let reply = frames.last().expect("prompt reply");
    assert_eq!(
        reply.get("result"),
        Some(&json!({"stopReason": "end_turn"})),
        "live turn must complete: {reply}"
    );
    let chunks: usize = frames
        .iter()
        .filter(|f| {
            f.get("method") == Some(&json!("session/update"))
                && f["params"]["update"]["sessionUpdate"] == json!("agent_message_chunk")
        })
        .count();
    eprintln!("live acp turn streamed {chunks} agent chunk(s)");

    let edited = std::fs::read_to_string(workspace.join("hello-acp.txt"))
        .expect("the turn creates hello-acp.txt");
    assert!(
        edited.contains(marker),
        "file carries the marker text: {edited:?}"
    );
    eprintln!("live acp file edit verified: {edited:?}");

    client.shutdown().await;
}

/// Live MCP proof: `session/new` carrying `mcpServers` is accepted at
/// construction. Without the `sessionMcp` grant request the live host
/// rejects this `capabilityRequired`, so success pins the whole path
/// (request → grant → forward). No turn runs: session creation alone
/// proves the wire shape. A broken command is deliberate — observed
/// live, it does not fail construction.
#[tokio::test]
async fn live_acp_mcp_servers_accepted_at_construction() {
    if !live_enabled() {
        eprintln!("skipped: set MUSE_LIVE_TESTS=1 for live-host tests");
        return;
    }
    let workspace = std::env::temp_dir().join(format!(
        "muse-bridge-live-mcp-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&workspace).expect("live workspace");

    let mut client = LiveAcp::connect().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1, "clientCapabilities": {}}}))
        .await;
    let init = client.recv().await;
    assert_eq!(init["result"]["protocolVersion"], json!(1));

    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
        "params": {"cwd": workspace.to_string_lossy(),
            "mcpServers": [
                {"name": "live-bogus",
                 "command": "/nonexistent/mcp-bridge-live-probe"},
            ]}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let created = frames.last().expect("session/new reply");
    assert!(
        created["result"]["sessionId"].is_string(),
        "config.mcpServers accepted at construction: {created}"
    );
    eprintln!(
        "live mcp session: {}",
        created["result"]["sessionId"].as_str().unwrap_or("?")
    );

    client.shutdown().await;
}
