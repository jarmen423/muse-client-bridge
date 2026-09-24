//! Live-host tests: the bridge against real `muse serve` (HANDOFF P7).
//!
//! Env-gated: every test returns early unless `MUSE_LIVE_TESTS=1`, so plain
//! `cargo test` (and CI without credentials) stays green. Live runs need
//! `muse login` and spend subscription usage (two minimal turns total).

use std::time::Duration;

use muse_bridge::dispatch::Dispatcher;
use muse_bridge::http::{AppState, router};
use muse_bridge::msp::host::{HostConfig, Supervisor};
use serde_json::json;

fn live_enabled() -> bool {
    std::env::var("MUSE_LIVE_TESTS").as_deref() == Ok("1")
}

fn live_config() -> HostConfig {
    HostConfig::from_env(
        std::env::temp_dir(),
        false,
        &HostConfig::host_bin_from_env(),
    )
}

struct LiveServer {
    base: String,
    client: reqwest::Client,
    supervisor: std::sync::Arc<Supervisor>,
    serve_task: tokio::task::JoinHandle<()>,
}

impl LiveServer {
    async fn start() -> Self {
        let supervisor = Supervisor::launch(live_config())
            .await
            .expect("live launch");
        // Provider mode: the live e2e turns prove a workspace-less bridge
        // serves real requests (the unit + scripted suites cover the set arm).
        let dispatcher =
            Dispatcher::new(supervisor.clone(), None, "denyUnmatched").expect("dispatcher");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let base = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let serve_task = tokio::spawn(async move {
            axum::serve(listener, router(AppState { dispatcher }))
                .await
                .expect("serve");
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(300))
            .build()
            .expect("client");
        Self {
            base,
            client,
            supervisor,
            serve_task,
        }
    }

    async fn stop(self) {
        self.serve_task.abort();
        self.supervisor.shutdown().await;
    }
}

#[tokio::test]
async fn live_handshake_and_catalog() {
    if !live_enabled() {
        eprintln!("skipped: set MUSE_LIVE_TESTS=1 for live-host tests");
        return;
    }
    let supervisor = Supervisor::launch(live_config())
        .await
        .expect("live launch");
    let info = supervisor
        .current()
        .await
        .handshake_info()
        .expect("handshake facts");
    assert_eq!(info.schema_version, Some(1));
    eprintln!("live host: {} compat={:?}", info.host_label(), info.compat);
    let dispatcher = Dispatcher::new(
        supervisor.clone(),
        Some(&std::env::temp_dir()),
        "denyUnmatched",
    )
    .expect("dispatcher");
    let catalog = dispatcher.models().await.expect("live model/list");
    assert!(!catalog.models.is_empty(), "live catalog must list models");
    eprintln!("live models: {}", catalog.models.len());
    supervisor.shutdown().await;
}

#[tokio::test]
async fn live_chat_completions_e2e() {
    if !live_enabled() {
        eprintln!("skipped: set MUSE_LIVE_TESTS=1 for live-host tests");
        return;
    }
    let server = LiveServer::start().await;
    let response = server
        .client
        .post(format!("{}/v1/chat/completions", server.base))
        .json(&json!({
            "model": "muse-spark-1.3",
            "stream": false,
            "messages": [{"role": "user", "content": "Reply with the single word: ok"}],
        }))
        .send()
        .await
        .expect("chat request sends");
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.expect("chat JSON body");
    let content = body["choices"][0]["message"]["content"]
        .as_str()
        .unwrap_or_default();
    assert!(!content.is_empty(), "live chat turn produced text");
    assert!(body["usage"]["total_tokens"].as_u64().unwrap_or(0) > 0);
    eprintln!("live chat content: {content:?}");
    server.stop().await;
}

#[tokio::test]
async fn live_responses_e2e() {
    if !live_enabled() {
        eprintln!("skipped: set MUSE_LIVE_TESTS=1 for live-host tests");
        return;
    }
    let server = LiveServer::start().await;
    let response = server
        .client
        .post(format!("{}/v1/responses", server.base))
        .json(&json!({
            "model": "muse-spark-1.3",
            "stream": false,
            "input": "Reply with the single word: ok",
        }))
        .send()
        .await
        .expect("responses request sends");
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.expect("responses JSON body");
    assert_eq!(body["status"], "completed");
    let text = body["output"]
        .as_array()
        .and_then(|items| items.iter().find(|item| item["type"] == "message"))
        .and_then(|message| message["content"][0]["text"].as_str())
        .unwrap_or_default();
    assert!(!text.is_empty(), "live responses turn produced text");
    eprintln!("live responses message text: {text:?}");
    server.stop().await;
}

#[tokio::test]
async fn live_session_start_omits_workspace_root() {
    if !live_enabled() {
        eprintln!("skipped: set MUSE_LIVE_TESTS=1 for live-host tests");
        return;
    }
    let supervisor = Supervisor::launch(live_config())
        .await
        .expect("live launch");
    let conn = supervisor.current().await;
    let result = conn
        .command(
            "session/start",
            &json!({
                "commandId": conn.mint_command_id(),
                "approvalMode": "denyUnmatched",
            }),
        )
        .await
        .expect("live host must accept session/start without workspaceRoot");
    let session_id = result["session"]["sessionId"].as_str().unwrap_or("");
    assert!(!session_id.is_empty(), "no sessionId adopted: {result}");
    assert!(
        result["session"]["workspaceRoot"].is_null(),
        "host should adopt null workspaceRoot: {result}"
    );
    eprintln!("live no-workspace session: {session_id}");
    supervisor.shutdown().await;
}
