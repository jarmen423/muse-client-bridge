//! HTTP compatibility tests: the full axum app over real loopback sockets
//! against scripted `fake_serve.py` hosts (HANDOFF P5). Proves status codes,
//! envelopes, and exact SSE byte sequences.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use muse_bridge::dispatch::Dispatcher;
use muse_bridge::http::{AppState, router};
use muse_bridge::msp::host::{HostConfig, Supervisor};
use serde_json::{Value, json};

fn fake_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake_serve.py")
}

fn scratch(test: &str, suffix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "muse-bridge-{}-{}-{}.{}",
        test,
        std::process::id(),
        uuid::Uuid::new_v4().simple(),
        suffix
    ));
    path
}

struct TestServer {
    base: String,
    client: reqwest::Client,
    supervisor: std::sync::Arc<Supervisor>,
    serve_task: tokio::task::JoinHandle<()>,
}

impl TestServer {
    async fn start(env: HashMap<&str, String>) -> Self {
        let config = HostConfig {
            bin: "python3".to_string(),
            subcommand: None,
            serve_args: vec![fake_path().to_string_lossy().into_owned()],
            cwd: std::env::temp_dir(),
            extra_env: env.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            timeout_override_ms: None,
            retry_base_delay_ms: 10,
        };
        let supervisor = Supervisor::launch(config).await.expect("launch");
        let dispatcher =
            Dispatcher::new(supervisor.clone(), &std::env::temp_dir(), "denyUnmatched")
                .expect("dispatcher");
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

async fn scripted_server(
    scenario: &str,
    test: &str,
    extra: &[(&str, &str)],
) -> (TestServer, PathBuf) {
    let log = scratch(test, "log");
    let mut env: HashMap<&str, String> = HashMap::from([
        ("FAKE_SCENARIO", scenario.to_string()),
        ("FAKE_LOG", log.to_string_lossy().into_owned()),
    ]);
    for (k, v) in extra {
        env.insert(k, v.to_string());
    }
    (TestServer::start(env).await, log)
}

/// Collect an SSE response body to text (watchdogged).
async fn collect_sse(response: reqwest::Response) -> String {
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );
    let mut body = String::new();
    let mut stream = response.bytes_stream();
    use futures::StreamExt as _;
    let collect = async {
        while let Some(chunk) = stream.next().await {
            body.push_str(core::str::from_utf8(&chunk.unwrap()).unwrap());
        }
        body
    };
    tokio::time::timeout(Duration::from_secs(20), collect)
        .await
        .expect("sse completes")
}

/// Split SSE bytes into `(event-name?, data-text)` frames (comments dropped).
fn parse_frames(sse: &str) -> Vec<(Option<String>, String)> {
    sse.split("\n\n")
        .filter(|block| !block.trim().is_empty())
        .map(|block| {
            let mut name = None;
            let mut data = Vec::new();
            for line in block.lines() {
                if let Some(n) = line.strip_prefix("event:") {
                    name = Some(n.trim().to_string());
                } else if let Some(d) = line.strip_prefix("data:") {
                    data.push(d.trim_start().to_string());
                }
            }
            (name, data.join("\n"))
        })
        .collect()
}

fn chat_body(messages: Value, extra: Value) -> Value {
    let mut body = json!({"model": "fake-a", "messages": messages});
    for (k, v) in extra.as_object().unwrap() {
        body[k] = v.clone();
    }
    body
}

#[tokio::test]
async fn chat_nonstream_golden() {
    let (server, _log) = scripted_server("turn-happy", "http-chat-json", &[]).await;
    let response = server
        .client
        .post(format!("{}/v1/chat/completions", server.base))
        .json(&chat_body(
            json!([{"role": "user", "content": "hi"}]),
            json!({}),
        ))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("json");
    assert_eq!(body["object"], "chat.completion");
    assert!(body["id"].as_str().unwrap().starts_with("chatcmpl_"));
    assert_eq!(body["choices"][0]["message"]["role"], "assistant");
    assert_eq!(body["choices"][0]["message"]["content"], "Hello, world");
    assert_eq!(body["choices"][0]["finish_reason"], "stop");
    assert_eq!(body["usage"]["prompt_tokens"], 100);
    assert_eq!(body["usage"]["completion_tokens"], 10);
    assert_eq!(body["usage"]["total_tokens"], 110);
    server.stop().await;
}

#[tokio::test]
async fn chat_stream_sequence_ends_done() {
    let (server, _log) = scripted_server("turn-happy", "http-chat-sse", &[]).await;
    for include_usage in [false, true] {
        let response = server
            .client
            .post(format!("{}/v1/chat/completions", server.base))
            .json(&chat_body(
                json!([{"role": "user", "content": "hi"}]),
                json!({"stream": true, "stream_options": {"include_usage": include_usage}}),
            ))
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), 200);
        let frames = parse_frames(&collect_sse(response).await);
        // role preface, two content deltas, stop chunk, [usage], [DONE].
        let datas: Vec<&str> = frames.iter().map(|(_, d)| d.as_str()).collect();
        assert!(datas[0].contains("\"role\":\"assistant\""), "{datas:?}");
        assert!(datas[1].contains("\"content\":\"Hello\""), "{datas:?}");
        assert!(datas[2].contains("\"content\":\", world\""), "{datas:?}");
        assert!(datas[3].contains("\"finish_reason\":\"stop\""), "{datas:?}");
        if include_usage {
            assert!(datas[4].contains("\"total_tokens\":110"), "{datas:?}");
            assert_eq!(datas[5], "[DONE]");
            assert_eq!(datas.len(), 6);
        } else {
            assert_eq!(datas[4], "[DONE]");
            assert_eq!(datas.len(), 5);
        }
        // Anonymous frames only (chat SSE uses no event names).
        assert!(frames.iter().all(|(name, _)| name.is_none()));
        // One stable id across chunks.
        let id: Value = serde_json::from_str(datas[0]).unwrap();
        for data in &datas[1..datas.len() - 1] {
            if *data == "[DONE]" {
                continue;
            }
            let chunk: Value = serde_json::from_str(data).unwrap();
            assert_eq!(chunk["id"], id["id"]);
        }
    }
    server.stop().await;
}

#[tokio::test]
async fn chat_stream_failure_is_error_frame_not_stop() {
    let (server, _log) = scripted_server("turn-failed", "http-chat-fail", &[]).await;
    let response = server
        .client
        .post(format!("{}/v1/chat/completions", server.base))
        .json(&chat_body(
            json!([{"role": "user", "content": "hi"}]),
            json!({"stream": true}),
        ))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 200);
    let frames = parse_frames(&collect_sse(response).await);
    let datas: Vec<&str> = frames.iter().map(|(_, d)| d.as_str()).collect();
    assert_eq!(datas.len(), 2, "{datas:?}"); // role preface + error frame
    let error: Value = serde_json::from_str(datas[1]).unwrap();
    assert_eq!(error["error"]["code"], "muse_login_required");
    assert!(
        !datas.iter().any(|d| d.contains("\"stop\"")),
        "must not claim stop: {datas:?}"
    );
    assert!(
        !datas.contains(&"[DONE]"),
        "no DONE after failure: {datas:?}"
    );
    // Same failure non-streamed is a 401 JSON.
    let response = server
        .client
        .post(format!("{}/v1/chat/completions", server.base))
        .json(&chat_body(
            json!([{"role": "user", "content": "hi"}]),
            json!({}),
        ))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 401);
    let body: Value = response.json().await.expect("json");
    assert_eq!(body["error"]["code"], "muse_login_required");
    server.stop().await;
}

#[tokio::test]
async fn responses_nonstream_golden() {
    let (server, _log) = scripted_server("turn-happy", "http-resp-json", &[]).await;
    let response = server
        .client
        .post(format!("{}/v1/responses", server.base))
        .json(&json!({"model": "fake-a", "input": "Hello?", "reasoning": {"effort": "low"}}))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("json");
    assert_eq!(body["object"], "response");
    assert!(body["id"].as_str().unwrap().starts_with("resp_"));
    assert_eq!(body["status"], "completed");
    assert_eq!(body["output"][0]["type"], "message");
    assert_eq!(body["output"][0]["content"][0]["type"], "output_text");
    assert_eq!(body["output"][0]["content"][0]["text"], "Hello, world");
    // Responses-API usage names (Codex's ResponseCompletedUsage REQUIRES
    // input_tokens/output_tokens; chat names fail its completed parse).
    assert_eq!(body["usage"]["input_tokens"], 100);
    assert_eq!(body["usage"]["output_tokens"], 10);
    assert_eq!(body["usage"]["total_tokens"], 110);
    assert!(body["usage"].get("prompt_tokens").is_none());
    server.stop().await;
}

#[tokio::test]
async fn responses_stream_codex_vocabulary() {
    let (server, _log) = scripted_server("turn-happy", "http-resp-sse", &[]).await;
    let response = server
        .client
        .post(format!("{}/v1/responses", server.base))
        .json(&json!({"model": "fake-a", "input": "Hello?", "stream": true}))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 200);
    let frames = parse_frames(&collect_sse(response).await);
    let names: Vec<&str> = frames
        .iter()
        .map(|(n, _)| n.as_deref().unwrap_or(""))
        .collect();
    assert_eq!(
        names,
        vec![
            "response.created",
            "response.output_item.added",
            "response.output_text.delta",
            "response.output_text.delta",
            "response.output_item.done",
            "response.completed",
        ],
        "{names:?}"
    );
    // Codex-required fields: created/completed carry response.id; deltas
    // carry delta; completed carries usage.
    let created: Value = serde_json::from_str(&frames[0].1).unwrap();
    let resp_id = created["response"]["id"].as_str().unwrap().to_string();
    assert!(!resp_id.is_empty());
    let delta: Value = serde_json::from_str(&frames[2].1).unwrap();
    assert_eq!(delta["delta"], "Hello");
    let done: Value = serde_json::from_str(&frames[4].1).unwrap();
    assert_eq!(done["item"]["content"][0]["text"], "Hello, world");
    let completed: Value = serde_json::from_str(&frames[5].1).unwrap();
    assert_eq!(completed["response"]["id"], resp_id);
    assert_eq!(completed["response"]["status"], "completed");
    assert_eq!(completed["response"]["usage"]["input_tokens"], 100);
    assert_eq!(completed["response"]["usage"]["output_tokens"], 10);
    assert_eq!(completed["response"]["usage"]["total_tokens"], 110);
    server.stop().await;
}

#[tokio::test]
async fn responses_stream_failure_is_failed_not_completed() {
    let (server, _log) = scripted_server("turn-failed", "http-resp-fail", &[]).await;
    let response = server
        .client
        .post(format!("{}/v1/responses", server.base))
        .json(&json!({"model": "fake-a", "input": "Hello?", "stream": true}))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 200);
    let frames = parse_frames(&collect_sse(response).await);
    let names: Vec<&str> = frames
        .iter()
        .map(|(n, _)| n.as_deref().unwrap_or(""))
        .collect();
    assert_eq!(
        names,
        vec![
            "response.created",
            "response.output_item.added",
            "response.failed"
        ],
        "{names:?}"
    );
    let failed: Value = serde_json::from_str(&frames[2].1).unwrap();
    assert_eq!(failed["response"]["status"], "failed");
    assert_eq!(failed["response"]["error"]["code"], "muse_login_required");
    server.stop().await;
}

#[tokio::test]
async fn models_healthz_routes() {
    let (server, _log) = scripted_server("serve", "http-misc", &[]).await;
    let response = server
        .client
        .get(format!("{}/v1/models", server.base))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("json");
    assert_eq!(body["object"], "list");
    assert_eq!(body["data"][0]["id"], "fake-a");
    assert_eq!(body["data"][0]["object"], "model");

    let response = server
        .client
        .get(format!("{}/healthz", server.base))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("json");
    assert_eq!(body["status"], "ok");
    assert!(
        body["host"]["label"]
            .as_str()
            .unwrap()
            .contains("fake-muse")
    );
    server.stop().await;
}

#[tokio::test]
async fn unknown_routes_methods_and_websocket() {
    let (server, _log) = scripted_server("serve", "http-errors", &[]).await;
    // 404 envelope.
    let response = server
        .client
        .get(format!("{}/v1/nope", server.base))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 404);
    let body: Value = response.json().await.expect("json");
    assert_eq!(body["error"]["code"], "unknown_route");
    // 405 envelope.
    let response = server
        .client
        .get(format!("{}/v1/chat/completions", server.base))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 405);
    // 426 on websocket upgrade (GET carries it simply).
    let response = server
        .client
        .get(format!("{}/v1/models", server.base))
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 426);
    let body: Value = response.json().await.expect("json");
    assert_eq!(body["error"]["code"], "websocket_not_supported");
    server.stop().await;
}

#[tokio::test]
async fn client_shaped_extra_fields_are_tolerated() {
    let (server, _log) = scripted_server("turn-happy", "http-tolerant", &[]).await;
    // Hermes-shaped chat extras.
    let response = server
        .client
        .post(format!("{}/v1/chat/completions", server.base))
        .json(&json!({
            "model": "fake-a",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": false,
            "temperature": 0.7, "top_p": 1.0, "max_tokens": 100,
            "tools": [{"type": "function", "function": {"name": "f"}}],
            "tool_choice": "auto", "thinking": {"type": "enabled"},
            "options": {"num_ctx": 8192}, "prompt_cache_key": "k",
            "stop": ["x"], "frequency_penalty": 0.0, "user": "u",
        }))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 200);
    // Codex-shaped responses extras.
    let response = server
        .client
        .post(format!("{}/v1/responses", server.base))
        .json(&json!({
            "model": "fake-a", "input": "hi", "stream": false,
            "instructions": "sys", "reasoning": {"effort": "low", "summary": "auto"},
            "client_metadata": {}, "prompt_cache_key": "k", "service_tier": "x",
            "text": {}, "include": [], "tools": [], "store": false,
            "previous_response_id": "resp_1", "conversation": "c",
            "temperature": 0.2, "top_p": 0.9, "truncation": "auto",
        }))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 200);
    server.stop().await;
}

#[tokio::test]
async fn caller_errors_are_json_400s() {
    let (server, _log) = scripted_server("turn-happy", "http-400s", &[]).await;
    // Empty prompt.
    let response = server
        .client
        .post(format!("{}/v1/chat/completions", server.base))
        .json(&json!({"model": "fake-a", "messages": []}))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 400);
    let body: Value = response.json().await.expect("json");
    assert_eq!(body["error"]["code"], "empty_prompt");
    // Malformed JSON (axum default would be plain text; we wrap it).
    let response = server
        .client
        .post(format!("{}/v1/chat/completions", server.base))
        .header("content-type", "application/json")
        .body("{oops")
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 400);
    let body: Value = response.json().await.expect("json");
    assert_eq!(body["error"]["code"], "invalid_json");
    server.stop().await;
}

#[tokio::test]
async fn unknown_model_is_400() {
    let (server, _log) = scripted_server(
        "fail:session/setModel:commandRejected:-32030",
        "http-model",
        &[("FAKE_FAIL_REASON", "invalid_model")],
    )
    .await;
    let response = server
        .client
        .post(format!("{}/v1/chat/completions", server.base))
        .json(&chat_body(
            json!([{"role": "user", "content": "hi"}]),
            json!({}),
        ))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 400);
    let body: Value = response.json().await.expect("json");
    assert_eq!(body["error"]["code"], "unknown_model");
    server.stop().await;
}

#[tokio::test]
async fn fingerprint_mismatch_sets_warn_header() {
    let (server, _log) = scripted_server(
        "serve",
        "http-fp",
        &[("FAKE_FINGERPRINT", "sha256:deadbeef")],
    )
    .await;
    let response = server
        .client
        .get(format!("{}/healthz", server.base))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), 200);
    let header = response.headers().get("x-msp-fingerprint-warn");
    assert!(header.is_some(), "warn header must be set on drift");
    let body: Value = response.json().await.expect("json");
    assert_eq!(body["host"]["compat"], "fingerprint_mismatch");
    server.stop().await;
}

/// Regression (found live in P7): the drain budget bounds the post-signal
/// drain only. A healthy server with no signal must keep serving past it.
#[tokio::test]
async fn drain_budget_never_stops_a_healthy_server() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(muse_bridge::http::serve_with_drain(
        listener,
        axum::Router::new().route("/ping", axum::routing::get(|| async { "pong" })),
        Duration::from_millis(100),
    ));
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        !task.is_finished(),
        "server must outlive its drain budget without a signal"
    );
    let body = reqwest::get(format!("http://127.0.0.1:{port}/ping"))
        .await
        .expect("get")
        .text()
        .await
        .expect("body");
    assert_eq!(body, "pong");
    task.abort();
}
