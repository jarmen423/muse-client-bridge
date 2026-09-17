//! ACP wire goldens: exact JSON strings for the session/update surface.
//!
//! Scripted-host only (no login). Dynamic ids (`acp-*`, `msg-*`, MSP
//! session ids) and the crate version normalize to placeholders before the
//! string comparison, so the goldens pin shapes, keys, and vocabulary —
//! not uuids. Regenerate with `DUMP_GOLDENS=1` (prints actuals, always
//! passes), review the diff against `src/acp/`, then freeze.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use muse_bridge::acp::server::{self, serve};
use muse_bridge::acp::sessions::{
    config_options, is_reasoning_effort, mode_from_msp, mode_to_msp, resolve_mode, session_modes,
};
use muse_bridge::dispatch::Dispatcher;
use muse_bridge::msp::host::{HostConfig, Supervisor};
use muse_bridge::msp::proto::{next_frame, write_frame};
use serde_json::{Value, json};
use tokio::io::BufReader;

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

struct AcpClient {
    tx: tokio::io::DuplexStream,
    rx: BufReader<tokio::io::DuplexStream>,
    server: tokio::task::JoinHandle<()>,
    supervisor: Arc<Supervisor>,
}

impl AcpClient {
    async fn connect(env: HashMap<&str, String>) -> Self {
        let supervisor = Supervisor::launch(test_config(env))
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
        tokio::time::timeout(Duration::from_secs(10), self.server)
            .await
            .expect("server shutdown timed out")
            .expect("server task");
        self.supervisor.shutdown().await;
    }
}

fn unique_dir(test: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "muse-bridge-golden-{test}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("tmpdir");
    dir
}

/// Normalize dynamic ids + crate version so goldens pin shape, not uuids.
fn normalize(value: Value) -> Value {
    match value {
        Value::String(s) => {
            if s.starts_with("acp-") {
                Value::String("<acp-sid>".to_string())
            } else if s.starts_with("msg-") {
                Value::String("<msg-id>".to_string())
            } else if s.starts_with("fake-sess-") {
                Value::String("<msp-sid>".to_string())
            } else {
                Value::String(s)
            }
        }
        Value::Array(items) => Value::Array(items.into_iter().map(normalize).collect()),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| {
                    if k == "version" && v.is_string() {
                        (k, Value::String("<version>".to_string()))
                    } else {
                        (k, normalize(v))
                    }
                })
                .collect(),
        ),
        other => other,
    }
}

/// Compare a normalized value's pretty string against a frozen golden. With
/// `DUMP_GOLDENS=1`, print the actual instead (review against `src/acp/`,
/// then freeze).
fn check_golden(name: &str, actual: &Value, expected: &str) {
    let actual_str = serde_json::to_string_pretty(&normalize(actual.clone())).expect("pretty");
    if std::env::var("DUMP_GOLDENS").as_deref() == Ok("1") {
        println!("--- GOLDEN {name} ---\n{actual_str}\n--- END {name} ---");
        return;
    }
    assert_eq!(actual_str, expected.trim(), "golden {name} drifted");
}

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
    // The advertisement trails the result; consume it so prompt-phase
    // windows start clean.
    let update = client.recv().await;
    assert_eq!(
        update["params"]["update"]["sessionUpdate"],
        json!("available_commands_update"),
        "advertisement trails the result: {update}"
    );
    sid
}

#[tokio::test]
async fn golden_initialize_v1() {
    let mut client = AcpClient::connect(HashMap::new()).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1, "clientCapabilities": {}}}))
        .await;
    let init = client.recv().await;
    assert_eq!(
        init["result"]["agentInfo"]["version"],
        json!(env!("CARGO_PKG_VERSION"))
    );
    check_golden("initialize_v1", &init, GOLDEN_INITIALIZE_V1);
    client.shutdown().await;
}

#[tokio::test]
async fn golden_initialize_v2() {
    let mut client = AcpClient::connect(HashMap::new()).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 2, "capabilities": {}}}))
        .await;
    let init = client.recv().await;
    assert_eq!(
        init["result"]["info"]["version"],
        json!(env!("CARGO_PKG_VERSION"))
    );
    check_golden("initialize_v2", &init, GOLDEN_INITIALIZE_V2);
    client.shutdown().await;
}

#[tokio::test]
async fn golden_session_new_v1() {
    let mut client = AcpClient::connect(HashMap::new()).await;
    let cwd = unique_dir("newv1");
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
    check_golden(
        "session_new_v1",
        &Value::Array(frames),
        GOLDEN_SESSION_NEW_V1,
    );
    client.shutdown().await;
}

#[tokio::test]
async fn golden_session_new_v2() {
    let mut client = AcpClient::connect(HashMap::new()).await;
    let cwd = unique_dir("newv2");
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
    check_golden(
        "session_new_v2",
        &Value::Array(frames),
        GOLDEN_SESSION_NEW_V2,
    );
    client.shutdown().await;
}

#[tokio::test]
async fn golden_v1_prompt_frames() {
    let mut client =
        AcpClient::connect(HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())])).await;
    let cwd = unique_dir("promptv1");
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid,
                "prompt": [{"type": "text", "text": "say hi"}]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    check_golden(
        "v1_prompt_frames",
        &Value::Array(frames),
        GOLDEN_V1_PROMPT_FRAMES,
    );
    client.shutdown().await;
}

#[tokio::test]
async fn golden_v2_prompt_frames() {
    let mut client =
        AcpClient::connect(HashMap::from([("FAKE_SCENARIO", "turn-happy".to_string())])).await;
    let cwd = unique_dir("promptv2");
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
    let update = client.recv().await;
    assert_eq!(
        update["params"]["update"]["sessionUpdate"],
        json!("available_commands_update"),
        "advertisement trails the result: {update}"
    );
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["say hi"]}}),
        )
        .await;
    let frames = client
        .recv_until(|f| {
            f.get("method") == Some(&json!("session/update"))
                && f["params"]["update"]["sessionUpdate"] == json!("state_update")
                && f["params"]["update"]["state"] == json!("idle")
        })
        .await;
    check_golden(
        "v2_prompt_frames",
        &Value::Array(frames),
        GOLDEN_V2_PROMPT_FRAMES,
    );
    client.shutdown().await;
}

#[tokio::test]
async fn golden_thought_chunk_v1() {
    let mut client = AcpClient::connect(HashMap::from([(
        "FAKE_SCENARIO",
        "turn-reasoning-quiet".to_string(),
    )]))
    .await;
    let cwd = unique_dir("thoughtgold");
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid, "prompt": ["think"]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    let thoughts: Vec<Value> = frames
        .into_iter()
        .filter(|f| {
            f.get("method") == Some(&json!("session/update"))
                && f["params"]["update"]["sessionUpdate"] == json!("agent_thought_chunk")
        })
        .collect();
    assert_eq!(thoughts.len(), 1);
    check_golden("thought_chunk_v1", &thoughts[0], GOLDEN_THOUGHT_CHUNK_V1);
    client.shutdown().await;
}

#[tokio::test]
async fn golden_tool_call_replay() {
    let mut env = HashMap::from([("FAKE_SCENARIO", "serve".to_string())]);
    env.insert("FAKE_HISTORY", "1".to_string());
    env.insert("FAKE_HISTORY_TOOL", "1".to_string());
    let mut client = AcpClient::connect(env).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/load",
            "params": {"sessionId": "fake-sess-1"}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(2))).await;
    let updates: Vec<&str> = frames
        .iter()
        .filter_map(|f| f["params"]["update"]["sessionUpdate"].as_str())
        .collect();
    assert_eq!(
        updates,
        vec!["user_message_chunk", "agent_message_chunk", "tool_call",],
        "replay precedes the result: {frames:?}"
    );
    // The advertisement trails the result instead.
    let update = client.recv().await;
    assert_eq!(
        update["params"]["update"]["sessionUpdate"],
        json!("available_commands_update"),
        "advertisement trails the result: {update}"
    );
    let tool = frames
        .iter()
        .find(|f| f["params"]["update"]["sessionUpdate"] == json!("tool_call"))
        .expect("tool_call replay");
    check_golden("tool_call_replay", tool, GOLDEN_TOOL_CALL_REPLAY);
    client.shutdown().await;
}

#[tokio::test]
async fn golden_compact_v1() {
    let mut client =
        AcpClient::connect(HashMap::from([("FAKE_SCENARIO", "serve".to_string())])).await;
    let cwd = unique_dir("compactgold");
    let acp_sid = v1_session(&mut client, &cwd).await;
    client
        .send(
            &json!({"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": acp_sid,
                "prompt": [{"type": "text", "text": "  /compact  "}]}}),
        )
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    check_golden("compact_v1", &Value::Array(frames), GOLDEN_COMPACT_V1);
    client.shutdown().await;
}

#[test]
fn golden_config_options_and_modes() {
    let models = vec![("fake-a".to_string(), "fake-a".to_string())];
    let v1 = config_options(1, "deny", "fake-model", "medium", &models);
    check_golden("config_options_v1", &v1, GOLDEN_CONFIG_OPTIONS_V1);
    let v2 = config_options(2, "ask", "fake-model", "ultra", &models);
    check_golden("config_options_v2", &v2, GOLDEN_CONFIG_OPTIONS_V2);
    check_golden("session_modes", &session_modes("ask"), GOLDEN_SESSION_MODES);
}

#[test]
fn golden_rpc_frames_and_versions() {
    check_golden(
        "result_frame",
        &server::result_frame(&json!(7), json!({"stopReason": "end_turn"})),
        GOLDEN_RESULT_FRAME,
    );
    check_golden(
        "error_frame",
        &server::error_frame(&json!(7), -32602, "unknown sessionId"),
        GOLDEN_ERROR_FRAME,
    );
    assert_eq!(server::negotiate_version(None), 1);
    assert_eq!(server::negotiate_version(Some(&json!(1))), 1);
    assert_eq!(server::negotiate_version(Some(&json!(2))), 2);
    assert_eq!(server::negotiate_version(Some(&json!(99))), 2);
    assert_eq!(server::negotiate_version(Some(&json!("x"))), 1);
}

#[test]
fn golden_mode_and_effort_vocab() {
    // Mode vocabulary in both directions (the P3 reference port).
    assert_eq!(mode_to_msp("ask"), Some("promptUnmatched"));
    assert_eq!(mode_to_msp("auto"), Some("allowAll"));
    assert_eq!(mode_to_msp("deny"), Some("denyUnmatched"));
    assert_eq!(mode_to_msp("turbo"), None);
    assert_eq!(mode_from_msp("allowAll"), "auto");
    assert_eq!(mode_from_msp("denyUnmatched"), "deny");
    assert_eq!(mode_from_msp("promptUnmatched"), "ask");
    assert_eq!(mode_from_msp("futureMode"), "ask");
    assert_eq!(resolve_mode("ask"), Some("promptUnmatched"));
    assert_eq!(resolve_mode("promptUnmatched"), Some("promptUnmatched"));
    assert_eq!(resolve_mode("onRequest"), Some("onRequest"));
    assert_eq!(resolve_mode("bogus"), None);
    // 8-tier effort incl. `max` (binary wins over the reference's 7).
    for tier in [
        "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
    ] {
        assert!(is_reasoning_effort(tier), "{tier}");
    }
    assert!(!is_reasoning_effort("extreme"));
    assert!(!is_reasoning_effort(""));
}

// --- Frozen goldens (regenerate with DUMP_GOLDENS=1, review, freeze) ---

const GOLDEN_INITIALIZE_V1: &str = r#"
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": {
    "agentCapabilities": {
      "loadSession": true,
      "mcpCapabilities": {
        "http": false,
        "sse": false
      },
      "promptCapabilities": {
        "audio": false,
        "embeddedContext": true,
        "image": true,
        "text": true
      },
      "sessionCapabilities": {
        "close": {},
        "fork": {},
        "list": {},
        "resume": {}
      }
    },
    "agentInfo": {
      "name": "muse-acp-bridge",
      "title": "Muse ACP Bridge",
      "version": "<version>"
    },
    "protocolVersion": 1
  }
}
"#;
const GOLDEN_INITIALIZE_V2: &str = r#"
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": {
    "_meta": {
      "steering": {
        "supported": true
      }
    },
    "authMethods": [],
    "capabilities": {
      "session": {
        "fork": {},
        "prompt": {
          "embeddedContext": {},
          "image": {}
        }
      }
    },
    "info": {
      "name": "muse-acp-bridge",
      "title": "Muse ACP Bridge",
      "version": "<version>"
    },
    "protocolVersion": 2
  }
}
"#;
const GOLDEN_SESSION_NEW_V1: &str = r#"[
  {
    "id": 2,
    "jsonrpc": "2.0",
    "result": {
      "_meta": {
        "mspSessionId": "<msp-sid>"
      },
      "configOptions": [
        {
          "category": "mode",
          "currentValue": "deny",
          "description": "How the agent handles tool approvals",
          "id": "mode",
          "name": "Session Mode",
          "options": [
            {
              "description": "Request permission for unmatched tools",
              "name": "Ask",
              "value": "ask"
            },
            {
              "description": "Allow all tools without asking",
              "name": "Auto",
              "value": "auto"
            },
            {
              "description": "Allow all tools and skip questions",
              "name": "Yolo",
              "value": "yolo"
            },
            {
              "description": "Deny unmatched tools",
              "name": "Deny",
              "value": "deny"
            }
          ],
          "type": "select"
        },
        {
          "category": "model",
          "currentValue": "fake-model",
          "id": "model",
          "name": "Model",
          "options": [
            {
              "name": "fake-a",
              "value": "fake-a"
            }
          ],
          "type": "select"
        },
        {
          "category": "thought_level",
          "currentValue": "medium",
          "description": "Reasoning effort sent with each prompt and steering message",
          "id": "reasoning_effort",
          "name": "Reasoning Effort",
          "options": [
            {
              "name": "None",
              "value": "none"
            },
            {
              "name": "Minimal",
              "value": "minimal"
            },
            {
              "name": "Low",
              "value": "low"
            },
            {
              "name": "Medium",
              "value": "medium"
            },
            {
              "name": "High",
              "value": "high"
            },
            {
              "name": "Extra High",
              "value": "xhigh"
            },
            {
              "name": "Max",
              "value": "max"
            },
            {
              "name": "Ultra",
              "value": "ultra"
            }
          ],
          "type": "select"
        }
      ],
      "modes": {
        "availableModes": [
          {
            "description": "Request permission for unmatched tools",
            "id": "ask",
            "name": "Ask"
          },
          {
            "description": "Allow all tools without asking",
            "id": "auto",
            "name": "Auto"
          },
          {
            "description": "Allow all tools and skip questions",
            "id": "yolo",
            "name": "Yolo"
          },
          {
            "description": "Deny unmatched tools",
            "id": "deny",
            "name": "Deny"
          }
        ],
        "currentModeId": "deny"
      },
      "sessionId": "<acp-sid>"
    }
  }
]"#;
const GOLDEN_SESSION_NEW_V2: &str = r#"[
  {
    "id": 2,
    "jsonrpc": "2.0",
    "result": {
      "_meta": {
        "mspSessionId": "<msp-sid>"
      },
      "configOptions": [
        {
          "category": "mode",
          "configId": "mode",
          "currentValue": "deny",
          "description": "How the agent handles tool approvals",
          "name": "Session Mode",
          "options": [
            {
              "description": "Request permission for unmatched tools",
              "name": "Ask",
              "value": "ask"
            },
            {
              "description": "Allow all tools without asking",
              "name": "Auto",
              "value": "auto"
            },
            {
              "description": "Allow all tools and skip questions",
              "name": "Yolo",
              "value": "yolo"
            },
            {
              "description": "Deny unmatched tools",
              "name": "Deny",
              "value": "deny"
            }
          ],
          "type": "select"
        },
        {
          "category": "model",
          "configId": "model",
          "currentValue": "fake-model",
          "name": "Model",
          "options": [
            {
              "name": "fake-a",
              "value": "fake-a"
            }
          ],
          "type": "select"
        },
        {
          "category": "thought_level",
          "configId": "reasoning_effort",
          "currentValue": "medium",
          "description": "Reasoning effort sent with each prompt and steering message",
          "name": "Reasoning Effort",
          "options": [
            {
              "name": "None",
              "value": "none"
            },
            {
              "name": "Minimal",
              "value": "minimal"
            },
            {
              "name": "Low",
              "value": "low"
            },
            {
              "name": "Medium",
              "value": "medium"
            },
            {
              "name": "High",
              "value": "high"
            },
            {
              "name": "Extra High",
              "value": "xhigh"
            },
            {
              "name": "Max",
              "value": "max"
            },
            {
              "name": "Ultra",
              "value": "ultra"
            }
          ],
          "type": "select"
        }
      ],
      "sessionId": "<acp-sid>"
    }
  }
]"#;
const GOLDEN_V1_PROMPT_FRAMES: &str = r#"
[
  {
    "jsonrpc": "2.0",
    "method": "session/update",
    "params": {
      "sessionId": "<acp-sid>",
      "update": {
        "content": {
          "text": "say hi",
          "type": "text"
        },
        "messageId": "<msg-id>",
        "sessionUpdate": "user_message_chunk"
      }
    }
  },
  {
    "jsonrpc": "2.0",
    "method": "session/update",
    "params": {
      "sessionId": "<acp-sid>",
      "update": {
        "content": {
          "text": "Hello",
          "type": "text"
        },
        "sessionUpdate": "agent_message_chunk"
      }
    }
  },
  {
    "jsonrpc": "2.0",
    "method": "session/update",
    "params": {
      "sessionId": "<acp-sid>",
      "update": {
        "content": {
          "text": ", world",
          "type": "text"
        },
        "sessionUpdate": "agent_message_chunk"
      }
    }
  },
  {
    "id": 3,
    "jsonrpc": "2.0",
    "result": {
      "stopReason": "end_turn"
    }
  }
]
"#;
const GOLDEN_V2_PROMPT_FRAMES: &str = r#"
[
  {
    "id": 3,
    "jsonrpc": "2.0",
    "result": {}
  },
  {
    "jsonrpc": "2.0",
    "method": "session/update",
    "params": {
      "sessionId": "<acp-sid>",
      "update": {
        "content": [
          {
            "text": "say hi",
            "type": "text"
          }
        ],
        "messageId": "<msg-id>",
        "sessionUpdate": "user_message"
      }
    }
  },
  {
    "jsonrpc": "2.0",
    "method": "session/update",
    "params": {
      "sessionId": "<acp-sid>",
      "update": {
        "sessionUpdate": "state_update",
        "state": "running"
      }
    }
  },
  {
    "jsonrpc": "2.0",
    "method": "session/update",
    "params": {
      "sessionId": "<acp-sid>",
      "update": {
        "content": {
          "text": "Hello",
          "type": "text"
        },
        "messageId": "<msg-id>",
        "sessionUpdate": "agent_message_chunk"
      }
    }
  },
  {
    "jsonrpc": "2.0",
    "method": "session/update",
    "params": {
      "sessionId": "<acp-sid>",
      "update": {
        "content": {
          "text": ", world",
          "type": "text"
        },
        "messageId": "<msg-id>",
        "sessionUpdate": "agent_message_chunk"
      }
    }
  },
  {
    "jsonrpc": "2.0",
    "method": "session/update",
    "params": {
      "sessionId": "<acp-sid>",
      "update": {
        "sessionUpdate": "state_update",
        "state": "idle",
        "stopReason": "end_turn"
      }
    }
  }
]
"#;
const GOLDEN_THOUGHT_CHUNK_V1: &str = r#"
{
  "jsonrpc": "2.0",
  "method": "session/update",
  "params": {
    "sessionId": "<acp-sid>",
    "update": {
      "content": {
        "text": "Committed thought",
        "type": "text"
      },
      "sessionUpdate": "agent_thought_chunk"
    }
  }
}
"#;
const GOLDEN_TOOL_CALL_REPLAY: &str = r#"
{
  "jsonrpc": "2.0",
  "method": "session/update",
  "params": {
    "sessionId": "<msp-sid>",
    "update": {
      "content": [],
      "kind": "read",
      "sessionUpdate": "tool_call",
      "status": "completed",
      "title": "read /tmp/h",
      "toolCallId": "h-tool-1"
    }
  }
}
"#;
const GOLDEN_COMPACT_V1: &str = r#"[
  {
    "jsonrpc": "2.0",
    "method": "session/update",
    "params": {
      "sessionId": "<acp-sid>",
      "update": {
        "content": {
          "text": "/compact",
          "type": "text"
        },
        "messageId": "<msg-id>",
        "sessionUpdate": "user_message_chunk"
      }
    }
  },
  {
    "jsonrpc": "2.0",
    "method": "session/update",
    "params": {
      "sessionId": "<acp-sid>",
      "update": {
        "content": {
          "text": "Compacting the session context.",
          "type": "text"
        },
        "sessionUpdate": "agent_message_chunk"
      }
    }
  },
  {
    "id": 3,
    "jsonrpc": "2.0",
    "result": {
      "stopReason": "end_turn"
    }
  }
]"#;
const GOLDEN_CONFIG_OPTIONS_V1: &str = r#"
[
  {
    "category": "mode",
    "currentValue": "deny",
    "description": "How the agent handles tool approvals",
    "id": "mode",
    "name": "Session Mode",
    "options": [
      {
        "description": "Request permission for unmatched tools",
        "name": "Ask",
        "value": "ask"
      },
      {
        "description": "Allow all tools without asking",
        "name": "Auto",
        "value": "auto"
      },
      {
        "description": "Allow all tools and skip questions",
        "name": "Yolo",
        "value": "yolo"
      },
      {
        "description": "Deny unmatched tools",
        "name": "Deny",
        "value": "deny"
      }
    ],
    "type": "select"
  },
  {
    "category": "model",
    "currentValue": "fake-model",
    "id": "model",
    "name": "Model",
    "options": [
      {
        "name": "fake-a",
        "value": "fake-a"
      }
    ],
    "type": "select"
  },
  {
    "category": "thought_level",
    "currentValue": "medium",
    "description": "Reasoning effort sent with each prompt and steering message",
    "id": "reasoning_effort",
    "name": "Reasoning Effort",
    "options": [
      {
        "name": "None",
        "value": "none"
      },
      {
        "name": "Minimal",
        "value": "minimal"
      },
      {
        "name": "Low",
        "value": "low"
      },
      {
        "name": "Medium",
        "value": "medium"
      },
      {
        "name": "High",
        "value": "high"
      },
      {
        "name": "Extra High",
        "value": "xhigh"
      },
      {
        "name": "Max",
        "value": "max"
      },
      {
        "name": "Ultra",
        "value": "ultra"
      }
    ],
    "type": "select"
  }
]
"#;
const GOLDEN_CONFIG_OPTIONS_V2: &str = r#"
[
  {
    "category": "mode",
    "configId": "mode",
    "currentValue": "ask",
    "description": "How the agent handles tool approvals",
    "name": "Session Mode",
    "options": [
      {
        "description": "Request permission for unmatched tools",
        "name": "Ask",
        "value": "ask"
      },
      {
        "description": "Allow all tools without asking",
        "name": "Auto",
        "value": "auto"
      },
      {
        "description": "Allow all tools and skip questions",
        "name": "Yolo",
        "value": "yolo"
      },
      {
        "description": "Deny unmatched tools",
        "name": "Deny",
        "value": "deny"
      }
    ],
    "type": "select"
  },
  {
    "category": "model",
    "configId": "model",
    "currentValue": "fake-model",
    "name": "Model",
    "options": [
      {
        "name": "fake-a",
        "value": "fake-a"
      }
    ],
    "type": "select"
  },
  {
    "category": "thought_level",
    "configId": "reasoning_effort",
    "currentValue": "ultra",
    "description": "Reasoning effort sent with each prompt and steering message",
    "name": "Reasoning Effort",
    "options": [
      {
        "name": "None",
        "value": "none"
      },
      {
        "name": "Minimal",
        "value": "minimal"
      },
      {
        "name": "Low",
        "value": "low"
      },
      {
        "name": "Medium",
        "value": "medium"
      },
      {
        "name": "High",
        "value": "high"
      },
      {
        "name": "Extra High",
        "value": "xhigh"
      },
      {
        "name": "Max",
        "value": "max"
      },
      {
        "name": "Ultra",
        "value": "ultra"
      }
    ],
    "type": "select"
  }
]
"#;
const GOLDEN_SESSION_MODES: &str = r#"
{
  "availableModes": [
    {
      "description": "Request permission for unmatched tools",
      "id": "ask",
      "name": "Ask"
    },
    {
      "description": "Allow all tools without asking",
      "id": "auto",
      "name": "Auto"
    },
    {
      "description": "Allow all tools and skip questions",
      "id": "yolo",
      "name": "Yolo"
    },
    {
      "description": "Deny unmatched tools",
      "id": "deny",
      "name": "Deny"
    }
  ],
  "currentModeId": "ask"
}
"#;
const GOLDEN_RESULT_FRAME: &str = r#"
{
  "id": 7,
  "jsonrpc": "2.0",
  "result": {
    "stopReason": "end_turn"
  }
}
"#;
const GOLDEN_ERROR_FRAME: &str = r#"
{
  "error": {
    "code": -32602,
    "message": "unknown sessionId"
  },
  "id": 7,
  "jsonrpc": "2.0"
}
"#;
