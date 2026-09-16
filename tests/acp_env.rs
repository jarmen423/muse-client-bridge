//! ACP env-isolated tests: process-env behavior in its own binary.
//!
//! `MUSE_APPROVAL_MODE` is read per `session/new`, and process env is
//! process-global — so these cases live alone in this file (a test binary
//! is one process; parallel tests in a shared binary would race the env).
//! Scripted-host only (no login).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use muse_bridge::acp::server::serve;
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
        "muse-bridge-acpenv-{test}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("tmpdir");
    dir
}

/// Startup approval mode: a bogus `MUSE_APPROVAL_MODE` fails `session/new`
/// atomically (no session starts), while either valid vocabulary flows to
/// `session/start`. One test, run alone in this binary — the env is global.
#[tokio::test]
#[allow(unsafe_code, reason = "process-env mutation in a single-test binary")]
async fn acp_startup_approval_mode_env() {
    let log = unique_dir("startupmode").join("fake.log");
    let cwd = unique_dir("startupmodecwd");
    let mut env = HashMap::new();
    env.insert("FAKE_LOG", log.to_string_lossy().into_owned());
    let mut client = AcpClient::connect(env).await;
    client
        .send(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1}}))
        .await;
    let _ = client.recv().await;

    // Bogus values fail loudly before any host traffic.
    // SAFETY: this binary holds a single test, so no parallel test can
    // observe the mutated env; the original value is restored at the end.
    let saved = std::env::var("MUSE_APPROVAL_MODE").ok();
    unsafe {
        std::env::set_var("MUSE_APPROVAL_MODE", "bogus");
    }
    client
        .send(&json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let err = client.recv().await;
    assert_eq!(err["error"]["code"], json!(-32602));
    assert!(
        err["error"]["message"]
            .as_str()
            .unwrap_or("")
            .contains("MUSE_APPROVAL_MODE"),
        "{err}"
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        !log_text.contains("session/start"),
        "nothing starts on a bogus mode: {log_text}"
    );

    // The ACP vocabulary resolves to the host enum on `session/start`.
    // SAFETY: see above.
    unsafe {
        std::env::set_var("MUSE_APPROVAL_MODE", "auto");
    }
    client
        .send(&json!({"jsonrpc": "2.0", "id": 3, "method": "session/new",
            "params": {"cwd": cwd.to_string_lossy()}}))
        .await;
    let frames = client.recv_until(|f| f.get("id") == Some(&json!(3))).await;
    assert!(frames.last().unwrap().get("result").is_some());
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        log_text.contains("mode=allowAll"),
        "auto resolves to allowAll: {log_text}"
    );

    // SAFETY: see above.
    unsafe {
        match saved {
            Some(value) => std::env::set_var("MUSE_APPROVAL_MODE", value),
            None => std::env::remove_var("MUSE_APPROVAL_MODE"),
        }
    }
    client.shutdown().await;
}
