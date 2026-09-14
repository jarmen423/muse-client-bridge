//! Scripted-host tests: the bridge against `fake_serve.py` (HANDOFF P2).
//!
//! No login needed. Each test spawns the fake with per-test env knobs (via
//! the child env, never process env) and unique files, so tests run in
//! parallel.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use muse_bridge::msp::host::{
    CompatStatus, ExhaustReason, HostConfig, LaunchError, MspConnection, Supervisor,
    SupervisorError, SupervisorStatus, launch,
};
use muse_bridge::msp::spawn::ExitClass;
use serde_json::json;

fn fake_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake_serve.py")
}

/// Unique scratch file per test (parallel-safe).
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

fn read_lines(path: &PathBuf) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

async fn wait_for_status(
    supervisor: &Supervisor,
    want: &SupervisorStatus,
    timeout: Duration,
) -> SupervisorStatus {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let status = supervisor.status().await;
        if &status == want || tokio::time::Instant::now() >= deadline {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn handshake_ok_records_host_facts() {
    let log = scratch("handshake-ok", "log");
    let config = test_config(HashMap::from([(
        "FAKE_LOG",
        log.to_string_lossy().into_owned(),
    )]));
    let spawned = launch(&config).await.expect("handshake must succeed");
    let info = spawned.conn.handshake_info().expect("facts recorded");
    assert_eq!(info.server_name, "fake-muse");
    assert_eq!(info.server_version, "9.9-test");
    assert_eq!(info.schema_version, Some(1));
    assert_eq!(info.compat, CompatStatus::Tested);
    assert_eq!(info.durability.as_deref(), Some("durable"));
    assert!(spawned.conn.is_alive());
    // `initialized` was sent (fake logs every inbound method). The notify is
    // flushed before `handshake()` returns, but the fake logs on receipt, so
    // poll briefly rather than racing it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let lines = loop {
        let lines = read_lines(&log);
        if lines.iter().any(|l| l.starts_with("initialized ")) {
            break lines;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "fake never logged initialized: {lines:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(
        lines.iter().any(|l| l.starts_with("initialize ")),
        "{lines:?}"
    );
    spawned.child.kill_and_reap().await;
}

#[tokio::test]
async fn schema_version_other_than_one_is_fatal() {
    let config = test_config(HashMap::from([("FAKE_SCHEMA_VERSION", "2".to_string())]));
    match launch(&config).await {
        Err(LaunchError::IncompatibleSchema { version, .. }) => {
            assert_eq!(version, Some(2));
        }
        Ok(_) => panic!("expected IncompatibleSchema, launch succeeded"),
        Err(other) => panic!("expected IncompatibleSchema, got {other}"),
    }
}

#[tokio::test]
async fn fingerprint_mismatch_warns_and_continues() {
    let config = test_config(HashMap::from([(
        "FAKE_FINGERPRINT",
        "sha256:deadbeef".to_string(),
    )]));
    let spawned = launch(&config).await.expect("drift must not fail");
    let info = spawned.conn.handshake_info().expect("facts");
    assert_eq!(info.compat, CompatStatus::FingerprintMismatch);
    assert_eq!(info.fingerprint.as_deref(), Some("sha256:deadbeef"));
    spawned.child.kill_and_reap().await;
}

#[tokio::test]
async fn pre_handshake_commands_are_rejected_locally() {
    let spawned = MspConnection::spawn(&test_config(HashMap::new()))
        .await
        .expect("spawn");
    let err = spawned
        .conn
        .command("session/start", &json!({"commandId": "c1"}))
        .await
        .expect_err("pre-handshake command must fail");
    assert_eq!(err.code, -32600);
    assert_eq!(err.kind(), Some("notInitialized"));
    // After the handshake the same connection serves.
    spawned.conn.handshake().await.expect("handshake");
    let result = spawned
        .conn
        .command("session/start", &json!({"commandId": "c1"}))
        .await
        .expect("post-handshake command");
    assert_eq!(result["session"]["sessionId"], "fake-sess-1");
    spawned.child.kill_and_reap().await;
}

#[tokio::test]
async fn silent_host_synthesizes_a_local_timeout() {
    let mut config = test_config(HashMap::from([(
        "FAKE_SCENARIO",
        "silent:model/list".to_string(),
    )]));
    config.timeout_override_ms = Some(200);
    let spawned = launch(&config).await.expect("handshake");
    let err = spawned
        .conn
        .command("model/list", &json!({}))
        .await
        .expect_err("silent host must time out");
    assert_eq!(err.code, -32603);
    assert_eq!(err.kind(), Some("timeout"));
    assert!(err.message.contains("model/list"), "{}", err.message);
    assert!(err.message.contains("200ms"), "{}", err.message);
    spawned.child.kill_and_reap().await;
}

#[tokio::test]
async fn overloaded_retries_reuse_the_same_command_id() {
    let log = scratch("retry-same-id", "log");
    let config = test_config(HashMap::from([
        ("FAKE_SCENARIO", "overloaded:session/start:2".to_string()),
        ("FAKE_LOG", log.to_string_lossy().into_owned()),
    ]));
    let spawned = launch(&config).await.expect("handshake");
    let command_id = spawned.conn.mint_command_id();
    let params = json!({"commandId": command_id, "workspaceRoot": "/tmp/x"});
    let result = spawned
        .conn
        .command_with_retry("session/start", &params)
        .await
        .expect("third attempt succeeds");
    assert_eq!(result["session"]["sessionId"], "fake-sess-1");
    // The fake transcript proves same-id retries: 3 sends, 1 id.
    let starts: Vec<String> = read_lines(&log)
        .into_iter()
        .filter(|l| l.starts_with("session/start "))
        .collect();
    assert_eq!(starts.len(), 3, "must send exactly 3 attempts");
    for line in &starts {
        assert!(
            line.starts_with(&format!("session/start cmd={command_id} ")),
            "{starts:?}"
        );
    }
    spawned.child.kill_and_reap().await;
}

#[tokio::test]
async fn input_too_large_never_retries() {
    let log = scratch("no-retry", "log");
    let config = test_config(HashMap::from([
        (
            "FAKE_SCENARIO",
            "fail:session/start:inputTooLarge:-32002".to_string(),
        ),
        ("FAKE_LOG", log.to_string_lossy().into_owned()),
    ]));
    let spawned = launch(&config).await.expect("handshake");
    let params = json!({"commandId": spawned.conn.mint_command_id()});
    let err = spawned
        .conn
        .command_with_retry("session/start", &params)
        .await
        .expect_err("must fail");
    assert_eq!(err.kind(), Some("inputTooLarge"));
    let starts = read_lines(&log)
        .into_iter()
        .filter(|l| l.starts_with("session/start "))
        .count();
    assert_eq!(starts, 1, "no blind retry on inputTooLarge");
    spawned.child.kill_and_reap().await;
}

#[tokio::test]
async fn same_command_id_with_different_payload_is_refused() {
    let log = scratch("id-conflict", "log");
    let config = test_config(HashMap::from([(
        "FAKE_LOG",
        log.to_string_lossy().into_owned(),
    )]));
    let spawned = launch(&config).await.expect("handshake");
    let params = json!({"commandId": "fixed-id", "workspaceRoot": "/tmp/a"});
    spawned
        .conn
        .command("session/start", &params)
        .await
        .expect("first send");
    let different = json!({"commandId": "fixed-id", "workspaceRoot": "/tmp/b"});
    let err = spawned
        .conn
        .command("session/start", &different)
        .await
        .expect_err("conflict must be refused");
    assert_eq!(err.kind(), Some("commandIdConflict"));
    // The conflicting send never reached the host.
    let starts = read_lines(&log)
        .into_iter()
        .filter(|l| l.starts_with("session/start "))
        .count();
    assert_eq!(starts, 1);
    spawned.child.kill_and_reap().await;
}

#[tokio::test]
async fn unknown_server_method_gets_method_not_found() {
    let verdict = scratch("probe-unknown", "verdict");
    let config = test_config(HashMap::from([
        ("FAKE_SCENARIO", "probe-unknown".to_string()),
        ("FAKE_VERDICT", verdict.to_string_lossy().into_owned()),
    ]));
    let spawned = launch(&config).await.expect("handshake");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(text) = std::fs::read_to_string(&verdict) {
            assert_eq!(text.trim(), "OK", "fake verdict: {text}");
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the fake's verdict"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    spawned.child.kill_and_reap().await;
}

#[tokio::test]
async fn garbage_before_handshake_does_not_break_the_connection() {
    let config = test_config(HashMap::from([("FAKE_SCENARIO", "garbage".to_string())]));
    let spawned = launch(&config).await.expect("handshake survives garbage");
    let models = spawned
        .conn
        .command("model/list", &json!({}))
        .await
        .expect("commands flow after garbage");
    assert_eq!(models["models"][0]["modelId"], "fake-a");
    spawned.child.kill_and_reap().await;
}

#[tokio::test]
async fn durable_host_death_restarts_three_times_then_exhausts() {
    let launches = scratch("restart-budget", "log");
    let config = test_config(HashMap::from([
        ("FAKE_SCENARIO", "die:1".to_string()),
        ("FAKE_LAUNCH_LOG", launches.to_string_lossy().into_owned()),
    ]));
    let supervisor = Supervisor::launch(config).await.expect("initial launch");
    let status = wait_for_status(
        &supervisor,
        &SupervisorStatus::Exhausted {
            reason: ExhaustReason::BudgetSpent {
                last_exit: Some(ExitClass::Unhandled),
            },
        },
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(
        status,
        SupervisorStatus::Exhausted {
            reason: ExhaustReason::BudgetSpent {
                last_exit: Some(ExitClass::Unhandled),
            }
        }
    );
    // 1 initial + 3 restarts, then the budget is spent.
    assert_eq!(read_lines(&launches).len(), 4);
    assert!(matches!(
        supervisor.ready().await,
        Err(SupervisorError::Exhausted(_))
    ));
    supervisor.shutdown().await;
}

#[tokio::test]
async fn ephemeral_host_death_fails_closed_without_restart() {
    let launches = scratch("ephemeral", "log");
    let config = test_config(HashMap::from([
        ("FAKE_SCENARIO", "die:1".to_string()),
        ("FAKE_DURABILITY", "ephemeral".to_string()),
        ("FAKE_LAUNCH_LOG", launches.to_string_lossy().into_owned()),
    ]));
    let supervisor = Supervisor::launch(config).await.expect("initial launch");
    let status = wait_for_status(
        &supervisor,
        &SupervisorStatus::Exhausted {
            reason: ExhaustReason::EphemeralHost,
        },
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(
        status,
        SupervisorStatus::Exhausted {
            reason: ExhaustReason::EphemeralHost
        }
    );
    assert_eq!(read_lines(&launches).len(), 1, "no restart on ephemeral");
    supervisor.shutdown().await;
}

#[tokio::test]
async fn config_exit_never_restarts_and_flags_401() {
    let launches = scratch("exit3", "log");
    let config = test_config(HashMap::from([
        ("FAKE_SCENARIO", "die:3".to_string()),
        ("FAKE_LAUNCH_LOG", launches.to_string_lossy().into_owned()),
    ]));
    let supervisor = Supervisor::launch(config).await.expect("initial launch");
    let status = wait_for_status(
        &supervisor,
        &SupervisorStatus::Exhausted {
            reason: ExhaustReason::NonRestartableExit {
                exit: ExitClass::Config,
            },
        },
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(
        status,
        SupervisorStatus::Exhausted {
            reason: ExhaustReason::NonRestartableExit {
                exit: ExitClass::Config
            }
        }
    );
    assert_eq!(read_lines(&launches).len(), 1, "exit 3 never restarts");
    match supervisor.ready().await {
        Err(e) => assert!(e.is_config_exit(), "{e:?}"),
        Ok(_) => panic!("ready() must fail once exhausted"),
    }
    supervisor.shutdown().await;
}

#[tokio::test]
async fn missing_binary_names_the_remedy() {
    let mut config = test_config(HashMap::new());
    config.bin = "/nonexistent/muse-xyz".to_string();
    match launch(&config).await {
        Err(LaunchError::Spawn(msg)) => {
            assert!(msg.contains("Muse CLI not found"), "{msg}");
            assert!(msg.contains("MUSE_CLI="), "{msg}");
        }
        Ok(_) => panic!("expected Spawn error, launch succeeded"),
        Err(other) => panic!("expected Spawn error, got {other}"),
    }
}

#[tokio::test]
async fn graceful_shutdown_exits_clean() {
    let supervisor = Supervisor::launch(test_config(HashMap::new()))
        .await
        .expect("launch");
    assert_eq!(supervisor.status().await, SupervisorStatus::Serving);
    let class = supervisor.shutdown().await;
    assert_eq!(class, ExitClass::Clean);
    assert_eq!(supervisor.status().await, SupervisorStatus::Shutdown);
    assert!(matches!(
        supervisor.ready().await,
        Err(SupervisorError::Shutdown)
    ));
}
