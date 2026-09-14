//! Diagnostics tests: `--support` bundle and `--selftest` probe against
//! `fake_serve.py` (HANDOFF P6). No login needed.

use std::path::PathBuf;

use muse_bridge::msp::host::HostConfig;
use muse_bridge::support::{collect_selftest, collect_support};

fn fake_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake_serve.py")
}

fn test_config(scenario: &str) -> HostConfig {
    HostConfig {
        bin: "python3".to_string(),
        subcommand: None,
        serve_args: vec![fake_path().to_string_lossy().into_owned()],
        cwd: std::env::temp_dir(),
        extra_env: vec![("FAKE_SCENARIO".to_string(), scenario.to_string())],
        timeout_override_ms: None,
        retry_base_delay_ms: 10,
    }
}

#[tokio::test]
async fn support_bundle_reports_live_handshake() {
    let bundle = collect_support(&test_config("turn-happy")).await;
    assert_eq!(bundle["bridge"]["supported_schema_version"], 1);
    assert_eq!(bundle["handshake"]["reachable"], true);
    assert_eq!(bundle["handshake"]["server"], "fake-muse/9.9-test");
    assert_eq!(bundle["handshake"]["schema_version"], 1);
    assert_eq!(bundle["handshake"]["compat"], "tested");
    assert!(bundle["stderr_tail"].is_string());
    assert_eq!(bundle["shutdown"], "Clean");
}

#[tokio::test]
async fn support_bundle_marks_unreachable_host() {
    let mut config = test_config("turn-happy");
    config.bin = "definitely-not-a-real-binary-xyz".to_string();
    let bundle = collect_support(&config).await;
    assert_eq!(bundle["handshake"]["reachable"], false);
    assert!(
        !bundle["handshake"]["error"]
            .as_str()
            .unwrap_or_default()
            .is_empty()
    );
}

#[tokio::test]
async fn selftest_passes_all_steps_on_happy_host() {
    let report = collect_selftest(
        &test_config("turn-happy"),
        &std::env::temp_dir(),
        "denyUnmatched",
    )
    .await;
    let names: Vec<&str> = report.steps.iter().map(|s| s.name).collect();
    assert_eq!(names, vec!["handshake", "model_list", "empty_turn"]);
    for step in &report.steps {
        assert!(step.pass, "{} failed: {}", step.name, step.detail);
    }
    assert!(report.hint.is_none());
}

#[tokio::test]
async fn selftest_fails_fast_when_host_will_not_spawn() {
    let mut config = test_config("turn-happy");
    config.bin = "definitely-not-a-real-binary-xyz".to_string();
    let report = collect_selftest(&config, &std::env::temp_dir(), "denyUnmatched").await;
    assert_eq!(report.steps.len(), 1);
    assert_eq!(report.steps[0].name, "handshake");
    assert!(!report.steps[0].pass);
    assert!(report.hint.is_none(), "missing binary is not a login state");
}

#[tokio::test]
async fn selftest_hints_login_on_auth_required_turn() {
    let report = collect_selftest(
        &test_config("turn-failed"),
        &std::env::temp_dir(),
        "denyUnmatched",
    )
    .await;
    let turn = report
        .steps
        .iter()
        .find(|s| s.name == "empty_turn")
        .expect("empty_turn step ran");
    assert!(!turn.pass);
    assert!(turn.detail.contains("authRequired"));
    let hint = report.hint.expect("login hint set");
    assert!(hint.contains("muse login"));
}
