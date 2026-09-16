//! Scripted-dispatch tests: translate → dispatch → fold against turn-playing
//! `fake_serve.py` scenarios (HANDOFF P4). No login needed.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use muse_bridge::dispatch::{CollectedTurn, DispatchError, Dispatcher, TurnHandle};
use muse_bridge::msp::fold::{OutputEvent, TurnOutcome};
use muse_bridge::msp::host::{HostConfig, Supervisor};
use muse_bridge::translate::{ChatRequest, ResponsesRequest, translate_chat, translate_responses};
use serde_json::json;

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

fn read_lines(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

/// Supervisor + dispatcher serving `scenario`, with the fake transcript at `log`.
/// `extra_env` overwrites (so a test can replace `FAKE_SCENARIO` wholesale).
async fn dispatcher_for(scenario: &str, log: &Path, extra_env: &[(&str, String)]) -> Dispatcher {
    dispatcher_for_with_workspace(scenario, log, extra_env, Some(&std::env::temp_dir())).await
}

/// [`dispatcher_for`] with an explicit workspace (`None` ⇒ provider mode:
/// no `workspaceRoot` is sent).
async fn dispatcher_for_with_workspace(
    scenario: &str,
    log: &Path,
    extra_env: &[(&str, String)],
    workspace: Option<&Path>,
) -> Dispatcher {
    let mut env: HashMap<&str, String> = HashMap::from([
        ("FAKE_SCENARIO", scenario.to_string()),
        ("FAKE_LOG", log.to_string_lossy().into_owned()),
    ]);
    for (k, v) in extra_env {
        env.insert(k, v.clone());
    }
    let supervisor = Supervisor::launch(test_config(env)).await.expect("launch");
    Dispatcher::new(supervisor, workspace, "denyUnmatched").expect("dispatcher")
}

async fn chat_turn(
    model: Option<String>,
    text: &str,
) -> (Option<String>, muse_bridge::translate::TurnInput) {
    let request: ChatRequest = serde_json::from_value(json!({
        "model": model,
        "messages": [{"role": "user", "content": text}],
    }))
    .expect("parse");
    let input = translate_chat(&request).await.expect("translate");
    (request.model, input)
}

/// Collect with a watchdog (a hung turn fails the test, never the suite).
async fn collect_watchdog(handle: &mut TurnHandle) -> CollectedTurn {
    tokio::time::timeout(Duration::from_secs(20), handle.collect())
        .await
        .expect("turn must settle within 20s")
}

#[tokio::test]
async fn full_turn_collects_text_and_usage() {
    let log = scratch("dispatch-happy", "log");
    let dispatcher = dispatcher_for("turn-happy", &log, &[]).await;
    let (model, input) = chat_turn(Some("fake-a".to_string()), "hi").await;
    let mut handle = dispatcher.run_turn(model, input).await.expect("run");
    let collected = collect_watchdog(&mut handle).await;
    assert_eq!(collected.text, "Hello, world");
    match collected.outcome {
        Some(TurnOutcome::Completed { usage }) => {
            assert_eq!(usage.prompt_tokens, 100);
            assert_eq!(usage.completion_tokens, 10);
            assert_eq!(usage.total_tokens, 110);
        }
        other => panic!("expected Completed, got {other:?}"),
    }
    // The fake transcript proves the orchestration: session → model → turn.
    let lines = read_lines(&log);
    assert!(
        lines.iter().any(|l| l.contains("mode=denyUnmatched")),
        "{lines:?}"
    );
    assert!(
        lines.iter().any(|l| l.contains("model=fake-a")),
        "{lines:?}"
    );
    assert!(lines.iter().any(|l| l.contains("nparts=1")), "{lines:?}");
    dispatcher.supervisor().shutdown().await;
}

#[tokio::test]
async fn provider_mode_turn_omits_workspace_root() {
    let log = scratch("dispatch-provider", "log");
    let dispatcher = dispatcher_for_with_workspace("turn-happy", &log, &[], None).await;
    let (_, input) = chat_turn(None, "hi").await;
    let mut handle = dispatcher.run_turn(None, input).await.expect("run");
    let collected = collect_watchdog(&mut handle).await;
    assert!(matches!(
        collected.outcome,
        Some(TurnOutcome::Completed { .. })
    ));
    // The fake transcript proves the omission: `ws=-` means the key was absent.
    let lines = read_lines(&log);
    let start = lines
        .iter()
        .find(|l| l.starts_with("session/start "))
        .expect("session/start logged");
    assert!(start.ends_with(" ws=-"), "workspaceRoot sent: {lines:?}");
    dispatcher.supervisor().shutdown().await;
}

#[tokio::test]
async fn approval_auto_deny_picks_first_non_approving_choice() {
    let log = scratch("dispatch-deny", "log");
    let dispatcher = dispatcher_for("turn-approval", &log, &[]).await;
    let (_, input) = chat_turn(None, "hi").await;
    let mut handle = dispatcher.run_turn(None, input).await.expect("run");
    let collected = collect_watchdog(&mut handle).await;
    assert!(matches!(
        collected.outcome,
        Some(TurnOutcome::Completed { .. })
    ));
    let lines = read_lines(&log);
    // The {} presentation receipt precedes the decide command.
    let receipt = lines.iter().position(|l| l == "<response> id=9100 result");
    let decide = lines
        .iter()
        .position(|l| l.contains("choice=c-deny") && l.contains("approval=ap-1"));
    match (receipt, decide) {
        (Some(r), Some(d)) => assert!(r < d, "receipt must precede decide: {lines:?}"),
        _ => panic!("missing receipt/decide in {lines:?}"),
    }
    dispatcher.supervisor().shutdown().await;
}

#[tokio::test]
async fn approval_with_no_deny_choice_cancels_the_turn() {
    let log = scratch("dispatch-allapprove", "log");
    let dispatcher = dispatcher_for("turn-approval-all-approve", &log, &[]).await;
    let (_, input) = chat_turn(None, "hi").await;
    let mut handle = dispatcher.run_turn(None, input).await.expect("run");
    let collected = collect_watchdog(&mut handle).await;
    assert!(matches!(
        collected.outcome,
        Some(TurnOutcome::Cancelled { .. })
    ));
    let lines = read_lines(&log);
    // Never approved: no decide at all, and the turn/cancel names the turn.
    assert!(
        !lines.iter().any(|l| l.contains("approval/decide")),
        "{lines:?}"
    );
    let start = lines
        .iter()
        .find(|l| l.starts_with("turn/start "))
        .expect("turn/start logged");
    let turn_id = start
        .split("cmd=")
        .nth(1)
        .and_then(|s| s.split(' ').next())
        .expect("cmd");
    assert!(
        lines
            .iter()
            .any(|l| l.starts_with("turn/cancel cmd=") && l.ends_with(&format!(" turn={turn_id}"))),
        "explicit turn/cancel missing in {lines:?}"
    );
    dispatcher.supervisor().shutdown().await;
}

#[tokio::test]
async fn user_input_auto_cancels_headless() {
    let log = scratch("dispatch-userinput", "log");
    let dispatcher = dispatcher_for("turn-userinput", &log, &[]).await;
    let (_, input) = chat_turn(None, "hi").await;
    let mut handle = dispatcher.run_turn(None, input).await.expect("run");
    let collected = collect_watchdog(&mut handle).await;
    assert!(matches!(
        collected.outcome,
        Some(TurnOutcome::Completed { .. })
    ));
    let lines = read_lines(&log);
    assert!(
        lines
            .iter()
            .any(|l| l.contains("userInput/cancel") && l.contains("reason=headless-bridge")),
        "{lines:?}"
    );
    dispatcher.supervisor().shutdown().await;
}

#[tokio::test]
async fn gap_mid_turn_splices_without_loss_or_dup() {
    let log = scratch("dispatch-gap", "log");
    let dispatcher = dispatcher_for("turn-gap", &log, &[]).await;
    let (_, input) = chat_turn(None, "hi").await;
    let mut handle = dispatcher.run_turn(None, input).await.expect("run");
    let collected = collect_watchdog(&mut handle).await;
    // Live "Hello" + paged suffix ", world": converged exactly once.
    assert_eq!(collected.text, "Hello, world");
    assert!(matches!(
        collected.outcome,
        Some(TurnOutcome::Completed { .. })
    ));
    let lines = read_lines(&log);
    assert!(lines.iter().any(|l| l.contains("view/page")), "{lines:?}");
    dispatcher.supervisor().shutdown().await;
}

#[tokio::test]
async fn turn_failed_terminal_surfaces_kind() {
    let log = scratch("dispatch-failed", "log");
    let dispatcher = dispatcher_for("turn-failed", &log, &[]).await;
    let (_, input) = chat_turn(None, "hi").await;
    let mut handle = dispatcher.run_turn(None, input).await.expect("run");
    let collected = collect_watchdog(&mut handle).await;
    match collected.outcome {
        Some(TurnOutcome::Failed {
            kind,
            message,
            retryable,
            ..
        }) => {
            assert_eq!(kind, "authRequired");
            assert!(message.contains("login"), "{message}");
            assert!(!retryable);
        }
        other => panic!("expected Failed, got {other:?}"),
    }
    dispatcher.supervisor().shutdown().await;
}

#[tokio::test]
async fn cancel_mid_tool_sends_explicit_turn_cancel() {
    let log = scratch("dispatch-cancel", "log");
    let dispatcher = dispatcher_for("turn-tool-slow", &log, &[]).await;
    let (_, input) = chat_turn(None, "hi").await;
    let mut handle = dispatcher.run_turn(None, input).await.expect("run");
    // Wait for the tool announce, then cancel like a disconnecting client.
    let announced = tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(event) = handle.events.recv().await {
            if matches!(event, OutputEvent::StatusLine(_)) {
                return true;
            }
        }
        false
    })
    .await
    .expect("announce must arrive");
    assert!(announced);
    handle.cancel();
    // The driver stops without a terminal (nobody is listening).
    let closed = tokio::time::timeout(Duration::from_secs(20), handle.collect()).await;
    assert!(closed.is_ok(), "driver must stop after cancel");
    let lines = read_lines(&log);
    let start = lines
        .iter()
        .find(|l| l.starts_with("turn/start "))
        .expect("turn/start logged");
    let turn_id = start
        .split("cmd=")
        .nth(1)
        .and_then(|s| s.split(' ').next())
        .expect("cmd");
    assert!(
        lines
            .iter()
            .any(|l| l.starts_with("turn/cancel cmd=") && l.ends_with(&format!(" turn={turn_id}"))),
        "explicit turn/cancel missing in {lines:?}"
    );
    dispatcher.supervisor().shutdown().await;
}

#[tokio::test]
async fn unknown_model_rejection_is_a_caller_error() {
    let log = scratch("dispatch-model", "log");
    let dispatcher = dispatcher_for(
        "serve",
        &log,
        &[
            (
                "FAKE_SCENARIO",
                "fail:session/setModel:commandRejected:-32030".to_string(),
            ),
            ("FAKE_FAIL_REASON", "invalid_model".to_string()),
        ],
    )
    .await;
    // Note: dispatcher_for's env insert order puts FAKE_SCENARIO twice; the
    // explicit extra wins (HashMap insert overwrites).
    let (_, input) = chat_turn(Some("bogus-model".to_string()), "hi").await;
    match dispatcher
        .run_turn(Some("bogus-model".to_string()), input)
        .await
    {
        Err(DispatchError::UnknownModel(model)) => assert_eq!(model, "bogus-model"),
        Ok(_) => panic!("expected UnknownModel, turn ran"),
        Err(other) => panic!("expected UnknownModel, got {other}"),
    }
    dispatcher.supervisor().shutdown().await;
}

#[tokio::test]
async fn approval_mode_mismatch_fails_never_downgrades() {
    let log = scratch("dispatch-mode", "log");
    let dispatcher = dispatcher_for(
        "serve",
        &log,
        &[("FAKE_MODE", "promptUnmatched".to_string())],
    )
    .await;
    let (_, input) = chat_turn(None, "hi").await;
    match dispatcher.run_turn(None, input).await {
        Err(DispatchError::ApprovalModeMismatch { requested, folded }) => {
            assert_eq!(requested, "denyUnmatched");
            assert_eq!(folded, "promptUnmatched");
        }
        Ok(_) => panic!("expected ApprovalModeMismatch, turn ran"),
        Err(other) => panic!("expected ApprovalModeMismatch, got {other}"),
    }
    dispatcher.supervisor().shutdown().await;
}

#[tokio::test]
async fn models_serve_catalog_and_retain_last_good() {
    use muse_bridge::dispatch::Dispatcher as D;
    let log = scratch("dispatch-models", "log");
    let dispatcher = dispatcher_for("serve", &log, &[]).await;
    let catalog = dispatcher.models().await.expect("catalog");
    assert_eq!(catalog.models.len(), 1);
    assert_eq!(catalog.models[0].id, "fake-a");

    // Without any cache a silent host fails the picker outright.
    let log2 = scratch("dispatch-models-empty", "log");
    let env: HashMap<&str, String> = HashMap::from([
        ("FAKE_SCENARIO", "silent:model/list".to_string()),
        ("FAKE_LOG", log2.to_string_lossy().into_owned()),
    ]);
    let mut config = test_config(env);
    config.timeout_override_ms = Some(200);
    let supervisor = Supervisor::launch(config).await.expect("launch");
    let uncached = D::new(
        supervisor.clone(),
        Some(&std::env::temp_dir()),
        "denyUnmatched",
    )
    .expect("dispatcher");
    let err = uncached
        .models()
        .await
        .expect_err("must fail without cache");
    assert!(matches!(err, DispatchError::Msp(_)), "{err:?}");

    // With the populated cache shared, the same silent host serves stale.
    let stale = D::new_with_cache(
        supervisor.clone(),
        Some(&std::env::temp_dir()),
        "denyUnmatched",
        dispatcher.model_cache().clone(),
    )
    .expect("dispatcher");
    let catalog = stale.models().await.expect("stale catalog serves");
    assert_eq!(catalog.models[0].id, "fake-a");
    supervisor.shutdown().await;
    dispatcher.supervisor().shutdown().await;
}

#[tokio::test]
async fn responses_requests_run_end_to_end() {
    let log = scratch("dispatch-responses", "log");
    let dispatcher = dispatcher_for("turn-happy", &log, &[]).await;
    let request: ResponsesRequest = serde_json::from_value(json!({
        "model": "fake-a",
        "instructions": "Be brief.",
        "input": "Hello?",
        "reasoning": {"effort": "low"},
    }))
    .expect("parse");
    let input = translate_responses(&request).await.expect("translate");
    assert_eq!(input.reasoning_effort.as_deref(), Some("low"));
    let mut handle = dispatcher
        .run_turn(request.model.clone(), input)
        .await
        .expect("run");
    let collected = collect_watchdog(&mut handle).await;
    assert_eq!(collected.text, "Hello, world");
    dispatcher.supervisor().shutdown().await;
}
