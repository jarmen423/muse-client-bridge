//! `--support` diagnostics bundle and `--selftest` live probe (SPEC §7).
//!
//! Both entry points take an explicit [`HostConfig`] so the maintained test
//! suite can drive them against `fake_serve.py`; production passes
//! [`HostConfig::from_env`]. `--support` always prints its JSON bundle (exit
//! 0); `--selftest` exits 0 only when every probe step passes.

use std::path::Path;
use std::time::Duration;

use serde_json::json;

use crate::dispatch::{CollectedTurn, DispatchError, Dispatcher};
use crate::msp::fold::TurnOutcome;
use crate::msp::host::{
    CompatStatus, HandshakeInfo, HostConfig, LaunchError, PINNED_FINGERPRINT,
    SUPPORTED_SCHEMA_VERSION, Supervisor,
};
use crate::translate::{ChatRequest, translate_chat};

/// How long `--selftest` waits for the probe turn to settle before failing.
const SELFTEST_TURN_TIMEOUT: Duration = Duration::from_secs(180);
/// Probe prompt: minimal, deterministic, cheap.
const SELFTEST_PROMPT: &str = "Reply with the single word: ok";
/// Prefix of probe output echoed into the report (never full content).
const SELFTEST_ECHO_CHARS: usize = 80;

/// `--support`: print the diagnostics bundle as JSON on stdout.
///
/// The bundle captures versions, the fingerprint pin vs the live handshake,
/// and the host stderr tail (SPEC §7). The host child is shut down before
/// returning. Always returns 0 once the bundle is printed — reachability is
/// data, not an exit code.
pub async fn run_support(config: &HostConfig) -> u8 {
    let bundle = collect_support(config).await;
    println!(
        "{}",
        serde_json::to_string_pretty(&bundle).unwrap_or_else(|_| "{}".to_string())
    );
    0
}

/// Build the `--support` bundle (pure I/O, no printing; unit-testable shape).
pub async fn collect_support(config: &HostConfig) -> serde_json::Value {
    let mut argv = vec![config.bin.clone()];
    if let Some(sub) = &config.subcommand {
        argv.push(sub.clone());
    }
    argv.extend(config.serve_args.iter().cloned());
    let cwd = config
        .cwd
        .canonicalize()
        .unwrap_or_else(|_| config.cwd.clone());
    let mut bundle = json!({
        "bridge": {
            "version": env!("CARGO_PKG_VERSION"),
            "pinned_fingerprint": PINNED_FINGERPRINT,
            "supported_schema_version": SUPPORTED_SCHEMA_VERSION,
        },
        "host": {
            "bin": config.bin,
            "argv": argv,
            "cwd": cwd.to_string_lossy(),
        },
    });

    match Supervisor::launch(config.clone()).await {
        Ok(supervisor) => {
            let conn = supervisor.current().await;
            if let Some(info) = conn.handshake_info() {
                bundle["handshake"] = handshake_json(&info);
            } else {
                bundle["handshake"] = json!({
                    "reachable": false,
                    "error": "launched but no handshake facts recorded",
                });
            }
            let tail = conn.stderr_tail().text();
            bundle["stderr_tail"] = json!(tail);
            let exit = supervisor.shutdown().await;
            bundle["shutdown"] = json!(format!("{exit:?}"));
        }
        Err(error) => {
            bundle["handshake"] = json!({
                "reachable": false,
                "error": error.to_string(),
            });
            bundle["stderr_tail"] = json!("");
            bundle["shutdown"] = json!("not launched");
        }
    }
    bundle
}

/// One step of the `--selftest` report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelftestStep {
    /// Step name (`handshake`, `model_list`, `empty_turn`).
    pub name: &'static str,
    /// Whether the step passed.
    pub pass: bool,
    /// One-line detail (safe for logs; no prompt echo beyond a prefix).
    pub detail: String,
}

/// `--selftest`: handshake + `model/list` + empty-turn probe, exit 0/1.
///
/// Prints one `PASS`/`FAIL` line per step plus a summary. Any failure (or a
/// host that never becomes ready) exits 1. A detected no-login state adds a
/// `muse login` hint line.
pub async fn run_selftest(
    config: &HostConfig,
    workspace_root: Option<&Path>,
    approval_mode: &str,
) -> u8 {
    let report = collect_selftest(config, workspace_root, approval_mode).await;
    let mut failed = 0;
    for step in &report.steps {
        if step.pass {
            println!("PASS {}: {}", step.name, step.detail);
        } else {
            println!("FAIL {}: {}", step.name, step.detail);
            failed += 1;
        }
    }
    if let Some(hint) = &report.hint {
        println!("HINT: {hint}");
    }
    if failed == 0 {
        println!("selftest: all {} steps passed", report.steps.len());
        0
    } else {
        println!("selftest: {failed} of {} steps failed", report.steps.len());
        1
    }
}

/// The `--selftest` result: per-step verdicts plus an optional login hint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelftestReport {
    /// Step verdicts in run order.
    pub steps: Vec<SelftestStep>,
    /// Set when a failure indicates the no-login state (SPEC §6 row).
    pub hint: Option<String>,
}

/// Run the `--selftest` steps without printing (maintained tests drive this).
pub async fn collect_selftest(
    config: &HostConfig,
    workspace_root: Option<&Path>,
    approval_mode: &str,
) -> SelftestReport {
    let mut steps = Vec::new();
    let mut hint = None;
    let supervisor = match Supervisor::launch(config.clone()).await {
        Ok(supervisor) => supervisor,
        Err(error) => {
            hint = launch_hint(&error);
            steps.push(SelftestStep {
                name: "handshake",
                pass: false,
                detail: error.to_string(),
            });
            return SelftestReport { steps, hint };
        }
    };

    let info = supervisor.current().await.handshake_info();
    match &info {
        Some(info) => steps.push(SelftestStep {
            name: "handshake",
            pass: true,
            detail: format!(
                "{} schema={} compat={} durability={}",
                info.host_label(),
                info.schema_version.unwrap_or(0),
                compat_label(&info.compat),
                info.durability.as_deref().unwrap_or("durable"),
            ),
        }),
        None => steps.push(SelftestStep {
            name: "handshake",
            pass: false,
            detail: "launched but no handshake facts recorded".to_string(),
        }),
    }

    let dispatcher = match Dispatcher::new(supervisor.clone(), workspace_root, approval_mode) {
        Ok(dispatcher) => dispatcher,
        Err(error) => {
            steps.push(SelftestStep {
                name: "model_list",
                pass: false,
                detail: format!("dispatcher setup failed: {error}"),
            });
            supervisor.shutdown().await;
            return SelftestReport { steps, hint };
        }
    };

    match dispatcher.models().await {
        Ok(catalog) => {
            let first = catalog
                .models
                .first()
                .map(|m| m.id.as_str())
                .unwrap_or("<empty>");
            steps.push(SelftestStep {
                name: "model_list",
                pass: true,
                detail: format!("{} model(s), first: {first}", catalog.models.len()),
            });
        }
        Err(error) => {
            hint = dispatch_hint(&error);
            steps.push(SelftestStep {
                name: "model_list",
                pass: false,
                detail: error.to_string(),
            });
        }
    }

    let (turn_step, turn_kind) = probe_empty_turn(&dispatcher).await;
    if turn_kind.as_deref() == Some("authRequired") {
        hint = Some(LOGIN_HINT.to_string());
    }
    steps.push(turn_step);
    supervisor.shutdown().await;
    SelftestReport { steps, hint }
}

/// Hint printed when a step fails in the no-login state (SPEC §6 row).
const LOGIN_HINT: &str = "host reports no usable credentials. Run `muse login`, then retry";

/// No-login detection on launch failures: `authRequired` on the wire is
/// decisive; any other `initialize` rejection earns the conditional hint
/// (an early exit-3 also lands here, as a failed handshake).
fn launch_hint(error: &LaunchError) -> Option<String> {
    match error {
        LaunchError::Handshake(e) if e.kind() == Some("authRequired") => {
            Some(LOGIN_HINT.to_string())
        }
        LaunchError::Handshake(_) => Some(format!(
            "{LOGIN_HINT} (if the host needs credentials; otherwise see the error above)"
        )),
        _ => None,
    }
}

/// No-login detection on dispatch failures (kind-based, never message text).
fn dispatch_hint(error: &DispatchError) -> Option<String> {
    match error {
        DispatchError::Msp(e) if e.kind() == Some("authRequired") => Some(LOGIN_HINT.to_string()),
        _ => None,
    }
}

/// The empty-turn probe: a minimal chat turn through the real
/// translate → dispatch → host path, settled exactly once. Returns the step
/// plus the structured failure kind (when the turn failed) for login-hint
/// detection — never inferred from message text.
async fn probe_empty_turn(dispatcher: &Dispatcher) -> (SelftestStep, Option<String>) {
    let fail = |detail: String| {
        (
            SelftestStep {
                name: "empty_turn",
                pass: false,
                detail,
            },
            None,
        )
    };
    let request: ChatRequest = serde_json::from_value(json!({
        "model": null,
        "messages": [{"role": "user", "content": SELFTEST_PROMPT}],
    }))
    .expect("selftest probe request is static JSON");
    let input = match translate_chat(&request).await {
        Ok(input) => input,
        Err(error) => return fail(format!("translate failed: {error}")),
    };
    let mut handle = match dispatcher.run_turn(None, input).await {
        Ok(handle) => handle,
        Err(error) => return fail(format!("setup failed: {error}")),
    };
    let collected = match tokio::time::timeout(SELFTEST_TURN_TIMEOUT, handle.collect()).await {
        Ok(collected) => collected,
        Err(_) => {
            handle.cancel();
            return fail(format!(
                "turn unsettled after {}s (cancelled)",
                SELFTEST_TURN_TIMEOUT.as_secs()
            ));
        }
    };
    let kind = match &collected.outcome {
        Some(TurnOutcome::Failed { kind, .. }) => Some(kind.clone()),
        _ => None,
    };
    (turn_verdict(&collected), kind)
}

/// Map a settled probe turn to its report step.
fn turn_verdict(collected: &CollectedTurn) -> SelftestStep {
    let echo: String = collected.text.chars().take(SELFTEST_ECHO_CHARS).collect();
    match &collected.outcome {
        Some(TurnOutcome::Completed { usage }) => SelftestStep {
            name: "empty_turn",
            pass: true,
            detail: format!(
                "completed, {} output chars, usage {}/{}/{}; echo: {echo:?}",
                collected.text.len(),
                usage.prompt_tokens,
                usage.completion_tokens,
                usage.total_tokens,
            ),
        },
        Some(TurnOutcome::Cancelled { .. }) => SelftestStep {
            name: "empty_turn",
            pass: false,
            detail: "turn was cancelled, expected completion".to_string(),
        },
        Some(TurnOutcome::Failed { kind, message, .. }) => SelftestStep {
            name: "empty_turn",
            pass: false,
            detail: format!("turn failed ({kind}): {message}"),
        },
        None => SelftestStep {
            name: "empty_turn",
            pass: false,
            detail: "turn stream ended without a terminal outcome".to_string(),
        },
    }
}

fn handshake_json(info: &HandshakeInfo) -> serde_json::Value {
    json!({
        "reachable": true,
        "server": info.host_label(),
        "schema_version": info.schema_version,
        "fingerprint": info.fingerprint,
        "compat": compat_label(&info.compat),
        "durability": info.durability,
    })
}

fn compat_label(compat: &CompatStatus) -> &'static str {
    match compat {
        CompatStatus::Tested => "tested",
        CompatStatus::FingerprintMismatch => "fingerprint_mismatch",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msp::fold::Usage;
    use crate::msp::proto::ErrorObject;

    fn wire_error(kind: Option<&str>) -> ErrorObject {
        ErrorObject {
            code: -32000,
            message: "fake".to_string(),
            data: kind.map(|k| serde_json::json!({"kind": k})),
        }
    }

    fn completed(text: &str) -> CollectedTurn {
        CollectedTurn {
            text: text.to_string(),
            reasoning: String::new(),
            status_lines: Vec::new(),
            outcome: Some(TurnOutcome::Completed {
                usage: Usage {
                    prompt_tokens: 7,
                    completion_tokens: 3,
                    total_tokens: 10,
                },
            }),
        }
    }

    #[test]
    fn verdict_passes_completed_and_truncates_echo() {
        let step = turn_verdict(&completed(&"x".repeat(200)));
        assert!(step.pass);
        assert!(step.detail.contains("usage 7/3/10"));
        assert!(!step.detail.contains(&"x".repeat(200)));
    }

    #[test]
    fn verdict_fails_cancelled_failed_and_missing() {
        let cancelled = CollectedTurn {
            outcome: Some(TurnOutcome::Cancelled {
                usage: Usage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                },
            }),
            ..Default::default()
        };
        assert!(!turn_verdict(&cancelled).pass);

        let failed = CollectedTurn {
            outcome: Some(TurnOutcome::Failed {
                kind: "overloaded".to_string(),
                message: "busy".to_string(),
                retryable: true,
                usage: Usage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                },
            }),
            ..Default::default()
        };
        let step = turn_verdict(&failed);
        assert!(!step.pass);
        assert!(step.detail.contains("overloaded"));

        assert!(!turn_verdict(&CollectedTurn::default()).pass);
    }

    #[test]
    fn login_hint_is_kind_based() {
        assert_eq!(
            launch_hint(&LaunchError::Handshake(wire_error(Some("authRequired")))),
            Some(LOGIN_HINT.to_string())
        );
        assert_eq!(
            dispatch_hint(&DispatchError::Msp(wire_error(Some("authRequired")))),
            Some(LOGIN_HINT.to_string())
        );
        assert!(dispatch_hint(&DispatchError::Msp(wire_error(Some("overloaded")))).is_none());
        assert!(dispatch_hint(&DispatchError::Msp(wire_error(None))).is_none());
        assert!(dispatch_hint(&DispatchError::HostDead).is_none());
        assert!(launch_hint(&LaunchError::Spawn("no bin".to_string())).is_none());
        assert!(
            launch_hint(&LaunchError::IncompatibleSchema {
                version: Some(2),
                fingerprint: "x".to_string(),
            })
            .is_none()
        );
    }

    #[test]
    fn other_handshake_failures_get_the_conditional_hint() {
        let hint = launch_hint(&LaunchError::Handshake(wire_error(Some("internal"))))
            .expect("conditional hint");
        assert!(hint.contains("muse login"));
        assert!(hint.contains("if the host needs credentials"));
    }

    #[test]
    fn handshake_json_marks_reachable() {
        let info = HandshakeInfo {
            server_name: "muse".to_string(),
            server_version: "1.2.1".to_string(),
            schema_version: Some(1),
            fingerprint: Some(PINNED_FINGERPRINT.to_string()),
            compat: CompatStatus::Tested,
            durability: None,
        };
        let value = handshake_json(&info);
        assert_eq!(value["reachable"], true);
        assert_eq!(value["server"], "muse/1.2.1");
        assert_eq!(value["compat"], "tested");
    }
}
