//! `muse-acp-bridge` binary: ACP-over-stdio adapter on the MSP core.
//!
//! With installer subcommand args (`install`, `uninstall`, `help`,
//! `--version`) this binary behaves like `muse-acp`'s CLI and exits; with
//! no (serve) args it launches one supervised `muse serve` child and speaks
//! ACP on stdin/stdout until the client disconnects. Logs go to stderr —
//! stdout is the ACP frame stream and must stay pipe-clean.
//!
//! Configuration is env-only (the arg surface belongs to the installer):
//! `MUSE_CLI`, `MUSE_SERVE_ARGS`, `MUSE_APPROVAL_MODE` (per-session posture,
//! `ask|auto|deny` or a host mode), `MUSE_TRUST_WORKSPACE=1` (passthrough),
//! `MUSE_COMMAND_TIMEOUT_MS`, `RUST_LOG`.

use std::process::ExitCode;

use muse_bridge::acp::{compat, server};
use muse_bridge::dispatch::Dispatcher;
use muse_bridge::msp::host::{HostConfig, Supervisor};

/// Exit status for fatal runtime failures (host launch, config).
const FATAL: u8 = 1;

fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // Logs go to stderr: stdout belongs to the ACP frame stream.
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // Installer subcommands first; `None` means serve mode (P2 owns stdio).
    if let Some(outcome) = compat::install::dispatch(&args).await {
        // Payload lines, not logging: stdout/stderr writes via locked
        // handles (the `support.rs` precedent — never `println!`).
        use std::io::Write as _;
        let mut out = std::io::stdout().lock();
        for line in &outcome.stdout_lines {
            let _ = writeln!(out, "{line}");
        }
        let mut err = std::io::stderr().lock();
        for line in &outcome.stderr_lines {
            let _ = writeln!(err, "{line}");
        }
        return ExitCode::from(outcome.code as u8);
    }
    init_logging();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            tracing::error!("{message}");
            ExitCode::from(FATAL)
        }
    }
}

async fn run() -> Result<(), String> {
    // ACP sessions carry their own cwd per `session/new`; the host child
    // parks in an inert directory until sessions start.
    let cwd = std::env::temp_dir().join("muse-acp-bridge-no-workspace");
    std::fs::create_dir_all(&cwd)
        .map_err(|e| format!("cannot create inert host dir {}: {e}", cwd.display()))?;
    let trust_workspace = std::env::var("MUSE_TRUST_WORKSPACE").as_deref() == Ok("1");
    let host_config = HostConfig::from_env(cwd, trust_workspace);
    let supervisor = Supervisor::launch(host_config)
        .await
        .map_err(|e| format!("msp host launch failed: {e}"))?;
    // The dispatcher feeds the ACP store its supervisor + model catalog; the
    // approval mode here is inert (posture is per-session via
    // `MUSE_APPROVAL_MODE`) but must still parse.
    let dispatcher = Dispatcher::new(supervisor.clone(), None, "denyUnmatched")
        .map_err(|e| format!("dispatcher setup failed: {e}"))?;
    if let Some(info) = supervisor.current().await.handshake_info() {
        tracing::info!(
            host = %info.host_label(),
            durability = %info.durability.as_deref().unwrap_or("durable"),
            compat = ?info.compat,
            "msp host ready"
        );
    }
    server::serve(
        tokio::io::BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
        dispatcher,
    )
    .await;
    let exit = supervisor.shutdown().await;
    tracing::info!(exit = ?exit, "muse-acp-bridge stopped");
    Ok(())
}
