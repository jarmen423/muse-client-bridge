//! `muse-bridge` binary: CLI entry, logging, startup, graceful shutdown.

use std::net::IpAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser as _;
use muse_bridge::cli::{Cli, Command, LogFormat};
use muse_bridge::dispatch::Dispatcher;
use muse_bridge::http::{self, AppState};
use muse_bridge::msp::host::{HostConfig, Supervisor};

/// Exit status for usage errors and unimplemented surfaces.
const USAGE: u8 = 2;
/// Exit status for fatal runtime failures (bind, host launch, config).
const FATAL: u8 = 1;
/// HTTP drain budget after SIGTERM/SIGINT before forcing host shutdown.
const DRAIN_BUDGET: Duration = Duration::from_secs(75);

fn init_logging(format: LogFormat) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // Logs go to stderr: stdout belongs to `--support`'s JSON bundle and
    // `--selftest`'s report lines (both must stay pipe-clean).
    match format {
        LogFormat::Json => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .with_target(false)
                .with_writer(std::io::stderr)
                .init();
        }
        LogFormat::Pretty => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_target(false)
                .with_writer(std::io::stderr)
                .init();
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    init_logging(cli.log_format);

    if let Some(command) = &cli.command {
        if cli.support || cli.selftest {
            eprintln!("muse-bridge: --support/--selftest cannot be combined with a subcommand");
            return ExitCode::from(USAGE);
        }
        match command {
            Command::Serve(args) => {
                // `Stdout` (not the `!Send` lock guard): each report line
                // locks internally, which is plenty for a management command.
                let mut out = std::io::stdout();
                match muse_bridge::service::run_serve(args, &mut out).await {
                    Ok(()) => return ExitCode::SUCCESS,
                    Err(e) => {
                        eprintln!("muse-bridge: {}", e.message);
                        return ExitCode::from(if e.usage { USAGE } else { FATAL });
                    }
                }
            }
        }
    }

    if cli.support || cli.selftest {
        let cwd = match host_cwd(cli.workspace_root.as_ref()) {
            Ok(cwd) => cwd,
            Err(Fatal { message, usage }) => {
                eprintln!("muse-bridge: {message}");
                return ExitCode::from(if usage { USAGE } else { FATAL });
            }
        };
        let host_config = HostConfig::from_env(cwd, cli.trust_workspace, &cli.muse_bin);
        if cli.support {
            muse_bridge::support::run_support(&host_config).await;
        }
        if cli.selftest {
            let code = muse_bridge::support::run_selftest(
                &host_config,
                cli.workspace_root.as_deref(),
                &cli.approval_mode,
            )
            .await;
            return ExitCode::from(code);
        }
        return ExitCode::SUCCESS;
    }

    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(Fatal { message, usage }) => {
            eprintln!("muse-bridge: {message}");
            ExitCode::from(if usage { USAGE } else { FATAL })
        }
    }
}

struct Fatal {
    message: String,
    usage: bool,
}

/// Host child cwd: the workspace when set, else a bridge-managed inert
/// directory (provider mode parks the child outside the operator's cwd).
fn host_cwd(workspace_root: Option<&PathBuf>) -> Result<PathBuf, Fatal> {
    match workspace_root {
        Some(root) => Ok(root.clone()),
        None => {
            let dir = std::env::temp_dir().join("muse-bridge-no-workspace");
            std::fs::create_dir_all(&dir).map_err(|e| {
                Fatal::runtime(format!(
                    "cannot create inert host dir {}: {e}",
                    dir.display()
                ))
            })?;
            Ok(dir)
        }
    }
}

impl Fatal {
    fn runtime(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            usage: false,
        }
    }

    fn usage(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            usage: true,
        }
    }
}

async fn run(cli: Cli) -> Result<(), Fatal> {
    // Loopback-only unless explicitly overridden (the bridge has no auth).
    let addr: IpAddr = cli
        .bind
        .parse()
        .map_err(|_| Fatal::usage(format!("--bind must be an IP address, got '{}'", cli.bind)))?;
    if !addr.is_loopback() && !cli.allow_remote {
        return Err(Fatal::usage(format!(
            "refusing non-loopback bind {addr} without --allow-remote (the bridge has no auth)"
        )));
    }
    if !addr.is_loopback() {
        tracing::warn!(bind = %addr, "bound to a non-loopback address with --allow-remote; ensure a trusted boundary");
    }

    if cli.workspace_root.is_none() {
        tracing::info!(
            "no --workspace-root: provider mode (the client owns the workspace; no workspaceRoot is sent)"
        );
    }
    let host_config = HostConfig::from_env(
        host_cwd(cli.workspace_root.as_ref())?,
        cli.trust_workspace,
        &cli.muse_bin,
    );
    let supervisor = Supervisor::launch(host_config)
        .await
        .map_err(|e| Fatal::runtime(e.to_string()))?;
    let dispatcher = Dispatcher::new(
        supervisor.clone(),
        cli.workspace_root.as_deref(),
        &cli.approval_mode,
    )
    .map_err(Fatal::runtime)?;
    if let Some(info) = supervisor.current().await.handshake_info() {
        tracing::info!(
            host = %info.host_label(),
            durability = %info.durability.as_deref().unwrap_or("durable"),
            compat = ?info.compat,
            "msp host ready"
        );
    }

    let listener = tokio::net::TcpListener::bind((addr, cli.port))
        .await
        .map_err(|e| Fatal::runtime(format!("failed to bind {addr}:{}: {e}", cli.port)))?;
    let local = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| format!("{addr}:{}", cli.port));
    tracing::info!(listen = %local, "muse-bridge serving");

    let app = http::router(AppState {
        dispatcher: dispatcher.clone(),
    });
    http::serve_with_drain(listener, app, DRAIN_BUDGET)
        .await
        .map_err(Fatal::runtime)?;
    let exit = supervisor.shutdown().await;
    tracing::info!(exit = ?exit, "muse-bridge stopped");
    Ok(())
}
