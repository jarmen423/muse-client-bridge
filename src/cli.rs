//! CLI surface for `muse-bridge` (SPEC §7, HANDOFF P0).
//!
//! Flags and env vars are the whole configuration surface; there is no config
//! file. Every behavior these select is implemented in later packets — this
//! module only parses and validates.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

/// Default TCP port for the loopback listener.
pub const DEFAULT_PORT: u16 = 17489;
/// Default bind address. Loopback-only unless `--allow-remote` is passed (P5).
pub const DEFAULT_BIND: &str = "127.0.0.1";
/// Default MSP session approval mode: headless-safe, never parks on a dialog.
pub const DEFAULT_APPROVAL_MODE: &str = "denyUnmatched";
/// Default `muse` binary lookup (PATH).
pub const DEFAULT_MUSE_BIN: &str = "muse";

/// `tracing-subscriber` output format for bridge logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    /// Single-line JSON per event (machine-readable).
    Json,
    /// Human-readable multi-field lines (local runs).
    Pretty,
}

/// Localhost OpenAI-compatible HTTP API driving `muse serve` over MSP/stdio.
#[derive(Debug, Clone, Parser)]
#[command(name = "muse-bridge", version, about, long_about = None)]
pub struct Cli {
    /// TCP port for the loopback listener.
    #[arg(long, env = "MUSE_BRIDGE_PORT", default_value_t = DEFAULT_PORT)]
    pub port: u16,

    /// Address to bind. Only loopback binds are accepted unless
    /// `--allow-remote` is passed.
    #[arg(long, default_value = DEFAULT_BIND)]
    pub bind: String,

    /// Allow binding a non-loopback address. The bridge has no auth; only
    /// use this behind a trusted boundary.
    #[arg(long)]
    pub allow_remote: bool,

    /// Workspace root handed to MSP `session/start` as `workspaceRoot`.
    ///
    /// Omit it for provider mode: the client owns the project/workspace,
    /// so the bridge sends no `workspaceRoot` and the host child runs
    /// parked in an inert empty directory instead of your cwd.
    #[arg(long)]
    pub workspace_root: Option<PathBuf>,

    /// MSP session approval mode (`allowAll|promptUnmatched|onRequest|denyUnmatched`).
    #[arg(long, default_value = DEFAULT_APPROVAL_MODE)]
    pub approval_mode: String,

    /// `muse` binary path (or bare name resolved via PATH).
    #[arg(long, env = "MUSE_CLI", default_value = DEFAULT_MUSE_BIN)]
    pub muse_bin: String,

    /// Pass `--trust-workspace` through to `muse serve`.
    #[arg(long)]
    pub trust_workspace: bool,

    /// Bridge log output format.
    #[arg(long, value_enum, default_value_t = LogFormat::Pretty)]
    pub log_format: LogFormat,

    /// Print versions, fingerprint pin vs live host, durability, and the last
    /// stderr tail; then exit 0.
    #[arg(long)]
    pub support: bool,

    /// Run handshake + `model/list` + minimal turn probe against a live host;
    /// exit 0 on success, 1 on failure.
    #[arg(long)]
    pub selftest: bool,

    /// Background-service management. Absent = run the server in the
    /// foreground (unchanged default).
    #[command(subcommand)]
    pub command: Option<Command>,
}

/// `muse-bridge` subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// Install and start the background service (or stop it with `--off`).
    Serve(ServeArgs),
}

/// `muse-bridge serve`: user-level autostart service lifecycle.
///
/// On Linux this writes a systemd user unit, on macOS a LaunchAgent plist,
/// on Windows a logon Scheduled Task — then enables and starts it and
/// verifies `/healthz`. `serve --off` stops and disables (the definition
/// file stays so re-enabling is cheap). The service always binds loopback;
/// remote exposure goes through the optional Tailscale TCP forward.
#[derive(Debug, Clone, Args)]
pub struct ServeArgs {
    /// Stop and disable the background service (undoes `serve`).
    #[arg(long)]
    pub off: bool,

    /// Also manage the Tailscale TCP forward for the bridge port
    /// (`tailscale serve --bg --tcp=<port> tcp://127.0.0.1:<port>`,
    /// removed by `--off --tailscale`). Set `MUSE_BRIDGE_TAILSCALE=true`
    /// to manage it on every `serve` without repeating the flag.
    #[arg(long, env = "MUSE_BRIDGE_TAILSCALE")]
    pub tailscale: bool,

    /// Report service + health status; exit 0 iff `/healthz` answers.
    #[arg(long)]
    pub status: bool,

    /// Print the install plan (file content + commands) without running it.
    #[arg(long)]
    pub dry_run: bool,

    /// TCP port baked into the service definition.
    #[arg(long)]
    pub port: Option<u16>,

    /// Workspace root handed to MSP `session/start` as `workspaceRoot`.
    #[arg(long)]
    pub workspace_root: Option<PathBuf>,

    /// MSP session approval mode (`allowAll|promptUnmatched|onRequest|denyUnmatched`).
    #[arg(long)]
    pub approval_mode: Option<String>,

    /// `muse` binary path (or bare name resolved via PATH).
    #[arg(long)]
    pub muse_bin: Option<String>,

    /// Pass `--trust-workspace` through to `muse serve`.
    #[arg(long)]
    pub trust_workspace: bool,

    /// Bridge log output format.
    #[arg(long, value_enum)]
    pub log_format: Option<LogFormat>,
}

impl Cli {
    /// Extra args appended to `muse serve`, whitespace-split (`MUSE_SERVE_ARGS`).
    pub fn serve_args() -> Vec<String> {
        std::env::var("MUSE_SERVE_ARGS")
            .unwrap_or_default()
            .split_whitespace()
            .map(str::to_string)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    /// clap reads `MUSE_*` process env on every parse, so every test here
    /// holds this lock: env-mutating tests and plain parse tests must never
    /// interleave (cargo runs tests on threads within one process).
    fn env_lock() -> MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("CLI parse must succeed")
    }

    #[test]
    fn defaults_match_the_contract() {
        let _guard = env_lock();
        let cli = parse(&["muse-bridge"]);
        assert_eq!(cli.port, 17489);
        assert_eq!(cli.bind, "127.0.0.1");
        assert!(!cli.allow_remote);
        assert!(cli.workspace_root.is_none(), "provider mode is the default");
        assert_eq!(cli.approval_mode, "denyUnmatched");
        assert_eq!(cli.muse_bin, "muse");
        assert!(!cli.trust_workspace);
        assert_eq!(cli.log_format, LogFormat::Pretty);
        assert!(!cli.support);
        assert!(!cli.selftest);
        assert!(
            cli.command.is_none(),
            "bare invocation runs the foreground server"
        );
    }

    #[test]
    fn every_flag_overrides_its_default() {
        let _guard = env_lock();
        let cli = parse(&[
            "muse-bridge",
            "--port",
            "8646",
            "--bind",
            "127.0.0.2",
            "--workspace-root",
            "/tmp/ws",
            "--approval-mode",
            "promptUnmatched",
            "--muse-bin",
            "/opt/muse",
            "--trust-workspace",
            "--log-format",
            "json",
            "--allow-remote",
        ]);
        assert_eq!(cli.port, 8646);
        assert_eq!(cli.bind, "127.0.0.2");
        assert!(cli.allow_remote);
        assert_eq!(cli.workspace_root, Some(PathBuf::from("/tmp/ws")));
        assert_eq!(cli.approval_mode, "promptUnmatched");
        assert_eq!(cli.muse_bin, "/opt/muse");
        assert!(cli.trust_workspace);
        assert_eq!(cli.log_format, LogFormat::Json);
    }

    #[test]
    fn support_and_selftest_are_independent_flags() {
        let _guard = env_lock();
        assert!(parse(&["muse-bridge", "--support"]).support);
        assert!(parse(&["muse-bridge", "--selftest"]).selftest);
        let both = parse(&["muse-bridge", "--support", "--selftest"]);
        assert!(both.support && both.selftest);
    }

    #[test]
    #[allow(unsafe_code, reason = "process-env mutation for env-precedence tests")]
    fn env_overrides_port_and_muse_bin() {
        let _guard = env_lock();
        // SAFETY: the module lock above serializes all env readers/writers in
        // this process's test run; nothing else here spawns threads that read
        // process env.
        unsafe {
            std::env::set_var("MUSE_BRIDGE_PORT", "9999");
            std::env::set_var("MUSE_CLI", "/env/muse");
        }
        let cli = parse(&["muse-bridge"]);
        assert_eq!(cli.port, 9999);
        assert_eq!(cli.muse_bin, "/env/muse");
        unsafe {
            std::env::remove_var("MUSE_BRIDGE_PORT");
            std::env::remove_var("MUSE_CLI");
        }
    }

    #[test]
    fn invalid_log_format_is_rejected() {
        let _guard = env_lock();
        let err = Cli::try_parse_from(["muse-bridge", "--log-format", "yaml"])
            .expect_err("yaml must not parse");
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidValue);
    }

    #[test]
    #[allow(unsafe_code, reason = "process-env mutation for env-precedence tests")]
    fn serve_args_splits_on_whitespace() {
        let _guard = env_lock();
        // SAFETY: serialized by the module lock; see above.
        unsafe {
            std::env::set_var("MUSE_SERVE_ARGS", "--no-session-log  --foo=bar");
        }
        assert_eq!(Cli::serve_args(), vec!["--no-session-log", "--foo=bar"]);
        unsafe {
            std::env::remove_var("MUSE_SERVE_ARGS");
        }
        assert!(Cli::serve_args().is_empty());
    }

    fn serve_args(args: &[&str]) -> ServeArgs {
        let cli = parse(args);
        let Some(Command::Serve(serve)) = cli.command else {
            panic!("expected `serve` subcommand, got {:?}", cli.command);
        };
        serve
    }

    #[test]
    fn serve_parses_with_flag_defaults() {
        let _guard = env_lock();
        let serve = serve_args(&["muse-bridge", "serve"]);
        assert!(!serve.off);
        assert!(!serve.tailscale);
        assert!(!serve.status);
        assert!(!serve.dry_run);
        assert_eq!(serve.port, None);
        assert_eq!(serve.workspace_root, None);
        assert_eq!(serve.approval_mode, None);
        assert_eq!(serve.muse_bin, None);
        assert!(!serve.trust_workspace);
        assert_eq!(serve.log_format, None);
    }

    #[test]
    fn serve_parses_every_flag() {
        let _guard = env_lock();
        let serve = serve_args(&[
            "muse-bridge",
            "serve",
            "--off",
            "--tailscale",
            "--dry-run",
            "--port",
            "8646",
            "--workspace-root",
            "/tmp/ws",
            "--approval-mode",
            "allowAll",
            "--muse-bin",
            "/opt/muse",
            "--trust-workspace",
            "--log-format",
            "json",
        ]);
        assert!(serve.off);
        assert!(serve.tailscale);
        assert!(serve.dry_run);
        assert!(!serve.status);
        assert_eq!(serve.port, Some(8646));
        assert_eq!(serve.workspace_root, Some(PathBuf::from("/tmp/ws")));
        assert_eq!(serve.approval_mode, Some("allowAll".to_string()));
        assert_eq!(serve.muse_bin, Some("/opt/muse".to_string()));
        assert!(serve.trust_workspace);
        assert_eq!(serve.log_format, Some(LogFormat::Json));
    }

    #[test]
    fn serve_status_parses() {
        let _guard = env_lock();
        let serve = serve_args(&["muse-bridge", "serve", "--status", "--tailscale"]);
        assert!(serve.status);
        assert!(serve.tailscale);
        assert!(!serve.off);
    }

    #[test]
    #[allow(unsafe_code, reason = "process-env mutation for env-precedence tests")]
    fn env_enables_tailscale_management() {
        let _guard = env_lock();
        // SAFETY: serialized by the module lock; see above.
        unsafe {
            std::env::set_var("MUSE_BRIDGE_TAILSCALE", "true");
        }
        let serve = serve_args(&["muse-bridge", "serve"]);
        assert!(
            serve.tailscale,
            "MUSE_BRIDGE_TAILSCALE=true manages the forward"
        );
        unsafe {
            std::env::remove_var("MUSE_BRIDGE_TAILSCALE");
        }
        let serve = serve_args(&["muse-bridge", "serve"]);
        assert!(!serve.tailscale, "unset env leaves the forward alone");
    }
}
