//! CLI surface for `muse-bridge` (SPEC §7, HANDOFF P0).
//!
//! Flags and env vars are the whole configuration surface; there is no config
//! file. Every behavior these select is implemented in later packets — this
//! module only parses and validates.

use std::path::PathBuf;

use clap::{Parser, ValueEnum};

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
}
