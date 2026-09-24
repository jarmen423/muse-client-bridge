//! `serve` subcommand: background-service lifecycle (SPEC §7).
//!
//! `muse-bridge serve` installs and starts a user-level autostart service,
//! then verifies `/healthz`; `serve --off` stops and disables it (the
//! definition file stays so re-enabling is cheap). Backends:
//!
//! - Linux: systemd user unit (`~/.config/systemd/user/muse-bridge.service`)
//! - macOS: LaunchAgent plist (`~/Library/LaunchAgents/muse-bridge.plist`)
//! - Windows: logon Scheduled Task (`muse-bridge`, `InteractiveToken` so it
//!   runs as you and inherits your `muse login`)
//!
//! The service always binds loopback with explicit flags baked in (never
//! `--bind`/`--allow-remote`: remote exposure goes through the optional
//! Tailscale TCP forward). All rendering is pure and golden-tested; only
//! [`run_serve`] touches the system.

use std::path::PathBuf;
use std::time::Duration;

use crate::cli::{DEFAULT_APPROVAL_MODE, DEFAULT_MUSE_BIN, DEFAULT_PORT, LogFormat, ServeArgs};
use crate::msp::host_bin::{ResolvedHost, resolve_host_bin};

/// systemd unit name, launchd label, and Scheduled Task name (one identity).
pub const SERVICE_NAME: &str = "muse-bridge";
const SYSTEMD_UNIT: &str = "muse-bridge.service";
const LAUNCHD_PLIST: &str = "muse-bridge.plist";
const LAUNCHD_LOG: &str = "muse-bridge.log";
const WINDOWS_LOG: &str = "muse-bridge.service.log";
/// `/healthz` wait budget after (re)start.
const HEALTH_POLL_BUDGET: Duration = Duration::from_secs(10);
/// Single subprocess timeout. Every child is bounded: a wedged
/// `systemctl`/`launchctl`/`schtasks`/`tailscale` must never hang `serve`.
const CMD_TIMEOUT: Duration = Duration::from_secs(30);

macro_rules! say {
    ($out:expr, $($arg:tt)*) => {
        writeln!($out, $($arg)*)
            .map_err(|e| ServeError::runtime(format!("failed to write output: {e}")))?
    };
}

/// `serve` failure. Mirrors `main.rs`'s `Fatal`: `usage` errors exit 2,
/// runtime failures exit 1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServeError {
    /// Human-readable failure (already contains the fix or the manual command).
    pub message: String,
    /// True for argument/validation errors (exit 2), false for runtime (exit 1).
    pub usage: bool,
}

impl ServeError {
    /// Runtime failure (exit 1): system state, missing tools, unhealthy service.
    pub fn runtime(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            usage: false,
        }
    }

    /// Usage error (exit 2): bad flags or values.
    pub fn usage(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            usage: true,
        }
    }
}

impl std::fmt::Display for ServeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

/// Service backend for this OS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    /// Linux systemd user unit.
    Systemd,
    /// macOS LaunchAgent.
    Launchd,
    /// Windows logon Scheduled Task.
    WindowsTask,
}

impl Platform {
    /// Backend for the compile target, or a clean error on unhandled OSes.
    pub fn current() -> Result<Self, ServeError> {
        if cfg!(target_os = "linux") {
            Ok(Self::Systemd)
        } else if cfg!(target_os = "macos") {
            Ok(Self::Launchd)
        } else if cfg!(target_os = "windows") {
            Ok(Self::WindowsTask)
        } else {
            Err(ServeError::usage(
                "serve supports Linux (systemd), macOS (launchd), and Windows (Scheduled Tasks); run muse-bridge in the foreground on this OS",
            ))
        }
    }

    fn describe(self) -> &'static str {
        match self {
            Self::Systemd => "systemd user unit (Linux)",
            Self::Launchd => "LaunchAgent (macOS)",
            Self::WindowsTask => "logon Scheduled Task (Windows)",
        }
    }
}

/// Service configuration: explicit `serve` flags over ambient env over defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedConfig {
    /// TCP port the service listens on (loopback).
    pub port: u16,
    /// Workspace root baked into the service definition, if any.
    pub workspace_root: Option<PathBuf>,
    /// One of the four wire approval modes.
    pub approval_mode: String,
    /// `muse` binary for the backend child.
    pub muse_bin: String,
    /// Pass `--trust-workspace` to the backend.
    pub trust_workspace: bool,
    /// Bridge log format.
    pub log_format: LogFormat,
    /// Ambient env carried into the definition (`MUSE_SERVE_ARGS`, `RUST_LOG`
    /// when set and non-empty). systemd/launchd bake these; Windows tasks
    /// inherit the user's persistent environment instead (warned at install).
    pub env_extra: Vec<(String, String)>,
}

/// Resolve [`ServeArgs`] against the process environment.
pub fn resolve(args: &ServeArgs) -> Result<ResolvedConfig, ServeError> {
    resolve_with(args, &|key| std::env::var(key).ok())
}

/// Resolve against an injectable env lookup (deterministic under test).
pub fn resolve_with(
    args: &ServeArgs,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<ResolvedConfig, ServeError> {
    let port = match args.port {
        Some(port) => port,
        None => match env("MUSE_BRIDGE_PORT") {
            None => DEFAULT_PORT,
            Some(raw) => raw.parse::<u16>().map_err(|_| {
                ServeError::usage(format!(
                    "MUSE_BRIDGE_PORT must be a port number, got '{raw}'"
                ))
            })?,
        },
    };
    if port == 0 {
        return Err(ServeError::usage(
            "port 0 is not valid for a service (the OS would assign a random port)",
        ));
    }
    let approval_mode = args
        .approval_mode
        .clone()
        .unwrap_or_else(|| DEFAULT_APPROVAL_MODE.to_string());
    // Same vocabulary and message as dispatch startup validation.
    if !matches!(
        approval_mode.as_str(),
        "allowAll" | "promptUnmatched" | "onRequest" | "denyUnmatched"
    ) {
        return Err(ServeError::usage(format!(
            "approval mode must be allowAll|promptUnmatched|onRequest|denyUnmatched, got '{approval_mode}'"
        )));
    }
    let mut env_extra = Vec::new();
    for key in ["MUSE_SERVE_ARGS", "RUST_LOG"] {
        if let Some(value) = env(key)
            && !value.is_empty()
        {
            env_extra.push((key.to_string(), value));
        }
    }
    Ok(ResolvedConfig {
        port,
        workspace_root: args.workspace_root.clone(),
        approval_mode,
        muse_bin: args
            .muse_bin
            .clone()
            .or_else(|| env("MUSE_CLI"))
            .unwrap_or_else(|| DEFAULT_MUSE_BIN.to_string()),
        trust_workspace: args.trust_workspace,
        log_format: args.log_format.unwrap_or(LogFormat::Pretty),
        env_extra,
    })
}

/// Resolve the backend binary to an absolute path using the INSTALLING shell's
/// `PATH`, failing fast when it cannot be found. A service's environment is
/// bare (systemd's minimal `PATH` lacks `~/.local/bin`), so baking a bare
/// `muse` would install a crash-looping service. Verified existence here is
/// what makes `serve` fail loudly instead of `enable --now`-ing a dud.
pub fn resolve_backend_bin(raw: &str) -> Result<String, ServeError> {
    resolve_backend_bin_with(
        raw,
        &std::env::var("PATH").unwrap_or_default(),
        &std::env::var("PATHEXT").unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_string()),
    )
}

/// Resolve against injectable `PATH`/`PATHEXT` (deterministic under test).
/// Separator-containing values pass through after an existence check; bare
/// names are searched (Windows reuses the spawn path's `host_bin` resolver,
/// so `serve` finds exactly what the bridge would launch).
pub fn resolve_backend_bin_with(
    raw: &str,
    path_env: &str,
    pathext: &str,
) -> Result<String, ServeError> {
    let located = if raw.contains('/') || raw.contains('\\') {
        raw.to_string()
    } else if cfg!(windows) {
        match resolve_host_bin(raw, path_env, pathext) {
            ResolvedHost::Direct(program) | ResolvedHost::Script(program) => program,
        }
    } else {
        std::env::split_paths(path_env)
            .map(|dir| dir.join(raw))
            .find(|candidate| candidate.is_file())
            .map(|found| found.to_string_lossy().into_owned())
            .unwrap_or_else(|| raw.to_string())
    };
    if !std::path::Path::new(&located).is_file() {
        return Err(ServeError::runtime(format!(
            "Muse CLI not found: '{raw}' (searched PATH). Install Muse Code, or pass --muse-bin /absolute/path/to/muse"
        )));
    }
    Ok(located)
}

/// Bridge argv baked into the service definition. Fully explicit: a service's
/// environment is bare, so nothing is left to ambient defaults. Never
/// includes `--bind`/`--allow-remote` (loopback-only by design).
pub fn bridge_argv(cfg: &ResolvedConfig) -> Vec<String> {
    let mut argv = vec!["--port".to_string(), cfg.port.to_string()];
    if let Some(root) = &cfg.workspace_root {
        argv.push("--workspace-root".to_string());
        argv.push(root.display().to_string());
    }
    argv.push("--approval-mode".to_string());
    argv.push(cfg.approval_mode.clone());
    argv.push("--muse-bin".to_string());
    argv.push(cfg.muse_bin.clone());
    if cfg.trust_workspace {
        argv.push("--trust-workspace".to_string());
    }
    argv.push("--log-format".to_string());
    argv.push(
        match cfg.log_format {
            LogFormat::Pretty => "pretty",
            LogFormat::Json => "json",
        }
        .to_string(),
    );
    argv
}

/// Quote one systemd `ExecStart` word (shell-like splitting: quote when the
/// word contains whitespace, quotes, or backslashes).
fn systemd_word(word: &str) -> String {
    if word
        .chars()
        .any(|c| c.is_whitespace() || c == '"' || c == '\'' || c == '\\')
    {
        format!("\"{}\"", word.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        word.to_string()
    }
}

/// Escape a value for a double-quoted systemd `Environment="KEY=VALUE"` line.
fn systemd_env_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Render the systemd user unit (pure; golden-tested).
pub fn render_systemd_unit(exe: &str, cfg: &ResolvedConfig) -> String {
    let mut exec = vec![systemd_word(exe)];
    exec.extend(bridge_argv(cfg).iter().map(|word| systemd_word(word)));
    let mut unit = format!(
        "[Unit]\nDescription=muse-bridge localhost OpenAI-compatible API (127.0.0.1:{port})\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=simple\nExecStart={exec}\nRestart=on-failure\nRestartSec=2\n",
        port = cfg.port,
        exec = exec.join(" ")
    );
    for (key, value) in &cfg.env_extra {
        unit.push_str(&format!(
            "Environment=\"{key}={value}\"\n",
            value = systemd_env_value(value)
        ));
    }
    unit.push_str("\n[Install]\nWantedBy=default.target\n");
    unit
}

/// Unit path: `$XDG_CONFIG_HOME/systemd/user/` or `~/.config/systemd/user/`.
pub fn systemd_unit_path(xdg_config_home: Option<&str>, home: &str) -> PathBuf {
    let base = match xdg_config_home {
        Some(dir) => PathBuf::from(dir),
        None => PathBuf::from(home).join(".config"),
    };
    base.join("systemd").join("user").join(SYSTEMD_UNIT)
}

/// Escape a value for plist/XML text.
fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Render the macOS LaunchAgent plist (pure; golden-tested).
/// `KeepAlive{SuccessfulExit:false}` mirrors systemd `Restart=on-failure`.
pub fn render_launchd_plist(exe: &str, cfg: &ResolvedConfig, log_path: &str) -> String {
    let mut args = String::new();
    for word in std::iter::once(exe.to_string()).chain(bridge_argv(cfg)) {
        args.push_str(&format!("\t\t<string>{}</string>\n", xml_escape(&word)));
    }
    let mut env_block = String::new();
    if !cfg.env_extra.is_empty() {
        env_block.push_str("\t<key>EnvironmentVariables</key>\n\t<dict>\n");
        for (key, value) in &cfg.env_extra {
            env_block.push_str(&format!(
                "\t\t<key>{}</key>\n\t\t<string>{}</string>\n",
                xml_escape(key),
                xml_escape(value)
            ));
        }
        env_block.push_str("\t</dict>\n");
    }
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\">\n<dict>\n\t<key>Label</key>\n\t<string>{SERVICE_NAME}</string>\n\t<key>ProgramArguments</key>\n\t<array>\n{args}\t</array>\n\t<key>RunAtLoad</key>\n\t<true/>\n\t<key>KeepAlive</key>\n\t<dict>\n\t\t<key>SuccessfulExit</key>\n\t\t<false/>\n\t</dict>\n\t<key>ThrottleInterval</key>\n\t<integer>10</integer>\n\t<key>StandardOutPath</key>\n\t<string>{log}</string>\n\t<key>StandardErrorPath</key>\n\t<string>{log}</string>\n{env_block}</dict>\n</plist>\n",
        log = xml_escape(log_path)
    )
}

/// Plist path: `~/Library/LaunchAgents/muse-bridge.plist`.
pub fn launchd_plist_path(home: &str) -> PathBuf {
    PathBuf::from(home)
        .join("Library")
        .join("LaunchAgents")
        .join(LAUNCHD_PLIST)
}

/// Log path for the LaunchAgent: `~/Library/Logs/muse-bridge.log`.
pub fn launchd_log_path(home: &str) -> PathBuf {
    PathBuf::from(home)
        .join("Library")
        .join("Logs")
        .join(LAUNCHD_LOG)
}

/// Quote one word for `cmd /c`. Words containing `cmd` metacharacters are
/// double-quoted; a literal `"` is refused (mangled quoting would run the
/// wrong command — fail loudly instead). Callers pre-escape `%` as `%%`
/// because `cmd` expands `%VAR%` even inside quotes.
fn cmd_quote(word: &str) -> Result<String, ServeError> {
    if word.contains('"') {
        return Err(ServeError::runtime(format!(
            "value contains a double quote and cannot be scheduled on Windows: {word}"
        )));
    }
    if word
        .chars()
        .any(|c| c.is_whitespace() || matches!(c, '&' | '|' | '<' | '>' | '(' | ')' | '^' | '!'))
    {
        Ok(format!("\"{word}\""))
    } else {
        Ok(word.to_string())
    }
}

/// Render the Scheduled Task XML (pure; golden-tested). `InteractiveToken`
/// runs the task as the logged-on user (no stored password, inherits
/// `muse login`); `RestartOnFailure` mirrors systemd `Restart=on-failure`.
/// Output goes to a log file because tasks capture no console output.
pub fn render_schtasks_xml(
    exe: &str,
    cfg: &ResolvedConfig,
    system_root: &str,
    work_dir: &str,
    log_path: &str,
) -> Result<String, ServeError> {
    let cmd = format!("{}\\System32\\cmd.exe", system_root.trim_end_matches('\\'));
    let mut inner = format!("\"{}\"", exe.replace('%', "%%"));
    if exe.contains('"') {
        return Err(ServeError::runtime(format!(
            "value contains a double quote and cannot be scheduled on Windows: {exe}"
        )));
    }
    for word in bridge_argv(cfg) {
        inner.push(' ');
        inner.push_str(&cmd_quote(&word.replace('%', "%%"))?);
    }
    if log_path.contains('"') {
        return Err(ServeError::runtime(format!(
            "value contains a double quote and cannot be scheduled on Windows: {log_path}"
        )));
    }
    inner.push_str(&format!(" >> \"{}\" 2>&1", log_path.replace('%', "%%")));
    // cmd's `/c "..."` form: cmd strips the outer pair (its documented
    // first/last-quote rule) and runs the inner command, quoted exe intact.
    let arguments = format!("/c \"{inner}\"");
    Ok(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<Task version=\"1.4\" xmlns=\"http://schemas.microsoft.com/windows/2004/02/mit/task\">\n  <RegistrationInfo>\n    <Description>muse-bridge localhost OpenAI-compatible API (127.0.0.1:{port})</Description>\n  </RegistrationInfo>\n  <Triggers>\n    <LogonTrigger>\n      <Enabled>true</Enabled>\n    </LogonTrigger>\n  </Triggers>\n  <Principals>\n    <Principal id=\"Author\">\n      <LogonType>InteractiveToken</LogonType>\n      <RunLevel>LeastPrivilege</RunLevel>\n    </Principal>\n  </Principals>\n  <Settings>\n    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>\n    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>\n    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>\n    <AllowStartOnDemand>true</AllowStartOnDemand>\n    <StartWhenAvailable>true</StartWhenAvailable>\n    <Enabled>true</Enabled>\n    <Hidden>false</Hidden>\n    <AllowHardTerminate>true</AllowHardTerminate>\n    <RestartOnFailure>\n      <Interval>PT1M</Interval>\n      <Count>999</Count>\n    </RestartOnFailure>\n  </Settings>\n  <Actions Context=\"Author\">\n    <Exec>\n      <Command>{cmd}</Command>\n      <Arguments>{args}</Arguments>\n      <WorkingDirectory>{work}</WorkingDirectory>\n    </Exec>\n  </Actions>\n</Task>\n",
        port = cfg.port,
        cmd = xml_escape(&cmd),
        args = xml_escape(&arguments),
        work = xml_escape(work_dir)
    ))
}

/// Encode task XML as UTF-16LE with BOM for `schtasks /create /xml`.
pub fn utf16le_bom(xml: &str) -> Vec<u8> {
    let mut bytes = vec![0xFF, 0xFE];
    for unit in xml.encode_utf16() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    bytes
}

/// First line of tool output (error summaries stay one line).
fn first_line(text: &str) -> &str {
    text.lines().next().unwrap_or_default()
}

/// Shell-ish rendering of a command for `--dry-run` and manual-fix hints.
pub fn display_cmd(prog: &str, args: &[String]) -> String {
    let mut parts = vec![prog.to_string()];
    for arg in args {
        if arg.chars().any(|c| c.is_whitespace() || c == '"') {
            parts.push(format!("\"{}\"", arg.replace('"', "\\\"")));
        } else {
            parts.push(arg.clone());
        }
    }
    parts.join(" ")
}

/// Captured child result (text decoded lossily; output is ASCII diagnostics).
struct CmdOut {
    success: bool,
    stdout: String,
    stderr: String,
}

/// Run one child with [`CMD_TIMEOUT`]. Never hangs, never prompts.
async fn run_cmd(prog: &str, args: &[String]) -> Result<CmdOut, ServeError> {
    let output = tokio::time::timeout(
        CMD_TIMEOUT,
        tokio::process::Command::new(prog).args(args).output(),
    )
    .await
    .map_err(|_| ServeError::runtime(format!("{prog} timed out after {}s", CMD_TIMEOUT.as_secs())))?
    .map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            ServeError::runtime(format!("'{prog}' not found on PATH"))
        } else {
            ServeError::runtime(format!("failed to run {prog}: {e}"))
        }
    })?;
    Ok(CmdOut {
        success: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

/// Single `/healthz` probe (2 s budget).
async fn health_ok(port: u16) -> bool {
    let url = format!("http://127.0.0.1:{port}/healthz");
    match tokio::time::timeout(Duration::from_secs(2), reqwest::get(url)).await {
        Ok(Ok(resp)) => resp.status().is_success(),
        _ => false,
    }
}

/// Poll `/healthz` until it answers or [`HEALTH_POLL_BUDGET`] expires.
async fn wait_healthy(port: u16) -> bool {
    let deadline = tokio::time::Instant::now() + HEALTH_POLL_BUDGET;
    loop {
        if health_ok(port).await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// --- Tailscale forward ------------------------------------------------------
// Verified against tailscale 1.102.4: per-port removal is
// `tailscale serve --tcp=<port> off` (never `reset`: that wipes every mapping
// on the node, including unrelated projects). Mutation needs root or a
// one-time `sudo tailscale set --operator=$USER`.

/// `tailscale serve --bg --tcp=<port> tcp://127.0.0.1:<port>` argv.
pub fn tailscale_set_argv(port: u16) -> Vec<String> {
    vec![
        "serve".to_string(),
        "--bg".to_string(),
        format!("--tcp={port}"),
        format!("tcp://127.0.0.1:{port}"),
    ]
}

/// `tailscale serve --tcp=<port> off` argv (scoped removal).
pub fn tailscale_off_argv(port: u16) -> Vec<String> {
    vec![
        "serve".to_string(),
        format!("--tcp={port}"),
        "off".to_string(),
    ]
}

/// `tailscale serve status --json` argv (unprivileged read).
pub fn tailscale_status_argv() -> Vec<String> {
    vec![
        "serve".to_string(),
        "status".to_string(),
        "--json".to_string(),
    ]
}

/// Whether `status --json` shows this port forwarded to loopback. Malformed
/// input means "not confirmed" (false), never an error: callers fall back to
/// the idempotent set, or attempt the removal anyway.
pub fn forward_present(status_json: &str, port: u16) -> bool {
    let Ok(doc) = serde_json::from_str::<serde_json::Value>(status_json) else {
        return false;
    };
    let key = port.to_string();
    match doc
        .get("TCP")
        .and_then(|tcp| tcp.get(&key))
        .and_then(|entry| entry.get("TCPForward"))
        .and_then(|target| target.as_str())
    {
        Some(target) => {
            target == format!("127.0.0.1:{port}") || target == format!("localhost:{port}")
        }
        None => false,
    }
}

/// Whether tailscale stderr is the operator/root refusal (retryable via sudo).
pub fn access_denied(stderr: &str) -> bool {
    stderr.contains("Access denied")
        || stderr.contains("serve config denied")
        || stderr.contains("set --operator")
}

/// Run a mutating tailscale command: direct first, then one `sudo -n` retry
/// (fails fast, never prompts) on access-denied. Returns the winning output
/// plus whether sudo was used. A failed sudo falls back to the direct output
/// so callers report the access-denied remediation, not sudo noise.
async fn run_tailscale(args: &[String]) -> Result<(CmdOut, bool), ServeError> {
    let direct = run_cmd("tailscale", args).await?;
    if direct.success || !access_denied(&direct.stderr) {
        return Ok((direct, false));
    }
    let mut sudo_args = vec!["-n".to_string(), "tailscale".to_string()];
    sudo_args.extend_from_slice(args);
    match run_cmd("sudo", &sudo_args).await {
        Ok(elevated) if elevated.success => Ok((elevated, true)),
        _ => Ok((direct, false)),
    }
}

/// Access-denied failure with the exact manual fix.
fn tailscale_denied(action: &str, argv: &[String]) -> ServeError {
    ServeError::runtime(format!(
        "tailscale refused to {action} (access denied). Run `sudo tailscale set --operator=$USER` once, then re-run; or run it manually: sudo {}",
        display_cmd("tailscale", argv)
    ))
}

// --- Native argv builders (shared by real runs, dry-runs, and tests) --------

fn systemd_reload_argv() -> Vec<String> {
    vec!["--user".to_string(), "daemon-reload".to_string()]
}

fn systemd_enable_now_argv() -> Vec<String> {
    vec![
        "--user".to_string(),
        "enable".to_string(),
        "--now".to_string(),
        SYSTEMD_UNIT.to_string(),
    ]
}

fn systemd_enable_argv() -> Vec<String> {
    vec![
        "--user".to_string(),
        "enable".to_string(),
        SYSTEMD_UNIT.to_string(),
    ]
}

fn systemd_restart_argv() -> Vec<String> {
    vec![
        "--user".to_string(),
        "restart".to_string(),
        SYSTEMD_UNIT.to_string(),
    ]
}

/// How `serve` (re)starts the unit after writing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartMode {
    /// Fresh or unchanged definition: start only if not already active.
    Start,
    /// Changed definition: restart so it takes effect promptly.
    Restart,
}

fn systemd_start_mode(state: FileState) -> StartMode {
    match state {
        FileState::Updated => StartMode::Restart,
        FileState::New | FileState::Unchanged => StartMode::Start,
    }
}

/// Whether schtasks stderr is the benign already-running refusal (re-serve
/// while the old instance is up), as opposed to a real start failure.
fn already_running(stderr: &str) -> bool {
    stderr.to_lowercase().contains("running")
}

fn systemd_disable_now_argv() -> Vec<String> {
    vec![
        "--user".to_string(),
        "disable".to_string(),
        "--now".to_string(),
        SYSTEMD_UNIT.to_string(),
    ]
}

fn systemd_is_active_argv() -> Vec<String> {
    vec![
        "--user".to_string(),
        "is-active".to_string(),
        "--quiet".to_string(),
        SYSTEMD_UNIT.to_string(),
    ]
}

fn systemd_active_state_argv() -> Vec<String> {
    vec![
        "--user".to_string(),
        "is-active".to_string(),
        SYSTEMD_UNIT.to_string(),
    ]
}

fn systemd_enabled_state_argv() -> Vec<String> {
    vec![
        "--user".to_string(),
        "is-enabled".to_string(),
        SYSTEMD_UNIT.to_string(),
    ]
}

fn systemd_restart_hint() -> String {
    format!("systemctl --user restart {SYSTEMD_UNIT}")
}

fn launchd_enable_argv(uid: &str) -> Vec<String> {
    vec!["enable".to_string(), format!("gui/{uid}/{SERVICE_NAME}")]
}

fn launchd_bootstrap_argv(uid: &str, plist: &str) -> Vec<String> {
    vec![
        "bootstrap".to_string(),
        format!("gui/{uid}"),
        plist.to_string(),
    ]
}

fn launchd_bootout_argv(uid: &str) -> Vec<String> {
    vec!["bootout".to_string(), format!("gui/{uid}/{SERVICE_NAME}")]
}

fn launchd_disable_argv(uid: &str) -> Vec<String> {
    vec!["disable".to_string(), format!("gui/{uid}/{SERVICE_NAME}")]
}

fn launchd_print_argv(uid: &str) -> Vec<String> {
    vec!["print".to_string(), format!("gui/{uid}/{SERVICE_NAME}")]
}

fn launchd_kickstart_hint(uid: &str) -> String {
    format!("launchctl kickstart -k gui/{uid}/{SERVICE_NAME}")
}

fn schtasks_create_argv(xml_path: &str) -> Vec<String> {
    vec![
        "/create".to_string(),
        "/tn".to_string(),
        SERVICE_NAME.to_string(),
        "/xml".to_string(),
        xml_path.to_string(),
        "/f".to_string(),
    ]
}

fn schtasks_change_argv(enable: bool) -> Vec<String> {
    vec![
        "/change".to_string(),
        "/tn".to_string(),
        SERVICE_NAME.to_string(),
        if enable { "/enable" } else { "/disable" }.to_string(),
    ]
}

fn schtasks_run_argv() -> Vec<String> {
    vec![
        "/run".to_string(),
        "/tn".to_string(),
        SERVICE_NAME.to_string(),
    ]
}

fn schtasks_end_argv() -> Vec<String> {
    vec![
        "/end".to_string(),
        "/tn".to_string(),
        SERVICE_NAME.to_string(),
    ]
}

fn schtasks_query_argv() -> Vec<String> {
    vec![
        "/query".to_string(),
        "/tn".to_string(),
        SERVICE_NAME.to_string(),
        "/fo".to_string(),
        "LIST".to_string(),
        "/v".to_string(),
    ]
}

// --- Entry point ------------------------------------------------------------

/// Run the `serve` subcommand, reporting to `out` (stdout). Writes go only to
/// the platform's service definition; `--dry-run` runs zero subprocesses.
pub async fn run_serve(
    args: &ServeArgs,
    out: &mut (dyn std::io::Write + Send),
) -> Result<(), ServeError> {
    run_serve_with(
        args,
        out,
        &|key| std::env::var(key).ok(),
        &resolve_backend_bin,
    )
    .await
}

/// Injectable-env variant (deterministic under test; production delegates
/// with the process environment and the real `PATH` resolver).
pub async fn run_serve_with(
    args: &ServeArgs,
    out: &mut (dyn std::io::Write + Send),
    env: &dyn Fn(&str) -> Option<String>,
    resolve_bin: &dyn Fn(&str) -> Result<String, ServeError>,
) -> Result<(), ServeError> {
    if args.off && args.status {
        return Err(ServeError::usage(
            "--status and --off are mutually exclusive",
        ));
    }
    let mut cfg = resolve_with(args, env)?;
    let platform = Platform::current()?;
    if args.status {
        return serve_status(platform, &cfg, args.tailscale, args.dry_run, out).await;
    }
    if args.off {
        return serve_off(platform, &cfg, args.tailscale, args.dry_run, out).await;
    }
    // Absolutize before anything else (including dry-run output): a missing
    // backend fails here, never as a crash-looping service. `--off`/`--status`
    // skip this on purpose — you must be able to turn off a broken install.
    let backend = resolve_bin(&cfg.muse_bin)?;
    if backend != cfg.muse_bin {
        say!(out, "backend: '{}' resolves to {backend}", cfg.muse_bin);
        cfg.muse_bin = backend;
    }
    serve_on(platform, &cfg, args.tailscale, args.dry_run, out).await
}

fn home_dir() -> Result<String, ServeError> {
    std::env::var("HOME").map_err(|_| ServeError::runtime("HOME is not set"))
}

fn current_exe_string() -> Result<String, ServeError> {
    std::env::current_exe()
        .map(|path| path.display().to_string())
        .map_err(|e| ServeError::runtime(format!("cannot locate the running binary: {e}")))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileState {
    New,
    Updated,
    Unchanged,
}

impl FileState {
    fn describe(self) -> &'static str {
        match self {
            Self::New => "installed",
            Self::Updated => "updated",
            Self::Unchanged => "already installed",
        }
    }
}

/// Write a service definition file, creating parents. Reports whether the
/// file is new, changed, or already identical (idempotent re-runs).
fn write_definition(path: &std::path::Path, content: &[u8]) -> Result<FileState, ServeError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| ServeError::runtime(format!("cannot create {}: {e}", parent.display())))?;
    }
    let state = match std::fs::read(path) {
        Err(_) => FileState::New,
        Ok(existing) if existing == content => FileState::Unchanged,
        Ok(_) => FileState::Updated,
    };
    if state != FileState::Unchanged {
        std::fs::write(path, content)
            .map_err(|e| ServeError::runtime(format!("cannot write {}: {e}", path.display())))?;
    }
    Ok(state)
}

/// Login uid for launchd's `gui/<uid>/` domain.
async fn launchd_uid() -> Result<String, ServeError> {
    let id = run_cmd("id", &["-u".to_string()]).await?;
    if !id.success {
        return Err(ServeError::runtime(
            "cannot determine the login uid (`id -u` failed)",
        ));
    }
    Ok(id.stdout.trim().to_string())
}

fn log_hint(platform: Platform) -> &'static str {
    match platform {
        Platform::Systemd => "inspect with `journalctl --user -u muse-bridge.service -e`",
        Platform::Launchd => "inspect with `tail ~/Library/Logs/muse-bridge.log`",
        Platform::WindowsTask => "inspect `%TEMP%\\muse-bridge.service.log`",
    }
}

/// Install, enable, start, and verify the service.
#[allow(clippy::too_many_lines)]
async fn serve_on(
    platform: Platform,
    cfg: &ResolvedConfig,
    tailscale: bool,
    dry_run: bool,
    out: &mut (dyn std::io::Write + Send),
) -> Result<(), ServeError> {
    let exe = current_exe_string()?;
    if dry_run {
        return dry_run_on(platform, cfg, &exe, tailscale, out);
    }
    // Per-platform restart command for the closing note (re-running `serve`
    // never restarts a healthy service, so rebuilds need an explicit restart).
    let restart = match platform {
        Platform::Systemd => {
            // Preflight before writing anything.
            let probe = run_cmd(
                "systemctl",
                &["--user".to_string(), "show-environment".to_string()],
            )
            .await?;
            if !probe.success {
                return Err(ServeError::runtime(format!(
                    "systemd user manager is not reachable ({}); run muse-bridge in the foreground instead",
                    first_line(&probe.stderr)
                )));
            }
            let home = home_dir()?;
            let path = systemd_unit_path(std::env::var("XDG_CONFIG_HOME").ok().as_deref(), &home);
            let state = write_definition(&path, render_systemd_unit(&exe, cfg).as_bytes())?;
            say!(
                out,
                "service definition {}: {}",
                state.describe(),
                path.display()
            );
            let reload = run_cmd("systemctl", &systemd_reload_argv()).await?;
            if !reload.success {
                return Err(ServeError::runtime(format!(
                    "systemctl daemon-reload failed: {}",
                    first_line(&reload.stderr)
                )));
            }
            // A changed definition restarts so it takes effect promptly (and
            // clears any backoff); new/unchanged never restart a healthy
            // service — re-running `serve` must not drop in-flight turns.
            let restarted = systemd_start_mode(state) == StartMode::Restart;
            if restarted {
                let enable = run_cmd("systemctl", &systemd_enable_argv()).await?;
                if !enable.success {
                    return Err(ServeError::runtime(format!(
                        "failed to enable the service: {}",
                        first_line(&enable.stderr)
                    )));
                }
                let restart = run_cmd("systemctl", &systemd_restart_argv()).await?;
                if !restart.success {
                    return Err(ServeError::runtime(format!(
                        "failed to restart the service: {}",
                        first_line(&restart.stderr)
                    )));
                }
            } else {
                let enable = run_cmd("systemctl", &systemd_enable_now_argv()).await?;
                if !enable.success {
                    return Err(ServeError::runtime(format!(
                        "failed to enable+start the service: {}",
                        first_line(&enable.stderr)
                    )));
                }
            }
            say!(
                out,
                "service enabled and {}",
                if restarted { "restarted" } else { "started" }
            );
            systemd_restart_hint()
        }
        Platform::Launchd => {
            let uid = launchd_uid().await?;
            let home = home_dir()?;
            let path = launchd_plist_path(&home);
            let log = launchd_log_path(&home);
            let content = render_launchd_plist(&exe, cfg, &log.display().to_string());
            let state = write_definition(&path, content.as_bytes())?;
            say!(
                out,
                "service definition {}: {}",
                state.describe(),
                path.display()
            );
            // bootout-first makes re-runs idempotent without parsing "already
            // loaded" failures; a missing job fails here and is ignored.
            let _ = run_cmd("launchctl", &launchd_bootout_argv(&uid)).await;
            let enable = run_cmd("launchctl", &launchd_enable_argv(&uid)).await?;
            if !enable.success {
                return Err(ServeError::runtime(format!(
                    "launchctl enable failed: {}",
                    first_line(&enable.stderr)
                )));
            }
            let boot = run_cmd(
                "launchctl",
                &launchd_bootstrap_argv(&uid, &path.display().to_string()),
            )
            .await?;
            if !boot.success {
                return Err(ServeError::runtime(format!(
                    "launchctl bootstrap failed: {}",
                    first_line(&boot.stderr)
                )));
            }
            say!(out, "service enabled and started");
            launchd_kickstart_hint(&uid)
        }
        Platform::WindowsTask => {
            if !cfg.env_extra.is_empty() {
                let keys: Vec<&str> = cfg.env_extra.iter().map(|(key, _)| key.as_str()).collect();
                say!(
                    out,
                    "note: {} from this shell are not carried into the Scheduled Task on Windows (tasks inherit your persistent user environment instead)",
                    keys.join(", ")
                );
            }
            let system_root =
                std::env::var("SystemRoot").unwrap_or_else(|_| "C:\\Windows".to_string());
            let work_dir = std::env::temp_dir();
            let log_path = work_dir.join(WINDOWS_LOG);
            let xml = render_schtasks_xml(
                &exe,
                cfg,
                &system_root,
                &work_dir.display().to_string(),
                &log_path.display().to_string(),
            )?;
            let import = work_dir.join(format!("muse-bridge-{}.xml", std::process::id()));
            std::fs::write(&import, utf16le_bom(&xml)).map_err(|e| {
                ServeError::runtime(format!("cannot write {}: {e}", import.display()))
            })?;
            let create = run_cmd(
                "schtasks",
                &schtasks_create_argv(&import.display().to_string()),
            )
            .await;
            let _ = std::fs::remove_file(&import);
            let create = create?;
            if !create.success {
                return Err(ServeError::runtime(format!(
                    "schtasks import failed: {}",
                    first_line(&create.stderr)
                )));
            }
            say!(out, "scheduled task {SERVICE_NAME} installed");
            let change = run_cmd("schtasks", &schtasks_change_argv(true)).await?;
            if !change.success {
                return Err(ServeError::runtime(format!(
                    "schtasks enable failed: {}",
                    first_line(&change.stderr)
                )));
            }
            let run = run_cmd("schtasks", &schtasks_run_argv()).await?;
            if !run.success {
                // Benign on re-serve: the old instance keeps serving while the
                // updated definition waits for the next task start.
                if already_running(&run.stderr) {
                    say!(
                        out,
                        "task already running (restart it to pick up service changes: {} then {})",
                        display_cmd("schtasks", &schtasks_end_argv()),
                        display_cmd("schtasks", &schtasks_run_argv())
                    );
                } else {
                    return Err(ServeError::runtime(format!(
                        "schtasks start failed: {}",
                        first_line(&run.stderr)
                    )));
                }
            } else {
                say!(out, "service enabled and started");
            }
            format!("schtasks /end /tn {SERVICE_NAME} then schtasks /run /tn {SERVICE_NAME}")
        }
    };
    if !wait_healthy(cfg.port).await {
        return Err(ServeError::runtime(format!(
            "service started but /healthz is not answering; {}",
            log_hint(platform)
        )));
    }
    if platform == Platform::Systemd {
        // Cross-check: a foreground bridge could be answering while the
        // service itself crash-loops. (launchd/schtasks state parsing is
        // locale-fragile, so only systemd gets the exact check.)
        let active = run_cmd("systemctl", &systemd_is_active_argv()).await?;
        if !active.success {
            return Err(ServeError::runtime(format!(
                "port {} answers /healthz but the service is not active — another process (a foreground muse-bridge?) owns the port; stop it, then run `{}`",
                cfg.port,
                systemd_restart_hint()
            )));
        }
    }
    say!(
        out,
        "healthy: http://127.0.0.1:{}/healthz answers",
        cfg.port
    );
    if tailscale {
        tailscale_on(cfg.port, out).await?;
    } else {
        say!(
            out,
            "tailscale: not managed (pass --tailscale to expose :{} on your tailnet)",
            cfg.port
        );
    }
    linger_hint(out).await;
    say!(
        out,
        "note: the service runs {exe}; re-run `muse-bridge serve` if you move the binary, `{restart}` after rebuilding"
    );
    Ok(())
}

/// Best-effort linger reminder (Linux only): user services stop at logout
/// unless lingering is enabled. Silent when it cannot be determined.
async fn linger_hint(out: &mut (dyn std::io::Write + Send)) {
    if !cfg!(target_os = "linux") {
        return;
    }
    let Ok(user) = std::env::var("USER") else {
        return;
    };
    let probe = run_cmd(
        "loginctl",
        &[
            "show-user".to_string(),
            user.clone(),
            "-p".to_string(),
            "Linger".to_string(),
            "--value".to_string(),
        ],
    )
    .await;
    if let Ok(probe) = probe
        && probe.success
        && probe.stdout.trim() != "yes"
    {
        let _ = writeln!(
            out,
            "note: user services stop at logout unless lingering is on: sudo loginctl enable-linger {user}"
        );
    }
}

/// Set the Tailscale TCP forward (idempotent: skips when already correct).
async fn tailscale_on(port: u16, out: &mut (dyn std::io::Write + Send)) -> Result<(), ServeError> {
    let status = run_cmd("tailscale", &tailscale_status_argv())
        .await
        .map_err(|e| {
            if e.message.contains("not found") {
                ServeError::runtime("'tailscale' not found on PATH but --tailscale was passed")
            } else {
                e
            }
        })?;
    if !status.success {
        return Err(ServeError::runtime(format!(
            "cannot query tailscale serve status: {} (is tailscaled running?)",
            first_line(&status.stderr)
        )));
    }
    if forward_present(&status.stdout, port) {
        say!(
            out,
            "tailscale TCP forward :{port} → 127.0.0.1:{port}: already in place"
        );
        return Ok(());
    }
    let argv = tailscale_set_argv(port);
    let (set, via_sudo) = run_tailscale(&argv).await?;
    if !set.success {
        if access_denied(&set.stderr) {
            return Err(tailscale_denied("set the TCP forward", &argv));
        }
        return Err(ServeError::runtime(format!(
            "tailscale failed to set the TCP forward: {}",
            first_line(&set.stderr)
        )));
    }
    say!(
        out,
        "tailscale TCP forward :{port} → 127.0.0.1:{port}: installed{}",
        if via_sudo { " (via sudo)" } else { "" }
    );
    Ok(())
}

/// Stop and disable the service (definition file kept for cheap re-enable).
async fn serve_off(
    platform: Platform,
    cfg: &ResolvedConfig,
    tailscale: bool,
    dry_run: bool,
    out: &mut (dyn std::io::Write + Send),
) -> Result<(), ServeError> {
    if dry_run {
        return dry_run_off(platform, cfg, tailscale, out);
    }
    match platform {
        Platform::Systemd => {
            let off = run_cmd("systemctl", &systemd_disable_now_argv()).await?;
            if !off.success {
                if off.stderr.contains("does not exist") {
                    say!(out, "service is not installed — bridge already off");
                } else {
                    return Err(ServeError::runtime(format!(
                        "failed to stop+disable the service: {}",
                        first_line(&off.stderr)
                    )));
                }
            } else {
                say!(out, "service stopped and disabled (definition file kept)");
            }
        }
        Platform::Launchd => {
            let uid = launchd_uid().await?;
            let out_ = run_cmd("launchctl", &launchd_bootout_argv(&uid)).await?;
            if !out_.success && !out_.stderr.contains("Could not find service") {
                return Err(ServeError::runtime(format!(
                    "launchctl bootout failed: {}",
                    first_line(&out_.stderr)
                )));
            }
            let disable = run_cmd("launchctl", &launchd_disable_argv(&uid)).await?;
            if !disable.success {
                return Err(ServeError::runtime(format!(
                    "launchctl disable failed: {}",
                    first_line(&disable.stderr)
                )));
            }
            if out_.success {
                say!(out, "service stopped and disabled (definition file kept)");
            } else {
                say!(out, "service is not installed — bridge already off");
            }
        }
        Platform::WindowsTask => {
            // Best-effort stop: fails when the task is not running, which is fine.
            let _ = run_cmd("schtasks", &schtasks_end_argv()).await;
            let change = run_cmd("schtasks", &schtasks_change_argv(false)).await?;
            if !change.success {
                if change.stderr.contains("cannot find") || change.stderr.contains("does not exist")
                {
                    say!(out, "service is not installed — bridge already off");
                } else {
                    return Err(ServeError::runtime(format!(
                        "schtasks disable failed: {}",
                        first_line(&change.stderr)
                    )));
                }
            } else {
                say!(out, "service stopped and disabled (task definition kept)");
            }
        }
    }
    if tailscale {
        tailscale_off(cfg.port, out).await?;
    } else {
        say!(
            out,
            "tailscale: forward (if any) left untouched (`serve --off --tailscale` removes it)"
        );
    }
    Ok(())
}

/// Remove the Tailscale TCP forward (scoped: never `reset`).
async fn tailscale_off(port: u16, out: &mut (dyn std::io::Write + Send)) -> Result<(), ServeError> {
    let status = match run_cmd("tailscale", &tailscale_status_argv()).await {
        Ok(status) => status,
        Err(e) => {
            say!(
                out,
                "tailscale: {} — forward (if any) left in place",
                e.message
            );
            return Ok(());
        }
    };
    if status.success && !forward_present(&status.stdout, port) {
        say!(out, "tailscale: no forward for :{port} — nothing to remove");
        return Ok(());
    }
    // Present, or state unknown (status failed): attempt the scoped removal.
    let argv = tailscale_off_argv(port);
    let (off, via_sudo) = run_tailscale(&argv).await?;
    if !off.success {
        if access_denied(&off.stderr) {
            return Err(tailscale_denied("remove the TCP forward", &argv));
        }
        if status.success {
            return Err(ServeError::runtime(format!(
                "tailscale failed to remove the TCP forward: {}",
                first_line(&off.stderr)
            )));
        }
        say!(
            out,
            "tailscale: could not confirm removal ({})",
            first_line(&off.stderr)
        );
        return Ok(());
    }
    say!(
        out,
        "tailscale TCP forward :{port} removed{}",
        if via_sudo { " (via sudo)" } else { "" }
    );
    Ok(())
}

/// Report service state + `/healthz`. Exits 0 iff the bridge answers.
async fn serve_status(
    platform: Platform,
    cfg: &ResolvedConfig,
    tailscale: bool,
    dry_run: bool,
    out: &mut (dyn std::io::Write + Send),
) -> Result<(), ServeError> {
    if dry_run {
        return dry_run_status(platform, cfg, tailscale, out);
    }
    match platform {
        Platform::Systemd => {
            let active = run_cmd("systemctl", &systemd_active_state_argv()).await?;
            let enabled = run_cmd("systemctl", &systemd_enabled_state_argv()).await?;
            // is-active/is-enabled exit nonzero when inactive — stdout still
            // carries the state. A blank enabled-state with "does not exist"
            // means no unit file at all.
            if !enabled.success
                && enabled.stdout.trim().is_empty()
                && (enabled.stderr.contains("does not exist") || enabled.stderr.contains("No such"))
            {
                say!(out, "service: not installed");
            } else {
                say!(
                    out,
                    "service: {} ({})",
                    nonblank(first_line(&active.stdout), "unknown"),
                    nonblank(first_line(&enabled.stdout), "unknown")
                );
            }
        }
        Platform::Launchd => match launchd_uid().await {
            Ok(uid) => {
                let print = run_cmd("launchctl", &launchd_print_argv(&uid)).await?;
                if print.success {
                    let state = print
                        .stdout
                        .lines()
                        .map(str::trim)
                        .find(|line| line.starts_with("state = "))
                        .unwrap_or("state unknown");
                    say!(out, "service: loaded ({state})");
                } else {
                    say!(out, "service: not loaded");
                }
            }
            Err(_) => say!(out, "service: unknown (cannot query launchctl)"),
        },
        Platform::WindowsTask => {
            let query = run_cmd("schtasks", &schtasks_query_argv()).await?;
            if query.success {
                let mut shown = false;
                for line in query.stdout.lines().filter(|line| {
                    let trimmed = line.trim_start();
                    trimmed.starts_with("Status:") || trimmed.starts_with("Last Result:")
                }) {
                    say!(out, "task {}", line.trim());
                    shown = true;
                }
                if !shown {
                    say!(out, "task: query answered but carried no status lines");
                }
            } else {
                say!(out, "service: not installed");
            }
        }
    }
    let healthy = health_ok(cfg.port).await;
    say!(
        out,
        "healthy: {} (http://127.0.0.1:{}/healthz)",
        if healthy { "yes" } else { "no" },
        cfg.port
    );
    if tailscale {
        match run_cmd("tailscale", &tailscale_status_argv()).await {
            Ok(status) if status.success => say!(
                out,
                "tailscale forward :{}: {}",
                cfg.port,
                if forward_present(&status.stdout, cfg.port) {
                    "present"
                } else {
                    "absent"
                }
            ),
            Ok(status) => say!(
                out,
                "tailscale: cannot query status ({})",
                first_line(&status.stderr)
            ),
            Err(e) => say!(out, "tailscale: {}", e.message),
        }
    }
    if healthy {
        Ok(())
    } else {
        Err(ServeError::runtime(format!(
            "bridge is not answering http://127.0.0.1:{}/healthz",
            cfg.port
        )))
    }
}

fn nonblank<'a>(value: &'a str, fallback: &'a str) -> &'a str {
    if value.trim().is_empty() {
        fallback
    } else {
        value
    }
}

// --- Dry runs (zero subprocesses) --------------------------------------------

fn dry_run_home() -> String {
    home_dir().unwrap_or_else(|_| "~".to_string())
}

fn dry_run_on(
    platform: Platform,
    cfg: &ResolvedConfig,
    exe: &str,
    tailscale: bool,
    out: &mut (dyn std::io::Write + Send),
) -> Result<(), ServeError> {
    say!(out, "platform: {}", platform.describe());
    match platform {
        Platform::Systemd => {
            let path = systemd_unit_path(
                std::env::var("XDG_CONFIG_HOME").ok().as_deref(),
                &dry_run_home(),
            );
            dry_run_file(
                out,
                &path.display().to_string(),
                &render_systemd_unit(exe, cfg),
            )?;
            say!(out, "commands:");
            say!(
                out,
                "  {}",
                display_cmd("systemctl", &systemd_reload_argv())
            );
            say!(
                out,
                "  {}",
                display_cmd("systemctl", &systemd_enable_now_argv())
            );
        }
        Platform::Launchd => {
            let home = dry_run_home();
            let path = launchd_plist_path(&home);
            let log = launchd_log_path(&home);
            dry_run_file(
                out,
                &path.display().to_string(),
                &render_launchd_plist(exe, cfg, &log.display().to_string()),
            )?;
            let uid = "$(id -u)";
            say!(out, "commands:");
            say!(
                out,
                "  {}",
                display_cmd("launchctl", &launchd_bootout_argv(uid))
            );
            say!(
                out,
                "  {}",
                display_cmd("launchctl", &launchd_enable_argv(uid))
            );
            say!(
                out,
                "  {}",
                display_cmd(
                    "launchctl",
                    &launchd_bootstrap_argv(uid, &path.display().to_string())
                )
            );
        }
        Platform::WindowsTask => {
            let system_root =
                std::env::var("SystemRoot").unwrap_or_else(|_| "C:\\Windows".to_string());
            let work_dir = std::env::temp_dir();
            let log_path = work_dir.join(WINDOWS_LOG);
            let xml = render_schtasks_xml(
                exe,
                cfg,
                &system_root,
                &work_dir.display().to_string(),
                &log_path.display().to_string(),
            )?;
            dry_run_file(out, "<temp-import-file> (deleted after import)", &xml)?;
            say!(out, "commands:");
            say!(
                out,
                "  {}",
                display_cmd("schtasks", &schtasks_create_argv("<temp-import-file>"))
            );
            say!(
                out,
                "  {}",
                display_cmd("schtasks", &schtasks_change_argv(true))
            );
            say!(out, "  {}", display_cmd("schtasks", &schtasks_run_argv()));
        }
    }
    say!(
        out,
        "health: poll http://127.0.0.1:{}/healthz (up to {}s)",
        cfg.port,
        HEALTH_POLL_BUDGET.as_secs()
    );
    if tailscale {
        say!(
            out,
            "tailscale: {}",
            display_cmd("tailscale", &tailscale_set_argv(cfg.port))
        );
    } else {
        say!(
            out,
            "tailscale: not managed (pass --tailscale to run: {})",
            display_cmd("tailscale", &tailscale_set_argv(cfg.port))
        );
    }
    Ok(())
}

fn dry_run_file(
    out: &mut (dyn std::io::Write + Send),
    path: &str,
    content: &str,
) -> Result<(), ServeError> {
    say!(out, "file: {path} (would write)");
    say!(out, "--- {path} ---");
    say!(out, "{}", content.trim_end());
    say!(out, "--- end ---");
    Ok(())
}

fn dry_run_off(
    platform: Platform,
    cfg: &ResolvedConfig,
    tailscale: bool,
    out: &mut (dyn std::io::Write + Send),
) -> Result<(), ServeError> {
    say!(out, "platform: {}", platform.describe());
    say!(out, "commands:");
    match platform {
        Platform::Systemd => say!(
            out,
            "  {}",
            display_cmd("systemctl", &systemd_disable_now_argv())
        ),
        Platform::Launchd => {
            let uid = "$(id -u)";
            say!(
                out,
                "  {}",
                display_cmd("launchctl", &launchd_bootout_argv(uid))
            );
            say!(
                out,
                "  {}",
                display_cmd("launchctl", &launchd_disable_argv(uid))
            );
        }
        Platform::WindowsTask => {
            say!(out, "  {}", display_cmd("schtasks", &schtasks_end_argv()));
            say!(
                out,
                "  {}",
                display_cmd("schtasks", &schtasks_change_argv(false))
            );
        }
    }
    if tailscale {
        say!(
            out,
            "tailscale: {}",
            display_cmd("tailscale", &tailscale_off_argv(cfg.port))
        );
    } else {
        say!(out, "tailscale: forward (if any) left untouched");
    }
    Ok(())
}

fn dry_run_status(
    platform: Platform,
    cfg: &ResolvedConfig,
    tailscale: bool,
    out: &mut (dyn std::io::Write + Send),
) -> Result<(), ServeError> {
    say!(out, "platform: {}", platform.describe());
    say!(out, "commands:");
    match platform {
        Platform::Systemd => {
            say!(
                out,
                "  {}",
                display_cmd("systemctl", &systemd_active_state_argv())
            );
            say!(
                out,
                "  {}",
                display_cmd("systemctl", &systemd_enabled_state_argv())
            );
        }
        Platform::Launchd => say!(
            out,
            "  {}",
            display_cmd("launchctl", &launchd_print_argv("$(id -u)"))
        ),
        Platform::WindowsTask => say!(out, "  {}", display_cmd("schtasks", &schtasks_query_argv())),
    }
    say!(
        out,
        "health: GET http://127.0.0.1:{}/healthz (single probe)",
        cfg.port
    );
    if tailscale {
        say!(
            out,
            "tailscale: {}",
            display_cmd("tailscale", &tailscale_status_argv())
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn serve_args() -> ServeArgs {
        ServeArgs {
            off: false,
            tailscale: false,
            status: false,
            dry_run: false,
            port: None,
            workspace_root: None,
            approval_mode: None,
            muse_bin: None,
            trust_workspace: false,
            log_format: None,
        }
    }

    fn env_of<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key: &str| {
            pairs
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| value.to_string())
        }
    }

    fn test_cfg() -> ResolvedConfig {
        ResolvedConfig {
            port: DEFAULT_PORT,
            workspace_root: None,
            approval_mode: DEFAULT_APPROVAL_MODE.to_string(),
            muse_bin: DEFAULT_MUSE_BIN.to_string(),
            trust_workspace: false,
            log_format: LogFormat::Pretty,
            env_extra: Vec::new(),
        }
    }

    #[test]
    fn resolve_defaults_without_flags_or_env() {
        let cfg = resolve_with(&serve_args(), &env_of(&[])).expect("defaults resolve");
        assert_eq!(cfg.port, 17489);
        assert_eq!(cfg.workspace_root, None);
        assert_eq!(cfg.approval_mode, "denyUnmatched");
        assert_eq!(cfg.muse_bin, "muse");
        assert!(!cfg.trust_workspace);
        assert_eq!(cfg.log_format, LogFormat::Pretty);
        assert!(cfg.env_extra.is_empty());
    }

    #[test]
    fn resolve_explicit_flags_win_over_env() {
        let mut args = serve_args();
        args.port = Some(8646);
        args.approval_mode = Some("allowAll".to_string());
        args.muse_bin = Some("/opt/muse".to_string());
        args.trust_workspace = true;
        args.log_format = Some(LogFormat::Json);
        let env = env_of(&[("MUSE_BRIDGE_PORT", "9999"), ("MUSE_CLI", "/env/muse")]);
        let cfg = resolve_with(&args, &env).expect("explicit flags resolve");
        assert_eq!(cfg.port, 8646);
        assert_eq!(cfg.approval_mode, "allowAll");
        assert_eq!(cfg.muse_bin, "/opt/muse");
        assert!(cfg.trust_workspace);
        assert_eq!(cfg.log_format, LogFormat::Json);
    }

    #[test]
    fn resolve_falls_back_to_env_then_bakes_leftovers() {
        let env = env_of(&[
            ("MUSE_BRIDGE_PORT", "9999"),
            ("MUSE_CLI", "/env/muse"),
            ("MUSE_SERVE_ARGS", "--no-session-log"),
            ("RUST_LOG", "debug"),
            ("MUSE_COMMAND_TIMEOUT_MS", "5000"),
        ]);
        let cfg = resolve_with(&serve_args(), &env).expect("env fallback resolves");
        assert_eq!(cfg.port, 9999);
        assert_eq!(cfg.muse_bin, "/env/muse");
        // Only the two carried vars are baked; every other env key is ignored.
        assert_eq!(
            cfg.env_extra,
            vec![
                (
                    "MUSE_SERVE_ARGS".to_string(),
                    "--no-session-log".to_string()
                ),
                ("RUST_LOG".to_string(), "debug".to_string()),
            ]
        );
    }

    #[test]
    fn resolve_rejects_bad_port_mode_and_empty_values() {
        let env = env_of(&[("MUSE_BRIDGE_PORT", "not-a-port")]);
        let err = resolve_with(&serve_args(), &env).expect_err("bad env port");
        assert!(err.usage);
        assert!(err.message.contains("MUSE_BRIDGE_PORT"), "{}", err.message);

        let mut args = serve_args();
        args.port = Some(0);
        let err = resolve_with(&args, &env_of(&[])).expect_err("port 0");
        assert!(err.usage);

        let mut args = serve_args();
        args.approval_mode = Some("bogus".to_string());
        let err = resolve_with(&args, &env_of(&[])).expect_err("bad mode");
        assert!(err.usage);
        assert_eq!(
            err.message,
            "approval mode must be allowAll|promptUnmatched|onRequest|denyUnmatched, got 'bogus'"
        );

        // Empty carried values are dropped, not baked as empty assignments.
        let env = env_of(&[("RUST_LOG", "")]);
        let cfg = resolve_with(&serve_args(), &env).expect("empty env resolves");
        assert!(cfg.env_extra.is_empty());
    }

    #[test]
    fn bridge_argv_is_fully_explicit_and_loopback_only() {
        let cfg = test_cfg();
        assert_eq!(
            bridge_argv(&cfg),
            vec![
                "--port",
                "17489",
                "--approval-mode",
                "denyUnmatched",
                "--muse-bin",
                "muse",
                "--log-format",
                "pretty",
            ]
        );
        for word in bridge_argv(&cfg) {
            assert!(
                word != "--bind" && word != "--allow-remote",
                "services stay loopback-only: {word}"
            );
        }

        let mut full = test_cfg();
        full.port = 8646;
        full.workspace_root = Some(PathBuf::from("/tmp/ws"));
        full.approval_mode = "allowAll".to_string();
        full.trust_workspace = true;
        full.log_format = LogFormat::Json;
        assert_eq!(
            bridge_argv(&full),
            vec![
                "--port",
                "8646",
                "--workspace-root",
                "/tmp/ws",
                "--approval-mode",
                "allowAll",
                "--muse-bin",
                "muse",
                "--trust-workspace",
                "--log-format",
                "json",
            ]
        );
    }

    #[test]
    fn systemd_unit_golden() {
        let got = render_systemd_unit("/home/josh/.local/bin/muse-bridge", &test_cfg());
        let expected = [
            "[Unit]",
            "Description=muse-bridge localhost OpenAI-compatible API (127.0.0.1:17489)",
            "After=network-online.target",
            "Wants=network-online.target",
            "",
            "[Service]",
            "Type=simple",
            "ExecStart=/home/josh/.local/bin/muse-bridge --port 17489 --approval-mode denyUnmatched --muse-bin muse --log-format pretty",
            "Restart=on-failure",
            "RestartSec=2",
            "",
            "[Install]",
            "WantedBy=default.target",
        ]
        .join("\n")
            + "\n";
        assert_eq!(got, expected);
    }

    #[test]
    fn systemd_unit_quotes_spaces_and_bakes_env() {
        let mut cfg = test_cfg();
        cfg.workspace_root = Some(PathBuf::from("/tmp/my ws"));
        cfg.muse_bin = "/opt/muse cli/muse".to_string();
        cfg.env_extra = vec![
            ("MUSE_SERVE_ARGS".to_string(), "--a 1".to_string()),
            ("RUST_LOG".to_string(), "debug".to_string()),
        ];
        let got = render_systemd_unit("/home/josh/my bins/muse-bridge", &cfg);
        assert!(
            got.contains("ExecStart=\"/home/josh/my bins/muse-bridge\" --port 17489 --workspace-root \"/tmp/my ws\" --approval-mode denyUnmatched --muse-bin \"/opt/muse cli/muse\" --log-format pretty"),
            "{got}"
        );
        assert!(
            got.contains("Environment=\"MUSE_SERVE_ARGS=--a 1\"\n"),
            "{got}"
        );
        assert!(got.contains("Environment=\"RUST_LOG=debug\"\n"), "{got}");
    }

    #[test]
    fn systemd_unit_path_honors_xdg() {
        assert_eq!(
            systemd_unit_path(None, "/home/josh"),
            PathBuf::from("/home/josh/.config/systemd/user/muse-bridge.service")
        );
        assert_eq!(
            systemd_unit_path(Some("/x/cfg"), "/home/josh"),
            PathBuf::from("/x/cfg/systemd/user/muse-bridge.service")
        );
    }

    #[test]
    fn launchd_plist_golden() {
        let got = render_launchd_plist(
            "/Users/josh/.local/bin/muse-bridge",
            &test_cfg(),
            "/Users/josh/Library/Logs/muse-bridge.log",
        );
        let expected = [
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
            "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">",
            "<plist version=\"1.0\">",
            "<dict>",
            "\t<key>Label</key>",
            "\t<string>muse-bridge</string>",
            "\t<key>ProgramArguments</key>",
            "\t<array>",
            "\t\t<string>/Users/josh/.local/bin/muse-bridge</string>",
            "\t\t<string>--port</string>",
            "\t\t<string>17489</string>",
            "\t\t<string>--approval-mode</string>",
            "\t\t<string>denyUnmatched</string>",
            "\t\t<string>--muse-bin</string>",
            "\t\t<string>muse</string>",
            "\t\t<string>--log-format</string>",
            "\t\t<string>pretty</string>",
            "\t</array>",
            "\t<key>RunAtLoad</key>",
            "\t<true/>",
            "\t<key>KeepAlive</key>",
            "\t<dict>",
            "\t\t<key>SuccessfulExit</key>",
            "\t\t<false/>",
            "\t</dict>",
            "\t<key>ThrottleInterval</key>",
            "\t<integer>10</integer>",
            "\t<key>StandardOutPath</key>",
            "\t<string>/Users/josh/Library/Logs/muse-bridge.log</string>",
            "\t<key>StandardErrorPath</key>",
            "\t<string>/Users/josh/Library/Logs/muse-bridge.log</string>",
            "</dict>",
            "</plist>",
        ]
        .join("\n")
            + "\n";
        assert_eq!(got, expected);
    }

    #[test]
    fn launchd_plist_carries_env_and_escapes_xml() {
        let mut cfg = test_cfg();
        cfg.muse_bin = "a<b&c".to_string();
        cfg.env_extra = vec![("RUST_LOG".to_string(), "a<b".to_string())];
        let got = render_launchd_plist("/bin/muse-bridge", &cfg, "/tmp/x.log");
        assert!(got.contains("<string>a&lt;b&amp;c</string>"), "{got}");
        assert!(got.contains("<key>EnvironmentVariables</key>"), "{got}");
        assert!(got.contains("<string>a&lt;b</string>"), "{got}");

        let bare = render_launchd_plist("/bin/muse-bridge", &test_cfg(), "/tmp/x.log");
        assert!(!bare.contains("EnvironmentVariables"), "{bare}");
    }

    #[test]
    fn launchd_paths_follow_convention() {
        assert_eq!(
            launchd_plist_path("/Users/josh"),
            PathBuf::from("/Users/josh/Library/LaunchAgents/muse-bridge.plist")
        );
        assert_eq!(
            launchd_log_path("/Users/josh"),
            PathBuf::from("/Users/josh/Library/Logs/muse-bridge.log")
        );
    }

    #[test]
    fn schtasks_xml_golden() {
        let got = render_schtasks_xml(
            "C:\\tools\\muse-bridge.exe",
            &test_cfg(),
            "C:\\Windows",
            "C:\\Temp",
            "C:\\Temp\\muse-bridge.service.log",
        )
        .expect("golden renders");
        let expected = [
            "<?xml version=\"1.0\" encoding=\"UTF-16\"?>",
            "<Task version=\"1.4\" xmlns=\"http://schemas.microsoft.com/windows/2004/02/mit/task\">",
            "  <RegistrationInfo>",
            "    <Description>muse-bridge localhost OpenAI-compatible API (127.0.0.1:17489)</Description>",
            "  </RegistrationInfo>",
            "  <Triggers>",
            "    <LogonTrigger>",
            "      <Enabled>true</Enabled>",
            "    </LogonTrigger>",
            "  </Triggers>",
            "  <Principals>",
            "    <Principal id=\"Author\">",
            "      <LogonType>InteractiveToken</LogonType>",
            "      <RunLevel>LeastPrivilege</RunLevel>",
            "    </Principal>",
            "  </Principals>",
            "  <Settings>",
            "    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>",
            "    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>",
            "    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>",
            "    <AllowStartOnDemand>true</AllowStartOnDemand>",
            "    <StartWhenAvailable>true</StartWhenAvailable>",
            "    <Enabled>true</Enabled>",
            "    <Hidden>false</Hidden>",
            "    <AllowHardTerminate>true</AllowHardTerminate>",
            "    <RestartOnFailure>",
            "      <Interval>PT1M</Interval>",
            "      <Count>999</Count>",
            "    </RestartOnFailure>",
            "  </Settings>",
            "  <Actions Context=\"Author\">",
            "    <Exec>",
            "      <Command>C:\\Windows\\System32\\cmd.exe</Command>",
            "      <Arguments>/c &quot;&quot;C:\\tools\\muse-bridge.exe&quot; --port 17489 --approval-mode denyUnmatched --muse-bin muse --log-format pretty &gt;&gt; &quot;C:\\Temp\\muse-bridge.service.log&quot; 2&gt;&amp;1&quot;</Arguments>",
            "      <WorkingDirectory>C:\\Temp</WorkingDirectory>",
            "    </Exec>",
            "  </Actions>",
            "</Task>",
        ]
        .join("\n")
            + "\n";
        assert_eq!(got, expected);
    }

    #[test]
    fn schtasks_quoting_covers_metachars_and_refuses_quotes() {
        assert_eq!(cmd_quote("--port").expect("plain"), "--port");
        assert_eq!(
            cmd_quote("C:\\my dir\\x").expect("space"),
            "\"C:\\my dir\\x\""
        );
        assert_eq!(cmd_quote("R&D").expect("ampersand"), "\"R&D\"");
        assert!(cmd_quote("say \"hi\"").is_err(), "literal quotes refuse");

        let mut cfg = test_cfg();
        cfg.workspace_root = Some(PathBuf::from("C:\\100%cover"));
        let got = render_schtasks_xml("C:\\t\\m.exe", &cfg, "C:\\Windows", "C:\\T", "C:\\T\\l.log")
            .expect("percent renders");
        assert!(got.contains("C:\\100%%cover"), "{got}");

        let mut cfg = test_cfg();
        cfg.workspace_root = Some(PathBuf::from("C:\\say\"hi"));
        assert!(
            render_schtasks_xml("C:\\t\\m.exe", &cfg, "C:\\Windows", "C:\\T", "C:\\T\\l.log")
                .is_err(),
            "quotes in values refuse rather than mangle"
        );
    }

    #[test]
    fn utf16le_bom_shapes_schtasks_import_bytes() {
        assert_eq!(
            utf16le_bom("A<T"),
            vec![0xFF, 0xFE, 0x41, 0x00, 0x3C, 0x00, 0x54, 0x00]
        );
    }

    #[test]
    fn tailscale_argv_pins_scoped_forms() {
        assert_eq!(
            tailscale_set_argv(17489),
            vec!["serve", "--bg", "--tcp=17489", "tcp://127.0.0.1:17489"]
        );
        assert_eq!(
            tailscale_off_argv(17489),
            vec!["serve", "--tcp=17489", "off"]
        );
        assert_eq!(tailscale_status_argv(), vec!["serve", "status", "--json"]);
    }

    #[test]
    fn forward_present_reads_live_shaped_status() {
        // Shape captured from `tailscale serve status --json` (1.102.4).
        let live = r#"{
  "TCP": {
    "17489": {
      "TCPForward": "127.0.0.1:17489"
    },
    "5734": {
      "HTTPS": true
    }
  },
  "Web": {}
}"#;
        assert!(forward_present(live, 17489));
        assert!(
            !forward_present(live, 5734),
            "HTTPS arm is not a TCP forward"
        );
        assert!(!forward_present(live, 9999));
        assert!(!forward_present("not json", 17489));
        assert!(!forward_present("{}", 17489));
        assert!(forward_present(
            r#"{"TCP": {"8080": {"TCPForward": "localhost:8080"}}}"#,
            8080
        ));
        assert!(
            !forward_present(
                r#"{"TCP": {"8080": {"TCPForward": "127.0.0.1:9090"}}}"#,
                8080
            ),
            "mismatched target is not our forward"
        );
    }

    #[test]
    fn access_denied_matches_operator_refusal() {
        assert!(access_denied(
            "sending serve config: Access denied: serve config denied\n\nUse 'sudo tailscale serve --bg --tcp=59999 tcp://127.0.0.1:59999'.\nTo not require root, use 'sudo tailscale set --operator=$USER' once.\n"
        ));
        assert!(!access_denied("failed to connect to tailscaled"));
        assert!(!access_denied(""));
    }

    #[test]
    fn display_cmd_quotes_for_copy_paste() {
        assert_eq!(
            display_cmd(
                "systemctl",
                &["--user".to_string(), "enable --now".to_string()]
            ),
            "systemctl --user \"enable --now\""
        );
        assert_eq!(display_cmd("id", &["-u".to_string()]), "id -u");
    }

    #[test]
    fn write_definition_reports_new_updated_unchanged() {
        let path = std::env::temp_dir().join(format!(
            "muse-bridge-serve-test-{}-{}.unit",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        assert_eq!(
            write_definition(&path, b"v1").expect("first write"),
            FileState::New
        );
        assert_eq!(
            write_definition(&path, b"v1").expect("identical rewrite"),
            FileState::Unchanged
        );
        assert_eq!(
            write_definition(&path, b"v2").expect("changed rewrite"),
            FileState::Updated
        );
        assert_eq!(std::fs::read(&path).expect("read back"), b"v2");
        std::fs::remove_file(&path).expect("cleanup");
    }

    /// Resolver that fails the test if resolution is attempted: validation,
    /// `--off`, and `--status` must never touch backend resolution.
    fn refuse_bin(_: &str) -> Result<String, ServeError> {
        Err(ServeError::usage(
            "must not resolve the backend on this path",
        ))
    }

    /// Resolver that maps every backend to a fixed stub (deterministic,
    /// ambient-`PATH`-independent dry-runs).
    fn stub_bin(_: &str) -> Result<String, ServeError> {
        Ok("/stub/muse".to_string())
    }

    #[tokio::test]
    async fn run_serve_rejects_status_with_off() {
        let mut args = serve_args();
        args.off = true;
        args.status = true;
        let mut out = Vec::new();
        let err = run_serve_with(&args, &mut out, &env_of(&[]), &refuse_bin)
            .await
            .expect_err("conflict");
        assert!(err.usage);
        assert!(
            err.message.contains("mutually exclusive"),
            "{}",
            err.message
        );
    }

    #[tokio::test]
    async fn run_serve_rejects_bad_approval_before_touching_anything() {
        let mut args = serve_args();
        args.approval_mode = Some("bogus".to_string());
        args.dry_run = true;
        let mut out = Vec::new();
        let err = run_serve_with(&args, &mut out, &env_of(&[]), &refuse_bin)
            .await
            .expect_err("bad mode");
        assert!(err.usage);
        assert!(out.is_empty(), "validation precedes all output");
    }

    #[tokio::test]
    async fn dry_run_on_prints_plan_without_subprocesses() {
        let mut args = serve_args();
        args.dry_run = true;
        let mut out = Vec::new();
        run_serve_with(&args, &mut out, &env_of(&[]), &stub_bin)
            .await
            .expect("dry run succeeds");
        let report = String::from_utf8(out).expect("utf8 report");
        assert!(report.contains("platform: "), "{report}");
        assert!(
            report.contains("poll http://127.0.0.1:17489/healthz"),
            "{report}"
        );
        assert!(
            report.contains("tailscale serve --bg --tcp=17489 tcp://127.0.0.1:17489"),
            "{report}"
        );
        assert!(
            report.contains("backend: 'muse' resolves to /stub/muse"),
            "{report}"
        );
        assert!(report.contains("--muse-bin /stub/muse"), "{report}");
        if cfg!(target_os = "linux") {
            assert!(report.contains("muse-bridge.service"), "{report}");
            assert!(
                report.contains("systemctl --user enable --now muse-bridge.service"),
                "{report}"
            );
        }
    }

    #[tokio::test]
    async fn dry_run_off_and_status_print_scoped_plans() {
        let mut args = serve_args();
        args.off = true;
        args.tailscale = true;
        args.dry_run = true;
        let mut out = Vec::new();
        run_serve_with(&args, &mut out, &env_of(&[]), &refuse_bin)
            .await
            .expect("dry off succeeds");
        let report = String::from_utf8(out).expect("utf8 report");
        assert!(
            report.contains("tailscale serve --tcp=17489 off"),
            "{report}"
        );
        assert!(!report.contains("reset"), "never suggest reset: {report}");

        let mut args = serve_args();
        args.status = true;
        args.tailscale = true;
        args.dry_run = true;
        let mut out = Vec::new();
        run_serve_with(&args, &mut out, &env_of(&[]), &refuse_bin)
            .await
            .expect("dry status succeeds");
        let report = String::from_utf8(out).expect("utf8 report");
        assert!(
            report.contains("GET http://127.0.0.1:17489/healthz"),
            "{report}"
        );
        assert!(report.contains("tailscale serve status --json"), "{report}");
    }

    #[test]
    fn backend_bin_resolves_bare_names_and_rejects_missing() {
        let dir = std::env::temp_dir().join(format!("muse-serve-bin-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("fixture dir");
        let fake = dir.join("muse-test-fake");
        std::fs::write(&fake, "#!/bin/sh\n").expect("fixture binary");
        let path_env = dir.to_string_lossy().into_owned();

        // Bare name found via the injected PATH.
        let got = resolve_backend_bin_with("muse-test-fake", &path_env, ".COM;.EXE")
            .expect("fake resolves");
        assert_eq!(got, fake.to_string_lossy());

        // Separator-containing values pass through after the existence check.
        let abs = fake.to_string_lossy().into_owned();
        assert_eq!(
            resolve_backend_bin_with(&abs, "", "").expect("absolute passes"),
            abs
        );

        // Missing binaries fail fast with the fix (never a crash-looping unit).
        let err = resolve_backend_bin_with("muse-test-absent", &path_env, "").expect_err("absent");
        assert!(!err.usage);
        assert!(
            err.message.contains("Muse CLI not found"),
            "{}",
            err.message
        );
        assert!(err.message.contains("--muse-bin"), "{}", err.message);
        let missing_abs = dir.join("nope").to_string_lossy().into_owned();
        assert!(
            resolve_backend_bin_with(&missing_abs, &path_env, "")
                .expect_err("missing abs")
                .message
                .contains("Muse CLI not found")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn systemd_restarts_only_on_changed_definitions() {
        assert_eq!(systemd_start_mode(FileState::New), StartMode::Start);
        assert_eq!(systemd_start_mode(FileState::Unchanged), StartMode::Start);
        assert_eq!(systemd_start_mode(FileState::Updated), StartMode::Restart);
        assert_eq!(
            systemd_enable_argv(),
            vec!["--user", "enable", "muse-bridge.service"]
        );
        assert_eq!(
            systemd_restart_argv(),
            vec!["--user", "restart", "muse-bridge.service"]
        );
    }

    #[test]
    fn already_running_matches_schtasks_refusal() {
        assert!(already_running(
            "ERROR: The task is currently running. (2,32)"
        ));
        assert!(!already_running("ERROR: Access is denied."));
        assert!(!already_running(""));
    }
}
