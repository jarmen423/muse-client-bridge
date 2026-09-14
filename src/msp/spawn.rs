//! Child process supervision: spawn, stderr capture, exit classification (SPEC §4).
//!
//! The spawn boundary is the security boundary: only the spawner can reach
//! the host's stdin, so v1 has no auth. stderr is captured (bounded tail),
//! never parsed. Exit codes are a contract — see [`ExitClass`].

use std::collections::VecDeque;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};

/// Graceful-drain budget after stdin EOF before SIGKILL (SPEC §4).
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);
/// stderr ring capacity: newest lines/bytes retained (SPEC §4).
pub const STDERR_MAX_LINES: usize = 100;
pub const STDERR_MAX_BYTES: usize = 8 * 1024;
/// Longest single stderr line retained; the remainder of an overlong line is
/// drained, never stored (a hostile/flooded stderr cannot grow memory).
const STDERR_MAX_LINE_BYTES: usize = 64 * 1024;

/// What to spawn: `bin [subcommand] [args...]` with `cwd` and inherited env.
#[derive(Debug, Clone)]
pub struct ChildSpec {
    /// Binary path or bare name (`MUSE_CLI` or `muse` on PATH).
    pub bin: String,
    /// Subcommand, normally `Some("serve")` (`None` only drives test fakes).
    pub subcommand: Option<String>,
    /// Extra args (`MUSE_SERVE_ARGS`, whitespace-split, plus passthroughs).
    pub args: Vec<String>,
    /// Working directory (bridge `--workspace-root`).
    pub cwd: PathBuf,
    /// Extra env vars (in addition to the inherited environment).
    pub extra_env: Vec<(String, String)>,
}

/// A freshly spawned host child: pipes plus the supervisable handle.
pub struct SpawnedChild {
    /// Host stdin (the bridge's write side; drop = EOF).
    pub stdin: ChildStdin,
    /// Host stdout (the reader task drains this from birth).
    pub stdout: ChildStdout,
    /// Supervision handle (reap/kill/shutdown + stderr tail).
    pub handle: ChildHandle,
}

/// Supervision handle for a spawned host child.
pub struct ChildHandle {
    child: tokio::sync::Mutex<Child>,
    /// Bounded stderr tail (captured, never parsed).
    pub stderr: Arc<StderrTail>,
    pid: Option<u32>,
}

impl ChildHandle {
    /// Non-blocking reap: `Some(class)` if the child has exited.
    pub async fn try_reap(&self) -> Option<ExitClass> {
        self.child
            .lock()
            .await
            .try_wait()
            .ok()
            .flatten()
            .map(|status| ExitClass::classify(&status))
    }

    /// Best-effort kill + blocking reap. Used when the child must go now
    /// (launch failure, wedged shutdown, restart): errors are logged, never
    /// propagated, and the return is a best-effort classification.
    pub async fn kill_and_reap(&self) -> ExitClass {
        let mut child = self.child.lock().await;
        // Kill errors (already exited) are fine; wait reaps either way.
        let _ = child.kill().await;
        match child.wait().await {
            Ok(status) => ExitClass::classify(&status),
            Err(e) => {
                tracing::warn!(error = %e, pid = ?self.pid, "failed to reap host child");
                ExitClass::Crash
            }
        }
    }

    /// Graceful shutdown: `stdin` is dropped (EOF) on entry, then the host
    /// gets [`SHUTDOWN_GRACE`] to drain before SIGKILL.
    ///
    /// NOTE: SPEC §4 names a SIGTERM middle step and process-group delivery;
    /// both need raw syscalls outside the locked dependency set, so v1
    /// escalates EOF → SIGKILL directly at the child. The live host exits on
    /// EOF in milliseconds; the kill path only fires on a wedged host whose
    /// session-end records v1 never reads back anyway (stateless sessions).
    pub async fn shutdown(&self, stdin: Option<ChildStdin>) -> ExitClass {
        drop(stdin); // EOF: orderly drain, session-end records, leases released
        let mut child = self.child.lock().await;
        match tokio::time::timeout(SHUTDOWN_GRACE, child.wait()).await {
            Ok(Ok(status)) => ExitClass::classify(&status),
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "host wait failed during shutdown");
                ExitClass::Crash
            }
            Err(_) => {
                tracing::warn!(
                    grace_secs = SHUTDOWN_GRACE.as_secs(),
                    "host ignored stdin EOF; SIGKILLing"
                );
                let _ = child.kill().await;
                match child.wait().await {
                    Ok(status) => ExitClass::classify(&status),
                    Err(e) => {
                        tracing::warn!(error = %e, "host reap failed after SIGKILL");
                        ExitClass::Crash
                    }
                }
            }
        }
    }
}

/// Spawn the host child with piped stdio and a stderr capture task.
///
/// Env is inherited (so `muse login` credentials apply); stdio errors become
/// actionable [`describe_spawn_error`] text.
pub fn spawn_child(spec: &ChildSpec) -> Result<SpawnedChild, String> {
    let mut cmd = Command::new(&spec.bin);
    if let Some(subcommand) = &spec.subcommand {
        cmd.arg(subcommand);
    }
    cmd.args(&spec.args);
    cmd.current_dir(&spec.cwd);
    cmd.envs(spec.extra_env.iter().cloned());
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd.kill_on_drop(true);
    let mut child = cmd
        .spawn()
        .map_err(|e| describe_spawn_error(&spec.bin, e))?;
    let pid = child.id();
    let stdin = child.stdin.take().ok_or("serve: no stdin pipe")?;
    let stdout = child.stdout.take().ok_or("serve: no stdout pipe")?;
    let stderr = child.stderr.take().ok_or("serve: no stderr pipe")?;
    // Never log argv: `MUSE_SERVE_ARGS` is user config and could carry secrets.
    tracing::info!(bin = %spec.bin, pid = ?pid, "spawned msp host");
    // The capture task ends at stderr EOF; the handle is dropped to detach.
    let (tail, _capture) = spawn_stderr_capture(stderr);
    Ok(SpawnedChild {
        stdin,
        stdout,
        handle: ChildHandle {
            child: tokio::sync::Mutex::new(child),
            stderr: tail,
            pid,
        },
    })
}

/// Turn a host spawn failure into the next user action.
pub fn describe_spawn_error(bin: &str, e: std::io::Error) -> String {
    match e.kind() {
        std::io::ErrorKind::NotFound => format!(
            "Muse CLI not found: '{bin}'. Install Muse Code \
             (https://dev.meta.ai/docs/muse-code), ensure it is on PATH, or set \
             MUSE_CLI=/absolute/path/to/muse"
        ),
        std::io::ErrorKind::PermissionDenied => format!(
            "Muse CLI is not executable: '{bin}'. Fix permissions or set \
             MUSE_CLI=/absolute/path/to/muse"
        ),
        _ => format!("failed to spawn '{bin} serve': {e}"),
    }
}

/// Host exit classification (SPEC §4, exit-classification guide).
///
/// Codes are separated by remedy, not severity. Unknown non-zero codes and
/// signal deaths are the crash row (forward-compatibility rule).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitClass {
    /// 0 — clean shutdown: stdin closed, drain completed, session-end
    /// records written. Mid-serve it is unexpected (restart); at shutdown it
    /// is the happy path.
    Clean,
    /// 1 — unhandled error the host could not express on the wire. Surface
    /// the captured stderr; the session log has no session-end record.
    Unhandled,
    /// 2 — usage error: bad `serve` args (client bug or version mismatch).
    /// Never self-heals: do not restart.
    Usage,
    /// 3 — configuration error: no usable config/profile/credentials.
    /// Never self-heals: do not restart; the user must fix configuration
    /// (usually `muse login`). Surfaces as HTTP 401 (SPEC §6).
    Config,
    /// 4 — session lease unavailable: another live process owns the session.
    /// Transient contention: restartable within budget.
    Lease,
    /// 5 — SDK surface unavailable: this build will not serve. No invocation
    /// will serve: do not restart.
    SdkOff,
    /// Anything else, or death by signal (incl. OOM kills): no session-end
    /// record exists. Restartable within budget.
    Crash,
}

impl ExitClass {
    /// Classify a waited-for exit status.
    pub fn classify(status: &std::process::ExitStatus) -> Self {
        match status.code() {
            Some(0) => ExitClass::Clean,
            Some(1) => ExitClass::Unhandled,
            Some(2) => ExitClass::Usage,
            Some(3) => ExitClass::Config,
            Some(4) => ExitClass::Lease,
            Some(5) => ExitClass::SdkOff,
            _ => ExitClass::Crash,
        }
    }

    /// Whether a restart might help (SPEC §4.4, exit guide). Usage/config/
    /// SDK-off exits never self-heal: restarting identically is a crash loop.
    pub fn restartable(&self) -> bool {
        matches!(
            self,
            ExitClass::Clean | ExitClass::Unhandled | ExitClass::Lease | ExitClass::Crash
        )
    }

    /// One-line remedy for logs (never host text).
    pub fn describe(&self) -> &'static str {
        match self {
            ExitClass::Clean => "clean shutdown",
            ExitClass::Unhandled => "unhandled host error (see stderr tail)",
            ExitClass::Usage => "bad serve arguments (do not retry)",
            ExitClass::Config => "host configuration/credentials missing (do not retry)",
            ExitClass::Lease => "session lease held by another process",
            ExitClass::SdkOff => "this build will not serve (do not retry)",
            ExitClass::Crash => "host crashed or was signalled",
        }
    }
}

/// Bounded stderr ring: newest [`STDERR_MAX_LINES`] lines / [`STDERR_MAX_BYTES`].
///
/// Captured, never parsed (the format is not a contract); surfaced on
/// failures and in `--support`.
pub struct StderrTail {
    state: Mutex<TailState>,
    _private: (),
}

struct TailState {
    lines: VecDeque<String>,
    bytes: usize,
}

impl StderrTail {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(TailState {
                lines: VecDeque::new(),
                bytes: 0,
            }),
            _private: (),
        })
    }

    fn push(&self, line: String) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.bytes += line.len();
        state.lines.push_back(line);
        while state.lines.len() > STDERR_MAX_LINES || state.bytes > STDERR_MAX_BYTES {
            if let Some(dropped) = state.lines.pop_front() {
                state.bytes = state.bytes.saturating_sub(dropped.len());
            } else {
                break;
            }
        }
    }

    /// Chronological tail text (oldest retained line first).
    pub fn text(&self) -> String {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let mut out = String::new();
        for (i, line) in state.lines.iter().enumerate() {
            if i > 0 {
                out.push('\n');
            }
            out.push_str(line);
        }
        out
    }

    /// Whether anything was captured.
    pub fn is_empty(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .lines
            .is_empty()
    }
}

/// Drain `stderr` into a bounded tail on its own task. Ends at stderr EOF.
pub fn spawn_stderr_capture(stderr: ChildStderr) -> (Arc<StderrTail>, tokio::task::JoinHandle<()>) {
    let tail = StderrTail::new();
    let task_tail = tail.clone();
    let handle = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr);
        let mut buf = Vec::new();
        loop {
            buf.clear();
            let n = match AsyncReadExt::take(&mut reader, (STDERR_MAX_LINE_BYTES + 1) as u64)
                .read_until(b'\n', &mut buf)
                .await
            {
                Ok(0) => break, // EOF
                Ok(n) => n,
                Err(_) => break,
            };
            let complete = buf.ends_with(b"\n");
            if complete {
                buf.pop();
                if buf.ends_with(b"\r") {
                    buf.pop();
                }
            } else if n == STDERR_MAX_LINE_BYTES + 1 {
                // Overlong line: keep the head, drain the rest.
                let mut chunk = Vec::new();
                loop {
                    chunk.clear();
                    match AsyncReadExt::take(&mut reader, 65536)
                        .read_until(b'\n', &mut chunk)
                        .await
                    {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            if chunk.ends_with(b"\n") {
                                break;
                            }
                        }
                    }
                }
                buf.extend_from_slice("…[line truncated]".as_bytes());
            }
            // stderr is human diagnostics, display-only: lossy is correct.
            task_tail.push(String::from_utf8_lossy(&buf).into_owned());
        }
    });
    (tail, handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawn_errors_name_the_next_user_action() {
        let msg = describe_spawn_error(
            "/opt/muse",
            std::io::Error::from(std::io::ErrorKind::NotFound),
        );
        assert!(msg.contains("Muse CLI not found"), "{msg}");
        assert!(msg.contains("'/opt/muse'"), "{msg}");
        assert!(msg.contains("MUSE_CLI="), "{msg}");

        let msg = describe_spawn_error(
            "/opt/muse",
            std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        );
        assert!(msg.contains("not executable"), "{msg}");

        let msg = describe_spawn_error(
            "muse",
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "boom"),
        );
        assert!(msg.contains("failed to spawn"), "{msg}");
    }

    #[test]
    fn stderr_tail_bounds_lines_and_bytes() {
        let tail = StderrTail::new();
        for i in 0..200 {
            tail.push(format!("line-{i:03}"));
        }
        let text = tail.text();
        assert_eq!(text.lines().count(), STDERR_MAX_LINES);
        assert!(text.starts_with("line-100"), "{text}");
        assert!(text.ends_with("line-199"), "{text}");

        let tail = StderrTail::new();
        tail.push("x".repeat(STDERR_MAX_BYTES + 1));
        tail.push("last".to_string());
        let text = tail.text();
        assert!(text.ends_with("last"), "{text}");
        assert!(
            text.len() <= STDERR_MAX_BYTES + "last".len() + 1,
            "len={}",
            text.len()
        );
    }

    #[cfg(unix)]
    fn exit_status(cmd: &str) -> std::process::ExitStatus {
        std::process::Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .status()
            .expect("sh must run")
    }

    #[cfg(unix)]
    #[test]
    fn exit_codes_classify_per_the_contract() {
        for (code, class) in [
            (0, ExitClass::Clean),
            (1, ExitClass::Unhandled),
            (2, ExitClass::Usage),
            (3, ExitClass::Config),
            (4, ExitClass::Lease),
            (5, ExitClass::SdkOff),
            (7, ExitClass::Crash),
            (42, ExitClass::Crash),
            (127, ExitClass::Crash),
        ] {
            let status = exit_status(&format!("exit {code}"));
            assert_eq!(ExitClass::classify(&status), class, "code {code}");
        }
        // Signal deaths are the crash row.
        assert_eq!(
            ExitClass::classify(&exit_status("kill -9 $$")),
            ExitClass::Crash
        );
        assert_eq!(
            ExitClass::classify(&exit_status("kill -TERM $$")),
            ExitClass::Crash
        );
    }

    #[test]
    fn only_self_healing_exits_restart() {
        for class in [
            ExitClass::Clean,
            ExitClass::Unhandled,
            ExitClass::Lease,
            ExitClass::Crash,
        ] {
            assert!(class.restartable(), "{class:?}");
        }
        for class in [ExitClass::Usage, ExitClass::Config, ExitClass::SdkOff] {
            assert!(!class.restartable(), "{class:?}");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_drains_on_eof_and_reaps_clean() {
        let spawned = spawn_child(&ChildSpec {
            bin: "sh".to_string(),
            subcommand: None,
            args: vec!["-c".to_string(), "cat > /dev/null".to_string()],
            cwd: std::env::temp_dir(),
            extra_env: vec![],
        })
        .expect("spawn sh");
        let class = spawned.handle.shutdown(Some(spawned.stdin)).await;
        assert_eq!(class, ExitClass::Clean);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stderr_capture_collects_child_output() {
        let spawned = spawn_child(&ChildSpec {
            bin: "sh".to_string(),
            subcommand: None,
            args: vec!["-c".to_string(), "echo hello-err >&2".to_string()],
            cwd: std::env::temp_dir(),
            extra_env: vec![],
        })
        .expect("spawn sh");
        let class = spawned.handle.shutdown(Some(spawned.stdin)).await;
        assert_eq!(class, ExitClass::Clean);
        // Capture races the reap; poll briefly.
        for _ in 0..100 {
            if !spawned.handle.stderr.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            spawned.handle.stderr.text().contains("hello-err"),
            "tail={:?}",
            spawned.handle.stderr.text()
        );
    }
}
