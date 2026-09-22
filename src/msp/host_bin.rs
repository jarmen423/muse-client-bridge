//! Resolve the Muse CLI on Windows.
//!
//! Meta's Windows installer publishes `muse.cmd`, not `muse.exe`.
//! `Command::new("muse")` only appends `.exe`, so it misses the shim and
//! the bridge exits before ACP starts. A `.cmd` file also cannot be
//! `CreateProcess`'d directly. It has to go through `cmd.exe /c`.

use std::path::Path;

/// A located Muse CLI, ready to turn into a process command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedHost {
    /// An executable `Command::new` can launch directly.
    Direct(String),
    /// A `.cmd` or `.bat` shim. Launch with `cmd.exe /d /s /c`.
    Script(String),
}

/// Find `bin` on `path_env` and decide how to launch it.
///
/// `path_env` is a Windows `PATH` (`dir;dir`). `pathext` is `PATHEXT`
/// (`.COM;.EXE;.BAT;.CMD`). A name that already contains a separator, or
/// that is not on `PATH`, is returned unchanged.
pub fn resolve_host_bin(bin: &str, path_env: &str, pathext: &str) -> ResolvedHost {
    let located = locate(bin, path_env, pathext);
    if is_cmd_script(&located) {
        ResolvedHost::Script(located)
    } else {
        ResolvedHost::Direct(located)
    }
}

/// The argument to `cmd.exe /d /s /c`. Outer quotes are stripped by `/s`.
pub fn cmd_c_payload(program: &str, args: &[String]) -> String {
    let mut inner = quote_cmd_arg(program);
    for arg in args {
        inner.push(' ');
        inner.push_str(&quote_cmd_arg(arg));
    }
    format!("\"{inner}\"")
}

fn locate(bin: &str, path_env: &str, pathext: &str) -> String {
    if bin.contains('/') || bin.contains('\\') {
        return bin.to_string();
    }
    let has_ext = Path::new(bin).extension().is_some();
    for dir in path_env.split(';').filter(|dir| !dir.is_empty()) {
        let dir = Path::new(dir);
        if has_ext {
            let candidate = dir.join(bin);
            if candidate.is_file() {
                return candidate.to_string_lossy().into_owned();
            }
            continue;
        }
        for ext in pathext_exts(pathext) {
            let candidate = dir.join(format!("{bin}{ext}"));
            if candidate.is_file() {
                return candidate.to_string_lossy().into_owned();
            }
        }
    }
    bin.to_string()
}

fn pathext_exts(pathext: &str) -> Vec<String> {
    pathext
        .split(';')
        .filter(|ext| !ext.is_empty())
        .map(|ext| {
            if ext.starts_with('.') {
                ext.to_string()
            } else {
                format!(".{ext}")
            }
        })
        .collect()
}

fn is_cmd_script(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.ends_with(".cmd") || lower.ends_with(".bat")
}

fn quote_cmd_arg(arg: &str) -> String {
    if arg.is_empty() || arg.contains([' ', '\t', '"', '&', '<', '>', '(', ')', '^', '%', '|']) {
        format!("\"{}\"", arg.replace('"', "\"\""))
    } else {
        arg.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_muse_resolves_to_the_cmd_shim() {
        let dir = std::env::temp_dir().join(format!("muse-host-bin-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let shim = dir.join("muse.CMD");
        std::fs::write(&shim, "@echo off\r\n").unwrap();

        let resolved = resolve_host_bin("muse", &dir.to_string_lossy(), ".COM;.EXE;.BAT;.CMD");
        assert_eq!(
            resolved,
            ResolvedHost::Script(shim.to_string_lossy().into_owned())
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn exe_wins_over_cmd_when_both_exist() {
        let dir = std::env::temp_dir().join(format!("muse-host-bin-exe-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("muse.EXE"), b"").unwrap();
        std::fs::write(dir.join("muse.CMD"), b"").unwrap();

        let resolved = resolve_host_bin("muse", &dir.to_string_lossy(), ".EXE;.CMD");
        assert!(matches!(resolved, ResolvedHost::Direct(path) if path.ends_with("muse.EXE")));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn absolute_cmd_path_is_a_script() {
        let resolved = resolve_host_bin(
            r"C:\Users\jfrie\AppData\Local\Programs\muse\muse.cmd",
            "",
            ".EXE",
        );
        assert!(matches!(resolved, ResolvedHost::Script(_)));
    }

    #[test]
    fn cmd_payload_quotes_a_path_with_spaces() {
        let payload = cmd_c_payload(r"C:\Program Files\muse\muse.cmd", &["serve".to_string()]);
        assert_eq!(payload, "\"\"C:\\Program Files\\muse\\muse.cmd\" serve\"");
    }
}
