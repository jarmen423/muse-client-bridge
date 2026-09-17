//! Dynamic slash commands from the local Muse skill registry.
//!
//! `muse skills list --json` reports the skills the session's workspace can
//! invoke (`/<id>` normalizes to `/skill <id>` before `turn/start`, matching
//! the CLI's command registry). The listing runs as a short-lived subprocess
//! beside the supervised `muse serve` host — the same `MUSE_CLI` binary, the
//! serve posture's `--trust-workspace` flag, and the session's workspace.
//! Any failure (older CLI without `skills`, a listing error, malformed
//! output) degrades to the static command set; it never fails session setup.

use std::time::Duration;

use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use crate::msp::host::HostConfig;

/// One invocable skill for `available_commands` and `/<id>` normalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillEntry {
    /// Stable invocation id (`/skill <id>`).
    pub id: String,
    /// Short human description for the command palette.
    pub description: String,
}

/// Bounded wait for `muse skills list` (a cold run pales next to the 15s
/// budget; a hung launcher must never stall session setup).
const SKILLS_LIST_TIMEOUT: Duration = Duration::from_secs(15);
/// Output cap (the registry is small; refuse runaway output instead of
/// buffering it).
const SKILLS_LIST_MAX_BYTES: usize = 4 * 1024 * 1024;
/// Command descriptions render single-line in client palettes; registry
/// text can run long, so clip at the first line and this many chars.
const DESCRIPTION_CAP_CHARS: usize = 160;

/// `muse skills list --json` for one session workspace. Errors degrade to
/// an empty list (the static commands still apply); never fatal.
pub async fn list_skills(config: &HostConfig, workspace: &str) -> Vec<SkillEntry> {
    match run_skills_list(config, workspace).await {
        Ok(skills) => {
            tracing::debug!(skills = skills.len(), workspace, "skill registry listed");
            skills
        }
        Err(reason) => {
            tracing::info!(reason, "skills list unavailable; static commands only");
            Vec::new()
        }
    }
}

/// Spawn + read + parse. `Err` carries a short human reason for the log.
async fn run_skills_list(config: &HostConfig, workspace: &str) -> Result<Vec<SkillEntry>, String> {
    let mut cmd = Command::new(&config.bin);
    cmd.args(["skills", "list", "--json"]);
    // Mirror the serve posture: workspace trust is what exposes
    // project/user skills to the session anyway.
    if config.serve_args.iter().any(|a| a == "--trust-workspace") {
        cmd.arg("--trust-workspace");
    }
    if !workspace.is_empty() {
        cmd.args(["--workspace", workspace]);
    }
    for (key, value) in &config.extra_env {
        cmd.env(key, value);
    }
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("spawn {}: {e}", config.bin))?;
    let stdout = child.stdout.take().expect("stdout piped");
    let stderr = child.stderr.take().expect("stderr piped");
    let reader = tokio::spawn(async move {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let _ = stdout
            .take(SKILLS_LIST_MAX_BYTES as u64)
            .read_to_end(&mut out)
            .await;
        let _ = stderr.take(64 * 1024).read_to_end(&mut err).await;
        (out, err)
    });
    let status = match tokio::time::timeout(SKILLS_LIST_TIMEOUT, child.wait()).await {
        Ok(status) => status.map_err(|e| format!("wait: {e}"))?,
        Err(_) => {
            let _ = child.kill().await;
            return Err(format!(
                "skills list timed out after {}s",
                SKILLS_LIST_TIMEOUT.as_secs()
            ));
        }
    };
    let (out, err) = reader.await.map_err(|e| format!("reader: {e}"))?;
    if !status.success() {
        let tail = String::from_utf8_lossy(&err);
        let tail = tail.lines().next().unwrap_or("").trim();
        return Err(format!("skills list exited {status}: {tail}"));
    }
    parse_skills_list(&out)
}

/// Parse `muse skills list --json`: `{skills: [...], diagnostics: [...]}`.
/// Entries without an id, or whose `activation` marks them off, are
/// dropped; anything else tolerates missing fields (rule 7).
fn parse_skills_list(bytes: &[u8]) -> Result<Vec<SkillEntry>, String> {
    let value: Value = serde_json::from_slice(bytes).map_err(|e| format!("skills JSON: {e}"))?;
    let skills = value
        .get("skills")
        .and_then(Value::as_array)
        .ok_or_else(|| "skills JSON has no skills array".to_string())?;
    let mut entries = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for skill in skills {
        let id = skill
            .get("id")
            .or_else(|| skill.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        if id.is_empty() || !seen.insert(id.clone()) {
            continue;
        }
        // `activation` is an open vocabulary; only explicit off-markers
        // exclude. Anything else (on, user-invocable-only, future values)
        // stays invocable — a dead entry fails loudly host-side, never here.
        match skill
            .get("activation")
            .and_then(Value::as_str)
            .unwrap_or("on")
        {
            "off" | "disabled" | "none" => continue,
            _ => {}
        }
        entries.push(SkillEntry {
            id,
            description: skill_description(skill),
        });
    }
    Ok(entries)
}

/// One palette line for a skill: `description` (else `display_name`) cut at
/// its first line and the char cap.
fn skill_description(skill: &Value) -> String {
    let raw = skill
        .get("description")
        .or_else(|| skill.get("short_description"))
        .or_else(|| skill.get("display_name"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let first_line = raw.lines().next().unwrap_or("").trim();
    let mut capped: String = first_line.chars().take(DESCRIPTION_CAP_CHARS).collect();
    if first_line.chars().count() > DESCRIPTION_CAP_CHARS {
        capped.push('…');
    }
    capped
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn list(skills: Value) -> Vec<u8> {
        json!({"diagnostics": [], "skills": skills})
            .to_string()
            .into_bytes()
    }

    #[test]
    fn parses_ids_descriptions_and_drops_unusable_entries() {
        let skills = list(json!([
            {"id": "grill", "description": "Grill a diff\nwith detail", "activation": "on"},
            {"name": "named-only", "description": "fallback id", "activation": "user-invocable-only"},
            {"id": "off-skill", "activation": "off"},
            {"id": "", "description": "no id"},
            {"id": "grill", "description": "dup id"},
            "not an object",
        ]));
        let entries = parse_skills_list(&skills).expect("parse");
        assert_eq!(entries.len(), 2, "{entries:?}");
        assert_eq!(entries[0].id, "grill");
        assert_eq!(entries[0].description, "Grill a diff");
        assert_eq!(entries[1].id, "named-only");
    }

    #[test]
    fn tolerates_garbage_and_missing_arrays() {
        assert!(parse_skills_list(b"{not json").is_err());
        assert!(parse_skills_list(b"{}").is_err());
        assert!(parse_skills_list(b"[]").is_err());
        let empty = parse_skills_list(&list(json!([]))).expect("empty parses");
        assert!(empty.is_empty());
    }

    #[test]
    fn descriptions_cap_at_one_line_and_160_chars() {
        let long = "x".repeat(300);
        let skills = list(json!([{"id": "long", "description": long, "activation": "on"}]));
        let entries = parse_skills_list(&skills).expect("parse");
        assert_eq!(entries[0].description.chars().count(), 161); // cap + ellipsis
    }
}
