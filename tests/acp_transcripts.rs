//! Vendored ACP/MSP transcript corpus regression net (FORK_PLAN P1).
//!
//! Every scenario under `tests/acp-protocol/transcripts/` is validated
//! structurally: manifest shape, NDJSON frame envelopes, JSON-RPC 2.0
//! framing, the `initialize` / `initialized` handshake, request/response id
//! correlation in both directions, UUIDv7 `commandId`s (AGENTS.md rule 3),
//! and schema-compat classification of each transcript's `initialize`
//! result (fixture fingerprints must never read as live-host `tested`).
//! P5 layers replay coverage on top; the ACP server itself lands in P2.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use muse_bridge::acp::compat::schema;
use serde_json::Value;

fn corpus_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/acp-protocol/transcripts")
}

/// Vendored scenario count. Update deliberately when re-vendoring from a new
/// upstream revision (see `tests/acp-protocol/PROVENANCE.md`).
const EXPECTED_SCENARIOS: usize = 48;

fn scenario_dirs() -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(corpus_root())
        .expect("read transcript corpus")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.is_dir())
        .collect();
    dirs.sort();
    dirs
}

fn is_uuid_v7(id: &str) -> bool {
    let parts: Vec<&str> = id.split('-').collect();
    parts.len() == 5
        && parts[0].len() == 8
        && parts[1].len() == 4
        && parts[2].len() == 4
        && parts[3].len() == 4
        && parts[4].len() == 12
        && parts[2].starts_with('7')
        && id.chars().all(|c| c == '-' || c.is_ascii_hexdigit())
}

fn check_scenario(dir: &Path) {
    let name = dir.file_name().unwrap().to_string_lossy().to_string();

    // Manifest shape.
    let manifest_text =
        std::fs::read_to_string(dir.join("manifest.json")).expect("read manifest.json");
    let manifest: Value = serde_json::from_str(&manifest_text).expect("parse manifest.json");
    assert_eq!(
        manifest.get("kind").and_then(Value::as_str),
        Some("mspTranscript"),
        "{name}: kind"
    );
    assert_eq!(
        manifest.get("scenario").and_then(Value::as_str),
        Some(name.as_str()),
        "{name}: scenario matches dir"
    );
    assert_eq!(
        manifest.get("schemaVersion").and_then(Value::as_u64),
        Some(1),
        "{name}: schemaVersion"
    );
    assert!(
        manifest
            .get("fingerprint")
            .and_then(Value::as_str)
            .is_some_and(|fp| !fp.is_empty()),
        "{name}: manifest fingerprint present"
    );

    // Frame envelopes.
    let transcript =
        std::fs::read_to_string(dir.join("transcript.ndjson")).expect("read transcript.ndjson");
    let lines: Vec<&str> = transcript
        .lines()
        .filter(|l| !l.trim().is_empty())
        .collect();
    assert!(!lines.is_empty(), "{name}: transcript is non-empty");
    let mut frames: Vec<(String, Value)> = Vec::with_capacity(lines.len());
    for (i, line) in lines.iter().enumerate() {
        let frame: Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("{name} line {i}: envelope: {e}"));
        let dirn = frame
            .get("dir")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("{name} line {i}: missing dir"));
        assert!(
            dirn == "client" || dirn == "server",
            "{name} line {i}: dir={dirn}"
        );
        let raw_text = frame
            .get("raw")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("{name} line {i}: missing raw"));
        let raw: Value =
            serde_json::from_str(raw_text).unwrap_or_else(|e| panic!("{name} line {i}: raw: {e}"));
        assert_eq!(
            raw.get("jsonrpc").and_then(Value::as_str),
            Some("2.0"),
            "{name} line {i}: jsonrpc version"
        );
        frames.push((dirn.to_string(), raw));
    }

    // Handshake: client initialize first, then its response, then initialized.
    assert_eq!(frames[0].0, "client", "{name}: first frame is client");
    assert_eq!(
        frames[0].1.get("method").and_then(Value::as_str),
        Some("initialize"),
        "{name}: first frame is initialize"
    );
    let init_id = frames[0].1.get("id").expect("initialize id").clone();
    let init_result = frames
        .iter()
        .find(|(d, r)| d == "server" && r.get("id").is_some_and(|id| *id == init_id))
        .unwrap_or_else(|| panic!("{name}: initialize response present"));
    let schema_obj = init_result
        .1
        .get("result")
        .and_then(|r| r.get("schema"))
        .unwrap_or_else(|| panic!("{name}: initialize result carries schema"));
    assert_eq!(
        schema_obj.get("version").and_then(Value::as_u64),
        Some(schema::SUPPORTED_SCHEMA_VERSION),
        "{name}: schema version"
    );
    let fingerprint = schema_obj
        .get("fingerprint")
        .and_then(Value::as_str)
        .unwrap_or("");
    assert!(
        frames
            .iter()
            .any(|(_, r)| r.get("method").and_then(Value::as_str) == Some("initialized")),
        "{name}: initialized notification present"
    );

    // Compat classification: fixture corpus must never look like a validated
    // live host, and must never be fatal.
    let compat = schema::classify(Some(schema::SUPPORTED_SCHEMA_VERSION), fingerprint);
    assert_ne!(
        compat.status,
        schema::Status::Tested,
        "{name}: transcript fingerprint is never live-host tested"
    );
    assert!(
        !compat.is_fatal(),
        "{name}: transcript fingerprint is never fatal"
    );
    if fingerprint == schema::TRANSCRIPT_FIXTURE_FINGERPRINT {
        assert_eq!(
            compat.status,
            schema::Status::Fixture,
            "{name}: fixture pin"
        );
    }

    // Request/response id correlation, both directions.
    let mut requests: HashSet<(String, String)> = HashSet::new();
    for (i, (dirn, raw)) in frames.iter().enumerate() {
        let id = raw.get("id").map(ToString::to_string);
        let is_request = raw.get("method").is_some();
        if is_request {
            if let Some(id) = id {
                assert!(
                    requests.insert((dirn.clone(), id.clone())),
                    "{name} line {i}: duplicate request id {id}"
                );
            }
            // AGENTS.md rule 3: commandIds are UUIDv7.
            if let Some(cmd) = raw
                .get("params")
                .and_then(|p| p.get("commandId"))
                .and_then(Value::as_str)
            {
                assert!(
                    is_uuid_v7(cmd),
                    "{name} line {i}: commandId {cmd} is UUIDv7"
                );
            }
        } else if let Some(id) = id {
            let peer = if dirn == "client" { "server" } else { "client" };
            assert!(
                requests.contains(&(peer.to_string(), id.clone())),
                "{name} line {i}: orphan response id {id}"
            );
            let is_result = raw.get("result").is_some();
            let is_error = raw.get("error").is_some();
            assert!(
                is_result ^ is_error,
                "{name} line {i}: response carries exactly one of result/error"
            );
        }
    }
}

#[test]
fn vendored_corpus_is_complete_and_well_formed() {
    let dirs = scenario_dirs();
    assert_eq!(
        dirs.len(),
        EXPECTED_SCENARIOS,
        "scenario count; update EXPECTED_SCENARIOS when re-vendoring"
    );
    for dir in &dirs {
        check_scenario(dir);
    }
}

#[test]
fn key_scenarios_cover_settle_and_reclaim_rules() {
    // Highest-risk P3 rules need corpus fixtures present (FORK_PLAN validation).
    for scenario in [
        "session-start",
        "turn-retry-scheduled",
        "turn-unqueued-round-trip",
        "approval-round-trip",
        "cancel-mid-turn",
        "gap-recovery-splice-fill",
    ] {
        assert!(
            corpus_root()
                .join(scenario)
                .join("transcript.ndjson")
                .is_file(),
            "missing key scenario: {scenario}"
        );
    }
}
