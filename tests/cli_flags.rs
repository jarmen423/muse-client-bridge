//! Binary-level CLI wiring: flags reach the subsystems they name.
//!
//! Regression: `--muse-bin` was parsed but never passed to `HostConfig`
//! (explicit flags silently used `MUSE_CLI`-or-`muse`). A service baking
//! `--muse-bin /abs/path` still spawned bare `muse` and crash-looped under
//! systemd's minimal `PATH`.

use std::process::Command;

fn bridge() -> Command {
    Command::new(env!("CARGO_BIN_EXE_muse-bridge"))
}

#[test]
fn muse_bin_flag_reaches_the_host_launch() {
    // A decoy MUSE_CLI proves flag-over-env precedence end to end: the
    // failure must name the FLAG value, never the env value or bare `muse`.
    let flag_bin = "/nonexistent-muse-bin-xyz-12345";
    let out = bridge()
        .arg("--muse-bin")
        .arg(flag_bin)
        .arg("--selftest")
        .env("MUSE_CLI", "/decoy-env-muse-abc-67890")
        .output()
        .expect("run muse-bridge --selftest");
    assert_eq!(out.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("FAIL handshake"),
        "unspawnable backend fails the handshake step: {stdout}"
    );
    assert!(
        stdout.contains(flag_bin),
        "flag value must flow through to the launch: {stdout}"
    );
    assert!(
        !stdout.contains("decoy-env-muse"),
        "env must not win over the flag: {stdout}"
    );
}
