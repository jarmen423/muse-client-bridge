# Vendored ACP/MSP transcript corpus (FORK_PLAN P1)

- **Source:** `.cache/reference/muse-acp/tests/protocol/transcripts/`
  (read-only; never modified), itself vendored from
  `github.com/meta-models/muse-code-sdk` @ `fbce769` — see upstream
  `PROVENANCE.md` and `LICENSE.muse-code-sdk` in the reference tree.
- **Contents:** 48 `mspTranscript` scenarios (each `manifest.json` +
  `transcript.ndjson`; `model-round-trip/` also carries
  `model-command-legs.ndjson`, `reconciliation-refold/` carries
  `records.jsonl`) plus upstream `README.md`.
- **Harness:** `tests/acp_transcripts.rs` validates every scenario:
  manifest/frame structure, JSON-RPC envelopes, `initialize` /
  `initialized` handshake shape, request/response id correlation,
  UUIDv7 `commandId`s, and schema-compat classification of each
  transcript's `initialize` result (fixture fingerprints must never
  classify as live-host `tested`).
- **Update rule:** re-copy from a single upstream revision only; keep
  the `TRANSCRIPT_FIXTURE_FINGERPRINT` pin in `src/acp/compat.rs` and
  the scenario-count pin in the harness in sync. P5 adds replay
  coverage on top of this structural net.
