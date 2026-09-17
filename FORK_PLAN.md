# FORK PLAN — ACP surface with full Muse CLI / MSP parity

> **Status note (2026-09-16):** P4's fail-closed auto-deny was superseded by
> TODO P9 — approvals now bridge to `session/request_permission` and user
> input to `elicitation/create` (fail closed only when the client lacks the
> surface). See TODO.md for the landed parity list.

## Goal

Add a `muse-acp`-derived ACP server surface to `muse-bridge` so IDE clients
(Zed, JetBrains) get full parity with what the Muse Code CLI (`muse` 1.2.1)
offers over MSP — closing the gaps and inconsistencies in `muse-acp` 0.3.0
as we go.

## Success Criteria

- An ACP client can connect to the new binary, open a session, run turns
  (text + file edits + tool approvals), and close cleanly.
- Every item in the parity checklist (§ Work Plan, P3–P4) is implemented or
  explicitly deferred with a logged warn — no silent gaps.
- Full suite green: existing tests + ported muse-acp tests + vendored
  transcripts; `cargo clippy -- -D warnings` and `cargo fmt --check` clean.
- At least one live-gated (`MUSE_LIVE_TESTS=1`) ACP file-edit task passes.
- HTTP surface (`/v1/chat`, `/v1/responses`, `/v1/models`) behavior unchanged.

## Context And Current Facts

- This repo: 9127 lines of `src/` (P6 done per HANDOFF packets) — tokio +
  axum async bridge: `src/msp/` (spawn, proto, host, fold),
  `src/http/` (chat, responses, models, sse, error), `src/dispatch.rs`,
  `src/translate.rs`, `src/support.rs`, `src/cli.rs`.
- `muse-acp` 0.3.0 (`.cache/reference/muse-acp`, git-ignored, do not modify):
  std-only, sync, single-session, ACP-only. Sources: `main.rs`, `acp.rs`,
  `msp.rs`, `fold.rs`, `compat.rs`, `zed.rs`, `json.rs`, `sha256.rs`;
  `tests/protocol/transcripts/` (~50 transcripts) + `fake_serve.py` fixture.
- HANDOFF Phase-0 locked "build fresh, port patterns — do NOT fork". This
  plan supersedes that decision **for the ACP surface only** (user request
  2026-09-16, backed by `deep-research` synthesis); the tokio core stays.
- Authoritative source order on conflict: (1) local binary schema
  (`.cache/reference/local-schema-ts`, `local-schema-json`, muse 1.2.1,
  fingerprint `sha256:c7ff6c5d…`) — binary wins; (2) live-probe facts in
  SPEC §4; (3) muse-acp source + transcripts; (4) SDK mirror + msp-docs
  (lags: missing 4 methods + 3 notifications; background only).
- Known drift to close (binary adds over SDK mirror): `item/readOutput`,
  `session/rename`, `session/setReasoningEffort`, `view/subscribe`, three
  `session/*Changed` notifications, `outputUnavailable`, `sessionMcp`,
  `userInputDialogs`. Live `session/started` is in NEITHER bundle —
  tolerate per AGENTS.md rule 7.

## Constraints And Non-goals

- Out of scope: WebSocket (426 on `Upgrade: websocket`), client tools,
  auth/credentials handling, bind beyond loopback, TLS, sticky sessions
  until v2.1.
- Global gates (every packet): clippy + fmt clean; no `println!` in `src/`;
  structured `tracing` with request/session/turn ids; never log header
  values/keys; message content at `debug` only.
- Runtime deps locked to Cargo.toml set (tokio, axum, serde/serde_json,
  clap, tracing/tracing-subscriber, uuid, futures + justified base64,
  reqwest). New deps need written justification. ACP port expected to need
  none (stdio + JSON).
- AGENTS.md's 14 non-negotiable MSP rules apply to all ported code.

## Key Decisions

1. **Reuse the ACP surface, rewrite the core.** Port `acp.rs`/`compat.rs`/
   `zed.rs` adapter logic onto our tokio multi-session MSP core
   (`src/msp/*`, `src/dispatch.rs`). Rejected: forking muse-acp as-is (sync,
   single-session, can't serve HTTP concurrently) and depending on it as a
   crate (its core assumptions conflict with our engine; HANDOFF rationale
   still holds for the core).
2. **Binary schema wins.** Where muse-acp, the SDK mirror, and
   `local-schema-*` disagree, implement the local binary's shape.
3. **Second binary, additive.** ACP ships as a new `[[bin]]` (`acp_main`,
   binary name TBD) plus new `src/acp/` and `tests/acp-*` files. No changes
   to HTTP behavior; rollback is deletion.
4. **Vendor the transcript corpus** under `tests/acp-protocol/` as the
   regression net, mirroring muse-acp's approach.
5. **Drop hand-rolled JSON/SHA** (`json.rs`, `sha256.rs`) — serde_json covers
   JSON; any hash need goes through an audited crate with justification.

## Recommended Approach

Port the proven adapter logic, adapt it to async multi-session dispatch, and
close each drift item against the binary schema:

| muse-acp source | Proven logic | Lands in |
|---|---|---|
| `src/msp.rs` | pending-map transport, exact `RpcId`, per-method timeouts, same-`commandId` retry | diff against `src/msp/proto.rs`, `src/msp/host.rs`; close gaps |
| `src/msp.rs` | reader trichotomy, NDJSON CRLF tolerance, 10 MiB cap | verify in `src/msp/proto.rs` |
| `src/msp.rs` | UUIDv7 minter (timestamp-ms + rand + same-ms seq) | verify in `src/msp/proto.rs` |
| `src/fold.rs` | idempotency sets (`done` + `usage_seen`), delta-concat, completed=truth | diff against `src/msp/fold.rs`; close gaps |
| `src/compat.rs` + `src/zed.rs` | compat table, Zed adaptations | new `src/acp/compat.rs` |
| `src/acp.rs` | ACP session/update mapping | new `src/acp/server.rs` + `src/acp/sessions.rs` |
| error mapping | MSP kind → client errors | new `src/acp/errors.rs` (SPEC §6 branching) |
| transcripts + `fake_serve.py` | regression corpus, scripted-host pattern | `tests/acp-protocol/`, extend `tests/fixtures/fake_serve.py` |

Inconsistency fixes vs muse-acp: (1) sync → tokio multi-session
multiplexing; (2) ACP-only → dual HTTP+ACP surface on one core;
(3) close every §Context drift item; (4) keep mandatory HTTP stream
terminals (`response.completed`, `[DONE]`); (5) report usage once with
per-endpoint names.

## Work Plan

- **P1 — Vendor corpus + compat port.** Vendor transcripts to
  `tests/acp-protocol/` with harness; port `compat.rs`/`zed.rs` to
  `src/acp/compat.rs`. No behavior change to existing surfaces.
- **P2 — ACP server skeleton.** `src/acp/server.rs` (stdio framing,
  dispatch), `src/acp/sessions.rs` (ACP session ↔ MSP session/turn via
  `dispatch.rs`), `src/bin/acp_main.rs`. First cut: one ACP session per
  process if multiplexing slips (see Risks).
- **P3 — Handshake / session / turn / view parity.** Line-by-line diff of
  `proto.rs`/`host.rs`/`fold.rs` against muse-acp `msp.rs`/`fold.rs`;
  implement: init/version-fatal/fp-warn/`initialized` gate; full session
  ops (start/resume/fork/list/read, setModel/rename/setReasoningEffort/
  compact/userShell); turn ops (start/steer/interrupt/cancel/unqueue);
  8-tier effort; ack≠outcome; unqueue/retract settle, retryScheduled never
  settles; open terminals; view routing, delta-concat, rev-iff-higher,
  cursor tracking, gap→page(100)+refold, subscribe/readOutput.
- **P4 — Approvals / errors / tolerance.** `src/acp/errors.rs`; fail-closed
  approvals (first non-approving choice, else cancel; never synthesize;
  log loudly with durable id); user-input auto-cancel; retry ONLY
  overloaded/backpressured ×3 same-`commandId`; unknown notifications/item
  kinds/enums/lines tolerated (rule 7); open-enum audit on all ported
  matches; stderr bounded-tail capture.
- **P5 — Tests + live proof.** Port ~70 muse-acp tests; ACP session/update
  goldens; live-gated ACP file-edit task.
- **P6 — Docs.** Refresh stale HANDOFF state line, update SPEC drift list,
  record client-compat results in release notes.

## Validation Plan

- `cargo test` — full suite incl. new `tests/acp_*` and vendored transcripts
  (scripted-host; no login needed). Expected: all green.
- `MUSE_LIVE_TESTS=1 cargo test` — incl. live-gated ACP file-edit task
  (needs `muse` login; skipped otherwise, CI-safe).
- `cargo clippy -- -D warnings` and `cargo fmt --check` — clean after every
  packet.
- Highest-risk check: P3 settle rules (`turn/retryScheduled` never settles;
  no hang on reclaim) — covered by scripted-host reclaim transcripts.
- Manual (release gate, SPEC §9 P3): Zed + JetBrains connect, run a file-edit
  turn, approve/deny a tool, cancel mid-tool; record in release notes.

## Risks / Rollback

- Deny-storm behavior under fail-closed approvals needs P7 live proof
  (requires `muse` login) — risk until then.
- ACP multi-session multiplexing is new design (muse-acp is single-session);
  fallback: one ACP session per process for the first cut.
- `session/started` shape evidence is transcript-only — code defensively
  (rule 7) until live-captured.
- Rollback: ACP is purely additive (new `src/acp/`, new bin, new tests).
  Revert by deleting those paths; HTTP surface and MSP core untouched.

## Open Questions

1. HANDOFF `code: none` state line is stale (9127L exist) — refresh in P6.
2. P7/P3 release gates (live proof, client-compat) still pending.
3. `session/started` shape is transcript-only — needs live capture.
4. `classify-serve-exits` body was unopened during research — verify before
   porting exit discipline.
5. SPEC §6 kinds `rateLimit` / `contextLength` / `contextWindow` ungrounded —
   verify against local schema + live probes.
6. `usage` from `view/page` docs missing — confirm "never expect" with a probe.
7. `sessionMcp` / `userInputDialogs` schema bodies need opening.
8. `fold.rs` / `translate.rs` / `spawn.rs` full bodies unread during research —
   diff against muse-acp in P3 before porting.
9. `json.rs` / `sha256.rs` / `zed.rs` / `compat.rs` line-level verification
   pending; open-enum itemization pending with it.
10. ACP binary name TBD (`muse-acp-bridge` proposed) — naming decision at P2.
