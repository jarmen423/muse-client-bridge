# HANDOFF — Build Muse Bridge (next session)

## Mission

Implement the Rust binary specified in [SPEC.md](/home/josh/code/muse-client-bridge/SPEC.md):
a localhost OpenAI-compatible HTTP API (`/v1/chat/completions` +
`/v1/responses` + `/v1/models`) that drives one `muse serve` child over
MSP/stdio. Research is DONE and spec'd — do not redo it. Build the
packets below in order; each packet's gate must pass before starting
the next.

## Read order (30 min)

1. [SPEC.md](/home/josh/code/muse-client-bridge/SPEC.md) — the normative contract. If you
   must deviate, update it in the same change.
2. [AGENTS.md](/home/josh/code/muse-client-bridge/AGENTS.md) — conventions + the 14
   non-negotiable MSP rules. Rule violations are the most likely bugs.
3. [PRD.md](/home/josh/code/muse-client-bridge/PRD.md) — original goals (context only).
4. `.cache/reference/README.md` — what's cached where.

## Current state

- **Code:** none. No `Cargo.toml`, no `src/`. Greenfield.
- **Docs:** PRD, SPEC, README, AGENTS.md written and consistent.
- **Cache:** `.cache/reference/` (470 MB, git-ignored). Do NOT re-fetch,
  commit, or modify it.
- **Verified live** (muse 1.2.1): handshake + fingerprint
  `sha256:c7ff6c5d…`, UUIDv7-only commandIds, `turnId == commandId`,
  durable default / ephemeral via `--no-session-log`, real
  `authRequired` failure shape. Probe scripts were `/tmp` scratch
  (gone) — rewrite from SPEC §4 if needed.

## Phase-0 reuse decision (locked)

muse-acp (`.cache/reference/muse-acp`) was fully read. Decision: **build
fresh, port patterns** — do NOT depend on it, vendor it, or fork it.
It is std-only/sync/single-purpose (ACP); we need tokio/axum async +
OpenAI mapping. DO port its proven logic: `pending`-map transport,
reader trichotomy, UUIDv7 minter, per-method timeouts, fold
idempotency sets, compat table, exit discipline (full reuse list in
the research notes summarized by SPEC §4–§5). Rationale recorded —
do not re-open without new evidence.

## Global gates (every packet)

- `cargo clippy -- -D warnings` clean; `cargo fmt --check` clean.
- No `println!` in `src/`; structured `tracing` with
  request/session/turn ids as fields.
- Never log header values/keys (any level); message content at
  `debug` only.
- Runtime deps locked to: tokio, axum, serde/serde_json, clap,
  tracing/tracing-subscriber, uuid, futures. Anything else needs a
  written justification.

---

## P0 — Scaffold + CLI skeleton

**Goal:** buildable binary with CLI surface stubbed.

**Concepts:** single binary, clap-derive CLI, loopback-only listener
config (bind enforced in P5, flag parsed here).

**Contract:** flags `--port` (default `17489`), `--bind` (default
`127.0.0.1`), `--workspace-root` (default `.`), `--approval-mode`
(default `denyUnmatched`), `--muse-bin` (default `muse`),
`--trust-workspace`, `--log-format json|pretty`,
`--support`, `--selftest`. Env: `MUSE_CLI`, `MUSE_SERVE_ARGS`,
`MUSE_COMMAND_TIMEOUT_MS`, `MUSE_BRIDGE_PORT`, `RUST_LOG`.
`--support`/`--selftest` may exit `2 not-implemented` until P6.

**Owned files:** `Cargo.toml`, `Cargo.lock`, `src/main.rs`,
`src/lib.rs` (if you want lib+bin; binary-only is fine).

**Forbidden:** any MSP/HTTP logic; touching `.cache/`.

**Tests:** CLI parse tests (defaults + overrides).

**Verification:**
```sh
cargo build --release && ./target/release/muse-bridge --help
# expect: flag list above, exit 0
cargo test && cargo clippy -- -D warnings && cargo fmt --check
# expect: all green
```

**Done when:** help output matches contract; gates green.

## P1 — `msp::proto` (framing, envelope, ids)

**Goal:** byte-correct NDJSON/JSON-RPC layer with zero protocol bugs.

**Concepts (SPEC §4):** tolerate `\r\n`, skip blank lines, skip
unparsable lines WITHOUT dropping the connection, enforce 10 MiB
frame cap both directions, ignore unknown top-level members.

**Contract:**
- Writer: one JSON object + `\n`, flush per frame, never pretty-print.
- Reader: `AsyncBufReadExt::read_line`, trim trailing `\r`, skip
  empty, `serde_json` parse failure → `tracing::warn!` + continue.
- Request ids: monotonic u64, `pending: HashMap<String, oneshot>`
  keyed by exact JSON rendering (number vs string never equal).
  Unknown response id → warn + continue.
- commandIds: UUIDv7, single minter per connection
  (timestamp-ms + randomness + same-ms sequence). Minter MUST be
  injectable/deterministic for tests.
- Response validation: exactly one of `result`/`error`; `result`
  must be an object; error must carry integer `code`, string
  `message`, `data.kind` (else local ProtocolError, connection
  survives).

**Verified facts:** host rejects non-v7 commandIds (`invalidParams`,
seen live); server request ids are positive ints (connection-scoped).

**Owned files:** `src/msp/mod.rs`, `src/msp/proto.rs`.

**Tests:** CRLF/blank/garbage-line survival; 10 MiB cap trip;
id-type inequality (`1` vs `"1"`); UUIDv7 shape + monotonicity under
same-ms burst; result/error validation matrix.

**Verification:**
```sh
cargo test msp::proto
# expect: all pass, incl. garbage-survival + cap tests
```

**Done when:** every fold-rule-adjacent byte behavior above has a
named test; gates green.

## P2 — `msp::spawn` + `msp::host` (child, handshake, commands)

**Goal:** supervised child + handshake + thin command plane.

**Concepts (SPEC §4–4.2):** spawn boundary is the security boundary;
EOF→30 s→SIGTERM→2 s→SIGKILL on the POSIX process group; stderr
captured never parsed; ack ≠ outcome (always await view truth in
later packets — this packet only delivers acks).

**Contract:**
- Spawn: `MUSE_CLI` or `muse` + `serve` + `MUSE_SERVE_ARGS`
  whitespace-split; piped stdin/stdout, piped+captured stderr;
  `cwd` = workspace-root; inherit env. Classify spawn failure
  (`NotFound` → PATH/`MUSE_CLI` guidance).
- Reader task drains stdout continuously from birth (before
  handshake) on its own task; never blocks on downstream work.
- Handshake: `initialize {clientInfo:{name:"muse_bridge",
  version:env!("CARGO_PKG_VERSION")}}` → require
  `schema.version == 1` (else fatal) → fingerprint warn-only +
  record → `initialized` notify + flush. No command before it.
- Pin: `sha256:c7ff6c5d1e89cd42f803aea1f05b8e72082f2099685802473eb726903484713b`.
- Commands: per-method timeouts 30 s control / 180 s
  session+history / 60 s default (`MUSE_COMMAND_TIMEOUT_MS`
  override); timeout → local `-32603` naming method/id/session.
  Retry ONLY `overloaded`/`backpressured`, same `commandId`,
  jittered backoff, max 3. Client-side: refuse same-`commandId`
  with different payload (bug).
- Server requests: `{}` reply ONLY for `approval/request`,
  `userInput/request`; all else typed `-32601 methodNotFound`.
  (Auto-decision POLICY is P4; this packet only routes.)
- Exit codes: 0 clean / 1 unhandled / 2 usage / 3 config /
  4 lease / 5 sdk-off / else-or-signal crash; unknown non-zero ⇒
  crash. Restart ≤3 (250/500/1000 ms) when durable or durability
  absent; ephemeral ⇒ fail closed, no restart.

**Verified facts:** live result shapes for initialize/session/start/
turn-start-ack; `--no-session-log` ⇒ `sessionDurability:"ephemeral"`.

**Owned files:** `src/msp/spawn.rs`, `src/msp/host.rs`,
`tests/fixtures/fake_serve.py` (scripted fixture host — copy the
muse-acp fixture pattern, don't invent a new harness).

**Tests (scripted-host):** handshake ok; version≠1 fatal;
fingerprint-mismatch warn-continues; pre-handshake command rejected
locally; timeout synthesis; retry-same-id on overloaded (fake host
counts); no-retry on inputTooLarge; unknown server method → 32601;
EOF{classify,restart≤3,durable} vs {fail-closed,ephemeral}.

**Verification:**
```sh
cargo test --test scripted_host
# expect: all pass; fake host transcript shows same-commandId retries
```

**Done when:** every contract bullet has a failing-without-it test;
gates green.

## P3 — `msp::fold` (per-request view fold)

**Goal:** turn a notification stream into exactly-once output events.

**Concepts (SPEC §4.3):** deltas accumulate, `item/completed` is
truth; replace-iff-higher-revision; gap means stale; await YOUR
turn's terminal only.

**Contract (input → output events):**
- Route by `sessionId`/`turnId`; ignore other sessions' events.
- `(itemId, field)` delta concat in cursor order; full-object truth
  on `item/completed`; accept `updated|completed` for never-started
  items; `truncated: true` surfaces metadata, never claims
  completeness.
- Track `viewCursor` from EVERY event. `view/gap{after,next}` →
  mark stale → forward `view/page{cursor:after,forward,limit:100}`
  → re-feed through fold with `done` + `usage_seen` idempotency
  sets → resume.
- Terminals for the awaited `turnId`: `turn/completed` (any
  `terminal` string: `completed`→ok, `cancelled`→cancelled, ALL
  else incl. unknown→failed with `error{kind,message,retryable?}`);
  `turn/unqueued`|`turn/retracted` settle now (cancelled);
  `turn/retryScheduled` NEVER settles (log attempt/max/delay).
- Usage: accumulate `session/tokenUsage` counted-once per
  `viewCursor` (`promptTokens` + `usage.outputTokens`); reconcile
  with `cumulative` at close. Snapshot `contextUsage: null` is
  normal — never blank known state; never expect usage from
  `view/page`.
- Unknown methods/kinds/enum values → debug-log + ignore. Includes
  live-but-undeclared `session/started`.
- Expose `cancel()` → `turn/cancel` with the EXPLICIT turnId
  (same-`commandId` discipline).

**Owned files:** `src/msp/fold.rs`.

**Tests:** delta-concat==final; out-of-order/duplicate delivery;
gap splice converges; each terminal arm (incl. unknown terminal
string, unqueued-no-completed, retryScheduled-no-settle);
usage counted-once across redelivery; unknown-kind survival.

**Verification:**
```sh
cargo test msp::fold
# expect: all pass; try deleting any one rule → its test fails
```

**Done when:** mutation-check above holds informally per rule;
gates green.

## P4 — `translate` + `dispatch` (mapping + orchestration)

**Goal:** one HTTP-request-equivalent turn, end to end against a
scripted host.

**Concepts (SPEC §4.4–§5):** one MSP session per request (stateless
v1); MSP owns history, clients resend it; approvals fail closed;
adopt folded ids, never echoes.

**Contract:**
- `session/start {workspaceRoot, approvalMode}` (mode flag, default
  `denyUnmatched`) → adopt `session.sessionId` +
  `approvalMode.mode` from the FOLD, verify mode matches request
  (mismatch ⇒ fail, never silently downgrade).
- Optional `session/setModel {model:{modelId}}` when request names
  one; record folded `session/modelChanged` as truth.
- `turn/start {input, reasoningEffort?}` default `ifBusy` (queue);
  expect `started`, `turnId` from ACK (never derive).
- `translate`: chat `messages[]` → single ordered text (system =
  leading context; `tool` msgs as `Tool result (<id>): …`);
  responses `instructions` + `input` items likewise (function-call
  items as labeled text; skip reasoning items — never resubmit
  provider reasoning); images (`image_url` http/https fetch with
  timeout+size cap, or `data:` decode) → MSP `image` parts,
  oversize ⇒ caller 400. `displayText`: `"<method> <model>"`.
  `reasoningEffort`: pass through
  none|minimal|low|medium|high|xhigh|ultra (`max`→`ultra`).
  `temperature` etc: accept, ignore, document.
- `tools`/`tool_choice`: accept, ignore (log at debug). NEVER emit
  client tool_calls (N2).
- Approvals: on `approval/request`, `approval/decide` with FIRST
  non-approving `availableChoices` entry + current
  `requirementId`; none exists ⇒ `turn/cancel` instead. Log every
  auto-decision with durable id. `userInput/request` ⇒
  `userInput/cancel {reason:"headless-bridge"}`.
- Stream mapping: `agentMessage` text → content deltas;
  `reasoning.summary.N` → reasoning deltas (part breaks clean);
  `toolCall` → marked status text (`[tool: name] …`), never silent;
  unknown kinds → `fallbackText` or skip (never crash).
- Output event enum consumed by P5 (define here, e.g.
  `ContentDelta(String)`, `ReasoningDelta(String)`,
  `StatusLine(String)`, `UsageSnapshot(…)` …keep it minimal).

**Owned files:** `src/translate.rs`, `src/dispatch.rs`.

**Forbidden:** HTTP/SSE shapes (P5 consumes the event enum only).

**Tests:** message-flatten goldens (incl. tool-role fold,
image `data:` URL, oversize-image 400); reasoning-effort map;
approval auto-deny selection + no-deny-choice ⇒ cancel;
user-input auto-cancel; full scripted-host turn incl.
cancel-mid-tool and gap-mid-turn.

**Verification:**
```sh
cargo test translate && cargo test --test scripted_dispatch
# expect: goldens byte-exact; cancel/gap scenarios pass
```

**Done when:** a scripted full turn produces the exact event
sequence P5 needs; gates green.

## P5 — `http/` (endpoints, SSE, errors)

**Goal:** the public OpenAI-compatible surface.

**Concepts (SPEC §3, §6):** liberal parsing (strict 400-on-unknown
BREAKS Hermes); every stream has its mandated terminal; auth is
ignored, not validated.

**Contract:**
- Bind `--bind` (default loopback; refuse non-loopback without an
  explicit `--allow-remote` escape hatch you add here — default
  closed).
- `POST /v1/chat/completions`: stream ⇒ SSE
  `text/event-stream`, `data: {chunk}` per content delta,
  `choices[{delta:{content?},finish_reason?}]`,
  `finish_reason:"stop"` always in v1, final usage chunk when
  `stream_options.include_usage`, terminal `data: [DONE]`, then
  close. Non-stream ⇒ full `ChatCompletion` JSON. Mid-stream
  failure ⇒ last `data: {"error":…}` frame, then close (no
  fake `"stop"`).
- `POST /v1/responses`: stream ⇒ `response.created{id}` →
  `response.output_item.added` → `response.output_text.delta`×N →
  `response.output_item.done` → `response.completed{id,usage}`
  (MANDATORY terminal — close-without-it is a spec violation).
  Failure ⇒ `response.failed{error:{code,message}}`. Non-stream ⇒
  full `Response` object. Accept+ignore `previous_response_id`,
  `conversation`, `client_metadata`, `prompt_cache_key`,
  `service_tier`, `text`, `include`, `tools`.
- `GET /v1/models` from cached `model/list` (retain-last-good).
- `GET /healthz` → `{"status":"ok","msp":"ready|starting"}`.
- ANY `Upgrade: websocket` → `426 Upgrade Required` (no WS code
  path anywhere). Unknown routes → OpenAI-shaped 404 JSON.
- `Authorization`/extra headers: ignored, never logged.
- Error table = SPEC §6 verbatim (401 login / 429+Retry-After /
  400 context / 500 crash / 503+Retry-After:5 dead-host).
  `Retry-After` on 429 AND 503.

**Owned files:** `src/http/mod.rs`, `src/http/chat.rs`,
`src/http/responses.rs`, `src/http/sse.rs`, `src/http/models.rs`,
`src/http/error.rs`.

**Tests:** golden SSE transcripts (both endpoints, success +
  mid-stream-failure); `[DONE]`/`response.completed` presence
  enforced even on empty output; 426 on WS upgrade; unknown-field
  tolerance (send Hermes/Codex-shaped extra fields); §6 mapping
  row-per-row; `/v1/models` retain-last-good.

**Verification:**
```sh
cargo test http
curl -s localhost:17489/v1/models          # expect {data:[...]} via fake host in dev? NO —
```
  NOTE: no curl-against-real-host until P7. P5 gate is golden tests only.

**Done when:** goldens byte-exact; every §6 row has a test; gates green.

## P6 — `support`, examples, docs sync

**Goal:** operability + client onboarding artifacts.

**Contract:** `--support` prints versions, pinned-vs-live
fingerprint, durability, last stderr tail. `--selftest` runs
handshake + `model/list` + minimal turn probe, exit 0/1, and
detects the `authRequired` no-login state with a `muse login`
hint. `examples/` gains: `model_catalog_json` template
(`ModelsResponse` shape — crib from
`.cache/reference/codex/codex-rs/models-manager/models.json`),
Hermes YAML snippet, Codex provider-table + Desktop root-key
snippets (must match README). README troubleshooting verified
against real behavior.

**Owned files:** `src/support.rs`, `examples/*`, README edits.

**Verification:**
```sh
./target/release/muse-bridge --support     # expect: versions+fingerprint+tail, exit 0
./target/release/muse-bridge --selftest    # expect: 0 logged-in / clear 401-hint logged-out
```

**Done when:** both commands behave per contract with and without
login; gates green.

## P7 — Live + client-compat gates (needs `muse login`)

**Goal:** prove it against reality. NOT optional for "done".

**Contract:**
- `MUSE_LIVE_TESTS=1 cargo test` green (live-host tests env-gated,
  skipped otherwise — CI-safe).
- One REAL task per endpoint (e.g. "create+edit a file in a scratch
  workspace"), not a hello-turn: exercises toolCall streaming,
  denyUnmatched auto-decisions, usage accumulation. Hello-only E2E
  does NOT pass this gate (top risk: SPEC §9).
- Manual P3 trio with results recorded: (1) Hermes CLI via custom
  provider (stream + tools-sent); (2) Codex CLI via custom table —
  confirm 426→HTTP fallback in trace; (3) Desktop via config
  overlay — picker lists bridge catalog. Snapshot
  `~/.codex/config.toml` before (3).

**Verification:** paste command transcripts + client outcomes into
the final report / release notes.

**Done when:** all green; OQ1 (reasoningEffort set) and OQ2
(Desktop catalog) closed with evidence; OQ answers written back
into SPEC §11.

---

## Definition of done

Release binary builds; P0–P6 gates green; P7 live + trio green;
SPEC §9 P0–P2 pass; clippy/fmt clean. P3 results recorded. Then —
and only then — "releasable".

---

## Post-P7: provider mode (2026-09-14)

`--workspace-root` is now optional (default: unset). The client
owns the project/workspace, so the bridge sends no `workspaceRoot`
in `session/start` (verified live: the host adopts `null`) and the
host child parks in `<temp>/muse-bridge-no-workspace` instead of
the operator's cwd. Bare `muse-bridge` with no flags is the
provider-shaped invocation. SPEC §4.4/§7 and SETUP updated;
P0's `--workspace-root` (default `.`) contract above is
superseded.
