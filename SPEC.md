# Muse Bridge — Specification

**Status:** draft for review · **Date:** 2026-09-14 · **Source of truth:** [PRD.md](/home/josh/code/muse-client-bridge/PRD.md)

Muse Bridge is a single Rust binary that runs on localhost, exposes an
OpenAI-compatible HTTP API, and translates each request into the Muse Session
Protocol (MSP) spoken to a `muse serve` child process over stdio. Model
credentials come from the user's existing `muse login` and are inherited by
`muse serve`; the bridge handles no API keys and stores no credentials.

## 1. Goals and non-goals

Goals (v1):

- G1. Serve Hermes Agent (Desktop App) via `POST /v1/chat/completions`
  (streaming SSE + non-streaming JSON).
- G2. Serve Codex clients (CLI, IDE, and ChatGPT Desktop's bundled
  `codex app-server`) via `POST /v1/responses` (streaming SSE;
  non-streaming JSON as a cheap second shape).
- G3. Serve `GET /v1/models` from MSP `model/list` so pickers and
  doctor flows work.
- G4. One `muse serve` child per bridge process; N concurrent HTTP
  requests multiplexed onto N MSP sessions on that one host.
- G5. Headless-safe by default: sessions start in `denyUnmatched`
  approval mode so a headless request can never park forever on an
  approval dialog.
- G6. Ship as a single binary (`cargo build --release`); only
  requirement is a `muse` binary on `PATH`.

Non-goals (v1):

- N1. No WebSocket server. Neither target client needs one (see §3,
  research question 1). The bridge answers any WS upgrade with
  `426 Upgrade Required` so Codex falls back to HTTP instantly.
- N2. No client-executed function calling. The bridge accepts (and
  ignores) the client's `tools`/`tool_choice` and lets the muse agent
  execute its own host tools opaquely. A real tool-call bridge
  (MSP approvals → client `tool_calls`) is a v2 design item (§8).
- N3. No Zed ACP surface in v1 (defer to v2; `muse-acp` proves the shape).
- N4. No auth, no TLS, no credentials storage. Binds loopback only.
- N5. No sticky cross-request sessions in v1: one MSP session per HTTP
  request (clients resend full history; MSP owns nothing across
  requests). Sticky sessions are a documented v2 option (§8).

## 2. Research answers (binding)

### Q1 — Can Hermes and/or ChatGPT apps consume WebSockets?

**Hermes: no.** Model traffic is exclusively the `openai` Python SDK
over HTTP(S) via httpx with SSE streaming. Zero `websocket`/`wss`
references exist in `agent/transports/`, `agent/client_lifecycle.py`,
or the `custom` provider plugin; the only websocket mentions in
`agent/`/`hermes_cli/` are unrelated (browser CDP, gateway liveness,
PTY). Evidence: `.cache/reference/hermes-agent/agent/transports/chat_completions.py`,
`.cache/reference/hermes-agent/agent/chat_completion_helpers.py:2712-2721,2803`.

**Codex: yes, but never required.** Codex speaks Responses-over-WebSocket
only when the provider sets `supports_websockets: true` (default
`false`; only the built-in `openai` provider sets it). Any other
provider — including every custom localhost table — uses plain HTTP
POST + SSE. When a WS handshake answers HTTP `426 Upgrade Required`,
Codex returns `FallbackToHttp` and proceeds over SSE with no user
impact. Evidence:
`codex-rs/model-provider-info/src/lib.rs:149-151,496` (`supports_websockets`),
`codex-rs/core/src/client.rs:936-944` (`responses_websocket_enabled`),
`codex-rs/core/src/client.rs:1740-1743` (426 → `FallbackToHttp`).

**ChatGPT Desktop app: has no WS or custom-endpoint UI of its own.**
Integration runs through its bundled `codex app-server`, which reads
`~/.codex/config.toml`. Pointing that file's root `openai_base_url` at
the bridge redirects Desktop model traffic to us (third-party recipe,
corroborated by first-party config keys `openai_base_url` /
`model_catalog_json` in `codex-rs/config/src/config_toml.rs:382-406`
and `codex-rs/core/src/config/mod.rs:3678-3684`). Because that path
keeps `model_provider = "openai"` (which has `supports_websockets =
true`), the bridge MUST answer WS upgrades with `426` for fast
fallback. It MUST NOT implement the WS submodule protocol.

**Decision:** no WS server; `426` on any `Upgrade: websocket` request.

### Q2 — `/v1/responses`?

**Both wire shapes are required — one per client family:**

| Client | Wire API | Evidence |
|---|---|---|
| Hermes Agent | `POST /v1/chat/completions` only. `codex_responses` mode triggers solely for official OpenAI/Meta hosts or profiles declaring it; a localhost `base_url` never mandates it, and a stale `codex_responses` mode on a custom endpoint is explicitly discarded. | `hermes-agent/hermes_cli/providers.py:289-325,352-373`, `hermes-agent/hermes_cli/runtime_provider.py:145-153` |
| Codex (CLI/IDE/Desktop) | `POST /v1/responses` only. `WireApi` has a single variant `Responses`; `wire_api = "chat"` is a hard config error. | `codex-rs/model-provider-info/src/lib.rs:66-95` |

So the bridge implements **both** `POST /v1/chat/completions` and
`POST /v1/responses` (+ `GET /v1/models`). There is no single endpoint
that serves both clients.

## 3. HTTP surface (normative)

Base URL: `http://127.0.0.1:<PORT>/v1` (default port: `17489`; override
via `--port` / `MUSE_BRIDGE_PORT`). Binds loopback only; no TLS.

### 3.1 `POST /v1/chat/completions` (Hermes)

Accept (superset-tolerant — **ignore unknown fields**; Hermes sends
`reasoning_effort`, `thinking`, `options.num_ctx`, `prompt_cache_key`,
`stream_options`, … and strict 400-on-unknown breaks it):

```jsonc
{
  "model": "muse-spark-1.3",
  "messages": [
    {"role": "system", "content": "..."},
    {"role": "user", "content": "..."},
    {"role": "assistant", "content": "...", "tool_calls": [...]},
    {"role": "tool", "tool_call_id": "...", "content": "..."}
  ],
  "tools": [{"type": "function", "function": {"name": "...", "parameters": {...}}}],
  "tool_choice": "auto",
  "temperature": 0.7,
  "stream": true,
  "stream_options": {"include_usage": true}
}
```

- `messages` → one MSP `turn/start` text input (§5.2). `tools` /
  `tool_choice` accepted and ignored in v1 (N2); never echo
  `tool_calls` in responses.
- Non-streaming (`stream` absent/false): return standard
  `ChatCompletion` (`id`, `object`, `created`, `model`,
  `choices[{index, message:{role, content}, finish_reason}]`, `usage?`).
  `finish_reason` is always `"stop"` in v1 (or `"length"` on MSP
  truncation; never `"tool_calls"`).
- Streaming (`stream: true`): OpenAI SSE — `Content-Type:
  text/event-stream`, one `data: {ChatCompletionChunk}` per MSP
  `item/delta`, terminal `data: [DONE]`. Chunks carry
  `choices[{delta:{content?}, finish_reason?}]`; when
  `stream_options.include_usage` is set, emit a final usage chunk
  (Hermes tolerates its absence, but support it).
- Errors: OpenAI error JSON (`{error:{message, type, code}}`) with
  mapped HTTP status (§6). Retryable outages SHOULD send
  `Retry-After` (Hermes honors it; SDK retries are disabled
  client-side).

### 3.2 `POST /v1/responses` (Codex / ChatGPT Desktop)

Codex always sends `stream: true` and POSTs to the provider-relative
path `/responses` (i.e. `{base_url}/responses`). Accept (ignore unknown
fields — Codex sends `client_metadata`, `prompt_cache_key`,
`service_tier`, `text`, `include`, …):

```jsonc
{
  "model": "muse-spark-1.3",
  "instructions": "...",
  "input": "… or [ResponseItem…]",
  "tools": [...],
  "tool_choice": "auto",
  "parallel_tool_calls": true,
  "reasoning": {"effort": "medium", "summary": "auto"},
  "store": false,
  "stream": true,
  "stream_options": {"reasoning_summary_delivery": "…"},
  "include": ["…"]
}
```

- Streaming SSE vocabulary the bridge MUST emit (Codex parses in
  `codex-rs/codex-api/src/sse/responses.rs:354-530`):
  1. `response.created` with `response.id` (bridge-minted `resp_…`).
  2. `response.output_item.added` (message item shell).
  3. `response.output_text.delta` × N (`delta` = text).
  4. `response.output_item.done` (full message item).
  5. `response.completed` with `response.id` + `usage` — **REQUIRED
     terminal**; a stream that closes without it is a client-side
     error (`"stream closed before response.completed"`).
- Failure path: `response.failed` with `response.error.{code,message}`
  (codes: `rate_limit_exceeded` → retried; `context_length_exceeded`,
  `insufficient_quota` → fatal; bridge maps MSP failures to the
  closest code, §6).
- Non-streaming (`stream: false`): return the full `Response` object.
  Rare from Codex, but implement — it is the same fold without chunk
  emission.
- `previous_response_id` / `conversation`: v1 ignores server-side
  threading (N5) but MUST accept and ignore the fields.
- `tools`: accepted and ignored in v1 (N2). Never emit
  `function_call` items; a turn that only ran host tools completes as a
  normal text response.

### 3.3 `GET /v1/models`

`{data: [{id, object: "model", created, owned_by}]}` sourced from MSP
`model/list` (`modelId` → `id`). Cache with retain-last-good on
failure (muse-acp pattern). Hermes treats this endpoint as optional
(static fallback); Codex model discovery prefers its catalog file but
probes `/models` — serve it always.

### 3.4 Fallback routes

- Any request with `Upgrade: websocket` → `426 Upgrade Required`
  (Codex 426 → HTTP fallback; §2 Q1). No WS handshake, ever.
- `GET /healthz` → `200 {"status":"ok","msp":"ready|starting"}` for
  supervisors. (Non-OpenAI extension; harmless.)
- Unknown routes → OpenAI-shaped 404 JSON.

### 3.5 Auth

Ignore `Authorization` entirely (any bearer, empty, or missing all
accepted) — same posture as Hermes' own subscription proxy. Never
require, validate, forward, or log credential values. Extra headers
(`User-Agent`, `OpenAI-Organization`, …) ignored.

## 4. MSP client (normative)

One `muse serve` child per bridge process, spawned at startup:

```text
muse serve [--trust-workspace] [extra args from MUSE_SERVE_ARGS]
```

- Binary path: `MUSE_CLI` env or `muse` on `PATH` (muse-acp
  convention). `cwd`: bridge `--workspace-root` when set, else a
  bridge-managed inert dir (`<temp>/muse-bridge-no-workspace`).
  Env: inherit (so `muse login` credentials apply).
- Cleanup decoder ring: `stdio` NDJSON, one JSON-RPC 2.0 object per
  line, `\n`-terminated, flushed writes; tolerate `\r\n` on input;
  skip blank lines; **skip unparsable lines without dropping the
  connection**.
- Reader: dedicated tokio task draining stdout continuously (stdio
  server never severs a wedged pipe — a blocked reader wedges the
  bridge, not the session).
- stderr: captured (bounded tail, 8 KiB / 100 lines), never parsed,
  surfaced on failures and in `--support` diagnostics.
- Shutdown: close stdin (EOF) → 30 s drain budget → SIGTERM → 2 s →
  SIGKILL, on the whole POSIX process group (SDK `spawn.ts`
  discipline). Exit codes classified per the exit-classification
  contract (0 clean / 1 unhandled / 2 usage / 3 config / 4 lease /
  5 SDK-surface-off / else crash); unknown non-zero ⇒ crash row.

### 4.1 Handshake

1. Send `initialize {clientInfo:{name:"muse_bridge",version:<cargo>},
   capabilities:{requestedCapabilities:["userShell"]}}`. Unknown
   entries are never granted (no error); record `grantedCapabilities`
   in the handshake facts.
2. Validate result: `schema.version == 1` else **fatal**;
   `schema.fingerprint` mismatch vs the pinned build fingerprint ⇒
   warn only (log + `X-MSP-Fingerprint-Warn` response header).
   Record `sessionDurability` and `serverInfo`.
3. Send `initialized` notification. No command before it is accepted.

Pinned fingerprint (muse 1.2.1, local schema export):
`sha256:c7ff6c5d1e89cd42f803aea1f05b8e72082f2099685802473eb726903484713b`.
Authoritative schema source is `muse schema generate-ts` on the
installed binary — verified NEWER than the published SDK mirror
(local adds `item/readOutput`, `session/rename`,
`session/setReasoningEffort`, `view/subscribe`,
`session/modelRouteUnserved`, `session/nameChanged`,
`session/reasoningEffortChanged`, `outputUnavailable`,
`sessionMcp`, `userInputDialogs`). Never treat the mirror as newer
than the binary.

### 4.2 Commands (the thin command plane)

- `commandId`: UUIDv7, single monotonic minter per connection
  (timestamp-ms + randomness + same-ms sequence). **Retries reuse the
  same `commandId`**; a fresh id on retry is a duplicate-execution
  bug. Client-side memory also rejects reusing one id with a
  different payload.
- JSON-RPC `id`: separate monotonic counter (string or int; keep the
  two id families un-conflated). `pending: HashMap<id, oneshot>`
  correlation; unknown response ids logged, never fatal.
- Per-method timeouts: 30 s control/handshake/queries, 180 s
  `session/start|resume|read` and `view/page`, 60 s default,
  overridable via `MUSE_COMMAND_TIMEOUT_MS`. Timeout ⇒ synthesize
  local `-32603` naming method/id/session (muse-acp `msp.rs:96-108`).
- Retryable-nothing-admitted: `overloaded` (-32001) and
  `backpressured` (-32031) ONLY → jittered exponential backoff, same
  `commandId`, max 3 attempts. `inputTooLarge` → shrink, never blind
  retry. All other errors → fail the HTTP request with mapping (§6).
- Admission ack ≠ outcome. Every command's truth arrives on the view
  stream; the bridge MUST await view terminals, never return on ack.
- Server-initiated requests: reply `{}` ONLY to `approval/request`
  and `userInput/request` (presentation receipt; real effect travels
  via `approval/decide` / `userInput/*` commands); every other
  server method gets typed `methodNotFound` (-32601). In v1 with
  `denyUnmatched`, approval requests should be rare — but handle them
  (auto-deny path, §5.4) rather than crash.

### 4.3 View fold (streaming truth)

Subscribe implicitly via `session/start` (never send
`view/subscribe`). Per HTTP request the bridge folds ONLY its own
turn's events; a shared dispatcher routes view notifications to the
request task by `sessionId`/`turnId`.

Fold rules (from the fold-model guide + `fold.rs`):

- Deltas accumulate per `(itemId, field)` in cursor order;
  `item/completed` is authoritative truth (concatenated deltas ==
  final field unless `truncated: true`).
- Replace-iff-higher-`revision` on full objects; accept
  `item/updated|completed` for never-`started` items.
- Track `viewCursor` from every event; on `view/gap`, forward
  `view/page` (limit 100) and re-feed events through the fold with
  `done` + `usage_seen` idempotency sets.
- Await **your** `turnId`'s `turn/completed` as THE terminal.
  `turn/unqueued` / `turn/retracted` settle immediately (cancelled);
  `turn/retryScheduled` NEVER settles (log attempt facts only).
- Terminals: `completed` → success; `cancelled` → client-disconnect
  mapping; anything else (incl. `failed` + unknown open values) →
  failure mapping (§6). Mid-turn failures arrive ONLY here, never as
  JSON-RPC errors.
- `session/tokenUsage` accumulates counted-once per `viewCursor`
  into OpenAI `usage`. `contextUsage: null` in snapshots is normal
  ("no tracked anchor") — never blank known state. Never expect
  usage from `view/page`.
- Unknown notification methods (e.g. live-but-undeclared
  `session/started`) and unknown item kinds / enum values:
  log-and-ignore / render-generically, never drop the connection.
- HTTP client disconnect ⇒ `turn/cancel {turnId}` (explicit id,
  same-`commandId` retry discipline) and release the request task.

### 4.4 Sessions

- v1: **one MSP session per HTTP request** (`session/start` with
  `workspaceRoot` = bridge workspace when `--workspace-root` is set,
  omitted in provider mode (verified live: the host adopts `null`),
  `approvalMode = denyUnmatched` unless `--approval-mode` overrides).
  Rationale: both clients resend
  full history per request, so the bridge stays stateless; MSP owns
  nothing across requests; no leak/GC design needed for v1.
- Model routing: request `model` → `session/setModel {model:{modelId}}`
  before `turn/start` when it names a known catalog id; unknown
  model strings are passed through as `modelId` verbatim first
  (server default on empty) — record folded `session/modelChanged`
  as the source of truth, never the echo.
- Restart: on host death, if durable (or durability absent ⇒ durable
  read), relaunch ≤3 with 250 ms / 500 ms / 1 s backoff, then fail
  in-flight requests `503 + Retry-After` (v1 has no resume: each
  request replays from scratch on retry). Ephemeral ⇒ fail closed.
- `session/list` merge / cross-restart continuity: deferred to the
  sticky-session v2 (§8).

## 5. Translation (normative)

### 5.1 Request → prompt

- Chat Completions: flatten `messages[]` in order into ONE text part:
  `system` → leading context block; `user`/`assistant` turns in order
  with role labels; `tool` messages folded as `Tool result
  (<tool_call_id>): …`; image parts (`image_url`) → MSP `image` parts
  (http(s) fetch with timeout + size cap, or `data:` URL decode;
  oversize ⇒ 400 `invalid_request_error`). Multiple text segments
  join in order (MSP joins multiple text parts).
- Responses: `instructions` → leading context block; `input` (string
  or item array) flattened like messages (function-call items and
  outputs rendered as labeled text; reasoning items skipped —
  never re-submit provider reasoning verbatim).
- `displayText`: short `"<method> <model>"` label for transcripts.
- `reasoningEffort`: map Responses `reasoning.effort`
  (`none|minimal|low|medium|high|xhigh|ultra` pass through;
  `max`→`ultra`) and Chat's `reasoning_effort` when present.
- `temperature` and friends: accepted, ignored (MSP has no
  per-turn sampler surface) — document, don't error.

### 5.2 Turn lifecycle per request

1. `session/start` (UUIDv7 `commandId`) → adopt folded
   `session.sessionId` (never trust an echo).
2. Optional `session/setModel`.
3. `turn/start {input, reasoningEffort?}` (default `ifBusy: queue`;
   fresh session ⇒ `disposition: started`, `turnId == commandId`).
4. Stream: map `item/started|delta|completed` (kind `agentMessage`,
   field `text`) → SSE chunks; `reasoning` `summary.N` →
   `reasoning_content` (chat) / reasoning summary parts (responses,
   best-effort); `toolCall` activity → status/progress text lines
   (chat: appended assistant text; responses: `response.output_text.delta`
   with `[tool: name]` prefixes — visibly marked, never silent).
5. Terminal `turn/completed` → close SSE (`[DONE]` / `response.completed`
   + usage) or return the JSON body.

### 5.3 Usage mapping

`session/tokenUsage` per completion: counted-once `promptTokens` /
raw `usage.outputTokens`. Accumulate across the turn;
`session/tokenUsage.cumulative` reconciles at close. Render per
endpoint with OpenAI's per-API names — chat:
`usage{prompt_tokens, completion_tokens, total_tokens}`;
responses: `usage{input_tokens, output_tokens, total_tokens}`
(Codex's `ResponseCompletedUsage` parser requires the latter;
chat names fail its `response.completed` parse — found live in
P7). No cost math in v1 (muse-acp lesson: any future cost
display is labeled estimate, `is_finite`-guarded, never in
billing fields).

### 5.4 Approvals and user input in v1

With `denyUnmatched`, unmatched subjects fail their tool call with a
typed model-visible denial — the turn continues, nothing parks. If an
`approval/request` nevertheless arrives (matched-rule prompt modes via
`--approval-mode` override), the bridge auto-selects the FIRST
non-approving choice (`fallback_deny`); if no deny choice exists it
cancels the turn rather than approve. `userInput/request` (which CAN
time out server-side, unlike approvals) ⇒ auto-`userInput/cancel`
with reason `headless-bridge`. All auto-decisions logged with
`approvalId`/`userInputId`. This is the fail-closed policy; v2 may
add a human-in-the-loop endpoint (§8).

## 6. Error mapping (normative)

| MSP signal | Chat Completions | Responses | HTTP |
|---|---|---|---|
| `turn/completed terminal=failed kind=authRequired` / exit 3 (config/creds) | `invalid_request_error` "muse login required" | `response.failed code=invalid_request_error` | 401 |
| `terminal=failed kind in (rateLimit, overloaded)` / `overloaded` / `backpressured` (retries exhausted) | `rate_limit_exceeded` + `Retry-After` | `response.failed code=rate_limit_exceeded` + `Retry-After` | 429 |
| `terminal=failed kind=contextLength/contextWindow` / `inputTooLarge` / `outputResultTooLarge` | `context_length_exceeded` | `response.failed code=context_length_exceeded` | 400 |
| `terminal=failed` other / `internal` / `commandRejected` / crash rows | `server_error` (+ stderr tail in `detail`, never parsed) | `response.failed code=server_error` | 500 |
| `terminal=cancelled` (client gone) | (connection already dead) | (connection already dead) | — |
| `invalidParams` from OUR request build (bug) | `server_error` + log | `response.failed code=server_error` | 500 |
| Caller 400s (bad JSON, oversize image, unknown route, WS-less 426 n/a) | `invalid_request_error` | `invalid_request_error` | 400/404 |
| Host dead + restart budget spent | `server_error` + `Retry-After: 5` | `response.failed code=server_error` + `Retry-After: 5` | 503 |

SSE streams that fail mid-turn: emit the terminal failure event
(chat: final chunk with `finish_reason: "stop"` + error trailer is
FORBIDDEN — instead close the stream after a last
`data: {"error": …}` frame per OpenAI convention… see note) then
close. NOTE: OpenAI SSE has no in-band error frame standard for chat
completions; established practice is mid-stream HTTP-200-then-close
with an `{"error":…}` data frame. The bridge does that for chat, and
`response.failed` for responses (first-class there).

## 7. Build, run, and operate

- Toolchain: Rust 1.88+ (edition 2024), Tokio (full: process, io,
  sync, time, signals), Axum 0.8, serde/serde_json, clap 4
  (derive), tracing + tracing-subscriber (json + pretty),
  uuid v7, futures. No other runtime deps without justification.
- `cargo build --release` ⇒ single binary `muse-bridge`.
  Requires `muse` on `PATH` at RUNTIME (not build time).
- CLI: `muse-bridge [--port 17489] [--bind 127.0.0.1]
  [--workspace-root <dir>] [--approval-mode denyUnmatched]
  [--muse-bin muse] [--trust-workspace] [--log-format json|pretty]
  [--support] [--selftest]`. `--support` prints versions,
  fingerprint pin vs live, last stderr tail; `--selftest` runs
  handshake + `model/list` + empty-turn probe and exits 0/1.
- Env: `MUSE_CLI`, `MUSE_SERVE_ARGS`, `MUSE_COMMAND_TIMEOUT_MS`,
  `MUSE_BRIDGE_PORT`, `RUST_LOG` — same names as muse-acp where
  they overlap.
- Logging: structured `tracing` events (request id, session id,
  turn id, view cursors, MSP `data.kind` on errors). No `println!`
  in shipped code. Never log message content above `debug`, never
  log headers/keys at any level.
- Config-file overlays the bridge DOCUMENTS (not writes):
  Hermes `custom_providers[] {base_url:
  http://127.0.0.1:17489/v1}`; Codex CLI custom
  `[model_providers.muse_bridge]` table; ChatGPT Desktop root-key
  overlay (`model_provider`, `openai_base_url`,
  `model_catalog_json`) with snapshot-first warning.

## 8. v2 roadmap (explicitly deferred)

- V2.1 Sticky sessions: `conversation`/`previous_response_id` (and a
  `X-Muse-Session` header for chat) → `session/resume` + cursor
  suffix, idle-unload aware, with session GC.
- V2.2 Tool-call bridge: MSP approval subjects → client
  `tool_calls`/`function_call` items; client tool results → next
  `turn/start` input. Requires rethinking N2 + approval UX.
- V2.3 Human-in-the-loop endpoint for approvals/user-input
  (`GET /v1/pending`, `POST /v1/decide`).
- V2.4 Zed ACP surface (sidecar binary reusing the MSP client crate;
  see muse-acp).
- V2.5 T3Code via ACP (same vehicle as V2.4).
- V2.6 `item/readOutput` fetch for `truncated: true` surfaces;
  image-output passthrough.

## 9. Validation plan

- P0 `cargo test`: NDJSON framing (CRLF tolerance, blank lines,
  unparsable-line survival, 10 MiB cap), UUIDv7 shape + monotonicity,
  same-`commandId` retry rule, fold rules (delta concat == final,
  replace-iff-higher, gap splice), SSE event sequences for both
  endpoints (golden), error-mapping table (§6) per row, 426-on-WS.
- P1 scripted-host tests (`tests/fixtures/fake_serve.py` pattern
  from muse-acp): handshake incl. fingerprint warn/fatal paths,
  full-turn event flow, cancel-mid-tool, approval auto-deny,
  user-input auto-cancel, `view/gap` refill, host-death restart ≤3.
- P2 live-host gate (env-gated, needs `muse login`): `--selftest`
  against real `muse serve`; one chat + one responses request E2E.
- P3 client-compat (manual, release gate): Hermes CLI pointed at
  the bridge (stream + tools-sent); Codex CLI via custom provider
  table (stream, 426 fallback observed in trace); ChatGPT Desktop
  via config overlay (model picker lists bridge catalog).
- Highest risk: §5.4 auto-decision interplay with real agent runs
  (deny storms stalling useful work) — P2/P3 must run a real
  file-editing task, not just a hello turn.

## 10. Reference index (what was cached, where)

All under `.cache/reference/` (git-ignored, documented, not committed):

- `muse-code-sdk/` — TS SDK + schema + transcripts mirror (note: BEHIND
  the local binary; see §4.1 drift list).
- `local-schema-ts/`, `local-schema-json/` — `muse schema` export
  from muse 1.2.1 (authoritative; fingerprint `c7ff6c5d…`).
- `msp-docs/` — 412 fetched doc pages (guides + generated method /
  notification / error / type reference + cookbook) + `txt/`
  extracted guide text.
- `hermes-agent/` — Hermes source (provider/transport evidence).
- `hermes-docs/` — 459 fetched doc pages incl. subscription-proxy,
  configuring-models, codex-app-server-runtime, ACP.
- `codex/` — openai/codex source (Responses client, SSE parser,
  provider config, catalog schema).
- `muse-acp/` — Rust ACP→MSP adapter (transport/fold/compat reuse
  patterns + 50 vendored transcripts).

## 11. Open questions

- OQ1. RESOLVED (P7, probed live against muse 1.2.1): `turn/start`
  accepts `reasoningEffort` in {none, minimal, low, medium, high,
  xhigh, max, ultra} — every tier acked `started` and cancelled
  cleanly; an unknown value rejects `invalidParams`. The bridge
  passes all eight tiers through verbatim.
- OQ2. RESOLVED (P7, probed live against Desktop 42.3.0's bundled
  codex 0.154.0-alpha.6.2): the bundled app-server honors
  `model_catalog_json` exactly like the CLI — `model/list` over
  stdio serves the catalog's models (`hidden: false`, full
  reasoning levels) under both a custom-provider config and the
  documented root overlay (`model_provider="openai"` +
  `openai_base_url` + `model_catalog_json`), with no login.
  The GUI picker pixels were not confirmed headless; `model/list`
  is what feeds the picker (`show_in_picker` ⇔ `visibility:
  list`).
- OQ3. `MUSE_SERVE_ARGS=--trust-workspace` default on/off for bridge
  sessions (skills/rules loading) — default OFF in v1, revisit.
- OQ4. RESOLVED: default moved `18789` → `17489`. The original
  `18789` collided with OpenClaw's default gateway port (verified
  against docs.openclaw.ai; the first collision check missed it).
  `17489` is unclaimed (web search + local listeners), outside
  the claw/cob/wpopenclaw 1878x–1880x block and clear of Hermes
  (8645) / LM Studio (1234) / Ollama (11434). Override remains
  `--port` / `MUSE_BRIDGE_PORT`.
