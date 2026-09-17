# AGENTS.md — working on Muse Bridge

This file is for contributors and coding agents. The normative build
contract is [SPEC.md](/home/josh/code/muse-client-bridge/SPEC.md); this file covers how to
work in the repo without breaking it.

## Project shape (planned)

```text
muse-client-bridge/
├── PRD.md SPEC.md README.md AGENTS.md
├── Cargo.toml Cargo.lock
├── src/
│   ├── main.rs          # CLI (clap), startup, signal handling
│   ├── http/            # axum router: chat, responses, models, healthz, 426
│   │   ├── chat.rs      # POST /v1/chat/completions (+SSE)
│   │   ├── responses.rs # POST /v1/responses (+SSE)
│   │   └── sse.rs       # shared SSE framing helpers
│   ├── msp/             # MSP client (tokio): spawn, proto, host, fold
│   │   ├── spawn.rs     # child spawn/supervise, exit classification, stderr tail
│   │   ├── proto.rs     # NDJSON framing, JSON-RPC envelope, pending map, UUIDv7
│   │   ├── host.rs      # handshake, commands, timeouts, retry, restart
│   │   └── fold.rs      # per-request view fold → output events
│   ├── dispatch.rs      # HTTP request → session/turn orchestration, mapping
│   ├── translate.rs     # OpenAI messages/input → MSP input parts (+images)
│   └── support.rs       # --support/--selftest diagnostics
├── tests/               # unit + scripted-host (fake_serve) + live-host (env-gated)
├── examples/            # model_catalog_json template, client config snippets
└── .cache/reference/    # fetched docs+source (GIT-IGNORED, never commit)
```

## Build / test / run

```sh
cargo build --release            # single binary: target/release/muse-bridge
cargo test                       # full suite (scripted-host; no login needed)
MUSE_LIVE_TESTS=1 cargo test     # includes env-gated live-host tests (needs muse login)
cargo clippy -- -D warnings      # must be clean before any PR
cargo fmt --check                # rustfmt clean
RUST_LOG=debug muse-bridge       # local run with content tracing
```

Rust 1.88+, edition 2024. Runtime deps are locked to: tokio, axum,
serde/serde_json, clap, tracing/tracing-subscriber, uuid, futures.
Justify any addition in the PR description (muse-acp's
no-convenience-deps discipline applies).

## Non-negotiable protocol rules

These are the bugs every MSP client writes once. Don't:

1. **Ack ≠ outcome.** `turn/start → accepted` means admitted. Await
   YOUR `turnId`'s `turn/completed`. Same for every command: truth
   arrives on the view stream.
2. **Retries reuse `commandId`.** Fresh UUIDv7 per LOGICAL command,
   same id on retry/replay. Never mint fresh for a retry.
3. **UUIDv7 or death.** The host rejects non-v7 commandIds with
   `invalidParams` (verified live). Single monotonic minter per
   connection (timestamp-ms + rand + same-ms sequence).
4. **`turn/unqueued`/`turn/retracted` settle; `turn/retryScheduled`
   never settles.** A turn-wait folding only `turn/completed` hangs
   on reclaim.
5. **Terminals are open.** `completed`→ok, `cancelled`→gone,
   everything else (incl. unknown strings) → failure. Never match
   exhaustively.
6. **Branch on `error.data.kind`, never on message text.** Codes are
   coarse; `message` is human-only, unstable.
7. **Tolerate the undeclared.** Unknown notifications
   (`session/started` is emitted live but absent from BOTH schema
   bundles), unknown item kinds/enums, unparsable lines: log,
   skip/ignore, survive. The connection outlives any one frame.
8. **Gap = stale.** `view/gap` ⇒ forward `view/page` + idempotent
   refold (`done` + `usage_seen` sets) before rendering current.
9. **Cursor-track everything; `contextUsage: null` is normal.**
   Never blank known usage on a null snapshot arm; never expect
   usage from `view/page`.
10. **`initialized` before anything; `schema.version == 1` fatal,
    fingerprint mismatch warn-only.** Pin lives in SPEC §4.1.
11. **Fail closed on approvals.** First non-approving choice, else
    cancel the turn. Never synthesize approval. Auto-decide loudly
    in logs with the durable id.
12. **Drain stdout on its own task.** The stdio server never severs
    a wedged pipe; a blocked reader wedges the bridge.
13. **stderr: capture, never parse.** Bounded tail into failures
    and `--support`.
14. **426, not WS.** Any `Upgrade: websocket` → `426 Upgrade
    Required`. Never implement the WS submodule.

## HTTP conventions

- Be liberal in what you accept: `#[serde(default)]` +
  `deny_unknown_fields`-NEVER on OpenAI request structs. Both
  clients send fields we ignore; 400-on-unknown breaks Hermes.
- Responses SSE for `/v1/responses` MUST end with
  `response.completed` (else Codex errors); chat SSE MUST end with
  `data: [DONE]`. A stream that just closes is a bug.
- Ignore `Authorization` and all extra headers. Never log header
  values or keys at any level; message content only at `debug`.
- Structured `tracing` everywhere (request/conn/session/turn ids as
  fields). No `println!` in shipped code.

## Reference cache (`.cache/reference/`)

Git-ignored working library — read it, don't commit it, don't
modify it (re-fetch instead). Layout + provenance:

| Path | Contents | Source |
|---|---|---|
| `muse-code-sdk/` | TS SDK mirror (schema+tests+transcripts) | `github.com/meta-models/muse-code-sdk` @ fetch 2026-09-14 |
| `local-schema-ts/`, `local-schema-json/` | **authoritative** MSP schema for muse 1.2.1 | `muse schema generate-{ts,json-schema}` |
| `msp-docs/` (+`txt/`) | 412 doc pages, guides extracted to text | `meta-models.github.io/muse-code-sdk` sitemap |
| `hermes-agent/` | Hermes source (provider evidence) | `github.com/NousResearch/hermes-agent` @ fetch date |
| `hermes-docs/` | 459 doc pages | `hermes-agent.nousresearch.com` sitemap |
| `codex/` | openai/codex source (client+config evidence) | `github.com/openai/codex` @ fetch date |
| `muse-acp/` | Rust ACP→MSP adapter + 50 transcripts | `github.com/BrokkAi/muse-acp` |
| `acp-protocol/` | ACP JSON schemas v1+v2 (+changelogs) | `agentclientprotocol/agent-client-protocol` @ 2026-09-17 |
| `zed/v1.19.2/` | Zed ACP client evidence (routing, pickers) | `zed-industries/zed` @ v1.19.2 |

Known drift: the SDK mirror LAGS the local binary (local adds
`item/readOutput`, `session/rename`, `session/setReasoningEffort`,
`view/subscribe`, three `session/*Changed` notifications,
`outputUnavailable`, `sessionMcp`, `userInputDialogs`). When they
conflict, the binary wins. Live `session/started` is in NEITHER
bundle — see rule 7.

When re-fetching would change evidence the SPEC cites, update the
SPEC's fingerprint pin and drift list in the same change.

## Testing discipline

- Unit tests for every fold rule, mapper row, and SSE sequence
  (golden strings, not mocks of our own code).
- Scripted-host tests via a `fake_serve.py`-style fixture host for:
  handshake paths, full-turn flow, cancel-mid-tool, approval
  auto-deny, user-input auto-cancel, `view/gap` refill, host-death
  restart budget. Mirror muse-acp's `tests/protocol/transcripts/`
  approach where transcripts apply.
- Live-host tests env-gated (`MUSE_LIVE_TESTS=1`), skipped
  otherwise — CI-safe without credentials.
- Client-compat checks (Hermes/Codex/Desktop) are manual release
  gates (SPEC §9 P3); record results in the release notes, not in
  the repo.

## Ask before

- Adding a runtime dependency.
- Changing the fingerprint pin or MSP behavior matrix.
- Anything touching auth/credentials handling (answer should stay:
  we handle none).
- Widening bind beyond loopback or adding TLS.
