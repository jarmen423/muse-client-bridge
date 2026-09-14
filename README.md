# Muse Bridge

Use your Muse Code subscription inside other agent clients. Muse Bridge is a
single localhost binary that exposes an OpenAI-compatible HTTP API and drives
a `muse serve` child process over the Muse Session Protocol (MSP).

- **Hermes Agent (Desktop App)** sends `POST /v1/chat/completions`
- **Codex CLI / IDE / ChatGPT Desktop** send `POST /v1/responses`
- No API keys handled, none stored. Auth comes from your existing
  `muse login`, inherited by `muse serve`.

> Status: implemented through P6 (HTTP, MSP client, diagnostics,
> examples). P7 live-host and client-compat gates are still pending.
> See [SPEC.md](/home/josh/code/muse-client-bridge/SPEC.md) for the normative build spec and [PRD.md](/home/josh/code/muse-client-bridge/PRD.md) for goals.

## Install

```sh
cargo build --release       # produces ./target/release/muse-bridge
```

Requires a `muse` binary on `PATH` at runtime (`muse login` first).

## Run

```sh
muse-bridge                                     # listens on http://127.0.0.1:17489/v1
muse-bridge --port 8646 --log-format pretty     # pretty logs, custom port
muse-bridge --selftest                          # handshake + model/list + probe turn, exit 0/1
muse-bridge --support                           # diagnostics bundle as JSON, exit 0
```

Quick checks:

```sh
curl http://127.0.0.1:17489/v1/models
curl http://127.0.0.1:17489/healthz
```

## Point Hermes at it

Merge this into Hermes's `config.yaml` (also in
[examples/hermes-config.yaml](/home/josh/code/muse-client-bridge/examples/hermes-config.yaml)).
Hermes fetches the model list from `{base_url}/models`, and the bridge
ignores the key:

```yaml
model:
  provider: "custom:muse-bridge"
  default: "muse-spark-1.3"
  base_url: "http://127.0.0.1:17489/v1"

custom_providers:
  - name: "muse-bridge"
    base_url: "http://127.0.0.1:17489/v1"
    models: ["muse-spark-1.3", "muse-spark-1.2"]
```

Hermes speaks `POST /v1/chat/completions` with SSE streaming. Plain
loopback HTTP needs no TLS settings.

## Point Codex / ChatGPT Desktop at it

Codex speaks only the Responses API (`POST /v1/responses`, always
streamed SSE). Two ways to connect:

**Codex CLI / IDE.** A custom provider table in `~/.codex/config.toml`
(also in
[examples/codex-config.toml](/home/josh/code/muse-client-bridge/examples/codex-config.toml)):

```toml
model_provider = "muse_bridge"
model = "muse-spark-1.3"

[model_providers.muse_bridge]
name = "Muse Bridge (local subscription proxy)"
base_url = "http://127.0.0.1:17489/v1"
wire_api = "responses"
```

**ChatGPT Desktop.** Its bundled `codex app-server` reads the same
file but ignores `--profile`. Snapshot `~/.codex/config.toml` first,
then set these **root** keys:

```toml
model_provider = "openai"
openai_base_url = "http://127.0.0.1:17489/v1"
model_catalog_json = "~/.codex/muse-catalog.json"
```

Fully quit and reopen ChatGPT Desktop after writing. The bridge answers
the openai provider's WebSocket probe with `426` so Desktop falls back
to HTTP instantly. Neither side needs WebSocket support.

`model_catalog_json` must be a `ModelsResponse` (`{"models": [...]}`).
Copy
[examples/model_catalog.json](/home/josh/code/muse-client-bridge/examples/model_catalog.json),
which adapts codex-rs's own fixture shape with bridge model slugs. Model
ids were live on 2026-09-14; confirm with `GET /v1/models`.

## Endpoints

| Endpoint | Client | Notes |
|---|---|---|
| `POST /v1/chat/completions` | Hermes | `stream: true` SSE + `data: [DONE]`; plain JSON otherwise |
| `POST /v1/responses` | Codex/Desktop | `stream: true` SSE (`response.created` … `response.completed`); full object otherwise |
| `GET /v1/models` | both | from MSP `model/list` |
| `GET /healthz` | supervisors | `{"status":"ok","msp":"ready\|starting"}` |
| `Upgrade: websocket` → `426` | Codex fallback | intentional; triggers instant HTTP fallback |

Unknown JSON fields are ignored on every endpoint; `Authorization` is
accepted in any form and never inspected.

## Behavior notes

- **One MSP session per HTTP request** (v1). Clients resend full
  history; the bridge stays stateless.
- **Approvals fail closed.** Sessions start in `denyUnmatched`: tool
  actions outside policy fail with a model-visible denial instead of
  parking your request on a dialog. Unrenderable prompts are
  auto-cancelled. Nothing in v1 auto-approves.
- **Tools run inside muse.** Your client's `tools` parameter is
  accepted and ignored; the agent's own host tools do the work and
  their activity streams back as status text. Client-executed
  function calling is a v2 item.
- **Usage** (`prompt/completion/total_tokens`) is real counted-once
  MSP token data. No cost or billing figures are ever emitted.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `401 … muse login required` | `muse serve` has no credentials. Run `muse login` (same user/env as the bridge). |
| `503` + `Retry-After` | host died and restart budget spent. Check bridge logs + `muse-bridge --support`. |
| `--selftest` FAIL + login hint | same no-login state, caught before serving. Run `muse login`, retry. |
| Desktop still hits OpenAI | config overlay not applied. Verify root keys in `~/.codex/config.toml`, fully quit Desktop, reopen. |
| Empty/slow first byte | agent is working (tool calls stream as status text); watch `reasoning_content` / status lines. |
| `muse: command not found` | bridge needs `muse` on `PATH`, or set `MUSE_CLI=/path/to/muse`. |

`RUST_LOG=debug` enables content-level tracing (request/response
bodies). Never share debug logs containing prompts you consider
private. `--support` prints versions, fingerprint pin vs live host,
and the last stderr tail for bug reports.

## Docs

- [PRD.md](/home/josh/code/muse-client-bridge/PRD.md), goals and reference list.
- [SPEC.md](/home/josh/code/muse-client-bridge/SPEC.md), normative spec (endpoints, MSP client, translation, errors, validation).
- [AGENTS.md](/home/josh/code/muse-client-bridge/AGENTS.md), contributor and coding-agent guide.
- `.cache/reference/`, fetched docs + source cache (git-ignored; see its README).
