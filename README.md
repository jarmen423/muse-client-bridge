# Muse Bridge

Use your Muse Code subscription inside other agent clients. Muse Bridge
drives a `muse serve` child process over the Muse Session Protocol (MSP)
and serves it two ways:

- **`muse-bridge`** (localhost HTTP, OpenAI-compatible):
  **Hermes Agent (Desktop App)** sends `POST /v1/chat/completions`,
  **Codex CLI / IDE / ChatGPT Desktop** send `POST /v1/responses`.
- **`muse-acp-bridge`** (ACP over stdio): spawned by **Zed** and
  **JetBrains IDEs** as a custom agent server — no ports involved.
- No API keys handled, none stored. Auth comes from your existing
  `muse login`, inherited by `muse serve`.

> Status: complete — HTTP surface (P0–P7: chat, responses,
> models, diagnostics, examples; live-host E2E green) plus the ACP
> surface for Zed/JetBrains (FORK_PLAN P1–P6 and parity P7–P9:
> slash commands, skills, subagent/workflow display + control,
> permission/user-input dialogs, MCP forwarding). Automated gates
> green (314 tests, clippy, fmt — verified 2026-09-20; live-host
> suite green 2026-09-17, needs `muse login`).
> Remaining: manual client-compat runs (Hermes, Codex, Desktop,
> Zed, JetBrains) — release gates recorded in release notes.
> See [SPEC.md](/home/josh/code/muse-client-bridge/SPEC.md) for the normative build spec and [PRD.md](/home/josh/code/muse-client-bridge/PRD.md) for goals.

## Install

```sh
cargo build --release       # ./target/release/muse-bridge + ./target/release/muse-acp-bridge
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

## Use it from Zed (ACP)

`muse-acp-bridge` speaks the Agent Client Protocol over stdio: Zed
spawns it per session, so there are no ports and no HTTP involved.

1. Build and log in:

   ```sh
   cargo build --release   # produces ./target/release/muse-acp-bridge
   muse login
   ```

2. Register it in `~/.config/zed/settings.json` (comment-preserving
   edit, writes a `.bak` backup):

   ```sh
   ./target/release/muse-acp-bridge install \
     --command "$PWD/target/release/muse-acp-bridge" \
     --env MUSE_APPROVAL_MODE=auto
   ```

   The bare default command is `muse-acp`, so pass `--command` with
   the absolute path (or a name on your `PATH`). `--env` entries land
   in the entry's `"env"` object; `MUSE_APPROVAL_MODE=auto` sets the
   starting posture (switchable later in the agent panel — see
   below). Preview with `--dry-run`; remove with
   `muse-acp-bridge uninstall`.

   This writes (entry name defaults to `muse-acp`):

   ```json
   "agent_servers": {
     "muse-acp": {
       "type": "custom",
       "command": "/home/josh/code/muse-client-bridge/target/release/muse-acp-bridge",
       "args": [],
       "env": {
         "MUSE_APPROVAL_MODE": "auto"
       }
     }
   }
   ```

3. Restart Zed (or reload settings), open the Agent panel, and select
   `muse-acp`. Try a file-edit task in a workspace folder, e.g. "add
   a `--version` flag to this CLI and a test for it".

Notes:

- **Approval mode is switchable in the Zed UI.** The agent panel's
  Session Mode picker (Ask/Auto/Yolo/Deny, plus model and
  reasoning-effort pickers) calls straight through to the host
  mid-session, so `MUSE_APPROVAL_MODE` only sets the *starting*
  posture — bogus values fail loudly. Yolo rides host `allowAll`
  and auto-declines user questions (never interrupts); it never
  auto-approves host approval requests (under `allowAll` the host
  sends none — any that arrive still surface). Bare `/models` and
  `/effort` (or an invalid tier) pop a selection form when the
  client has an elicitation surface, else a card. Under `ask`, host
  approvals surface as Zed's permission dialog and host questions
  as elicitation forms; a client without those surfaces still
  fails closed (deny/cancel), never auto-approves.
- **Import sessions works.** `session/list` merges live sessions
  with durable host sessions (past TUI/CLI runs), each with title
  and last-activity time, and every entry loads via `session/load`
  with history replay — Zed's importer just works.
- **Slash commands:** the advertised list is live — protocol
  commands (`/help /status /usage /name /models /effort /recap
  /stop /goal /tasks /subagents /workflows /compact /exit`) plus
  every skill `muse skills list` reports for the workspace.
  Model-spawned children also stream as their own `tool_call`
  blocks (spawn → progress → terminal), TUI-style, and
  `/subagents <verb> [target] [text]` controls them (stop,
  interrupt, close, resume, reopen, message, followup, result —
  missing targets pop a selector, message bodies a text field).
  `/<id>` maps to Muse's `/skill`
  grammar; protocol commands settle locally and never start a turn.
  A leading space escapes execution (` /plan` stays literal text).
  CLI-only commands with no MSP entry point (`/deep-research`,
  `/side`, `/memory`, `/rules`, `/mcp`, `/export`, `/init`, …)
  are not intercepted — they reach the model as literal prompt
  text (per-command verdicts in TODO.md).
- **Client MCP servers** (`session/new` `mcpServers`) forward into
  session construction: stdio entries (`name` + `command`) and URL
  entries ride `config.mcpServers`; entries with neither warn and
  skip, never failing the session. Resume/load/fork re-attach an
  existing session, so servers only attach at creation.
- **Sessions:** each Zed session gets its own MSP session rooted at
  the folder you opened; closing the Zed session ends it. Session
  list, resume, and fork are all exposed to the client.
- **Logs** go to the bridge's stderr, captured by Zed from the
  spawned process. Other env knobs (via `--env`): `MUSE_CLI`,
  `MUSE_SERVE_ARGS`, `MUSE_TRUST_WORKSPACE=1`,
  `MUSE_COMMAND_TIMEOUT_MS`, `RUST_LOG` (`debug` for content
  tracing).
- **Alongside upstream `muse-acp`:** no need to uninstall it — the
  binaries have different names and can't conflict at runtime. Both
  installers default to the entry name `muse-acp`, so a default
  install *replaces* that entry; pass `--name muse-acp-bridge` to
  keep both and compare. Note upstream's `uninstall` removes
  whatever sits under `muse-acp`.
- **JetBrains IDEs:** `muse-acp-bridge install-intellij --command
  <absolute path>` (absolute path required) writes
  `~/.jetbrains/acp.json`.

### Remote dev (Zed SSH)

For a remote workspace the split is: **binary on the VM, config on
your local machine.** Zed spawns the agent server on the remote host
but reads `settings.json` locally.

1. On the VM: install `muse-acp-bridge` (e.g.
   `~/.local/bin/muse-acp-bridge`), install the `muse` CLI, and run
   `muse login` there as the same user Zed SSHes in as.
2. On your local machine: run the installer (any copy of the binary
   — it only edits local settings) with the **remote** path, or
   hand-edit `~/.config/zed/settings.json`:

   ```sh
   muse-acp-bridge install \
     --command /home/you/.local/bin/muse-acp-bridge \
     --env MUSE_APPROVAL_MODE=auto
   ```

   The installer doesn't validate the command for Zed entries, and
   an absolute path sidesteps PATH issues entirely.
3. Connect to the remote project and select the agent in the Agent
   panel.

If you work both locally and remotely with one settings file, register
a second entry (`--name muse-acp-remote` with the VM path) and pick
the right one per workspace.

## HTTP endpoints

| Endpoint | Client | Notes |
|---|---|---|
| `POST /v1/chat/completions` | Hermes | `stream: true` SSE + `data: [DONE]`; plain JSON otherwise |
| `POST /v1/responses` | Codex/Desktop | `stream: true` SSE (`response.created` … `response.completed`); full object otherwise |
| `GET /v1/models` | both | from MSP `model/list` |
| `GET /healthz` | supervisors | `{"status":"ok","msp":"ready\|starting"}` |
| `Upgrade: websocket` → `426` | Codex fallback | intentional; triggers instant HTTP fallback |

Unknown JSON fields are ignored on every endpoint; `Authorization` is
accepted in any form and never inspected.

Any other OpenAI-compatible client (ZCode, etc.) works the same
way: point its base URL at `http://127.0.0.1:17489/v1` — chat-style
clients use `/v1/chat/completions`, Responses-style clients use
`/v1/responses`, and `/v1/models` serves discovery.

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
| Zed agent fails to start / command not found | `"command"` isn't on Zed's `PATH`. Re-run install with `--command /abs/path/to/muse-acp-bridge`. |
| Zed agent denies every tool action | `ask` posture shows a permission dialog per action — dismissed dialogs fail closed. Flip Session Mode to Auto in the agent panel, or set `"MUSE_APPROVAL_MODE": "auto"` in the entry's `env`. |
| `muse login required` inside Zed | same as HTTP: run `muse login` as the same user Zed runs under. |

`RUST_LOG=debug` enables content-level tracing (request/response
bodies). Never share debug logs containing prompts you consider
private. `--support` prints versions, fingerprint pin vs live host,
and the last stderr tail for bug reports.

## Docs

- [PRD.md](/home/josh/code/muse-client-bridge/PRD.md), goals and reference list.
- [SPEC.md](/home/josh/code/muse-client-bridge/SPEC.md), normative spec (endpoints, MSP client, translation, errors, validation).
- [FORK_PLAN.md](/home/josh/code/muse-client-bridge/FORK_PLAN.md), ACP surface plan (parity checklist, packets P1–P6, open questions).
- [AGENTS.md](/home/josh/code/muse-client-bridge/AGENTS.md), contributor and coding-agent guide.
- `.cache/reference/`, fetched docs + source cache (git-ignored; see its README).
