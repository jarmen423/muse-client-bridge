# Setup guide

Get from a fresh machine to a working bridge, then point your clients
at it. For the full endpoint reference see
[README.md](/home/josh/code/muse-client-bridge/README.md); for client config file templates see
[examples](/home/josh/code/muse-client-bridge/examples).

## Prerequisites

- Rust 1.88 or newer (`rustc --version`).
- The `muse` CLI on `PATH`, with `muse login` completed. The bridge
  never handles credentials; `muse serve` inherits yours.

## Build

```sh
cargo build --release
# ./target/release/muse-bridge and ./target/release/muse-acp-bridge
```

Or skip the toolchain and install from a release (replace `v0.1.1`
with the latest tag on the
[releases page](https://github.com/jarmen423/muse-client-bridge/releases)).
Each release has two binaries. `muse-bridge` is the HTTP server.
`muse-acp-bridge` is what T3 Code, Zed, and JetBrains spawn.

```sh
# Linux (x86_64), HTTP bridge
curl -LO https://github.com/jarmen423/muse-client-bridge/releases/download/v0.1.1/muse-bridge-linux-x86_64
curl -LO https://github.com/jarmen423/muse-client-bridge/releases/download/v0.1.1/muse-bridge-linux-x86_64.sha256
sha256sum -c muse-bridge-linux-x86_64.sha256
chmod +x muse-bridge-linux-x86_64
mv muse-bridge-linux-x86_64 ~/.local/bin/muse-bridge   # or anywhere on PATH

# Linux (x86_64), ACP bridge for T3 Code / Zed / JetBrains
curl -LO https://github.com/jarmen423/muse-client-bridge/releases/download/v0.1.1/muse-acp-bridge-linux-x86_64
curl -LO https://github.com/jarmen423/muse-client-bridge/releases/download/v0.1.1/muse-acp-bridge-linux-x86_64.sha256
sha256sum -c muse-acp-bridge-linux-x86_64.sha256
chmod +x muse-acp-bridge-linux-x86_64
mv muse-acp-bridge-linux-x86_64 ~/.local/bin/muse-acp-bridge
```

macOS (Apple Silicon) is the same shape with
`muse-bridge-macos-aarch64` and `muse-acp-bridge-macos-aarch64`. The binaries are unsigned, so on
first run macOS may refuse one: right-click it in Finder, choose
Open, and confirm, or run
`xattr -d com.apple.quarantine ~/.local/bin/muse-acp-bridge`.

On Windows, install Meta's native Muse CLI first. WSL is not required.

```powershell
irm https://dev.meta.ai/install.ps1 | iex
```

The installer writes `muse.cmd` under `%LOCALAPPDATA%\Programs\muse` and adds that directory to your user PATH. Open a new terminal and run `muse login`.

The Windows release files are already named `muse-acp-bridge.exe` and `muse-bridge.exe`. Download the one your client launches into that same directory. Do not rename it. T3 Code, Zed, and JetBrains need `muse-acp-bridge.exe`. Hermes and Codex need `muse-bridge.exe`.

```powershell
$dir = "$env:LOCALAPPDATA\Programs\muse"
Invoke-WebRequest https://github.com/jarmen423/muse-client-bridge/releases/download/v0.1.2/muse-acp-bridge.exe -OutFile "$dir\muse-acp-bridge.exe"
```

The one prerequisite that matters is not the bridge: you need the
`muse` CLI installed with `muse login` completed in the same
environment, or every request fails. Verify with
`muse-bridge --selftest` before pointing clients at it.

## First run and verify

```sh
./target/release/muse-bridge --selftest
```

Expect three `PASS` lines and exit 0. If you see a `HINT` about
`muse login`, run that first (same user and environment as the
bridge), then retry.

```sh
./target/release/muse-bridge &
curl http://127.0.0.1:17489/healthz
curl http://127.0.0.1:17489/v1/models
```

Both should answer. `GET /v1/models` lists the live model ids; use
those exact strings in client configs, since the catalog changes
server-side.

## Everyday running

```sh
muse-bridge [--trust-workspace] [--port N]
```

- No workspace config needed: by default the bridge runs in
  provider mode — the client owns the project/workspace, the bridge
  sends no `workspaceRoot`, and the backend child parks in an inert
  empty directory. Pass `--workspace-root <dir>` only when you want
  the backend agent itself to work in a local directory.
- `--trust-workspace` lets the backend load that workspace's skills
  and rules (`.agents/skills/`, `AGENTS.md`). Off by default. Your
  user skills (`~/.agents/skills/`) load on every turn either way.
  In provider mode (no `--workspace-root`) there is no workspace to
  trust, so the flag has no effect.
- `--approval-mode` stays `denyUnmatched` unless you have a reason.
  It fails closed: actions outside policy are denied with a
  model-visible message instead of parking on a dialog.
- Env overrides: `MUSE_BRIDGE_PORT`, `MUSE_CLI` (muse binary path),
  `MUSE_SERVE_ARGS` (extra args for `muse serve`), `RUST_LOG`
  (`debug` logs message content; never share those logs blindly).

## Hermes: main provider

Merge into Hermes's `config.yaml`:

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

Hermes fetches its model list from `{base_url}/models`. No API key is
needed; the bridge ignores `Authorization`.

## Hermes: subagents on the bridge

To keep Hermes's parent agent on its normal provider while subagents
run on your Muse subscription, pin the delegation route (this also
works when the parent itself is on the bridge):

```yaml
custom_providers:
  - name: "muse-bridge"
    base_url: "http://127.0.0.1:17489/v1"
    models: ["muse-spark-1.3"]

delegation:
  provider: "custom:muse-bridge"
  model: "muse-spark-1.3"
```

Verified: a delegated child runs `platform=subagent` on this exact
route (Hermes matches custom endpoints by URL) and returns its
summary to the parent. The child is the full muse agent with its own
tools and your user skills.

## Skills and conventions

Two separate systems, both operator-controlled:

- Backend user skills (`~/.agents/skills/`) apply to every bridge
  turn with no flags.
- A Hermes skill can move backend-side when it is plain text and
  conventions (style, workflow, review habits). Validate first,
  then install:
  `muse skills validate ~/.hermes/skills/<name>` followed by
  `muse skills install ~/.hermes/skills/<name> --scope user`.
  Skills that drive Hermes-only machinery (gateway, cron,
  messaging) will load but misfire, so cherry-pick.
- Hermes runtime state (memory, persona, hooks) does not cross the
  bridge. Anything Hermes puts in the message text, including its
  system prompt, arrives as leading context.

## Codex CLI and ChatGPT Desktop

Copy [examples/codex-config.toml](/home/josh/code/muse-client-bridge/examples/codex-config.toml) into
`~/.codex/config.toml` for the CLI. For Desktop, set the root keys
(`model_provider`, `openai_base_url`, `model_catalog_json`) per
[examples/README.md](/home/josh/code/muse-client-bridge/examples/README.md), using
[examples/model_catalog.json](/home/josh/code/muse-client-bridge/examples/model_catalog.json) as the catalog.
Back up `~/.codex/config.toml` before changing it, and fully quit
Desktop after writing.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `--selftest` FAIL + login hint, or HTTP `401` | Run `muse login` as the bridge user, retry. |
| `failed to bind 127.0.0.1:17489` | Something owns the port (historical note: pre-release default `18789` collided with OpenClaw). Pass `--port` / `MUSE_BRIDGE_PORT`. |
| Codex warns `Model metadata ... not found` | Point `model_catalog_json` at the example catalog. |
| `muse: command not found` | Put `muse` on `PATH` or set `MUSE_CLI=/path/to/muse`. On Windows that file is `muse.cmd`. v0.1.2 and later launch it. |

`muse-bridge --support` prints a JSON bundle (versions, fingerprint
pin vs live, stderr tail) for bug reports.

## Cutting a release

Push a tag. CI builds Linux, Windows, and macOS copies of both binaries, smoke-tests each, and attaches them to the release:

```sh
git tag v0.1.1 && git push origin v0.1.1
```

Re-running a failed release is safe (assets upload with `--clobber`).
Every push and PR also runs the test/clippy/fmt gates on Linux plus a
Windows compile check.
