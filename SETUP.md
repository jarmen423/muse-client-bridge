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
# binary: ./target/release/muse-bridge
```

Or skip the toolchain: download `muse-bridge-<os>-<arch>` from the
latest [GitHub Release](https://github.com/jarmen423/muse-client-bridge/releases),
`chmod +x` it on Linux/macOS, and run it. Windows users should run
the Linux binary inside WSL2 (Meta ships no native Windows `muse`
CLI, so a native bridge binary would have nothing to drive).

## First run and verify

```sh
./target/release/muse-bridge --selftest
```

Expect three `PASS` lines and exit 0. If you see a `HINT` about
`muse login`, run that first (same user and environment as the
bridge), then retry.

```sh
./target/release/muse-bridge --workspace-root ~/code/my-project &
curl http://127.0.0.1:17489/healthz
curl http://127.0.0.1:17489/v1/models
```

Both should answer. `GET /v1/models` lists the live model ids; use
those exact strings in client configs, since the catalog changes
server-side.

## Everyday running

```sh
muse-bridge --workspace-root <dir> [--trust-workspace] [--port N]
```

- `--workspace-root` is the directory the backend agent works in.
  One bridge serves one workspace.
- `--trust-workspace` lets the backend load that workspace's skills
  and rules (`.agents/skills/`, `AGENTS.md`). Off by default. Your
  user skills (`~/.agents/skills/`) load on every turn either way.
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
| `muse: command not found` | Put `muse` on `PATH` or set `MUSE_CLI=/path/to/muse`. |

`muse-bridge --support` prints a JSON bundle (versions, fingerprint
pin vs live, stderr tail) for bug reports.

## Cutting a release

Push a tag; CI builds Linux, Windows, and macOS binaries, smoke
tests each, and attaches them to the release:

```sh
git tag v0.1.0 && git push origin v0.1.0
```

Re-running a failed release is safe (assets upload with `--clobber`).
Every push and PR also runs the test/clippy/fmt gates on Linux plus a
Windows compile check.
