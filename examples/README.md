# Client configuration examples

Point a client at a running bridge (`muse-bridge --port 17489`, loopback
only). Model ids below were live on 2026-09-14. Confirm with
`GET /v1/models`, the catalog changes server-side.

## Codex CLI config

`codex-config.toml` merges into `~/.codex/config.toml`. It uses a custom
`[model_providers.*]` table with `wire_api = "responses"`. The bridge
serves plain SSE over HTTP and answers `426 Upgrade Required` to
websocket upgrades; Codex falls back automatically.

## Hermes config

`hermes-config.yaml` merges into Hermes's `config.yaml`. Hermes fetches
the model list from `{base_url}/models`.

## ChatGPT Desktop catalog

Desktop reads its model picker from a catalog file referenced by
`model_catalog_json` in its config overlay (root keys `model_provider`,
`openai_base_url`, `model_catalog_json`).

> Snapshot-first warning: back up the existing Desktop config before
> overlaying, and keep a copy of the stock catalog. The overlay replaces
> Desktop's model list wholesale.

`model_catalog.json` adapts codex-rs's own `models.json` fixture shape
(`ModelsResponse { models: Vec<ModelInfo> }`). Same keys, bridge model
slugs, `context_window` 1007997 (live `model/list`),
`prefer_websockets: false`, and the full six reasoning levels. The
bridge passes every tier through to MSP verbatim, and the host accepts
all of them (SPEC OQ1, probed live). Every entry needs
`base_instructions` (or `model_messages.instructions_template`):
Codex rejects catalog rows that have neither. The live catalog also
lists `-contributor` variants of both models; add rows for them if you
want them in the picker.
