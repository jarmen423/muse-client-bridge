
  Already at parity

  ┌─────────────────┬────────────────┬──────────────────┐
  │ Area            │ CLI            │ Bridge           │
  ├─────────────────┼────────────────┼──────────────────┤
  │ Approval switch │ ask/auto/deny  │ yes, live picker │
  │ Model switch    │ /model picker  │ yes, live picker │
  │ Effort tiers    │ 8 tiers        │ yes, picker now  │
  │ Sessions        │ resume/fork    │ yes, both        │
  │ /compact        │ compact        │ yes, inline      │
  │ /skill invoke   │ any skill      │ yes, syntax      │
  │ Queue/steer     │ composer queue │ yes, v2 steer    │
  │ Images          │ attach         │ yes              │
  └─────────────────┴────────────────┴──────────────────┘

  Queued in approved P7–P9

  ┌────────────────────┬───────────────────┬───────────────────┐
  │ Area               │ CLI               │ Bridge            │
  ├────────────────────┼───────────────────┼───────────────────┤
  │ Skill slash        │ /every-skill      │ P7, dynamic list  │
  │ /help/status/usage │ cards             │ P8, from fold     │
  │ /name/models/exit  │ rename/list/quit  │ P8, host calls    │
  │ /effort/recap      │ set effort, recap │ P8, new finds     │
  │ Permission dialog  │ ask UI            │ P9, port upstream │
  │ User questions     │ ask UI            │ P9, port upstream │
  └────────────────────┴───────────────────┴───────────────────┘

  Genuine gaps (need research or design)

  • /side, /memory, /rules, /mcp — host mapping unknown; needs a spike before committing.
  • /init, /deep-research — agentic flows; could be sent as directed turns, needs design.
  • /export — we'd have to write transcript files to disk; new behavior, your call.
  • /goal, /tasks, /workflows, /subagents, /stop — goal/subagent/workflow engine; a project, not a command.
  • Permission profiles, sandbox toggles, worktrees — CLI startup-only surfaces with no ACP shape yet.
  • Cross-session messaging — not surfaced.

  N/A in Zed: /login, /logout, /clear, /theme, /voice, /vim, /copy, /feedback, /settings, /keymap,
  clipboard/keymap/terminal chrome.

  Two notes: the /plan ambiguity resolved — the registry has no /plan builtin (/usage covers subscription), so
  our /plan → skill mapping stands. And P3's "full session ops" claim was core-side only: session/rename and
  session/setReasoningEffort exist in MSP but have no ACP exposure today — P8 will be their first


## P7–P9 landed — spike results (this change)

P7–P9 are implemented: dynamic skill slash commands ride `muse skills list
--json` per session workspace; `/help` `/status` `/usage` `/name` `/models`
`/exit` `/effort` `/recap` are bridge-local protocol commands (they never
reach `turn/start`); approvals bridge through `session/request_permission`
and user-input through `elicitation/create` with reply correlation, both
fail-closed.

The "genuine gaps" spike is resolved — evidence below is from the 1.3.0
binary strings plus the local MSP schema bundle.

### MSP-backed, worth doing

- **Client MCP servers** — `SessionConfig.mcpServers` is wire-real: ACP
  `session/new`'s `mcpServers` maps to `{transport:"stdio", command, args,
  env}` per name (remote transports exist too). We currently tolerate and
  drop them; forwarding is a small, honest win.
- **`/stop`** — `turn/cancel` on the in-flight turn. Marginal: ACP already
  has `session/cancel`.
- **`/goal` (read-only card)** — `session/goalChanged` already folds into
  session facts; a display card is cheap. `/goal edit` has no MSP setter —
  goal control rides internal `TurnSubmitPayload` fields, not the wire.

### Not bridgeable — the CLI says so itself

- `/side`, `/new`, `/clear` — "requires the interactive runtime client":
  TUI-only, no MSP entry point.
- `/init` — "TUI /init does not accept arguments; use `muse init` in a
  shell": a CLI subcommand, not a session command.
- `/deep-research` — workflow-engine launch (`usage: /deep-research
  <question>`); no workflow-launch method exists in MSP.
- `/memory`, `/rules`, `/mcp`, `/plugins`, `/export` — local file/auth
  surfaces (MEMORY.md, rules files, `muse mcp login|logout`, plugin
  registry, transcript/trajectory export over the retained log). No wire
  methods.
- `/goal edit`, permission profiles, sandbox/worktree toggles — TUI
  startup/runtime-client surfaces.
- Cross-session messaging — agent *tools* (`list_peer_sessions`,
  `send_session_message`), not client-callable MSP methods.

### Partially backed — needs a design call before committing

- `/tasks` `/workflows` `/subagents` — MSP exposes control ops
  (`subagent/stop|interrupt|readResult|sendMessage|resume|followupTask|
  close|reopen`) but **no list method**; membership lives in view items
  and `session/todoListChanged` (already folded → ACP `plan` updates).
  A display card is feasible; management UX has no ACP shape.
