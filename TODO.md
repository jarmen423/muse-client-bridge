
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

### MSP-backed, worth doing — DONE (this change)

- **Client MCP servers** — DONE: `session/new`'s `mcpServers` forwards
  into `session/start`'s `config.mcpServers` (stdio entries → their
  `{transport:"stdio", command, args, env}` arm, URL entries →
  `streamableHttp`), gated on the `sessionMcp` grant the bridge now
  requests at `initialize` (ungranted hosts reject construction with
  `capabilityRequired` — verified live; without the grant the servers
  drop loudly and the session still succeeds). Unmappable entries
  warn-and-skip per entry. Observed live: a broken server does NOT
  fail construction — the session starts idle. Resume/load/fork
  re-attach, so they keep tolerate-and-ignore.
- **`/stop`** — DONE: protocol command, best-effort `turn/cancel` on
  every in-flight turn (shared helper with `session/cancel`), inline
  card. Idle sessions report "No in-flight turn to stop."
- **`/goal` (read-only card)** — DONE: renders the folded
  `session/goalChanged` block (objective/status/percent/current/next).
  Fixed `status_card`/`recap_card` to read `Goal.objective` (they read
  `summary|title|text`, which never exist on the wire shape).
  `/goal edit` still has no MSP setter — out of scope as before.

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

### Partially backed — `/tasks` done, engines deferred (this change)

- `/tasks` — DONE: protocol command rendering the folded
  `session/todoListChanged` items (`[x]/[~]/[ ]/[-]` marks, the running
  item in its `activeForm`). Unknown statuses stay open, never
  finished — same rule as the ACP `plan` entries.
- `/workflows` `/subagents` — DEFERRED (no wire to map): MSP exposes
  control ops (`subagent/stop|interrupt|readResult|sendMessage|resume|
  followupTask|close|reopen`) but **no list method**, and membership
  lives in view items the bridge does not retain — there is nothing
  truthful for a card to show. Management verbs have no ACP shape
  either. They stay plain prompt text (the model sees them); revisit
  if MSP gains a list method or ACP gains a subagent surface.
