
  Already at parity

  ┌─────────────────┬────────────────┬──────────────────┐
  │ Area            │ CLI            │ Bridge           │
  ├─────────────────┼────────────────┼──────────────────┤
  │ Approval switch │ ask/auto/deny  │ yes, live picker │
  │ Yolo mode       │ n/a (TUI ask)  │ yes, picker+auto │
  │ Model switch    │ /model picker  │ yes, live picker │
  │ Effort tiers    │ 8 tiers        │ yes, picker now  │
  │ Sessions        │ resume/fork    │ yes, both        │
  │ /compact        │ compact        │ yes, inline      │
  │ Child blocks    │ TUI blocks     │ yes, tool_call   │
  │ /subagents      │ spawn list     │ yes, card        │
  │ Child verbs     │ 8 ctrls        │ yes, all 8       │
  │ /workflows      │ run list       │ yes, card        │
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
  • /goal, /tasks, /stop — LANDED as read-only cards (see the
  spike section). /workflows, /subagents — display LANDED
  (retention + live blocks + cards); all eight control verbs
  LANDED via `/subagents <verb> [target] [text]` + selectors.
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
- `/workflows` `/subagents` — DISPLAY DONE (this change;
  supersedes the DEFERRED verdict below it): the "no list method"
  reading was wrong — the view stream IS the list. The observer
  now retains `subagent`/`workflow` items session-wide
  (replace-iff-higher `revision`, idle completions included),
  streams each child as its own `tool_call` block on transitions
  (spawn → progress → terminal, TUI-style), and `/subagents` +
  `/workflows` cards plus history replay read the retention
  (resume seeds it, so imported sessions show children).
  VERBS DONE (this change): all eight ride `/subagents <verb>
  [target] [text]` — targets resolve by id prefix (durable id
  retained for control) with an elicitation selector fallback,
  `message`/`followup` take body text inline or via a select+text
  form, `result` renders the retained envelope and consumes via
  `readResult` only when nothing is kept. Every verb reports
  admission; outcomes land in the child's block. `/deep-research`
  stays plain prompt text: no workflow-launch method exists.
  - ~~DEFERRED (no wire to map): MSP exposes control ops but **no
    list method**, and membership lives in view items the bridge
    does not retain — revisit if MSP gains a list method.~~

## Zed 1.19.2 client behavior (verified from source, this change)

Evidence in `.cache/reference/zed/v1.19.2/`, ACP schemas in
`.cache/reference/acp-protocol/`.

- `session/new` updates must trail the result: Zed registers update
  routing only after the response arrives, so a pre-result
  `available_commands_update` is dropped and `/` stays empty. (Load
  and resume pre-register — history replay still streams first, per
  spec.) Both references send result first; so do we now, on all
  four session RPCs.
- Zed 1.19.2 negotiates ACP **v1**: the prominent ModeSelector
  renders only with `modes` and NO `configOptions` (`config_state`
  is all-or-nothing), but we keep `configOptions` (mode/model/
  effort buttons, one per option) because that is where the model
  and effort pickers live — `modes` rides along on v1 for
  ModeSelector-path clients. ACP v2 defines no modes at all, but
  Zed's `agent-client-protocol` 2.0.0 crate still reads `modes`
  and sends `session/set_mode`.
- Import sessions: Zed calls `session/list` (cwd-filtered) and
  opens entries via `session/load` (gated on the v1
  `loadSession` cap, which we advertise). Entries need absolute
  `cwd`s — rootless host rows are skipped, never emitted blank —
  and titles + `updatedAt` come from folded/host metadata.
