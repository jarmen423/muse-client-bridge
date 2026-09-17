#!/usr/bin/env python3
"""Fake `muse serve` MSP host for the bridge's scripted integration tests.

Speaks just enough MSP JSON-RPC (line-delimited, over stdin/stdout) for the
handshake, then plays one scripted scenario per run. Scenario knobs arrive
via env (the Rust tests pass per-test values through the child env, so tests
stay parallel-safe):

  FAKE_SCENARIO   serve (default) | silent:<method> | overloaded:<method>:<n>
                  | fail:<method>:<kind>:<code> | die:<code> | probe-unknown
                  | garbage | turn-happy | turn-approval
                  | turn-approval-all-approve | turn-approval-no-id
                  | turn-userinput | turn-userinput-form | turn-gap | turn-failed | turn-tool-slow
                  | turn-tool | turn-reasoning | turn-reasoning-quiet
                  | turn-unqueued | turn-retracted | turn-retract-then-completed
                  | turn-retry-then-completed | turn-queued | turn-children
  FAKE_LOG        file recording received "METHOD cmd=<commandId|-> ..." lines
                  (key methods append assertion details; client responses to
                  server requests log as "<response> id=<id> result|error")
  FAKE_INPUT      file recording one {"method","params"} JSON object per line
                  for turn/start, turn/steer, session/start, session/resume,
                  session/fork, session/compact, session/setModel,
                  session/setApprovalMode (assert what the bridge sent the host)
  FAKE_LAUNCH_LOG file recording one line per process start (restart counting)
  FAKE_VERDICT    file the probe-unknown scenario writes OK/FAIL into
  FAKE_SCHEMA_VERSION / FAKE_FINGERPRINT / FAKE_DURABILITY ("absent" omits it)
  FAKE_MODE       approval mode the fake serves (default denyUnmatched)
  FAKE_FAIL_REASON extra `reason` on fail-scenario errors (default "")
  FAKE_HISTORY    "1" seeds resume/read history with one user+agent turn
  FAKE_HISTORY_FORK "1" adds fork-point items (dup agent text, userShell
                  without a turn) to the seeded history
  FAKE_HISTORY_TOOL "1" adds a completed toolCall item to the seeded history
  FAKE_GRANT_USERSHELL "1" grants the userShell capability at initialize
  FAKE_WITHHOLD_SESSIONMCP "1" withholds the sessionMcp grant (default:
                  granted — proves the bridge gates config.mcpServers)
  FAKE_CRASH_AFTER_TURN_START "1" exits(1) right after the next turn/start
                  ack (mid-turn host death); one-shot when FAKE_RESTART_MARKER
                  names a file (crash once, behave after the supervisor
                  restarts us — the replacement sees the marker)
  FAKE_RESTART_MARKER path gating the one-shot crash above
  FAKE_COMPACT    "noop" makes session/compact answer status noop
  FAKE_OVERLOADED "method:n[,method:n]" answers the first n calls of each
                  method with kind=overloaded (combinable with any scenario,
                  unlike the overloaded:<method>:<n> scenario form)
  FAKE_NOISE      "1" prefixes turn-happy with rule-7 noise: an unparsable
                  line, an unknown notification, an unknown item kind, and an
                  unknown item-status enum (the turn must still complete)
  FAKE_GOAL       "1" emits session/goalChanged after every session/read
                  result (drives /goal and the status/recap goal line)
  FAKE_TODOS      "1" emits session/todoListChanged after every session/read
                  result (drives /tasks and plan updates)

Reads stdin until EOF, then exits 0 (like the real host's orderly drain).
"""

import json
import os
import select
import sys
import time

PIN = "sha256:c7ff6c5d1e89cd42f803aea1f05b8e72082f2099685802473eb726903484713b"
SCHEMA_VERSION = int(os.environ.get("FAKE_SCHEMA_VERSION", "1"))
FINGERPRINT = os.environ.get("FAKE_FINGERPRINT", PIN)
DURABILITY = os.environ.get("FAKE_DURABILITY", "durable")
SCENARIO = os.environ.get("FAKE_SCENARIO", "serve")
LOG = os.environ.get("FAKE_LOG", "")
LAUNCH_LOG = os.environ.get("FAKE_LAUNCH_LOG", "")
VERDICT = os.environ.get("FAKE_VERDICT", "")
FAKE_MODE = os.environ.get("FAKE_MODE", "denyUnmatched")
FAKE_FAIL_REASON = os.environ.get("FAKE_FAIL_REASON", "")
FAKE_HISTORY = os.environ.get("FAKE_HISTORY", "")
GRANT_USERSHELL = os.environ.get("FAKE_GRANT_USERSHELL", "")
WITHHOLD_SESSIONMCP = os.environ.get("FAKE_WITHHOLD_SESSIONMCP", "")
FAKE_COMPACT = os.environ.get("FAKE_COMPACT", "accepted")
FAKE_INPUT = os.environ.get("FAKE_INPUT", "")
FAKE_CRASH_AFTER_TURN_START = os.environ.get("FAKE_CRASH_AFTER_TURN_START", "")
FAKE_RESTART_MARKER = os.environ.get("FAKE_RESTART_MARKER", "")
FAKE_HISTORY_FORK = os.environ.get("FAKE_HISTORY_FORK", "")
FAKE_HISTORY_TOOL = os.environ.get("FAKE_HISTORY_TOOL", "")
FAKE_GOAL = os.environ.get("FAKE_GOAL", "")
FAKE_TODOS = os.environ.get("FAKE_TODOS", "")

MSP_SID = "fake-sess-1"
OVERLOADED_LEFT = {}
SILENT_METHOD = None
FAIL_RULE = None  # (method, kind, code)
DIE_CODE = None
PROBE_UNKNOWN = False
GARBAGE = False

TURN_SCRIPT = None
parts = SCENARIO.split(":")
if parts[0] == "silent" and len(parts) == 2:
    SILENT_METHOD = parts[1]
elif parts[0] == "overloaded" and len(parts) == 3:
    OVERLOADED_LEFT[parts[1]] = int(parts[2])
elif parts[0] == "fail" and len(parts) == 4:
    FAIL_RULE = (parts[1], parts[2], int(parts[3]))
elif parts[0] == "die" and len(parts) == 2:
    DIE_CODE = int(parts[1])
elif SCENARIO == "probe-unknown":
    PROBE_UNKNOWN = True
elif SCENARIO == "garbage":
    GARBAGE = True
elif SCENARIO.startswith("turn-"):
    TURN_SCRIPT = SCENARIO
elif SCENARIO != "serve":
    sys.stderr.write(f"fake_serve: unknown scenario {SCENARIO!r}\n")
    sys.exit(2)
if TURN_SCRIPT not in (None, "turn-happy", "turn-approval", "turn-approval-all-approve",
                       "turn-approval-no-id", "turn-userinput", "turn-userinput-form", "turn-gap",
                       "turn-failed", "turn-tool-slow", "turn-tool",
                       "turn-reasoning", "turn-reasoning-quiet", "turn-unqueued",
                       "turn-retracted", "turn-retract-then-completed",
                       "turn-retry-then-completed", "turn-queued",
                       "turn-children"):
    sys.stderr.write(f"fake_serve: unknown turn script {TURN_SCRIPT!r}\n")
    sys.exit(2)
for rule in os.environ.get("FAKE_OVERLOADED", "").split(","):
    rule = rule.strip()
    if not rule:
        continue
    try:
        overloaded_method, overloaded_n = rule.split(":")
        OVERLOADED_LEFT[overloaded_method.strip()] = int(overloaded_n)
    except ValueError:
        sys.stderr.write(f"fake_serve: bad FAKE_OVERLOADED rule {rule!r}\n")
        sys.exit(2)


def log_launch():
    if LAUNCH_LOG:
        with open(LAUNCH_LOG, "a") as f:
            f.write(f"launch pid={os.getpid()}\n")


def log_method(method, params):
    if not LOG:
        return
    params = params if isinstance(params, dict) else {}
    cmd = params.get("commandId", "-")
    detail = ""
    if method == "initialize":
        caps = (params.get("capabilities") or {}).get("requestedCapabilities") or []
        detail = f" caps={','.join(caps)}"
    elif method == "turn/start":
        detail = f" nparts={len(params.get('input', []) or [])}"
    elif method == "turn/cancel":
        detail = f" turn={params.get('turnId', '-')}"
    elif method == "approval/decide":
        detail = f" approval={params.get('approvalId', '-')} choice={params.get('choiceId', '-')}"
    elif method == "userInput/cancel":
        detail = f" ui={params.get('userInputId', '-')} reason={params.get('reason', '-')}"
    elif method == "session/start":
        detail = f" mode={params.get('approvalMode', '-')} ws={params.get('workspaceRoot', '-')}"
        mcp = (params.get("config") or {}).get("mcpServers") or {}
        if mcp:
            detail += f" mcp={','.join(sorted(mcp))}"
    elif method == "session/setModel":
        detail = f" model={(params.get('model') or {}).get('modelId', '-')}"
    elif method == "session/setApprovalMode":
        detail = f" mode={params.get('mode', '-')}"
    elif method == "session/resume":
        detail = f" sid={params.get('sessionId', '-')} history={params.get('history', '-')}"
    elif method == "session/fork":
        detail = f" src={params.get('sessionId', '-')} cut={(params.get('cutPoint') or {}).get('lastTurnId', '<all>')}"
    elif method == "session/compact":
        detail = f" sid={params.get('sessionId', '-')}"
    elif method == "session/rename":
        detail = f" name={params.get('name', '-')}"
    elif method == "session/setReasoningEffort":
        detail = f" effort={params.get('reasoningEffort', '-')}"
    elif method == "userInput/answer":
        detail = f" ui={params.get('userInputId', '-')} answers={params.get('answers', '-')}"
    elif method == "turn/steer":
        detail = f" expected={params.get('expectedTurnId', '-')}"
    elif method == "turn/unqueue":
        detail = f" turn={params.get('turnId', '-')}"
    elif method.startswith("subagent/"):
        detail = f" sub={params.get('subagentId', '-')}"
    elif method == "view/page":
        detail = f" cursor={params.get('cursor', '<genesis>')}"
    with open(LOG, "a") as f:
        f.write(f"{method} cmd={cmd}{detail}\n")


def log_response(msg):
    if not LOG:
        return
    kind = "result" if "result" in msg else "error"
    with open(LOG, "a") as f:
        f.write(f"<response> id={msg.get('id')} {kind}\n")


INPUT_METHODS = ("turn/start", "turn/steer", "session/start", "session/resume",
                 "session/fork", "session/compact", "session/setModel",
                 "session/setApprovalMode")


def log_input(method, params):
    if not FAKE_INPUT or method not in INPUT_METHODS:
        return
    with open(FAKE_INPUT, "a") as f:
        f.write(json.dumps({"method": method, "params": params}) + "\n")


def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def initialize_result():
    grants = [] if WITHHOLD_SESSIONMCP else ["sessionMcp"]
    if GRANT_USERSHELL:
        grants.append("userShell")
    result = {
        "experimentalApi": False,
        "grantedCapabilities": grants,
        "museHome": "/tmp/fake-muse-home",
        "platformFamily": "unix",
        "platformOs": "linux",
        "schema": {"fingerprint": FINGERPRINT, "version": SCHEMA_VERSION},
        "serverInfo": {"name": "fake-muse", "version": "9.9-test"},
        "userAgent": "fake-build/test",
    }
    if DURABILITY != "absent":
        result["sessionDurability"] = DURABILITY
    return result


def session_obj():
    return {
        "activeTurnId": None,
        "approvalMode": {"lastCommandId": None, "mode": FAKE_MODE, "source": "startup"},
        "createdAt": "2026-09-14T00:00:00Z",
        "forkedFrom": None,
        "modelId": "fake-model",
        "name": "fake session",
        "path": "/tmp/fake-session.jsonl" if DURABILITY != "ephemeral" else "",
        "providerId": "fake",
        "sessionId": MSP_SID,
        "status": "idle",
        "turnCount": 0,
        "updatedAt": "2026-09-14T00:00:00Z",
        "workspaceRoot": "/tmp/fake-ws",
    }


def history_items():
    """Seeded resume/read history (FAKE_HISTORY=1): one user + one agent turn,
    plus fork-point items (FAKE_HISTORY_FORK=1) and a toolCall item
    (FAKE_HISTORY_TOOL=1). Items carry turnIds so fork points resolve."""
    if not FAKE_HISTORY:
        return []
    items = [
        {"itemId": "h-user-1", "kind": "userMessage", "turnId": "h-turn-1",
         "revision": 1, "status": "completed", "text": "seeded question"},
        {"itemId": "h-agent-1", "kind": "agentMessage", "turnId": "h-turn-1",
         "revision": 2, "status": "completed", "text": "seeded answer"},
    ]
    if FAKE_HISTORY_FORK:
        items += [
            {"itemId": "msg-fork", "kind": "agentMessage", "turnId": "turn-1",
             "revision": 1, "status": "completed", "text": "fork here"},
            {"itemId": "msg-fork-dup", "kind": "agentMessage", "turnId": "turn-2",
             "revision": 1, "status": "completed", "text": "fork here"},
            {"itemId": "msg-fork-2", "kind": "agentMessage", "turnId": "turn-2",
             "revision": 1, "status": "completed", "text": "later answer"},
            {"itemId": "shell-no-turn", "kind": "userShell", "turnId": None,
             "revision": 1, "status": "completed", "commandText": "git status"},
        ]
    if FAKE_HISTORY_TOOL:
        items.append(
            {"itemId": "h-tool-1", "kind": "toolCall", "turnId": "h-turn-1",
             "revision": 2, "status": "completed", "tool": "read",
             "args": {"path": "/tmp/h"}, "fallbackText": "read /tmp/h"},
        )
    return items


def answer(method, req_id, params):
    if method == "initialize":
        return {"result": initialize_result()}
    if method == "model/list":
        return {
            "result": {
                "models": [
                    {
                        "contextLimit": 1000,
                        "cost": None,
                        "description": None,
                        "displayLabel": "fake-a",
                        "isActive": False,
                        "isDefault": True,
                        "modelId": "fake-a",
                        "outputLimit": 100,
                        "profileId": None,
                        "providerId": "fake",
                        "releaseDate": None,
                    },
                ],
                "profileId": None,
                "providerId": "fake",
                "source": "providerCatalog",
            }
        }
    if method == "session/start":
        # Like the live host, `session/started` precedes the ack.
        send({"jsonrpc": "2.0", "method": "session/started",
              "params": {"session": session_obj()}})
        return {"result": {"session": session_obj(), "viewCursor": "v:fake:1"}}
    if method == "session/setModel":
        return {
            "result": {
                "commandId": params.get("commandId", ""),
                "status": "accepted",
                "modelId": (params.get("model") or {}).get("modelId", ""),
            }
        }
    if method == "session/setApprovalMode":
        return {
            "result": {
                "commandId": params.get("commandId", ""),
                "status": "accepted",
                "applyOutcome": "applied",
                "effectiveMode": {"mode": params.get("mode", FAKE_MODE)},
            }
        }
    if method == "session/resume":
        return {
            "result": {
                "session": session_obj(),
                "history": {"mode": "inline", "items": history_items()},
                "pendingRequests": [],
                "viewCursor": "v:fake:2",
            }
        }
    if method == "session/list":
        listed = dict(session_obj())
        host_only = dict(session_obj())
        host_only["sessionId"] = "fake-sess-host-only"
        host_only["updatedAt"] = "2026-09-15T00:00:00Z"
        return {"result": {"sessions": [listed, host_only]}}
    if method == "session/read":
        return {"result": {
            "session": session_obj(),
            "history": {"mode": "inline", "items": history_items()},
            "pendingRequests": [],
        }}
    if method == "session/rename":
        return {"result": {
            "commandId": params.get("commandId", ""),
            "status": "accepted",
            "name": params.get("name", ""),
        }}
    if method == "session/setReasoningEffort":
        return {"result": {
            "commandId": params.get("commandId", ""),
            "status": "accepted",
            "reasoningEffort": params.get("reasoningEffort", ""),
        }}
    if method == "session/fork":
        forked = dict(session_obj())
        forked["sessionId"] = "fake-sess-fork"
        forked["forkedFrom"] = {"commandId": params.get("commandId", ""),
                                "sessionId": params.get("sessionId", "")}
        return {
            "result": {
                "session": forked,
                "history": {"mode": "none", "items": []},
                "pendingRequests": [],
                "viewCursor": "v:fake:3",
            }
        }
    if method == "session/compact":
        if FAKE_COMPACT == "noop":
            return {"result": {"commandId": params.get("commandId", ""),
                               "status": "noop", "reason": "nothing to compact"}}
        return {"result": {"commandId": params.get("commandId", ""),
                           "status": "accepted"}}
    if method == "turn/steer":
        return {
            "result": {
                "commandId": params.get("commandId", ""),
                "status": "accepted",
                "turnId": params.get("expectedTurnId", ""),
            }
        }
    if method == "turn/unqueue":
        return {
            "result": {
                "commandId": params.get("commandId", ""),
                "status": "accepted",
                "turnId": params.get("turnId", ""),
            }
        }
    if method == "turn/start":
        cid = params.get("commandId", "")
        return {
            "result": {
                "commandId": cid,
                "disposition": "started",
                "startedNewTurn": True,
                "status": "accepted",
                "turnId": cid,
            }
        }
    if method in ("turn/cancel", "turn/interrupt"):
        return {
            "result": {
                "commandId": params.get("commandId", ""),
                "status": "accepted",
                "turnId": params.get("turnId", ""),
            }
        }
    if method.startswith("subagent/"):
        # All eight control methods ack admission-only (CommandAccepted).
        return {
            "result": {
                "commandId": params.get("commandId", ""),
                "status": "accepted",
            }
        }
    if method == "approval/decide":
        return {
            "result": {
                "commandId": params.get("commandId", ""),
                "status": "accepted",
                "approvalId": params.get("approvalId", ""),
                "terminal": True,
            }
        }
    if method in ("userInput/cancel", "userInput/answer"):
        return {
            "result": {
                "commandId": params.get("commandId", ""),
                "status": "accepted",
                "userInputId": params.get("userInputId", ""),
            }
        }
    if method == "view/page":
        return {"result": {"events": [], "nextCursor": None}}
    return {
        "error": {
            "code": -32601,
            "message": f"method not found: {method}",
            "data": {"kind": "methodNotFound", "retryable": False},
        }
    }


# --- Turn scripts (P4 dispatch tests) --------------------------------------
# After the turn/start ack, the fake plays one scripted view sequence.
# Cursor strings are synthetic but ordered; turn ids echo the commandId.

CURSOR_SEQ = [10]


def next_cursor():
    CURSOR_SEQ[0] += 1
    return f"v:fake:{CURSOR_SEQ[0]}"


def notify(method, params):
    params = dict(params)
    params.setdefault("sessionId", MSP_SID)
    params.setdefault("viewCursor", next_cursor())
    send({"jsonrpc": "2.0", "method": method, "params": params})


def agent_item(item_id, turn_id, rev, status, text):
    return {
        "itemId": item_id, "kind": "agentMessage", "turnId": turn_id,
        "revision": rev, "status": status, "text": text,
    }


def complete_turn(turn_id, terminal, error=None):
    params = {"turnId": turn_id, "terminal": terminal}
    if error is not None:
        params["error"] = error
    notify("turn/completed", params)


def emit_usage(turn_id):
    notify("session/tokenUsage", {
        "turnId": turn_id, "promptTokens": 100,
        "usage": {"outputTokens": 10},
        "cumulative": {"promptTokens": 100, "outputTokens": 10, "totalTokens": 110},
    })


PENDING = {"turn": None, "deadline": None}
QUEUED = []


def reasoning_item(item_id, turn_id, rev, status, summary=None):
    item = {
        "itemId": item_id, "kind": "reasoning", "turnId": turn_id,
        "revision": rev, "status": status,
    }
    if summary is not None:
        item["summary"] = summary
    return item


def tool_item(item_id, turn_id, rev, status, output=""):
    return {
        "itemId": item_id, "kind": "toolCall", "turnId": turn_id,
        "revision": rev, "status": status, "tool": "shell",
        "visibleOutput": output,
    }


def play_script(turn_id):
    """Emit the post-ack script for TURN_SCRIPT (called after the ack)."""
    if TURN_SCRIPT == "turn-happy":
        if os.environ.get("FAKE_NOISE", ""):
            # Rule-7 noise: the client must survive all of it and still
            # complete the turn below.
            sys.stdout.write("{not json\n")
            sys.stdout.flush()
            notify("session/futureThing", {"weird": [1, 2, {"x": None}]})
            notify("item/started", {"item": {
                "itemId": "z1", "kind": "hologram", "turnId": turn_id,
                "revision": 1, "status": "inProgress",
                "fallbackText": "shiny future",
            }})
            notify("item/completed", {"item": {
                "itemId": "z1", "kind": "hologram", "turnId": turn_id,
                "revision": 2, "status": "completed",
                "fallbackText": "shiny future",
            }})
            notify("item/started", {"item": {
                "itemId": "t9", "kind": "toolCall", "turnId": turn_id,
                "revision": 1, "status": "inProgress", "tool": "shell",
            }})
            notify("item/completed", {"item": {
                "itemId": "t9", "kind": "toolCall", "turnId": turn_id,
                "revision": 2, "status": "quantum", "tool": "shell",
            }})
        notify("turn/started", {"turnId": turn_id})
        notify("item/started", {"item": agent_item("m1", turn_id, 1, "inProgress", "")})
        notify("item/delta", {"itemId": "m1", "field": "text", "delta": "Hello"})
        notify("item/delta", {"itemId": "m1", "field": "text", "delta": ", world"})
        notify("item/completed", {"item": agent_item("m1", turn_id, 2, "completed", "Hello, world")})
        emit_usage(turn_id)
        complete_turn(turn_id, "completed")
    elif TURN_SCRIPT == "turn-approval":
        notify("turn/started", {"turnId": turn_id})
        PENDING["turn"] = turn_id
        sendApproval(turn_id, all_approve=False)
    elif TURN_SCRIPT == "turn-approval-all-approve":
        notify("turn/started", {"turnId": turn_id})
        PENDING["turn"] = turn_id
        sendApproval(turn_id, all_approve=True)
    elif TURN_SCRIPT == "turn-approval-no-id":
        notify("turn/started", {"turnId": turn_id})
        PENDING["turn"] = turn_id
        sendApproval(turn_id, all_approve=False, approval_id=None)
    elif TURN_SCRIPT == "turn-userinput":
        notify("turn/started", {"turnId": turn_id})
        PENDING["turn"] = turn_id
        send({
            "jsonrpc": "2.0", "id": 9200, "method": "userInput/request",
            "params": {
                "sessionId": MSP_SID, "viewCursor": next_cursor(),
                "userInputId": "ui-1",
                "prompt": {"kind": "text", "text": "your name?"},
                "timeoutMs": 5000,
            },
        })
    elif TURN_SCRIPT == "turn-userinput-form":
        notify("turn/started", {"turnId": turn_id})
        PENDING["turn"] = turn_id
        send({
            "jsonrpc": "2.0", "id": 9200, "method": "userInput/request",
            "params": {
                "sessionId": MSP_SID, "viewCursor": next_cursor(),
                "userInputId": "ui-1",
                "questions": [{
                    "id": "color", "header": "Pick",
                    "question": "Which color?",
                    "options": [{"label": "red"}, {"label": "red"},
                                {"label": "blue"}],
                    "selection": {"mode": "single"},
                }],
                "timeoutMs": 5000,
            },
        })
    elif TURN_SCRIPT == "turn-gap":
        notify("turn/started", {"turnId": turn_id})
        notify("item/started", {"item": agent_item("m1", turn_id, 1, "inProgress", "")})
        notify("item/delta", {"itemId": "m1", "field": "text", "delta": "Hello"})
        PENDING["turn"] = turn_id
        notify("view/gap", {"after": "v:fake:12", "next": "v:fake:19"})
    elif TURN_SCRIPT == "turn-failed":
        notify("turn/started", {"turnId": turn_id})
        complete_turn(turn_id, "failed", {
            "kind": "authRequired", "message": "fake login required", "retryable": False,
        })
    elif TURN_SCRIPT == "turn-tool-slow":
        notify("turn/started", {"turnId": turn_id})
        notify("item/started", {"item": {
            "itemId": "t1", "kind": "toolCall", "turnId": turn_id,
            "revision": 1, "status": "inProgress", "tool": "shell",
        }})
        PENDING["turn"] = turn_id
        PENDING["deadline"] = time.monotonic() + 12.0
    elif TURN_SCRIPT == "turn-tool":
        notify("turn/started", {"turnId": turn_id})
        notify("item/started", {"item": tool_item("t1", turn_id, 1, "inProgress")})
        notify("item/completed", {"item": tool_item("t1", turn_id, 2, "completed", "tool bytes")})
        notify("item/completed", {"item": agent_item("m1", turn_id, 1, "completed", "wrapped")})
        emit_usage(turn_id)
        complete_turn(turn_id, "completed")
    elif TURN_SCRIPT == "turn-reasoning":
        notify("turn/started", {"turnId": turn_id})
        rid = "r1"
        notify("item/started", {"item": reasoning_item(rid, turn_id, 1, "inProgress")})
        notify("item/delta", {"itemId": rid, "field": "summary.0", "delta": "Considering "})
        notify("item/delta", {"itemId": rid, "field": "summary.0", "delta": "the schema"})
        notify("item/delta", {"itemId": rid, "field": "summary.1", "delta": "Then testing"})
        notify("item/completed", {"item": reasoning_item(
            rid, turn_id, 2, "completed", ["Considering the schema", "Then testing"])})
        notify("item/completed", {"item": agent_item("m1", turn_id, 1, "completed", "done")})
        emit_usage(turn_id)
        complete_turn(turn_id, "completed")
    elif TURN_SCRIPT == "turn-reasoning-quiet":
        notify("turn/started", {"turnId": turn_id})
        notify("item/completed", {"item": reasoning_item(
            "r9", turn_id, 1, "completed", ["Committed thought"])})
        notify("item/completed", {"item": agent_item("m1", turn_id, 1, "completed", "done")})
        emit_usage(turn_id)
        complete_turn(turn_id, "completed")
    elif TURN_SCRIPT == "turn-unqueued":
        notify("turn/started", {"turnId": turn_id})
        notify("turn/unqueued", {"turnId": turn_id})
    elif TURN_SCRIPT == "turn-retracted":
        notify("turn/started", {"turnId": turn_id})
        notify("turn/retracted", {"turnId": turn_id, "commandId": turn_id})
    elif TURN_SCRIPT == "turn-retract-then-completed":
        notify("turn/started", {"turnId": turn_id})
        notify("turn/retracted", {"turnId": turn_id, "commandId": turn_id})
        # A late terminal after the retract: the fold must settle once.
        complete_turn(turn_id, "cancelled")
    elif TURN_SCRIPT == "turn-retry-then-completed":
        notify("turn/started", {"turnId": turn_id})
        notify("turn/retryScheduled", {
            "turnId": turn_id, "attempt": 1, "nextAttempt": 2,
            "maxAttempts": 3, "retryDelayMs": 2000,
            "reason": "provider stream disconnected",
        })
        notify("item/completed", {"item": agent_item("m1", turn_id, 1, "completed", "recovered")})
        emit_usage(turn_id)
        complete_turn(turn_id, "completed")
    elif TURN_SCRIPT == "turn-queued":
        notify("turn/started", {"turnId": turn_id})
        QUEUED.append(turn_id)
        if len(QUEUED) == 2:
            for held in QUEUED:
                emit_usage(held)
                complete_turn(held, "completed")
            QUEUED.clear()
    elif TURN_SCRIPT == "turn-children":
        notify("turn/started", {"turnId": turn_id})
        notify("item/started", {"item": {
            "itemId": "child-1", "kind": "subagent", "turnId": turn_id,
            "revision": 1, "objective": "Explore the schema",
            "controlStatus": "starting", "subagentId": "sa-1", "depth": 1,
        }})
        notify("item/updated", {"item": {
            "itemId": "child-1", "kind": "subagent", "turnId": turn_id,
            "revision": 2, "status": "inProgress",
            "objective": "Explore the schema",
            "controlStatus": "running", "subagentId": "sa-1", "depth": 1,
        }})
        notify("item/started", {"item": {
            "itemId": "wf-1", "kind": "workflow", "turnId": turn_id,
            "revision": 1, "scriptId": "research", "entryId": "entry-1",
            "triggerSource": "modelProposal", "children": [],
        }})
        notify("item/completed", {"item": {
            "itemId": "child-1", "kind": "subagent", "turnId": turn_id,
            "revision": 3, "status": "completed",
            "objective": "Explore the schema",
            "controlStatus": "closed", "subagentId": "sa-1", "depth": 1,
            "durationMs": 1500,
            "result": {"summary": "schema mapped", "text": "tables: users, orders",
                       "artifactRefs": [], "evidenceRefs": []},
        }})
        notify("item/started", {"item": {
            "itemId": "child-2", "kind": "subagent", "turnId": turn_id,
            "revision": 1, "objective": "Write the migration",
            "controlStatus": "starting", "subagentId": "sa-2", "depth": 1,
        }})
        notify("item/updated", {"item": {
            "itemId": "child-2", "kind": "subagent", "turnId": turn_id,
            "revision": 2, "status": "inProgress",
            "objective": "Write the migration",
            "controlStatus": "running", "subagentId": "sa-2", "depth": 1,
        }})
        notify("item/completed", {"item": {
            "itemId": "wf-1", "kind": "workflow", "turnId": turn_id,
            "revision": 2, "status": "completed", "scriptId": "research",
            "entryId": "entry-1", "triggerSource": "modelProposal",
            "message": "two findings",
            "children": [
                {"childId": "a", "attempt": 1, "status": "succeeded",
                 "label": "search", "terminal": "completed"},
                {"childId": "b", "attempt": 1, "status": "succeeded",
                 "label": "read", "terminal": "completed"},
            ],
        }})
        notify("item/completed", {"item": agent_item("m1", turn_id, 1, "completed", "done")})
        emit_usage(turn_id)
        complete_turn(turn_id, "completed")


def sendApproval(turn_id, all_approve, approval_id="ap-1"):
    choices = [
        {"choiceId": "c-allow-always", "decision": "approveAlways", "label": "Always"},
        {"choiceId": "c-allow", "decision": "approve", "label": "Allow"},
    ]
    if all_approve:
        choices.append({"choiceId": "c-approve-session", "decision": "approveForSession", "label": "Session"})
    else:
        choices.append({"choiceId": "c-deny", "decision": "deny", "label": "Deny"})
    params = {
        "sessionId": MSP_SID, "viewCursor": next_cursor(),
        "currentRequirementId": {"approvalId": approval_id or "", "sourceIndex": 0},
        "toolName": "shell",
        "subject": {"kind": "shell", "command": "rm -rf /"},
        "availableChoices": choices,
        "isOutboundStale": False,
    }
    if approval_id is not None:
        params["approvalId"] = approval_id
    send({
        "jsonrpc": "2.0", "id": 9100, "method": "approval/request",
        "params": params,
    })


def on_decide(turn_id):
    if PENDING["turn"] == turn_id:
        PENDING["turn"] = None
        emit_usage(turn_id)
        complete_turn(turn_id, "completed")


def on_userinput_cancel(turn_id):
    if PENDING["turn"] == turn_id:
        PENDING["turn"] = None
        emit_usage(turn_id)
        complete_turn(turn_id, "completed")


def on_turn_cancel(turn_id):
    if PENDING["turn"] == turn_id:
        PENDING["turn"] = None
        PENDING["deadline"] = None
        complete_turn(turn_id, "cancelled")


def answer_page(turn_id):
    # The gap refill: full truth for the gapped region.
    return {"result": {
        "events": [
            {"method": "item/completed", "params": {
                "sessionId": MSP_SID, "viewCursor": "v:fake:15",
                "item": agent_item("m1", turn_id, 2, "completed", "Hello, world"),
            }},
        ],
        "nextCursor": None,
    }}


STDIN_BUF = bytearray()


# Sentinel: read_line returns EOF only at end-of-file (blank lines are "").
EOF = object()


def read_line(timeout):
    """Next stdin line (no terminator), EOF at end-of-file, None on tick.

    Reads fd 0 directly: `select` must never mix with buffered `sys.stdin`
    (readahead would strand lines in userspace while `select` sleeps).
    """
    while True:
        nl = STDIN_BUF.find(b"\n")
        if nl >= 0:
            line = bytes(STDIN_BUF[:nl])
            del STDIN_BUF[:nl + 1]
            return line.decode("utf-8", "replace")
        ready, _, _ = select.select([0], [], [], timeout)
        if not ready:
            return None
        try:
            chunk = os.read(0, 65536)
        except OSError:
            chunk = b""
        if chunk == b"":
            if STDIN_BUF:
                line = bytes(STDIN_BUF)
                STDIN_BUF.clear()
                return line.decode("utf-8", "replace")
            return EOF
        STDIN_BUF.extend(chunk)


def main():
    log_launch()
    initialized = False
    garbage_sent = False
    # Server request ids are positive ints from a per-process counter.
    server_id = 9000
    while True:
        line = read_line(1.0)
        if line is None:
            # Tick: fail loudly if a cancel never arrives (test bug, not hang).
            if PENDING["deadline"] is not None and time.monotonic() > PENDING["deadline"]:
                turn_id = PENDING["turn"]
                PENDING["turn"] = None
                PENDING["deadline"] = None
                complete_turn(turn_id, "failed", {
                    "kind": "internal", "message": "fake: no turn/cancel arrived", "retryable": False,
                })
            continue
        if line is EOF:
            break  # EOF: orderly drain, exit 0
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        method = msg.get("method", "")
        req_id = msg.get("id")
        params = msg.get("params") or {}
        if req_id is not None and method == "":
            log_response(msg)
        else:
            log_method(method or "<response>", params if isinstance(params, dict) else {})

        if req_id is None:
            # Client notification.
            if method == "initialized":
                initialized = True
                if DIE_CODE is not None:
                    sys.exit(DIE_CODE)
                if PROBE_UNKNOWN:
                    server_id += 1
                    send({
                        "jsonrpc": "2.0",
                        "id": server_id,
                        "method": "future/request",
                        "params": {"ping": True},
                    })
            continue

        if req_id is not None and method == "":
            # Response to our server-initiated request (probe-unknown verdict).
            if PROBE_UNKNOWN and req_id == server_id and VERDICT:
                err = msg.get("error") or {}
                ok = err.get("code") == -32601 and (err.get("data") or {}).get("kind") == "methodNotFound"
                with open(VERDICT, "w") as f:
                    f.write("OK\n" if ok else f"FAIL got={line[:200]}\n")
            continue

        if not initialized and method != "initialize":
            send({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32600,
                    "message": "not initialized",
                    "data": {"kind": "notInitialized", "retryable": False},
                },
            })
            continue

        if GARBAGE and not garbage_sent and method == "initialize":
            garbage_sent = True
            sys.stdout.write("\n   \n{not json\n42\n[1,2]\n")
            sys.stdout.flush()

        if SILENT_METHOD is not None and method == SILENT_METHOD:
            continue  # never answer: the client must time out
        if method in OVERLOADED_LEFT and OVERLOADED_LEFT[method] > 0:
            OVERLOADED_LEFT[method] -= 1
            send({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32001,
                    "message": "fake host busy",
                    "data": {"kind": "overloaded", "retryable": True},
                },
            })
            continue
        if FAIL_RULE is not None and method == FAIL_RULE[0]:
            _m, kind, code = FAIL_RULE
            data = {"kind": kind, "retryable": False}
            if FAKE_FAIL_REASON:
                data["reason"] = FAKE_FAIL_REASON
            send({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": code, "message": f"fake {kind}", "data": data},
            })
            continue

        params_dict = params if isinstance(params, dict) else {}
        log_input(method, params_dict)
        if TURN_SCRIPT == "turn-gap" and method == "view/page" and PENDING["turn"] is not None:
            page = answer_page(PENDING["turn"])
            page["jsonrpc"] = "2.0"
            page["id"] = req_id
            send(page)
            turn_id = PENDING["turn"]
            PENDING["turn"] = None
            emit_usage(turn_id)
            complete_turn(turn_id, "completed")
            continue
        body = answer(method, req_id, params_dict)
        body["jsonrpc"] = "2.0"
        body["id"] = req_id
        send(body)
        if method == "session/read" and "result" in body:
            # Read-triggered (not start-triggered): the bridge's per-session
            # observer subscribes after session creation, so facts emitted
            # here always land on a live subscription.
            if FAKE_GOAL:
                notify("session/goalChanged", {"goal": {
                    "objective": "Ship the fake feature", "status": "active",
                    "percentComplete": 40, "currentWork": "fake tests",
                    "nextWork": "fake docs",
                }})
            if FAKE_TODOS:
                notify("session/todoListChanged", {"items": [
                    {"text": "fake done", "status": "completed"},
                    {"text": "Write fake tests", "status": "inProgress",
                     "activeForm": "Writing fake tests"},
                    {"text": "fake todo", "status": "pending"},
                ], "revision": 1, "sourceTool": "fake"})
        if (FAKE_CRASH_AFTER_TURN_START and method == "turn/start"
                and "result" in body):
            sys.stdout.flush()
            if FAKE_RESTART_MARKER and os.path.exists(FAKE_RESTART_MARKER):
                pass  # replacement host: behave sanely after the restart
            else:
                if FAKE_RESTART_MARKER:
                    with open(FAKE_RESTART_MARKER, "w") as f:
                        f.write("crashed")
                os._exit(1)
        if TURN_SCRIPT is not None and method == "turn/start" and "result" in body:
            play_script(body["result"]["turnId"])
        elif TURN_SCRIPT is not None and method == "approval/decide" and PENDING["turn"]:
            on_decide(PENDING["turn"])
        elif TURN_SCRIPT is not None and method in ("userInput/cancel", "userInput/answer") \
                and PENDING["turn"]:
            on_userinput_cancel(PENDING["turn"])
        elif TURN_SCRIPT is not None and method == "turn/cancel":
            on_turn_cancel(params_dict.get("turnId"))


main()
