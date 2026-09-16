#!/usr/bin/env python3
"""Fake `muse serve` MSP host for the bridge's scripted integration tests.

Speaks just enough MSP JSON-RPC (line-delimited, over stdin/stdout) for the
handshake, then plays one scripted scenario per run. Scenario knobs arrive
via env (the Rust tests pass per-test values through the child env, so tests
stay parallel-safe):

  FAKE_SCENARIO   serve (default) | silent:<method> | overloaded:<method>:<n>
                  | fail:<method>:<kind>:<code> | die:<code> | probe-unknown
                  | garbage | turn-happy | turn-approval
                  | turn-approval-all-approve | turn-userinput | turn-gap
                  | turn-failed | turn-tool-slow
  FAKE_LOG        file recording received "METHOD cmd=<commandId|-> ..." lines
                  (key methods append assertion details; client responses to
                  server requests log as "<response> id=<id> result|error")
  FAKE_LAUNCH_LOG file recording one line per process start (restart counting)
  FAKE_VERDICT    file the probe-unknown scenario writes OK/FAIL into
  FAKE_SCHEMA_VERSION / FAKE_FINGERPRINT / FAKE_DURABILITY ("absent" omits it)
  FAKE_MODE       approval mode the fake serves (default denyUnmatched)
  FAKE_FAIL_REASON extra `reason` on fail-scenario errors (default "")

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
                       "turn-userinput", "turn-gap", "turn-failed", "turn-tool-slow"):
    sys.stderr.write(f"fake_serve: unknown turn script {TURN_SCRIPT!r}\n")
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
    if method == "turn/start":
        detail = f" nparts={len(params.get('input', []) or [])}"
    elif method == "turn/cancel":
        detail = f" turn={params.get('turnId', '-')}"
    elif method == "approval/decide":
        detail = f" approval={params.get('approvalId', '-')} choice={params.get('choiceId', '-')}"
    elif method == "userInput/cancel":
        detail = f" ui={params.get('userInputId', '-')} reason={params.get('reason', '-')}"
    elif method == "session/start":
        detail = f" mode={params.get('approvalMode', '-')} ws={params.get('workspaceRoot', '-')}"
    elif method == "session/setModel":
        detail = f" model={(params.get('model') or {}).get('modelId', '-')}"
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


def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def initialize_result():
    result = {
        "experimentalApi": False,
        "grantedCapabilities": [],
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
        "path": "/tmp/fake-session.jsonl" if DURABILITY != "ephemeral" else "",
        "providerId": "fake",
        "sessionId": MSP_SID,
        "status": "idle",
        "turnCount": 0,
        "updatedAt": "2026-09-14T00:00:00Z",
        "workspaceRoot": "/tmp/fake-ws",
    }


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


def play_script(turn_id):
    """Emit the post-ack script for TURN_SCRIPT (called after the ack)."""
    if TURN_SCRIPT == "turn-happy":
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


def sendApproval(turn_id, all_approve):
    choices = [
        {"choiceId": "c-allow-always", "decision": "approveAlways", "label": "Always"},
        {"choiceId": "c-allow", "decision": "approve", "label": "Allow"},
    ]
    if all_approve:
        choices.append({"choiceId": "c-approve-session", "decision": "approveForSession", "label": "Session"})
    else:
        choices.append({"choiceId": "c-deny", "decision": "deny", "label": "Deny"})
    send({
        "jsonrpc": "2.0", "id": 9100, "method": "approval/request",
        "params": {
            "sessionId": MSP_SID, "viewCursor": next_cursor(),
            "approvalId": "ap-1",
            "currentRequirementId": {"approvalId": "ap-1", "sourceIndex": 0},
            "toolName": "shell",
            "subject": {"kind": "command", "text": "rm -rf /"},
            "availableChoices": choices,
            "isOutboundStale": False,
        },
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
        if TURN_SCRIPT is not None and method == "turn/start" and "result" in body:
            play_script(body["result"]["turnId"])
        elif TURN_SCRIPT is not None and method == "approval/decide" and PENDING["turn"]:
            on_decide(PENDING["turn"])
        elif TURN_SCRIPT is not None and method == "userInput/cancel" and PENDING["turn"]:
            on_userinput_cancel(PENDING["turn"])
        elif TURN_SCRIPT is not None and method == "turn/cancel":
            on_turn_cancel(params_dict.get("turnId"))


main()
