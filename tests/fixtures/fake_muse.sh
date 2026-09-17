#!/usr/bin/env bash
# Fake `muse` binary for tests: `skills list --json` answers a canned
# registry; every other argv execs the fake MSP serve script.
set -euo pipefail

if [ "${1:-}" = "skills" ]; then
    cat <<'JSON'
{"diagnostics":[],"skills":[
  {"activation":"on","description":"A fake workspace skill","id":"fake-skill","scope":"workspace"},
  {"activation":"user-invocable-only","description":"Another fake skill","id":"other-skill","scope":"workspace"},
  {"activation":"off","description":"Disabled and filtered","id":"off-skill","scope":"workspace"}
]}
JSON
    exit 0
fi

exec python3 "$(dirname "${BASH_SOURCE[0]}")/fake_serve.py" "$@"
