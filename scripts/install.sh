#!/bin/sh
# Register only the delivery service entry and its internal role/helper support.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PYTHON=${DEVFLOW_PYTHON:-python3.12}
if [ "${1:-}" = "--delivery-launchers" ]; then
  shift
  exec "$PYTHON" -B "$ROOT/scripts/install-delivery-launchers.py" "$@"
fi
FORCE=false
if [ "${1:-}" = "--force" ]; then FORCE=true; shift; fi
if [ "$#" -gt 2 ]; then
  echo "Usage: install.sh [--force] [skills-dir] [codex-home]" >&2
  exit 2
fi
SKILLS=${1:-"${HOME}/.agents/skills"}
CODEX_DIR=${2:-"${CODEX_HOME:-${HOME}/.codex}"}
backup=$("$PYTHON" -B "$ROOT/scripts/install-rollback.py" capture "$ROOT" "$SKILLS" "$CODEX_DIR")
if "$PYTHON" -B "$ROOT/scripts/install-service-entry.py" "$SKILLS" "$CODEX_DIR" "$FORCE" "$backup"; then
  "$PYTHON" -B "$ROOT/scripts/install-rollback.py" discard "$backup"
else
  result=$?
  if [ "$result" -eq 3 ]; then
    "$PYTHON" -B "$ROOT/scripts/install-rollback.py" restore-checkout "$backup"
  else
    "$PYTHON" -B "$ROOT/scripts/install-rollback.py" restore "$backup"
  fi
  exit 1
fi
