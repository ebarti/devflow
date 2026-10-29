#!/bin/sh
# Adopt an independent production runtime, then register its service entry.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PYTHON=${DEVFLOW_PYTHON:-python3.12}
if [ "${1:-}" = "--release-state-check" ]; then
  shift
  unset TZ PYTHONPATH PYTHONOPTIMIZE
  exec "${DEVFLOW_PYTHON:-$ROOT/runtime/.venv/bin/python}" -B "$ROOT/scripts/release-state-validator.py" "$@"
fi
if [ "${1:-}" = "--delivery-launchers" ]; then
  shift
  exec "$PYTHON" -B "$ROOT/scripts/install-delivery-launchers.py" "$@"
fi
if [ "${1:-}" = "--development" ]; then
  shift
  export DEVFLOW_INSTALL_MODE=development
fi
if [ "${DEVFLOW_INSTALL_MODE:-production}" != "development" ]; then
  exec "$PYTHON" -B "$ROOT/scripts/install-production.py" "$@"
fi
if [ "$#" -lt 2 ]; then
  echo "Development installation requires explicit isolated skills-dir and codex-home." >&2
  exit 2
fi
FORCE=false
if [ "${1:-}" = "--force" ]; then FORCE=true; shift; fi
if [ "$#" -gt 2 ]; then
  echo "Usage: install.sh [--development] [--force] [skills-dir] [codex-home]" >&2
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
