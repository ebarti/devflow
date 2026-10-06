#!/bin/sh
# The public installer regressions create and remove only isolated temporary homes.
set -eu
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
source_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
cd "$source_root"
python=${DEVFLOW_PYTHON:-python3.12}
"$python" -B -m unittest discover -s tests -p 'test_service_entry_installation.py' -v
"$python" -B -m unittest discover -s tests -p 'test_upgrade_installation.py' -v
"$python" -B -m unittest discover -s tests -p 'test_delivery*.py' -v
