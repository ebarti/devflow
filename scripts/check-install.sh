#!/bin/sh
# The installation smoke check runs entirely in a temporary directory.
set -eu

source_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
install_fixture=$(mktemp -d)
trap 'rm -rf "$install_fixture"' EXIT HUP INT TERM
destination="$install_fixture/skills"
devflow_python=${DEVFLOW_PYTHON:-python3.12}

sh "$source_root/scripts/install.sh" "$destination" "$install_fixture/codex"
for name in devflow devflow-defining-work devflow-planning \
    devflow-coordinating devflow-implementing devflow-reviewing devflow-verifying \
    devflow-delivering; do
    test -f "$destination/$name/SKILL.md"
done
for name in state.py github.py legacy.py telemetry.py measurements.py schema.sql; do
    test -r "$destination/devflow/scripts/$name"
done
"$devflow_python" -B "$destination/devflow/scripts/state.py" --help > /dev/null
"$devflow_python" -B "$destination/devflow/scripts/github.py" --help > /dev/null
"$devflow_python" -B "$destination/devflow/scripts/telemetry.py" --help > /dev/null
test -s "$install_fixture/codex/hooks.json"

# Installed hooks keep the installing interpreter even with an empty PATH.
"$devflow_python" -B - "$install_fixture/codex/hooks.json" <<'PY'
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys

hooks_path = Path(sys.argv[1])
config = json.loads(hooks_path.read_text())
command, = {hook["command"] for groups in config["hooks"].values()
            for group in groups for hook in group["hooks"]}
assert shlex.split(command)[0] == str(Path(sys.executable).resolve())
env = dict(os.environ, PATH="", XDG_STATE_HOME=str(hooks_path.parent / "state"))
result = subprocess.run(["/bin/sh", "-c", command], input="{}", text=True,
                        capture_output=True, check=True, env=env)
assert json.loads(result.stdout) == {}
PY

# Switching checkouts requires force, including when the old link is broken.
mkdir "$install_fixture/previous"
rm "$destination/devflow" "$destination/devflow-planning"
ln -s "$install_fixture/previous" "$destination/devflow"
ln -s "$install_fixture/missing" "$destination/devflow-planning"
if sh "$source_root/scripts/install.sh" "$destination" "$install_fixture/codex" > /dev/null 2>&1; then
    printf 'Conflicting symlinks were accepted without --force.\n' >&2
    exit 1
fi
test "$(readlink "$destination/devflow")" = "$install_fixture/previous"
cp "$install_fixture/codex/hooks.json" "$install_fixture/hooks-before.json"
ln -s "$("$devflow_python" -c 'import sys; print(sys.executable)')" "$install_fixture/python override"
DEVFLOW_PYTHON="$install_fixture/python override" sh "$source_root/scripts/install.sh" --force "$destination" "$install_fixture/codex"
for skill in "$source_root"/skills/*; do
    test "$(readlink "$destination/${skill##*/}")" = "$skill"
done
test ! -e "$install_fixture/previous/devflow"
cmp "$install_fixture/hooks-before.json" "$install_fixture/codex/hooks.json"

# A file or directory blocks the whole reinstall before any link is changed.
mkdir "$install_fixture/conflicts"
ln -s "$install_fixture/previous" "$install_fixture/conflicts/devflow"
for kind in file directory; do
    conflict="$install_fixture/conflicts/devflow-delivering"
    if [ "$kind" = file ]; then
        printf 'keep\n' > "$conflict"
    else
        rm "$conflict"
        mkdir "$conflict"
    fi
    if sh "$source_root/scripts/install.sh" --force "$install_fixture/conflicts" "$install_fixture/codex" > /dev/null 2>&1; then
        printf 'Conflicting %s was accepted with --force.\n' "$kind" >&2
        exit 1
    fi
    test "$(readlink "$install_fixture/conflicts/devflow")" = "$install_fixture/previous"
    if [ "$kind" = file ]; then test "$(cat "$conflict")" = keep; else test -d "$conflict"; fi
done
printf 'Installation check passed.\n'
