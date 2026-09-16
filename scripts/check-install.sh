#!/bin/sh
# The installation smoke check runs entirely in a temporary directory.
set -eu

source_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
install_fixture=$(mktemp -d)
install_fixture=$(CDPATH= cd -- "$install_fixture" && pwd -P)
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
ln -s "$source_root/skills/devflow-obsolete-install-smoke" "$destination/devflow-obsolete-install-smoke"
ln -s "$install_fixture/unrelated" "$destination/unrelated"
ln -s "$("$devflow_python" -c 'import sys; print(sys.executable)')" "$install_fixture/python override"
DEVFLOW_PYTHON="$install_fixture/python override" sh "$source_root/scripts/install.sh" --force "$destination" "$install_fixture/codex"
for skill in "$source_root"/skills/*; do
    test "$(readlink "$destination/${skill##*/}")" = "$skill"
done
test ! -e "$install_fixture/previous/devflow"
cmp "$install_fixture/hooks-before.json" "$install_fixture/codex/hooks.json"
test ! -L "$destination/devflow-obsolete-install-smoke"
test -L "$destination/unrelated"

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

# Upgrade a local installation between two release tags; no remote service is used.
release_source="$install_fixture/releases"
mkdir "$release_source"
cp -R "$source_root/scripts" "$source_root/skills" "$release_source/"
mkdir "$release_source/skills/devflow-retired"
printf '%s\n' 'Temporary installation smoke skill.' > "$release_source/skills/devflow-retired/SKILL.md"
git -C "$release_source" init -q
git -C "$release_source" config user.name 'Installation smoke'
git -C "$release_source" config user.email 'install@example.invalid'
git -C "$release_source" add .
git -C "$release_source" commit -qm 'Initial installation'
git -C "$release_source" tag v0.0.1
git -C "$release_source" rm -qr skills/devflow-retired
git -C "$release_source" commit -qm 'Remove retired skill'
git -C "$release_source" tag v0.0.2
git clone -q "$release_source" "$install_fixture/checkout"
git -C "$install_fixture/checkout" checkout -q --detach v0.0.1
sh "$install_fixture/checkout/scripts/install.sh" "$install_fixture/upgraded-skills" "$install_fixture/upgraded-codex" > /dev/null
test -L "$install_fixture/upgraded-skills/devflow-retired"
sh "$install_fixture/checkout/scripts/update.sh" v0.0.2 "$install_fixture/upgraded-skills" "$install_fixture/upgraded-codex" > /dev/null
test "$(git -C "$install_fixture/checkout" rev-parse HEAD)" = "$(git -C "$release_source" rev-parse v0.0.2)"
test ! -L "$install_fixture/upgraded-skills/devflow-retired"
test "$(readlink "$install_fixture/upgraded-skills/devflow")" = "$install_fixture/checkout/skills/devflow"
printf '\n# Local edit\n' >> "$install_fixture/checkout/scripts/install.sh"
if sh "$install_fixture/checkout/scripts/update.sh" v0.0.1 > /dev/null 2>&1; then
    printf 'Upgrade accepted tracked edits.\n' >&2
    exit 1
fi
test "$(git -C "$install_fixture/checkout" rev-parse HEAD)" = "$(git -C "$release_source" rev-parse v0.0.2)"

"$devflow_python" -B "$source_root/scripts/candidate.py" --prepare-only "$install_fixture/trial" > /dev/null
test -f "$install_fixture/trial/codex/skills/devflow/SKILL.md"
test -s "$install_fixture/trial/codex/hooks.json"
test -s "$install_fixture/trial/candidate.json"
printf 'Installation check passed.\n'
