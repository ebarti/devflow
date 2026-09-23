#!/bin/sh
# The installation smoke check runs entirely in a temporary directory.
set -eu
# Fixture repositories must not inherit the developer's global Git configuration (signing, hooks, templates).
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1

source_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
install_fixture=$(mktemp -d)
install_fixture=$(CDPATH= cd -- "$install_fixture" && pwd -P)
trap 'rm -rf "$install_fixture"' EXIT HUP INT TERM
destination="$install_fixture/skills"
devflow_python=${DEVFLOW_PYTHON:-python3.12}

sh "$source_root/scripts/install.sh" "$destination" "$install_fixture/codex"
for name in devflow devflow-defining-work devflow-planning \
    devflow-coordinating devflow-implementing devflow-reviewing devflow-verifying \
    devflow-merging; do
    test -f "$destination/$name/SKILL.md"
done
for name in state.py github.py legacy.py telemetry.py measurements.py schema.sql; do
    test -r "$destination/devflow/scripts/$name"
done
test -r "$source_root/scripts/install-guard.py"
test -r "$install_fixture/codex/.devflow-install.json"
test -r "$install_fixture/codex/.devflow-hook.py"
for agent in "$source_root"/agents/*.toml; do
    installed="$install_fixture/codex/agents/${agent##*/}"
    test -f "$installed" && test ! -L "$installed"
    cmp "$agent" "$installed"
done
test -f "$install_fixture/codex/agents/.devflow-agent-manifest.json"
"$devflow_python" -B - "$install_fixture/codex/agents" <<'PY'
import pathlib
import re
import sys
import tomllib

agents = {}
for path in sorted(pathlib.Path(sys.argv[1]).glob("devflow-*.toml")):
    with path.open("rb") as handle:
        agent = tomllib.load(handle)
    assert agent["name"] == path.stem, path
    assert agent["description"] and agent["developer_instructions"].strip(), path
    if agent["name"] == "devflow-coordinator":
        # Record/tracker access inherits the main task's existing permissions.
        assert "sandbox_mode" not in agent and "default_permissions" not in agent, path
    else:
        assert agent["sandbox_mode"] in {"read-only", "workspace-write"}, path
        # Leaves return reports; the execution coordinator records them.
        assert not re.search(r"state helper|state\.py|github\.py", agent["developer_instructions"]), path
    agents[agent["name"]] = agent
assert set(agents) == {"devflow-coordinator", "devflow-implementer",
                       "devflow-reviewer", "devflow-verifier"}, sorted(agents)
# The workflow defines every agent's default model and effort; a project overrides with its own file.
assert all(agent.get("model") and agent.get("model_reasoning_effort") for agent in agents.values()), sorted(agents)
for name, effort in {"devflow-coordinator": "high", "devflow-implementer": "xhigh",
                     "devflow-reviewer": "xhigh", "devflow-verifier": "xhigh"}.items():
    assert (agents[name]["model"], agents[name]["model_reasoning_effort"]) == ("gpt-6-sol", effort), name
PY
"$devflow_python" -B "$destination/devflow/scripts/state.py" --help > /dev/null
"$devflow_python" -B "$destination/devflow/scripts/github.py" --help > /dev/null
"$devflow_python" -B "$destination/devflow/scripts/telemetry.py" --help > /dev/null
test -s "$install_fixture/codex/hooks.json"

# Existing native-role workaround: exact regular agent copies are preserved.
copy_home="$install_fixture/regular-copies"
mkdir -p "$copy_home/codex/agents"
for agent in "$source_root"/agents/*.toml; do
    cp "$agent" "$copy_home/codex/agents/${agent##*/}"
done
printf 'unrelated\n' > "$copy_home/codex/agents/custom-agent.toml"
sh "$source_root/scripts/install.sh" "$copy_home/skills" "$copy_home/codex" > /dev/null
cp "$copy_home/codex/agents/.devflow-agent-manifest.json" "$install_fixture/copy-manifest-before.json"
sh "$source_root/scripts/install.sh" --force "$copy_home/skills" "$copy_home/codex" > /dev/null
cmp "$install_fixture/copy-manifest-before.json" "$copy_home/codex/agents/.devflow-agent-manifest.json"
for agent in "$source_root"/agents/*.toml; do
    target="$copy_home/codex/agents/${agent##*/}"
    test -f "$target" && test ! -L "$target"
    cmp "$agent" "$target"
done
test "$(cat "$copy_home/codex/agents/custom-agent.toml")" = unrelated
"$devflow_python" -B "$copy_home/codex/.devflow-hook.py" --check > /dev/null
# A stale recorded hash cannot authorize replacing a current matching copy.
"$devflow_python" -B - "$copy_home/codex/agents/.devflow-agent-manifest.json" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
manifest = json.loads(path.read_text())
manifest["agents"]["devflow-implementer.toml"]["sha256"] = "0" * 64
path.write_text(json.dumps(manifest))
PY
sh "$source_root/scripts/install.sh" "$copy_home/skills" "$copy_home/codex" > /dev/null
cmp "$source_root/agents/devflow-implementer.toml" "$copy_home/codex/agents/devflow-implementer.toml"
"$devflow_python" -B - "$copy_home/codex/agents/.devflow-agent-manifest.json" "$source_root/agents/devflow-implementer.toml" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

manifest = json.loads(Path(sys.argv[1]).read_text())
assert manifest["agents"]["devflow-implementer.toml"]["sha256"] == hashlib.sha256(Path(sys.argv[2]).read_bytes()).hexdigest()
PY
# Prior owned agent symlinks migrate to readable regular copies.
rm "$copy_home/codex/agents/devflow-coordinator.toml"
ln -s "$source_root/agents/devflow-coordinator.toml" "$copy_home/codex/agents/devflow-coordinator.toml"
sh "$source_root/scripts/install.sh" "$copy_home/skills" "$copy_home/codex" > /dev/null
test -f "$copy_home/codex/agents/devflow-coordinator.toml"
test ! -L "$copy_home/codex/agents/devflow-coordinator.toml"
cp "$copy_home/codex/hooks.json" "$install_fixture/copy-hooks-before.json"
printf '\n# Drifted copy\n' >> "$copy_home/codex/agents/devflow-implementer.toml"
if "$devflow_python" -B "$copy_home/codex/.devflow-hook.py" --check > /dev/null; then
    printf 'Drifted regular agent copy went undetected.\n' >&2
    exit 1
fi
if sh "$source_root/scripts/install.sh" --force "$copy_home/skills" "$copy_home/codex" > /dev/null 2>&1; then
    printf 'Differing regular agent copy was overwritten with --force.\n' >&2
    exit 1
fi
cmp "$install_fixture/copy-hooks-before.json" "$copy_home/codex/hooks.json"
test ! -L "$copy_home/codex/agents/devflow-implementer.toml"
test "$(cat "$copy_home/codex/agents/custom-agent.toml")" = unrelated

# A differing copy in a fresh install fails before creating any skill links or hooks.
fresh_conflict="$install_fixture/regular-conflict"
mkdir -p "$fresh_conflict/codex/agents"
printf 'custom definition\n' > "$fresh_conflict/codex/agents/devflow-implementer.toml"
if sh "$source_root/scripts/install.sh" "$fresh_conflict/skills" "$fresh_conflict/codex" > /dev/null 2>&1; then
    printf 'Fresh differing regular agent copy was accepted.\n' >&2
    exit 1
fi
test ! -e "$fresh_conflict/skills"
test ! -e "$fresh_conflict/codex/hooks.json"
test "$(cat "$fresh_conflict/codex/agents/devflow-implementer.toml")" = 'custom definition'

# An unrelated hook with the Devflow marker is a conflict, before any destination mutation.
hook_conflict="$install_fixture/hook-conflict"
mkdir -p "$hook_conflict/codex"
printf '%s\n' '{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"custom hook","statusMessage":"Record Devflow metrics"}]}]}}' > "$hook_conflict/codex/hooks.json"
cp "$hook_conflict/codex/hooks.json" "$install_fixture/hook-conflict-before.json"
if sh "$source_root/scripts/install.sh" --force "$hook_conflict/skills" "$hook_conflict/codex" > /dev/null 2>&1; then
    printf 'Unowned metric hook was replaced.\n' >&2
    exit 1
fi
test ! -e "$hook_conflict/skills"
test ! -e "$hook_conflict/codex/agents"
test ! -e "$hook_conflict/codex/.devflow-install.json"
cmp "$install_fixture/hook-conflict-before.json" "$hook_conflict/codex/hooks.json"

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
ln -s "$source_root/skills/devflow-delivering" "$destination/devflow-delivering"
ln -s "$install_fixture/unrelated" "$destination/unrelated"
ln -s "$source_root/agents/devflow-obsolete-install-smoke.toml" "$install_fixture/codex/agents/devflow-obsolete-install-smoke.toml"
ln -s "$source_root/agents/devflow-deliverer.toml" "$install_fixture/codex/agents/devflow-deliverer.toml"
ln -s "$source_root/agents/devflow-definer.toml" "$install_fixture/codex/agents/devflow-definer.toml"
ln -s "$source_root/agents/devflow-planner.toml" "$install_fixture/codex/agents/devflow-planner.toml"
ln -s "$install_fixture/unrelated" "$install_fixture/codex/agents/unrelated.toml"
ln -s "$("$devflow_python" -c 'import sys; print(sys.executable)')" "$install_fixture/python override"
DEVFLOW_PYTHON="$install_fixture/python override" sh "$source_root/scripts/install.sh" --force "$destination" "$install_fixture/codex"
for skill in "$source_root"/skills/*; do
    test "$(readlink "$destination/${skill##*/}")" = "$skill"
done
test ! -e "$install_fixture/previous/devflow"
cmp "$install_fixture/hooks-before.json" "$install_fixture/codex/hooks.json"
test ! -L "$destination/devflow-obsolete-install-smoke"
test ! -L "$destination/devflow-delivering"
test -L "$destination/unrelated"
for agent in "$source_root"/agents/*; do
    installed="$install_fixture/codex/agents/${agent##*/}"
    test -f "$installed" && test ! -L "$installed"
    cmp "$agent" "$installed"
done
test ! -L "$install_fixture/codex/agents/devflow-obsolete-install-smoke.toml"
test ! -L "$install_fixture/codex/agents/devflow-deliverer.toml"
test ! -L "$install_fixture/codex/agents/devflow-definer.toml"
test ! -L "$install_fixture/codex/agents/devflow-planner.toml"
test -L "$install_fixture/codex/agents/unrelated.toml"

# A file or directory blocks the whole reinstall before any link is changed.
mkdir "$install_fixture/conflicts"
ln -s "$install_fixture/previous" "$install_fixture/conflicts/devflow"
for kind in file directory; do
    conflict="$install_fixture/conflicts/devflow-merging"
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
cp -R "$source_root/scripts" "$source_root/skills" "$source_root/agents" "$release_source/"
mkdir "$release_source/skills/devflow-retired"
printf '%s\n' 'Temporary installation smoke skill.' > "$release_source/skills/devflow-retired/SKILL.md"
printf '%s\n' 'name = "devflow-retired"' > "$release_source/agents/devflow-retired.toml"
printf '%s\n' 'name = "devflow-modified-retired"' > "$release_source/agents/devflow-modified-retired.toml"
git -C "$release_source" init -q
git -C "$release_source" config user.name 'Installation smoke'
git -C "$release_source" config user.email 'install@example.invalid'
git -C "$release_source" add .
git -C "$release_source" commit -qm 'Initial installation'
git -C "$release_source" tag v0.0.1
git -C "$release_source" rm -qr skills/devflow-retired agents/devflow-retired.toml agents/devflow-modified-retired.toml
printf '\n# Version 2\n' >> "$release_source/agents/devflow-implementer.toml"
git -C "$release_source" add agents/devflow-implementer.toml
git -C "$release_source" commit -qm 'Remove retired skill'
git -C "$release_source" tag v0.0.2
git clone -q "$release_source" "$install_fixture/checkout"
git -C "$install_fixture/checkout" checkout -q --detach v0.0.1
sh "$install_fixture/checkout/scripts/install.sh" "$install_fixture/upgraded-skills" "$install_fixture/upgraded-codex" > /dev/null
test -L "$install_fixture/upgraded-skills/devflow-retired"
test -f "$install_fixture/upgraded-codex/agents/devflow-retired.toml"
test ! -L "$install_fixture/upgraded-codex/agents/devflow-retired.toml"
"$devflow_python" -B "$install_fixture/upgraded-codex/.devflow-hook.py" --check > /dev/null
printf '\n# User edit\n' >> "$install_fixture/upgraded-codex/agents/devflow-modified-retired.toml"
git -C "$install_fixture/checkout" checkout -q --detach v0.0.2
if "$devflow_python" -B "$install_fixture/upgraded-codex/.devflow-hook.py" --check > /dev/null; then
    printf 'Mutable checkout drift went undetected.\n' >&2
    exit 1
fi
"$devflow_python" -B - "$install_fixture/upgraded-codex/.devflow-hook.py" <<'PY'
import json
import subprocess
import sys

result = subprocess.run([sys.executable, "-B", sys.argv[1]], input="{}", text=True,
                        capture_output=True, check=True)
assert "installation drift" in json.loads(result.stdout)["systemMessage"]
PY
git -C "$install_fixture/checkout" checkout -q --detach v0.0.1
sh "$install_fixture/checkout/scripts/update.sh" v0.0.2 "$install_fixture/upgraded-skills" "$install_fixture/upgraded-codex" > /dev/null
"$devflow_python" -B "$install_fixture/upgraded-codex/.devflow-hook.py" --check > /dev/null
test "$(git -C "$install_fixture/checkout" rev-parse HEAD)" = "$(git -C "$release_source" rev-parse v0.0.2)"
test ! -L "$install_fixture/upgraded-skills/devflow-retired"
test ! -L "$install_fixture/upgraded-codex/agents/devflow-retired.toml"
test ! -e "$install_fixture/upgraded-codex/agents/devflow-retired.toml"
test -f "$install_fixture/upgraded-codex/agents/devflow-modified-retired.toml"
test "$(tail -1 "$install_fixture/upgraded-codex/agents/devflow-modified-retired.toml")" = '# User edit'
test -f "$install_fixture/upgraded-codex/agents/devflow-implementer.toml"
test ! -L "$install_fixture/upgraded-codex/agents/devflow-implementer.toml"
cmp "$install_fixture/checkout/agents/devflow-implementer.toml" "$install_fixture/upgraded-codex/agents/devflow-implementer.toml"
test "$(readlink "$install_fixture/upgraded-skills/devflow")" = "$install_fixture/checkout/skills/devflow"
printf '\n# Local edit\n' >> "$install_fixture/checkout/scripts/install.sh"
if sh "$install_fixture/checkout/scripts/update.sh" v0.0.1 > /dev/null 2>&1; then
    printf 'Upgrade accepted tracked edits.\n' >&2
    exit 1
fi
test "$(git -C "$install_fixture/checkout" rev-parse HEAD)" = "$(git -C "$release_source" rev-parse v0.0.2)"

"$devflow_python" -B "$source_root/scripts/candidate.py" --prepare-only "$install_fixture/trial" > /dev/null
test -f "$install_fixture/trial/codex/skills/devflow/SKILL.md"
test -f "$install_fixture/trial/codex/agents/devflow-implementer.toml"
test ! -L "$install_fixture/trial/codex/agents/devflow-implementer.toml"
test -f "$install_fixture/trial/codex/agents/devflow-coordinator.toml"
test ! -L "$install_fixture/trial/codex/agents/devflow-coordinator.toml"
"$devflow_python" -B - "$install_fixture/trial/codex/config.toml" <<'PY'
import sys
import tomllib

with open(sys.argv[1], "rb") as handle:
    config = tomllib.load(handle)
assert (config["model"], config["model_reasoning_effort"]) == ("gpt-6-astra", "xhigh")
PY
test -s "$install_fixture/trial/codex/hooks.json"
test -s "$install_fixture/trial/candidate.json"
printf 'Installation check passed.\n'
