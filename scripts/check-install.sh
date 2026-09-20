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
for agent in "$source_root"/agents/*.toml; do
    test "$(readlink "$install_fixture/codex/agents/${agent##*/}")" = "$agent"
done
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
    assert (agents[name]["model"], agents[name]["model_reasoning_effort"]) == ("gpt-5.6-sol", effort), name
PY
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
    test "$(readlink "$install_fixture/codex/agents/${agent##*/}")" = "$agent"
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
git -C "$release_source" init -q
git -C "$release_source" config user.name 'Installation smoke'
git -C "$release_source" config user.email 'install@example.invalid'
git -C "$release_source" add .
git -C "$release_source" commit -qm 'Initial installation'
git -C "$release_source" tag v0.0.1
git -C "$release_source" rm -qr skills/devflow-retired agents/devflow-retired.toml
git -C "$release_source" commit -qm 'Remove retired skill'
git -C "$release_source" tag v0.0.2
git clone -q "$release_source" "$install_fixture/checkout"
git -C "$install_fixture/checkout" checkout -q --detach v0.0.1
sh "$install_fixture/checkout/scripts/install.sh" "$install_fixture/upgraded-skills" "$install_fixture/upgraded-codex" > /dev/null
test -L "$install_fixture/upgraded-skills/devflow-retired"
test -L "$install_fixture/upgraded-codex/agents/devflow-retired.toml"
sh "$install_fixture/checkout/scripts/update.sh" v0.0.2 "$install_fixture/upgraded-skills" "$install_fixture/upgraded-codex" > /dev/null
test "$(git -C "$install_fixture/checkout" rev-parse HEAD)" = "$(git -C "$release_source" rev-parse v0.0.2)"
test ! -L "$install_fixture/upgraded-skills/devflow-retired"
test ! -L "$install_fixture/upgraded-codex/agents/devflow-retired.toml"
test "$(readlink "$install_fixture/upgraded-codex/agents/devflow-implementer.toml")" = "$install_fixture/checkout/agents/devflow-implementer.toml"
test "$(readlink "$install_fixture/upgraded-skills/devflow")" = "$install_fixture/checkout/skills/devflow"
printf '\n# Local edit\n' >> "$install_fixture/checkout/scripts/install.sh"
if sh "$install_fixture/checkout/scripts/update.sh" v0.0.1 > /dev/null 2>&1; then
    printf 'Upgrade accepted tracked edits.\n' >&2
    exit 1
fi
test "$(git -C "$install_fixture/checkout" rev-parse HEAD)" = "$(git -C "$release_source" rev-parse v0.0.2)"

"$devflow_python" -B "$source_root/scripts/candidate.py" --prepare-only "$install_fixture/trial" > /dev/null
test -f "$install_fixture/trial/codex/skills/devflow/SKILL.md"
test -L "$install_fixture/trial/codex/agents/devflow-implementer.toml"
test -L "$install_fixture/trial/codex/agents/devflow-coordinator.toml"
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
