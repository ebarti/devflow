#!/bin/sh
# Link bundled skills and copy bundled agent definitions into the host directories.
set -eu

force=false
if [ "${1:-}" = "--force" ]; then
    force=true
    shift
fi
if [ "$#" -gt 2 ]; then
    printf 'Usage: %s [--force] [skills-directory] [codex-home]\n' "$0" >&2
    exit 2
fi

source_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
destination=${1:-"${HOME}/.agents/skills"}
codex_directory=${2:-"${CODEX_HOME:-${HOME}/.codex}"}
devflow_python=${DEVFLOW_PYTHON:-python3.12}

# Stop on any skill conflict before changing a link; force replaces only symlinks.
check_links() {
    for source in "$1"/*; do
        [ -e "$source" ] || continue
        target="$2/${source##*/}"
        if [ -e "$target" ] || [ -L "$target" ]; then
            if [ -L "$target" ]; then
                if [ "$force" = true ] || [ "$(readlink "$target")" = "$source" ]; then
                    continue
                fi
            fi
            printf 'Existing path preserved: %s\nUse --force for symlinks; relocate other conflicting paths.\n' "$target" >&2
            exit 1
        fi
    done
}

# Agent definitions are regular files because Codex cannot load these roles through
# symlinks reliably. The manifest proves which unchanged copies this installer owns.
manage_agents() {
    "$devflow_python" -B - "$1" "$source_root" "$codex_directory/agents" "$force" <<'PY'
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import sys
import tempfile

mode, source_root_arg, agent_directory_arg, force_arg = sys.argv[1:]
source_root = Path(source_root_arg)
source_directory = source_root / "agents"
agent_directory = Path(agent_directory_arg)
codex_directory = agent_directory.parent
manifest_path = agent_directory / ".devflow-agent-manifest.json"
force = force_arg == "true"


def fail(message):
    print(message, file=sys.stderr)
    raise SystemExit(1)


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def path_exists(path):
    return os.path.lexists(path)


def validate_destination_shape():
    for label, path in (("Codex home", codex_directory), ("Agent directory", agent_directory)):
        if path_exists(path) and not path.is_dir():
            fail(f"{label} path is not a directory: {path}")


def load_manifest():
    if not path_exists(manifest_path):
        return {"schema_version": 1, "source_root": str(source_root), "agents": {}}
    if manifest_path.is_symlink() or not manifest_path.is_file():
        fail(f"Agent ownership manifest is not a regular file: {manifest_path}")
    try:
        manifest = json.loads(manifest_path.read_text())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        fail(f"Cannot read agent ownership manifest {manifest_path}: {error}")
    if (not isinstance(manifest, dict) or manifest.get("schema_version") != 1
            or not isinstance(manifest.get("source_root"), str)
            or not isinstance(manifest.get("agents"), dict)):
        fail(f"Unsupported agent ownership manifest: {manifest_path}")
    recorded_root = Path(manifest["source_root"])
    if not recorded_root.is_absolute():
        fail(f"Invalid source_root in agent ownership manifest: {manifest_path}")
    for name, entry in manifest["agents"].items():
        expected_source = recorded_root / "agents" / name
        if (not isinstance(name, str) or Path(name).name != name
                or not name.startswith("devflow-") or not name.endswith(".toml")
                or not isinstance(entry, dict)
                or entry.get("source") != str(expected_source)
                or not isinstance(entry.get("sha256"), str)
                or re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]) is None):
            fail(f"Invalid agent entry {name!r} in ownership manifest: {manifest_path}")
    return manifest


def prepare_destination():
    temporary_paths = []
    try:
        agent_directory.mkdir(parents=True, exist_ok=True)
        for _ in range(2):
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=".devflow-agent-preflight.", dir=agent_directory
            )
            os.close(descriptor)
            temporary_paths.append(Path(temporary_name))
        os.replace(temporary_paths[0], temporary_paths[1])
    except OSError as error:
        fail(f"Cannot prepare agent directory {agent_directory}: {error}")
    finally:
        for path in temporary_paths:
            if path_exists(path):
                path.unlink()


def source_files():
    return sorted(
        path for path in source_directory.iterdir()
        if path.is_file() and not path.is_symlink() and path.name.startswith("devflow-")
        and path.suffix == ".toml"
    )


def preflight(manifest, sources):
    recorded_root = manifest["source_root"]
    if manifest["agents"] and recorded_root != str(source_root) and not force:
        fail(
            f"Agent copies belong to another Devflow checkout: {recorded_root}\n"
            "Use --force to migrate unchanged owned copies or agent symlinks."
        )
    for source in sources:
        target = agent_directory / source.name
        if not path_exists(target):
            continue
        if target.is_symlink():
            if force or os.readlink(target) == str(source):
                continue
            fail(
                f"Existing agent symlink preserved: {target}\n"
                "Use --force to migrate agent symlinks from another checkout."
            )
        if not target.is_file():
            fail(f"Existing agent path preserved: {target}\nRelocate the conflicting path.")
        entry = manifest["agents"].get(source.name)
        if entry is None:
            fail(
                f"Unmanaged agent file preserved: {target}\n"
                "Relocate it before installation; --force does not replace regular files."
            )
        if digest(target) != entry["sha256"]:
            fail(
                f"Modified managed agent file preserved: {target}\n"
                "Restore or relocate it before installation; --force does not replace local edits."
            )


def copy_agent(source, target):
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{source.name}.", dir=agent_directory)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as output, source.open("rb") as input_file:
            shutil.copyfileobj(input_file, output)
        temporary.chmod(stat.S_IMODE(source.stat().st_mode))
        os.replace(temporary, target)
    finally:
        if path_exists(temporary):
            temporary.unlink()


def write_manifest(manifest):
    content = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(prefix=".devflow-agent-manifest.", dir=agent_directory)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w") as handle:
            handle.write(content)
        temporary.chmod(0o644)
        os.replace(temporary, manifest_path)
    finally:
        if path_exists(temporary):
            temporary.unlink()


validate_destination_shape()
manifest = load_manifest()
sources = source_files()
preflight(manifest, sources)
if mode == "check":
    prepare_destination()
    raise SystemExit(0)
if mode != "apply":
    fail(f"Unknown agent installation mode: {mode}")

agent_directory.mkdir(parents=True, exist_ok=True)
current_names = {source.name for source in sources}
entries = dict(manifest["agents"])
for name, entry in list(entries.items()):
    if name in current_names:
        continue
    target = agent_directory / name
    if not path_exists(target):
        del entries[name]
    elif not target.is_symlink() and target.is_file() and digest(target) == entry["sha256"]:
        target.unlink()
        del entries[name]
    elif manifest["source_root"] != str(source_root):
        # A modified obsolete copy survives a checkout migration, but the new
        # checkout must not claim ownership of content it never installed.
        del entries[name]

for source in sources:
    target = agent_directory / source.name
    source_hash = digest(source)
    entry = manifest["agents"].get(source.name)
    unchanged_copy = (
        path_exists(target) and not target.is_symlink() and target.is_file()
        and entry is not None and digest(target) == source_hash
    )
    if not unchanged_copy:
        if target.is_symlink():
            target.unlink()
        copy_agent(source, target)
    entries[source.name] = {"source": str(source), "sha256": source_hash}

# Preserve the former link-pruning behavior for obsolete links from this checkout.
for target in agent_directory.iterdir():
    if not target.is_symlink():
        continue
    previous = source_directory / target.name
    if os.readlink(target) == str(previous) and not path_exists(previous):
        target.unlink()

write_manifest({
    "schema_version": 1,
    "source_root": str(source_root),
    "agents": entries,
})
PY
}

make_links() {
    mkdir -p "$2"
    for source in "$1"/*; do
        [ -e "$source" ] || continue
        target="$2/${source##*/}"
        if [ "$force" = true ] && [ -L "$target" ]; then
            ln -sfn "$source" "$target"
        elif [ ! -L "$target" ]; then
            ln -s "$source" "$target"
        fi
    done
}

# Remove only obsolete links owned by this checkout.
prune_links() {
    for target in "$2"/*; do
        [ -L "$target" ] || continue
        previous="$1/${target##*/}"
        if [ "$(readlink "$target")" = "$previous" ] && [ ! -e "$previous" ]; then
            unlink "$target"
        fi
    done
}

check_links "$source_root/skills" "$destination"
manage_agents check
make_links "$source_root/skills" "$destination"
manage_agents apply
"$devflow_python" -B "$destination/devflow/scripts/telemetry.py" install --codex-home "$codex_directory"
prune_links "$source_root/skills" "$destination"
printf 'Skills linked in %s\nAgent definitions copied into %s\nKeep this checkout at %s.\n' \
    "$destination" "$codex_directory/agents" "$source_root"
