#!/usr/bin/env python3.12
"""Preflight and install loadable regular agent definitions without replacing custom files."""
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import sys
import tempfile


def fail(message):
    raise ValueError(message)


def exists(path):
    return os.path.lexists(path)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path, description):
    if path.is_symlink() or not path.is_file():
        fail(f"{description} is not a regular file: {path}")
    try:
        return json.loads(path.read_text())
    except (OSError, UnicodeError, ValueError) as exc:
        fail(f"Cannot read {description} {path}: {exc}")


def link_target(path):
    return Path(os.path.abspath(path.parent / os.readlink(path)))


def check_skills_destination(raw_path):
    target = Path(os.path.abspath(os.path.expanduser(raw_path)))
    for path in reversed((target, *target.parents)):
        if exists(path) and not path.is_dir():
            fail(f"Skills destination has a non-directory component: {path}")
    ancestor = target
    while not exists(ancestor):
        ancestor = ancestor.parent
    if not os.access(ancestor, os.W_OK | os.X_OK):
        fail(f"Skills destination cannot be created or changed: {ancestor}")


def manifest_for(path, source_root):
    if not exists(path):
        return {"schema_version": 1, "source_root": str(source_root), "agents": {}}
    saved = read_json(path, "agent ownership manifest")
    if (not isinstance(saved, dict) or saved.get("schema_version") != 1
            or not isinstance(saved.get("source_root"), str)
            or not Path(saved["source_root"]).is_absolute()
            or not isinstance(saved.get("agents"), dict)):
        fail(f"Unsupported agent ownership manifest: {path}")
    for name, entry in saved["agents"].items():
        source = Path(saved["source_root"]) / "agents" / name
        if (not isinstance(name, str) or Path(name).name != name
                or not name.startswith("devflow-") or not name.endswith(".toml")
                or not isinstance(entry, dict) or entry.get("source") != str(source)
                or not isinstance(entry.get("sha256"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", entry["sha256"])):
            fail(f"Invalid agent entry {name!r} in {path}")
    return saved


def hook_pin(codex_home, source_root, force):
    pin_path = codex_home / ".devflow-install.json"
    guard_path = codex_home / ".devflow-hook.py"
    pin = None
    if exists(pin_path):
        pin = read_json(pin_path, "Devflow installation pin")
        if (not isinstance(pin, dict) or not isinstance(pin.get("source"), str)
                or not Path(pin["source"]).is_absolute()
                or not isinstance(pin.get("files"), dict)
                or not isinstance(pin["files"].get("scripts/install-guard.py"), str)):
            fail(f"Invalid Devflow installation pin: {pin_path}")
        if pin["source"] != str(source_root) and not force:
            fail("Devflow hook belongs to another checkout; use --force after checking its source")
        if not exists(guard_path) or guard_path.is_symlink() or not guard_path.is_file():
            fail(f"Installed Devflow hook guard is missing or changed: {guard_path}")
        if digest(guard_path) != pin["files"]["scripts/install-guard.py"]:
            fail(f"Installed Devflow hook guard was modified: {guard_path}")
    elif exists(guard_path):
        fail(f"Unowned Devflow hook guard preserved: {guard_path}")

    hooks_path = codex_home / "hooks.json"
    if exists(hooks_path):
        hooks = read_json(hooks_path, "hooks configuration")
        if not isinstance(hooks, dict) or not isinstance(hooks.get("hooks", {}), dict):
            fail(f"Invalid hooks configuration: {hooks_path}")
        for groups in hooks.get("hooks", {}).values():
            if not isinstance(groups, list):
                fail(f"Invalid hooks configuration: {hooks_path}")
            for group in groups:
                if not isinstance(group, dict) or not isinstance(group.get("hooks", []), list):
                    fail(f"Invalid hooks configuration: {hooks_path}")
                for item in group.get("hooks", []):
                    if not isinstance(item, dict):
                        fail(f"Invalid hooks configuration: {hooks_path}")
                    if item.get("statusMessage") != "Record Devflow metrics":
                        continue
                    try:
                        command = shlex.split(item["command"])
                    except (KeyError, TypeError, ValueError):
                        fail(f"Unrecognized Devflow hook command in {hooks_path}")
                    if (item.get("type") == "command" and len(command) == 3
                            and Path(command[0]).is_absolute() and command[1] == "-B"
                            and Path(command[2]).resolve() == guard_path.resolve() and pin):
                        continue
                    if (item.get("type") == "command" and len(command) == 4
                            and Path(command[0]).is_absolute() and command[1] == "-B"
                            and command[-1] == "hook"
                            and command[-2].endswith("/skills/devflow/scripts/telemetry.py")):
                        previous = str(Path(command[-2]).parents[3])
                        if previous == str(source_root) or force:
                            continue
                    fail(f"Unrecognized or differently pinned Devflow hook in {hooks_path}")
    return pin


def atomic_write(path, content, mode=0o644):
    descriptor, name = tempfile.mkstemp(prefix=".devflow-agent-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
        temporary.chmod(mode)
        os.replace(temporary, path)
    finally:
        if exists(temporary):
            temporary.unlink()


def plan(source_root, skills, codex_home, force):
    agent_dir = codex_home / "agents"
    for label, path in (("Codex home", codex_home), ("Agent directory", agent_dir)):
        if exists(path) and not path.is_dir():
            fail(f"{label} is not a directory: {path}")
    sources = {path.name: path for path in (source_root / "agents").glob("devflow-*.toml")}
    if not sources:
        fail(f"No bundled agent definitions in {source_root / 'agents'}")
    if any(not path.is_file() or path.is_symlink() for path in sources.values()):
        fail("Agent sources must be regular files")
    hashes = {name: digest(path) for name, path in sources.items()}
    manifest_path = agent_dir / ".devflow-agent-manifest.json"
    manifest = manifest_for(manifest_path, source_root)
    pin = hook_pin(codex_home, source_root, force)
    if manifest["agents"] and manifest["source_root"] != str(source_root) and not force:
        fail("Agent copies belong to another checkout; use --force after checking its source")
    previous_roots = {str(source_root), manifest["source_root"]}
    if pin:
        previous_roots.add(pin["source"])
    skill_link = skills / "devflow"
    if skill_link.is_symlink() and link_target(skill_link).parts[-2:] == ("skills", "devflow"):
        previous_roots.add(str(link_target(skill_link).parents[1]))

    actions = {}
    for name, source in sources.items():
        target = agent_dir / name
        if not exists(target):
            actions[name] = "copy"
        elif target.is_symlink():
            target_path = link_target(target)
            owned_roots = {str(Path(root) / "agents" / name) for root in previous_roots}
            if str(target_path) not in owned_roots or (target_path != source and not force):
                fail(f"Unowned or differently pinned agent symlink preserved: {target}")
            actions[name] = "copy"
        elif target.is_file():
            actual = digest(target)
            recorded = (manifest["agents"].get(name) or {}).get("sha256")
            if actual == hashes[name]:
                actions[name] = "preserve"
            elif recorded and actual == recorded:
                actions[name] = "copy"
            else:
                fail(f"Modified or custom agent file preserved: {target}")
        else:
            fail(f"Existing agent path preserved: {target}")

    obsolete = []
    for name, entry in manifest["agents"].items():
        if name in sources:
            continue
        target = agent_dir / name
        if exists(target) and target.is_file() and not target.is_symlink() and digest(target) == entry["sha256"]:
            obsolete.append(target)
    for root in previous_roots:
        for target in agent_dir.glob("devflow-*.toml") if agent_dir.is_dir() else ():
            if target.name not in sources and target.is_symlink() and link_target(target) == Path(root) / "agents" / target.name:
                obsolete.append(target)
    return sources, hashes, actions, sorted(set(obsolete)), manifest_path


def main():
    if len(sys.argv) != 6 or sys.argv[1] not in {"preflight", "apply"}:
        fail("usage: install-agents.py preflight|apply SOURCE_ROOT SKILLS CODEX_HOME FORCE")
    mode, source_root, skills, codex_home, force = sys.argv[1:]
    check_skills_destination(skills)
    source_root, skills, codex_home = (Path(path).expanduser().resolve() for path in (source_root, skills, codex_home))
    sources, hashes, actions, obsolete, manifest_path = plan(source_root, skills, codex_home, force == "true")
    if mode == "preflight":
        return 0
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    for target in obsolete:
        target.unlink()
    for name, action in actions.items():
        if action == "copy":
            source = sources[name]
            atomic_write(manifest_path.parent / name, source.read_bytes(), source.stat().st_mode & 0o777)
    manifest = {"schema_version": 1, "source_root": str(source_root),
                "agents": {name: {"source": str(source), "sha256": hashes[name]}
                           for name, source in sorted(sources.items())}}
    content = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    if not exists(manifest_path) or manifest_path.read_bytes() != content:
        atomic_write(manifest_path, content)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError) as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1)
