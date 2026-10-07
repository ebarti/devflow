#!/usr/bin/env python3.12
"""Preflight and retire unchanged registrations from the previous installers."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import shlex
import stat
import subprocess
from pathlib import Path

V022 = "e966cf89e057abc9a2629faf957a2ec175599b53"
EVENTS = {"SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse",
          "PermissionRequest", "PreCompact", "PostCompact", "SubagentStart",
          "SubagentStop", "Stop", "Interrupt", "SessionEnd"}


def reject(path, reason):
    raise ValueError(f"Previous install preserved: {path}: {reason}; restore the recorded "
                     "bytes/pointer or relocate this conflicting registration, then rerun install.sh")


def owned(path, guard, *, link=False):
    try:
        if link:
            guard.path(str(path.parent))
            info = path.lstat()
            if not stat.S_ISLNK(info.st_mode) or info.st_uid != os.getuid():
                reject(path, "not an owned registration symlink")
            return os.readlink(path)
        return guard.read(path)
    except (OSError, ValueError) as exc:
        reject(path, str(exc))


def inventory(source, revision):
    if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
        reject(source, "invalid historical source revision")
    command = ["git", "-C", str(source)]
    if subprocess.run([*command, "merge-base", "--is-ancestor", revision, "HEAD"],
                      capture_output=True, check=False).returncode:
        reject(source, "historical revision is not in this checkout's ancestry")
    result = {}
    entries = subprocess.check_output([*command, "ls-tree", "-rz", revision,
                                       "--", "skills", "agents", "scripts"])
    for entry in entries.split(b"\0"):
        if not entry:
            continue
        metadata, name = entry.split(b"\t", 1)
        mode, kind, oid = metadata.decode().split()
        name = name.decode()
        if mode not in {"100644", "100755"} or kind != "blob":
            reject(source / name, "historical inventory contains a non-regular source")
        raw = subprocess.check_output([*command, "cat-file", "blob", oid])
        result[name] = hashlib.sha256(raw).hexdigest()
    return result


def plan(source, skills, home, guard):
    pin_path, hook_path = home / ".devflow-install.json", home / ".devflow-hook.py"
    hooks_path = home / "hooks.json"
    direct = [skills / item.name for item in (source / "skills").iterdir()
              if os.path.lexists(skills / item.name / "SKILL.md")]
    if not direct and not any(os.path.lexists(p) for p in (pin_path, hook_path)):
        return None
    for folder in (source, skills, home):
        try:
            guard.directory(folder)
        except (ValueError, OSError) as exc:
            reject(folder, str(exc))
    pin = None
    if os.path.lexists(pin_path):
        try:
            pin = json.loads(owned(pin_path, guard))
        except (ValueError, TypeError) as exc:
            reject(pin_path, f"invalid installation pin ({exc})")
        if (not isinstance(pin, dict) or pin.get("source") != str(source)
                or pin.get("skills") != str(skills) or not isinstance(pin.get("files"), dict)):
            reject(pin_path, "pin does not own this source and skills destination")
        expected = inventory(source, pin.get("head"))
        if pin["files"] != expected:
            reject(pin_path, "recorded hashes differ from the historical Git inventory")
        raw = owned(hook_path, guard)
        if hashlib.sha256(raw).hexdigest() != expected.get("scripts/install-guard.py"):
            reject(hook_path, "installed hook guard was modified")
        hook_target = str(hook_path)
    else:
        if os.path.lexists(hook_path):
            reject(hook_path, "hook guard has no ownership pin")
        # Published v0.2.2 predates pins/manifests. Only its exact tracked
        # inventory and generated telemetry registrations establish this case.
        expected = inventory(source, V022)
        hook_target = str(skills / "devflow/scripts/telemetry.py")
    manifest = home / "agents/.devflow-agent-manifest.json"
    if os.path.lexists(manifest):
        owned(manifest, guard)
    for name in expected:
        if name.startswith("agents/") and os.path.lexists(home / name):
            owned(home / name, guard, link=(home / name).is_symlink())
    links = []
    for name in sorted(expected):
        parts = Path(name).parts
        if len(parts) != 3 or parts[0] != "skills" or parts[2] != "SKILL.md":
            continue
        target = skills / parts[1]
        if os.path.lexists(target):
            if owned(target, guard, link=True) != str(source / "skills" / parts[1]):
                reject(target, "skill pointer differs from its recorded source")
            links.append(target)
    for target in direct:
        if target not in links:
            reject(target, "direct skill is not in the previous installation inventory")
    # A checkout switched by update.sh legitimately has new source bytes.
    # Verify against its current Git objects instead of the old installation pin.
    current = inventory(source, subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip())
    for name, digest in current.items():
        if name.startswith(("skills/", "agents/")):
            if hashlib.sha256(owned(source / name, guard)).hexdigest() != digest:
                reject(source / name, "source file differs from the current Git revision")
    if not os.path.lexists(hooks_path):
        reject(hooks_path, "previous metrics hook registrations are missing")
    raw = owned(hooks_path, guard)
    try:
        hooks = json.loads(raw)
        updated = copy.deepcopy(hooks)
        groups_by_event = updated["hooks"]
        if not isinstance(groups_by_event, dict):
            raise ValueError("hooks must be an object")
        removed = set()
        interpreter = None
        for event, groups in groups_by_event.items():
            if not isinstance(groups, list):
                raise ValueError("hook groups must be arrays")
            retained_groups = []
            for group in groups:
                changed = False
                items = group.get("hooks", [])
                if not isinstance(items, list):
                    raise ValueError("hook items must be arrays")
                retained = []
                for item in items:
                    command_text = item.get("command", "")
                    try:
                        words = shlex.split(command_text) if isinstance(command_text, str) else []
                    except ValueError:
                        words = []
                    refers_to_owned = isinstance(command_text, str) and (
                        hook_target in command_text or any(
                            os.path.normpath(word) == hook_target for word in words))
                    if item.get("statusMessage") != "Record Devflow metrics":
                        if refers_to_owned:
                            reject(hooks_path, f"modified registration refers to the owned hook in {event}")
                        retained.append(item)
                        continue
                    command = shlex.split(command_text)
                    suffix = ["-B", hook_target] + ([] if pin else ["hook"])
                    if (event not in EVENTS or set(group) != {"hooks"} or len(items) != 1
                            or set(item) != {
                            "type", "command", "timeout", "statusMessage"}
                            or item["type"] != "command" or type(item["timeout"]) is not int
                            or item["timeout"] != 3
                            or len(command) != len(suffix) + 1
                            or not Path(command[0]).is_absolute()
                            or str(Path(command[0])) != command[0]
                            or os.path.normpath(command[0]) != command[0]
                            or command_text != shlex.join(command)
                            or (interpreter is not None and command[0] != interpreter)
                            or command[1:] != suffix
                            or event in removed):
                        reject(hooks_path, f"modified or ambiguous Devflow hook in {event}")
                    interpreter = command[0]
                    removed.add(event)
                    changed = True
                if changed:
                    group["hooks"] = retained
                if not changed or retained or set(group) != {"hooks"}:
                    retained_groups.append(group)
            groups[:] = retained_groups
        if removed != EVENTS:
            reject(hooks_path, "previous generated hook inventory is incomplete")
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        reject(hooks_path, str(exc))
    helper = skills / ".devflow-helpers"
    if skills / "devflow" in links and os.path.lexists(helper):
        reject(helper, "script-only migration destination already exists")
    return {"links": links, "pin": pin_path if pin else None, "hook": hook_path if pin else None,
            "hooks": hooks_path, "raw": raw, "updated": updated, "helper": helper}


def apply(migration, source, skills, agents, rollback, backup):
    if migration is None:
        return
    compatibility = skills / "devflow"
    if compatibility in migration["links"]:
        helper = migration["helper"]
        helper.mkdir(mode=0o700)
        rollback.created(backup, helper)
        for child in (source / "skills/devflow").iterdir():
            if child.name not in {"SKILL.md", "__pycache__"}:
                (helper / child.name).symlink_to(child)
                rollback.created(backup, helper / child.name)
        stage = skills / (".devflow-helper-pointer-" + str(os.getpid()))
        stage.symlink_to(helper)
        try:
            rollback.effect(backup, compatibility, stage)
        finally:
            stage.unlink(missing_ok=True)
    for target in migration["links"]:
        if target != compatibility:
            rollback.effect(backup, target)
    updated = (json.dumps(migration["updated"], indent=2) + "\n").encode()
    if json.loads(migration["raw"]) != migration["updated"]:
        mode = stat.S_IMODE(migration["hooks"].stat().st_mode)
        agents.ROLLBACK = backup
        agents.atomic_write(migration["hooks"], updated, mode)
    for key in ("hook", "pin"):
        if migration[key]:
            rollback.effect(backup, migration[key])
