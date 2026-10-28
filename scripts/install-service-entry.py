#!/usr/bin/env python3.12
"""Install one service skill and internal roles; preserve existing script-only helpers."""

from __future__ import annotations

import hashlib
import importlib.util
import os
import stat
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MUTATING = False


def load(name):
    spec = importlib.util.spec_from_file_location(
        name, ROOT / "scripts" / (name + ".py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def hashes(root, guard=None):
    result = {}
    for file in root.rglob("*"):
        if "__pycache__" in file.parts or file.suffix == ".pyc":
            continue
        if file.is_symlink():
            raise ValueError("helper source contains an alias: " + str(file))
        if not file.is_dir():
            raw = guard.read(file) if guard else file.read_bytes()
            result[str(file.relative_to(root))] = hashlib.sha256(raw).hexdigest()
    return result


def helper_upgrade(compatibility, source, guard, migration):
    """Authenticate historical/current Git bytes before exchanging one child link."""
    link = compatibility / "scripts"
    try:
        target = link.resolve(strict=True)
        if compatibility.is_symlink() and guard.link_state(compatibility) != {
            "type": "symlink",
            "target": str(compatibility.resolve(strict=True)),
        }:
            raise ValueError("compatibility root registration contains an alias")
        registered = compatibility.resolve(strict=True) / "scripts"
        guard.path(str(registered.parent))
        info = registered.lstat()
        actual, current = hashes(target, guard), hashes(source, guard)
        env = dict(os.environ, GIT_OPTIONAL_LOCKS="0")

        def git(root, *args):
            return subprocess.check_output(
                ["git", "-C", str(root), *args], env=env, text=True, stderr=subprocess.PIPE
            ).strip()

        prefix = "skills/devflow/scripts/"
        latest = migration.inventory(ROOT, git(ROOT, "rev-parse", "HEAD"))
        latest = {
            name[len(prefix) :]: digest
            for name, digest in latest.items()
            if name.startswith(prefix)
        }
        if current != latest:
            raise ValueError("current helper files differ from current Git inventory")
        if info.st_uid != os.getuid():
            raise ValueError("scripts registration is not owned")
        if stat.S_ISLNK(info.st_mode) and os.readlink(registered) != str(target):
            raise ValueError("scripts registration contains an alias")
        if actual == current:
            return None  # Keep already-compatible real paths used by old work.
        if not stat.S_ISLNK(info.st_mode):
            raise ValueError("scripts is not an owned canonical registration symlink")
        old = target.parents[2]
        if target != old / "skills/devflow/scripts":
            raise ValueError("scripts is not a historical source directory")
        guard.directory(old)
        reference = registered.parent / "references"
        if guard.link_state(reference) != {
            "type": "symlink",
            "target": str(old / "skills/devflow/references"),
        }:
            raise ValueError("references does not identify the same historical source")
        if git(old, "rev-parse", "--show-toplevel") != str(old):
            raise ValueError("historical helper source is not its own Git checkout")
        origin = git(ROOT, "config", "--get", "remote.origin.url")
        if not origin or git(old, "config", "--get", "remote.origin.url") != origin:
            raise ValueError("historical helper Git origin differs from current source")
        previous = migration.inventory(ROOT, git(old, "rev-parse", "HEAD"))
        previous = {
            name[len(prefix) :]: digest
            for name, digest in previous.items()
            if name.startswith(prefix)
        }
        if set(actual) != set(previous):
            difference = sorted(set(actual) ^ set(previous))
            raise ValueError(
                "historical helper file inventory changed: "
                + ", ".join(str(target / name) for name in difference)
            )
        # A prior repair may already match the exact current Git blob. Unknown
        # edits never qualify, and no historical source file is rewritten.
        for name, digest in actual.items():
            if digest not in (previous[name], latest.get(name)):
                raise ValueError("unrecognized helper bytes: " + str(target / name))
        return registered
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        raise ValueError(f"Retained helper preserved: {link}: {exc}") from exc


def plan(skills, home, force, *, migration_source=None, predecessor=None):
    """Read-only complete registration checks shared by preflight and apply."""
    agents = load("install-agents")
    rollback = load("install-rollback")
    rollback.exchange_function()
    guard = load("install-delivery-launchers")
    if migration_source is None:
        if not load("installation-paths").development(skills, home):
            raise ValueError("production registrations require scripts/install.sh adoption")
    agents.check_skills_destination(str(skills))
    migration_helper = load("install-migration")
    if predecessor is not None and (predecessor != migration_source or predecessor == ROOT):
        raise ValueError("agent predecessor must be the exact distinct source being migrated")
    migration = migration_helper.plan(migration_source or ROOT, skills, home, guard)
    if (predecessor is None and migration is not None and migration_source is not None
            and migration_source != ROOT):
        predecessor = migration_source
    predecessor_revision = (migration["revision"] if migration is not None
                            and migration_source is not None and migration_source != ROOT else None)
    _, _, actions, obsolete, _ = agents.plan(
        ROOT, skills, home, force, predecessor=predecessor,
        predecessor_revision=predecessor_revision)
    service = skills / "devflow-local-delivery"
    source = ROOT / "runtime/desktop/devflow-local-delivery"
    plugin = home / "plugins/cache/devflow-local/devflow"
    if plugin.exists():
        entries = list(plugin.glob("*/skills/devflow-local-delivery/SKILL.md"))
        if (
            len(entries) != 1
            or guard.read(entries[0]) != guard.read(source / "SKILL.md")
            or os.path.lexists(service)
        ):
            raise ValueError("existing service entry is ambiguous or changed")
    elif os.path.lexists(service) and guard.link_state(service) != {
        "type": "symlink",
        "target": str(source),
    }:
        if migration_source is None or guard.link_state(service) != {
            "type": "symlink", "target": str(migration_source / "runtime/desktop/devflow-local-delivery")
        }:
            raise ValueError("foreign service entry preserved")
        if hashes(service.resolve(), guard) != hashes(source, guard):
            raise ValueError("previous service source differs; use its supported installation migration")
    compatibility = skills / "devflow"
    children = [
        p
        for p in (ROOT / "skills/devflow").iterdir()
        if p.name not in {"SKILL.md", "__pycache__"}
    ]
    scripts_upgrade = None
    adopted = []
    if os.path.lexists(compatibility) and not (
        migration and compatibility in migration["links"]
    ):
        # Preserve the authentic old real paths used by already-running work.
        guard.directory(compatibility.resolve())
        if {p.name for p in compatibility.iterdir()} != {p.name for p in children}:
            raise ValueError("script-only helper directory has unexpected children")
        for child in children:
            target = (compatibility / child.name).resolve(strict=True)
            guard.path(str(target))
            guard.directory(target)
            registered = compatibility.resolve(strict=True) / child.name
            if registered.is_symlink() and guard.link_state(registered) != {
                "type": "symlink", "target": str(target),
            }:
                raise ValueError(
                    "retained child registration contains an alias: " + str(registered)
                )
            # Preserve old references and source; authenticate executable helpers
            # before changing only their public child registration.
            if child.name == "scripts":
                scripts_upgrade = helper_upgrade(compatibility, child, guard, migration_helper)
            if migration_source is not None and target != child:
                # A production adoption must retire every checkout dependency,
                # including documentation links. Only authenticated historical
                # Git bytes and canonical child links may move automatically.
                if not registered.is_symlink():
                    raise ValueError("production adoption requires an owned helper child link")
                old = target.parents[2]
                if target != old / "skills/devflow" / child.name:
                    raise ValueError("helper child does not identify a source checkout")
                git = load("installation-paths").git
                if git(old, "remote", "get-url", "origin") != git(ROOT, "remote", "get-url", "origin"):
                    raise ValueError("helper source origin differs from production source")
                inventory = migration_helper.inventory(ROOT, git(old, "rev-parse", "HEAD"))
                prefix = "skills/devflow/" + child.name + "/"
                expected = {name[len(prefix):]: digest for name, digest in inventory.items()
                            if name.startswith(prefix)}
                if hashes(target, guard) != expected:
                    raise ValueError("modified historical helper preserved during production adoption")
                adopted.append((registered, child))
    return {"migration": migration, "scripts_upgrade": scripts_upgrade, "adopted": adopted,
            "plugin": plugin, "service": service, "source": source, "compatibility": compatibility,
            "children": children, "obsolete": obsolete, "actions": actions,
            "predecessor": predecessor, "predecessor_revision": predecessor_revision}


def install(skills, home, force, backup=None, *, migration_source=None, lease_fd=None, predecessor=None):
    global MUTATING
    prepared = plan(skills, home, force, migration_source=migration_source, predecessor=predecessor)
    agents, rollback, migration_helper = (load(name) for name in
                                         ("install-agents", "install-rollback", "install-migration"))
    migration, scripts_upgrade, adopted = (prepared[name] for name in ("migration", "scripts_upgrade", "adopted"))
    plugin, service, source = (prepared[name] for name in ("plugin", "service", "source"))
    compatibility, children = prepared["compatibility"], prepared["children"]
    obsolete, actions = prepared["obsolete"], prepared["actions"]
    predecessor = prepared["predecessor"]
    predecessor_revision = prepared["predecessor_revision"]
    MUTATING = True
    migration_helper.apply(migration, ROOT, skills, agents, rollback, backup)
    if scripts_upgrade and not adopted:
        # Staging stays inside the existing protected snapshot; no public
        # temporary file is removed after an uncertain creation/exchange.
        stage = Path(backup) / "helper-scripts-new"
        stage.symlink_to(ROOT / "skills/devflow/scripts")
        rollback.effect(backup, scripts_upgrade, stage)
    for registered, child in adopted:
        stage = Path(backup) / ("adopt-" + child.name)
        stage.symlink_to(child)
        rollback.effect(backup, registered, stage)
    if migration_source is not None and os.path.lexists(service) and service.resolve() != source:
        stage = Path(backup) / "adopt-service"
        stage.symlink_to(source)
        rollback.effect(backup, service, stage)
    if migration_source is not None or obsolete or any(action == "copy" for action in actions.values()):
        subprocess.run(
            [
                sys.executable,
                "-B",
                str(ROOT / "scripts/install-agents.py"),
                "apply",
                str(ROOT),
                str(skills),
                str(home),
                str(force).lower(),
                *([str(backup)] if backup else []),
                *(["--predecessor", str(predecessor)] if predecessor is not None else []),
                *(["--predecessor-revision", predecessor_revision]
                  if predecessor_revision is not None else []),
            ],
            check=True,
            pass_fds=(() if lease_fd is None else (lease_fd,)),
        )
    skills.mkdir(parents=True, exist_ok=True)
    if not os.path.lexists(compatibility):
        compatibility.mkdir(mode=0o700)
        rollback.created(backup, compatibility)
        for child in children:
            (compatibility / child.name).symlink_to(child)
            rollback.created(backup, compatibility / child.name)
    if not plugin.exists() and not os.path.lexists(service):
        service.symlink_to(source)
        rollback.created(backup, service)
    print("Installed the delivery service entry and internal roles; previous owned registrations migrated when present.")


if __name__ == "__main__":
    try:
        install(
            Path(sys.argv[1]).absolute(),
            Path(sys.argv[2]).absolute(),
            sys.argv[3] == "true",
            Path(sys.argv[4]) if len(sys.argv) == 5 else None,
        )
    except (
        ValueError,
        OSError,
        KeyError,
        TypeError,
        subprocess.SubprocessError,
    ) as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1 if MUTATING else 3) from exc
