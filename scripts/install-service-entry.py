#!/usr/bin/env python3.12
"""Install one service skill and internal roles; preserve existing script-only helpers."""

from __future__ import annotations

import hashlib
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(
        name, ROOT / "scripts" / (name + ".py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def hashes(root):
    result = {}
    for file in root.rglob("*"):
        if "__pycache__" in file.parts or file.suffix == ".pyc":
            continue
        if file.is_symlink():
            raise ValueError("helper source contains an alias")
        if file.is_file():
            result[str(file.relative_to(root))] = hashlib.sha256(
                file.read_bytes()
            ).hexdigest()
    return result


def install(skills, home, force):
    agents = load("install-agents")
    guard = load("install-delivery-launchers")
    agents.check_skills_destination(str(skills))
    # Retire existing global registrations explicitly. Normal installation neither
    # removes foreign host state nor recreates the old controller hooks.
    if any(
        os.path.lexists(home / n) for n in (".devflow-install.json", ".devflow-hook.py")
    ):
        raise ValueError("retire inspected old Devflow hook/guard registrations first")
    for name in (ROOT / "skills").iterdir():
        if (skills / name.name / "SKILL.md").exists():
            raise ValueError(
                "retire inspected direct-agent skills before service installation"
            )
    _, _, actions, obsolete, _ = agents.plan(ROOT, skills, home, force)
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
        raise ValueError("foreign service entry preserved")
    compatibility = skills / "devflow"
    children = [
        p
        for p in (ROOT / "skills/devflow").iterdir()
        if p.name not in {"SKILL.md", "__pycache__"}
    ]
    if os.path.lexists(compatibility):
        # Preserve the authentic old real paths used by already-running work.
        guard.directory(compatibility.resolve())
        if {p.name for p in compatibility.iterdir()} != {p.name for p in children}:
            raise ValueError("script-only helper directory has unexpected children")
        for child in children:
            target = (compatibility / child.name).resolve(strict=True)
            guard.path(str(target))
            guard.directory(target)
            # Older active work keeps its reference documents. Only executable
            # helpers must match the service's compatibility contract.
            if child.name == "scripts" and hashes(target) != hashes(child):
                raise ValueError(
                    "retained helper/reference bytes differ from current source"
                )
    if obsolete or any(action == "copy" for action in actions.values()):
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
            ],
            check=True,
        )
    skills.mkdir(parents=True, exist_ok=True)
    if not os.path.lexists(compatibility):
        compatibility.mkdir(mode=0o700)
        for child in children:
            (compatibility / child.name).symlink_to(child)
    if not plugin.exists() and not os.path.lexists(service):
        service.symlink_to(source)
    print("Installed the delivery service entry and internal roles; hooks unchanged.")


if __name__ == "__main__":
    try:
        install(
            Path(sys.argv[1]).absolute(),
            Path(sys.argv[2]).absolute(),
            sys.argv[3] == "true",
        )
    except (
        ValueError,
        OSError,
        KeyError,
        TypeError,
        subprocess.SubprocessError,
    ) as exc:
        raise SystemExit(str(exc)) from exc
