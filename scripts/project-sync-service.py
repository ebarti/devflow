#!/usr/bin/env python3.12
"""Install the independent feature/Project synchronizer as a managed macOS service."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import plistlib
import shutil
import subprocess
import sys
import tempfile

LABEL = "com.ebarti.devflow.project-sync"
ROOT = Path(__file__).resolve().parents[1]


def owned(path):
    if path.is_symlink():
        raise ValueError("refusing a symlinked service file")
    if not path.exists():
        return None
    value = plistlib.loads(path.read_bytes())
    if value.get("Label") != LABEL or value.get("DevflowManaged") is not True:
        raise ValueError("existing service is not owned by Devflow")
    return value


def atomic(path, content):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink():
        raise ValueError("refusing a symlinked service artifact")
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as target:
        target.write(content)
        staged = Path(target.name)
    staged.chmod(0o600)
    staged.replace(path)


def launch(*args, absent_ok=False):
    result = subprocess.run(["/bin/launchctl", *args], text=True,
                            capture_output=True, timeout=20, check=False)
    if result.returncode:
        error = result.stderr or result.stdout
        if absent_ok and any(value in error.casefold() for value in (
            "could not find service", "no such process", "service not loaded",
        )):
            return False
        raise RuntimeError(error.strip())
    return True


def install(args, path):
    previous = owned(path)
    config_path = args.config.resolve(strict=True)
    config_bytes = config_path.read_bytes()
    config = json.loads(config_bytes)
    if config.get("version") != 1 or not config.get("owners"):
        raise ValueError("supply a synchronizer configuration with explicit runtime owners")
    for owner in config["owners"]:
        if not Path(owner).is_absolute() or not Path(owner).is_file():
            raise ValueError("runtime owner configuration must be an existing absolute path")
    gh = shutil.which("gh")
    if not gh:
        raise ValueError("GitHub CLI is unavailable")
    revision = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"],
                                       text=True).strip()
    root = config_path.parent
    marker = root / "project-sync-activation.json"
    config_hash = hashlib.sha256(config_bytes).hexdigest()
    token = hashlib.sha256(json.dumps([str(ROOT), revision, str(config_path), config_hash])
                           .encode()).hexdigest()
    arguments = [str(Path(sys.executable)), "-B", "-m", "devflow_temporal.delivery_project_sync",
                 "--config", str(config_path), "--activation-file", str(marker),
                 "--activation-token", token, "--expected-revision", revision,
                 "--config-sha256", config_hash]
    logs = [root / "project-sync.log", root / "project-sync.error.log"]
    for log in logs:
        if log.is_symlink():
            raise ValueError("refusing a symlinked service log")
        fd = os.open(log, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
        os.close(fd)
        log.chmod(0o600)
    data = {"Label": LABEL, "DevflowManaged": True, "ProgramArguments": arguments,
            "WorkingDirectory": str(ROOT), "RunAtLoad": True, "KeepAlive": True,
            "ThrottleInterval": 30, "ProcessType": "Background",
            "StandardOutPath": str(logs[0]), "StandardErrorPath": str(logs[1]),
            "EnvironmentVariables": {"PATH": str(Path(gh).parent) + ":/usr/bin:/bin",
                                     "DEVFLOW_GH": gh,
                                     "PYTHONPATH": str(ROOT / "runtime/src")}}
    old_plist = path.read_bytes() if previous else None
    old_marker = marker.read_bytes() if marker.exists() else None
    stopped = False
    target = f"gui/{os.getuid()}"
    atomic(path, plistlib.dumps(data))
    if not args.no_start:
        try:
            if previous and launch("print", target + "/" + LABEL, absent_ok=True):
                launch("bootout", target, str(path))
                stopped = True
            launch("bootstrap", target, str(path))
            atomic(marker, json.dumps({"token": token}).encode())
        except BaseException:
            if launch("print", target + "/" + LABEL, absent_ok=True):
                launch("bootout", target, str(path))
            if old_plist is None:
                path.unlink(missing_ok=True)
            else:
                atomic(path, old_plist)
            if old_marker is None:
                marker.unlink(missing_ok=True)
            else:
                atomic(marker, old_marker)
            if stopped:
                launch("bootstrap", target, str(path))
            raise
    return {"service": str(path), "config": str(config_path), "revision": revision,
            "active": not args.no_start}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--launch-agents", type=Path,
                        default=Path.home() / "Library/LaunchAgents")
    commands = parser.add_subparsers(dest="action", required=True)
    installing = commands.add_parser("install")
    installing.add_argument("--config", type=Path, required=True)
    installing.add_argument("--no-start", action="store_true")
    commands.add_parser("inspect")
    commands.add_parser("stop")
    args = parser.parse_args()
    path = args.launch_agents / (LABEL + ".plist")
    if args.action == "install":
        if sys.platform != "darwin" and not args.no_start:
            raise ValueError("service activation requires macOS")
        result = install(args, path)
    elif args.action == "inspect":
        result = owned(path)
    else:
        if owned(path):
            launch("bootout", f"gui/{os.getuid()}", str(path), absent_ok=True)
        result = {"stopped": True, "service": str(path)}
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
