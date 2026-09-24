#!/usr/bin/env python3.12
"""Install or remove the deterministic macOS launchd issue reconciler."""

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


LABEL = "com.ebarti.devflow.reconcile"


def owned(path):
    if path.is_symlink():
        raise ValueError("refusing symlinked launch agent: " + str(path))
    if not path.exists():
        return None
    data = plistlib.loads(path.read_bytes())
    if data.get("Label") != LABEL or data.get("DevflowManaged") is not True:
        raise ValueError("existing launch agent is not owned by Devflow: " + str(path))
    return data


def launchctl(binary, *args, allow_absent=False):
    result = subprocess.run([binary, *args], text=True, capture_output=True, timeout=20)
    if result.returncode:
        message = result.stderr.strip() or result.stdout.strip() or "launchctl failed"
        if allow_absent and any(text in message.casefold() for text in
                                ("could not find service", "no such process", "service not loaded")):
            return False
        raise RuntimeError(message)
    return True


def service_loaded(binary):
    return launchctl(binary, "print", f"gui/{os.getuid()}/{LABEL}", allow_absent=True)


def install(args, path):
    if sys.platform != "darwin" and not args.no_start:
        raise ValueError("launchd activation is available only on macOS; use reconcile.py once elsewhere")
    existing = owned(path)
    binary = str(Path(args.launchctl).expanduser().resolve(strict=True)) if not args.no_start else None
    source = Path(__file__).resolve().parents[1]
    script = (source / "skills/devflow/scripts/reconcile.py").resolve(strict=True)
    interpreter = Path(sys.executable).resolve(strict=True)
    gh = Path(args.gh or shutil.which("gh") or "").expanduser().resolve()
    if not gh.is_file() or not os.access(gh, os.X_OK):
        raise ValueError("supply an executable gh path")
    db = Path(args.db).expanduser().resolve()
    if not db.is_absolute():
        raise ValueError("state database must be absolute")
    logs = Path(args.codex_home).expanduser().resolve() / "logs"
    manifest = Path(args.codex_home).expanduser().resolve() / ".devflow-install.json"
    marker = Path(args.codex_home).expanduser().resolve() / ".devflow-reconcile-active.json"
    if logs.is_symlink():
        raise ValueError("refusing symlinked service log directory: " + str(logs))
    logs.mkdir(parents=True, exist_ok=True, mode=0o700)
    logs.chmod(0o700)
    out = logs / "devflow-reconcile.log"
    err = logs / "devflow-reconcile.error.log"
    for target in (out, err):
        if target.is_symlink():
            raise ValueError("refusing symlinked service log: " + str(target))
        fd = os.open(target, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
        os.close(fd)
        target.chmod(0o600)
    revision = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    token = hashlib.sha256(json.dumps([str(script), revision,
        hashlib.sha256(script.read_bytes()).hexdigest(), str(interpreter), str(gh),
        str(db), args.interval, args.limit], sort_keys=True).encode()).hexdigest()
    data = {"Label": LABEL, "DevflowManaged": True, "DevflowToken": token,
            "ProgramArguments": [str(interpreter), "-B", str(script), "--db", str(db),
                                 "--manifest", str(manifest), "--activation-marker", str(marker),
                                 "--activation-token", token,
                                 "daemon", "--interval", str(args.interval), "--limit", str(args.limit)],
            "EnvironmentVariables": {"PATH": str(gh.parent) + ":/usr/bin:/bin",
                                     "DEVFLOW_GH": str(gh)},
            "RunAtLoad": True, "KeepAlive": True, "StandardOutPath": str(out),
            "StandardErrorPath": str(err), "ProcessType": "Background"}
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    prior = path.read_bytes() if existing else None
    prior_marker = marker.read_bytes() if marker.exists() else None
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".devflow-reconcile-", delete=False) as temp:
        temp.write(plistlib.dumps(data, sort_keys=True))
        staged = Path(temp.name)
    staged.chmod(0o600)
    staged.replace(path)
    if not args.no_start:
        target = f"gui/{os.getuid()}"
        stopped_previous = False
        try:
            if existing and service_loaded(binary):
                launchctl(binary, "bootout", target, str(path))
                stopped_previous = True
            launchctl(binary, "bootstrap", target, str(path))
            # This is the activation commit point. A newly bootstrapped daemon
            # waits for its exact token, so an uncertain bootstrap cannot
            # migrate SQLite before launchctl reports success.
            temporary = marker.with_name(marker.name + ".tmp-" + str(os.getpid()))
            temporary.write_text(json.dumps({"token": token}, sort_keys=True) + "\n")
            temporary.chmod(0o600)
            temporary.replace(marker)
        except BaseException as exc:
            try:
                if service_loaded(binary):
                    launchctl(binary, "bootout", target, str(path))
            except BaseException:
                pass
            if prior is None:
                path.unlink(missing_ok=True)
            else:
                path.write_bytes(prior)
                path.chmod(0o600)
            if prior_marker is None:
                marker.unlink(missing_ok=True)
            else:
                marker.write_bytes(prior_marker)
                marker.chmod(0o600)
            if stopped_previous:
                try:
                    launchctl(binary, "bootstrap", target, str(path))
                except BaseException as rollback:
                    raise RuntimeError(f"service upgrade failed: {exc}; prior service restart failed: {rollback}") from exc
            raise
    return {"service": str(path), "db": str(db), "script": str(script),
            "gh": str(gh), "active": not args.no_start}


def uninstall(args, path):
    existing = owned(path)
    if not existing:
        return {"service": str(path), "removed": False}
    if not args.no_stop:
        if sys.platform != "darwin":
            raise ValueError("launchd stop is available only on macOS; use --no-stop for a fixture")
        binary = str(Path(args.launchctl).expanduser().resolve(strict=True))
        if service_loaded(binary):
            launchctl(binary, "bootout", f"gui/{os.getuid()}", str(path))
    path.unlink()
    arguments = existing.get("ProgramArguments", [])
    if "--activation-marker" in arguments:
        marker = Path(arguments[arguments.index("--activation-marker") + 1])
        if marker.exists() and json.loads(marker.read_text()).get("token") == existing.get("DevflowToken"):
            marker.unlink()
    return {"service": str(path), "removed": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--launch-agents", default=str(Path.home() / "Library/LaunchAgents"))
    parser.add_argument("--launchctl", default=os.environ.get("DEVFLOW_LAUNCHCTL", "/bin/launchctl"),
                        help="Absolute launchctl path")
    commands = parser.add_subparsers(dest="command", required=True)
    setting = commands.add_parser("install")
    setting.add_argument("--db", default=str(Path(os.environ.get("XDG_STATE_HOME",
                        str(Path.home() / ".local/state"))) / "devflow/workflow.sqlite3"))
    setting.add_argument("--codex-home", default=os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
    setting.add_argument("--gh")
    setting.add_argument("--interval", type=int, default=60)
    setting.add_argument("--limit", type=int, default=20)
    setting.add_argument("--no-start", action="store_true")
    removing = commands.add_parser("uninstall")
    removing.add_argument("--no-stop", action="store_true")
    commands.add_parser("inspect")
    args = parser.parse_args()
    path = Path(args.launch_agents).expanduser().resolve() / (LABEL + ".plist")
    try:
        if args.command == "install":
            if args.interval < 15 or args.limit < 1 or args.limit > 100:
                raise ValueError("interval must be at least 15 and limit must be 1..100")
            result = install(args, path)
        elif args.command == "uninstall":
            result = uninstall(args, path)
        else:
            result = {"service": str(path), "installed": bool(owned(path)),
                      "configuration": owned(path)}
        print(json.dumps(result, sort_keys=True))
        return 0
    except (OSError, ValueError, RuntimeError, plistlib.InvalidFileException) as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
