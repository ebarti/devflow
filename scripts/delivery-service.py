#!/usr/bin/env python3.12
"""Install or remove the macOS launchd supervisor of one Devflow delivery stack.

``install`` stages a per-configuration agent under the private state root and
writes nothing launchd reads; ``--activate`` copies it into ``~/Library/LaunchAgents``
and bootstraps it. The agent runs ``devflow-delivery --config PATH supervise`` at
login, periodically and again after a failure: an idempotent start that honours an
owner's ``stop`` until the next explicit start. ``active`` reports the activated
agents that supervise a stack from one runtime checkout, so an upgrade can refuse
while one would relaunch a worker from the changed source.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import plistlib
import shlex
import shutil
import subprocess
import sys
import tempfile


PREFIX = "com.ebarti.devflow.delivery."
ROOT = Path(__file__).resolve().parents[1]


def label(config):
    return PREFIX + hashlib.sha256(str(config).encode()).hexdigest()[:12]


def configuration(value):
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ValueError("service configuration path must be absolute")
    path = path.resolve(strict=True)
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("state_root"), str):
        raise ValueError("service configuration must name state_root")
    state_root = Path(data["state_root"])
    if not state_root.is_absolute():
        raise ValueError("service configuration state_root must be absolute")
    return path, data, state_root


def owned(path, expected=None):
    """The agent at ``path`` when Devflow wrote it; None when absent."""
    if path.is_symlink():
        raise ValueError("refusing symlinked launch agent: " + str(path))
    if not path.exists():
        return None
    data = plistlib.loads(path.read_bytes())
    if data.get("DevflowManaged") is not True or (
            expected is not None and data.get("Label") != expected):
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


def service_loaded(binary, name):
    return launchctl(binary, "print", f"gui/{os.getuid()}/{name}", allow_absent=True)


def private_file(target):
    if target.is_symlink():
        raise ValueError("refusing symlinked service log: " + str(target))
    fd = os.open(target, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    os.close(fd)
    target.chmod(0o600)


def private_directory(folder):
    if folder.is_symlink():
        raise ValueError("refusing symlinked private directory: " + str(folder))
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    folder.chmod(0o700)


def executable(value, description):
    path = Path(value or "").expanduser()
    if not value or not path.is_absolute():
        raise ValueError(f"supply an absolute executable {description} path")
    path = path.resolve()
    if not path.is_file() or not os.access(path, os.X_OK):
        raise ValueError(f"supply an executable {description} path: {path}")
    return path


def write_private(path, content):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".devflow-delivery-",
                                     delete=False) as temp:
        temp.write(content)
        staged = Path(temp.name)
    staged.chmod(0o600)
    staged.replace(path)


def supervised_runtime(entry, config, explicit=None):
    """The runtime checkout the agent's entry point runs.

    The canonical launcher links ``devflow-delivery`` to a wrapper under its
    journal that execs ``<checkout>/runtime/.venv/bin/devflow-delivery``: the
    journal names the runtime and the wrapper names its exec target, so argv[0]
    never does. A bare entry lives in that ``.venv``. ``--runtime`` names the
    checkout when none of those apply.
    """
    if explicit:
        return Path(explicit).expanduser().resolve()
    journal = entry.parent.parent / "manifest.json"
    if entry.parent.name == "entries" and journal.is_file():
        recorded = json.loads(journal.read_text(encoding="utf-8")).get("runtime_dir")
        if isinstance(recorded, str) and recorded:
            return Path(recorded).resolve()
    head = entry.read_bytes()[:4096]
    if head.startswith(b"#!"):
        for line in head.decode(errors="replace").splitlines():
            if not line.startswith("exec "):
                continue
            words = shlex.split(line[len("exec "):])
            if "--config" in words and Path(words[words.index("--config") + 1]).resolve() != config:
                raise ValueError(f"{entry} is bound to another configuration: "
                                 f"{words[words.index('--config') + 1]}")
            target = Path(words[0]) if words else Path()
            if target.parent.name == "bin" and target.parent.parent.name == ".venv":
                return target.parents[2].resolve()
    if entry.parent.name == "bin" and entry.parent.parent.name == ".venv":
        return entry.parents[2].resolve()
    raise ValueError(f"supply --runtime: the runtime checkout {entry} runs cannot be derived")


def agent(args, name, config, data, state_root):
    entry = executable(args.devflow_delivery or shutil.which("devflow-delivery")
                       or str(ROOT / "runtime/.venv/bin/devflow-delivery"), "devflow-delivery")
    runtime = supervised_runtime(entry, config, args.runtime)
    temporal = executable(args.temporal or data.get("temporal_bin") or shutil.which("temporal"),
                          "Temporal CLI")
    private_directory(state_root)
    out = state_root / "launchd.log"
    err = state_root / "launchd.error.log"
    private_file(out)
    private_file(err)
    # The stack runs role commands (codex, gh, git, node, ...) found on the
    # installing shell's PATH; launchd would otherwise supply a bare system PATH.
    search = [str(entry.parent), str(temporal.parent)]
    search += [item for item in os.environ.get("PATH", "").split(os.pathsep) if item]
    search += ["/usr/bin", "/bin"]
    return {"Label": name, "DevflowManaged": True, "DevflowConfig": str(config),
            # The checkout this agent would relaunch a worker from; upgrades of
            # that checkout refuse while the agent is active.
            "DevflowRuntime": str(runtime),
            "ProgramArguments": [str(entry), "--config", str(config), "supervise"],
            "EnvironmentVariables": {"PATH": os.pathsep.join(dict.fromkeys(search))},
            "RunAtLoad": True, "KeepAlive": {"SuccessfulExit": False},
            "StartInterval": args.interval, "ThrottleInterval": 30,
            "AbandonProcessGroup": True,
            # Temporal, the worker, the API and every role and check inherit the
            # agent's band; Background would throttle the whole stack.
            "ProcessType": "Standard",
            "StandardOutPath": str(out), "StandardErrorPath": str(err)}


def install(args, name, config, data, state_root, staged, active):
    if args.activate and sys.platform != "darwin":
        raise ValueError("launchd activation is available only on macOS; stage without --activate")
    owned(staged, name)
    existing = owned(active, name)
    binary = str(Path(args.launchctl).expanduser().resolve(strict=True)) if args.activate else None
    content = plistlib.dumps(agent(args, name, config, data, state_root), sort_keys=True)
    write_private(staged, content)
    result = {"staged": str(staged), "label": name, "config": str(config),
              "logs": [str(state_root / "launchd.log"), str(state_root / "launchd.error.log")],
              "active": existing is not None, "service": str(active)}
    if not args.activate:
        return {**result, "stale_active": existing is not None and active.read_bytes() != content}
    target = f"gui/{os.getuid()}"
    prior = active.read_bytes() if existing else None
    write_private(active, content)
    stopped_previous = False
    try:
        if existing and service_loaded(binary, name):
            launchctl(binary, "bootout", target, str(active))
            stopped_previous = True
        launchctl(binary, "bootstrap", target, str(active))
    except BaseException as exc:
        try:
            if service_loaded(binary, name):
                launchctl(binary, "bootout", target, str(active))
        except BaseException:
            pass
        if prior is None:
            active.unlink(missing_ok=True)
        else:
            active.write_bytes(prior)
            active.chmod(0o600)
        if stopped_previous:
            try:
                launchctl(binary, "bootstrap", target, str(active))
            except BaseException as rollback:
                raise RuntimeError(f"service upgrade failed: {exc}; prior service restart "
                                   f"failed: {rollback}") from exc
        raise
    return {**result, "active": True, "stale_active": False}


def uninstall(args, name, staged, active):
    existing = owned(active, name)
    if existing and not args.no_stop:
        if sys.platform != "darwin":
            raise ValueError("launchd stop is available only on macOS; use --no-stop for a fixture")
        binary = str(Path(args.launchctl).expanduser().resolve(strict=True))
        if service_loaded(binary, name):
            launchctl(binary, "bootout", f"gui/{os.getuid()}", str(active))
    removed = []
    for path in (active, staged):
        if owned(path, name) is not None:
            path.unlink()
            removed.append(str(path))
    return {"label": name, "removed": removed}


def active_agents(launch_agents, runtime):
    """Activated Devflow delivery agents that supervise a stack from ``runtime``.

    An agent that recorded no runtime is reported for every checkout: refusing
    an upgrade beats relaunching a worker from changed source.
    """
    runtime = Path(runtime).expanduser().resolve()
    folder = Path(launch_agents).expanduser()
    found = []
    for path in sorted(folder.glob(PREFIX + "*.plist")) if folder.is_dir() else []:
        try:
            data = owned(path)
        except ValueError:
            continue
        recorded = data.get("DevflowRuntime")
        if isinstance(recorded, str) and recorded:
            supervised = Path(recorded).resolve()
            if supervised != runtime and not supervised.is_relative_to(runtime):
                continue
        found.append({"service": str(path), "label": data.get("Label"),
                      "config": data.get("DevflowConfig"), "runtime": recorded})
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--launch-agents",
                        default=os.environ.get("DEVFLOW_LAUNCH_AGENTS",
                                               str(Path.home() / "Library/LaunchAgents")))
    parser.add_argument("--launchctl", default=os.environ.get("DEVFLOW_LAUNCHCTL", "/bin/launchctl"),
                        help="Absolute launchctl path")
    commands = parser.add_subparsers(dest="command", required=True)
    setting = commands.add_parser("install")
    setting.add_argument("--config", required=True, help="absolute delivery service configuration")
    setting.add_argument("--devflow-delivery", help="absolute devflow-delivery executable")
    setting.add_argument("--temporal", help="absolute Temporal CLI executable")
    setting.add_argument("--runtime", help="runtime checkout the entry runs; derived from the "
                         "canonical launcher's journal or wrapper, or the entry's .venv, when omitted")
    setting.add_argument("--interval", type=int, default=60,
                         help="seconds between supervised start passes")
    setting.add_argument("--activate", action="store_true",
                         help="install the staged agent into launchd now")
    removing = commands.add_parser("uninstall")
    removing.add_argument("--config", required=True)
    removing.add_argument("--no-stop", action="store_true")
    inspecting = commands.add_parser("inspect")
    inspecting.add_argument("--config", required=True)
    activated = commands.add_parser("active")
    activated.add_argument("--runtime", required=True,
                           help="runtime checkout whose activated agents to report")
    args = parser.parse_args()
    try:
        if args.command == "active":
            found = active_agents(args.launch_agents, args.runtime)
            print(json.dumps(found, sort_keys=True))
            for item in found:
                print(f"launchd agent {item['label']} supervises the delivery stack for "
                      f"{item['config']} from this checkout; uninstall it first: "
                      f"python3.12 {Path(__file__).name} uninstall --config {item['config']}",
                      file=sys.stderr)
            sys.exit(3 if found else 0)
        if not 10 <= getattr(args, "interval", 60) <= 3600:
            raise ValueError("interval must be between 10 and 3600 seconds")
        config, data, state_root = configuration(args.config)
        name = label(config)
        staged = state_root / "launchd" / (name + ".plist")
        active = Path(args.launch_agents).expanduser() / (name + ".plist")
        if args.command == "install":
            result = install(args, name, config, data, state_root, staged, active)
        elif args.command == "uninstall":
            result = uninstall(args, name, staged, active)
        else:
            staged_agent, active_agent = owned(staged, name), owned(active, name)
            result = {"label": name, "staged": str(staged), "service": str(active),
                      "installed": staged_agent is not None, "active": active_agent is not None,
                      "stale_active": (active_agent is not None and staged_agent is not None
                                       and staged_agent != active_agent),
                      **({"agent": staged_agent} if staged_agent else {})}
        print(json.dumps(result, sort_keys=True))
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        print(f"delivery-service: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
