#!/usr/bin/env python3.12
"""Report the delivery workers launched from one runtime checkout that are alive.

A worker is reported when its command line is the lifecycle's worker launch, its
configuration's ``worker-ready.json`` names that PID with a matching start
identity, and the marker's interpreter or source root lies under the checkout.
Exit status 3 reports live workers (one stop command per worker on stderr); 0
reports none. ``scripts/update.sh`` refuses to change the checkout while one
exists, because the long-lived worker imports modules lazily.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

LAUNCH = " -m devflow_temporal.delivery_control --config "


def start_identities(pid):
    """The start identity as the lifecycle records it, and as older markers did."""
    forms = []
    for env in ({**os.environ, "LC_ALL": "C", "TZ": "UTC0"}, None):
        result = subprocess.run(["ps", "-p", str(pid), "-o", "lstart="], capture_output=True,
                                text=True, check=False, env=env)
        value = result.stdout.strip()
        if result.returncode == 0 and value:
            forms.append(value)
    return forms


def under(path, runtime, *, parent=False):
    if not isinstance(path, str) or not path:
        return False
    candidate = Path(path)
    return (candidate.parent if parent else candidate).resolve().is_relative_to(runtime)


def registered_workers(runtime):
    runtime = Path(runtime).expanduser().resolve()
    listing = subprocess.run(["ps", "-axo", "pid=,command="], capture_output=True,
                             text=True, check=True).stdout
    live = []
    for line in listing.splitlines():
        pid, _, command = line.strip().partition(" ")
        # The lifecycle launches `<python> -m devflow_temporal.delivery_control
        # --config PATH worker [--deployment-name ...]`. argv[0] is not trusted:
        # framework Pythons re-exec and rewrite it.
        head, marker, tail = command.partition(LAUNCH)
        config_text, separator, rest = tail.partition(" worker")
        if (not pid.isdigit() or not marker or not separator
                or (rest and not rest.startswith(" --deployment-name "))):
            continue
        config = Path(config_text)
        try:
            state_root = Path(json.loads(config.read_text(encoding="utf-8"))["state_root"])
            registered = json.loads((state_root / "worker-ready.json").read_text(encoding="utf-8"))
        except (OSError, ValueError, KeyError, TypeError):
            continue
        if (not isinstance(registered, dict) or registered.get("pid") != int(pid)
                or registered.get("config_path") != str(config)):
            continue
        # Markers that predate the recorded interpreter fall back to argv[0].
        interpreter = registered.get("interpreter", head)
        if not (under(interpreter, runtime, parent=True)
                or under(registered.get("source_root"), runtime)):
            continue
        if registered.get("identity") not in start_identities(int(pid)):
            continue
        entry = Path(interpreter).parent / "devflow-delivery" if interpreter else "devflow-delivery"
        live.append({"pid": int(pid), "config": str(config),
                     "stop": [str(entry), "--config", str(config), "stop"]})
    return live


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", required=True,
                        help="runtime checkout whose live workers to report")
    args = parser.parse_args()
    try:
        workers = registered_workers(args.runtime)
    except (OSError, subprocess.SubprocessError) as exc:
        print(f"delivery-workers: {exc}", file=sys.stderr)
        sys.exit(1)
    print(json.dumps(workers, sort_keys=True))
    for worker in workers:
        print(f"delivery worker {worker['pid']} is running from this checkout for "
              f"{worker['config']}; stop it first: " + " ".join(worker["stop"]), file=sys.stderr)
    sys.exit(3 if workers else 0)


if __name__ == "__main__":
    main()
