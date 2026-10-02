"""Trusted child start gate; no candidate/provider effect before durable GO."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from .delivery_resources import read_private, write_private


def main() -> int:
    folder = Path(sys.argv[1])
    launch = read_private(folder / "launch.json")
    identity = subprocess.check_output(
        ["ps", "-p", str(os.getpid()), "-o", "lstart="], text=True
    ).strip()
    write_private(folder / "ready.json", {"pid": os.getpid(), "identity": identity})
    if sys.stdin.readline().strip() != "GO":
        return 2
    environment = {
        **launch["environment"],
        "DEVFLOW_MANAGED_DEPTH": "1",
        "DEVFLOW_NATIVE_PID": str(os.getpid()),
        "DEVFLOW_NATIVE_JOURNAL": str(folder / "native-process.json"),
    }
    os.execvpe(launch["argv"][0], launch["argv"], environment)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
