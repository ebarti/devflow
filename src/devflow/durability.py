"""Small filesystem durability primitives; flush failures must reach the caller."""

import fcntl
import os
import sys
from pathlib import Path


def flush_descriptor(descriptor):
    os.fsync(descriptor)
    if sys.platform == "darwin":
        # fsync alone need not flush a drive's volatile write cache on macOS.
        fcntl.fcntl(descriptor, fcntl.F_FULLFSYNC)


def flush_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        flush_descriptor(descriptor)
    finally:
        os.close(descriptor)


def durable_directory(path, *, mode=0o700):
    """Persist one namespace before creating descendants, including on retries.

    The nearest existing directory and its immediate parent must be accessible:
    without creation provenance, that entry could be an interrupted mkdir. Each
    missing component is then flushed before creating the next. Higher existing
    ancestors are outside this call.
    """
    path = Path(path).absolute()
    missing = []
    existing = path
    while not existing.exists():
        missing.append(existing)
        existing = existing.parent
    # A prior call may have stopped between mkdir(existing) and its parent flush.
    # Ordered creation means no higher newly created namespace can remain pending.
    flush_directory(existing)
    flush_directory(existing.parent)
    for directory in reversed(missing):
        directory.mkdir(mode=mode, exist_ok=True)
        flush_directory(directory)
        flush_directory(directory.parent)
