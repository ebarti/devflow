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
    """Create ancestors and persist each new namespace entry before returning."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True, mode=mode)
    # Persist ancestors too: a previous failed attempt may already have created them.
    for directory in (path, *path.parents):
        flush_directory(directory)
