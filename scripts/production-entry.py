#!/usr/bin/env python3.12
"""Stable stdlib bootstrap: lease production ownership before mutable imports."""

import fcntl
import ctypes
import json
import os
import stat
import sys
import uuid
from pathlib import Path

MODULES = {"devflow_temporal.cli", "devflow_temporal.delivery_control",
           "devflow_temporal.delivery_mcp", "devflow_temporal.delivery_project_sync"}
NAMES = {"devflow_temporal.cli": "devflow-temporal",
         "devflow_temporal.delivery_control": "devflow-delivery",
         "devflow_temporal.delivery_mcp": "devflow-delivery-mcp",
         "devflow_temporal.delivery_project_sync": "devflow-project-sync"}


def directory_identity(path):
    """The permanent-runtime owner's opened-object volume/birth identity.

    The outer bootstrap cannot import the mutable SDK to observe its own
    identity. Keep its stdlib observation equivalent to that existing proof.
    """
    path = Path(path)
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("installation identity is not canonical")
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        info = os.fstat(descriptor)
        current = path.lstat()
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o022
                or (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino)):
            raise ValueError("installation identity is not an owned protected directory")
        volume = None
        if sys.platform == "darwin":
            class Attributes(ctypes.Structure):
                _fields_ = [("count", ctypes.c_uint16), ("reserved", ctypes.c_uint16),
                            ("common", ctypes.c_uint32), ("volume", ctypes.c_uint32),
                            ("directory", ctypes.c_uint32), ("file", ctypes.c_uint32),
                            ("fork", ctypes.c_uint32)]
            attributes = Attributes(count=5, volume=0x80040000)
            buffer = ctypes.create_string_buffer(20)
            library = ctypes.CDLL(None, use_errno=True)
            if library.fgetattrlist(descriptor, ctypes.byref(attributes), buffer, len(buffer), 0):
                raise OSError(ctypes.get_errno(), "installation volume inspection unavailable")
            if int.from_bytes(buffer.raw[:4], sys.byteorder) != 20 or not any(buffer.raw[4:20]):
                raise ValueError("installation volume identity is incomplete")
            volume = str(uuid.UUID(bytes=buffer.raw[4:20]))
        return {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid,
                "mode": stat.S_IMODE(info.st_mode), "volume_uuid": volume,
                "birthtime": getattr(info, "st_birthtime", None)}
    finally:
        os.close(descriptor)


def same_identity(before, current):
    if isinstance(before, list):
        # Legacy raw records never imply continuity after device renumbering.
        return before == [current["device"], current["inode"], current["uid"]]
    if before == current:
        return True
    if not isinstance(before, dict) or before.get("device") == current["device"]:
        return False
    return bool(before.get("volume_uuid") and before["volume_uuid"] == current["volume_uuid"]
                and before.get("birthtime") is not None and before["birthtime"] == current["birthtime"]
                and {key: value for key, value in before.items() if key != "device"}
                == {key: value for key, value in current.items() if key != "device"})


def private_json(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > 4 * 1024 * 1024):
            raise ValueError("production bootstrap evidence is not private and owned")
        return json.loads(os.read(fd, info.st_size + 1))
    finally:
        os.close(fd)


def main():
    root, module = Path(sys.argv[1]), sys.argv[2]
    if (root != Path.home().resolve() / ".local/share/devflow/developer-workflow"
            or root.resolve() != root or module not in MODULES):
        raise ValueError("production bootstrap does not name the fixed owner and controller")
    probe = os.environ.get("DEVFLOW_INSTALLATION_PROBE_FD", "")
    if probe:
        if not probe.isdigit() or sys.argv[3:] != ["--help"]:
            raise ValueError("installation readiness probes may only request controller help")
        lock = int(probe)
        actual, expected = os.fstat(lock), (root.parent / ".production-install.lock").stat()
        if (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino):
            raise ValueError("readiness probe did not inherit the actual installation owner")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    else:
        lock = os.open(root.parent / ".production-install.lock", os.O_RDWR | os.O_NOFOLLOW)
        fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
    receipt = private_json(root.parent / ".production-install.json")
    if (receipt.get("managed_owner_version") != 1 or (not probe and receipt.get("phase") != "verified")
            or not same_identity(receipt.get("target_identity"), directory_identity(root))):
        raise ValueError("production installation is unfinished or its owner changed")
    update = root.parent / ".production-update.json"
    if not probe and update.exists() and private_json(update).get("phase") not in {"verified", "rolled_back"}:
        raise ValueError("production update is unfinished; replay its retained recovery command before launching work")
    os.set_inheritable(lock, True)
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith("PYTHON") and key not in {"VIRTUAL_ENV", "__PYVENV_LAUNCHER__"}}
    environment["DEVFLOW_INSTALLATION_LOCK_FD"] = str(lock)
    python = root / "runtime/.venv/bin/python"
    # Preserve the project's console-entry-point contract: not every entry
    # module has a __main__ block, so `python -m` can silently do no work.
    program = ("import importlib,sys;entry=sys.argv.pop(1);name=sys.argv.pop(1);"
               "sys.argv[0]=entry;sys.exit(importlib.import_module(name).main())")
    entry = root / "runtime/.venv/bin" / NAMES[module]
    os.execve(python, [str(python), "-I", "-B", "-c", program, str(entry), module, *sys.argv[3:]], environment)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError) as exc:
        print("production runtime: " + str(exc), file=sys.stderr)
        raise SystemExit(1) from exc
