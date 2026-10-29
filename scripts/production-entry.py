#!/usr/bin/env python3.12
"""Stable stdlib bootstrap: lease production ownership before mutable imports."""

import fcntl
import ctypes
import json
import importlib.util
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


def layout_helper():
    spec = importlib.util.spec_from_file_location(
        "production_entry_layout", Path(__file__).with_name("production-layout.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def namespace_requested(arguments, *, seen=None):
    """Classify disclosed config/owner paths before importing application code."""
    candidates = []
    for index, argument in enumerate(arguments):
        option, equals, attached = argument.partition("=")
        is_config = len(option) >= 3 and "--config".startswith(option)
        if is_config and not equals and index + 1 < len(arguments):
            candidates.append(Path(arguments[index + 1]).expanduser().absolute())
        elif is_config and equals:
            candidates.append(Path(attached).expanduser().absolute())
    seen = set() if seen is None else seen
    namespace = Path.home().resolve() / ".local/state/devflow/namespaces"
    for file in candidates:
        resolved = file.resolve(strict=False)
        if file.is_relative_to(namespace) or resolved.is_relative_to(namespace):
            return True
        if resolved in seen:
            continue
        seen.add(resolved)
        try:
            descriptor = os.open(resolved, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except OSError:
            continue  # The ordinary application retains its existing config error.
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode):
                continue
            # Match the application's existing JSON size contract. Bound this
            # read to the opened file's size, and reject any concurrent drift.
            raw = os.pread(descriptor, info.st_size + 1, 0)
            after, named = os.fstat(descriptor), resolved.lstat()
            fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
            if len(raw) != info.st_size or any(getattr(info, key) != getattr(current, key)
                    for current in (after, named) for key in fields):
                raise ValueError("bootstrap configuration changed during classification")
            try:
                data = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
        except OSError as exc:
            raise ValueError("bootstrap configuration changed during classification") from exc
        finally:
            os.close(descriptor)
        if not isinstance(data, dict):
            continue
        tracking = data.get("tracking_db")
        if isinstance(tracking, str):
            database = Path(tracking)
            if database.is_relative_to(namespace) or database.resolve(strict=False).is_relative_to(namespace):
                return True
        owners = data.get("owners")
        if isinstance(owners, list) and namespace_requested(
                [value for owner in owners if isinstance(owner, str) for value in ("--config", owner)], seen=seen):
            return True
    return False


def main():
    root, module = Path(sys.argv[1]), sys.argv[2]
    layout = layout_helper()
    owner = layout.owner_directory()
    python_mode = module == "--python"
    if root.resolve(strict=True) != root or (module not in MODULES and not python_mode):
        raise ValueError("production bootstrap does not name an owned source and controller")
    probe = os.environ.get("DEVFLOW_INSTALLATION_PROBE_FD", "")
    if probe:
        if root != owner / "developer-workflow":
            layout.prepared(root)
        if not probe.isdigit() or sys.argv[3:] != ["--help"]:
            raise ValueError("installation readiness probes may only request controller help")
        lock = int(probe)
        actual, expected = os.fstat(lock), (owner / ".production-install.lock").stat()
        if (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino):
            raise ValueError("readiness probe did not inherit the actual installation owner")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    else:
        installed = layout.installed(root)
        if installed is None:
            raise ValueError("production bootstrap has no active owner")
        lock = os.open(owner / ".production-install.lock", os.O_RDWR | os.O_NOFOLLOW)
        fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
        borrowed = os.environ.get("DEVFLOW_INSTALLATION_LOCK_FD", "")
        if borrowed.isdigit() and int(borrowed) != lock:
            descriptor = int(borrowed)
            try:
                before, current = os.fstat(descriptor), os.fstat(lock)
                if (before.st_dev, before.st_ino) == (current.st_dev, current.st_ino):
                    os.close(descriptor)  # Close only; never unlock a parent's borrowed OFD.
            except OSError:
                pass
    receipt = private_json(owner / ".production-install.json")
    if (receipt.get("managed_owner_version") != 1 or (not probe and receipt.get("phase") != "verified")
            or (not probe and not same_identity(receipt.get("target_identity"), directory_identity(root)))):
        raise ValueError("production installation is unfinished or its owner changed")
    update = owner / ".production-update.json"
    if not probe and update.exists() and private_json(update).get("phase") not in {"verified", "rolled_back"}:
        raise ValueError("production update is unfinished; replay its retained recovery command before launching work")
    os.set_inheritable(lock, True)
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith("PYTHON") and key not in {"VIRTUAL_ENV", "__PYVENV_LAUNCHER__"}}
    environment["DEVFLOW_INSTALLATION_LOCK_FD"] = str(lock)
    environment.pop("DEVFLOW_SOURCE_START_FD", None)
    python = root / "runtime/.venv/bin/python"
    qualified = python_mode or namespace_requested(sys.argv[3:])
    if not probe and installed["version"] == 2 and qualified:
        import importlib.util

        spec = importlib.util.spec_from_file_location("source_startup", Path(__file__).with_name("production-source.py"))
        source = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(source)
        witness = source.start(root, installed["birth"], lock, python)
        environment[source.FD_ENV] = str(witness)
    elif python_mode:
        raise ValueError("qualified Python execution requires an active source generation")
    flags = [str(python), "-I", "-B", "-X", "pycache_prefix=/dev/null"]
    if python_mode:
        if not sys.argv[3:] or sys.argv[3] not in {"-c", "-m"}:
            raise ValueError("qualified Python requires -c CODE or -m MODULE")
        os.execve(python, [*flags, *sys.argv[3:]], environment)
    # Preserve the project's console-entry-point contract: not every entry
    # module has a __main__ block, so `python -m` can silently do no work.
    program = ("import importlib,sys;entry=sys.argv.pop(1);name=sys.argv.pop(1);"
               "sys.argv[0]=entry;sys.exit(importlib.import_module(name).main())")
    entry = root / "runtime/.venv/bin" / NAMES[module]
    os.execve(python, [*flags, "-c", program, str(entry), module, *sys.argv[3:]], environment)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError) as exc:
        print("production runtime: " + str(exc), file=sys.stderr)
        raise SystemExit(1) from exc
