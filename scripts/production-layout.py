#!/usr/bin/env python3.12
"""Stdlib ownership namespace for v1 roots and explicit immutable generations.

Recognizing a prepared directory is not activation. A source generation must be
the exact active owner before an ordinary process may import or use its runtime.
The original fixed-root v1 namespace and identity remain independently strict.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import re
import stat
from pathlib import Path

GENERATION_SCHEMA = "devflow.production-generation/v2"
ACTIVE_SCHEMA = "devflow.production-active-generation/v2"
ALLOCATION_SCHEMA = "devflow.production-generation-allocation/v2"
GENERATION_NAME = re.compile(r"[0-9a-f]{32}")


def owner_directory():
    return Path.home().resolve(strict=True) / ".local/share/devflow"


def private_json(path):
    path = Path(path)
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("production layout evidence is not canonical")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        keys = ("st_dev", "st_ino", "st_uid", "st_mode", "st_nlink", "st_size",
                "st_mtime_ns", "st_ctime_ns")
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1
                or before.st_size > 4 * 1024 * 1024):
            raise ValueError("production layout evidence is not bounded, private and owned")
        with os.fdopen(os.dup(descriptor), "rb") as stream:
            raw = stream.read(before.st_size + 1)
        after, named = os.fstat(descriptor), path.lstat()
        if (len(raw) != before.st_size
                or any(getattr(before, key) != getattr(after, key) for key in keys)
                or any(getattr(after, key) != getattr(named, key) for key in keys)):
            raise ValueError("production layout evidence changed while reading")
        return json.loads(raw), hashlib.sha256(raw).hexdigest()
    finally:
        os.close(descriptor)


def _identity():
    spec = importlib.util.spec_from_file_location(
        "production_layout_identity", Path(__file__).with_name("production-entry.py"))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def _parent(path):
    info = path.lstat()
    if (path.resolve(strict=True) != path or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid() or info.st_mode & 0o022):
        raise ValueError("production layout parent is not canonical, owned and protected")


def _namespace(root):
    root = Path(root)
    owner = owner_directory()
    if (root.name != "source" or not GENERATION_NAME.fullmatch(root.parent.name)
            or root.parent.parent != owner / ".production-generations"):
        raise ValueError("runtime is not an explicit production generation")
    for folder in (owner, root.parent.parent, root.parent, root):
        _parent(folder)
    return owner, root


def _objects(root, value):
    identity = _identity()
    if (not identity.same_identity(value.get("root_identity"), identity.directory_identity(root))
            or not identity.same_identity(value.get("environment_identity"),
                                          identity.directory_identity(root / "runtime/.venv"))):
        raise ValueError("production generation object changed after its ownership proof")


def prepared(root):
    """A completed allocation permits inspection, never ordinary execution."""
    owner, root = _namespace(root)
    path = root.parent / "allocation.json"
    value, sha = private_json(path)
    if (value.get("schema") != ALLOCATION_SCHEMA or value.get("state") != "provisioned"
            or value.get("generation_id") != root.parent.name or value.get("root") != str(root)
            or not isinstance(value.get("revision"), str)
            or not re.fullmatch(r"[0-9a-f]{40}", value["revision"])
            or not isinstance(value.get("tree"), str)
            or not re.fullmatch(r"[0-9a-f]{40}", value["tree"])):
        raise ValueError("production generation has no exact completed allocation")
    _objects(root, value)
    return {"owner": owner, "root": root, "generation_id": root.parent.name,
            "allocation": value, "allocation_path": path, "allocation_sha256": sha}


def generation(root):
    """Authenticate a ready generation without granting it active source ownership."""
    owner, root = _namespace(root)
    path = owner / ".production-generation-births" / (root.parent.name + ".json")
    value, sha = private_json(path)
    if (value.get("schema") != GENERATION_SCHEMA or value.get("state") != "verified"
            or value.get("generation_id") != root.parent.name or value.get("root") != str(root)
            or not isinstance(value.get("revision"), str)
            or not re.fullmatch(r"[0-9a-f]{40}", value["revision"])
            or not isinstance(value.get("tree"), str)
            or not re.fullmatch(r"[0-9a-f]{40}", value["tree"])):
        raise ValueError("production generation has no exact complete birth authority")
    _objects(root, value)
    return {"owner": owner, "root": root, "generation_id": root.parent.name,
            "birth": value, "birth_path": path, "birth_sha256": sha}


def installed(root):
    """Find only the fixed v1 root or a fully authenticated active v2 generation."""
    root = Path(root)
    owner = owner_directory()
    if root == owner / "developer-workflow":
        return {"owner": owner, "root": root, "version": 1}
    if root.parent.parent != owner / ".production-generations":
        return None  # Explicit development source has no production layout grant.
    value = generation(root)
    active, active_sha = private_json(owner / ".production-active.json")
    if (active.get("schema") != ACTIVE_SCHEMA
            or active.get("generation_id") != value["generation_id"]
            or active.get("root") != str(root)
            or active.get("birth_sha256") != value["birth_sha256"]):
        raise ValueError("production generation is prepared but is not the active owner")
    return {**value, "version": 2, "active": active, "active_sha256": active_sha}
