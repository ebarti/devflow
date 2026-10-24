"""Amended execution authority retains its independently owned live scheduling limit."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from contextlib import closing
from pathlib import Path

from .contracts import digest
from .delivery_config import DeliveryConfig
from .delivery_execution_config import load_execution_config, operational_config, transport_binding

FIELD = "operational_capacity"


def _owner_bytes(path):
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("operational owner configuration path changed")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_mode & 0o022
            or before.st_nlink != 1
            or before.st_size > 4 * 1024 * 1024
        ):
            raise ValueError("operational owner configuration is not a bounded owned file")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            raw = stream.read(4 * 1024 * 1024 + 1)
        after = os.fstat(fd)
        if len(raw) != before.st_size or any(
            getattr(before, key) != getattr(after, key)
            for key in (
                "st_dev",
                "st_ino",
                "st_uid",
                "st_mode",
                "st_nlink",
                "st_size",
                "st_mtime_ns",
                "st_ctime_ns",
            )
        ):
            raise ValueError("operational owner configuration changed during observation")
        return raw
    finally:
        os.close(fd)


def submitted_owner(spec, *, db=None):
    """The original run, rather than a caller/private Store, selects its owner."""
    current = load_execution_config(spec)
    if Path(spec["policy"]["tracking_db"]).resolve() != current.tracking_db.resolve():
        raise ValueError("amended operational transport differs from its frozen database")
    if db is None:
        with closing(
            sqlite3.connect(current.tracking_db.as_uri() + "?mode=ro", uri=True)
        ) as source:
            source.execute("PRAGMA query_only=ON")
            return submitted_owner(spec, db=source)
    row = db.execute(
        "SELECT request_json,request_digest FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
    ).fetchone()
    original = json.loads(row[0]) if row else None
    if (
        not isinstance(original, dict)
        or original.get("request_digest") != row[1]
        or original.get("request_digest") != spec.get("request_digest")
        or any(
            original.get(key) != spec.get(key)
            for key in (
                "run_id",
                "work_id",
                "repository_key",
                "state_dir",
                "checkout",
                "source_path",
                "origin_url",
                "github_repo",
                "provider",
                "authorized_endpoint",
            )
        )
    ):
        raise ValueError("operational owner lost its original submitted authority")
    if spec["policy"].get("capacity", 2) != original["policy"].get("capacity", 2):
        raise ValueError("amended execution changed its original frozen policy capacity")
    owner = load_execution_config(original)
    if transport_binding(owner) != transport_binding(current):
        raise ValueError("operational owner transport differs from the amended execution")
    return original, owner


def live_owner(spec, *, db=None):
    original, frozen = submitted_owner(spec, db=db)
    try:
        raw = _owner_bytes(frozen.path)
        live = DeliveryConfig._from_raw(frozen.path, json.loads(raw))
    except (OSError, ValueError, TypeError, KeyError) as exc:
        raise ValueError("canonical live operational owner is unavailable or changed") from exc
    if transport_binding(live) != transport_binding(frozen) or {
        k: v for k, v in live.raw.items() if k != "capacity"
    } != {k: v for k, v in frozen.raw.items() if k != "capacity"}:
        raise ValueError("live operational owner changed noncapacity execution authority")
    capacity = live.raw.get("capacity", 2)
    if type(capacity) is not int or capacity < 1:
        raise ValueError("live operational capacity is not a positive integer")
    return live, {
        "owner_path": str(frozen.path),
        "frozen_owner_config_digest": original["config_digest"],
        "submitted_spec_sha256": digest(original),
        "live_config_sha256": hashlib.sha256(raw).hexdigest(),
        "live_config_digest": digest(live.raw),
        "capacity": capacity,
        "policy_capacity": spec["policy"]["capacity"],
    }


def _acknowledged_owner(spec, amended, *, db=None):
    live, receipt = live_owner(spec, db=db)
    value = amended.raw.get("capacity", 2)
    if type(value) is not int or value != live.raw.get("capacity", 2):
        raise ValueError("private amendment must retain the canonical live operational capacity")
    if transport_binding(amended) != transport_binding(live):
        raise ValueError("private amendment changed the operational owner transport")
    return live, receipt


def acknowledge(spec, amended, *, db=None):
    return _acknowledged_owner(spec, amended, db=db)[1]


def supervisor_config(store, spec):
    """A private execution file cannot appoint itself scheduling owner."""
    _original, owner = submitted_owner(spec)
    if Path(spec["config_path"]) == owner.path:
        return operational_config(store.config)
    frozen = load_execution_config(spec)
    if store.config.path != frozen.path or digest(store.config.raw) != spec["config_digest"]:
        raise ValueError("private supervisor configuration differs from current execution")
    return _acknowledged_owner(spec, frozen)[0]
