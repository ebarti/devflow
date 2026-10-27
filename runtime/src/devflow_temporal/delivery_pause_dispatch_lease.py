"""Full-body custody for a pause-enabled Temporal start dispatcher."""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import stat
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from . import delivery_pause as pause
from .contracts import canonical_json
from .delivery_resources import _ancestors, private_directory


class DispatchBusy(RuntimeError):
    """Another dispatcher retains the exact start body."""


def initialize(db):
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pause_dispatch_leases (
        root_id TEXT PRIMARY KEY, workflow_id TEXT NOT NULL, owner_generation INTEGER NOT NULL,
        lock_json TEXT NOT NULL, generation INTEGER NOT NULL, owner TEXT NOT NULL
    )""")


def lock_path(store, root_id):
    directory = Path(store.config.state_root) / "pause-dispatch"
    return directory / (hashlib.sha256(root_id.encode()).hexdigest() + ".lock")


def identity(info):
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1 or info.st_size != 0):
        raise ValueError("pause dispatcher lock is not its private owned file")
    return {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid}


@contextmanager
def dispatch_lease(store, spec, item):
    """Acquire before any readback; retain through every local acknowledgement."""
    root_id = spec["run_id"]
    workflow_id = item["workflow_id"] or "delivery-" + root_id
    path = lock_path(store, root_id)
    private_directory(path.parent)
    if stat.S_IMODE(path.parent.stat().st_mode) != 0o700:
        raise ValueError("pause dispatcher lock directory is not private")
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        _ancestors(path)
        held = identity(os.fstat(fd))
        os.fsync(fd)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        if identity(path.lstat()) != held:
            raise ValueError("pause dispatcher lock identity changed")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise DispatchBusy("pause dispatcher is already active") from exc
        with pause.root_mutex(store, root_id), store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            control = pause._row(db, root_id)
            run = db.execute("""SELECT r.workflow_id,r.request_digest,r.request_json,
                r.recovery_json,o.state AS outbox_state
                FROM delivery_runs r JOIN delivery_outbox o USING(run_id)
                WHERE r.run_id=?""", (root_id,)).fetchone()
            if control["state"] != "running":
                raise pause.PauseDeferred("pause fence closed before dispatcher observation",
                                          root_id=root_id, operation_key="start:" + workflow_id,
                                          step="dispatch", epoch=control["epoch"])
            if (run is None or control["workflow_id"] != workflow_id
                    or (run["workflow_id"] or "delivery-" + root_id) != workflow_id
                    or run["outbox_state"] not in {"pending", "unknown"}
                    or any(run[key] != item[key] for key in
                           ("request_digest", "request_json", "recovery_json"))
                    or identity(path.lstat()) != held):
                raise ValueError("pause dispatcher no longer owns the selected outbox")
            prior = db.execute("SELECT * FROM delivery_pause_dispatch_leases WHERE root_id=?",
                               (root_id,)).fetchone()
            if prior is not None and (prior["workflow_id"] != workflow_id
                    or prior["owner_generation"] != control["owner_generation"]
                    or json.loads(prior["lock_json"]) != held):
                raise ValueError("pause dispatcher lock or owner changed")
            generation = prior["generation"] + 1 if prior else 1
            db.execute("""INSERT OR REPLACE INTO delivery_pause_dispatch_leases
                VALUES (?,?,?,?,?,?)""", (root_id, workflow_id, control["owner_generation"],
                                         canonical_json(held), generation, uuid4().hex))
        yield
    finally:
        os.close(fd)


def record_when_running(store, spec, function, *args, **kwargs):
    """Fence a short local preflight/outage projection against pause acceptance."""
    root_id = spec["run_id"]
    with pause.root_mutex(store, root_id):
        with store._connect() as db:
            control = pause._row(db, root_id)
            row = db.execute("SELECT workflow_id,request_digest FROM delivery_runs WHERE run_id=?",
                             (root_id,)).fetchone()
            if (control["state"] != "running" or row is None
                    or control["workflow_id"] != (row["workflow_id"] or "delivery-" + root_id)
                    or row["request_digest"] != spec.get("request_digest")):
                return False
        return function(*args, **kwargs) is not False
