"""Durable admission fence for a queued, never-started delivery."""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import stat
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from .contracts import canonical_json, digest


def initialize(db):
    """Create only the queued-intake control and its negative inventories."""
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pause_control (
        root_id TEXT PRIMARY KEY, state TEXT NOT NULL, revision INTEGER NOT NULL,
        epoch INTEGER NOT NULL, workflow_id TEXT NOT NULL, execution_run_id TEXT NOT NULL,
        owner_generation INTEGER NOT NULL, checkpoint_json TEXT, updated_at TEXT NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pause_actors (
        root_id TEXT NOT NULL, workflow_id TEXT NOT NULL, execution_run_id TEXT NOT NULL,
        owner_generation INTEGER NOT NULL, actor_run_id TEXT NOT NULL,
        PRIMARY KEY(root_id, workflow_id))""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pause_actor_status (
        root_id TEXT NOT NULL, workflow_id TEXT NOT NULL, state TEXT NOT NULL,
        PRIMARY KEY(root_id, workflow_id))""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pause_clock_receipts (
        root_id TEXT NOT NULL, epoch INTEGER NOT NULL, workflow_id TEXT NOT NULL,
        execution_run_id TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT,
        PRIMARY KEY(root_id, epoch, workflow_id))""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pause_intervals (
        root_id TEXT NOT NULL, epoch INTEGER NOT NULL, paused_at TEXT NOT NULL,
        resumed_at TEXT, PRIMARY KEY(root_id, epoch))""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pause_intake_checkpoints (
        root_id TEXT NOT NULL, epoch INTEGER NOT NULL, payload_json TEXT NOT NULL,
        payload_sha256 TEXT NOT NULL, PRIMARY KEY(root_id, epoch))""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pause_expiries (
        root_id TEXT NOT NULL, epoch INTEGER NOT NULL, workflow_id TEXT NOT NULL,
        execution_run_id TEXT NOT NULL, request_digest TEXT NOT NULL,
        receipt_json TEXT NOT NULL, PRIMARY KEY(root_id, epoch))""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pause_commands (
        command_id TEXT PRIMARY KEY, root_id TEXT NOT NULL, request_digest TEXT NOT NULL,
        response_json TEXT NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pause_steps (
        root_id TEXT NOT NULL, operation_key TEXT NOT NULL, step TEXT NOT NULL,
        epoch INTEGER NOT NULL, workflow_id TEXT NOT NULL, execution_run_id TEXT NOT NULL,
        owner_generation INTEGER NOT NULL, admitted_at TEXT NOT NULL,
        PRIMARY KEY(root_id, operation_key, step))""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pause_pending_merges (
        root_id TEXT NOT NULL, workflow_id TEXT NOT NULL, grant_sha256 TEXT NOT NULL,
        state TEXT NOT NULL, execution_run_id TEXT, PRIMARY KEY(root_id, workflow_id))""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pause_feature_children (
        root_id TEXT NOT NULL, child_id TEXT NOT NULL, assignment_key TEXT NOT NULL,
        workflow_id TEXT NOT NULL, generation INTEGER NOT NULL, state TEXT NOT NULL,
        execution_run_id TEXT NOT NULL, spec_json TEXT NOT NULL,
        PRIMARY KEY(root_id, assignment_key), UNIQUE(root_id, child_id))""")
    from .delivery_pause_dispatch import initialize as initialize_start_receipts
    from .delivery_pause_dispatch_lease import initialize as initialize_dispatch_leases
    from .delivery_pause_intake_pump import initialize as initialize_intake_pump

    initialize_start_receipts(db)
    initialize_dispatch_leases(db)
    initialize_intake_pump(db)


class PauseDeferred(RuntimeError):
    """The named step did not start because the owner's pause fence is closed."""

    def __init__(self, message, *, root_id=None, operation_key=None, step=None, epoch=None):
        super().__init__(message)
        self.root_id = root_id
        self.operation_key = operation_key
        self.step = step
        self.epoch = epoch


def require_no_expiry(db, root_id, epoch):
    if db.execute("""SELECT 1 FROM delivery_pause_expiries
        WHERE root_id=? AND epoch=?""", (root_id, epoch)).fetchone():
        raise ValueError("delivery limit expired; pause settlement is pending")


def _now():
    return datetime.now(UTC).isoformat()


def _nonempty(value, name):
    if not isinstance(value, str) or not value or len(value) > 512:
        raise ValueError(f"invalid {name}")


def _epoch(value):
    if type(value) is not int or value < 0:
        raise ValueError("invalid pause epoch")


@contextmanager
def root_mutex(store, root_id):
    """Lock order: owner/slot, resource custody, root mutex, SQLite write."""
    _nonempty(root_id, "root ID")
    directory = Path(store.config.state_root) / "pause-locks"
    try:
        directory.mkdir(mode=0o700)
    except FileExistsError:
        pass
    info = directory.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o700):
        raise ValueError("pause lock directory is not private and owned")
    path = directory / (hashlib.sha256(root_id.encode()).hexdigest() + ".lock")
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        current = path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
                or (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino)):
            raise ValueError("pause lock is not private and owned")
        fcntl.flock(fd, fcntl.LOCK_EX)
        current = path.lstat()
        if (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino):
            raise ValueError("pause lock changed while acquiring it")
        yield
    finally:
        os.close(fd)


def _row(db, root_id):
    row = db.execute("SELECT * FROM delivery_pause_control WHERE root_id=?", (root_id,)).fetchone()
    if row is None:
        raise ValueError("pause owner is not registered")
    return dict(row)


def request_pause(store, root_id, command_id, expected_revision, *, intake_only=False):
    if not intake_only:
        raise ValueError("running-workflow pause is not supported")
    _nonempty(command_id, "command ID")
    if type(expected_revision) is not int or expected_revision < 0:
        raise ValueError("invalid control revision")
    request_digest = digest({"action": "pause", "root_id": root_id,
                             "expected_revision": expected_revision})
    with root_mutex(store, root_id), store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        prior = db.execute("SELECT * FROM delivery_pause_commands WHERE command_id=?",
                           (command_id,)).fetchone()
        if prior is not None:
            if prior["request_digest"] != request_digest:
                raise ValueError("pause command ID reused with different inputs")
            return json.loads(prior["response_json"])
        row = _row(db, root_id)
        if row["revision"] != expected_revision or row["state"] != "running":
            raise ValueError("pause control state or revision changed")
        if intake_only:
            run = db.execute("""SELECT r.execution_state,o.state AS outbox_state
                FROM delivery_runs r JOIN delivery_outbox o USING(run_id)
                WHERE r.run_id=?""", (root_id,)).fetchone()
            if (run is None or run["execution_state"] != "queued"
                    or run["outbox_state"] not in {"pending", "unknown"}
                    or row["execution_run_id"]
                    or db.execute("SELECT 1 FROM delivery_pause_steps WHERE root_id=? "
                                  "AND step='start' LIMIT 1", (root_id,)).fetchone()
                    or db.execute("SELECT 1 FROM delivery_pause_actors WHERE root_id=? LIMIT 1",
                                  (root_id,)).fetchone()):
                raise ValueError("intake pause is unavailable after start admission")
        revision, epoch = row["revision"] + 1, row["epoch"] + 1
        db.execute("""UPDATE delivery_pause_control SET state='pause_requested', revision=?,
                      epoch=?, checkpoint_json=NULL, updated_at=? WHERE root_id=?""",
                   (revision, epoch, _now(), root_id))
        response = {"root_id": root_id, "state": "pause_requested", "revision": revision,
                    "epoch": epoch}
        db.execute("INSERT INTO delivery_pause_commands VALUES (?, ?, ?, ?)",
                   (command_id, root_id, request_digest, canonical_json(response)))
        return response


def publish_paused(store, root_id, epoch):
    """Publish paused only after every live actor proves a workflow clock start."""
    _epoch(epoch)
    with root_mutex(store, root_id), store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = _row(db, root_id)
        if row["epoch"] != epoch or row["state"] != "pausing" or not row["checkpoint_json"]:
            raise ValueError("pause checkpoint is not ready for publication")
        if _missing_clock(db, root_id, epoch, finished=False):
            raise ValueError("pause workflow clock start acknowledgements are pending")
        stamp = _now()
        db.execute("INSERT INTO delivery_pause_intervals VALUES (?, ?, ?, NULL)",
                   (root_id, epoch, stamp))
        db.execute("""UPDATE delivery_pause_control SET state='paused', revision=revision+1,
                      updated_at=? WHERE root_id=?""", (stamp, root_id))
        return _row(db, root_id)


def begin_resume(store, root_id, command_id, expected_revision, epoch):
    """Record explicit resume intent without opening the admission fence."""
    _nonempty(command_id, "command ID")
    _epoch(epoch)
    if type(expected_revision) is not int or expected_revision < 0:
        raise ValueError("invalid control revision")
    request = {"action": "resume", "root_id": root_id, "expected_revision": expected_revision,
               "epoch": epoch}
    request_digest = digest(request)
    with root_mutex(store, root_id), store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        prior = db.execute("SELECT * FROM delivery_pause_commands WHERE command_id=?",
                           (command_id,)).fetchone()
        if prior is not None:
            if prior["request_digest"] != request_digest:
                raise ValueError("pause command ID reused with different inputs")
            return json.loads(prior["response_json"])
        row = _row(db, root_id)
        if (row["state"] != "paused" or row["revision"] != expected_revision
                or row["epoch"] != epoch):
            raise ValueError("resume control state, revision or epoch changed")
        checkpoint = json.loads(row["checkpoint_json"])
        if not checkpoint["resumable"]:
            raise ValueError("checkpoint requires external effect reconciliation before resume")
        if _missing_clock(db, root_id, epoch, finished=False):
            raise ValueError("pause workflow clock start acknowledgements are pending")
        revision = row["revision"] + 1
        db.execute("""UPDATE delivery_pause_control SET state='resuming', revision=?,
                      updated_at=? WHERE root_id=?""", (revision, _now(), root_id))
        response = {"root_id": root_id, "state": "resuming", "revision": revision,
                    "epoch": epoch}
        db.execute("INSERT INTO delivery_pause_commands VALUES (?, ?, ?, ?)",
                   (command_id, root_id, request_digest, canonical_json(response)))
        return response


def _missing_clock(db, root_id, epoch, *, finished):
    condition = "r.finished_at IS NULL" if finished else "r.started_at IS NULL"
    return db.execute("""SELECT 1 FROM delivery_pause_actors a
        JOIN delivery_pause_actor_status s ON s.root_id=a.root_id AND s.workflow_id=a.workflow_id
        LEFT JOIN delivery_pause_clock_receipts r ON r.root_id=a.root_id
            AND r.workflow_id=a.workflow_id AND r.epoch=?
            AND r.execution_run_id=a.execution_run_id
        WHERE a.root_id=? AND s.state IN ('running', 'resume_pending') AND ("""
        + condition + ") LIMIT 1", (epoch, root_id)).fetchone() is not None


@contextmanager
def start_admission(store, root_id, workflow_id, owner_generation, *, epoch, validate,
                    retry_selected=False):
    """Hold the root fence until the Temporal start handoff has been sent."""
    _epoch(epoch)
    if not callable(validate) or type(retry_selected) is not bool:
        raise ValueError("Temporal start validator is required")
    with root_mutex(store, root_id):
        with store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = _row(db, root_id)
            if (row["workflow_id"] != workflow_id or row["owner_generation"] != owner_generation
                    or row["execution_run_id"]):
                raise ValueError("Temporal start owner changed or already bound")
            validate(db, row)
            if row["state"] != "running":
                raise PauseDeferred("pause fence closed before Temporal start", root_id=root_id,
                                    operation_key="start:" + workflow_id, step="start",
                                    epoch=row["epoch"])
            if row["epoch"] != epoch:
                raise ValueError("Temporal start pause epoch changed")
            old = db.execute("""SELECT * FROM delivery_pause_steps WHERE root_id=?
                              AND operation_key=? AND step='start'""",
                             (root_id, "start:" + workflow_id)).fetchone()
            if old is not None:
                if (not retry_selected or old["workflow_id"] != workflow_id
                        or old["owner_generation"] != owner_generation
                        or old["execution_run_id"] != ""):
                    raise ValueError("Temporal start already sent or handoff unknown")
            else:
                db.execute(
                    "INSERT INTO delivery_pause_steps VALUES (?, ?, 'start', ?, ?, '', ?, ?)",
                    (root_id, "start:" + workflow_id, epoch, workflow_id, owner_generation, _now()))
        yield


