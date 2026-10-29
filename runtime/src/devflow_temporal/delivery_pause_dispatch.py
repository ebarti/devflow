"""Fence the initial Temporal start against a persisted pause request."""
from __future__ import annotations

import asyncio
import hashlib
import os
import re
import sqlite3
import stat
import sys
from contextlib import asynccontextmanager, contextmanager

from .delivery_pause import start_admission
from .delivery_pause_execution import (
    authenticate_closed_original_start,
    authenticate_original_start,
    original_request,
    require_original_in_transaction,
)

_RECEIPT_SCHEMA = {
    "delivery_pause_start_receipts": """CREATE TABLE IF NOT EXISTS delivery_pause_start_receipts (
        root_id TEXT PRIMARY KEY, workflow_id TEXT NOT NULL, execution_run_id TEXT NOT NULL,
        owner_generation INTEGER NOT NULL, start_epoch INTEGER NOT NULL, start_token TEXT NOT NULL,
        request_digest TEXT NOT NULL, request_json_sha256 TEXT NOT NULL,
        recovery_json_sha256 TEXT NOT NULL, start_spec_sha256 TEXT NOT NULL,
        event1_sha256 TEXT NOT NULL)""",
    "pause_start_receipt_no_update": """CREATE TRIGGER IF NOT EXISTS pause_start_receipt_no_update
        BEFORE UPDATE ON delivery_pause_start_receipts BEGIN
        SELECT RAISE(ABORT,'immutable pause start receipt'); END""",
    "pause_start_receipt_no_delete": """CREATE TRIGGER IF NOT EXISTS pause_start_receipt_no_delete
        BEFORE DELETE ON delivery_pause_start_receipts BEGIN
        SELECT RAISE(ABORT,'immutable pause start receipt'); END""",
    "pause_start_receipt_no_replace": """CREATE TRIGGER IF NOT EXISTS pause_start_receipt_no_replace
        BEFORE INSERT ON delivery_pause_start_receipts
        WHEN EXISTS(SELECT 1 FROM delivery_pause_start_receipts WHERE root_id=NEW.root_id)
        BEGIN SELECT RAISE(ABORT,'immutable pause start receipt'); END""",
}


def initialize(db):
    """Bootstrap immutable start receipts before any dispatcher can run."""
    for statement in _RECEIPT_SCHEMA.values():
        db.execute(statement)
    require_start_receipt_schema(db)


def require_start_receipt_schema(db):
    """Check exact receipt and trigger definitions using bounded SQLite metadata."""
    def normalized(sql):
        return re.sub(r"\s+", "", sql).lower().replace("ifnotexists", "")

    for name, expected in _RECEIPT_SCHEMA.items():
        kind = "table" if name == "delivery_pause_start_receipts" else "trigger"
        row = db.execute("SELECT type,sql FROM sqlite_master WHERE name=?", (name,)).fetchone()
        if (row is None or row[0] != kind or not isinstance(row[1], str)
                or normalized(row[1]) != normalized(expected)):
            raise ValueError("pause start receipt schema or protection changed")


@asynccontextmanager
async def _held(context):
    """Acquire and release off-loop, joining both through repeated cancellation."""
    from .delivery_pause_async import wait_owned

    acquiring = asyncio.create_task(asyncio.to_thread(context.__enter__))
    cancelled = await wait_owned(acquiring)
    if cancelled:
        try:
            acquiring.result()
        except BaseException:
            pass
        else:
            cleanup = asyncio.create_task(asyncio.to_thread(
                context.__exit__, None, None, None))
            await wait_owned(cleanup)
            cleanup.result()
        raise asyncio.CancelledError
    value = acquiring.result()
    try:
        yield value
    except BaseException:
        error = sys.exc_info()
        cleanup = asyncio.create_task(asyncio.to_thread(context.__exit__, *error))
        cancelled = await wait_owned(cleanup)
        suppressed = cleanup.result()
        if cancelled:
            raise asyncio.CancelledError from None
        if isinstance(error[1], asyncio.CancelledError) or not suppressed:
            raise
    else:
        cleanup = asyncio.create_task(asyncio.to_thread(context.__exit__, None, None, None))
        cancelled = await wait_owned(cleanup)
        cleanup.result()
        if cancelled:
            raise asyncio.CancelledError


async def send_start(store, spec, workflow_id, send):
    """Send exactly one selected start while the root admission mutex is held."""
    if (type(spec.get("pause_intake_version")) is not int
            or spec["pause_intake_version"] != 1 or "pause_control_version" in spec
            or type(spec.get("execution_authority_version")) is not int
            or spec["execution_authority_version"] != 1
            or spec.get("feature_delivery") or spec.get("feature_worker")):
        raise ValueError("start admission requires exact ordinary intake authority")
    original = await asyncio.to_thread(original_request, store, spec["run_id"])
    if original["workflow_id"] != workflow_id:
        raise ValueError("pause start is not the selected delivery workflow")

    def validate(db, row):
        if row["root_id"] != spec["run_id"]:
            raise ValueError("pause start authority changed")
        require_original_in_transaction(db, spec["run_id"],
                                        {"workflow_id": workflow_id,
                                         "execution_run_id": None}, original)

    # Epoch zero is the only initial start; queued paused work must first be
    # explicitly resumed, which supplies the current epoch via the root record.
    def epoch():
        with store._connect() as db:
            row = db.execute("SELECT epoch FROM delivery_pause_control WHERE root_id=?",
                             (spec["run_id"],)).fetchone()
            if row is None:
                raise ValueError("pause root was not created at submission")
            return row["epoch"]

    current_epoch = await asyncio.to_thread(epoch)
    async with _held(start_admission(store, spec["run_id"], workflow_id, 1,
                                     epoch=current_epoch, validate=validate,
                                     retry_selected=True)):
        return await asyncio.wait_for(send(), timeout=30)


async def bind_start(store, spec, workflow_id, execution_run_id):
    """Acknowledge an exact SDK start without projecting a running execution."""
    from .delivery_pause_async import joined_thread

    if (type(spec.get("pause_intake_version")) is not int
            or spec["pause_intake_version"] != 1 or "pause_control_version" in spec
            or type(spec.get("execution_authority_version")) is not int
            or spec["execution_authority_version"] != 1
            or spec.get("feature_delivery") or spec.get("feature_worker")):
        raise ValueError("start receipt requires exact ordinary intake authority")
    identity = {"workflow_id": workflow_id, "execution_run_id": execution_run_id}
    async with _held(_start_witness(store)) as witness:
        try:
            original = await asyncio.wait_for(
                authenticate_original_start(store, spec, identity, bounded=True), timeout=15)
        except ValueError:
            original = await asyncio.wait_for(
                authenticate_closed_original_start(store, spec, identity), timeout=15)
        return await joined_thread(_acknowledge_selected_start, store, spec, identity,
                                   original, witness)


@contextmanager
def _start_witness(store):
    """Pin a live source identity and an observer connection across SDK reads."""
    path = store.config.tracking_db
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        pinned = os.fstat(fd)
        if not stat.S_ISREG(pinned.st_mode) or pinned.st_uid != os.getuid():
            raise ValueError("pause start tracking database identity is unproven")
        db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5,
                             check_same_thread=False)
        try:
            db.execute("PRAGMA query_only=ON")
            version = db.execute("PRAGMA data_version").fetchone()[0]
            witness = (db, fd, path, pinned.st_dev, pinned.st_ino, version)
            _check_start_witness(witness)
            yield witness
        finally:
            db.close()
    finally:
        os.close(fd)


def _check_start_witness(witness):
    db, fd, path, device, inode, version = witness
    current, named = os.fstat(fd), path.lstat()
    if ((current.st_dev, current.st_ino) != (device, inode)
            or (named.st_dev, named.st_ino) != (device, inode)):
        raise ValueError("pause start tracking database was replaced")
    if db.execute("PRAGMA data_version").fetchone()[0] != version:
        raise ValueError("pause start authority changed during SDK observation")


_RAW_ORIGINAL_SQL = """SELECT 1 FROM delivery_runs AS r INDEXED BY sqlite_autoindex_delivery_runs_1
    JOIN delivery_execution_authority AS a
    INDEXED BY sqlite_autoindex_delivery_execution_authority_1 ON a.workflow_id=?
    WHERE r.run_id=? AND COALESCE(r.workflow_id,?)=?
    AND r.request_digest=? AND r.request_json=? AND r.recovery_json IS ?
    AND a.run_id=? AND a.request_digest=? AND a.request_json=?
    AND a.recovery_json IS ? AND a.start_spec_json=? AND a.start_token=?
    AND a.generation=? AND (a.execution_run_id IS NULL OR a.execution_run_id=?)"""


def _require_raw_original(db, root_id, identity, reservation):
    """Recheck bounded raw authority via primary keys, without decoding JSON."""
    workflow_id, execution_run_id = identity["workflow_id"], identity["execution_run_id"]
    row = db.execute(_RAW_ORIGINAL_SQL,
        (workflow_id, root_id, "delivery-" + root_id, workflow_id,
         reservation["request_digest"], reservation["request_json"], reservation["recovery_json"],
         root_id, reservation["request_digest"], reservation["request_json"],
         reservation["recovery_json"], reservation["start_spec_json"], reservation["start_token"],
         reservation["generation"], execution_run_id)).fetchone()
    if row is None:
        raise ValueError("pause delivery owner changed after SDK observation")


def _acknowledge_selected_start(store, spec, identity, original, witness=None):
    """Commit only the original start receipt, not a running actor or projection."""
    from .delivery_pause import _now, root_mutex

    if witness is None:
        with _start_witness(store) as local_witness:
            return _acknowledge_selected_start(store, spec, identity, original, local_witness)
    _check_start_witness(witness)

    root_id, workflow_id, execution_run_id = (
        spec["run_id"], identity["workflow_id"], identity["execution_run_id"])
    reservation = original.get("execution_authority")
    proof = original.get("sdk_start_proof")
    if (not isinstance(reservation, dict) or not isinstance(proof, dict)
            or set(proof) != {"event1_sha256"}
            or not isinstance(proof["event1_sha256"], str)
            or len(proof["event1_sha256"]) != 64
            or any(c not in "0123456789abcdef" for c in proof["event1_sha256"])):
        raise ValueError("pause start lacks exact SDK history proof")

    raw_fields = (reservation.get("request_json"), reservation.get("recovery_json"),
                  reservation.get("start_spec_json"))
    if (not isinstance(raw_fields[0], str) or not isinstance(raw_fields[2], str)
            or (raw_fields[1] is not None and not isinstance(raw_fields[1], str))
            or sum(len((value or "").encode()) for value in raw_fields) > 4 * 1024 * 1024):
        raise ValueError("pause start authority exceeds its bounded observation")

    def sha(text):
        return hashlib.sha256((text or "").encode()).hexdigest()

    fixed = (root_id, workflow_id, execution_run_id, 1, reservation["start_token"],
             reservation["request_digest"], sha(reservation["request_json"]),
             sha(reservation["recovery_json"]), sha(reservation["start_spec_json"]),
             proof["event1_sha256"])
    path = store.config.tracking_db
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        pinned = os.fstat(descriptor)
        if not stat.S_ISREG(pinned.st_mode) or pinned.st_uid != os.getuid():
            raise ValueError("pause start tracking database identity is unproven")

        def require_identity():
            current = os.fstat(descriptor)
            named = path.lstat()
            if ((current.st_dev, current.st_ino) != (pinned.st_dev, pinned.st_ino)
                    or (named.st_dev, named.st_ino) != (pinned.st_dev, pinned.st_ino)):
                raise ValueError("pause start tracking database was replaced")

        with store._connect() as db:
            db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 4 * 1024 * 1024)
            require_identity()
            version = db.execute("PRAGMA data_version").fetchone()[0]
            db.execute("BEGIN")
            _require_raw_original(db, root_id, identity, reservation)
            db.commit()
            if db.execute("PRAGMA data_version").fetchone()[0] != version:
                raise ValueError("pause start authority changed during observation")
            with root_mutex(store, root_id):
                db.execute("BEGIN IMMEDIATE")
                require_identity()
                _check_start_witness(witness)
                if db.execute("PRAGMA data_version").fetchone()[0] != version:
                    raise ValueError("pause start authority changed before acknowledgement")
                _require_raw_original(db, root_id, identity, reservation)
                require_start_receipt_schema(db)
                row = db.execute("SELECT * FROM delivery_pause_control WHERE root_id=?",
                                 (root_id,)).fetchone()
                if (row is None or row["state"] != "running" or row["workflow_id"] != workflow_id
                        or row["owner_generation"] != 1
                        or row["execution_run_id"] not in ("", execution_run_id)):
                    raise ValueError("pause start owner or fence changed")
                start = db.execute("""SELECT * FROM delivery_pause_steps WHERE root_id=?
                    AND operation_key=? AND step='start'""",
                    (root_id, "start:" + workflow_id)).fetchone()
                if (start is None or start["workflow_id"] != workflow_id
                        or start["execution_run_id"] != "" or start["owner_generation"] != 1
                        or start["epoch"] != row["epoch"]):
                    raise ValueError("pause start has no admitted original handoff")
                reservation = original.get("execution_authority")
                if reservation is None:
                    raise ValueError("pause start has no selected execution authority")
                run = db.execute("SELECT work_id FROM delivery_runs WHERE run_id=?",
                                 (root_id,)).fetchone()
                claim = db.execute("SELECT owner FROM claims WHERE work_id=?",
                                   (run["work_id"],)).fetchone()
                if claim is not None and claim["owner"] != "external:devflow:" + root_id:
                    raise ValueError("delivery work claim belongs to a successor")
                outbox = db.execute("SELECT state FROM delivery_outbox WHERE run_id=?",
                                    (root_id,)).fetchone()
                if outbox is None or outbox["state"] not in {"pending", "unknown", "sent"}:
                    raise ValueError("pause start outbox no longer owns the original admission")
                receipt = (*fixed[:4], start["epoch"], *fixed[4:])
                saved = db.execute("SELECT * FROM delivery_pause_start_receipts WHERE root_id=?",
                                   (root_id,)).fetchone()
                if saved is not None and tuple(saved) != receipt:
                    raise ValueError("pause start receipt changed")
                if (saved is None) != (outbox["state"] != "sent"):
                    raise ValueError("pause start receipt and outbox do not have atomic provenance")
                if saved is None:
                    db.execute("INSERT INTO delivery_pause_start_receipts "
                               "VALUES (?,?,?,?,?,?,?,?,?,?,?)", receipt)
                db.execute("UPDATE delivery_execution_authority SET execution_run_id=? "
                           "WHERE workflow_id=?", (execution_run_id, workflow_id))
                if outbox["state"] != "sent":
                    db.execute("UPDATE delivery_outbox SET state='sent',last_error=NULL,"
                               "updated_at=? WHERE run_id=?", (_now(), root_id))
                db.commit()  # Keep the root mutex until all three writes are durable.
                return {"state": "start_recorded", "workflow_id": workflow_id,
                        "execution_run_id": execution_run_id}
    finally:
        os.close(descriptor)
