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
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pause_denials (
        root_id TEXT NOT NULL, operation_key TEXT NOT NULL, step TEXT NOT NULL,
        epoch INTEGER NOT NULL, workflow_id TEXT NOT NULL, execution_run_id TEXT NOT NULL,
        owner_generation INTEGER NOT NULL, denied_at TEXT NOT NULL,
        PRIMARY KEY(root_id, operation_key, step, epoch))""")
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


_REFUSALS = {
    "scope": "pause control scope is not enabled by this service",
    "epoch": "invalid pause epoch",
    "revision": "invalid control revision",
    "conflict": "pause command ID reused with different inputs",
    "pause_state": "pause control state or revision changed",
    "start_admitted": "intake pause is unavailable after start admission",
    "resume_state": "resume control state, revision or epoch changed",
    "checkpoint": "checkpoint requires external effect reconciliation before resume",
    "clock": "pause workflow clock start acknowledgements are pending",
}


class PauseRefused(ValueError):
    """A named rejection established before this pause command changed state."""

    def __init__(self, reason):
        self.reason = reason
        super().__init__(_REFUSALS[reason])


def _command_digest(root_id, action, body):
    request = {"action": action, "root_id": root_id,
               "expected_revision": body["expected_revision"]}
    if action == "resume":
        request["epoch"] = body["epoch"]
    return digest(request)


def _command_identity(root_id, action, body):
    keys = {"command_id", "expected_revision"} | ({"epoch"} if action == "resume" else set())
    if (action not in {"pause", "resume"} or not isinstance(body, dict) or set(body) != keys
            or not isinstance(root_id, str) or not root_id or len(root_id) > 512
            or not isinstance(body["command_id"], str) or not body["command_id"]
            or len(body["command_id"]) > 512):
        return None
    return _command_digest(root_id, action, body)


def _authenticated_command(row):
    """Check the identity available in a historical success or refusal."""
    response = json.loads(row["response_json"])
    if not isinstance(response, dict):
        return False
    if response.get("state") == "refused":
        action, body = response.get("action"), response.get("command")
        return (isinstance(body, dict) and body.get("command_id") == row["command_id"]
                and _command_identity(row["root_id"], action, body) == row["request_digest"]
                and refusal_from_row(row, row["root_id"], action, body) is not None)
    if (set(response) != {"root_id", "state", "revision", "epoch"}
            or response["root_id"] != row["root_id"]
            or response["state"] not in {"pause_requested", "resuming"}
            or type(response["revision"]) is not int or response["revision"] < 1
            or type(response["epoch"]) is not int or response["epoch"] < 0
            or (response["state"] == "pause_requested" and response["epoch"] < 1)):
        return False
    action = "pause" if response["state"] == "pause_requested" else "resume"
    body = {"command_id": row["command_id"],
            "expected_revision": response["revision"] - 1}
    if action == "resume":
        body["epoch"] = response["epoch"]
    return _command_identity(row["root_id"], action, body) == row["request_digest"]


def refusal_from_row(row, root_id, action, body):
    """Read a bound refusal, or the immutable conflicting command occupying its ID."""
    expected = _command_identity(root_id, action, body)
    if expected is None or row is None:
        return None
    if row["request_digest"] != expected:
        # An occupied ID is evidence of conflict only if its own retained
        # identity authenticates; a damaged digest is not a refusal.
        return _REFUSALS["conflict"] if _authenticated_command(row) else None
    if row["root_id"] != root_id:
        return None
    response = json.loads(row["response_json"])
    if (not isinstance(response, dict)
            or set(response) != {"state", "root_id", "action", "command", "reason", "digest"}
            or response["state"] != "refused" or response["root_id"] != root_id
            or response["action"] != action or response["digest"] != expected
            or canonical_json(response["command"]) != canonical_json(body)
            or response["reason"] not in _REFUSALS):
        return None
    return _REFUSALS[response["reason"]]


def remember_refusal(store, root_id, action, body, reason):
    """Reserve an exactly identified no-effect command before returning its refusal."""
    expected = _command_identity(root_id, action, body)
    if expected is None or reason not in _REFUSALS:
        return None
    with root_mutex(store, root_id), store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM delivery_pause_commands WHERE command_id=?",
                         (body["command_id"],)).fetchone()
        if row is not None:
            # A concurrent successful replay must remain a success; never replace it.
            return refusal_from_row(row, root_id, action, body)
        response = {"state": "refused", "root_id": root_id, "action": action,
                    "command": body, "reason": reason, "digest": expected}
        db.execute("INSERT INTO delivery_pause_commands VALUES (?, ?, ?, ?)",
                   (body["command_id"], root_id, expected, canonical_json(response)))
        return _REFUSALS[reason]


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
        raise PauseRefused("revision")
    request_digest = digest({"action": "pause", "root_id": root_id,
                             "expected_revision": expected_revision})
    with root_mutex(store, root_id), store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        prior = db.execute("SELECT * FROM delivery_pause_commands WHERE command_id=?",
                           (command_id,)).fetchone()
        if prior is not None:
            if prior["request_digest"] != request_digest:
                raise PauseRefused("conflict")
            if not _authenticated_command(prior):
                raise ValueError("invalid pause command receipt")
            response = json.loads(prior["response_json"])
            if response["state"] == "refused":
                body = {"command_id": command_id, "expected_revision": expected_revision}
                if refusal_from_row(prior, root_id, "pause", body) is None:
                    raise ValueError("invalid pause refusal receipt")
                raise PauseRefused(response["reason"])
            return response
        row = _row(db, root_id)
        if row["revision"] != expected_revision or row["state"] != "running":
            raise PauseRefused("pause_state")
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
                raise PauseRefused("start_admitted")
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
        raise PauseRefused("revision")
    request = {"action": "resume", "root_id": root_id, "expected_revision": expected_revision,
               "epoch": epoch}
    request_digest = digest(request)
    with root_mutex(store, root_id), store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        prior = db.execute("SELECT * FROM delivery_pause_commands WHERE command_id=?",
                           (command_id,)).fetchone()
        if prior is not None:
            if prior["request_digest"] != request_digest:
                raise PauseRefused("conflict")
            if not _authenticated_command(prior):
                raise ValueError("invalid pause command receipt")
            response = json.loads(prior["response_json"])
            if response["state"] == "refused":
                body = {"command_id": command_id, "expected_revision": expected_revision,
                        "epoch": epoch}
                if refusal_from_row(prior, root_id, "resume", body) is None:
                    raise ValueError("invalid pause refusal receipt")
                raise PauseRefused(response["reason"])
            return response
        row = _row(db, root_id)
        if (row["state"] != "paused" or row["revision"] != expected_revision
                or row["epoch"] != epoch):
            raise PauseRefused("resume_state")
        checkpoint = json.loads(row["checkpoint_json"])
        if not checkpoint["resumable"]:
            raise PauseRefused("checkpoint")
        if _missing_clock(db, root_id, epoch, finished=False):
            raise PauseRefused("clock")
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
def launch_admission_graph(store, bindings, operation_key, step, *, workflow_id,
                           execution_run_id, owner_generation, validate_graph, graph_guard=None):
    """Admit one actual handoff under every authenticated ancestor fence.

    The caller owns actor and graph authentication; this internal interface does
    not accept a validator from a public request. Locks are retained through the
    caller's handoff, but the tracking write transaction is committed first.
    An admitted step cannot be repeated without a separate readback proving the
    outcome. A denial is recorded before it is returned to the owning workflow.
    """
    from contextlib import ExitStack

    if not isinstance(bindings, dict) or not bindings or not callable(validate_graph):
        raise ValueError("pause graph bindings and validator are required")
    for value, name in ((operation_key, "operation key"), (step, "step"),
                        (workflow_id, "workflow ID"), (execution_run_id, "execution run ID")):
        _nonempty(value, name)
    if type(owner_generation) is not int or owner_generation < 1:
        raise ValueError("invalid pause actor generation")
    for root_id, epoch in bindings.items():
        _nonempty(root_id, "root ID")
        _epoch(epoch)

    class Handoff:
        active = True

        def no_process_started(self):
            """Withdraw only after the immediate Popen reports a pre-exec failure."""
            if not self.active:
                raise ValueError("pause handoff no longer holds its admission locks")
            with store._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                validate_graph(db, bindings, workflow_id, execution_run_id, owner_generation)
                for root_id, epoch in sorted(bindings.items()):
                    row = _row(db, root_id)
                    actor = db.execute("""SELECT a.execution_run_id,a.owner_generation,s.state
                        FROM delivery_pause_actors a JOIN delivery_pause_actor_status s
                        ON s.root_id=a.root_id AND s.workflow_id=a.workflow_id
                        WHERE a.root_id=? AND a.workflow_id=?""",
                        (root_id, workflow_id)).fetchone()
                    if (row["state"] != "running" or row["epoch"] != epoch or actor is None
                            or actor["state"] != "running"
                            or actor["execution_run_id"] != execution_run_id
                            or actor["owner_generation"] != owner_generation):
                        raise ValueError("pause actor changed before no-launch acknowledgement")
                    removed = db.execute("""DELETE FROM delivery_pause_steps WHERE root_id=?
                        AND operation_key=? AND step=? AND epoch=? AND workflow_id=?
                        AND execution_run_id=? AND owner_generation=?""",
                        (root_id, operation_key, step, epoch, workflow_id,
                         execution_run_id, owner_generation)).rowcount
                    if removed != 1:
                        raise ValueError("pause handoff no-launch identity changed")
            self.active = False

    with ExitStack() as stack:
        for root_id in sorted(bindings):
            stack.enter_context(root_mutex(store, root_id))
        if graph_guard is not None:
            stack.enter_context(graph_guard())
        denied = None
        with store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            validate_graph(db, bindings, workflow_id, execution_run_id, owner_generation)
            controls = []
            for root_id in sorted(bindings):
                row = _row(db, root_id)
                actor = db.execute("""SELECT a.execution_run_id,a.owner_generation,s.state
                    FROM delivery_pause_actors a JOIN delivery_pause_actor_status s
                    ON s.root_id=a.root_id AND s.workflow_id=a.workflow_id
                    WHERE a.root_id=? AND a.workflow_id=?""", (root_id, workflow_id)).fetchone()
                if (actor is None or actor["execution_run_id"] != execution_run_id
                        or actor["owner_generation"] != owner_generation
                        or actor["state"] not in {"running", "resume_pending"}):
                    raise ValueError("pause graph actor changed or completed")
                controls.append((row, actor["state"]))
            for row, actor_state in controls:
                if (row["state"] == "running" and actor_state == "running"
                        and row["epoch"] == bindings[row["root_id"]]):
                    continue
                db.execute("""INSERT OR IGNORE INTO delivery_pause_denials
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", (row["root_id"], operation_key, step,
                    row["epoch"], workflow_id, execution_run_id, owner_generation, _now()))
                existing = db.execute("""SELECT * FROM delivery_pause_denials WHERE root_id=?
                    AND operation_key=? AND step=? AND epoch=?""",
                    (row["root_id"], operation_key, step, row["epoch"])).fetchone()
                if (existing["workflow_id"] != workflow_id
                        or existing["execution_run_id"] != execution_run_id
                        or existing["owner_generation"] != owner_generation):
                    raise ValueError("pause denial belongs to a different actor")
                if denied is None:
                    denied = row
            if denied is None:
                for row, _actor_state in controls:
                    old = db.execute("""SELECT 1 FROM delivery_pause_steps WHERE root_id=?
                        AND operation_key=? AND step=?""",
                        (row["root_id"], operation_key, step)).fetchone()
                    if old is not None:
                        raise ValueError("pause handoff already admitted or outcome unknown")
                    db.execute("INSERT INTO delivery_pause_steps VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                               (row["root_id"], operation_key, step, row["epoch"], workflow_id,
                                execution_run_id, owner_generation, _now()))
        if denied is not None:
            raise PauseDeferred("pause fence closed before handoff", root_id=denied["root_id"],
                                operation_key=operation_key, step=step, epoch=denied["epoch"])
        handoff = Handoff()
        try:
            yield handoff
        finally:
            handoff.active = False


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


