"""Seal closed Temporal histories so recovery outlives namespace retention.

Recovery admissions authenticate a stopped run through its closed Temporal
execution. Temporal deletes a closed history once the namespace retention
expires, which made every later resume of that run impossible. A closure is
therefore sealed while Temporal still retains it, or imported later from an
authentic exported history. Sealing never writes to Temporal: the original
events, memo and result are preserved verbatim and rebuilt into exactly the
closure the live service would have returned.
"""

from __future__ import annotations

import errno
import hashlib
import json
import logging
import lzma
import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from temporalio.api.enums.v1 import WorkflowTaskFailedCause
from temporalio.client import WorkflowExecutionStatus, WorkflowHistory
from temporalio.service import RPCError, RPCStatusCode

from .contracts import canonical_json, digest
from .delivery_codec import DELIVERY_DATA_CONVERTER

logger = logging.getLogger(__name__)

WORKFLOW_TYPE = "DevflowDeliveryWorkflow"
_TERMINAL = (
    ("workflow_execution_completed_event_attributes", "COMPLETED"),
    ("workflow_execution_failed_event_attributes", "FAILED"),
    ("workflow_execution_timed_out_event_attributes", "TIMED_OUT"),
    ("workflow_execution_terminated_event_attributes", "TERMINATED"),
    ("workflow_execution_canceled_event_attributes", "CANCELED"),
    ("workflow_execution_continued_as_new_event_attributes", "CONTINUED_AS_NEW"),
)
CLOSED_STATUSES = {
    WorkflowExecutionStatus.COMPLETED: "COMPLETED",
    WorkflowExecutionStatus.FAILED: "FAILED",
    WorkflowExecutionStatus.TIMED_OUT: "TIMED_OUT",
    WorkflowExecutionStatus.TERMINATED: "TERMINATED",
    WorkflowExecutionStatus.CANCELED: "CANCELED",
}
SCHEMA = (
    """CREATE TABLE IF NOT EXISTS delivery_closures (
        workflow_id TEXT NOT NULL,
        execution_run_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        status TEXT NOT NULL,
        closed_at TEXT NOT NULL,
        request_digest TEXT,
        recovery_digest TEXT,
        history_sha256 TEXT NOT NULL,
        event_count INTEGER NOT NULL,
        source TEXT NOT NULL,
        source_sha256 TEXT,
        sealed_at TEXT NOT NULL,
        PRIMARY KEY (workflow_id, execution_run_id)
    )""",
    """CREATE TABLE IF NOT EXISTS delivery_closure_observations (
        workflow_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        state TEXT NOT NULL,
        detail TEXT,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_check_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )""",
)
_RUNNING_BACKOFF = (timedelta(minutes=1), timedelta(hours=1))
_ERROR_BACKOFF = (timedelta(seconds=30), timedelta(hours=1))


class SealConflict(ValueError):
    """A different history is already sealed for the same execution."""


class ClosureUnavailable(ValueError):
    """Temporal no longer retains a closed execution and nothing sealed it."""

    def __init__(self, workflow_id: str):
        super().__init__(
            "continuation predecessor Temporal closure is unproven: Temporal no longer "
            f"retains workflow {workflow_id} and no sealed closure exists; import its "
            "exported history with `devflow-delivery import-closure`"
        )
        self.workflow_id = workflow_id


def _now() -> datetime:
    return datetime.now(UTC)


def not_found(exc: BaseException) -> bool:
    return isinstance(exc, RPCError) and exc.status == RPCStatusCode.NOT_FOUND


async def closure_facts(history: WorkflowHistory) -> dict[str, Any]:
    """Validate a complete delivery history and decode the facts it closes with."""
    events = list(history.events)
    if not events:
        raise ValueError("workflow history has no events")
    for expected, event in enumerate(events, start=1):
        if event.event_id != expected:
            raise ValueError(f"workflow history is not contiguous at event {expected}")
    first = events[0]
    if not first.HasField("workflow_execution_started_event_attributes"):
        raise ValueError("workflow history does not begin with its start event")
    start = first.workflow_execution_started_event_attributes
    if start.workflow_type.name != WORKFLOW_TYPE:
        raise ValueError(
            f"workflow history has type {start.workflow_type.name!r}, not {WORKFLOW_TYPE}"
        )
    if start.workflow_id and start.workflow_id != history.workflow_id:
        raise ValueError(
            f"workflow history starts workflow {start.workflow_id}, not {history.workflow_id}"
        )
    if start.continued_execution_run_id:
        raise ValueError("workflow history continues another execution")
    status = None
    for index, event in enumerate(events, start=1):
        closing = next((name for field, name in _TERMINAL if event.HasField(field)), None)
        if closing and index != len(events):
            raise ValueError(f"workflow history closes at event {index} before its end")
        status = closing
    if status is None:
        raise ValueError("workflow history is incomplete: its last event does not close it")
    if status == "CONTINUED_AS_NEW":
        raise ValueError("workflow history continued as new instead of closing")
    memo: dict[str, Any] = {}
    for key in ("request_digest", "recovery_digest"):
        field = start.memo.fields[key] if key in start.memo.fields else None
        memo[key] = (await DELIVERY_DATA_CONVERTER.decode([field]))[0] if field else None
    inputs = (
        await DELIVERY_DATA_CONVERTER.decode(list(start.input.payloads))
        if start.input.payloads else []
    )
    result = None
    if status == "COMPLETED":
        payloads = list(events[-1].workflow_execution_completed_event_attributes.result.payloads)
        result = (await DELIVERY_DATA_CONVERTER.decode(payloads))[0] if payloads else None
    # A reset preserves the original start event. Its server-written fork event
    # carries the new execution identity; never seal it under its predecessor.
    execution_run_id = start.original_execution_run_id
    for event in events:
        if event.HasField("workflow_task_failed_event_attributes"):
            fork = event.workflow_task_failed_event_attributes
            if fork.new_run_id:
                if (not fork.base_run_id or fork.cause !=
                        WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW):
                    raise ValueError("workflow reset identity has no server fork event")
                execution_run_id = fork.new_run_id
    return {
        "workflow_id": history.workflow_id,
        "execution_run_id": execution_run_id,
        "status": status,
        "closed_at": events[-1].event_time.ToDatetime(tzinfo=UTC).isoformat(),
        "request_digest": memo["request_digest"],
        "recovery_digest": memo["recovery_digest"],
        "inputs": inputs,
        "result": result,
        "event_count": len(events),
    }


def history_json(history: WorkflowHistory) -> bytes:
    """Canonical bytes of a history; protobuf JSON emits map entries in arbitrary order."""
    return canonical_json(json.loads(history.to_json())).encode("utf-8")


def history_sha256(history: WorkflowHistory) -> str:
    return hashlib.sha256(history_json(history)).hexdigest()


def _private_root(store) -> Path:
    root = Path(store.config.state_root) / "closures"
    try:
        root.mkdir(mode=0o700)
    except FileExistsError:
        pass
    info = root.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError("sealed closure directory is not an owned directory")
    if stat.S_IMODE(info.st_mode) != 0o700:
        os.chmod(root, 0o700)
    return root


def _read_sealed(root: Path, sha256: str) -> bytes:
    path = root / f"{sha256}.json.xz"
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise ValueError("sealed closure history is a symlink") from exc
        raise
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            raise ValueError("sealed closure history is not an owned file")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            data = lzma.decompress(stream.read())
    finally:
        os.close(descriptor)
    if hashlib.sha256(data).hexdigest() != sha256:
        raise ValueError("sealed closure history does not match its recorded digest")
    return data


def _write_sealed(root: Path, data: bytes) -> str:
    sha256 = hashlib.sha256(data).hexdigest()
    path = root / f"{sha256}.json.xz"
    if path.exists() or path.is_symlink():
        _read_sealed(root, sha256)
        return sha256
    temporary = root / f".{sha256}.{os.getpid()}.new"
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600,
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(lzma.compress(data))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()
    return sha256


def sealed_record(store, workflow_id: str) -> dict[str, Any] | None:
    with store._connect() as db:
        rows = db.execute(
            "SELECT * FROM delivery_closures WHERE workflow_id=?", (workflow_id,),
        ).fetchall()
    if len(rows) > 1:
        raise ValueError(f"several sealed executions share workflow {workflow_id}")
    return dict(rows[0]) if rows else None


def seal(
    store, history: WorkflowHistory, facts: dict[str, Any], *, run_id: str, source: str,
    source_sha256: str | None = None,
) -> dict[str, Any]:
    """Record one closed history once; a different history for it is refused."""
    if source not in {"temporal", "archive"}:
        raise ValueError("sealed closure source must be temporal or archive")
    data = history_json(history)
    sha256 = _write_sealed(_private_root(store), data)
    record = {
        "workflow_id": facts["workflow_id"],
        "execution_run_id": facts["execution_run_id"],
        "run_id": run_id,
        "status": facts["status"],
        "closed_at": facts["closed_at"],
        "request_digest": facts["request_digest"],
        "recovery_digest": facts["recovery_digest"],
        "history_sha256": sha256,
        "event_count": facts["event_count"],
        "source": source,
        "source_sha256": source_sha256,
        "sealed_at": _now().isoformat(),
    }
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        existing = db.execute(
            "SELECT * FROM delivery_closures WHERE workflow_id=?", (record["workflow_id"],),
        ).fetchone()
        if existing is not None:
            existing = dict(existing)
            # Delivery workflow IDs are never reused (REJECT_DUPLICATE), so a workflow
            # has exactly one closed execution; anything else is refused, not shadowed.
            if (existing["execution_run_id"] != record["execution_run_id"]
                    or existing["history_sha256"] != sha256 or existing["run_id"] != run_id):
                raise SealConflict(
                    f"a different history is already sealed for workflow {record['workflow_id']}"
                )
            return existing
        db.execute(
            f"INSERT INTO delivery_closures ({','.join(record)}) "
            f"VALUES ({','.join('?' for _ in record)})",
            tuple(record.values()),
        )
        db.execute(
            "DELETE FROM delivery_closure_observations WHERE workflow_id=?",
            (record["workflow_id"],),
        )
    return record


async def sealed_closure(store, workflow_id: str) -> dict[str, Any] | None:
    """Rebuild the closure a live describe/result would return, from the sealed copy."""
    record = sealed_record(store, workflow_id)
    if record is None:
        return None
    data = _read_sealed(_private_root(store), record["history_sha256"])
    history = WorkflowHistory.from_json(workflow_id, data.decode("utf-8"))
    facts = await closure_facts(history)
    for key in ("execution_run_id", "status", "request_digest", "recovery_digest",
                "event_count"):
        if facts[key] != record[key]:
            raise ValueError(f"sealed closure history disagrees with its record ({key})")
    return {
        "workflow_id": workflow_id,
        "execution_run_id": record["execution_run_id"],
        "closed_at": record["closed_at"],
        "request_digest": record["request_digest"],
        "recovery_digest": record["recovery_digest"],
        "status": record["status"],
        "result": facts["result"],
        "history": history,
    }


async def seal_from_temporal(store, client, description, *, run_id: str) -> dict[str, Any]:
    """Seal a retained closed execution unless the same execution is already sealed."""
    import asyncio

    status = CLOSED_STATUSES.get(description.status)
    if status is None or not description.close_time:
        raise ValueError(f"workflow {description.id} is not closed")

    def existing() -> dict[str, Any] | None:
        with store._connect() as db:
            row = db.execute(
                "SELECT * FROM delivery_closures WHERE workflow_id=? AND execution_run_id=?",
                (description.id, description.run_id),
            ).fetchone()
        return dict(row) if row is not None else None

    found = await asyncio.to_thread(existing)
    if found is not None:
        return found
    exact = client.get_workflow_handle(description.id, run_id=description.run_id)
    history = await asyncio.wait_for(exact.fetch_history(), timeout=60)
    facts = await closure_facts(history)
    if (facts["execution_run_id"] != description.run_id or facts["status"] != status
            or facts["closed_at"] != description.close_time.isoformat()):
        raise ValueError(f"Temporal history of {description.id} disagrees with its description")
    return await asyncio.to_thread(
        seal, store, history, facts, run_id=run_id, source="temporal",
    )


def _current_workflow(store, run_id: str, row: dict[str, Any]) -> str:
    """Only the run's own started execution can be imported for it."""
    with store._connect() as db:
        outbox = db.execute(
            "SELECT 1 FROM delivery_outbox WHERE run_id=? AND state='sent'", (run_id,),
        ).fetchone()
    if outbox is None:
        raise ValueError(f"run {run_id} never started a Temporal workflow")
    return row["workflow_id"] or f"delivery-{run_id}"


async def import_archived_closure(
    store, client, *, run_id: str, workflow_id: str, history_path: Path,
) -> dict[str, Any]:
    """Seal an authentic exported history for a closure Temporal no longer retains."""
    raw = Path(history_path).read_bytes()
    source_sha256 = hashlib.sha256(raw).hexdigest()
    try:
        history = WorkflowHistory.from_json(workflow_id, raw.decode("utf-8"))
    except (ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"archived history is not a Temporal history: {exc}") from exc
    facts = await closure_facts(history)
    spec = store.effective_spec(run_id)
    if not store.owns_execution(spec):
        raise ValueError("this service does not own the run's frozen execution")
    with store._connect() as db:
        found = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
    if found is None:
        raise ValueError(f"run {run_id} does not exist")
    row = dict(found)
    current = _current_workflow(store, run_id, row)
    if workflow_id != current:
        raise ValueError(
            f"run {run_id} is bound to workflow {current}, not {workflow_id}"
        )
    if facts["status"] not in {"COMPLETED", "FAILED"}:
        raise ValueError(f"archived execution closed {facts['status']}, not COMPLETED or FAILED")
    if facts["request_digest"] != row["request_digest"]:
        raise ValueError("archived history memo binds a different request digest")
    recovery = json.loads(row["recovery_json"]) if row["recovery_json"] else None
    if facts["recovery_digest"] != (digest(recovery) if recovery is not None else None):
        raise ValueError("archived history memo binds a different recovery digest")
    inputs = facts["inputs"]
    if (len(inputs) != 2 or not isinstance(inputs[0], dict)
            or inputs[0].get("run_id") != run_id or inputs[1] != recovery):
        raise ValueError("archived history was started with different workflow inputs")
    result = facts["result"]
    if facts["status"] == "COMPLETED":
        if not isinstance(result, dict) or result.get("run_id", run_id) != run_id:
            raise ValueError("archived history returned a result for a different run")
        if row["outcome"] in {"delivered", "blocked", "cancelled"} and (
                result.get("outcome") != row["outcome"]
                or result.get("iteration") != row["iteration"]):
            raise ValueError(
                "archived terminal result disagrees with the run's projected outcome "
                f"({result.get('outcome')}/{result.get('iteration')} vs "
                f"{row['outcome']}/{row['iteration']})"
            )
    for handle in (client.get_workflow_handle(workflow_id, run_id=facts["execution_run_id"]),
                   client.get_workflow_handle(workflow_id)):
        try:
            retained = await handle.describe()
        except Exception as exc:
            if not not_found(exc):
                raise ValueError(
                    f"Temporal retention of {workflow_id} is unverified: {exc}"
                ) from exc
        else:
            raise ValueError(
                f"Temporal still retains workflow {workflow_id} (run {retained.run_id}); "
                "it is sealed from Temporal directly"
            )
    record = seal(store, history, facts, run_id=run_id, source="archive",
                  source_sha256=source_sha256)
    logger.info("Imported archived closure %s/%s for run %s", workflow_id,
                facts["execution_run_id"], run_id)
    return record


def _due_candidates(store, limit: int) -> list[dict[str, Any]]:
    now = _now().isoformat()
    with store._connect() as db:
        rows = db.execute(
            """SELECT r.run_id, COALESCE(r.workflow_id, 'delivery-' || r.run_id) AS workflow_id,
                      r.request_digest
               FROM delivery_runs r JOIN delivery_outbox o ON o.run_id = r.run_id
               WHERE o.state = 'sent'
                 AND NOT EXISTS (SELECT 1 FROM delivery_closures c
                     WHERE c.workflow_id = COALESCE(r.workflow_id, 'delivery-' || r.run_id))
                 AND NOT EXISTS (SELECT 1 FROM delivery_closure_observations b
                     WHERE b.workflow_id = COALESCE(r.workflow_id, 'delivery-' || r.run_id)
                       AND (b.state = 'expired' OR b.next_check_at > ?))
               ORDER BY r.created_at LIMIT ?""",
            (now, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def _observe(store, workflow_id: str, run_id: str, state: str, detail: str | None) -> None:
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        previous = db.execute(
            "SELECT attempts FROM delivery_closure_observations WHERE workflow_id=?",
            (workflow_id,),
        ).fetchone()
        attempts = (previous["attempts"] if previous else 0) + 1
        low, high = _RUNNING_BACKOFF if state == "running" else _ERROR_BACKOFF
        delay = min(high, low * (2 ** min(attempts - 1, 16)))
        now = _now()
        db.execute(
            """INSERT INTO delivery_closure_observations
               (workflow_id, run_id, state, detail, attempts, next_check_at, updated_at)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(workflow_id) DO UPDATE SET state=excluded.state,
                 detail=excluded.detail, attempts=excluded.attempts,
                 next_check_at=excluded.next_check_at, updated_at=excluded.updated_at""",
            (workflow_id, run_id, state, (detail or "")[:600] or None, attempts,
             (now + delay).isoformat(), now.isoformat()),
        )


async def seal_closed_executions(store, client, *, limit: int = 10) -> list[dict[str, Any]]:
    """Seal closed executions of started runs while Temporal still retains them."""
    import asyncio

    results = []
    for row in await asyncio.to_thread(_due_candidates, store, limit):
        workflow_id, run_id = row["workflow_id"], row["run_id"]
        try:
            spec = await asyncio.to_thread(store.effective_spec, run_id)
            if not await asyncio.to_thread(store.owns_execution, spec):
                # Another service's (or an unloadable) run: back off so it cannot keep
                # occupying the batch ahead of runs this service must seal.
                await asyncio.to_thread(_observe, store, workflow_id, run_id, "unowned",
                                        "run is not owned by this service's transport")
                results.append({"run_id": run_id, "workflow_id": workflow_id,
                                "state": "unowned"})
                continue
            handle = client.get_workflow_handle(workflow_id)
            description = await asyncio.wait_for(handle.describe(), timeout=15)
            if description.status not in CLOSED_STATUSES:
                await asyncio.to_thread(_observe, store, workflow_id, run_id, "running", None)
                results.append({"run_id": run_id, "workflow_id": workflow_id,
                                "state": "running"})
                continue
            memo = await description.memo_value("request_digest", None)
            if memo != row["request_digest"]:
                await asyncio.to_thread(_observe, store, workflow_id, run_id, "foreign",
                                        "request digest differs from the run")
                results.append({"run_id": run_id, "workflow_id": workflow_id,
                                "state": "foreign"})
                continue
            record = await seal_from_temporal(store, client, description, run_id=run_id)
            results.append({"run_id": run_id, "workflow_id": workflow_id,
                            "state": "sealed", "status": record["status"]})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not_found(exc):
                await asyncio.to_thread(_observe, store, workflow_id, run_id, "expired",
                                        "Temporal no longer retains this execution")
                logger.warning(
                    "Run %s: Temporal no longer retains %s and it was never sealed; "
                    "import its exported history with `devflow-delivery import-closure`",
                    run_id, workflow_id,
                )
                results.append({"run_id": run_id, "workflow_id": workflow_id,
                                "state": "expired"})
                continue
            logger.warning("Run %s: sealing %s failed (%s)", run_id, workflow_id,
                           type(exc).__name__)
            await asyncio.to_thread(_observe, store, workflow_id, run_id, "error",
                                    f"{type(exc).__name__}: {exc}")
            results.append({"run_id": run_id, "workflow_id": workflow_id, "state": "error"})
    return results
