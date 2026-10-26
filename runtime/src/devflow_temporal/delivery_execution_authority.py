"""Durable selection and verification of an ordinary Temporal execution."""

from __future__ import annotations

import asyncio
import json
import secrets
from contextvars import ContextVar
from datetime import timedelta

from .contracts import canonical_json, digest

VERSION = "execution_authority_version"
MEMO = "delivery_start_authority"
_ACTIVITY_AUTHORITY = ContextVar("delivery_execution_authority", default=None)


def activity_authority():
    value = _ACTIVITY_AUTHORITY.get()
    if value is None:
        raise ValueError("activity has no authenticated delivery execution")
    return dict(value)


def initialize(db):
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_execution_authority (
        workflow_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES delivery_runs(run_id),
        request_digest TEXT NOT NULL, request_json TEXT NOT NULL, recovery_json TEXT,
        start_spec_json TEXT NOT NULL, start_token TEXT NOT NULL,
        execution_run_id TEXT, generation INTEGER NOT NULL DEFAULT 1)""")


def _current(db, run_id):
    row = db.execute(
        """SELECT r.request_digest,r.request_json,r.recovery_json,r.workflow_id,r.work_id,
        o.state AS outbox_state FROM delivery_runs r JOIN delivery_outbox o USING(run_id)
        WHERE r.run_id=?""",
        (run_id,),
    ).fetchone()
    if row is None:
        raise ValueError("delivery execution has no durable outbox owner")
    return dict(row)


def _selected(row, run_id):
    return row["workflow_id"] or "delivery-" + run_id


def _require_reservation(row, saved, run_id):
    if (
        not saved
        or saved["run_id"] != run_id
        or saved["workflow_id"] != _selected(row, run_id)
        or saved["request_digest"] != row["request_digest"]
        or saved["request_json"] != row["request_json"]
        or saved["recovery_json"] != row["recovery_json"]
    ):
        raise ValueError("delivery execution owner or original request changed")


def reserve(store, item, spec):
    """Reserve an unpredictable start identity under the outbox owner CAS."""
    run_id = spec["run_id"]
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = _current(db, run_id)
        workflow_id = _selected(row, run_id)
        if (
            workflow_id != (item["workflow_id"] or "delivery-" + run_id)
            or any(row[k] != item[k] for k in ("request_digest", "request_json", "recovery_json"))
            or row["outbox_state"] not in ("pending", "unknown")
        ):
            raise ValueError("delivery outbox owner changed before start reservation")
        saved = db.execute(
            "SELECT * FROM delivery_execution_authority WHERE workflow_id=?", (workflow_id,)
        ).fetchone()
        if saved is None:
            db.execute(
                """INSERT INTO delivery_execution_authority
                (workflow_id,run_id,request_digest,request_json,recovery_json,start_spec_json,start_token)
                VALUES (?,?,?,?,?,?,?)""",
                (
                    workflow_id,
                    run_id,
                    row["request_digest"],
                    row["request_json"],
                    row["recovery_json"],
                    canonical_json(spec),
                    secrets.token_hex(32),
                ),
            )
            saved = db.execute(
                "SELECT * FROM delivery_execution_authority WHERE workflow_id=?", (workflow_id,)
            ).fetchone()
        saved = dict(saved)
        _require_reservation(row, saved, run_id)
        return saved


def read_reservation(store, run_id, workflow_id):
    with store._connect() as db:
        row = _current(db, run_id)
        saved = db.execute(
            "SELECT * FROM delivery_execution_authority WHERE workflow_id=?", (workflow_id,)
        ).fetchone()
        saved = dict(saved) if saved else None
        _require_reservation(row, saved, run_id)
        return saved


async def authenticate_original_start(
    store, reservation, execution_run_id, *, client=None, require_running=True
):
    """Authenticate bounded event 1 of the exact run, never the latest memo alone."""
    from temporalio.client import Client, WorkflowExecutionStatus, WorkflowHistory

    from .delivery_codec import DELIVERY_DATA_CONVERTER
    from .delivery_retained_closures import _bounded_payloads

    if not isinstance(execution_run_id, str) or not execution_run_id:
        raise ValueError("delivery execution has no SDK run identity")
    if client is None:
        client = await Client.connect(
            store.config.temporal_address,
            namespace=store.config.raw.get("temporal_namespace", "default"),
            data_converter=DELIVERY_DATA_CONVERTER,
        )
    workflow_id = reservation["workflow_id"]
    handle = client.get_workflow_handle(workflow_id, run_id=execution_run_id)
    description = await handle.describe()
    if (
        description.id != workflow_id
        or description.run_id != execution_run_id
        or description.workflow_type != "DevflowDeliveryWorkflow"
        or (require_running and description.status != WorkflowExecutionStatus.RUNNING)
    ):
        raise ValueError("delivery execution lacks its original running SDK start")
    history = handle.fetch_history_events(
        page_size=1, skip_archival=True, rpc_timeout=timedelta(seconds=5)
    )
    await history.fetch_next_page()
    page = history.current_page or []
    if (
        not page
        or page[0].event_id != 1
        or page[0].ByteSize() > 32 * 1024**2
        or not page[0].HasField("workflow_execution_started_event_attributes")
    ):
        raise ValueError("delivery execution has no bounded original start history")
    start = page[0].workflow_execution_started_event_attributes
    if (
        start.workflow_type.name != "DevflowDeliveryWorkflow"
        or start.task_queue.name != store.config.queue
        or start.workflow_id not in ("", workflow_id)
        or start.original_execution_run_id != execution_run_id
        or start.continued_execution_run_id
    ):
        raise ValueError("delivery execution cannot adopt a reset or foreign start")
    _bounded_payloads(WorkflowHistory(workflow_id, [page[0]]))
    recovery = json.loads(reservation["recovery_json"]) if reservation["recovery_json"] else None
    memo = {"request_digest": reservation["request_digest"], MEMO: reservation["start_token"]}
    if recovery:
        memo["recovery_digest"] = digest(recovery)
    if set(start.memo.fields) != set(memo):
        raise ValueError("delivery start memo differs from its reservation")
    for key, value in memo.items():
        if await DELIVERY_DATA_CONVERTER.decode([start.memo.fields[key]]) != [value]:
            raise ValueError("delivery start memo differs from its reservation")
    inputs = await DELIVERY_DATA_CONVERTER.decode(list(start.input.payloads))
    if inputs != [json.loads(reservation["start_spec_json"]), recovery]:
        raise ValueError("delivery start inputs differ from its reservation")


def bind(store, reservation, execution_run_id):
    """Commit an authenticated observation against the current owner."""
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = _current(db, reservation["run_id"])
        claim = db.execute("SELECT owner FROM claims WHERE work_id=?", (row["work_id"],)).fetchone()
        if claim is not None and claim["owner"] != "external:devflow:" + reservation["run_id"]:
            raise ValueError("delivery work claim belongs to a successor")
        current = db.execute(
            "SELECT * FROM delivery_execution_authority WHERE workflow_id=?",
            (reservation["workflow_id"],),
        ).fetchone()
        current = dict(current) if current else None
        _require_reservation(row, current, reservation["run_id"])
        if any(
            current[key] != value for key, value in reservation.items() if key != "execution_run_id"
        ):
            raise ValueError("delivery start reservation changed after SDK observation")
        if current["execution_run_id"] not in (None, execution_run_id):
            raise ValueError("delivery execution differs from its original SDK binding")
        db.execute(
            "UPDATE delivery_execution_authority SET execution_run_id=? WHERE workflow_id=?",
            (execution_run_id, reservation["workflow_id"]),
        )
        return {
            "workflow_id": reservation["workflow_id"],
            "execution_run_id": execution_run_id,
            "run_id": reservation["run_id"],
            "request_digest": reservation["request_digest"],
            "generation": reservation["generation"],
        }


def require_current(db, spec, authority, *, require_claim=True):
    """Read-only check for use under the caller's held tracking transaction.

    The caller must retain this transaction through GO and compose it with the
    registry assignment and debit fences. This check performs no RPC or writes.
    """
    if spec.get(VERSION) != 1:
        raise ValueError("legacy delivery has no exact execution proof")
    if (
        not isinstance(authority, dict)
        or set(authority)
        != {"workflow_id", "execution_run_id", "run_id", "request_digest", "generation"}
        or authority["run_id"] != spec.get("run_id")
    ):
        raise ValueError("delivery handoff lacks authenticated execution authority")
    row = _current(db, authority["run_id"])
    if (
        json.loads(row["request_json"]).get(VERSION) != 1
        or spec.get("request_digest") != row["request_digest"]
    ):
        raise ValueError("delivery handoff differs from its original request")
    claim = db.execute("SELECT owner FROM claims WHERE work_id=?", (row["work_id"],)).fetchone()
    if (require_claim and claim is None) or (
        claim is not None and claim["owner"] != "external:devflow:" + authority["run_id"]
    ):
        raise ValueError("delivery work claim belongs to a successor or is unavailable")
    saved = db.execute(
        "SELECT * FROM delivery_execution_authority WHERE workflow_id=?",
        (authority["workflow_id"],),
    ).fetchone()
    saved = dict(saved) if saved else None
    _require_reservation(row, saved, authority["run_id"])
    if (
        saved["execution_run_id"] != authority["execution_run_id"]
        or not saved["execution_run_id"]
        or saved["request_digest"] != authority["request_digest"]
        or saved["generation"] != authority["generation"]
    ):
        raise ValueError("delivery handoff execution binding changed")
    return dict(authority)


def require_activity_current(db, run_id):
    """Fence same-process activity mutations under the caller's transaction."""
    value = _ACTIVITY_AUTHORITY.get()
    if value is None:
        return
    if value["run_id"] != run_id:
        raise ValueError("delivery activity cannot mutate another run")
    row = _current(db, run_id)
    require_current(db, json.loads(row["request_json"]), value, require_claim=False)


async def authenticate_activity(store, spec):
    """Select identity from SDK context only; request fields cannot select it."""
    from temporalio import activity

    info = activity.info()
    if (
        info.workflow_type != "DevflowDeliveryWorkflow"
        or info.namespace != store.config.raw.get("temporal_namespace", "default")
        or info.task_queue != store.config.queue
        or not info.workflow_id
        or not info.workflow_run_id
        or not store.owns_execution(spec)
    ):
        raise ValueError("delivery activity lacks its configured SDK execution")
    reservation = await asyncio.to_thread(read_reservation, store, spec["run_id"], info.workflow_id)
    if reservation["execution_run_id"] is None:
        await asyncio.wait_for(
            authenticate_original_start(store, reservation, info.workflow_run_id), 15
        )
    return await asyncio.to_thread(bind, store, reservation, info.workflow_run_id)
