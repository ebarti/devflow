"""Canonical merge closure and SDK-origin generation custody.

An original delivered workflow, a merge grant, and each merge execution have
different lifetimes. No caller-selected execution or standalone file grants
permission to replace the current merge or write through its run-wide row.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat

from .contracts import digest

FIELD = "merge_execution"
PATCH = "ordinary-merge-execution-v1"
_ORIGIN = ContextVar("devflow_merge_execution", default=None)


@contextmanager
def existing_lease(config, *, exclusive=False):
    """Use the original admission fence without creating a missing witness."""
    from .delivery_feature_execution import registry_path
    from .delivery_resources import _ancestors

    path = registry_path(config).parent / "installation-recovery.lock"
    _ancestors(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)

    def authenticate():
        info, named = os.fstat(descriptor), path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
                or (info.st_dev, info.st_ino) != (named.st_dev, named.st_ino)):
            raise ValueError("merge execution lease lost its original private inode")

    try:
        authenticate()
        try:
            fcntl.flock(descriptor, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("predecessor merge execution still owns its effect lease") from exc
        authenticate()
        yield
        authenticate()
    finally:
        os.close(descriptor)


async def to_thread(store, spec, function, *args, **kwargs):
    """A cancelled asyncio waiter cannot drop its still-running body's lease."""
    def execute():
        with existing_lease(store.config):
            with store._connect() as db:
                require_current(db, spec, required=True)
            return function(*args, **kwargs)
    return await asyncio.to_thread(execute)


def identity():
    value = _ORIGIN.get()
    if value is None or value["purpose"] != "activity":
        raise ValueError("merge activity has no authenticated execution generation")
    return dict(value["execution"])


def require_current(db, spec, *, required=False, phase=None):
    value = _ORIGIN.get()
    if value is None:
        if required:
            raise ValueError("merge activity has no authenticated execution generation")
        return
    saved = db.execute("SELECT * FROM delivery_merge_requests WHERE run_id=?", (spec["run_id"],)).fetchone()
    if (
        not saved
        or value["run_id"] != spec["run_id"]
        or value["spec_sha256"] != digest(spec)
        or saved["workflow_id"] != value["execution"]["workflow_id"]
        or digest(json.loads(saved["grant_json"])) != value["grant_sha256"]
        or json.loads(saved["grant_json"])["spec_sha256"] != digest(spec)
        or value["purpose"] == "dispatch" and phase != "queued"
    ):
        raise ValueError("merge execution generation changed")


@contextmanager
def activity_scope(store, request):
    from temporalio import activity

    info = activity.info()  # SDK context only; never default to the current row.
    execution = {"workflow_id": info.workflow_id, "execution_run_id": info.workflow_run_id}
    if (
        info.workflow_namespace != store.config.raw.get("temporal_namespace", "default")
        or not all(isinstance(v, str) and v for v in execution.values())
        or FIELD in request and request[FIELD] != execution
    ):
        raise ValueError("merge activity SDK execution identity differs from its request")
    value = {"purpose": "activity", "execution": execution,
             "run_id": request["spec"]["run_id"], "spec_sha256": digest(request["spec"]),
             "grant_sha256": request["merge_authorization"]}
    token = _ORIGIN.set(value)
    try:
        with existing_lease(store.config):
            with store._connect() as db:
                require_current(db, request["spec"], required=True)
            yield
    finally:
        _ORIGIN.reset(token)


@contextmanager
def dispatch_scope(store, spec, selected):
    """Only the exact selected durable start may record its queued error."""
    value = {"purpose": "dispatch", "execution": {"workflow_id": selected["workflow_id"]},
             "run_id": spec["run_id"], "spec_sha256": digest(spec),
             "grant_sha256": digest(json.loads(selected["grant_json"]))}
    token = _ORIGIN.set(value)
    try:
        with existing_lease(store.config):
            with store._connect() as db:
                require_current(db, spec, required=True, phase="queued")
            yield
    finally:
        _ORIGIN.reset(token)


async def _closed(store, spec, saved):
    from temporalio.client import Client, WorkflowExecutionStatus, WorkflowHistory

    from .delivery_closures import _TERMINAL, history_json
    from .delivery_codec import DELIVERY_DATA_CONVERTER
    from .delivery_retained_closures import _bounded_payloads

    namespace = store.config.raw.get("temporal_namespace", "default")
    client = await Client.connect(store.config.temporal_address, namespace=namespace,
                                  data_converter=DELIVERY_DATA_CONVERTER)
    handle = client.get_workflow_handle(saved["workflow_id"])
    description = await handle.describe()
    if (
        description.id != saved["workflow_id"]
        or description.workflow_type != "DevflowMergeWorkflow"
        or not description.run_id
        or description.status != WorkflowExecutionStatus.COMPLETED
        or not description.close_time
    ):
        raise ValueError("predecessor merge execution is not authentically closed")
    exact = client.get_workflow_handle(description.id, run_id=description.run_id)
    iterator = exact.fetch_history_events(page_size=128, skip_archival=True,
                                          rpc_timeout=timedelta(seconds=10))
    events, tokens = [], set()
    total = 0
    for _ in range(128):
        await iterator.fetch_next_page()
        page = iterator.current_page or []
        total += sum(event.ByteSize() for event in page)
        if total > 32 * 1024**2 or len(events) + len(page) > 10000:
            raise ValueError("merge closure exceeds its full history byte/event bound")
        events.extend(page)
        if iterator.next_page_token is None:
            break
        if iterator.next_page_token in tokens:
            raise ValueError("merge closure repeats a history page token")
        tokens.add(iterator.next_page_token)
    else:
        raise ValueError("merge closure exceeds its full history page bound")
    if not events or any(e.event_id != n for n, e in enumerate(events, 1)):
        raise ValueError("merge closure history is empty or incomplete")
    first, last = events[0], events[-1]
    if not first.HasField("workflow_execution_started_event_attributes") or not last.HasField(
        "workflow_execution_completed_event_attributes"
    ):
        raise ValueError("merge closure lacks its exact start or completed end")
    start = first.workflow_execution_started_event_attributes
    if (
        start.workflow_type.name != "DevflowMergeWorkflow"
        or start.workflow_id not in ("", saved["workflow_id"])
        or start.original_execution_run_id != description.run_id
        or start.continued_execution_run_id
        or set(start.memo.fields) != {"merge_authorization"}
        or last.event_time.ToDatetime(tzinfo=UTC) != description.close_time
        or any(e.HasField(name) for e in events[:-1] for name, _ in _TERMINAL)
        or any(e.HasField("workflow_task_failed_event_attributes")
               and e.workflow_task_failed_event_attributes.new_run_id for e in events)
    ):
        raise ValueError("merge closure has a foreign, reset, or changed execution")
    history = WorkflowHistory(saved["workflow_id"], events)
    _bounded_payloads(history)  # Existing finite expansion proof before codec decode.
    grant = digest(json.loads(saved["grant_json"]))
    inputs = await DELIVERY_DATA_CONVERTER.decode(list(start.input.payloads))
    memo = await DELIVERY_DATA_CONVERTER.decode([start.memo.fields["merge_authorization"]])
    payloads = list(last.workflow_execution_completed_event_attributes.result.payloads)
    result = await DELIVERY_DATA_CONVERTER.decode(payloads)
    if inputs != [spec, grant] or memo != [grant] or result != [json.loads(saved["result_json"] or "null")]:
        raise ValueError("merge closure differs from its exact original spec, grant or result")
    raw = history_json(history)
    if len(raw) > 64 * 1024**2:
        raise ValueError("merge closure JSON exceeds its bound")
    return {"workflow_id": description.id, "execution_run_id": description.run_id,
            "namespace": namespace, "workflow_type": "DevflowMergeWorkflow", "status": "COMPLETED",
            "closed_at": description.close_time.isoformat(), "grant_sha256": grant,
            "original_spec_sha256": digest(spec), "result_sha256": digest(result[0]),
            "history_sha256": hashlib.sha256(raw).hexdigest(), "event_count": len(events)}


def predecessor_closure(store, spec, saved):
    """Read-only configured canonical SDK evidence; absent/unknown never passes."""
    if not store.owns_execution(spec):
        raise ValueError("merge closure transport differs from its original authority")
    try:
        return asyncio.run(asyncio.wait_for(_closed(store, spec, saved), timeout=30))
    except (TimeoutError, RuntimeError) as exc:
        raise ValueError("predecessor merge closure is unavailable") from exc
