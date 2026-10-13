"""Resume an interrupted controller from its retained Temporal history.

Temporal's reset forks the original history; it does not reset application repair
budgets or manufacture a completed result. Completed activity results before the
resume point are replayed. Native/effect journals still guard repeated activities.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import ResetReapplyType, WorkflowTaskFailedCause
from temporalio.api.workflowservice.v1 import ResetWorkflowExecutionRequest
from temporalio.common import RetryPolicy, WorkflowIDConflictPolicy, WorkflowIDReusePolicy

from .contracts import digest
from .delivery_closures import closure_facts, history_sha256, seal
from .delivery_codec import DELIVERY_DATA_CONVERTER
from .delivery_preparation import execution_retired
from .payload import payload_digest

INTENT = "interrupted_execution_recovery_requested"
RESULT = "interrupted_execution_recovered"
CLOSED = frozenset({"FAILED", "TIMED_OUT", "TERMINATED"})
# Repeated controller failure on unchanged code needs a repair, not an endless loop.
MAX_RECOVERIES = 3


def _records(store, run_id, kind):
    with store._connect() as db:
        return [
            json.loads(row[0])
            for row in db.execute(
                "SELECT payload_json FROM delivery_events WHERE run_id=? AND type=? "
                "ORDER BY sequence",
                (run_id, kind),
            )
        ]


def _record(store, row, kind, payload):
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        if any(
            json.loads(item[0]).get("key") == payload["key"]
            for item in db.execute(
                "SELECT payload_json FROM delivery_events WHERE run_id=? AND type=?",
                (row["run_id"], kind),
            )
        ):
            return
        current = db.execute(
            "SELECT revision FROM delivery_runs WHERE run_id=?", (row["run_id"],)
        ).fetchone()
        store._event(
            db,
            row["run_id"],
            current[0],
            kind,
            (
                "Resuming interrupted controller from retained history"
                if kind == RESULT
                else "Requested recovery of interrupted controller; repair budget retained"
            ),
            payload,
        )
        if kind == RESULT:
            db.execute(
                "DELETE FROM delivery_closure_observations WHERE workflow_id=?",
                (payload["workflow_id"],),
            )


def _guard(store, row, spec):
    if execution_retired(spec) or not store.owns_execution(spec):
        raise ValueError("interrupted execution no longer belongs to this service")
    with store._connect() as db:
        current = db.execute(
            "SELECT * FROM delivery_runs WHERE run_id=?", (row["run_id"],)
        ).fetchone()
        if current is None or any(
            current[key] != row[key]
            for key in (
                "workflow_id",
                "request_digest",
                "recovery_json",
                "protocol_revision",
                "iteration",
                "outcome",
            )
        ):
            raise ValueError("interrupted execution changed during recovery")
        if (
            row["outcome"] not in (None, "blocked")
            or db.execute(
                "SELECT 1 FROM delivery_mutations WHERE run_id=? AND "
                "(state IN ('pending','unknown') OR (kind='cancel' AND state='complete'))",
                (row["run_id"],),
            ).fetchone()
        ):
            raise ValueError("another command or cancellation fences controller recovery")
        claim = store.state.claim_for(db, spec["work_id"])
        if (
            claim is not None
            and claim["owner"] != "external:devflow:" + row["run_id"]
            or claim is None
            and row["outcome"] is None
        ):
            raise ValueError("interrupted work claim changed")
        if db.execute(
            "SELECT 1 FROM delivery_attempts WHERE run_id=? AND "
            "(state!='finished' OR cleanup!='confirmed')",
            (row["run_id"],),
        ).fetchone():
            raise ValueError("native attempt cleanup must finish before controller recovery")
        latest = next(
            (
                item["run_id"]
                for item in db.execute(
                    "SELECT run_id,issue_url FROM delivery_runs ORDER BY rowid DESC"
                )
                if store.state.issue_resource(item["issue_url"])
                == store.state.issue_resource(spec["issue_url"])
            ),
            None,
        )
        if latest != row["run_id"]:
            raise ValueError("a successor already owns this issue")
    if spec.get("feature_delivery"):
        from .delivery_feature_execution import registry, require_execution

        if spec.get("feature_worker"):
            require_execution(store, spec)
        else:
            shared = registry(spec)
            with shared.connect() as db:
                shared.require(db, spec["feature_delivery"]["owner"], active=False)
    if spec.get("provider") != "fake":
        from .delivery_orphans import _observe_registered_process
        from .delivery_resources import RunResources

        root = Path(spec["state_dir"]) / "resources"
        if root.exists():
            with RunResources(spec, read_only=True).locked() as manifest:
                paths = list(manifest["processes"])
            for path in paths:
                _observe_registered_process(spec, Path(path))
        elif any(Path(spec["state_dir"]).rglob("native-process.json")):
            raise ValueError("native process has no owning resource manifest")


def _resume_point(history):
    """Retry the failed activity's scheduling task, or the final controller task."""
    failure = history.events[-1].workflow_execution_failed_event_attributes.failure
    while failure.ByteSize():
        if failure.HasField("activity_failure_info"):
            scheduled_id = failure.activity_failure_info.scheduled_event_id
            scheduled = next(
                (event for event in history.events if event.event_id == scheduled_id), None
            )
            if scheduled and scheduled.HasField("activity_task_scheduled_event_attributes"):
                attributes = scheduled.activity_task_scheduled_event_attributes
                return attributes.workflow_task_completed_event_id
        if not failure.HasField("cause"):
            break
        failure = failure.cause
    points = [
        event.event_id
        for event in history.events
        if any(
            event.HasField(field)
            for field in (
                "workflow_task_completed_event_attributes",
                "workflow_task_started_event_attributes",
            )
        )
    ]
    # A workflow that expired while queued has never run application code. Only
    # its start, queue and close events may exist; start those exact inputs again.
    if not points:
        if any(
            not any(
                event.HasField(field)
                for field in (
                    "workflow_execution_started_event_attributes",
                    "workflow_task_scheduled_event_attributes",
                    "workflow_execution_timed_out_event_attributes",
                    "workflow_execution_terminated_event_attributes",
                )
            )
            for event in history.events
        ):
            raise ValueError("interrupted execution has no safe workflow task to resume")
        return None
    return max(points)


async def recover_interrupted(store, client, row, spec, description):
    """Called by the existing bounded maintenance sweep, also for legacy admissions."""
    intents = await asyncio.to_thread(_records, store, row["run_id"], INTENT)
    results = await asyncio.to_thread(_records, store, row["run_id"], RESULT)
    pending = [item for item in intents if not any(r["key"] == item["key"] for r in results)]
    # Resolve a lost response from server-authenticated reset lineage. Never issue
    # another reset against an active/newer execution merely to recover a response.
    if pending:
        prior = pending[-1]
        if prior["workflow_id"] != description.id:
            raise ValueError("recovery intent belongs to a different workflow")
        if prior["execution_run_id"] != description.run_id:
            history = await client.get_workflow_handle(
                description.id, run_id=description.run_id
            ).fetch_history()
            linked = any(
                event.HasField("workflow_task_failed_event_attributes")
                and event.workflow_task_failed_event_attributes.cause
                == WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW
                and event.workflow_task_failed_event_attributes.base_run_id
                == prior["execution_run_id"]
                and event.workflow_task_failed_event_attributes.new_run_id == description.run_id
                and event.event_id == prior["resume_event_id"]
                for event in history.events
            )
            if prior["resume_event_id"] is None:
                started = history.events[0].workflow_execution_started_event_attributes
                marker = started.memo.fields.get("interrupted_recovery_key")
                linked = bool(
                    marker and (await DELIVERY_DATA_CONVERTER.decode([marker]))[0] == prior["key"]
                )
            if not linked:
                raise ValueError("new execution is not the requested recovery")
            await asyncio.to_thread(
                _record, store, row, RESULT, {**prior, "recovered_run_id": description.run_id}
            )
            return {"state": "execution_recovered", "execution_run_id": description.run_id}
    if (
        description.status.name not in CLOSED
        or not description.close_time
        or row["outcome"] not in (None, "blocked")
    ):
        return None
    if (
        client.namespace != store.config.raw.get("temporal_namespace", "default")
        or client.service_client.config.target_host != store.config.temporal_address
    ):
        raise ValueError("controller recovery transport differs from the admitted owner")
    await asyncio.to_thread(_guard, store, row, spec)
    history = await client.get_workflow_handle(
        description.id, run_id=description.run_id
    ).fetch_history()
    facts = await closure_facts(history)
    recovery = json.loads(row["recovery_json"]) if row["recovery_json"] else None
    starts = [
        await asyncio.to_thread(store.submitted_spec, row["run_id"]),
        await asyncio.to_thread(store.spec, row["run_id"]),
        spec,
    ]
    expected_recovery = digest(recovery) if recovery else None
    inputs = facts["inputs"]
    if (
        facts["execution_run_id"] != description.run_id
        or facts["status"] != description.status.name
        or facts["workflow_id"] != (row["workflow_id"] or "delivery-" + row["run_id"])
        or facts["closed_at"] != description.close_time.isoformat()
        or description.workflow_type != "DevflowDeliveryWorkflow"
        or facts["request_digest"] != row["request_digest"]
        or facts["recovery_digest"] != expected_recovery
        or await description.memo_value("request_digest", None) != row["request_digest"]
        or await description.memo_value("recovery_digest", None) != expected_recovery
        or not inputs
        or inputs[0] not in starts
        or (inputs[1] if len(inputs) == 2 else None) != recovery
        or len(inputs) not in (1, 2)
    ):
        raise ValueError("interrupted history does not authenticate this admission")
    runtime = await asyncio.to_thread(payload_digest, Path(__file__).resolve().parent)
    prior = next((item for item in intents if item["execution_run_id"] == description.run_id), None)
    if prior is None:
        if sum(item["runtime_payload_sha256"] == runtime for item in intents) >= MAX_RECOVERIES:
            raise ValueError(
                "controller recovery exhausted three attempts on unchanged runtime; "
                "repair the reported controller failure before retrying"
            )
        prior = {
            "workflow_id": description.id,
            "execution_run_id": description.run_id,
            "status": facts["status"],
            "history_sha256": history_sha256(history),
            "resume_event_id": _resume_point(history),
            "runtime_payload_sha256": runtime,
        }
        prior["key"] = digest(prior)
    if prior["history_sha256"] != history_sha256(history):
        raise ValueError("interrupted history changed after recovery intent")
    await asyncio.to_thread(seal, store, history, facts, run_id=row["run_id"], source="temporal")
    await asyncio.to_thread(_guard, store, row, spec)
    fresh = await client.get_workflow_handle(description.id).describe()
    if fresh.run_id != description.run_id or fresh.status != description.status:
        raise ValueError("current execution changed before recovery dispatch")
    await asyncio.to_thread(_record, store, row, INTENT, prior)
    if prior["resume_event_id"] is None:
        start = history.events[0].workflow_execution_started_event_attributes
        memo = {
            key: (await DELIVERY_DATA_CONVERTER.decode([value]))[0]
            for key, value in start.memo.fields.items()
        }
        restarted = await client.start_workflow(
            "DevflowDeliveryWorkflow",
            args=inputs,
            id=description.id,
            task_queue=start.task_queue.name,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
            id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
            retry_policy=RetryPolicy(maximum_attempts=1),
            execution_timeout=start.workflow_execution_timeout.ToTimedelta() or None,
            run_timeout=start.workflow_run_timeout.ToTimedelta() or None,
            memo={**memo, "interrupted_recovery_key": prior["key"]},
        )
        recovered_run_id = restarted.result_run_id
    else:
        response = await client.workflow_service.reset_workflow_execution(
            ResetWorkflowExecutionRequest(
                namespace=client.namespace,
                workflow_execution=WorkflowExecution(
                    workflow_id=description.id, run_id=description.run_id
                ),
                workflow_task_finish_event_id=prior["resume_event_id"],
                request_id=prior["key"],
                reason="Devflow interrupted-controller recovery; existing repair budget retained",
                reset_reapply_type=ResetReapplyType.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
            ),
            retry=False,
        )
        recovered_run_id = response.run_id
    await asyncio.to_thread(
        _record, store, row, RESULT, {**prior, "recovered_run_id": recovered_run_id}
    )
    return {"state": "execution_recovered", "execution_run_id": recovered_run_id}
