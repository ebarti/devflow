"""Reconcile a failed terminal projection from its authentic completed execution."""
from __future__ import annotations

import json
import logging
from copy import deepcopy
from datetime import datetime

from temporalio.client import WorkflowExecutionStatus

from .contracts import digest
from .delivery_codec import DELIVERY_DATA_CONVERTER
from .delivery_preparation import execution_retired

logger = logging.getLogger(__name__)
EVENT = "closed_projection_reconciled"
FIELDS = ("phase", "execution_state", "iteration", "outcome", "cleanup", "error")
JSON_FIELDS = {"candidate": "candidate_json", "pull_request": "pr_json",
               "checks": "checks_json", "tracker": "tracker_json", "usage": "usage_json",
               "decision": "decision_json", "intake": "intake_json"}


def _same_row(row, projection):
    return (all(row[key] == projection.get(key) for key in FIELDS)
            and row["protocol_revision"] == projection["protocol_revision"]
            and all(json.loads(row[column] or "null") == projection.get(key)
                    for key, column in JSON_FIELDS.items()))


async def _settled_feature_checks(history, spec, state, projection_id, expected):
    """Bind the coordinator's postprojection readback to its settled checkpoint."""
    readback = state.get("checks", {}).get("feature_readback")
    feature = spec.get("feature_delivery")
    if (not feature or spec.get("feature_worker") or not isinstance(readback, dict)
            or readback.get("state") != "confirmed"
            or state.get("checks") != {**expected, "feature_readback": readback}):
        raise ValueError("completed feature settlement checks changed")
    scheduled, completed = {}, {}
    for event in history.events:
        if event.HasField("activity_task_scheduled_event_attributes"):
            attributes = event.activity_task_scheduled_event_attributes
            name = attributes.activity_type.name
            if (event.event_id > projection_id and name in {
                    "delivery_feature_settle_effects", "delivery_feature_stop"}):
                values = await DELIVERY_DATA_CONVERTER.decode(attributes.input.payloads)
                if len(values) != 1:
                    raise ValueError("completed feature settlement request is malformed")
                scheduled[event.event_id] = (name, values[0])
        if event.HasField("activity_task_completed_event_attributes"):
            attributes = event.activity_task_completed_event_attributes
            if attributes.scheduled_event_id in scheduled:
                values = await DELIVERY_DATA_CONVERTER.decode(attributes.result.payloads)
                if len(values) != 1:
                    raise ValueError("completed feature settlement result is malformed")
                completed[attributes.scheduled_event_id] = (event.event_id, values[0])
    readbacks = [key for key, (name, request) in scheduled.items()
                 if name == "delivery_feature_settle_effects" and request == {"spec": spec}
                 and key in completed and completed[key][1] == readback]
    settlements = [key for key, (name, _request) in scheduled.items()
                   if name == "delivery_feature_settle_effects"]
    stops = [key for key, (name, _request) in scheduled.items()
             if name == "delivery_feature_stop"]
    checkpoint = {"state": state, "completed_chunks": state["checks"].get(
        "feature_progress", {}).get("completed", [])}
    if (not readbacks or max(readbacks) != max(settlements)
            or not stops or max(stops) not in completed
            or completed[max(readbacks)][0] >= max(stops)
            or scheduled[max(stops)][1] != {"spec": spec, "checkpoint": checkpoint}
            or completed[max(stops)][1] != {"owner": feature["owner"], "state": "stopped"}):
        raise ValueError("completed feature settlement checkpoint is unproven")
    return {**expected, "feature_readback": readback}


async def reconcile_projection(store, client, row):
    """Only controller-owned history/result bytes may repair this read model."""
    run_id = row["run_id"]
    spec = store.effective_spec(run_id)
    if (spec.get("workflow_failure_boundary_version") != 1
            or execution_retired(spec) or not store.owns_execution(spec)):
        raise ValueError("completed projection has no current service authority")
    workflow_id = row["workflow_id"] or "delivery-" + run_id
    description = await client.get_workflow_handle(workflow_id).describe()
    if description.status != WorkflowExecutionStatus.COMPLETED:
        return {"state": "not_completed"}
    if (not description.close_time or description.id != workflow_id
            or description.workflow_type != "DevflowDeliveryWorkflow"):
        raise ValueError("completed projection workflow identity changed")
    recovery = json.loads(row["recovery_json"]) if row["recovery_json"] else None
    if (await description.memo_value("request_digest", None) != row["request_digest"]
            or await description.memo_value("recovery_digest", None)
            != (digest(recovery) if recovery else None)):
        raise ValueError("completed projection start memo changed")
    handle = client.get_workflow_handle(workflow_id, run_id=description.run_id)
    state = await handle.result()
    failure = state.get("checks", {}).get("terminal_projection_failure", {})
    if not failure:
        return {"state": "no_failed_projection"}
    history = await handle.fetch_history()
    if not history.events or not history.events[0].HasField(
            "workflow_execution_started_event_attributes"):
        raise ValueError("completed projection start history is missing")
    inputs = await DELIVERY_DATA_CONVERTER.decode(
        history.events[0].workflow_execution_started_event_attributes.input.payloads)
    starts = [spec] if recovery else [store.submitted_spec(run_id), store.spec(run_id), spec]
    if (len(inputs) != 2 or inputs[0] not in starts or inputs[1] != recovery
            or inputs[0].get("workflow_failure_boundary_version") != 1):
        raise ValueError("completed projection immutable start input changed")
    scheduled, failed, last_projection = {}, set(), None
    for event in history.events:
        if event.HasField("activity_task_scheduled_event_attributes"):
            attributes = event.activity_task_scheduled_event_attributes
            if attributes.activity_type.name == "delivery_project":
                values = await DELIVERY_DATA_CONVERTER.decode(attributes.input.payloads)
                if len(values) != 1 or not isinstance(values[0], dict):
                    raise ValueError("completed projection request is malformed")
                scheduled[event.event_id] = values[0]
                last_projection = event.event_id
        if event.HasField("activity_task_failed_event_attributes"):
            failed.add(event.activity_task_failed_event_attributes.scheduled_event_id)
        if event.HasField("activity_task_timed_out_event_attributes"):
            failed.add(event.activity_task_timed_out_event_attributes.scheduled_event_id)
    event_id = failure.get("scheduled_event_id")
    projection = scheduled.get(event_id)
    if (failure.get("version") != 1 or type(event_id) is not int
            or event_id not in failed or event_id != last_projection or projection is None
            or digest(projection) != failure.get("projection_sha256")
            or projection.get("spec") != spec
            or projection.get("event_type") not in {"delivered", "blocked", "cancelled"}
            or failure.get("event_type") != projection["event_type"]
            or failure.get("protocol_revision") != projection.get("protocol_revision")
            or projection.get("key") != (
                f"{projection['event_type']}:{projection.get('iteration')}:"
                f"{projection.get('protocol_revision')}")):
        raise ValueError("completed result is not bound to its failed terminal request")
    expected_checks = {**(projection.get("checks") or {}), "terminal_projection_failure": failure}
    if state.get("checks") != expected_checks:
        expected_checks = await _settled_feature_checks(
            history, spec, state, event_id, expected_checks)
    if (state.get("run_id") != run_id or state.get("revision") != projection["protocol_revision"]
            or state.get("checks") != expected_checks
            or any(state.get(key) != projection.get(key) for key in (
                *(key for key in FIELDS if key != "cleanup"),
                "candidate", "pull_request", "tracker", "usage", "decision", "intake"))
            or state.get("outcome") != projection["event_type"]
            or state.get("cleanup") != (
                "confirmed" if projection.get("cleanup") == "confirmed" else "unknown")):
        raise ValueError("completed terminal result differs from its failed projection")
    desired = {**projection, "checks": deepcopy(state["checks"]), "cleanup": state["cleanup"]}
    key = "closed-project:" + description.run_id + ":" + digest(state)
    with store._connect() as db:
        events = [dict(item) for item in db.execute(
            "SELECT * FROM delivery_events WHERE run_id=? ORDER BY sequence", (run_id,))]
    observations = [event for event in events if event["type"] == EVENT
                    and json.loads(event["payload_json"]).get("key") == key]
    if observations:
        # An already reconciled execution cannot overwrite any subsequent row.
        return {"state": "observed", "key": key}
    if row["protocol_revision"] == projection["protocol_revision"]:
        # The activity may have committed before losing its response. Preserve
        # that original event and add only the finished-result observation.
        if (not _same_row(row, projection) or not events
                or events[-1]["run_revision"] != row["revision"]
                or json.loads(events[-1]["payload_json"]).get("key") != projection["key"]):
            raise ValueError("completed projection would overwrite a newer row")
    elif ((row["protocol_revision"] or 0) > projection["protocol_revision"]
          or row["outcome"] is not None
          or datetime.fromisoformat(row["updated_at"]) > description.close_time):
        raise ValueError("completed projection would overwrite a newer row")
    if (projection.get("candidate") is None and row["candidate_json"] is not None
            or projection.get("pull_request") is None and row["pr_json"] is not None):
        raise ValueError("completed projection cannot discard a retained candidate or publication")
    store.project(
        run_id, **{name: desired.get(name) for name in (*FIELDS, *JSON_FIELDS,
                                                      "protocol_revision")},
        event_type=EVENT, message="Observed the authentic completed controller terminal result",
        key=key, expected_revision=row["revision"],
    )
    return {"state": "reconciled", "key": key, "workflow_id": workflow_id,
            "execution_run_id": description.run_id}


async def reconcile_completed_projections(store, client, *, seen=None, after_run_id=""):
    """Bound each maintenance pass; a new same-run execution has a new identity."""
    if seen is None:
        seen = set()
    with store._connect() as db:
        rows = [dict(row) for row in db.execute(
            "SELECT r.* FROM delivery_runs r JOIN delivery_outbox o ON o.run_id=r.run_id "
            "ORDER BY r.run_id")]
    selected = [row for row in rows
                if json.loads(row["request_json"]).get("workflow_failure_boundary_version") == 1
                and (row["run_id"], row["workflow_id"]) not in seen]
    selected = ([row for row in selected if row["run_id"] > after_run_id]
                + [row for row in selected if row["run_id"] <= after_run_id])[:100]
    results = []
    for row in selected:
        try:
            observed = await reconcile_projection(store, client, row)
        except Exception as exc:
            logger.warning("Run %s completed projection remains unproven (%s)",
                           row["run_id"], type(exc).__name__)
            observed = {"state": "unproven", "error_type": type(exc).__name__}
        if observed["state"] in {"observed", "reconciled", "no_failed_projection"}:
            seen.add((row["run_id"], row["workflow_id"]))
        results.append({"run_id": row["run_id"], **observed})
    return results
