"""Reconcile a failed terminal projection from its authentic completed execution."""
from __future__ import annotations

import asyncio
import json
import logging
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from temporalio.client import WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

from .contracts import digest
from .delivery_codec import DELIVERY_DATA_CONVERTER
from .delivery_preparation import execution_retired

logger = logging.getLogger(__name__)
EVENT = "closed_projection_reconciled"
FEATURE_STOP_EVENT = "closed_feature_stop_reconciled"
FIELDS = ("phase", "execution_state", "iteration", "outcome", "cleanup", "error")
JSON_FIELDS = {"candidate": "candidate_json", "pull_request": "pr_json",
               "checks": "checks_json", "tracker": "tracker_json", "usage": "usage_json",
               "decision": "decision_json", "intake": "intake_json"}
TERMINAL_EVENTS = frozenset({"delivered", "blocked", "cancelled"})
# Maintenance never re-reads these; a new same-run execution has a new identity.
SETTLED = frozenset({"observed", "reconciled", "no_failed_projection", "closed", "closed_observed"})
PASS_LIMIT = 100
BACKOFF_BASE = timedelta(seconds=30)
BACKOFF_CAP = timedelta(hours=1)
OPEN_EXECUTION_BACKOFF_CAP = timedelta(minutes=5)


def _same_row(row, projection):
    return (all(row[key] == projection.get(key) for key in FIELDS)
            and row["protocol_revision"] == projection["protocol_revision"]
            and all(json.loads(row[column] or "null") == projection.get(key)
                    for key, column in JSON_FIELDS.items()))


def _cleanup_row(row, spec, description, scheduled, events, *, status="COMPLETED"):
    """Prove contiguous cleanup writes over an exact history-bound projection.

    The latest maintenance key binds the closure and finalization receipt. Older
    keys retain no receipt bytes; their ordinary projection events account for
    the intervening row revisions. No newer delivery field may be composed.
    """
    checks = json.loads(row["checks_json"] or "{}")
    receipt = checks.get("resource_cleanup")
    if (not isinstance(receipt, dict) or not isinstance(receipt.get("processes"), list)
            or row["cleanup"] not in {"confirmed", "unknown"}
            or receipt.get("state") != row["cleanup"]
            or receipt.get("resource_cleanup") != row["cleanup"]):
        return None
    if status not in {"COMPLETED", "FAILED"}:
        raise ValueError('cleanup projection requires an authenticated completed or failed closure')
    closed = {"workflow_id": description.id, "execution_run_id": description.run_id,
              "closed_at": description.close_time.isoformat(), "status": status}
    key = digest({"closed_workflow": closed, "native_observations": receipt["processes"],
                  "resource_cleanup": receipt})
    maintenance = next((event for event in reversed(events)
                        if event["type"] == "resource_cleanup_reconciled"
                        and event["run_revision"] == row["revision"]
                        and json.loads(event["payload_json"]).get("key") == key), None)
    if (maintenance is None
            or datetime.fromisoformat(maintenance["timestamp"]) <= description.close_time
            or datetime.fromisoformat(row["updated_at"])
            > datetime.fromisoformat(maintenance["timestamp"])):
        return None
    for projection in reversed(list(scheduled.values())):
        if (projection.get("spec") != spec
                or projection.get("protocol_revision") != row["protocol_revision"]
                or projection.get("key") != (
                    f"{projection.get('event_type')}:{projection.get('iteration')}:"
                    f"{projection.get('protocol_revision')}")
                or {k: v for k, v in checks.items() if k != "resource_cleanup"}
                != {k: v for k, v in (projection.get("checks") or {}).items()
                    if k != "resource_cleanup"}):
            continue
        original = {**row, "cleanup": projection.get("cleanup"),
                    "checks_json": json.dumps(projection.get("checks"))}
        if not _same_row(original, projection):
            continue
        event = next((event for event in reversed(events)
                      if event["sequence"] < maintenance["sequence"]
                      and event["run_revision"] < row["revision"]
                      and event["type"] == projection["event_type"]
                      and json.loads(event["payload_json"]).get("key") == projection["key"]), None)
        if event is None or datetime.fromisoformat(event["timestamp"]) > description.close_time:
            continue
        revision, written_at, writer = event["run_revision"], description.close_time, event
        for later in events:
            if not event["sequence"] < later["sequence"] <= maintenance["sequence"]:
                continue
            if later["run_revision"] <= revision:
                continue  # An observation does not advance the projected row.
            payload = json.loads(later["payload_json"])
            marker = payload.get("key")
            timestamp = datetime.fromisoformat(later["timestamp"])
            if (later["run_revision"] != revision + 1
                    or later["type"] != "resource_cleanup_reconciled"
                    or set(payload) != {"key", "iteration", "candidate_id"}
                    or payload["iteration"] != row["iteration"]
                    or payload["candidate_id"] is not None
                    or not isinstance(marker, str) or len(marker) != 64
                    or any(char not in "0123456789abcdef" for char in marker)
                    or timestamp <= written_at):
                break
            revision, written_at, writer = later["run_revision"], timestamp, later
        else:
            if revision == row["revision"] and writer is maintenance:
                return {**original, "revision": event["run_revision"],
                        "updated_at": event["timestamp"]}, event, receipt
    return None


def _terminal_request(projection):
    """A terminal event, or the closed tracker deadline that replaced it, binds a row."""
    event = projection.get("event_type")
    if event in TERMINAL_EVENTS:
        return projection.get("outcome") == event
    if event != "tracker_deadline":
        return False
    checkpoint = (projection.get("checks") or {}).get("terminal_tracker_checkpoint")
    if (not isinstance(checkpoint, dict) or checkpoint.get("closed") is not True
            or checkpoint.get("waiting") is not False
            or checkpoint.get("event") not in TERMINAL_EVENTS
            or checkpoint.get("outcome") != checkpoint.get("event")):
        return False
    if checkpoint.get("state") == "conflicted":
        return all(projection.get(key) == "blocked"
                   for key in ("phase", "execution_state", "outcome"))
    return (checkpoint.get("state") == "pending" and projection.get("outcome") is None
            and projection.get("phase") == "waiting_tracker")


def bound_failed_projection(projection, failure):
    """The failure record names exactly this scheduled projection request."""
    return (isinstance(failure, dict) and failure.get("version") == 1
            and type(failure.get("scheduled_event_id")) is int
            and digest(projection) == failure.get("projection_sha256")
            and failure.get("event_type") == projection.get("event_type")
            and failure.get("protocol_revision") == projection.get("protocol_revision")
            and projection.get("key") == (
                f"{projection.get('event_type')}:{projection.get('iteration')}:"
                f"{projection.get('protocol_revision')}"))


def failed_terminal_tail(state, request, event_id):
    """A failed last projection as the closed tail, bound by the completed result's record.

    The result's checks are that request's checks plus the record the controller
    added after the failure, so the tail equals the reconciled row.
    """
    checks = state.get("checks") if isinstance(state, dict) else None
    failure = checks.get("terminal_projection_failure") if isinstance(checks, dict) else None
    if (failure is None or failure.get("scheduled_event_id") != event_id
            or not bound_failed_projection(request, failure)
            or not _terminal_request(request)
            or checks != {**(request.get("checks") or {}), "terminal_projection_failure": failure}
            or state.get("cleanup") != (
                "confirmed" if request.get("cleanup") == "confirmed" else "unknown")):
        return None
    return {**request, "checks": deepcopy(checks), "cleanup": state["cleanup"]}


def _events(store, run_id):
    with store._connect() as db:
        return [dict(item) for item in db.execute(
            "SELECT * FROM delivery_events WHERE run_id=? ORDER BY sequence", (run_id,))]


def _observe(store, run_id, event_type, message, payload):
    """Append one keyed observation without touching the projected row."""
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        if any(json.loads(item[0]).get("key") == payload["key"] for item in db.execute(
                "SELECT payload_json FROM delivery_events WHERE run_id=? AND type=?",
                (run_id, event_type))):
            return
        current = db.execute("SELECT revision FROM delivery_runs WHERE run_id=?",
                             (run_id,)).fetchone()
        if current is None:
            raise ValueError("run ID not found")
        store._event(db, run_id, current[0], event_type, message, payload)


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


async def _bound_feature_stop(history, spec, state, record):
    """The failed stop request is the controller's own scheduled input, nothing else."""
    scheduled, failed = {}, set()
    for event in history.events:
        if event.HasField("activity_task_scheduled_event_attributes"):
            attributes = event.activity_task_scheduled_event_attributes
            if attributes.activity_type.name == "delivery_feature_stop":
                values = await DELIVERY_DATA_CONVERTER.decode(attributes.input.payloads)
                if len(values) != 1 or not isinstance(values[0], dict):
                    raise ValueError("completed feature stop request is malformed")
                scheduled[event.event_id] = values[0]
        if event.HasField("activity_task_failed_event_attributes"):
            failed.add(event.activity_task_failed_event_attributes.scheduled_event_id)
        if event.HasField("activity_task_timed_out_event_attributes"):
            failed.add(event.activity_task_timed_out_event_attributes.scheduled_event_id)
    event_id = record.get("scheduled_event_id")
    request = scheduled.get(event_id)
    before_record = {**state, "checks": {key: value for key, value in state["checks"].items()
                                         if key != "feature_stop_failure"}}
    if (record.get("version") != 1 or type(event_id) is not int or event_id not in failed
            or request is None or event_id != max(scheduled)
            or digest(request) != record.get("request_sha256")
            or request.get("spec") != spec or not spec.get("feature_delivery")
            or spec.get("feature_worker")
            or request.get("checkpoint") != {"state": before_record, "completed_chunks": []}):
        raise ValueError("completed result is not bound to its failed feature stop request")
    return request


async def _replay_feature_stop(store, run_id, spec, description, state, record, request):
    key = "closed-feature-stop:" + description.run_id + ":" + digest(state)
    if any(event["type"] == FEATURE_STOP_EVENT
           and json.loads(event["payload_json"]).get("key") == key
           for event in await asyncio.to_thread(_events, store, run_id)):
        return {"state": "observed", "feature_stop_key": key}
    from .delivery_feature_activities import stop_feature

    # The same guarded local effect the controller scheduled, from its exact request.
    stopped = await asyncio.to_thread(stop_feature, request["spec"], request["checkpoint"])
    if stopped != {"owner": spec["feature_delivery"]["owner"], "state": "stopped"}:
        raise ValueError("replayed feature stop did not settle the recorded owner")
    await asyncio.to_thread(
        _observe, store, run_id, FEATURE_STOP_EVENT,
        "Replayed the completed controller's failed feature stop from its scheduled request",
        {"key": key, "workflow_id": description.id, "execution_run_id": description.run_id,
         "scheduled_event_id": record["scheduled_event_id"]})
    return {"state": "reconciled", "feature_stop_key": key}


async def observe_terminal_projection(store, row, spec, description, state, history, failure,
                                      expected_extra):
    """Authenticate a failed terminal projection without projecting or replaying it."""
    run_id = row["run_id"]
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
    if (projection is None or event_id not in failed or event_id != last_projection
            or not bound_failed_projection(projection, failure)
            or projection.get("spec") != spec or not _terminal_request(projection)):
        raise ValueError("completed result is not bound to its failed terminal request")
    expected_checks = {**(projection.get("checks") or {}),
                       "terminal_projection_failure": failure, **expected_extra}
    if state.get("checks") != expected_checks:
        expected_checks = await _settled_feature_checks(
            history, spec, state, event_id, expected_checks)
    if (state.get("run_id") != run_id or state.get("revision") != projection["protocol_revision"]
            or state.get("checks") != expected_checks
            or any(state.get(key) != projection.get(key) for key in (
                *(key for key in FIELDS if key != "cleanup"),
                "candidate", "pull_request", "tracker", "usage", "decision", "intake"))
            or state.get("cleanup") != (
                "confirmed" if projection.get("cleanup") == "confirmed" else "unknown")):
        raise ValueError("completed terminal result differs from its failed projection")
    desired = {**projection, "checks": deepcopy(state["checks"]), "cleanup": state["cleanup"]}
    key = "closed-project:" + description.run_id + ":" + digest(state)
    events = await asyncio.to_thread(_events, store, run_id)
    observations = [event for event in events if event["type"] == EVENT
                    and json.loads(event["payload_json"]).get("key") == key]
    if observations:
        # An already reconciled execution cannot overwrite any subsequent row.
        return {"state": "observed", "key": key}, None
    current = row
    maintained = _cleanup_row(row, spec, description, scheduled, events)
    if maintained:
        row, last_event, receipt = maintained
        desired = {**desired, "cleanup": current["cleanup"],
                   "checks": {**desired["checks"], "resource_cleanup": deepcopy(receipt)}}
    else:
        # Observations do not write the row: bind its committing event to a
        # scheduled request instead of interpreting an arbitrary trailing event.
        keys = {request["key"] for request in scheduled.values() if request.get("key")}
        last_event = next((event for event in reversed(events)
                           if event["run_revision"] == row["revision"]
                           and json.loads(event["payload_json"]).get("key") in keys), None)
    if row["protocol_revision"] == projection["protocol_revision"]:
        # The activity may have committed before losing its response. Preserve
        # that original event and add only the finished-result observation.
        last_key = json.loads(last_event["payload_json"]).get("key") if last_event else None
        if last_event is None:
            raise ValueError("completed projection would overwrite a newer row")
        if last_key == projection["key"]:
            if not _same_row(row, projection):
                raise ValueError("completed projection would overwrite a newer row")
        elif (projection["event_type"] != "tracker_deadline" or row["outcome"] is not None
              or last_key not in {
                  f"{name}:{projection.get('iteration')}:{projection['protocol_revision']}"
                  for name in ("tracker_pending", "tracker_retry_required")}):
            # Only this terminal pass's own committed readback event may precede
            # its closed deadline at the same protocol revision.
            raise ValueError("completed projection would overwrite a newer row")
    elif ((row["protocol_revision"] or 0) > projection["protocol_revision"]
          or row["outcome"] is not None
          or datetime.fromisoformat(row["updated_at"]) > description.close_time):
        raise ValueError("completed projection would overwrite a newer row")
    if (projection.get("candidate") is None and row["candidate_json"] is not None
            or projection.get("pull_request") is None and row["pr_json"] is not None):
        raise ValueError("completed projection cannot discard a retained candidate or publication")
    return {"state": "reconciled", "key": key}, {
        "run_id": run_id,
        "fields": {name: desired.get(name)
                   for name in (*FIELDS, *JSON_FIELDS, "protocol_revision")},
        "key": key,
        "expected_revision": current["revision"],
        "expected_row": current,
    }


async def _reconcile_terminal(store, row, spec, description, state, history, failure,
                              expected_extra):
    observed, request = await observe_terminal_projection(
        store, row, spec, description, state, history, failure, expected_extra)
    if request is not None:
        await asyncio.to_thread(
            store.project, request["run_id"], **request["fields"], event_type=EVENT,
            message="Observed the authentic completed controller terminal result",
            key=request["key"], expected_revision=request["expected_revision"],
            expected_row=request["expected_row"],
        )
    return observed


async def reconcile_projection(store, client, row):
    """Only controller-owned history/result bytes may repair this read model."""
    run_id = row["run_id"]
    spec = await asyncio.to_thread(store.effective_spec, run_id)
    if (execution_retired(spec)
            or not await asyncio.to_thread(store.owns_execution, spec)):
        raise ValueError("completed projection has no current service authority")
    workflow_id = row["workflow_id"] or "delivery-" + run_id
    sealed = None
    try:
        description = await client.get_workflow_handle(workflow_id).describe()
    except RPCError as exc:
        if exc.status != RPCStatusCode.NOT_FOUND:
            raise
        from .delivery_closures import sealed_closure

        sealed = await sealed_closure(store, workflow_id)
        if sealed is None:
            return {"state": "not_started"}
        async def memo_value(key, default=None):
            return sealed.get(key, default)
        description = SimpleNamespace(
            id=sealed["workflow_id"], run_id=sealed["execution_run_id"],
            status=WorkflowExecutionStatus[sealed["status"]],
            close_time=datetime.fromisoformat(sealed["closed_at"]),
            workflow_type="DevflowDeliveryWorkflow", memo_value=memo_value,
        )
    if sealed is None:
        from .delivery_interrupted_recovery import recover_interrupted

        recovered = await recover_interrupted(store, client, row, spec, description)
        if recovered:
            return recovered
    if description.status != WorkflowExecutionStatus.COMPLETED:
        return {"state": "not_completed", "status": description.status.name,
                "closed": description.close_time is not None}
    if spec.get("workflow_failure_boundary_version") != 1:
        return {"state": "no_failed_projection"}
    if (not description.close_time or description.id != workflow_id
            or description.workflow_type != "DevflowDeliveryWorkflow"):
        raise ValueError("completed projection workflow identity changed")
    recovery = json.loads(row["recovery_json"]) if row["recovery_json"] else None
    if (await description.memo_value("request_digest", None) != row["request_digest"]
            or await description.memo_value("recovery_digest", None)
            != (digest(recovery) if recovery else None)):
        raise ValueError("completed projection start memo changed")
    handle = client.get_workflow_handle(workflow_id, run_id=description.run_id)
    state = sealed["result"] if sealed else await handle.result()
    checks = state.get("checks", {}) if isinstance(state, dict) else {}
    failure = checks.get("terminal_projection_failure") if isinstance(checks, dict) else None
    stop_record = checks.get("feature_stop_failure") if isinstance(checks, dict) else None
    if not failure and not stop_record:
        return {"state": "no_failed_projection"}
    history = sealed["history"] if sealed else await handle.fetch_history()
    if not history.events or not history.events[0].HasField(
            "workflow_execution_started_event_attributes"):
        raise ValueError("completed projection start history is missing")
    inputs = await DELIVERY_DATA_CONVERTER.decode(
        history.events[0].workflow_execution_started_event_attributes.input.payloads)
    starts = [spec] if recovery else [
        await asyncio.to_thread(store.submitted_spec, run_id),
        await asyncio.to_thread(store.spec, run_id), spec]
    if (len(inputs) != 2 or inputs[0] not in starts or inputs[1] != recovery
            or inputs[0].get("workflow_failure_boundary_version") != 1):
        raise ValueError("completed projection immutable start input changed")
    # Authenticate every record before any row write or replayed effect.
    stop_request = (await _bound_feature_stop(history, spec, state, stop_record)
                    if stop_record else None)
    observed, refused = {"state": "observed"}, None
    if failure:
        try:
            observed = await _reconcile_terminal(
                store, row, spec, description, state, history, failure,
                {"feature_stop_failure": stop_record} if stop_record else {})
        except Exception as exc:
            # The stop request is bound to the result on its own; a row that
            # refuses its projection must not keep the feature owner active.
            refused = exc
    if stop_request:
        stopped = await _replay_feature_stop(
            store, run_id, spec, description, state, stop_record, stop_request)
        observed = {**observed, **stopped, "state": "reconciled" if "reconciled" in {
            observed["state"], stopped["state"]} else "observed"}
    if refused is not None:
        raise refused
    return {**observed, "workflow_id": workflow_id, "execution_run_id": description.run_id}


def _iso(moment):
    return moment.isoformat(timespec="microseconds")


def _runtime_generation():
    from .payload import payload_digest

    return payload_digest(Path(__file__).resolve().parent)


def _candidates(store, now, limit, generation):
    """Flagged rows whose current execution identity is unsettled and due."""
    placeholders = ",".join("?" * len(SETTLED))
    with store._connect() as db:
        return [dict(row) for row in db.execute(
            f"""SELECT r.* FROM delivery_runs r
                LEFT JOIN delivery_closed_projections p
                  ON p.run_id=r.run_id
                 AND p.workflow_id=COALESCE(r.workflow_id,'delivery-'||r.run_id)
                WHERE (json_extract(r.request_json,'$.workflow_failure_boundary_version')=1
                       OR r.outcome IS NULL OR r.outcome='blocked')
                  AND EXISTS (SELECT 1 FROM delivery_outbox o WHERE o.run_id=r.run_id)
                  AND (p.state IS NULL OR (p.state='closed'
                       AND (r.outcome IS NULL OR r.outcome='blocked'))
                       OR (p.state NOT IN ({placeholders})
                       AND (p.next_check_at IS NULL OR p.next_check_at<=?
                            OR p.revision!=r.revision
                            OR (p.state='unproven' AND COALESCE(p.key,'')!=?))))
                ORDER BY COALESCE(p.next_check_at,''), r.run_id LIMIT ?""",
            (*sorted(SETTLED), _iso(now), generation, limit))]


def _remember(store, row, observed, now):
    """Persist the verdict; unsettled executions back off exponentially."""
    workflow_id = row["workflow_id"] or "delivery-" + row["run_id"]
    state = observed["state"]
    if state == "not_completed" and observed.get("closed"):
        state = "closed_observed"
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        previous = db.execute(
            "SELECT attempts,state,key FROM delivery_closed_projections "
            "WHERE run_id=? AND workflow_id=?", (row["run_id"], workflow_id)).fetchone()
        # Backoff counts consecutive identical verdicts; a changed verdict restarts it.
        attempts = 0 if state in SETTLED else (
            previous["attempts"] + 1 if previous and previous["state"] == state
            and (state != 'unproven' or previous['key'] == observed.get('key')) else 1)
        next_check = None
        if state not in SETTLED:
            cap = (OPEN_EXECUTION_BACKOFF_CAP if state in {"not_completed", "not_started"}
                   else BACKOFF_CAP)
            next_check = _iso(now + min(cap, BACKOFF_BASE * 2 ** min(attempts - 1, 12)))
        db.execute(
            """INSERT INTO delivery_closed_projections
               (run_id,workflow_id,state,attempts,revision,next_check_at,error,key,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?)
               ON CONFLICT(run_id,workflow_id) DO UPDATE SET state=excluded.state,
               attempts=excluded.attempts,revision=excluded.revision,
               next_check_at=excluded.next_check_at,error=excluded.error,key=excluded.key,
               updated_at=excluded.updated_at""",
            (row["run_id"], workflow_id, state, attempts, row["revision"], next_check,
             observed.get("error_type"), observed.get("key"), _iso(now)))


async def reconcile_completed_projections(store, client, *, now=None, limit=PASS_LIMIT):
    """An installer fence defers observation and its skip/backoff writers together."""
    from .delivery_interrupted_recovery import (
        InstallationRecoveryDeferred,
        installation_recovery_lease,
    )

    try:
        with installation_recovery_lease(store.config):
            return await _reconcile_completed_projections(store, client, now=now, limit=limit)
    except InstallationRecoveryDeferred:
        return []


async def _reconcile_completed_projections(store, client, *, now=None, limit=PASS_LIMIT):
    """Bound each maintenance pass to due candidates; all SQLite work stays off the loop."""
    from .delivery_interrupted_recovery import InstallationRecoveryDeferred

    now = now or datetime.now(UTC)
    results = []
    generation = await asyncio.to_thread(_runtime_generation)
    for row in await asyncio.to_thread(_candidates, store, now, limit, generation):
        try:
            observed = await reconcile_projection(store, client, row)
        except asyncio.CancelledError:
            raise
        except InstallationRecoveryDeferred:
            continue  # No skip record, settled verdict or backoff write while installation owns it.
        except Exception as exc:
            logger.warning("Run %s completed projection remains unproven (%s)",
                           row["run_id"], type(exc).__name__)
            observed = {"state": "unproven", "error_type": type(exc).__name__,
                        "reason": str(exc)[:300]}
            from .delivery_automatic_retry import _record_skip

            await asyncio.to_thread(_record_skip, store, row, exc)
        if observed['state'] == 'unproven':
            observed = {**observed, 'key': generation}
        await asyncio.to_thread(_remember, store, row, observed, now)
        results.append({"run_id": row["run_id"], **observed})
    return results
