"""Resume an interrupted controller from its retained Temporal history.

Temporal's reset forks the original history; it does not reset application repair
budgets or manufacture a completed result. Completed activity results before the
resume point are replayed. Native/effect journals still guard repeated activities.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import stat
from contextlib import contextmanager
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


class InstallationRecoveryDeferred(RuntimeError):
    """The installer owns the shared recovery fence; maintenance must stay read-only."""


@contextmanager
def installation_recovery_lease(config, *, exclusive=False):
    """Serialize installation with effectful resets across every local service."""
    from .delivery_feature_execution import registry_path
    from .delivery_resources import _ancestors, private_directory

    path = registry_path(config).parent / 'installation-recovery.lock'
    private_directory(path.parent)
    _ancestors(path)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)

    def authenticate():
        info, current = os.fstat(descriptor), path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
                or (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino)):
            raise ValueError('installation recovery lease changed or is not a private owned file')

    try:
        authenticate()
        try:
            fcntl.flock(descriptor, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise InstallationRecoveryDeferred('installation recovery lease is held') from exc
        authenticate()
        yield path
    finally:
        os.close(descriptor)


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


def _guard(store, row, spec, *, terminal_settlement=False):
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
            require_execution(store, spec, terminal_settlement=terminal_settlement)
        else:
            shared = registry(spec)
            with shared.connect() as db:
                shared.require(db, spec["feature_delivery"]["owner"], active=False)
    if spec.get("provider") != "fake":
        from .delivery_orphans import _observe_registered_processes
        from .delivery_resources import RunResources

        root = Path(spec["state_dir"]) / "resources"
        if root.exists():
            with RunResources(spec, read_only=True).locked() as manifest:
                paths = list(manifest["processes"])
            for _ in _observe_registered_processes(spec, paths):
                pass
        elif any(Path(spec["state_dir"]).rglob("native-process.json")):
            raise ValueError("native process has no owning resource manifest")


def _failure_types(failure):
    result = []
    while failure.ByteSize():
        if failure.HasField('application_failure_info'):
            result.append(failure.application_failure_info.type)
        if not failure.HasField('cause'):
            break
        failure = failure.cause
    return result


def _terminal_projection_row(store, row, spec, projected, acknowledged_revision, facts):
    """Accept only closure-bound orphan maintenance over the acknowledged projection."""
    from datetime import datetime
    from types import SimpleNamespace

    from .delivery_closed_projection import _cleanup_row, _same_row

    if row['revision'] == acknowledged_revision and _same_row(row, projected):
        return True
    receipt = json.loads(row['checks_json'] or '{}').get('resource_cleanup') or {}
    if (row['cleanup'] != 'confirmed' or receipt.get('state') != 'confirmed'
            or receipt.get('resource_cleanup') != 'confirmed'
            or receipt.get('process_cleanup') != 'observed-native-confirmed'):
        return False
    with store._connect() as db:
        events = [dict(event) for event in db.execute(
            'SELECT * FROM delivery_events WHERE run_id=? AND run_revision>=? ORDER BY sequence',
            (spec['run_id'], acknowledged_revision))]
    description = SimpleNamespace(id=facts['workflow_id'], run_id=facts['execution_run_id'],
                                  close_time=datetime.fromisoformat(facts['closed_at']))
    bound = _cleanup_row(row, spec, description, {'acknowledged': projected}, events,
                         status=facts['status'])
    return bool(bound and bound[0]['revision'] == acknowledged_revision
                and _same_row(bound[0], projected))


async def _terminal_drain_point(store, row, spec, history):
    """Replay only settlement after an acknowledged coordinator prelaunch stop."""
    from .delivery_broker import DeliveryBroker
    from .delivery_feature_execution import registry, require_execution
    from .delivery_policy_recovery import _rows
    from .delivery_session_custody import implementation_custody
    from .delivery_stopped_resume import _unstarted_repair

    if not spec.get('feature_worker') or not history.events[-1].HasField(
            'workflow_execution_failed_event_attributes'):
        return None
    shared = registry(spec)
    current = shared.current(spec['feature_delivery']['owner']['issue_id'])
    if not current or current['state'] != 'draining':
        return None
    require_execution(store, spec, terminal_settlement=True)
    owner = shared.token(current)
    scheduled, failed, completed, updates = [], {}, {}, {}
    for event in history.events:
        if event.HasField('activity_task_scheduled_event_attributes'):
            attrs = event.activity_task_scheduled_event_attributes
            payloads = await DELIVERY_DATA_CONVERTER.decode(list(attrs.input.payloads))
            if len(payloads) != 1 or not isinstance(payloads[0], dict):
                return None
            scheduled.append((event, attrs.activity_type.name, payloads[0]))
        elif event.HasField('activity_task_failed_event_attributes'):
            attrs = event.activity_task_failed_event_attributes
            failed[attrs.scheduled_event_id] = (event.event_id, _failure_types(attrs.failure))
        elif event.HasField('activity_task_completed_event_attributes'):
            attrs = event.activity_task_completed_event_attributes
            payloads = await DELIVERY_DATA_CONVERTER.decode(list(attrs.result.payloads))
            completed[attrs.scheduled_event_id] = (
                event.event_id, payloads[0] if payloads else None)
        elif event.HasField('workflow_execution_update_accepted_event_attributes'):
            updates[event.event_id] = event.workflow_execution_update_accepted_event_attributes
    if [name for _, name, _ in scheduled[-3:]] != [
            'delivery_finalize_resources', 'delivery_terminal_tracker', 'delivery_project']:
        return None
    (finalizer, _, final_request), (tracker, _, tracker_request), (projection, _, request) = (
        scheduled[-3:])
    point = finalizer.activity_task_scheduled_event_attributes.workflow_task_completed_event_id
    failure = history.events[-1].workflow_execution_failed_event_attributes.failure
    if (_failure_types(failure) != ['ProjectionRejected', 'OwnershipConflict']
            or not failure.HasField('activity_failure_info')
            or failure.activity_failure_info.scheduled_event_id != projection.event_id
            or failed.get(finalizer.event_id, (None, []))[1] != ['OwnershipConflict']
            or failed.get(projection.event_id, (None, []))[1]
            != ['ProjectionRejected', 'OwnershipConflict']
            or any(item.get('spec') != spec for item in (final_request, tracker_request, request))
            or final_request.get('outcome') != 'cancelled'
            or final_request.get('uncertain') is not True
            or request.get('event_type') != 'tracker_deadline'
            or request.get('phase') != request.get('outcome') or request.get('outcome') != 'blocked'
            or request.get('execution_state') != 'blocked'
            or request.get('cleanup') != 'unknown'
            or request.get('iteration') != row['iteration']
            or tracker_request.get('status') != 'blocked'
            or tracker_request.get('release') is not False
            or completed.get(tracker.event_id, (None, {}))[1].get('reason') != 'OwnershipConflict'
            or completed[tracker.event_id][1].get('retryable') is not False):
        return None
    role = next((item for item in reversed(scheduled[:-3]) if item[1] == 'delivery_role'), None)
    acknowledged = [item for item in scheduled[:-3] if item[1] == 'delivery_project'
                    and item[0].event_id in completed]
    if not role or not acknowledged:
        return None
    role_event, _, role_request = role
    projected_event, _, projected = acknowledged[-1]
    if (role_request.get('role') != 'implement' or role_request.get('spec') != spec
            or role_request.get('iteration') != row['iteration']
            or failed.get(role_event.event_id, (None, []))[1] != ['OwnershipConflict']
            or not role_event.event_id < failed[role_event.event_id][0] < point
            or projected.get('event_type') != 'role_started'
            or projected.get('spec') != spec or projected.get('iteration') != row['iteration']
            or projected.get('phase') != 'repair' or projected.get('outcome') is not None
            or projected.get('execution_state') != 'running'
            or not _terminal_projection_row(
                store, row, spec, projected,
                completed[projected_event.event_id][1].get('revision'),
                await closure_facts(history))):
        return None
    revision, cancelled = projected['protocol_revision'], None
    cancel_events = []
    for event in history.events:
        if not event.HasField('workflow_execution_update_completed_event_attributes'):
            continue
        attrs = event.workflow_execution_update_completed_event_attributes
        accepted = updates.get(attrs.accepted_event_id)
        if not accepted or accepted.accepted_request.input.name != 'cancel':
            return None
        supplied = await DELIVERY_DATA_CONVERTER.decode(
            list(accepted.accepted_request.input.args.payloads))
        outcomes = await DELIVERY_DATA_CONVERTER.decode(list(attrs.outcome.success.payloads))
        if len(supplied) != 1 or len(outcomes) != 1:
            return None
        command, state = supplied[0], outcomes[0]
        if (attrs.meta.update_id != accepted.accepted_request.meta.update_id
                or not role_event.event_id < attrs.accepted_event_id < event.event_id < point
                or command != {'command_id': 'feature-stop-' + digest(owner)[:16] + '-'
                               + spec['run_id'] + '-' + str(revision),
                               'expected_revision': revision,
                               'reason': 'Coordinator stopping; preserve the feature checkpoint'}
                or state.get('revision') != revision + 1
                or (cancelled and {k: v for k, v in state.items() if k != 'revision'}
                    != {k: v for k, v in cancelled.items() if k != 'revision'})):
            return None
        revision, cancelled = state['revision'], state
        cancel_events.append(event.event_id)
    if not cancelled or request.get('protocol_revision') != revision + 1:
        return None
    start = await DELIVERY_DATA_CONVERTER.decode(list(
        history.events[0].workflow_execution_started_event_attributes.input.payloads))
    previous = json.loads(row['recovery_json'] or 'null')
    if (len(start) != 2 or start != [spec, previous]
            or history.workflow_id != (row['workflow_id'] or 'delivery-' + spec['run_id'])
            or role_request.get('candidate') != cancelled.get('candidate')
            or request.get('candidate') != cancelled.get('candidate')):
        return None
    from .delivery_feature_revision_recovery import retained_recovery

    details = retained_recovery(previous)
    _, attempts, effects, _ = _rows(store, spec['run_id'])
    session = implementation_custody(spec, cancelled, attempts, details)
    candidate = DeliveryBroker(store, spec).candidate()
    seal = {'predecessor_spec': spec, 'state': cancelled, 'candidate': candidate,
            'attempts': attempts, 'effects': effects, 'original_recovery': previous,
            'session_id': (session or {}).get('session_id'),
            **({'session_custody': session} if session else {})}
    with store._connect() as db:
        if not _unstarted_repair(seal, details, db, closing=True):
            return None
    return {'resume_event_id': point, 'finalizer_request_sha256': digest(final_request),
            'iteration': row['iteration'], 'owner': owner, 'cancel_events': cancel_events,
            'candidate': candidate, 'attempts_sha256': digest(attempts),
            'effects_sha256': digest(effects), 'session_custody_sha256': digest(session),
            'recovery_digest': digest(previous)}


async def terminal_finalization_uncertain(store, request):
    """Only an acknowledged history reset can resolve prelaunch caller uncertainty."""
    from temporalio import activity
    from temporalio.exceptions import ApplicationError

    from .delivery_closures import sealed_closure

    if not activity.in_activity():
        return request.get('uncertain', False)
    info, spec = activity.info(), request['spec']
    intents = [item for item in _records(store, spec['run_id'], INTENT)
               if item.get('workflow_id') == info.workflow_id and item.get('terminal_settlement')]
    if not intents:
        return request.get('uncertain', False)
    results = _records(store, spec['run_id'], RESULT)
    matched = [(intent, result) for intent in intents for result in results
               if result.get('key') == intent['key']
               and result.get('recovered_run_id') == info.workflow_run_id]
    if len(matched) != 1:
        raise ApplicationError('terminal recovery acknowledgement is unconfirmed',
                               type='ResourceCleanupTransient')
    intent, result = matched[0]
    if (result != {**intent, 'recovered_run_id': info.workflow_run_id}
            or intent['key'] != digest({k: v for k, v in intent.items() if k != 'key'})
            or info.workflow_id != store.active_workflow_id(spec['run_id'])):
        raise ValueError('terminal recovery lost its exact reset acknowledgement')
    closed = await sealed_closure(store, info.workflow_id,
                                   execution_run_id=intent['execution_run_id'])
    with store._connect() as db:
        row = dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                              (spec['run_id'],)).fetchone())
    proof = await _terminal_drain_point(store, row, spec, closed['history']) if closed else None
    if (not closed or closed['status'] != 'FAILED'
            or history_sha256(closed['history']) != intent['history_sha256']
            or proof != intent['terminal_settlement']
            or intent['resume_event_id'] != proof['resume_event_id']
            or proof['finalizer_request_sha256'] != digest(request)):
        raise ValueError('terminal finalizer lost its sealed prelaunch failure proof')
    # RunResources still reconciles every registered process and derives unknown
    # if any actor, port, lease or resource identity cannot be confirmed.
    return False


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
    """Hold shared installation custody across all recovery writes and Temporal effects."""
    with installation_recovery_lease(store.config):
        return await _recover_interrupted(store, client, row, spec, description)


async def _recover_interrupted(store, client, row, spec, description):
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
    terminal = await _terminal_drain_point(store, row, spec, history)
    await asyncio.to_thread(_guard, store, row, spec, terminal_settlement=bool(terminal))
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
            "resume_event_id": terminal['resume_event_id'] if terminal else _resume_point(history),
            "runtime_payload_sha256": runtime,
            **({'terminal_settlement': terminal} if terminal else {}),
        }
        prior["key"] = digest(prior)
    if prior["history_sha256"] != history_sha256(history):
        raise ValueError("interrupted history changed after recovery intent")
    await asyncio.to_thread(seal, store, history, facts, run_id=row["run_id"], source="temporal")
    await asyncio.to_thread(_guard, store, row, spec, terminal_settlement=bool(terminal))
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
