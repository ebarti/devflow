"""Authenticate closed feature custody without reviving historical executions."""
from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from datetime import datetime
from types import SimpleNamespace

from temporalio.client import Client, WorkflowExecutionStatus

from .contracts import digest
from .delivery_closures import closure_facts, history_sha256, sealed_closure
from .delivery_codec import DELIVERY_DATA_CONVERTER

SETTLEMENT = {'delivery_feature_settle_workers'}


class FailedFeatureCoordinator(ValueError):
    """A failed coordinator is closed, but has no successful workflow result."""

    def __init__(self, closed):
        super().__init__('continuation predecessor Temporal closure is unproven')
        self.closed = closed


async def failed_checkpoint(history, spec, row, closed):
    """Use only an acknowledged terminal projection preceding a settlement failure."""
    events = list(history.events)
    if (not events or not events[-1].HasField('workflow_execution_failed_event_attributes')
            or not spec.get('feature_delivery') or spec.get('feature_worker')
            or row['recovery_json'] is not None or closed['recovery_digest'] is not None
            or closed['request_digest'] != spec['request_digest']):
        raise ValueError('failed feature closure lacks its original execution authority')
    start = events[0].workflow_execution_started_event_attributes
    if start.workflow_type.name != 'DevflowDeliveryWorkflow':
        raise ValueError('failed feature history has a different workflow type')
    memo = {}
    for key in ('request_digest', 'recovery_digest'):
        values = (await DELIVERY_DATA_CONVERTER.decode([start.memo.fields[key]])
                  if key in start.memo.fields else [None])
        memo[key] = values[0]
    if any(memo[key] != closed[key] for key in memo):
        raise ValueError('failed feature start memo differs from its closed execution')
    inputs = await DELIVERY_DATA_CONVERTER.decode(start.input.payloads)
    initial = inputs[0] if inputs else {}
    if (initial != json.loads(row['request_json'])
            or any(initial.get(key) != spec.get(key) for key in
            ('run_id', 'request_digest', 'work_id', 'config_digest', 'base_sha',
             'github_repo', 'feature_delivery')) or any(value is not None for value in inputs[1:])):
        raise ValueError('failed feature history starts from different authority')
    scheduled = {e.event_id: e.activity_task_scheduled_event_attributes for e in events
                 if e.HasField('activity_task_scheduled_event_attributes')}
    completed = {e.activity_task_completed_event_attributes.scheduled_event_id: e.event_id
                 for e in events if e.HasField('activity_task_completed_event_attributes')}
    failures = {e.activity_task_failed_event_attributes.scheduled_event_id for e in events
                if e.HasField('activity_task_failed_event_attributes')}
    failure = events[-1].workflow_execution_failed_event_attributes.failure.activity_failure_info
    failed = scheduled.get(failure.scheduled_event_id)
    projections = [(event_id, value) for event_id, value in scheduled.items()
                   if value.activity_type.name == 'delivery_project']
    if (not projections or failed is None or failed.activity_type.name not in SETTLEMENT
            or failure.scheduled_event_id not in failures
            or failure.activity_type.name != failed.activity_type.name):
        raise ValueError('failed feature did not stop at a settlement boundary')
    event_id, projection = projections[-1]
    acknowledged = completed.get(event_id)
    if (acknowledged is None or failure.scheduled_event_id <= acknowledged
            or any(value.activity_type.name not in SETTLEMENT
                   for key, value in scheduled.items() if key > acknowledged)):
        raise ValueError('failed feature lacks an acknowledged terminal projection')
    payloads = await DELIVERY_DATA_CONVERTER.decode(projection.input.payloads)
    failure_inputs = await DELIVERY_DATA_CONVERTER.decode(failed.input.payloads)
    projected = payloads[0] if len(payloads) == 1 else {}
    if (projected.get('spec') != spec or len(failure_inputs) != 1
            or failure_inputs[0].get('spec') != spec
            or projected.get('outcome') not in {'blocked', 'cancelled'}
            or projected.get('event_type') != projected.get('outcome')
            or projected.get('phase') != projected.get('outcome')
            or projected.get('cleanup') != 'confirmed'
            or type(projected.get('protocol_revision')) is not int
            or projected['protocol_revision'] < 1
            or projected.get('execution_state') != (
                'blocked' if projected.get('outcome') == 'blocked' else 'terminal')):
        raise ValueError('failed feature terminal projection is not a stopped coordinator')
    for key in ('phase', 'execution_state', 'outcome', 'cleanup', 'error', 'iteration',
                'protocol_revision'):
        if row[key] != projected.get(key):
            raise ValueError('failed feature projection changed after its checkpoint')
    for key, column in (('candidate', 'candidate_json'), ('pull_request', 'pr_json'),
                        ('checks', 'checks_json'), ('tracker', 'tracker_json'),
                        ('usage', 'usage_json'), ('decision', 'decision_json'),
                        ('intake', 'intake_json')):
        if json.loads(row[column] or 'null') != projected.get(key):
            raise ValueError('failed feature evidence changed after its checkpoint')
    state = {key: value for key, value in projected.items()
             if key not in {'spec', 'key', 'message', 'event_type', 'protocol_revision'}}
    state.update(run_id=spec['run_id'], revision=projected['protocol_revision'])
    return {**closed, 'workflow_status': 'FAILED', 'result': state,
            'terminal_projection_event_id': acknowledged,
            'history_sha256': history_sha256(history)}


def closed_coordinator(store, run_id, *, preserve=True):
    try:
        if preserve:
            return store._completed_temporal_result(run_id)
        return store._completed_temporal_result(run_id, preserve=False)
    except FailedFeatureCoordinator as failure:
        closed = failure.closed
    if preserve:
        spec = store.effective_spec(run_id)
    else:
        from .delivery_feature_handoff import readonly_feature_spec

        spec = readonly_feature_spec(store, run_id)
    if (not spec.get('feature_delivery') or spec.get('feature_worker')
            or not store.owns_execution(spec)
            or closed['workflow_id'] != store.active_workflow_id(run_id)):
        raise ValueError('failed feature closure belongs to another execution')
    with store._connect() as db:
        row = dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?', (run_id,)).fetchone())

    async def read():
        from .delivery_closures import not_found, sealed_closure

        client = await Client.connect(store.config.temporal_address,
            namespace=store.config.raw.get('temporal_namespace', 'default'),
            data_converter=DELIVERY_DATA_CONVERTER)
        handle = client.get_workflow_handle(closed['workflow_id'],
                                           run_id=closed['execution_run_id'])
        try:
            description = await handle.describe()
        except Exception as exc:
            if not not_found(exc):
                raise
            # Retention deleted the history; only its sealed original can stand in.
            if preserve:
                sealed = await sealed_closure(store, closed['workflow_id'])
            else:
                sealed = await sealed_closure(store, closed['workflow_id'], read_only=True)
            if (sealed is None or sealed['status'] != 'FAILED'
                    or sealed['execution_run_id'] != closed['execution_run_id']
                    or sealed['closed_at'] != closed['closed_at']):
                raise ValueError('failed feature Temporal closure changed') from exc
            return await failed_checkpoint(sealed['history'], spec, row, closed)
        if (description.status != WorkflowExecutionStatus.FAILED
                or not description.close_time or description.run_id != closed['execution_run_id']
                or description.close_time.isoformat() != closed['closed_at']):
            raise ValueError('failed feature Temporal closure changed')
        return await failed_checkpoint(await handle.fetch_history(), spec, row, closed)

    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(copy_context().run,
                           lambda: asyncio.run(asyncio.wait_for(read(), timeout=30))).result(35)


def _completed_row(store, spec, row, closed, projections):
    """Bind the closed result, allowing only authenticated later cleanup maintenance."""
    from .delivery_closed_projection import FIELDS, JSON_FIELDS, _cleanup_row, _events, _same_row

    state = closed['result']
    expected = {**state, 'protocol_revision': state['revision']}
    if any(row[key] != spec.get(key) for key in
           ('run_id', 'request_digest', 'work_id', 'issue_url', 'repository_key')):
        raise ValueError('closed feature immutable row changed')
    # Recovery retains the workflow's candidate counter; the store counts its
    # own projections. The canonical candidate below binds these two histories.
    if row['cleanup'] != 'confirmed':
        raise ValueError('closed feature cleanup remains unconfirmed')
    if _same_row(row, expected):
        return
    checks = json.loads(row['checks_json'] or '{}')
    if spec.get('feature_worker'):
        from .delivery_policy_recovery import reconciled_cleanup

        maintenance = reconciled_cleanup(store, row, state)
        if maintenance and _same_row(row, {**expected, 'cleanup': 'confirmed',
                'checks': {**state['checks'], 'resource_cleanup': maintenance['resource_cleanup']},
                'tracker': maintenance['tracker']}):
            return
    original = {**row, 'cleanup': state.get('cleanup'),
                'checks_json': json.dumps(state.get('checks'))}
    receipt = checks.get('resource_cleanup')
    if (not _same_row(original, expected)
            or {k: v for k, v in checks.items() if k != 'resource_cleanup'}
            != {k: v for k, v in state.get('checks', {}).items() if k != 'resource_cleanup'}
            or not isinstance(receipt, dict) or receipt.get('state') != 'confirmed'
            or receipt.get('resource_cleanup') != 'confirmed'
            or not isinstance(receipt.get('processes'), list)):
        raise ValueError('closed feature projection differs from its terminal result')
    # All remaining delivery fields have already matched; no maintenance row may
    # compose a newer candidate, gate, tracker or plan result with this closure.
    events = _events(store, spec['run_id'])
    closed_at = datetime.fromisoformat(closed['closed_at'])
    description = SimpleNamespace(id=closed['workflow_id'], run_id=closed['execution_run_id'],
                                  close_time=closed_at)
    if _cleanup_row(row, spec, description, projections, events):
        return
    # A failed terminal projection can first be restored by the existing completed
    # projection reconciler. Its authenticated result key is the new baseline;
    # subsequent row writers must be only the same closure's cleanup maintenance.
    baseline_key = 'closed-project:' + closed['execution_run_id'] + ':' + digest(state)
    baseline = next((event for event in reversed(events)
                     if event['type'] == 'closed_projection_reconciled'
                     and json.loads(event['payload_json']).get('key') == baseline_key), None)
    if baseline is None:
        raise ValueError('closed feature cleanup lacks its terminal projection receipt')
    closure = {'workflow_id': closed['workflow_id'],
               'execution_run_id': closed['execution_run_id'],
               'closed_at': closed['closed_at'], 'status': 'COMPLETED'}
    maintenance_key = digest({'closed_workflow': closure,
                             'native_observations': receipt['processes'],
                             'resource_cleanup': receipt})
    revision, written_at, last = (baseline['run_revision'],
                                  datetime.fromisoformat(baseline['timestamp']), baseline)
    if written_at <= closed_at:
        raise ValueError('closed feature projection receipt precedes its closure')
    for event in events:
        if event['sequence'] <= baseline['sequence'] or event['run_revision'] <= revision:
            continue
        payload = json.loads(event['payload_json'])
        timestamp = datetime.fromisoformat(event['timestamp'])
        if (event['run_revision'] != revision + 1
                or event['type'] != 'resource_cleanup_reconciled'
                or set(payload) != {'key', 'iteration', 'candidate_id'}
                or payload['iteration'] != row['iteration'] or payload['candidate_id'] is not None
                or not isinstance(payload['key'], str) or len(payload['key']) != 64
                or any(char not in '0123456789abcdef' for char in payload['key'])
                or timestamp <= written_at):
            raise ValueError('closed feature changed after its terminal projection receipt')
        revision, written_at, last = event['run_revision'], timestamp, event
    if revision != row['revision'] or datetime.fromisoformat(row['updated_at']) > written_at:
        raise ValueError('closed feature cleanup maintenance is not bound to its closure')
    if last is baseline:
        # The ordinary pump can finalize native cleanup before restoring the
        # failed terminal projection. Authenticate that composed baseline by
        # replaying the existing cleanup proof over its exact scheduled prior
        # projection and recorded writers; no later cleanup write is required.
        before = [event for event in events if event['sequence'] < baseline['sequence']]
        previous = next((event for event in reversed(before)
                         if event['run_revision'] == baseline['run_revision'] - 1
                         and event['type'] == 'resource_cleanup_reconciled'
                         and json.loads(event['payload_json']).get('key') == maintenance_key), None)
        if (previous is not None
                and datetime.fromisoformat(previous['timestamp']) < written_at):
            for projection in projections.values():
                prior = {**row, **{key: projection.get(key) for key in FIELDS},
                         'protocol_revision': projection.get('protocol_revision'),
                         'revision': previous['run_revision'], 'updated_at': previous['timestamp'],
                         **{column: json.dumps(projection.get(key))
                            for key, column in JSON_FIELDS.items()},
                         'cleanup': row['cleanup'], 'checks_json': json.dumps({
                             **(projection.get('checks') or {}), 'resource_cleanup': receipt})}
                if _cleanup_row(prior, spec, description, projections, before):
                    return
        raise ValueError('closed feature composed cleanup lacks its original maintenance proof')
    if (last['type'] != 'resource_cleanup_reconciled'
            or json.loads(last['payload_json']).get('key') != maintenance_key):
        raise ValueError('closed feature cleanup maintenance is not bound to its closure')


async def completed_settlement(history, store, spec, row, closed):
    """Authenticate a completed coordinator's actual failed final worker settlement."""
    facts = await closure_facts(history)
    inputs = facts['inputs']
    if (facts['status'] != 'COMPLETED' or not spec.get('feature_delivery')
            or spec.get('feature_worker') or row['recovery_json'] is not None
            or facts['recovery_digest'] is not None
            or any(facts[key] != closed.get(key) for key in (
                'workflow_id', 'execution_run_id', 'closed_at', 'request_digest',
                'recovery_digest', 'result'))
            or facts['request_digest'] != spec['request_digest']
            or len(inputs) != 2 or inputs[0] != json.loads(row['request_json'])
            or inputs[1] is not None
            or inputs[0].get('feature_delivery') != spec.get('feature_delivery')):
        raise ValueError('completed settlement lacks its original execution authority')
    state = closed['result']
    if (not isinstance(state, dict) or state.get('run_id') != spec['run_id']
            or state.get('outcome') not in {'blocked', 'cancelled'}
            or type(state.get('revision')) is not int or state['revision'] < 1
            or row['protocol_revision'] != state['revision']):
        raise ValueError('completed settlement lacks its current terminal checkpoint')
    scheduled, failures, completed, projections = {}, {}, set(), {}
    for event in history.events:
        if event.HasField('activity_task_scheduled_event_attributes'):
            attributes = event.activity_task_scheduled_event_attributes
            name = attributes.activity_type.name
            if name == 'delivery_feature_stop':
                raise ValueError('completed coordinator already scheduled its feature stop')
            if name in SETTLEMENT or name == 'delivery_project':
                values = await DELIVERY_DATA_CONVERTER.decode(attributes.input.payloads)
                if len(values) != 1 or not isinstance(values[0], dict):
                    raise ValueError('completed settlement request is malformed')
                if name in SETTLEMENT:
                    scheduled[event.event_id] = values[0]
                else:
                    from .delivery_closed_projection import failed_terminal_tail

                    projections[event.event_id] = (failed_terminal_tail(
                        state, values[0], event.event_id) or values[0])
        if event.HasField('activity_task_failed_event_attributes'):
            attributes = event.activity_task_failed_event_attributes
            failures[attributes.scheduled_event_id] = event.event_id
        if event.HasField('activity_task_completed_event_attributes'):
            completed.add(event.activity_task_completed_event_attributes.scheduled_event_id)
    event_id = max(scheduled, default=0)
    request = scheduled.get(event_id)
    workers = request.get('workers') if request else None
    if (event_id not in failures or event_id in completed
            or set(request) != {'spec', 'workers'} or request['spec'] != spec
            or not isinstance(workers, dict)
            or any(not isinstance(key, str) or not key or not isinstance(value, str) or not value
                   for key, value in workers.items())):
        raise ValueError('completed coordinator did not fail its recorded worker settlement')
    _completed_row(store, spec, row, closed, projections)
    return {**closed, 'workflow_status': 'COMPLETED', 'history_sha256': history_sha256(history),
            'settlement': {'action': 'settle_closed_coordinator_workers',
                'workflow_id': facts['workflow_id'], 'execution_run_id': facts['execution_run_id'],
                'closed_at': facts['closed_at'], 'history_sha256': history_sha256(history),
                'result_sha256': digest(state), 'scheduled_event_id': event_id,
                'failed_event_id': failures[event_id], 'request_sha256': digest(request),
                'workers': workers}}


async def _closed_history(store, workflow_id, closed):
    from .delivery_execution_config import is_observation_mode

    if is_observation_mode():
        sealed = await sealed_closure(store, workflow_id, read_only=True)
    else:
        sealed = await sealed_closure(store, workflow_id)
    if sealed is not None:
        bound = {key: sealed[key] for key in ('workflow_id', 'execution_run_id', 'closed_at',
                                            'request_digest', 'recovery_digest', 'result')}
        if closed is not None and bound != closed:
            raise ValueError('completed feature Temporal closure changed')
        return sealed['history'], bound
    if closed is None or closed['workflow_id'] != workflow_id:
        raise ValueError('completed feature settlement history is unproven')
    client = await Client.connect(store.config.temporal_address,
        namespace=store.config.raw.get('temporal_namespace', 'default'),
        data_converter=DELIVERY_DATA_CONVERTER)
    handle = client.get_workflow_handle(workflow_id, run_id=closed['execution_run_id'])
    return await handle.fetch_history(), closed


def closed_settlement(store, run_id, *, sealed_only=False):
    """Read closure proof for admission; status projection uses only an existing seal."""
    spec = store.effective_spec(run_id)
    if not store.owns_execution(spec):
        raise ValueError('completed settlement belongs to another service transport')
    with store._connect() as db:
        row = dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?', (run_id,)).fetchone())
    workflow_id = store.active_workflow_id(run_id)
    closed = None if sealed_only else closed_coordinator(store, run_id)

    async def read():
        history, bound = await _closed_history(store, workflow_id, closed)
        return await completed_settlement(history, store, spec, row, bound)

    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(copy_context().run,
                           lambda: asyncio.run(asyncio.wait_for(read(), timeout=30))).result(35)


async def completed_worker(history, store, spec, row, closed):
    """Bind a worker's current closed execution to its admitted input and terminal row."""
    facts = await closure_facts(history)
    recovery = json.loads(row['recovery_json']) if row['recovery_json'] else None
    # Reset histories retain the original start input. These are the same frozen
    # admitted inputs the interrupted-execution guard authenticates before reset.
    starts = [store.submitted_spec(spec['run_id']), store.spec(spec['run_id']), spec]
    inputs, state = facts['inputs'], facts['result']
    if (facts['status'] != 'COMPLETED' or not spec.get('feature_worker')
            or any(facts[key] != closed.get(key) for key in (
                'workflow_id', 'execution_run_id', 'closed_at', 'request_digest',
                'recovery_digest', 'result'))
            or facts['request_digest'] != row['request_digest']
            or facts['recovery_digest'] != (digest(recovery) if recovery else None)
            or len(inputs) not in (1, 2) or inputs[0] not in starts
            or (inputs[1] if len(inputs) == 2 else None) != recovery
            or not isinstance(state, dict) or state.get('run_id') != spec['run_id']
            or state.get('outcome') not in {'delivered', 'blocked', 'cancelled'}
            or type(state.get('revision')) is not int or state['revision'] < 1
            or row['protocol_revision'] != state['revision']):
        raise ValueError('closed worker history does not authenticate its current checkpoint')
    from .delivery_closed_projection import failed_terminal_tail

    projections = {}
    for event in history.events:
        if event.HasField('activity_task_scheduled_event_attributes'):
            attributes = event.activity_task_scheduled_event_attributes
            if attributes.activity_type.name == 'delivery_project':
                values = await DELIVERY_DATA_CONVERTER.decode(attributes.input.payloads)
                if len(values) != 1 or not isinstance(values[0], dict):
                    raise ValueError('closed worker projection request is malformed')
                projections[event.event_id] = (failed_terminal_tail(
                    state, values[0], event.event_id) or values[0])
    _completed_row(store, spec, row, closed, projections)
    return {**closed, 'history_sha256': history_sha256(history)}


def closed_worker(store, run_id, workflow_id, *, sealed_only=False):
    spec = store.effective_spec(run_id)
    if not store.owns_execution(spec) or workflow_id != store.active_workflow_id(run_id):
        raise ValueError('closed worker workflow or service custody changed')
    with store._connect() as db:
        row = dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?', (run_id,)).fetchone())
    from .delivery_execution_config import is_observation_mode

    closed = (None if sealed_only else store._completed_temporal_result(
        run_id, workflow_id=workflow_id, **({"preserve": False} if is_observation_mode() else {})))

    async def read():
        history, bound = await _closed_history(store, workflow_id, closed)
        proof = await completed_worker(history, store, spec, row, bound)
        with store._connect() as db:
            current = dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                                      (run_id,)).fetchone())
        if (current != row or store.effective_spec(run_id) != spec
                or store.active_workflow_id(run_id) != workflow_id):
            raise ValueError('closed worker checkpoint changed during closure readback')
        return proof

    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(copy_context().run,
                           lambda: asyncio.run(asyncio.wait_for(read(), timeout=30))).result(35)
