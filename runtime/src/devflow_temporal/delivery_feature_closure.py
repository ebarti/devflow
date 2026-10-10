"""Authenticate a failed coordinator's terminal checkpoint without reviving it."""
from __future__ import annotations

import asyncio
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client, WorkflowExecutionStatus

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
            'history_sha256': hashlib.sha256(history.to_json().encode()).hexdigest()}


def closed_coordinator(store, run_id):
    try:
        return store._completed_temporal_result(run_id)
    except FailedFeatureCoordinator as failure:
        closed = failure.closed
    spec = store.effective_spec(run_id)
    if (not spec.get('feature_delivery') or spec.get('feature_worker')
            or not store.owns_execution(spec)
            or closed['workflow_id'] != store.active_workflow_id(run_id)):
        raise ValueError('failed feature closure belongs to another execution')
    with store._connect() as db:
        row = dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?', (run_id,)).fetchone())

    async def read():
        client = await Client.connect(store.config.temporal_address,
            namespace=store.config.raw.get('temporal_namespace', 'default'),
            data_converter=DELIVERY_DATA_CONVERTER)
        handle = client.get_workflow_handle(closed['workflow_id'],
                                           run_id=closed['execution_run_id'])
        description = await handle.describe()
        if (description.status != WorkflowExecutionStatus.FAILED
                or not description.close_time or description.run_id != closed['execution_run_id']
                or description.close_time.isoformat() != closed['closed_at']):
            raise ValueError('failed feature Temporal closure changed')
        return await failed_checkpoint(await handle.fetch_history(), spec, row, closed)

    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: asyncio.run(asyncio.wait_for(read(), timeout=30))).result(35)
