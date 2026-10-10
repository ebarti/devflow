"""Feature waits retain ownership without recording every observation in history."""
from __future__ import annotations

import asyncio
import json
import shutil
from copy import deepcopy
from types import SimpleNamespace

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from test_delivery_feature_execution import feature_service
from test_delivery_feature_workflow_temporal import FeatureActivities
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import register_worker, worker_spec
from devflow_temporal.delivery_github_contract import ordered_chunks
from devflow_temporal.delivery_workflow import DeliveryWorkflow


def test_wait_reference_authenticates_frozen_input_and_existing_worker(service, monkeypatch):
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    spec = store.effective_spec(request['run_id'])
    chunk = ordered_chunks(json.loads(request['accepted_plan']))[0]
    child = worker_spec(spec, chunk, {'url': 'https://github.com/example/fixture/issues/10'},
                        kind='build', base_sha=spec['base_sha'], base_branch='main')
    register_worker(store, spec, child)
    reference = {'config_path': spec['config_path'], 'run_id': spec['run_id'],
                 'spec_digest': digest(spec), 'child_id': child['run_id']}
    assert activities._worker_wait_spec(reference) == spec
    with pytest.raises(OwnershipConflict, match='frozen specification'):
        activities._worker_wait_spec({**reference, 'spec_digest': 'another-input'})
    with pytest.raises(OwnershipConflict, match='own this worker'):
        activities._worker_wait_spec({**reference, 'child_id': 'another-worker'})
    raw = deepcopy(store.config.raw)
    raw['max_repairs'] = 9
    store.config.path.write_text(json.dumps(raw))
    with pytest.raises(ValueError, match='configuration changed'):
        activities._worker_wait_spec(reference)


@pytest.mark.parametrize('cancel', [False, True])
async def test_long_worker_wait_has_bounded_history_and_remains_cancellable(
    tmp_path, monkeypatch, cancel,
):
    fixtures = FeatureActivities()
    first_stream = fixtures.record['manifest']['plan']['workstreams'][0]
    first_stream['chunks'] = first_stream['chunks'][:1]
    fixtures.record['manifest']['plan']['workstreams'] = [first_stream]
    started, release = asyncio.Event(), asyncio.Event()
    observations = 0
    spec = {'run_id': 'bounded-wait-' + str(cancel), 'provider': 'fake',
            'config_path': '/frozen/config', 'authorized_endpoint': 'published_unmerged',
            'feature_delivery': {'version': 1}, 'policy': {'max_repairs': 10},
            'large_frozen_configuration': 'x' * 70_000}

    async def observe(payload):
        nonlocal observations
        observations += 1
        if observations >= 100:
            started.set()
            await release.wait()
            return {'closed': True, 'outcome': 'delivered'}
        return {'closed': False}

    async def fast_observation(_):
        await asyncio.sleep(.001)

    monkeypatch.setattr(activities, '_worker_wait_spec', lambda _: spec)
    monkeypatch.setattr(activities, 'delivery_feature_worker_result', observe)
    monkeypatch.setattr(activities, 'asyncio', SimpleNamespace(
        to_thread=asyncio.to_thread, sleep=fast_observation))

    @activity.defn(name='delivery_feature_reserve')
    async def reserve(payload):
        return {'spec': {'run_id': payload['kind'] + ':' + payload['chunk_id']},
                'workflow_id': 'existing-child', 'completed': payload['kind'] == 'build'}

    handlers = [f for f in fixtures.handlers() if
                activity._Definition.must_from_callable(f).name != 'delivery_feature_reserve']
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which('temporal'),
        dev_server_database_filename=str(tmp_path / 'temporal.sqlite3'),
    ) as environment:
        async with Worker(environment.client, task_queue=spec['run_id'],
                          workflows=[DeliveryWorkflow],
                          activities=[*handlers, reserve, activities.delivery_feature_wait_worker]):
            handle = await environment.client.start_workflow(DeliveryWorkflow.run, spec,
                id=spec['run_id'], task_queue=spec['run_id'])
            await asyncio.wait_for(started.wait(), 20)
            if not cancel:
                release.set()
                await asyncio.wait_for(fixtures.ready.wait(), 20)
            state = await handle.query(DeliveryWorkflow.status)
            await handle.execute_update(DeliveryWorkflow.cancel, {
                'command_id': 'stop-existing-worker', 'expected_revision': state['revision'],
                'reason': 'Preserve the same feature and worker',
            })
            result = await asyncio.wait_for(handle.result(), 15)
            history = await handle.fetch_history()
    assert result['outcome'] == 'cancelled'
    assert observations >= 100
    waits = [e.activity_task_scheduled_event_attributes for e in history.events
             if e.HasField('activity_task_scheduled_event_attributes') and
             e.activity_task_scheduled_event_attributes.activity_type.name
             == 'delivery_feature_wait_worker']
    assert len(waits) == 1, 'Observation count must not increase persisted activity count'
    assert sum(len(p.data) for a in waits for p in a.input.payloads) < 1024
    assert not any(name == 'delivery_feature_merge' for name, _ in fixtures.calls)
    assert any(name == 'delivery_feature_settle_workers' for name, _ in fixtures.calls)
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
