from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from temporalio import activity, workflow
from temporalio.client import WorkflowHistory
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from test_delivery_store import service as service
from test_delivery_terminal_recovery import ControlledTrackerCheckpoint

from devflow_temporal import delivery_activities
from devflow_temporal.delivery_config import scope_amended_spec
from devflow_temporal.delivery_workflow import DeliveryWorkflow


def request(spec):
    return {'spec': spec, 'phase': 'implement', 'execution_state': 'running',
            'event_type': 'implement', 'message': 'Starting the same accepted work',
            'key': 'implement:0:2', 'iteration': 0, 'protocol_revision': 2}


def test_new_admissions_freeze_projection_retry_version(service):
    store, supplied = service
    assert store.config.admit(supplied)['projection_retry_version'] == 1


@pytest.mark.parametrize('legacy', [False, True])
def test_scope_amendment_preserves_original_projection_options(service, legacy):
    store, supplied = service
    store.submit(supplied)
    original = store.spec(supplied['run_id'])
    if legacy:
        original.pop('projection_retry_version')
    amended = json.loads(store.config.path.read_text())
    amended['repositories']['fixture']['allowed_paths'].append('tests/extra.py')
    path = store.config.state_root / 'amendment.json'
    path.write_text(json.dumps(amended))
    path.chmod(0o600)
    effective = scope_amended_spec(original, path,
                                  hashlib.sha256(path.read_bytes()).hexdigest(),
                                  ['tests/extra.py'])
    assert effective.get('projection_retry_version') == original.get('projection_retry_version')
    assert ('projection_retry_version' in effective) == ('projection_retry_version' in original)


@pytest.mark.asyncio
@pytest.mark.parametrize('version', [None, 1])
async def test_projection_retry_options_are_frozen(monkeypatch, version):
    captured = {}

    async def execute(name, payload, **options):
        captured.update(name=name, request=payload, **options)
        return {}

    monkeypatch.setattr(workflow, 'execute_activity', execute)
    spec = {} if version is None else {'projection_retry_version': version}
    await DeliveryWorkflow()._activity('delivery_project', request(spec))
    if version is None:
        assert captured['retry_policy'].maximum_attempts == 1
        assert captured['start_to_close_timeout'].total_seconds() == 7200
        assert 'schedule_to_close_timeout' not in captured
    else:
        assert captured['retry_policy'].maximum_attempts == 3
        assert captured['start_to_close_timeout'].total_seconds() == 45
        assert captured['schedule_to_close_timeout'].total_seconds() == 180
        assert captured['retry_policy'].initial_interval.total_seconds() == 2


@pytest.mark.asyncio
async def test_projection_contention_does_not_block_other_activities(monkeypatch):
    def project(*_args, **_kwargs):
        time.sleep(0.08)
        return {'revision': 2, 'phase': 'implement'}

    monkeypatch.setattr(delivery_activities, '_context',
                        lambda *_a, **_kw: (SimpleNamespace(project=project), None))
    progressing = asyncio.Event()
    task = asyncio.create_task(delivery_activities.delivery_project(request(
        {'run_id': 'projection-fixture', 'projection_retry_version': 1})))
    await asyncio.sleep(0)
    assert not task.done(), 'projection held the shared event loop until SQL completed'
    progressing.set()
    assert progressing.is_set()
    assert await task == {'revision': 2, 'phase': 'implement'}


@pytest.mark.asyncio
@pytest.mark.parametrize('message,retryable', [
    ('database is locked', True), ('database table is locked', True),
    ('database is busy', True), ('database disk image is malformed', False),
    ('unable to open database file', False),
])
async def test_only_known_sql_contention_is_retryable(monkeypatch, message, retryable):
    def project(*_args, **_kwargs):
        raise sqlite3.OperationalError(message)

    monkeypatch.setattr(delivery_activities, '_context',
                        lambda *_a, **_kw: (SimpleNamespace(project=project), None))
    with pytest.raises(ApplicationError) as failure:
        await delivery_activities.delivery_project(request(
            {'run_id': 'projection-fixture', 'projection_retry_version': 1}))
    assert failure.value.non_retryable is not retryable


@pytest.mark.asyncio
@pytest.mark.parametrize('reason', ['run ID not found', 'frozen configuration changed'])
async def test_semantic_conflicts_are_not_retried(monkeypatch, reason):
    def context(*_args, **_kwargs):
        raise ValueError(reason)

    monkeypatch.setattr(delivery_activities, '_context', context)
    with pytest.raises(ApplicationError) as failure:
        await delivery_activities.delivery_project(request(
            {'run_id': 'projection-fixture', 'projection_retry_version': 1}))
    assert failure.value.non_retryable is True


@pytest.mark.asyncio
@pytest.mark.parametrize('key', [None, '', 42])
async def test_retryable_projection_requires_original_deduplication_key(monkeypatch, key):
    calls = []
    monkeypatch.setattr(delivery_activities, '_context',
                        lambda *_a, **_kw: calls.append('context'))
    payload = request({'projection_retry_version': 1})
    payload['key'] = key
    with pytest.raises(ApplicationError) as failure:
        await delivery_activities.delivery_project(payload)
    assert failure.value.non_retryable is True
    assert calls == []


@pytest.mark.asyncio
async def test_cancelled_sql_wait_cannot_overwrite_terminal_projection(service, monkeypatch):
    store, supplied = service
    store.submit(supplied)
    spec = store.spec(supplied['run_id'])
    started, release, finished = (threading.Event() for _ in range(3))
    real_project = store.project

    def waiting_project(*args, **kwargs):
        started.set()
        try:
            assert release.wait(5), 'owned SQL fixture was not released'
            return real_project(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(store, 'project', waiting_project)
    monkeypatch.setattr(delivery_activities, '_context', lambda *_a, **_kw: (store, None))
    task = asyncio.create_task(delivery_activities.delivery_project(request(spec)))
    try:
        assert await asyncio.to_thread(started.wait, 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        real_project(supplied['run_id'], phase='cancelled', execution_state='terminal',
                     event_type='cancelled', message='User cancellation', outcome='cancelled',
                     cleanup='confirmed', key='cancelled:0:3')
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 2)
    with store._connect() as db:
        row = db.execute('SELECT phase,outcome,cleanup FROM delivery_runs WHERE run_id=?',
                         (supplied['run_id'],)).fetchone()
        assert tuple(row) == ('cancelled', 'cancelled', 'confirmed')
        count = db.execute("SELECT count(*) FROM delivery_events "
                           "WHERE run_id=? AND type='implement'",
                           (supplied['run_id'],)).fetchone()[0]
        assert count == 0


@pytest.mark.asyncio
async def test_legacy_projection_preserves_original_error_contract(monkeypatch):
    def project(*_args, **_kwargs):
        raise sqlite3.OperationalError('database is locked')

    monkeypatch.setattr(delivery_activities, '_context',
                        lambda *_a, **_kw: (SimpleNamespace(project=project), None))
    with pytest.raises(sqlite3.OperationalError):
        await delivery_activities.delivery_project(request({'run_id': 'legacy-projection'}))


@workflow.defn
class ProjectionRetryFixture(DeliveryWorkflow):
    @workflow.run
    async def run(self, spec):
        await self._activity('delivery_project', request(spec))
        return {'run_id': spec['run_id'], 'effect_count': 1}


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['contention', 'lost_completion'])
async def test_real_temporal_retries_exact_projection_without_duplicate_events(
    service, monkeypatch, failure,
):
    store, supplied = service
    store.submit(supplied)
    spec = store.spec(supplied['run_id'])
    spec['projection_retry_version'] = 1
    calls = []
    real_project = store.project

    def project(*args, **kwargs):
        calls.append(kwargs['key'])
        if failure == 'contention' and len(calls) == 1:
            raise sqlite3.OperationalError('database is locked')
        return real_project(*args, **kwargs)

    monkeypatch.setattr(store, 'project', project)
    monkeypatch.setattr(delivery_activities, '_context', lambda *_a, **_kw: (store, None))

    @activity.defn(name='delivery_project')
    async def project_activity(payload):
        result = await delivery_activities.delivery_project(payload)
        if failure == 'lost_completion' and activity.info().attempt == 1:
            # Simulate losing the acknowledgement AFTER the real SQL transaction.
            raise ApplicationError('projection completion transport interrupted')
        return result

    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which('temporal'),
    ) as environment:
        async with Worker(environment.client, task_queue='projection-retry',
                          workflows=[ProjectionRetryFixture],
                          workflow_runner=UnsandboxedWorkflowRunner(),
                          activities=[project_activity]):
            result = await asyncio.wait_for(environment.client.execute_workflow(
                ProjectionRetryFixture.run, spec, id='projection-retry',
                task_queue='projection-retry'), timeout=25)
    assert result == {'run_id': supplied['run_id'], 'effect_count': 1}
    assert calls == ['implement:0:2', 'implement:0:2']
    with store._connect() as db:
        rows = db.execute("SELECT payload_json FROM delivery_events "
                          "WHERE run_id=? AND type='implement'", (supplied['run_id'],)).fetchall()
        assert len(rows) == 1
        assert json.loads(rows[0][0])['key'] == 'implement:0:2'


@pytest.mark.asyncio
async def test_previous_source_terminal_projection_history_replays_unchanged():
    path = Path(__file__).parent / 'fixtures/tracker/c04-terminal-previous-history.json'
    result = await Replayer(workflows=[ControlledTrackerCheckpoint],
                            workflow_runner=UnsandboxedWorkflowRunner()).replay_workflow(
        WorkflowHistory.from_json('previous-projection', path.read_text()))
    assert result.replay_failure is None
