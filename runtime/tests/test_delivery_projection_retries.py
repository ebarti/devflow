from __future__ import annotations

import asyncio
import json
import shutil
import sqlite3
import threading
from pathlib import Path

import pytest
from temporalio import activity, workflow
from temporalio.client import WorkflowHistory
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from test_delivery_store import service as service
from test_delivery_terminal_recovery import ControlledTrackerCheckpoint

from devflow_temporal import delivery_activities
from devflow_temporal.delivery_workflow import DeliveryWorkflow


def request(spec):
    return {'spec': spec, 'phase': 'implement', 'execution_state': 'running',
            'event_type': 'implement', 'message': 'Starting the same accepted work',
            'key': 'implement:0:2', 'iteration': 0, 'protocol_revision': 2}


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
