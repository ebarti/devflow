"""Tracker lifecycle assessment uses the real closed Temporal predecessor."""
from __future__ import annotations

import asyncio
import json
import shutil
from datetime import timedelta
from threading import Event

import httpx
import pytest
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_terminal_recovery import (
    ClosedTerminalFixture,
    _Expired,
)
from test_delivery_terminal_recovery import (
    legacy_tracker_admission as legacy_tracker_admission,
)

from devflow_temporal import delivery_closures as closures
from devflow_temporal import delivery_terminal_recovery as terminal
from devflow_temporal.delivery_activities import delivery_project
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_resources import RunResources


@pytest.mark.asyncio
async def test_public_tracker_closed_history_assessment_and_fenced_admission(
    native_configuration, monkeypatch,
):
    config, request = native_configuration
    app = create_app(config.path)
    service = app.state.delivery
    service.store.submit(request)
    spec = service.store.spec(request['run_id'])
    calls = []

    @activity.defn(name='delivery_finalize_resources')
    async def cleanup(payload):
        calls.append('cleanup')
        with service.store._connect() as db:
            service.store.state.release_work(db, spec['work_id'],
                                             f"external:devflow:{spec['run_id']}")
        return RunResources(payload['spec']).finalize('blocked')

    @activity.defn(name='delivery_terminal_tracker')
    async def tracker(payload):
        calls.append('tracker')
        return {'state': 'pending', 'pending': True,
                'desired': payload['status'], 'readback_at': 'pending'}

    async with local_temporal(dev_server_existing_path=shutil.which('temporal')) as environment:
        async def client():
            return environment.client

        monkeypatch.setattr(service, 'client', client)
        monkeypatch.setattr(service, 'healthy_client', client)
        async with Worker(environment.client, task_queue=config.queue,
                          workflows=[ClosedTerminalFixture],
                          workflow_runner=UnsandboxedWorkflowRunner(),
                          activities=[delivery_project, cleanup, tracker]):
            handle = await environment.client.start_workflow(
                ClosedTerminalFixture.run, args=[spec, None],
                id=f"delivery-{request['run_id']}", task_queue=config.queue,
                run_timeout=timedelta(seconds=20),
                memo={'request_digest': spec['request_digest']},
            )
            await asyncio.wait_for(handle.result(), 25)
            before = service.store.detail(request['run_id'])
            assert before['phase'] == 'waiting_tracker'
            payload = {'command_id': 'tracker-valid',
                       'expected_revision': before['protocol_revision']}
            assess_url = f"/api/runs/{request['run_id']}/assess-transition"
            execute_url = f"/api/runs/{request['run_id']}/reconcile-tracker"
            transport = httpx.ASGITransport(app=app, client=('127.0.0.1', 10001))
            async with httpx.AsyncClient(transport=transport,
                                        base_url='http://127.0.0.1:18770') as browser:
                session = (await browser.get('/api/session')).json()
                headers = {'Origin': 'http://127.0.0.1:18770',
                           'X-Devflow-CSRF': session['csrf_token']}

                async def assess(command):
                    response = await browser.post(assess_url, json={
                        'kind': 'reconcile_tracker', 'request': command})
                    assert response.status_code == 200, response.text
                    return response.json()

                def records():
                    with service.store._connect() as db:
                        return {table: db.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
                                for table in ('delivery_closures', 'delivery_commands',
                                              'delivery_mutations', 'delivery_effects')}

                original_records = records()
                good = await assess(payload)
                assert good['eligible'] and good['proof_digest']
                assert good['observed_revision'] == payload['expected_revision']
                assert records() == original_records
                assert closures.sealed_record(service.store, handle.id) is None

                # A read-only assessment must not repair configuration modes.
                snapshots = config.state_root / 'configuration-snapshots'
                assert snapshots.is_dir()
                original_mode = snapshots.stat().st_mode & 0o777
                try:
                    snapshots.chmod(0o755)
                    configuration = await assess(payload)
                    assert not configuration['eligible']
                    assert snapshots.stat().st_mode & 0o777 == 0o755
                finally:
                    snapshots.chmod(original_mode)

                with service.store._connect() as db:
                    existing_command = db.execute(
                        'SELECT command_id FROM delivery_commands LIMIT 1').fetchone()[0]
                collision = await assess({**payload, 'command_id': existing_command})
                assert not collision['eligible']
                assert 'command_identity_conflict' in {
                    item['code'] for item in collision['blockers']}
                assert records() == original_records

                async def expired_client():
                    return _Expired(environment.client, handle.id)

                monkeypatch.setattr(service, 'client', expired_client)
                closure_root = config.state_root / 'closures'
                if closure_root.exists():
                    assert not list(closure_root.iterdir())
                    closure_root.rmdir()
                # A retained index with missing bytes must not recreate the root.
                with service.store._connect() as db:
                    db.execute(
                        'INSERT INTO delivery_closures VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                        (handle.id, handle.result_run_id, request['run_id'], 'COMPLETED',
                         '2026-01-01T00:00:00+00:00', spec['request_digest'], None,
                         '0' * 64, 1, 'temporal', None, '2026-01-01T00:00:00+00:00'))
                missing = await assess(payload)
                assert not missing['eligible']
                assert 'terminal_history_or_custody_unknown' in {
                    item['code'] for item in missing['blockers']}
                assert not closure_root.exists()
                with service.store._connect() as db:
                    db.execute('DELETE FROM delivery_closures WHERE workflow_id=?', (handle.id,))
                assert records() == original_records
                monkeypatch.setattr(service, 'client', client)

                async def unavailable_client():
                    raise RPCError('fixture transport unavailable', RPCStatusCode.UNAVAILABLE, b'')

                monkeypatch.setattr(service, 'client', unavailable_client)
                unavailable = await assess(payload)
                assert not unavailable['eligible']
                assert 'terminal_history_or_custody_unknown' in {
                    item['code'] for item in unavailable['blockers']}
                monkeypatch.setattr(service, 'client', client)

                # A candidate not supported by the authenticated source proof is unknown.
                with service.store._connect() as db:
                    original_candidate = db.execute(
                        'SELECT candidate_json FROM delivery_runs WHERE run_id=?',
                        (request['run_id'],),
                    ).fetchone()[0]
                    db.execute('UPDATE delivery_runs SET candidate_json=? WHERE run_id=?',
                               (json.dumps({'id': 'different', 'head': 'different'}),
                                request['run_id']))
                candidate = await assess(payload)
                assert not candidate['eligible']
                with service.store._connect() as db:
                    db.execute('UPDATE delivery_runs SET candidate_json=? WHERE run_id=?',
                               (original_candidate, request['run_id']))

                # Current issue ownership remains an independent admission predicate.
                with service.store._connect() as db:
                    service.store.state.update(db, 'work', {'id': spec['work_id'],
                        'issue': spec['issue_url'].rsplit('/', 1)[0] + '/999'}, None)
                owner = await assess(payload)
                assert not owner['eligible']
                assert 'owner_proof_unknown' in {item['code'] for item in owner['blockers']}
                with service.store._connect() as db:
                    service.store.state.update(db, 'work', {'id': spec['work_id'],
                                                          'issue': spec['issue_url']}, None)

                stale = {'command_id': 'tracker-stale',
                         'expected_revision': payload['expected_revision'] - 1}
                assessed = await assess(stale)
                assert 'revision_changed' in {item['code'] for item in assessed['blockers']}
                refused = await browser.post(execute_url, json=stale, headers=headers)
                assert refused.status_code == 409, refused.text
                assert 'revision_changed' in {
                    item['code'] for item in refused.json()['detail']['blockers']}

                # Tampering with the database-selected closure after its bytes
                # were authenticated cannot authorize a successor.
                original_capture = terminal._capture_closure
                async def changed_capture(store, run_id, closed):
                    proof = await original_capture(store, run_id, closed)
                    with store._connect() as db:
                        db.execute('UPDATE delivery_closures SET source_sha256=? '
                                   'WHERE workflow_id=? AND execution_run_id=?',
                                   ('changed', closed['workflow_id'], closed['execution_run_id']))
                    return proof

                monkeypatch.setattr(terminal, '_capture_closure', changed_capture)
                changed_closure = {**payload, 'command_id': 'tracker-closure-race'}
                response = await browser.post(execute_url, json=changed_closure, headers=headers)
                assert response.status_code == 409, response.text
                assert 'checkpoint_changed' in {
                    item['code'] for item in response.json()['detail']['blockers']}
                with service.store._connect() as db:
                    db.execute('UPDATE delivery_closures SET source_sha256=NULL '
                               'WHERE workflow_id=?', (handle.id,))
                monkeypatch.setattr(terminal, '_capture_closure', original_capture)

                # A second, unselected run under the same workflow identity is
                # ambiguous and cannot inherit the captured run's proof.
                async def changed_selection(store, run_id, closed):
                    proof = await original_capture(store, run_id, closed)
                    with store._connect() as db:
                        db.execute(
                            'INSERT INTO delivery_closures SELECT workflow_id,?,run_id,status,'
                            'closed_at,request_digest,recovery_digest,history_sha256,event_count,'
                            'source,source_sha256,sealed_at FROM delivery_closures '
                            'WHERE workflow_id=? AND execution_run_id=?',
                            ('different-run', closed['workflow_id'], closed['execution_run_id']))
                    return proof

                monkeypatch.setattr(terminal, '_capture_closure', changed_selection)
                changed_run = {**payload, 'command_id': 'tracker-run-race'}
                response = await browser.post(execute_url, json=changed_run, headers=headers)
                assert response.status_code == 409, response.text
                assert 'terminal_history_or_custody_unknown' in {
                    item['code'] for item in response.json()['detail']['blockers']}
                with service.store._connect() as db:
                    db.execute('DELETE FROM delivery_closures WHERE workflow_id=? '
                               'AND execution_run_id=?', (handle.id, 'different-run'))
                monkeypatch.setattr(terminal, '_capture_closure', original_capture)

                # An ownership change after initial observation is rechecked under
                # the actual SQLite admission fence, before the successor is queued.
                original_snapshot = terminal.snapshot
                changed = False
                def change_after_observation(store, observed_spec, **kwargs):
                    nonlocal changed
                    result = original_snapshot(store, observed_spec, **kwargs)
                    if not changed and not kwargs.get('read_only'):
                        changed = True
                        with store._connect() as db:
                            store.state.update(db, 'work', {'id': spec['work_id'],
                                'issue': spec['issue_url'].rsplit('/', 1)[0] + '/999'}, None)
                    return result

                monkeypatch.setattr(terminal, 'snapshot', change_after_observation)
                raced = {**payload, 'command_id': 'tracker-owner-race'}
                response = await browser.post(execute_url, json=raced, headers=headers)
                assert response.status_code == 409, response.text
                with service.store._connect() as db:
                    assert (db.execute('SELECT count(*) FROM delivery_commands').fetchone()[0]
                            == original_records['delivery_commands'])
                    service.store.state.update(db, 'work', {'id': spec['work_id'],
                                                          'issue': spec['issue_url']}, None)
                monkeypatch.setattr(terminal, 'snapshot', original_snapshot)

                # A slow Temporal observation holds no SQLite writer fence.
                # Timeout refuses the successor and allows an unrelated writer.
                original_tail = terminal.closed_tail
                entered = asyncio.Event()
                async def slow_tail(*args, **kwargs):
                    if kwargs.get('preserve', True):
                        entered.set()
                        await asyncio.sleep(5)
                    return await original_tail(*args, **kwargs)

                monkeypatch.setattr(terminal, 'closed_tail', slow_tail)
                monkeypatch.setattr(terminal, 'CLOSED_HISTORY_TIMEOUT_SECONDS', 3)
                slow = {**payload, 'command_id': 'tracker-slow'}
                timed_out = asyncio.create_task(
                    browser.post(execute_url, json=slow, headers=headers))
                await asyncio.wait_for(entered.wait(), 3)
                started = Event()
                def competing_writer():
                    with service.store._connect() as db:
                        started.set()
                        db.execute('BEGIN IMMEDIATE')
                        db.execute('UPDATE delivery_runs SET updated_at=updated_at WHERE run_id=?',
                                   (request['run_id'],))

                writer = asyncio.create_task(asyncio.to_thread(competing_writer))
                assert await asyncio.to_thread(started.wait, 2)
                await asyncio.wait_for(writer, 2)
                assert not timed_out.done()
                refused = await timed_out
                assert refused.status_code == 409, refused.text
                assert 'terminal_history_or_custody_unknown' in {
                    item['code'] for item in refused.json()['detail']['blockers']}
                with service.store._connect() as db:
                    assert (db.execute('SELECT count(*) FROM delivery_commands').fetchone()[0]
                            == original_records['delivery_commands'])

                # Canceling a slow observation does not queue an operation. The
                # same command can be retried after observation is available.
                monkeypatch.setattr(terminal, 'CLOSED_HISTORY_TIMEOUT_SECONDS', 30)
                entered.clear()
                command_task = asyncio.create_task(
                    browser.post(execute_url, json=payload, headers=headers))
                await asyncio.wait_for(entered.wait(), 3)
                writer = asyncio.create_task(asyncio.to_thread(competing_writer))
                await asyncio.wait_for(writer, 1)
                command_task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await command_task
                monkeypatch.setattr(terminal, 'closed_tail', original_tail)

                result = await browser.post(execute_url, json=payload, headers=headers)
                assert result.status_code == 200, result.text
                assert result.json()['reconciliation_only']
                saved = service.store.pending_starts()[0]
                recovery = json.loads(saved['recovery_json'])
                proof = recovery['closure_proof']
                assert proof['record']['execution_run_id'] == handle.result_run_id
                assert proof['record']['history_sha256'] == recovery['closed']['history_sha256']
                assert proof['transport_sha256'] and proof['projection_sha256']
                assert closures.sealed_record(service.store, handle.id) is not None
                replay = await browser.post(execute_url, json=payload, headers=headers)
                assert replay.json() == result.json()
                assert calls.count('cleanup') == 1
