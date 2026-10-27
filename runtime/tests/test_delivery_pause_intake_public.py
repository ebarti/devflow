"""Public intake-only pause admission and actual dispatcher fencing."""
from types import SimpleNamespace

import httpx
import pytest
from temporalio import workflow
from test_delivery_store import service as service

from devflow_temporal.delivery_api import _dispatch_start, create_app
from devflow_temporal.delivery_pause import start_admission


@workflow.defn(name="DevflowDeliveryWorkflow")
class IntakeWorker:
    @workflow.run
    async def run(self, spec, recovery=None):
        await workflow.wait_condition(lambda: False)


def _controlled_cutover(monkeypatch):
    # This seam is fixture-only; it is not an installation witness.
    from contextlib import nullcontext

    from devflow_temporal import delivery_pause_context, delivery_pause_namespace

    monkeypatch.setattr(delivery_pause_context, 'require_intake_dispatcher_cutover',
                        lambda _config: None)
    monkeypatch.setattr(delivery_pause_namespace, 'intake_namespace_scope',
                        lambda _config: nullcontext())


@pytest.mark.asyncio
async def test_public_start_paused_survives_service_recreation_and_blocks_dispatch(
        service, monkeypatch):
    # Controlled cutover seam: production admission remains closed until verified.
    _controlled_cutover(monkeypatch)
    store, request = service
    app = create_app(store.config.path)
    origin = app.state.delivery.config.dashboard_url
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin) as client:
        session = await client.get('/api/session')
        headers = {'Origin': origin, 'X-Devflow-CSRF': session.json()['csrf_token']}
        response = await client.post('/api/runs', json={**request, 'start_paused': True},
                                     headers=headers)
        assert response.status_code == 200, response.text
        status = await client.get('/api/runs/run-1/pause-control')
        assert status.status_code == 200
        assert status.json()['scope'] == 'before_start'
        assert status.json()['observed'] == 'pause_requested'
        assert not status.json()['pause_eligible']
        assert (await client.get('/api/runs/run-1')).status_code == 200
    reopened = create_app(store.config.path)
    new_store = reopened.state.delivery.store
    spec = new_store.effective_spec('run-1')
    assert spec['pause_intake_version'] == 1
    assert 'pause_control_version' not in spec
    item = new_store.pending_starts()[0]

    class NoSDK:
        def get_workflow_handle(self, *_args, **_kwargs):
            pytest.fail('closed intake fence reached the SDK')

    await _dispatch_start(SimpleNamespace(store=new_store), NoSDK(), item, spec)
    with new_store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_pause_steps "
                          "WHERE root_id='run-1' AND step='start'").fetchone()[0] == 0
        assert new_store.state.claim_for(db, 'work-1')['owner'] == 'external:devflow:run-1'
        outbox = db.execute("SELECT state FROM delivery_outbox WHERE run_id='run-1'").fetchone()
        assert outbox[0] == 'pending'


@pytest.mark.asyncio
async def test_public_intake_pause_refuses_a_previously_admitted_start(service, monkeypatch):
    _controlled_cutover(monkeypatch)
    store, request = service
    app = create_app(store.config.path)
    origin = app.state.delivery.config.dashboard_url
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin) as client:
        session = await client.get('/api/session')
        headers = {'Origin': origin, 'X-Devflow-CSRF': session.json()['csrf_token']}
        assert (await client.post('/api/runs', json={**request, 'start_paused': True},
                                  headers=headers)).status_code == 200
        current = app.state.delivery.store
        with current._connect() as db:
            db.execute("UPDATE delivery_pause_control SET state='running' WHERE root_id='run-1'")
        with start_admission(current, 'run-1', 'delivery-run-1', 1, epoch=1,
                             validate=lambda *_: None):
            pass
        status = (await client.get('/api/runs/run-1/pause-control')).json()
        assert not status['pause_eligible']
        rejected = await client.post('/api/runs/run-1/pause', headers=headers,
                                     json={'command_id': 'late-pause',
                                           'expected_revision': status['revision']})
        assert rejected.status_code == 409
        assert 'after start admission' in rejected.text


@pytest.mark.asyncio
async def test_public_admission_refuses_without_dispatcher_cutover(service):
    store, request = service
    app = create_app(store.config.path)
    origin = app.state.delivery.config.dashboard_url
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin) as client:
        session = await client.get('/api/session')
        headers = {'Origin': origin, 'X-Devflow-CSRF': session.json()['csrf_token']}
        response = await client.post('/api/runs', json={**request, 'start_paused': True},
                                     headers=headers)
        assert response.status_code == 409
        assert 'fresh namespace' in response.text
        with app.state.delivery.store._connect() as db:
            assert db.execute("SELECT 1 FROM delivery_runs WHERE run_id='run-1'").fetchone() is None


@pytest.mark.asyncio
async def test_public_intake_pump_restart_resume_and_real_temporal_dispatch(service, monkeypatch):
    """SDK, service and dispatcher are real; fresh-installation cutover is controlled."""
    import asyncio
    import json

    from temporal_test_server import local_temporal
    from temporalio.service import RPCError, RPCStatusCode
    from temporalio.worker import UnsandboxedWorkflowRunner, Worker

    _controlled_cutover(monkeypatch)
    old, request = service
    async with local_temporal() as environment:
        raw = json.loads(old.config.path.read_text())
        raw['temporal_address'] = environment.client.service_client.config.target_host
        old.config.path.write_text(json.dumps(raw))
        app = create_app(old.config.path)
        origin = app.state.delivery.config.dashboard_url
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                         base_url=origin) as client:
                session = await client.get('/api/session')
                headers = {'Origin': origin, 'X-Devflow-CSRF': session.json()['csrf_token']}
                response = await client.post('/api/runs', headers=headers,
                                             json={**request, 'start_paused': True})
                assert response.status_code == 200, response.text
                async with asyncio.timeout(30):
                    while True:
                        status = (await client.get('/api/runs/run-1/pause-control')).json()
                        if status['observed'] == 'paused':
                            break
                        await asyncio.sleep(.1)
                assert status['checkpoint']['kind'] == 'unstarted_intake'
                assert status['checkpoint']['resumable'] is True
                with pytest.raises(RPCError) as missing:
                    await environment.client.get_workflow_handle('delivery-run-1').describe()
                assert missing.value.status == RPCStatusCode.NOT_FOUND
                with app.state.delivery.store._connect() as db:
                    claim_before = dict(app.state.delivery.store.state.claim_for(db, 'work-1'))
                    run_before = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'")
                                      .fetchone())
        # A fresh service reads the durable pause before the explicit resume.
        restarted = create_app(old.config.path)
        from devflow_temporal import delivery_pause_dispatch

        original_send_start = delivery_pause_dispatch.send_start
        entered, release = asyncio.Event(), asyncio.Event()
        first_dispatch = True

        async def race_before_start(*args, **kwargs):
            nonlocal first_dispatch
            if first_dispatch:
                first_dispatch = False
                entered.set()
                await release.wait()
            return await original_send_start(*args, **kwargs)

        monkeypatch.setattr(delivery_pause_dispatch, 'send_start', race_before_start)
        async with (Worker(environment.client, task_queue=restarted.state.delivery.config.queue,
                           workflow_runner=UnsandboxedWorkflowRunner(), workflows=[IntakeWorker]),
                    restarted.router.lifespan_context(restarted)):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=restarted),
                                         base_url=origin) as client:
                session = await client.get('/api/session')
                headers = {'Origin': origin, 'X-Devflow-CSRF': session.json()['csrf_token']}
                status = (await client.get('/api/runs/run-1/pause-control')).json()
                assert status['observed'] == 'paused'
                response = await client.post('/api/runs/run-1/resume', headers=headers,
                                             json={'command_id': 'resume-intake',
                                                   'expected_revision': status['revision'],
                                                   'epoch': status['epoch']})
                assert response.status_code == 200, response.text
                # The real dispatcher owns its lease but has not admitted the
                # start. A concurrent public pause must win before its SDK send.
                await asyncio.wait_for(entered.wait(), timeout=30)
                status = (await client.get('/api/runs/run-1/pause-control')).json()
                assert status['pause_eligible']
                raced = await client.post('/api/runs/run-1/pause', headers=headers,
                                          json={'command_id': 'pause-race',
                                                'expected_revision': status['revision']})
                assert raced.status_code == 200, raced.text
                release.set()
                async with asyncio.timeout(30):
                    while True:
                        status = (await client.get('/api/runs/run-1/pause-control')).json()
                        if status['observed'] == 'paused':
                            break
                        await asyncio.sleep(.1)
                with pytest.raises(RPCError) as missing:
                    await environment.client.get_workflow_handle('delivery-run-1').describe()
                assert missing.value.status == RPCStatusCode.NOT_FOUND
                resumed = await client.post('/api/runs/run-1/resume', headers=headers,
                                            json={'command_id': 'resume-after-race',
                                                  'expected_revision': status['revision'],
                                                  'epoch': status['epoch']})
                assert resumed.status_code == 200, resumed.text
                async with asyncio.timeout(30):
                    while True:
                        with restarted.state.delivery.store._connect() as db:
                            row = db.execute("SELECT state FROM delivery_outbox "
                                             "WHERE run_id='run-1'").fetchone()
                        if row[0] == 'sent':
                            break
                        await asyncio.sleep(.1)
                handle = environment.client.get_workflow_handle('delivery-run-1')
                description = await handle.describe()
                status = (await client.get('/api/runs/run-1/pause-control')).json()
                assert not status['pause_eligible']
                with restarted.state.delivery.store._connect() as db:
                    control = db.execute("SELECT * FROM delivery_pause_control "
                                         "WHERE root_id='run-1'").fetchone()
                    assert control['execution_run_id'] == ''
                    receipt = dict(db.execute("SELECT * FROM delivery_pause_start_receipts "
                                              "WHERE root_id='run-1'").fetchone())
                    assert receipt['execution_run_id'] == description.run_id
                    assert db.execute("SELECT COUNT(*) FROM delivery_pause_actors "
                                      "WHERE root_id='run-1'").fetchone()[0] == 0
                    claim_after = restarted.state.delivery.store.state.claim_for(db, 'work-1')
                    assert dict(claim_after) == claim_before
                    run_after = db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'")
                    run_after = run_after.fetchone()
                    assert run_after['request_json'] == run_before['request_json']
                from devflow_temporal.delivery_pause_dispatch import bind_start

                spec = restarted.state.delivery.store.effective_spec('run-1')
                await bind_start(restarted.state.delivery.store, spec, 'delivery-run-1',
                                 description.run_id)
                with restarted.state.delivery.store._connect() as db:
                    replay = dict(db.execute("SELECT * FROM delivery_pause_start_receipts "
                                             "WHERE root_id='run-1'").fetchone())
                    assert replay == receipt
                    assert db.execute("SELECT COUNT(*) FROM delivery_pause_start_receipts "
                                      "WHERE root_id='run-1'").fetchone()[0] == 1
                assert (await handle.describe()).run_id == description.run_id
                late = await client.post('/api/runs/run-1/pause', headers=headers,
                                         json={'command_id': 'late-pause',
                                               'expected_revision': status['revision']})
                assert late.status_code == 409
                assert 'after start admission' in late.text
                await handle.terminate('owned fixture cleanup')


@pytest.mark.asyncio
async def test_public_service_refuses_unqualified_full_workflow_pause(service, monkeypatch):
    store, _request = service
    app = create_app(store.config.path)
    monkeypatch.setattr(app.state.delivery.store, 'submitted_spec',
                        lambda _run: {'pause_control_version': 1})
    origin = app.state.delivery.config.dashboard_url
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin) as client:
        session = await client.get('/api/session')
        headers = {'Origin': origin, 'X-Devflow-CSRF': session.json()['csrf_token']}
        for path, body in (('/api/runs/run-1/pause',
                            {'command_id': 'pause', 'expected_revision': 0}),
                           ('/api/runs/run-1/resume',
                            {'command_id': 'resume', 'expected_revision': 0, 'epoch': 1})):
            response = await client.post(path, headers=headers, json=body)
            assert response.status_code == 409
            assert 'scope is not enabled' in response.text
        assert (await client.get('/api/runs/run-1/pause-control')).status_code == 409


@pytest.mark.asyncio
async def test_pause_wins_before_dispatch_description_failure(service, monkeypatch):
    import asyncio

    _controlled_cutover(monkeypatch)
    store, request = service
    app = create_app(store.config.path)
    origin = app.state.delivery.config.dashboard_url
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin) as client:
        session = await client.get('/api/session')
        headers = {'Origin': origin, 'X-Devflow-CSRF': session.json()['csrf_token']}
        assert (await client.post('/api/runs', headers=headers,
                                  json={**request, 'start_paused': True})).status_code == 200
        current = app.state.delivery.store
        with current._connect() as db:
            db.execute("UPDATE delivery_pause_control SET state='running' WHERE root_id='run-1'")
        item = current.pending_starts()[0]
        spec = current.effective_spec('run-1')
        entered, release = asyncio.Event(), asyncio.Event()

        class Handle:
            async def describe(self):
                entered.set()
                await release.wait()
                raise OSError('controlled pre-start read failure')

        class SDK:
            def get_workflow_handle(self, *_args, **_kwargs):
                return Handle()

        task = asyncio.create_task(
            _dispatch_start(SimpleNamespace(store=current), SDK(), item, spec))
        await asyncio.wait_for(entered.wait(), timeout=5)
        status = (await client.get('/api/runs/run-1/pause-control')).json()
        response = await client.post('/api/runs/run-1/pause', headers=headers,
                                     json={'command_id': 'pause-describe',
                                           'expected_revision': status['revision']})
        assert response.status_code == 200, response.text
        with current._connect() as db:
            before = dict(db.execute("SELECT * FROM delivery_outbox WHERE run_id='run-1'")
                          .fetchone())
        release.set()
        await asyncio.wait_for(task, timeout=5)
        with current._connect() as db:
            after = dict(db.execute("SELECT * FROM delivery_outbox WHERE run_id='run-1'")
                         .fetchone())
            assert after == before
            control = db.execute("SELECT state FROM delivery_pause_control WHERE root_id='run-1'")
            assert control.fetchone()[0] == 'pause_requested'


@pytest.mark.asyncio
async def test_pause_wins_before_conflicting_start_memo_is_recorded(service, monkeypatch):
    import asyncio

    _controlled_cutover(monkeypatch)
    store, request = service
    app = create_app(store.config.path)
    origin = app.state.delivery.config.dashboard_url
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin) as client:
        session = await client.get('/api/session')
        headers = {'Origin': origin, 'X-Devflow-CSRF': session.json()['csrf_token']}
        assert (await client.post('/api/runs', headers=headers,
                                  json={**request, 'start_paused': True})).status_code == 200
        current = app.state.delivery.store
        with current._connect() as db:
            db.execute("UPDATE delivery_pause_control SET state='running' WHERE root_id='run-1'")
        item = current.pending_starts()[0]
        spec = current.effective_spec('run-1')
        entered, release = asyncio.Event(), asyncio.Event()

        class Description:
            async def memo_value(self, *_args):
                entered.set()
                await release.wait()
                return 'conflicting-request'

        class Handle:
            async def describe(self):
                return Description()

        class SDK:
            def get_workflow_handle(self, *_args, **_kwargs):
                return Handle()

        task = asyncio.create_task(
            _dispatch_start(SimpleNamespace(store=current), SDK(), item, spec))
        await asyncio.wait_for(entered.wait(), timeout=5)
        status = (await client.get('/api/runs/run-1/pause-control')).json()
        response = await client.post('/api/runs/run-1/pause', headers=headers,
                                     json={'command_id': 'pause-memo',
                                           'expected_revision': status['revision']})
        assert response.status_code == 200, response.text
        with current._connect() as db:
            before = dict(db.execute("SELECT * FROM delivery_outbox WHERE run_id='run-1'")
                          .fetchone())
        release.set()
        await asyncio.wait_for(task, timeout=5)
        with current._connect() as db:
            after = dict(db.execute("SELECT * FROM delivery_outbox WHERE run_id='run-1'")
                         .fetchone())
            assert after == before
            control = db.execute("SELECT state FROM delivery_pause_control WHERE root_id='run-1'")
            assert control.fetchone()[0] == 'pause_requested'


@pytest.mark.asyncio
async def test_namespace_scope_covers_submit_and_public_pause_commits(service, monkeypatch):
    from contextlib import contextmanager

    from devflow_temporal import delivery_pause as pause
    from devflow_temporal import delivery_pause_namespace as namespace

    _controlled_cutover(monkeypatch)
    active, events = [], []

    @contextmanager
    def scope(_config):
        active.append(True)
        events.append('enter')
        try:
            yield
        finally:
            active.pop()
            events.append('exit')

    monkeypatch.setattr(namespace, 'intake_namespace_scope', scope)
    store, request = service
    app = create_app(store.config.path)
    actual_submit = app.state.delivery.store._submit

    def submit(*args, **kwargs):
        assert active == [True]
        return actual_submit(*args, **kwargs)

    monkeypatch.setattr(app.state.delivery.store, '_submit', submit)
    original_pause = pause.request_pause

    def request_pause(*args, **kwargs):
        assert active == [True]
        return original_pause(*args, **kwargs)

    monkeypatch.setattr(pause, 'request_pause', request_pause)
    origin = app.state.delivery.config.dashboard_url
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin) as client:
        session = await client.get('/api/session')
        headers = {'Origin': origin, 'X-Devflow-CSRF': session.json()['csrf_token']}
        response = await client.post('/api/runs', headers=headers,
                                     json={**request, 'start_paused': True})
        assert response.status_code == 200, response.text
        with app.state.delivery.store._connect() as db:
            db.execute("UPDATE delivery_pause_control SET state='running' WHERE root_id='run-1'")
        status = (await client.get('/api/runs/run-1/pause-control')).json()
        response = await client.post('/api/runs/run-1/pause', headers=headers,
                                     json={'command_id': 'scoped-pause',
                                           'expected_revision': status['revision']})
        assert response.status_code == 200, response.text
    assert events == ['enter', 'exit', 'enter', 'exit'] and not active
