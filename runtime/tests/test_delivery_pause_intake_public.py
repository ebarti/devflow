"""Public intake-only pause admission and actual dispatcher fencing."""

import httpx
import pytest
from temporalio import workflow
from test_delivery_store import service as service

from devflow_temporal.delivery_api import create_app
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
async def test_public_intake_pump_restart_resume_and_real_temporal_dispatch(service, monkeypatch):
    """SDK, service and dispatcher are real; fresh-installation cutover is controlled."""
    import asyncio
    import json
    from contextlib import AsyncExitStack

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
        from devflow_temporal import delivery_pause_dispatch, delivery_pause_intake_pump

        original_pump = delivery_pause_intake_pump.pump
        original_features = restarted.state.delivery.reproject_features_once
        dispatch_complete, intake_complete = asyncio.Event(), asyncio.Event()
        parked = asyncio.Event()
        final_epoch, sent_observed = None, False

        async def complete_intake(store, *args, **kwargs):
            result = await original_pump(store, *args, **kwargs)
            if (store is restarted.state.delivery.store and final_epoch is not None
                    and any(item.get('root_id') == 'run-1' and item.get('epoch') == final_epoch
                            and item.get('state') == 'running' for item in result['results'])):
                # The real activation and its joined status cleanup have returned.
                intake_complete.set()
                await parked.wait()
            return result

        async def complete_dispatch():
            result = await original_features()
            if sent_observed:
                # All preceding real maintenance has returned before cancellation.
                dispatch_complete.set()
                await parked.wait()
            return result

        monkeypatch.setattr(delivery_pause_intake_pump, 'pump', complete_intake)
        monkeypatch.setattr(restarted.state.delivery, 'reproject_features_once', complete_dispatch)
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
                    AsyncExitStack() as service_lifetime):
            await service_lifetime.enter_async_context(restarted.router.lifespan_context(restarted))
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
                final_epoch = status['epoch']
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
                        sent_observed = row[0] == 'sent'
                        if (sent_observed and dispatch_complete.is_set()
                                and intake_complete.is_set()):
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
                late = await client.post('/api/runs/run-1/pause', headers=headers,
                                         json={'command_id': 'late-pause',
                                               'expected_revision': status['revision']})
                assert late.status_code == 409
                assert 'after start admission' in late.text
                # Stop at pure fixture gates; dispatch finally joins its retained retry task.
                await service_lifetime.aclose()
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
                await handle.terminate('owned fixture cleanup')
