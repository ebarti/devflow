"""Public intake-only pause admission and actual dispatcher fencing."""

import httpx
import pytest
from temporalio import workflow
from test_delivery_pause_intake import unstarted as unstarted
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


@pytest.mark.asyncio
async def test_public_pause_operations_reconcile_lost_acknowledgements(unstarted, monkeypatch):
    """A lost response never repeats a fenced command or fabricates its outcome."""
    import asyncio

    from test_delivery_pause_intake import begin_intake_settlement, controlled_no_start

    from devflow_temporal import delivery_pause as pause
    from devflow_temporal import delivery_pause_intake as intake

    _controlled_cutover(monkeypatch)
    store, spec, run = unstarted
    root = spec['run_id']
    app = create_app(store.config.path)
    origin = app.state.delivery.config.dashboard_url
    calls = []
    original_pause = pause.request_pause

    def lost_pause(*args, **kwargs):
        calls.append('pause')
        original_pause(*args, **kwargs)
        raise OSError('fixture lost acknowledgement after commit')

    monkeypatch.setattr(pause, 'request_pause', lost_pause)

    async def operation(client, headers, envelope):
        response = await client.post('/api/operations', headers=headers, json=envelope)
        assert response.status_code in (200, 202), response.text
        async with asyncio.timeout(10):
            while True:
                result = (await client.get('/api/operations/' + envelope['operation_id'])).json()
                if result['state'] not in {'pending', 'running'}:
                    return result
                await asyncio.sleep(.02)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin) as client:
        session = await client.get('/api/session')
        headers = {'Origin': origin, 'X-Devflow-CSRF': session.json()['csrf_token']}
        status = (await client.get(f'/api/runs/{root}/pause-control')).json()
        envelope = {'operation_id': 'pause-op', 'run_id': root, 'kind': 'pause',
                    'request': {'command_id': 'pause-cmd', 'expected_revision': status['revision']}}
        lost = await operation(client, headers, envelope)
        assert lost['state'] == 'unknown'
        assert (await client.get(f'/api/runs/{root}/pause-control')).json()[
            'observed'] == 'pause_requested'
        # Alias resolves to the original unknown operation; changed authority is refused.
        alias = await client.post('/api/operations', headers=headers,
                                  json={**envelope, 'operation_id': 'pause-alias'})
        assert alias.json()['operation_id'] == 'pause-op'
        conflict = await client.post('/api/operations', headers=headers,
            json={**envelope, 'operation_id': 'pause-conflict',
                  'request': {**envelope['request'], 'expected_revision': 99}})
        assert conflict.status_code == 409
        assert calls == ['pause']

    # Recreate the API against the same owned database, then reconcile only the
    # exact committed domain receipt. No dispatcher or operation is replayed.
    restarted = create_app(store.config.path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=restarted),
                                 base_url=origin) as client:
        session = await client.get('/api/session')
        headers = {'Origin': origin, 'X-Devflow-CSRF': session.json()['csrf_token']}
        recovered = (await client.post('/api/operations/pause-op/reconcile',
                                       headers=headers)).json()
        assert recovered['state'] == 'succeeded'
        assert recovered['result'] == {'root_id': root, 'state': 'pause_requested',
                                       'revision': 1, 'epoch': 1}
        assert calls == ['pause']
        begin_intake_settlement(store, root)
        intake.seal_unstarted(store, root, 1, run['revision'],
                              prove_unstarted=controlled_no_start(store))
        paused = pause.publish_paused(store, root, 1)
        original_resume = pause.begin_resume

        def lost_resume(*args, **kwargs):
            calls.append('resume')
            original_resume(*args, **kwargs)
            raise OSError('fixture lost acknowledgement after commit')

        monkeypatch.setattr(pause, 'begin_resume', lost_resume)
        resume = {'operation_id': 'resume-op', 'run_id': root, 'kind': 'resume',
                  'request': {'command_id': 'resume-cmd', 'expected_revision': paused['revision'],
                              'epoch': 1}}
        # Simulate a restart after transport persistence but before admission.
        # Startup must leave this resume pending until the user repeats it.
        import json

        from devflow_temporal.contracts import digest

        with store._connect() as db:
            db.execute("""INSERT INTO delivery_operations
                (operation_id,run_id,kind,stage,request_digest,request_json,state,
                 created_at,updated_at,progress,domain_command_id)
                VALUES (?,?,'resume','execution',?,?,'pending','2026','2026','queued',?)""",
                (resume['operation_id'], root,
                 digest({'run_id': root, 'kind': 'resume', 'request': resume['request']}),
                 json.dumps(resume['request']), resume['request']['command_id']))
        await restarted.state.delivery.resume_pending_operations()
        held = (await client.get('/api/operations/resume-op')).json()
        assert held['state'] == 'pending' and held['next_action'] == 'resubmit_same_operation'
        assert (await client.get(f'/api/runs/{root}/pause-control')).json()['observed'] == 'paused'
        assert calls == ['pause']
        assert (await operation(client, headers, resume))['state'] == 'unknown'
        assert (await client.get(f'/api/runs/{root}/pause-control')).json()[
            'observed'] == 'resuming'
        result = (await client.post('/api/operations/resume-op/reconcile', headers=headers)).json()
        assert result['state'] == 'succeeded'
        assert result['result'] == {'root_id': root, 'state': 'resuming',
                                    'revision': paused['revision'] + 1, 'epoch': 1}
        assert calls == ['pause', 'resume']
        # A mismatched request cannot turn another command's receipt into success.
        with store._connect() as db:
            db.execute("UPDATE delivery_operations SET state='unknown',result_json=NULL,"
                       "request_json=? WHERE operation_id='resume-op'",
                       ('{"command_id":"resume-cmd","expected_revision":999,"epoch":1}',))
        mismatch = (await client.post('/api/operations/resume-op/reconcile',
                                      headers=headers)).json()
        assert mismatch['state'] == 'unknown'
        # Even another genuine receipt cannot be substituted for this operation.
        with store._connect() as db:
            db.execute("UPDATE delivery_operations SET kind='pause',request_json=?,"
                       "domain_command_id='pause-cmd' WHERE operation_id='resume-op'",
                       ('{"command_id":"pause-cmd","expected_revision":0}',))
        substituted = (await client.post('/api/operations/resume-op/reconcile',
                                         headers=headers)).json()
        assert substituted['state'] == 'unknown'
        assert calls == ['pause', 'resume']


@pytest.mark.asyncio
async def test_cli_and_mcp_use_the_same_durable_pause_commands(unstarted, monkeypatch, tmp_path):
    """Actual CLI and MCP clients share the public API and never start the service."""
    import asyncio
    import json
    import socket
    import subprocess
    import sys

    import uvicorn
    from mcp.shared.memory import create_connected_server_and_client_session
    from test_delivery_pause_intake import begin_intake_settlement, controlled_no_start

    from devflow_temporal import delivery_pause as pause
    from devflow_temporal import delivery_pause_intake as intake
    from devflow_temporal.delivery_mcp import build_server

    _controlled_cutover(monkeypatch)
    store, spec, run = unstarted
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    listener.setblocking(False)
    port = listener.getsockname()[1]
    host = f'http://127.0.0.1:{port}'
    config = json.loads(store.config.path.read_text())
    config['dashboard_url'] = host
    store.config.path.write_text(json.dumps(config))
    app = create_app(store.config.path)

    async def dormant():
        await asyncio.Event().wait()

    app.state.delivery.dispatch_loop = dormant
    app.state.delivery.pause_intake_loop = dormant
    server = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=port, log_level='error'))
    serving = asyncio.create_task(server.serve(sockets=[listener]))

    async def cli(command, request=None):
        argv = [sys.executable, '-m', 'devflow_temporal.delivery_control',
                '--config', str(store.config.path), command, '--id', spec['run_id']]
        if request is not None:
            request_path = tmp_path / (command + '.json')
            request_path.write_text(json.dumps(request))
            argv += ['--request', str(request_path)]
        result = await asyncio.to_thread(subprocess.run, argv, capture_output=True, text=True,
                                         timeout=15)
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)

    async def completed(client, op):
        async with asyncio.timeout(10):
            while True:
                result = (await client.get('/api/operations/' + op)).json()
                if result['state'] not in {'pending', 'running'}:
                    return result
                await asyncio.sleep(.02)

    try:
        async with asyncio.timeout(10):
            while not server.started:
                await asyncio.sleep(.02)
        assert (await cli('pause-control'))['scope'] == 'before_start'
        request = {'operation_id': 'cross-pause', 'command_id': 'cross-pause-cmd',
                   'expected_revision': 0}
        first = await cli('pause', request)
        assert first['operation_id'] == 'cross-pause'
        async with httpx.AsyncClient(base_url=host) as client:
            result = await completed(client, 'cross-pause')
            assert result['state'] == 'succeeded' and result['result']['state'] == 'pause_requested'
            async with create_connected_server_and_client_session(
                build_server(store.config.path)
            ) as mcp:
                status = await mcp.call_tool('get_pause_control', {'run_id': spec['run_id']})
                assert not status.isError
                assert json.loads(status.content[0].text)['observed'] == 'pause_requested'
                repeated = await mcp.call_tool('pause_run', {
                    'run_id': spec['run_id'], 'request_json': json.dumps(request)})
                assert not repeated.isError
                assert json.loads(repeated.content[0].text)['operation_id'] == 'cross-pause'
                begin_intake_settlement(store, spec['run_id'])
                intake.seal_unstarted(store, spec['run_id'], 1, run['revision'],
                                      prove_unstarted=controlled_no_start(store))
                paused = pause.publish_paused(store, spec['run_id'], 1)
                resume = {'operation_id': 'cross-resume', 'command_id': 'cross-resume-cmd',
                          'expected_revision': paused['revision'], 'epoch': 1}
                response = await mcp.call_tool('resume_run', {
                    'run_id': spec['run_id'], 'request_json': json.dumps(resume)})
                assert not response.isError
                assert json.loads(response.content[0].text)['operation_id'] == 'cross-resume'
                result = await completed(client, 'cross-resume')
                assert result['state'] == 'succeeded' and result['result']['state'] == 'resuming'
                repeated = await cli('resume', resume)
                assert repeated['operation_id'] == 'cross-resume'
                assert (await cli('pause-control'))['observed'] == 'resuming'
                with store._connect() as db:
                    assert db.execute('SELECT COUNT(*) FROM delivery_pause_commands '
                                      'WHERE root_id=?', (spec['run_id'],)).fetchone()[0] == 2
    finally:
        server.should_exit = True
        await asyncio.wait_for(serving, 10)
        listener.close()
