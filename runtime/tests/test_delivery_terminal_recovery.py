"""Real Temporal checkpoint and public terminal-only readback recovery."""
from __future__ import annotations

import asyncio
import shutil

import httpx
import pytest
from temporalio import activity, workflow
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration

from devflow_temporal.delivery_activities import delivery_project
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@workflow.defn(name="ControlledTrackerCheckpoint")
class ControlledTrackerCheckpoint(DeliveryWorkflow):
    @workflow.run
    async def run(self, spec, outcome):
        self.state = {'phase': outcome, 'execution_state': 'terminal', 'outcome': outcome,
                      'iteration': 2, 'revision': 13, 'cleanup': 'none', 'checks': {},
                      'roles': [], 'run_id': spec['run_id']}
        await self._project(spec, outcome, 'Controlled terminal fixture; no model or publication')
        return self.state


@pytest.mark.asyncio
@pytest.mark.parametrize('lost_receipt', [False, True])
@pytest.mark.parametrize('outcome', ['delivered', 'blocked', 'cancelled'])
async def test_real_pending_terminal_remains_open_and_public_retry_delivers(
    native_configuration, monkeypatch, lost_receipt, outcome,
):
    config, request = native_configuration
    app = create_app(config.path)
    service = app.state.delivery
    service.store.submit(request)
    spec = service.store.spec(request['run_id'])
    calls = []

    @activity.defn(name='delivery_finalize_resources')
    async def cleanup(_request):
        calls.append('cleanup')
        return {'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
                'resource_cleanup': 'confirmed'}

    @activity.defn(name='delivery_terminal_tracker')
    async def tracker(request):
        calls.append('tracker')
        pending = calls.count('tracker') <= 3
        return {'state': 'pending' if pending else 'consistent', 'pending': pending,
                'desired': request['status'], 'readback_at': 'pending' if pending else 'fresh'}

    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which('temporal'),
    ) as environment:
        class Client:
            def get_workflow_handle(self, *args, **kwargs):
                handle = environment.client.get_workflow_handle(*args, **kwargs)

                class Handle:
                    async def execute_update(self, *args, **kwargs):
                        nonlocal lost_receipt
                        result = await handle.execute_update(*args, **kwargs)
                        if lost_receipt:
                            lost_receipt = False
                            raise RPCError('receipt unavailable', RPCStatusCode.UNAVAILABLE, b'')
                        return result

                return Handle()

        async def client():
            return Client()

        monkeypatch.setattr(service, 'client', client)
        async with Worker(environment.client, task_queue='terminal-checkpoint',
                          workflows=[ControlledTrackerCheckpoint],
                          workflow_runner=UnsandboxedWorkflowRunner(),
                          activities=[delivery_project, cleanup, tracker]):
            handle = await environment.client.start_workflow(
                ControlledTrackerCheckpoint.run, args=[spec, outcome],
                id=f"delivery-{request['run_id']}",
                task_queue='terminal-checkpoint',
            )
            async with asyncio.timeout(25):
                while True:
                    state = await handle.query('status')
                    checkpoint = state.get('checks', {}).get('terminal_tracker_checkpoint', {})
                    if checkpoint.get('waiting'):
                        break
                    await asyncio.sleep(0.05)
            assert (await handle.describe()).close_time is None
            assert state['phase'] == 'waiting_tracker' and state['outcome'] is None
            assert calls == ['cleanup', 'tracker', 'tracker', 'tracker']
            projected = service.store.detail(request['run_id'])
            assert projected['outcome'] is None and projected['tracker']['state'] == 'pending'
            transport = httpx.ASGITransport(app=app, client=('127.0.0.1', 10001))
            async with httpx.AsyncClient(transport=transport,
                                        base_url='http://127.0.0.1:18770') as browser:
                endpoint = f"/api/runs/{request['run_id']}/reconcile-tracker"
                payload = {'command_id': 'same-terminal-retry',
                           'expected_revision': state['revision']}
                assert (await browser.post(endpoint, json=payload)).status_code == 403
                session = await browser.get('/api/session')
                headers = {'Origin': 'http://127.0.0.1:18770',
                           'X-Devflow-CSRF': session.json()['csrf_token']}
                receipt = await browser.post(endpoint, json=payload, headers=headers)
                assert receipt.status_code in {200, 503}, receipt.text
                result = await asyncio.wait_for(handle.result(), 10)
                assert result['outcome'] == outcome
                assert result['checks']['terminal_tracker_checkpoint']['cycles'] == 2
                final = service.store.detail(request['run_id'])
                assert final['outcome'] == outcome and final['error'] is None
                assert final['tracker']['state'] == 'consistent'
                if receipt.status_code == 503:
                    with service.store._connect() as db:
                        mutation = db.execute('SELECT * FROM delivery_mutations').fetchone()
                        assert mutation['state'] == 'unknown'
                    receipt = await browser.post(endpoint, json=payload, headers=headers)
                    assert receipt.status_code == 200, receipt.text
                repeated = await browser.post(endpoint, json=payload, headers=headers)
                assert repeated.status_code == 200 and repeated.json() == receipt.json()
                assert calls == ['cleanup', 'tracker', 'tracker', 'tracker', 'tracker']
                conflict = await browser.post(endpoint, json={**payload, 'expected_revision': 999},
                                              headers=headers)
                assert conflict.status_code == 409
