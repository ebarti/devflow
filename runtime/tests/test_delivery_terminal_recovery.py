"""Real Temporal checkpoint and public terminal-only readback recovery."""
from __future__ import annotations

import asyncio
import json
import shutil
import sys
from datetime import timedelta

import httpx
import pytest
from temporal_test_server import local_temporal
from temporalio import activity, workflow
from temporalio.client import WorkflowFailureError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration

from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_activities import (
    delivery_accept_plan,
    delivery_finalize_resources,
    delivery_prepare,
    delivery_project,
    delivery_terminal_preflight,
)
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_resources import RunResources
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture(autouse=True)
def legacy_tracker_admission(monkeypatch):
    """These recovery cases represent specs admitted before automatic retries."""
    original = DeliveryConfig.admit

    def admit(self, request):
        spec = original(self, request)
        spec.pop('tracker_retry_version', None)
        spec['policy'].pop('tracker_retry_seconds', None)
        spec['policy_digest'] = digest(spec['policy'])
        return spec

    monkeypatch.setattr(DeliveryConfig, 'admit', admit)


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
@pytest.mark.parametrize('outcome,lost_receipt', [
    ('delivered', False), ('delivered', True), ('blocked', True), ('cancelled', True),
])
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

    async with local_temporal(
        dev_server_existing_path=shutil.which('temporal'),
    ) as environment:
        class Client:
            def get_workflow_handle(self, *args, **kwargs):
                handle = environment.client.get_workflow_handle(*args, **kwargs)

                class Handle:
                    def __getattr__(self, name):
                        return getattr(handle, name)

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


@workflow.defn(name='DevflowDeliveryWorkflow')
class ClosedTerminalFixture(DeliveryWorkflow):
    @workflow.run
    async def run(self, spec, recovery=None):
        if recovery:
            return await super().run(spec, recovery)
        self.state = {'run_id': spec['run_id'], 'phase': 'blocked',
                      'execution_state': 'blocked', 'outcome': 'blocked',
                      'iteration': 2, 'revision': 13, 'cleanup': 'none', 'checks': {},
                      'roles': [], 'error': 'Preserved failed gate'}
        await self._project(spec, 'blocked', 'Preserved failed gate')
        return self.state

    async def _finish_terminal_tracker(self, spec, checkpoint):
        # Exercise the production deadline path in seconds rather than minutes.
        if not self.terminal_reconciliation_only:
            checkpoint['deadline'] = (workflow.now() + timedelta(seconds=8)).isoformat()
        return await super()._finish_terminal_tracker(spec, checkpoint)


@pytest.mark.asyncio
@pytest.mark.parametrize('timed_out', [False, True])
async def test_authentic_closed_tail_queues_only_tracker_successor(
    native_configuration, monkeypatch, timed_out,
):
    config, request = native_configuration
    app = create_app(config.path)
    service = app.state.delivery
    service.store.submit(request)
    spec = service.store.spec(request['run_id'])
    calls = []
    historical = {'role': 'implement', 'iteration': 2, 'status': 'findings',
                  'session_id': 'preserved-session', 'cleanup': 'confirmed',
                  'finish_reason': 'done', 'findings': ['Preserved failed gate']}
    with service.store._connect() as db:
        db.execute('INSERT INTO delivery_attempts '
                   '(job_key,run_id,role,iteration,candidate_id,state,session_id,'
                   'result_json,cleanup) '
                   "VALUES (?,?,?,?,?,'finished',?,?,'confirmed')",
                   ('original-attempt', request['run_id'], 'implement', 2, 'original-candidate',
                    historical['session_id'], canonical_json(historical)))

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
        pending = calls.count('tracker') <= 3
        if not pending:
            with service.store._connect() as db:
                service.store.state.release_work(db, spec['work_id'],
                                                 f"external:devflow:{spec['run_id']}")
        return {'state': 'pending' if pending else 'consistent', 'pending': pending,
                'desired': payload['status'], 'readback_at': 'pending' if pending else 'fresh'}

    pause_once = timed_out

    @activity.defn(name='delivery_terminal_preflight')
    async def preflight(payload):
        nonlocal pause_once
        if pause_once:
            pause_once = False
            await asyncio.sleep(2)
        return await delivery_terminal_preflight(payload)

    async with local_temporal(
        dev_server_existing_path=shutil.which('temporal'),
    ) as environment:
        async def client():
            return environment.client

        monkeypatch.setattr(service, 'client', client)
        monkeypatch.setattr(service, 'healthy_client', client)
        queue = config.queue
        async with Worker(environment.client, task_queue=queue,
                          workflows=[ClosedTerminalFixture],
                          workflow_runner=UnsandboxedWorkflowRunner(),
                          activities=[delivery_project, cleanup, tracker,
                                      preflight]):
            handle = await environment.client.start_workflow(
                ClosedTerminalFixture.run, args=[spec, None],
                id=f"delivery-{request['run_id']}", task_queue=queue,
                run_timeout=timedelta(seconds=7) if timed_out else timedelta(seconds=20),
                memo={'request_digest': spec['request_digest']},
            )
            try:
                result = await asyncio.wait_for(handle.result(), 25)
                assert not timed_out
                assert result['outcome'] is None
                assert result['checks']['terminal_tracker_checkpoint']['closed']
            except Exception:
                if not timed_out:
                    raise
                assert (await handle.describe()).status.name == 'TIMED_OUT'
            assert calls == ['cleanup', 'tracker', 'tracker', 'tracker']
            before = service.store.detail(request['run_id'])
            assert before['phase'] == 'waiting_tracker'
            transport = httpx.ASGITransport(app=app, client=('127.0.0.1', 10001))
            async with httpx.AsyncClient(transport=transport,
                                        base_url='http://127.0.0.1:18770') as browser:
                session = await browser.get('/api/session')
                headers = {'Origin': 'http://127.0.0.1:18770',
                           'X-Devflow-CSRF': session.json()['csrf_token']}
                endpoint = f"/api/runs/{request['run_id']}/reconcile-tracker"
                payload = {'command_id': 'sealed-terminal-successor',
                           'expected_revision': before['protocol_revision']}
                # The actual public state helper permits rebinding released work.
                # That must never redirect the frozen terminal authority.
                with service.store._connect() as db:
                    service.store.state.update(db, 'work', {'id': spec['work_id'],
                        'issue': spec['issue_url'].rsplit('/', 1)[0] + '/999'}, None)
                refused = await browser.post(endpoint, json=payload, headers=headers)
                assert refused.status_code == 409 and 'frozen authority' in refused.text
                assert service.store.pending_starts()[0]['workflow_id'] is None
                with service.store._connect() as db:
                    service.store.state.update(db, 'work', {'id': spec['work_id'],
                                                          'issue': spec['issue_url']}, None)
                receipt = await browser.post(endpoint, json=payload, headers=headers)
                assert receipt.status_code == 200, receipt.text
                assert receipt.json()['reconciliation_only']
                recovery = service.store.pending_starts()[0]
                sealed = json.loads(recovery['recovery_json'])
                assert sealed['state']['roles'] == [historical]
                assert sealed['closed']['status'] == ('TIMED_OUT' if timed_out else 'COMPLETED')
                assert sealed['closed']['history_sha256']
                public = (await browser.get(f"/api/runs/{request['run_id']}")).json()['run']
                proof = public['terminal_tracker_recovery']
                assert proof['reconciliation_only']
                assert proof['closed_history_sha256'] == sealed['closed']['history_sha256']
                assert proof['seal'] == sealed['seal']
                assert proof['predecessor_execution_run_id'] == (await handle.describe()).run_id
                repeated = await browser.post(endpoint, json=payload, headers=headers)
                assert repeated.json() == receipt.json()
                if timed_out:
                    # The first sealed successor times out before projecting
                    # anything. Authentic start input must support a fresh
                    # reconciliation-only command without losing the checkpoint.
                    interrupted = await environment.client.start_workflow(
                        ClosedTerminalFixture.run, args=[spec, sealed],
                        id=receipt.json()['workflow_id'], task_queue=queue,
                        run_timeout=timedelta(seconds=1),
                        memo={'request_digest': spec['request_digest'],
                              'recovery_digest': digest(sealed)},
                    )
                    service.store.mark_start(request['run_id'], accepted=True)
                    with pytest.raises(WorkflowFailureError):
                        await asyncio.wait_for(interrupted.result(), 5)
                    assert (await interrupted.describe()).status.name == 'TIMED_OUT'
                    payload = {'command_id': 'after-preflight-timeout',
                               'expected_revision': service.store.detail(
                                   request['run_id'])['protocol_revision']}
                    receipt = await browser.post(endpoint, json=payload, headers=headers)
                    assert receipt.status_code == 200, receipt.text
                    assert receipt.json()['reconciliation_only']
                await service.dispatch_once()
                successor = environment.client.get_workflow_handle(receipt.json()['workflow_id'])
                final = await asyncio.wait_for(successor.result(), 15)
                assert final['outcome'] == 'blocked' and final['roles'] == [historical]
                assert calls == ['cleanup', 'tracker', 'tracker', 'tracker', 'tracker']
                projected = service.store.detail(request['run_id'])
                assert projected['outcome'] == 'blocked'
                assert projected['error'] == 'Preserved failed gate'
                assert projected['tracker']['state'] == 'consistent'
                history = await successor.fetch_history()
                started = history.events[0].workflow_execution_started_event_attributes
                assert started.workflow_run_timeout.seconds == 14 * 60
                assert started.workflow_execution_timeout.seconds == 72 * 60 * 60
                names = {event.activity_task_scheduled_event_attributes.activity_type.name
                         for event in history.events
                         if event.HasField('activity_task_scheduled_event_attributes')}
                assert names == {'delivery_terminal_preflight', 'delivery_terminal_tracker',
                                 'delivery_project'}
                replay = await browser.post(endpoint, json=payload, headers=headers)
                assert replay.json() == receipt.json()
                assert len(service.store.detail(request['run_id'])['roles']) == 1


@workflow.defn(name='DevflowDeliveryWorkflow')
class PreparedIntakeTerminalFixture(ClosedTerminalFixture):
    @workflow.run
    async def run(self, spec, recovery=None):
        return await DeliveryWorkflow.run(self, spec, recovery)

    async def _run_iterations(self, spec, **_kwargs):
        return await self._stop(spec, 'Controlled post-intake gate failure')


@workflow.defn(name='DevflowDeliveryWorkflow')
class CancelledBeforeImplementationFixture(ClosedTerminalFixture):
    @workflow.run
    async def run(self, spec, recovery=None):
        if recovery:
            return await DeliveryWorkflow.run(self, spec, recovery)
        candidate = await self._activity('fixture_candidate', {'spec': spec})
        self.state = {'run_id': spec['run_id'], 'phase': 'waiting_decision',
                      'execution_state': 'waiting_decision', 'outcome': None,
                      'iteration': 0, 'revision': 0, 'cleanup': 'none', 'checks': {},
                      'candidate': candidate, 'roles': [], 'error': None}
        await self._project(spec, 'decision_required', 'Controlled initial decision')
        await workflow.wait_condition(lambda: self.cancel_requested)
        self.state['outcome'] = 'cancelled'
        await self._project(spec, 'cancelled', 'Cancelled before implementation')
        return self.state


@pytest.mark.asyncio
@pytest.mark.parametrize('dirty', [False, True])
async def test_closed_cancelled_recovery_preserves_cleanup_source_contract(
    native_configuration, monkeypatch, dirty,
):
    config, request = native_configuration
    app = create_app(config.path)
    service = app.state.delivery
    service.store.submit(request)
    spec = service.store.spec(request['run_id'])
    broker = DeliveryBroker(service.store, spec)
    broker.prepare()
    if dirty:
        (broker.checkout / 'preserved.txt').write_text('Preserved cancelled candidate\n')
    candidate = broker.candidate()
    calls = []

    @activity.defn(name='fixture_candidate')
    async def candidate_readback(_payload):
        return candidate

    @activity.defn(name='delivery_terminal_tracker')
    async def tracker(payload):
        calls.append('tracker')
        pending = len(calls) <= 3
        if not pending:
            with service.store._connect() as db:
                service.store.state.release_work(db, spec['work_id'],
                                                 f"external:devflow:{spec['run_id']}")
        return {'state': 'pending' if pending else 'consistent', 'pending': pending,
                'desired': payload['status'], 'readback_at': 'fresh'}

    async with local_temporal(
        dev_server_existing_path=shutil.which('temporal'),
    ) as environment:
        async def client():
            return environment.client

        monkeypatch.setattr(service, 'client', client)
        monkeypatch.setattr(service, 'healthy_client', client)
        async with Worker(environment.client, task_queue=config.queue,
                          workflows=[CancelledBeforeImplementationFixture],
                          workflow_runner=UnsandboxedWorkflowRunner(),
                          activities=[candidate_readback, delivery_project,
                                      delivery_finalize_resources, tracker,
                                      delivery_terminal_preflight]):
            handle = await environment.client.start_workflow(
                CancelledBeforeImplementationFixture.run, args=[spec, None],
                id=f"delivery-{spec['run_id']}", task_queue=config.queue,
                memo={'request_digest': spec['request_digest']},
            )
            async with asyncio.timeout(10):
                while service.store.detail(spec['run_id'])['phase'] != 'waiting_decision':
                    await asyncio.sleep(0.05)
            transport = httpx.ASGITransport(app=app, client=('127.0.0.1', 10001))
            async with httpx.AsyncClient(transport=transport,
                                        base_url='http://127.0.0.1:18770') as browser:
                session = await browser.get('/api/session')
                headers = {'Origin': 'http://127.0.0.1:18770',
                           'X-Devflow-CSRF': session.json()['csrf_token']}
                endpoint = f"/api/runs/{spec['run_id']}"
                revision = service.store.detail(spec['run_id'])['protocol_revision']
                cancel = await browser.post(endpoint + '/cancel', headers=headers, json={
                    'command_id': 'cancel-before-implementation',
                    'expected_revision': revision, 'reason': 'Controlled initial cancellation'})
                assert cancel.status_code == 200, cancel.text
                stopped = await asyncio.wait_for(handle.result(), 15)
                assert stopped['outcome'] is None and stopped['phase'] == 'waiting_tracker'
                assert broker.checkout.exists() == dirty
                resources = RunResources(spec)
                cleanup = (resources.root / 'finalization.json').read_bytes()
                manifest = resources.manifest.read_bytes()
                receipt = json.loads(cleanup)
                entry = next(r for r in receipt['roots'] if r['kind'] == 'checkout')
                assert entry['state'] == ('retained' if dirty else 'removed')
                if not dirty:
                    assert entry['clean_base_removal']['candidate'] == {
                        k: candidate[k] for k in ('head', 'id', 'content_sha256')}
                elif dirty:
                    # A retained dirty source is still live-hashed, even for
                    # cancellation. Changing it cannot authorize tracker recovery.
                    changed = broker.checkout / 'preserved.txt'
                    changed.write_text('Changed after cleanup\n')
                    refused = await browser.post(endpoint + '/reconcile-tracker',
                        headers=headers, json={'command_id': 'cancelled-terminal-readback',
                        'expected_revision': stopped['revision']})
                    assert refused.status_code == 409 and 'candidate source' in refused.text
                    changed.write_text('Preserved cancelled candidate\n')
                payload = {'command_id': 'cancelled-terminal-readback',
                           'expected_revision': stopped['revision']}
                accepted = await browser.post(endpoint + '/reconcile-tracker',
                                              headers=headers, json=payload)
                assert accepted.status_code == 200, accepted.text
                await service.dispatch_once()
                successor = environment.client.get_workflow_handle(accepted.json()['workflow_id'])
                final = await asyncio.wait_for(successor.result(), 10)
                assert final['outcome'] == 'cancelled' and final['candidate'] == candidate
                assert final['roles'] == [] and len(calls) == 4
                assert broker.checkout.exists() == dirty
                assert (resources.root / 'finalization.json').read_bytes() == cleanup
                assert resources.manifest.read_bytes() == manifest
                assert (await browser.post(endpoint + '/reconcile-tracker',
                                           headers=headers, json=payload)).json() == accepted.json()
                assert service.store.detail(spec['run_id'])['tracker']['state'] == 'consistent'


def test_clean_cancelled_removal_proof_survives_lost_git_completion(
    native_configuration, monkeypatch,
):
    import subprocess

    from devflow_temporal.delivery_store import DeliveryStore

    config, request = native_configuration
    store = DeliveryStore(config)
    store.submit(request)
    spec = store.spec(request['run_id'])
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    candidate = broker.candidate()
    resources = RunResources(spec)
    original = subprocess.run
    lost = True

    def remove_then_lose_receipt(argv, **kwargs):
        nonlocal lost
        result = original(argv, **kwargs)
        if lost and argv[0] == 'git' and argv[3:5] == ['worktree', 'remove']:
            lost = False
            raise subprocess.TimeoutExpired(argv, 30)
        return result

    monkeypatch.setattr('devflow_temporal.delivery_resources.subprocess.run',
                        remove_then_lose_receipt)
    first = resources.finalize('cancelled')
    assert first['state'] == 'unknown' and not broker.checkout.exists()
    final = resources.finalize('cancelled')
    assert final['state'] == 'confirmed'
    entry = next(r for r in final['roots'] if r['kind'] == 'checkout')
    assert entry['state'] == 'already_absent'
    assert entry['clean_base_removal']['candidate'] == {
        k: candidate[k] for k in ('head', 'id', 'content_sha256')}


@pytest.mark.skipif(sys.platform != 'darwin', reason='actual native preparation required')
@pytest.mark.asyncio
async def test_closed_recovery_binds_real_preparation_and_automatic_intake_evolution(
    native_configuration, monkeypatch,
):
    config, request = native_configuration
    config.raw['execution_mode'] = 'trusted-local'
    config.path.write_text(json.dumps(config.raw))
    app = create_app(config.path)
    service = app.state.delivery
    service.store.submit(request)
    submitted = service.store.submitted_spec(request['run_id'])
    calls = []

    @activity.defn(name='delivery_intake')
    async def plan(_payload):
        calls.append('intake')
        return {'status': 'plan', 'cleanup': 'confirmed', 'plan': {
            'scope': 'Controlled original README scope', 'steps': ['Inspect owned README'],
            'verification': ['Controlled owning gate'], 'acceptance': ['Owned README checked'],
        }}

    @activity.defn(name='delivery_tracker_start')
    async def started(_payload):
        return {'state': 'consistent'}

    @activity.defn(name='delivery_terminal_tracker')
    async def tracker(_payload):
        calls.append('tracker')
        consistent = calls.count('tracker') > 3
        if consistent:
            with service.store._connect() as db:
                service.store.state.release_work(db, submitted['work_id'],
                    f"external:devflow:{submitted['run_id']}")
        return {'state': 'consistent' if consistent else 'pending', 'pending': not consistent}

    async with local_temporal(
        dev_server_existing_path=shutil.which('temporal'),
    ) as environment:
        async def client():
            return environment.client

        monkeypatch.setattr(service, 'client', client)
        monkeypatch.setattr(service, 'healthy_client', client)
        async with Worker(environment.client, task_queue=config.queue,
                          workflows=[PreparedIntakeTerminalFixture],
                          workflow_runner=UnsandboxedWorkflowRunner(),
                          activities=[delivery_prepare, delivery_project, delivery_accept_plan,
                                      delivery_finalize_resources, delivery_terminal_preflight,
                                      plan, started, tracker]):
            # Dispatch the original raw admission through the actual service.
            await service.dispatch_once()
            original = environment.client.get_workflow_handle(f"delivery-{request['run_id']}")
            result = await asyncio.wait_for(original.result(), 30)
            assert result['outcome'] is None
            effective = service.store.effective_spec(request['run_id'])
            assert effective != submitted and effective['accepted_plan']
            assert effective['policy']['native_identity']
            transport = httpx.ASGITransport(app=app, client=('127.0.0.1', 10001))
            async with httpx.AsyncClient(transport=transport,
                                        base_url='http://127.0.0.1:18770') as browser:
                session = await browser.get('/api/session')
                receipt = await browser.post(f"/api/runs/{request['run_id']}/reconcile-tracker",
                    json={'command_id': 'after-real-intake',
                          'expected_revision': result['revision']},
                    headers={'Origin': 'http://127.0.0.1:18770',
                             'X-Devflow-CSRF': session.json()['csrf_token']})
                assert receipt.status_code == 200, receipt.text
                recovery = json.loads(service.store.pending_starts()[0]['recovery_json'])
                assert recovery['closed']['input_spec'] == submitted
                assert recovery['closed']['projection']['spec'] == effective
                await service.dispatch_once()
                successor = environment.client.get_workflow_handle(receipt.json()['workflow_id'])
                final = await asyncio.wait_for(successor.result(), 15)
                assert final['outcome'] == 'blocked'
                assert calls == ['intake', 'tracker', 'tracker', 'tracker', 'tracker']
                history = await successor.fetch_history()
                names = {e.activity_task_scheduled_event_attributes.activity_type.name
                         for e in history.events if e.HasField(
                             'activity_task_scheduled_event_attributes')}
                assert names == {'delivery_terminal_preflight', 'delivery_terminal_tracker',
                                 'delivery_project'}
