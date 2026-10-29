from __future__ import annotations

import asyncio
import json

import pytest
from temporal_test_server import local_temporal
from test_delivery_gate_retry import unpublished as unpublished
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture
def stopped(unpublished, monkeypatch):
    store, broker, state, _ = unpublished
    monkeypatch.setattr(store, '_ensure_no_remote_pr', lambda *_: None)
    command = {'continuation_kind': resume.KIND, 'command_id': 'resume-1',
               'expected_revision': state['revision'], 'expected_iteration': state['iteration'],
               'expected_candidate_id': broker.candidate()['id'],
               'expected_candidate_head': broker.candidate()['head'], 'additional_iterations': 2}
    return store, broker, state, command


def saved(store):
    with store._connect() as db:
        return json.loads(db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0])


def project(store, state):
    store.project('run-1', phase='blocked', execution_state='blocked', event_type='blocked',
                  message=state['error'], candidate=state['candidate'],
                  pull_request=state['pull_request'], checks=state['checks'],
                  iteration=state['iteration'], protocol_revision=state['revision'],
                  outcome='blocked', cleanup='confirmed', error=state['error'])


@pytest.mark.parametrize('drift', ['source', 'zero'])
@pytest.mark.asyncio
async def test_public_resume_rejects_unowned_or_unsealed_stop(stopped, drift):
    store, broker, state, command = stopped
    if drift == 'source':
        (broker.checkout / 'README.md').write_text('Unsealed post-stop edit\n')
    elif drift == 'scope':
        (broker.checkout / 'foreign.md').write_text('Escaped scope\n')
    elif drift == 'claim':
        with store._connect() as db:
            store.state.claim_work(db, broker.spec['work_id'], 'external:devflow:run-1',
                                   store.config.dashboard_url)
    elif drift == 'cleanup':
        state['roles'][0]['cleanup'] = 'unknown'
    elif drift == 'attempt':
        with store._connect() as db:
            db.execute("UPDATE delivery_attempts SET state='running'")
    elif drift == 'remote':
        _git(broker.checkout, 'push', 'origin', 'HEAD:refs/heads/' + broker.spec['branch'])
    elif drift == 'origin':
        _git(broker.source, 'remote', 'set-url', 'origin', 'https://example.invalid/foreign')
    elif drift == 'plan':
        monkey = pytest.MonkeyPatch()
        monkey.setattr(store, 'effective_spec', lambda _: {**broker.spec, 'accepted_plan': ''})
    elif drift == 'candidate':
        command['expected_candidate_id'] = 'a' * 64
    else:
        command['additional_iterations'] = 0 if drift == 'zero' else 20
    try:
        with pytest.raises(ValueError):
            store.continue_repair('run-1', command)
        with store._connect() as db:
            assert not db.execute(
                "SELECT 1 FROM delivery_commands WHERE command_id='resume-1'").fetchone()
            assert store.state.claim_for(db, broker.spec['work_id']) is None or drift == 'claim'
        import httpx

        from devflow_temporal.delivery_api import create_app

        app = create_app(store.config.path)
        app.state.delivery.store = store
        host = store.config.dashboard_url
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url=host) as client:
            token = (await client.get('/api/session')).json()['csrf_token']
            preview = await client.post('/api/runs/run-1/assess-transition', json={
                'kind': resume.KIND, 'request': command})
            actual = await client.post('/api/runs/run-1/continue-repair', json=command,
                                       headers={'Origin': host, 'X-Devflow-CSRF': token})
            assert preview.status_code == 200 and not preview.json()['eligible']
            code = 'stopped_candidate_changed' if drift == 'source' else 'pending_repair_unproven'
            assert code in {item['code'] for item in preview.json()['blockers']}
            assert actual.status_code == 409 and actual.json()['detail'] == preview.json()
        with store._connect() as db:
            assert not db.execute(
                "SELECT 1 FROM delivery_commands WHERE command_id='resume-1'").fetchone()
    finally:
        if drift == 'plan':
            monkey.undo()


@pytest.mark.asyncio
async def test_public_resume_runs_normal_gates_in_real_temporal_and_replays(
    stopped, tmp_path, monkeypatch,
):
    import shutil

    import httpx
    from temporalio import activity
    from temporalio.worker import Replayer, Worker

    from devflow_temporal.delivery_activities import delivery_project, delivery_repair_preflight
    from devflow_temporal.delivery_api import create_app

    store, _, original_state, command = stopped
    monkeypatch.setattr("devflow_temporal.delivery_store.DeliveryStore._ensure_no_remote_pr",
                        lambda *_: None)
    app = create_app(store.config.path)
    app.state.delivery.store = store
    calls = []

    @activity.defn(name='delivery_role')
    async def role(body):
        calls.append(body['role'])
        broker = DeliveryBroker(store, body['spec'])
        if body['role'] == 'implement':
            assert body['resume_session'] == 'original-implementation'
            (broker.checkout / 'README.md').write_text('Repaired after controller handoff\n')
        return {'status': 'pass', 'role': body['role'], 'iteration': body['iteration'],
                'session_id': ('original-implementation' if body['role'] == 'implement'
                               else 'independent-' + body['role']),
                'candidate': broker.candidate(), 'cleanup': 'confirmed', 'findings': []}

    @activity.defn(name='delivery_publish')
    async def publish(body):
        calls.append('publish')
        return {'number': 7, 'head': body['candidate']['head'],
                'candidate': body['candidate'], 'state': 'OPEN'}

    @activity.defn(name='delivery_precheck')
    async def precheck(body):
        calls.append('precheck')
        return {'state': 'passed', 'candidate_id': body['candidate']['id']}

    @activity.defn(name='delivery_checks')
    async def checks(body):
        calls.append('checks')
        return {'state': 'passed', 'candidate_id': body['candidate']['id']}

    @activity.defn(name='delivery_ci')
    async def ci(body):
        calls.append('ci')
        return {'state': 'passed', 'head': body['pull_request']['head']}

    @activity.defn(name='delivery_tracker_start')
    async def tracker_start(_):
        return {'state': 'consistent'}

    @activity.defn(name='delivery_tracker')
    async def tracker(_):
        calls.append('tracker')
        return {'state': 'consistent'}

    async with local_temporal(
        dev_server_existing_path=shutil.which('temporal'),
        dev_server_database_filename=str(tmp_path / 'resume-temporal.sqlite3'),
    ) as environment:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(
                app=app, client=('127.0.0.1', 10001)), base_url=store.config.dashboard_url) as api:
            session = (await api.get('/api/session')).json()
            headers = {'Origin': store.config.dashboard_url,
                       'X-Devflow-CSRF': session['csrf_token']}
            preflight = await api.post('/api/runs/run-1/repair-admission-preflight',
                                       json=command, headers=headers)
            assert preflight.status_code == 200, preflight.text
            admitted = await api.post('/api/runs/run-1/continue-repair', json=command,
                                      headers=headers)
            assert admitted.status_code == 200, admitted.text
        spec, recovery = store.effective_spec('run-1'), saved(store)
        async with Worker(environment.client, task_queue='resume-fixture',
                          workflows=[DeliveryWorkflow], activities=[
                              delivery_project, delivery_repair_preflight, role, publish,
                              precheck, checks, ci, tracker_start, tracker]):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, args=[spec, recovery], id=admitted.json()['workflow_id'],
                task_queue='resume-fixture')
            result = await asyncio.wait_for(handle.result(), 30)
            assert result['outcome'] == 'delivered'
            assert result['roles'][0] == original_state['roles'][0]
            assert calls == ['implement', 'precheck', 'publish', 'checks',
                             'review', 'verify', 'ci', 'tracker']
            assert saved(store)['state']['error'] == original_state['error']
            await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
                await handle.fetch_history())
