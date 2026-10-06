from __future__ import annotations

import asyncio
import copy
import json

import pytest
from test_delivery_gate_retry import unpublished as unpublished
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_resources import RunResources, _gate_evidence_root
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


def test_public_resume_is_finite_idempotent_and_retains_terminal_failure(stopped):
    store, broker, state, command = stopped
    before = copy.deepcopy(state)
    assert store.repair_admission_preflight('run-1', command)['authorized_through_iteration'] == 2
    admitted = store.continue_repair('run-1', command)
    assert store.continue_repair('run-1', command) == admitted
    spec = store.effective_spec('run-1')
    recovery = saved(store)
    assert recovery['state'] == before
    assert resume.readback(store, spec, recovery) == {'state': 'confirmed'}
    assert RunResources(spec).root == broker.state_dir / 'resources'
    assert _gate_evidence_root(spec) == (
        broker.state_dir / 'stopped-resumes' / recovery['command_digest'] / 'evidence')
    with store._connect() as db:
        assert not db.execute('SELECT * FROM delivery_repair_grants').fetchone()
    with pytest.raises(ValueError, match='different inputs'):
        store.continue_repair('run-1', {**command, 'additional_iterations': 1})


@pytest.mark.parametrize('foreign', [False, True])
def test_original_admission_authenticates_default_workflow_identity(stopped, monkeypatch, foreign):
    store, _, _, command = stopped
    original_read = store._completed_temporal_result
    closed = original_read('run-1')
    closed['workflow_id'] = 'delivery-foreign' if foreign else 'delivery-run-1'
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET workflow_id=NULL WHERE run_id='run-1'")
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *a, **k: closed)
    if foreign:
        with pytest.raises(ValueError, match='closed finalized delivery'):
            store.repair_admission_preflight('run-1', command)
    else:
        assert store.repair_admission_preflight('run-1', command)['preflight'] is True
        admitted = store.continue_repair('run-1', command)
        assert saved(store)['state'] == closed['result']
        assert admitted['workflow_id'] != closed['workflow_id']


@pytest.mark.parametrize('drift', ['source', 'scope', 'claim', 'cleanup', 'attempt',
                                    'remote', 'origin', 'plan', 'candidate', 'finite'])
def test_public_resume_rejects_unowned_or_unsealed_stop(stopped, drift):
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
        command['additional_iterations'] = 20
    try:
        with pytest.raises(ValueError):
            store.continue_repair('run-1', command)
        with store._connect() as db:
            assert not db.execute(
                "SELECT 1 FROM delivery_commands WHERE command_id='resume-1'").fetchone()
            assert store.state.claim_for(db, broker.spec['work_id']) is None or drift == 'claim'
    finally:
        if drift == 'plan':
            monkey.undo()


@pytest.mark.parametrize('drift', ['source', 'attempt'])
def test_runtime_preparation_drift_cannot_consume_resume(stopped, monkeypatch, drift):
    store, broker, _, command = stopped
    def prepare(spec, *_):
        if drift == 'source':
            (broker.checkout / 'README.md').write_text('Changed during native preparation\n')
        else:
            with store._connect() as db:
                db.execute("UPDATE delivery_attempts SET finished_at='later'")
        return spec
    monkeypatch.setattr(resume, 'prepare_runtime', prepare)
    with pytest.raises(ValueError, match='changed'):
        store.continue_repair('run-1', command)
    with store._connect() as db:
        assert not db.execute(
                "SELECT 1 FROM delivery_commands WHERE command_id='resume-1'").fetchone()
        assert store.state.claim_for(db, broker.spec['work_id']) is None


def blocked_source(stopped, *, session='original-implementation', prelaunch=False):
    store, broker, state, command = stopped
    old_candidate = state['candidate']
    (broker.checkout / 'README.md').write_text('Retained partial implementation probe\n')
    actual = broker.candidate()
    receipt = {'status': 'blocked', 'session_id': session, 'cleanup': 'confirmed',
               'findings': ['Controller handoff missing'],
               **({'finish_reason': 'prelaunch'} if prelaunch else {})}
    state['roles'] = [{**receipt, 'role': 'implement', 'iteration': 0, 'candidate': actual}]
    with store._connect() as db:
        db.execute('UPDATE delivery_attempts SET result_json=?,session_id=?',
                   (canonical_json(receipt), session))
    assert state['candidate'] == old_candidate
    command['expected_candidate_id'] = actual['id']
    return actual


def test_partial_failed_implementation_is_admitted_only_with_exact_original_receipt(stopped):
    store, _, _, command = stopped
    actual = blocked_source(stopped)
    store.continue_repair('run-1', command)
    recovery = saved(store)
    assert recovery['execution_candidate'] == actual
    assert recovery['state']['candidate'] != actual
    assert recovery['session_id'] == 'original-implementation'


@pytest.mark.parametrize('kind', ['prelaunch', 'preparation'])
def test_authenticated_no_provider_stop_allows_first_session(stopped, kind):
    store, _, state, command = stopped
    blocked_source(stopped, session=None, prelaunch=kind == 'prelaunch')
    if kind == 'preparation':
        state['roles'][0]['implementation_preparation'] = {
            'state': 'failed', 'cleanup': 'confirmed', 'results': [{'passed': False}]}
        with store._connect() as db:
            db.execute('DELETE FROM delivery_attempts')
    store.continue_repair('run-1', command)
    assert saved(store)['session_id'] is None


def test_resume_rejects_failed_baseline_before_feature_work(stopped, monkeypatch):
    store, broker, _, command = stopped
    spec = {**broker.spec, 'baseline_checks_version': 1,
            'policy': {**broker.spec['policy'], 'baseline_checks': [{'id': 'docs-runtime'}]}}
    monkeypatch.setattr(store, 'effective_spec', lambda _: spec)
    with pytest.raises(ValueError, match='passed immutable baseline'):
        store.continue_repair('run-1', command)


def test_successive_explicit_resumes_append_authority_preserving_original_history(stopped,
                                                                                monkeypatch):
    store, _, state, command = stopped
    first = store.continue_repair('run-1', command)
    original = saved(store)
    second_state = {**copy.deepcopy(state), 'iteration': 2, 'revision': 15,
                    'error': 'Later independent gate failed'}
    project(store, second_state)
    spec = store.effective_spec('run-1')
    with store._connect() as db:
        store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
    closed = {'workflow_id': first['workflow_id'], 'execution_run_id': 'second-execution',
              'request_digest': spec['request_digest'], 'recovery_digest': digest(original),
              'result': second_state}
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *_args, **_kw: closed)
    second_command = {**command, 'command_id': 'resume-2', 'expected_revision': 15,
                      'expected_iteration': 2}
    second = store.continue_repair('run-1', second_command)
    assert second['authorized_through_iteration'] == 4
    assert second['workflow_id'] != first['workflow_id']
    assert saved(store)['original_recovery'] == original
    assert store.continue_repair('run-1', command) == first
    assert resume.readback(
        store, store.effective_spec('run-1'), saved(store)) == {'state': 'confirmed'}
    with store._connect() as db:
        assert db.execute('SELECT count(*) FROM delivery_commands').fetchone()[0] == 3


def test_resume_waits_for_tracker_and_preserves_session_and_gate_sequence(stopped, monkeypatch):
    store, _, _, command = stopped
    store.continue_repair('run-1', command)
    spec, recovery = store.effective_spec('run-1'), saved(store)
    flow = DeliveryWorkflow()
    tracker_calls = []
    gates = []
    async def activity(name, body, **_):
        assert name == 'delivery_tracker_start'
        tracker_calls.append(body)
        return {'state': 'pending' if len(tracker_calls) == 1 else 'consistent'}
    async def confirmed(*_):
        return True
    async def nothing(*_):
        pass
    async def iterations(actual_spec, **kwargs):
        gates.append(kwargs)
        assert actual_spec == spec
        assert flow.state['roles'] == recovery['state']['roles']
        return {'outcome': 'exercise-normal-iterations'}
    monkeypatch.setattr(flow, '_activity', activity)
    monkeypatch.setattr(flow, '_confirm_repair_preflight', confirmed)
    monkeypatch.setattr(flow, '_project', nothing)
    monkeypatch.setattr(flow, '_wait_repair_readback', nothing)
    monkeypatch.setattr(flow, '_run_iterations', iterations)
    result = asyncio.run(flow._resume_stopped(spec, recovery))
    assert result['outcome'] == 'exercise-normal-iterations'
    assert len(tracker_calls) == 2
    assert gates[0]['prior_implementer_session'] == 'original-implementation'
    assert gates[0]['authorized_max_iteration'] == 2
    assert gates[0]['start_iteration'] == 1
    assert gates[0]['recovery'] is None


def test_several_explicit_resumes_retain_history_without_recursive_json_duplication(stopped,
                                                                                 monkeypatch):
    store, _, state, command = stopped
    sizes = []
    for index in range(6):
        admitted = store.continue_repair('run-1', {
            **command, 'command_id': 'resume-' + str(index),
            'expected_iteration': state['iteration'], 'expected_revision': state['revision']})
        recovery = saved(store)
        assert 'row' not in recovery and 'result' not in recovery['closed']
        sizes.append(len(canonical_json(recovery)))
        if index == 5:
            break
        state = {**copy.deepcopy(state), 'iteration': state['iteration'] + 2,
                 'revision': state['revision'] + 6}
        project(store, state)
        spec = store.effective_spec('run-1')
        with store._connect() as db:
            store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
        closed = {'workflow_id': admitted['workflow_id'],
                  'execution_run_id': 'execution-' + str(index),
                  'request_digest': spec['request_digest'], 'recovery_digest': digest(recovery),
                  'result': state}
        monkeypatch.setattr(store, '_completed_temporal_result',
                           lambda *_a, _closed=closed, **_k: _closed)
    assert sizes[-1] < sizes[0] * 7


@pytest.mark.asyncio
async def test_public_resume_runs_normal_gates_in_real_temporal_and_replays(
    stopped, tmp_path, monkeypatch,
):
    import shutil

    import httpx
    from temporalio import activity
    from temporalio.testing import WorkflowEnvironment
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

    async with await WorkflowEnvironment.start_local(
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


@pytest.mark.parametrize('old_grant', [False, True])
def test_native_resume_guard_uses_only_the_current_explicit_ceiling(stopped, old_grant):
    from devflow_temporal.delivery_native_guard import validate_native_turn

    store, _, _, command = stopped
    command['additional_iterations'] = 1
    store.continue_repair('run-1', command)
    spec = store.effective_spec('run-1')
    if old_grant:
        with store._connect() as db:
            db.execute('INSERT INTO delivery_repair_grants '
                       '(run_id,command_id,predecessor_workflow_id,predecessor_execution_run_id,'
                       'predecessor_result_digest,granted_iterations,maximum_iteration,granted_at) '
                       'VALUES (?,?,?,?,?,?,?,?)',
                       ('run-1', 'older-grant', 'older-workflow', 'older-execution',
                        'a' * 64, 2, 8, 'historical'))
    validate_native_turn(spec, 'implement', 1, store)
    validate_native_turn(spec, 'review', 1, store)
    with pytest.raises(ValueError, match='finite turn limit'):
        validate_native_turn(spec, 'implement', 2, store)
