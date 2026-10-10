from __future__ import annotations

import asyncio
import json

import pytest
from test_delivery_metadata_recovery import published as published
from test_delivery_store import service as service
from test_delivery_store import submit_historical_admission

from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_gate_retry import KIND, PREPUBLICATION_KIND, readback
from devflow_temporal.delivery_resources import RunResources
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture
def unpublished(service, monkeypatch):
    store, request = service
    submit_historical_admission(store, request, monkeypatch)
    spec = store.spec('run-1')
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    (broker.checkout / 'README.md').write_text('Owned unpublished candidate\n')
    candidate = broker.candidate()
    implementation = {'role': 'implement', 'iteration': 0, 'status': 'pass',
                      'session_id': 'original-implementation', 'cleanup': 'confirmed',
                      'candidate': candidate}
    checks = {'prepublish': {'state': 'failed', 'source_unchanged': True,
                            'candidate_id': candidate['id'],
                            'results': [{'id': 'packaging', 'passed': False,
                                         'cleanup': 'confirmed', 'exit_code': 1}]}}
    state = {'run_id': 'run-1', 'revision': 9, 'iteration': 0, 'phase': 'blocked',
             'execution_state': 'blocked', 'outcome': 'blocked', 'cleanup': 'confirmed',
             'error': 'prepublication repair limit exhausted', 'candidate': candidate,
             'pull_request': None, 'candidate_revision': 2, 'roles': [implementation],
             'checks': checks, 'usage': {}, 'findings': ['Historical packaging failure']}
    store.project('run-1', phase='blocked', execution_state='blocked', event_type='blocked',
                  message=state['error'], candidate=candidate, pull_request=None, checks=checks,
                  iteration=0, protocol_revision=9, outcome='blocked', cleanup='confirmed',
                  error=state['error'])
    with store._connect() as db:
        db.execute("INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,"
                   "state,session_id,result_json,cleanup) VALUES "
                   "('original','run-1','implement',0,?,'finished',?,?, 'confirmed')",
                   (candidate['id'], implementation['session_id'], canonical_json(implementation)))
        db.execute("UPDATE delivery_runs SET workflow_id='delivery-run-1' WHERE run_id='run-1'")
        store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
    closed = {'workflow_id': 'delivery-run-1', 'execution_run_id': 'closed-original',
              'request_digest': spec['request_digest'], 'recovery_digest': None, 'result': state}
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *_args, **_kw: closed)
    monkeypatch.setattr(DeliveryBroker, '_existing_pr', lambda *_args, **_kw: None)
    monkeypatch.setattr('devflow_temporal.delivery_gate_retry._stopped_cleanup', lambda _: {})
    request = {'continuation_kind': PREPUBLICATION_KIND, 'command_id': 'retry-prechecks-1',
               'expected_revision': 9, 'expected_iteration': 0,
               'expected_candidate_id': candidate['id'],
               'expected_candidate_head': candidate['head'], 'additional_iterations': 0}
    return store, broker, state, request


def preparation_stop(store, state, *, installer=False):
    failure = state['checks']['prepublish']['results'][0]
    failure.update(id='locked-environment', exit_code=None,
                   failure_kind='preparation', launched=False)
    if installer:
        failure.update(argv=['uv', 'sync', '--locked'], exit_code=1, launched=True)
        failure.pop('failure_kind')
    state['error'] = ('environment preparation failed: locked-environment; '
                      'candidate retained without requesting code repair')
    store.project(state['run_id'], phase='blocked', execution_state='blocked',
                  event_type='blocked', message=state['error'], error=state['error'],
                  checks=state['checks'], outcome='blocked')


@pytest.mark.parametrize('installer', [False, True])
def test_preparation_retry_preserves_zero_repair_authority(unpublished, installer):
    store, broker, state, request = unpublished
    preparation_stop(store, state, installer=installer)
    original = store.spec('run-1')
    candidate = broker.candidate()
    assert store.repair_admission_preflight('run-1', request)['additional_iterations'] == 0
    result = store.continue_repair('run-1', request)
    assert result['implementation_authority'] is False and result['additional_iterations'] == 0
    assert broker.candidate() == candidate
    assert store.effective_spec('run-1')['policy'] == original['policy']
    with store._connect() as db:
        recovery = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_gate_admissions WHERE run_id='run-1'"
        ).fetchone()[0])
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
    assert recovery['state'] == state
    assert recovery['state']['iteration'] == 0


@pytest.mark.parametrize('drift', ['error', 'untyped', 'launched', 'cleanup', 'candidate',
                                 'source_unchanged', 'implementation', 'projection'])
def test_preparation_retry_rejects_unsealed_failure(unpublished, drift):
    store, _, state, request = unpublished
    preparation_stop(store, state)
    precheck = state['checks']['prepublish']
    failure = precheck['results'][0]
    if drift == 'error':
        state['error'] = state['error'].replace('locked-environment', 'unrelated-check')
    elif drift == 'untyped':
        failure.pop('failure_kind')
    elif drift == 'launched':
        failure['launched'] = True
    elif drift == 'cleanup':
        failure['cleanup'] = 'unknown'
    elif drift == 'candidate':
        precheck['candidate_id'] = 'f' * 64
    elif drift == 'source_unchanged':
        precheck['source_unchanged'] = False
    elif drift == 'implementation':
        state['roles'][0]['status'] = 'findings'
    else:
        failure['id'] = 'different-result'
    if drift != 'projection':
        store.project('run-1', phase='blocked', execution_state='blocked', event_type='blocked',
                      message=state['error'], error=state['error'], checks=state['checks'],
                      outcome='blocked')
    with pytest.raises(ValueError, match='closed, finalized failed gate'):
        store.continue_repair('run-1', request)
    with store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_gate_admissions').fetchone()
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()


def test_unpublished_retry_retains_candidate_failure_budget_and_one_admission(unpublished):
    store, broker, state, request = unpublished
    before = broker.candidate()
    assert store.repair_admission_preflight('run-1', request)['additional_iterations'] == 0
    admitted = store.continue_repair('run-1', request)
    assert store.continue_repair('run-1', request) == admitted
    spec = store.effective_spec('run-1')
    assert spec['policy'] == store.spec('run-1')['policy']
    assert broker.candidate() == before
    with store._connect() as db:
        recovery = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_gate_admissions WHERE run_id='run-1'"
        ).fetchone()[0])
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
        assert store.state.claim_for(db, spec['work_id'])['owner'] == 'external:devflow:run-1'
    assert recovery['state']['checks'] == state['checks']
    assert readback(store, spec, recovery) is None
    assert RunResources(spec).root.name == 'resources'
    with pytest.raises(ValueError):
        store.continue_repair('run-1', {**request, 'command_id': 'second-retry'})


@pytest.mark.parametrize('drift', ['source', 'assessment', 'cleanup', 'gate', 'scope', 'budget'])
def test_unpublished_retry_rejects_changed_or_unsealed_authority(unpublished, drift):
    store, broker, state, request = unpublished
    if drift == 'source':
        (broker.checkout / 'README.md').write_text('Changed after failure\n')
    elif drift == 'scope':
        (broker.checkout / 'outside.txt').write_text('Outside allowed paths\n')
    elif drift == 'assessment':
        state['roles'][0]['status'] = 'findings'
    elif drift == 'cleanup':
        state['cleanup'] = 'unknown'
    elif drift == 'gate':
        state['checks']['prepublish']['source_unchanged'] = False
    else:
        request['additional_iterations'] = 1
    with pytest.raises(ValueError):
        store.continue_repair('run-1', request)

    with store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_gate_admissions').fetchone()


def test_report_assessment_has_a_distinct_role_attempt_even_at_same_generation(missing_report):
    from devflow_temporal.supervisor import DeliverySupervisor

    store, broker, _, _, request, _, _ = missing_report
    original = store.effective_spec('run-1')
    supervisor = DeliverySupervisor(store, capacity=1)
    body = {'spec': original, 'role': 'verify', 'iteration': 2,
            'candidate': broker.candidate()}
    old_key, _ = supervisor._claim(body)
    with store._connect() as db:
        db.execute('DELETE FROM delivery_attempts WHERE job_key=?', (old_key,))
    store.continue_repair('run-1', request)
    current = store.effective_spec('run-1')
    new_key, cached = supervisor._claim({**body, 'spec': current})
    assert old_key != new_key and cached is None


@pytest.mark.parametrize('gate_passes', [True, False])
@pytest.mark.parametrize('preparation_failed', [True, False])
def test_unpublished_retry_runs_checks_before_publication_and_independent_roles(
    unpublished, monkeypatch, gate_passes, preparation_failed,
):
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", lambda _: True)
    store, _, state, request = unpublished
    if preparation_failed:
        preparation_stop(store, state)
    store.continue_repair('run-1', request)
    spec = store.effective_spec('run-1')
    with store._connect() as db:
        recovery = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_gate_admissions WHERE run_id='run-1'"
        ).fetchone()[0])
    flow = DeliveryWorkflow()
    calls = []

    async def execute(name, body, **kw):
        calls.append((name, body.get('role')))
        if name == 'delivery_tracker_start' or name == 'delivery_tracker':
            return {'state': 'consistent'}
        if name == 'delivery_precheck':
            return {'state': 'passed' if gate_passes else 'failed', 'cleanup': 'confirmed'}
        if name == 'delivery_publish':
            return {'number': 7, 'head': recovery['candidate']['head'],
                    'candidate': recovery['candidate']}
        if name == 'delivery_role':
            assert body['role'] in {'review', 'verify'}
            return {'role': body['role'], 'iteration': 0, 'status': 'pass',
                    'session_id': 'independent-' + body['role'], 'candidate': recovery['candidate']}
        return {'state': 'passed', 'cleanup': 'confirmed'}

    async def project(*args):
        pass

    async def stop(_spec, reason):
        flow.state.update(outcome='blocked', error=reason)
        return flow.state

    monkeypatch.setattr(flow, '_activity', execute)
    monkeypatch.setattr(flow, '_project', project)
    monkeypatch.setattr(flow, '_stop', stop)
    result = asyncio.run(flow._resume_published_gates(spec, recovery))
    names = [name for name, _ in calls]
    assert result['iteration'] == 0
    assert result['roles'][0] == state['roles'][0]
    assert ('delivery_role', 'implement') not in calls
    if gate_passes:
        assert result['outcome'] == 'delivered'
        assert names.index('delivery_precheck') < names.index('delivery_publish')
        assert [role for name, role in calls if name == 'delivery_role'] == ['review', 'verify']
        assert names.index('delivery_publish') < names.index('delivery_ci')
    else:
        assert result['outcome'] == 'blocked'
        assert 'delivery_publish' not in names
        assert 'delivery_role' not in names
        assert 'delivery_ci' not in names


@pytest.fixture
def stopped(published, monkeypatch):
    store, broker, state, closed, _, _ = published
    spec = store.spec('run-1')
    state['cleanup'] = 'confirmed'
    state['roles'].append({'role': 'verify', 'iteration': 1, 'status': 'findings',
                           'findings': ['Receipt omits source hash semantics'],
                           'session_id': 'old-qa', 'cleanup': 'confirmed'})
    with store._connect() as db:
        db.execute("INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,"
                   "state,session_id,result_json,cleanup) VALUES "
                   "('old-qa','run-1','verify',1,?,'finished','old-qa',?,'confirmed')",
                   (state['candidate']['id'], canonical_json(state['roles'][-1])))
        db.execute("UPDATE delivery_runs SET cleanup='confirmed',workflow_id='delivery-run-1' "
                   "WHERE run_id='run-1'")
        store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
    monkeypatch.setattr('devflow_temporal.delivery_gate_retry._stopped_cleanup', lambda _: {})
    request = {'continuation_kind': KIND, 'command_id': 'retry-gates-1',
               'expected_revision': 13, 'expected_iteration': 1,
               'expected_candidate_id': state['candidate']['id'],
               'expected_pr_number': 7, 'expected_pr_head': state['candidate']['head'],
               'additional_iterations': 0}
    return store, broker, state, closed, request


def test_same_run_admission_replay_and_unchanged_source(stopped):
    store, broker, state, _, request = stopped
    before = broker.candidate()
    assert store.repair_admission_preflight('run-1', request)['implementation_authority'] is False
    result = store.continue_repair('run-1', request)
    assert result['additional_iterations'] == 0
    assert store.continue_repair('run-1', request) == result
    assert broker.candidate() == before
    spec = store.effective_spec('run-1')
    assert spec['policy'] == store.spec('run-1')['policy']
    assert spec['policy']['max_repairs'] == store.spec('run-1')['policy']['max_repairs']
    with store._connect() as db:
        recovery = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_gate_admissions WHERE run_id='run-1'"
        ).fetchone()[0])
        assert db.execute('SELECT count(*) FROM delivery_repair_grants').fetchone()[0] == 0
    assert readback(store, spec, recovery)['number'] == 7
    assert recovery['state']['roles'][-1]['status'] == 'findings'
    assert RunResources(spec).root.name == 'resources'
    with pytest.raises(ValueError):
        store.continue_repair('run-1', {**request, 'command_id': 'retry-gates-2'})
    with pytest.raises(ValueError, match='different inputs'):
        store.continue_repair('run-1', {**request, 'expected_pr_head': 'a' * 40})


def test_selector_preflight_reads_original_custody_before_sealing_execution(stopped, monkeypatch):
    store, _, _, _, request = stopped
    original = store.effective_spec('run-1')
    initialize = DeliveryBroker.__init__

    def guarded(self, store, spec):
        assert spec == original  # New selectors have not received admission custody yet.
        initialize(self, store, spec)

    def recipes(spec, checkout, evidence):
        assert spec['verification_test_paths'] == ['worker/tests/test_owned.py']
        return [{'id': 'focused-tests', 'plan_provenance': {'test_paths':
                                                        spec['verification_test_paths']}}]

    monkeypatch.setattr(DeliveryBroker, '__init__', guarded)
    monkeypatch.setattr('devflow_temporal.delivery_plan_checks.planned_checks', recipes)
    selected = {**request, 'verification_test_paths': ['worker/tests/test_owned.py']}
    assert store.repair_admission_preflight('run-1', selected)['additional_iterations'] == 0
    store.continue_repair('run-1', selected)
    assert store.effective_spec('run-1')['verification_test_paths'] == selected[
        'verification_test_paths']


@pytest.mark.parametrize('field,value', [('additional_iterations', 1),
                                        ('expected_revision', 12),
                                        ('expected_pr_head', 'a' * 40),
                                        ('expected_candidate_id', 'a' * 64)])
def test_changed_authority_is_rejected_before_admission(stopped, field, value):
    store, _, _, _, request = stopped
    with pytest.raises(ValueError):
        store.continue_repair('run-1', {**request, field: value})
    with store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_gate_admissions').fetchone()


@pytest.mark.parametrize('qa_status', ['pass', 'findings'])
def test_workflow_runs_fresh_gates_without_implementation_or_publication(
    stopped, monkeypatch, qa_status,
):
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", lambda _: True)
    store, _, state, _, request = stopped
    store.continue_repair('run-1', request)
    spec = store.effective_spec('run-1')
    spec['policy']['host_sandbox'] = 'trusted-local'
    with store._connect() as db:
        recovery = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_gate_admissions WHERE run_id='run-1'"
        ).fetchone()[0])
    # Workflow-only fixture: both halves bind the selected receipt-capable profile.
    recovery['execution_spec'] = spec
    flow = DeliveryWorkflow()
    calls = []

    async def execute(name, body, **kw):
        calls.append((name, body.get('role')))
        if name == 'delivery_tracker_start':
            return {'state': 'consistent'}
        if name == 'delivery_role':
            assert body['role'] in {'review', 'verify'}
            assert ('delivery_checks', None) in calls
            assert body['check_evidence']['candidate_id'] == recovery['candidate']['id']
            return {'role': body['role'], 'iteration': 1,
                    'status': qa_status if body['role'] == 'verify' else 'pass',
                    'findings': ['Fresh QA failure'] if qa_status != 'pass' else [],
                    'session_id': 'fresh-' + body['role'], 'candidate': recovery['candidate']}
        if name == 'delivery_tracker':
            return {'state': 'consistent'}
        return {'state': 'passed', 'cleanup': 'confirmed',
                'candidate_id': recovery['candidate']['id']}

    async def project(*args):
        pass

    async def stop(_spec, reason):
        flow.state.update(outcome='blocked', error=reason)
        return flow.state

    monkeypatch.setattr(flow, '_stop', stop)
    monkeypatch.setattr(flow, '_activity', execute)
    monkeypatch.setattr(flow, '_project', project)
    result = asyncio.run(flow._resume_published_gates(spec, recovery))
    assert result['outcome'] == ('delivered' if qa_status == 'pass' else 'blocked')
    assert [role for name, role in calls if name == 'delivery_role'] == ['review', 'verify']
    assert not any('publish' in name for name, _ in calls)
    assert result['iteration'] == state['iteration']
    assert result['roles'][-3]['status'] == 'findings'
    assert result['checks']['qa']['state'] == ('passed' if qa_status == 'pass' else 'failed')
    if qa_status != 'pass':
        assert not any(name == 'delivery_ci' for name, _ in calls)
        assert result['error'] == 'repair limit exhausted'


def test_fresh_gate_attempt_cannot_reuse_the_failed_qa_job(stopped):
    from devflow_temporal.supervisor import DeliverySupervisor

    store, broker, _, _, request = stopped
    original = store.spec('run-1')
    supervisor = DeliverySupervisor(store, capacity=1)
    body = {'spec': original, 'role': 'verify', 'iteration': 1,
            'candidate': broker.candidate()}
    old_key, _ = supervisor._claim(body)
    # This seam exercises durable attempt identity only; no provider launches.
    with store._connect() as db:
        db.execute("DELETE FROM delivery_attempts WHERE job_key=?", (old_key,))
    store.continue_repair('run-1', request)
    current = store.effective_spec('run-1')
    new_key, cached = supervisor._claim({**body, 'spec': current})
    assert new_key != old_key
    assert cached is None


def test_finalized_run_can_receive_one_real_code_repair_and_keep_its_grant(stopped):
    store, broker, state, _, old_request = stopped
    with store._connect() as db:
        row = db.execute("SELECT request_json FROM delivery_runs WHERE run_id='run-1'").fetchone()
        original = json.loads(row[0])
        original['resource_cleanup_version'] = 1  # Explicit modern fake fixture; no native claim.
        db.execute("UPDATE delivery_runs SET request_json=? WHERE run_id='run-1'",
                   (canonical_json(original),))
    spec = store.spec('run-1')
    resources = RunResources(spec)
    resources.scratch('original', 'checks')
    receipt = resources.finalize('blocked')
    state['checks'] = {'resource_cleanup': receipt}
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET checks_json=? WHERE run_id='run-1'",
                   (canonical_json(state['checks']),))
    request = {k: v for k, v in old_request.items() if k != 'continuation_kind'}
    request.update(command_id='real-code-repair-1', additional_iterations=1)
    before = broker.candidate()
    preflight = store.repair_admission_preflight('run-1', request)
    assert preflight['diagnostics'] == ['Receipt omits source hash semantics']
    result = store.continue_repair('run-1', request)
    assert result['authorized_through_iteration'] == 2
    assert store.continue_repair('run-1', request) == result
    current = store.effective_spec('run-1')
    assert current == spec
    with store._connect() as db:
        recovery = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'"
        ).fetchone()[0])
    assert recovery['state']['cleanup'] == 'confirmed'
    assert recovery['finalized_checkpoint']
    assert broker.candidate() == before
    store.repair_preflight(current, recovery)
    with pytest.raises(ValueError, match='one repair grant'):
        store.continue_repair('run-1', {**request, 'command_id': 'another-repair'})


def first_published_after_repair(
    stopped, monkeypatch,
):
    from copy import deepcopy

    from devflow_temporal.contracts import digest

    store, broker, state, closed, request = stopped
    with store._connect() as db:
        original = json.loads(db.execute(
            "SELECT request_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0])
        original['resource_cleanup_version'] = 1
        db.execute("UPDATE delivery_runs SET request_json=? WHERE run_id='run-1'",
                   (canonical_json(original),))
    resources = RunResources(store.spec('run-1'))
    resources.scratch('original', 'checks')
    state['checks'] = {'resource_cleanup': resources.finalize('blocked')}
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET checks_json=? WHERE run_id='run-1'",
                   (canonical_json(state['checks']),))
    grant = {k: v for k, v in request.items() if k != 'continuation_kind'}
    grant.update(command_id='one-source-grant', additional_iterations=1)
    store.continue_repair('run-1', grant)
    with store._connect() as db:
        prior = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0])
    state.update(iteration=2, revision=14)
    state['roles'].extend([{**deepcopy(state['roles'][1]), 'iteration': 2},
                           {**deepcopy(state['roles'][-1]), 'iteration': 2}])
    closed.update(workflow_id='delivery-run-1-repair-continuation-1',
                  recovery_digest=digest(prior))
    work_id = store.spec('run-1')['work_id']
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET phase='blocked',execution_state='blocked',"
                   "outcome='blocked',iteration=2,protocol_revision=14,error=?,cleanup='confirmed'"
                   " WHERE run_id='run-1'", (state['error'],))
        store.state.release_work(db, work_id, 'external:devflow:run-1')
    request.update(command_id='post-repair-assessment', expected_iteration=2, expected_revision=14)
    before_grant = canonical_json(prior)
    store.continue_repair('run-1', request)
    effective = store.effective_spec('run-1')
    assert effective['gate_retry_stage'] == 'published'
    assert effective['policy']['max_repairs'] == store.spec('run-1')['policy']['max_repairs']
    assert RunResources(effective).root.name == 'resources'
    with store._connect() as db:
        current = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0])
        assert canonical_json(current['original_recovery']) == before_grant
        grant = db.execute('SELECT granted_iterations FROM delivery_repair_grants').fetchone()
        assert grant[0] == 1
    return store, broker, state, closed, request, current


def test_first_published_assessment_after_finalized_repair_keeps_historical_grant(
    stopped, monkeypatch,
):
    first_published_after_repair(stopped, monkeypatch)


@pytest.fixture
def missing_report(stopped, monkeypatch):
    from copy import deepcopy

    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_check_evidence import retain_artifacts

    store, broker, state, closed, request, previous = first_published_after_repair(
        stopped, monkeypatch)
    spec = store.effective_spec('run-1')
    reference = retain_artifacts(broker.state_dir / 'missing-report', state['candidate'])
    local = {'state': 'passed', 'candidate_id': state['candidate']['id'],
             'results': [{'id': 'diff', 'passed': True, 'artifacts': reference}]}
    # Frozen fixture's configured check becomes a test before admission in reality;
    # only the recipe-observation seam uses this synthetic classification here.
    observation = deepcopy(spec)
    observation['policy']['checks'][0]['kind'] = 'test'
    from devflow_temporal.delivery_gate_retry import missing_planned_report as observe
    monkeypatch.setattr('devflow_temporal.delivery_gate_retry.missing_planned_report',
                        lambda _spec, current, owned: observe(observation, current, owned))
    monkeypatch.setattr('devflow_temporal.delivery_plan_checks.planned_junit_recipes',
                        lambda *_: [{'plan_provenance': {'recipe': 'checks.scripts',
                                                        'metadata': {'tracked': 'hash'}}}])
    state.update(revision=23, checks={'local': local})
    state['roles'][-1]['candidate'] = state['candidate']
    store.project('run-1', phase='blocked', execution_state='blocked', event_type='blocked',
                  message=state['error'], candidate=state['candidate'], checks=state['checks'],
                  iteration=2, protocol_revision=23, outcome='blocked',
                  cleanup='confirmed', error=state['error'])
    with store._connect() as db:
        store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
        workflow = db.execute("SELECT workflow_id FROM delivery_runs WHERE run_id='run-1'")
        closed.update(workflow_id=workflow.fetchone()[0], recovery_digest=digest(previous))
    return store, broker, state, closed, {**request, 'command_id': 'missing-report-reassessment',
                                        'expected_revision': 23}, previous, reference


def test_missing_accepted_report_has_one_zero_source_reassessment_and_preserves_grant(
    missing_report, monkeypatch,
):
    from devflow_temporal.contracts import digest

    store, broker, state, closed, request, previous, _ = missing_report
    original_bytes = (broker.state_dir / 'published-gates-admission/admission.json').read_bytes()
    result = store.continue_repair('run-1', request)
    assert result['workflow_id'].endswith('report-gates-retry-1')
    assert result['implementation_authority'] is False
    spec = store.effective_spec('run-1')
    with store._connect() as db:
        current = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_gate_admissions WHERE run_id='run-1'"
        ).fetchone()[0])
        granted = db.execute('SELECT granted_iterations FROM delivery_repair_grants').fetchone()
        assert granted[0] == 1
    assert current['original_recovery'] == previous
    assert current['seal']['report_observation']['omitted_recipes']
    assert readback(store, spec, current)['head'] == state['candidate']['head']
    assert RunResources(spec).root.name == 'resources'
    assert ((broker.state_dir / 'published-gates-admission/admission.json').read_bytes()
            == original_bytes)
    state['revision'] = 33
    store.project('run-1', phase='blocked', execution_state='blocked', event_type='blocked',
                  message=state['error'], candidate=state['candidate'], checks=state['checks'],
                  iteration=2, protocol_revision=33, outcome='blocked',
                  cleanup='confirmed', error=state['error'])
    with store._connect() as db:
        store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
        workflow = db.execute("SELECT workflow_id FROM delivery_runs WHERE run_id='run-1'")
        closed.update(workflow_id=workflow.fetchone()[0], recovery_digest=digest(current))
    with pytest.raises(ValueError, match='closed, finalized'):
        store.continue_repair('run-1', {**request, 'command_id': 'third-report-attempt',
                                      'expected_revision': 33})


@pytest.mark.parametrize('drift', ['candidate', 'local-failed', 'manifest', 'recipe-present',
                                 'no-delegation', 'source-turn'])
def test_missing_report_reassessment_rejects_changed_or_absent_proof(missing_report, drift,
                                                                  monkeypatch):
    from pathlib import Path

    store, _, state, _, request, _, reference = missing_report
    if drift == 'candidate':
        state['candidate']['id'] = 'f' * 64
    elif drift == 'local-failed':
        state['checks']['local']['state'] = 'failed'
    elif drift == 'manifest':
        Path(reference['path']).write_text('{}')
    elif drift == 'recipe-present':
        state['checks']['local']['results'][0]['plan_provenance'] = {'recipe': 'checks.scripts'}
    elif drift == 'no-delegation':
        monkeypatch.setattr('devflow_temporal.delivery_plan_checks.planned_junit_recipes',
                            lambda *_: [])
    else:
        request['additional_iterations'] = 1
    if drift in {'local-failed', 'recipe-present'}:
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET checks_json=? WHERE run_id='run-1'",
                       (canonical_json(state['checks']),))
    with pytest.raises(ValueError):
        store.continue_repair('run-1', request)


def test_second_prepublication_retry_preserves_history_and_has_no_third_grant(unpublished,
                                                                            monkeypatch):
    from copy import deepcopy

    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_gate_retry import snapshot

    store, broker, original_state, request = unpublished
    store.continue_repair('run-1', request)
    first_spec = store.effective_spec('run-1')
    with store._connect() as db:
        previous = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_gate_admissions WHERE run_id='run-1'"
        ).fetchone()[0])
    old_file = broker.state_dir / 'gates-admission/admission.json'
    old_bytes = old_file.read_bytes()

    def finalize(spec, recovery, revision):
        state = deepcopy(original_state)
        state['revision'] = revision
        store.project('run-1', phase='blocked', execution_state='blocked', event_type='blocked',
                      message=state['error'], candidate=state['candidate'], checks=state['checks'],
                      iteration=0, protocol_revision=revision, outcome='blocked',
                      cleanup='confirmed', error=state['error'])
        with store._connect() as db:
            store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
            workflow = db.execute("SELECT workflow_id FROM delivery_runs WHERE run_id='run-1'")
            closed = {'workflow_id': workflow.fetchone()[0],
                      'request_digest': spec['request_digest'], 'recovery_digest': digest(recovery),
                      'result': state}
        monkeypatch.setattr(store, '_completed_temporal_result', lambda *_args, **_kw: closed)
        return {**request, 'command_id': f'retry-prechecks-{revision}',
                'expected_revision': revision}

    second_request = finalize(first_spec, previous, 19)
    native_spec = deepcopy(first_spec)
    native_spec.update(provider='codex')
    native_spec['policy'].update(host_sandbox='trusted-local',
                                 native_identity={'runtime_payload_sha256': 'same-runtime'})
    with monkeypatch.context() as m:
        m.setattr(store, 'effective_spec', lambda _: native_spec)
        m.setattr('devflow_temporal.delivery_native_preparation.native_identity',
                  lambda _: {'runtime_payload_sha256': 'same-runtime'})
        with pytest.raises(ValueError, match='repaired measured runtime'):
            snapshot(store, 'run-1', PREPUBLICATION_KIND)

    admitted = store.continue_repair('run-1', second_request)
    assert admitted['workflow_id'].endswith('gates-retry-2')
    assert admitted['additional_iterations'] == 0
    assert store.continue_repair('run-1', request)['workflow_id'].endswith('gates-retry-1')
    spec = store.effective_spec('run-1')
    with store._connect() as db:
        recovery = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_gate_admissions WHERE run_id='run-1'"
        ).fetchone()[0])
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
    assert recovery['original_spec'] == store.spec('run-1')
    assert recovery['original_recovery'] == previous
    assert readback(store, spec, recovery) is None
    assert old_file.read_bytes() == old_bytes
    assert RunResources(spec).root.name == 'resources'
    assert DeliveryBroker(store, spec).evidence_dir.parent.name == 'gates-admission-2'
    third = finalize(spec, recovery, 29)
    with pytest.raises(ValueError, match='closed, finalized'):
        store.continue_repair('run-1', third)


@pytest.mark.parametrize('change', [None, 'projection', 'missing-projection', 'journal',
                                  'run', 'policy', 'path', 'malformed'])
def test_consumed_gate_controller_requires_its_private_run_and_policy_journal(tmp_path, change):
    from copy import deepcopy

    from devflow_temporal.delivery_gate_retry import _consumed_payloads
    from devflow_temporal.delivery_resources import read_private, write_private

    state_dir = tmp_path / 'runs/original'
    path = state_dir / 'gates-admission/evidence/prechecks/native/native-process.json'
    spec = {'run_id': 'original', 'state_dir': str(state_dir), 'policy_digest': 'a' * 64,
            'policy': {'native_identity': {'runtime_payload_sha256': '1' * 64}}}
    process = {'journal': str(path), 'exit_code': 1, 'cleanup': 'observed-native-confirmed',
               'runtime_identity': {'revision': '2' * 40, 'runtime_payload_sha256': '2' * 64}}
    journal = {'intent': {'run_id': 'original', 'policy_digest': 'b' * 64},
               'runtime_identity': deepcopy(process['runtime_identity']),
               'result': deepcopy(process)}
    previous = {'original_spec': {'policy_digest': 'b' * 64},
                'execution_spec': {'policy_digest': 'a' * 64}}
    if change == 'projection':
        process['runtime_identity']['runtime_payload_sha256'] = '3' * 64
    elif change == 'missing-projection':
        del process['runtime_identity']
    elif change == 'journal':
        journal['runtime_identity']['runtime_payload_sha256'] = '3' * 64
    elif change == 'run':
        journal['intent']['run_id'] = 'other'
    elif change == 'policy':
        journal['intent']['policy_digest'] = 'c' * 64
    elif change == 'path':
        process['journal'] = str(tmp_path / 'other/native-process.json')
    elif change == 'malformed':
        process['runtime_identity']['runtime_payload_sha256'] = True
        journal['runtime_identity'] = deepcopy(process['runtime_identity'])
        journal['result'] = deepcopy(process)
    write_private(path, journal)
    before = path.read_bytes()
    state = {'checks': {'prepublish': {'results': [{'native_process': process}]}}}
    if change:
        with pytest.raises(ValueError, match='gate .*controller'):
            _consumed_payloads(spec, state, previous)
    else:
        assert _consumed_payloads(spec, state, previous) == {'2' * 64}
        assert read_private(path)['intent']['policy_digest'] != spec['policy_digest']
    assert path.read_bytes() == before


def test_historical_gate_processes_keep_their_frozen_runtime_comparison(tmp_path):
    from devflow_temporal.delivery_gate_retry import _consumed_payloads
    from devflow_temporal.delivery_resources import write_private

    path = tmp_path / 'runs/original/native-process.json'
    process = {'journal': str(path), 'exit_code': 1, 'cleanup': 'observed-native-confirmed'}
    write_private(path, {'intent': {'run_id': 'original', 'policy_digest': 'a' * 64},
                         'result': process})
    spec = {'run_id': 'original', 'state_dir': str(path.parent), 'policy_digest': 'a' * 64,
            'policy': {'native_identity': {'runtime_payload_sha256': '1' * 64}}}
    assert _consumed_payloads(spec, {'roles': [], 'checks': {
        'prepublish': {'results': [{'native_process': process}]}}}, None) == {'1' * 64}


def test_gate_retry_preserves_the_implementation_session_home(unpublished):
    from devflow_temporal.delivery_sandbox import _native_role_home

    store, _, _, request = unpublished
    original = store.effective_spec('run-1')
    prior = _native_role_home({'spec': original, 'role': 'implement', 'iteration': 0})
    old_review = _native_role_home({'spec': original, 'role': 'review', 'iteration': 1})
    store.continue_repair('run-1', request)
    retried = store.effective_spec('run-1')
    assert _native_role_home({'spec': retried, 'role': 'implement', 'iteration': 1}) == prior
    assert _native_role_home({'spec': retried, 'role': 'review', 'iteration': 1}) != old_review
