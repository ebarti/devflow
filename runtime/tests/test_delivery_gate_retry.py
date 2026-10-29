from __future__ import annotations

import json

import pytest
from test_delivery_metadata_recovery import published as published
from test_delivery_store import service as service
from test_delivery_store import submit_historical_admission

from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_gate_retry import KIND, PREPUBLICATION_KIND
from devflow_temporal.delivery_resources import RunResources


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
    monkeypatch.setattr('devflow_temporal.delivery_gate_retry._stopped_cleanup',
                        lambda _, **_kw: {})
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


@pytest.mark.parametrize('drift', ['source', 'budget'])
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
    monkeypatch.setattr('devflow_temporal.delivery_gate_retry._stopped_cleanup',
                        lambda _, **_kw: {})
    request = {'continuation_kind': KIND, 'command_id': 'retry-gates-1',
               'expected_revision': 13, 'expected_iteration': 1,
               'expected_candidate_id': state['candidate']['id'],
               'expected_pr_number': 7, 'expected_pr_head': state['candidate']['head'],
               'additional_iterations': 0}
    return store, broker, state, closed, request


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


@pytest.mark.asyncio
async def test_public_gate_retry_rechecks_candidate_and_owner(unpublished, monkeypatch):
    """A valid preview cannot authorize changed source or a competing owner."""
    import copy

    import httpx

    from devflow_temporal import delivery_gate_retry
    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_api import create_app

    store, broker, state, command = unpublished
    def renew(spec, *_args):
        renewed = copy.deepcopy(spec)
        renewed['policy']['measured_runtime'] = 'renewed'
        renewed['policy_digest'] = digest(renewed['policy'])
        return renewed
    monkeypatch.setattr(delivery_gate_retry, 'prepare_runtime', renew)
    app = create_app(store.config.path)
    app.state.delivery.store = store
    host = store.config.dashboard_url
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=host) as client:
        token = (await client.get('/api/session')).json()['csrf_token']
        headers = {'Origin': host, 'X-Devflow-CSRF': token}
        async def assess():
            return await client.post('/api/runs/run-1/assess-transition', json={
                'kind': PREPUBLICATION_KIND, 'request': command})
        assert (await assess()).json()['eligible']
        path = broker.checkout / 'README.md'
        original = path.read_bytes()
        path.write_text('Changed after preflight\n')
        changed = await assess()
        response = await client.post('/api/runs/run-1/continue-repair', json=command,
                                     headers=headers)
        assert not changed.json()['eligible']
        assert 'gate_candidate_changed' in {b['code'] for b in changed.json()['blockers']}
        assert response.status_code == 409 and response.json()['detail'] == changed.json()
        path.write_bytes(original)
        with store._connect() as db:
            store.state.claim_work(db, broker.spec['work_id'], 'external:other', 'fixture')
        owned = await assess()
        response = await client.post('/api/runs/run-1/continue-repair', json=command,
                                     headers=headers)
        assert not owned.json()['eligible']
        assert 'gate_owner_conflict' in {b['code'] for b in owned.json()['blockers']}
        assert owned.json()['proof_digest'] is None
        assert response.status_code == 409 and response.json()['detail'] == owned.json()
        # A diagnostic that observes this stopped subject owns a different route.
        from devflow_temporal import delivery_native_diagnostic as diagnostic

        monkeypatch.setattr(diagnostic, '_readonly', lambda value: value)
        diagnostic_request = {
            'command_id': 'diagnostic', 'expected_revision': state['revision'],
            'expected_projection_revision': 0, 'expected_iteration': state['iteration'],
            'expected_candidate_id': command['expected_candidate_id'],
            'expected_candidate_head': command['expected_candidate_head'],
            'expected_content_sha256': '0' * 64, 'expected_pr_number': 1,
            'expected_pr_head': '0' * 40, 'check_id': diagnostic.CHECK_ID,
            'expected_check_sha256': diagnostic.CHECK_SHA256,
        }
        diagnostic_result = await client.post('/api/runs/run-1/assess-transition', json={
            'kind': diagnostic.KIND, 'request': diagnostic_request})
        assert not diagnostic_result.json()['eligible']
        assert diagnostic_result.json()['operation'] == diagnostic.KIND
        with store._connect() as db:
            assert not db.execute('SELECT 1 FROM delivery_gate_admissions').fetchone()
            store.state.release_work(db, broker.spec['work_id'], 'external:other')
        assert (await assess()).json()['eligible']
        accepted = await client.post('/api/runs/run-1/continue-repair', json=command,
                                     headers=headers)
        assert accepted.status_code == 200, accepted.text
        replay = await client.post('/api/runs/run-1/continue-repair', json=command,
                                   headers=headers)
        assert replay.json() == accepted.json()
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_gate_admissions').fetchone()[0] == 1
    assert state['error'] == 'prepublication repair limit exhausted'
