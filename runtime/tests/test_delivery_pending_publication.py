from __future__ import annotations

import asyncio
import json
from copy import deepcopy

import pytest
from test_delivery_gate_retry import unpublished as unpublished
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_pending_publication import readback
from devflow_temporal.delivery_resources import RunResources
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture
def pending(unpublished, monkeypatch):
    store, broker, original, gate_request = unpublished

    def renew(spec, *_args):
        return {**deepcopy(spec), 'policy_digest': digest({'measured_runtime': 'renewed'})}

    monkeypatch.setattr('devflow_temporal.delivery_gate_retry.prepare_runtime', renew)
    store.continue_repair('run-1', gate_request)
    spec = store.effective_spec('run-1')
    with store._connect() as db:
        previous = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'"
        ).fetchone()[0])
    broker = DeliveryBroker(store, spec)
    checked_candidate = broker.candidate()
    broker._effect('publish:run-1:0', 'publish', {
        'iteration': 0, 'input_candidate_id': original['candidate']['id'],
    })
    _git(broker.checkout, 'add', 'README.md')
    _git(broker.checkout, 'commit', '-sqm', 'docs: document the owned feature')
    _git(broker.checkout, 'push', 'origin', 'HEAD:refs/heads/' + spec['branch'])
    state = deepcopy(original)
    state.update(error='publication unresolved: ActivityError', revision=13,
                 candidate=checked_candidate)
    state['checks']['prepublish'].update(state='passed', results=[
        {'id': 'packaging', 'passed': True, 'cleanup': 'confirmed', 'exit_code': 0}])
    store.project('run-1', phase='blocked', execution_state='blocked', event_type='blocked',
                  message=state['error'], candidate=state['candidate'], pull_request=None,
                  checks=state['checks'], iteration=0, protocol_revision=13, outcome='blocked',
                  cleanup='confirmed', error=state['error'])
    with store._connect() as db:
        store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
        workflow = db.execute("SELECT workflow_id FROM delivery_runs WHERE run_id='run-1'")
        workflow_id = workflow.fetchone()[0]
    closed = {'workflow_id': workflow_id, 'request_digest': spec['request_digest'],
              'recovery_digest': digest(previous), 'result': state}
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *_args, **_kw: closed)
    monkeypatch.setattr('devflow_temporal.delivery_pending_publication._stopped_cleanup',
                        lambda _: {})
    request = {'command_id': 'retry-publication-1', 'expected_revision': 13,
               'expected_candidate_id': state['candidate']['id'],
               'expected_head': broker.candidate()['head'], 'expected_pr_number': 0}
    return store, broker, state, request


def test_pending_retry_binds_pushed_commit_and_original_effect_without_source_turn(pending):
    store, broker, state, request = pending
    before = broker.candidate()
    admitted = store.recover_publication('run-1', request)
    assert admitted['implementation_authority'] is False
    assert store.recover_publication('run-1', request) == admitted
    spec = store.effective_spec('run-1')
    assert spec['policy']['max_repairs'] == store.spec('run-1')['policy']['max_repairs']
    assert broker.candidate() == before
    with store._connect() as db:
        recovery = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'"
        ).fetchone()[0])
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
    assert recovery['state'] == state
    assert readback(store, spec, recovery)['head'] == before['head']
    assert RunResources(spec).root.name == 'resources'
    with pytest.raises(ValueError):
        store.recover_publication('run-1', {**request, 'command_id': 'second-publication'})


@pytest.mark.parametrize('drift', ['source', 'head', 'checks', 'effect'])
def test_pending_retry_rejects_changed_pushed_source_or_failed_checkpoint(pending, drift):
    store, broker, state, request = pending
    if drift == 'source':
        (broker.checkout / 'README.md').write_text('Changed after checking\n')
    elif drift == 'head':
        request['expected_head'] = 'a' * 40
    elif drift == 'checks':
        state['checks']['prepublish']['results'][0]['passed'] = False
    else:
        with store._connect() as db:
            db.execute("UPDATE delivery_effects SET request_json='{}' WHERE kind='publish'")
    with pytest.raises(ValueError):
        store.recover_publication('run-1', request)


def test_pending_workflow_completes_effect_then_independent_gates_without_implementation(pending,
                                                                                    monkeypatch):
    store, broker, state, request = pending
    store.recover_publication('run-1', request)
    spec = store.effective_spec('run-1')
    with store._connect() as db:
        recovery = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'"
        ).fetchone()[0])
    flow = DeliveryWorkflow()
    calls = []

    async def execute(name, body, **kw):
        calls.append((name, body.get('role')))
        if name in {'delivery_tracker_start', 'delivery_tracker'}:
            return {'state': 'consistent'}
        if name == 'delivery_publish':
            assert body['candidate'] == state['candidate']
            return {'number': 7, 'head': broker.candidate()['head'],
                    'candidate': broker.candidate()}
        if name == 'delivery_role':
            assert body['role'] in {'review', 'verify'}
            return {'status': 'pass', 'role': body['role'], 'candidate': broker.candidate(),
                    'session_id': 'independent-' + body['role']}
        return {'state': 'passed', 'cleanup': 'confirmed'}

    async def project(*args):
        pass

    monkeypatch.setattr(flow, '_activity', execute)
    monkeypatch.setattr(flow, '_project', project)
    result = asyncio.run(flow._resume_pending_publication(spec, recovery))
    assert result['outcome'] == 'delivered'
    assert result['roles'][0] == state['roles'][0]
    assert ('delivery_role', 'implement') not in calls
    assert [role for name, role in calls if name == 'delivery_role'] == ['review', 'verify']
    assert 'delivery_precheck' not in [name for name, _ in calls]
    assert calls.index(('delivery_publish', None)) < calls.index(('delivery_role', 'review'))
    assert ('delivery_ci', None) in calls


def test_published_assessment_after_publication_recovery_keeps_all_admissions(pending, monkeypatch):
    from devflow_temporal.delivery_gate_retry import KIND

    store, broker, old_state, request = pending
    store.recover_publication('run-1', request)
    spec = store.effective_spec('run-1')
    with store._connect() as db:
        previous = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'"
        ).fetchone()[0])
    original_bytes = {name: (broker.state_dir / name / 'admission.json').read_bytes()
                      for name in ('gates-admission', 'publication-retry')}
    candidate = DeliveryBroker(store, spec).candidate()
    publication = {'number': 7, 'head': candidate['head'], 'candidate': candidate}
    broker._finish_effect('publish:run-1:0', publication)
    monkeypatch.setattr(DeliveryBroker, '_existing_pr', lambda *_a, **_kw: {
        'number': 7, 'state': 'OPEN', 'isDraft': False, 'headRefOid': candidate['head']})
    state = deepcopy(old_state)
    state.update(candidate=candidate, pull_request=publication, revision=23,
                 error='repair limit exhausted')
    state['roles'].append({'role': 'review', 'iteration': 0, 'status': 'findings',
                           'findings': ['Missing broker evidence'], 'cleanup': 'confirmed'})
    store.project('run-1', phase='blocked', execution_state='blocked', event_type='blocked',
                  message=state['error'], candidate=candidate, pull_request=publication,
                  checks=state['checks'], iteration=0, protocol_revision=23, outcome='blocked',
                  cleanup='confirmed', error=state['error'])
    with store._connect() as db:
        store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
        workflow = db.execute("SELECT workflow_id FROM delivery_runs WHERE run_id='run-1'")
        closed = {'workflow_id': workflow.fetchone()[0], 'request_digest': spec['request_digest'],
                  'recovery_digest': digest(previous), 'result': state}
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *_a, **_kw: closed)
    retry = {'continuation_kind': KIND, 'command_id': 'published-evidence-assessment',
             'expected_revision': 23, 'expected_iteration': 0,
             'expected_candidate_id': candidate['id'], 'expected_pr_number': 7,
             'expected_pr_head': candidate['head'], 'additional_iterations': 0}
    result = store.continue_repair('run-1', retry)
    assert result['implementation_authority'] is False
    assert 'published-gates-retry-1' in result['workflow_id']
    current = store.effective_spec('run-1')
    assert current['policy']['max_repairs'] == spec['policy']['max_repairs']
    assert DeliveryBroker(store, current).evidence_dir.parent.name == 'published-gates-admission'
    assert DeliveryBroker(store, current).candidate()['id'] == candidate['id']
    for name, data in original_bytes.items():
        assert (broker.state_dir / name / 'admission.json').read_bytes() == data
    with store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
    with pytest.raises(ValueError):
        store.continue_repair('run-1', {**retry, 'command_id': 'second-published-assessment'})
