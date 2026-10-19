"""A fixed-budget ordinary delivery can execute an unchanged unstarted grant."""
from __future__ import annotations

import copy

import pytest

from test_delivery_stopped_resume import project, saved
from test_delivery_store import service as service
from test_delivery_unused_budget_resume import fixed_stop

from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_broker import DeliveryBroker


@pytest.fixture
def unstarted(fixed_stop, monkeypatch):
    store, broker, state, command = fixed_stop
    original = copy.deepcopy(store.submitted_spec('run-1'))
    first = store.continue_repair('run-1', command)
    prior = saved(store)
    spec = store.effective_spec('run-1')
    candidate = DeliveryBroker(store, spec).candidate()
    later = copy.deepcopy(state)
    later.update(iteration=2, revision=15, candidate=candidate,
                 error='implementer did not establish a pass')
    project(store, later)
    with store._connect() as db:
        store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
    closed = {'workflow_id': first['workflow_id'], 'execution_run_id': 'closed-unstarted',
              'request_digest': spec['request_digest'], 'recovery_digest': digest(prior),
              'result': later}
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *_a, **_kw: closed)
    retry = {'continuation_kind': resume.KIND, 'command_id': 'execute-original-unstarted-turn',
             'expected_revision': 15, 'expected_iteration': 2,
             'expected_candidate_id': candidate['id'],
             'expected_candidate_head': candidate['head'], 'additional_iterations': 0}
    return store, broker, original, prior, later, retry


def test_ordinary_unstarted_turn_preserves_its_fixed_ceiling_and_original_session(unstarted, monkeypatch):
    store, _broker, original, prior, later, command = unstarted
    preflight = store.repair_admission_preflight('run-1', command)
    assert preflight['authorized_through_iteration'] == 2
    admitted = store.continue_repair('run-1', command)
    assert store.continue_repair('run-1', command) == admitted
    recovery = saved(store)
    assert recovery['pending_repair']['iteration'] == 2
    assert recovery['maximum_iteration'] == 2
    assert recovery['session_id'] == prior['session_id'] == 'original-implementation'
    assert store.effective_spec('run-1')['policy']['max_repairs'] == 2
    assert store.submitted_spec('run-1') == original
    assert recovery['state'] == later
    assert resume.readback(store, store.effective_spec('run-1'), recovery) == {'state': 'confirmed'}
    with store._connect() as db:
        assert db.execute('SELECT count(*) FROM delivery_runs').fetchone()[0] == 1
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
    import asyncio
    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    flow = DeliveryWorkflow()
    spec = store.effective_spec('run-1')
    async def confirmed(*_):
        return True
    async def activity(name, body, **_):
        assert name == 'delivery_tracker_start' and body['spec'] == spec
        return {'state': 'consistent'}
    async def project_state(*_):
        pass
    async def iterations(actual, **body):
        assert actual == spec
        assert body['start_iteration'] == body['authorized_max_iteration'] == 2
        assert body['prior_implementer_session'] == 'original-implementation'
        assert body['repair_findings'] == recovery['pending_repair']['findings']
        return {'outcome': 'original-turn'}
    monkeypatch.setattr(flow, '_confirm_repair_preflight', confirmed)
    monkeypatch.setattr(flow, '_activity', activity)
    monkeypatch.setattr(flow, '_project', project_state)
    monkeypatch.setattr(flow, '_run_iterations', iterations)
    assert asyncio.run(flow._resume_stopped(spec, recovery)) == {'outcome': 'original-turn'}


@pytest.mark.parametrize('drift', ['provider_attempt', 'effect', 'source', 'session', 'extra_turn'])
def test_ordinary_unstarted_proof_cannot_replay_consumed_or_changed_authority(unstarted, drift):
    store, broker, _original, prior, later, command = unstarted
    if drift == 'source':
        (broker.checkout / 'README.md').write_text('unsealed replacement source\n')
    elif drift == 'session':
        later['roles'][0]['session_id'] = 'different-session'
        project(store, later)
    elif drift == 'extra_turn':
        command = {**command, 'additional_iterations': 1}
    else:
        with store._connect() as db:
            if drift == 'provider_attempt':
                db.execute("INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,"
                           "state,session_id,result_json,cleanup) VALUES"
                           "('consumed','run-1','implement',2,?,'finished',?,'{}','confirmed')",
                           (command['expected_candidate_id'], prior['session_id']))
            else:
                db.execute("INSERT INTO delivery_effects(effect_key,run_id,kind,request_json,"
                           "state,observed_json,updated_at) VALUES"
                           "('changed','run-1','prepare','{}','complete','{}','later')")
    with pytest.raises(ValueError):
        store.continue_repair('run-1', command)
    with store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_commands WHERE command_id=?',
                              (command['command_id'],)).fetchone()
