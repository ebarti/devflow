"""A source-ready ordinary issue resumes gates after proved controller-only launch failure."""
from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path

import pytest
from test_delivery_store import _git, service as service
from test_delivery_stopped_resume import project, saved
from test_delivery_unused_budget_resume import fixed_stop

from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_resources import private_directory
from devflow_temporal.delivery_workflow import DeliveryWorkflow
from devflow_temporal.supervisor import DeliverySupervisor


@pytest.fixture
def assessment_stop(fixed_stop, monkeypatch):
    store, broker, state, command = fixed_stop
    _git(broker.checkout, 'add', 'README.md')
    _git(broker.checkout, '-c', 'commit.gpgsign=false', 'commit', '-qm', 'Accepted source')
    candidate = broker.candidate()
    state['candidate'] = candidate
    state['error'] = 'repair limit exhausted'
    raw = {'status': 'pass', 'summary': 'Accepted implementation', 'findings': [],
           'session_id': 'original-implementation', 'cleanup': 'confirmed'}
    state['roles'] = [{**raw, 'role': 'implement', 'iteration': state['iteration'],
                       'candidate': candidate}]
    state['pull_request'] = {'number': 7, 'head': candidate['head'], 'candidate': candidate,
                            'url': 'https://github.com/example/fixture/pull/7'}
    state['checks'].update(prepublish={'state': 'passed', 'candidate_id': candidate['id'],
                                      'source_unchanged': True, 'results': []},
                          local={'state': 'passed', 'candidate_id': candidate['id'],
                                 'source_unchanged': True, 'results': []},
                          review={'state': 'failed', 'candidate_id': candidate['id']},
                          qa={'state': 'failed', 'candidate_id': candidate['id']})
    attempts = []
    with store._connect() as db:
        db.execute('UPDATE delivery_attempts SET candidate_id=?,result_json=?',
                   (candidate['id'], canonical_json(raw)))
        for role in ['review', 'verify']:
            request = {'spec': broker.spec, 'role': role, 'iteration': state['iteration'],
                       'candidate': candidate}
            key = DeliverySupervisor._job_key(request)
            folder = Path(broker.spec['state_dir']) / 'attempts' / key
            private_directory(folder)
            result = {'status': 'blocked', 'summary': 'role launch failed before any provider process started',
                      'findings': ['implementation artifacts describe another candidate content'],
                      'session_id': None, 'cleanup': 'confirmed', 'usage': None,
                      'finish_reason': 'prelaunch'}
            state['roles'].append({**result, 'role': role, 'iteration': state['iteration'],
                                    'candidate': candidate})
            attempt = {'job_key': key, 'run_id': 'run-1', 'role': role,
                       'iteration': state['iteration'], 'candidate_id': candidate['id'],
                       'state': 'finished', 'result_path': str(folder / 'result.json'),
                       'result_json': canonical_json(result), 'cleanup': 'confirmed'}
            columns = ','.join(attempt)
            db.execute(f'INSERT INTO delivery_attempts({columns}) VALUES ({",".join("?" for _ in attempt)})',
                       tuple(attempt.values()))
            attempts.append(attempt)
    project(store, state)
    closed = {**store._completed_temporal_result('run-1'), 'result': state}
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *_a, **_k: copy.deepcopy(closed))
    # Only the external GitHub transport is modeled; Git/source/store/admission run normally.
    monkeypatch.setattr(resume, 'published_identity', lambda *_a: state['pull_request'])
    command.update(command_id='resume-unlaunched-assessments', additional_iterations=0,
                   expected_candidate_id=candidate['id'], expected_candidate_head=candidate['head'])
    return store, broker, state, command, attempts


def test_public_assessment_prelaunch_resume_preserves_source_pr_and_paid_iteration(assessment_stop, monkeypatch):
    store, broker, state, command, _ = assessment_stop
    original = copy.deepcopy(store.submitted_spec('run-1'))
    stopped = copy.deepcopy(state)
    observed = resume.snapshot(store, 'run-1')
    assert resume._unlaunched_assessments(observed), {
        'state': {k: observed['state'].get(k) for k in ['error', 'phase', 'iteration']},
        'roles': observed['state']['roles'], 'attempts': observed['attempts'],
        'spec': {k: observed['predecessor_spec'].get(k) for k in ['gate_admission', 'policy_digest']}}
    assert resume.interrupted_resume(observed), 'eligible assessment gates need an interrupted plan'
    assert store.repair_admission_preflight('run-1', command)['authorized_through_iteration'] == 1
    store.continue_repair('run-1', command)
    recovery = saved(store)
    assert recovery['state'] == stopped
    assert recovery['maximum_iteration'] == recovery['resume_iteration'] == 1
    assert recovery['resume_stage'] == 'published'
    assert recovery['gate_evidence']['implementation_job_key']
    assert store.submitted_spec('run-1') == original
    assert broker.candidate() == stopped['candidate']
    assert resume.readback(store, store.effective_spec('run-1'), recovery) == {'state': 'confirmed'}
    flow = DeliveryWorkflow()
    async def confirmed(*_a): return True
    async def activity(*_a, **_k): return {'state': 'consistent'}
    async def projected(*_a, **_k): pass
    async def iterations(_spec, **body):
        assert body['published_checkpoint'] is True
        assert body['start_iteration'] == body['authorized_max_iteration'] == 1
        assert body['prior_implementer_session'] == 'original-implementation'
        return {'outcome': 'gates-only'}
    monkeypatch.setattr(flow, '_confirm_repair_preflight', confirmed)
    monkeypatch.setattr(flow, '_activity', activity)
    monkeypatch.setattr(flow, '_project', projected)
    monkeypatch.setattr(flow, '_run_iterations', iterations)
    assert asyncio.run(flow._resume_stopped(store.effective_spec('run-1'), recovery)) == {
        'outcome': 'gates-only'}


@pytest.mark.parametrize('drift', ['pid', 'session', 'result', 'files', 'candidate',
                                  'local-failure', 'prepublish-failure', 'source-changed', 'coding-finding'])
def test_assessment_retry_rejects_started_work_changed_source_and_real_findings(assessment_stop, drift):
    store, broker, state, command, attempts = assessment_stop
    if drift in {'pid', 'session', 'result'}:
        field, value = {'pid': ('pid', 123), 'session': ('session_id', 'started'),
                        'result': ('result_json', '{}')}[drift]
        with store._connect() as db:
            db.execute(f'UPDATE delivery_attempts SET {field}=? WHERE job_key=?',
                       (value, attempts[0]['job_key']))
    elif drift == 'files':
        Path(attempts[0]['result_path']).with_name('launch.json').write_text('{}')
    elif drift == 'candidate':
        (broker.checkout / 'README.md').write_text('unsealed later change\n')
    elif drift == 'coding-finding':
        state['roles'][-1].update(status='findings', finish_reason='done', session_id='real-review')
        project(store, state)
    else:
        stage = 'prepublish' if drift == 'prepublish-failure' else 'local'
        field = 'state' if drift.endswith('failure') else 'source_unchanged'
        state['checks'][stage][field] = 'failed' if field == 'state' else False
        project(store, state)
    with pytest.raises(ValueError):
        store.continue_repair('run-1', command)


def test_one_unlaunched_review_can_resume_alongside_an_actual_passing_assessment(assessment_stop):
    store, _broker, state, command, attempts = assessment_stop
    state['error'] = 'independent assessment launch failed before provider'
    role = state['roles'][-1]
    role.update(status='pass', summary='Completed independent QA', findings=[],
                session_id='independent-qa', finish_reason='done')
    raw = {k: v for k, v in role.items() if k not in {'role', 'iteration', 'candidate'}}
    with store._connect() as db:
        db.execute('UPDATE delivery_attempts SET session_id=?,result_json=? WHERE job_key=?',
                   ('independent-qa', canonical_json(raw), attempts[-1]['job_key']))
    state['checks']['qa']['state'] = 'passed'
    project(store, state)
    assert store.repair_admission_preflight('run-1', command)['authorized_through_iteration'] == 1
