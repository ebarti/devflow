"""A closed coordinator can hand off after its worker's separate recovery closes."""
from __future__ import annotations

import json

import pytest
from test_delivery_feature_execution import feature_service
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import (
    continue_feature,
    register_worker,
    registry,
    worker_key,
    worker_spec,
)
from devflow_temporal.delivery_github_contract import ordered_chunks


@pytest.fixture
def recovered_worker(service, monkeypatch):
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    parent = store.effective_spec(request['run_id'])
    chunk = ordered_chunks(json.loads(request['accepted_plan']))[0]
    child = worker_spec(parent, chunk, {'url': 'https://github.com/example/fixture/issues/10'},
                        kind='build', base_sha=parent['base_sha'], base_branch='main')
    register_worker(store, parent, child)
    token = parent['feature_delivery']['owner']
    shared = registry(parent)
    shared.repair(token, 'prior-product-cycle', 'retained original product repair')
    shared.drain(token)
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET phase='blocked',outcome='blocked',"
                   "execution_state='blocked',cleanup='confirmed' WHERE run_id=?",
                   (parent['run_id'],))
        db.execute("UPDATE delivery_runs SET phase='delivered',outcome='delivered',"
                   "execution_state='terminal',cleanup='confirmed' WHERE run_id=?",
                   (child['run_id'],))
    observed = []

    def closed(run_id, **kwargs):
        observed.append((run_id, kwargs.get('workflow_id')))
        return {'result': {'outcome': 'blocked' if run_id == parent['run_id'] else 'delivered'}}

    monkeypatch.setattr(store, '_completed_temporal_result', closed)
    monkeypatch.setattr(activities, 'current_record', lambda _: {'preserved': True})
    command = {'command_id': 'continue-recovered-feature', 'expected_revision': 1}
    return store, parent, child, shared, token, command, observed


def test_public_continuation_settles_recovered_worker_without_changing_budget(recovered_worker):
    store, parent, child, shared, token, command, observed = recovered_worker
    budget = shared.budget(token['issue_id'])
    result = continue_feature(store, parent['run_id'], command)
    successor = store.effective_spec(result['run_id'])
    assert successor['feature_delivery']['owner']['generation'] == token['generation'] + 1
    assert shared.budget(token['issue_id']) == budget
    assert (child['run_id'], store.active_workflow_id(child['run_id'])) in observed
    with shared.connect() as db:
        previous = dict(db.execute('SELECT * FROM execution_workers WHERE worker_key=?',
                                    (worker_key(child['run_id'], token),)).fetchone())
    assert previous['state'] == 'finished'
    assert continue_feature(store, parent['run_id'], command) == result
    assert store.submitted_spec(child['run_id']) == child


@pytest.mark.parametrize('failure', ['parent-live', 'child-live', 'attempt', 'outcome', 'effect',
                                   'resources', 'wrong-feature'])
def test_handoff_rejects_unsettled_or_unconfirmed_workers(recovered_worker, monkeypatch, failure):
    store, parent, child, shared, token, command, _ = recovered_worker
    if failure in {'parent-live', 'child-live'}:
        original = store._completed_temporal_result

        def closed(run_id, **kwargs):
            target = parent['run_id'] if failure == 'parent-live' else child['run_id']
            if run_id == target:
                raise ValueError('continuation predecessor Temporal closure is unproven')
            return original(run_id, **kwargs)

        monkeypatch.setattr(store, '_completed_temporal_result', closed)
    elif failure == 'effect':
        # A retained external operation cannot be bypassed by finishing the worker.
        with shared.connect() as db:
            db.execute('INSERT INTO execution_effects VALUES (?,?,?,?,?,?,?,?,?)',
                       (token['issue_id'], 'pending', token['generation'], 'publication',
                        'frozen', '{}', 'pending', None, 'original'))
    elif failure == 'resources':
        def unfinished(*_):
            raise OwnershipConflict('worker resource cleanup remains unresolved')
        monkeypatch.setattr(activities, 'finish_worker', unfinished)
    else:
        with store._connect() as db:
            if failure == 'attempt':
                db.execute("INSERT INTO delivery_attempts(job_key,run_id,role,iteration,"
                           "candidate_id,state,cleanup) VALUES (? ,?,'verify',0,'fixed',"
                           "'running','unknown')", ('unfinished', child['run_id']))
            elif failure == 'outcome':
                db.execute('UPDATE delivery_runs SET outcome=NULL WHERE run_id=?',
                           (child['run_id'],))
            else:
                other = json.loads(json.dumps(child))
                other['feature_delivery']['owner']['issue_id'] = 'other-feature'
                db.execute('UPDATE delivery_runs SET request_json=? WHERE run_id=?',
                           (json.dumps(other), child['run_id']))
    with pytest.raises((ValueError, OwnershipConflict)):
        continue_feature(store, parent['run_id'], command)
    current = shared.current(token['issue_id'])
    assert shared.token(current) == token and current['state'] == 'draining'
