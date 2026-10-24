"""Interrupted finalizers preserve absence of a producer receipt, not invented evidence."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest
from test_delivery_automatic_retry_cleanup import later_cleanup as later_cleanup
from test_delivery_store import service as service

from devflow_temporal import delivery_automatic_retry as retry
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_orphans import reconcile_attempt
from devflow_temporal.delivery_resources import write_private


@pytest.fixture
def unrecorded(later_cleanup):
    store, request, historical, calls, external = later_cleanup
    historical['result']['checks']['resource_cleanup'] = {
        'state': 'unknown', 'process_cleanup': 'unknown', 'resource_cleanup': 'unknown',
        'reason': 'ActivityError',
    }
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET checks_json=?',
                   (json.dumps(historical['result']['checks']),))
    external['ready'] = True
    return store, request, historical, calls, external


def observation(store, request, historical):
    with store._connect() as db:
        records = [json.loads(row[0]) for row in db.execute(
            "SELECT payload_json FROM delivery_events WHERE run_id=? "
            "AND type='resource_cleanup_reconciled'", (request['run_id'],))]
    saved = [record for record in records if 'observed_resources' in record]
    assert len(saved) == 1
    record = saved[0]
    assert record['original_result_digest'] == digest(historical['result'])
    assert record['original_checks'] == historical['result']['checks']
    assert record['producer_receipt_source'] == 'not_recorded'
    assert 'predecessor_resources' not in record
    archive = Path(store.spec(request['run_id'])['state_dir']) / 'resources' / (
        'cleanup-observations') / record['original_result_digest']
    assert not archive.joinpath('finalization.json').exists()
    for name, sha in record['observed_resources'].items():
        assert hashlib.sha256(archive.joinpath(name).read_bytes()).hexdigest() == sha
    return archive, record


def test_owner_maintenance_preserves_missing_producer_binding_and_observes_cleanup(unrecorded):
    store, request, historical, calls, external = unrecorded
    before = copy.deepcopy(historical)
    root = Path(store.spec(request['run_id'])['state_dir']) / 'resources'
    observed = root.joinpath('finalization.json').read_bytes()
    assert len(retry.retry_once(store)) == 1
    archive, _ = observation(store, request, historical)
    assert archive.joinpath('observed-finalization.json').read_bytes() == observed
    assert historical == before
    assert 'receipt_sha256' not in historical['result']['checks']['resource_cleanup']
    assert ('finalize', 'blocked', False) in calls
    assert external['external_sets'] == 1
    with store._connect() as db:
        old = db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                         (request['run_id'],)).fetchone()
        assert old['cleanup'] == 'confirmed'
        assert old['phase'] == old['outcome'] == 'blocked'
        assert json.loads(old['checks_json'])['failure'] == before['result']['checks']['failure']


@pytest.mark.parametrize('interrupted', ['finalizer', 'tracker', 'base'])
def test_missing_receipt_observation_replays_after_canonical_files_change(unrecorded, interrupted):
    store, request, historical, calls, external = unrecorded
    before = copy.deepcopy(historical)
    if interrupted == 'finalizer':
        external['crash_finalizing'] = True
    elif interrupted == 'tracker':
        external['lost_ack'] = True
    else:
        external['base_unknown'] = True
    assert retry.retry_once(store) == []
    archive, record = observation(store, request, historical)
    preserved = {name: archive.joinpath(name).read_bytes() for name in record['observed_resources']}
    store._automatic_retry_waits.clear()
    external['base_unknown'] = False
    assert len(retry.retry_once(store)) == 1
    assert observation(store, request, historical)[1] == record
    assert all(archive.joinpath(name).read_bytes() == raw for name, raw in preserved.items())
    assert historical == before
    assert external['external_sets'] == 1


def test_public_cleanup_preserves_failed_ci_and_original_result_without_bound_receipt(
    unrecorded, monkeypatch,
):
    store, request, historical, _calls, external = unrecorded
    candidate = {'id': 'b' * 64, 'head': 'a' * 40}
    publication = {'number': 12, 'head': candidate['head'], 'state': 'OPEN'}
    ci = {'state': 'failed', 'head': candidate['head'], 'findings': ['owned failed CI']}
    state = historical['result']
    state.update(revision=7, iteration=0, error='required CI failed', candidate=candidate,
                 pull_request=publication, tracker=None)
    state['checks']['ci'] = ci
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET protocol_revision=7,iteration=0,error=?, '
                   'candidate_json=?,pr_json=?,tracker_json=?,checks_json=?',
                   (state['error'], json.dumps(candidate), json.dumps(publication), 'null',
                    json.dumps(state['checks'])))
        original_request = db.execute('SELECT request_json FROM delivery_runs').fetchone()[0]
    spec = store.effective_spec(request['run_id'])
    spec['provider'] = 'codex'
    spec['policy']['execution_backend'] = 'native-macos'
    spec['policy_digest'] = digest(spec['policy'])
    monkeypatch.setattr(store, 'effective_spec', lambda _: spec)
    before = copy.deepcopy(historical)
    result = reconcile_attempt(store, run_id=request['run_id'], role='cleanup', iteration=0)
    assert result['cleanup'] == 'confirmed' and result['changed'] is True
    observation(store, request, historical)
    assert historical == before
    with store._connect() as db:
        row = db.execute('SELECT * FROM delivery_runs').fetchone()
        assert row['request_json'] == original_request
        assert row['error'] == 'required CI failed'
        assert row['phase'] == row['outcome'] == row['execution_state'] == 'blocked'
        assert json.loads(row['checks_json'])['ci'] == ci
        assert json.loads(row['pr_json']) == publication
        assert json.loads(row['candidate_json']) == candidate
        assert store.state.claim_for(db, request['work_id']) is None
    assert external['external_sets'] == 1
    again = reconcile_attempt(store, run_id=request['run_id'], role='cleanup', iteration=0)
    assert again['changed'] is False
    assert external['external_sets'] == 1


@pytest.mark.parametrize('defect', ['partial_path', 'null_hash', 'empty_hash', 'invalid_hash',
                                   'declared_mismatch', 'declared_wrong_path', 'confirmed',
                                   'malformed_producer'])
def test_missing_receipt_support_never_waives_a_declared_or_malformed_binding(unrecorded, defect):
    store, request, historical, calls, _external = unrecorded
    producer = historical['result']['checks']['resource_cleanup']
    root = Path(store.spec(request['run_id'])['state_dir']) / 'resources'
    if defect == 'partial_path':
        producer['receipt'] = str(root / 'finalization.json')
    elif defect in {'null_hash', 'empty_hash', 'invalid_hash', 'declared_mismatch'}:
        producer['receipt_sha256'] = {
            'null_hash': None, 'empty_hash': '', 'invalid_hash': 'not-a-sha256',
            'declared_mismatch': '0' * 64,
        }[defect]
    elif defect == 'declared_wrong_path':
        producer.update(receipt='/unrelated/finalization.json', receipt_sha256=hashlib.sha256(
            root.joinpath('finalization.json').read_bytes()).hexdigest())
    elif defect == 'confirmed':
        producer.update(state='confirmed', process_cleanup='observed-native-confirmed',
                        resource_cleanup='confirmed')
    else:
        historical['result']['checks']['resource_cleanup'] = None
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET checks_json=?',
                   (json.dumps(historical['result']['checks']),))
        row = dict(db.execute('SELECT * FROM delivery_runs').fetchone())
    with pytest.raises(ValueError):
        retry._close_cleanup(store, row, store.spec(request['run_id']), historical,
                             lambda: False, published=True)
    assert not any(item[0] in {'finalize', 'tracker'} for item in calls)
    assert not root.joinpath('cleanup-observations').exists()
    assert not root.joinpath('predecessor-resources').exists()


@pytest.mark.parametrize('name', ['manifest.json', 'observed-finalization.json'])
def test_unrecorded_producer_keeps_archive_tamper_guards_on_retry(unrecorded, name):
    store, request, historical, calls, external = unrecorded
    external['crash_finalizing'] = True
    assert retry.retry_once(store) == []
    archive, _ = observation(store, request, historical)
    write_private(archive / name, {'tampered': True})
    finalizations = calls.count(('finalize', 'blocked', False))
    store._automatic_retry_waits.clear()
    assert retry.retry_once(store) == []
    assert calls.count(('finalize', 'blocked', False)) == finalizations
    assert not any(item[0] == 'tracker' for item in calls)


@pytest.mark.parametrize('name', ['manifest.json', 'finalization.json'])
def test_unrecorded_producer_rechecks_observed_bytes_before_archive(unrecorded, name):
    store, request, _historical, calls, external = unrecorded
    root = Path(store.spec(request['run_id'])['state_dir']) / 'resources'
    external['on_publication'] = lambda: write_private(root / name, {'raced': True})
    assert retry.retry_once(store) == []
    assert not root.joinpath('cleanup-observations').exists()
    assert not any(item[0] in {'finalize', 'tracker'} for item in calls)
