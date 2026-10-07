"""Logical controller transitions; native/Temporal/tracker boundaries are opaque doubles."""
from __future__ import annotations

import copy
import hashlib
import json
from contextlib import contextmanager
from pathlib import Path

import pytest
from test_delivery_automatic_retry import stopped
from test_delivery_store import service as service

from devflow_temporal import delivery_automatic_retry as retry
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore


@pytest.fixture
def later_cleanup(service, monkeypatch):
    store, request, closed = stopped(service, monkeypatch)
    historical = copy.deepcopy(closed)
    historical['result']['cleanup'] = 'unknown'
    historical['result']['checks']['resource_cleanup'].update(
        state='unknown', resource_cleanup='unknown', process_cleanup='unknown')
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET cleanup=?,checks_json=?',
                   ('unknown', json.dumps(historical['result']['checks'])))
        store.state.claim_work(db, request['work_id'],
                               'external:devflow:' + request['run_id'], 'fixture')
    from devflow_temporal.delivery_resources import write_private
    resources = Path(store.spec(request['run_id'])['state_dir']) / 'resources'
    write_private(resources / 'manifest.json', {'run_id': request['run_id'], 'original': True})
    write_private(resources / 'finalization.json', {'state': 'unknown', 'outcome': 'blocked'})
    original_hash = hashlib.sha256((resources / 'finalization.json').read_bytes()).hexdigest()
    historical['result']['checks']['resource_cleanup']['receipt_sha256'] = original_hash
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET checks_json=?',
                   (json.dumps(historical['result']['checks']),))
    calls = []
    external = {'ready': False, 'hash': original_hash, 'tracker': 'consistent'}
    monkeypatch.setattr(DeliveryStore, '_completed_temporal_result',
                        lambda *_a, **_k: copy.deepcopy(historical))

    def observe(_spec, *, unknown_allowed=False):
        calls.append(('observe', unknown_allowed))
        if external.get('on_observe'):
            external['on_observe'](unknown_allowed)
        if not external['ready']:
            raise ValueError('original monitor is not observably finished')
        return {'manifest_sha256': hashlib.sha256(
                    (resources / 'manifest.json').read_bytes()).hexdigest(),
                'finalization_sha256': external['hash'],
                'journal_sha256': {'opaque': external.get('journal', 'original')}}

    class Resources:
        def __init__(self, spec, **_kwargs):
            assert spec['run_id'] == request['run_id']

        @contextmanager
        def locked(self):
            yield {}

        def finalize(self, outcome, *, uncertain=False):
            calls.append(('finalize', outcome, uncertain))
            assert external['ready'] and not uncertain
            write_private(resources / 'manifest.json', {'run_id': request['run_id'],
                                                        'finalization': 'later'})
            if external.pop('crash_finalizing', False):
                raise RuntimeError('opaque crash after replacing manifest')
            write_private(resources / 'finalization.json', {'state': 'confirmed',
                                                            'outcome': 'blocked'})
            external['hash'] = hashlib.sha256(
                (resources / 'finalization.json').read_bytes()).hexdigest()
            return {'state': 'confirmed', 'resource_cleanup': 'confirmed',
                    'process_cleanup': 'observed-native-confirmed',
                    'receipt_sha256': external['hash']}

    def tracker(spec, status, *, release, terminal, reason=None):
        calls.append(('tracker', status, release, terminal))
        assert status == 'blocked' and release and terminal
        if external.get('on_tracker'):
            external['on_tracker']()
        if external['tracker'] == 'consistent' and not external.get('released'):
            with store._connect() as db:
                store.state.release_work(db, spec['work_id'],
                                         'external:devflow:' + spec['run_id'])
            external['released'] = True
            external['external_sets'] = external.get('external_sets', 0) + 1
        if external.get('lost_ack'):
            external['lost_ack'] = False
            raise RuntimeError('opaque terminal acknowledgement lost after release')
        return {'state': external['tracker'], 'pending': external['tracker'] != 'consistent'}

    def publication(_store, spec):
        calls.append(('publication',))
        if external.get('on_publication'):
            external['on_publication']()
        if external.get('base_unknown'):
            raise ValueError('opaque fresh approved base unavailable')
        return spec['base_sha']

    monkeypatch.setattr(retry, 'observe_finalized_resources', observe)
    monkeypatch.setattr(retry, 'observe_completed_resources',
                        lambda spec, _attempts: observe(spec, unknown_allowed=True), raising=False)
    monkeypatch.setattr(retry, 'RunResources', Resources, raising=False)
    monkeypatch.setattr(retry, '_tracker_sync', tracker, raising=False)
    monkeypatch.setattr(retry, 'fresh_unpublished_base', publication)
    def absent(_store, _spec):
        calls.append(('publication',))
        if external.get('on_publication'):
            external['on_publication']()
    monkeypatch.setattr(retry, 'unpublished', absent, raising=False)
    return store, request, historical, calls, external


def test_unknown_closure_later_observed_cleanup_releases_own_claim_and_retries(later_cleanup):
    store, request, historical, calls, external = later_cleanup
    assert retry.retry_once(store) == []
    assert not any(call[0] in {'finalize', 'tracker'} for call in calls)
    external['ready'] = True
    store._automatic_retry_waits.clear()
    saved = copy.deepcopy(historical)
    admitted = retry.retry_once(store)
    assert len(admitted) == 1
    assert historical == saved  # Original closed result is never rewritten.
    assert ('observe', True) in calls and ('observe', False) in calls
    assert ('tracker', 'blocked', True, True) in calls
    assert calls.index(('publication',)) < calls.index(('finalize', 'blocked', False))
    assert retry.retry_once(store) == []
    with store._connect() as db:
        old = db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                         (request['run_id'],)).fetchone()
        assert old['outcome'] == old['phase'] == old['execution_state'] == 'blocked'
        assert (json.loads(old['checks_json'])['failure']
                == historical['result']['checks']['failure'])
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 2


def test_finished_attempt_does_not_hide_later_cleanup_obligation(later_cleanup):
    store, request, _historical, calls, external = later_cleanup
    from devflow_temporal.supervisor import DeliverySupervisor

    job, _ = DeliverySupervisor(store, capacity=1)._claim({
        'spec': store.spec(request['run_id']), 'role': 'implement', 'iteration': 0,
        'candidate': {'id': 'fixture', 'head': 'fixture', 'worktree_sha256': 'fixture'}})
    with store._connect() as db:
        db.execute("UPDATE delivery_attempts SET state='finished',cleanup='confirmed' "
                   'WHERE job_key=?', (job,))
    external['ready'] = True
    assert len(retry.retry_once(store)) == 1
    assert ('observe', True) in calls


def test_exhausted_ceiling_still_finishes_cleanup_and_claim_closure(service, monkeypatch):
    fixture_store, request = service
    raw = copy.deepcopy(fixture_store.config.raw)
    raw['max_attempts'] = 1
    fixture_store.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(fixture_store.config.path))
    fixture = later_cleanup.__wrapped__((store, request), monkeypatch)
    store, request, _historical, calls, external = fixture
    external['ready'] = True
    assert retry.retry_once(store) == []
    assert ('tracker', 'blocked', True, True) in calls
    with store._connect() as db:
        assert store.state.claim_for(db, request['work_id']) is None
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1


@pytest.mark.parametrize('obstacle', ['foreign_claim', 'effect_unknown', 'cancel',
                                     'unfinished_attempt', 'changed_failure', 'changed_record'])
def test_later_cleanup_cannot_bypass_original_safety_guards(later_cleanup, obstacle):
    store, request, historical, calls, external = later_cleanup
    external['ready'] = True
    with store._connect() as db:
        if obstacle == 'foreign_claim':
            store.state.release_work(db, request['work_id'],
                                     'external:devflow:' + request['run_id'])
            store.state.claim_work(db, request['work_id'], 'foreign', 'fixture')
        elif obstacle == 'effect_unknown':
            db.execute('INSERT INTO delivery_effects VALUES (?,?,?,?,?,?,?)',
                       ('effect', request['run_id'], 'tracker', '{}', 'unknown', None, 'now'))
        elif obstacle == 'cancel':
            db.execute('INSERT INTO delivery_mutations '
                       '(command_id,run_id,kind,request_digest,state) VALUES (?,?,?,?,?)',
                       ('cancel', request['run_id'], 'cancel', 'opaque', 'complete'))
        elif obstacle == 'changed_failure':
            historical['result']['checks']['failure']['cause_type'] = 'different'
        elif obstacle == 'changed_record':
            db.execute("UPDATE delivery_runs SET request_digest='changed'")
    if obstacle == 'unfinished_attempt':
        from devflow_temporal.supervisor import DeliverySupervisor
        DeliverySupervisor(store, capacity=1)._claim({
            'spec': store.spec(request['run_id']), 'role': 'implement', 'iteration': 0,
            'candidate': {'id': 'fixture', 'head': 'fixture', 'worktree_sha256': 'fixture'}})
    assert retry.retry_once(store) == []
    assert not any(call[0] in {'finalize', 'tracker'} for call in calls)


def test_unacknowledged_terminal_tracker_cannot_admit(later_cleanup):
    store, _request, _historical, calls, external = later_cleanup
    external.update(ready=True, tracker='pending')
    assert retry.retry_once(store) == []
    assert ('tracker', 'blocked', True, True) in calls
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1


def test_later_closure_survives_base_failure_without_repeating_effects_or_events(later_cleanup):
    store, request, historical, calls, external = later_cleanup
    external.update(ready=True, base_unknown=True)
    assert retry.retry_once(store) == []
    with store._connect() as db:
        events = list(db.execute("SELECT payload_json FROM delivery_events "
                                 "WHERE type='resource_cleanup_reconciled'"))
    completed = [json.loads(item[0]) for item in events if 'tracker' in json.loads(item[0])]
    assert len(completed) == 1
    assert completed[0]['original_checks'] == historical['result']['checks']
    count = calls.count(('finalize', 'blocked', False))
    store._automatic_retry_waits.clear()
    external['base_unknown'] = False
    assert len(retry.retry_once(store)) == 1
    assert calls.count(('finalize', 'blocked', False)) == count
    assert external['external_sets'] == 1
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_events "
                          "WHERE type='resource_cleanup_reconciled'").fetchone()[0] == 2
        assert db.execute('SELECT outcome FROM delivery_runs WHERE run_id=?',
                          (request['run_id'],)).fetchone()[0] == 'blocked'


def test_lost_tracker_acknowledgement_reads_back_release_before_one_successor(later_cleanup):
    store, _request, _historical, _calls, external = later_cleanup
    external.update(ready=True, lost_ack=True)
    assert retry.retry_once(store) == []
    assert external['external_sets'] == 1
    store._automatic_retry_waits.clear()
    assert len(retry.retry_once(store)) == 1
    assert external['external_sets'] == 1


@pytest.mark.parametrize('changed', ['row', 'claim', 'effect', 'stop'])
def test_publication_readback_rechecks_guard_before_cleanup_effect(later_cleanup, changed):
    store, request, _historical, calls, external = later_cleanup
    external['ready'] = True
    stop = [False]
    def change():
        if changed == 'stop':
            stop[0] = True
            return
        with store._connect() as db:
            if changed == 'row':
                db.execute('UPDATE delivery_runs SET revision=revision+1')
            elif changed == 'claim':
                store.state.release_work(db, request['work_id'],
                                         'external:devflow:' + request['run_id'])
                store.state.claim_work(db, request['work_id'], 'foreign', 'fixture')
            else:
                db.execute('INSERT INTO delivery_effects VALUES (?,?,?,?,?,?,?)',
                           ('raced', request['run_id'], 'tracker', '{}', 'unknown', None, 'now'))
    external['on_publication'] = change
    assert retry.retry_once(store, stopped=lambda: stop[0]) == []
    assert not any(call[0] in {'finalize', 'tracker'} for call in calls)


def test_registered_records_changed_during_finalization_cannot_release_claim(later_cleanup):
    store, _request, _historical, calls, external = later_cleanup
    external['ready'] = True
    def changed(unknown_allowed):
        if not unknown_allowed:
            external['journal'] = 'changed'
    external['on_observe'] = changed
    assert retry.retry_once(store) == []
    assert not any(call[0] == 'tracker' for call in calls)


@pytest.mark.parametrize('changed', ['row', 'claim', 'effect'])
def test_second_publication_observation_rechecks_before_tracker_release(later_cleanup, changed):
    store, request, _historical, calls, external = later_cleanup
    external['ready'] = True
    observations = [0]
    def changed_second():
        observations[0] += 1
        if observations[0] != 2:
            return
        with store._connect() as db:
            if changed == 'row':
                db.execute('UPDATE delivery_runs SET revision=revision+1')
            elif changed == 'claim':
                store.state.release_work(db, request['work_id'],
                                         'external:devflow:' + request['run_id'])
                store.state.claim_work(db, request['work_id'], 'foreign', 'fixture')
            else:
                db.execute('INSERT INTO delivery_effects VALUES (?,?,?,?,?,?,?)',
                           ('raced', request['run_id'], 'tracker', '{}', 'unknown', None, 'now'))
    external['on_publication'] = changed_second
    assert retry.retry_once(store) == []
    assert ('finalize', 'blocked', False) in calls
    assert not any(call[0] == 'tracker' for call in calls)
