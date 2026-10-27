from __future__ import annotations

import copy
import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from test_delivery_store import service as service

from devflow_temporal import delivery_automatic_retry as retry
from devflow_temporal.delivery_store import DeliveryStore


def stopped(service, monkeypatch):
    store, request = service
    store.submit(request)
    store.mark_start(request["run_id"], accepted=True)
    failure = {'classification': 'transient', 'stage': 'preparing',
               'reason': 'transport unavailable', 'cause_type': 'TimeoutError'}
    checks = {'failure': failure, 'resource_cleanup': {
        'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
        'resource_cleanup': 'confirmed', 'receipt_sha256': 'fixture-finalization'}}
    store.project(request['run_id'], phase='blocked', execution_state='blocked',
                  event_type='blocked', message='retained failure', outcome='blocked',
                  cleanup='confirmed', checks=checks)
    with store._connect() as db:
        store.state.release_work(db, request['work_id'], 'external:devflow:' + request['run_id'])
    spec = store.spec(request['run_id'])
    closed = {'workflow_id': 'delivery-' + request['run_id'], 'execution_run_id': 'execution',
              'closed_at': '2026-10-07T00:00:00+00:00', 'request_digest': spec['request_digest'],
              'recovery_digest': None, 'result': {'run_id': request['run_id'], 'outcome': 'blocked',
              'phase': 'blocked', 'execution_state': 'blocked',
              'cleanup': 'confirmed', 'checks': checks}}
    monkeypatch.setattr(DeliveryStore, '_completed_temporal_result', lambda *_a, **_k: closed)
    monkeypatch.setattr(retry, 'observe_finalized_resources',
                        lambda *_a, **_k: {'finalization_sha256': 'fixture-finalization'})
    monkeypatch.setattr(retry, 'fresh_unpublished_base', lambda *_a: spec['base_sha'])
    return store, request, closed


def test_concurrent_retry_admits_only_one_successor(service, monkeypatch):
    store, request, _ = stopped(service, monkeypatch)
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: retry.retry_once(store), range(2)))
    with store._connect() as db:
        rows = [json.loads(row[0]) for row in db.execute('SELECT request_json FROM delivery_runs')]
        assert len(rows) == 2
        assert sum(row.get('supersedes_run_id') == request['run_id'] for row in rows) == 1


def test_exhausted_budget_never_creates_new_attempt(service, monkeypatch):
    store, request = service
    raw = copy.deepcopy(store.config.raw)
    raw['max_attempts'] = 1
    store.config.path.write_text(json.dumps(raw))
    from devflow_temporal.delivery_config import DeliveryConfig
    store = DeliveryStore(DeliveryConfig.load(store.config.path))
    store, request, _ = stopped((store, request), monkeypatch)
    assert retry.retry_once(store) == []
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1


@pytest.mark.asyncio
async def test_service_loop_admits_closed_transient_successor_without_an_operator(
    service, monkeypatch,
):
    import asyncio

    from devflow_temporal.delivery_api import DeliveryService
    store, request, _ = stopped(service, monkeypatch)
    app = object.__new__(DeliveryService)
    app.store = store
    async def idle():
        pass
    real_sleep = asyncio.sleep
    async def end(_seconds):
        for _ in range(100):
            with store._connect() as db:
                if db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 2:
                    raise asyncio.CancelledError
            await real_sleep(.01)
        raise AssertionError('background retry did not admit its eligible successor')
    monkeypatch.setattr(app, 'dispatch_once', idle)
    monkeypatch.setattr(app, 'dispatch_questions_once', idle)
    monkeypatch.setattr(asyncio, 'sleep', end)
    with pytest.raises(asyncio.CancelledError):
        await app.dispatch_loop()
    with store._connect() as db:
        rows = [json.loads(row[0]) for row in db.execute('SELECT request_json FROM delivery_runs')]
        assert len(rows) == 2
        assert rows[1]['supersedes_run_id'] == request['run_id']


def test_actual_cleanup_must_match_closed_controller_finalization_hash(service, monkeypatch):
    store, request, _ = stopped(service, monkeypatch)
    monkeypatch.setattr(retry, 'observe_finalized_resources',
                        lambda *_a: {'finalization_sha256': 'changed-finalization'})
    assert retry.retry_once(store) == []
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1


@pytest.mark.parametrize('change', ['newer_issue', 'effect_pending'])
def test_transaction_rechecks_late_issue_and_operator_effects(service, monkeypatch, change):
    store, request, _ = stopped(service, monkeypatch)
    def race(_store, spec):
        if change == 'newer_issue':
            store.submit({**request, 'command_id': 'new', 'run_id': 'new', 'branch': 'fix/new'})
            with store._connect() as db:
                store.state.release_work(db, request['work_id'], 'external:devflow:new')
        else:
            with store._connect() as db:
                if change == 'mutation_unknown':
                    db.execute('INSERT INTO delivery_mutations '
                               '(command_id,run_id,kind,request_digest,state) '
                               "VALUES ('cancel',?,'cancel','digest','unknown')",
                               (request['run_id'],))
                else:
                    db.execute('INSERT INTO delivery_effects VALUES (?,?,?,?,?,?,?)',
                               ('late', request['run_id'], 'tracker', '{}', 'pending', None, 'now'))
        return spec['base_sha']
    monkeypatch.setattr(retry, 'fresh_unpublished_base', race)
    assert retry.retry_once(store) == []
    with store._connect() as db:
        assert not any(json.loads(row[0]).get('supersedes_run_id') == request['run_id']
                       for row in db.execute('SELECT request_json FROM delivery_runs'))


def test_automatic_generations_spend_one_original_total_ceiling(service, monkeypatch):
    store, request, closed = stopped(service, monkeypatch)
    closures = {request['run_id']: closed}
    monkeypatch.setattr(DeliveryStore, '_completed_temporal_result',
                        lambda _store, run_id, **_k: closures[run_id])
    current = request['run_id']
    for _ in range(2):
        admitted = retry.retry_once(store)
        assert len(admitted) == 1
        current = admitted[0]['run_id']
        spec = store.spec(current)
        store.mark_start(current, accepted=True)
        store.project(current, phase='blocked', execution_state='blocked', outcome='blocked',
                      event_type='blocked', message='retained transient', cleanup='confirmed',
                      checks=closed['result']['checks'])
        with store._connect() as db:
            store.state.release_work(db, spec['work_id'], 'external:devflow:' + current)
        closures[current] = {**closed, 'workflow_id': 'delivery-' + current,
                             'request_digest': spec['request_digest'],
                             'result': {**closed['result'], 'run_id': current}}
    assert retry.retry_once(store) == []
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 3
