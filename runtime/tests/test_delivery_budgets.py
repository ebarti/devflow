from __future__ import annotations

import copy
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer
from test_delivery_store import service as service

from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_native_guard import validate_native_turn
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow


def configured(store, **values):
    raw = copy.deepcopy(store.config.raw)
    raw.update(values)
    store.config.path.write_text(json.dumps(raw))
    return DeliveryStore(DeliveryConfig.load(store.config.path))


def next_request(request, number, **values):
    return {**request, 'run_id': f'run-{number}', 'command_id': f'command-{number}',
            'work_id': f'work-{number}', 'branch': f'feat/attempt-{number}', **values}


def finish(store, request, *, archived=False, outcome='blocked'):
    store.project(request['run_id'], phase=outcome, execution_state=outcome,
                  event_type=outcome, message='fixture ended', outcome=outcome,
                  cleanup='confirmed')
    with store._connect() as db:
        store.state.release_work(db, request['work_id'],
                                 'external:devflow:' + request['run_id'])
        if archived:
            db.execute('UPDATE delivery_dashboard_state SET archived=1 WHERE run_id=?',
                       (request['run_id'],))


def test_fourth_explicit_submission_keeps_all_three_original_attempts(service):
    store, request = service
    store = configured(store, max_attempts=3)
    originals = {}
    for number in range(1, 4):
        previous = next_request(request, number)
        store.submit(previous)
        originals[previous['run_id']] = store.submitted_spec(previous['run_id'])
        finish(store, previous, archived=True)
    fourth = next_request(request, 4)
    assert store.submit(fourth)['existing'] is False
    assert store.submit(fourth)['existing'] is False  # same durable command receipt
    for run_id, spec in originals.items():
        assert store.submitted_spec(run_id) == spec
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 4
        assert db.execute('SELECT COUNT(*) FROM delivery_outbox').fetchone()[0] == 4


@pytest.mark.parametrize('archived', [False, True])
def test_internal_automatic_admission_cannot_bypass_exhausted_budget(service, archived):
    store, request = service
    store = configured(store, max_attempts=1)
    store.submit(request)
    finish(store, request, archived=archived)
    successor = next_request(request, 2, work_id=request['work_id'],
                             supersedes_run_id=request['run_id'])
    with store._connect() as db:
        predecessor = dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                                      (request['run_id'],)).fetchone())
        original_work = store.state.row(db, 'works', request['work_id'])
    with pytest.raises(ValueError, match='issue attempt budget'):
        store.submit(successor, _automatic={'row': predecessor})
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1
        assert db.execute('SELECT COUNT(*) FROM delivery_outbox').fetchone()[0] == 1
        assert store.state.row(db, 'works', successor['work_id']) == original_work
        assert store.state.claim_for(db, request['work_id']) is None


def test_admission_race_cannot_create_an_extra_run_or_claim(service):
    store, request = service
    store = configured(store, max_attempts=1)

    def submit(number):
        try:
            return store.submit(next_request(request, number))
        except ValueError as exc:
            return str(exc)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(submit, [2, 3]))
    assert sum(isinstance(result, dict) for result in results) == 1
    assert sum(isinstance(result, str) and 'already claimed' in result
               for result in results) == 1
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1
        assert db.execute('SELECT COUNT(*) FROM delivery_outbox').fetchone()[0] == 1
        assert db.execute('SELECT COUNT(*) FROM claims').fetchone()[0] == 1


def test_native_turn_cannot_use_old_grant_rows_to_extend_a_new_budget(service):
    store, request = service
    store = configured(store, max_attempts=3, max_repairs=1)
    store.submit(request)
    with store._connect() as db:
        db.execute('INSERT INTO delivery_repair_grants VALUES (?,?,?,?,?,?,?,?)',
                   (request['run_id'], 'old-grant', 'old-workflow', 'old-execution',
                    'a' * 64, 2, 8, 'historical'))
    spec = store.submitted_spec(request['run_id'])
    validate_native_turn(spec, 'implement', 1, store)
    with pytest.raises(ValueError, match='finite turn limit'):
        validate_native_turn(spec, 'implement', 2, store)


@pytest.mark.asyncio
async def test_previous_admission_history_keeps_its_original_workflow_path():
    path = Path(__file__).parent / 'fixtures' / 'intake-required-history.json'
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
        WorkflowHistory.from_json('delivery-run-1', path.read_text()))
