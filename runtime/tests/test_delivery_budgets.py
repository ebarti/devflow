from __future__ import annotations

import copy
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer
from test_delivery_store import service as service

from devflow_temporal import delivery_policy_recovery, delivery_stopped_resume
from devflow_temporal.contracts import digest
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


def test_configuration_freezes_both_budgets_and_public_policy(service):
    store, request = service
    store = configured(store, max_attempts=3, max_repairs=1)
    spec = store.config.admit(request)
    assert spec['retry_budget_version'] == 1
    assert spec['policy']['max_attempts'] == 3
    assert spec['policy']['max_repairs'] == 1
    assert store.config.public_policy()['max_attempts'] == 3


@pytest.mark.parametrize('maximum', [0, -1, 11, True, '3', 1.5, None])
def test_invalid_issue_budget_is_rejected_before_admission(service, maximum):
    store, request = service
    store = configured(store, max_attempts=maximum)
    with pytest.raises(ValueError, match='max_attempts'):
        store.submit(request)
    assert store.pending_starts() == []


@pytest.mark.parametrize('outcome', ['blocked', 'delivered', 'cancelled'])
@pytest.mark.parametrize('archived', [False, True])
def test_owner_rerun_preserves_terminal_and_archived_history(service, outcome, archived):
    store, request = service
    store = configured(store, max_attempts=1)
    store.submit(request)
    finish(store, request, archived=archived, outcome=outcome)
    original = store.submitted_spec(request['run_id'])
    rerun = next_request(request, 2, issue_url=request['issue_url'].replace('/3', '/003'))
    assert store.submit(rerun)['existing'] is False
    assert store.submitted_spec(request['run_id']) == original
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 2
        assert db.execute('SELECT COUNT(*) FROM delivery_outbox').fetchone()[0] == 2
        assert store.state.claim_for(db, rerun['work_id']) is not None
        assert db.execute('SELECT outcome FROM delivery_runs WHERE run_id=?',
                          (request['run_id'],)).fetchone()[0] == outcome


@pytest.mark.parametrize('later', [5, None])
def test_owner_rerun_uses_current_policy_without_changing_original(service, later):
    store, request = service
    store = configured(store, max_attempts=2)
    store.submit(request)
    original = store.submitted_spec(request['run_id'])
    finish(store, request)
    raw = copy.deepcopy(store.config.raw)
    if later is None:
        raw.pop('max_attempts')
    else:
        raw['max_attempts'] = later
    store.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(store.config.path))
    second = next_request(request, 2)
    store.submit(second)
    spec = store.submitted_spec(second['run_id'])
    assert spec['retry_budget_version'] == 1
    assert spec['policy']['max_attempts'] == (3 if later is None else later)
    finish(store, second)
    assert store.submit(next_request(request, 3))['existing'] is False
    assert store.submitted_spec(request['run_id']) == original


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


def test_command_replays_and_different_issues_do_not_spend_another_attempt(service):
    store, request = service
    store = configured(store, max_attempts=1)
    first = store.submit(request)
    assert store.submit(request) == first
    assert store.submit({**request, 'command_id': 'replayed-command'})['existing'] is True
    other = next_request(request, 2, issue_url=request['issue_url'].replace('/3', '/4'))
    assert store.submit(other)['existing'] is False


def test_owner_rerun_preserves_legacy_admissions(service, monkeypatch):
    store, request = service
    # Retained pre-budget input, rather than a new admission under today's defaults.
    legacy = store.config.admit(request)
    legacy.pop('automatic_retry_version', None)
    legacy.pop('retry_budget_version', None)
    legacy['policy'].pop('max_attempts', None)
    legacy['policy_digest'] = digest(legacy['policy'])
    with monkeypatch.context() as historical:
        historical.setattr(DeliveryConfig, 'admit', lambda *_args: copy.deepcopy(legacy))
        store.submit(request)
    assert 'retry_budget_version' not in store.submitted_spec(request['run_id'])
    original = store.submitted_spec(request['run_id'])
    finish(store, request, archived=True)
    store = configured(store, max_attempts=1)
    assert store.submit(next_request(request, 2))['existing'] is False
    assert store.submitted_spec(request['run_id']) == original


@pytest.mark.parametrize('archived', [False, True])
def test_internal_automatic_admission_cannot_bypass_exhausted_budget(service, archived):
    store, request = service
    store = configured(store, max_attempts=1)
    store.submit(request)
    finish(store, request, archived=archived)
    successor = next_request(request, 2)
    with store._connect() as db:
        predecessor = dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                                      (request['run_id'],)).fetchone())
    with pytest.raises(ValueError, match='issue attempt budget'):
        store.submit(successor, _automatic={'row': predecessor})
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1
        assert db.execute('SELECT COUNT(*) FROM delivery_outbox').fetchone()[0] == 1
        assert store.state.row(db, 'works', successor['work_id']) is None


def test_reloading_configuration_does_not_extend_an_admitted_repair_limit(service):
    store, request = service
    store = configured(store, max_attempts=3, max_repairs=1)
    store.submit(request)
    original = store.submitted_spec(request['run_id'])
    store = configured(store, max_attempts=5, max_repairs=3)
    assert store.submitted_spec(request['run_id']) == original
    with pytest.raises(ValueError, match='finite turn limit'):
        validate_native_turn(original, 'implement', 2, store)


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


def test_new_budget_rejects_operator_extensions_before_runtime_effects(service, monkeypatch):
    store, request = service
    store = configured(store, max_attempts=3, max_repairs=1)
    store.submit(request)

    def unexpected(*args, **kwargs):
        pytest.fail('a budget extension reached stopped runtime preparation')

    monkeypatch.setattr(delivery_stopped_resume, 'snapshot', unexpected)
    command = {'continuation_kind': 'stopped_delivery_resume', 'command_id': 'extend-budget',
               'expected_revision': 1, 'expected_iteration': 1, 'expected_candidate_id': 'a' * 64,
               'expected_candidate_head': 'b' * 40, 'additional_iterations': 2}
    with pytest.raises(ValueError, match='fixed repair budget'):
        store.continue_repair(request['run_id'], command)
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_commands').fetchone()[0] == 1


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
async def test_workflow_cannot_extend_its_frozen_loop_budget(service):
    store, request = service
    store = configured(store, max_attempts=3, max_repairs=1)
    spec = store.config.admit(request)
    flow = DeliveryWorkflow()
    flow.state = {'iteration': 1}
    with pytest.raises(ValueError, match='fixed repair budget'):
        await flow._run_iterations(spec, start_iteration=2, prior_implementer_session=None,
                                   repair_findings=[], continuation=None, recovery=None,
                                   authorized_max_iteration=3)


@pytest.mark.asyncio
async def test_previous_admission_history_keeps_its_original_workflow_path():
    path = Path(__file__).parent / 'fixtures' / 'intake-required-history.json'
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
        WorkflowHistory.from_json('delivery-run-1', path.read_text()))


@pytest.mark.parametrize('entry', ['precheck', 'recover'])
@pytest.mark.parametrize('maximum', [None, 5])
def test_retired_policy_recovery_rejects_calls_without_any_effect(
    service, monkeypatch, entry, maximum,
):
    store, request = service
    if maximum is not None:
        store = configured(store, max_attempts=maximum)
    store.submit(request)
    original = store.submitted_spec(request['run_id'])
    assert original['retry_budget_version'] == 1
    assert original['policy']['max_attempts'] == (3 if maximum is None else maximum)
    # Later config changes cannot erase the admitted marker.
    store.config.raw.pop('max_attempts', None)
    store.config.path.write_text(json.dumps(store.config.raw))
    before_files = {str(path): path.read_bytes() for path in store.config.state_root.rglob('*')
                    if path.is_file()}
    with store._connect() as db:
        before = '\n'.join(db.iterdump())

    def unexpected(*args, **kwargs):
        pytest.fail('policy budget rejection must precede inspection or preparation')

    monkeypatch.setattr(store, 'intake_execution_spec', unexpected)
    monkeypatch.setattr(delivery_policy_recovery, '_lock', unexpected)
    with pytest.raises(AttributeError, match='policy_recovery_precheck|recover_execution'):
        if entry == 'precheck':
            store.policy_recovery_precheck(request['run_id'])
        else:
            store.recover_execution(request['run_id'], {
                'command_id': 'extend-policy-budget', 'additional_iterations': 1,
                'expected_precheck_sha256': 'a' * 64, 'config_path': 'unused.json',
                'config_sha256': 'b' * 64,
            })
    with store._connect() as db:
        assert '\n'.join(db.iterdump()) == before
    assert {str(path): path.read_bytes() for path in store.config.state_root.rglob('*')
            if path.is_file()} == before_files
