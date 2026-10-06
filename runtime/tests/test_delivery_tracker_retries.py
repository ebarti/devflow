from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from temporalio import activity
from temporalio.client import WorkflowHistory
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_store import service as service
from test_delivery_terminal_recovery import ControlledTrackerCheckpoint

from devflow_temporal import delivery_activities
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_config import scope_amended_spec
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture
def clock(monkeypatch):
    now = [datetime(2026, 10, 6, tzinfo=UTC)]
    waits = []

    async def wait(predicate, *, timeout):
        if predicate():
            return
        waits.append(timeout.total_seconds())
        now[0] += timeout
        await asyncio.sleep(0)
        raise TimeoutError

    async def sleep(duration):
        now[0] += duration
        await asyncio.sleep(0)

    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.now', lambda: now[0])
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.wait_condition', wait)
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.sleep', sleep)
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.patched', lambda _: True)
    return now, waits


def spec():
    return {'run_id': 'tracker-fixture', 'provider': 'fake', 'tracker_retry_version': 1,
            'terminal_tracker_version': 1, 'resource_cleanup_version': 1,
            'policy': {'tracker_retry_seconds': 60, 'max_repairs': 2}}


@pytest.mark.asyncio
async def test_initial_transient_readbacks_retry_without_a_role_or_operator(clock):
    controller = DeliveryWorkflow()
    calls = []

    async def execute(name, request, **_):
        calls.append((name, request))
        if name == 'delivery_prepare':
            return {'candidate': {'id': 'unchanged'}}
        if name == 'delivery_tracker_start':
            pending = sum(n == name for n, _ in calls) < 4
            return {'state': 'pending' if pending else 'consistent', 'retryable': pending}
        return {}

    async def iterations(_spec, **_):
        controller.state['outcome'] = 'reached-feature-work'
        return controller.state

    controller._activity = execute
    controller._run_iterations = iterations
    result = await controller.run(spec())
    assert result['outcome'] == 'reached-feature-work'
    assert sum(n == 'delivery_tracker_start' for n, _ in calls) == 4
    assert result['iteration'] == 0
    assert not any(n == 'delivery_role' for n, _ in calls)
    assert clock[1] == [2, 4, 8]


@pytest.mark.asyncio
async def test_terminal_pending_readbacks_retry_after_three_attempts_without_signal(clock):
    controller = DeliveryWorkflow()
    controller.state = {'phase': 'delivered', 'execution_state': 'terminal',
                        'outcome': 'delivered', 'iteration': 0, 'revision': 1,
                        'cleanup': 'none', 'checks': {}}
    calls = []

    async def execute(name, request, **_):
        calls.append((name, request))
        if name == 'delivery_finalize_resources':
            return {'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
                    'resource_cleanup': 'confirmed'}
        if name == 'delivery_terminal_tracker':
            pending = sum(n == name for n, _ in calls) <= 3
            return {'state': 'pending' if pending else 'consistent', 'retryable': pending}
        return {}

    controller._activity = execute
    await controller._project(spec(), 'delivered', 'finished')
    assert controller.state['outcome'] == 'delivered'
    assert controller.state['tracker']['state'] == 'consistent'
    assert sum(n == 'delivery_terminal_tracker' for n, _ in calls) == 4
    assert sum(n == 'delivery_finalize_resources' for n, _ in calls) == 1
    assert not any(r.get('event_type') == 'tracker_retry_required' for _, r in calls)
    assert clock[1] and max(clock[1]) <= 30


@pytest.mark.asyncio
async def test_initial_ownership_conflict_stops_without_retry_or_feature_work(clock):
    controller = DeliveryWorkflow()
    calls = []

    async def execute(name, request, **_):
        calls.append(name)
        if name == 'delivery_prepare':
            return {'candidate': {'id': 'unchanged'}}
        if name == 'delivery_tracker_start':
            return {'state': 'pending', 'retryable': False, 'reason': 'foreign ownership'}
        if name == 'delivery_terminal_tracker':
            return {'state': 'pending', 'retryable': False, 'reason': 'foreign ownership'}
        if name == 'delivery_finalize_resources':
            return {'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
                    'resource_cleanup': 'confirmed'}
        return {}

    controller._activity = execute
    result = await controller.run(spec())
    assert result['outcome'] != 'delivered'
    assert calls.count('delivery_tracker_start') == 1
    assert calls.count('delivery_terminal_tracker') == 1
    assert not any(n == 'delivery_role' for n in calls)
    assert clock[1] == []


@pytest.mark.asyncio
async def test_terminal_conflict_never_projects_a_delivered_outcome(clock):
    controller = DeliveryWorkflow()
    controller.state = {'phase': 'delivered', 'execution_state': 'terminal',
                        'outcome': 'delivered', 'iteration': 0, 'revision': 1,
                        'cleanup': 'none', 'checks': {}}
    projected = []

    async def execute(name, request, **_):
        if name == 'delivery_finalize_resources':
            return {'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
                    'resource_cleanup': 'confirmed'}
        if name == 'delivery_terminal_tracker':
            return {'state': 'pending', 'retryable': False, 'reason': 'foreign owner'}
        projected.append(request)
        return {}

    controller._activity = execute
    await controller._project(spec(), 'delivered', 'finished')
    assert controller.state['outcome'] == 'blocked'
    assert all(p['outcome'] == 'blocked' for p in projected)
    assert clock[1] == []


@pytest.mark.asyncio
async def test_initial_transient_deadline_is_bounded_without_feature_work(clock):
    controller = DeliveryWorkflow()
    controller.state = {'checks': {}, 'revision': 0, 'tracker': None,
                        'phase': 'tracker_start', 'execution_state': 'running',
                        'iteration': 0, 'outcome': None}
    requests = []

    async def execute(name, request, **_):
        if name == 'delivery_tracker_start':
            requests.append(request)
        return {'state': 'pending', 'retryable': True}

    controller._activity = execute
    start = clock[0][0]
    result = await controller._start_tracker(spec())
    assert result['state'] == 'pending'
    assert (clock[0][0] - start).total_seconds() == 60
    assert [r['timeout_seconds'] for r in requests] == [60, 58, 54, 46, 30]


@pytest.mark.asyncio
async def test_cancellation_wakes_initial_readback_without_another_tracker_effect(clock):
    controller = DeliveryWorkflow()
    calls = []

    async def execute(name, request, **_):
        calls.append(name)
        if name == 'delivery_prepare':
            return {'candidate': {'id': 'unchanged'}}
        if name == 'delivery_tracker_start':
            controller.cancel_requested = True
            return {'state': 'pending', 'retryable': True}
        if name == 'delivery_finalize_resources':
            return {'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
                    'resource_cleanup': 'confirmed'}
        if name == 'delivery_terminal_tracker':
            return {'state': 'consistent'}
        return {}

    controller._activity = execute
    result = await controller.run(spec())
    assert result['outcome'] == 'cancelled'
    assert calls.count('delivery_tracker_start') == 1
    assert not any(n == 'delivery_role' for n in calls)
    assert clock[1] == []


@pytest.mark.parametrize('timeout', [0, 59, 3601, True, '600'])
def test_invalid_tracker_deadline_is_rejected_before_admission(service, timeout):
    store, request = service
    store.config.raw['tracker_retry_seconds'] = timeout
    with pytest.raises(ValueError, match='tracker_retry_seconds'):
        store.config.admit(request)


def test_tracker_deadline_defaults_for_new_admissions_and_is_frozen(service):
    store, request = service
    default = store.config.admit(request)
    assert default['tracker_retry_version'] == 1
    assert default['policy']['tracker_retry_seconds'] == 600
    store.config.raw['tracker_retry_seconds'] = 900
    admitted = store.config.admit(request)
    assert admitted['tracker_retry_version'] == 1
    assert admitted['policy']['tracker_retry_seconds'] == 900


@pytest.mark.parametrize('legacy', [False, True])
def test_scope_amendment_preserves_original_tracker_retry_policy(service, legacy):
    store, request = service
    store.submit(request)
    original = store.spec(request['run_id'])
    if legacy:
        original.pop('tracker_retry_version')
        original['policy'].pop('tracker_retry_seconds')
        original['policy_digest'] = digest(original['policy'])
    amended = json.loads(store.config.path.read_text())
    amended['repositories']['fixture']['allowed_paths'].append('tests/extra.py')
    path = store.config.state_root / 'amendment.json'
    path.write_text(json.dumps(amended))
    path.chmod(0o600)
    effective = scope_amended_spec(original, path,
                                  hashlib.sha256(path.read_bytes()).hexdigest(),
                                  ['tests/extra.py'])
    assert effective.get('tracker_retry_version') == original.get('tracker_retry_version')
    assert ('tracker_retry_version' in effective) == ('tracker_retry_version' in original)
    assert effective['policy'].get('tracker_retry_seconds') == (
        original['policy'].get('tracker_retry_seconds')
    )
    assert effective['policy_digest'] == digest(effective['policy'])


@pytest.mark.parametrize('error_type,retryable', [
    ('GitHubTransientError', True), ('TimeoutExpired', True),
    ('ValueError', False), ('RuntimeError', False), ('OSError', False),
])
def test_tracker_activity_preserves_typed_helper_error(monkeypatch, tmp_path,
                                                       error_type, retryable):
    store = SimpleNamespace(config=SimpleNamespace(
        raw={'repositories': {'fixture': {'project_url': 'https://example.invalid/project',
                                         'assignee': 'owner'}}},
        helpers_dir=tmp_path, tracking_db=tmp_path / 'fixture.sqlite3'))
    monkeypatch.setattr(delivery_activities, '_context', lambda _spec: (store, None))
    monkeypatch.setattr(delivery_activities.subprocess, 'run', lambda *_, **__: SimpleNamespace(
        returncode=1, stdout='', stderr=json.dumps({'error': 'fixture', 'error_type': error_type})))
    result = delivery_activities._tracker_sync(
        {'repository_key': 'fixture', 'run_id': 'fixture', 'work_id': 'work'},
        'in-progress', release=False)
    assert result.get('retryable') is retryable


@pytest.mark.parametrize('diagnostic,transient', [
    ('HTTP 503: Service unavailable', True), ('HTTP 429: Too Many Requests', True),
    ('dial tcp: connection refused', True), ('HTTP 403: Resource not accessible', False),
    ('HTTP 404: Not Found', False), ('unknown gh failure', False),
])
def test_helper_classifies_only_known_transport_errors(monkeypatch, diagnostic, transient):
    root = Path(__file__).resolve().parents[2] / 'skills' / 'devflow' / 'scripts'
    monkeypatch.syspath_prepend(str(root))
    module_spec = importlib.util.spec_from_file_location(
        'tracker_github_fixture', root / 'github.py',
    )
    helper = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(helper)
    monkeypatch.setattr(helper.subprocess, 'run', lambda *_, **__: subprocess.CompletedProcess(
        ['gh'], 1, '', diagnostic))
    with pytest.raises(RuntimeError) as caught:
        helper.gh('issue', 'view', 'example/fixture')
    assert (type(caught.value).__name__ == 'GitHubTransientError') is transient


@pytest.mark.asyncio
async def test_actual_previous_history_replays_unchanged():
    history = Path(__file__).parent / 'fixtures' / 'intake-required-history.json'
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
        WorkflowHistory.from_json('prior-intake', history.read_text()))


@pytest.mark.asyncio
async def test_actual_previous_terminal_history_replays_unchanged():
    history = Path(__file__).parent / 'fixtures' / 'tracker' / 'c04-terminal-previous-history.json'
    await Replayer(workflows=[ControlledTrackerCheckpoint],
                   workflow_runner=UnsandboxedWorkflowRunner()).replay_workflow(
        WorkflowHistory.from_json('prior-terminal', history.read_text()))


@pytest.mark.asyncio
async def test_real_terminal_retry_survives_worker_replacement_without_operator(
    native_configuration, tmp_path,
):
    config, request = native_configuration
    config.raw['tracker_retry_seconds'] = 60
    config.path.write_text(json.dumps(config.raw))
    store = create_app(config.path).state.delivery.store
    store.submit(request)
    admitted = store.spec(request['run_id'])
    calls = []

    @activity.defn(name='delivery_finalize_resources')
    async def cleanup(_request):
        calls.append('cleanup')
        return {'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
                'resource_cleanup': 'confirmed'}

    @activity.defn(name='delivery_terminal_tracker')
    async def tracker(_request):
        calls.append('tracker')
        pending = calls.count('tracker') <= 3
        return {'state': 'pending' if pending else 'consistent', 'retryable': pending}

    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which('temporal'),
    ) as environment:
        options = dict(task_queue='automatic-tracker', workflows=[ControlledTrackerCheckpoint],
                       workflow_runner=UnsandboxedWorkflowRunner(),
                       activities=[delivery_activities.delivery_project, cleanup, tracker])
        async with Worker(environment.client, **options):
            handle = await environment.client.start_workflow(
                ControlledTrackerCheckpoint.run, args=[admitted, 'delivered'],
                id=f"delivery-{admitted['run_id']}", task_queue='automatic-tracker',
            )
            async with asyncio.timeout(15):
                while calls.count('tracker') < 3:
                    await asyncio.sleep(0.05)
            assert (await handle.describe()).close_time is None
        async with Worker(environment.client, **options):
            result = await asyncio.wait_for(handle.result(), 40)
        assert result['outcome'] == 'delivered' and result['tracker']['state'] == 'consistent'
        assert result['iteration'] == 2
        assert calls == ['cleanup', 'tracker', 'tracker', 'tracker', 'tracker']
        assert not any(
            e['type'] == 'tracker_retry_required' for e in store.events(request['run_id'])
        )
        history = await handle.fetch_history()
        (tmp_path / 'automatic-tracker-history.json').write_text(history.to_json())
