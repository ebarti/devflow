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

from devflow_temporal import delivery_activities, delivery_broker, delivery_terminal_recovery
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
                # The activity call precedes the durable automatic retry timer.
                while True:
                    history = await handle.fetch_history()
                    if any(
                        event.HasField('timer_started_event_attributes')
                        and event.timer_started_event_attributes.start_to_fire_timeout.seconds == 30
                        for event in history.events
                    ):
                        break
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


def terminal_readback_controller(monkeypatch, tmp_path, failure, *, failures=4, conflict=None):
    admitted = spec()
    admitted.update(provider='codex', branch='feat/fixture', github_repo='example/fixture',
                    origin_url='https://github.com/example/fixture.git')
    head = 'a' * 40
    pr = {'number': 7, 'head': head, 'url': 'https://github.com/example/fixture/pull/7'}
    observed = {'number': 7, 'url': pr['url'], 'state': 'OPEN', 'isDraft': False,
                'baseRefName': 'main', 'headRefName': admitted['branch'], 'headRefOid': head,
                'title': 'fix: owned change'}
    if conflict == 'closed':
        observed['state'] = 'CLOSED'
    elif conflict == 'foreign':
        observed['headRefName'] = 'foreign'
    elif conflict == 'head':
        observed['headRefOid'] = 'b' * 40
    broker = object.__new__(delivery_broker.DeliveryBroker)
    broker.spec, broker.source = admitted, tmp_path
    broker._publication_base_ref = lambda **_: 'main'
    monkeypatch.setattr(delivery_terminal_recovery, 'DeliveryBroker', lambda *_: broker)
    monkeypatch.setattr(delivery_activities, '_context', lambda _: (object(), broker))
    queries, syncs, projected = [], [], []

    def command(argv, **_):
        query = 'gh' if argv[0] == 'gh' else 'git' if 'ls-remote' in argv else 'origin'
        if query != 'origin':
            queries.append(query)
        if query == failure.split('_')[0] and queries.count(query) <= failures:
            if failure.endswith('timeout'):
                raise subprocess.TimeoutExpired(argv, 60)
            return subprocess.CompletedProcess(argv, 1, '', 'fixture query unavailable')
        if query == 'gh':
            values = [observed, observed] if conflict == 'multiple' else [observed]
            return subprocess.CompletedProcess(argv, 0, json.dumps(values), '')
        value = '' if conflict == 'absent' else head + '\trefs/heads/' + admitted['branch']
        output = admitted['origin_url'] if query == 'origin' else value
        return subprocess.CompletedProcess(argv, 0, output, '')

    monkeypatch.setattr(delivery_broker.subprocess, 'run', command)

    def synchronize(_spec, status, *, release, **_):
        syncs.append((status, release))
        return {'state': 'consistent', 'pending': False}

    monkeypatch.setattr(delivery_activities, '_tracker_sync', synchronize)
    controller = DeliveryWorkflow()
    controller.state = {'phase': 'delivered', 'execution_state': 'terminal',
                        'outcome': 'delivered', 'iteration': 0, 'revision': 1,
                        'cleanup': 'none', 'checks': {}, 'candidate': {'head': head},
                        'pull_request': pr}

    async def execute(name, request, **_):
        if name == 'delivery_finalize_resources':
            return {'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
                    'resource_cleanup': 'confirmed'}
        if name == 'delivery_terminal_tracker':
            return await delivery_activities.delivery_terminal_tracker(request)
        projected.append(request)
        return {}

    controller._activity = execute
    return controller, admitted, queries, syncs, projected


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['gh_failure', 'gh_timeout', 'git_failure', 'git_timeout'])
async def test_real_terminal_pr_transport_readback_retries_then_finishes(
    monkeypatch, tmp_path, clock, failure,
):
    controller, admitted, queries, syncs, projected = terminal_readback_controller(
        monkeypatch, tmp_path, failure)
    await controller._project(admitted, 'delivered', 'finished')
    assert controller.state['outcome'] == 'delivered'
    assert controller.state['checks']['terminal_tracker_checkpoint']['state'] == 'confirmed'
    assert queries.count(failure.split('_')[0]) == 5
    assert syncs == [('in-review', True)]
    assert not any(p['event_type'] == 'tracker_deadline' for p in projected)


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['gh_failure', 'git_failure'])
async def test_real_terminal_pr_transport_deadline_stays_recoverable(
    monkeypatch, tmp_path, clock, failure,
):
    controller, admitted, queries, syncs, projected = terminal_readback_controller(
        monkeypatch, tmp_path, failure, failures=100)
    await controller._project(admitted, 'delivered', 'finished')
    assert controller.state['phase'] == 'waiting_tracker'
    assert controller.state['outcome'] is None
    checkpoint = controller.state['checks']['terminal_tracker_checkpoint']
    assert checkpoint['closed'] and checkpoint['state'] == 'pending'
    assert queries.count(failure.split('_')[0]) >= 3
    assert not syncs
    assert projected[-1]['event_type'] == 'tracker_deadline'


@pytest.mark.asyncio
@pytest.mark.parametrize('conflict', ['multiple', 'closed', 'foreign', 'head', 'absent'])
async def test_real_terminal_pr_identity_conflict_never_retries_or_releases(
    monkeypatch, tmp_path, clock, conflict,
):
    controller, admitted, queries, syncs, projected = terminal_readback_controller(
        monkeypatch, tmp_path, 'gh_failure', failures=0, conflict=conflict)
    await controller._project(admitted, 'delivered', 'finished')
    assert controller.state['outcome'] == 'blocked'
    assert controller.state['checks']['terminal_tracker_checkpoint']['state'] == 'conflicted'
    assert queries.count('gh') == 1
    assert not syncs and not clock[1]
    assert all(p['outcome'] != 'delivered' for p in projected)


@pytest.mark.asyncio
@pytest.mark.parametrize('failure,reason', [('gh_failure', 'BrokerReadbackUnavailable'),
                                          ('git_failure', 'RuntimeError')])
async def test_legacy_published_transport_failure_keeps_original_result_shape(
    monkeypatch, tmp_path, failure, reason,
):
    controller, admitted, _, syncs, _ = terminal_readback_controller(
        monkeypatch, tmp_path, failure, failures=100)
    admitted.pop('tracker_retry_version')
    result = await delivery_activities.delivery_terminal_tracker({
        'spec': admitted, 'status': 'in-review', 'release': True,
        'candidate': controller.state['candidate'],
        'pull_request': controller.state['pull_request'],
    })
    assert result['state'] == 'pending' and result['reason'] == reason
    assert 'retryable' not in result and not syncs
