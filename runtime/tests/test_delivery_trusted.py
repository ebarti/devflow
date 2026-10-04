"""Trusted host permission selection and truthful terminal tracker projection."""
from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import pytest
from agent_runtime_kit import FilesystemAccess, PermissionMode
from agent_runtime_kit.adapters.codex import _approval_mode, _task_sandbox
from openai_codex import ApprovalMode, Sandbox
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration

from devflow_temporal.delivery_activities import delivery_terminal_tracker
from devflow_temporal.delivery_sandbox import native_check_argv
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow
from devflow_temporal.role_runner import _task


@pytest.fixture(autouse=True)
def terminal_unit_clock(monkeypatch):
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.now',
                        lambda: datetime(2026, 10, 3, tzinfo=UTC))
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.patched', lambda _name: True)


def test_trusted_mode_uses_supported_noninteractive_sdk_contract(native_configuration):
    config, request = native_configuration
    config.raw['execution_mode'] = 'trusted-local'
    store = DeliveryStore(config)
    store.submit(request)
    spec = store.spec(request['run_id'])
    task = _task({'spec': spec, 'role': 'implement', 'iteration': 0,
                  'candidate': {'id': '0' * 64, 'head': '0' * 40},
                  'workspace': spec['checkout']})
    assert task.permissions.filesystem == FilesystemAccess.FULL_ACCESS
    assert task.permissions.mode == PermissionMode.STRICT
    assert task.permissions.native_profile is None
    assert _task_sandbox(task.permissions, Sandbox) == Sandbox.full_access
    assert _approval_mode(task.permissions.mode, ApprovalMode) == ApprovalMode.deny_all
    assert native_check_argv(spec, 'unused', Path('/tmp'), ['xcrun', '--find', 'clang']) == [
        'xcrun', '--find', 'clang',
    ]
    assert spec['terminal_tracker_version'] == 1
    constrained = deepcopy(spec)
    constrained['policy']['host_sandbox'] = 'native-profile'
    assert native_check_argv(constrained, 'devflow-check', Path('/tmp'), ['true'])[:3] == [
        spec['policy']['codex_bin'], 'sandbox', '-P',
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize('outcome', ['blocked', 'cancelled', 'delivered'])
@pytest.mark.parametrize('confirmed', [True, False])
async def test_terminal_sync_replaces_stale_tracker_after_cleanup(outcome, confirmed):
    controller = DeliveryWorkflow()
    controller.state = {
        'phase': outcome, 'execution_state': 'blocked', 'outcome': outcome,
        'iteration': 2, 'revision': 13, 'cleanup': 'none', 'checks': {},
        'tracker': {'state': 'consistent', 'desired': 'in-progress; claim retained',
                    'readback_at': 'stale'},
    }
    calls = []

    async def execute(name, request, **_kwargs):
        calls.append((name, request))
        if name == 'delivery_finalize_resources':
            return {'state': 'confirmed' if confirmed else 'unknown',
                    'process_cleanup': 'observed-native-confirmed' if confirmed else 'unknown',
                    'resource_cleanup': 'confirmed' if confirmed else 'unknown'}
        if name == 'delivery_terminal_tracker':
            return {'state': 'consistent', 'pending': False, 'readback_at': 'fresh',
                    'desired': request['status'], 'release': request['release']}
        return {}

    controller._activity = execute
    await controller._project({'resource_cleanup_version': 1, 'terminal_tracker_version': 1},
                              outcome, 'terminal')
    assert [name for name, _ in calls] == [
        'delivery_finalize_resources', 'delivery_terminal_tracker', 'delivery_project',
    ]
    tracker = calls[-1][1]['tracker']
    assert tracker['readback_at'] == 'fresh'
    assert tracker['release'] == confirmed
    expected = 'in-review' if outcome == 'delivered' and confirmed else 'blocked'
    assert tracker['desired'] == expected
    assert calls[-1][1]['cleanup'] == ('confirmed' if confirmed else 'unknown')


@pytest.mark.asyncio
@pytest.mark.parametrize('new_history', [False, True])
async def test_cleanup_patch_keeps_legacy_activity_order_and_updates_only_new_projection(
    monkeypatch, new_history,
):
    controller = DeliveryWorkflow()
    controller.state = {'phase': 'blocked', 'execution_state': 'blocked', 'outcome': 'blocked',
                        'iteration': 0, 'revision': 1, 'cleanup': 'none', 'checks': {}}
    calls = []

    async def execute(name, request, **_kwargs):
        calls.append(name)
        if name == 'delivery_finalize_resources':
            return {'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
                    'resource_cleanup': 'confirmed'}
        assert request['cleanup'] == ('confirmed' if new_history else 'none')
        return {}

    controller._activity = execute
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.patched',
                        lambda _name: new_history)
    await controller._project({'resource_cleanup_version': 1}, 'blocked', 'final')
    assert calls == ['delivery_finalize_resources', 'delivery_project']


@pytest.mark.asyncio
async def test_terminal_tracker_failure_is_pending_not_previous_consistent(monkeypatch):
    def fail(*_args, **_kwargs):
        raise TimeoutError('unavailable')

    monkeypatch.setattr('devflow_temporal.delivery_activities._tracker_sync', fail)
    result = await delivery_terminal_tracker({'spec': {'provider': 'codex'},
                                             'status': 'blocked', 'release': True})
    assert result['state'] == 'pending'
    assert result['pending'] is True
    assert result['readback_at']
    assert result['desired'] == 'blocked; claim released'


@pytest.mark.skipif(__import__('sys').platform != 'darwin', reason='actual macOS host required')
def test_trusted_preparation_and_broker_check_compile_with_discovered_sdk(
    native_configuration,
):
    import json
    import sys

    from devflow_temporal.delivery_broker import DeliveryBroker
    from devflow_temporal.delivery_preparation import prepare_authority
    from devflow_temporal.delivery_resources import RunResources

    config, request = native_configuration
    config.raw['execution_mode'] = 'trusted-local'
    command = [str(Path(sys.executable).resolve()), '-c',
               "import os,pathlib,subprocess; "
               "sdk=subprocess.check_output(['xcrun','--show-sdk-path'],text=True).strip(); "
               "assert pathlib.Path(sdk).is_dir(); "
               "p=pathlib.Path(os.environ['TMPDIR']); "
               "(p/'probe.c').write_text('int main(void){return 0;}'); "
               "subprocess.run(['xcrun','clang',str(p/'probe.c'),'-o',str(p/'probe')],check=True); "
               "subprocess.run([str(p/'probe')],check=True); print('2 passed')"]
    check = {**config.raw['repositories']['fixture']['checks'][0], 'argv': command}
    config.raw['repositories']['fixture']['checks'] = [check]
    config.path.write_text(json.dumps(config.raw))
    store = DeliveryStore(config)
    store.submit(request)
    spec = prepare_authority(store, store.spec(request['run_id']))
    assert spec['policy']['native_identity']['execution_mode'] == 'trusted-local'
    broker = DeliveryBroker(store, spec)
    try:
        candidate = broker.prepare()['candidate']
        result = broker.run_checks(0, candidate)
        assert result['state'] == 'passed', result
        assert result['results'][0]['test_count'] == 2
    finally:
        receipt = RunResources(spec).finalize('cancelled')
    assert receipt['resource_cleanup'] == 'confirmed'


@pytest.mark.asyncio
async def test_transient_final_readback_finishes_original_transition(monkeypatch):
    controller = DeliveryWorkflow()
    controller.state = {'phase': 'delivered', 'execution_state': 'terminal', 'outcome': 'delivered',
                        'iteration': 0, 'revision': 1, 'cleanup': 'none', 'checks': {}}
    requested = []
    projections = []

    async def no_wait(_duration):
        return None

    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.sleep', no_wait)

    async def execute(name, request, **_kwargs):
        if name == 'delivery_finalize_resources':
            return {'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
                    'resource_cleanup': 'confirmed'}
        if name == 'delivery_terminal_tracker':
            requested.append(request['status'])
            return {'state': 'pending' if len(requested) == 1 else 'consistent',
                    'pending': len(requested) == 1, 'desired': request['status']}
        projections.append(request)
        return {}

    controller._activity = execute
    await controller._project({'resource_cleanup_version': 1, 'terminal_tracker_version': 1},
                              'delivered', 'final')
    assert requested == ['in-review', 'in-review']
    assert projections[0]['outcome'] is None
    assert projections[0]['phase'] == 'waiting_tracker'
    assert projections[-1]['outcome'] == 'delivered'
    assert projections[-1]['error'] is None


@pytest.mark.parametrize('status', ['blocked', 'in-review'])
def test_terminal_tracker_uses_actual_helper_ack_before_release(intake_fixture, monkeypatch,
                                                              status):
    import contextlib
    import importlib.util
    import io
    import json
    import subprocess
    import sys

    from devflow_temporal.delivery_activities import _tracker_sync
    from devflow_temporal.delivery_config import DeliveryConfig

    path, request = intake_fixture
    config = DeliveryConfig.load(path)
    repository = config.raw['repositories']['fixture']
    repository.update(project_url='https://github.com/users/example/projects/1',
                      assignee='example')
    store = DeliveryStore(config)
    store.submit(request)
    spec = store.spec(request['run_id'])
    monkeypatch.setattr('devflow_temporal.delivery_activities._context',
                        lambda _spec: (store, None))
    monkeypatch.setitem(sys.modules, 'state', store.state)
    helper_spec = importlib.util.spec_from_file_location('github', config.helpers_dir / 'github.py')
    helper = importlib.util.module_from_spec(helper_spec)
    monkeypatch.setitem(sys.modules, 'github', helper)
    helper_spec.loader.exec_module(helper)
    reconciliation = importlib.util.spec_from_file_location('reconcile',
                                                          config.helpers_dir / 'reconcile.py')
    module = importlib.util.module_from_spec(reconciliation)
    monkeypatch.setitem(sys.modules, 'reconcile', module)
    reconciliation.loader.exec_module(module)
    selected = {'host': 'github.com', 'url': repository['project_url'], 'id': 'project',
                'field': 'field', 'option': 'option',
                'status': 'Blocked' if status == 'blocked' else 'In review'}
    item = {'id': 'item', 'project': {'id': 'project'},
            'fieldValueByName': {'optionId': 'option', 'name': selected['status']}}
    monkeypatch.setattr(helper, 'project', lambda *_args: selected)
    monkeypatch.setattr(helper, 'view', lambda issue: {
        'id': 'issue', 'url': issue, 'title': 'Fixture', 'state': 'OPEN',
        'assignees': [{'login': 'example'}],
    })
    monkeypatch.setattr(helper, 'project_item', lambda *_args: item)
    monkeypatch.setattr(helper, 'legacy_project_item', lambda *_args: item)
    calls = []
    fail_audit = True

    def owning_cli(command, **_kwargs):
        nonlocal fail_audit
        calls.append(command)
        if 'audit' in command and fail_audit:
            fail_audit = False
            raise subprocess.TimeoutExpired(command, 120)
        output, error = io.StringIO(), io.StringIO()
        monkeypatch.setattr(sys, 'argv', command[1:])
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(error):
            code = helper.main()
        return subprocess.CompletedProcess(command, code, output.getvalue(),
                                           error.getvalue())

    monkeypatch.setattr('devflow_temporal.delivery_activities.subprocess.run', owning_cli)
    result = _tracker_sync(spec, status, release=True, terminal=True,
                           reason='Observed gate failure')
    assert result['state'] == 'consistent', result
    assert len(calls) == 1 and '--release' in calls[0]
    if status == 'blocked':
        assert '--reason' in calls[0]
    with store._connect() as db:
        assert store.state.claim_for(db, spec['work_id']) is None
        intent = db.execute('SELECT * FROM reconcile_intents').fetchone()
        assert intent['state'] == 'acknowledged'
        assert json.loads(intent['payload'])['status'] == status
        work = store.state.row(db, 'works', spec['work_id'])
        assert json.loads(work['details'])['github']['sync']['status'] == status
    with pytest.raises(subprocess.TimeoutExpired):
        _tracker_sync(spec, status, release=True, terminal=True)
    observed = _tracker_sync(spec, status, release=True, terminal=True)
    assert observed['state'] == 'consistent'
    assert observed['readback_at'] >= result['readback_at']
    assert ['set' if 'set' in call else 'audit' for call in calls] == ['set', 'audit', 'audit']

    def wrong_issue_audit(command, **kwargs):
        result = owning_cli(command, **kwargs)
        data = json.loads(result.stdout)
        data['issue'] = spec['issue_url'].rsplit('/', 1)[0] + '/999'
        return subprocess.CompletedProcess(command, result.returncode,
                                           json.dumps(data), result.stderr)

    monkeypatch.setattr('devflow_temporal.delivery_activities.subprocess.run', wrong_issue_audit)
    assert _tracker_sync(spec, status, release=True, terminal=True)['state'] == 'pending'
    monkeypatch.setattr('devflow_temporal.delivery_activities.subprocess.run', owning_cli)
    count = len(calls)
    with store._connect() as db:
        store.state.update(db, 'work', {'id': spec['work_id'],
                          'issue': spec['issue_url'].rsplit('/', 1)[0] + '/999'}, None)
    conflicted = _tracker_sync(spec, status, release=True, terminal=True)
    assert conflicted['state'] == 'pending' and 'frozen authority' in conflicted['reason']
    assert len(calls) == count  # No set/audit can follow the replacement issue.


def test_real_pytest_numbered_fixture_artifacts_survive_navigation_links(tmp_path):
    import subprocess
    import sys

    from devflow_temporal.delivery_check_evidence import retain_artifacts, verify_manifest

    script = tmp_path / 'test_real_artifacts.py'
    script.write_text(
        'import pytest\n@pytest.mark.parametrize("case",range(8))\n'
        'def test_case(case,tmp_path,tmp_path_factory):\n'
        ' root=tmp_path_factory.mktemp("pagination-boundaries")\n'
        ' for index in range(6):\n'
        '  (root/f"{index}.pdf").write_bytes(b"synthetic PDF")\n'
        '  (tmp_path/f"{index}.png").write_bytes(b"synthetic page PNG")\n'
        '  (root/f"{index}.html").write_text("<p>synthetic source</p>")\n'
        '  (root/f"{index}-bbox.html").write_text("<p>synthetic bbox</p>")\n'
        ' (root/"measurements.json").write_text("{}")\n'
    )
    folder = tmp_path / 'check'
    folder.mkdir()
    subprocess.run([sys.executable, '-m', 'pytest', str(script), '-q',
                    '--basetemp', str(folder / 'pytest-artifacts')],
                   check=True, capture_output=True)
    assert any(path.is_symlink() for path in (folder / 'pytest-artifacts').rglob('*'))
    ref = retain_artifacts(folder, {'id': 'candidate'})
    manifest = verify_manifest(ref, 'candidate', tmp_path)
    assert manifest['count'] == 200
    assert sum(item['relative_path'].endswith('.pdf') for item in manifest['artifacts']) == 48
    assert sum(item['relative_path'].endswith('.png') for item in manifest['artifacts']) == 48
    assert sum(item['relative_path'].endswith('-bbox.html') for item in manifest['artifacts']) == 48
    assert sum(item['relative_path'].endswith('.html') for item in manifest['artifacts']) == 96
    assert sum(
        item['relative_path'].endswith('measurements.json') for item in manifest['artifacts']
    ) == 8
    import shutil

    shutil.rmtree(folder / 'pytest-artifacts')  # Only this disposable fixture's transient outputs.
    assert verify_manifest(ref, 'candidate', tmp_path) == manifest


def test_retained_artifact_manifest_rejects_mutation_and_links(tmp_path):
    from devflow_temporal.delivery_check_evidence import retain_artifacts, verify_manifest

    source = tmp_path / 'pytest-artifacts' / 'trial'
    source.mkdir(parents=True)
    (source / 'case.pdf').write_bytes(b'%PDF-1.7 synthetic output')
    (source / 'page.png').write_bytes(b'controlled PNG bytes')
    ref = retain_artifacts(tmp_path, {'id': 'candidate'})
    manifest = verify_manifest(ref, 'candidate', tmp_path)
    assert manifest['count'] == 2
    assert {Path(item['path']).suffix for item in manifest['artifacts']} == {'.pdf', '.png'}
    Path(manifest['artifacts'][0]['path']).write_bytes(b'altered')
    with pytest.raises(ValueError, match='changed'):
        verify_manifest(ref, 'candidate', tmp_path)
    (source / '00-leak.png').symlink_to('/etc/hosts')
    with pytest.raises(ValueError, match='regular file'):
        retain_artifacts(tmp_path, {'id': 'candidate'})


@pytest.mark.skipif(__import__('sys').platform != 'darwin', reason='actual macOS host required')
def test_actual_broker_preserves_binary_evidence_after_resource_finalization(native_configuration):
    import base64
    import hashlib
    import json
    import sys

    from devflow_temporal.delivery_broker import DeliveryBroker
    from devflow_temporal.delivery_preparation import prepare_authority
    from devflow_temporal.delivery_resources import RunResources

    config, request = native_configuration
    config.raw['execution_mode'] = 'trusted-local'
    check = {**config.raw['repositories']['fixture']['checks'][0], 'kind': 'test', 'argv': [
        str(Path(sys.executable).resolve()), '-c',
        "import os,pathlib,shlex; p=pathlib.Path(shlex.split(os.environ['PYTEST_ADDOPTS'])[0]"
        ".split('=',1)[1]); p.mkdir(exist_ok=True); "
        "(p/'trial.pdf').write_bytes(b'%PDF-1.7 Synthetic'); "
        "(p/'page.png').write_bytes(b'controlled png'); print('2 passed')",
    ]}
    config.raw['repositories']['fixture']['checks'] = [check]
    config.path.write_text(json.dumps(config.raw))
    store = DeliveryStore(config)
    store.submit(request)
    spec = prepare_authority(store, store.spec(request['run_id']))
    broker = DeliveryBroker(store, spec)
    candidate = broker.prepare()['candidate']
    result = broker.run_checks(0, candidate)
    assert result['state'] == 'passed'
    reference = result['results'][0]['artifacts']
    assert reference['count'] == 2
    store.project(spec['run_id'], phase='blocked', execution_state='blocked', event_type='blocked',
                  message='controlled evidence fixture', checks={'local': result},
                  iteration=0, outcome='blocked', cleanup='none')
    finalized = RunResources(spec).finalize('blocked')
    assert finalized['resource_cleanup'] == 'confirmed'
    manifest = json.loads(Path(reference['path']).read_bytes())
    assert manifest['count'] == 2
    assert all(Path(item['path']).exists() for item in manifest['artifacts'])
    indexed = store.evidence_index(spec['run_id'])
    pdf = next(item for item in indexed if item['label'] == 'trial.pdf')
    value = store.evidence(spec['run_id'], pdf['id'])
    assert base64.b64decode(value['base64']) == b'%PDF-1.7 Synthetic'
    assert value['sha256'] == hashlib.sha256(b'%PDF-1.7 Synthetic').hexdigest()
    assert value['media_type'] == 'application/pdf'
    assert value['content_url'].endswith('/content')


@pytest.mark.asyncio
async def test_exhausted_tracker_checkpoint_stays_open_and_explicitly_resumes(monkeypatch):
    import asyncio

    from temporalio.exceptions import ApplicationError

    controller = DeliveryWorkflow()
    controller.state = {'phase': 'delivered', 'execution_state': 'terminal', 'outcome': 'delivered',
                        'iteration': 2, 'revision': 13, 'cleanup': 'none', 'checks': {}}
    calls = []
    exhausted = asyncio.Event()

    async def execute(name, request, **_kwargs):
        calls.append(name)
        if name == 'delivery_finalize_resources':
            return {'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
                    'resource_cleanup': 'confirmed'}
        if name == 'delivery_terminal_tracker':
            count = calls.count(name)
            return {'state': 'pending' if count <= 3 else 'consistent',
                    'pending': count <= 3, 'desired': request['status']}
        if request['event_type'] == 'tracker_retry_required':
            exhausted.set()
        return {}

    async def no_wait(_duration):
        return None

    async def condition(predicate, **_kwargs):
        while not predicate():
            await asyncio.sleep(0.001)

    controller._activity = execute
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.sleep', no_wait)
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.wait_condition', condition)
    task = asyncio.create_task(controller._project(
        {'resource_cleanup_version': 1, 'terminal_tracker_version': 1}, 'delivered', 'final',
    ))
    try:
        await asyncio.wait_for(exhausted.wait(), 2)
        assert not task.done() and controller.state['outcome'] is None
        assert controller.state['phase'] == 'waiting_tracker'
        assert calls.count('delivery_terminal_tracker') == 3
        revision = controller.state['revision']
        with pytest.raises(ApplicationError, match='stale'):
            await controller.reconcile_tracker({'expected_revision': revision - 1})
        with pytest.raises(ApplicationError, match='frozen'):
            await controller.cancel({'expected_revision': revision, 'reason': 'Late cancellation'})
        await controller.reconcile_tracker({'expected_revision': revision})
        await asyncio.wait_for(task, 2)
        assert controller.state['outcome'] == 'delivered'
        assert controller.state['checks']['terminal_tracker_checkpoint']['cycles'] == 2
        assert calls.count('delivery_terminal_tracker') == 4
        assert calls.count('delivery_finalize_resources') == 1
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_authorized_repair_discards_only_active_terminal_checkpoint(monkeypatch):
    controller = DeliveryWorkflow()
    checkpoint = {'state': 'confirmed', 'outcome': 'blocked'}
    candidate = {'id': 'same-candidate'}
    previous = {'run_id': 'same-run', 'phase': 'blocked', 'outcome': 'blocked',
                'iteration': 2, 'revision': 13, 'cleanup': 'none',
                'candidate': candidate,
                'roles': [{'role': 'implement', 'session_id': 'same-session'}],
                'checks': {'terminal_tracker_checkpoint': checkpoint, 'local': {'state': 'failed'}}}
    recovery = {'state': previous, 'candidate': candidate, 'session_id': 'same-session',
                'findings': ['Owning gate failure'], 'maximum_iteration': 3,
                'additional_iterations': 1}

    async def condition(predicate, **_kwargs):
        assert predicate()

    async def project(_spec, event, _message):
        if event == 'repair_preflight_started':
            assert 'terminal_tracker_checkpoint' not in controller.state['checks']
            await controller.cancel({'expected_revision': controller.state['revision'],
                                     'reason': 'Cancel the newly authorized repair'})

    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.wait_condition', condition)
    controller._project = project
    result = await controller._resume_repair({'run_id': 'same-run', 'policy': {'max_repairs': 2}},
                                           recovery)
    assert result['outcome'] == 'cancelled' and controller.cancel_requested
    assert previous['checks']['terminal_tracker_checkpoint'] == checkpoint
    assert previous['checks']['local']['state'] == 'failed'
    assert previous['roles'][0]['session_id'] == 'same-session'


def test_controller_selected_qa_effort_does_not_rewrite_historical_policy(native_configuration):
    config, request = native_configuration
    store = DeliveryStore(config)
    store.submit(request)
    spec = store.spec(request['run_id'])
    frozen = deepcopy(spec)
    task = _task({'spec': spec, 'role': 'verify', 'iteration': 4,
                  'candidate': {'id': '0' * 64, 'head': '0' * 40},
                  'workspace': spec['checkout'],
                  'execution_role_policy': {'model': 'gpt-6.1-sol', 'effort': 'high'}})
    assert task.model == 'gpt-6.1-sol' and task.reasoning_effort == 'high'
    assert spec == frozen and spec['policy']['roles']['verify']['effort'] == 'max'
