"""Trusted host permission selection and truthful terminal tracker projection."""
from __future__ import annotations

from copy import deepcopy
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
