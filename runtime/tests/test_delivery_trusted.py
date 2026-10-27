"""Trusted host permission selection and truthful terminal tracker projection."""
from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration

from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow
from devflow_temporal.role_runner import _task


@pytest.fixture(autouse=True)
def terminal_unit_clock(monkeypatch):
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.now',
                        lambda: datetime(2026, 10, 3, tzinfo=UTC))
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.patched', lambda _name: True)


@pytest.mark.asyncio
@pytest.mark.parametrize('outcome', ['blocked', 'delivered'])
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


@pytest.mark.parametrize('role', ['review', 'verify'])
def test_independent_roles_receive_same_candidate_receipts_and_reject_stale_evidence(
    native_configuration, role,
):
    config, request = native_configuration
    config.raw['execution_mode'] = 'trusted-local'
    config.path.write_text(json.dumps(config.raw))
    store = DeliveryStore(config)
    store.submit(request)
    spec = store.spec(request['run_id'])
    evidence = {'state': 'passed', 'candidate_id': '0' * 64, 'results': []}
    body = {'spec': spec, 'role': role, 'iteration': 0,
            'candidate': {'id': '0' * 64, 'head': '0' * 40},
            'workspace': spec['checkout'], 'check_evidence': evidence}
    assert 'Broker local check receipts' in _task(body).goal
    with pytest.raises(ValueError, match='another candidate'):
        _task({**body, 'check_evidence': {**evidence, 'candidate_id': '1' * 64}})
