"""Cold read-only callers must not activate a configured runtime or its dispatch loop."""

from __future__ import annotations

import pytest
from test_delivery_startup import config as config

from devflow_temporal import delivery_control as control
from devflow_temporal.delivery_client import DeliveryClient, read_only_client
from devflow_temporal.delivery_config import DeliveryConfig

READ_TOOLS = [
    ('get_service', {}), ('list_runs', {}), ('get_run', {'run_id': 'absent'}),
    ('get_run_detail', {'run_id': 'absent'}),
    ('get_pause_control', {'run_id': 'absent'}),
    ('get_operation', {'operation_id': 'absent'}),
    ('get_run_operations', {'run_id': 'absent'}),
    ('read_evidence', {'run_id': 'absent', 'evidence_id': 'missing'}),
    ('gates_only_preflight', {'run_id': 'absent'}),
    ('repair_admission_preflight', {'run_id': 'absent', 'request_json': '{}'}),
]

RETIRED_COMMANDS = [
    'recovery-preflight', 'recover-execution', 'metadata-preflight',
    'reconcile-published-metadata',
]


@pytest.mark.parametrize('command', [*RETIRED_COMMANDS, 'unknown-command'])
def test_unsupported_public_command_rejects_before_configuration_or_lifecycle(
    tmp_path, monkeypatch, command, capsys,
):
    calls = []
    path = tmp_path / 'absent' / 'service.json'
    for name in (
        '_config', 'api_client', 'read_only_client',
        'ensure_service_running', 'service_start', 'service_stop',
    ):
        monkeypatch.setattr(control, name,
                            lambda *_args, _name=name, **_kwargs: calls.append(_name))
    monkeypatch.setattr(DeliveryConfig, 'load',
                        classmethod(lambda *_args: calls.append('load_config')))
    monkeypatch.setattr('sys.argv', ['devflow-delivery', '--config', str(path), command])
    with pytest.raises(SystemExit) as failure:
        control.main()
    assert failure.value.code == 2
    assert 'invalid choice' in capsys.readouterr().err
    assert calls == []
    assert not path.parent.exists()


def test_warm_read_client_uses_get_without_session_or_start(config, monkeypatch):
    calls = []
    monkeypatch.setattr(control, 'ensure_service_running', lambda _cfg: pytest.fail('started'))

    def request(_caller, method, path):
        calls.append((method, path))
        if path == '/api/runs/run%2Fone/status':
            return {'version': 1, 'source': 'persisted_projection',
                    'run': {'run_id': 'run/one', 'protocol_revision': 7}}
        return {'fixture': path}

    monkeypatch.setattr(DeliveryClient, '_request', request)
    caller = read_only_client(config.path)
    assert caller.service() == {'fixture': '/api/service'}
    caller.runs()
    assert caller.status('run/one') == {
        'version': 1, 'source': 'persisted_projection',
        'run': {'run_id': 'run/one', 'protocol_revision': 7},
    }
    caller.detail('run/one')
    caller.pause_control('run/one')
    caller.operation('operation/one')
    caller.run_operations('run/one')
    caller.evidence('run/one', 'artifact/one')
    assert calls == [
        ('GET', '/api/service'), ('GET', '/api/runs'),
        ('GET', '/api/runs/run%2Fone/status'), ('GET', '/api/runs/run%2Fone'),
        ('GET', '/api/runs/run%2Fone/pause-control'),
        ('GET', '/api/operations/operation%2Fone'), ('GET', '/api/runs/run%2Fone/operations'),
        ('GET', '/api/runs/run%2Fone/evidence/artifact%2Fone'),
    ]
    assert caller.csrf == ''
    assert not config.state_root.exists()
