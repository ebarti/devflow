"""Cold read-only callers must not activate a configured runtime or its dispatch loop."""

from __future__ import annotations

import argparse
import asyncio
import json
import socket
import threading
import time

import pytest
import uvicorn
from mcp.shared.memory import create_connected_server_and_client_session
from test_delivery_startup import config as config

from devflow_temporal import delivery_control as control
from devflow_temporal.delivery_api import DeliveryService, create_app
from devflow_temporal.delivery_client import DeliveryClient, ServiceUnavailable, read_only_client
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_mcp import build_server

READ_TOOLS = [
    ('get_service', {}), ('list_runs', {}), ('get_run', {'run_id': 'absent'}),
    ('read_evidence', {'run_id': 'absent', 'evidence_id': 'missing'}),
    ('recovery_preflight', {'run_id': 'absent'}),
    ('gates_only_preflight', {'run_id': 'absent'}),
    ('metadata_preflight', {'run_id': 'absent', 'request_json': '{}'}),
    ('repair_admission_preflight', {'run_id': 'absent', 'request_json': '{}'}),
]


@pytest.mark.asyncio
@pytest.mark.parametrize('tool,arguments', READ_TOOLS)
async def test_cold_read_mcp_does_not_start_api_or_dispatch(config, monkeypatch, tool, arguments):
    socket_fixture = socket.socket()
    socket_fixture.bind(('127.0.0.1', 0))
    port = socket_fixture.getsockname()[1]
    config.path.write_text(json.dumps({**config.raw, 'dashboard_url': f'http://127.0.0.1:{port}'}))
    configured = DeliveryConfig.load(config.path)
    calls = []
    servers = []
    real_request = DeliveryClient._request

    def bounded_request(caller, method, path, body=None, *, timeout=30):
        return real_request(caller, method, path, body, timeout=min(timeout, .2))

    monkeypatch.setattr(DeliveryClient, '_request', bounded_request)

    async def dispatch_once(_service):
        calls.append('dispatch')

    async def no_question_effect(_service):
        return None

    async def health(_service):
        _service.temporal_status = 'connected'

    monkeypatch.setattr(DeliveryService, 'dispatch_once', dispatch_once)
    monkeypatch.setattr(DeliveryService, 'dispatch_questions_once', no_question_effect)
    monkeypatch.setattr(DeliveryService, 'healthy_client', health)

    def start(current, _deadline):
        calls.append('start')
        socket_fixture.close()
        # Real temporary API lifespan starts its real background dispatch loop.
        api = create_app(current.path)
        server = uvicorn.Server(uvicorn.Config(
            api, host='127.0.0.1', port=port, log_level='error'))
        thread = threading.Thread(target=server.run)
        servers.append((server, thread))
        thread.start()
        deadline = time.monotonic() + 3
        while not server.started:
            assert time.monotonic() < deadline
            time.sleep(.01)
        return {'dashboard_url': current.dashboard_url, 'processes': {}}

    monkeypatch.setattr(control, '_start', start)
    try:
        server = build_server(configured.path)
        async with create_connected_server_and_client_session(server) as session:
            result = await session.call_tool(tool, arguments)
        assert calls == []  # Neither the startup path nor background dispatch was activated.
        assert result.isError
        assert 'local service request failed' in result.content[0].text
        assert not configured.state_root.exists()
    finally:
        socket_fixture.close()
        for server, thread in servers:
            server.should_exit = True
            await asyncio.to_thread(thread.join, 3)
            assert not thread.is_alive()


@pytest.mark.parametrize('command', [
    'runs', 'run', 'evidence', 'recovery-preflight', 'gates-only-preflight',
    'metadata-preflight', 'repair-admission-preflight',
])
def test_cold_cli_reads_report_unavailable_without_start(config, monkeypatch, command, capsys):
    calls = []
    monkeypatch.setattr(control, 'ensure_service_running', lambda _cfg: calls.append('start'))

    def unavailable(_caller, *_args, **_kwargs):
        raise ServiceUnavailable('fixture service not listening')

    monkeypatch.setattr(DeliveryClient, '_request', unavailable)
    body = config.path.parent / 'preflight.json'
    body.write_text('{}')
    args = argparse.Namespace(command=command, config=str(config.path), id='absent',
                              evidence_id='missing', request=body)
    with pytest.raises(SystemExit) as failure:
        control._run(args, argparse.ArgumentParser())
    assert failure.value.code == 1
    assert capsys.readouterr().err.endswith("fixture service not listening\n")
    assert calls == []
    assert not config.state_root.exists()


def test_warm_read_client_uses_get_without_session_or_start(config, monkeypatch):
    calls = []
    monkeypatch.setattr(control, 'ensure_service_running', lambda _cfg: pytest.fail('started'))

    def request(_caller, method, path):
        calls.append((method, path))
        return {'fixture': path}

    monkeypatch.setattr(DeliveryClient, '_request', request)
    caller = read_only_client(config.path)
    assert caller.service() == {'fixture': '/api/service'}
    caller.runs()
    caller.status('run/one')
    caller.evidence('run/one', 'artifact/one')
    assert calls == [
        ('GET', '/api/service'), ('GET', '/api/runs'),
        ('GET', '/api/runs/run%2Fone'), ('GET', '/api/runs/run%2Fone/evidence/artifact%2Fone'),
    ]
    assert caller.csrf == ''
    assert not config.state_root.exists()
