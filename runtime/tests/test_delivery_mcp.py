"""Official MCP discovery and forwarding over the existing local client."""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from devflow_temporal.delivery_client import DeliveryClient
from devflow_temporal.delivery_mcp import build_server


@pytest.mark.asyncio
async def test_live_disable_uses_existing_transport_off_loop_without_startup(monkeypatch):
    config = Path("/fixture/service.json")
    seen = []

    def forbidden(*_args):
        pytest.fail("live disable must not start or restart a service")

    def disable(run_id, payload):
        with pytest.raises(RuntimeError, match="no running event loop"):
            asyncio.get_running_loop()
        seen.append((run_id, payload))
        return {"delivery_limits_disabled": True}

    monkeypatch.setattr("devflow_temporal.delivery_mcp.client", forbidden)
    monkeypatch.setattr("devflow_temporal.delivery_mcp.read_only_client", lambda path:
                        SimpleNamespace(disable_delivery_limits=disable))
    payload = {"command_id": "disable-same", "expected_revision": 4, "reason": "user request"}
    async with create_connected_server_and_client_session(build_server(config)) as session:
        result = await session.call_tool("disable_delivery_limits", {
            "run_id": "same-run", "request_json": json.dumps(payload),
        })
        assert not result.isError
        assert json.loads(result.content[0].text) == {"delivery_limits_disabled": True}
    assert seen == [("same-run", payload)]


def test_client_service_uses_existing_policy_route(monkeypatch):
    calls = []
    monkeypatch.setattr(
        DeliveryClient,
        "_request",
        lambda self, *args: (
            calls.append(args) or {"policy": {"authorized_endpoint": "published_unmerged"}}
        ),
    )
    caller = object.__new__(DeliveryClient)
    assert caller.service()["policy"]["authorized_endpoint"] == "published_unmerged"
    assert calls == [("GET", "/api/service")]


@pytest.mark.asyncio
async def test_official_mcp_discovery_annotations_and_service_forwarding(monkeypatch):
    config = Path("/fixture/service.json")
    seen = []
    policy = {"policy": {"repositories": [{"key": "fixture", "base_ref": "main"}]}}

    def client(path, factory):
        seen.append((factory, path))
        return SimpleNamespace(
            service=lambda: policy,
            reconcile_tracker=lambda run_id, request: {"run_id": run_id, **request},
            gates_only_preflight=lambda run_id: {"run_id": run_id},
            admit_gates_only=lambda run_id, request: {"run_id": run_id, **request},
            repair_admission_preflight=lambda run_id, request: {"run_id": run_id, **request},
            continue_repair=lambda run_id, request: {"run_id": run_id, **request},
        )

    monkeypatch.setattr("devflow_temporal.delivery_mcp.client", lambda path: client(path, "write"))
    monkeypatch.setattr("devflow_temporal.delivery_mcp.read_only_client",
                        lambda path: client(path, "read"))
    async with create_connected_server_and_client_session(build_server(config)) as session:
        tools = {tool.name: tool for tool in (await session.list_tools()).tools}
        assert set(tools) == {
            "get_service",
            "start_service",
            "submit_run",
            "list_runs",
            "get_run",
            "read_evidence",
            "answer_decision",
            "continue_feature",
            "revise_feature_plan",
            "merge_feature",
            "cancel_run",
            "disable_delivery_limits",
            "reconcile_tracker",
            "gates_only_preflight",
            "admit_gates_only",
            "repair_admission_preflight",
            "continue_repair",
        }
        for name in (
            "get_service",
            "list_runs",
            "get_run",
            "read_evidence",
            "gates_only_preflight",
            "repair_admission_preflight",
        ):
            assert tools[name].annotations.readOnlyHint is True
            assert tools[name].annotations.destructiveHint is False
            assert tools[name].annotations.openWorldHint is False
        for name in (
            "start_service",
            "submit_run",
            "answer_decision",
            "continue_feature",
            "revise_feature_plan",
            "merge_feature",
            "cancel_run",
            "disable_delivery_limits",
            "reconcile_tracker",
            "admit_gates_only",
            "continue_repair",
        ):
            assert tools[name].annotations.readOnlyHint is False
            assert tools[name].annotations.openWorldHint is True
            assert tools[name].annotations.idempotentHint is True
        assert tools["cancel_run"].annotations.destructiveHint is True
        assert tools["submit_run"].inputSchema["required"] == ["request_json"]
        assert tools["get_service"].inputSchema.get("required", []) == []
        assert tools["start_service"].inputSchema.get("required", []) == []
        assert tools["start_service"].annotations.destructiveHint is False
        result = await session.call_tool("get_service")
        assert not result.isError and json.loads(result.content[0].text) == policy
        reconciled = await session.call_tool(
            "reconcile_tracker",
            {
                "run_id": "same-run",
                "request_json": '{"command_id":"same-tracker","expected_revision":13}',
            },
        )
        assert json.loads(reconciled.content[0].text) == {
            "run_id": "same-run",
            "command_id": "same-tracker",
            "expected_revision": 13,
        }
        for name in (
            "admit_gates_only",
            "repair_admission_preflight",
            "continue_repair",
        ):
            result = await session.call_tool(
                name, {"run_id": "same-run", "request_json": '{"command_id":"same-authority"}'}
            )
            assert not result.isError
            assert json.loads(result.content[0].text) == {
                "run_id": "same-run",
                "command_id": "same-authority",
            }
        result = await session.call_tool("gates_only_preflight", {"run_id": "same-run"})
        assert not result.isError and json.loads(result.content[0].text) == {"run_id": "same-run"}
        assert seen == [(factory, config) for factory in (
            "read", "write", "write", "read", "write", "read",
        )]


@pytest.mark.asyncio
@pytest.mark.parametrize(('tool', 'method'), [
    ('start_service', 'service'), ('submit_run', 'submit'),
    ('answer_decision', 'decision'), ('continue_feature', 'continue_feature'),
    ('revise_feature_plan', 'revise_feature_plan'), ('merge_feature', 'decision'),
    ('cancel_run', 'cancel'), ('reconcile_tracker', 'reconcile_tracker'),
    ('admit_gates_only', 'admit_gates_only'), ('continue_repair', 'continue_repair'),
])
async def test_mutations_start_and_forward_off_the_mcp_loop_once(monkeypatch, tool, method):
    calls = []
    payload = {'command_id': 'same-authority', 'expected_revision': 13, 'answer': 'merge'}

    def startup(path):
        calls.append(('startup', path))
        # The real lifecycle readiness helpers use asyncio.run too. Close the
        # coroutine if the old on-loop adapter rejects it before execution.
        readiness = asyncio.sleep(0)
        try:
            asyncio.run(readiness)
        finally:
            readiness.close()

        def forward(*args):
            with pytest.raises(RuntimeError, match='no running event loop'):
                asyncio.get_running_loop()
            calls.append((method, args))
            return {'forwarded': method}

        return SimpleNamespace(**{method: forward})

    monkeypatch.setattr('devflow_temporal.delivery_mcp.client', startup)
    args = {} if tool == 'start_service' else {'request_json': json.dumps(payload)}
    if tool not in {'start_service', 'submit_run'}:
        args['run_id'] = 'same-run'
    async with create_connected_server_and_client_session(
        build_server(Path("/fixture/config"))
    ) as session:
        result = await session.call_tool(tool, args)
        assert not result.isError, result.content
        assert json.loads(result.content[0].text) == {'forwarded': method}
    expected = (() if tool == 'start_service' else (payload,) if tool == 'submit_run'
                else ('same-run', payload))
    assert calls == [('startup', Path('/fixture/config')), (method, expected)]


@pytest.mark.asyncio
async def test_uncertain_mcp_mutation_is_not_replayed(monkeypatch):
    calls = []

    def startup(_path):
        calls.append('startup')

        def uncertain(*_args):
            calls.append('mutation')
            raise RuntimeError('response lost after external effect')

        return SimpleNamespace(continue_repair=uncertain)

    monkeypatch.setattr('devflow_temporal.delivery_mcp.client', startup)
    async with create_connected_server_and_client_session(
        build_server(Path("/fixture/config"))
    ) as session:
        result = await session.call_tool('continue_repair', {
            'run_id': 'same-run', 'request_json': '{"command_id":"stable"}',
        })
        assert result.isError
    assert calls == ['startup', 'mutation']


@pytest.mark.asyncio
async def test_async_submission_keeps_per_request_origin_and_rejects_mismatch(monkeypatch):
    first = '11111111-1111-4111-8111-111111111111'
    second = '22222222-2222-4222-8222-222222222222'
    monkeypatch.setenv('CODEX_THREAD_ID', '33333333-3333-4333-8333-333333333333')
    seen = []
    monkeypatch.setattr('devflow_temporal.delivery_mcp.client', lambda _: SimpleNamespace(
        submit=lambda request: (seen.append(request.copy()) or request)))
    async with create_connected_server_and_client_session(
        build_server(Path("/fixture/config"))
    ) as session:
        for origin in (first, second):
            result = await session.call_tool('submit_run', {
                'request_json': '{"command_id":"per-request"}',
            }, meta={'threadId': origin})
            assert not result.isError
            assert json.loads(result.content[0].text)['origin_thread_id'] == origin
        mismatch = await session.call_tool('submit_run', {
            'request_json': json.dumps({'command_id': 'no-write', 'origin_thread_id': first}),
        }, meta={'threadId': second})
        assert mismatch.isError
    assert [request['origin_thread_id'] for request in seen] == [first, second]
