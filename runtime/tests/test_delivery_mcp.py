"""Official MCP discovery and forwarding over the existing local client."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from devflow_temporal.delivery_client import DeliveryClient
from devflow_temporal.delivery_mcp import build_server


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

    def client(path):
        seen.append(path)
        return SimpleNamespace(
            service=lambda: policy,
            recovery_preflight=lambda run_id: {"run_id": run_id, "precheck_sha256": "a" * 64},
            recover_execution=lambda run_id, request: {"run_id": run_id, **request},
            reconcile_tracker=lambda run_id, request: {"run_id": run_id, **request},
            reconcile_published_metadata=lambda run_id, request: {"run_id": run_id, **request},
            gates_only_preflight=lambda run_id: {"run_id": run_id},
            admit_gates_only=lambda run_id, request: {"run_id": run_id, **request},
            metadata_preflight=lambda run_id, request: {"run_id": run_id, **request},
            repair_admission_preflight=lambda run_id, request: {"run_id": run_id, **request},
            continue_repair=lambda run_id, request: {"run_id": run_id, **request},
        )

    monkeypatch.setattr("devflow_temporal.delivery_mcp.client", client)
    async with create_connected_server_and_client_session(build_server(config)) as session:
        tools = {tool.name: tool for tool in (await session.list_tools()).tools}
        assert set(tools) == {
            "get_service",
            "submit_run",
            "list_runs",
            "get_run",
            "read_evidence",
            "answer_decision",
            "cancel_run",
            "recovery_preflight",
            "recover_execution",
            "reconcile_tracker",
            "reconcile_published_metadata",
            "gates_only_preflight",
            "admit_gates_only",
            "metadata_preflight",
            "repair_admission_preflight",
            "continue_repair",
        }
        for name in (
            "get_service",
            "list_runs",
            "get_run",
            "read_evidence",
            "recovery_preflight",
            "gates_only_preflight",
            "metadata_preflight",
            "repair_admission_preflight",
        ):
            assert tools[name].annotations.readOnlyHint is True
            assert tools[name].annotations.destructiveHint is False
            assert tools[name].annotations.openWorldHint is False
        for name in (
            "submit_run",
            "answer_decision",
            "cancel_run",
            "recover_execution",
            "reconcile_tracker",
            "reconcile_published_metadata",
            "admit_gates_only",
            "continue_repair",
        ):
            assert tools[name].annotations.readOnlyHint is False
            assert tools[name].annotations.openWorldHint is True
            assert tools[name].annotations.idempotentHint is True
        assert tools["cancel_run"].annotations.destructiveHint is True
        assert tools["submit_run"].inputSchema["required"] == ["request_json"]
        assert tools["get_service"].inputSchema.get("required", []) == []
        result = await session.call_tool("get_service")
        assert not result.isError and json.loads(result.content[0].text) == policy
        preflight = await session.call_tool("recovery_preflight", {"run_id": "same-run"})
        assert json.loads(preflight.content[0].text)["precheck_sha256"] == "a" * 64
        recovered = await session.call_tool(
            "recover_execution",
            {
                "run_id": "same-run",
                "request_json": '{"command_id":"same-grant"}',
            },
        )
        assert json.loads(recovered.content[0].text) == {
            "run_id": "same-run",
            "command_id": "same-grant",
        }
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
            "reconcile_published_metadata",
            "admit_gates_only",
            "metadata_preflight",
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
        assert seen == [config] * 10
