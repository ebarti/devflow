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
    monkeypatch.setattr(DeliveryClient, "_request", lambda self, *args: calls.append(args) or {
        "policy": {"authorized_endpoint": "published_unmerged"}
    })
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
        return SimpleNamespace(service=lambda: policy)

    monkeypatch.setattr("devflow_temporal.delivery_mcp.client", client)
    async with create_connected_server_and_client_session(build_server(config)) as session:
        tools = {tool.name: tool for tool in (await session.list_tools()).tools}
        assert set(tools) == {
            "get_service", "submit_run", "list_runs", "get_run", "read_evidence",
            "answer_decision", "cancel_run",
        }
        for name in ("get_service", "list_runs", "get_run", "read_evidence"):
            assert tools[name].annotations.readOnlyHint is True
            assert tools[name].annotations.destructiveHint is False
            assert tools[name].annotations.openWorldHint is False
        for name in ("submit_run", "answer_decision", "cancel_run"):
            assert tools[name].annotations.readOnlyHint is False
            assert tools[name].annotations.openWorldHint is True
            assert tools[name].annotations.idempotentHint is True
        assert tools["cancel_run"].annotations.destructiveHint is True
        assert tools["submit_run"].inputSchema["required"] == ["request_json"]
        assert tools["get_service"].inputSchema.get("required", []) == []
        result = await session.call_tool("get_service")
        assert not result.isError and json.loads(result.content[0].text) == policy
        assert seen == [config]
