"""Existing JSON controls preserve reconsideration fields and stable replay input."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from mcp.shared.memory import create_connected_server_and_client_session
from test_delivery_store import service as service

from devflow_temporal import delivery_control as cli
from devflow_temporal.delivery_client import DeliveryClient
from devflow_temporal.delivery_mcp import build_server


def request():
    return {
        "command_id": "fresh-context-review",
        "expected_revision": 9,
        "reason": "Investigate the same gate correction under changed authenticated derivation.",
        "reconsideration": {
            "version": 1,
            "kind": "derivation_context_change",
            "rejected_revision_id": "revision-" + "a" * 24,
            "proposal_sha256": "b" * 64,
            "expected_runtime_revision": "c" * 40,
            "expected_runtime_payload_sha256": "d" * 64,
        },
    }


@pytest.mark.asyncio
async def test_mcp_reconsideration_description_and_exact_client_forwarding(monkeypatch):
    calls = []
    value = request()
    receipt = {"run_id": "fresh-coordinator", "repair_budget": {"used": 6, "maximum": 10}}
    caller = object.__new__(DeliveryClient)

    def transport(method, route, payload, **_kwargs):
        calls.append((method, route, payload))
        return receipt

    monkeypatch.setattr(caller, "_request", transport)
    monkeypatch.setattr("devflow_temporal.delivery_mcp.client", lambda _path: caller)
    async with create_connected_server_and_client_session(
        build_server(Path("/fixture/service.json"))
    ) as session:
        tools = {tool.name: tool for tool in (await session.list_tools()).tools}
        description = tools["revise_feature_plan"].description
        assert "reconsideration" in description
        for field in value["reconsideration"]:
            assert field in description
        assert "fresh investigation and independent review only" in description
        assert "original rejection" in description and "repair use remain" in description
        assert tools["revise_feature_plan"].inputSchema["required"] == ["run_id", "request_json"]
        assert tools["revise_feature_plan"].annotations.idempotentHint is True
        arguments = {"run_id": "stopped-feature", "request_json": json.dumps(value)}
        for _ in range(2):
            result = await session.call_tool("revise_feature_plan", arguments)
            assert not result.isError
            assert json.loads(result.content[0].text) == receipt
    assert calls == [("POST", "/api/runs/stopped-feature/revise-feature-plan", value)] * 2


def test_cli_preserves_whole_reconsideration_request_after_uncertain_response(
    service, tmp_path, monkeypatch, capsys,
):
    store, _ = service
    value = request()
    path = tmp_path / "revision-request.json"
    path.write_text(json.dumps(value))
    seen = []
    receipt = {"run_id": "same-recorded-successor"}
    caller = SimpleNamespace(revise_feature_plan=lambda run_id, body: (
        seen.append((run_id, body)) or receipt
    ))
    monkeypatch.setattr(cli, "api_client", lambda _path: caller)
    args = argparse.Namespace(config=str(store.config.path),
                              command="revise-feature-plan", request=path, id="stopped-feature")
    for _ in range(2):
        cli._run(args, argparse.ArgumentParser())
        assert json.loads(capsys.readouterr().out) == receipt
    assert seen == [("stopped-feature", value)] * 2
    assert json.loads(path.read_text()) == value
