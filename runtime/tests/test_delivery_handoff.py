"""A stopped configured service is reachable only through an intentional write."""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest
from mcp.shared.memory import create_connected_server_and_client_session
from test_delivery_startup import config as config

from devflow_temporal import delivery_control as control
from devflow_temporal.delivery_mcp import build_server


@pytest.mark.asyncio
@pytest.mark.parametrize("tool", ["recovery_preflight", "reconcile_published_metadata"])
async def test_retired_mcp_tools_reject_without_start_or_stop(tmp_path, monkeypatch, tool):
    calls = []
    path = tmp_path / "absent" / "service.json"
    for name in ("client", "read_only_client"):
        monkeypatch.setattr(f"devflow_temporal.delivery_mcp.{name}",
                            lambda *_args, _name=name: calls.append(_name))
    for name in ("ensure_service_running", "service_start", "service_stop"):
        monkeypatch.setattr(control, name,
                            lambda *_args, _name=name, **_kwargs: calls.append(_name))
    arguments = {"run_id": "run-1"}
    if tool != "recovery_preflight":
        arguments["request_json"] = "{}"
    async with create_connected_server_and_client_session(build_server(path)) as session:
        tools = {item.name for item in (await session.list_tools()).tools}
        result = await session.call_tool(tool, arguments)
    assert tool not in tools
    assert result.isError and f"Unknown tool: {tool}" in result.content[0].text
    assert calls == []
    assert not path.parent.exists()


@pytest.mark.parametrize("command,exit_code", [("status", 0), ("token", 1)])
def test_cli_status_and_token_leave_absent_state_and_database(config, command, exit_code):
    result = subprocess.run(
        [sys.executable, "-m", "devflow_temporal.delivery_control",
         "--config", str(config.path), command],
        capture_output=True, text=True, timeout=10,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert result.returncode == exit_code, result.stderr
    if command == "status":
        assert json.loads(result.stdout)["processes"] == {}
    else:
        assert "service-token" in result.stderr and result.stdout == ""
    assert not config.state_root.exists()
    assert not config.tracking_db.exists()
