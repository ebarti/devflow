"""A stopped configured service is reachable only through an intentional write."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import subprocess
import sys
import threading
import time

import pytest
import uvicorn
from mcp.shared.memory import create_connected_server_and_client_session
from test_delivery_startup import config as config

from devflow_temporal import delivery_control as control
from devflow_temporal.delivery_api import DeliveryService, create_app
from devflow_temporal.delivery_client import DeliveryClient
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_mcp import build_server


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["submit_run", "answer_decision", "cancel_run"])
async def test_stopped_mcp_handoff_starts_explicitly_before_required_reads(
    config, monkeypatch, mutation,
):
    reserved = socket.socket()
    reserved.bind(("127.0.0.1", 0))
    port = reserved.getsockname()[1]
    config.path.write_text(json.dumps({**config.raw, "dashboard_url": f"http://127.0.0.1:{port}"}))
    config = DeliveryConfig.load(config.path)
    starts, effects, servers = [], [], []
    real_request = DeliveryClient._request

    def bounded_request(caller, method, path, body=None, *, timeout=30):
        return real_request(caller, method, path, body, timeout=min(timeout, 2))

    monkeypatch.setattr(DeliveryClient, "_request", bounded_request)

    async def health(service):
        service.temporal_status = "connected"

    monkeypatch.setattr(DeliveryService, "healthy_client", health)

    def start(current, _deadline):
        starts.append("start")
        reserved.close()
        app = create_app(current.path)
        store = app.state.delivery.store

        def submit(payload):
            effects.append(("submit_run", payload))
            return {"run_id": payload["run_id"], "phase": "preparing"}

        def begin(run_id, command_id, kind, payload):
            effects.append((kind, run_id, command_id, payload))
            return {"run_id": run_id, "phase": "accepted", "revision": 8}

        monkeypatch.setattr(store, "submit", submit)
        monkeypatch.setattr(store, "begin_mutation", begin)
        monkeypatch.setattr(store, "detail", lambda run_id: {
            "id": run_id, "protocol_revision": 7, "events": [],
            "pending_decision": {"id": "question-1", "revision": 2, "candidate_revision": 3},
        })
        monkeypatch.setattr(store, "evidence_index", lambda _run_id: [])
        # Real API authentication/forwarding; no Temporal or background dispatch.
        api = uvicorn.Server(uvicorn.Config(
            app, host="127.0.0.1", port=port, lifespan="off", log_level="error"))
        thread = threading.Thread(target=api.run)
        servers.append((api, thread))
        thread.start()
        deadline = time.monotonic() + 3
        while not api.started:
            assert time.monotonic() < deadline
            time.sleep(.01)
        control._write_manifest(current, {})
        return {"dashboard_url": current.dashboard_url, "processes": {}}

    monkeypatch.setattr(control, "_start", start)
    monkeypatch.setattr(control, "_ready", lambda *_args: bool(servers and servers[-1][0].started))
    try:
        async with create_connected_server_and_client_session(build_server(config.path)) as session:
            read = "get_service" if mutation == "submit_run" else "get_run"
            arguments = {} if read == "get_service" else {"run_id": "run-1"}
            unavailable = await session.call_tool(read, arguments)
            assert unavailable.isError
            assert "local service request failed" in unavailable.content[0].text
            assert starts == effects == []
            assert not config.state_root.exists() and not config.tracking_db.exists()

            started = await session.call_tool("start_service")
            assert not started.isError, started.content
            assert json.loads(started.content[0].text)["policy"]["repositories"][0]["key"] == (
                "fixture")
            # Repeated explicit start and later writes preserve the already running API.
            assert not (await session.call_tool("start_service")).isError
            current = await session.call_tool(read, arguments)
            assert not current.isError
            current = json.loads(current.content[0].text)
            if mutation == "submit_run":
                policy = current["policy"]["repositories"][0]
                listed = await session.call_tool("list_runs")
                assert json.loads(listed.content[0].text) == {"runs": [], "next_cursor": None}
                payload = {
                    "command_id": "submit-1", "run_id": "run-1", "work_id": "work-1",
                    "issue_url": "https://github.com/example/fixture/issues/1",
                    "repository_key": policy["key"], "goal": "Fix the linked issue",
                    "base_ref": policy["base_ref"], "branch": "fix/devflow-1",
                    "authorized_endpoint": "published_unmerged",
                }
                arguments = {"request_json": json.dumps(payload)}
                expected = [("submit_run", payload)]
            else:
                run = current["run"]
                payload = {"command_id": "continue-1",
                           "expected_revision": run["protocol_revision"]}
                if mutation == "answer_decision":
                    decision = run["pending_decision"]
                    payload.update(
                        decision_id=decision["id"], decision_revision=decision["revision"],
                        candidate_revision=decision["candidate_revision"], answer="yes")
                else:
                    payload["reason"] = "User requested cancellation"
                arguments = {"run_id": "run-1", "request_json": json.dumps(payload)}
                expected = [("decision" if mutation == "answer_decision" else "cancel",
                             "run-1", "continue-1", payload)]
            accepted = await session.call_tool(mutation, arguments)
            assert not accepted.isError, accepted.content
            assert json.loads(accepted.content[0].text)["run_id"] == "run-1"
            assert starts == ["start"] and effects == expected
    finally:
        reserved.close()
        for api, thread in servers:
            api.should_exit = True
            await asyncio.to_thread(thread.join, 3)
            assert not thread.is_alive()


class RoutedClient:
    def __init__(self, factory):
        self.factory = factory

    def __getattr__(self, method):
        return lambda *_args, **_kwargs: {"factory": self.factory, "method": method}


@pytest.mark.asyncio
@pytest.mark.parametrize("tool,arguments,factory,method", [
    ("get_service", {}, "read", "service"),
    ("list_runs", {}, "read", "runs"),
    ("get_run", {"run_id": "run-1"}, "read", "status"),
    ("read_evidence", {"run_id": "run-1", "evidence_id": "one"}, "read", "evidence"),
    ("gates_only_preflight", {"run_id": "run-1"}, "read", "gates_only_preflight"),
    ("repair_admission_preflight", {"run_id": "run-1", "request_json": "{}"},
     "read", "repair_admission_preflight"),
    ("start_service", {}, "write", "service"),
    ("submit_run", {"request_json": "{}"}, "write", "submit"),
    ("answer_decision", {"run_id": "run-1", "request_json": "{}"}, "write", "decision"),
    ("cancel_run", {"run_id": "run-1", "request_json": "{}"}, "write", "cancel"),
    *[(name, {"run_id": "run-1", "request_json": "{}"}, "write", name) for name in (
        "reconcile_tracker", "admit_gates_only", "continue_repair",
    )],
])
async def test_mcp_tools_use_distinct_read_and_starting_factories(
    config, monkeypatch, tool, arguments, factory, method,
):
    monkeypatch.setattr("devflow_temporal.delivery_mcp.client", lambda _path: RoutedClient("write"))
    monkeypatch.setattr("devflow_temporal.delivery_mcp.read_only_client",
                        lambda _path: RoutedClient("read"))
    async with create_connected_server_and_client_session(build_server(config.path)) as session:
        result = await session.call_tool(tool, arguments)
    assert not result.isError, result.content
    assert json.loads(result.content[0].text) == {"factory": factory, "method": method}


@pytest.mark.asyncio
@pytest.mark.parametrize("tool", [
    "recovery_preflight", "recover_execution", "metadata_preflight",
    "reconcile_published_metadata",
])
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


@pytest.mark.parametrize("command,factory,method", [
    ("runs", "read", "runs"), ("run", "read", "status"), ("evidence", "read", "evidence"),
    *[(name, "read", name.replace("-", "_")) for name in (
        "gates-only-preflight", "repair-admission-preflight",
    )],
    *[(name, "write", name.replace("-", "_")) for name in (
        "submit", "decision", "cancel", "reconcile-tracker", "recover-publication",
        "admit-gates-only", "continue-repair", "retry-prelaunch", "amend-scope",
    )],
])
def test_cli_commands_use_distinct_read_and_starting_factories(
    config, monkeypatch, capsys, command, factory, method,
):
    monkeypatch.setattr(control, "api_client", lambda _path: RoutedClient("write"))
    monkeypatch.setattr(control, "read_only_client", lambda _path: RoutedClient("read"))
    request = config.path.parent / "request.json"
    request.write_text("{}")
    args = argparse.Namespace(command=command, config=str(config.path), id="run-1",
                              evidence_id="one", request=request)
    control._run(args, argparse.ArgumentParser())
    assert json.loads(capsys.readouterr().out) == {"factory": factory, "method": method}


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
