"""Retired admission surfaces refuse calls before any execution or state effect."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from historical_replay import replay_designated_history
from mcp.shared.memory import create_connected_server_and_client_session
from test_delivery_api import api_fixture as api_fixture

from devflow_temporal import delivery_policy_recovery
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_client import DeliveryClient
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_mcp import build_server
from devflow_temporal.delivery_store import DeliveryStore


@pytest.mark.parametrize("command", ["recovery-preflight", "recover-execution"])
def test_retired_cli_command_rejects_before_reading_configuration(tmp_path, command):
    path = tmp_path / "absent-state" / "service.json"
    environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    result = subprocess.run(
        [sys.executable, "-B", "-m", "devflow_temporal.delivery_control",
         "--config", str(path), command, "--id", "stopped"],
        env=environment, text=True, capture_output=True, timeout=15,
    )
    assert result.returncode == 2, result.stderr
    assert "invalid choice" in result.stderr
    assert not path.parent.exists()


@pytest.mark.asyncio
async def test_retired_http_routes_refuse_even_a_valid_local_mutation(api_fixture, monkeypatch):
    path, _ = api_fixture
    app = create_app(path)
    store = app.state.delivery.store
    calls = []
    for name in ("policy_recovery_precheck", "recover_execution"):
        monkeypatch.setattr(store, name, lambda *args: calls.append(args) or {"grant": True},
                            raising=False)
    before = {item: hashlib.sha256(item.read_bytes()).hexdigest()
              for item in path.parent.rglob("*") if item.is_file()}
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as browser:
        session = await browser.get("/api/session")
        headers = {"Origin": "http://127.0.0.1:18770",
                   "X-Devflow-CSRF": session.json()["csrf_token"]}
        read = await browser.get("/api/runs/stopped/recovery-preflight")
        write = await browser.post("/api/runs/stopped/recover-execution",
                                   json={"command_id": "retired"}, headers=headers)
    assert (read.status_code, write.status_code) == (404, 404)
    assert calls == []
    assert before == {item: hashlib.sha256(item.read_bytes()).hexdigest()
                      for item in path.parent.rglob("*") if item.is_file()}


@pytest.mark.asyncio
async def test_retired_mcp_tools_refuse_without_constructing_a_client(tmp_path, monkeypatch):
    calls = []

    def client(path):
        calls.append(path)
        return SimpleNamespace(recovery_preflight=lambda _: {"grant": True},
                               recover_execution=lambda *_: {"grant": True})

    monkeypatch.setattr("devflow_temporal.delivery_mcp.client", client)
    path = tmp_path / "absent-state" / "service.json"
    async with create_connected_server_and_client_session(build_server(path)) as session:
        tools = {tool.name for tool in (await session.list_tools()).tools}
        for name in ("recovery_preflight", "recover_execution"):
            arguments = {"run_id": "stopped"}
            if name == "recover_execution":
                arguments["request_json"] = json.dumps({"command_id": "retired"})
            result = await session.call_tool(name, arguments)
            assert result.isError
            assert name not in tools
    assert calls == []
    assert not path.parent.exists()


@pytest.mark.parametrize("name", ["policy_recovery_precheck", "recover_execution"])
def test_retired_direct_store_grants_have_no_state_access(api_fixture, monkeypatch, name):
    path, _ = api_fixture
    store = create_app(path).state.delivery.store
    calls = []

    def forbidden(*_args):
        calls.append("state access")
        raise AssertionError("retired admission reached existing run authority")

    monkeypatch.setattr(store, "intake_execution_spec", forbidden)
    with pytest.raises(AttributeError):
        method = getattr(store, name)
        if name == "recover_execution":
            method("stopped", {"command_id": "retired"})
        else:
            method("stopped")
    assert calls == []
    assert not hasattr(DeliveryStore, name)


@pytest.mark.parametrize("name", ["recovery_preflight", "recover_execution"])
def test_retired_client_wrapper_is_absent(name):
    assert not hasattr(DeliveryClient, name)


@pytest.mark.parametrize("tamper", [None, "missing", "original", "recovery", "maximum", "config"])
def test_actual_c04_policy_row_retains_its_durable_validation(monkeypatch, tamper):
    """Use genuine c04 row/config bytes; substitute only historical file transport."""
    path = Path(__file__).parent / "fixtures" / "policy-c04-admitted-row.json.gz"
    recorded = json.loads(gzip.decompress(path.read_bytes()))
    original = recorded["original"]
    recovery = json.loads(recorded["run"]["recovery_json"])
    grant = deepcopy(recorded["grant"])
    content = recorded["trusted_config_bytes"].encode()

    def private_bytes(path, root):
        assert path == Path(recovery["effective_spec"]["config_path"])
        assert root == Path(original["state_dir"]).parents[1]
        return content

    def load(cls, path):
        field = ("original_config_bytes" if str(path) == original["config_path"]
                 else "trusted_config_bytes")
        assert str(path) in {original["config_path"], recovery["effective_spec"]["config_path"]}
        return cls(path=path, raw=json.loads(recorded[field]))

    monkeypatch.setattr(delivery_policy_recovery, "_private_bytes", private_bytes)
    monkeypatch.setattr(DeliveryConfig, "load", classmethod(load))
    if tamper == "missing":
        grant = None
    elif tamper in {"original", "recovery"}:
        grant[tamper + "_spec_digest" if tamper == "original" else "recovery_digest"] = "f" * 64
    elif tamper == "maximum":
        grant["maximum_iteration"] += 1
    elif tamper == "config":
        content += b" "
    if tamper:
        with pytest.raises(ValueError, match="not durable|configuration bytes changed"):
            delivery_policy_recovery.effective_spec(None, original, recovery, grant)
    else:
        assert delivery_policy_recovery.effective_spec(None, original, recovery, grant) == (
            recorded["effective_spec"])


@pytest.mark.asyncio
@pytest.mark.parametrize("mode,expected_sha256", [
    ("completed", "29e4e592ab39e77a410ca6dd55177b975d0b173969e0fd9ca35d996ff25ee4bc"),
    ("suspended", "ff5dd1eb650799a79c814296322f2236a5abc9103b817f235a51fbbdab6c1739"),
])
async def test_actual_c04_policy_history_replays_byte_exact(mode, expected_sha256, tmp_path):
    path = Path(__file__).parent / "fixtures" / f"policy-c04-{mode}-history.json.gz"
    content = gzip.decompress(path.read_bytes())
    assert hashlib.sha256(content).hexdigest() == expected_sha256
    await replay_designated_history(path, tmp_path, "c04-policy-" + mode)
    assert hashlib.sha256(content).hexdigest() == expected_sha256
