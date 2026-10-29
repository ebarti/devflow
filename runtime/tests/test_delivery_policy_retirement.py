"""Retired admission surfaces refuse calls before any execution or state effect."""

from __future__ import annotations

import gzip
import hashlib
import json
from copy import deepcopy
from pathlib import Path

import httpx
import pytest
from test_delivery_api import api_fixture as api_fixture

from devflow_temporal import delivery_policy_recovery
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_config import DeliveryConfig


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


@pytest.mark.parametrize("tamper", [None, "config"])
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
