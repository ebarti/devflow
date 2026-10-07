from __future__ import annotations

import argparse
import json
import subprocess

import httpx
import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_store import service as service

from devflow_temporal import delivery_control
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_client import DeliveryClient


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,endpoint,store_method,mutation",
    [
        ("POST", "metadata-preflight", "metadata_preflight", False),
        ("POST", "repair-admission-preflight", "repair_admission_preflight", False),
        ("GET", "gates-only-preflight", "gates_only_preflight", False),
        ("POST", "reconcile-published-metadata", "reconcile_published_metadata", True),
        ("POST", "admit-gates-only", "admit_gates_only", True),
        ("POST", "continue-repair", "continue_repair", True),
    ],
)
async def test_stopped_reconciliation_api_authentication_and_exact_request_forwarding(
    api_fixture,
    monkeypatch,
    method,
    endpoint,
    store_method,
    mutation,
):
    path, _ = api_fixture
    app = create_app(path)
    calls = []
    payload = {"command_id": "stable-existing-command", "authority_sha256": "a" * 64}

    def called(*args):
        calls.append(args)
        return {"accepted": True, "existing": len(calls) > 1}

    monkeypatch.setattr(app.state.delivery.store, store_method, called)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as browser:
        route = "/api/runs/stopped/" + endpoint
        args = {"json": payload} if method == "POST" else {}
        assert (
            await browser.request(method, route, headers={"Host": "evil.local"}, **args)
        ).status_code == 403
        assert calls == []
        if mutation:
            assert (await browser.request(method, route, **args)).status_code == 403
            assert calls == []
            session = await browser.get("/api/session")
            headers = {
                "Origin": "http://127.0.0.1:18770",
                "X-Devflow-CSRF": session.json()["csrf_token"],
            }
        else:
            headers = {}
        assert (await browser.request(method, route, headers=headers, **args)).json()["accepted"]
        assert calls == ([("stopped", payload)] if method == "POST" else [("stopped",)])

        def unavailable(*_args):
            raise subprocess.TimeoutExpired(["git", "ls-remote"], 30)

        monkeypatch.setattr(app.state.delivery.store, store_method, unavailable)
        response = await browser.request(method, route, headers=headers, **args)
        assert response.status_code == 409
        assert "timed out" in response.json()["detail"]
        assert len(calls) == 1  # No transport-level automatic mutation replay.


@pytest.mark.parametrize(
    "command",
    [
        "metadata-preflight",
        "repair-admission-preflight",
        "gates-only-preflight",
        "reconcile-published-metadata",
        "admit-gates-only",
        "continue-repair",
    ],
)
def test_cli_preserves_stable_request_and_client_encodes_run_identity(
    api_fixture,
    monkeypatch,
    capsys,
    command,
):
    path, _ = api_fixture
    caller = DeliveryClient(delivery_control.DeliveryConfig.load(path))
    calls = []

    def request(method, endpoint, body=None, *, timeout=30):
        calls.append((method, endpoint, body, timeout))
        return {"command_id": body["command_id"] if body else None}

    monkeypatch.setattr(caller, "_request", request)
    factories = []

    def client(factory):
        factories.append(factory)
        return caller

    monkeypatch.setattr(delivery_control, "api_client", lambda _path: client("write"))
    monkeypatch.setattr(delivery_control, "read_only_client", lambda _path: client("read"))
    payload = {"command_id": "same-command", "expected_head": "a" * 40}
    request_path = path.parent / "reconciliation-request.json"
    request_path.write_text(json.dumps(payload))
    args = argparse.Namespace(
        config=str(path),
        command=command,
        id="owned/run",
        request=None if command == "gates-only-preflight" else request_path,
        evidence_id=None,
    )
    delivery_control._run(args, argparse.ArgumentParser())
    assert factories == ["read" if command in {
        "metadata-preflight", "repair-admission-preflight", "gates-only-preflight",
    } else "write"]
    method = "GET" if command == "gates-only-preflight" else "POST"
    assert calls == [
        (
            method,
            "/api/runs/owned%2Frun/" + command,
            None if method == "GET" else payload,
            180 if command in {"continue-repair", "reconcile-published-metadata"} else 120,
        )
    ]
    assert json.loads(capsys.readouterr().out)["command_id"] == (
        None if method == "GET" else "same-command"
    )


@pytest.mark.parametrize(
    "queued,running",
    [
        ("metadata_validation_queued", "metadata_validation"),
        ("gates_only_queued", "gates_only"),
    ],
)
def test_new_stopped_successors_record_pending_and_acknowledged_dispatch(service, queued, running):
    store, request = service
    store.submit(request)
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET phase=?", (queued,))
    store.mark_start("run-1", accepted=False, error="Temporal ID conflict")
    assert store.detail("run-1")["phase"] == queued
    with store._connect() as db:
        row = db.execute("SELECT state,attempts,last_error FROM delivery_outbox").fetchone()
        assert tuple(row) == ("unknown", 1, "Temporal ID conflict")
    store.mark_start("run-1", accepted=True)
    assert store.detail("run-1")["phase"] == running
    assert store.pending_starts() == []
