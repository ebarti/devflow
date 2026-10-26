from __future__ import annotations

import asyncio
import threading

import httpx
import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_ci_inventory import ci_case as ci_case
from test_delivery_native_diagnostic import stopped_diagnostic as stopped_diagnostic
from test_delivery_store import service as service

from devflow_temporal.delivery_api import create_app


@pytest.mark.asyncio
async def test_public_operation_survives_caller_and_deduplicates(api_fixture, monkeypatch):
    path, _ = api_fixture
    started, release = threading.Event(), threading.Event()
    calls = []

    def slow(store, run_id):
        calls.append(run_id)
        started.set()
        assert release.wait(10)
        return {"run_id": run_id, "observation": "complete"}

    monkeypatch.setattr("devflow_temporal.delivery_merge_followup.preflight", slow)
    app = create_app(path)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 1234))
    envelope = {
        "operation_id": "operation-1",
        "run_id": "run-1",
        "kind": "merge-preflight",
        "request": {},
    }
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as client:
        response = await client.post("/api/operations", json=envelope)
        assert response.status_code == 202
        assert await asyncio.to_thread(started.wait, 5)
        duplicates = await asyncio.gather(
            *[client.post("/api/operations", json=envelope) for _ in range(5)]
        )
        assert all(item.status_code == 202 for item in duplicates)
        conflict = await client.post("/api/operations", json={**envelope, "request": {"other": 1}})
        assert conflict.status_code == 409
        assert (await client.get("/api/operations/operation-1")).json()["state"] == "running"
        release.set()
        for _ in range(100):
            result = (await client.get("/api/operations/operation-1")).json()
            if result["state"] == "succeeded":
                break
            await asyncio.sleep(0.02)
        assert result["result"] == {"run_id": "run-1", "observation": "complete"}
        assert result["authority"] == "current_authority_unverified"
        assert (await client.post("/api/operations", json=envelope)).json()["result"] == result[
            "result"
        ]
        assert calls == ["run-1"]


@pytest.mark.asyncio
async def test_restart_dispatches_only_pending_and_preserves_uncertain(api_fixture, monkeypatch):
    from devflow_temporal.contracts import digest

    path, _ = api_fixture
    app = create_app(path)
    store = app.state.delivery.store
    calls = []
    monkeypatch.setattr(
        "devflow_temporal.delivery_merge_followup.preflight",
        lambda store, run: calls.append(run) or {"run_id": run},
    )
    request_digest = digest({"run_id": "run-1", "kind": "merge-preflight", "request": {}})
    with store._connect() as db:
        for operation_id, state in [("pending", "pending"), ("interrupted", "running")]:
            db.execute(
                """INSERT INTO delivery_operations
                (operation_id, run_id, kind, stage, request_digest, request_json,
                 state, created_at, updated_at, progress)
                 VALUES (?, 'run-1', 'merge-preflight', 'observation', ?, '{}', ?,
                         '2026', '2026', 'test')""",
                (operation_id, request_digest, state),
            )
    restarted = create_app(path)
    await restarted.state.delivery.resume_pending_operations()
    transport = httpx.ASGITransport(app=restarted, client=("127.0.0.1", 1234))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as client:
        for _ in range(100):
            pending = (await client.get("/api/operations/pending")).json()
            if pending["state"] == "succeeded":
                break
            await asyncio.sleep(0.02)
        assert pending["state"] == "succeeded"
        assert (await client.get("/api/operations/interrupted")).json()["state"] == "unknown"
        assert calls == ["run-1"]


@pytest.mark.asyncio
async def test_real_diagnostic_preflight_and_ack_reconcile(stopped_diagnostic, monkeypatch):
    from devflow_temporal import delivery_native_diagnostic as diagnostic

    case = stopped_diagnostic
    started, release = threading.Event(), threading.Event()
    snapshot = diagnostic.snapshot

    def slow(*args):
        started.set()
        assert release.wait(30)
        return snapshot(*args)

    monkeypatch.setattr(diagnostic, "snapshot", slow)
    app = create_app(case.store.config.path)
    host = case.store.config.dashboard_url
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 1234))
    async with httpx.AsyncClient(transport=transport, base_url=host) as client:
        token = (await client.get("/api/session")).json()["csrf_token"]
        headers = {"Origin": host, "X-Devflow-CSRF": token}
        envelope = {
            "operation_id": "diagnostic-observe",
            "run_id": "run-1",
            "kind": "native-diagnostic-preflight",
            "request": case.diagnostic_request,
        }
        assert (await client.post("/api/operations", json=envelope)).status_code == 202
        assert await asyncio.to_thread(started.wait, 10)
        assert (await client.get("/api/operations/diagnostic-observe")).json()["state"] == "running"
        release.set()
        for _ in range(500):
            operation = (await client.get("/api/operations/diagnostic-observe")).json()
            if operation["state"] not in ("pending", "running"):
                break
            await asyncio.sleep(0.02)
        assert operation["state"] == "succeeded", operation
        ack = {
            **case.diagnostic_request,
            "preflight_key": operation["result"]["preflight_key"],
            "acknowledgement": diagnostic.ACK,
        }
        envelope = {
            "operation_id": "diagnostic-ack",
            "run_id": "run-1",
            "kind": "ack-native-diagnostic",
            "request": ack,
        }
        assert (
            await client.post("/api/operations", json=envelope, headers=headers)
        ).status_code in (200, 202)
        for _ in range(500):
            operation = (await client.get("/api/operations/diagnostic-ack")).json()
            if operation["state"] not in ("pending", "running"):
                break
            await asyncio.sleep(0.02)
        assert operation["state"] == "succeeded", operation
        receipt = operation["result"]
        assert receipt["state"] == "queued"
        # Simulate a crash after the owner's durable acknowledgement and before
        # the transport journal recorded it, then reconcile the exact receipt.
        with app.state.delivery.store._connect() as db:
            db.execute(
                "UPDATE delivery_operations SET state='unknown', result_json=NULL "
                "WHERE operation_id='diagnostic-ack'"
            )
        recovered = await client.post("/api/operations/diagnostic-ack/reconcile", headers=headers)
        assert recovered.json()["state"] == "succeeded", recovered.text
        assert recovered.json()["result"] == receipt
        with app.state.delivery.store._connect() as db:
            assert db.execute("SELECT COUNT(*) FROM delivery_native_diagnostics").fetchone()[0] == 1


@pytest.mark.asyncio
async def test_diagnostic_wake_survives_transient_row_failure(monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace

    from devflow_temporal.delivery_native_diagnostic import dispatch_once

    class Database:
        def execute(self, *_args):
            return iter([{"identity": "original", "run_id": "run-1"}])

    class Store:
        attempts = 0

        @contextmanager
        def _connect(self):
            yield Database()

        def effective_spec(self, _run):
            self.attempts += 1
            if self.attempts == 1:
                raise OSError("temporary observation failure")
            return {}

    store = Store()
    diagnostic_service = SimpleNamespace(store=store, _native_diagnostic_pending=True)
    monkeypatch.setattr("devflow_temporal.delivery_preparation.execution_retired", lambda _: True)
    await dispatch_once(diagnostic_service)
    assert diagnostic_service._native_diagnostic_pending
    await dispatch_once(diagnostic_service)
    assert store.attempts == 1  # bounded retry
    diagnostic_service._native_diagnostic_retry_at = 0
    await dispatch_once(diagnostic_service)
    assert store.attempts == 2
    assert not diagnostic_service._native_diagnostic_pending
    assert not diagnostic_service.native_diagnostic_tasks


@pytest.mark.asyncio
async def test_real_ci_ack_reconciles_original_receipt(ci_case):
    from devflow_temporal import delivery_ci_inventory as ci

    c = ci_case
    app = create_app(c.store.config.path)
    host = c.store.config.dashboard_url
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 1234))
    async with httpx.AsyncClient(transport=transport, base_url=host) as client:
        token = (await client.get("/api/session")).json()["csrf_token"]
        headers = {"Origin": host, "X-Devflow-CSRF": token}
        pre = await client.post("/api/runs/run-1/ci-inventory-preflight", json=c.command)
        assert pre.status_code == 200, pre.text
        ack = {
            **c.command,
            "preflight_key": pre.json()["preflight_key"],
            "acknowledgement": ci.ACK,
        }
        envelope = {
            "operation_id": "ci-ack",
            "run_id": "run-1",
            "kind": "ack-ci-inventory",
            "request": ack,
        }
        assert (
            await client.post("/api/operations", json=envelope, headers=headers)
        ).status_code in (200, 202)
        for _ in range(500):
            operation = (await client.get("/api/operations/ci-ack")).json()
            if operation["state"] not in ("pending", "running"):
                break
            await asyncio.sleep(0.02)
        assert operation["state"] == "succeeded", operation
        receipt = operation["result"]
        with app.state.delivery.store._connect() as db:
            db.execute(
                "UPDATE delivery_operations SET state='unknown', result_json=NULL "
                "WHERE operation_id='ci-ack'"
            )
        recovered = await client.post("/api/operations/ci-ack/reconcile", headers=headers)
        assert recovered.json()["state"] == "succeeded", recovered.text
        assert recovered.json()["result"] == receipt
        with app.state.delivery.store._connect() as db:
            assert db.execute("SELECT COUNT(*) FROM delivery_ci_inventories").fetchone()[0] == 1
            assert not db.execute("SELECT 1 FROM delivery_repair_grants").fetchone()


@pytest.mark.asyncio
async def test_reconcile_uses_only_exact_temporal_domain_receipt(api_fixture):
    import json

    from devflow_temporal.contracts import digest

    path, submit = api_fixture
    app = create_app(path)
    store = app.state.delivery.store
    store.submit(submit)
    payload = {
        "command_id": "original-cancel",
        "expected_revision": 1,
        "reason": "stop",
    }
    result = {"run_id": "run-1", "phase": "cancelling", "revision": 2}
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_mutations "
            "(command_id,run_id,kind,request_digest,state,response_json) VALUES (?,?,?,?,?,?)",
            (
                "original-cancel",
                "run-1",
                "cancel",
                digest(payload),
                "complete",
                json.dumps(result),
            ),
        )
        rejected = {**payload, "command_id": "rejected-cancel"}
        db.execute(
            "INSERT INTO delivery_mutations "
            "(command_id,run_id,kind,request_digest,state,response_json) VALUES (?,?,?,?,?,?)",
            (
                "rejected-cancel",
                "run-1",
                "cancel",
                digest(rejected),
                "rejected",
                json.dumps({"error": "revision changed"}),
            ),
        )
        for op, request in [
            ("matching", payload),
            ("different", {**payload, "reason": "changed"}),
            ("rejected", rejected),
        ]:
            db.execute(
                """INSERT INTO delivery_operations
                (operation_id,run_id,kind,stage,request_digest,request_json,state,
                 created_at,updated_at,progress)
                VALUES (?, 'run-1', 'cancel', 'execution', ?, ?, 'unknown',
                        '2026','2026','unknown')""",
                (
                    op,
                    digest({"run_id": "run-1", "kind": "cancel", "request": request}),
                    json.dumps(request),
                ),
            )
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 1234))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as client:
        token = (await client.get("/api/session")).json()["csrf_token"]
        headers = {"Origin": "http://127.0.0.1:18770", "X-Devflow-CSRF": token}
        same = (await client.post("/api/operations/matching/reconcile", headers=headers)).json()
        different = (
            await client.post("/api/operations/different/reconcile", headers=headers)
        ).json()
        assert same["state"] == "succeeded" and same["result"] == result
        assert different["state"] == "unknown"
        rejected = (await client.post("/api/operations/rejected/reconcile", headers=headers)).json()
        assert rejected["state"] == "refused" and rejected["error"] == "revision changed"


@pytest.mark.asyncio
async def test_submit_receipt_recovers_and_is_listed_on_its_run(api_fixture):
    path, submitted = api_fixture
    app = create_app(path)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 1234))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as client:
        token = (await client.get("/api/session")).json()["csrf_token"]
        headers = {"Origin": "http://127.0.0.1:18770", "X-Devflow-CSRF": token}
        envelope = {
            "operation_id": "submit-1",
            "run_id": None,
            "kind": "submit",
            "request": submitted,
        }
        assert (
            await client.post("/api/operations", json=envelope, headers=headers)
        ).status_code in (200, 202)
        for _ in range(200):
            operation = (await client.get("/api/operations/submit-1")).json()
            if operation["state"] not in ("pending", "running"):
                break
            await asyncio.sleep(0.02)
        assert operation["state"] == "succeeded", operation
        assert operation["run_id"] == submitted["run_id"]
        assert len((await client.get("/api/runs/run-1/operations")).json()["operations"]) == 1
        result = operation["result"]
        with app.state.delivery.store._connect() as db:
            db.execute(
                "UPDATE delivery_operations SET state='unknown',result_json=NULL "
                "WHERE operation_id='submit-1'"
            )
        recovered = (
            await client.post("/api/operations/submit-1/reconcile", headers=headers)
        ).json()
        assert recovered["state"] == "succeeded" and recovered["result"] == result
        assert (await client.post("/api/operations", json=envelope, headers=headers)).json()[
            "result"
        ] == result


def test_status_client_uses_bounded_projection_and_keeps_detail_explicit(monkeypatch):
    from types import SimpleNamespace

    from devflow_temporal.delivery_client import DeliveryClient

    client = DeliveryClient(SimpleNamespace())
    calls = []

    def request(method, path):
        calls.append((method, path))
        return {"version": 1, "source": "persisted_projection", "run": {"run_id": "run-1"}}

    monkeypatch.setattr(client, "_request", request)
    assert client.status("run-1")["source"] == "persisted_projection"
    client.detail("run-1")
    assert calls == [("GET", "/api/runs/run-1/status"), ("GET", "/api/runs/run-1")]


def test_operation_lock_is_shared_by_database_not_service_root(tmp_path):
    import os
    from types import SimpleNamespace

    from devflow_temporal.delivery_operations import _lock

    database = tmp_path / "delivery.sqlite3"
    database.touch()
    first = SimpleNamespace(config=SimpleNamespace(tracking_db=database, state_root=tmp_path / "a"))
    second = SimpleNamespace(
        config=SimpleNamespace(tracking_db=database, state_root=tmp_path / "b")
    )
    lock = _lock(first, "operation-1")
    assert lock is not None
    try:
        assert _lock(second, "operation-1") is None
    finally:
        os.close(lock)
    lock = _lock(second, "operation-1")
    assert lock is not None
    os.close(lock)


@pytest.mark.asyncio
async def test_cancelled_dispatch_waiter_cannot_release_running_body(api_fixture):
    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_operations import _execute, _recover_abandoned, read

    path, _ = api_fixture
    app = create_app(path)
    service = app.state.delivery
    started, release = threading.Event(), threading.Event()
    calls = []

    def slow():
        calls.append("started")
        started.set()
        assert release.wait(10)
        return {"run_id": "run-1"}

    async def endpoint(_request, _run_id):
        return await asyncio.to_thread(slow)

    with service.store._connect() as db:
        db.execute(
            """INSERT INTO delivery_operations
            (operation_id,run_id,kind,stage,request_digest,request_json,state,
             created_at,updated_at,progress)
            VALUES ('cancelled-waiter','run-1','merge-preflight','observation',?, '{}',
                    'pending','2026','2026','queued')""",
            (digest({"run_id": "run-1", "kind": "merge-preflight", "request": {}}),),
        )
    endpoints = {"merge_preflight": endpoint}
    waiter = asyncio.create_task(
        asyncio.to_thread(_execute, service, endpoints, "cancelled-waiter")
    )
    assert await asyncio.to_thread(started.wait, 5)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert not await asyncio.to_thread(_recover_abandoned, service.store, "cancelled-waiter")
    assert read(service.store, "cancelled-waiter")["state"] == "running"
    await asyncio.to_thread(_execute, service, endpoints, "cancelled-waiter")
    assert calls == ["started"]
    release.set()
    for _ in range(200):
        operation = read(service.store, "cancelled-waiter")
        if operation["state"] == "succeeded":
            break
        await asyncio.sleep(0.02)
    assert operation["state"] == "succeeded"
    assert calls == ["started"]


@pytest.mark.asyncio
async def test_network_response_loss_before_and_after_completion(api_fixture, monkeypatch):
    import json
    import socket

    import uvicorn

    path, _ = api_fixture
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    port = listener.getsockname()[1]
    host = f"http://127.0.0.1:{port}"
    config = json.loads(path.read_text())
    config["dashboard_url"] = host
    path.write_text(json.dumps(config))
    started, release = threading.Event(), threading.Event()
    calls = []

    def slow(_store, run_id):
        calls.append(run_id)
        started.set()
        assert release.wait(15)
        return {"run_id": run_id, "observation": "complete"}

    monkeypatch.setattr("devflow_temporal.delivery_merge_followup.preflight", slow)
    app = create_app(path)

    async def dormant_dispatch():
        await asyncio.Event().wait()

    app.state.delivery.dispatch_loop = dormant_dispatch
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    envelope = {
        "operation_id": "socket-loss",
        "run_id": "run-1",
        "kind": "merge-preflight",
        "request": {},
    }
    body = json.dumps(envelope).encode()

    async def send_without_reading():
        _, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(
            (
                f"POST /api/operations HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
                f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n"
                "Connection: close\r\n\r\n"
            ).encode()
            + body
        )
        await writer.drain()
        return writer

    try:
        for _ in range(200):
            if server.started:
                break
            await asyncio.sleep(0.02)
        assert server.started
        first = await send_without_reading()
        assert await asyncio.to_thread(started.wait, 5)
        first.close()
        await first.wait_closed()
        async with httpx.AsyncClient(base_url=host) as client:
            assert (await client.get("/api/operations/socket-loss")).json()["state"] == "running"
            release.set()
            for _ in range(200):
                operation = (await client.get("/api/operations/socket-loss")).json()
                if operation["state"] == "succeeded":
                    break
                await asyncio.sleep(0.02)
            assert operation["result"] == {"run_id": "run-1", "observation": "complete"}
            duplicate = await send_without_reading()
            duplicate.close()
            await duplicate.wait_closed()
            assert (await client.post("/api/operations", json=envelope)).json()[
                "result"
            ] == operation["result"]
            assert calls == ["run-1"]
    finally:
        release.set()
        server.should_exit = True
        await asyncio.wait_for(serving, 10)
        listener.close()
