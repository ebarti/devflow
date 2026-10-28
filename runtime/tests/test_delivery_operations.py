from __future__ import annotations

import asyncio
import threading

import httpx
import pytest
from temporalio import workflow
from test_delivery_api import api_fixture as api_fixture
from test_delivery_ci_inventory import ci_case as ci_case
from test_delivery_store import service as service

from devflow_temporal.delivery_api import create_app


@workflow.defn(sandboxed=False)
class OwnedUpdateWorkflow:
    def __init__(self):
        self.updates = 0

    @workflow.run
    async def run(self):
        await workflow.wait_condition(lambda: False)

    @workflow.update(name="decision")
    async def decision(self, _request: dict) -> dict:
        self.updates += 1
        return {"phase": "investigating", "revision": 7}

    @workflow.query(name="update_count")
    def update_count(self) -> int:
        return self.updates


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


@pytest.mark.asyncio
async def test_original_temporal_update_is_reconciled_after_local_receipt_loss(
    api_fixture, monkeypatch, tmp_path
):
    import json

    from temporal_test_server import local_temporal
    from temporalio.worker import Worker

    path, submit = api_fixture
    async with local_temporal(
        dev_server_database_filename=str(tmp_path / "temporal.sqlite3"),
    ) as env:
        config = json.loads(path.read_text())
        config["temporal_address"] = env.client.service_client.config.target_host
        path.write_text(json.dumps(config))
        app = create_app(path)
        service = app.state.delivery
        service.store.submit(submit)
        original_finish = service.store.finish_mutation

        def lose_local_receipt(*_args):
            raise OSError("simulated process loss after remote update")

        monkeypatch.setattr(service.store, "finish_mutation", lose_local_receipt)
        async with Worker(
            env.client, task_queue="operation-update", workflows=[OwnedUpdateWorkflow]
        ):
            original_workflow = await env.client.start_workflow(
                OwnedUpdateWorkflow.run, id="delivery-run-1", task_queue="operation-update"
            )
            transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 1234))
            async with httpx.AsyncClient(
                transport=transport, base_url="http://127.0.0.1:18770"
            ) as client:
                token = (await client.get("/api/session")).json()["csrf_token"]
                headers = {"Origin": "http://127.0.0.1:18770", "X-Devflow-CSRF": token}
                body = {
                    "command_id": "original-decision",
                    "expected_revision": 1,
                    "decision_id": "decision-1",
                    "decision_revision": 1,
                    "candidate_revision": 1,
                    "answer": "continue",
                }
                envelope = {
                    "operation_id": "decision-op",
                    "run_id": "run-1",
                    "kind": "decision",
                    "request": body,
                }
                assert (
                    await client.post("/api/operations", json=envelope, headers=headers)
                ).status_code in (200, 202)
                for _ in range(500):
                    operation = (await client.get("/api/operations/decision-op")).json()
                    if operation["state"] not in ("pending", "running"):
                        break
                    await asyncio.sleep(0.02)
                assert operation["state"] == "unknown", operation
                assert len(operation["effect_refs"]) == 1
                reference = operation["effect_refs"][0]
                assert reference["type"] == "temporal_update"
                assert reference["workflow_id"] == "delivery-run-1"
                assert reference["workflow_run_id"]
                assert reference["command_id"] == "original-decision"
                assert reference["name"] == "decision"
                assert reference["request_digest"] == operation["request_digest"]
                original_address = service.config.raw["temporal_address"]
                service.config.raw["temporal_address"] = "127.0.0.1:1"
                different_transport = (
                    await client.post("/api/operations/decision-op/reconcile", headers=headers)
                ).json()
                assert different_transport["state"] == "unknown"
                service.config.raw["temporal_address"] = original_address
                # The current projection may subsequently identify a different
                # workflow; the recorded original execution remains the only
                # authorized receipt to query.
                with service.store._connect() as db:
                    db.execute(
                        "UPDATE delivery_runs SET workflow_id='delivery-successor' "
                        "WHERE run_id='run-1'"
                    )
                aliases = await asyncio.gather(
                    *[
                        client.post(
                            "/api/operations",
                            json={**envelope, "operation_id": f"alias-{i}"},
                            headers=headers,
                        )
                        for i in range(4)
                    ]
                )
                assert all(item.status_code == 202 for item in aliases)
                assert all(item.json()["operation_id"] == "decision-op" for item in aliases)
                changed = {
                    **envelope,
                    "operation_id": "changed-op",
                    "request": {**body, "answer": "changed"},
                }
                assert (
                    await client.post("/api/operations", json=changed, headers=headers)
                ).status_code == 409
                other_kind = await client.post(
                    "/api/operations",
                    headers=headers,
                    json={
                        "operation_id": "other-kind",
                        "run_id": "run-1",
                        "kind": "cancel",
                        "request": {
                            "command_id": body["command_id"],
                            "expected_revision": 1,
                            "reason": "stop",
                        },
                    },
                )
                assert other_kind.status_code == 409
                direct = await client.post("/api/runs/run-1/decision", json=body, headers=headers)
                assert direct.status_code == 409
                monkeypatch.setattr(service.store, "finish_mutation", original_finish)
                recovered = (
                    await client.post("/api/operations/decision-op/reconcile", headers=headers)
                ).json()
                assert recovered["state"] == "succeeded", recovered
                assert recovered["result"] == {
                    "run_id": "run-1",
                    "phase": "investigating",
                    "revision": 7,
                }
                with service.store._connect() as db:
                    row = db.execute(
                        "SELECT state,response_json FROM delivery_mutations "
                        "WHERE command_id='original-decision'"
                    ).fetchone()
                    assert row["state"] == "complete"
                assert await original_workflow.query("update_count") == 1

                # An absent original update is not proof that an effect is safe
                # to replay. Querying it leaves the new operation unknown.
                from devflow_temporal.contracts import digest

                missing_body = {**body, "command_id": "missing-decision"}
                missing_digest = digest(
                    {
                        "run_id": "run-1",
                        "kind": "decision",
                        "request": missing_body,
                    }
                )
                missing_ref = {
                    **reference,
                    "command_id": "missing-decision",
                    "request_digest": missing_digest,
                }
                service.store.begin_mutation("run-1", "missing-decision", "decision", missing_body)
                with service.store._connect() as db:
                    db.execute(
                        "INSERT INTO delivery_operations "
                        "(operation_id,run_id,kind,stage,request_digest,request_json,state,"
                        "created_at,updated_at,progress,effect_refs) "
                        "VALUES ('missing-op','run-1','decision','execution',?,?,'unknown',"
                        "'2026','2026','unknown',?)",
                        (missing_digest, json.dumps(missing_body), json.dumps([missing_ref])),
                    )
                missing = (
                    await client.post("/api/operations/missing-op/reconcile", headers=headers)
                ).json()
                assert missing["state"] == "unknown"
                assert await original_workflow.query("update_count") == 1


@pytest.mark.asyncio
async def test_running_owner_loss_is_discovered_by_status_and_run_list(api_fixture):
    import os

    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_operations import _lock

    path, _ = api_fixture
    app = create_app(path)
    store = app.state.delivery.store
    with store._connect() as db:
        for op in ("lost-status", "lost-list"):
            db.execute(
                """INSERT INTO delivery_operations
                (operation_id,run_id,kind,stage,request_digest,request_json,state,
                 created_at,updated_at,progress)
                VALUES (?,'run-1','merge-preflight','observation',?,'{}','running',
                        '2026','2026','running')""",
                (op, digest({"run_id": "run-1", "kind": "merge-preflight", "request": {}})),
            )
    first, second = _lock(store, "lost-status"), _lock(store, "lost-list")
    assert first is not None and second is not None
    try:
        await app.state.delivery.resume_pending_operations()
        transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 1234))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://127.0.0.1:18770"
        ) as client:
            assert (await client.get("/api/operations/lost-status")).json()["state"] == "running"
            os.close(first)
            first = None
            assert (await client.get("/api/operations/lost-status")).json()["state"] == "unknown"
            os.close(second)
            second = None
            records = (await client.get("/api/runs/run-1/operations")).json()["operations"]
            assert {item["state"] for item in records} == {"unknown"}
    finally:
        if first is not None:
            os.close(first)
        if second is not None:
            os.close(second)


@pytest.mark.asyncio
async def test_concurrent_operation_aliases_share_one_domain_command(api_fixture, monkeypatch):
    path, submit = api_fixture
    app = create_app(path)
    entered, release = threading.Event(), threading.Event()
    calls = []

    def slow(request):
        calls.append(request)
        entered.set()
        assert release.wait(10)
        return {"run_id": request["run_id"], "phase": "accepted"}

    monkeypatch.setattr(app.state.delivery.store, "submit", slow)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 1234))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as client:
        token = (await client.get("/api/session")).json()["csrf_token"]
        headers = {"Origin": "http://127.0.0.1:18770", "X-Devflow-CSRF": token}

        async def create(index):
            return await client.post(
                "/api/operations",
                headers=headers,
                json={
                    "operation_id": f"submit-alias-{index}",
                    "run_id": None,
                    "kind": "submit",
                    "request": submit,
                },
            )

        try:
            responses = await asyncio.gather(*[create(i) for i in range(5)])
            assert all(response.status_code == 202 for response in responses)
            assert len({response.json()["operation_id"] for response in responses}) == 1
            assert await asyncio.to_thread(entered.wait, 5)
            changed = await client.post(
                "/api/operations",
                headers=headers,
                json={
                    "operation_id": "submit-changed",
                    "run_id": None,
                    "kind": "submit",
                    "request": {**submit, "goal": "different"},
                },
            )
            assert changed.status_code == 409
            assert len(calls) == 1
        finally:
            release.set()
        selected = responses[0].json()["operation_id"]
        for _ in range(200):
            operation = (await client.get("/api/operations/" + selected)).json()
            if operation["state"] == "succeeded":
                break
            await asyncio.sleep(0.02)
        assert operation["state"] == "succeeded" and len(calls) == 1


@pytest.mark.asyncio
async def test_new_operation_cannot_resend_prior_unowned_temporal_mutation(api_fixture):
    path, submit = api_fixture
    app = create_app(path)
    store = app.state.delivery.store
    store.submit(submit)
    body = {
        "command_id": "old-uncertain",
        "expected_revision": 1,
        "decision_id": "decision-1",
        "decision_revision": 1,
        "candidate_revision": 1,
        "answer": "continue",
    }
    store.begin_mutation("run-1", body["command_id"], "decision", body)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 1234))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as client:
        token = (await client.get("/api/session")).json()["csrf_token"]
        headers = {"Origin": "http://127.0.0.1:18770", "X-Devflow-CSRF": token}
        envelope = {
            "operation_id": "new-op",
            "run_id": "run-1",
            "kind": "decision",
            "request": body,
        }
        await client.post("/api/operations", json=envelope, headers=headers)
        for _ in range(200):
            operation = (await client.get("/api/operations/new-op")).json()
            if operation["state"] not in ("pending", "running"):
                break
            await asyncio.sleep(0.02)
        assert operation["state"] == "unknown"
        assert "prior domain mutation is uncertain" in operation["error"]
        assert operation["effect_refs"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("cutpoint", ["running", "succeeded"])
async def test_public_process_restart_preserves_operation_outcome(
    api_fixture, tmp_path, unused_tcp_port, cutpoint
):
    import json
    import os
    import subprocess
    import sys

    path, _ = api_fixture
    config = json.loads(path.read_text())
    port = unused_tcp_port
    host = f"http://127.0.0.1:{port}"
    config["dashboard_url"] = host
    path.write_text(json.dumps(config))
    marker = tmp_path / "handler-invocations"
    script = tmp_path / "owned_api.py"
    script.write_text("""
import asyncio
import sys
import time
from pathlib import Path
import uvicorn
from devflow_temporal.delivery_api import create_app
from devflow_temporal import delivery_merge_followup
path, marker, cutpoint, port = sys.argv[1:]
def observed(_store, run_id):
    with open(marker, 'a', encoding='utf-8') as stream:
        stream.write('one\\n')
        stream.flush()
    if cutpoint == 'running':
        time.sleep(30)
    return {'run_id': run_id, 'observation': 'complete'}
delivery_merge_followup.preflight = observed
app = create_app(Path(path))
async def dormant():
    await asyncio.Event().wait()
app.state.delivery.dispatch_loop = dormant
uvicorn.run(app, host='127.0.0.1', port=int(port), log_level='error')
""")
    child = None

    async def start():
        process = subprocess.Popen(
            [sys.executable, str(script), str(path), str(marker), cutpoint, str(port)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=os.environ.copy(),
        )
        async with httpx.AsyncClient(base_url=host, timeout=1) as client:
            for _ in range(200):
                if process.poll() is not None:
                    raise AssertionError(f"owned API exited {process.returncode}")
                try:
                    if (await client.get("/api/session")).status_code == 200:
                        return process
                except httpx.TransportError:
                    pass
                await asyncio.sleep(0.05)
        process.kill()
        process.wait(timeout=5)
        raise AssertionError("owned API did not start")

    def stop(process):
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=10)

    envelope = {
        "operation_id": "restart-op",
        "run_id": "run-1",
        "kind": "merge-preflight",
        "request": {},
    }
    try:
        child = await start()
        async with httpx.AsyncClient(base_url=host, timeout=3) as client:
            assert (await client.post("/api/operations", json=envelope)).status_code in (200, 202)
            for _ in range(200):
                operation = (await client.get("/api/operations/restart-op")).json()
                if operation["state"] == cutpoint and marker.exists():
                    break
                await asyncio.sleep(0.02)
            assert operation["state"] == cutpoint
        stop(child)
        child = await start()
        async with httpx.AsyncClient(base_url=host, timeout=3) as client:
            recovered = (await client.get("/api/operations/restart-op")).json()
            assert recovered["state"] == ("unknown" if cutpoint == "running" else "succeeded")
            assert (await client.post("/api/operations", json=envelope)).json()[
                "operation_id"
            ] == "restart-op"
            if cutpoint == "succeeded":
                assert recovered["result"] == {"run_id": "run-1", "observation": "complete"}
        assert marker.read_text().splitlines() == ["one"]
    finally:
        stop(child)


@pytest.mark.asyncio
async def test_mismatched_admission_receipt_stays_unknown(api_fixture):
    import json

    from devflow_temporal.contracts import digest

    path, submit = api_fixture
    app = create_app(path)
    store = app.state.delivery.store
    store.submit(submit)
    body = {"command_id": "ambiguous-command", "expected_revision": 3}
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_commands VALUES (?,?,?,?)",
            (
                body["command_id"],
                "run-1",
                digest({"run_id": "run-1", **body}),
                json.dumps({"run_id": "run-1", "phase": "scope_amendment_queued"}),
            ),
        )
        db.execute(
            """INSERT INTO delivery_operations
            (operation_id,run_id,kind,stage,request_digest,request_json,state,
             created_at,updated_at,progress,domain_command_id)
            VALUES ('ambiguous-op','run-1','retry-prelaunch','admission',?,?,
                    'unknown','2026','2026','unknown',?)""",
            (
                digest({"run_id": "run-1", "kind": "retry-prelaunch", "request": body}),
                json.dumps(body),
                body["command_id"],
            ),
        )
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 1234))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as client:
        token = (await client.get("/api/session")).json()["csrf_token"]
        headers = {"Origin": "http://127.0.0.1:18770", "X-Devflow-CSRF": token}
        result = (
            await client.post("/api/operations/ambiguous-op/reconcile", headers=headers)
        ).json()
        assert result["state"] == "unknown"


@pytest.mark.asyncio
async def test_feature_continuation_receipt_authenticates_the_original_predecessor(api_fixture):
    import json

    from devflow_temporal.contracts import digest

    path, submit = api_fixture
    app = create_app(path)
    store = app.state.delivery.store
    store.submit(submit)
    body = {"command_id": "feature-continuation", "expected_revision": 3}
    successor = "run-feature-" + digest({"run": "run-1", "command": body})[:20]
    response = {"run_id": successor, "phase": "accepted"}
    with store._connect() as db:
        original = db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone()
        spec = json.loads(original["request_json"])
        spec["feature_delivery"] = {
            "owner": {
                "run_id": "run-1",
                "issue_id": "issue-1",
                "store_path": "fixture-store",
                "generation": 1,
            }
        }
        db.execute(
            "UPDATE delivery_runs SET request_json=? WHERE run_id='run-1'", (json.dumps(spec),)
        )
        spec.update(run_id=successor)
        # A supported store relocation may change the operational path while
        # the immutable issue and owner-generation lineage remain intact.
        spec["feature_delivery"]["owner"].update(
            run_id=successor, generation=2, store_path="relocated-store"
        )
        db.execute(
            """INSERT INTO delivery_runs
            (run_id,request_digest,request_json,work_id,issue_url,repository_key,phase,
             execution_state,revision,created_at,updated_at)
            VALUES (?,?,?,?,?,?,'accepted','queued',1,'2026','2026')""",
            (
                successor,
                "fixture",
                json.dumps(spec),
                original["work_id"],
                original["issue_url"],
                original["repository_key"],
            ),
        )
        db.execute(
            "INSERT INTO delivery_commands VALUES (?,?,?,?)",
            (body["command_id"], successor, "original-domain-digest", json.dumps(response)),
        )
        db.execute(
            """INSERT INTO delivery_operations
            (operation_id,run_id,kind,stage,request_digest,request_json,state,
             created_at,updated_at,progress,domain_command_id)
            VALUES ('feature-op','run-1','continue-feature','admission',?,?,
                    'unknown','2026','2026','unknown',?)""",
            (
                digest({"run_id": "run-1", "kind": "continue-feature", "request": body}),
                json.dumps(body),
                body["command_id"],
            ),
        )
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 1234))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as client:
        token = (await client.get("/api/session")).json()["csrf_token"]
        headers = {"Origin": "http://127.0.0.1:18770", "X-Devflow-CSRF": token}
        result = (await client.post("/api/operations/feature-op/reconcile", headers=headers)).json()
        assert result["state"] == "succeeded" and result["result"] == response


@pytest.mark.asyncio
async def test_real_feature_continuation_receipt_recovers_original_successor(service, monkeypatch):
    import json

    from test_delivery_feature_execution import feature_service

    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_feature_execution import continue_feature, registry

    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    token = spec["feature_delivery"]["owner"]
    registry(spec).stop(token, "stopped", {})
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET phase='cancelled',outcome='cancelled',"
            "execution_state='terminal' WHERE run_id=?",
            (spec["run_id"],),
        )
        store.state.release_work(db, spec["work_id"], "external:devflow:" + spec["run_id"])
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_: {"result": {}})
    body = {"command_id": "original-feature-command", "expected_revision": 1}
    response = continue_feature(store, spec["run_id"], body)
    original_owner = registry(spec).current(token["issue_id"])
    app = create_app(store.config.path)
    with store._connect() as db:
        db.execute(
            """INSERT INTO delivery_operations
            (operation_id,run_id,kind,stage,request_digest,request_json,state,
             created_at,updated_at,progress,domain_command_id)
            VALUES ('feature-receipt',?,'continue-feature','admission',?,?,
                    'unknown','2026','2026','unknown',?)""",
            (
                spec["run_id"],
                digest({"run_id": spec["run_id"], "kind": "continue-feature", "request": body}),
                json.dumps(body),
                body["command_id"],
            ),
        )
    host = store.config.dashboard_url
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 1234))
    async with httpx.AsyncClient(transport=transport, base_url=host) as client:
        csrf = (await client.get("/api/session")).json()["csrf_token"]
        headers = {"Origin": host, "X-Devflow-CSRF": csrf}
        result = (
            await client.post("/api/operations/feature-receipt/reconcile", headers=headers)
        ).json()
        assert result["state"] == "succeeded" and result["result"] == response
    assert registry(spec).current(token["issue_id"]) == original_owner
    with store._connect() as db:
        assert (
            db.execute(
                "SELECT COUNT(*) FROM delivery_commands WHERE command_id=?", (body["command_id"],)
            ).fetchone()[0]
            == 1
        )


def test_admission_receipt_rejects_merged_run_id_digest_alias(api_fixture):
    import json

    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_operation_receipts import receipt

    path, submit = api_fixture
    store = create_app(path).state.delivery.store
    store.submit(submit)
    valid = {"command_id": "collision-command", "expected_revision": 3}
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_commands VALUES (?,?,?,?)",
            (
                valid["command_id"],
                "run-1",
                digest({"run_id": "run-1", **valid}),
                json.dumps({"run_id": "run-1", "phase": "repair_prelaunch_retry_queued"}),
            ),
        )
    # Merging the caller's extra run_id into the digest would otherwise make
    # this malformed request indistinguishable from the stored valid one.
    malformed = {**valid, "run_id": "run-1"}
    assert (
        receipt(store, {"kind": "retry-prelaunch", "run_id": "run-1", "request": malformed}) is None
    )
