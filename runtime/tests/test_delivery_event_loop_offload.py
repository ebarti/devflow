"""Store work must leave the API and worker event loops free to serve others."""
from __future__ import annotations

import asyncio
import threading

import httpx
import pytest
from temporalio.testing import ActivityEnvironment
from test_delivery_api import api_fixture as api_fixture

from devflow_temporal import delivery_activities
from devflow_temporal.delivery_api import create_app


def _off_loop() -> None:
    assert threading.current_thread() is not threading.main_thread(), "store call on the loop"
    with pytest.raises(RuntimeError):
        asyncio.get_running_loop()


@pytest.mark.asyncio
async def test_api_reads_and_streams_off_the_event_loop(api_fixture, monkeypatch):
    path, request = api_fixture
    app = create_app(path)
    store = app.state.delivery.store
    store.submit(request)
    observed = []
    for name in ("detail", "evidence_index", "evidence", "spec", "feature", "events"):
        original = getattr(store, name)

        def wrapped(*args, _name=name, _original=original, **kwargs):
            _off_loop()
            observed.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(store, name, wrapped)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://127.0.0.1:18770") as browser:
        assert (await browser.get("/api/runs/run-1")).status_code == 200
        assert (await browser.get("/api/runs/run-1/evidence/missing")).status_code == 404
    # Nested store reads (detail -> spec) are recorded too; every one ran off the loop.
    assert {"detail", "evidence_index", "evidence"} <= set(observed)
    before_stream = len(observed)

    # Stream one event through the real ASGI route, then disconnect the client.
    disconnected = asyncio.Event()
    messages = []
    received = False

    async def receive():
        nonlocal received
        if not received:
            received = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await disconnected.wait()
        return {"type": "http.disconnect"}

    async def send(message):
        messages.append(message)
        if message["type"] == "http.response.body" and message.get("body"):
            disconnected.set()

    await asyncio.wait_for(
        app(
            {
                "type": "http",
                "asgi": {"version": "3.0", "spec_version": "2.3"},
                "http_version": "1.1",
                "method": "GET",
                "scheme": "http",
                "path": "/api/runs/run-1/events",
                "raw_path": b"/api/runs/run-1/events",
                "query_string": b"",
                "root_path": "",
                "client": ("127.0.0.1", 10001),
                "server": ("127.0.0.1", 18770),
                "headers": [(b"host", b"127.0.0.1:18770")],
            },
            receive,
            send,
        ),
        timeout=2,
    )
    assert messages[0]["status"] == 200
    assert {"spec", "feature", "events"} <= set(observed[before_stream:])


@pytest.mark.asyncio
async def test_activities_build_their_store_off_the_event_loop(monkeypatch):
    class Store:
        def effective_spec(self, run_id):
            return {"run_id": run_id, "policy": {}, "provider": "codex"}

        def accept_intake_plan(self, run_id, revision, plan_digest, plan, *, authorization=None):
            return {"run_id": run_id, "revision": revision}

    class Broker:
        async def checks(self, pull_request):
            return {"state": "passed", "pull_request": pull_request}

    built = []

    def store_factory(_config):
        _off_loop()
        built.append("store")
        return Store()

    def context(_spec, **_kwargs):
        _off_loop()
        built.append("context")
        return Store(), Broker()

    monkeypatch.setattr(delivery_activities, "DeliveryStore", store_factory)
    monkeypatch.setattr(delivery_activities, "load_execution_config", lambda _spec: object())
    monkeypatch.setattr(delivery_activities, "_context", context)
    monkeypatch.setattr(delivery_activities, "_tracker_sync",
                        lambda *_args, **_kwargs: {"state": "consistent", "pending": False})
    monkeypatch.setattr("devflow_temporal.delivery_merge.merge_verified",
                        lambda _store, _spec, _request, _cancelled: {"merged": True})
    monkeypatch.setattr("devflow_temporal.delivery_terminal_recovery.published_readback",
                        lambda *_args, **_kwargs: None)
    environment = ActivityEnvironment()
    spec = {"run_id": "run-1", "policy": {}, "provider": "codex"}
    accepted = await environment.run(delivery_activities.delivery_accept_plan, {
        "spec": spec, "plan_revision": 1, "plan_digest": "digest", "plan": {}})
    assert accepted == {"run_id": "run-1", "revision": 1}
    checks = await environment.run(delivery_activities.delivery_ci,
                                   {"spec": spec, "pull_request": {"number": 1}})
    assert checks["state"] == "passed"
    merged = await environment.run(delivery_activities.delivery_merge, {"spec": spec})
    assert merged == {"merged": True}
    tracked = await environment.run(delivery_activities.delivery_terminal_tracker, {
        "spec": spec, "status": "done", "pull_request": {"number": 1}, "release": True})
    assert tracked == {"state": "consistent", "pending": False}
    assert built == ["store", "context", "context", "context"]


@pytest.mark.asyncio
async def test_maintenance_passes_read_the_store_off_the_event_loop(api_fixture, monkeypatch):
    from types import SimpleNamespace

    from temporalio.client import WorkflowExecutionStatus

    from devflow_temporal.delivery_closed_projection import reconcile_completed_projections
    from devflow_temporal.delivery_orphans import reconcile_closed_native

    path, request = api_fixture
    # This pure fixture must not contend with parallel installer tests or the
    # user's real installation-recovery lease.
    import json

    raw = json.loads(path.read_text())
    raw["execution_registry"] = str(path.parent / "ownership/registry.sqlite3")
    path.write_text(json.dumps(raw))
    app = create_app(path)
    store = app.state.delivery.store
    store.submit(request)
    with store._connect() as db:
        # An unfinished attempt makes the native orphan pass inspect this run.
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,"
                   "state,result_path) VALUES (?,?,?,?,?,'queued',?)",
                   ("q" * 64, "run-1", "implement", 0, "candidate", "/tmp/result.json"))
    observed = []
    for name in ("_connect", "effective_spec", "owns_execution", "submitted_spec", "spec"):
        original = getattr(store, name)

        def wrapped(*args, _name=name, _original=original, **kwargs):
            _off_loop()
            observed.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(store, name, wrapped)

    class Handle:
        async def describe(self):
            return SimpleNamespace(status=WorkflowExecutionStatus.RUNNING, close_time=None)

    class Client:
        namespace = "default"
        service_client = SimpleNamespace(
            config=SimpleNamespace(target_host=store.config.temporal_address))

        def get_workflow_handle(self, _workflow_id, run_id=None):
            return Handle()

    results = await reconcile_completed_projections(store, Client())
    assert results == [{"run_id": "run-1", "state": "not_completed", "status": "RUNNING",
                        "closed": False}]
    assert {"_connect", "effective_spec", "owns_execution"} <= set(observed)
    observed.clear()
    await reconcile_closed_native(store, Client())
    assert {"_connect", "effective_spec"} <= set(observed)
    observed.clear()
    await app.state.delivery.reproject_features_once()
    assert observed and set(observed) == {"_connect"}
