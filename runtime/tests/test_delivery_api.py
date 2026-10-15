from __future__ import annotations

import asyncio
import json
import subprocess
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker

from devflow_temporal.delivery_activities import (
    _run_role,
    delivery_browser_qa,
    delivery_checks,
    delivery_precheck,
    delivery_prepare,
    delivery_project,
)
from devflow_temporal.delivery_api import DeliveryService, create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_workflow import DeliveryWorkflow


def _git(path: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(path), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.mark.asyncio
async def test_idle_dispatch_reports_actual_temporal_health(monkeypatch):
    class Temporal:
        service_client = None
        healthy = True
        probes = 0

        async def check_health(self):
            self.probes += 1
            return self.healthy

    class Store:
        calls = 0

        def pending_starts(self):
            self.calls += 1
            return []

    temporal = Temporal()
    temporal.service_client = temporal
    store = Store()
    service = object.__new__(DeliveryService)
    service.store = store
    service.temporal_status = "disconnected"
    service._health_client = None

    async def connect():
        return temporal

    monkeypatch.setattr(service, "client", connect)
    await service.dispatch_once()
    assert service.temporal_status == "connected"
    assert temporal.probes == 1
    assert store.calls == 1

    temporal.healthy = False
    with pytest.raises(RuntimeError, match="Temporal health check failed"):
        await service.dispatch_once()
    assert service.temporal_status == "disconnected"
    assert service._health_client is None
    assert store.calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("activity_fn", [delivery_precheck, delivery_checks])
async def test_check_activity_keeps_temporal_loop_responsive(activity_fn, monkeypatch, tmp_path):
    started = threading.Event()
    release = threading.Event()

    class Broker:
        native_cleanup_confirmed = True

        def _native_cancelled(self):
            return False

        def run_prechecks(self, _iteration, _candidate):
            started.set()
            assert release.wait(2)
            return {"state": "passed"}

        run_checks = run_prechecks

    monkeypatch.setattr(
        "devflow_temporal.delivery_activities._context", lambda _spec: (
            SimpleNamespace(config=SimpleNamespace(state_root=tmp_path, raw={})), Broker()
        )
    )
    task = asyncio.create_task(activity_fn(
        {"spec": {"provider": "codex"}, "iteration": 0, "candidate": {}}
    ))
    try:
        beginning = time.monotonic()
        assert await asyncio.to_thread(started.wait, 1)
        await asyncio.sleep(0.02)
        assert time.monotonic() - beginning < 0.5
        assert not task.done()
    finally:
        release.set()
    assert await task == {"state": "passed"}




@pytest.mark.asyncio
@pytest.mark.parametrize('activity_fn', [delivery_precheck, delivery_checks, _run_role])
async def test_check_preparation_failure_keeps_diagnostics_without_claiming_a_launch(
    activity_fn, monkeypatch, tmp_path,
):
    from devflow_temporal.delivery_broker import CheckPreparationFailure

    class Broker:
        native_cleanup_confirmed = True

        def _native_cancelled(self):
            return False

        def run_prechecks(self, _iteration, _candidate):
            raise CheckPreparationFailure('planned-dependencies', ValueError('owned root rejected'))

        run_checks = run_prechecks
        run_implementation_preparation = run_prechecks

        def candidate(self):
            return {'id': 'candidate'}

    monkeypatch.setattr('devflow_temporal.delivery_activities._context', lambda _: (
        SimpleNamespace(config=SimpleNamespace(state_root=tmp_path, raw={})), Broker()))
    monkeypatch.setattr('devflow_temporal.delivery_activities.get_supervisor', lambda _: None)
    monkeypatch.setattr('devflow_temporal.delivery_activities._role_context',
                        lambda request, *_: (request, tmp_path, None, None))
    result = await activity_fn({'spec': {'provider': 'codex',
                                        'policy': {'host_sandbox': 'trusted-local'}},
                               'role': 'implement', 'iteration': 0,
                               'candidate': {'id': 'candidate'}})
    if activity_fn is _run_role:
        assert result['status'] == 'blocked' and result['session_id'] is None
        assert result['findings'] == ['owned root rejected']
        result = result['implementation_preparation']
    assert result['state'] == 'failed' and result['cleanup'] == 'confirmed'
    assert result['source_unchanged'] is True
    assert result['results'][0]['launched'] is False
    assert result['diagnostic'] == 'owned root rejected'


@pytest.fixture
def api_fixture(tmp_path: Path) -> tuple[Path, dict]:
    source = tmp_path / "source"
    source.mkdir()
    (source / "README.md").write_text("Fixture\n")
    _git(source, "init", "-q")
    _git(source, "config", "user.name", "Fixture")
    _git(source, "config", "user.email", "fixture@example.invalid")
    _git(source, "add", "README.md")
    _git(source, "commit", "-qm", "initial")
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--bare", "-q", str(remote)], check=True)
    _git(source, "remote", "add", "origin", str(remote))
    root = Path(__file__).resolve().parents[2]
    config = {
        "version": 1,
        "tracking_db": str(tmp_path / "tracking.sqlite3"),
        "state_root": str(tmp_path / "state"),
        "helpers_dir": str(root / "skills" / "devflow" / "scripts"),
        "codex_bin": "/usr/bin/false",
        "provider": "fake",
        "dashboard_url": "http://127.0.0.1:18770",
        "repositories": {
            "fixture": {
                "source_path": str(source),
                "origin_url": str(remote),
                "github_repo": "example/fixture",
                "base_ref": "HEAD",
                "expected_base_sha": _git(source, "rev-parse", "HEAD"),
                "allowed_paths": ["README.md"],
            }
        },
        "roles": {
            role: {"model": "fake", "effort": "low"} for role in ("implement", "review", "verify")
        },
    }
    path = tmp_path / "service.json"
    path.write_text(json.dumps(config))
    request = {
        "command_id": "submit-1",
        "run_id": "run-1",
        "work_id": "work-1",
        "issue_url": "https://github.com/example/fixture/issues/3",
        "repository_key": "fixture",
        "goal": "Change fixture",
        "accepted_plan": "One bounded edit",
        "base_ref": "HEAD",
        "branch": "feat/fixture",
        "authorized_endpoint": "published_unmerged",
    }
    return path, request




def _foreign_queued_service(api_fixture, key, value):
    path, request = api_fixture
    owner = create_app(path).state.delivery
    raw = json.loads(path.read_text())
    raw[key] = value
    foreign_path = path.with_name("foreign-service.json")
    foreign_path.write_text(json.dumps(raw))
    foreign = create_app(foreign_path).state.delivery
    foreign.store.submit(request)
    return owner, foreign, request




@pytest.mark.asyncio
@pytest.mark.parametrize(
    "key,value", [("temporal_address", "127.0.0.1:17399"), ("queue", "another-queue")]
)
async def test_shared_database_cannot_dispatch_another_service_run(
    api_fixture,
    monkeypatch,
    key,
    value,
):
    owner, foreign, request = _foreign_queued_service(api_fixture, key, value)
    before = foreign.store.detail(request["run_id"])

    async def wrong_transport():
        pytest.fail("foreign outbox selected the wrong service transport")

    monkeypatch.setattr(owner, "healthy_client", wrong_transport)
    await owner.dispatch_once()
    assert foreign.store.detail(request["run_id"]) == before
    assert len(foreign.store.pending_starts()) == 1


def test_foreign_predecessor_readback_rejected_before_temporal_connection(
    api_fixture, monkeypatch,
):
    owner, _foreign, request = _foreign_queued_service(
        api_fixture, "temporal_address", "127.0.0.1:17399",
    )

    async def wrong_transport(*_args, **_kwargs):
        pytest.fail("foreign predecessor was queried through this service transport")

    monkeypatch.setattr("devflow_temporal.delivery_store.Client.connect", wrong_transport)
    with pytest.raises(ValueError, match="service transport ownership"):
        owner.store._completed_temporal_result(request["run_id"])




def _dispatch_snapshot(store, run_ids):
    with store._connect() as db:
        return {
            run_id: {
                "outbox": tuple(db.execute(
                    "SELECT state,attempts,last_error FROM delivery_outbox WHERE run_id=?",
                    (run_id,)).fetchone()),
                "revision": db.execute(
                    "SELECT revision FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()[0],
                "events": db.execute(
                    "SELECT COUNT(*) FROM delivery_events WHERE run_id=?", (run_id,)).fetchone()[0],
            }
            for run_id in run_ids
        }


def _queued_runs(service, request, count):
    run_ids = []
    for number in range(1, count + 1):
        run_id = f"run-{number}"
        service.store.submit({
            **request, "command_id": f"submit-{number}", "run_id": run_id,
            "work_id": f"work-{number}", "branch": f"feat/fixture-{number}",
            "issue_url": f"https://github.com/example/fixture/issues/{number}",
        })
        run_ids.append(run_id)
    return run_ids


class _Description:
    def __init__(self, memo):
        self.memo = memo

    async def memo_value(self, key, default=None):
        return self.memo.get(key, default)


@pytest.mark.asyncio
async def test_poison_row_cannot_starve_newer_runs_or_churn_their_timelines(
    api_fixture, monkeypatch,
):
    path, request = api_fixture
    service = create_app(path).state.delivery
    run_ids = _queued_runs(service, request, 3)
    describe_errors = {"delivery-run-1": RPCStatusCode.INTERNAL}
    started = []

    class Handle:
        def __init__(self, workflow_id):
            self.workflow_id = workflow_id

        async def describe(self):
            if self.workflow_id in describe_errors:
                raise RPCError("deadline exceeded", describe_errors[self.workflow_id], b"")
            if self.workflow_id == "delivery-run-3":
                return _Description({"request_digest": "stale-digest"})
            raise RPCError("not found", RPCStatusCode.NOT_FOUND, b"")

    class Temporal:
        def get_workflow_handle(self, workflow_id):
            return Handle(workflow_id)

        async def start_workflow(self, _workflow, **kwargs):
            started.append(kwargs["id"])

    async def healthy():
        return Temporal()

    clock = [time.time()]
    monkeypatch.setattr(service, "healthy_client", healthy)
    monkeypatch.setattr("devflow_temporal.delivery_api.time",
                        SimpleNamespace(time=lambda: clock[0]))
    before = _dispatch_snapshot(service.store, run_ids)
    await service.dispatch_once()
    # The newer run-2 starts although the older run-1 fails ahead of it.
    assert started == ["delivery-run-2"]
    first = _dispatch_snapshot(service.store, run_ids)
    assert first["run-1"]["outbox"] == ("pending", 1, "RPCError INTERNAL: deadline exceeded")
    assert first["run-1"]["revision"] == before["run-1"]["revision"] + 1
    assert first["run-1"]["events"] == before["run-1"]["events"] + 1
    assert first["run-2"]["outbox"][0] == "sent"
    # A different request holds run-3's workflow ID: the run is blocked once,
    # its outbox entry is terminal, and a new submission supersedes it.
    assert first["run-3"]["outbox"][0] == "conflict"
    assert "belongs to a different request" in first["run-3"]["outbox"][2]
    assert first["run-3"]["revision"] == before["run-3"]["revision"] + 1
    assert first["run-3"]["events"] == before["run-3"]["events"] + 1
    blocked = service.store.detail("run-3")
    assert (blocked["phase"], blocked["outcome"], blocked["execution_state"]) == (
        "blocked", "blocked", "blocked")
    assert "belongs to a different request" in blocked["error"]
    assert blocked["error"].endswith("submit the work again naming this run in supersedes_run_id")
    assert blocked["dispatch"]["state"] == "conflict"
    assert blocked["events"][-1]["type"] == "blocked"
    assert [row["run_id"] for row in service.store.pending_starts()] == ["run-1"]
    superseding = service.store.submit({
        **request, "command_id": "submit-3b", "run_id": "run-3b", "work_id": "work-3",
        "branch": "feat/fixture-3b", "supersedes_run_id": "run-3",
        "issue_url": "https://github.com/example/fixture/issues/3",
    })
    assert superseding["run_id"] == "run-3b"
    assert [row["run_id"] for row in service.store.pending_starts()] == ["run-1", "run-3b"]
    # The first retry is immediate and counts an attempt without a new event;
    # the row then backs off, so further immediate cycles touch nothing.
    for _ in range(3):
        await service.dispatch_once()
    repeated = _dispatch_snapshot(service.store, run_ids)
    assert repeated["run-1"]["outbox"] == ("pending", 2, "RPCError INTERNAL: deadline exceeded")
    assert repeated["run-1"]["revision"] == first["run-1"]["revision"]
    assert repeated["run-1"]["events"] == first["run-1"]["events"]
    assert repeated["run-3"] == first["run-3"]
    # The superseding run starts in the meantime; the blocked one stays retired.
    assert started == ["delivery-run-2", "delivery-run-3b"]
    # A changed reason after the backoff is surfaced once more.
    describe_errors["delivery-run-1"] = RPCStatusCode.UNAVAILABLE
    clock[0] += 25
    await service.dispatch_once()
    changed = _dispatch_snapshot(service.store, run_ids)
    assert changed["run-1"]["outbox"] == ("pending", 3, "RPCError UNAVAILABLE: deadline exceeded")
    assert changed["run-1"]["revision"] == first["run-1"]["revision"] + 1
    assert changed["run-1"]["events"] == first["run-1"]["events"] + 1
    await service.dispatch_once()
    assert _dispatch_snapshot(service.store, run_ids) == changed
    # Once the failure clears, the row starts on its next due cycle.
    describe_errors.clear()
    clock[0] += 45
    await service.dispatch_once()
    assert started == ["delivery-run-2", "delivery-run-3b", "delivery-run-1"]
    assert _dispatch_snapshot(service.store, run_ids)["run-1"]["outbox"][0] == "sent"
    assert service.store.pending_starts() == []


@pytest.mark.asyncio
async def test_rejected_start_is_unknown_for_that_row_only_and_backs_off(
    api_fixture, monkeypatch,
):
    path, request = api_fixture
    service = create_app(path).state.delivery
    run_ids = _queued_runs(service, request, 2)
    started = []

    async def missing():
        raise RPCError("not found", RPCStatusCode.NOT_FOUND, b"")

    class Temporal:
        def get_workflow_handle(self, _workflow_id):
            return SimpleNamespace(describe=missing)

        async def start_workflow(self, _workflow, **kwargs):
            if kwargs["id"] == "delivery-run-1":
                raise RPCError("payload too large", RPCStatusCode.INVALID_ARGUMENT, b"")
            started.append(kwargs["id"])

    async def healthy():
        service.temporal_status = "connected"
        return Temporal()

    clock = [time.time()]
    monkeypatch.setattr(service, "healthy_client", healthy)
    monkeypatch.setattr("devflow_temporal.delivery_api.time",
                        SimpleNamespace(time=lambda: clock[0]))
    await service.dispatch_once()
    assert started == ["delivery-run-2"]
    assert service.temporal_status == "connected"
    first = _dispatch_snapshot(service.store, run_ids)
    # The start was sent, so its acknowledgement is unknown until the memo is read.
    assert first["run-1"]["outbox"] == (
        "unknown", 1, "RPCError INVALID_ARGUMENT: payload too large")
    assert first["run-2"]["outbox"][0] == "sent"
    # The immediate retry reads no memo (not found) and fails the same way; the
    # row then backs off without further timeline noise.
    for _ in range(2):
        await service.dispatch_once()
    again = _dispatch_snapshot(service.store, run_ids)
    assert again["run-1"]["outbox"] == (
        "unknown", 2, "RPCError INVALID_ARGUMENT: payload too large")
    assert again["run-1"]["revision"] == first["run-1"]["revision"]
    assert again["run-1"]["events"] == first["run-1"]["events"]
    clock[0] += 25
    await service.dispatch_once()
    assert _dispatch_snapshot(service.store, run_ids)["run-1"]["outbox"][1] == 3
    assert started == ["delivery-run-2"]










@pytest.mark.asyncio
async def test_tokenless_local_api_csrf_submit_replay_and_conflict(api_fixture):
    path, request = api_fixture
    app = create_app(path)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as browser:
        assert (await browser.get("/api/runs")).json() == {"runs": [], "next_cursor": None}
        token_path = Path(json.loads(path.read_text())["state_root"]) / "service-token"
        assert not token_path.exists()
        assert (
            await browser.post(
                "/api/session",
                json={"token": "unused-legacy-token"},
                headers={"Origin": "http://evil.local"},
            )
        ).status_code == 403
        session = await browser.get("/api/session")
        assert session.status_code == 200
        assert session.headers["cache-control"] == "no-store"
        assert "HttpOnly" in session.headers["set-cookie"]
        assert "SameSite=strict" in session.headers["set-cookie"]
        csrf = session.json()["csrf_token"]
        assert browser.cookies.get("devflow_session")
        assert (
            await browser.post(
                "/api/runs", json=request, headers={"Origin": "http://127.0.0.1:18770"}
            )
        ).status_code == 403
        headers = {"Origin": "http://127.0.0.1:18770", "X-Devflow-CSRF": csrf}
        assert (
            await browser.post(
                "/api/runs", json=request, headers={**headers, "Origin": "http://evil.local"}
            )
        ).status_code == 403
        first = await browser.post("/api/runs", json=request, headers=headers)
        assert first.status_code == 200
        assert first.json()["run_id"] == "run-1"
        assert (
            await browser.post("/api/runs", json=request, headers=headers)
        ).json() == first.json()
        assert (
            await browser.post("/api/runs", json={**request, "goal": "changed"}, headers=headers)
        ).status_code == 409
        detail = (await browser.get("/api/runs/run-1")).json()
        assert detail["run"]["phase"] == "accepted"
        assert detail["events"][0]["type"] == "accepted"
        assert detail["evidence"] == []
        assert (await browser.get("/api/runs/run-1/evidence/not-indexed")).status_code == 404
        assert not token_path.exists()






@pytest.mark.asyncio
@pytest.mark.parametrize(
    "peer,host",
    [("192.0.2.1", "127.0.0.1:18770"), ("127.0.0.1", "localhost:18770")],
)
async def test_tokenless_boundary_rejects_foreign_peers_and_hosts(api_fixture, peer, host):
    path, _request = api_fixture
    app = create_app(path)
    transport = httpx.ASGITransport(app=app, client=(peer, 10001))
    async with httpx.AsyncClient(transport=transport, base_url=f"http://{host}") as browser:
        for endpoint in ("/", "/api/session", "/api/runs", "/api/service"):
            assert (await browser.get(endpoint)).status_code == 403








@pytest.mark.asyncio
async def test_terminal_cancel_is_conflict_without_pending_mutation(api_fixture):
    path, request = api_fixture
    app = create_app(path)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as browser:
        login = await browser.get("/api/session")
        headers = {
            "Origin": "http://127.0.0.1:18770",
            "X-Devflow-CSRF": login.json()["csrf_token"],
        }
        assert (await browser.post("/api/runs", json=request, headers=headers)).status_code == 200
        app.state.delivery.store.project(
            "run-1",
            phase="blocked",
            execution_state="blocked",
            event_type="blocked",
            message="preparation failed",
            outcome="blocked",
        )
        response = await browser.post(
            "/api/runs/run-1/cancel",
            json={"command_id": "cancel-blocked", "expected_revision": 1, "reason": "stop"},
            headers=headers,
        )
        assert response.status_code == 409
        assert "already terminal" in response.json()["detail"]
        with app.state.delivery.store._connect() as db:
            assert db.execute("SELECT COUNT(*) FROM delivery_mutations").fetchone()[0] == 0




@pytest.mark.asyncio
@pytest.mark.parametrize("project_delay", [0, 6])
async def test_public_decision_and_cancel_use_temporal_revision_after_worker_restart(
    api_fixture, tmp_path, project_delay
):
    path, request = api_fixture
    config = json.loads(path.read_text())
    config["repositories"]["fixture"]["initial_decision_prompt"] = "Proceed?"
    path.write_text(json.dumps(config))
    app = create_app(path)
    store = app.state.delivery.store

    @activity.defn(name="delivery_tracker_start")
    async def tracker_start_stub(_payload):
        return {"state": "consistent"}

    @activity.defn(name="delivery_role")
    async def role_stub(payload):
        return {"status": "blocked", "candidate": payload["candidate"]}

    delayed_runs = set()

    @activity.defn(name="delivery_project")
    async def project_fixture(payload):
        # Exercise scheduling beyond the former five-second readiness budget.
        run_id = payload["spec"]["run_id"]
        if run_id not in delayed_runs:
            delayed_runs.add(run_id)
            await asyncio.sleep(project_delay)
        return await delivery_project(payload)

    activities = [project_fixture, delivery_prepare, tracker_start_stub, role_stub]
    async with local_temporal(
        dev_server_database_filename=str(tmp_path / "public-decision.sqlite3")
    ) as environment:

        async def temporal_client():
            return environment.client

        app.state.delivery.client = temporal_client
        transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://127.0.0.1:18770"
        ) as browser:
            login = await browser.get("/api/session")
            headers = {
                "Origin": "http://127.0.0.1:18770",
                "X-Devflow-CSRF": login.json()["csrf_token"],
            }
            for number, action in ((1, "decision"), (2, "cancel")):
                submitted = {
                    **request,
                    "command_id": f"submit-{number}",
                    "run_id": f"run-{number}",
                    "work_id": f"work-{number}",
                    "issue_url": f"https://github.com/example/fixture/issues/{number + 2}",
                    "branch": f"feat/fixture-{number}",
                }
                response = await browser.post("/api/runs", json=submitted, headers=headers)
                assert response.status_code == 200
                queue = f"public-{action}"
                async with Worker(
                    environment.client,
                    task_queue=queue,
                    workflows=[DeliveryWorkflow],
                    activities=activities,
                ):
                    handle = await environment.client.start_workflow(
                        DeliveryWorkflow.run,
                        store.spec(submitted["run_id"]),
                        id="delivery-" + submitted["run_id"],
                        task_queue=queue,
                    )
                    store.mark_start(submitted["run_id"], accepted=True)

                    async def pending_decision(run_id):
                        while not store.detail(run_id)["decisions"]:
                            await asyncio.sleep(0.05)

                    await asyncio.wait_for(pending_decision(submitted["run_id"]), 15)
                # The public command is sent after a worker restart, using the
                # HTTP revision rather than the unrelated SQLite event revision.
                async with Worker(
                    environment.client,
                    task_queue=queue,
                    workflows=[DeliveryWorkflow],
                    activities=activities,
                ):
                    detail = (await browser.get(f"/api/runs/{submitted['run_id']}")).json()["run"]
                    assert detail["phase"] == "waiting_decision"
                    assert detail["revision"] == detail["protocol_revision"]
                    assert detail["projection_revision"] != detail["revision"]
                    if action == "decision":
                        pending = detail["decisions"][0]
                        body = {
                            "command_id": "answer-1",
                            "expected_revision": detail["revision"],
                            "decision_id": pending["id"],
                            "decision_revision": pending["revision"],
                            "candidate_revision": pending["candidate_revision"],
                            "answer": "proceed",
                        }
                        update = await browser.post(
                            f"/api/runs/{submitted['run_id']}/decision",
                            json=body,
                            headers=headers,
                        )
                        expected_outcome = "blocked"
                    else:
                        update = await browser.post(
                            f"/api/runs/{submitted['run_id']}/cancel",
                            json={
                                "command_id": "cancel-2",
                                "expected_revision": detail["revision"],
                                "reason": "stop fixture",
                            },
                            headers=headers,
                        )
                        expected_outcome = "cancelled"
                    assert update.status_code == 200, update.text
                    final = await asyncio.wait_for(handle.result(), 15)
                    assert final["outcome"] == expected_outcome


@pytest.mark.asyncio
@pytest.mark.parametrize("browser_entry_delay", [0, 6])
async def test_public_cancel_remains_responsive_during_blocking_browser_activity(
    api_fixture, tmp_path, monkeypatch, browser_entry_delay
):
    path, request = api_fixture
    config = json.loads(path.read_text())
    config["repositories"]["fixture"]["browser_qa"] = {"id": "owned-browser"}
    path.write_text(json.dumps(config))
    app = create_app(path)
    store = app.state.delivery.store
    entered = threading.Event()
    release = threading.Event()

    def blocking_browser(_broker, _iteration, candidate):
        time.sleep(browser_entry_delay)
        entered.set()
        assert release.wait(timeout=10)
        return {"state": "passed", "cleanup": "confirmed", "candidate_id": candidate["id"]}

    monkeypatch.setattr(DeliveryBroker, "run_browser_qa", blocking_browser)

    @activity.defn(name="delivery_precheck")
    async def precheck_stub(_payload):
        return {"state": "passed"}

    @activity.defn(name="delivery_tracker_start")
    async def tracker_start_stub(_payload):
        return {"state": "consistent"}

    @activity.defn(name="delivery_publish")
    async def publish_stub(payload):
        return {"candidate": payload["candidate"], "head": payload["candidate"]["head"]}

    @activity.defn(name="delivery_role")
    async def role_stub(payload):
        return {
            "status": "pass",
            "candidate": payload["candidate"],
            "session_id": f"fake:{payload['role']}",
        }

    @activity.defn(name="delivery_checks")
    async def checks_stub(_payload):
        return {"state": "passed"}

    async with local_temporal(
        dev_server_database_filename=str(tmp_path / "browser-cancel.sqlite3")
    ) as environment:

        async def temporal_client():
            return environment.client

        app.state.delivery.client = temporal_client
        transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://127.0.0.1:18770"
        ) as browser:
            login = await browser.get("/api/session")
            headers = {
                "Origin": "http://127.0.0.1:18770",
                "X-Devflow-CSRF": login.json()["csrf_token"],
            }
            submitted = await browser.post("/api/runs", json=request, headers=headers)
            assert submitted.status_code == 200
            async with Worker(
                environment.client,
                task_queue="public-browser-cancel",
                workflows=[DeliveryWorkflow],
                activities=[
                    delivery_project,
                    delivery_prepare,
                    precheck_stub,
                    tracker_start_stub,
                    publish_stub,
                    role_stub,
                    checks_stub,
                    delivery_browser_qa,
                ],
            ):
                handle = await environment.client.start_workflow(
                    DeliveryWorkflow.run,
                    store.spec(request["run_id"]),
                    id="delivery-" + request["run_id"],
                    task_queue="public-browser-cancel",
                )
                store.mark_start(request["run_id"], accepted=True)
                try:
                    async def browser_entered():
                        while not entered.is_set():
                            await asyncio.sleep(0.05)

                    await asyncio.wait_for(browser_entered(), 15)
                    detail = (await browser.get("/api/runs/run-1")).json()["run"]
                    assert detail["phase"] == "browser_qa"
                    cancelled = await asyncio.wait_for(
                        browser.post(
                            "/api/runs/run-1/cancel",
                            json={
                                "command_id": "cancel-during-browser",
                                "expected_revision": detail["protocol_revision"],
                                "reason": "stop browser fixture",
                            },
                            headers=headers,
                        ),
                        3,
                    )
                    assert cancelled.status_code == 200, cancelled.text
                    waiting = (await browser.get("/api/runs/run-1")).json()["run"]
                    assert waiting["execution_state"] == "cancelling"
                finally:
                    release.set()
                final = await asyncio.wait_for(handle.result(), 15)
                assert final["outcome"] == "cancelled"
                assert store.detail(request["run_id"])["outcome"] == "cancelled"
