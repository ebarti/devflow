from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
import threading
import time
from pathlib import Path

import httpx
import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from devflow_temporal.delivery_activities import (
    delivery_browser_qa,
    delivery_checks,
    delivery_precheck,
    delivery_prepare,
    delivery_project,
)
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_container import ContainerUnknown
from devflow_temporal.delivery_workflow import DeliveryWorkflow


def _git(path: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(path), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.mark.asyncio
@pytest.mark.parametrize("activity_fn", [delivery_precheck, delivery_checks])
async def test_contained_check_activity_keeps_temporal_loop_responsive(activity_fn, monkeypatch):
    started = threading.Event()
    release = threading.Event()

    class Broker:
        def run_prechecks(self, _iteration, _candidate):
            started.set()
            assert release.wait(2)
            return {"state": "passed"}

        run_checks = run_prechecks

    monkeypatch.setattr(
        "devflow_temporal.delivery_activities._context", lambda _spec: (None, Broker())
    )
    task = asyncio.create_task(activity_fn({"spec": {}, "iteration": 0, "candidate": {}}))
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
@pytest.mark.parametrize("activity_fn", [delivery_precheck, delivery_checks, delivery_browser_qa])
async def test_contained_effect_uncertainty_is_returned_for_durable_projection(
    activity_fn, monkeypatch
):
    class Broker:
        def run_prechecks(self, _iteration, _candidate):
            raise ContainerUnknown("Docker inspection became unavailable")

        run_checks = run_prechecks
        run_browser_qa = run_prechecks

    monkeypatch.setattr(
        "devflow_temporal.delivery_activities._context", lambda _spec: (None, Broker())
    )
    result = await activity_fn(
        {"spec": {"provider": "codex"}, "iteration": 0, "candidate": {"id": "candidate"}}
    )
    assert result == {
        "state": "unknown",
        "cleanup": "unknown",
        "candidate_id": "candidate",
        "reason": "ContainerUnknown",
    }


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


@pytest.mark.asyncio
async def test_local_api_auth_csrf_submit_replay_and_conflict(api_fixture):
    path, request = api_fixture
    app = create_app(path)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as browser:
        assert (await browser.get("/api/session")).json() == {"authenticated": False}
        assert (await browser.get("/api/runs")).status_code == 401
        token = (
            (Path(json.loads(path.read_text())["state_root"]) / "service-token").read_text().strip()
        )
        assert (
            await browser.post(
                "/api/session", json={"token": token}, headers={"Origin": "http://evil.local"}
            )
        ).status_code == 403
        login = await browser.post(
            "/api/session", json={"token": token}, headers={"Origin": "http://127.0.0.1:18770"}
        )
        assert login.status_code == 200
        csrf = login.json()["csrf_token"]
        assert browser.cookies.get("devflow_session")
        assert (
            await browser.post(
                "/api/runs", json=request, headers={"Origin": "http://127.0.0.1:18770"}
            )
        ).status_code == 403
        headers = {"Origin": "http://127.0.0.1:18770", "X-Devflow-CSRF": csrf}
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


@pytest.mark.asyncio
@pytest.mark.parametrize("binding_source", ["effect", "projection"])
async def test_public_evidence_reads_contained_role_and_browser_logs(api_fixture, binding_source):
    path, request = api_fixture
    app = create_app(path)
    store = app.state.delivery.store
    store.submit(request)
    root = Path(store.spec("run-1")["state_dir"])
    role_log = root / "attempts" / "implement-0" / "container" / "container.log"
    role_log.parent.mkdir(parents=True)
    role_log.write_text("contained role output\n")
    role_sha = hashlib.sha256(role_log.read_bytes()).hexdigest()
    with store._connect() as db:
        db.execute(
            """INSERT INTO delivery_attempts
               (job_key,run_id,role,iteration,candidate_id,state,result_json,cleanup)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                "implement-0",
                "run-1",
                "implement",
                0,
                "candidate-1",
                "finished",
                json.dumps({"container_log_sha256": role_sha}),
                "confirmed",
            ),
        )
    browser_folder = root / "browser-qa" / "0"
    browser_log = browser_folder / "container" / "container.log"
    browser_log.parent.mkdir(parents=True)
    browser_log.write_text("4 passed\n")
    browser_sha = hashlib.sha256(browser_log.read_bytes()).hexdigest()
    receipt = browser_folder / "receipt.json"
    saved_receipt = {
        "iteration": 0,
        "log": str(browser_log),
        "log_sha256": browser_sha,
        "state": "passed",
        "test_count": 4,
    }
    receipt.write_text(json.dumps(saved_receipt))
    receipt_sha = hashlib.sha256(receipt.read_bytes()).hexdigest()
    bound_result = {**saved_receipt, "receipt": str(receipt), "receipt_sha256": receipt_sha}
    if binding_source == "effect":
        with store._connect() as db:
            db.execute(
                """INSERT INTO delivery_effects
                   (effect_key,run_id,kind,request_json,state,observed_json,updated_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    "browser_qa:run-1:0",
                    "run-1",
                    "browser_qa",
                    "{}",
                    "complete",
                    json.dumps(bound_result),
                    "2026-09-26T00:00:00Z",
                ),
            )
    else:
        store.project(
            "run-1",
            phase="accepted",
            execution_state="queued",
            event_type="browser_receipt_bound",
            message="Owned browser receipt projected",
            checks={"browser_qa": bound_result},
            key="browser_receipt_bound:0",
        )
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as browser:
        token = (Path(json.loads(path.read_text())["state_root"]) / "service-token").read_text()
        assert (
            await browser.post(
                "/api/session",
                json={"token": token.strip()},
                headers={"Origin": "http://127.0.0.1:18770"},
            )
        ).status_code == 200
        listed = (await browser.get("/api/runs/run-1")).json()["evidence"]
        assert {item["id"] for item in listed} == {
            "role-implement-0",
            "browser-qa-0-log",
            "browser-qa-0-receipt",
        }
        role = (await browser.get("/api/runs/run-1/evidence/role-implement-0")).json()
        qa = (await browser.get("/api/runs/run-1/evidence/browser-qa-0-log")).json()
        assert role["text"] == "contained role output\n"
        assert role["sha256"] == role_sha
        assert qa["text"] == "4 passed\n"
        assert qa["sha256"] == browser_sha
        qa_receipt_url = "/api/runs/run-1/evidence/browser-qa-0-receipt"
        receipt.write_text(json.dumps({**saved_receipt, "state": "failed", "test_count": 0}))
        assert (await browser.get(qa_receipt_url)).status_code == 404
        assert (await browser.get("/api/runs/run-1/evidence/browser-qa-0-log")).status_code == 404
        receipt.write_text(json.dumps(saved_receipt))
        role_log.write_text("changed after result\n")
        browser_log.write_text("changed after receipt\n")
        assert (await browser.get("/api/runs/run-1/evidence/role-implement-0")).status_code == 404
        assert (await browser.get("/api/runs/run-1/evidence/browser-qa-0-log")).status_code == 404
        role_log.unlink()
        (role_log.parent.parent / "process.log").write_text("unbound native fallback\n")
        assert (await browser.get("/api/runs/run-1/evidence/role-implement-0")).status_code == 404
        receipt.write_text(
            json.dumps(
                {
                    **saved_receipt,
                    "log_sha256": hashlib.sha256(browser_log.read_bytes()).hexdigest(),
                }
            )
        )
        assert (await browser.get(qa_receipt_url)).status_code == 404
        assert (await browser.get("/api/runs/run-1/evidence/browser-qa-0-log")).status_code == 404
        receipt.write_text(json.dumps(saved_receipt))
        browser_log.unlink()
        (browser_folder / "browser-qa.log").write_text("unbound browser fallback\n")
        assert (await browser.get("/api/runs/run-1/evidence/browser-qa-0-log")).status_code == 404


@pytest.mark.asyncio
async def test_public_evidence_keeps_historical_codex_logs(api_fixture):
    path, request = api_fixture
    app = create_app(path)
    store = app.state.delivery.store
    store.submit(request)
    root = Path(store.spec("run-1")["state_dir"])
    with store._connect() as db:
        frozen = json.loads(
            db.execute("SELECT request_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0]
        )
        frozen["provider"] = "codex"  # A run created before contained Codex execution.
        db.execute(
            "UPDATE delivery_runs SET request_json=? WHERE run_id='run-1'", (json.dumps(frozen),)
        )
        for role in ("implement", "review"):
            db.execute(
                """INSERT INTO delivery_attempts
                   (job_key,run_id,role,iteration,candidate_id,state,result_json,cleanup)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    f"{role}-0",
                    "run-1",
                    role,
                    0,
                    "candidate-1",
                    "finished" if role == "implement" else "unknown",
                    json.dumps({"status": "pass"}) if role == "implement" else None,
                    "confirmed" if role == "implement" else "unknown",
                ),
            )
    native_role_log = root / "attempts" / "implement-0" / "process.log"
    native_role_log.parent.mkdir(parents=True)
    native_role_log.write_text("historical native role\n")
    marked_role = root / "attempts" / "review-0"
    (marked_role / "container").mkdir(parents=True)
    (marked_role / "container" / "container-intent.json").write_text("{}")
    (marked_role / "process.log").write_text("forged native fallback\n")
    native_qa = root / "browser-qa" / "0"
    native_qa.mkdir(parents=True)
    native_qa_log = native_qa / "browser-qa.log"
    native_qa_log.write_text("historical browser QA\n")
    native_log_sha = hashlib.sha256(native_qa_log.read_bytes()).hexdigest()
    native_receipt = native_qa / "receipt.json"
    native_receipt.write_text(
        json.dumps({"iteration": 0, "log": str(native_qa_log), "log_sha256": native_log_sha})
    )
    native_receipt_sha = hashlib.sha256(native_receipt.read_bytes()).hexdigest()
    with store._connect() as db:
        db.execute(
            """INSERT INTO delivery_effects
               (effect_key,run_id,kind,request_json,state,observed_json,updated_at)
               VALUES (?,?,?,?,?,?,?)""",
            (
                "browser_qa:run-1:0",
                "run-1",
                "browser_qa",
                "{}",
                "complete",
                json.dumps(
                    {
                        "iteration": 0,
                        "log": str(native_qa_log),
                        "log_sha256": native_log_sha,
                        "receipt": str(native_receipt),
                        "receipt_sha256": native_receipt_sha,
                    }
                ),
                "2026-09-26T00:00:00Z",
            ),
        )
    marked_qa = root / "browser-qa" / "1"
    (marked_qa / "container").mkdir(parents=True)
    (marked_qa / "container" / "container-intent.json").write_text("{}")
    (marked_qa / "browser-qa.log").write_text("forged browser fallback\n")
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as browser:
        token = (Path(json.loads(path.read_text())["state_root"]) / "service-token").read_text()
        assert (
            await browser.post(
                "/api/session",
                json={"token": token.strip()},
                headers={"Origin": "http://127.0.0.1:18770"},
            )
        ).status_code == 200
        listed = (await browser.get("/api/runs/run-1")).json()["evidence"]
        assert {item["id"] for item in listed} == {
            "role-implement-0",
            "browser-qa-0-log",
            "browser-qa-0-receipt",
        }
        role = (await browser.get("/api/runs/run-1/evidence/role-implement-0")).json()
        qa = (await browser.get("/api/runs/run-1/evidence/browser-qa-0-log")).json()
        assert role["text"] == "historical native role\n"
        assert qa["text"] == "historical browser QA\n"
        assert (await browser.get("/api/runs/run-1/evidence/role-review-0")).status_code == 404
        assert (await browser.get("/api/runs/run-1/evidence/browser-qa-1-log")).status_code == 404


@pytest.mark.asyncio
async def test_terminal_cancel_is_conflict_without_pending_mutation(api_fixture):
    path, request = api_fixture
    app = create_app(path)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as browser:
        token = (
            (Path(json.loads(path.read_text())["state_root"]) / "service-token").read_text().strip()
        )
        login = await browser.post(
            "/api/session", json={"token": token}, headers={"Origin": "http://127.0.0.1:18770"}
        )
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
async def test_public_supersede_requires_new_branch_and_preserves_prior_checkout(api_fixture):
    path, request = api_fixture
    app = create_app(path)
    store = app.state.delivery.store
    store.submit(request)
    old_spec = store.spec(request["run_id"])
    old_checkout = DeliveryBroker(store, old_spec).prepare()["checkout"]
    store.mark_start(request["run_id"], accepted=True)
    store.project(
        request["run_id"],
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message="pre-role preparation failed",
        outcome="blocked",
    )
    prior_head = _git(Path(old_checkout), "rev-parse", "HEAD")
    successor = {
        **request,
        "command_id": "submit-successor",
        "run_id": "run-2",
        "supersedes_run_id": request["run_id"],
    }
    restarted = create_app(path)
    transport = httpx.ASGITransport(app=restarted, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as browser:
        token_path = Path(json.loads(path.read_text())["state_root"]) / "service-token"
        token = token_path.read_text().strip()
        login = await browser.post(
            "/api/session",
            json={"token": token},
            headers={"Origin": "http://127.0.0.1:18770"},
        )
        headers = {
            "Origin": "http://127.0.0.1:18770",
            "X-Devflow-CSRF": login.json()["csrf_token"],
        }
        refused = await browser.post("/api/runs", json=successor, headers=headers)
        assert refused.status_code == 409
        assert "choose a new branch" in refused.json()["detail"]
        with store._connect() as db:
            claim = store.state.claim_for(db, request["work_id"])
            assert claim["owner"] == "external:devflow:run-1"
            assert (
                db.execute("SELECT COUNT(*) FROM delivery_runs WHERE run_id='run-2'").fetchone()[0]
                == 0
            )
        admitted = await browser.post(
            "/api/runs",
            json={**successor, "branch": "feat/fixture-v2", "command_id": "submit-v2"},
            headers=headers,
        )
        assert admitted.status_code == 200, admitted.text
        with store._connect() as db:
            assert store.state.claim_for(db, request["work_id"])["owner"] == (
                "external:devflow:run-2"
            )
    assert Path(old_checkout).is_dir()
    assert _git(Path(old_checkout), "rev-parse", "HEAD") == prior_head


@pytest.mark.asyncio
async def test_public_decision_and_cancel_use_temporal_revision_after_worker_restart(
    api_fixture, tmp_path
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

    activities = [delivery_project, delivery_prepare, tracker_start_stub, role_stub]
    async with await WorkflowEnvironment.start_local(
        dev_server_database_filename=str(tmp_path / "public-decision.sqlite3")
    ) as environment:

        async def temporal_client():
            return environment.client

        app.state.delivery.client = temporal_client
        transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://127.0.0.1:18770"
        ) as browser:
            token = (Path(config["state_root"]) / "service-token").read_text().strip()
            login = await browser.post(
                "/api/session",
                json={"token": token},
                headers={"Origin": "http://127.0.0.1:18770"},
            )
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
                    for _ in range(100):
                        if store.detail(submitted["run_id"])["decisions"]:
                            break
                        await asyncio.sleep(0.05)
                    assert store.detail(submitted["run_id"])["decisions"]
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
async def test_public_cancel_remains_responsive_during_blocking_browser_activity(
    api_fixture, tmp_path, monkeypatch
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

    async with await WorkflowEnvironment.start_local(
        dev_server_database_filename=str(tmp_path / "browser-cancel.sqlite3")
    ) as environment:

        async def temporal_client():
            return environment.client

        app.state.delivery.client = temporal_client
        transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://127.0.0.1:18770"
        ) as browser:
            token = (Path(config["state_root"]) / "service-token").read_text().strip()
            login = await browser.post(
                "/api/session",
                json={"token": token},
                headers={"Origin": "http://127.0.0.1:18770"},
            )
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
                assert await asyncio.wait_for(asyncio.to_thread(entered.wait), 5)
                try:
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
