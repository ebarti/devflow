from __future__ import annotations

import asyncio
import json
import subprocess
from pathlib import Path

import httpx
import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from devflow_temporal.delivery_activities import delivery_prepare, delivery_project
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_workflow import DeliveryWorkflow


def _git(path: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(path), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


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
