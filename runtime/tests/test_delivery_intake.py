"""Raw-goal intake protocol through the public API and a real Temporal dev server."""

from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
import sys
from pathlib import Path

import httpx
import pytest
from agent_runtime_kit import FilesystemAccess
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from devflow_temporal.delivery_activities import (
    delivery_accept_plan,
    delivery_intake,
    delivery_prepare,
    delivery_project,
)
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_control import main as delivery_main
from devflow_temporal.delivery_workflow import DeliveryWorkflow
from devflow_temporal.role_runner import _task
from devflow_temporal.supervisor import _contained_role_spec


def _git(path: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(path), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture
def intake_fixture(tmp_path: Path) -> tuple[Path, dict]:
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

    def plan(text: str) -> dict:
        return {
            "status": "plan", "summary": "Scoped fixture plan", "questions": [],
            "plan": {
                "scope": text,
                "steps": ["Change README within the configured path policy"],
                "verification": ["Inspect README and run the fixture check"],
                "acceptance": ["README reflects the requested wording"],
            },
        }

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
                "source_path": str(source), "origin_url": str(remote),
                "github_repo": "example/fixture", "base_ref": "HEAD",
                "expected_base_sha": _git(source, "rev-parse", "HEAD"),
                "allowed_paths": ["README.md"],
            }
        },
        "roles": {
            role: {"model": "fake", "effort": "low"}
            for role in ("intake", "implement", "review", "verify")
        },
        "fake_intake": [
            {"status": "questions", "summary": "Needs scope", "plan": {}, "questions": [
                {"id": "wording", "prompt": "Which wording?", "options": ["A", "B"]}
            ]},
            {"status": "questions", "summary": "Needs format", "plan": {}, "questions": [
                {"id": "format", "prompt": "Which format?", "options": ["Plain", "Markdown"]}
            ]},
            plan("First proposal"),
            plan("Revised proposal"),
        ],
    }
    path = tmp_path / "service.json"
    path.write_text(json.dumps(config))
    request = {
        "command_id": "submit-1", "run_id": "run-1", "work_id": "work-1",
        "issue_url": "https://github.com/example/fixture/issues/3",
        "repository_key": "fixture", "goal": "Change fixture wording",
        "base_ref": "HEAD", "branch": "feat/fixture",
        "authorized_endpoint": "published_unmerged",
    }
    return path, request


def test_raw_goal_admission_and_legacy_plan(intake_fixture):
    path, request = intake_fixture
    app = create_app(path)
    store = app.state.delivery.store
    accepted = store.submit(request)
    assert accepted["phase"] == "accepted"
    assert store.spec("run-1")["intake_required"] is True
    assert store.effective_spec("run-1")["accepted_plan"] == ""
    assert store.submit(request) == accepted
    with pytest.raises(ValueError, match="different inputs"):
        store.submit({**request, "goal": "conflict"})
    legacy = {
        **request, "command_id": "submit-legacy", "run_id": "run-legacy",
        "work_id": "work-legacy", "issue_url": "https://github.com/example/fixture/issues/4",
        "branch": "feat/legacy", "accepted_plan": "Previously accepted plan",
    }
    store.submit(legacy)
    assert store.spec("run-legacy")["intake_required"] is False


def test_unpinned_intake_role_rejected_before_work_claim(intake_fixture):
    path, request = intake_fixture
    config = json.loads(path.read_text())
    config["roles"]["intake"] = {}
    path.write_text(json.dumps(config))
    store = create_app(path).state.delivery.store
    with pytest.raises(ValueError, match="intake model and effort"):
        store.submit(request)
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_runs").fetchone()[0] == 0
        assert store.state.row(db, "works", request["work_id"]) is None


def test_intake_prompt_has_frozen_issue_and_work_context(intake_fixture):
    path, request = intake_fixture
    store = create_app(path).state.delivery.store
    store.submit(request)
    spec = store.spec("run-1")
    contained = _contained_role_spec(
        spec, {**spec["policy"], "host_sandbox": "native-profile"}
    )
    task = _task({
        "spec": contained, "role": "intake", "iteration": 0,
        "candidate": {"id": "candidate", "head": spec["base_sha"]},
        "workspace": "/work",
    })
    assert contained["work_id"] == spec["work_id"]
    assert contained["issue_url"] == spec["issue_url"]
    assert task.permissions.filesystem == FilesystemAccess.READ_ONLY
    assert 'Frozen work ID: "work-1"' in task.goal
    assert 'Frozen issue URL: "https://github.com/example/fixture/issues/3"' in task.goal


def test_cli_reports_stale_answer_without_traceback(intake_fixture, tmp_path, monkeypatch, capsys):
    path, _ = intake_fixture
    answer_path = tmp_path / "stale-answer.json"
    answer_path.write_text(json.dumps({"command_id": "stale-answer"}))

    class Client:
        def decision(self, _run_id, _request):
            raise ValueError("service HTTP 409: decision changed")

        def __getattr__(self, _name):
            return lambda *_args: None

    monkeypatch.setattr("devflow_temporal.delivery_control.api_client", lambda _path: Client())
    monkeypatch.setattr(sys, "argv", [
        "devflow-delivery", "--config", str(path), "decision",
        "--id", "run-1", "--request", str(answer_path),
    ])
    with pytest.raises(SystemExit) as exit_info:
        delivery_main()
    assert exit_info.value.code == 1
    output = capsys.readouterr()
    assert "service HTTP 409: decision changed" in output.err
    assert "Traceback" not in output.err


@pytest.mark.asyncio
async def test_raw_goal_questions_revision_restart_plan_change_and_acceptance(
    intake_fixture, tmp_path
):
    path, request = intake_fixture
    app = create_app(path)
    store = app.state.delivery.store
    implement_calls = []

    @activity.defn(name="delivery_tracker_start")
    async def tracker_start(_payload):
        return {"state": "consistent"}

    @activity.defn(name="delivery_role")
    async def role_stub(payload):
        implement_calls.append(payload["spec"]["accepted_plan"])
        return {"status": "blocked", "candidate": payload["candidate"]}

    activities = [
        delivery_project, delivery_prepare, delivery_intake, delivery_accept_plan,
        tracker_start, role_stub,
    ]
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "intake-temporal.sqlite3"),
    ) as environment:
        async def temporal_client():
            return environment.client

        app.state.delivery.client = temporal_client
        transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://127.0.0.1:18770"
        ) as browser:
            token = (
                Path(json.loads(path.read_text())["state_root"]) / "service-token"
            ).read_text().strip()
            login = await browser.post(
                "/api/session", json={"token": token},
                headers={"Origin": "http://127.0.0.1:18770"},
            )
            headers = {
                "Origin": "http://127.0.0.1:18770",
                "X-Devflow-CSRF": login.json()["csrf_token"],
            }
            submitted = await browser.post("/api/runs", json=request, headers=headers)
            assert submitted.status_code == 200, submitted.text
            queue = "intake-fixture"
            async with Worker(
                environment.client, task_queue=queue,
                workflows=[DeliveryWorkflow], activities=activities,
            ):
                handle = await environment.client.start_workflow(
                    DeliveryWorkflow.run, store.spec(request["run_id"]),
                    id="delivery-run-1", task_queue=queue,
                )
                store.mark_start("run-1", accepted=True)

                async def pending(kind: str, prompt: str | None = None) -> dict:
                    for _ in range(150):
                        detail = (await browser.get("/api/runs/run-1")).json()["run"]
                        decisions = detail["decisions"]
                        if (
                            decisions and decisions[0].get("kind") == kind
                            and (prompt is None or decisions[0].get("prompt") == prompt)
                        ):
                            return detail
                        await asyncio.sleep(0.05)
                    raise AssertionError(f"{kind} decision was not projected")

                first = await pending("question")
                assert first["decisions"][0]["prompt"] == "Which wording?"
                assert implement_calls == []
            # The Temporal wait and question survive losing the worker process.
            async with Worker(
                environment.client, task_queue=queue,
                workflows=[DeliveryWorkflow], activities=activities,
            ):
                first = (await browser.get("/api/runs/run-1")).json()["run"]
                decision = first["decisions"][0]
                body = {
                    "command_id": "answer-wording", "expected_revision": first["revision"],
                    "decision_id": decision["id"],
                    "decision_revision": decision["revision"],
                    "candidate_revision": decision["candidate_revision"],
                    "answer": "Use my own wording",
                }
                accepted = await browser.post(
                    "/api/runs/run-1/decision", json=body, headers=headers
                )
                assert accepted.status_code == 200, accepted.text
                replay = await browser.post("/api/runs/run-1/decision", json=body, headers=headers)
                assert replay.json() == accepted.json()
                second = await pending("question", "Which format?")
                assert second["decisions"][0]["prompt"] == "Which format?"
                assert second["intake"]["answers"][0]["answer"] == "Use my own wording"
                stale = await browser.post(
                    "/api/runs/run-1/decision",
                    json={**body, "command_id": "stale-answer"}, headers=headers,
                )
                assert stale.status_code == 409
                decision = second["decisions"][0]
                response = await browser.post(
                    "/api/runs/run-1/decision",
                    json={
                        "command_id": "answer-format", "expected_revision": second["revision"],
                        "decision_id": decision["id"],
                        "decision_revision": decision["revision"],
                        "candidate_revision": decision["candidate_revision"],
                        "answer": "Markdown",
                    }, headers=headers,
                )
                assert response.status_code == 200, response.text
                proposed = await pending("plan")
                assert proposed["intake"]["plans"][-1]["content"]["scope"] == "First proposal"
                assert implement_calls == []
                decision = proposed["decisions"][0]
                changed = await browser.post(
                    "/api/runs/run-1/decision",
                    json={
                        "command_id": "change-plan", "expected_revision": proposed["revision"],
                        "decision_id": decision["id"],
                        "decision_revision": decision["revision"],
                        "candidate_revision": decision["candidate_revision"],
                        "answer": "change", "response": "Use the revised scope",
                    }, headers=headers,
                )
                assert changed.status_code == 200, changed.text
                for _ in range(150):
                    revised = await pending("plan")
                    if revised["decisions"][0]["revision"] == 2:
                        break
                    await asyncio.sleep(0.05)
                assert revised["intake"]["plans"][-1]["content"]["scope"] == "Revised proposal"
                assert implement_calls == []
                decision = revised["decisions"][0]
                approved = await browser.post(
                    "/api/runs/run-1/decision",
                    json={
                        "command_id": "accept-plan", "expected_revision": revised["revision"],
                        "decision_id": decision["id"],
                        "decision_revision": decision["revision"],
                        "candidate_revision": decision["candidate_revision"],
                        "answer": "proceed",
                    }, headers=headers,
                )
                assert approved.status_code == 200, approved.text
                final = await asyncio.wait_for(handle.result(), 20)
                assert final["outcome"] == "blocked"
                assert len(implement_calls) == 1
                assert "Revised proposal" in implement_calls[0]
                assert "First proposal" not in implement_calls[0]
                detail = (await browser.get("/api/runs/run-1")).json()["run"]
                assert detail["intake"]["accepted_plan"]["revision"] == 2
                assert store.effective_spec("run-1")["accepted_plan"] == implement_calls[0]


@pytest.mark.asyncio
async def test_cancellation_while_waiting_for_clarification(intake_fixture, tmp_path):
    path, request = intake_fixture
    app = create_app(path)
    store = app.state.delivery.store
    store.submit(request)
    calls = []

    @activity.defn(name="delivery_role")
    async def role_stub(payload):
        calls.append(payload)
        return {"status": "blocked", "candidate": payload["candidate"]}

    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "cancel-temporal.sqlite3"),
    ) as environment:
        queue = "intake-cancel"
        async with Worker(
            environment.client, task_queue=queue, workflows=[DeliveryWorkflow],
            activities=[delivery_project, delivery_prepare, delivery_intake, role_stub],
        ):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, store.spec("run-1"),
                id="delivery-run-1", task_queue=queue,
            )
            store.mark_start("run-1", accepted=True)
            for _ in range(150):
                detail = store.detail("run-1")
                if detail["decisions"]:
                    break
                await asyncio.sleep(0.05)
            assert detail["phase"] == "waiting_question"
            result = await handle.execute_update(
                "cancel", {"expected_revision": detail["revision"], "reason": "stop"},
                id="cancel-waiting",
            )
            assert result["phase"] == "cancelling"
            final = await asyncio.wait_for(handle.result(), 15)
            assert final["outcome"] == "cancelled"
            assert calls == []
            with store._connect() as db:
                assert store.state.claim_for(db, request["work_id"]) is None
            retry = {
                **request, "command_id": "submit-2", "run_id": "run-2",
                "branch": "feat/fixture-retry",
            }
            assert store.submit(retry)["phase"] == "accepted"
            cancel_event = next(
                event for event in store.events("run-1") if event["type"] == "cancelled"
            )
            store.project(
                "run-1", phase="cancelled", execution_state="terminal",
                event_type="cancelled", message="Cancellation reached a role boundary",
                outcome="cancelled", cleanup="confirmed_after_role_boundary",
                key=cancel_event["payload"]["key"],
            )
            with store._connect() as db:
                assert store.state.claim_for(db, request["work_id"])["owner"] == (
                    "external:devflow:run-2"
                )
