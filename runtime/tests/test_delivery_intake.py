"""Raw-goal intake protocol through the public API and a real Temporal dev server."""

from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import httpx
import pytest
from agent_runtime_kit import FilesystemAccess
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.client import WorkflowHistory
from temporalio.exceptions import ApplicationError
from temporalio.worker import Replayer, Worker

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_activities import (
    delivery_accept_plan,
    delivery_intake,
    delivery_prepare,
    delivery_project,
)
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_config import scope_amended_spec
from devflow_temporal.delivery_control import main as delivery_main
from devflow_temporal.delivery_preparation import run_binding
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow
from devflow_temporal.role_runner import _task


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
                {"id": "wording", "prompt": "Which wording?", "options": ["A", "B"], "blocker": {
                    "unknown": "The required exact legal wording is absent",
                    "evidence_checked": ["README and the frozen goal specify no exact wording"],
                    "why_no_safe_default": "Invented wording cannot satisfy the exact requirement",
                }}
            ]},
            {"status": "questions", "summary": "Needs format", "plan": {}, "questions": [
                {"id": "format", "prompt": "Which format?", "options": ["Plain", "Markdown"],
                 "blocker": {
                     "unknown": "The downstream consumer's required input format is unspecified",
                     "evidence_checked": ["No consumer schema or sample exists in the fixture"],
                     "why_no_safe_default": "Choosing incorrectly breaks the required consumer",
                 }}
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
    assert store.spec("run-1")["plan_approval"] == "automatic"
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


@pytest.mark.parametrize("policy", [None, False, 1, {}, [], "", "AUTO", "sometimes"])
def test_invalid_plan_approval_rejected_before_claim(intake_fixture, policy):
    path, request = intake_fixture
    store = create_app(path).state.delivery.store
    with pytest.raises(ValueError, match="plan_approval"):
        store.submit({**request, "plan_approval": policy})
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_runs").fetchone()[0] == 0
        assert store.state.row(db, "works", request["work_id"]) is None


def test_required_plan_approval_is_explicit_and_conflicts_with_accepted_plan(intake_fixture):
    path, request = intake_fixture
    store = create_app(path).state.delivery.store
    with pytest.raises(ValueError, match="contradicts"):
        store.submit({**request, "plan_approval": "required", "accepted_plan": "Existing plan"})
    with store._connect() as db:
        assert store.state.row(db, "works", request["work_id"]) is None
    required = {**request, "plan_approval": "required"}
    receipt = store.submit(required)
    assert store.spec("run-1")["plan_approval"] == "required"
    assert store.submit(required) == receipt
    with pytest.raises(ValueError, match="different inputs"):
        store.submit({**required, "plan_approval": "automatic"})


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


@pytest.mark.asyncio
async def test_public_real_submit_is_durable_without_attestation_or_docker_readback(
    intake_fixture, monkeypatch
):
    path, request = intake_fixture
    config = json.loads(path.read_text())
    config["provider"] = "codex"
    repository = config["repositories"]["fixture"]
    repository.update({
        "prepublish_checks": [{"id": "precheck", "argv": ["/usr/bin/true"]}],
        "checks": [{"id": "test", "argv": ["/usr/bin/true"]}],
        "required_ci": ["test"],
        "project_url": "https://github.com/orgs/example/projects/1",
        "assignee": "example",
    })
    path.write_text(json.dumps(config))

    app = create_app(path)
    store = app.state.delivery.store
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url=config["dashboard_url"]) as browser:
        origin = {"Origin": config["dashboard_url"]}
        login = await browser.get("/api/session")
        assert login.status_code == 200
        response = await browser.post(
            "/api/runs", json=request,
            headers={**origin, "X-Devflow-CSRF": login.json()["csrf_token"]},
        )
    assert response.status_code == 200
    assert response.json()["phase"] == "preparing"
    spec = store.spec(request["run_id"])
    assert spec["preparation_version"] == 1 and "preparation" not in spec
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_runs").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM delivery_outbox").fetchone()[0] == 1
        assert store.state.row(db, "works", request["work_id"]) is not None



def test_intake_prompt_has_frozen_issue_and_work_context(intake_fixture):
    path, request = intake_fixture
    store = create_app(path).state.delivery.store
    store.submit(request)
    spec = store.spec("run-1")
    native = {**spec, "policy": {**spec["policy"], "host_sandbox": "native-profile"}}
    task = _task({
        "spec": native, "role": "intake", "iteration": 0,
        "candidate": {"id": "candidate", "head": spec["base_sha"]},
        "workspace": "/work",
    })
    assert native["work_id"] == spec["work_id"]
    assert native["issue_url"] == spec["issue_url"]
    assert task.permissions.filesystem == FilesystemAccess.READ_ONLY
    assert 'Frozen work ID: "work-1"' in task.goal
    assert 'Frozen issue URL: "https://github.com/example/fixture/issues/3"' in task.goal
    assert 'Frozen plan approval: "automatic"' in task.goal
    assert "routine implementation choices do not need another user approval" in task.goal
    assert "reasonable reversible assumptions" in task.goal
    assert "no reasonable safe default" in task.goal
    assert "Never choose a callback thread or destination" in task.goal
    question_schema = task.output_schema["properties"]["questions"]["items"]
    assert "blocker" in question_schema["required"]
    historical = {key: value for key, value in native.items()
                  if key != "blocking_questions_version"}
    old_task = _task({"spec": historical, "role": "intake", "iteration": 0,
                      "candidate": {"id": "candidate", "head": spec["base_sha"]},
                      "workspace": "/work"})
    assert "blocker" not in old_task.output_schema["properties"]["questions"]["items"]["required"]


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
    request = {**request, "plan_approval": "required"}
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
    async with local_temporal(
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
            login = await browser.get("/api/session")
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
                assert "authorization" not in detail["intake"]["accepted_plan"]
                await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
                    await handle.fetch_history()
                )


@pytest.mark.asyncio
@pytest.mark.parametrize("required_plan", [False, True])
async def test_cancellation_while_waiting_for_intake(intake_fixture, tmp_path, required_plan):
    path, request = intake_fixture
    if required_plan:
        request = {**request, "plan_approval": "required"}
        config = json.loads(path.read_text())
        config["fake_intake"] = config["fake_intake"][2:3]
        path.write_text(json.dumps(config))
    app = create_app(path)
    store = app.state.delivery.store
    store.submit(request)
    calls = []

    @activity.defn(name="delivery_role")
    async def role_stub(payload):
        calls.append(payload)
        return {"status": "blocked", "candidate": payload["candidate"]}

    async with local_temporal(
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
            assert detail["phase"] == ("waiting_plan" if required_plan else "waiting_question")
            if required_plan:
                decision = detail["decisions"][0]
                await handle.execute_update(
                    "decision", {
                        "command_id": "cancel-plan", "expected_revision": detail["revision"],
                        "decision_id": decision["id"], "decision_revision": decision["revision"],
                        "candidate_revision": decision["candidate_revision"], "answer": "cancel",
                    }, id="cancel-plan",
                )
            else:
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
                assert db.execute(
                    "SELECT closed_at FROM runtime_sessions WHERE id=?",
                    ("external:devflow:run-1",),
                ).fetchone()[0] is not None
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
                assert db.execute(
                    "SELECT closed_at FROM runtime_sessions WHERE id=?",
                    ("external:devflow:run-2",),
                ).fetchone()[0] is None


def _authorization(spec):
    return {
        "source": "run_authorization", "command_id": spec["command_id"],
        "request_digest": spec["request_digest"], "policy_digest": spec["policy_digest"],
        "authorized_endpoint": spec["authorized_endpoint"],
    }


def _record_plan(store, plan, authorization, *, pending_question=False):
    store.project(
        "run-1", phase="investigating", execution_state="running",
        event_type="plan_recorded", message="Exact fixture plan recorded",
        intake={
            "round": 0, "plans": [{"revision": 1, "digest": digest(plan), "content": plan,
                                    "state": "proposed", "authorization": authorization}],
            "questions": [{"state": "pending"}] if pending_question else [],
            "answers": [], "accepted_plan": None, "change_requests": [],
        },
    )


def test_automatic_plan_binding_checks_authority_revision_and_restart(intake_fixture):
    path, request = intake_fixture
    store = create_app(path).state.delivery.store
    receipt = store.submit(request)
    submitted = store.spec("run-1")
    config = json.loads(path.read_text())
    plan = config["fake_intake"][2]["plan"]
    authorization = _authorization(submitted)
    _record_plan(store, plan, authorization)
    for drift in (
        None,
        {**authorization, "source": "human_decision"},
        {**authorization, "command_id": "other-submit"},
        {**authorization, "request_digest": "0" * 64},
        {**authorization, "policy_digest": "0" * 64},
        {**authorization, "authorized_endpoint": "merged"},
    ):
        with pytest.raises(ValueError, match="frozen run authorization"):
            store.accept_intake_plan("run-1", 1, digest(plan), plan, authorization=drift)
    for revision, plan_hash, content in (
        (2, digest(plan), plan),
        (1, "0" * 64, plan),
        (1, digest(plan), {**plan, "scope": "Changed"}),
    ):
        with pytest.raises(ValueError, match="revision|changed"):
            store.accept_intake_plan("run-1", revision, plan_hash, content,
                                     authorization=authorization)
    _record_plan(store, plan, authorization, pending_question=True)
    with pytest.raises(ValueError, match="clarification remains pending"):
        store.accept_intake_plan("run-1", 1, digest(plan), plan, authorization=authorization)
    _record_plan(store, plan, {**authorization, "command_id": "other-submit"})
    with pytest.raises(ValueError, match="authorization changed"):
        store.accept_intake_plan("run-1", 1, digest(plan), plan, authorization=authorization)
    _record_plan(store, plan, authorization)
    accepted = store.accept_intake_plan("run-1", 1, digest(plan), plan,
                                        authorization=authorization)
    restarted = DeliveryStore(store.config)
    assert restarted.accept_intake_plan("run-1", 1, digest(plan), plan,
                                        authorization=authorization) == accepted
    assert restarted.submit(request) == receipt
    assert restarted.spec("run-1") == submitted
    assert restarted.effective_spec("run-1") == accepted
    assert restarted.intake_execution_spec("run-1") == accepted
    assert restarted.detail("run-1")["intake"]["accepted_plan"] == {
        "revision": 1, "digest": digest(plan), "content": plan, "authorization": authorization,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("with_question", [False, True])
async def test_automatic_intake_reaches_implementation_without_plan_answer(
    intake_fixture, tmp_path, with_question
):
    path, request = intake_fixture
    config = json.loads(path.read_text())
    expected_plan = config["fake_intake"][2]["plan"]
    config["fake_intake"] = (
        [config["fake_intake"][0]] if with_question else []
    ) + [config["fake_intake"][2]]
    path.write_text(json.dumps(config))
    app = create_app(path)
    store = app.state.delivery.store
    calls = []
    acceptance_attempts = []
    committed_without_completion = asyncio.Event()

    @activity.defn(name="delivery_accept_plan")
    async def accept_plan(payload):
        accepted = await delivery_accept_plan(payload)
        acceptance_attempts.append(accepted)
        if not with_question and len(acceptance_attempts) == 1:
            committed_without_completion.set()
            raise ApplicationError("simulated lost completion after durable plan binding")
        return accepted

    @activity.defn(name="delivery_tracker_start")
    async def tracker(_payload):
        return {"state": "consistent"}

    @activity.defn(name="delivery_role")
    async def implement(payload):
        calls.append(payload)
        return {"status": "blocked", "candidate": payload["candidate"]}

    activities = [delivery_project, delivery_prepare, delivery_intake, accept_plan,
                  tracker, implement]
    async with local_temporal(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "automatic-temporal.sqlite3"),
    ) as environment:
        async def client():
            return environment.client

        app.state.delivery.client = client
        transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
        async with httpx.AsyncClient(transport=transport, base_url=config["dashboard_url"]) as api:
            login = await api.get("/api/session")
            headers = {"Origin": config["dashboard_url"],
                       "X-Devflow-CSRF": login.json()["csrf_token"]}
            receipt = await api.post("/api/runs", json=request, headers=headers)
            assert receipt.status_code == 200
            assert (await api.post("/api/runs", json=request, headers=headers)).json() == (
                receipt.json()
            )
            submitted = store.spec("run-1")
            queue = "automatic-intake"
            async with Worker(environment.client, task_queue=queue,
                              workflows=[DeliveryWorkflow], activities=activities):
                handle = await environment.client.start_workflow(
                    DeliveryWorkflow.run, submitted, id="delivery-run-1", task_queue=queue,
                )
                store.mark_start("run-1", accepted=True)
                if with_question:
                    for _ in range(150):
                        waiting = store.detail("run-1")
                        if waiting["phase"] == "waiting_question":
                            break
                        await asyncio.sleep(0.05)
                    assert waiting["phase"] == "waiting_question"
                    assert calls == []
                else:
                    await asyncio.wait_for(committed_without_completion.wait(), 20)
                    assert store.detail("run-1")["intake"]["accepted_plan"] is not None
                    assert calls == []
            # Clarification is a durable wait; a replacement worker consumes the
            # actual user answer and then binds the plan without another decision.
            async with Worker(environment.client, task_queue=queue,
                              workflows=[DeliveryWorkflow], activities=activities):
                if with_question:
                    decision = waiting["decisions"][0]
                    answer = {"command_id": "answer-question",
                              "expected_revision": waiting["revision"],
                              "decision_id": decision["id"],
                              "decision_revision": decision["revision"],
                              "candidate_revision": decision["candidate_revision"],
                              "answer": "A"}
                    response = await api.post("/api/runs/run-1/decision", json=answer,
                                              headers=headers)
                    assert response.status_code == 200, response.text
                    assert (await api.post("/api/runs/run-1/decision", json=answer,
                                           headers=headers)).json() == response.json()
                result = await asyncio.wait_for(handle.result(), 20)
            assert result["phase"] == "blocked"  # Stub intentionally stops at implementation.
            assert len(calls) == 1 and calls[0]["role"] == "implement"
            assert len(acceptance_attempts) == (1 if with_question else 2)
            assert all(item == calls[0]["spec"] for item in acceptance_attempts)
            assert json.loads(calls[0]["spec"]["accepted_plan"]) == expected_plan
            detail = (await api.get("/api/runs/run-1")).json()["run"]
            assert detail["decisions"] == []
            assert store.spec("run-1") == submitted
            assert store.effective_spec("run-1") == calls[0]["spec"]
            accepted = detail["intake"]["accepted_plan"]
            assert accepted == {"revision": 1, "digest": digest(expected_plan),
                                "content": expected_plan,
                                "authorization": _authorization(submitted)}
            assert "command_id" not in accepted  # No invented human Proceed answer.
            assert len(detail["intake"]["answers"]) == int(with_question)
            events = store.events("run-1")
            assert not any(event["type"] == "plan_pending" for event in events)
            assert sum(event["type"] == "plan_accepted" for event in events) == 1
            with store._connect() as db:
                decisions = db.execute(
                    "SELECT COUNT(*) FROM delivery_mutations WHERE kind='decision'"
                ).fetchone()[0]
            assert decisions == int(with_question)
            await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
                await handle.fetch_history()
            )


@pytest.mark.asyncio
async def test_pre_policy_intake_history_replays_with_original_human_gate():
    # Captured from ba5f677's workflow, using real Temporal and fake roles, before
    # adding the automatic branch. Includes a real Proceed update and acceptance.
    path = Path(__file__).parent / "fixtures" / "intake-required-history.json"
    history = WorkflowHistory.from_json("delivery-run-1", path.read_text())
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)


@pytest.mark.parametrize("approval", ["automatic", "required", None])
@pytest.mark.parametrize("provider_limit", [None, 1, 3])
def test_scope_amendment_preserves_policy_bound_plan_and_historical_identity(
    intake_fixture, approval, provider_limit
):
    path, request = intake_fixture
    if approval is not None:
        request = {**request, "plan_approval": approval,
                   "origin_thread_id": "01a0c8be-e849-7ef2-ad81-78ccdb4b4275"}
    store = create_app(path).state.delivery.store
    store.submit(request)
    spec = store.spec("run-1")
    if provider_limit is None:
        spec["policy"].pop("provider_max_attempts")
    else:
        spec["policy"]["provider_max_attempts"] = provider_limit
    spec["policy_digest"] = digest(spec["policy"])
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET request_json=? WHERE run_id='run-1'",
                   (json.dumps(spec),))
    if approval is None:
        # Historical durable input, not a new public submission.
        spec.pop("plan_approval")
        spec.pop("blocking_questions_version")
        spec.pop("publication_summary")
        spec["goal"] += ". Preserve the existing detailed execution instructions."
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET request_json=? WHERE run_id='run-1'",
                       (json.dumps(spec),))
    authorization = _authorization(spec) if approval == "automatic" else None
    plan = json.loads(path.read_text())["fake_intake"][2]["plan"]
    _record_plan(store, plan, authorization)
    original = store.accept_intake_plan("run-1", 1, digest(plan), plan,
                                        authorization=authorization)
    before = store.detail("run-1")["intake"]
    amended_config = json.loads(path.read_text())
    amended_config["repositories"]["fixture"]["allowed_paths"].append("tests/fixture.py")
    amended_path = store.config.state_root / "amendment.json"
    amended_path.write_text(json.dumps(amended_config))
    amended_path.chmod(0o600)
    effective = scope_amended_spec(original, amended_path,
                                   hashlib.sha256(amended_path.read_bytes()).hexdigest(),
                                   ["tests/fixture.py"])
    assert effective.get("plan_approval") == original.get("plan_approval")
    assert effective["policy"].get("provider_max_attempts") == original["policy"].get(
        "provider_max_attempts")
    assert ("provider_max_attempts" in effective["policy"]) == (
        "provider_max_attempts" in original["policy"])
    assert ("plan_approval" in effective) == ("plan_approval" in original)
    assert effective.get("origin_thread_id") == original.get("origin_thread_id")
    assert effective.get("publication_summary") == original.get("publication_summary")
    assert ("publication_summary" in effective) == ("publication_summary" in original)
    assert effective.get("blocking_questions_version") == original.get("blocking_questions_version")
    assert ("blocking_questions_version" in effective) == ("blocking_questions_version" in original)
    assert effective["accepted_plan"] == original["accepted_plan"]
    assert effective["intake_required"] is True
    assert effective["request_digest"] == original["request_digest"]
    assert effective["policy_digest"] != original["policy_digest"]
    assert store.detail("run-1")["intake"] == before
    assert store.spec("run-1") == spec
    if approval is not None:
        changed_origin = {**spec, "origin_thread_id": "01a100ac-efd3-7dd2-9f25-504381f0dcd9"}
        assert run_binding(changed_origin) != run_binding(spec)
        assert run_binding({**spec, "plan_approval": "required"}) != run_binding(
            {**spec, "plan_approval": "automatic"}
        )
