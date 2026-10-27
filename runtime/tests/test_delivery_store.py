from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import shutil
import socket
import sqlite3
import subprocess
from pathlib import Path

import httpx
import pytest
from temporal_test_server import local_temporal
from temporalio import activity, workflow
from temporalio.client import Client, WorkflowUpdateFailedError
from temporalio.converter import DataConverter
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from devflow_temporal import delivery_store
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_activities import (
    delivery_prepare,
    delivery_project,
    delivery_repair_preflight,
)
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import (
    DeliveryBroker,
)
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_repair import (
    RepairReadbackPending,
)
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow


def _git(path: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(path), *args], check=True, capture_output=True, text=True
    )
    return completed.stdout.strip()


def _write_lock_free(store: DeliveryStore) -> None:
    """External observations must not run while the store holds the write lock."""
    probe = sqlite3.connect(store.config.tracking_db, timeout=0)
    try:
        probe.execute("BEGIN IMMEDIATE")
        probe.rollback()
    except sqlite3.OperationalError as exc:
        raise AssertionError("tracking database write lock held during observation") from exc
    finally:
        probe.close()


@pytest.fixture
def service(tmp_path: Path) -> tuple[DeliveryStore, dict]:
    source = tmp_path / "source"
    source.mkdir()
    (source / "README.md").write_text("Test repository\n")
    _git(source, "init", "-q")
    _git(source, "config", "user.name", "Delivery Test")
    _git(source, "config", "user.email", "delivery@example.invalid")
    _git(source, "add", "README.md")
    _git(source, "commit", "-qm", "Fixture")
    origin = tmp_path / "origin.git"
    subprocess.run(["git", "init", "--bare", "-q", str(origin)], check=True)
    _git(source, "remote", "add", "origin", str(origin))
    root = Path(__file__).resolve().parents[2]
    configuration = {
        "version": 1,
        "tracking_db": str(tmp_path / "workflow.sqlite3"),
        "state_root": str(tmp_path / "service-state"),
        "helpers_dir": str(root / "skills" / "devflow" / "scripts"),
        "codex_bin": "/usr/bin/false",
        "provider": "fake",
        "execution_registry": str(tmp_path / "execution-ownership" / "registry.sqlite3"),
        "repositories": {
            "fixture": {
                "source_path": str(source),
                "origin_url": str(origin),
                "github_repo": "example/fixture",
                "base_ref": "HEAD",
                "expected_base_sha": _git(source, "rev-parse", "HEAD"),
                "allowed_paths": ["README.md"],
            }
        },
        "roles": {
            "implement": {"model": "fixture", "effort": "low"},
            "review": {"model": "fixture", "effort": "low"},
            "verify": {"model": "fixture", "effort": "low"},
        },
    }
    path = tmp_path / "service.json"
    path.write_text(json.dumps(configuration))
    request = {
        "command_id": "command-1",
        "run_id": "run-1",
        "work_id": "work-1",
        "issue_url": "https://github.com/example/fixture/issues/3",
        "repository_key": "fixture",
        "goal": "Change a fixture",
        "accepted_plan": "Make one bounded edit and test it.",
        "base_ref": "HEAD",
        "branch": "feat/fixture",
        "authorized_endpoint": "published_unmerged",
    }
    return DeliveryStore(DeliveryConfig.load(path)), request


def submit_before_readiness(store, request, monkeypatch):
    """Retain the original contract of a native command admitted before readiness."""
    spec = store.config.admit(request, _readiness_base={})
    with monkeypatch.context() as historical:
        historical.setattr(DeliveryConfig, "admit", lambda *_a, **_kw: copy.deepcopy(spec))
        return store.submit(request)


def submit_historical_admission(store, request, monkeypatch):
    """Set up a pre-budget admission for tests of retained legacy continuations."""
    spec = store.config.admit(request, _readiness_base={})
    spec.pop("automatic_retry_version", None)
    spec.pop("retry_budget_version", None)
    spec.pop("assessment_batch_version", None)
    spec.pop("validation_schedule_version", None)
    spec.pop("merge_reconciliation_version", None)
    spec.pop("readiness_version", None)
    spec.pop("workspace_custody_version", None)
    spec["policy"].pop("readiness", None)
    spec["policy"].pop("max_attempts", None)
    spec["policy_digest"] = digest(spec["policy"])
    with monkeypatch.context() as historical:
        historical.setattr(DeliveryConfig, "admit", lambda *_args: copy.deepcopy(spec))
        return store.submit(request)


def _failed_published_repair_fixture(store, request, monkeypatch):
    """One published failed gate with sealed role/PR/cleanup evidence."""
    submit_historical_admission(store, request, monkeypatch)
    store.mark_start(request["run_id"], accepted=True)
    spec = store.spec(request["run_id"])
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    (broker.checkout / "README.md").write_text("Repaired fixture\n")
    _git(broker.checkout, "add", "README.md")
    _git(
        broker.checkout,
        "-c",
        "user.name=Delivery Test",
        "-c",
        "user.email=delivery@example.invalid",
        "commit",
        "-qm",
        "candidate",
    )
    _git(broker.checkout, "push", "origin", f"HEAD:refs/heads/{spec['branch']}")
    candidate = broker.candidate()
    pr = {
        "number": 7,
        "url": "https://github.com/example/fixture/pull/7",
        "state": "OPEN",
        "head": candidate["head"],
        "base": spec["base_sha"],
        "candidate": candidate,
    }
    monkeypatch.setattr(
        DeliveryBroker,
        "_existing_pr",
        lambda self: {
            "number": 7,
            "url": pr["url"],
            "state": "OPEN",
            "headRefOid": candidate["head"],
        },
    )
    broker._effect(
        "publish:run-1:2", "publish", {"iteration": 2, "input_candidate_id": candidate["id"]}
    )
    broker._finish_effect("publish:run-1:2", pr)
    roles = [
        {
            "role": "implement",
            "iteration": 2,
            "status": "pass",
            "cleanup": "confirmed",
            "session_id": "original-session",
            "candidate": candidate,
        },
        {
            "role": "review",
            "iteration": 2,
            "status": "blocked",
            "cleanup": "confirmed",
            "session_id": "independent-session",
            "candidate": candidate,
            "findings": ["Specific current-candidate regression"],
        },
    ]
    checks = {"review": {"state": "failed", "candidate_id": candidate["id"]}}
    with store._connect() as db:
        for role in roles:
            db.execute(
                """INSERT INTO delivery_attempts
                   (job_key,run_id,role,iteration,candidate_id,state,session_id,result_json,cleanup)
                   VALUES (?,?,?,?,?,'finished',?,?,'confirmed')""",
                (
                    f"role:{role['role']}:2",
                    request["run_id"],
                    role["role"],
                    2,
                    candidate["id"],
                    role["session_id"],
                    json.dumps(role),
                ),
            )
    state = {
        "run_id": request["run_id"],
        "phase": "blocked",
        "execution_state": "blocked",
        "outcome": "blocked",
        "cleanup": "none",
        "error": "repair limit exhausted",
        "revision": 9,
        "iteration": 2,
        "candidate": candidate,
        "pull_request": pr,
        "roles": roles,
        "checks": checks,
        "tracker": {},
        "usage": {},
        "findings": ["Specific current-candidate regression"],
        "decision": None,
        "candidate_revision": 2,
    }
    store.project(
        request["run_id"],
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message="repair limit exhausted",
        candidate=candidate,
        pull_request=pr,
        checks=checks,
        iteration=2,
        protocol_revision=9,
        outcome="blocked",
        cleanup="none",
        error="repair limit exhausted",
    )
    closed = {
        "workflow_id": "delivery-run-1",
        "execution_run_id": "closed-execution",
        "closed_at": "2026-09-27T00:00:00+00:00",
        "request_digest": spec["request_digest"],
        "recovery_digest": None,
        "result": state,
    }
    monkeypatch.setattr(store, "_completed_temporal_result", lambda _id, **_kw: closed)
    command = {
        "command_id": "grant-repair-1",
        "expected_revision": 9,
        "expected_iteration": 2,
        "expected_candidate_id": candidate["id"],
        "expected_pr_number": 7,
        "expected_pr_head": candidate["head"],
        "additional_iterations": 2,
    }
    return spec, state, command


def _sealed_prelaunch_retry_fixture(store, request, monkeypatch, *, required_ci=False):
    if required_ci:
        store.config.raw["repositories"]["fixture"]["required_ci"] = [
            "Web unit, types, and build"
        ]
        store.config.path.write_text(json.dumps(store.config.raw))
    spec, state, grant_command = _failed_published_repair_fixture(store, request, monkeypatch)
    store.continue_repair(request["run_id"], grant_command)
    store.mark_start(request["run_id"], accepted=True)
    with store._connect() as db:
        original = json.loads(
            db.execute(
                "SELECT recovery_json FROM delivery_runs WHERE run_id=?", (request["run_id"],)
            ).fetchone()[0]
        )
    candidate = state["candidate"]
    findings = ["Reviewed candidate must preserve a Required pin after a failed save"]
    completed = [
        {
            "role": "implement",
            "iteration": 3,
            "status": "pass",
            "cleanup": "confirmed",
            "session_id": "original-session",
            "candidate": candidate,
        },
        {
            "role": "review",
            "iteration": 3,
            "status": "findings",
            "cleanup": "confirmed",
            "session_id": "review-session-3",
            "candidate": candidate,
            "findings": findings,
        },
        {
            "role": "implement",
            "iteration": 4,
            "status": "blocked",
            "cleanup": "confirmed",
            "session_id": None,
            "finish_reason": "prelaunch",
            "findings": ["ValueError"],
        },
    ]
    state2 = {
        **state,
        "revision": 14,
        "iteration": 4,
        "error": "implementer did not establish a pass",
        "roles": [*state["roles"], *completed],
        "checks": {},
    }
    failed_key = "failed-prelaunch-4"
    folder = Path(spec["state_dir"]) / "attempts" / failed_key
    folder.mkdir(parents=True, mode=0o700)
    with store._connect() as db:
        for role in completed:
            key = failed_key if role["iteration"] == 4 else f"role:{role['role']}:3"
            db.execute(
                """INSERT INTO delivery_attempts
                   (job_key,run_id,role,iteration,candidate_id,state,session_id,
                    result_json,result_path,cleanup)
                   VALUES (?,?,?,?,?,'finished',?,?,?,'confirmed')""",
                (
                    key,
                    request["run_id"],
                    role["role"],
                    role["iteration"],
                    candidate["id"],
                    role["session_id"],
                    json.dumps(role),
                    str(folder / "result.json") if key == failed_key else None,
                ),
            )
    store.project(
        request["run_id"],
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message=state2["error"],
        candidate=candidate,
        pull_request=state2["pull_request"],
        checks={},
        iteration=4,
        protocol_revision=14,
        outcome="blocked",
        cleanup="none",
        error=state2["error"],
    )
    closed = {
        "workflow_id": "delivery-run-1-repair-continuation-1",
        "execution_run_id": "closed-prelaunch-repair",
        "closed_at": "2026-09-27T01:00:00+00:00",
        "request_digest": spec["request_digest"],
        "recovery_digest": digest(original),
        "result": state2,
    }
    monkeypatch.setattr(store, "_completed_temporal_result", lambda _id, **_kw: closed)
    command = {
        "command_id": "retry-prelaunch-1",
        "expected_revision": 14,
        "expected_iteration": 4,
        "expected_candidate_id": candidate["id"],
        "expected_pr_number": 7,
        "expected_pr_head": candidate["head"],
    }
    return spec, state2, command, folder


def _scope_amendment_fixture(store, request, monkeypatch, *, retry_generation=0):
    source = Path(store.config.raw["repositories"]["fixture"]["source_path"])
    additions = ["tests/capability-a.test.ts", "tests/capability-b.test.ts"]
    (source / "tests").mkdir()
    for name in additions:
        (source / name).write_text("expect(138).toBe(138)\n")
    _git(source, "add", "tests")
    _git(source, "commit", "-qm", "Tracked test consumers")
    store.config.raw["repositories"]["fixture"]["expected_base_sha"] = _git(
        source, "rev-parse", "HEAD"
    )
    store.config.path.write_text(json.dumps(store.config.raw))
    original, initial, grant = _failed_published_repair_fixture(
        store, request, monkeypatch
    )
    store.continue_repair(request["run_id"], grant)
    store.mark_start(request["run_id"], accepted=True)
    with store._connect() as db:
        previous_recovery = json.loads(
            db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0]
        )
        if retry_generation:
            previous_recovery["kind"] = "repair_prelaunch_retry"
            db.execute(
                "UPDATE delivery_runs SET recovery_json=? WHERE run_id='run-1'",
                (json.dumps(previous_recovery),),
            )
    broker = DeliveryBroker(store, original)
    (broker.checkout / "README.md").write_text("Further owned implementation\n")
    candidate = broker.candidate()
    role3 = {
        "role": "implement", "iteration": 3, "status": "pass",
        "cleanup": "confirmed", "session_id": "original-session", "candidate": candidate,
    }
    review3 = {
        "role": "review", "iteration": 3, "status": "findings",
        "cleanup": "confirmed", "session_id": "independent-session-3",
        "candidate": candidate, "findings": ["Preserve the new Required capability"],
    }
    raw4 = {
        "role": "implement", "iteration": 4, "status": "findings",
        "session_id": "original-session",
        "finish_reason": "done",
        "findings": ["Two tracked manifest tests expect 137, observed 138"],
    }
    role4 = {**raw4, "cleanup": "confirmed", "candidate": candidate}
    roles = [*initial["roles"], role3, review3, role4]
    identity = {
        "run_id": request["run_id"], "role": "implement", "iteration": 4,
        "candidate_id": initial["candidate"]["id"],
        "policy_digest": original["policy_digest"],
    }
    final_job_key = digest({**identity, **(
        {"attempt_generation": retry_generation} if retry_generation else {}
    )})
    folder = Path(original["state_dir"]) / "attempts" / final_job_key
    folder.mkdir(parents=True, mode=0o700)
    receipt = folder / "result.json"
    receipt.write_text(json.dumps(raw4))
    receipt.chmod(0o600)
    with store._connect() as db:
        if retry_generation:
            db.execute(
                """INSERT INTO delivery_attempts
                   (job_key,run_id,role,iteration,candidate_id,state,session_id,
                    result_json,result_path,cleanup)
                   VALUES (?,?,?,?,?,'finished',NULL,?,NULL,'confirmed')""",
                (
                    digest(identity), request["run_id"], "implement", 4,
                    initial["candidate"]["id"],
                    json.dumps({"status": "blocked", "finish_reason": "prelaunch",
                                "cleanup": "confirmed", "session_id": None}),
                ),
            )
        for key, role, path in (
            ("role-implement-3", role3, None),
            ("role-review-3", review3, None),
            (final_job_key, {**raw4, "cleanup": "confirmed"}, receipt),
        ):
            db.execute(
                """INSERT INTO delivery_attempts
                   (job_key,run_id,role,iteration,candidate_id,state,session_id,
                    result_json,result_path,cleanup)
                   VALUES (?,?,?,?,?,'finished',?,?,?,'confirmed')""",
                (
                    key, request["run_id"], role["role"], role["iteration"],
                    (initial["candidate"]["id"] if key == final_job_key
                     else candidate["id"]), role["session_id"], json.dumps(role),
                    str(path) if path else None,
                ),
            )
    state = {
        **initial,
        "revision": 23,
        "iteration": 4,
        "error": "implementer did not establish a pass",
        "candidate": initial["candidate"],  # Public projection can lag the final activity.
        "roles": roles,
        "checks": {},
        "candidate_revision": 4,
    }
    store.project(
        request["run_id"], phase="blocked", execution_state="blocked",
        event_type="blocked", message=state["error"], candidate=initial["candidate"],
        pull_request=state["pull_request"], checks={}, iteration=4,
        protocol_revision=23, outcome="blocked", cleanup="none", error=state["error"],
    )
    closed = {
        "workflow_id": "delivery-run-1-repair-continuation-1",
        "execution_run_id": "closed-implementation-4",
        "closed_at": "2026-09-27T02:48:01+00:00",
        "request_digest": original["request_digest"],
        "recovery_digest": digest(previous_recovery),
        "result": state,
    }
    monkeypatch.setattr(store, "_completed_temporal_result", lambda _id, **_kw: closed)
    amended = copy.deepcopy(store.config.raw)
    amended["repositories"]["fixture"]["allowed_paths"] += additions
    path = store.config.state_root / "amendments" / "one.json"
    path.parent.mkdir(mode=0o700)
    path.write_text(json.dumps(amended))
    path.chmod(0o600)
    command = {
        "command_id": "amend-scope-1", "expected_revision": 23,
        "expected_iteration": 4, "expected_candidate_id": candidate["id"],
        "expected_pr_number": 7, "expected_pr_head": candidate["head"],
        "added_paths": additions, "amended_config_path": str(path),
        "amended_config_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    return original, state, command, closed


@pytest.mark.asyncio
@pytest.mark.parametrize("image_outage", [False, True])
async def test_public_scope_amendment_dispatches_one_original_session_temporal_role(
    service, monkeypatch, image_outage
):
    store, request = service
    async with local_temporal() as environment:
        store.config.raw["temporal_address"] = environment.client.service_client.config.target_host
        store.config.raw["queue"] = "scope-amendment-public-test"
        store.config.path.write_text(json.dumps(store.config.raw))
        original, state, command, closed = _scope_amendment_fixture(
            store, request, monkeypatch
        )
        monkeypatch.setattr(
            DeliveryStore, "_completed_temporal_result", lambda self, _id, **_kw: closed
        )
        app = create_app(store.config.path)
        origin = app.state.delivery.config.dashboard_url
        observed = []

        @activity.defn(name="delivery_tracker_start")
        async def tracker_stub(_payload):
            return {"state": "consistent"}

        @activity.defn(name="delivery_role")
        async def role_stub(payload):
            observed.append(payload)
            return {
                "role": "implement", "iteration": payload["iteration"],
                "status": "findings", "cleanup": "confirmed",
                "session_id": payload["resume_session"],
                "findings": ["fixture stops at the role boundary"],
            }

        async with Worker(
            environment.client,
            task_queue="scope-amendment-public-test",
            workflows=[DeliveryWorkflow],
            activities=[delivery_project, delivery_repair_preflight, tracker_stub, role_stub],
        ):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url=origin
            ) as browser:
                login = await browser.get("/api/session")
                assert login.status_code == 200
                posted = await browser.post(
                    "/api/runs/run-1/amend-scope", json=command,
                    headers={
                        "Origin": origin,
                        "X-Devflow-CSRF": login.json()["csrf_token"],
                    },
                )
                assert posted.status_code == 200, posted.text
                assert posted.json()["authorized_through_iteration"] == 5
                repeated = await browser.post(
                    "/api/runs/run-1/amend-scope", json=command,
                    headers={
                        "Origin": origin,
                        "X-Devflow-CSRF": login.json()["csrf_token"],
                    },
                )
                assert repeated.json() == posted.json()
            outage_calls = []
            if image_outage:
                admitted = delivery_store.scope_amended_spec

                def intermittent_readback(*args):
                    outage_calls.append(True)
                    if len(outage_calls) == 1:
                        raise RepairReadbackPending("temporary native authority readback outage")
                    return admitted(*args)

                monkeypatch.setattr(
                    delivery_store, "scope_amended_spec", intermittent_readback
                )
            await app.state.delivery.dispatch_once()
            result = await asyncio.wait_for(
                environment.client.get_workflow_handle(posted.json()["workflow_id"]).result(),
                timeout=30,
            )
    assert result["iteration"] == state["iteration"] + 1
    assert result["outcome"] == "blocked"  # The fixture role returned findings.
    assert len(observed) == 1
    if image_outage:
        assert len(outage_calls) >= 2
        assert any(
            event["type"] == "repair_preflight_pending"
            for event in store.detail("run-1")["events"]
        )
    assert observed[0]["resume_session"] == "original-session"
    assert observed[0]["candidate"]["id"] == state["roles"][-1]["candidate"]["id"]
    assert observed[0]["spec"]["policy"]["allowed_paths"] == [
        "README.md", *command["added_paths"]
    ]
    assert observed[0]["findings"] == [
        *state["roles"][-1]["findings"],
    ]
    with store._connect() as db:
        assert json.loads(
            db.execute("SELECT request_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0]
        ) == original
        assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM delivery_scope_amendments").fetchone()[0] == 1
        assert db.execute(
            "SELECT COUNT(*) FROM delivery_effects WHERE effect_key LIKE '%:5'"
        ).fetchone()[0] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("qa_state", ["passed", "unknown", "stale", "missing"])
async def test_managed_browser_qa_precedes_independent_verify_and_blocks_failure(
    qa_state, tmp_path,
):
    import zipfile
    from xml.etree import ElementTree

    from devflow_temporal.delivery_resources import private_directory, write_private
    from devflow_temporal.delivery_sandbox import _browser_qa_evidence
    calls = []
    candidate = {"id": "candidate-1", "head": "head-1"}
    spec = {
        "run_id": f"browser-qa-{qa_state}",
        "provider": "fake",
        "state_dir": str(tmp_path / f"browser-qa-{qa_state}"),
        "policy": {"max_repairs": 0, "browser_qa": {"id": "owned-browser"}},
    }

    folder = Path(spec["state_dir"]) / "browser-qa/0"
    private_directory(folder)
    receipt_path, log_path = folder / "receipt.json", folder / "browser-qa.log"
    receipt_sha, log_sha = None, None

    @activity.defn(name="delivery_project")
    async def project_stub(_payload):
        return {"revision": 1}

    @activity.defn(name="delivery_prepare")
    async def prepare_stub(_payload):
        return {"candidate": candidate}

    @activity.defn(name="delivery_tracker_start")
    async def start_stub(_payload):
        return {"state": "consistent"}

    @activity.defn(name="delivery_role")
    async def role_stub(payload):
        role = payload["role"]
        calls.append(role)
        if role in {"review", "verify"}:
            # Synthetic provider boundary: reject absent/stale real retained
            # receipt/artifact bytes instead of unconditionally returning pass.
            if not payload.get("qa_evidence"):
                return {"status": "findings", "findings": ["JUnit/traces remain missing"],
                        "candidate": candidate}
            try:
                _browser_qa_evidence(payload)
            except ValueError:
                return {"status": "findings", "findings": ["Stale browser evidence"],
                        "candidate": candidate}
            assert payload["qa_evidence"] == {
                "path": str(receipt_path), "sha256": receipt_sha,
                "log": str(log_path), "log_sha256": log_sha,
                "candidate_id": candidate["id"], "iteration": 0}
            receipt = json.loads(receipt_path.read_bytes())
            for artifact in receipt["artifacts"]:
                if not Path(artifact["path"]).is_file():
                    return {"status": "findings", "findings": ["Required trace is missing"],
                            "candidate": candidate}
                data = Path(artifact["path"]).read_bytes()
                assert hashlib.sha256(data).hexdigest() == artifact["sha256"]
            assert ElementTree.parse(folder / "junit.xml").find("testcase") is not None
            with zipfile.ZipFile(folder / "trace.zip") as trace:
                assert json.loads(trace.read("trace.trace"))["candidate_id"] == candidate["id"]
        return {"status": "pass", "session_id": f"fake:{role}", "candidate": candidate}

    @activity.defn(name="delivery_precheck")
    async def precheck_stub(_payload):
        return {"state": "passed"}

    @activity.defn(name="delivery_publish")
    async def publish_stub(_payload):
        return {"candidate": candidate, "head": candidate["head"]}

    @activity.defn(name="delivery_browser_qa")
    async def browser_stub(_payload):
        nonlocal receipt_sha, log_sha
        calls.append("browser_qa")
        # Explicit synthetic browser producer; real native reporter generation
        # is covered separately. Preserve actual JUnit/trace/log bytes here.
        (folder / "junit.xml").write_text('<testsuite><testcase name="owned"/></testsuite>')
        with zipfile.ZipFile(folder / "trace.zip", "w") as trace:
            trace.writestr("trace.trace", json.dumps({"candidate_id": candidate["id"]}))
        artifacts = [{"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                     for path in (folder / "junit.xml", folder / "trace.zip")]
        log_path.write_bytes(b"1 passed: synthetic browser actor\n")
        log_path.chmod(0o600)
        log_sha = hashlib.sha256(log_path.read_bytes()).hexdigest()
        receipt = {"state": "passed", "candidate_id": (
            "stale" if qa_state == "stale" else candidate["id"]), "iteration": 0,
            "log_sha256": log_sha, "artifacts": artifacts}
        write_private(receipt_path, receipt)
        if qa_state == "missing":
            (folder / "trace.zip").unlink()
        receipt_sha = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
        return {"state": "unknown" if qa_state == "unknown" else "passed",
                "cleanup": "unknown" if qa_state == "unknown" else "confirmed",
                "receipt": str(receipt_path), "receipt_sha256": receipt_sha,
                "log": str(log_path), "log_sha256": log_sha}

    @activity.defn(name="delivery_checks")
    async def checks_stub(_payload):
        calls.append("local_checks")
        return {"state": "passed"}

    @activity.defn(name="delivery_ci")
    async def ci_stub(_payload):
        return {"state": "passed"}

    @activity.defn(name="delivery_tracker")
    async def tracker_stub(_payload):
        return {"state": "consistent"}

    async with local_temporal() as environment:
        async with Worker(
            environment.client,
            task_queue=f"browser-qa-{qa_state}",
            workflows=[DeliveryWorkflow],
            activities=[
                project_stub,
                prepare_stub,
                start_stub,
                role_stub,
                precheck_stub,
                publish_stub,
                browser_stub,
                checks_stub,
                ci_stub,
                tracker_stub,
            ],
        ):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run,
                spec,
                id=spec["run_id"],
                task_queue=spec["run_id"],
            )
            result = await handle.result()
    assert calls[:3] == ["implement", "local_checks", "browser_qa"]
    assert ("review" in calls) is (qa_state != "unknown")
    assert ("verify" in calls) is (qa_state == "passed")
    assert result["outcome"] == ("delivered" if qa_state == "passed" else "blocked")
    if qa_state == "unknown":
        assert result["cleanup"] == "unknown"


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown_stage", ["prepublish", "local"])
async def test_managed_check_unknown_keeps_terminal_cleanup_quarantined(unknown_stage):
    candidate = {"id": "candidate-1", "head": "head-1"}
    spec = {
        "run_id": f"unknown-{unknown_stage}",
        "provider": "fake",
        "policy": {"max_repairs": 0, "browser_qa": None},
    }

    @activity.defn(name="delivery_project")
    async def project_stub(_payload):
        return {"revision": 1}

    @activity.defn(name="delivery_prepare")
    async def prepare_stub(_payload):
        return {"candidate": candidate}

    @activity.defn(name="delivery_tracker_start")
    async def start_stub(_payload):
        return {"state": "consistent"}

    @activity.defn(name="delivery_role")
    async def role_stub(payload):
        return {
            "status": "pass",
            "session_id": f"fake:{payload['role']}",
            "candidate": candidate,
        }

    @activity.defn(name="delivery_precheck")
    async def precheck_stub(_payload):
        return (
            {"state": "unknown", "cleanup": "unknown"}
            if unknown_stage == "prepublish"
            else {"state": "passed"}
        )

    @activity.defn(name="delivery_publish")
    async def publish_stub(_payload):
        return {"candidate": candidate, "head": candidate["head"]}

    @activity.defn(name="delivery_checks")
    async def checks_stub(_payload):
        return {"state": "unknown", "cleanup": "unknown"}

    async with local_temporal() as environment:
        async with Worker(
            environment.client,
            task_queue=spec["run_id"],
            workflows=[DeliveryWorkflow],
            activities=[
                project_stub,
                prepare_stub,
                start_stub,
                role_stub,
                precheck_stub,
                publish_stub,
                checks_stub,
            ],
        ):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run,
                spec,
                id=spec["run_id"],
                task_queue=spec["run_id"],
            )
            result = await handle.result()
    assert result["outcome"] == "blocked"
    assert result["cleanup"] == "unknown"
    assert result["checks"][unknown_stage]["state"] == "unknown"


@pytest.mark.asyncio
async def test_managed_decision_wait_survives_worker_restart(service, tmp_path):
    original, request = service
    config = json.loads(original.config.path.read_text())
    config["repositories"]["fixture"]["initial_decision_prompt"] = "Proceed with this run?"
    original.config.path.write_text(json.dumps(config))
    store = DeliveryStore(DeliveryConfig.load(original.config.path))
    store.submit(request)
    admitted = store.spec(request["run_id"])
    calls = []

    @activity.defn(name="delivery_tracker_start")
    async def tracker_start_stub(_payload):
        return {"state": "consistent"}

    @activity.defn(name="delivery_role")
    async def role_stub(payload):
        calls.append(payload["role"])
        return {"status": "blocked", "candidate": payload["candidate"]}

    activities = [delivery_project, delivery_prepare, tracker_start_stub, role_stub]
    async with local_temporal(
        dev_server_database_filename=str(tmp_path / "decision-temporal.sqlite3")
    ) as environment:
        queue = "managed-decision-restart"
        async with Worker(
            environment.client,
            task_queue=queue,
            workflows=[DeliveryWorkflow],
            activities=activities,
        ):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, admitted, id="delivery-run-1", task_queue=queue
            )
            store.mark_start(request["run_id"], accepted=True)
            for _ in range(50):
                if store.detail(request["run_id"])["decisions"]:
                    break
                await asyncio.sleep(0.05)
            waiting = store.detail(request["run_id"])
            assert waiting["decisions"][0]["id"] == "run-1:initial"
            assert waiting["decisions"][0]["candidate_revision"] == 1
            assert calls == []
        async with Worker(
            environment.client,
            task_queue=queue,
            workflows=[DeliveryWorkflow],
            activities=activities,
        ):
            current = await handle.query("status")
            assert current["phase"] == "waiting_decision"
            with pytest.raises(WorkflowUpdateFailedError):
                await handle.execute_update(
                    "decision",
                    {
                        "expected_revision": current["revision"],
                        "decision_id": "wrong",
                        "decision_revision": 1,
                        "candidate_revision": 1,
                        "answer": "proceed",
                    },
                )
            accepted = await handle.execute_update(
                "decision",
                {
                    "expected_revision": current["revision"],
                    "decision_id": "run-1:initial",
                    "decision_revision": 1,
                    "candidate_revision": 1,
                    "answer": "proceed",
                },
            )
            assert accepted["decision"] is None
            final = await asyncio.wait_for(handle.result(), 15)
        assert final["outcome"] == "blocked"
        assert calls == ["implement"]
        assert store.detail(request["run_id"])["decisions"] == []


@pytest.mark.asyncio
async def test_managed_cancel_wins_over_overlapping_failed_precheck(service, tmp_path):
    original, request = service
    config = json.loads(original.config.path.read_text())
    config["repositories"]["fixture"]["allowed_paths"].append("devflow-fake-change.txt")
    original.config.path.write_text(json.dumps(config))
    store = DeliveryStore(DeliveryConfig.load(original.config.path))
    store.submit(request)
    entered = asyncio.Event()
    release = asyncio.Event()

    @activity.defn(name="delivery_tracker_start")
    async def tracker_start_stub(_payload):
        return {"state": "consistent"}

    @activity.defn(name="delivery_precheck")
    async def failing_precheck(_payload):
        entered.set()
        await release.wait()
        return {"state": "failed", "results": []}

    @activity.defn(name="delivery_role")
    async def role_stub(payload):
        return {
            "status": "pass",
            "summary": "Fixture role completed",
            "findings": [],
            "candidate": payload["candidate"],
            "session_id": "fake:implement",
            "usage": None,
        }

    activities = [
        delivery_project,
        delivery_prepare,
        role_stub,
        tracker_start_stub,
        failing_precheck,
    ]
    async with local_temporal(
        dev_server_database_filename=str(tmp_path / "cancel-temporal.sqlite3")
    ) as environment:
        async with Worker(
            environment.client,
            task_queue="managed-cancel-race",
            workflows=[DeliveryWorkflow],
            activities=activities,
        ):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run,
                store.spec(request["run_id"]),
                id="delivery-run-1",
                task_queue="managed-cancel-race",
            )
            store.mark_start(request["run_id"], accepted=True)
            await asyncio.wait_for(entered.wait(), 15)
            current = await handle.query("status")
            assert current["phase"] == "prepublish_checks"
            accepted = await handle.execute_update(
                "cancel", {"expected_revision": current["revision"], "reason": "stop now"}
            )
            assert accepted["execution_state"] == "cancelling"
            release.set()
            final = await asyncio.wait_for(handle.result(), 15)
        assert final["outcome"] == "cancelled"
        assert final["cleanup"] == "confirmed_after_role_boundary"
        detail = store.detail(request["run_id"])
        assert detail["outcome"] == "cancelled"
        assert detail["phase"] == "cancelled"
        assert detail["checks"]["prepublish"]["state"] == "failed"
        assert (
            next(gate for gate in detail["phase_gates"] if gate["id"] == "prepublish")["state"]
            == "failed"
        )


@pytest.mark.asyncio
async def test_cancelled_role_with_unknown_teardown_retains_unknown_cleanup(service, tmp_path):
    original, request = service
    store = original
    store.submit(request)
    entered = asyncio.Event()
    release = asyncio.Event()

    @activity.defn(name="delivery_tracker_start")
    async def tracker_start_stub(_payload):
        return {"state": "consistent"}

    @activity.defn(name="delivery_role")
    async def ambiguous_role(payload):
        entered.set()
        await release.wait()
        with store._connect() as db:
            db.execute(
                """INSERT INTO delivery_attempts
                   (job_key,run_id,role,iteration,candidate_id,state,cleanup)
                   VALUES (?,?,?,0,?,'unknown','unknown')""",
                ("unknown-attempt", request["run_id"], "implement", payload["candidate"]["id"]),
            )
        return {
            "status": "recovery_unknown",
            "summary": "Child exited without a final receipt",
            "findings": ["outcome ambiguous"],
            "candidate": payload["candidate"],
            "session_id": None,
            "cleanup": "unknown",
            "finish_reason": "recovery_unknown",
        }

    async with local_temporal(
        dev_server_database_filename=str(tmp_path / "unknown-cancel.sqlite3")
    ) as environment:
        async with Worker(
            environment.client,
            task_queue="unknown-cancel",
            workflows=[DeliveryWorkflow],
            activities=[delivery_project, delivery_prepare, tracker_start_stub, ambiguous_role],
        ):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run,
                store.spec(request["run_id"]),
                id="delivery-run-1",
                task_queue="unknown-cancel",
            )
            store.mark_start(request["run_id"], accepted=True)
            await asyncio.wait_for(entered.wait(), 15)
            status = await handle.query("status")
            accepted = await handle.execute_update(
                "cancel", {"expected_revision": status["revision"], "reason": "stop"}
            )
            assert accepted["execution_state"] == "cancelling"
            release.set()
            final = await asyncio.wait_for(handle.result(), 15)
    assert final["outcome"] == "cancelled"
    assert final["cleanup"] == "unknown"
    detail = store.detail(request["run_id"])
    assert detail["cleanup"] == "unknown"
    assert detail["capacity"]["active"] == 1


@workflow.defn(name="LegacyUnencodedEcho")
class LegacyUnencodedEcho:
    @workflow.run
    async def run(self, value: dict) -> dict:
        return value


@pytest.mark.asyncio
@pytest.mark.skipif(not shutil.which("temporal"), reason="local Temporal CLI required")
async def test_large_public_submit_dispatches_through_real_temporal_and_safe_activities(
    service, tmp_path
):
    store, request = service
    request["accepted_plan"] = "".join(
        f"Review criterion {index:06d}: preserve sealed evidence.\n"
        for index in range(53_000)
    )
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    raw = json.loads(store.config.path.read_text())
    raw.update({
        "temporal_address": f"127.0.0.1:{port}",
        "queue": "delivery-large-payload-test",
    })
    store.config.path.write_text(json.dumps(raw))
    app = create_app(store.config.path)
    service_runtime = app.state.delivery
    seen: dict[str, int] = {}

    @activity.defn(name="delivery_prepare")
    async def prepare_stub(payload):
        seen["prepare_plan_bytes"] = len(payload["spec"]["accepted_plan"].encode())
        return await delivery_prepare(payload)

    @activity.defn(name="delivery_tracker_start")
    async def tracker_start_stub(_payload):
        return {"state": "consistent"}

    @activity.defn(name="delivery_role")
    async def role_stub(payload):
        seen[payload["role"]] = len(payload["spec"]["accepted_plan"].encode())
        return {
            "role": payload["role"], "iteration": payload["iteration"],
            "status": "pass", "cleanup": "confirmed", "findings": [],
            "summary": "Disposable safe activity passed",
            "session_id": f"fake:{payload['role']}",
            "candidate": payload["candidate"],
        }

    @activity.defn(name="delivery_precheck")
    async def precheck_stub(payload):
        return {"state": "passed", "candidate_id": payload["candidate"]["id"]}

    @activity.defn(name="delivery_publish")
    async def publish_stub(payload):
        return {
            "number": 1, "url": "https://example.invalid/pull/1",
            "state": "OPEN", "head": payload["candidate"]["head"],
            "base": payload["spec"]["base_sha"],
            "candidate": payload["candidate"],
        }

    @activity.defn(name="delivery_checks")
    async def checks_stub(payload):
        return {"state": "passed", "candidate_id": payload["candidate"]["id"]}

    @activity.defn(name="delivery_ci")
    async def ci_stub(payload):
        return {"state": "passed", "head": payload["pull_request"]["head"]}

    @activity.defn(name="delivery_tracker")
    async def tracker_stub(_payload):
        return {"state": "consistent", "observed": {"fixture": True}}

    server = await asyncio.create_subprocess_exec(
        "temporal", "server", "start-dev", "--headless", "--ip", "127.0.0.1",
        "--port", str(port), "--db-filename", str(tmp_path / "temporal.sqlite3"),
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        client = None
        for _ in range(100):
            try:
                client = await Client.connect(
                    f"127.0.0.1:{port}", data_converter=DELIVERY_DATA_CONVERTER
                )
                break
            except Exception as exc:
                if server.returncode is not None:
                    raise RuntimeError("disposable Temporal exited") from exc
                await asyncio.sleep(0.1)
        assert client is not None
        origin = app.state.delivery.config.dashboard_url
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=origin
        ) as browser:
            login = await browser.get("/api/session")
            assert login.status_code == 200
            headers = {
                "Origin": origin,
                "X-Devflow-CSRF": login.json()["csrf_token"],
            }
            accepted = await browser.post("/api/runs", json=request, headers=headers)
            assert accepted.status_code == 200
            replay = await browser.post("/api/runs", json=request, headers=headers)
            assert replay.json() == accepted.json()
            with service_runtime.store._connect() as db:
                row = db.execute(
                    "SELECT request_digest FROM delivery_runs WHERE run_id=?",
                    (request["run_id"],),
                ).fetchone()
                receipt = db.execute(
                    "SELECT request_digest,response_json FROM delivery_commands "
                    "WHERE command_id=?", (request["command_id"],),
                ).fetchone()
                assert len(receipt["request_digest"]) == 64
                assert json.loads(receipt["response_json"]) == accepted.json()
                plain = await DataConverter.default.encode([
                    service_runtime.store.spec(request["run_id"])
                ])
                assert len(plain[0].data) > 2_000_000
            async with Worker(
                client, task_queue=raw["queue"], workflows=[DeliveryWorkflow],
                activities=[
                    delivery_project, prepare_stub, tracker_start_stub, role_stub,
                    precheck_stub, publish_stub, checks_stub, ci_stub, tracker_stub,
                ],
            ):
                await service_runtime.dispatch_once()
                result = await asyncio.wait_for(
                    client.get_workflow_handle("delivery-run-1").result(), timeout=35
                )
            assert result["outcome"] == "delivered"
            assert seen == {
                key: len(request["accepted_plan"].encode())
                for key in ("prepare_plan_bytes", "implement", "review", "verify")
            }
            detail = await browser.get("/api/runs/run-1")
            assert detail.json()["run"]["outcome"] == "delivered"
            description = await client.get_workflow_handle("delivery-run-1").describe()
            assert await description.memo_value("request_digest", None) == row["request_digest"]

            legacy_client = await Client.connect(f"127.0.0.1:{port}")
            legacy_value = {"history": "unencoded result"}
            async with Worker(
                legacy_client, task_queue=raw["queue"] + "-legacy",
                workflows=[LegacyUnencodedEcho],
                workflow_runner=UnsandboxedWorkflowRunner(),
            ):
                legacy = await legacy_client.start_workflow(
                    LegacyUnencodedEcho.run, legacy_value,
                    id="legacy-unencoded", task_queue=raw["queue"] + "-legacy",
                    memo={"receipt_digest": digest(legacy_value)},
                )
                assert await asyncio.wait_for(legacy.result(), timeout=10) == legacy_value
            older = client.get_workflow_handle("legacy-unencoded")
            assert await older.result() == legacy_value
            assert await (await older.describe()).memo_value(
                "receipt_digest", None
            ) == digest(legacy_value)
            with service_runtime.store._connect() as db:
                unchanged = db.execute(
                    "SELECT request_digest,response_json FROM delivery_commands "
                    "WHERE command_id=?", (request["command_id"],),
                ).fetchone()
                assert tuple(unchanged) == tuple(receipt)
                assert db.execute(
                    "SELECT state FROM delivery_outbox WHERE run_id=?",
                    (request["run_id"],),
                ).fetchone()[0] == "sent"
    finally:
        server.terminate()
        await server.wait()
