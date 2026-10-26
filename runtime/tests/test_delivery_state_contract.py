from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
from temporal_test_server import local_temporal
from temporalio import activity, workflow
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_store import service as service

from devflow_temporal.delivery_activities import delivery_project, delivery_tracker_start
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_workflow import DeliveryWorkflow, _initial_controller_state


@activity.defn(name="delivery_gates_readback")
async def owned_gate_admission(request):
    assert request["recovery"]["execution_spec"] == request["spec"]
    return {"state": "confirmed", "scope": "controlled owned recovery admission boundary"}


@activity.defn(name="delivery_checks")
async def failing_owned_command(request):
    completed = await asyncio.to_thread(
        subprocess.run, [sys.executable, "-c", "print('owned check fails'); raise SystemExit(7)"],
        capture_output=True, text=True, check=False,
    )
    assert completed.returncode == 7
    return {"state": "failed", "cleanup": "confirmed", "source_unchanged": True,
            "results": [{"id": "owned-required", "exit_code": completed.returncode,
                         "output": completed.stdout}]}


@workflow.defn
class OwnedCiRetry:
    @workflow.run
    async def run(self, request):
        return await DeliveryWorkflow()._resume_ci(
            request["spec"], request["recovery"], recheck_local=True,
        )


def project(store, event, *, phase=None, state="running", checks=None, revision=None):
    return store.project(
        "run-1", phase=phase or event, execution_state=state, event_type=event,
        message=event, iteration=0, protocol_revision=revision, checks=checks,
    )


def gates(store):
    return {gate["id"]: gate["state"] for gate in store.detail("run-1")["phase_gates"]}


@pytest.mark.parametrize("observed,expected", [("failed", "failed"), ("unknown", "unknown")])
def test_same_iteration_retry_result_supersedes_previous_completion(service, observed, expected):
    store, request = service
    store.submit(request)
    project(store, "ci_wait", checks={"local": {"state": "passed"}})
    assert gates(store)["local_checks"] == "completed"
    project(store, "checks_retry_started", phase="checks",
            checks={"local": {"state": "passed"}})
    assert gates(store)["local_checks"] == "pending"
    project(store, "blocked", state="blocked", checks={"local": {"state": observed}})
    assert gates(store)["local_checks"] == expected


def test_new_retry_is_pending_while_previous_failure_is_carried(service):
    store, request = service
    store.submit(request)
    project(store, "ci_wait", checks={"local": {"state": "passed"}})
    project(store, "blocked", state="blocked", checks={"local": {"state": "failed"}})
    project(store, "checks_retry_started", phase="checks",
            checks={"local": {"state": "failed"}})
    assert gates(store)["local_checks"] == "pending"
    project(store, "ci_retry_started", phase="waiting_ci",
            checks={"local": {"state": "passed"}, "ci": {"state": "failed"}})
    assert gates(store)["local_checks"] == "completed"
    assert gates(store)["required_ci"] == "pending"
    project(store, "tracker_started", phase="reconciling_tracker",
            checks={"local": {"state": "passed"}, "ci": {"state": "passed"}})
    assert gates(store)["required_ci"] == "completed"


def test_gate_reset_survives_display_event_window(service):
    store, request = service
    store.submit(request)
    project(store, "candidate_ready")
    project(store, "ci_wait")
    project(store, "gates_retry_started", phase="prepublish_checks", checks={})
    for _ in range(205):
        project(store, "diagnostic", phase="prepublish_checks")
    detail = store.detail("run-1")
    assert len(detail["events"]) == 200
    assert gates(store)["prepublish"] == "pending"
    assert gates(store)["local_checks"] == "pending"


def test_late_projection_cannot_erase_newer_operator_control(service):
    store, request = service
    store.submit(request)
    control = {"disabled": True, "protocol_revision": 5, "command_id": "owned-command"}
    project(store, "control", phase="waiting", state="waiting",
            checks={"delivery_limits_control": control}, revision=5)
    with ThreadPoolExecutor(max_workers=2) as pool:
        stale = pool.submit(project, store, "checks_started", phase="checks",
                            checks={"local": {"state": "passed"}}, revision=4)
        latest = pool.submit(project, store, "current_readback", phase="waiting",
                             state="waiting", revision=5)
        stale.result()
        latest.result()
    detail = store.detail("run-1")
    assert detail["phase"] == "waiting"
    assert detail["protocol_revision"] == 5
    assert detail["checks"]["delivery_limits_control"] == control
    assert "checks_started" not in {event["type"] for event in detail["events"]}


def test_stale_projection_preserves_all_current_persisted_fields(service):
    store, request = service
    store.submit(request)
    project(store, "current", phase="waiting", state="waiting", revision=9,
            checks={"ci": {"state": "unknown"}})
    with store._connect() as db:
        before = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
    returned = project(store, "old", phase="implement", revision=8, checks={})
    with store._connect() as db:
        after = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
    assert returned == before == after
    assert json.loads(after["checks_json"])["ci"]["state"] == "unknown"


def test_persisted_status_remains_available_without_environment_proof(service, monkeypatch):
    store, request = service
    store.submit(request)
    original = store.submitted_spec("run-1")
    project(store, "ci_wait", revision=3, checks={"local": {"state": "passed"}})

    def unavailable(*args, **kwargs):
        raise AssertionError("ordinary recorded status must not reconstruct execution custody")

    monkeypatch.setattr(store, "_connect", unavailable)
    monkeypatch.setattr(store, "effective_spec", unavailable)
    status = store.persisted_status("run-1")
    assert status["state_contract"]["version"] == 1
    assert status["state_contract"]["environment"]["verification"] == "not_reconstructed"
    assert status["state_contract"]["execution"]["verification"] == "recorded"
    assert status["state_contract"]["authorization"]["policy_digest"] == original["policy_digest"]
    local = next(g for g in status["phase_gates"] if g["id"] == "local_checks")
    assert local["state"] == "completed"


def test_capacity_edit_changes_no_recorded_authorization_or_candidate(service):
    store, request = service
    store.submit(request)
    candidate = {"id": "owned-candidate", "head": "a" * 40}
    store.project("run-1", phase="checks", execution_state="running", event_type="checks_started",
                  message="Owned candidate", candidate=candidate)
    before = store.persisted_status("run-1")
    original = store.submitted_spec("run-1")
    raw = json.loads(store.config.path.read_text())
    raw["capacity"] = 1
    store.config.path.write_text(json.dumps(raw))
    after = store.persisted_status("run-1")
    assert before == after
    assert after["candidate"] == candidate
    assert store.submitted_spec("run-1") == original


@pytest.mark.asyncio
async def test_public_recorded_status_reads_decision_without_launch_proof(service, monkeypatch):
    store, request = service
    store.submit(request)
    decision = {"id": "owned-decision", "kind": "approval", "revision": 2}
    store.project("run-1", phase="waiting_approval", execution_state="waiting",
                  event_type="decision_pending", message="Owned decision",
                  protocol_revision=2, decision=decision)
    app = create_app(store.config.path)

    def unavailable(*args, **kwargs):
        raise AssertionError("public recorded status must not reconstruct launch proof")

    monkeypatch.setattr(app.state.delivery.store, "effective_spec", unavailable)
    monkeypatch.setattr(app.state.delivery.store, "detail", unavailable)
    monkeypatch.setattr(app.state.delivery.store, "_connect", unavailable)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url=store.config.dashboard_url) as browser:
        response = await browser.get("/api/runs/run-1/status")
        missing = await browser.get("/api/runs/absent/status")
    assert response.status_code == 200
    assert missing.status_code == 404
    result = response.json()
    assert result["version"] == 1 and result["source"] == "persisted_projection"
    assert result["run"]["decisions"] == [decision]
    assert result["run"]["protocol_revision"] == 2
    assert result["run"]["state_contract"]["environment"]["verification"] == "not_reconstructed"


@pytest.mark.asyncio
async def test_real_failed_retry_reaches_public_gate_as_failed(service, tmp_path):
    """Real Temporal/controller/failed subprocess/ASGI; gate admission is controlled."""
    store, request = service
    store.submit(request)
    spec = store.spec("run-1")
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    candidate = broker.candidate()
    publication = {"number": 1, "url": "https://example.invalid/pull/1",
                   "head": candidate["head"], "candidate": candidate}
    previous = _initial_controller_state(spec)
    previous.update(revision=3, candidate=candidate, pull_request=publication, phase="waiting_ci",
                    checks={"local": {"state": "passed"}, "ci": {"state": "unknown"}})
    store.project("run-1", phase="waiting_ci", execution_state="running", event_type="ci_wait",
                  message="Earlier local completion", iteration=0, protocol_revision=3,
                  candidate=candidate, pull_request=publication, checks=previous["checks"])
    recovery = {"kind": "published_ci_retry", "execution_spec": spec, "state": previous,
                "candidate": candidate, "publication": publication,
                "command": {"additional_iterations": 0}, "seal": {"local_download_retry": True}}
    database = str(tmp_path / "temporal.sqlite3")
    async with local_temporal(dev_server_database_filename=database) as env:
        async with Worker(env.client, task_queue="owned-gate-contract", workflows=[OwnedCiRetry],
                          activities=[owned_gate_admission, failing_owned_command,
                                      delivery_project, delivery_tracker_start],
                          workflow_runner=UnsandboxedWorkflowRunner()):
            result = await env.client.execute_workflow(
                OwnedCiRetry.run, {"spec": spec, "recovery": recovery}, id="owned-gate-retry",
                task_queue="owned-gate-contract",
            )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(store.config.path)),
                                 base_url=store.config.dashboard_url) as browser:
        response = await browser.get("/api/runs/run-1")
        assert response.status_code == 200
        public = response.json()["run"]
    assert result["outcome"] == "blocked"
    assert result["checks"]["local"]["results"][0]["exit_code"] == 7
    assert public["checks"]["local"]["state"] == "failed"
    assert next(g for g in public["phase_gates"] if g["id"] == "local_checks")["state"] == "failed"
    assert public["state_contract"]["authorization"]["policy_digest"] == spec["policy_digest"]
    assert store.submitted_spec("run-1") == spec
