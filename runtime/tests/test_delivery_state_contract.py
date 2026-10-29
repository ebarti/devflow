from __future__ import annotations

import asyncio
import subprocess
import sys
import threading
from datetime import timedelta

import httpx
import pytest
from temporal_test_server import local_temporal
from temporalio import activity, workflow
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_store import service as service

from devflow_temporal.delivery_activities import delivery_project, delivery_tracker_start
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_store import DeliveryStore
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


@workflow.defn
class OwnedProjection:
    @workflow.run
    async def run(self, request):
        return await workflow.execute_activity(
            "delivery_project", request, start_to_close_timeout=timedelta(seconds=15),
        )


def project(store, event, *, phase=None, state="running", checks=None, revision=None):
    return store.project(
        "run-1", phase=phase or event, execution_state=state, event_type=event,
        message=event, iteration=0, protocol_revision=revision, checks=checks,
    )


@pytest.mark.asyncio
async def test_real_late_activity_cannot_project_over_successor_owner(
    service, monkeypatch, tmp_path,
):
    store, request = service
    store.submit(request)
    spec = store.spec("run-1")
    entered, release = threading.Event(), threading.Event()
    original_project = DeliveryStore.project

    def held_projection(self, run_id, **values):
        if values.get("event_type") == "old_owner":
            entered.set()
            assert release.wait(10)
        return original_project(self, run_id, **values)

    monkeypatch.setattr(DeliveryStore, "project", held_projection)
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET workflow_id='owned-old-owner' WHERE run_id='run-1'")
    payload = {"spec": spec, "phase": "checks", "execution_state": "running",
               "event_type": "old_owner", "message": "Old body already passed custody check",
               "key": "old-owner:0:2", "iteration": 0, "protocol_revision": 2}
    database = str(tmp_path / "temporal.sqlite3")
    async with local_temporal(dev_server_database_filename=database) as env:
        async with Worker(env.client, task_queue="owned-projection-owner",
                          workflows=[OwnedProjection], activities=[delivery_project],
                          workflow_runner=UnsandboxedWorkflowRunner()):
            handle = await env.client.start_workflow(
                OwnedProjection.run, payload, id="owned-old-owner",
                task_queue="owned-projection-owner",
            )
            try:
                assert await asyncio.to_thread(entered.wait, 10)
                with store._connect() as db:
                    db.execute("BEGIN IMMEDIATE")
                    db.execute("UPDATE delivery_runs SET workflow_id='owned-new-owner',"
                               "phase='repair_continuation_queued',execution_state='queued',"
                               "protocol_revision=2,revision=revision+1 WHERE run_id='run-1'")
                release.set()
                assert (await asyncio.wait_for(handle.result(), 10))["phase"] == (
                    "repair_continuation_queued")
            finally:
                release.set()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(store.config.path)),
                                 base_url=store.config.dashboard_url) as browser:
        public = await browser.get("/api/runs/run-1/status")
    assert public.status_code == 200
    assert public.json()["run"]["phase"] == "repair_continuation_queued"
    assert "old_owner" not in {event["type"] for event in store.events("run-1")}


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
