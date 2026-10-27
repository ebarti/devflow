"""Real legacy delivery workflow after an intake-only pause and resume."""

import asyncio
import json

import httpx
import pytest
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_pause_intake_public import _controlled_cutover
from test_delivery_pause_question import intake_fixture as intake_fixture
from test_delivery_questions import THREAD, _queue_binary

from devflow_temporal import delivery_activities as activities
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.mark.asyncio
async def test_public_intake_resume_real_workflow_question_sender(intake_fixture, tmp_path,
                                                                 monkeypatch):
    # Only fresh-installation cutover is controlled. Workflow, activities,
    # tracking DB, SDK start history and queue subprocess are real fixture paths.
    _controlled_cutover(monkeypatch)
    async with local_temporal() as env:
        path, request = intake_fixture
        binary, log = _queue_binary(tmp_path)
        config = json.loads(path.read_text())
        config["codex_bin"] = str(binary)
        config["temporal_address"] = env.client.service_client.config.target_host
        path.write_text(json.dumps(config))
        app = create_app(path)
        store = app.state.delivery.store
        origin = store.config.dashboard_url

        @activity.defn(name="delivery_tracker_start")
        async def tracker(_request):
            return {"state": "consistent"}

        @activity.defn(name="delivery_role")
        async def role(request):
            return {"status": "blocked", "candidate": request["candidate"]}

        @activity.defn(name="delivery_intake")
        async def controlled_intake(request):
            # Controlled provider output: the local native fixture cannot prove
            # descendant cleanup, so it cannot be used as a real provider result.
            return {**config["fake_intake"][0], "role": "intake", "cleanup": "confirmed",
                    "iteration": request["iteration"], "candidate": request["candidate"],
                    "input_candidate_id": request["candidate"]["id"], "provider": "fake"}

        registered = [activities.delivery_project, activities.delivery_prepare,
                      controlled_intake, activities.delivery_accept_plan, tracker, role]
        async with (Worker(env.client, task_queue=store.config.queue,
                           workflow_runner=UnsandboxedWorkflowRunner(),
                           workflows=[DeliveryWorkflow], activities=registered,
                           interceptors=[activities.DatabaseContentionInterceptor()]),
                    app.router.lifespan_context(app)):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                         base_url=origin) as client:
                session = await client.get("/api/session")
                headers = {"Origin": origin, "X-Devflow-CSRF": session.json()["csrf_token"]}
                submitted = await client.post("/api/runs", headers=headers, json={
                    **request, "origin_thread_id": THREAD, "start_paused": True})
                assert submitted.status_code == 200, submitted.text
                async with asyncio.timeout(40):
                    while True:
                        status = (await client.get("/api/runs/run-1/pause-control")).json()
                        if status["observed"] == "paused":
                            break
                        await asyncio.sleep(.1)
                resumed = await client.post("/api/runs/run-1/resume", headers=headers, json={
                    "command_id": "resume-question", "expected_revision": status["revision"],
                    "epoch": status["epoch"]})
                assert resumed.status_code == 200, resumed.text
                async with asyncio.timeout(60):
                    while True:
                        rows = store.question_notifications("run-1")
                        if rows and rows[0]["state"] == "queued":
                            break
                        await asyncio.sleep(.1)
                with store._connect() as db:
                    root = db.execute("SELECT * FROM delivery_pause_control WHERE root_id='run-1'")
                    assert root.fetchone()["execution_run_id"] == ""
                    assert db.execute("SELECT COUNT(*) FROM delivery_pause_actors"
                                      ).fetchone()[0] == 0
                    assert db.execute("SELECT COUNT(*) FROM delivery_pause_start_receipts"
                                      ).fetchone()[0] == 1
                    assert db.execute("SELECT state FROM delivery_outbox").fetchone()[0] == "sent"
                assert len(log.read_text().splitlines()) == 1
                detail = store.detail("run-1")
                assert detail["phase"] == "waiting_question"
                assert detail["decisions"][0]["prompt"] == "Which wording?"
                await app.state.delivery.dispatch_questions_once()
                assert len(log.read_text().splitlines()) == 1
                await env.client.get_workflow_handle("delivery-run-1").terminate("fixture cleanup")
