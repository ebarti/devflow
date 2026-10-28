"""Real SDK successor, with controlled recovery admission and original start receipt."""

import asyncio
import json
from datetime import timedelta
from types import SimpleNamespace

import pytest
from temporal_test_server import local_temporal
from temporalio import workflow
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_pause_question import _controlled_intake_started
from test_delivery_pause_question import intake_fixture as intake_fixture
from test_delivery_questions import BLOCKER

from devflow_temporal import delivery_activities as activities
from devflow_temporal.delivery_api import _dispatch_start
from devflow_temporal.delivery_question_sender import pump_blocking_questions


@workflow.defn(name="DevflowDeliveryWorkflow")
class SuccessorQuestion:
    @workflow.run
    async def run(self, spec, recovery=None):
        decision = {"id": "run-1:question:2", "kind": "question", "revision": 2,
                    "candidate_revision": 1, "prompt": "Successor question?",
                    "options": ["Yes", "No"], "state": "pending", "blocker": BLOCKER}
        await workflow.execute_activity("delivery_project", {
            "spec": spec, "phase": "waiting_question", "execution_state": "waiting",
            "event_type": "question_pending", "message": "Successor question",
            "decision": decision, "protocol_revision": 5, "key": "successor-question",
        }, start_to_close_timeout=timedelta(seconds=30))
        await workflow.wait_condition(lambda: False)


@pytest.mark.asyncio
async def test_real_sdk_successor_projects_and_sends_question(intake_fixture, tmp_path,
                                                            monkeypatch):
    async with local_temporal() as env:
        path, _request = intake_fixture
        config = json.loads(path.read_text())
        config["temporal_address"] = env.client.service_client.config.target_host
        path.write_text(json.dumps(config))
        store, log = _controlled_intake_started(intake_fixture, tmp_path, monkeypatch)
        # The original start receipt and recovery admission are controlled SQL
        # fixtures; the successor SDK event-1 and projection activity are real.
        recovery = json.dumps({"kind": "owned-recovery"}, sort_keys=True, separators=(",", ":"))
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET workflow_id=?,recovery_json=? WHERE run_id=?",
                       ("delivery-run-1-successor", recovery, "run-1"))
            db.execute("UPDATE delivery_outbox SET state='pending' WHERE run_id='run-1'")
        spec = store.effective_spec("run-1")
        async with Worker(env.client, task_queue=store.config.queue,
                          workflow_runner=UnsandboxedWorkflowRunner(),
                          workflows=[SuccessorQuestion], activities=[activities.delivery_project],
                          interceptors=[activities.DatabaseContentionInterceptor()]):
            await _dispatch_start(SimpleNamespace(store=store, config=store.config), env.client,
                                  store.pending_starts()[0], spec)
            async with asyncio.timeout(30):
                while True:
                    detail = store.detail("run-1")
                    if detail["decisions"] and detail["decisions"][0]["id"] == "run-1:question:2":
                        break
                    await asyncio.sleep(.1)
            handle = env.client.get_workflow_handle("delivery-run-1-successor")
            sdk = await handle.describe()
            with store._connect() as db:
                selected = db.execute("SELECT execution_run_id FROM delivery_execution_authority "
                                      "WHERE workflow_id=?", (handle.id,)).fetchone()
                assert selected[0] == sdk.run_id
            await asyncio.to_thread(pump_blocking_questions, store)
            await asyncio.to_thread(pump_blocking_questions, store)
            assert len(log.read_text().splitlines()) == 1
            notifications = store.question_notifications("run-1")
            assert {row["decision_id"]: row["state"] for row in notifications} == {
                "run-1:question:1": "suppressed", "run-1:question:2": "queued"}
            await handle.terminate("owned successor fixture cleanup")
