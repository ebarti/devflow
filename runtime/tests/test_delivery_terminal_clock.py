"""Real terminal-only reconciliation after the original product clock expired."""

import asyncio
import json

import httpx
import pytest
import test_delivery_terminal_recovery as terminal
from temporal_test_server import local_temporal
from temporalio import activity, workflow
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_intake import intake_fixture
from test_delivery_native import native_configuration

from devflow_temporal.delivery_activities import delivery_project, delivery_terminal_preflight
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_resources import RunResources
from devflow_temporal.delivery_store import DeliveryStore


@workflow.defn(name="DevflowDeliveryWorkflow")
class TerminalClockFixture(terminal.ClosedTerminalFixture):
    fixture_outcome = "blocked"

    @workflow.run
    async def run(self, spec, recovery=None):
        if recovery:
            return await super().run(spec, recovery)
        outcome = self.fixture_outcome
        self.state = {
            "run_id": spec["run_id"],
            "phase": outcome,
            "execution_state": outcome,
            "outcome": outcome,
            "iteration": 2,
            "revision": 13,
            "cleanup": "none",
            "checks": {},
            "roles": [],
            "error": "Preserved failed gate",
        }
        await self._project(spec, outcome, "Preserved failed gate")
        return self.state


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True], ids=["modern", "legacy"])
@pytest.mark.parametrize("outcome", ["blocked", "cancelled"])
async def test_expired_product_clock_preserves_terminal_checkpoint(
    tmp_path, monkeypatch, legacy, outcome
):
    # Root cause: the successor started the expired product timer before restoring
    # terminal settlement mode, allowing preflight to replace the sealed verdict.
    if legacy:
        terminal.legacy_tracker_admission.__wrapped__(monkeypatch)
    config, request = native_configuration.__wrapped__(intake_fixture.__wrapped__(tmp_path))
    raw = json.loads(config.path.read_text())
    raw["execution_registry"] = str(tmp_path / "registry" / "registry.sqlite3")
    raw["delivery_timeout_seconds"] = 2
    config.path.write_text(json.dumps(raw))
    config = DeliveryConfig.load(config.path)
    app = create_app(config.path)
    service = app.state.delivery
    service.store.submit(request)
    spec = service.store.spec(request["run_id"])
    monkeypatch.setattr(TerminalClockFixture, "fixture_outcome", outcome)
    original_clock = spec["delivery_admitted_at"]
    calls = []

    @activity.defn(name="delivery_finalize_resources")
    async def cleanup(payload):
        calls.append("cleanup")
        with service.store._connect() as db:
            service.store.state.release_work(
                db, spec["work_id"], "external:devflow:" + spec["run_id"]
            )
        return RunResources(payload["spec"]).finalize(outcome)

    @activity.defn(name="delivery_terminal_tracker")
    async def tracker(payload):
        calls.append("tracker")
        pending = calls.count("tracker") <= 3
        return {
            "state": "pending" if pending else "consistent",
            "pending": pending,
            "retryable": pending,
            "desired": payload["status"],
            "readback_at": "fixture-current",
        }

    async with local_temporal(
        dev_server_database_filename=str(tmp_path / "temporal.sqlite3")
    ) as env:

        async def client():
            return env.client

        monkeypatch.setattr(service, "client", client)
        monkeypatch.setattr(service, "healthy_client", client)
        async with Worker(
            env.client,
            task_queue=config.queue,
            workflows=[TerminalClockFixture],
            workflow_runner=UnsandboxedWorkflowRunner(),
            activities=[delivery_project, cleanup, tracker, delivery_terminal_preflight],
        ):
            original = await env.client.start_workflow(
                TerminalClockFixture.run,
                args=[spec, None],
                id="delivery-" + spec["run_id"],
                task_queue=config.queue,
                memo={"request_digest": spec["request_digest"]},
            )
            prior = await asyncio.wait_for(original.result(), 25)
            assert prior["outcome"] is None
            assert prior["checks"]["terminal_tracker_checkpoint"]["outcome"] == outcome
            assert calls == ["cleanup", "tracker", "tracker", "tracker"]
            before = service.store.detail(spec["run_id"])
            assert before["phase"] == "waiting_tracker"
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url=config.dashboard_url
            ) as browser:
                session = await browser.get("/api/session")
                headers = {
                    "Origin": config.dashboard_url,
                    "X-Devflow-CSRF": session.json()["csrf_token"],
                }
                endpoint = f"/api/runs/{spec['run_id']}/reconcile-tracker"
                payload = {
                    "command_id": "reconcile-original-terminal",
                    "expected_revision": before["protocol_revision"],
                }
                receipt = await browser.post(endpoint, headers=headers, json=payload)
                assert receipt.status_code == 200, receipt.text
                assert receipt.json()["reconciliation_only"]
                service.store = DeliveryStore(config)
                repeated = await browser.post(endpoint, headers=headers, json=payload)
                assert repeated.status_code == 200 and repeated.json() == receipt.json()
                queued = service.store.pending_starts()
                assert len(queued) == 1
                assert json.loads(queued[0]["recovery_json"])["kind"] == "terminal_tracker_recovery"
                assert service.store.spec(spec["run_id"])["delivery_admitted_at"] == original_clock
                await service.dispatch_once()
                successor = env.client.get_workflow_handle(receipt.json()["workflow_id"])
                final = await asyncio.wait_for(successor.result(), 20)
                public = (await browser.get(f"/api/runs/{spec['run_id']}")).json()["run"]
                history = await successor.fetch_history()
                activities = [
                    e.activity_task_scheduled_event_attributes.activity_type.name
                    for e in history.events
                    if e.HasField("activity_task_scheduled_event_attributes")
                ]
                assert "delivery_terminal_preflight" in activities
                assert "delivery_terminal_tracker" in activities
                assert set(activities) <= {
                    "delivery_project",
                    "delivery_terminal_preflight",
                    "delivery_terminal_tracker",
                }
                assert final["outcome"] == public["outcome"] == outcome
                assert final["error"] == "Preserved failed gate"
                assert final["checks"]["terminal_tracker_checkpoint"]["outcome"] == outcome
                assert final["roles"] == []
                assert calls == ["cleanup", "tracker", "tracker", "tracker", "tracker"]
                assert service.store.spec(spec["run_id"])["delivery_admitted_at"] == original_clock
