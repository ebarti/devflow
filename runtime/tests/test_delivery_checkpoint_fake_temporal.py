"""Public recovery through a real Temporal controller and the fixed fake provider."""

from __future__ import annotations

import asyncio
import json
import shutil
from contextlib import suppress
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_store import service as service

from devflow_temporal import delivery_activities as activities
from devflow_temporal.delivery_api import _dispatch_start, create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_continuation import session_state_paths
from devflow_temporal.delivery_resource_maintenance import prune
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.mark.asyncio
async def test_public_recovery_resumes_real_fake_implementation(service, monkeypatch):
    """Catch a false resumable ACK when the real fake role cannot be continued."""
    async with local_temporal() as environment:
        original, request = service
        config = json.loads(original.config.path.read_text())
        config["temporal_address"] = environment.client.service_client.config.target_host
        config["workspace_custody_version"] = 1
        config["repositories"]["fixture"]["allowed_paths"].append("devflow-fake-change.txt")
        original.config.path.write_text(json.dumps(config))
        store = DeliveryStore(DeliveryConfig.load(original.config.path))
        # Fake admissions normally omit terminal resource cleanup. Enable the real
        # finalization activity in this fixture so it covers the retention contract.
        admitted_spec = store.config.admit(request)
        admitted_spec["resource_cleanup_version"] = 1
        with monkeypatch.context() as admission:
            admission.setattr(DeliveryConfig, "admit", lambda *_args, **_kwargs: admitted_spec)
            store.submit(request)
        spec = store.effective_spec(request["run_id"])
        broker = DeliveryBroker(store, spec)
        calls = []

        @activity.defn(name="delivery_precheck")
        async def stopped_gate(body):
            calls.append(("stopped-gate", body["iteration"]))
            return {"state": "failed", "source_unchanged": True,
                    "candidate_id": body["candidate"]["id"], "results": [{
                        "id": "fixture-prerequisite", "passed": False,
                        "failure_kind": "preparation", "launched": False,
                        "cleanup": "confirmed"}]}

        @activity.defn(name="delivery_tracker_start")
        async def tracker_start(_body):
            return {"state": "consistent"}

        service_context = SimpleNamespace(store=store, config=store.config)
        async with Worker(
            environment.client, task_queue=store.config.queue,
            workflow_runner=UnsandboxedWorkflowRunner(), workflows=[DeliveryWorkflow],
            activities=[activities.delivery_project, activities.delivery_prepare,
                        activities.delivery_role, activities.delivery_finalize_resources,
                        stopped_gate, tracker_start],
            interceptors=[activities.DatabaseContentionInterceptor()],
        ):
            await _dispatch_start(
                service_context, environment.client, store.pending_starts()[0], spec)
            original_handle = environment.client.get_workflow_handle("delivery-" + spec["run_id"])
            try:
                stopped = await asyncio.wait_for(original_handle.result(), 30)
            except BaseException:
                with suppress(Exception):
                    await original_handle.terminate("owned fixture cleanup")
                raise

        assert stopped["outcome"] == "blocked" and stopped["cleanup"] == "confirmed"
        assert stopped["checks"]["prepublish"]["state"] == "failed"
        assert calls == [("stopped-gate", 0)]
        implementation = stopped["roles"][0]
        assert implementation["status"] == "pass" and implementation["cleanup"] == "confirmed"
        session_id = implementation["session_id"]
        assert session_id == "fake:" + spec["run_id"] + ":implement"
        candidate = stopped["candidate"]
        home = Path(spec["state_dir"]) / "role-homes/implement"
        (home / "codex/sessions").mkdir(parents=True, mode=0o700, exist_ok=True)
        assert not list((home / "codex/sessions").rglob("*"))
        # The normal provider-session contract still refuses missing transcripts.
        with pytest.raises(ValueError, match="continuation session transcript is missing"):
            session_state_paths(home, session_id)
        with store._connect() as db:
            attempt = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=? AND role='implement'",
                (spec["run_id"],)).fetchone()
            assert attempt["state"] == "finished" and attempt["cleanup"] == "confirmed"
            assert json.loads(attempt["result_json"])["cleanup"] == "confirmed"
            assert json.loads(Path(attempt["result_path"]).read_text()) == json.loads(
                attempt["result_json"])
            # The tracker boundary is a fixture; emulate its terminal claim release.
            store.state.release_work(db, spec["work_id"], "external:devflow:" + spec["run_id"])
            row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                                  (spec["run_id"],)).fetchone())

        # No actual GitHub operation is made by the fixture's tracker or publisher.
        monkeypatch.setattr(DeliveryStore, "_ensure_no_remote_pr", lambda *_args: None)
        app = create_app(store.config.path)
        app.state.delivery.store = store
        url = store.config.dashboard_url.rstrip("/")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=url,
        ) as client:
            headers = {"origin": url, "x-devflow-csrf": (await client.get("/api/session")).json()[
                "csrf_token"]}
            path = "/api/runs/" + spec["run_id"] + "/checkpoints"
            saved = await client.post(path, headers=headers, json={
                "command_id": "fake-temporal-save", "expected_revision": row["revision"]})
            assert saved.status_code == 200, saved.text
            assert saved.json()["resumable"]
            pruned = prune(store, spec["run_id"])
            assert any(root["path"] == str(broker.checkout) and root["state"] == "retained"
                       for root in pruned["roots"])
            retained = await client.get(path + "/" + saved.json()["checkpoint_id"])
            assert retained.status_code == 200 and retained.json()["resumable"]
            shutil.rmtree(Path(spec["source_path"]))
            shutil.rmtree(broker.checkout)
            if home.exists():
                shutil.rmtree(home)
            restored = await client.post(
                path + "/" + saved.json()["checkpoint_id"] + "/restore", headers=headers,
                json={"command_id": "fake-temporal-restore", "expected_revision": row["revision"]})
            assert restored.status_code == 200, restored.text
            assert restored.json()["execution_started"] is False
            assert broker.candidate() == candidate
            assert (broker.checkout / "devflow-fake-change.txt").read_text() == (
                "Deterministic fake change 0\n")
            command = {"continuation_kind": "stopped_delivery_resume",
                       "command_id": "fake-temporal-resume",
                       "expected_revision": stopped["revision"],
                       "expected_iteration": stopped["iteration"],
                       "expected_candidate_id": candidate["id"],
                       "expected_candidate_head": candidate["head"], "additional_iterations": 1}
            response = await client.post("/api/runs/" + spec["run_id"] + "/continue-repair",
                                         headers=headers, json=command)
            assert response.status_code == 200, response.text
            assert response.json()["run_id"] == spec["run_id"]
            assert response.json()["authorized_through_iteration"] == 1
            replay = await client.post("/api/runs/" + spec["run_id"] + "/continue-repair",
                                       headers=headers, json=command)
            assert replay.status_code == 200 and replay.json() == response.json()

        with store._connect() as db:
            current = db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id=?",
                                 (spec["run_id"],)).fetchone()
            recovery = json.loads(current["recovery_json"])
            assert recovery["candidate"] == candidate and recovery["session_id"] == session_id
            assert recovery["state"]["checks"] == stopped["checks"]
            assert recovery["state"]["usage"] == stopped["usage"]
            assert db.execute("SELECT count(*) FROM delivery_runs").fetchone()[0] == 1
            assert store.state.claim_for(db, spec["work_id"])["owner"] == (
                "external:devflow:" + spec["run_id"])
            assert not db.execute("SELECT 1 FROM delivery_effects WHERE kind='publish'").fetchone()

        @activity.defn(name="delivery_role")
        async def role(body):
            if body["role"] == "implement":
                assert body["resume_session"] == session_id
                assert body["candidate"] == candidate
                calls.append(("continued-implementation", body["iteration"]))
                return await activities.delivery_role(body)
            return {"status": "pass", "role": body["role"], "iteration": body["iteration"],
                    "session_id": "fixture-" + body["role"], "candidate": body["candidate"],
                    "cleanup": "confirmed", "findings": []}

        @activity.defn(name="delivery_precheck")
        async def passing_gate(body):
            calls.append(("passing-gate", body["iteration"]))
            return {"state": "passed", "candidate_id": body["candidate"]["id"]}

        @activity.defn(name="delivery_publish")
        async def publish(body):
            calls.append(("fixture-publication", body["iteration"]))
            return {"number": 7, "head": body["candidate"]["head"],
                    "candidate": body["candidate"], "state": "OPEN"}

        @activity.defn(name="delivery_checks")
        async def checks(body):
            return {"state": "passed", "candidate_id": body["candidate"]["id"]}

        @activity.defn(name="delivery_ci")
        async def ci(body):
            return {"state": "passed", "head": body["pull_request"]["head"]}

        @activity.defn(name="delivery_tracker")
        async def tracker(_body):
            return {"state": "consistent"}

        async with Worker(
            environment.client, task_queue=store.config.queue,
            workflow_runner=UnsandboxedWorkflowRunner(), workflows=[DeliveryWorkflow],
            activities=[activities.delivery_project, activities.delivery_repair_preflight,
                        activities.delivery_finalize_resources, role, passing_gate, publish,
                        checks, ci, tracker_start, tracker],
            interceptors=[activities.DatabaseContentionInterceptor()],
        ):
            await _dispatch_start(service_context, environment.client, store.pending_starts()[0],
                                  store.effective_spec(spec["run_id"]))
            successor = environment.client.get_workflow_handle(response.json()["workflow_id"])
            try:
                result = await asyncio.wait_for(successor.result(), 30)
            except BaseException:
                with suppress(Exception):
                    await successor.terminate("owned fixture cleanup")
                raise

        assert result["outcome"] == "delivered"  # Fixture publication and gates only.
        assert result["roles"][0] == implementation
        assert result["roles"][1]["session_id"] == session_id
        assert result["usage"]["implement:0"] == stopped["usage"]["implement:0"]
        assert calls == [("stopped-gate", 0), ("continued-implementation", 1),
                         ("passing-gate", 1), ("fixture-publication", 1)]
        assert (broker.checkout / "devflow-fake-change.txt").read_text() == (
            "Deterministic fake change 1\n")
