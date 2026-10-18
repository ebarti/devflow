"""Delivery time bounds cancel actual activities and retain real dirty Git work."""
from __future__ import annotations

import asyncio
import json
import shutil
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from temporalio import activity, workflow
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from test_delivery_store import service as service

from devflow_temporal import delivery_limits, delivery_store
from devflow_temporal.delivery_activities import delivery_finalize_resources, delivery_project
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.mark.parametrize("value", [True, 0, -1, 86401, 1.5, "60"])
def test_bad_time_policy_rejected_before_admission(value):
    with pytest.raises(ValueError):
        delivery_limits.policy({"delivery_timeout_seconds": value})


def test_progress_clock_ignores_poll_and_phase_changes(monkeypatch):
    start = datetime(2026, 10, 18, tzinfo=UTC)
    now = [start]
    monkeypatch.setattr(workflow, "now", lambda: now[0])
    state = {"candidate": {"id": "same"}, "roles": [], "checks": {}}
    clock = {"admitted_at": start.isoformat(), "last_progress_at": start.isoformat()}
    controller = SimpleNamespace(delivery_clock=clock, state=state)
    delivery_limits.observe(controller)
    now[0] += timedelta(minutes=5)
    state.update(phase="waiting_ci", revision=100)
    state["checks"]["ci"] = {"state": "pending", "attempt": 42}
    delivery_limits.observe(controller)
    assert clock["last_progress_at"] == start.isoformat()
    state["checks"]["local"] = {"state": "passed", "candidate_id": "same"}
    delivery_limits.observe(controller)
    assert clock["last_progress_at"] == now[0].isoformat()
    assert clock["admitted_at"] == start.isoformat()


def configured(service, key, seconds):
    original, request = service
    path = original.config.path
    raw = json.loads(path.read_text())
    raw[key] = seconds
    path.write_text(json.dumps(raw))
    return DeliveryStore(DeliveryConfig.load(path)), request


def test_replaying_admission_does_not_reset_its_clock(service, monkeypatch):
    store, request = configured(service, "delivery_timeout_seconds", 3600)
    store.submit(request)
    started = store.spec(request["run_id"])["delivery_admitted_at"]
    monkeypatch.setattr(delivery_store, "_now", lambda: "2030-01-01T00:00:00+00:00")
    store.submit(request)
    assert store.spec(request["run_id"])["delivery_admitted_at"] == started


@pytest.mark.asyncio
@pytest.mark.parametrize("key,kind", [("delivery_timeout_seconds", "elapsed"),
                                     ("no_progress_timeout_seconds", "no_progress")])
async def test_actual_timeout_cancels_then_finalizes_and_replays(
    service, tmp_path, monkeypatch, key, kind,
):
    started, cancelled = asyncio.Event(), asyncio.Event()
    finalized = []

    @activity.defn(name="delivery_prepare")
    async def hang(_payload):
        started.set()
        try:
            while True:
                activity.heartbeat("owned fixture activity")
                await asyncio.sleep(0.1)
        finally:
            cancelled.set()

    @activity.defn(name="delivery_finalize_resources")
    async def finalize(payload):
        assert cancelled.is_set(), "cleanup cannot precede stopped owned activity"
        result = await delivery_finalize_resources(payload)
        finalized.append(result)
        return result

    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "clock-temporal.sqlite3"),
    ) as env:
        admitted = DeliveryConfig.admit

        def resource_aware_admission(self, request):
            # Controlled fake provider uses the production resource-aware protocol.
            return {**admitted(self, request), "resource_cleanup_version": 1}

        monkeypatch.setattr(DeliveryConfig, "admit", resource_aware_admission)
        store, request = configured(service, key, 5)
        store.submit(request)
        spec = store.spec(request["run_id"])
        broker = DeliveryBroker(store, spec)
        broker.prepare()
        dirty = broker.checkout / "README.md"
        dirty.write_text("Keep this unfinished candidate\n")
        async with Worker(env.client, task_queue="bounded-delivery", workflows=[DeliveryWorkflow],
                          activities=[hang, finalize, delivery_project],
                          default_heartbeat_throttle_interval=timedelta(seconds=1)):

            handle = await env.client.start_workflow(
                DeliveryWorkflow.run, spec, id="delivery-" + spec["run_id"],
                task_queue="bounded-delivery",
            )
            await asyncio.wait_for(started.wait(), 15)
            result = await asyncio.wait_for(handle.result(), 20)
            assert cancelled.is_set()
            assert result["outcome"] == "cancelled"
            limit = result["checks"]["delivery_limit"]
            assert limit["kind"] == kind
            assert "time limit reached" in result["error"]
            assert limit["handoff"]["evidence_root"] == spec["state_dir"]
            assert limit["handoff"]["run_id"] == spec["run_id"]
            assert not result["roles"] and result["iteration"] == 0
            assert finalized and finalized[-1]["state"] == "confirmed"
            assert dirty.read_text() == "Keep this unfinished candidate\n"
            history = await handle.fetch_history()
        await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)


def test_capacity_change_and_feature_continuation_keep_original_elapsed_anchor(
    service, monkeypatch,
):
    from test_delivery_feature_execution import feature_service

    from devflow_temporal.delivery_feature_execution import continue_feature, registry

    configured_store, request = configured(service, "delivery_timeout_seconds", 3600)
    store, request, _ = feature_service((configured_store, request), monkeypatch)
    store.submit(request)
    original = store.effective_spec(request["run_id"])
    registry(original).stop(original["feature_delivery"]["owner"], "stopped", {})
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET phase='cancelled',outcome='cancelled',"
                   "execution_state='terminal' WHERE run_id=?", (original["run_id"],))
        store.state.release_work(db, original["work_id"], "external:devflow:" + original["run_id"])
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_: {"result": {}})
    monkeypatch.setattr(delivery_store, "_now", lambda: "2030-01-01T00:00:00+00:00")
    raw = json.loads(store.config.path.read_text())
    raw["capacity"] = 6
    store.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(store.config.path))
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_: {"result": {}})
    command = {"command_id": "continue-with-same-deadline", "expected_revision": 1}
    result = continue_feature(store, original["run_id"], command)
    successor = store.effective_spec(result["run_id"])
    assert successor["delivery_admitted_at"] == original["delivery_admitted_at"]
    assert successor["policy"]["delivery_timeout_seconds"] == 3600
    issue = successor["feature_delivery"]["owner"]["issue_id"]
    assert registry(successor).budget(issue)["used"] == 0
