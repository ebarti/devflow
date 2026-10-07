"""Required CI is exact-head verification, not a twenty-minute race."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer
from test_delivery_store import service as service

from devflow_temporal import delivery_activities, delivery_broker
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_config import DeliveryConfig, scope_amended_spec
from devflow_temporal.delivery_workflow import DeliveryWorkflow


class Clock:
    def __init__(self):
        self.seconds = 0
        self.delays = []

    def time(self):
        return self.seconds

    async def sleep(self, seconds):
        self.delays.append(seconds)
        self.seconds += seconds


async def observe(monkeypatch, *, limit=10800, mode="slow", legacy=False):
    clock = Clock()
    broker = object.__new__(delivery_broker.DeliveryBroker)
    broker.spec = {"policy": {"required_ci": ["Build"]}, "github_repo": "example/fixture"}
    if not legacy:
        broker.spec["policy"]["ci_wait_seconds"] = limit
    reads = []

    def readback(*args, **kwargs):
        reads.append(clock.seconds)
        if mode == "transient" and clock.seconds < 1800:
            raise RuntimeError("GitHub readback temporarily unavailable")
        conclusion = "SUCCESS" if clock.seconds >= 1800 else None
        if mode == "failure":
            conclusion = "FAILURE"
        head = "replacement" if mode == "changed_head" else "candidate"
        return json.dumps({"headRefOid": head, "statusCheckRollup": [
            {"__typename": "CheckRun", "name": "Build", "conclusion": conclusion},
        ]})

    async def off_loop(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(delivery_broker, "_run", readback)
    monkeypatch.setattr(delivery_broker.asyncio, "get_running_loop", lambda: clock)
    monkeypatch.setattr(delivery_broker.asyncio, "sleep", clock.sleep)
    monkeypatch.setattr(delivery_broker.asyncio, "to_thread", off_loop)
    return await broker.checks({"number": 1, "head": "candidate"}), clock, reads


@pytest.mark.asyncio
async def test_healthy_ci_can_finish_after_twenty_minutes(monkeypatch):
    result, clock, _ = await observe(monkeypatch)
    assert result["state"] == "passed", f"healthy CI abandoned after {clock.seconds}s"
    assert result["head"] == "candidate"
    assert 1800 <= clock.seconds < 1900
    assert max(clock.delays) > min(clock.delays)
    assert max(clock.delays) <= 60


@pytest.mark.asyncio
async def test_readback_errors_remain_bounded_and_can_recover(monkeypatch):
    result, _, _ = await observe(monkeypatch, mode="transient")
    assert result["state"] == "passed"


@pytest.mark.asyncio
async def test_configured_deadline_returns_pending_without_extending_it(monkeypatch):
    result, clock, _ = await observe(monkeypatch, limit=61)
    assert result["state"] == "pending"
    assert clock.seconds == 61
    assert result["checks"]["Build"]["conclusion"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("mode,expected", [("failure", "failed"), ("changed_head", "stale")])
async def test_failure_and_head_change_stop_immediately(monkeypatch, mode, expected):
    result, clock, reads = await observe(monkeypatch, mode=mode)
    assert result["state"] == expected
    assert clock.seconds == 0
    assert len(reads) == 1


@pytest.mark.asyncio
async def test_old_frozen_policy_keeps_twenty_minute_behavior(monkeypatch):
    result, clock, _ = await observe(monkeypatch, legacy=True)
    assert result["state"] == "pending"
    assert clock.seconds == 1200
    assert set(clock.delays) == {15}


@pytest.mark.asyncio
async def test_github_read_does_not_block_worker_event_loop(monkeypatch):
    import threading

    started, release = threading.Event(), threading.Event()
    broker = object.__new__(delivery_broker.DeliveryBroker)
    broker.spec = {"policy": {"required_ci": ["Build"], "ci_wait_seconds": 60},
                   "github_repo": "example/fixture"}

    def readback(*args, **kwargs):
        started.set()
        # The watchdog prevents this regression from hanging the test suite.
        release.wait(1)
        return json.dumps({"headRefOid": "candidate", "statusCheckRollup": [
            {"__typename": "CheckRun", "name": "Build", "conclusion": "SUCCESS"},
        ]})

    monkeypatch.setattr(delivery_broker, "_run", readback)
    pending = asyncio.create_task(broker.checks({"number": 1, "head": "candidate"}))
    try:
        while not started.is_set():
            await asyncio.sleep(0)
        assert not pending.done(), "synchronous GitHub read blocked the whole worker"
        release.set()
        assert (await pending)["state"] == "passed"
    finally:
        release.set()
        await pending


@pytest.mark.parametrize("configured", [None, 60, 3600, 43200])
def test_deadline_is_frozen_at_admission(service, configured):
    store, request = service
    raw = dict(store.config.raw)
    if configured is not None:
        raw["ci_wait_seconds"] = configured
    store.config.path.write_text(json.dumps(raw))
    config = DeliveryConfig.load(store.config.path)
    expected = 10800 if configured is None else configured
    assert config.admit(request)["policy"]["ci_wait_seconds"] == expected
    assert config.public_policy()["ci_wait_seconds"] == expected
    with pytest.raises(ValueError):
        config.admit({**request, "ci_wait_seconds": expected + 1})


@pytest.mark.parametrize("invalid", [True, 59, 43201, 60.0, "3600"])
def test_deadline_rejects_invalid_config(service, invalid):
    store, _ = service
    raw = {**store.config.raw, "ci_wait_seconds": invalid}
    store.config.path.write_text(json.dumps(raw))
    with pytest.raises(ValueError, match="ci_wait_seconds"):
        DeliveryConfig.load(store.config.path)


@pytest.mark.asyncio
@pytest.mark.parametrize("configured", [None, 3600, 10800])
async def test_activity_window_follows_only_new_frozen_policy(monkeypatch, configured):
    calls = []

    async def execute(name, request, **options):
        calls.append((name, options))
        return {"state": "passed"}

    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.execute_activity", execute)
    policy = {} if configured is None else {"ci_wait_seconds": configured}
    flow = DeliveryWorkflow()
    await flow._activity("delivery_ci", {"spec": {"policy": policy}}, hours=1)
    options = calls[0][1]
    if configured is None:
        assert options["start_to_close_timeout"] == timedelta(hours=1)
        assert "heartbeat_timeout" not in options
        assert options["retry_policy"].maximum_attempts == 1
    else:
        assert options["start_to_close_timeout"] > timedelta(seconds=configured)
        assert options["schedule_to_close_timeout"] == options["start_to_close_timeout"]
        assert options["heartbeat_timeout"] == timedelta(seconds=30)


@pytest.mark.asyncio
async def test_ci_activity_heartbeats_and_cancels_its_readback(monkeypatch):
    entered, cancelled = asyncio.Event(), asyncio.Event()
    heartbeats = []

    async def checks(pr):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    broker = SimpleNamespace(checks=checks)
    monkeypatch.setattr(delivery_activities, "_context", lambda spec: (None, broker))
    monkeypatch.setattr(delivery_activities.activity, "heartbeat", heartbeats.append)
    request = {"spec": {"run_id": "run-ci", "policy": {"ci_wait_seconds": 10800}},
               "pull_request": {"number": 1, "head": "candidate"}}
    pending = asyncio.create_task(delivery_activities.delivery_ci(request))
    await entered.wait()
    try:
        assert heartbeats, "pending CI does not heartbeat"
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_previous_code_history_through_ci_still_replays():
    path = Path(__file__).parent / "fixtures" / "required_ci" / "c04-ci-previous-history.json"
    history = WorkflowHistory.from_json("delivery-run-1-stopped-resume-1", path.read_text())
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)


@pytest.mark.parametrize("legacy", [False, True])
def test_scope_amendment_preserves_frozen_ci_policy(service, legacy):
    store, request = service
    original = store.config.admit(request)
    original["request_digest"] = digest(request)
    if legacy:
        original["policy"].pop("ci_wait_seconds")
        original["policy_digest"] = digest(original["policy"])
    raw = json.loads(store.config.path.read_text())
    raw["repositories"]["fixture"]["allowed_paths"].append("tests/extra.py")
    path = store.config.state_root / "ci-amendment.json"
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_text(json.dumps(raw))
    path.chmod(0o600)
    effective = scope_amended_spec(
        original, path, hashlib.sha256(path.read_bytes()).hexdigest(), ["tests/extra.py"]
    )
    assert ("ci_wait_seconds" in effective["policy"]) == ("ci_wait_seconds" in original["policy"])
    assert effective["policy"].get("ci_wait_seconds") == original["policy"].get("ci_wait_seconds")
    assert effective["policy_digest"] == digest(effective["policy"])
