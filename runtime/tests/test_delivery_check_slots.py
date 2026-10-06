from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_delivery_resources import spec

from devflow_temporal import delivery_activities as activities
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_native_process import NativeProcess, process_table


@pytest.fixture
def owned_checks(tmp_path, monkeypatch):
    active = 0
    maximum = 0
    lock = threading.Lock()
    brokers = {}
    hearts = []
    config = SimpleNamespace(state_root=tmp_path, raw={"check_concurrency": 2})
    database = tmp_path / "fixture.sqlite3"
    with sqlite3.connect(database) as db:
        db.execute("CREATE TABLE delivery_runs(run_id TEXT, phase TEXT)")

    class Broker(DeliveryBroker):
        def __init__(self, value):
            self.spec = value
            self.store = SimpleNamespace(_connect=self.connect)
            self.native_cleanup_confirmed = True
            self.check_cancelled = lambda: False
            self.started = threading.Event()
            self.finished = threading.Event()
            self.result = None

        def connect(self):
            db = sqlite3.connect(database)
            db.row_factory = sqlite3.Row
            return db

        def run_checks(self, iteration, candidate):
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
            self.started.set()
            try:
                # The harmless process creates a real descendant, observed and
                # stopped by the production PID/start-identity monitor.
                code = (
                    "import subprocess,sys,time; "
                    "subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']); "
                    f"time.sleep({self.spec['sleep']})"
                )
                self.native_cleanup_confirmed = False
                self.result = NativeProcess(
                    self.spec, Path(self.spec["state_dir"]) / "native",
                    argv=[sys.executable, "-c", code], cwd=tmp_path,
                    environment={"PATH": os.environ["PATH"]},
                    timeout=self.spec["timeout"], cancelled=self._native_cancelled,
                ).run()
                self.native_cleanup_confirmed = (
                    self.result["cleanup"] == "observed-native-confirmed"
                )
                return {"state": "passed", "cleanup": "confirmed"}
            finally:
                with lock:
                    active -= 1
                self.finished.set()

        run_prechecks = run_checks
        run_browser_qa = run_checks

    def context(value):
        broker = brokers[value["run_id"]]
        return SimpleNamespace(config=config), broker

    def request(name, seconds=0.6, timeout=5):
        value = spec(tmp_path, name)
        value.update(provider="codex", sleep=seconds, timeout=timeout)
        brokers[name] = Broker(value)
        return {"spec": value, "iteration": 0, "candidate": {"id": name}}

    monkeypatch.setattr(activities, "_context", context)
    monkeypatch.setattr(activities.activity, "in_activity", lambda: True)
    monkeypatch.setattr(activities.activity, "heartbeat", hearts.append)
    monkeypatch.setattr(activities, "_CHECK_HEARTBEAT_INTERVAL", 0.05, raising=False)
    return SimpleNamespace(
        request=request, brokers=brokers, hearts=hearts, config=config,
        maximum=lambda: maximum,
    )


async def wait_until(predicate):
    async with asyncio.timeout(10):
        while not predicate():
            await asyncio.sleep(0.02)


def assert_stopped(broker):
    assert broker.result["cleanup"] == "observed-native-confirmed"
    journal = json.loads(Path(broker.result["journal"]).read_text())
    table = process_table()
    assert len(journal["owned"]) >= 2
    assert not any(
        table.get(int(pid), {}).get("identity") == entry["identity"]
        and not table[int(pid)]["stat"].startswith("Z")
        for pid, entry in journal["owned"].items()
    )


async def test_checks_overlap_but_obey_finite_slots(owned_checks):
    requests = [owned_checks.request(str(i)) for i in range(3)]
    await asyncio.gather(*(activities.delivery_checks(value) for value in requests))
    assert owned_checks.maximum() == 2
    for broker in owned_checks.brokers.values():
        assert_stopped(broker)


@pytest.mark.parametrize("stop", ["cancel", "timeout"])
async def test_activity_stop_joins_real_native_tree_before_slot_reuse(owned_checks, stop):
    owned_checks.config.raw["check_concurrency"] = 1
    request = owned_checks.request("stopped", seconds=2)
    broker = owned_checks.brokers["stopped"]
    task = asyncio.create_task(activities.delivery_checks(request))
    journal = Path(broker.spec["state_dir"]) / "native/native-process.json"
    await wait_until(journal.exists)
    await asyncio.sleep(0.2)
    following = asyncio.create_task(activities.delivery_precheck(owned_checks.request("following")))
    try:
        if stop == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(task, timeout=0.05)
        assert broker.finished.is_set()
        assert broker.result["cancelled"] is True
        assert_stopped(broker)
    finally:
        await wait_until(broker.finished.is_set)
        await following
    assert owned_checks.maximum() == 1


async def test_waiting_heartbeats_and_native_timeout_starts_after_admission(owned_checks):
    owned_checks.config.raw["check_concurrency"] = 1
    first = asyncio.create_task(activities.delivery_checks(owned_checks.request("first", 1.3)))
    await wait_until(owned_checks.brokers["first"].started.is_set)
    second = asyncio.create_task(
        activities.delivery_browser_qa(owned_checks.request("queued", seconds=0.2, timeout=1))
    )
    await asyncio.gather(first, second)
    queued = owned_checks.brokers["queued"]
    assert any(h["run_id"] == "queued" and h["stage"] == "waiting-check-slot"
               for h in owned_checks.hearts)
    assert queued.result["timed_out"] is False
    assert_stopped(queued)


async def test_cancelled_waiter_never_launches(owned_checks):
    owned_checks.config.raw["check_concurrency"] = 1
    first = asyncio.create_task(activities.delivery_checks(owned_checks.request("first")))
    await wait_until(owned_checks.brokers["first"].started.is_set)
    request = owned_checks.request("queued")
    queued = asyncio.create_task(activities.delivery_checks(request))
    await asyncio.sleep(0.1)
    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued
    await first
    await asyncio.sleep(0.1)
    launched = owned_checks.brokers["queued"].started.is_set()
    # On old code the thread survives cancellation; join it to keep test resources owned.
    if launched:
        await wait_until(owned_checks.brokers["queued"].finished.is_set)
    assert not launched
