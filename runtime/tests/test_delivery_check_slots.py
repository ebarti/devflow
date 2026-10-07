from __future__ import annotations

import asyncio
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import threading
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from temporalio import workflow
from temporalio.client import WorkflowFailureError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_resources import spec
from test_delivery_store import service as service

from devflow_temporal import delivery_activities as activities
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_native_process import NativeProcess, process_table, reconcile_process
from devflow_temporal.delivery_resources import RunResources
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@workflow.defn
class CheckSlotCancellationFixture:
    @workflow.run
    async def run(self, request: dict) -> dict:
        return await DeliveryWorkflow()._activity(
            "delivery_checks", request, hours=request.get("hours", 2),
        )


@pytest.fixture
def owned_checks(tmp_path, monkeypatch):
    active = 0
    maximum = 0
    lock = threading.Lock()
    brokers = {}
    hearts = []
    in_activity = activities.activity.in_activity
    heartbeat = activities.activity.heartbeat
    config = SimpleNamespace(state_root=tmp_path, raw={"check_concurrency": 2})
    database = tmp_path / "fixture.sqlite3"
    with sqlite3.connect(database) as db:
        db.execute("CREATE TABLE delivery_runs(run_id TEXT, phase TEXT, recovery_json TEXT)")

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

    def context(value, **_options):
        broker = brokers[value["run_id"]]
        return SimpleNamespace(config=config, _connect=broker.connect), broker

    def request(name, seconds=0.6, timeout=5):
        value = spec(tmp_path, name)
        value.update(provider="codex", sleep=seconds, timeout=timeout)
        brokers[name] = Broker(value)
        return {"spec": value, "iteration": 0, "candidate": {"id": name}}

    monkeypatch.setattr(activities, "_context", context)
    monkeypatch.setattr(activities.activity, "in_activity", lambda: True)
    monkeypatch.setattr(activities.activity, "heartbeat", hearts.append)
    monkeypatch.setattr(activities, "_CHECK_HEARTBEAT_INTERVAL", 0.05, raising=False)
    retained = len(activities._UNCLEAN_CHECK_SLOTS)
    yield SimpleNamespace(
        request=request, brokers=brokers, hearts=hearts, config=config,
        maximum=lambda: maximum,
        in_activity=in_activity, heartbeat=heartbeat,
        retained_slots=retained,
    )
    for descriptor in activities._UNCLEAN_CHECK_SLOTS[retained:]:
        os.close(descriptor)
    del activities._UNCLEAN_CHECK_SLOTS[retained:]


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


def project_cancel(broker):
    db = broker.connect()
    with db:
        # The API writes this phase after the workflow accepts cancellation.
        db.execute("INSERT INTO delivery_runs(run_id, phase) VALUES (?, 'cancelling')",
                   (broker.spec["run_id"],))
    db.close()


@pytest.mark.parametrize("activity_name, stage", [
    ("delivery_checks", "waiting-check-slot"),
    ("delivery_precheck", "waiting-check-slot"),
    ("delivery_browser_qa", "waiting-check-slot"),
    ("delivery_baseline_checks", "waiting-check-slot"),
    ("delivery_browser_qa", "waiting-browser-ports"),
])
async def test_run_cancel_while_queued_confirms_cleanup_without_launch(
    owned_checks, activity_name, stage,
):
    owned_checks.config.raw["check_concurrency"] = 1
    first_request = owned_checks.request("first", 1.0)
    request = owned_checks.request("queued")
    first_activity = activities.delivery_checks
    if stage == "waiting-browser-ports":
        for value in (first_request, request):
            value["spec"]["policy"]["browser_qa"] = {"ports": {"web": 14001, "api": 14002}}
        first_activity = activities.delivery_browser_qa
    if activity_name == "delivery_baseline_checks":
        request.pop("candidate")  # Baseline's production request has no candidate.
    first = asyncio.create_task(first_activity(first_request))
    await wait_until(owned_checks.brokers["first"].started.is_set)
    queued = asyncio.create_task(getattr(activities, activity_name)(request))
    try:
        await wait_until(lambda: any(
            heart["run_id"] == "queued" and heart["stage"] == stage
            for heart in owned_checks.hearts
        ))
        project_cancel(owned_checks.brokers["queued"])
        outcome = (await asyncio.gather(queued, return_exceptions=True))[0]
    finally:
        await first
    print("queued run cancellation outcome:", repr(outcome))
    assert not owned_checks.brokers["queued"].started.is_set()
    assert isinstance(outcome, dict), f"queued activity raised {outcome!r}"
    assert outcome["state"] == "failed"
    assert outcome["cleanup"] == "confirmed"
    assert outcome["cancelled"] is True
    assert not owned_checks.brokers["queued"].result


async def test_run_cancel_after_admission_does_not_launch_and_releases_slot(
    owned_checks, monkeypatch, tmp_path,
):
    request = owned_checks.request("admitted")
    broker = owned_checks.brokers["admitted"]
    original = activities._try_check_lock

    def cancel_after_lock(path):
        descriptor = original(path)
        if descriptor is not None:
            project_cancel(broker)
        return descriptor

    monkeypatch.setattr(activities, "_try_check_lock", cancel_after_lock)
    result = await activities.delivery_precheck(request)
    assert result["cleanup"] == "confirmed"
    assert result["cancelled"] is True
    assert not broker.started.is_set()
    descriptor = original(tmp_path / "check-execution/slot-0.lock")
    assert descriptor is not None
    os.close(descriptor)


@pytest.mark.parametrize("activity_name", [
    "delivery_checks", "delivery_precheck", "delivery_browser_qa", "delivery_baseline_checks",
])
@pytest.mark.parametrize("cleanup_confirmed", [True, False])
async def test_run_cancel_at_native_launch_guard_returns_confirmed_outcome(
    owned_checks, monkeypatch, tmp_path, activity_name, cleanup_confirmed,
):
    request = owned_checks.request("before-launch")
    broker = owned_checks.brokers["before-launch"]
    broker.native_cleanup_confirmed = cleanup_confirmed
    process = NativeProcess(
        request["spec"], Path(broker.spec["state_dir"]) / "native",
        argv=[sys.executable, "-c", "raise AssertionError('must not launch')"],
        cwd=tmp_path, environment={"PATH": os.environ["PATH"]}, timeout=5,
    )

    def cancel_before_launch(*_args):
        project_cancel(broker)
        return broker._run_native_check(process)

    for name in ("run_checks", "run_prechecks", "run_browser_qa"):
        monkeypatch.setattr(broker, name, cancel_before_launch)
    monkeypatch.setattr("devflow_temporal.delivery_baseline.run_baseline_checks",
                        cancel_before_launch)
    if activity_name == "delivery_baseline_checks":
        request.pop("candidate")
    result = await getattr(activities, activity_name)(request)
    assert result["cleanup"] == ("confirmed" if cleanup_confirmed else "unknown")
    assert result["state"] == ("failed" if cleanup_confirmed else "unknown")
    assert result["cancelled"] is True
    assert broker.native_cleanup_confirmed is cleanup_confirmed
    assert len(activities._UNCLEAN_CHECK_SLOTS) == (
        owned_checks.retained_slots + (not cleanup_confirmed)
    )
    assert not process.journal.exists()


@pytest.mark.parametrize("cleanup_confirmed", [True, False])
async def test_run_cancel_before_implementation_preparation_retains_cleanup_truth(
    owned_checks, monkeypatch, tmp_path, cleanup_confirmed,
):
    request = owned_checks.request("prepare-cancel")
    request["role"] = "implement"
    request["spec"]["policy"]["host_sandbox"] = "trusted-local"
    broker = owned_checks.brokers["prepare-cancel"]
    broker.checkout = tmp_path
    broker.native_cleanup_confirmed = cleanup_confirmed
    monkeypatch.setattr(broker, "candidate", lambda: request["candidate"])

    class UnlaunchedRole:
        def retained_request(self, _request):
            return None

        async def run(self, _request):
            pytest.fail("cancelled preparation must not launch a role")

    monkeypatch.setattr(activities, "get_supervisor", lambda _store: UnlaunchedRole())
    process = NativeProcess(
        request["spec"], Path(broker.spec["state_dir"]) / "native",
        argv=[sys.executable, "-c", "raise AssertionError('must not launch')"],
        cwd=tmp_path, environment={"PATH": os.environ["PATH"]}, timeout=5,
    )
    db = broker.connect()
    with db:
        db.execute("INSERT INTO delivery_runs(run_id, phase) VALUES (?, 'running')",
                   (broker.spec["run_id"],))
    db.close()

    def cancel_preparation(*_args):
        db = broker.connect()
        with db:
            db.execute("UPDATE delivery_runs SET phase='cancelling' WHERE run_id=?",
                       (broker.spec["run_id"],))
        db.close()
        return broker._run_native_check(process)

    monkeypatch.setattr(broker, "run_implementation_preparation", cancel_preparation)
    result = await activities.delivery_role(request)
    assert result["status"] == "blocked"
    assert result["cleanup"] == ("confirmed" if cleanup_confirmed else "unknown")
    assert not process.journal.exists()


async def test_run_cancel_while_queued_reaches_terminal_resource_cleanup(
    owned_checks, monkeypatch,
):
    owned_checks.config.raw["check_concurrency"] = 1
    request = owned_checks.request("workflow-queued")
    request["spec"]["resource_cleanup_version"] = 1
    request["spec"]["policy"].update(max_repairs=0, prepublish_checks=[{"id": "fixture"}])
    scratch = RunResources(request["spec"]).scratch("check", "queued")
    (scratch / "owned.txt").write_text("temporary fixture")
    execution = DeliveryWorkflow()
    monkeypatch.setattr(workflow, "patched", lambda _name: True)

    async def ready_condition(predicate, **_options):
        assert predicate()

    monkeypatch.setattr(workflow, "wait_condition", ready_condition)

    async def dispatch(name, payload, **_options):
        if name == "delivery_prepare":
            return {"candidate": request["candidate"]}
        if name == "delivery_tracker_start":
            return {"state": "consistent"}
        if name == "delivery_role":
            return {"status": "pass", "candidate": payload["candidate"],
                    "cleanup": "confirmed", "session_id": "fixture:implement"}
        if name == "delivery_precheck":
            try:
                return await activities.delivery_precheck(payload)
            except asyncio.CancelledError as exc:
                # An activity exception without a server cancel is an activity failure.
                raise RuntimeError("precheck activity failed") from exc
        if name == "delivery_finalize_resources":
            return await activities.delivery_finalize_resources(payload)
        assert name == "delivery_project"
        return {}

    monkeypatch.setattr(execution, "_activity", dispatch)
    first = asyncio.create_task(activities.delivery_checks(owned_checks.request("first", 1.0)))
    await wait_until(owned_checks.brokers["first"].started.is_set)
    pending = asyncio.create_task(execution.run(request["spec"]))
    try:
        await wait_until(lambda: any(
            heart["run_id"] == "workflow-queued" and heart["stage"] == "waiting-check-slot"
            for heart in owned_checks.hearts
        ))
        accepted = await execution.cancel(
            {"expected_revision": execution.state["revision"], "reason": "fixture stop"},
        )
        assert accepted["phase"] == "cancelling"
        project_cancel(owned_checks.brokers["workflow-queued"])
        result = await pending
    finally:
        await first
    assert result["outcome"] == "cancelled"
    assert result["cleanup"] == "confirmed"
    assert result["checks"]["resource_cleanup"]["resource_cleanup"] == "confirmed"
    assert not scratch.exists()
    assert not owned_checks.brokers["workflow-queued"].started.is_set()


@pytest.mark.parametrize("slots", [0, -1, 33, True, 1.5, "2", None])
def test_configuration_rejects_nonfinite_or_invalid_slots(service, slots):
    store, _ = service
    raw = {**store.config.raw, "check_concurrency": slots}
    store.config.path.write_text(json.dumps(raw))
    with pytest.raises(ValueError, match="check_concurrency"):
        DeliveryConfig.load(store.config.path)


async def test_slot_lock_is_shared_with_another_worker_process(owned_checks, tmp_path):
    root = tmp_path / "check-execution"
    root.mkdir()
    ready = tmp_path / "worker-ready"
    code = (
        "import os,fcntl,pathlib,sys; "
        "fd=os.open(sys.argv[1],os.O_CREAT|os.O_RDWR,0o600); "
        "fcntl.flock(fd,fcntl.LOCK_EX); pathlib.Path(sys.argv[2]).touch(); sys.stdin.read()"
    )
    worker = subprocess.Popen(
        [sys.executable, "-c", code, str(root / "slot-0.lock"), str(ready)],
        stdin=subprocess.PIPE,
    )
    try:
        await wait_until(ready.exists)
        await asyncio.gather(
            activities.delivery_checks(owned_checks.request("one")),
            activities.delivery_precheck(owned_checks.request("two")),
        )
        assert owned_checks.maximum() == 1
    finally:
        worker.communicate(timeout=5)


async def test_slot_is_held_until_real_monitor_confirms_cleanup(owned_checks, monkeypatch):
    import devflow_temporal.delivery_native_process as native

    owned_checks.config.raw["check_concurrency"] = 1
    stopped = threading.Event()
    release = threading.Event()
    original = native.stop_observed

    def pause_after_stop(owned):
        result = original(owned)
        if not stopped.is_set():
            stopped.set()
            assert release.wait(5)
        return result

    monkeypatch.setattr(native, "stop_observed", pause_after_stop)
    first = asyncio.create_task(activities.delivery_checks(owned_checks.request("first", 0.3)))
    await wait_until(stopped.is_set)
    following = asyncio.create_task(activities.delivery_checks(owned_checks.request("following")))
    first.cancel()
    try:
        await asyncio.sleep(0.15)
        assert not first.done()
        assert not owned_checks.brokers["following"].started.is_set()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await first
        await following
    assert_stopped(owned_checks.brokers["first"])


async def test_unknown_cleanup_does_not_release_occupied_slot(owned_checks, monkeypatch):
    owned_checks.config.raw["check_concurrency"] = 1
    original = NativeProcess.run
    retained = len(activities._UNCLEAN_CHECK_SLOTS)

    def unknown(process):
        result = original(process)
        result["cleanup"] = "unknown"  # Simulate uncertain evidence after real teardown.
        return result

    monkeypatch.setattr(NativeProcess, "run", unknown)
    try:
        result = await activities.delivery_checks(owned_checks.request("uncertain"))
        assert result == {"state": "unknown", "cleanup": "unknown"}
        assert len(activities._UNCLEAN_CHECK_SLOTS) == retained + 1
        task = asyncio.create_task(activities.delivery_checks(owned_checks.request("queued")))
        await asyncio.sleep(0.15)
        assert not owned_checks.brokers["queued"].started.is_set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        for descriptor in activities._UNCLEAN_CHECK_SLOTS[retained:]:
            os.close(descriptor)
        del activities._UNCLEAN_CHECK_SLOTS[retained:]


async def test_native_timeout_confirms_tree_cleanup_and_releases_slot(owned_checks):
    owned_checks.config.raw["check_concurrency"] = 1
    first = asyncio.create_task(
        activities.delivery_checks(owned_checks.request("timeout", seconds=3, timeout=1))
    )
    await wait_until(owned_checks.brokers["timeout"].started.is_set)
    second = asyncio.create_task(activities.delivery_checks(owned_checks.request("following")))
    await asyncio.gather(first, second)
    broker = owned_checks.brokers["timeout"]
    assert broker.result["timed_out"] is True
    assert_stopped(broker)
    assert owned_checks.maximum() == 1


async def test_broker_native_runner_tracks_cleanup_and_owned_cancellation(owned_checks, tmp_path):
    request = owned_checks.request("broker-callback")
    broker = owned_checks.brokers["broker-callback"]
    cancelled = threading.Event()
    broker.check_cancelled = cancelled.is_set
    process = NativeProcess(
        request["spec"], Path(request["spec"]["state_dir"]) / "native",
        argv=[sys.executable, "-c", "import subprocess,sys,time; "
              "subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']);"
              "time.sleep(30)"],
        cwd=tmp_path, environment={"PATH": os.environ["PATH"]}, timeout=60,
        cancelled=broker._native_cancelled,
    )
    pending = asyncio.create_task(asyncio.to_thread(broker._run_native_check, process))
    await wait_until(process.journal.exists)
    await asyncio.sleep(0.2)
    assert broker.native_cleanup_confirmed is False
    cancelled.set()
    broker.result = await pending
    assert broker.native_cleanup_confirmed is True
    assert broker.result["cancelled"] is True
    assert_stopped(broker)


async def test_physical_worker_death_releases_os_slot_before_orphan_cleanup(tmp_path):
    """Document B4's boundary: OS locks cannot establish orphan reconciliation."""
    owned = spec(tmp_path, "dead-worker")
    input_path = tmp_path / "owned-spec.json"
    input_path.write_text(json.dumps(owned))
    slot = tmp_path / "slot.lock"
    folder = Path(owned["state_dir"]) / "native"
    code = (
        "import fcntl,json,os,pathlib,sys; "
        "from devflow_temporal.delivery_native_process import NativeProcess; "
        "spec=json.loads(pathlib.Path(sys.argv[1]).read_text()); "
        "fd=os.open(sys.argv[2],os.O_CREAT|os.O_RDWR,0o600); fcntl.flock(fd,fcntl.LOCK_EX); "
        "NativeProcess(spec,pathlib.Path(sys.argv[3]),argv=[sys.executable,'-c',"
        "\"import subprocess,sys,time;subprocess.Popen([sys.executable,'-c',"
        "'import time;time.sleep(30)']);time.sleep(30)\"], "
        "cwd=pathlib.Path(sys.argv[3]).parent,environment={'PATH':os.environ['PATH']},"
        "timeout=60).run()"
    )
    worker = subprocess.Popen([sys.executable, "-c", code, str(input_path), str(slot), str(folder)])
    journal = folder / "native-process.json"

    def observed_tree():
        return journal.exists() and len(json.loads(journal.read_text())["owned"]) >= 2

    descriptor = None
    try:
        await wait_until(observed_tree)
        worker.kill()
        worker.wait(timeout=5)
        import fcntl

        descriptor = os.open(slot, os.O_RDWR)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        owned_pids = json.loads(journal.read_text())["owned"]
        table = process_table()
        assert any(table.get(int(pid), {}).get("identity") == entry["identity"]
                   and not table[int(pid)]["stat"].startswith("Z")
                   for pid, entry in owned_pids.items())
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.wait(timeout=5)
        if descriptor is not None:
            os.close(descriptor)
        if journal.exists():
            cleanup = reconcile_process(journal)
            assert cleanup["observed_owned_stopped"]
            assert cleanup["cleanup"] == "unknown"  # Interrupted monitor remains honest.


@pytest.mark.parametrize("stop", ["cancel", "timeout"])
async def test_temporal_stop_reaches_native_monitor_and_frees_slot(
    owned_checks, monkeypatch, tmp_path, stop,
):
    monkeypatch.setattr(activities.activity, "in_activity", owned_checks.in_activity)
    monkeypatch.setattr(activities.activity, "heartbeat", owned_checks.heartbeat)
    owned_checks.config.raw["check_concurrency"] = 1
    request = owned_checks.request("temporal-stopped", seconds=5, timeout=10)
    if stop == "timeout":
        request["hours"] = 0.5 / 3600
    broker = owned_checks.brokers["temporal-stopped"]
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "isolated-temporal.sqlite3"),
    ) as environment:
        async with Worker(
            environment.client, task_queue="check-slots-cancel",
            workflows=[CheckSlotCancellationFixture],
            workflow_runner=UnsandboxedWorkflowRunner(), activities=[activities.delivery_checks],
            max_heartbeat_throttle_interval=timedelta(seconds=0.1),
            default_heartbeat_throttle_interval=timedelta(seconds=0.1),
        ):
            handle = await environment.client.start_workflow(
                CheckSlotCancellationFixture.run, request, id=f"check-slots-{stop}",
                task_queue="check-slots-cancel",
            )
            await wait_until(broker.started.is_set)
            await asyncio.sleep(0.2)
            following = asyncio.create_task(
                activities.delivery_precheck(owned_checks.request("following"))
            )
            if stop == "cancel":
                await handle.cancel()
            with pytest.raises(WorkflowFailureError):
                await asyncio.wait_for(handle.result(), 10)
            await wait_until(broker.finished.is_set)
            await following
    assert broker.result["cancelled"] is True
    assert broker.result["timed_out"] is False
    assert_stopped(broker)
    assert owned_checks.maximum() == 1
