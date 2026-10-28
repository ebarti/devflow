from __future__ import annotations

import asyncio
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from temporal_test_server import local_temporal
from temporalio import workflow
from temporalio.client import WorkflowFailureError
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_resources import spec
from test_delivery_store import service as service

from devflow_temporal import delivery_activities as activities
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_native_process import NativeProcess, process_table
from devflow_temporal.delivery_resources import read_private, write_private
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
                    "import subprocess,sys,time\n"
                    "subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'])\n"
                )
                if self.spec.get("completion_barrier"):
                    # Cancellation fixtures must remain running until the test
                    # releases them or the real native monitor stops the tree.
                    code += (
                        "from pathlib import Path\n"
                        f"release = Path({self.spec['completion_barrier']!r})\n"
                        "while not release.exists():\n"
                        "    time.sleep(0.02)\n"
                    )
                else:
                    code += f"time.sleep({self.spec['sleep']})"
                self.native_cleanup_confirmed = False
                process = NativeProcess(
                    self.spec, Path(self.spec["state_dir"]) / "native",
                    argv=[sys.executable, "-c", code], cwd=tmp_path,
                    environment={"PATH": os.environ["PATH"]},
                    timeout=self.spec["timeout"], cancelled=self._native_cancelled,
                )
                self.native_journals += (process.journal,)
                self.result = process.run()
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
    for entry in activities._UNCLEAN_CHECK_SLOTS[retained:]:
        for descriptor in entry["descriptors"]:
            os.close(descriptor)
    del activities._UNCLEAN_CHECK_SLOTS[retained:]


async def wait_until(predicate):
    async with asyncio.timeout(10):
        while not predicate():
            await asyncio.sleep(0.02)


def observed_native_tree(journal: Path) -> bool:
    if not journal.exists():
        return False
    value = read_private(journal)
    owned = value.get("owned", {})
    return (
        value.get("phase") == "authorized"
        and isinstance(owned, dict)
        and len(owned) >= 2
        and all(isinstance(entry, dict) and isinstance(entry.get("identity"), str)
                and entry["identity"] for entry in owned.values())
    )


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
async def test_activity_stop_joins_real_native_tree_before_slot_reuse(owned_checks, stop, tmp_path):
    owned_checks.config.raw["check_concurrency"] = 1
    request = owned_checks.request("stopped", seconds=2)
    completion_barrier = tmp_path / "release-activity-stop-fixture"
    request["spec"]["completion_barrier"] = str(completion_barrier)
    broker = owned_checks.brokers["stopped"]
    task = asyncio.create_task(activities.delivery_checks(request))
    journal = Path(broker.spec["state_dir"]) / "native/native-process.json"
    await wait_until(lambda: observed_native_tree(journal))
    following = asyncio.create_task(activities.delivery_precheck(owned_checks.request("following")))
    try:
        assert not broker.finished.is_set()
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
        completion_barrier.touch()
        await wait_until(broker.finished.is_set)
        await following
    assert owned_checks.maximum() == 1


def project_cancel(broker):
    db = broker.connect()
    with db:
        # The API writes this phase after the workflow accepts cancellation.
        db.execute("INSERT INTO delivery_runs(run_id, phase) VALUES (?, 'cancelling')",
                   (broker.spec["run_id"],))
    db.close()


@pytest.mark.parametrize(
    "activity_name, stage",
    [("delivery_checks", "waiting-check-slot"), ("delivery_browser_qa", "waiting-browser-ports")],
)
async def test_run_cancel_while_queued_confirms_cleanup_without_launch(
    owned_checks,
    activity_name,
    stage,
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


async def test_unknown_cleanup_retains_slot_until_recorded_processes_are_gone(
    owned_checks, monkeypatch,
):
    owned_checks.config.raw["check_concurrency"] = 1
    monkeypatch.setattr(activities, "_UNCLEAN_CHECK_RECHECK_INTERVAL", 0.05, raising=False)
    original = NativeProcess.run
    retained = len(activities._UNCLEAN_CHECK_SLOTS)
    stray = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])

    def unknown(process):
        result = original(process)
        result["cleanup"] = "unknown"  # Uncertain evidence after a real teardown.
        return result

    monkeypatch.setattr(NativeProcess, "run", unknown)
    try:
        result = await activities.delivery_checks(owned_checks.request("uncertain"))
        assert result == {"state": "unknown", "cleanup": "unknown"}
        assert len(activities._UNCLEAN_CHECK_SLOTS) == retained + 1
        entry = activities._UNCLEAN_CHECK_SLOTS[-1]
        monkeypatch.setattr(NativeProcess, "run", original)
        # Only after the retention snapshot does the journal record a monitor
        # whose exit nobody observed: every proof re-reads the journal.
        journal = Path(owned_checks.brokers["uncertain"].result["journal"])
        value = read_private(journal)
        value["monitor"] = {"pid": stray.pid, **process_table()[stray.pid]}
        write_private(journal, value)
        task = asyncio.create_task(activities.delivery_checks(owned_checks.request("queued")))
        await asyncio.sleep(0.3)
        # A live recorded process keeps the slot retained across admissions.
        assert not owned_checks.brokers["queued"].started.is_set()
        assert len(activities._UNCLEAN_CHECK_SLOTS) == retained + 1
        stray.kill()
        stray.wait()
        # Once every recorded process is proven gone, the queued gate is admitted.
        assert await asyncio.wait_for(task, 10) == {"state": "passed", "cleanup": "confirmed"}
        assert len(activities._UNCLEAN_CHECK_SLOTS) == retained
        assert_stopped(owned_checks.brokers["queued"])
        assert list(entry["journals"]) == [str(journal)] and entry["ports"] == []
        assert len(entry["journals"][str(journal)]) >= 3  # owned tree plus its monitor
    finally:
        stray.kill()
        stray.wait()
        for entry in activities._UNCLEAN_CHECK_SLOTS[retained:]:
            for descriptor in entry["descriptors"]:
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


async def check_slot_timeout_diagnostic(error, broker, journal, handle):
    """Retain finite original failure facts without changing its refusal."""
    from runtime_location_scenario import ci_failure_diagnostics

    if not ci_failure_diagnostics():
        return

    def number(value, limit=2**31 - 1):
        return value if type(value) is int and 0 <= value <= limit else None

    def result_facts(value):
        if not isinstance(value, dict):
            return {"state": "unavailable"}
        result = {}
        for key in ("timed_out", "cancelled", "monitoring_complete", "stdio_drained"):
            if type(value.get(key)) is bool:
                result[key] = value[key]
        for key, allowed in (
            ("state", {"finished", "unknown", "passed", "failed", "blocked"}),
            ("cleanup", {"observed-native-confirmed", "confirmed", "unknown"}),
        ):
            if isinstance(value.get(key), str) and value[key] in allowed:
                result[key] = value[key]
        exit_code = value.get("exit_code")
        if type(exit_code) is int and -(2**31) <= exit_code < 2**31:
            result["exit_code"] = exit_code
        return result

    try:
        if (handle.id != "check-slots-timeout" or not isinstance(handle.run_id, str)
                or not handle.run_id or handle.run_id != handle.result_run_id
                or broker.spec.get("run_id") != "temporal-stopped"
                or journal != Path(broker.spec["state_dir"]) / "native/native-process.json"):
            return
        facts = {"broker_started": broker.started.is_set(),
                 "broker_finished": broker.finished.is_set(),
                 "cleanup_confirmed": broker.native_cleanup_confirmed is True,
                 "broker_result": result_facts(broker.result)}
        try:
            descriptor = os.open(journal, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                info = os.fstat(descriptor)
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                        or info.st_mode & 0o777 != 0o600 or info.st_nlink not in (0, 1)
                        or info.st_size > 262144):
                    raise ValueError("fixture journal observation unavailable")
                raw = os.read(descriptor, 262145)
                if len(raw) > 262144:
                    raise ValueError("fixture journal observation exceeds bound")
                value = json.loads(raw)
            finally:
                os.close(descriptor)
            phase = value.get("phase")
            facts["journal_phase"] = phase if phase in {
                "allocated", "authorized", "finished", "unknown",
            } else "other"
            facts["journal_result"] = result_facts(value.get("result"))
            owned = value.get("owned")
            if isinstance(owned, dict):
                facts["owned_entries_capped"] = min(len(owned), 64)
        except BaseException:
            facts["journal_phase"] = "unavailable"
        events = []
        try:
            async with asyncio.timeout(2):
                description = await handle.describe(rpc_timeout=timedelta(seconds=2))
                facts["pending_activities"] = [{
                    "state": number(int(item.state), 100),
                    "attempt": number(item.attempt),
                    "maximum_attempts": number(item.maximum_attempts),
                    "current_retry_seconds": number(item.current_retry_interval.seconds, 86400),
                    "current_retry_nanos": number(item.current_retry_interval.nanos, 999999999),
                    "last_failure_timeout_type": number(int(
                        item.last_failure.timeout_failure_info.timeout_type), 100)
                    if item.last_failure.HasField("timeout_failure_info") else None,
                } for item in description.raw_description.pending_activities[:4]]
                origin = None
                async for event in handle.fetch_history_events(
                    page_size=32, wait_new_event=False, rpc_timeout=timedelta(seconds=2),
                ):
                    moment = event.event_time.seconds * 10**9 + event.event_time.nanos
                    if origin is None:
                        origin = moment
                    row = {"type": number(int(event.event_type), 1000),
                           "id": number(event.event_id),
                           "elapsed_milliseconds": number((moment - origin) // 10**6, 86400000)}
                    for kind in ("scheduled", "started", "completed", "timed_out", "failed"):
                        field = f"activity_task_{kind}_event_attributes"
                        if not event.HasField(field):
                            continue
                        attributes = getattr(event, field)
                        if kind == "started":
                            row["attempt"] = number(attributes.attempt)
                        if kind == "scheduled":
                            row.update(
                                start_to_close=number(
                                    attributes.start_to_close_timeout.seconds, 86400),
                                start_to_close_nanos=number(
                                    attributes.start_to_close_timeout.nanos, 999999999),
                                maximum_attempts=number(attributes.retry_policy.maximum_attempts),
                                retry_initial_seconds=number(
                                    attributes.retry_policy.initial_interval.seconds, 86400),
                                retry_maximum_seconds=number(
                                    attributes.retry_policy.maximum_interval.seconds, 86400),
                            )
                        if (kind in {"timed_out", "failed"}
                                and attributes.failure.HasField("timeout_failure_info")):
                            row["timeout_type"] = number(int(
                                attributes.failure.timeout_failure_info.timeout_type), 100)
                    events.append(row)
                    if len(events) == 64:
                        break
            facts["history_status"] = "observed"
        except BaseException:
            facts["history_status"] = "unavailable"
        facts["history"] = events
        text = json.dumps({"check_slot_timeout_observation": facts}, sort_keys=True)
        if len(text) <= 16384:
            error.add_note(text)
    except BaseException:
        pass


@pytest.mark.parametrize("stop", ["cancel", "timeout"])
async def test_temporal_cancel_or_monitor_timeout_preserves_native_slot_lifecycle(
    owned_checks, monkeypatch, tmp_path, stop,
):
    monkeypatch.setattr(activities.activity, "in_activity", owned_checks.in_activity)
    monkeypatch.setattr(activities.activity, "heartbeat", owned_checks.heartbeat)
    owned_checks.config.raw["check_concurrency"] = 1
    request = owned_checks.request("temporal-stopped", seconds=5, timeout=10)
    completion_barrier = tmp_path / "release-cancel-fixture"
    if stop == "cancel":
        request["spec"]["completion_barrier"] = str(completion_barrier)
    else:
        request["hours"] = 0.5 / 3600
    broker = owned_checks.brokers["temporal-stopped"]
    async with local_temporal(
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
            following = None
            try:
                handle = await environment.client.start_workflow(
                    CheckSlotCancellationFixture.run, request, id=f"check-slots-{stop}",
                    task_queue="check-slots-cancel",
                )
                journal = Path(broker.spec["state_dir"]) / "native/native-process.json"
                await wait_until(lambda: observed_native_tree(journal))
                following = asyncio.create_task(
                    activities.delivery_precheck(owned_checks.request("following"))
                )
                if stop == "cancel":
                    assert not broker.finished.is_set()
                    await handle.cancel()
                    with pytest.raises(WorkflowFailureError):
                        await asyncio.wait_for(handle.result(), 10)
                else:
                    # Losing a monitor attempt does not end the durable check or
                    # free its slot while it is still running. Its replacement
                    # reattaches and returns the original check's result.
                    try:
                        result = await asyncio.wait_for(handle.result(), 15)
                    except TimeoutError as error:
                        try:
                            if handle.result_run_id:
                                observation = environment.client.get_workflow_handle(
                                    handle.id, run_id=handle.result_run_id,
                                )
                                await check_slot_timeout_diagnostic(
                                    error, broker, journal, observation,
                                )
                        except BaseException:
                            pass
                        raise
                    assert result == {"state": "passed", "cleanup": "confirmed"}
                    history = await handle.fetch_history()
                    started = [event.activity_task_started_event_attributes
                               for event in history.events
                               if event.HasField("activity_task_started_event_attributes")]
                    assert started[-1].attempt > 1
            finally:
                if stop == "cancel":
                    completion_barrier.touch()
                if broker.started.is_set():
                    await wait_until(broker.finished.is_set)
                if following is not None:
                    await following
    assert broker.result["cancelled"] is (stop == "cancel")
    assert broker.result["timed_out"] is False
    assert_stopped(broker)
    assert owned_checks.maximum() == 1


def attempt_info(attempt=1, maximum_attempts=3, budget=timedelta(hours=2),
                 elapsed=timedelta(0), activity_id="gate", workflow_run_id="run"):
    """What activity.info() reports for a native gate under the production options."""
    return SimpleNamespace(
        attempt=attempt, retry_policy=SimpleNamespace(maximum_attempts=maximum_attempts),
        schedule_to_close_timeout=budget, scheduled_time=datetime.now(UTC) - elapsed,
        activity_id=activity_id, workflow_run_id=workflow_run_id,
    )


@pytest.mark.parametrize("replacement", ["timed_out", "worker_shutdown"])
async def test_replaced_check_attempt_keeps_native_gate_running_for_the_retry(
    owned_checks, monkeypatch, replacement,
):
    # What the SDK reports after a heartbeat timeout, a lost attempt or a
    # worker shutdown: the attempt is superseded, nobody requested a cancel,
    # and the activity still has attempts and budget left.
    monkeypatch.setattr(activities.activity, "cancellation_details", lambda: SimpleNamespace(
        cancel_requested=False, timed_out=False, not_found=False, worker_shutdown=False,
        **{replacement: True},
    ))
    monkeypatch.setattr(activities.activity, "info", lambda: attempt_info())
    request = owned_checks.request("one", seconds=2, timeout=60)
    broker = owned_checks.brokers["one"]
    folder = Path(request["spec"]["state_dir"]) / "native"
    journal = folder / "native-process.json"
    superseded = asyncio.create_task(activities.delivery_checks(request))
    await wait_until(lambda: observed_native_tree(journal))
    superseded.cancel()
    with pytest.raises(asyncio.CancelledError):
        await superseded
    assert not (folder / "cancel").exists()
    assert not broker.finished.is_set()
    # The Temporal retry joins the same durable invocation and scores its real result.
    assert await activities.delivery_checks(request) == {"state": "passed", "cleanup": "confirmed"}
    native = broker.result
    assert native["cancelled"] is False and native["exit_code"] == 0
    assert native["timed_out"] is False
    assert not (folder / "cancel").exists()
    assert read_private(journal)["phase"] == "finished"
    assert_stopped(broker)


@pytest.mark.parametrize('queued_finished', [True, False])
def test_cancelled_queued_retries_keep_the_live_native_predecessor(queued_finished):
    key = ('queued-native-custody', object())
    signals = [(threading.Event(), threading.Event()) for _ in range(4)]
    first, second, third, fourth = signals
    try:
        assert activities._claim_gate(key, 1, *first) is None
        assert activities._claim_gate(key, 2, *second) is first[1]
        activities._release_gate(key, second[1])  # Attempt 2 timed out while still queued.
        assert activities._claim_gate(key, 3, *third) is first[1]
        assert not first[1].is_set()
        if queued_finished:
            activities._release_gate(key, third[1])
            assert key in activities._ACTIVE_CHECKS
            activities._release_gate(key, first[1])
            assert key not in activities._ACTIVE_CHECKS
        else:
            activities._release_gate(key, first[1])
            assert activities._claim_gate(key, 4, *fourth) is third[1]
            activities._release_gate(key, fourth[1])
            assert key in activities._ACTIVE_CHECKS
            activities._release_gate(key, third[1])
            assert key not in activities._ACTIVE_CHECKS
    finally:
        for _, finished in signals:
            activities._release_gate(key, finished)
