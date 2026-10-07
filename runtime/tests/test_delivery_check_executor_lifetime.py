"""Actual native check/executor lifetime regressions on disposable macOS resources."""

from __future__ import annotations

import asyncio
import contextvars
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from importlib.metadata import distribution
from pathlib import Path
from types import SimpleNamespace

import pytest
import pytest_asyncio
from temporalio import activity
from test_delivery_api import api_fixture as api_fixture

from devflow_temporal import delivery_activities, delivery_native_process
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_preparation import prepare_authority, verify_prepared_spec
from devflow_temporal.delivery_resources import RunResources, read_private
from devflow_temporal.delivery_store import DeliveryStore

pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="native macOS execution authority")
EXECUTION_CONTEXT = contextvars.ContextVar("owned_check_context", default="missing")

COMMAND = r'''
import json,os,signal,subprocess,sys,time
from pathlib import Path
journal=Path(os.environ['DEVFLOW_NATIVE_JOURNAL'])
state=next(p for p in journal.parents if p.parent.name=='runs')
controls=json.loads((state/'probe-controls.json').read_text())
if controls.get('ignore_term'):
    signal.signal(signal.SIGTERM,signal.SIG_IGN)
assert os.environ['DEVFLOW_MANAGED_DEPTH']=='1'
assert Path('README.md').read_text()=='Fixture\n'
child=subprocess.Popen([sys.executable,'-c',
    'import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(30)'])
observed_deadline=time.monotonic()+5
while str(child.pid) not in json.loads(journal.read_text()).get('owned',{}):
    assert time.monotonic()<observed_deadline,'owned child was not observed'
    time.sleep(.02)
with (state/'probe-trace.jsonl').open('a') as stream:
    stream.write(json.dumps({'event':'started','mode':'check','pid':os.getpid(),
        'child':child.pid,'time_ns':time.monotonic_ns(),'journal':str(journal)})+'\n')
(state/'check-started').touch()
deadline=time.monotonic()+20
while controls.get('hold_check') and not (state/'release-check').exists():
    assert time.monotonic()<deadline,'owned fixture release deadline'
    time.sleep(.02)
print('2 passed')
'''


async def wait_until(predicate):
    async with asyncio.timeout(15):
        while not predicate():
            await asyncio.sleep(0.02)


@dataclass
class OwnedRun:
    spec: dict
    broker: DeliveryBroker
    candidate: dict
    state: Path

    def request(self):
        return {"spec": self.spec, "iteration": 0, "candidate": self.candidate}

    def release(self):
        (self.state / "release-check").touch()

    def started(self):
        return (self.state / "check-started").exists()

    def trace(self):
        path = self.state / "probe-trace.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def journal(self):
        return Path(next(event["journal"] for event in self.trace() if event["mode"] == "check"))


def live_owned(payload):
    table = delivery_native_process.process_table()
    return {
        pid: entry
        for pid, entry in payload.get("owned", {}).items()
        if table.get(int(pid), {}).get("identity") == entry["identity"]
        and not table[int(pid)]["stat"].startswith("Z")
    }


@pytest_asyncio.fixture
async def native_case(api_fixture, tmp_path, monkeypatch):
    config_path, submission = api_fixture
    command = tmp_path / "owned-check.py"
    command.write_text(COMMAND)
    auth = tmp_path / "owned-auth.json"
    auth.write_text('{"OPENAI_API_KEY":"unusable-owned-fixture"}')
    auth.chmod(0o600)
    raw = json.loads(config_path.read_text())
    raw.update(
        provider="codex",
        execution_mode="trusted-local",
        capacity=1,
        check_concurrency=1,
        codex_auth_path=str(auth),
        codex_bin=str(distribution("openai-codex-cli-bin").locate_file("codex_cli_bin/bin/codex")),
    )
    raw["roles"] = {role: {"model": "gpt-6.1-sol", "effort": "high"} for role in raw["roles"]}
    check = {
        "id": "owned-executor-check",
        "argv": [sys.executable, "-I", str(command), "check"],
        "timeout_seconds": 20,
        "test_count_regex": r"(\d+) passed",
        "min_tests": 2,
    }
    raw["repositories"]["fixture"].update(
        prepublish_checks=[check],
        checks=[check],
        required_ci=["test"],
        project_url="https://github.com/users/example/projects/1",
        assignee="example",
    )
    config_path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(config_path))
    runs, tasks, hearts = [], [], []
    # Only opaque Temporal activity context is substituted. Configuration,
    # authority/profile validation, broker, process tree and resources are real.
    monkeypatch.setattr(activity, "in_activity", lambda: True)
    monkeypatch.setattr(
        activity,
        "heartbeat",
        lambda value: hearts.append({**value, "observed_ns": time.monotonic_ns()}),
    )
    monkeypatch.setattr(
        activity,
        "info",
        lambda: SimpleNamespace(
            attempt=1,
            workflow_id="owned-check",
            activity_id="owned-check",
            workflow_run_id="owned-check",
            heartbeat_details=(),
        ),
    )

    def new(name, **controls):
        store.submit(
            {
                **submission,
                "command_id": "submit-" + name,
                "run_id": name,
                "work_id": "work-" + name,
                "branch": "feat/" + name,
                "issue_url": f"https://github.com/example/fixture/issues/{len(runs) + 3}",
            }
        )
        spec = prepare_authority(store, store.submitted_spec(name))
        verify_prepared_spec(spec)
        broker = DeliveryBroker(store, spec)
        candidate = broker.prepare()["candidate"]
        state = Path(spec["state_dir"])
        (state / "probe-controls.json").write_text(json.dumps(controls))
        run = OwnedRun(spec, broker, candidate, state)
        runs.append(run)
        return run

    def start(run):
        task = asyncio.create_task(delivery_activities.delivery_precheck(run.request()))
        tasks.append(task)
        return task

    try:
        yield SimpleNamespace(
            new=new, start=start, runs=runs, tasks=tasks, hearts=hearts, store=store, tmp=tmp_path
        )
    finally:
        for run in runs:
            run.release()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 20)
        records = []
        for path in tmp_path.rglob("native-process.json"):
            payload = read_private(path)
            owned = {int(pid): entry for pid, entry in payload.get("owned", {}).items()}
            if payload.get("monitor"):
                owned[payload["monitor"]["pid"]] = payload["monitor"]
            stopped = await asyncio.to_thread(delivery_native_process.stop_observed, owned)
            records.append({"journal": str(path), "owned": owned, "observed_stopped": stopped})
            assert stopped
        for run in runs:
            receipt = await asyncio.to_thread(RunResources(run.spec).finalize, "blocked")
            (run.state / "probe-finalization.json").write_text(json.dumps(receipt, indent=2))
        (tmp_path / "probe-owned-processes.json").write_text(json.dumps(records, indent=2))
        (tmp_path / "probe-heartbeats.json").write_text(json.dumps(hearts, indent=2))


def owned_executor_loop(operation):
    loop = asyncio.new_event_loop()
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="owned-check-lifetime")
    loop.set_default_executor(executor)
    try:
        return loop.run_until_complete(operation(loop))
    finally:
        loop.run_until_complete(loop.shutdown_default_executor())
        loop.close()
        executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_cancelled_executor_waiter_joins_actual_native_tree(native_case):
    case = native_case
    run = case.new("executor-waiter", hold_check=True, ignore_term=True)

    async def probe(loop):
        active = asyncio.create_task(delivery_activities.delivery_precheck(run.request()))
        try:
            await wait_until(run.started)
            payload = read_private(run.journal())
            assert len(payload["owned"]) >= 2 and live_owned(payload)
            # Loop shutdown can cancel each nested task independently. Locate
            # this activity's executor waiter without depending on its helper name.
            checks = []
            for task in asyncio.all_tasks(loop):
                coroutine = task.get_coro()
                while coroutine is not None:
                    if getattr(coroutine, "__name__", "") == "_execute_check":
                        checks.append(coroutine)
                    coroutine = getattr(coroutine, "cr_await", None)
            assert len(checks) == 1
            waiter = checks[0].cr_frame.f_locals["pending"]
            assert isinstance(waiter, asyncio.Task) and waiter is not active
            started = time.monotonic()
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await active
            remaining = live_owned(payload)
            (case.tmp / "executor-waiter-observation.json").write_text(
                json.dumps(
                    {
                        "elapsed_until_activity_return": time.monotonic() - started,
                        "original": payload,
                        "owned_live_after_return": remaining,
                        "fault": "cancel actual nested executor asyncio waiter",
                        "limit": "Opaque Temporal context only; no live SDK cancellation delivery",
                    },
                    indent=2,
                )
            )
            assert not remaining, "activity returned before the real executor tree stopped"
        finally:
            run.release()
            if not active.done():
                active.cancel()
            await asyncio.gather(active, return_exceptions=True)

    await asyncio.to_thread(owned_executor_loop, probe)


@pytest.mark.asyncio
async def test_queued_executor_cancellation_joins_before_possible_late_start(native_case):
    case = native_case
    run = case.new("queued-executor")

    async def probe(loop):
        entered, release = threading.Event(), threading.Event()

        def hold_executor():
            entered.set()
            assert release.wait(10), "owned executor release deadline"

        blocker = loop.run_in_executor(None, hold_executor)
        active = asyncio.create_task(delivery_activities.delivery_precheck(run.request()))
        try:
            await wait_until(
                lambda: (
                    entered.is_set()
                    and any(
                        h["run_id"] == run.spec["run_id"] and h["stage"] == "waiting-check-slot"
                        for h in case.hearts
                    )
                )
            )
            active.cancel()
            await asyncio.sleep(0.15)
            assert not active.done(), "cancelled activity abandoned a possible later executor"
            assert not run.started()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await active
            await blocker
            assert not run.started()
        finally:
            release.set()
            if not active.done():
                active.cancel()
            await asyncio.gather(active, blocker, return_exceptions=True)

    await asyncio.to_thread(owned_executor_loop, probe)


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["cancel", "timeout"])
async def test_outer_stop_joins_native_tree_before_slot_reuse(native_case, reason):
    case = native_case
    first = case.new("outer-stop", hold_check=True, ignore_term=True)
    second = case.new("slot-follower")
    active = case.start(first)
    await wait_until(first.started)
    payload = read_private(first.journal())
    queued = case.start(second)
    await wait_until(
        lambda: any(
            h["run_id"] == second.spec["run_id"] and h["stage"] == "waiting-check-slot"
            for h in case.hearts
        )
    )
    if reason == "cancel":
        active.cancel()
        with pytest.raises(asyncio.CancelledError):
            await active
    else:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(active, timeout=0.05)
    assert not live_owned(payload), "cancel/timeout returned before its owned tree stopped"
    assert (await queued)["state"] == "passed"
    assert len([event for event in second.trace() if event["mode"] == "check"]) == 1


@pytest.mark.asyncio
async def test_queued_run_cancellation_preserves_known_prelaunch_cleanup(native_case):
    case = native_case
    first = case.new("phase-holder", hold_check=True)
    second = case.new("phase-queued")
    active = case.start(first)
    await wait_until(first.started)
    queued = case.start(second)
    await wait_until(
        lambda: any(
            h["run_id"] == second.spec["run_id"] and h["stage"] == "waiting-check-slot"
            for h in case.hearts
        )
    )
    with case.store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute(
            "UPDATE delivery_runs SET phase='cancelling' WHERE run_id=?", (second.spec["run_id"],)
        )
    result = await queued
    assert result["cleanup"] == "confirmed" and result["cancelled"] is True
    assert not second.started()
    assert not RunResources(second.spec).manifest.exists() or all(
        "prechecks" not in journal
        for journal in read_private(RunResources(second.spec).manifest)["processes"]
    )
    first.release()
    assert (await active)["state"] == "passed"


@pytest.mark.asyncio
async def test_original_context_reaches_real_executor_operation(native_case):
    case = native_case
    run = case.new("context-check")
    observed = []

    def execute(broker):
        observed.append(EXECUTION_CONTEXT.get())
        return broker.run_prechecks(0, run.candidate)

    token = EXECUTION_CONTEXT.set("owned-original-context")
    try:
        result = await delivery_activities._execute_check(run.request(), execute)
    finally:
        EXECUTION_CONTEXT.reset(token)
    assert observed == ["owned-original-context"]
    assert result["state"] == "passed"
