"""Launch-window deaths release their capacity slot instead of leaking it (N-12)."""
from __future__ import annotations

import asyncio
import fcntl
import json
import os
import subprocess
import sys
import threading

import pytest
from test_dead_native_monitor import attempt, kill_owned_monitor, until
from test_delivery_api import api_fixture as api_fixture
from test_delivery_prelaunch_capacity import native_role as native_role
from test_delivery_store import _sealed_prelaunch_retry_fixture
from test_delivery_store import service as service

from devflow_temporal import delivery_native_process as native
from devflow_temporal import supervisor
from devflow_temporal.delivery_activities import delivery_role
from devflow_temporal.delivery_orphans import _complete_attempt, reconcile_attempt
from devflow_temporal.delivery_resources import (
    RunResources,
    observe_completed_resources,
    observe_finalized_resources,
    private_directory,
    read_private,
)
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.supervisor import DeliverySupervisor

MONITOR_SCRIPT = (
    "import json, os, signal, sys\n"
    "from pathlib import Path\n"
    "from devflow_temporal import delivery_native_process as native\n"
    "request = json.load(sys.stdin)\n"
    "folder = Path(request.pop('folder'))\n"
    "spec = request.pop('spec')\n"
    "request['cwd'] = Path(request['cwd'])\n"
    "request['ports'] = tuple(request['ports'])\n"
    "monitor_lock = request.pop('monitor_lock', None)\n"
    "def die(*args, **kwargs):\n"
    "    os.kill(os.getpid(), signal.SIGKILL)\n"
    "{hook}"
    "native.NativeProcess(spec, folder, **request,\n"
    "    cancelled=lambda: native._cancel_requested(folder),\n"
    ")._run(monitor=True, monitor_lock=monitor_lock)\n"
)
HOOKS = {
    # The monitor dies while spawning its child: journal, launch.json and
    # process.log exist, nothing was authorized.
    "spawn": (
        "original_popen = native.subprocess.Popen\n"
        "def popen(argv, *args, **kwargs):\n"
        "    if 'devflow_temporal.native_child' in argv:\n"
        "        die()\n"
        "    return original_popen(argv, *args, **kwargs)\n"
        "native.subprocess.Popen = popen\n"
    ),
    # The monitor dies right after journaling, before launch.json exists.
    "launch": (
        "original = native.write_private\n"
        "def write(path, value):\n"
        "    if path.name == 'launch.json':\n"
        "        die()\n"
        "    return original(path, value)\n"
        "native.write_private = write\n"
    ),
    # The monitor dies as it reads ready.json: its gated child is alive and
    # waiting for a GO that never comes; the journal is still "allocated".
    "ready": (
        "original_read = native.read_private\n"
        "def read(path):\n"
        "    if path.name == 'ready.json':\n"
        "        die()\n"
        "    return original_read(path)\n"
        "native.read_private = read\n"
    ),
}


def _lose_monitor(monkeypatch, died_at):
    original_popen = subprocess.Popen
    script = MONITOR_SCRIPT.format(hook=HOOKS[died_at])

    def popen(argv, *args, **kwargs):
        if argv[-1] == "devflow_temporal.delivery_native_process":
            argv = [sys.executable, "-I", "-c", script]
        return original_popen(argv, *args, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", popen)


def _assert_not_started_journal(folder, died_at):
    journal = read_private(folder / "native-process.json")
    assert journal["phase"] == "finished" and journal["monitoring_complete"] is False
    assert journal["result"]["provider_outcome"] == "not_started"
    assert journal["result"]["interrupted_monitor"] is True
    assert journal["cleanup_reconciliation"]["previous_phase"] == "allocated"
    assert (folder / "launch.json").exists() == (died_at != "launch")
    assert not (folder / "start.json").exists() and not (folder / "result.json").exists()
    observed = native.process_table()
    assert not any(observed.get(int(pid), {}).get("identity") == entry["identity"]
                   and not observed[int(pid)]["stat"].startswith("Z")
                   for pid, entry in journal["owned"].items())
    if died_at == "ready":
        ready = read_private(folder / "ready.json")
        assert str(ready["pid"]) in journal["owned"]
    assert native.reconcile_process(folder / "native-process.json")["cleanup"] \
        == "observed-native-confirmed"
    assert read_private(folder / "native-process.json") == journal
    assert native.reconciled_not_started(folder)
    return journal


def _occupied(case):
    with case.store._connect() as db:
        return db.execute("SELECT COUNT(*) FROM delivery_attempts WHERE state IN "
                          "('starting','running','unknown')").fetchone()[0]


def _dead_owner(case, state):
    # The owner claimed its slot (or a stale observer marked it unknown) and
    # died before any monitor journaled, so no provider can have started.
    case.owner._claim(case.request)
    with case.store._connect() as db:
        db.execute("UPDATE delivery_attempts SET state=?,started_at='prior-owner',"
                   "finished_at=?,cleanup=? WHERE job_key=?",
                   (state, "prior-owner" if state == "unknown" else None,
                    "unknown" if state == "unknown" else "none", case.key))


def _assert_launch_absent(case, result):
    row = attempt(case)
    assert row["state"] == "finished" and row["cleanup"] == "confirmed"
    # The prelaunch contract: no provider process is recorded on the row.
    assert row["pid"] is None and row["process_identity"] is None and row["session_id"] is None
    assert _occupied(case) == 0
    assert result["status"] == "blocked" and result["finish_reason"] == "prelaunch"
    assert result["cleanup"] == "confirmed" and result["session_id"] is None
    saved = json.loads(row["result_json"])
    assert saved["finish_reason"] == "prelaunch" and saved["status"] == "blocked"
    assert not (case.state / "invocations").exists()
    audit = [event for event in case.store.events(case.request["spec"]["run_id"])
             if event["type"] == "native_attempt_cleanup_reconciled"]
    assert len(audit) == 1
    assert audit[0]["payload"]["original_attempts"][0]["state"] != "finished"
    return row


def _admits(case, role):
    owner = DeliverySupervisor(DeliveryStore(case.store.config), capacity=1)
    key, _ = owner._claim({**case.request, "role": role})
    started = owner._enter_capacity(key)
    if started:
        owner._mark_prelaunch_blocked(key, "fixture ends before launch")
    return bool(started)


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_state", ["starting", "unknown"])
async def test_retry_finalizes_dead_owner_that_never_journaled(native_role, prior_state):
    case = native_role
    _dead_owner(case, prior_state)
    result = await delivery_role(case.request)
    _assert_launch_absent(case, result)
    assert case.created == []
    assert not (case.folder / "native-process.json").exists()
    assert await delivery_role(case.request) == result
    assert _admits(case, "review")


@pytest.mark.asyncio
async def test_live_owner_in_its_prelaunch_window_keeps_the_slot(native_role):
    case = native_role
    _dead_owner(case, "starting")
    # Another live worker holds the owner lock while it prepares this launch.
    lock_path = DeliverySupervisor._owner_lock_path(case.request["spec"], case.key)
    private_directory(lock_path.parent)
    held = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(held, fcntl.LOCK_EX)
    try:
        result = await delivery_role(case.request)
        assert result["cleanup"] == "unknown" and result["status"] == "recovery_unknown"
        assert attempt(case)["state"] == "unknown" and _occupied(case) == 1
        assert not _admits(case, "review")
        with pytest.raises(ValueError, match="not provably absent"):
            _complete_attempt(case.store, case.request["spec"], attempt(case))
        outcome = reconcile_attempt(case.store, job_key=case.key)
        assert outcome["changed"] is False and outcome["held_locks"] == ["attempt-owner"]
        # The attempt folder stays untouched: retry-prelaunch needs it empty.
        assert not (case.folder.exists() and any(case.folder.iterdir()))
        assert attempt(case)["state"] == "unknown"
    finally:
        os.close(held)
    # The owner died: its released lock lets the next admission reclaim the slot.
    assert _admits(case, "verify")
    row = attempt(case)
    assert row["state"] == "finished" and row["cleanup"] == "confirmed"
    assert json.loads(row["result_json"])["finish_reason"] == "prelaunch"
    assert case.created == []


@pytest.mark.asyncio
async def test_real_owner_paused_in_launch_window_keeps_its_slot(native_role, monkeypatch):
    case = native_role
    entered, release = threading.Event(), threading.Event()

    def paused(request, folder):
        # The owner is alive between its capacity claim and its first journal write.
        entered.set()
        assert release.wait(60)
        return ("fixture", {"PATH": "/usr/bin:/bin"})

    monkeypatch.setattr(supervisor, "prepare_native_role", paused)
    task = asyncio.create_task(delivery_role(case.request))
    assert await asyncio.to_thread(entered.wait, 60)
    assert attempt(case)["state"] == "starting"
    assert not (case.folder / "native-process.json").exists()
    lock_path = DeliverySupervisor._owner_lock_path(case.request["spec"], case.key)
    probe = os.open(lock_path, os.O_RDWR)
    try:
        with pytest.raises(BlockingIOError):
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        os.close(probe)
    # Neither a competing admission, the sweep nor the CLI may take the slot.
    assert not await asyncio.to_thread(_admits, case, "review")
    with pytest.raises(ValueError, match="not provably absent"):
        _complete_attempt(case.store, case.request["spec"], attempt(case))
    outcome = await asyncio.to_thread(reconcile_attempt, case.store, job_key=case.key)
    assert outcome["changed"] is False and outcome["held_locks"] == ["attempt-owner"]
    assert attempt(case)["state"] == "starting"
    release.set()
    result = await asyncio.wait_for(task, 60)
    assert result["finish_reason"] == "fixture"
    assert (case.state / "invocations").read_text().split() == ["one"]
    assert attempt(case)["state"] == "finished"


@pytest.mark.asyncio
@pytest.mark.parametrize("resolution", ["waiter", "sweep"])
@pytest.mark.parametrize("died_at", ["spawn", "launch", "ready"])
async def test_monitor_lost_before_authorization_is_launch_absent(
        native_role, monkeypatch, died_at, resolution):
    case = native_role
    _lose_monitor(monkeypatch, died_at)
    if resolution == "sweep":
        # The waiting worker is lost right after its monitor; a later sweep
        # resolves the unknown row from the journal alone.
        complete = supervisor.DeliverySupervisor._complete_native

        def lost(self, *args, **kwargs):
            raise RuntimeError("fixture worker lost before completion")

        monkeypatch.setattr(supervisor.DeliverySupervisor, "_complete_native", lost)
        unknown = await delivery_role(case.request)
        assert unknown["cleanup"] == "unknown" and attempt(case)["state"] == "unknown"
        monkeypatch.setattr(supervisor.DeliverySupervisor, "_complete_native", complete)
        receipt = _complete_attempt(case.store, case.request["spec"], attempt(case))
        assert receipt["cleanup"] == "observed-native-confirmed"
        assert json.loads(attempt(case)["result_json"])["finish_reason"] == "prelaunch"
    # A live waiter, or a later retry after the sweep, publishes the same result.
    result = await delivery_role(case.request)
    row = _assert_launch_absent(case, result)
    assert len(case.created) == 1
    _assert_not_started_journal(case.folder, died_at)
    assert await delivery_role(case.request) == result
    assert attempt(case) == row
    assert _admits(case, "review")


@pytest.mark.asyncio
@pytest.mark.parametrize("died_at", ["spawn", "launch", "ready"])
async def test_monitor_lost_before_authorization_passes_automatic_retry_observation(
        native_role, monkeypatch, died_at):
    case = native_role
    _lose_monitor(monkeypatch, died_at)
    result = await delivery_role(case.request)
    row = _assert_launch_absent(case, result)
    _assert_not_started_journal(case.folder, died_at)
    # Automatic retry authenticates the blocked run's attempts against their
    # registered journals before reusing anything.
    assert RunResources(case.request["spec"]).finalize("blocked")["outcome"] == "blocked"
    observed = observe_completed_resources(case.request["spec"], [row])
    assert observed["journal_sha256"]
    assert observe_finalized_resources(case.request["spec"])["finalization_sha256"]


@pytest.mark.parametrize("died_at", ["spawn", "launch", "ready"])
def test_monitor_lost_before_authorization_is_retryable_prelaunch(
        service, monkeypatch, tmp_path, died_at):
    store, request = service
    spec, _state, command, folder = _sealed_prelaunch_retry_fixture(store, request, monkeypatch)
    _lose_monitor(monkeypatch, died_at)
    # The sealed fixture run is the retry's authority; only the launch itself
    # needs the native backend the monitor insists on.
    authority = {**spec, "policy": {**spec["policy"], "execution_backend": "native-macos"}}
    outcome = native.NativeProcess(
        authority, folder, argv=[sys.executable, "-c", "pass"], cwd=tmp_path,
        environment={"PATH": os.environ["PATH"]}, timeout=10, cancelled=lambda: False,
    ).run()
    assert outcome["interrupted_monitor"] is True
    assert outcome["provider_outcome"] == "not_started"
    _assert_not_started_journal(folder, died_at)
    assert any(folder.iterdir())
    # The folder keeps its evidence yet proves no provider ran: retry-prelaunch
    # and the resumed implementer's readback both accept it.
    response = store.retry_prelaunch(request["run_id"], command)
    assert response["phase"] == "repair_prelaunch_retry_queued"
    assert store.retry_prelaunch(request["run_id"], command) == response
    with store._connect() as db:
        run = db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id=?",
                         (request["run_id"],)).fetchone()
    store.repair_preflight(spec, json.loads(run["recovery_json"]))


@pytest.mark.asyncio
async def test_interrupted_provider_attempt_keeps_blocking_custody_observation(native_role):
    case = native_role
    (case.state / "hold-provider").touch()
    task = asyncio.create_task(delivery_role(case.request))
    try:
        await until(lambda: (case.state / "provider-started").exists())
        # The provider ran: its outcome stays unknown however clean the teardown.
        kill_owned_monitor(read_private(case.folder / "native-process.json"))
        (case.state / "release-provider").touch()
        result = await asyncio.wait_for(task, 60)
    finally:
        (case.state / "release-provider").touch()
        await asyncio.gather(task, return_exceptions=True)
    row = attempt(case)
    journal = read_private(case.folder / "native-process.json")
    assert result["finish_reason"] == "interrupted_monitor" and row["cleanup"] == "confirmed"
    assert journal["result"]["provider_outcome"] == "unknown"
    assert native.interrupted_cleanup_proven(journal)
    assert not native.reconciled_not_started(case.folder)
    spec = case.request["spec"]
    assert RunResources(spec).finalize("blocked")["outcome"] == "blocked"
    with pytest.raises(ValueError, match="live actor or port"):
        observe_completed_resources(spec, [row])
    with pytest.raises(ValueError, match="finalized inventory"):
        observe_finalized_resources(spec)


def test_closed_sweep_finalizes_launch_window_attempt_and_resources(api_fixture):
    from pathlib import Path

    from devflow_temporal import delivery_orphans
    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_resources import RunResources, write_private

    path, submission = api_fixture
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(submission)
    spec = store.effective_spec(submission["run_id"])
    store.project(spec["run_id"], phase="blocked", execution_state="blocked",
                  event_type="blocked", message="Original failure", outcome="blocked",
                  error="Original failure", cleanup="unknown")
    key = "a" * 64
    with store._connect() as db:
        # The owner died between its capacity claim and any monitor journal; the
        # workflow closed afterwards with the slot still counted as occupied.
        db.execute("INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,"
                   "state,result_path,started_at,finished_at,cleanup) VALUES (?,?,?,?,?,?,?,?,?,?)",
                   (key, spec["run_id"], "implement", 0, "original", "unknown",
                    str(Path(spec["state_dir"]) / "attempts" / key / "result.json"),
                    "prior-owner", "prior-owner", "unknown"))
        original = dict(db.execute("SELECT * FROM delivery_runs").fetchone())
    resources = RunResources(spec)
    with resources.locked() as manifest:
        write_private(resources.manifest, manifest)
    delivery_orphans._reconcile(store, original, spec, {"status": "TERMINATED"})
    with store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_attempts WHERE job_key=?",
                              (key,)).fetchone())
        current = dict(db.execute("SELECT * FROM delivery_runs").fetchone())
    assert row["state"] == "finished" and row["cleanup"] == "confirmed"
    assert json.loads(row["result_json"])["finish_reason"] == "prelaunch"
    receipt = json.loads(current["checks_json"])["resource_cleanup"]
    assert receipt["resource_cleanup"] == "confirmed" and current["cleanup"] == "confirmed"
    types = [event["type"] for event in store.events(spec["run_id"])]
    assert types.count("native_attempt_cleanup_reconciled") == 1
    assert types.count("resource_cleanup_reconciled") == 2
    # Nothing is left to reconcile: a second closed sweep observes no change.
    delivery_orphans._reconcile(store, original, spec, {"status": "TERMINATED"})
    assert [event["type"] for event in store.events(spec["run_id"])] == types


@pytest.mark.asyncio
async def test_reconcile_attempt_command_finalizes_dead_owner(native_role, monkeypatch, capsys):
    from devflow_temporal import delivery_control

    case = native_role
    _dead_owner(case, "unknown")
    outcome = reconcile_attempt(case.store, run_id=case.request["spec"]["run_id"],
                                role="implement", iteration=0)
    assert outcome["changed"] is True and outcome["state_before"] == "unknown"
    assert outcome["state"] == "finished" and outcome["cleanup"] == "confirmed"
    assert outcome["receipt"]["launch_absent"] is True
    _assert_launch_absent(case, outcome["result"])
    again = reconcile_attempt(case.store, job_key=case.key)
    assert again["changed"] is False and again["result"] == outcome["result"]
    with pytest.raises(ValueError, match="no matching unfinished native attempt"):
        reconcile_attempt(case.store, run_id=case.request["spec"]["run_id"],
                          role="implement", iteration=0)
    monkeypatch.setattr(sys, "argv", [
        "devflow-delivery", "--config", str(case.store.config.path),
        "reconcile-attempt", "--job-key", case.key,
    ])
    delivery_control.main()
    printed = json.loads(capsys.readouterr().out)
    assert printed["state"] == "finished" and printed["changed"] is False
    assert printed["result"] == outcome["result"]
