"""Launch-window deaths release their capacity slot instead of leaking it (N-12)."""
from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys

import pytest
from test_dead_native_monitor import attempt
from test_delivery_api import api_fixture as api_fixture
from test_delivery_prelaunch_capacity import native_role as native_role

from devflow_temporal import delivery_native_process as native
from devflow_temporal import supervisor
from devflow_temporal.delivery_activities import delivery_role
from devflow_temporal.delivery_orphans import _complete_attempt, reconcile_attempt
from devflow_temporal.delivery_resources import private_directory, read_private
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
}


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
@pytest.mark.parametrize("resolution", ["waiter", "sweep"])
@pytest.mark.parametrize("died_at", ["spawn", "launch"])
async def test_monitor_lost_before_authorization_is_launch_absent(
        native_role, monkeypatch, died_at, resolution):
    case = native_role
    original_popen = subprocess.Popen
    script = MONITOR_SCRIPT.format(hook=HOOKS[died_at])

    def popen(argv, *args, **kwargs):
        if argv[-1] == "devflow_temporal.delivery_native_process":
            argv = [sys.executable, "-I", "-c", script]
        return original_popen(argv, *args, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", popen)
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
    journal = read_private(case.folder / "native-process.json")
    assert journal["phase"] == "finished" and journal["monitoring_complete"] is False
    assert journal["result"]["provider_outcome"] == "not_started"
    assert journal["result"]["interrupted_monitor"] is True
    assert journal["cleanup_reconciliation"]["previous_phase"] == "allocated"
    assert (case.folder / "launch.json").exists() == (died_at == "spawn")
    observed = native.process_table()
    assert not any(observed.get(int(pid), {}).get("identity") == entry["identity"]
                   and not observed[int(pid)]["stat"].startswith("Z")
                   for pid, entry in journal["owned"].items())
    assert native.reconcile_process(case.folder / "native-process.json")["cleanup"] \
        == "observed-native-confirmed"
    assert read_private(case.folder / "native-process.json") == journal
    assert await delivery_role(case.request) == result
    assert attempt(case) == row
    assert _admits(case, "review")


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
