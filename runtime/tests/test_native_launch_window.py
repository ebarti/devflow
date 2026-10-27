"""Launch-window deaths release their capacity slot instead of leaking it (N-12)."""
from __future__ import annotations

import asyncio
import fcntl
import json
import os
import threading

import pytest
from test_dead_native_monitor import attempt
from test_delivery_api import api_fixture as api_fixture
from test_delivery_prelaunch_capacity import native_role as native_role
from test_delivery_store import service as service

from devflow_temporal import delivery_native_process as native
from devflow_temporal import supervisor
from devflow_temporal.delivery_activities import delivery_role
from devflow_temporal.delivery_orphans import _complete_attempt, reconcile_attempt
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
@pytest.mark.parametrize("retained_request", [False, True])
async def test_retry_finalizes_dead_owner_that_never_journaled(
    native_role, prior_state, retained_request,
):
    case = native_role
    _dead_owner(case, prior_state)
    if retained_request:
        from devflow_temporal.delivery_resources import write_private
        write_private(case.folder / "request.json", {
            **case.request, "workspace": str(case.broker.checkout), "review_diff": None,
        })
    result = await delivery_role(case.request)
    _assert_launch_absent(case, result)
    assert case.created == []
    assert native.reconciled_not_started(case.folder, receipt=result["native_process"])
    assert not (case.folder / "native-process.json").exists()
    assert await delivery_role(case.request) == result
    assert _admits(case, "review")


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
