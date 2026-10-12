from __future__ import annotations

import asyncio
import fcntl
import json
import os
import signal
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_prelaunch_capacity import PROVIDER
from test_delivery_prelaunch_capacity import native_role as native_role

from devflow_temporal import delivery_native_process as native
from devflow_temporal import delivery_orphans
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_activities import delivery_role
from devflow_temporal.delivery_resources import read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.supervisor import DeliverySupervisor


async def until(predicate):
    deadline = time.monotonic() + 10
    while not predicate():
        assert time.monotonic() < deadline, "fixture did not reach its controlled boundary"
        await asyncio.sleep(0.02)


def kill_owned_monitor(journal):
    monitor = journal["monitor"]
    assert native.process_table()[monitor["pid"]]["identity"] == monitor["identity"]
    os.kill(monitor["pid"], signal.SIGKILL)


def attempt(case, key=None):
    with case.store._connect() as db:
        return dict(db.execute("SELECT * FROM delivery_attempts WHERE job_key=?",
                               (key or case.key,)).fetchone())


def assert_reconciled(case, original_journal, result):
    row = attempt(case)
    assert row["state"] == "finished" and row["cleanup"] == "confirmed"
    assert result["status"] == "recovery_unknown"
    assert result["finish_reason"] == "interrupted_monitor"
    assert result["cleanup"] == "confirmed" and result["session_id"] is None
    journal = read_private(case.folder / "native-process.json")
    assert journal["phase"] == "finished" and journal["monitoring_complete"] is False
    assert journal["result"]["stdio_drained"] is False
    assert journal["result"]["provider_outcome"] == "unknown"
    proof = journal["cleanup_reconciliation"]
    assert proof["previous_journal_sha256"] == digest(original_journal)
    assert proof["previous_phase"] == "authorized"
    observed = native.process_table()
    assert not any(observed.get(int(pid), {}).get("identity") == identity["identity"]
                   and not observed[int(pid)]["stat"].startswith("Z")
                   for pid, identity in journal["owned"].items())
    audit = [event for event in case.store.events(case.request["spec"]["run_id"])
             if event["type"] == "native_attempt_cleanup_reconciled"]
    assert len(audit) == 1
    assert audit[0]["payload"]["original_attempts"][0]["state"] != "finished"
    assert audit[0]["payload"]["native_observations"][0]["cleanup_reconciliation"] == proof
    assert (case.state / "invocations").read_text().splitlines() == ["one"]
    return row, journal


@pytest.mark.asyncio
@pytest.mark.parametrize("partial_pass", [False, True])
async def test_sigkill_monitor_confirms_cleanup_without_promoting_provider_result(
    native_role, partial_pass,
):
    case = native_role
    (case.state / "hold-provider").touch()
    task = asyncio.create_task(delivery_role(case.request))
    try:
        await until(lambda: (case.state / "provider-started").exists())
        if partial_pass:
            write_private(case.folder / "result.json", {
                "status": "pass", "summary": "unobserved provider receipt",
                "findings": [], "session_id": "unverified-session", "usage": None,
            })
        original = read_private(case.folder / "native-process.json")
        kill_owned_monitor(original)
        if not partial_pass:
            (case.state / "release-provider").touch()
        result = await asyncio.wait_for(task, 15)
        row, journal = assert_reconciled(case, original, result)
        assert await delivery_role(case.request) == result
        assert attempt(case) == row
        assert read_private(case.folder / "native-process.json") == journal
        next_request = {**case.request, "role": "review"}
        next_key, existing = case.owner._claim(next_request)
        assert existing is None
        case.owner.capacity = 1
        assert case.owner._enter_capacity(next_key)
        assert attempt(case, next_key)["state"] == "starting"
        case.owner._mark_prelaunch_blocked(next_key, "fixture ends before launch")
    finally:
        (case.state / "release-provider").touch()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("resolution", ["capacity", "orphan", "concurrent"])
async def test_restart_reconciles_dead_owner_before_next_capacity_admission(
    native_role, monkeypatch, tmp_path, resolution,
):
    case = native_role
    # Include an observed descendant in a separate process group, so cleanup
    # cannot be certified from the direct child or one process group alone.
    (tmp_path / "provider.py").write_text("import subprocess\n" + PROVIDER.replace(
        "(state/'provider-started').touch()",
        "child=subprocess.Popen([sys.executable,'-I','-c','import time; time.sleep(30)'],"
        "start_new_session=True)\n(state/'provider-started').touch()"))
    (case.state / "hold-provider").touch()
    original_cancel = case.process_type._request_cancel

    def lose_observer(process):
        if (case.state / "provider-started").exists():
            raise RuntimeError("fixture observer lost after launch")
        return original_cancel(process)

    monkeypatch.setattr(case.process_type, "_request_cancel", lose_observer)
    result = await delivery_role(case.request)
    assert result["cleanup"] == "unknown" and attempt(case)["state"] == "unknown"
    monkeypatch.setattr(case.process_type, "_request_cancel", original_cancel)
    owner = DeliverySupervisor(DeliveryStore(case.store.config), capacity=1)
    next_key, _ = owner._claim({**case.request, "role": "review"})
    # A lost observer or closed workflow cannot release a still-live monitor.
    assert owner._enter_capacity(next_key) is None
    assert attempt(case)["state"] == "unknown"
    await until(lambda: len(read_private(case.folder / "native-process.json")["owned"]) >= 2)
    original = read_private(case.folder / "native-process.json")
    kill_owned_monitor(original)
    await until(lambda: native.process_table().get(original["monitor"]["pid"], {}).get(
        "identity") != original["monitor"]["identity"])
    if resolution == "orphan":
        from devflow_temporal.delivery_orphans import _complete_attempt

        _complete_attempt(case.store, case.request["spec"], attempt(case))
    if resolution == "concurrent":
        other_key, _ = owner._claim({**case.request, "role": "verify"})
        with ThreadPoolExecutor(max_workers=2) as executor:
            admissions = list(executor.map(owner._enter_capacity, [next_key, other_key]))
        assert sum(bool(value) for value in admissions) == 1
        with case.store._connect() as db:
            assert db.execute("SELECT COUNT(*) FROM delivery_attempts WHERE state IN "
                              "('starting','running','unknown')").fetchone()[0] == 1
        for key, admitted in zip([next_key, other_key], admissions, strict=True):
            if admitted:
                owner._mark_prelaunch_blocked(key, "fixture ends before launch")
    else:
        assert owner._enter_capacity(next_key)
        owner._mark_prelaunch_blocked(next_key, "fixture ends before launch")
    saved = json.loads(attempt(case)["result_json"])
    row, journal = assert_reconciled(case, original, saved)
    assert row["cleanup"] == "confirmed"
    assert native.reconcile_process(case.folder / "native-process.json")["cleanup"] \
        == "observed-native-confirmed"
    assert read_private(case.folder / "native-process.json") == journal
    assert owner._mark_unknown(case.key, "late stale observer") == saved
    assert attempt(case) == row


@pytest.fixture
def dead_journal(tmp_path):
    path = tmp_path / "attempt" / "native-process.json"
    identity = {"ppid": 1, "pgid": 999998, "stat": "S", "identity": "dead-fixture-identity"}
    journal = {"intent": {"run_id": "fixture", "policy_digest": "fixture-policy",
                          "ports": []}, "ports": [], "phase": "authorized",
               "monitoring_complete": False, "owned": {"999998": identity},
               "monitor": {"pid": 999997, **identity}}
    write_private(path, journal)
    for name in ("native-monitor.lock", "native-process.lock"):
        (path.parent / name).touch(mode=0o600)
    return path, journal


@pytest.mark.parametrize("lock_name", ["native-monitor.lock", "native-process.lock"])
@pytest.mark.parametrize("defect", ["held", "missing", "nonprivate", "replaced",
                                      "replaced-after-restart"])
def test_interrupted_cleanup_refuses_missing_or_unowned_lifecycle_lock(
    dead_journal, lock_name, defect,
):
    path, original = dead_journal
    lock = path.parent / lock_name
    descriptor = None
    supplied = {}
    if defect == "replaced-after-restart":
        original["lifecycle_locks"] = {
            name: {"device": item.stat().st_dev, "inode": item.stat().st_ino,
                   "uid": item.stat().st_uid}
            for name in ("native-monitor.lock", "native-process.lock")
            for item in [path.parent / name]}
        write_private(path, original)
    try:
        if defect in {"held", "replaced"}:
            descriptor = os.open(lock, os.O_RDWR)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if defect == "missing":
            lock.unlink()
        elif defect == "nonprivate":
            lock.chmod(0o644)
        elif defect in {"replaced", "replaced-after-restart"}:
            lock.rename(lock.with_suffix(".original"))
            lock.touch(mode=0o600)
            if descriptor is not None:
                supplied[lock_name] = descriptor
        receipt = native.reconcile_process(path, _locks=supplied)
        assert receipt["cleanup"] == "unknown"
        assert read_private(path) == original
    finally:
        if descriptor is not None:
            os.close(descriptor)


def test_live_monitor_and_child_are_never_stopped_by_capacity_reconciliation(dead_journal):
    path, journal = dead_journal
    child = subprocess.Popen([sys.executable, "-I", "-c", "import time; time.sleep(30)"])
    try:
        observed = native.process_table()
        journal.update(monitor={"pid": os.getpid(), **observed[os.getpid()]},
                       owned={str(child.pid): observed[child.pid]})
        write_private(path, journal)
        receipt = native.reconcile_process(path)
        assert receipt["cleanup"] == "unknown" and child.poll() is None
        assert read_private(path) == journal
    finally:
        child.terminate()
        child.wait(timeout=5)


def test_pid_reuse_does_not_authorize_signalling_foreign_process(dead_journal):
    path, journal = dead_journal
    child = subprocess.Popen([sys.executable, "-I", "-c", "import time; time.sleep(30)"])
    try:
        journal["owned"] = {str(child.pid): {**native.process_table()[child.pid],
                                           "identity": "previous-owner-not-this-process"}}
        write_private(path, journal)
        receipt = native.reconcile_process(path)
        assert receipt["cleanup"] == "observed-native-confirmed"
        assert child.poll() is None
        assert read_private(path)["result"]["provider_outcome"] == "unknown"
    finally:
        child.terminate()
        child.wait(timeout=5)


def test_foreign_listener_prevents_cleanup_until_port_is_gone(dead_journal):
    path, journal = dead_journal
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        journal["ports"] = journal["intent"]["ports"] = [port]
        write_private(path, journal)
        receipt = native.reconcile_process(path)
        assert receipt["cleanup"] == "unknown" and receipt["owned_ports_clear"] is False
        assert read_private(path) == journal
    assert native.reconcile_process(path)["cleanup"] == "observed-native-confirmed"


@pytest.mark.parametrize("defect", ["identity", "monitor", "inspection"])
def test_incomplete_custody_or_inspection_keeps_cleanup_unknown(dead_journal, monkeypatch, defect):
    path, journal = dead_journal
    if defect == "identity":
        del journal["owned"]["999998"]["identity"]
    elif defect == "monitor":
        del journal["monitor"]
    else:
        def unavailable():
            raise RuntimeError("fixture process inspection unavailable")
        monkeypatch.setattr(native, "process_table", unavailable)
    write_private(path, journal)
    assert native.reconcile_process(path)["cleanup"] == "unknown"
    assert read_private(path) == journal


def test_legacy_dead_monitor_without_inode_fields_can_still_be_reconciled(dead_journal):
    path, original = dead_journal
    assert "lifecycle_locks" not in original
    assert native.reconcile_process(path)["cleanup"] == "observed-native-confirmed"
    journal = read_private(path)
    assert "lifecycle_locks" not in journal
    assert journal["cleanup_reconciliation"]["previous_journal_sha256"] == digest(original)
    assert journal["result"]["provider_outcome"] == "unknown"


@pytest.mark.asyncio
async def test_failed_capacity_sweep_never_finalizes_role_waiting_for_capacity(
    native_role, monkeypatch, tmp_path,
):
    case = native_role
    (case.state / "hold-provider").touch()
    original_cancel = case.process_type._request_cancel

    def lose_observer(process):
        if (case.state / "provider-started").exists():
            raise RuntimeError("fixture observer lost after launch")
        return original_cancel(process)

    monkeypatch.setattr(case.process_type, "_request_cancel", lose_observer)
    result = await delivery_role(case.request)
    assert result["cleanup"] == "unknown" and attempt(case)["state"] == "unknown"
    monkeypatch.setattr(case.process_type, "_request_cancel", original_cancel)
    await until(lambda: len(read_private(case.folder / "native-process.json")["owned"]) >= 1)
    original = read_private(case.folder / "native-process.json")
    kill_owned_monitor(original)
    await until(lambda: native.process_table().get(original["monitor"]["pid"], {}).get(
        "identity") != original["monitor"]["identity"])
    # The dead owner holds the only slot until a capacity sweep reconciles it.
    config = json.loads(case.store.config.path.read_text())
    config["capacity"] = 1
    case.store.config.path.write_text(json.dumps(config))
    sweeps = []

    def unavailable_process_table():
        sweeps.append(time.monotonic())
        raise subprocess.CalledProcessError(1, ["ps"], stderr="fixture process table outage")

    monkeypatch.setattr(delivery_orphans, "process_table", unavailable_process_table)
    # The recovery generation queues behind its own dead predecessor.
    waiting_request = {**case.request, "attempt_generation": 1}
    waiting_key = case.owner._job_key(waiting_request)
    waiting = asyncio.create_task(delivery_role(waiting_request))
    try:
        await until(lambda: len(sweeps) >= 1)
        await asyncio.wait({waiting}, timeout=2.5)
        # The outage is charged to the sweep, never to the role queued behind it.
        assert not waiting.done()
        assert attempt(case, waiting_key)["state"] == "queued"
        assert attempt(case)["state"] == "unknown"
        assert len(sweeps) >= 2
        # A persistent deferral becomes visible on the run, not only in the log.
        run_id = case.request["spec"]["run_id"]
        await until(lambda: any(event["type"] == "native_capacity_deferred"
                                for event in case.store.events(run_id)))
        deferred = [event for event in case.store.events(run_id)
                    if event["type"] == "native_capacity_deferred"][0]
        assert deferred["payload"]["job_key"] == waiting_key
        assert deferred["payload"]["consecutive_deferrals"] == 3
        assert "capacity sweep could not resolve 1 native attempt" in deferred["message"]
        monkeypatch.setattr(delivery_orphans, "process_table", native.process_table)
        await until(lambda: attempt(case, waiting_key)["state"] != "queued")
        (case.state / "release-provider").touch()
        result = await asyncio.wait_for(waiting, 20)
    finally:
        (case.state / "release-provider").touch()
        await asyncio.gather(waiting, return_exceptions=True)
    assert result["finish_reason"] == "fixture" and result["session_id"] == "fixture-session"
    row = attempt(case, waiting_key)
    assert row["state"] == "finished" and row["cleanup"] == "confirmed"
    reconciled = attempt(case)
    assert reconciled["state"] == "finished" and reconciled["cleanup"] == "confirmed"
    assert json.loads(reconciled["result_json"])["finish_reason"] == "interrupted_monitor"
    assert (case.state / "invocations").read_text().splitlines() == ["one", "one"]
