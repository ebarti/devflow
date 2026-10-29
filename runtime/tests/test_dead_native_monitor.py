from __future__ import annotations

import asyncio
import os
import signal
import socket
import subprocess
import sys
import time

import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_prelaunch_capacity import native_role as native_role

from devflow_temporal import delivery_native_process as native
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_activities import delivery_role
from devflow_temporal.delivery_resources import read_private, write_private


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


@pytest.mark.parametrize("defect", ["identity", "inspection"])
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
