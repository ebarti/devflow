from __future__ import annotations

import asyncio
import json
import os
import re
import signal
import socket
import sqlite3
import stat
import subprocess
import sys
import time
from contextlib import closing
from pathlib import Path

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


def provider_boundary_diagnostic(case, task, completed=()):
    """Only finite facts from this fixture's original task and owned startup files."""
    from runtime_location_scenario import ci_failure_diagnostics

    if not ci_failure_diagnostics():
        return None

    def choice(value, allowed):
        return value if isinstance(value, str) and value in allowed else "other"

    def public_frames(frames):
        package = Path(native.__file__).parent
        result = []
        for frame, line in frames:
            module = frame.f_globals.get("__name__")
            function = frame.f_code.co_name
            if (not isinstance(module, str) or not re.fullmatch(
                    r"devflow_temporal(?:\.[a-z_][a-z0-9_]*){0,3}", module)
                    or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,79}", function)
                    or type(line) is not int or not 0 < line <= 100000):
                continue
            relative = module.removeprefix("devflow_temporal").lstrip(".")
            expected = package / (relative.replace(".", "/") + ".py" if relative
                                  else "__init__.py")
            if Path(frame.f_code.co_filename) == expected:
                result.append({"module": module, "function": function, "line": line})
        return result[-8:]

    def bounded_file(name):
        try:
            descriptor = os.open(case.folder / name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                info = os.fstat(descriptor)
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                        or info.st_mode & 0o077 or info.st_size > 262144):
                    return None
                value = os.read(descriptor, 262145)
                return value if len(value) <= 262144 else None
            finally:
                os.close(descriptor)
        except OSError:
            return None

    types = {"ValueError", "RuntimeError", "AssertionError", "TypeError", "KeyError",
             "OSError", "PermissionError", "FileNotFoundError", "TimeoutError",
             "ImportError", "ModuleNotFoundError", "SyntaxError", "CancelledError",
             "NativeProcessUnknown", "ApplicationError"}
    facts = {"task_state": "pending", "journal": {"state": "unavailable"},
             "attempt": {"state": "unavailable"}}
    if completed or task.done():
        if completed:
            value = completed[0]
        elif task.cancelled():
            value = asyncio.CancelledError()
        else:
            value = task.exception()
            if value is None:
                value = task.result()
        if isinstance(value, BaseException):
            facts.update(task_state="cancelled" if isinstance(value, asyncio.CancelledError)
                         else "raised", exception_type=choice(type(value).__name__, types))
            frames, trace = [], value.__traceback__
            while trace is not None and len(frames) < 64:
                frames.append((trace.tb_frame, trace.tb_lineno))
                trace = trace.tb_next
            facts["source_frames"] = public_frames(frames)
        else:
            facts["task_state"] = "returned"
            if isinstance(value, dict):
                facts["result"] = {
                    "status": choice(value.get("status"), {"pass", "blocked", "recovery_unknown"}),
                    "cleanup": choice(value.get("cleanup"), {"confirmed", "unknown"}),
                    "finish_reason": choice(value.get("finish_reason"), {
                        "prelaunch", "fixture", "interrupted_monitor", "done", "timeout"})}
    else:
        frames, seen, current = [], set(), task.get_coro()
        for _ in range(16):
            if current is None or id(current) in seen:
                break
            seen.add(id(current))
            frame = getattr(current, "cr_frame", None) or getattr(current, "gi_frame", None)
            if frame is not None:
                frames.append((frame, frame.f_lineno))
                # Follow only the child retained by this original public heartbeat,
                # never an all-tasks/thread/process scan.
                child = frame.f_locals.get("pending")
                if (frame.f_globals.get("__name__") == "devflow_temporal.delivery_activities"
                        and frame.f_code.co_name == "_with_heartbeat"
                        and isinstance(child, asyncio.Task)):
                    current = child.get_coro()
                    continue
            current = getattr(current, "cr_await", None) or getattr(current, "gi_yieldfrom", None)
        facts["source_frames"] = public_frames(frames)
    raw = bounded_file("native-process.json")
    if raw is not None:
        try:
            journal = json.loads(raw)
            result = journal.get("result") or {}
            facts["journal"] = {
                "state": "available", "phase": choice(journal.get("phase"), {
                    "allocated", "authorized", "finished", "unknown"}),
                "monitor_recorded": isinstance(journal.get("monitor"), dict),
                "owned_count": min(len(journal["owned"]), 1000)
                if isinstance(journal.get("owned"), dict) else None,
                "result_state": choice(result.get("state"), {"finished", "unknown"}),
                "exit_code": result.get("exit_code") if type(result.get("exit_code")) is int
                and -2**31 <= result["exit_code"] < 2**31 else None}
        except (ValueError, TypeError, AttributeError):
            pass
    try:
        with closing(sqlite3.connect(case.store.config.tracking_db.as_uri() + "?mode=ro",
                                    uri=True, timeout=0.1)) as db:
            row = db.execute("SELECT state,cleanup FROM delivery_attempts WHERE job_key=?",
                             (case.key,)).fetchone()
        if row:
            facts["attempt"] = {"state": choice(row[0], {
                "starting", "running", "unknown", "finished"}),
                "cleanup": choice(row[1], {"confirmed", "unknown", "none"})}
    except (OSError, sqlite3.Error):
        pass
    facts["startup_logs"] = {}
    for role, name in (("monitor", "monitor.log"), ("process", "process.log")):
        raw = bounded_file(name)
        marker = re.search(rb"(?:^|\n)([A-Za-z]+Error|NativeProcessUnknown):", raw) if raw else None
        facts["startup_logs"][role] = {"bounded_private_read": raw is not None,
            "python_exception_marker": choice(marker[1].decode("ascii"), types) if marker else None}
    return facts


def record_provider_boundary(error, case, task, phase, completed=()):
    try:
        facts = provider_boundary_diagnostic(case, task, completed)
        if facts is not None:
            facts["observation_phase"] = phase
            text = json.dumps({"provider_boundary_observation": facts}, sort_keys=True)
            error.add_note(text)
            print(text, file=sys.stderr, flush=True)
    except BaseException:
        pass  # Original boundary failure and task result must remain authoritative.


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
    boundary_error = None
    try:
        try:
            await until(lambda: (case.state / "provider-started").exists())
        except AssertionError as exc:
            boundary_error = exc
            record_provider_boundary(exc, case, task, "deadline")
            raise
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
        completed = await asyncio.gather(task, return_exceptions=True)
        if boundary_error is not None:
            record_provider_boundary(boundary_error, case, task, "settled", completed)


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
