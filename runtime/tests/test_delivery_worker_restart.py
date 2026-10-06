from __future__ import annotations

import concurrent.futures
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from devflow_temporal.delivery_native_process import NativeProcess, process_table, stop_observed
from devflow_temporal.delivery_resources import RunResources, read_private


@pytest.mark.parametrize("outcome", ["complete", "cancel", "timeout", "changed"])
def test_owned_child_completes_once_after_worker_is_killed(tmp_path, outcome):
    state = tmp_path / "runs" / "restart"
    state.mkdir(parents=True, mode=0o700)
    spec = {
        "run_id": "restart", "state_dir": str(state),
        "checkout": str(tmp_path / "checkout"), "policy_digest": "a" * 64,
        "policy": {"execution_backend": "native-macos"},
    }
    scratch = RunResources(spec).scratch("role", "worker-restart")
    started, release = scratch / "started", scratch / "release"
    invocations = scratch / "invocations"
    command = (
        "import os,pathlib,time; "
        f"p=pathlib.Path({str(invocations)!r}); "
        "p.open('a').write(str(os.getpid())+'\\n'); "
        "print('started',flush=True); "
        f"pathlib.Path({str(started)!r}).touch(); "
        f"release=pathlib.Path({str(release)!r}); "
        "exec('while not release.exists(): time.sleep(0.02)'); "
        "print('completed',flush=True)"
    )
    folder = state / "attempt"
    request = {
        "argv": [sys.executable, "-c", command], "cwd": str(scratch),
        "environment": {"PATH": "/usr/bin:/bin"},
        "timeout": 1 if outcome == "timeout" else 15,
    }
    worker_script = tmp_path / "worker.py"
    worker_script.write_text(
        "import json,sys\nfrom pathlib import Path\n"
        "from devflow_temporal.delivery_native_process import NativeProcess\n"
        f"spec=json.loads({json.dumps(spec)!r})\n"
        f"request=json.loads({json.dumps(request)!r})\n"
        "request['cwd']=Path(request['cwd'])\n"
        f"NativeProcess(spec,Path({str(folder)!r}),**request).run()\n"
    )
    worker_log = (tmp_path / "worker.log").open("wb")
    worker = subprocess.Popen(
        [sys.executable, "-I", str(worker_script)],
        stdout=worker_log, stderr=subprocess.STDOUT, start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not started.exists():
            assert worker.poll() is None, (tmp_path / "worker.log").read_text()
            assert time.monotonic() < deadline, "test child did not start"
            time.sleep(0.02)
        worker.kill()
        worker.wait(timeout=5)
        assert worker.returncode < 0
        if outcome == "changed":
            with pytest.raises(ValueError, match="authority changed"):
                NativeProcess(
                    spec, folder, **{**request, "cwd": scratch, "argv": [sys.executable, "-V"]},
                    cancelled=lambda: True,
                ).run()
            assert not (folder / "cancel").exists()
        if outcome in {"complete", "changed"}:
            release.touch()
        result = NativeProcess(
            spec, folder, **{**request, "cwd": scratch},
            cancelled=lambda: outcome == "cancel",
        ).run()
        assert result["cleanup"] == "observed-native-confirmed"
        assert result["cancelled"] == (outcome == "cancel")
        assert result["timed_out"] == (outcome == "timeout")
        if outcome in {"complete", "changed"}:
            assert result["exit_code"] == 0
            assert Path(result["log"]).read_text().splitlines() == ["started", "completed"]
        else:
            assert result["exit_code"] != 0
        assert len(invocations.read_text().splitlines()) == 1
        monitor = read_private(folder / "native-process.json")["monitor"]
        assert monitor["pid"] in result["observed_owned_pids"]
        entry = process_table().get(monitor["pid"])
        assert (entry is None or entry["identity"] != monitor["identity"]
                or entry["stat"].startswith("Z"))
        final_status = "cancelled" if outcome == "cancel" else (
            "blocked" if outcome == "timeout" else "delivered"
        )
        assert RunResources(spec).finalize(final_status)["resource_cleanup"] == "confirmed"
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.wait(timeout=5)
        worker_log.close()
        journal = folder / "native-process.json"
        if journal.exists():
            saved = read_private(journal)
            owned = {int(pid): value for pid, value in saved.get("owned", {}).items()}
            if saved.get("monitor"):
                owned[saved["monitor"]["pid"]] = saved["monitor"]
            assert stop_observed(owned)


def test_monitor_spawn_failure_can_retry_without_repeating_command(tmp_path, monkeypatch):
    from devflow_temporal import delivery_native_process

    state = tmp_path / "runs" / "spawn"
    state.mkdir(parents=True, mode=0o700)
    spec = {
        "run_id": "spawn", "state_dir": str(state),
        "checkout": str(tmp_path / "checkout"), "policy_digest": "a" * 64,
        "policy": {"execution_backend": "native-macos"},
    }
    invocations = tmp_path / "invocations"
    process = NativeProcess(
        spec, state / "attempt", argv=[sys.executable, "-c",
            f"import pathlib; pathlib.Path({str(invocations)!r}).open('a').write('one\\n')"],
        cwd=tmp_path, environment={"PATH": "/usr/bin:/bin"}, timeout=5,
    )
    original = subprocess.Popen
    first = True

    def spawn(*args, **kwargs):
        nonlocal first
        if first:
            first = False
            raise OSError("temporary process launch failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(delivery_native_process.subprocess, "Popen", spawn)
    with pytest.raises(OSError, match="temporary process launch failure"):
        process.run()
    assert not invocations.exists()
    result = process.run()
    assert result["exit_code"] == 0
    assert result["cleanup"] == "observed-native-confirmed"
    assert invocations.read_text().splitlines() == ["one"]


def test_overlapping_waiters_share_one_monitor_and_command(tmp_path):
    state = tmp_path / "runs" / "overlap"
    state.mkdir(parents=True, mode=0o700)
    spec = {
        "run_id": "overlap", "state_dir": str(state),
        "checkout": str(tmp_path / "checkout"), "policy_digest": "a" * 64,
        "policy": {"execution_backend": "native-macos"},
    }
    invocations = tmp_path / "invocations"
    process = NativeProcess(
        spec, state / "attempt", argv=[sys.executable, "-c",
            "import pathlib,time; "
            f"pathlib.Path({str(invocations)!r}).open('a').write('one\\n'); "
            "time.sleep(0.3); print('completed')"],
        cwd=tmp_path, environment={"PATH": "/usr/bin:/bin"}, timeout=5,
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: process.run(), range(2)))
    assert results[0] == results[1]
    assert results[0]["cleanup"] == "observed-native-confirmed"
    assert results[0]["exit_code"] == 0
    assert invocations.read_text().splitlines() == ["one"]
    assert Path(results[0]["log"]).read_text().splitlines() == ["completed"]


def test_concurrent_cancellation_is_one_atomic_signal(tmp_path, monkeypatch):
    from devflow_temporal.delivery_resources import write_private

    state = tmp_path / "runs" / "cancel"
    state.mkdir(parents=True, mode=0o700)
    spec = {
        "run_id": "cancel", "state_dir": str(state),
        "checkout": str(tmp_path / "checkout"), "policy_digest": "a" * 64,
        "policy": {"execution_backend": "native-macos"},
    }
    process = NativeProcess(
        spec, state / "attempt", argv=[sys.executable, "-V"], cwd=tmp_path,
        environment={"PATH": "/usr/bin:/bin"}, timeout=5, cancelled=lambda: True,
    )
    write_private(process.journal, {"intent": process._intent()})
    created, finish = threading.Event(), threading.Event()
    original = os.open

    def hold_first_signal(path, *args, **kwargs):
        descriptor = original(path, *args, **kwargs)
        if (Path(path).parent == process.folder
                and Path(path).name.startswith("cancel") and not created.is_set()):
            created.set()
            assert finish.wait(10), "signal creator was not released"
        return descriptor

    monkeypatch.setattr(os, "open", hold_first_signal)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(process._request_cancel)
        try:
            assert created.wait(10), "first cancellation did not create a signal"
            second = pool.submit(process._request_cancel)
            second.result(timeout=10)
        finally:
            finish.set()
        first.result(timeout=10)
