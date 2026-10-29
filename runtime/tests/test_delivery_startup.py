from __future__ import annotations

import io
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest

from devflow_temporal import delivery_control as control
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_client import DeliveryClient, ServiceUnavailable
from devflow_temporal.delivery_config import DeliveryConfig


@pytest.fixture
def config(tmp_path, monkeypatch):
    # These lifecycle fixtures have no Temporal namespace; its admission gate
    # is exercised separately with authenticated RPC fixtures.
    monkeypatch.setattr(control, "_ensure_namespace_retention", lambda *_args: None)
    bundle = tmp_path / "dashboard.html"
    bundle.write_text("dashboard fixture")
    monkeypatch.setattr(control, "_dashboard_bundle", lambda: bundle)
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "tracking_db": str(tmp_path / "tracking.sqlite3"),
                "state_root": str(tmp_path / "state"),
                "helpers_dir": str(Path(__file__).resolve().parents[2] / "skills/devflow/scripts"),
                "codex_bin": "/usr/bin/true",
                "provider": "fake",
                "repositories": {"fixture": {"base_ref": "main", "github_repo": "example/fixture"}},
                "roles": {name: {} for name in ("implement", "review", "verify")},
            }
        )
    )
    return DeliveryConfig.load(path)


def _manifest(config):
    processes = {
        name: {"pid": index, "identity": "fixture"}
        for index, name in enumerate(("temporal", "worker", "api"), 1)
    }
    control._private_directory(config.state_root)
    control._write_manifest(config, processes)
    return {"config_path": str(config.path), "processes": processes}


def test_concurrent_cold_callers_and_explicit_start_converge(config, monkeypatch):
    launches = []

    def start(current, _deadline):
        launches.append(1)
        time.sleep(0.05)
        return _manifest(current)

    monkeypatch.setattr(control, "_start", start)
    monkeypatch.setattr(control, "_ready", lambda *_args: True)
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _index: control.service_start(config), range(6)))
    assert len(launches) == 1
    assert all(result["processes"] == results[0]["processes"] for result in results)
    control.ensure_service_running(config)
    assert len(launches) == 1
    assert (config.state_root / "service.lock").stat().st_mode & 0o777 == 0o600


def test_request_transport_failure_sends_mutation_once(config):
    class BrokenTransport:
        calls = 0

        def open(self, request, **_kwargs):
            if request.method == "GET":
                assert request.full_url.endswith("/api/session")
                return io.BytesIO(b'{"csrf_token":"anonymous-csrf"}')
            self.calls += 1
            assert request.method == "POST"
            assert request.get_header("X-devflow-csrf") == "anonymous-csrf"
            assert json.loads(request.data) == {"command_id": "stable-command"}
            raise urllib.error.URLError("connection lost after send")

    caller = DeliveryClient(config)
    caller.opener = BrokenTransport()
    with pytest.raises(ServiceUnavailable, match="connection lost after send"):
        caller.submit({"command_id": "stable-command"})
    assert caller.opener.calls == 1


def test_foreign_occupied_port_is_untouched(config, monkeypatch):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        configured = DeliveryConfig(
            config.path, {**config.raw, "dashboard_url": f"http://127.0.0.1:{port}"}
        )
        monkeypatch.setattr(
            control, "_launch", lambda *_args: pytest.fail("launched on foreign port")
        )
        with pytest.raises(ValueError, match=f"port {port} is already occupied"):
            control.ensure_service_running(configured)
        assert not control._free("127.0.0.1", port)


def test_startup_timeout_cleans_launched_processes(config, monkeypatch):
    configured = DeliveryConfig(config.path, {**config.raw, "service_start_timeout": 0.2})
    launched = []
    original_launch = control._launch

    def launch(current, name, _argv):
        process = original_launch(
            current, name, [sys.executable, "-c", "import time; time.sleep(30)"]
        )
        launched.append(process)
        return process

    monkeypatch.setattr(control, "_launch", launch)
    monkeypatch.setattr(control, "_wait_port", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(control, "_ready", lambda *_args: False)
    monkeypatch.setattr(control, "_free", lambda *_args: True)
    monkeypatch.setattr(control, "DeliveryStore",
                        lambda _config: SimpleNamespace(seed_execution_snapshots=dict))
    monkeypatch.setattr(control.shutil, "which", lambda _name: sys.executable)
    with pytest.raises(ValueError, match="timed out.*logs:"):
        control.ensure_service_running(configured)
    assert launched
    assert all(not control._owned(process) for process in launched)
    assert not control._manifest(config).exists()


def test_cross_process_lock_times_out_without_starting(config, monkeypatch):
    control._private_directory(config.state_root)
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import fcntl,sys,time; f=open(sys.argv[1],'w'); "
            "fcntl.flock(f,fcntl.LOCK_EX); print('locked',flush=True); time.sleep(30)",
            str(config.state_root / "service.lock"),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "locked"
        (config.state_root / "service.lock").chmod(0o600)
        monkeypatch.setattr(control, "_start", lambda *_args: pytest.fail("ignored process lock"))
        with pytest.raises(ValueError, match="timed out"):
            control.ensure_service_running(
                DeliveryConfig(config.path, {**config.raw, "service_start_timeout": 0.1})
            )
    finally:
        holder.kill()
        holder.wait()


def _api_info(current, manifest, _deadline=None):
    return {"pid": manifest["processes"]["api"]["pid"], "temporal": "connected",
            "config_digest": digest(current.raw),
            "transport_digest": control.transport_digest(current)}


def _register_worker(config, transport=None, pid=2):
    marker = {"pid": pid, "identity": "fixture", "config_path": str(config.path)}
    if transport is not None:
        marker["transport_digest"] = transport
    (config.state_root / "worker-ready.json").write_text(json.dumps(marker))
    return marker


def _edited(config, **changes):
    config.path.write_text(json.dumps({**json.loads(config.path.read_text()), **changes}))
    return DeliveryConfig.load(config.path)


def _fake_stack(config, monkeypatch):
    """Owned fixture PIDs 1-3; signals and launches are recorded, never performed."""
    alive = {1, 2, 3}
    signals, launched = [], []
    monkeypatch.setattr(control, "_process_identity",
                        lambda pid: "fixture" if pid in alive else None)
    monkeypatch.setattr(control, "_local_identity",
                        lambda pid, **_kwargs: "fixture" if pid in alive else None)
    monkeypatch.setattr(control.os, "waitpid", lambda *_args: (0, 0))
    monkeypatch.setattr(control.os, "getpgid", lambda pid: pid)

    def kill(pid, sig):
        signals.append((pid, sig))
        alive.discard(pid)

    def launch(current, name, argv):
        pid = 4 + len(launched)
        launched.append((name, argv[-1], current.dashboard_url))
        alive.add(pid)
        return {"pid": pid, "identity": "fixture"}

    def ready(current, manifest, _deadline=None):
        processes = manifest["processes"]
        if set(processes) != {"temporal", "worker", "api"} or any(
                process["pid"] not in alive for process in processes.values()):
            return None
        return _api_info(current, manifest)

    monkeypatch.setattr(control.os, "killpg", kill)
    monkeypatch.setattr(control, "_launch", launch)
    monkeypatch.setattr(control, "_free", lambda *_args: True)
    monkeypatch.setattr(control, "_ready", ready)
    return alive, signals, launched


@pytest.mark.parametrize("problem", ["hostname", "occupied"])
def test_restart_keeps_the_running_api_when_the_edited_one_cannot_start(config, monkeypatch,
                                                                         problem):
    manifest = _manifest(config)
    manifest["processes"]["api"]["dashboard_url"] = config.dashboard_url
    control._write_manifest(config, manifest["processes"])
    _register_worker(config, control.transport_digest(config))
    real_free = control._free
    _alive, signals, launched = _fake_stack(config, monkeypatch)
    monkeypatch.setattr(control, "_free", real_free)
    monkeypatch.setattr(control, "service_stop", lambda *_args: pytest.fail("stopped the stack"))
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        if problem == "occupied":
            sock.listen()
        address = {"hostname": f"http://localhost:{port}", "path": f"http://127.0.0.1:{port}/",
                   "occupied": f"http://127.0.0.1:{port}"}[problem]
        expected = {"hostname": "loopback HTTP address", "path": "only a loopback host and port",
                    "occupied": f"port {port} is already occupied"}[problem]
        # The file loads, but the API could not start on it: refuse before stopping anything.
        with pytest.raises(ValueError, match=expected):
            control.service_restart(_edited(config, dashboard_url=address))
    assert signals == [] and launched == []
    assert control._read_manifest(config)["processes"] == manifest["processes"]


@pytest.mark.parametrize("operation", ["observe", "stop"])
def test_external_controller_recognizes_exited_children_without_reaping_them(operation):
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                             start_new_session=True)
    try:
        receipt = {"pid": child.pid, "identity": control._process_identity(child.pid)}
        assert receipt["identity"]
        if operation == "observe":
            os.kill(child.pid, signal.SIGTERM)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                state = subprocess.run(["ps", "-p", str(child.pid), "-o", "stat="],
                                       capture_output=True, text=True, check=True).stdout.strip()
                if state.startswith("Z"):
                    break
                time.sleep(0.01)
            assert state.startswith("Z")
        observer = subprocess.run([
            sys.executable, "-B", "-c",
            "import json,sys; from devflow_temporal import delivery_control as c; "
            "p=json.loads(sys.argv[1]); "
            "print(json.dumps(c._owned(p) if sys.argv[2]=='observe' else c._terminate('api',p)))",
            json.dumps(receipt), operation,
        ], capture_output=True, text=True, timeout=12)
        assert observer.returncode == 0, observer.stderr
        assert json.loads(observer.stdout) == (False if operation == "observe" else "stopped")
        # Only this parent can reap the target; the controller merely observed its exit.
        assert child.wait(timeout=5) == -signal.SIGTERM
    finally:
        if child.poll() is None:
            child.kill()
        child.wait()


def test_external_stop_still_kills_live_descendants_of_a_zombie_leader(tmp_path):
    ready = tmp_path / "descendant-ready"
    descendant = (
        "import os,signal,time; from pathlib import Path; "
        "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
        f"Path({str(ready)!r}).write_text(str(os.getpid())); time.sleep(60)"
    )
    parent = subprocess.Popen([
        sys.executable, "-c",
        "import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',sys.argv[1]]); "
        "time.sleep(60)", descendant,
    ], start_new_session=True)
    try:
        receipt = {"pid": parent.pid, "identity": control._process_identity(parent.pid)}
        deadline = time.monotonic() + 5
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.exists()
        observer = subprocess.run([
            sys.executable, "-B", "-c",
            "import json,sys; from devflow_temporal import delivery_control as c; "
            "print(c._terminate('api',json.loads(sys.argv[1])))", json.dumps(receipt),
        ], capture_output=True, text=True, timeout=12)
        assert observer.returncode == 0, observer.stderr
        assert observer.stdout.strip() == "stopped"
        assert parent.wait(timeout=5) == -signal.SIGTERM
        state = subprocess.run(["ps", "-p", ready.read_text(), "-o", "stat="],
                               capture_output=True, text=True, check=False)
        assert state.returncode != 0 or state.stdout.strip().startswith("Z")
    finally:
        try:
            os.killpg(parent.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        parent.wait()
