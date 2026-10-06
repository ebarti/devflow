from __future__ import annotations

import io
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import uvicorn

from devflow_temporal import delivery_control as control
from devflow_temporal.delivery_api import DeliveryService, create_app
from devflow_temporal.delivery_client import DeliveryClient, ServiceUnavailable, client
from devflow_temporal.delivery_config import DeliveryConfig


@pytest.fixture
def config(tmp_path, monkeypatch):
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
                "repositories": {"fixture": {"base_ref": "main"}},
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


def test_shared_client_ensures_service_without_token_file(config, monkeypatch):
    calls = []

    def ensure(current):
        calls.append("ensure")
        control._private_directory(current.state_root)

    def request(_caller, method, path, body=None, **_kwargs):
        calls.append((method, path, body))
        return {"csrf_token": "csrf"}

    monkeypatch.setattr(control, "ensure_service_running", ensure)
    monkeypatch.setattr(DeliveryClient, "_request", request)
    assert client(config.path).csrf == "csrf"
    assert calls == ["ensure", ("GET", "/api/session", None)]
    assert not (config.state_root / "service-token").exists()


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


def test_explicit_stop_waits_for_start_lock(config, monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    stopped = threading.Event()

    def start(current, _deadline):
        entered.set()
        assert release.wait(2)
        return _manifest(current)

    monkeypatch.setattr(control, "_start", start)
    monkeypatch.setattr(control, "_stop", lambda *_args: stopped.set())
    with ThreadPoolExecutor(max_workers=2) as pool:
        starting = pool.submit(control.service_start, config)
        assert entered.wait(1)
        stopping = pool.submit(control.service_stop, config)
        assert not stopped.wait(0.05)
        release.set()
        starting.result()
        stopping.result()
    assert stopped.is_set()


def test_partial_stack_recovery_stops_only_recorded_identity(config, monkeypatch):
    manifest = _manifest(config)
    manifest["processes"]["api"]["identity"] = "replaced"
    control._write_manifest(config, manifest["processes"])
    signals = []
    alive = {1, 2}
    monkeypatch.setattr(
        control, "_process_identity", lambda pid: "fixture" if pid in alive else None
    )
    monkeypatch.setattr(control.os, "waitpid", lambda *_args: (0, 0))
    monkeypatch.setattr(control.os, "getpgid", lambda pid: pid)

    def kill(pid, sig):
        signals.append((pid, sig))
        alive.remove(pid)

    monkeypatch.setattr(control.os, "killpg", kill)
    monkeypatch.setattr(control, "_start", lambda current, _deadline: _manifest(current))
    control.ensure_service_running(config)
    assert signals == [(2, signal.SIGTERM), (1, signal.SIGTERM)]


def test_stale_manifest_is_replaced_without_signalling(config, monkeypatch):
    _manifest(config)
    monkeypatch.setattr(control, "_process_identity", lambda _pid: None)
    monkeypatch.setattr(control.os, "waitpid", lambda *_args: (0, 0))
    monkeypatch.setattr(control.os, "killpg", lambda *_args: pytest.fail("stale PID was killed"))
    monkeypatch.setattr(control, "_start", lambda current, _deadline: _manifest(current))
    assert control.ensure_service_running(config)["config_path"] == str(config.path)


@pytest.mark.parametrize("status", [401, 409])
def test_http_failure_never_restarts_owned_stack(config, monkeypatch, status):
    _manifest(config)
    monkeypatch.setattr(control, "_owned", lambda _process: True)

    def login(_caller, **_kwargs):
        raise ValueError(f"service HTTP {status}: refused")

    monkeypatch.setattr(DeliveryClient, "login", login)
    monkeypatch.setattr(
        control, "_stop", lambda *_args: pytest.fail("HTTP error restarted service")
    )
    with pytest.raises(ValueError, match=f"service HTTP {status}"):
        control.ensure_service_running(config)


def test_existing_readiness_timeout_preserves_live_stack(config, monkeypatch):
    manifest = _manifest(config)
    monkeypatch.setattr(control, "_owned", lambda _process: True)

    def unavailable(_caller, **_kwargs):
        raise ServiceUnavailable("read timed out")

    monkeypatch.setattr(DeliveryClient, "login", unavailable)
    monkeypatch.setattr(control, "_stop", lambda *_args: pytest.fail("interrupted live stack"))
    monkeypatch.setattr(control, "_start", lambda *_args: pytest.fail("restarted live stack"))
    with pytest.raises(ValueError, match="timed out.*logs:"):
        control.ensure_service_running(
            DeliveryConfig(config.path, {**config.raw, "service_start_timeout": 0.1})
        )
    assert control._read_manifest(config) == manifest


def test_database_contention_readiness_preserves_live_stack(config, monkeypatch):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    config.path.write_text(json.dumps({**config.raw, "dashboard_url": f"http://127.0.0.1:{port}"}))
    config = DeliveryConfig.load(config.path)

    async def healthy(service):
        service.temporal_status = "connected"

    monkeypatch.setattr(DeliveryService, "healthy_client", healthy)
    app = create_app(config.path)
    manifest = _manifest(config)
    manifest["processes"]["api"]["pid"] = os.getpid()
    control._write_manifest(config, manifest["processes"])
    (config.state_root / "worker-ready.json").write_text(
        json.dumps({"pid": 2, "identity": "fixture", "config_path": str(config.path)})
    )
    monkeypatch.setattr(control, "_owned", lambda _process: True)
    monkeypatch.setattr(control, "_stop", lambda *_args: pytest.fail("interrupted live stack"))
    monkeypatch.setattr(control, "_start", lambda *_args: pytest.fail("restarted live stack"))
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, lifespan="off", log_level="error")
    )
    serving = threading.Thread(target=server.run)
    serving.start()
    try:
        deadline = time.monotonic() + 3
        while not server.started:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        # The real API's store connection waits behind a normal writer for longer
        # than _ready's two-second HTTP timeout, without losing process ownership.
        db = sqlite3.connect(config.tracking_db, check_same_thread=False)
        db.execute("BEGIN IMMEDIATE")

        def release():
            time.sleep(2.2)
            db.rollback()
            db.close()

        writer = threading.Thread(target=release)
        writer.start()
        try:
            result = control.ensure_service_running(config)
        finally:
            writer.join()
        assert result["processes"] == manifest["processes"]
        assert control._read_manifest(config) == manifest
    finally:
        server.should_exit = True
        serving.join(timeout=3)
        assert not serving.is_alive()


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


def test_client_renews_anonymous_csrf_before_each_command_without_a_token_file(config):
    class Transport:
        sessions = 0
        sent = []

        def open(self, request, **_kwargs):
            if request.method == "GET":
                assert request.full_url.endswith("/api/session")
                self.sessions += 1
                return io.BytesIO(json.dumps({"csrf_token": f"csrf-{self.sessions}"}).encode())
            self.sent.append(request.get_header("X-devflow-csrf"))
            return io.BytesIO(b'{}')

    caller = DeliveryClient(config)
    caller.opener = Transport()
    caller.submit({"command_id": "first"})
    caller.submit({"command_id": "second"})
    assert caller.opener.sent == ["csrf-1", "csrf-2"]
    assert not (config.state_root / "service-token").exists()


@pytest.mark.parametrize("status", [401, 409])
def test_request_http_failure_remains_application_error(config, status):
    class Refused:
        def open(self, request, **_kwargs):
            raise urllib.error.HTTPError(
                request.full_url, status, "refused", {}, io.BytesIO(b'{"detail":"conflict"}')
            )

    caller = DeliveryClient(config)
    caller.opener = Refused()
    with pytest.raises(ValueError, match=f"service HTTP {status}: conflict") as error:
        caller.runs()
    assert not isinstance(error.value, ServiceUnavailable)


def test_readiness_requires_owned_api_and_worker_registration(config, monkeypatch):
    manifest = _manifest(config)
    monkeypatch.setattr(control, "_owned", lambda _process: True)
    monkeypatch.setattr(DeliveryClient, "login", lambda *_args, **_kwargs: None)
    info = {"pid": 3, "temporal": "connected"}
    monkeypatch.setattr(DeliveryClient, "_request", lambda *_args, **_kwargs: info)
    assert not control._ready(config, manifest, time.monotonic() + 2)
    marker = config.state_root / "worker-ready.json"
    marker.write_text(
        json.dumps({"pid": 2, "identity": "fixture", "config_path": str(config.path)})
    )
    assert control._ready(config, manifest, time.monotonic() + 2)
    info["pid"] = 999
    assert not control._ready(config, manifest, time.monotonic() + 2)
    info.update(pid=3, temporal="disconnected")
    assert not control._ready(config, manifest, time.monotonic() + 2)


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
    monkeypatch.setattr(control, "DeliveryStore", lambda _config: None)
    monkeypatch.setattr(control.shutil, "which", lambda _name: sys.executable)
    with pytest.raises(ValueError, match="timed out.*logs:"):
        control.ensure_service_running(configured)
    assert launched
    assert all(not control._owned(process) for process in launched)
    assert not control._manifest(config).exists()


def test_cli_read_failure_is_concise_and_status_does_not_start(config, monkeypatch, capsys):
    monkeypatch.setattr(
        control, "read_only_client",
        lambda _config: (_ for _ in ()).throw(ServiceUnavailable("service unavailable"))
    )
    monkeypatch.setattr(sys, "argv", ["devflow-delivery", "--config", str(config.path), "runs"])
    with pytest.raises(SystemExit) as result:
        control.main()
    assert result.value.code == 1
    assert capsys.readouterr().err == "devflow-delivery: service unavailable\n"
    monkeypatch.setattr(sys, "argv", ["devflow-delivery", "--config", str(config.path), "status"])
    control.main()
    assert json.loads(capsys.readouterr().out)["processes"] == {}
    assert not config.state_root.exists()


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
