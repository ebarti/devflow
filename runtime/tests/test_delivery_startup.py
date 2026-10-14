from __future__ import annotations

import http.server
import io
import json
import os
import signal
import socket
import socketserver
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest
import uvicorn

from devflow_temporal import delivery_control as control
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_api import DeliveryService, create_app
from devflow_temporal.delivery_client import DeliveryClient, ServiceUnavailable, client
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


@pytest.mark.parametrize("dispatched_failure", [False, True])
def test_feature_continuation_uses_extended_timeout_without_transport_replay(
    config, dispatched_failure
):
    payload = {"command_id": "one-feature-continuation", "expected_revision": 5}
    response = {"run_id": "recorded-successor"}

    class RecordedTransport:
        calls = []

        def open(self, request, *, timeout):
            self.calls.append((request.method, request.full_url, timeout))
            assert timeout == 180
            if request.method == "GET":
                assert request.full_url.endswith("/api/session")
                return io.BytesIO(b'{"csrf_token":"continuation-csrf"}')
            assert request.method == "POST"
            assert request.full_url.endswith("/api/runs/feature%2Fowner/continue-feature")
            assert request.get_header("X-devflow-csrf") == "continuation-csrf"
            assert request.get_header("Origin") == config.dashboard_url.rstrip("/")
            assert json.loads(request.data) == payload
            if dispatched_failure:
                raise TimeoutError("response lost after continuation dispatch")
            return io.BytesIO(json.dumps(response).encode())

    caller = DeliveryClient(config)
    caller.opener = RecordedTransport()
    if dispatched_failure:
        with pytest.raises(ServiceUnavailable, match="after continuation dispatch"):
            caller.continue_feature("feature/owner", payload)
    else:
        assert caller.continue_feature("feature/owner", payload) == response
    assert [method for method, _url, _timeout in caller.opener.calls] == ["GET", "POST"]


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
    # A worker that registered the transport it polls is the same registration.
    info.update(temporal="connected")
    marker.write_text(json.dumps({"pid": 2, "identity": "fixture", "config_path": str(config.path),
                                  "transport_digest": control.transport_digest(config)}))
    assert control._ready(config, manifest, time.monotonic() + 2)
    assert control._registered_worker(config, manifest["processes"])["transport_digest"] == (
        control.transport_digest(config))


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


def test_ensure_service_running_never_reloads_a_stale_api(config, monkeypatch):
    # A transport edit must not split the API from the worker it dispatches to.
    manifest = _manifest(config)
    _register_worker(config, control.transport_digest(config))
    _alive, signals, launched = _fake_stack(config, monkeypatch)
    monkeypatch.setattr(control, "_stop", lambda *_args: pytest.fail("stopped the stack"))
    monkeypatch.setattr(control, "_start",
                        lambda *_args, **_kwargs: pytest.fail("restarted the stack"))
    for edited in (_edited(config, queue="moved-queue"), _edited(config, capacity=3)):
        assert control.ensure_service_running(edited)["processes"] == manifest["processes"]
    assert signals == [] and launched == []


def test_restart_validates_the_edited_configuration_before_stopping_the_api(config, monkeypatch):
    manifest = _manifest(config)
    _register_worker(config, control.transport_digest(config))
    _alive, signals, launched = _fake_stack(config, monkeypatch)
    monkeypatch.setattr(control, "service_stop", lambda *_args: pytest.fail("stopped the stack"))
    monkeypatch.setattr(control, "_stop", lambda *_args: pytest.fail("stopped the stack"))
    broken = _edited(config, helpers_dir="/nonexistent/typo/skills/devflow/scripts")
    with pytest.raises(ValueError, match="state helper is missing"):
        control.service_restart(broken)
    assert signals == [] and launched == []
    assert control._read_manifest(config)["processes"] == manifest["processes"]
    # Ensuring with the broken file leaves the running stack alone as well.
    assert control.ensure_service_running(broken)["processes"] == manifest["processes"]
    assert signals == []


@pytest.mark.parametrize("api_state", ["absent", "dead"])
def test_ensure_service_running_relaunches_only_a_missing_api(config, monkeypatch, api_state):
    manifest = _manifest(config)
    processes = dict(manifest["processes"])
    if api_state == "absent":
        processes.pop("api")
    control._write_manifest(config, processes)
    _register_worker(config, control.transport_digest(config))
    alive, signals, launched = _fake_stack(config, monkeypatch)
    if api_state == "dead":
        alive.discard(3)
    monkeypatch.setattr(control, "_stop", lambda *_args: pytest.fail("stopped the worker"))
    monkeypatch.setattr(control, "_start",
                        lambda *_args, **_kwargs: pytest.fail("restarted the stack"))
    result = control.ensure_service_running(config)
    assert signals == []
    assert [name for name, *_ in launched] == ["api"]
    assert result["processes"]["api"]["pid"] == 4
    assert result["processes"]["api"]["dashboard_url"] == config.dashboard_url
    assert result["processes"]["worker"] == manifest["processes"]["worker"]
    assert control._read_manifest(config)["processes"] == result["processes"]


def test_failed_api_relaunch_never_stops_a_registered_worker(config, monkeypatch):
    manifest = _manifest(config)
    processes = dict(manifest["processes"])
    processes.pop("api")
    control._write_manifest(config, processes)
    _register_worker(config, control.transport_digest(config))
    _alive, signals, launched = _fake_stack(config, monkeypatch)
    # The relaunched API exits at once: its PID never becomes owned.
    monkeypatch.setattr(control, "_launch", lambda *_args: (
        launched.append(("api",)) or {"pid": 4, "identity": "fixture"}))
    monkeypatch.setattr(control, "_stop", lambda *_args: pytest.fail("stopped the worker"))
    monkeypatch.setattr(control, "_start",
                        lambda *_args, **_kwargs: pytest.fail("restarted the stack"))
    for _ in range(2):
        with pytest.raises(ValueError, match="api exited during service startup"):
            control.ensure_service_running(config)
    assert signals == [] and len(launched) == 2
    assert control._read_manifest(config)["processes"]["worker"] == manifest["processes"]["worker"]


def test_restart_reloads_the_api_member_without_signalling_the_worker(config, monkeypatch):
    original_launch = control._launch

    def launch(current, name, _argv):
        return original_launch(
            current, name, [sys.executable, "-c", "import time; time.sleep(30)"])

    control._private_directory(config.state_root)
    processes = {"temporal": {"pid": 1, "identity": "fixture"},
                 "worker": {"pid": 2, "identity": "fixture"}}
    processes["api"] = launch(config, "api", [])
    control._write_manifest(config, processes)
    _register_worker(config, control.transport_digest(config))
    real_identity = control._process_identity
    monkeypatch.setattr(control, "_process_identity",
                        lambda pid: "fixture" if pid in {1, 2} else real_identity(pid))
    monkeypatch.setattr(control, "_launch", launch)
    monkeypatch.setattr(control, "_free", lambda *_args: True)
    monkeypatch.setattr(control, "_stop", lambda *_args: pytest.fail("signalled the worker"))
    monkeypatch.setattr(control, "_start",
                        lambda *_args, **_kwargs: pytest.fail("restarted the stack"))
    monkeypatch.setattr(control, "_ready", _api_info)
    old = processes["api"]
    try:
        # A non-transport edit: the worker's registered transport still matches.
        result = control.service_restart(_edited(config, capacity=5))
        new = result["processes"]["api"]
        assert result["restarted"] == ["api"]
        assert new["pid"] != old["pid"]
        assert new["dashboard_url"] == config.dashboard_url
        assert not control._owned(old)
        assert control._owned(new)
        assert result["processes"]["worker"] == processes["worker"]
        assert control._read_manifest(config)["processes"]["api"] == new
    finally:
        for pid in {old["pid"], control._read_manifest(config)["processes"]["api"]["pid"]}:
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


@pytest.mark.parametrize("api_state", ["ready", "dead"])
@pytest.mark.parametrize("registered", ["other-transport", "unrecorded"])
def test_restart_after_a_transport_change_takes_the_ordinary_stop_and_start(
    config, monkeypatch, registered, api_state,
):
    manifest = _manifest(config)
    calls = []
    # A dead API beside a worker on another transport is not relaunched alone either.
    monkeypatch.setattr(control, "_owned",
                        lambda process: api_state == "ready" or process.get("pid") != 3)
    monkeypatch.setattr(control, "_ready",
                        _api_info if api_state == "ready" else lambda *_args: None)
    if registered == "other-transport":
        # The worker registered the transport it polls; the edited file names another.
        _register_worker(config, control.transport_digest(config))
        edited = _edited(config, queue="moved-queue")
    else:
        # A worker that predates transport recording cannot prove it matches.
        _register_worker(config)
        edited = config
    monkeypatch.setattr(control, "_restart_api",
                        lambda *_args: pytest.fail("reloaded only the API"))
    monkeypatch.setattr(control, "service_stop", lambda _current: calls.append("stop") or {})
    monkeypatch.setattr(control, "ensure_service_running", lambda _current: (
        calls.append("start") or {"processes": manifest["processes"]}))
    assert control.service_restart(edited)["restarted"] == ["temporal", "worker", "api"]
    assert calls == ["stop", "start"]


def test_restart_after_a_dashboard_move_reaches_the_api_where_it_listens(config, monkeypatch):
    ports = []
    for _ in range(2):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            ports.append(sock.getsockname()[1])
    old = _edited(config, dashboard_url=f"http://127.0.0.1:{ports[0]}", service_start_timeout=5)
    manifest = _manifest(old)
    manifest["processes"]["api"]["dashboard_url"] = old.dashboard_url
    control._write_manifest(old, manifest["processes"])
    _register_worker(old, control.transport_digest(old))
    monkeypatch.setattr(control, "_owned", lambda _process: True)
    monkeypatch.setattr(control.os, "killpg", lambda *_args: pytest.fail("signalled"))
    info = {"pid": 3, "temporal": "connected"}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            body = json.dumps({"csrf_token": "x"} if self.path == "/api/session"
                              else info).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    class FixtureHTTPServer(http.server.ThreadingHTTPServer):
        def server_bind(self):
            # This numeric loopback fixture needs no reverse DNS resolution.
            socketserver.TCPServer.server_bind(self)
            self.server_name, self.server_port = self.server_address[:2]

    server = FixtureHTTPServer(("127.0.0.1", ports[0]), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    reloaded = []
    monkeypatch.setattr(control, "_restart_api", lambda current, *_args: (
        reloaded.append(current.dashboard_url) or {"processes": manifest["processes"]}))
    try:
        moved = _edited(config, dashboard_url=f"http://127.0.0.1:{ports[1]}")
        # The running API is probed where it listens; the relaunch gets the new address.
        assert control.service_restart(moved)["restarted"] == ["api"]
        assert reloaded == [moved.dashboard_url]
        # Ensuring names the move instead of probing an address nothing serves.
        with pytest.raises(ValueError, match="restart` to move it to"):
            control.ensure_service_running(moved)
    finally:
        server.shutdown()


def test_start_reports_seeded_snapshots(config, monkeypatch):
    report = {"published": "0" * 64, "unavailable": [{"run_id": "run-1", "reason": "fixture"}]}
    monkeypatch.setattr(control, "DeliveryStore",
                        lambda _config: SimpleNamespace(seed_execution_snapshots=lambda: report))
    monkeypatch.setattr(control, "_launch", lambda _config, name, _argv: {
        "pid": {"temporal": 1, "worker": 2, "api": 3}[name], "identity": "fixture"})
    monkeypatch.setattr(control, "_owned", lambda _process: True)
    monkeypatch.setattr(control, "_wait_port", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(control, "_ready", lambda *_args: True)
    monkeypatch.setattr(control, "_free", lambda *_args: True)
    monkeypatch.setattr(control.shutil, "which", lambda _name: sys.executable)
    assert control.ensure_service_running(config)["configuration_snapshots"] == report


@pytest.mark.parametrize("transport", ["equal", "edited"])
def test_ensure_relaunches_a_dead_api_only_onto_the_worker_transport(config, monkeypatch,
                                                                     transport):
    _manifest(config)
    # The worker registered the transport it polls; the API then died.
    _register_worker(config, control.transport_digest(config))
    alive, signals, launched = _fake_stack(config, monkeypatch)
    alive.discard(3)
    monkeypatch.setattr(control, "_stop", lambda *_args: pytest.fail("stopped the worker"))
    monkeypatch.setattr(control, "_start",
                        lambda *_args, **_kwargs: pytest.fail("restarted the stack"))
    if transport == "edited":
        # A pending transport edit: relaunching the API on it would split the stack.
        with pytest.raises(ValueError, match="restart` to move the whole service"):
            control.ensure_service_running(_edited(config, queue="moved-queue"))
        assert launched == [] and signals == []
        assert set(control._read_manifest(config)["processes"]) == {"temporal", "worker", "api"}
    else:
        result = control.ensure_service_running(config)
        assert [(name, url) for name, _argv, url in launched] == [("api", config.dashboard_url)]
        assert result["processes"]["api"]["pid"] == 4 and signals == []


@pytest.mark.parametrize("problem", ["hostname", "path", "occupied"])
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


def test_restart_after_an_api_that_exits_brings_back_only_the_api(config, monkeypatch):
    _manifest(config)
    _register_worker(config, control.transport_digest(config))
    alive, signals, launched = _fake_stack(config, monkeypatch)
    attempts = []

    def launch(_config, name, _argv):
        # The first relaunched API exits at once; the next one stays up.
        pid = 4 + len(attempts)
        attempts.append((name, pid))
        if len(attempts) > 1:
            alive.add(pid)
        return {"pid": pid, "identity": "fixture"}

    monkeypatch.setattr(control, "_launch", launch)
    monkeypatch.setattr(control, "service_stop", lambda *_args: pytest.fail("stopped the stack"))
    monkeypatch.setattr(control, "_stop", lambda *_args: pytest.fail("stopped the worker"))
    with pytest.raises(ValueError, match="api exited during service startup"):
        control.service_restart(config)
    assert signals == [(3, signal.SIGTERM)]
    # The obvious second restart relaunches only the API; the worker is never signalled.
    result = control.service_restart(config)
    assert result["restarted"] == ["api"]
    assert result["processes"]["api"]["pid"] == 5
    assert signals == [(3, signal.SIGTERM)] and launched == []


def test_service_lock_mode_drift_heals_but_a_symlink_never_does(config):
    control._private_directory(config.state_root)
    lock = config.state_root / "service.lock"
    lock.touch()
    lock.chmod(0o644)
    assert control.service_stop(config) == {}
    assert lock.stat().st_mode & 0o777 == 0o600
    lock.unlink()
    lock.symlink_to(config.path)
    with pytest.raises(ValueError, match="owned private file"):
        control.service_stop(config)


def test_lifecycle_identity_is_zone_independent_and_accepts_legacy_manifests(monkeypatch):
    from devflow_temporal.supervisor import _process_identity as local_identity

    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                             start_new_session=True)
    try:
        monkeypatch.setenv("TZ", "UTC")
        canonical = control._process_identity(child.pid)
        local = local_identity(child.pid)
        assert canonical and local
        monkeypatch.setenv("TZ", "Asia/Tokyo")
        # The recorded form does not move with the host's zone; the local one does.
        assert control._process_identity(child.pid) == canonical
        assert local_identity(child.pid) != local
        monkeypatch.setenv("TZ", "UTC")
        # Manifests written before identities were canonical still prove ownership.
        assert control._owned({"pid": child.pid, "identity": canonical})
        assert control._owned({"pid": child.pid, "identity": local})
        assert not control._owned({"pid": child.pid, "identity": "Thu Jan  1 00:00:00 1970"})
    finally:
        child.kill()
        child.wait()


def test_adjudication_binds_service_members_recorded_in_canonical_form(monkeypatch):
    """The controller binding reads real lstart output on a non-UTC host."""
    from devflow_temporal.delivery_investigation_adjudication import _service_member
    from devflow_temporal.delivery_native_process import process_table

    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                             start_new_session=True)
    try:
        monkeypatch.setenv("TZ", "Europe/Madrid")
        canonical = control._process_identity(child.pid)
        table = process_table()
        # The table prints the host's local form; the manifest holds the canonical one.
        assert table[child.pid]["identity"] != canonical
        assert _service_member(table, {"pid": child.pid, "identity": canonical})
        assert _service_member(table, {"pid": child.pid,
                                       "identity": table[child.pid]["identity"]})
        assert not _service_member(table, {"pid": child.pid,
                                           "identity": "Thu Jan  1 00:00:00 1970"})
        assert not _service_member(table, {"pid": 2 ** 22 + 7, "identity": canonical})
    finally:
        child.kill()
        child.wait()
