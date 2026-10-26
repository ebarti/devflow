from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from devflow_temporal import delivery_control as control
from devflow_temporal import supervisor
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.supervisor import ProcessObservationUnknown


@pytest.mark.parametrize("failure", ["nonzero", "empty", "warning", "timeout", "oserror"])
def test_unavailable_primary_observation_never_proves_a_live_process_absent(monkeypatch, failure):
    probes = []

    def inspect(_argv, **kwargs):
        assert kwargs["timeout"] == 5
        if failure == "timeout":
            raise subprocess.TimeoutExpired("ps", 5)
        if failure == "oserror":
            raise OSError("process inspection unavailable")
        return SimpleNamespace(
            returncode=1 if failure == "nonzero" else 0,
            stdout="matching-start" if failure == "warning" else "",
            stderr="inspection incomplete" if failure in {"nonzero", "warning"} else "",
        )

    monkeypatch.setattr(supervisor.subprocess, "run", inspect)
    monkeypatch.setattr(supervisor.os, "kill", lambda pid, sig: probes.append((pid, sig)))
    with pytest.raises(ProcessObservationUnknown, match="ownership remains unknown"):
        supervisor._process_identity(7001, canonical=True, require_observation=True)
    assert probes == [(7001, 0)]


def test_failed_observer_needs_independent_absence_not_permission_denial(monkeypatch):
    monkeypatch.setattr(supervisor.subprocess, "run", lambda *_args, **_kwargs: SimpleNamespace(
        returncode=1, stdout="", stderr="inspection unavailable"))

    def absent(_pid, _signal):
        raise ProcessLookupError()

    monkeypatch.setattr(supervisor.os, "kill", absent)
    assert supervisor._process_identity(7001, require_observation=True) is None

    def denied(_pid, _signal):
        raise PermissionError()

    monkeypatch.setattr(supervisor.os, "kill", denied)
    with pytest.raises(ProcessObservationUnknown):
        supervisor._process_identity(7001, require_observation=True)


def test_legacy_identity_fallback_does_not_convert_observation_failure_to_replacement(monkeypatch):
    observed = []

    def identity(_pid, **kwargs):
        observed.append(kwargs)
        if kwargs.get("canonical"):
            return "canonical-start"
        raise ProcessObservationUnknown("legacy observation unavailable")

    monkeypatch.setattr(control, "_local_identity", identity)
    monkeypatch.setattr(control.os, "waitpid", lambda *_args: (0, 0))
    monkeypatch.setattr(control.subprocess, "run", lambda *_args, **_kwargs: SimpleNamespace(
        returncode=0, stdout="S"))
    with pytest.raises(ProcessObservationUnknown, match="legacy observation unavailable"):
        control._owned({"pid": 7001, "identity": "legacy-start"})
    assert observed == [
        {"canonical": True, "require_observation": True},
        {"require_observation": True},
    ]


@pytest.mark.parametrize("observation", ["unknown", "foreign_generation"])
def test_unverified_or_foreign_generation_is_never_reaped(monkeypatch, observation):
    def identity(_pid):
        if observation == "unknown":
            raise ProcessObservationUnknown("identity observation unavailable")
        return None

    monkeypatch.setattr(control, "_process_identity", identity)
    monkeypatch.setattr(control, "_local_identity", lambda *_args, **_kwargs: "foreign-start")
    monkeypatch.setattr(control.os, "waitpid", lambda *_args: pytest.fail("reaped unverified PID"))
    if observation == "unknown":
        with pytest.raises(ProcessObservationUnknown):
            control._owned({"pid": 7001, "identity": "recorded-start"})
    else:
        assert not control._owned({"pid": 7001, "identity": "recorded-start"})


def _configuration(tmp_path: Path) -> DeliveryConfig:
    path = tmp_path / "config.json"
    value = {
        "version": 1,
        "tracking_db": str(tmp_path / "tracking.sqlite3"),
        "state_root": str(tmp_path / "state"),
        "helpers_dir": str(Path(__file__).resolve().parents[2] / "skills/devflow/scripts"),
        "codex_bin": "/usr/bin/false",
        "provider": "fake",
        "repositories": {"fixture": {"base_ref": "main", "github_repo": "example/fixture"}},
        "roles": {name: {} for name in ("implement", "review", "verify")},
    }
    path.write_text(json.dumps(value))
    path.chmod(0o600)
    config = DeliveryConfig.load(path)
    control._private_directory(config.state_root)
    return config


@pytest.mark.parametrize("failure_stage", ["during_member_stop", "after_member_stop"])
def test_unknown_after_partial_stop_retains_inventory_and_supervisor_hold(
    tmp_path, monkeypatch, failure_stage,
):
    config = _configuration(tmp_path)
    processes = {name: {"pid": pid, "identity": "matching-start"}
                 for name, pid in (("api", 7001), ("worker", 7002))}
    control._write_manifest(config, processes)
    before = control._manifest(config).read_bytes()
    worker_marker = config.state_root / "worker-ready.json"
    worker_marker.write_text("retained registration")
    signals = []
    stopped = set()

    def identity(pid):
        if (pid == 7002 or (failure_stage == "during_member_stop" and pid in stopped)):
            raise ProcessObservationUnknown("member observation unavailable")
        return None if pid in stopped else "matching-start"

    def terminate(pid, sig):
        signals.append((pid, sig))
        stopped.add(pid)

    monkeypatch.setattr(control, "_process_identity", identity)
    monkeypatch.setattr(control.os, "waitpid", lambda *_args: (0, 0))
    monkeypatch.setattr(control.os, "getpgid", lambda pid: pid)
    monkeypatch.setattr(control.os, "killpg", terminate)
    with pytest.raises(control.ServiceObservationUnknown, match="stop is pending") as pending:
        control.service_stop(config)
    assert pending.value.results["worker"] == "observation_unknown"
    assert pending.value.results["api"] == (
        "observation_unknown" if failure_stage == "during_member_stop" else "stopped")
    assert signals == [(7001, signal.SIGTERM)]
    assert control._manifest(config).read_bytes() == before
    assert worker_marker.read_text() == "retained registration"
    intent = json.loads(control._stopped_marker(config).read_text())
    assert intent["command"] == "stop" and "requested_at" in intent and "stopped_at" not in intent
    monkeypatch.setattr(control, "_start", lambda *_args, **_kwargs: pytest.fail("restarted"))
    assert control.ensure_service_running(config, unless_stopped=True)["stopped"]


def _cli(config: DeliveryConfig, command: str, *, env: dict[str, str] | None = None):
    return subprocess.run(
        [sys.executable, "-B", "-m", "devflow_temporal.delivery_control",
         "--config", str(config.path), command],
        env=env or os.environ, text=True, capture_output=True, timeout=15,
    )


@pytest.mark.parametrize("recorded_identity", ["matching", "replaced"])
def test_public_stop_retains_unknown_live_member_then_recovers(tmp_path, recorded_identity):
    config = _configuration(tmp_path)
    child = subprocess.Popen(
        [sys.executable, "-B", "-c", "import time; time.sleep(60)"],
        start_new_session=True, stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1",
           "PYTHONPATH": str(Path(control.__file__).resolve().parents[1])}
    try:
        identity = control._process_identity(child.pid)
        assert identity
        if recorded_identity == "replaced":
            identity = "Thu Jan  1 00:00:00 1970"
        process = {"pid": child.pid, "identity": identity,
                   "log": str(config.state_root / "api.log")}
        control._write_manifest(config, {"api": process})
        manifest = control._manifest(config)
        original = manifest.read_bytes()
        worker_marker = config.state_root / "worker-ready.json"
        worker_marker.write_text("retained registration")
        initial = _cli(config, "status", env=env)
        assert initial.returncode == 0, initial.stderr
        assert json.loads(initial.stdout)["processes"]["api"]["running"] == (
            recorded_identity == "matching")

        adapter = tmp_path / "bin"
        adapter.mkdir()
        ps = adapter / "ps"
        real_ps = "/bin/ps" if Path("/bin/ps").is_file() else "/usr/bin/ps"
        ps.write_text(
            '#!/bin/sh\ncase "$*" in\n'
            '  *lstart=*) echo "fixture: identity observation unavailable" >&2; exit 1 ;;\n'
            f'  *) exec {real_ps} "$@" ;;\nesac\n'
        )
        ps.chmod(0o700)
        unavailable = {**env, "PATH": str(adapter) + os.pathsep + os.environ["PATH"]}
        refused = _cli(config, "stop", env=unavailable)
        assert refused.returncode == 1
        assert "ownership remains unknown" in refused.stderr
        assert child.poll() is None
        assert manifest.read_bytes() == original
        assert worker_marker.read_text() == "retained registration"
        assert json.loads(control._stopped_marker(config).read_text())["command"] == "stop"
        status = _cli(config, "status", env=unavailable)
        assert status.returncode == 1 and "ownership remains unknown" in status.stderr
        assert json.loads(status.stdout)["processes"]["api"]["running"] is None
        assert json.loads(refused.stdout) == {"api": "observation_unknown"}
        restored = _cli(config, "status", env=env)
        assert restored.returncode == 0, restored.stderr
        assert json.loads(restored.stdout)["processes"]["api"]["pid"] == child.pid
        assert json.loads(restored.stdout)["processes"]["api"]["running"] == (
            recorded_identity == "matching")

        stopped = _cli(config, "stop", env=env)
        assert stopped.returncode == 0, stopped.stderr
        assert json.loads(stopped.stdout)["api"] == (
            "stopped" if recorded_identity == "matching" else "already_stopped_or_changed")
        assert not manifest.exists() and not worker_marker.exists()
        if recorded_identity == "matching":
            assert child.wait(timeout=5) == -signal.SIGTERM
        else:
            assert child.poll() is None
        assert json.loads(_cli(config, "status", env=env).stdout)["processes"] == {}
    finally:
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGKILL)
        child.wait(timeout=5)


def test_public_mixed_stack_reports_unknown_member_and_stops_later_known_members(tmp_path):
    config = _configuration(tmp_path)
    children = {name: subprocess.Popen(
        [sys.executable, "-B", "-c", "import time; time.sleep(60)"],
        start_new_session=True, stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    ) for name in ("api", "worker", "temporal")}
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1",
           "PYTHONPATH": str(Path(control.__file__).resolve().parents[1])}
    try:
        processes = {name: {"pid": child.pid, "identity": control._process_identity(child.pid)}
                     for name, child in children.items()}
        control._write_manifest(config, processes)
        original = control._manifest(config).read_bytes()
        marker = config.state_root / "worker-ready.json"
        marker.write_text("retained registration")
        adapter = tmp_path / "bin"
        adapter.mkdir()
        ps = adapter / "ps"
        real_ps = "/bin/ps" if Path("/bin/ps").is_file() else "/usr/bin/ps"
        ps.write_text(
            '#!/bin/sh\ncase "$*" in\n'
            f'  "-p {children["api"].pid} -o lstart=") '
            'echo "fixture: api identity observation unavailable" >&2; exit 1 ;;\n'
            f'  *) exec {real_ps} "$@" ;;\nesac\n'
        )
        ps.chmod(0o700)
        unavailable = {**env, "PATH": str(adapter) + os.pathsep + os.environ["PATH"]}
        status = _cli(config, "status", env=unavailable)
        assert status.returncode == 1, status.stderr
        members = json.loads(status.stdout)["processes"]
        assert members["api"]["running"] is None
        assert members["api"]["observation"] == "unknown"
        assert members["worker"]["running"] is True
        assert members["temporal"]["running"] is True
        stopped = _cli(config, "stop", env=unavailable)
        assert stopped.returncode == 1, stopped.stderr
        assert json.loads(stopped.stdout) == {
            "api": "observation_unknown", "worker": "stopped", "temporal": "stopped",
        }
        assert children["api"].poll() is None
        assert children["worker"].wait(timeout=5) == -signal.SIGTERM
        assert children["temporal"].wait(timeout=5) == -signal.SIGTERM
        assert control._manifest(config).read_bytes() == original
        assert marker.read_text() == "retained registration"
        assert control._stopped_marker(config).exists()
        status = _cli(config, "status", env=unavailable)
        assert status.returncode == 1
        members = json.loads(status.stdout)["processes"]
        assert members["api"]["running"] is None
        assert members["worker"]["running"] is False
        assert members["temporal"]["running"] is False
        reconciled = _cli(config, "stop", env=env)
        assert reconciled.returncode == 0, reconciled.stderr
        assert json.loads(reconciled.stdout) == {
            "api": "stopped", "worker": "already_stopped_or_changed",
            "temporal": "already_stopped_or_changed",
        }
        assert children["api"].wait(timeout=5) == -signal.SIGTERM
        assert not control._manifest(config).exists() and not marker.exists()
    finally:
        for child in children.values():
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=5)


@pytest.mark.parametrize("leader_exits_first", [False, True])
def test_direct_parent_keeps_zombie_identity_until_live_descendants_settle(
    tmp_path, leader_exits_first,
):
    ready = tmp_path / "descendant.pid"
    release = tmp_path / "release"
    descendant = (
        "import os,signal,time; from pathlib import Path; "
        "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
        f"Path({str(ready)!r}).write_text(str(os.getpid())); time.sleep(60)"
    )
    leader = subprocess.Popen([
        sys.executable, "-B", "-c",
        "import subprocess,sys,time; from pathlib import Path; "
        "subprocess.Popen([sys.executable,'-B','-c',sys.argv[1]]); "
        "release=Path(sys.argv[2]); "
        "exec('while not release.exists(): time.sleep(.01)')",
        descendant, str(release),
    ], start_new_session=True)
    try:
        receipt = {"pid": leader.pid, "identity": control._process_identity(leader.pid)}
        assert receipt["identity"]
        deadline = time.monotonic() + 5
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        assert ready.exists()
        if leader_exits_first:
            release.touch()
            while time.monotonic() < deadline:
                state = subprocess.run(["ps", "-p", str(leader.pid), "-o", "stat="],
                                       capture_output=True, text=True, timeout=5)
                if state.stdout.strip().startswith("Z"):
                    break
                time.sleep(.01)
            assert state.stdout.strip().startswith("Z")
        assert control._owned(receipt)
        assert control._terminate("fixture", receipt) == "stopped"
        leader.wait(timeout=5)
        assert not control._owned(receipt)
        state = subprocess.run(["ps", "-p", ready.read_text(), "-o", "stat="],
                               capture_output=True, text=True, timeout=5)
        assert state.returncode != 0 or state.stdout.strip().startswith("Z")
    finally:
        try:
            os.killpg(leader.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        leader.wait(timeout=5)
