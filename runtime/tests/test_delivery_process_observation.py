from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from devflow_temporal import delivery_control as control
from devflow_temporal.delivery_config import DeliveryConfig


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
