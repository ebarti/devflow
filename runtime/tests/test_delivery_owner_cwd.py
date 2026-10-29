from __future__ import annotations

import json
import sys
import time
from types import SimpleNamespace

from devflow_temporal import delivery_control as control


def test_owner_launch_survives_removal_of_disposable_caller_cwd(tmp_path, monkeypatch):
    state = tmp_path / "durable-state"
    state.mkdir(mode=0o700)
    caller = tmp_path / "disposable-caller"
    caller.mkdir()
    child = """
import json
import os
import sys
import time
from pathlib import Path

state = Path(sys.argv[1])
(state / "ready").write_text(os.getcwd())
while not (state / "caller-removed").exists():
    time.sleep(0.01)
try:
    result = {"cwd": os.getcwd()}
    Path("relative-output.txt").write_text("resumed")
except OSError as error:
    result = {"error": type(error).__name__}
(state / "resumed.json").write_text(json.dumps(result))
while True:
    time.sleep(1)
"""
    process = None

    def wait_for(path):
        deadline = time.monotonic() + 5
        while not path.exists() and time.monotonic() < deadline:
            assert control._owned(process), "owned child exited before its receipt"
            time.sleep(0.05)
        assert path.exists(), "owned child did not write its receipt"

    try:
        monkeypatch.chdir(caller)
        process = control._launch(
            SimpleNamespace(state_root=state, tracking_db=state / "tracking.sqlite3"),
            "cwd-probe",
            [sys.executable, "-B", "-c", child, str(state)],
        )
        wait_for(state / "ready")
        monkeypatch.chdir(tmp_path)
        caller.rmdir()
        assert not caller.exists()
        (state / "caller-removed").touch()
        wait_for(state / "resumed.json")
        assert control._owned(process)
        assert json.loads((state / "resumed.json").read_text()) == {"cwd": str(state)}
        assert (state / "relative-output.txt").read_text() == "resumed"
    finally:
        monkeypatch.chdir(tmp_path)
        if process is not None:
            assert control._terminate("cwd-probe", process) in {
                "stopped", "already_stopped", "already_stopped_or_changed"
            }
            assert not control._owned(process)
