"""Real native monitor clocks with a private, controlled provider-child fixture."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from test_delivery_resources import spec

from devflow_temporal import delivery_native_process as native
from devflow_temporal.delivery_provider_capacity import WAIT_FILE
from devflow_temporal.delivery_resources import RunResources

CHILD = r'''
import os,sys,time
from pathlib import Path
from devflow_temporal.delivery_native_process import process_table
from devflow_temporal.delivery_provider_capacity import CapacityWait
from devflow_temporal.delivery_resources import read_private,write_private
folder=Path(sys.argv[1]); pid=os.getpid(); identity=process_table()[pid]['identity']
write_private(folder/'ready.json',{'pid':pid,'identity':identity})
assert sys.stdin.readline().strip()=='GO'
journal=read_private(folder/'native-process.json')
clock=CapacityWait(folder,journal['intent']['run_id'],pid,identity)
for _ in range(2):
    clock.begin(0,1)
    time.sleep(3 if sys.argv[2]=='abandoned' else 1.1)
    clock.finish()
(folder/'completed').touch()
'''


@pytest.mark.parametrize("mode", ["role", "abandoned"])
def test_monitor_excludes_only_owned_capacity_wait_and_keeps_cancellation(
    tmp_path, monkeypatch, mode,
):
    owned = spec(tmp_path)
    resources = RunResources(owned)
    scratch = resources.scratch("role", "capacity")
    folder = Path(owned["state_dir"]) / "attempt"
    popen = native.subprocess.Popen

    def controlled_child(argv, *args, **kwargs):
        # Only the provider-child fixture is substituted. The actual monitor,
        # private-file authentication, PID birth checks, clocks and teardown run.
        if argv[1:4] == ["-I", "-m", "devflow_temporal.native_child"]:
            argv = [sys.executable, "-I", "-c", CHILD, str(folder), mode]
        return popen(argv, *args, **kwargs)

    monkeypatch.setattr(native.subprocess, "Popen", controlled_child)
    argv = ([sys.executable, "-I", "-m", "devflow_temporal.role_runner",
             str(folder / "request.json")] if mode != "ordinary" else
            [sys.executable, "-c", "fixture ordinary command"])
    process = native.NativeProcess(
        owned, folder, argv=argv, cwd=scratch, environment={"PATH": "/usr/bin:/bin"},
        timeout=1, cancelled=lambda: mode == "cancel" and (folder / WAIT_FILE).exists())
    result = process._run()
    assert result["cleanup"] == "observed-native-confirmed"
    assert result["timed_out"] == (mode in {"ordinary", "abandoned"})
    assert result["cancelled"] == (mode == "cancel")
    assert (folder / "completed").exists() == (mode == "role")
    assert resources.finalize("blocked")["resource_cleanup"] == "confirmed"
