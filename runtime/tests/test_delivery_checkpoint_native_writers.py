"""Owned native monitor/provider custody survives loss of its Python body."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
from test_delivery_api import api_fixture as api_fixture
from test_native_interrupted_gate import native_gates as native_gates

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_native_process import process_table, stop_observed
from devflow_temporal.delivery_resources import read_private
from devflow_temporal.delivery_writer_custody import (
    CheckpointWriters,
    native_custody,
    native_descriptors,
    shared,
)


def test_native_carrier_refuses_foreign_run_writable_or_closed_descriptors(native_gates):
    spec = native_gates.spec
    with shared(spec):
        carrier = native_custody(spec)
        assert native_descriptors(spec, carrier) == tuple(carrier["descriptors"])
        with pytest.raises(ValueError, match="admitted run"):
            native_descriptors(spec, {**carrier, "run_id": "unrelated-owned-fixture"})
        path = Path(spec["state_dir"]) / "workspace-writers.lock"
        writable = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
        try:
            with pytest.raises(ValueError, match="read-only witness"):
                native_descriptors(spec, {**carrier, "descriptors": [writable]})
        finally:
            os.close(writable)
    with pytest.raises((OSError, ValueError)):
        native_descriptors(spec, carrier)


@pytest.mark.asyncio
async def test_native_monitor_and_provider_keep_writer_witness_after_parent_loss(
    native_gates, tmp_path,
):
    g = native_gates
    folder = Path(g.spec["state_dir"]) / "owned-native-writer"
    provider_ready, release = tmp_path / "provider-ready", tmp_path / "provider-release"
    provider_code = """import fcntl,os,pathlib,sys,time
target=(int(sys.argv[3]),int(sys.argv[4]))
for fd in range(3,256):
    try:
        info=os.fstat(fd)
        if (info.st_dev,info.st_ino)==target:
            fcntl.flock(fd,fcntl.LOCK_UN)
    except OSError:
        pass
pathlib.Path(sys.argv[1]).touch()
while not pathlib.Path(sys.argv[2]).exists():
    time.sleep(.02)
print('1 passed')
"""
    request = tmp_path / "writer-native.json"
    request.write_text(json.dumps({"spec": g.spec, "folder": str(folder),
                                   "ready": str(provider_ready), "release": str(release),
                                   "provider_code": provider_code}))
    script = """import json,os,sys
from pathlib import Path
from devflow_temporal.delivery_native_process import NativeProcess
from devflow_temporal.delivery_writer_custody import shared
r=json.load(open(sys.argv[1]));spec=r['spec']
identity=json.load(open(Path(spec['state_dir'])/'workspace-owner.json'))['writer_custody']['identity']
with shared(spec):
    process=NativeProcess(spec,Path(r['folder']),
        argv=[sys.executable,'-B','-c',r['provider_code'],r['ready'],r['release'],
              str(identity['device']),str(identity['inode'])],
        cwd=Path(spec['checkout']),timeout=25,
        environment={'PATH':os.environ['PATH'],'HOME':str(Path(r['folder'])/'owned-home')})
    process.run()
"""
    errors = tmp_path / "body-stderr"
    with errors.open("w") as output:
        body = subprocess.Popen([sys.executable, "-B", "-c", script, str(request)],
                                stdout=subprocess.DEVNULL, stderr=output)
    observed = {}
    try:
        deadline = time.monotonic() + 25
        while not provider_ready.exists():
            assert body.poll() is None, errors.read_text()
            assert time.monotonic() < deadline, errors.read_text()
            await asyncio.sleep(.03)
        journal = read_private(folder / "native-process.json")
        observed = {**{int(pid): value for pid, value in journal["owned"].items()},
                    journal["monitor"]["pid"]: journal["monitor"]}
        assert journal["phase"] == "authorized"
        proof = journal["writer_custody"]
        assert proof["schema"] == "devflow-native-writer-proof-v2"
        assert proof["guard"] == {"kind": "independent-monitor-shared-v1",
                                  "identity": proof["binding"]["identity"]}
        assert proof["intent_sha256"] == digest(journal["intent"])
        assert proof["journal"] == str(folder / "native-process.json")
        assert proof["binding"] == journal["intent"]["writer_custody"]
        assert proof["binding"]["run_id"] == g.spec["run_id"]
        body.kill()
        body.wait(timeout=5)
        with pytest.raises(ValueError, match="writer.*owns"):
            with CheckpointWriters(g.store, g.spec):
                pytest.fail("detached native body lost the run-wide writer witness")
        release.touch()
        deadline = time.monotonic() + 25
        while True:
            try:
                with CheckpointWriters(g.store, g.spec):
                    break
            except ValueError:
                assert time.monotonic() < deadline
                await asyncio.sleep(.03)
        final = read_private(folder / "native-process.json")
        assert final["phase"] == "finished"
        assert final["monitoring_complete"] is True
        assert final["result"]["cleanup"] == "observed-native-confirmed"
    finally:
        release.touch()
        if body.poll() is None:
            body.kill()
            body.wait(timeout=5)
        # Stop only this fixture's recorded exact process identities if its
        # normal release could not complete; never search or kill by command name.
        live = process_table()
        remaining = {pid: value for pid, value in observed.items()
                     if live.get(pid, {}).get("identity") == value["identity"]}
        if remaining:
            await asyncio.to_thread(stop_observed, remaining)
