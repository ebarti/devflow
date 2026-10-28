"""Real thread/process lifetimes guard checkpoint acknowledgement and restoration."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from test_delivery_checkpoints import stopped as stopped
from test_delivery_store import service as service

from devflow_temporal.delivery_checkpoints import create_checkpoint, restore_checkpoint
from devflow_temporal.delivery_resources import read_private, write_private
from devflow_temporal.delivery_writer_custody import (
    NAME,
    CheckpointWriters,
    activity_custody,
    inherited_descriptors,
    native_custody,
    require_writer,
    shared,
    to_thread,
)


@pytest.mark.asyncio
async def test_supervisor_monitor_thread_keeps_custody_until_prelaunch_failure_is_joined(
    stopped, monkeypatch,
):
    from devflow_temporal.supervisor import DeliverySupervisor

    store, spec, broker, _row = stopped
    owner = DeliverySupervisor(store, capacity=1)
    request = {"spec": spec, "role": "implement", "iteration": 0,
               "workspace": str(broker.checkout), "candidate": broker.candidate()}
    key, prior = owner._claim(request)
    assert prior is None
    entered, release = threading.Event(), threading.Event()

    def fail_preparation(actual):
        # Exercise the real supervisor executor hop, then stop before any
        # provider launch. This is a failed preparation, never a fake verdict.
        assert native_custody(actual) is not None
        entered.set()
        assert release.wait(10)
        raise ValueError("owned fixture preparation refused before provider launch")

    monkeypatch.setattr("devflow_temporal.delivery_preparation.verify_prepared_spec",
                        fail_preparation)
    pending = asyncio.create_task(owner._run_native(request, key))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        pending.cancel()
        await asyncio.sleep(0)
        assert not pending.done(), "cancelled supervisor abandoned its actual executor"
        with pytest.raises(ValueError, match="writer.*owns"):
            with CheckpointWriters(store, spec):
                pytest.fail("cancelled supervisor released its running executor custody")
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await pending
    with store._connect() as db:
        attempt = dict(db.execute("SELECT * FROM delivery_attempts WHERE job_key=?",
                                  (key,)).fetchone())
    assert attempt["state"] == "finished" and attempt["cleanup"] == "confirmed"
    assert json.loads(attempt["result_json"])["finish_reason"] == "prelaunch"
    assert not (Path(spec["state_dir"]) / "attempts" / key / "native-process.json").exists()
    with CheckpointWriters(store, spec):
        pass


def test_child_unlock_cannot_release_the_actual_body_private_lease(stopped, tmp_path):
    store, spec, _broker, _row = stopped
    ready, release = tmp_path / "child-unlocked", tmp_path / "child-release"
    child = None
    with shared(spec) as lease:
        exported = inherited_descriptors()
        assert lease.descriptor not in exported
        script = """import fcntl,os,sys,time
from pathlib import Path
for value in sys.argv[3:]: fcntl.flock(int(value),fcntl.LOCK_UN)
Path(sys.argv[1]).touch()
while not Path(sys.argv[2]).exists(): time.sleep(.01)
"""
        child = subprocess.Popen([sys.executable, "-B", "-c", script, str(ready), str(release),
                                  *map(str, exported)], pass_fds=exported)
        try:
            deadline = time.monotonic() + 5
            while not ready.exists():
                assert child.poll() is None
                assert time.monotonic() < deadline
                time.sleep(.01)
            with pytest.raises(ValueError, match="writer.*owns"):
                with CheckpointWriters(store, spec):
                    pytest.fail("child unlocked the trusted actual body description")
        finally:
            release.touch()
            child.wait(timeout=5)
    with CheckpointWriters(store, spec):
        pass


@pytest.mark.asyncio
async def test_inline_async_activity_custody_excludes_checkpoint_during_await(stopped):
    store, spec, _broker, row = stopped
    async with activity_custody(spec):
        await asyncio.sleep(0)
        with pytest.raises(ValueError, match="writer.*owns"):
            create_checkpoint(store, spec["run_id"], "inline-still-running", row["revision"])
    assert create_checkpoint(store, spec["run_id"], "inline-finished", row["revision"])["resumable"]


def test_readonly_body_cannot_promote_or_mutate_direct_workspace_entry(stopped):
    store, spec, broker, _row = stopped
    with shared(spec, read_only=True):
        for mutation in (lambda: require_writer(spec), broker.prepare,
                         lambda: broker.gate_checkout("review", 1, broker.candidate())):
            with pytest.raises(ValueError, match="read-only writer custody"):
                mutation()


def test_nested_readonly_scope_cannot_use_outer_writer_authority(stopped):
    from devflow_temporal.delivery_writer_custody import (
        reconciliation_read_only,
        require_current_writer,
    )

    _store, spec, _broker, _row = stopped
    with shared(spec):
        require_current_writer()
        with shared(spec, read_only=True):
            assert reconciliation_read_only(spec)
            with pytest.raises(ValueError, match="read-only writer custody"):
                require_current_writer()
            with pytest.raises(ValueError, match="read-only writer custody"):
                require_writer(spec)
        require_current_writer()


def test_shared_merge_witness_serves_two_exact_run_custodies(stopped, tmp_path, monkeypatch):
    from types import SimpleNamespace

    from devflow_temporal.delivery_checkpoint_merge_lease import SharedCheckpointMergeLease
    from devflow_temporal.delivery_feature_execution import registry_path

    store, spec, _broker, _row = stopped
    from devflow_temporal.delivery_broker import DeliveryBroker

    store.submit({"run_id": "run-2", "command_id": "owned-second-participant",
                  "work_id": "work-2", "issue_url": spec["issue_url"].rsplit("/", 1)[0] + "/4",
                  "repository_key": spec["repository_key"], "base_ref": spec["base_ref"],
                  "goal": spec["goal"], "accepted_plan": spec["accepted_plan"],
                  "authorized_endpoint": spec["authorized_endpoint"],
                  "branch": "feat/owned-second-participant"})
    second_spec = store.effective_spec("run-2")
    DeliveryBroker(store, second_spec)  # Independent source owner; no worktree needed.
    # The fixture owns this installation witness; unrelated runtime locks are untouched.
    path = tmp_path / "owned-installation" / "installation-recovery.lock"
    path.parent.mkdir(mode=0o700)
    path.touch(mode=0o600)
    original = registry_path
    monkeypatch.setattr("devflow_temporal.delivery_feature_execution.registry_path",
                        lambda config: path.parent / "registry.sqlite3"
                        if config is store.config else original(config))
    monkeypatch.setattr("devflow_temporal.delivery_checkpoint_merge_lease.registry_path",
                        lambda config: path.parent / "registry.sqlite3"
                        if config is store.config else original(config))
    with store._connect() as db:
        for item in (spec, second_spec):
            db.execute("INSERT INTO delivery_merge_requests "
                       "(run_id,command_id,grant_json,workflow_id,phase,created_at,updated_at) "
                       "VALUES (?,?,'{}',?,'queued','now','now')",
                       (item["run_id"], "owned-merge-" + item["run_id"],
                        "owned-workflow-" + item["run_id"]))
    with SharedCheckpointMergeLease(store.config) as lease:
        with CheckpointWriters(store, spec, shared_merge_lease=lease) as first:
            with CheckpointWriters(store, second_spec, shared_merge_lease=lease) as second:
                with store._connect() as db:
                    first.verify(db)
                    second.verify(db)
    with pytest.raises(ValueError, match="not active"):
        with CheckpointWriters(store, spec, shared_merge_lease=lease):
            pass
    with pytest.raises(ValueError, match="authenticated owner"):
        CheckpointWriters(store, spec, shared_merge_lease=SimpleNamespace(verify=lambda _: None))


@pytest.mark.parametrize("damage", ["missing", "replaced", "old_receipt"])
def test_observation_never_creates_missing_or_legacy_writer_proof(stopped, damage):
    store, spec, _broker, row = stopped
    witness = Path(spec["state_dir"]) / NAME
    if damage == "old_receipt":
        path = Path(spec["state_dir"]) / "workspace-owner.json"
        receipt = read_private(path)
        receipt.pop("writer_custody")
        write_private(path, receipt)
    else:
        witness.unlink()
        if damage == "replaced":
            witness.write_bytes(b"")
            witness.chmod(0o600)
    with pytest.raises((ValueError, FileNotFoundError), match="witness|No such file"):
        create_checkpoint(store, spec["run_id"], "save", row["revision"])
    if damage == "missing":
        assert not witness.exists()
    assert not list((Path(spec["state_dir"]) / "checkpoints").glob("*/manifest.json"))


@pytest.mark.asyncio
async def test_cancelled_waiter_keeps_actual_body_excluded_until_thread_finishes(stopped):
    store, spec, _broker, row = stopped
    entered, release, done = threading.Event(), threading.Event(), threading.Event()

    def body():
        entered.set()
        try:
            assert release.wait(10)
        finally:
            done.set()

    task = asyncio.create_task(to_thread(spec, body))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(ValueError, match="writer.*owns"):
            create_checkpoint(store, spec["run_id"], "save", row["revision"])
    finally:
        release.set()
        assert await asyncio.to_thread(done.wait, 5)
    # The body is done before this ACK. The original budget and run owner remain.
    receipt = create_checkpoint(store, spec["run_id"], "save", row["revision"])
    assert receipt["resumable"] is True


@pytest.mark.asyncio
async def test_cancelled_pool_queue_entry_never_runs_after_checkpoint_ack(stopped):
    store, spec, _broker, row = stopped
    loop = asyncio.get_running_loop()
    prior = loop._default_executor
    executor = ThreadPoolExecutor(max_workers=1)
    loop.set_default_executor(executor)
    occupied, release, executed = threading.Event(), threading.Event(), threading.Event()

    def occupied_body():
        occupied.set()
        release.wait(10)

    blocker = loop.run_in_executor(None, occupied_body)
    try:
        assert occupied.wait(5)
        pending = asyncio.create_task(to_thread(spec, executed.set))
        await asyncio.sleep(0)  # The wrapper acquires its lease before pool submission.
        with pytest.raises(ValueError, match="writer.*owns"):
            create_checkpoint(store, spec["run_id"], "save", row["revision"])
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        receipt = create_checkpoint(store, spec["run_id"], "save", row["revision"])
        assert receipt["resumable"]
        release.set()
        await blocker
        await loop.run_in_executor(None, lambda: None)
        assert not executed.is_set()
    finally:
        release.set()
        await blocker
        loop._default_executor = prior
        executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_subprocess_retains_run_witness_after_actual_python_owner_dies(stopped, tmp_path):
    store, spec, _broker, row = stopped
    request = tmp_path / "spec.json"
    request.write_text(json.dumps(spec))
    child_pid = tmp_path / "child-pid"
    release = tmp_path / "release"
    script = """import json,os,subprocess,sys
from devflow_temporal.delivery_writer_custody import shared,inherited_descriptors
spec=json.load(open(sys.argv[1]))
child_code='import pathlib,sys,time; p=pathlib.Path(sys.argv[1]); '\
           'exec("while not p.exists(): time.sleep(.02)")'
with shared(spec):
    child=subprocess.Popen([sys.executable,'-B','-c',child_code,sys.argv[3]],
                           pass_fds=inherited_descriptors())
    open(sys.argv[2],'w').write(str(child.pid))
    os._exit(0)
"""
    parent = subprocess.Popen([sys.executable, "-B", "-c", script, str(request),
                               str(child_pid), str(release)],
                              stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    pid = None
    try:
        assert parent.wait(timeout=10) == 0, parent.stderr.read().decode()
        pid = int(child_pid.read_text())
        with pytest.raises(ValueError, match="writer.*owns"):
            create_checkpoint(store, spec["run_id"], "save", row["revision"])
        release.touch()
        deadline = time.monotonic() + 10
        while True:
            try:
                with CheckpointWriters(store, spec):
                    break
            except ValueError:
                assert time.monotonic() < deadline
                await asyncio.sleep(.02)
        assert create_checkpoint(store, spec["run_id"], "save", row["revision"])["resumable"]
    finally:
        release.touch()
        if parent.poll() is None:
            parent.kill()
            parent.wait(timeout=5)
        if pid is not None:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass


def test_restore_cannot_overlap_a_late_actual_writer(stopped):
    import shutil

    store, spec, broker, row = stopped
    receipt = create_checkpoint(store, spec["run_id"], "save", row["revision"])
    shutil.rmtree(broker.checkout)
    with shared(spec):
        with pytest.raises(ValueError, match="writer.*owns"):
            restore_checkpoint(store, spec["run_id"], receipt["checkpoint_id"],
                               "restore", row["revision"])
    assert not broker.checkout.exists()
    assert restore_checkpoint(store, spec["run_id"], receipt["checkpoint_id"],
                              "restore", row["revision"])["restored"]


def test_renewal_anchor_is_preserved_and_cannot_be_recreated_by_restore(stopped):
    import shutil

    store, spec, broker, row = stopped
    with store._connect() as db:
        db.execute("INSERT INTO delivery_native_renewal_anchors VALUES (?,?,?,?)",
                   (spec["run_id"], "owned-renewal", "a" * 64, "b" * 64))
    receipt = create_checkpoint(store, spec["run_id"], "save", row["revision"])
    manifest = read_private(Path(spec["state_dir"]) / "checkpoints" /
                            receipt["checkpoint_id"] / "manifest.json")
    assert len(manifest["frozen"]["delivery_native_renewal_anchors"]) == 1
    with store._connect() as db:
        db.execute("DELETE FROM delivery_native_renewal_anchors WHERE run_id=?", (spec["run_id"],))
    shutil.rmtree(broker.checkout)
    with pytest.raises(ValueError, match="durable run, budget or effects changed"):
        restore_checkpoint(store, spec["run_id"], receipt["checkpoint_id"],
                           "restore", row["revision"])
    with store._connect() as db:
        assert db.execute("SELECT 1 FROM delivery_native_renewal_anchors").fetchone() is None
