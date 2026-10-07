from __future__ import annotations

import concurrent.futures
import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from devflow_temporal.delivery_native_process import NativeProcess, process_table
from devflow_temporal.delivery_resources import RunResources, read_private


def test_private_read_reopens_atomically_replaced_inode(tmp_path, monkeypatch):
    from devflow_temporal import delivery_resources

    path = tmp_path / 'journal.json'
    replacement = tmp_path / 'replacement.json'
    for target, value in [(path, 'old'), (replacement, 'current')]:
        target.write_text(json.dumps({'generation': value}))
        target.chmod(0o600)
    original_lstat, original_fstat = Path.lstat, os.fstat
    replaced = False

    def replace(info):
        nonlocal replaced
        if not replaced and stat.S_ISREG(info.st_mode):
            replaced = True
            os.replace(replacement, path)
            fields = list(info)
            fields[3] = 0  # The opened old inode was unlinked by atomic publication.
            return os.stat_result(fields)
        return info

    monkeypatch.setattr(Path, 'lstat', lambda self: replace(original_lstat(self)))
    monkeypatch.setattr(os, 'fstat', lambda fd: replace(original_fstat(fd)))
    assert delivery_resources.read_private(path) == {'generation': 'current'}
    assert replaced


def test_private_read_uses_the_validated_descriptor(tmp_path, monkeypatch):
    path = tmp_path / 'journal.json'
    replacement = tmp_path / 'replacement.json'
    path.write_text(json.dumps({'generation': 'validated'}))
    path.chmod(0o600)
    replacement.write_text(json.dumps({'generation': 'unvalidated'}))
    original_lstat, original_fstat = Path.lstat, os.fstat
    replaced = False

    def replace(info):
        nonlocal replaced
        if not replaced and stat.S_ISREG(info.st_mode):
            replaced = True
            os.replace(replacement, path)
        return info

    monkeypatch.setattr(Path, 'lstat', lambda self: replace(original_lstat(self)))
    monkeypatch.setattr(os, 'fstat', lambda fd: replace(original_fstat(fd)))
    assert read_private(path) == {'generation': 'validated'}


@pytest.mark.parametrize('violation', ['mode', 'symlink', 'hardlink', 'fifo', 'directory'])
def test_private_read_still_rejects_unowned_evidence(tmp_path, violation):
    path = tmp_path / 'journal.json'
    path.write_text('{}')
    path.chmod(0o600)
    if violation == 'mode':
        path.chmod(0o644)
    elif violation == 'symlink':
        original = tmp_path / 'original.json'
        path.rename(original)
        path.symlink_to(original)
    elif violation == 'hardlink':
        os.link(path, tmp_path / 'second.json')
    elif violation == 'fifo':
        path.unlink()
        os.mkfifo(path, mode=0o600)
    else:
        path.unlink()
        path.mkdir(mode=0o700)
    with pytest.raises(ValueError, match='private owned file'):
        read_private(path)


def test_private_read_preserves_snapshots_during_actual_atomic_publication(tmp_path):
    from devflow_temporal.delivery_resources import write_private

    path = tmp_path / 'journal.json'
    write_private(path, {'generation': 0})

    def publish():
        for generation in range(1, 201):
            write_private(path, {'generation': generation})

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        writer = executor.submit(publish)
        reads = 0
        while not writer.done() or reads < 1000:
            snapshot = read_private(path)
            assert set(snapshot) == {'generation'}
            assert 0 <= snapshot['generation'] <= 200
            reads += 1
        writer.result()
    assert read_private(path) == {'generation': 200}


def test_private_read_preserves_an_authenticated_snapshot_under_repeated_replacement(
    tmp_path, monkeypatch,
):
    path = tmp_path / 'journal.json'
    path.write_text(json.dumps({'generation': 0}))
    path.chmod(0o600)
    replacements = []
    for generation in range(1, 13):
        replacement = tmp_path / f'publication-{generation}.json'
        replacement.write_text(json.dumps({'generation': generation}))
        replacement.chmod(0o600)
        replacements.append(replacement)
    original_fstat = os.fstat
    published = 0

    def publish_before_stat(fd):
        nonlocal published
        # Real publications unlink the opened inode at every observation boundary.
        for _ in range(4):
            os.replace(replacements[published], path)
            published += 1
        return original_fstat(fd)

    monkeypatch.setattr(os, 'fstat', publish_before_stat)
    snapshot = read_private(path)
    assert published > 3
    assert 0 <= snapshot['generation'] < published
    assert json.loads(path.read_text()) == {'generation': published}


@pytest.mark.parametrize('violation', ['mode', 'symlink', 'hardlink', 'owner'])
def test_private_read_rechecks_replacement_security(tmp_path, monkeypatch, violation):
    path = tmp_path / 'journal.json'
    replacement = tmp_path / 'replacement.json'
    path.write_text('{}')
    path.chmod(0o600)
    replacement.write_text('{}')
    replacement.chmod(0o600)
    original_fstat = os.fstat
    replaced = False

    def replace(fd):
        nonlocal replaced
        info = original_fstat(fd)
        if not replaced:
            replaced = True
            os.replace(replacement, path)
            if violation == 'mode':
                path.chmod(0o644)
            elif violation == 'symlink':
                path.rename(replacement)
                path.symlink_to(replacement)
            elif violation == 'hardlink':
                os.link(path, replacement)
            fields = list(info)
            fields[3] = 0
            return os.stat_result(fields)
        if violation == 'owner':
            fields = list(info)
            fields[4] = os.getuid() + 1
            return os.stat_result(fields)
        return info

    monkeypatch.setattr(os, 'fstat', replace)
    with pytest.raises(ValueError, match='private owned file'):
        read_private(path)

def spec(root: Path, name: str = "run") -> dict:
    state = root / "runs" / name
    state.mkdir(parents=True, mode=0o700)
    return {
        "run_id": name,
        "state_dir": str(state),
        "checkout": str(root / "checkouts" / name),
        "source_path": str(root / "source"),
        "base_sha": "a" * 40,
        "branch": "fix/fixture",
        "policy_digest": "a" * 64,
        "policy": {"execution_backend": "native-macos"},
    }


def test_plan_environment_has_real_broker_ownership_and_safe_cleanup(tmp_path):
    from test_delivery_store import _git

    from devflow_temporal.delivery_broker import DeliveryBroker

    owned = spec(tmp_path)
    owned['policy']['host_sandbox'] = 'trusted-local'
    owned['accepted_plan'] = json.dumps({'verification': ['Run test_owned.py']})
    root = Path(owned['checkout'])
    root.parent.mkdir(parents=True)
    resources = RunResources(owned)
    resources.register(root, 'checkout')
    root.mkdir()
    resources.created(root)
    project = root / 'worker'
    (project / 'tests').mkdir(parents=True)
    (project / 'tests/test_owned.py').write_text('def test_owned(): assert True\n')
    (project / 'pyproject.toml').write_text('[project]\nname="fixture"\nversion="1"\n')
    (project / 'uv.lock').write_text('version=1\n')
    _git(root, 'init', '-q')
    _git(root, 'add', '.')
    broker = DeliveryBroker.__new__(DeliveryBroker)
    broker.spec = owned
    outputs = broker._register_generated(root, ['worker/.venv'])
    assert outputs == [project / '.venv']
    (project / '.venv').mkdir()
    (project / '.venv/package').write_text('owned fixture dependency')
    broker._record_generated(outputs)
    for other in ['.venv', 'outside/.venv', '../foreign', '/tmp/foreign']:
        with pytest.raises(ValueError):
            broker._register_generated(root, [other])
    sentinel = tmp_path / 'precious'
    sentinel.write_text('SAFE')
    assert resources.finalize('blocked')['state'] == 'confirmed'
    assert not (project / '.venv').exists()
    assert sentinel.read_text() == 'SAFE'
    assert resources.finalize('blocked')['state'] == 'confirmed'


@pytest.mark.parametrize(
    "outcome", ["delivered", "cancelled", "blocked", "preparation_error", "timeout"]
)
def test_terminal_finalization_removes_transients_and_preserves_durable_evidence(tmp_path, outcome):
    owned = spec(tmp_path)
    resources = RunResources(owned)
    scratch = resources.scratch("role", "implement")
    (scratch / "scratch.txt").write_text("temporary")
    evidence = Path(owned["state_dir"]) / "role-result.json"
    evidence.write_text("DURABLE")
    sentinel = tmp_path / "unrelated"
    sentinel.write_text("SAFE")
    result = resources.finalize(outcome)
    assert result["resource_cleanup"] == "confirmed"
    assert result["roots"][0]["state"] == "removed"
    assert not (Path(owned["state_dir"]) / "transient").exists()
    assert evidence.read_text() == "DURABLE"
    assert sentinel.read_text() == "SAFE"
    retry = resources.finalize(outcome)
    assert retry["roots"][0]["state"] == "already_absent"
    assert read_private(Path(retry["receipt"]))["state"] == "confirmed"


def test_replaced_root_fails_closed_and_nested_symlink_never_follows_sentinel(tmp_path):
    owned = spec(tmp_path)
    resources = RunResources(owned)
    scratch = resources.scratch("check", "one")
    sentinel = tmp_path / "outside"
    sentinel.mkdir()
    (sentinel / "precious").write_text("SAFE")
    (scratch / "link").symlink_to(sentinel, target_is_directory=True)
    assert resources.finalize("cancelled")["state"] == "confirmed"
    assert (sentinel / "precious").read_text() == "SAFE"
    second = spec(tmp_path, "second")
    other = RunResources(second)
    other.scratch("check", "two")
    root = Path(second["state_dir"]) / "transient"
    root.rename(root.with_name("original"))
    root.symlink_to(sentinel, target_is_directory=True)
    result = other.finalize("cancelled")
    assert result["state"] == "unknown"
    assert result["roots"][0]["state"] == "failed_unknown"
    assert (sentinel / "precious").read_text() == "SAFE"


def test_concurrent_independent_roots_and_restart_after_removal(tmp_path, monkeypatch):
    specs = [spec(tmp_path, name) for name in ("one", "two")]
    owners = [RunResources(value) for value in specs]
    for owner in owners:
        owner.scratch("check", "one").joinpath("file").write_text("temp")
    import devflow_temporal.delivery_resources as module

    real = module.remove_directory
    first = True

    def interrupted(path, identity):
        nonlocal first
        real(path, identity)
        if first:
            first = False
            raise KeyboardInterrupt("worker lost after removal, before final receipt")

    monkeypatch.setattr(module, "remove_directory", interrupted)
    with pytest.raises(KeyboardInterrupt):
        owners[0].finalize("cancelled")
    monkeypatch.setattr(module, "remove_directory", real)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda owner: owner.finalize("cancelled"), owners))
    assert all(item["state"] == "confirmed" for item in results)
    assert not any((Path(value["state_dir"]) / "transient").exists() for value in specs)


def test_unstarted_existing_native_journal_remains_owned_for_finalization(tmp_path):
    from devflow_temporal.delivery_resources import private_directory, write_private

    owned = spec(tmp_path)
    resources = RunResources(owned)
    scratch = resources.scratch("check", "earlier")
    folder = Path(owned["state_dir"]) / "attempt"
    private_directory(folder)
    journal = folder / "native-process.json"
    write_private(journal, {"phase": "authorized", "owned": {}, "ports": [],
                            "monitoring_complete": False})
    NativeProcess(
        owned, folder, argv=[sys.executable, "-c", "raise AssertionError('must not launch')"],
        cwd=scratch, environment={"PATH": "/usr/bin:/bin"}, timeout=5,
    )
    manifest = read_private(resources.manifest)
    assert manifest["processes"] == [str(journal)]
    cleanup = resources.finalize("cancelled")
    assert cleanup["process_cleanup"] == "unknown"
    assert scratch.exists()


def test_real_observed_detached_child_is_stopped_and_journal_replays_without_effect(tmp_path):
    owned = spec(tmp_path)
    resources = RunResources(owned)
    scratch = resources.scratch("role", "one")
    marker = scratch / "child-pid"
    effect = scratch / "effect"
    child_code = "import time; time.sleep(30)"
    script = (
        "import pathlib,subprocess,sys,time; "
        f"pathlib.Path({str(effect)!r}).write_text('one invocation'); "
        f"child=subprocess.Popen([sys.executable,'-c',{child_code!r}],start_new_session=True); "
        f"pathlib.Path({str(marker)!r}).write_text(str(child.pid)); time.sleep(0.8)"
    )
    process = NativeProcess(
        owned,
        Path(owned["state_dir"]) / "attempt",
        argv=[sys.executable, "-c", script],
        cwd=scratch,
        environment={"PATH": "/usr/bin:/bin"},
        timeout=5,
    )
    first = process.run()
    child_pid = int(marker.read_text())
    assert child_pid in first["observed_owned_pids"]
    assert first["cleanup"] == "observed-native-confirmed"
    entry = process_table().get(child_pid)
    assert entry is None or entry["stat"].startswith("Z")
    assert process.run() == first
    assert effect.read_text() == "one invocation"
    final = resources.finalize("delivered")
    assert final["resource_cleanup"] == "confirmed"
    assert not scratch.exists()
    assert Path(first["log"]).is_file()


def test_interrupted_native_monitor_does_not_relaunch_and_retains_recovery_material(tmp_path):
    owned = spec(tmp_path)
    resources = RunResources(owned)
    scratch = resources.scratch("role", "one")
    process = NativeProcess(
        owned,
        Path(owned["state_dir"]) / "attempt",
        argv=[sys.executable, "-c", "raise RuntimeError('must never launch')"],
        cwd=scratch,
        environment={"PATH": "/usr/bin:/bin"},
        timeout=5,
    )
    from devflow_temporal.delivery_resources import write_private

    write_private(
        process.journal,
        {
            "phase": "authorized",
            "owned": {},
            "ports": [],
            "monitoring_complete": False,
            "intent": {
                "run_id": owned["run_id"],
                "policy_digest": owned["policy_digest"],
                "argv": process.argv,
                "cwd": str(scratch),
                "environment_sha256": __import__(
                    "devflow_temporal.contracts", fromlist=["digest"]
                ).digest(process.environment),
                "timeout": 5,
                "ports": [],
            },
        },
    )
    assert process.run()["cleanup"] == "unknown"
    assert not (process.folder / "launch.json").exists()
    result = resources.finalize("blocked")
    assert result["resource_cleanup"] == "unknown"
    assert result["roots"][0]["state"] == "retained"
    assert scratch.exists()


def owned_worktree(tmp_path):
    owned = spec(tmp_path)
    source = Path(owned["source_path"])
    source.mkdir()
    for args in (
        ["init"],
        ["config", "user.name", "Fixture"],
        ["config", "user.email", "fixture@example.test"],
    ):
        subprocess.run(["git", "-C", str(source), *args], check=True, capture_output=True)
    (source / "file").write_text("base")
    subprocess.run(["git", "-C", str(source), "add", "."], check=True)
    subprocess.run(
        ["git", "-C", str(source), "commit", "-m", "base"], check=True, capture_output=True
    )
    owned["base_sha"] = subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True
    ).strip()
    checkout = Path(owned["checkout"])
    checkout.parent.mkdir()
    resources = RunResources(owned)
    resources.register(checkout, "checkout")
    subprocess.run(
        ["git", "-C", str(source), "worktree", "add", "-b", owned["branch"], str(checkout)],
        check=True,
        capture_output=True,
    )
    resources.created(checkout)
    return owned, resources, source, checkout


def test_dirty_and_blocked_worktrees_are_retained_with_reason(tmp_path):
    _owned, resources, _source, checkout = owned_worktree(tmp_path)
    (checkout / "file").write_text("dirty")
    result = resources.finalize("cancelled")
    assert result["roots"][0]["state"] == "retained"
    assert "dirty" in result["roots"][0]["reason"]
    assert (checkout / "file").read_text() == "dirty"
    (checkout / "file").write_text("base")
    result = resources.finalize("blocked")
    assert "blocked-run" in result["roots"][0]["reason"]
    assert checkout.exists()
    assert resources.finalize("cancelled")["roots"][0]["state"] == "removed"
    assert not checkout.exists()


def test_unpushed_then_published_worktree_requires_remote_and_no_live_user(tmp_path):
    owned, resources, source, checkout = owned_worktree(tmp_path)
    (checkout / "file").write_text("candidate")
    subprocess.run(
        ["git", "-C", str(checkout), "commit", "-am", "candidate"], check=True, capture_output=True
    )
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(source), "remote", "add", "origin", str(remote)], check=True)
    result = resources.finalize("delivered")
    assert result["roots"][0]["state"] == "retained"
    assert "unpushed" in result["roots"][0]["reason"] and checkout.exists()
    subprocess.run(
        ["git", "-C", str(checkout), "push", "origin", owned["branch"]],
        check=True,
        capture_output=True,
    )
    if sys.platform == "darwin":
        import time

        ready = tmp_path / "user-ready"
        user = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import pathlib,sys,time; "
                "pathlib.Path(sys.argv[1]).write_text('ready'); time.sleep(30)",
                str(ready),
            ],
            cwd=checkout,
        )
        try:
            deadline = time.monotonic() + 5
            while not ready.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert ready.exists()
            result = resources.finalize("delivered")
            assert result["roots"][0]["state"] == "retained"
            assert "live process" in result["roots"][0]["reason"]
            assert user.poll() is None
        finally:
            user.terminate()
            user.wait(timeout=5)
    assert resources.finalize("delivered")["roots"][0]["state"] == "removed"
    assert not checkout.exists() and (source / "file").read_text() == "base"


@pytest.mark.skipif(sys.platform != "darwin", reason="actual macOS port identity inspection")
def test_unrelated_process_and_port_survive_owned_timeout_and_cleanup(tmp_path):
    import socket

    from devflow_temporal.delivery_native_process import stop_observed

    owned = spec(tmp_path)
    resources = RunResources(owned)
    scratch = resources.scratch("check", "deadline")
    unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        table = process_table()
        # A recycled/mismatched identity must never be signalled.
        assert stop_observed({unrelated.pid: {**table[unrelated.pid], "identity": "different"}})
        assert unrelated.poll() is None
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            port = listener.getsockname()[1]
            with pytest.raises(ValueError, match="another process"):
                NativeProcess(
                    owned,
                    Path(owned["state_dir"]) / "busy-port",
                    argv=[sys.executable, "-c", "raise AssertionError('never launched')"],
                    cwd=scratch,
                    environment={"PATH": "/usr/bin:/bin"},
                    timeout=1,
                    ports=(port,),
                )
            result = NativeProcess(
                owned,
                Path(owned["state_dir"]) / "timeout",
                argv=[sys.executable, "-c", "import time; time.sleep(30)"],
                cwd=scratch,
                environment={"PATH": "/usr/bin:/bin"},
                timeout=1,
            ).run()
            assert result["timed_out"] is True
            assert result["cleanup"] == "observed-native-confirmed"
            receipt = resources.finalize("timeout")
            assert receipt["resource_cleanup"] == "confirmed" and not scratch.exists()
            assert unrelated.poll() is None and listener.getsockname()[1] == port
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=5)


def test_finite_continuation_allocates_fresh_scratch_and_preserves_session(tmp_path):
    owned = spec(tmp_path)
    resources = RunResources(owned)
    original = resources.scratch("role", "implement")
    session = Path(owned["state_dir"]) / "role-homes" / "implement" / "session.json"
    session.parent.mkdir(parents=True)
    session.write_text("durable prior context")
    resources.finalize("blocked")
    assert not original.exists()
    resumed = resources.scratch("role", "implement")
    assert resumed.exists() and resumed == original
    assert session.read_text() == "durable prior context"
    assert (
        read_private(resources.manifest)["roots"][str(Path(owned["state_dir"]) / "transient")][
            "generation"
        ]
        == 1
    )
    assert resources.finalize("blocked")["state"] == "confirmed"


def test_removed_generated_gate_retry_and_reallocation_reject_symlink_ancestor(tmp_path):
    owned, resources, source, checkout = owned_worktree(tmp_path)
    owned["policy"].update(max_repairs=1, browser_qa={"artifact_paths": ["qa-artifacts"]})
    gate = Path(owned["state_dir"]) / "gates/2/verify"
    gate.parent.mkdir(parents=True)

    def create_gate():
        resources.register(gate, "gate")
        subprocess.run(
            ["git", "-C", str(source), "worktree", "add", "--detach", str(gate)],
            check=True,
            capture_output=True,
        )
        resources.created(gate)
        for name in ("node_modules", "qa-artifacts"):
            path = gate / name
            resources.register(path, "generated")
            path.mkdir()
            path.joinpath("temporary").write_text("owned")
            resources.created(path)

    create_gate()
    first = resources.finalize("blocked")
    assert first["state"] == "confirmed" and checkout.exists() and not gate.exists()
    retry = resources.finalize("blocked")
    assert retry["state"] == "confirmed"
    assert all(
        item["state"] == "already_absent" for item in retry["roots"] if item["kind"] != "checkout"
    )
    create_gate()  # a new authorized generation uses the registered owned path
    assert read_private(resources.manifest)["roots"][str(gate)]["generation"] == 1
    assert resources.finalize("blocked")["state"] == "confirmed" and not gate.exists()
    sentinel = tmp_path / "unrelated-sentinel"
    sentinel.mkdir()
    sentinel.joinpath("precious").write_text("SAFE")
    gate.parent.rmdir()
    gate.parent.symlink_to(sentinel, target_is_directory=True)
    assert resources.finalize("blocked")["state"] == "unknown"
    assert sentinel.joinpath("precious").read_text() == "SAFE"


def test_public_terminal_cleanup_derives_only_exact_finalization_and_keeps_recorded_history(
    tmp_path,
):
    import hashlib

    from devflow_temporal.delivery_resources import projected_cleanup

    owned = spec(tmp_path)
    resources = RunResources(owned)
    resources.scratch("check", "finite")
    receipt = resources.finalize("blocked")
    checks = {"resource_cleanup": receipt}
    assert projected_cleanup(owned, checks, "none", terminal=True) == "confirmed"
    assert projected_cleanup(owned, checks, "none", terminal=False) == "none"
    assert projected_cleanup(owned, checks, "unknown", terminal=True) == "unknown"
    assert projected_cleanup(owned, {}, "none", terminal=True) == "none"
    changed = {**receipt, "resource_cleanup": "unknown"}
    assert (
        projected_cleanup(owned, {"resource_cleanup": changed}, "none", terminal=True) == "unknown"
    )
    path = Path(receipt["receipt"])
    original = path.read_bytes()
    path.write_bytes(original + b" ")
    assert projected_cleanup(owned, checks, "none", terminal=True) == "unknown"
    checks["resource_cleanup"] = {
        **receipt,
        "receipt_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    assert projected_cleanup(owned, checks, "none", terminal=True) == "confirmed"
