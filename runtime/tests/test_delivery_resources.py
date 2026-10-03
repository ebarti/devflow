from __future__ import annotations

import concurrent.futures
import subprocess
import sys
from pathlib import Path

import pytest

from devflow_temporal.delivery_native_process import NativeProcess, process_table
from devflow_temporal.delivery_resources import RunResources, read_private


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
