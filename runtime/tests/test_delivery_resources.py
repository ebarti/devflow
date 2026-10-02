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


def test_dirty_blocked_and_unpushed_worktrees_are_retained_with_reason(tmp_path):
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
