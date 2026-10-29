from __future__ import annotations

import subprocess
import sys
from pathlib import Path

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
