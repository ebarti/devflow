"""Synthetic execution-prerequisite regressions; never launch native processes."""

from pathlib import Path

from test_delivery_native_addon import addon as addon

from devflow_temporal.delivery_resources import RunResources, read_private
from devflow_temporal.delivery_sandbox import _native_env


def test_all_temp_environment_aliases_use_owned_root_and_do_not_inherit_credentials(monkeypatch):
    monkeypatch.setenv("TMPDIR", "/foreign/long/temp")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "synthetic-secret")
    scratch = Path("/private/tmp/dftmp-controlled/0123456789abcdef")
    env = _native_env(Path("/durable/home"), Path("/durable/codex"), scratch, short_temp=True)
    assert all(env[key] == str(scratch) for key in ["TMPDIR", "TMP", "TEMP"])
    assert "AWS_SECRET_ACCESS_KEY" not in env


def test_short_scratch_cleanup_preserves_durable_evidence_and_foreign_directories(
    tmp_path, monkeypatch
):
    state = tmp_path / "run-test"
    state.mkdir()
    durable = state / "original-command.log"
    durable.write_text("original failure")
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    sentinel = foreign / "keep.txt"
    sentinel.write_text("KEEP")
    root = tmp_path / "short"
    monkeypatch.setattr(RunResources, "_execution_scratch_root", lambda self: root)
    resources = RunResources({"run_id": state.name, "state_dir": str(state)})
    scratch = resources.execution_scratch("checks", "api")
    (scratch / "temporary.txt").write_text("temporary")
    (scratch / "alias").symlink_to(foreign, target_is_directory=True)
    result = resources.finalize("blocked")
    assert result["state"] == "confirmed" and not root.exists()
    assert durable.read_text() == "original failure"
    assert sentinel.read_text() == "KEEP"
    recreated = resources.execution_scratch("checks", "next-api")
    assert recreated.is_dir()
    assert read_private(resources.manifest)["roots"][str(root)]["generation"] == 1
    resources.finalize("blocked")
