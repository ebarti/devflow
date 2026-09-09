import subprocess

import pytest

import devflow.installation as installation
from devflow.errors import WorkflowError
from devflow.installation import plan_install, resolve_workflow


def apply_install(manifest, **overrides):
    return installation.apply_install(manifest, **{
        "approved_paths": manifest["owned_paths"], "approved_root": manifest["install_root"],
        "approved_plan_id": manifest["plan_id"], **overrides})


def rollback_install(manifest, **overrides):
    return installation.rollback_install(manifest, **{
        "approved_paths": manifest["owned_paths"], "approved_root": manifest["install_root"],
        "approved_plan_id": manifest["plan_id"], **overrides})


def git(path, *args):
    return subprocess.check_output(["git", "-C", str(path), *args]).decode().strip()


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "source"
    path.mkdir()
    git(path, "init", "-q")
    git(path, "config", "user.email", "synthetic@example.invalid")
    git(path, "config", "user.name", "Synthetic Fixture")
    (path / "skills/devflow").mkdir(parents=True)
    (path / "skills/devflow/SKILL.md").write_text("Synthetic workflow\n")
    (path / "pyproject.toml").write_text("[project]\nname='synthetic'\nversion='0.1.0'\n")
    (path / "uv.lock").write_text("version = 1\n")
    git(path, "add", ".")
    git(path, "commit", "-qm", "test: synthetic release")
    return path


def plan(source, tmp_path, **kwargs):
    target = tmp_path / "host/skills/devflow"
    return plan_install(source, git(source, "rev-parse", "HEAD"), tmp_path / "managed",
                        links={target: "skills/devflow"}, owned_paths=[target], **kwargs)


def test_install_roundtrip_retains_work_and_nonadopting_consumer(source, tmp_path):
    legacy = tmp_path / "legacy.md"
    legacy.write_text("legacy instructions and model preferences")
    target = tmp_path / "host/skills/devflow"
    target.parent.mkdir(parents=True)
    target.symlink_to(legacy)
    claude = tmp_path / "claude.md"
    claude.symlink_to(legacy)
    sentinel = tmp_path / "user-work"
    sentinel.write_text("do not touch")
    manifest = plan(source, tmp_path, consumers=[target, claude])
    applied = apply_install(manifest)
    assert target.resolve().joinpath("SKILL.md").read_text() == "Synthetic workflow\n"
    assert claude.read_text() == legacy.read_text()
    assert apply_install(applied) == applied
    rolled_back = rollback_install(applied)
    assert rolled_back["status"] == "rolled_back"
    assert target.resolve() == legacy
    assert sentinel.read_text() == "do not touch"
    assert (tmp_path / "managed/releases" / manifest["revision"]).exists()


def test_reject_unowned_and_changed_after_plan(source, tmp_path):
    target = tmp_path / "target"
    target.write_text("owned by user")
    with pytest.raises(WorkflowError, match="explicitly enrolled"):
        plan_install(source, git(source, "rev-parse", "HEAD"), tmp_path / "managed",
                     links={target: "skills/devflow"}, owned_paths=[])
    manifest = plan(source, tmp_path)
    changed = tmp_path / "host/skills/devflow"
    changed.parent.mkdir(parents=True)
    changed.write_text("new user work")
    with pytest.raises(WorkflowError, match="changed after planning"):
        apply_install(manifest)
    assert changed.read_text() == "new user work"


def test_shared_target_and_symlink_parent_are_rejected(source, tmp_path):
    shared = tmp_path / "shared"
    shared.write_text("legacy")
    consumer = tmp_path / "claude"
    consumer.symlink_to(shared)
    with pytest.raises(WorkflowError, match="shared consumer"):
        plan_install(source, git(source, "rev-parse", "HEAD"), tmp_path / "managed",
                     links={shared: "skills/devflow"}, owned_paths=[shared], consumers=[consumer])
    alias = tmp_path / "alias"
    alias.symlink_to(tmp_path / "real")
    with pytest.raises(WorkflowError, match="symlink parent"):
        plan_install(source, git(source, "rev-parse", "HEAD"), tmp_path / "managed",
                     links={alias / "skill": "skills/devflow"}, owned_paths=[alias / "skill"])


def test_dirty_unpinned_and_tampered_plan_rejected(source, tmp_path):
    with pytest.raises(WorkflowError, match="full immutable"):
        plan_install(source, "HEAD", tmp_path / "managed", links={}, owned_paths=[])
    manifest = plan(source, tmp_path)
    manifest["operations"][0]["path"] = str(tmp_path / "user-file")
    with pytest.raises(WorkflowError, match="plan content changed"):
        apply_install(manifest)
    (source / "untracked").write_text("user")
    with pytest.raises(WorkflowError, match="clean"):
        plan(source, tmp_path)


def test_rollback_preserves_post_install_edits(source, tmp_path):
    applied = apply_install(plan(source, tmp_path))
    target = tmp_path / "host/skills/devflow"
    target.unlink()
    target.write_text("user replaced link")
    with pytest.raises(WorkflowError, match="preserves user work"):
        rollback_install(applied)
    assert target.read_text() == "user replaced link"


def test_apply_failure_restores_previous_entries(source, tmp_path, monkeypatch):
    first, second = tmp_path / "a", tmp_path / "b"
    first.write_text("previous a")
    second.write_text("previous b")
    manifest = plan_install(source, git(source, "rev-parse", "HEAD"), tmp_path / "managed",
                            links={}, files={first: "after a", second: "after b"},
                            owned_paths=[first, second])
    original = installation._write_entry
    failed = False

    def write(path, state):
        nonlocal failed
        if str(path) == str(second) and not failed:
            failed = True
            raise OSError("synthetic disk failure")
        return original(path, state)

    monkeypatch.setattr(installation, "_write_entry", write)
    with pytest.raises(OSError, match="synthetic"):
        apply_install(manifest)
    assert first.read_text() == "previous a"
    assert second.read_text() == "previous b"


def test_repository_coexistence_and_active_attempt_precedence():
    pin = "a" * 40
    lock = {"schema_version": 1, "revision": pin}
    assert resolve_workflow(repository_lock=lock, legacy_version="legacy")["version"] == pin
    assert resolve_workflow(repository_lock=None, legacy_version="legacy")["version"] == "legacy"
    assert resolve_workflow(active_version="b" * 40, repository_lock=lock,
                            legacy_version="legacy")["version"] == "b" * 40
    with pytest.raises(WorkflowError, match="full pin"):
        resolve_workflow(repository_lock={}, legacy_version="legacy")


def test_forged_manifest_cannot_authorize_its_own_paths(source, tmp_path):
    manifest = plan(source, tmp_path)
    original_id = manifest["plan_id"]
    original_paths = list(manifest["owned_paths"])
    victim = tmp_path / "user-config"
    manifest["operations"][0]["path"] = str(victim)
    manifest["owned_paths"] = [str(victim)]
    manifest["plan_id"] = installation._json_hash(
        {key: value for key, value in manifest.items() if key != "plan_id"})
    with pytest.raises(WorkflowError, match="plan content changed"):
        apply_install(manifest, approved_plan_id=original_id, approved_paths=original_paths)
    with pytest.raises(WorkflowError, match="independent approval"):
        apply_install(manifest, approved_paths=original_paths)
    assert not victim.exists()


def test_rollback_rejects_forged_before_bytes_and_applied_state(source, tmp_path):
    import copy

    applied = apply_install(plan(source, tmp_path))
    changed = copy.deepcopy(applied)
    changed["operations"][0]["before"] = {"kind": "file", "content": "ZXZpbA==", "mode": 0o600}
    with pytest.raises(WorkflowError, match="plan content changed"):
        rollback_install(changed)
    changed = copy.deepcopy(applied)
    changed["applied_states"][0]["resolved_hash"] = "forged"
    with pytest.raises(WorkflowError, match="durable applied receipt"):
        rollback_install(changed)
    assert (tmp_path / "host/skills/devflow").is_symlink()


def test_two_immutable_releases_coexist(source, tmp_path):
    first = apply_install(plan(source, tmp_path))
    (source / "skills/devflow/SKILL.md").write_text("Synthetic workflow version 2\n")
    git(source, "add", ".")
    git(source, "commit", "-qm", "test: synthetic second release")
    second = apply_install(plan(source, tmp_path))
    assert first["release_dir"] != second["release_dir"]
    from pathlib import Path

    assert Path(first["release_dir"]).joinpath("skills/devflow/SKILL.md").read_text() == (
        "Synthetic workflow\n")
    rollback_install(second)
    assert (tmp_path / "host/skills/devflow/SKILL.md").read_text() == "Synthetic workflow\n"


def crash_during_apply(manifest, tmp_path):
    """Kill after a real swap, before the durable post-swap journal update."""
    import json
    import os
    import signal
    import sys
    from pathlib import Path

    plan_path = tmp_path / "synthetic-plan.json"
    plan_path.write_text(json.dumps(manifest))
    code = """
import json, os, signal, sys
from devflow import installation
manifest = json.loads(open(sys.argv[1]).read())
original = installation._write_entry
first = manifest['operations'][0]['path']
def write(path, state):
    original(path, state)
    if str(path) == first:
        os.kill(os.getpid(), signal.SIGKILL)
installation._write_entry = write
installation.apply_install(manifest, approved_paths=manifest['owned_paths'],
    approved_root=manifest['install_root'], approved_plan_id=manifest['plan_id'])
"""
    result = subprocess.run([sys.executable, "-c", code, str(plan_path)],
                            env=dict(os.environ, PYTHONPATH=str(Path(installation.__file__).parents[2])),
                            capture_output=True, text=True)
    assert result.returncode == -signal.SIGKILL, result.stderr
    receipt = json.loads((Path(manifest["install_root"]) / "install-manifests" /
                          (manifest["plan_id"] + ".json")).read_text())
    assert receipt["status"] == "applying"
    assert receipt["journal"]["pending_index"] == 0
    return receipt


@pytest.mark.parametrize("direction", ["apply", "rollback"])
def test_killed_apply_recovers_from_original_plan_and_preserves_sentinels(source, tmp_path, direction):
    first, second = tmp_path / "a", tmp_path / "b"
    first.write_text("before a")
    second.write_text("before b")
    sentinel_a, sentinel_b = tmp_path / "user-work", tmp_path / "history"
    sentinel_a.write_text("private user work")
    sentinel_b.write_text("retained history")
    manifest = plan_install(source, git(source, "rev-parse", "HEAD"), tmp_path / "managed",
                            links={}, files={first: "after a", second: "after b"},
                            owned_paths=[first, second])
    crash_during_apply(manifest, tmp_path)
    assert first.read_text() == "after a"
    assert second.read_text() == "before b"
    result = (apply_install if direction == "apply" else rollback_install)(manifest)
    assert result["status"] == ("applied" if direction == "apply" else "rolled_back")
    prefix = "after" if direction == "apply" else "before"
    assert first.read_text() == prefix + " a"
    assert second.read_text() == prefix + " b"
    assert sentinel_a.read_text() == "private user work"
    assert sentinel_b.read_text() == "retained history"


def test_killed_apply_third_state_blocks_recovery_without_more_writes(source, tmp_path):
    first, second = tmp_path / "a", tmp_path / "b"
    first.write_text("before a")
    second.write_text("before b")
    manifest = plan_install(source, git(source, "rev-parse", "HEAD"), tmp_path / "managed",
                            links={}, files={first: "after a", second: "after b"},
                            owned_paths=[first, second])
    crash_during_apply(manifest, tmp_path)
    second.write_text("user edit after crash")
    for recovery in (apply_install, rollback_install):
        with pytest.raises(WorkflowError, match="unexpected state"):
            recovery(manifest)
        assert first.read_text() == "after a"
        assert second.read_text() == "user edit after crash"


def test_installed_revision_checks_metadata_and_full_content(source, tmp_path):
    from pathlib import Path

    applied = apply_install(plan(source, tmp_path))
    root = Path(applied["release_dir"])
    metadata = installation.installed_release(root)
    assert metadata["revision"] == applied["revision"]
    assert metadata["tree"] == git(source, "rev-parse", "HEAD^{tree}")
    skill = root / "skills/devflow/SKILL.md"
    skill.chmod(0o600)
    skill.write_text("altered release")
    with pytest.raises(WorkflowError, match="does not match its marker"):
        installation.installed_release(root)
