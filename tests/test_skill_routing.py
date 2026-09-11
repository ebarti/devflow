import hashlib
import json
import re
import shutil
import sqlite3
from pathlib import Path

import pytest
from test_installation import apply_install, git
from test_installation import source as source_fixture

from devflow.cli import main
from devflow.errors import WorkflowError
from devflow.installation import plan_install
from devflow.skill_routing import SKILLS, STAGE_SKILLS, catalog, resolve_catalog, route_actions

ROOT = Path(__file__).parents[1]


@pytest.fixture
def source(tmp_path):
    return source_fixture.__wrapped__(tmp_path)


def test_discovery_and_resolution_are_read_only_before_any_work(tmp_path, capsys):
    state = tmp_path / "private"
    args = ["--repository", str(tmp_path), "--state-dir", str(state), "--json"]
    assert main(["skill", "list", *args]) == 0
    result = json.loads(capsys.readouterr().out)["result"]
    assert {item["name"] for item in result["skills"]} == set(SKILLS)
    for item in result["skills"]:
        request = tmp_path / "request.json"
        request.write_text(json.dumps({"name": item["name"]}))
        assert main(["skill", "resolve", "--request-file", str(request), *args]) == 0
        resolved = json.loads(capsys.readouterr().out)["result"]["skill"]
        raw = Path(resolved["path"]).read_bytes()
        assert resolved["sha256"] == hashlib.sha256(raw).hexdigest()
        assert resolved == item
    assert not state.exists()
    assert not (tmp_path / ".devflow").exists()


def test_stage_handoffs_resolve_within_the_same_packaged_catalog():
    entries = catalog(ROOT)
    for entry in entries:
        path = Path(entry["path"])
        for target in re.findall(r"\]\(([^)]+)\)", path.read_text()):
            if not target.startswith("https://"):
                assert (path.parent / target.split("#")[0]).resolve().is_file(), (path, target)
    assert {item["name"] for item in entries if not item["compatibility"]} == set(STAGE_SKILLS)


def test_control_and_external_recovery_have_distinct_stage_owners():
    actions = [
        {"kind": "launch_role", "role": "qa"},
        {"kind": "import_gate_result", "role": "review"},
        {"kind": "reconcile_action", "action": {"operation": "send_role"}},
        {"kind": "reconcile_action", "action": {"operation": "push_branch"}},
        {"kind": "run_check", "recipe_id": "parity"},
    ]
    observed = route_actions(actions)
    assert [item["skill"] for item in observed] == [
        "devflow-coordinating", "devflow-coordinating", "devflow-coordinating",
        "devflow-delivering", "devflow-verifying",
    ]
    assert observed[0]["role_skill"] == "devflow-verifying"
    assert observed[1]["role_skill"] == "devflow-reviewing"
    assert all("skill" not in action for action in actions)


def stage_release(source, tmp_path):
    shutil.copytree(ROOT / "skills", source / "skills", dirs_exist_ok=True)
    (source / "pyproject.toml").write_text("[project]\nname='synthetic'\nversion='0.5.0'\n")
    git(source, "add", ".")
    git(source, "commit", "-qm", "test: synthetic stage release")
    links = {tmp_path / "host/skills" / name: "skills/" + name for name in SKILLS}
    return plan_install(source, git(source, "rev-parse", "HEAD"), tmp_path / "managed",
                        links=links, owned_paths=links)


def test_install_exposes_all_stages_and_rejects_a_missing_stage(source, tmp_path):
    manifest = stage_release(source, tmp_path)
    installed = apply_install(manifest)
    assert len(catalog(Path(installed["release_dir"]))) == len(SKILLS)
    for name in SKILLS:
        assert (tmp_path / "host/skills" / name / "SKILL.md").is_file()
    git(source, "rm", "skills/devflow-planning/SKILL.md")
    git(source, "commit", "-qm", "test: synthetic broken packaging")
    with pytest.raises(WorkflowError, match="complete stage skill catalog"):
        plan_install(source, git(source, "rev-parse", "HEAD"), tmp_path / "other",
                     links={}, owned_paths=[])


def test_active_attempt_uses_original_installed_skills_after_global_upgrade(source, tmp_path):
    manifest = stage_release(source, tmp_path)
    first = apply_install(manifest)
    first_revision = first["revision"]
    original = Path(first["release_dir"]) / "skills/devflow-planning/SKILL.md"
    first_hash = hashlib.sha256(original.read_bytes()).hexdigest()
    changed = source / "skills/devflow-planning/SKILL.md"
    changed.write_text(changed.read_text() + "\nSynthetic changed policy.\n")
    git(source, "add", ".")
    git(source, "commit", "-qm", "test: synthetic policy update")
    links = {tmp_path / "host/skills" / name: "skills/" + name for name in SKILLS}
    second_manifest = plan_install(source, git(source, "rev-parse", "HEAD"), tmp_path / "managed",
                                   links=links, owned_paths=links)
    second = apply_install(second_manifest)
    repository = tmp_path / "repository"
    shutil.copytree(ROOT / "fixtures/repositories/prose", repository)
    lock = repository / ".devflow/workflow.lock"
    lock.write_text('schema_version = 1\nversion = "0.5.0"\nrevision = "' + second["revision"] + '"\n')
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    historical = {"lifecycle": "active", "attempt": {"workflow_snapshot_id": "first"},
                  "records": {"workflow_snapshot:first": {"package_revision": first_revision}}}
    with sqlite3.connect(state_dir / "state.sqlite3") as db:
        db.execute("CREATE TABLE works (work_id TEXT PRIMARY KEY, state TEXT)")
        db.execute("INSERT INTO works VALUES (?,?)", ("original-work", json.dumps(historical)))
    resolved = resolve_catalog(repository, state_dir=state_dir, work_id="original-work",
                               release_root=tmp_path / "managed")
    selected = next(item for item in resolved["skills"] if item["name"] == "devflow-planning")
    assert resolved["package_revision"] == first_revision
    assert selected["path"] == str(original)
    assert selected["sha256"] == first_hash
    assert (tmp_path / "host/skills/devflow-planning/SKILL.md").read_bytes() != original.read_bytes()
