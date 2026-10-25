"""Ordinary stopped scope grants retain their accepted commit as main advances."""

from __future__ import annotations

import asyncio
import copy
import shutil
from pathlib import Path

import pytest
import test_delivery_blocked_scope as blocked
from test_delivery_blocked_scope import ADDED, SESSION, admitted, scope_case
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.delivery_activities import delivery_repair_preflight
from devflow_temporal.delivery_resources import read_private


@pytest.fixture
def named_scope(service, monkeypatch):
    return scope_case(service, monkeypatch, named_base=True)


def advance_main(case):
    source = case.broker.source
    (source / "README.md").write_text("Independent main branch advancement\n")
    _git(source, "add", "README.md")
    _git(source, "commit", "-qm", "Advance main independently of stopped work")
    _git(source, "push", "origin", "main")
    head = _git(source, "rev-parse", "refs/remotes/origin/main")
    assert head != case.spec["base_sha"]
    _git(source, "merge-base", "--is-ancestor", case.spec["base_sha"], head)
    return head


def test_public_scope_preflight_keeps_accepted_base_after_main_fast_forward(named_scope):
    case = named_scope
    original = copy.deepcopy(case.state)
    head = advance_main(case)
    observed = case.store.repair_admission_preflight("run-1", case.command)
    assert observed["authorized_through_iteration"] == 4
    assert observed["candidate"] == case.partial
    assert case.state == original
    assert case.spec["base_sha"] != head
    assert case.spec["publication_base_ref"] == "main"


def test_fresh_admission_still_rejects_the_moved_expected_base(named_scope):
    case = named_scope
    advance_main(case)
    request = {key: case.spec[key] for key in (
        "command_id", "run_id", "work_id", "issue_url", "repository_key", "goal",
        "accepted_plan", "base_ref", "branch", "authorized_endpoint",
    )}
    with pytest.raises(ValueError, match="base ref moved from the accepted plan"):
        case.store.config.admit(request)


def rewrite_main(case):
    source = case.broker.source
    _git(source, "checkout", "--orphan", "unrelated-main")
    _git(source, "add", ".")
    _git(source, "commit", "-qm", "Unrelated history is not the accepted baseline")
    _git(source, "push", "--force", "origin", "HEAD:refs/heads/main")


def test_public_scope_admission_and_worker_keep_original_source_and_allowance(named_scope):
    case = named_scope
    original = copy.deepcopy(case.state)
    raw = {p: p.read_bytes() for p in case.folder.iterdir() if p.is_file()}
    current_main = advance_main(case)
    _response, current, recovery = admitted(case)
    assert current["base_sha"] == case.spec["base_sha"] != current_main
    assert current["base_ref"] == "refs/remotes/origin/main"
    assert current["publication_base_ref"] == "main"
    assert current["accepted_plan"] == case.spec["accepted_plan"]
    assert current["policy"]["max_repairs"] == 10 and recovery["maximum_iteration"] == 4
    assert current["policy"]["source_scope"]["allowed_files"] == ADDED
    assert recovery["session_id"] == SESSION
    assert recovery["state"] == original
    assert asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery})) == {
        "state": "confirmed"
    }
    assert {p: p.read_bytes() for p in raw} == raw
    assert _git(case.broker.source, "rev-parse", "refs/remotes/origin/main") == current_main


@pytest.mark.parametrize("defect", ["rewrite", "missing_ref", "foreign_origin", "missing_commit"])
def test_public_scope_preflight_refuses_invalid_frozen_source(named_scope, defect):
    case = named_scope
    advance_main(case)
    if defect == "rewrite":
        rewrite_main(case)
    elif defect == "missing_ref":
        _git(case.broker.source, "update-ref", "-d", "refs/remotes/origin/main")
    elif defect == "foreign_origin":
        _git(case.broker.source, "remote", "set-url", "origin",
             str(case.broker.source / "foreign.git"))
    else:
        sha = case.spec["base_sha"]
        (case.broker.source / ".git/objects" / sha[:2] / sha[2:]).unlink()
    with pytest.raises((ValueError, RuntimeError, OSError)):
        case.store.repair_admission_preflight("run-1", case.command)
    with case.store._connect() as db:
        assert case.store.state.claim_for(db, case.spec["work_id"]) is None
        assert not db.execute("SELECT 1 FROM delivery_commands WHERE command_id=?",
                              (case.command["command_id"],)).fetchone()


def test_public_scope_never_accepts_a_caller_base_override(named_scope):
    case = named_scope
    head = advance_main(case)
    command = {**case.command, "base_sha": head}
    with pytest.raises(ValueError):
        case.store.repair_admission_preflight("run-1", command)


def test_admission_rechecks_descendant_branch_after_runtime_preparation(named_scope, monkeypatch):
    case = named_scope
    advance_main(case)

    def prepare(spec, *_):
        rewrite_main(case)
        return copy.deepcopy(spec)

    monkeypatch.setattr(resume, "prepare_runtime", prepare)
    with pytest.raises(ValueError, match="descendant branch"):
        case.store.continue_repair("run-1", case.command)
    with case.store._connect() as db:
        assert case.store.state.claim_for(db, case.spec["work_id"]) is None
        assert not db.execute("SELECT 1 FROM delivery_commands WHERE command_id=?",
                              (case.command["command_id"],)).fetchone()


@pytest.mark.parametrize("stage", ["before", "during"])
def test_worker_rechecks_original_base_ancestry_before_launch(named_scope, monkeypatch, stage):
    case = named_scope
    advance_main(case)
    _response, current, recovery = admitted(case)
    if stage == "before":
        rewrite_main(case)
    else:
        observed = resume.published_identity

        def publication(*args):
            result = observed(*args)
            rewrite_main(case)
            return result

        monkeypatch.setattr(resume, "published_identity", publication)
    with pytest.raises(ValueError, match="descendant branch"):
        asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery}))
    assert case.state["roles"][-1]["status"] == "blocked"


def test_historical_scope_custody_does_not_require_current_source_refs(named_scope):
    case = named_scope
    advance_main(case)
    _response, current, recovery = admitted(case)
    _git(case.broker.source, "update-ref", "-d", "refs/remotes/origin/main")
    with case.store._connect() as db:
        assert resume.custody(db, recovery) == current
    with pytest.raises(ValueError, match="descendant branch"):
        asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery}))


@pytest.mark.parametrize("stage", ["preflight", "worker"])
@pytest.mark.parametrize("replacement", ["symlink", "copy"])
def test_current_scope_source_identity_is_not_replaced_by_equal_git_bytes(
    named_scope, stage, replacement
):
    case = named_scope
    advance_main(case)
    current, recovery = None, None
    if stage == "worker":
        _response, current, recovery = admitted(case)
    checkout = case.broker.checkout
    retained = checkout.with_name(checkout.name + "-retained")
    checkout.rename(retained)
    try:
        if replacement == "symlink":
            checkout.symlink_to(retained, target_is_directory=True)
        else:
            shutil.copytree(retained, checkout, symlinks=True)
        with pytest.raises((ValueError, RuntimeError, OSError)):
            if stage == "preflight":
                case.store.repair_admission_preflight("run-1", case.command)
            else:
                asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery}))
    finally:
        if checkout.is_symlink():
            checkout.unlink()
        else:
            shutil.rmtree(checkout)
        retained.rename(checkout)


def test_scope_does_not_acquire_new_main_configuration_or_file_authority(named_scope):
    case = named_scope
    source = case.broker.source
    new = source / ".codex/config.toml"
    new.parent.mkdir()
    new.write_text('model = "unapproved-new-main-model"\n')
    _git(source, "add", ".codex/config.toml")
    _git(source, "commit", "-qm", "Unrelated new main metadata")
    _git(source, "push", "origin", "main")
    _response, current, _recovery = admitted(case)
    assert current["base_sha"] == case.spec["base_sha"]
    assert current["policy"]["roles"] == case.spec["policy"]["roles"]
    assert current["policy"]["source_scope"]["allowed_files"] == ADDED
    assert not (case.broker.checkout / ".codex").exists()


@pytest.mark.asyncio
async def test_named_base_fast_forward_runs_real_worker_finalizer_and_replay(service, monkeypatch):
    original = blocked.scope_case
    cases = []

    def scoped(*args, **kwargs):
        case = original(*args, **kwargs, named_base=True)
        advance_main(case)
        cases.append(case)
        return case

    monkeypatch.setattr(blocked, "scope_case", scoped)
    await blocked.test_public_native_blocked_scope_runs_worker_same_session_and_all_normal_gates(
        service, monkeypatch
    )
    case = cases[0]
    current = case.store.effective_spec("run-1")
    assert current["base_sha"] == case.spec["base_sha"]
    assert current["publication_base_ref"] == "main"
    assert current["base_sha"] != _git(case.broker.source, "rev-parse", "refs/remotes/origin/main")
    assert read_private(Path(case.folder) / "result.json")["status"] == "blocked"
