"""Ordinary stopped scope grants retain their accepted commit as main advances."""

from __future__ import annotations

import copy

import pytest
from test_delivery_blocked_scope import scope_case
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_config as configuration


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


def rewrite_main(case):
    source = case.broker.source
    _git(source, "checkout", "--orphan", "unrelated-main")
    _git(source, "add", ".")
    _git(source, "commit", "-qm", "Unrelated history is not the accepted baseline")
    _git(source, "push", "--force", "origin", "HEAD:refs/heads/main")


@pytest.mark.parametrize("view", ["graft", "replace", "packed_replace"])
def test_public_scope_rejects_altered_git_ancestry_or_original_object_view(named_scope, view):
    case = named_scope
    descendant = advance_main(case)
    if view == "graft":
        rewrite_main(case)
        unrelated = _git(case.broker.source, "rev-parse", "refs/remotes/origin/main")
        (case.broker.source / ".git/info/grafts").write_text(
            unrelated + " " + case.spec["base_sha"] + "\n"
        )
    else:
        _git(case.broker.source, "replace", case.spec["base_sha"], descendant)
        if view == "packed_replace":
            _git(case.broker.source, "pack-refs", "--all", "--prune")
    with pytest.raises(ValueError, match="altered Git object view"):
        configuration.ordinary_scope_base(case.store.config, case.spec)
    with pytest.raises((ValueError, RuntimeError, OSError)):
        case.store.repair_admission_preflight("run-1", case.command)
    with case.store._connect() as db:
        assert case.store.state.claim_for(db, case.spec["work_id"]) is None
        assert not db.execute("SELECT 1 FROM delivery_commands WHERE command_id=?",
                              (case.command["command_id"],)).fetchone()
