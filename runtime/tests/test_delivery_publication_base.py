from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_broker
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import publication_base_ref
from devflow_temporal.delivery_repair import published_identity
from devflow_temporal.delivery_store import DeliveryStore


def bind_default(source: Path) -> str:
    _git(source, "branch", "-M", "main")
    head = _git(source, "rev-parse", "HEAD")
    _git(source, "update-ref", "refs/remotes/origin/main", head)
    _git(source, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main")
    return head


@pytest.mark.parametrize("raw", ["main", "origin/main", "refs/heads/main", "HEAD", "sha"])
def test_branch_resolution_keeps_the_pinned_object(service, raw):
    store, _ = service
    source = Path(store.config.raw["repositories"]["fixture"]["source_path"])
    head = bind_default(source)
    assert publication_base_ref(source, head if raw == "sha" else raw, head) == "main"


@pytest.mark.parametrize("case", ["missing-default", "moved-default", "tag"])
def test_unbound_commit_or_tag_cannot_guess_a_publication_branch(service, case):
    store, _ = service
    source = Path(store.config.raw["repositories"]["fixture"]["source_path"])
    head = bind_default(source)
    raw = head
    if case == "missing-default":
        _git(source, "symbolic-ref", "--delete", "refs/remotes/origin/HEAD")
    elif case == "moved-default":
        (source / "README.md").write_text("Later base\n")
        _git(source, "commit", "-qam", "docs: advance default")
        _git(source, "update-ref", "refs/remotes/origin/main", _git(source, "rev-parse", "HEAD"))
    else:
        _git(source, "tag", "pinned-tag", head)
        raw = "pinned-tag"
    with pytest.raises(ValueError, match="publication"):
        publication_base_ref(source, raw, head)


def test_native_admission_freezes_branch_before_roles_or_effects(native_configuration):
    config, request = native_configuration
    repository = config.raw["repositories"]["fixture"]
    head = bind_default(Path(repository["source_path"]))
    repository.update(base_ref=head, expected_base_sha=head)
    spec = config.admit({**request, "base_ref": head})
    assert spec["base_ref"] == spec["base_sha"] == head
    assert spec["publication_base_ref"] == "main"


def test_unbound_native_pin_is_rejected_before_accepting_work(native_configuration):
    config, request = native_configuration
    repository = config.raw["repositories"]["fixture"]
    source = Path(repository["source_path"])
    head = bind_default(source)
    repository.update(base_ref=head, expected_base_sha=head)
    _git(source, "symbolic-ref", "--delete", "refs/remotes/origin/HEAD")
    store = DeliveryStore(config)
    with pytest.raises(ValueError, match="publication"):
        store.submit({**request, "base_ref": head})
    with store._connect() as db:
        assert db.execute("SELECT count(*) FROM delivery_runs").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM delivery_commands").fetchone()[0] == 0


@pytest.mark.parametrize("case", [
    "unchanged", "advanced", "remote-only", "default-changed", "paginated",
    "page-retarget", "retarget",
    "retarget-restored", "retarget-pinned", "retarget-restored-pinned",
    "default-changed-pinned",
    "incomplete-history", "missing-history", "readback-race", "api-error",
    "missing-receipt", "wrong-receipt", "unrelated-live-tip", "foreign-branch",
    "foreign-head", "foreign-origin",
])
def test_legacy_sha_publication_uses_branch_for_creation_and_readback(
    service, monkeypatch, case,
):
    store, request = service
    repository = store.config.raw["repositories"]["fixture"]
    source = Path(repository["source_path"])
    base = bind_default(source)
    repository.update(base_ref=base, expected_base_sha=base)
    store.config.path.write_text(json.dumps(store.config.raw))
    request = {**request, "base_ref": base}
    store.submit(request)
    broker = DeliveryBroker(store, store.spec(request["run_id"]))
    broker.prepare()
    broker.state_dir.mkdir(parents=True, mode=0o700)
    (broker.checkout / "README.md").write_text("Owned document\n")
    before = broker.candidate()
    created = []
    pr_changes = {}
    history_changes = {}
    timeline = {"totalCount": 1, "pageInfo": {"hasNextPage": False},
                "nodes": [{"__typename": "PullRequestCommit"}]}
    live_tip = None
    api_failed = False
    original_run = delivery_broker._run

    def live_pr():
        return {
            "number": 9, "url": "https://github.com/example/fixture/pull/9",
            "state": "OPEN", "isDraft": False, "baseRefName": "main",
            "headRefName": request["branch"],
            "headRefOid": _git(broker.checkout, "rev-parse", "HEAD"),
            "title": "docs: document the owned feature",
            **pr_changes,
        }

    def run(argv, **kwargs):
        if argv[:3] == ["gh", "pr", "create"]:
            assert argv[argv.index("--base") + 1] == "main"
            created.append(argv)
            return "https://github.com/example/fixture/pull/9"
        if argv[:3] == ["gh", "pr", "list"]:
            if not created:
                return "[]"
            return json.dumps([live_pr()])
        if argv[:3] == ["gh", "api", "graphql"]:
            if api_failed:
                raise RuntimeError("unavailable API")
            found = live_pr()
            found.update(baseRef={"name": found["baseRefName"], "target": {
                "oid": live_tip or _git(source, "rev-parse", "refs/remotes/origin/main"),
            }}, timelineItems=timeline)
            if case in {"paginated", "page-retarget"} and timeline['totalCount'] == 101:
                if any(arg.startswith('after=') for arg in argv):
                    found['timelineItems'] = {
                        'totalCount': 101, 'pageInfo': {'hasNextPage': False},
                        'nodes': [{'__typename': 'BaseRefChangedEvent' if case == 'page-retarget'
                                  else 'PullRequestCommit'}],
                    }
                else:
                    found['timelineItems'] = {
                        'totalCount': 101, 'pageInfo': {'hasNextPage': True, 'endCursor': 'page1'},
                        'nodes': [{'__typename': 'PullRequestCommit'}] * 100,
                    }
            found.update(history_changes)
            return json.dumps({"data": {"repository": {"pullRequest": found}}})
        return original_run(argv, **kwargs)

    monkeypatch.setattr(delivery_broker, "_run", run)
    result = broker.publish(0, before)
    assert result["base"] == base and result["state"] == "OPEN"
    if case == "remote-only":
        remote = source.parent / "remote-writer"
        _git(source, "push", "origin", "main")
        _git(source.parent, "clone", "--branch", "main", str(source.parent / "origin.git"),
             str(remote))
        _git(remote, "config", "user.name", "Remote Test")
        _git(remote, "config", "user.email", "remote@example.invalid")
        (remote / "README.md").write_text("Remote-only newer upstream\n")
        _git(remote, "commit", "-qam", "docs: advance remote only")
        _git(remote, "push", "origin", "main")
        live_tip = _git(remote, "rev-parse", "HEAD")
        with pytest.raises(subprocess.CalledProcessError):
            _git(source, "cat-file", "-e", live_tip + "^{commit}")
        refs_before = _git(source, "show-ref")
        fetch_head = (source / ".git/FETCH_HEAD").read_bytes() if (
            source / ".git/FETCH_HEAD").exists() else None
    elif case not in {"unchanged", "retarget-pinned", "retarget-restored-pinned",
                       "default-changed-pinned"}:
        (source / "README.md").write_text("Later upstream document\n")
        _git(source, "commit", "-qam", "docs: advance default after publication")
        _git(source, "update-ref", "refs/remotes/origin/main", _git(source, "rev-parse", "HEAD"))
    if case in {"paginated", "page-retarget"}:
        timeline["totalCount"] = 101
    if case in {"default-changed", "retarget", "retarget-pinned", "default-changed-pinned"}:
        _git(source, "update-ref", "refs/remotes/origin/other", _git(source, "rev-parse", "HEAD"))
        _git(source, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/other")
    if case in {"retarget", "retarget-restored", "retarget-pinned",
                "retarget-restored-pinned"}:
        timeline["nodes"].append({"__typename": "BaseRefChangedEvent"})
        timeline["totalCount"] += 1
        if case in {"retarget", "retarget-pinned"}:
            pr_changes["baseRefName"] = "other"
    elif case == "incomplete-history":
        timeline["pageInfo"]["hasNextPage"] = True
    elif case == "missing-history":
        timeline["nodes"] = []
    elif case == "readback-race":
        history_changes["headRefOid"] = "a" * 40
    elif case == "api-error":
        api_failed = True
    elif case in {"missing-receipt", "wrong-receipt"}:
        with store._connect() as db:
            if case == "missing-receipt":
                db.execute("DELETE FROM delivery_effects WHERE kind='publish'")
            else:
                db.execute("UPDATE delivery_effects SET observed_json=? WHERE kind='publish'",
                           (json.dumps({**result, "number": 10}),))
    elif case == "unrelated-live-tip":
        live_tip = _git(source, "commit-tree", _git(source, "rev-parse", "HEAD^{tree}"),
                        "-m", "docs: unrelated rewritten target")
    elif case == "foreign-branch":
        pr_changes["headRefName"] = "foreign"
    elif case == "foreign-head":
        pr_changes["headRefOid"] = "a" * 40
    elif case == "foreign-origin":
        _git(source, "remote", "set-url", "origin", "https://example.invalid/foreign.git")
    if case not in {"unchanged", "advanced", "remote-only", "default-changed",
                    "default-changed-pinned", "paginated"}:
        with pytest.raises((ValueError, RuntimeError)):
            published_identity(broker, broker.candidate(), result)
        return
    assert published_identity(broker, broker.candidate(), result)["number"] == 9
    assert broker._existing_pr()["baseRefName"] == "main"
    if case == "remote-only":
        assert _git(source, "show-ref") == refs_before
        assert ((source / ".git/FETCH_HEAD").read_bytes() if (
            source / ".git/FETCH_HEAD").exists() else None) == fetch_head
    assert broker.publish(0, before) == result
    if case != "unchanged":
        (broker.checkout / "README.md").write_text("Owned document with reviewed repair\n")
        repaired = broker.publish(1, broker.candidate())
        assert repaired["number"] == result["number"]
        assert repaired["head"] != result["head"]
        assert repaired["base"] == base
    assert store.spec(request["run_id"])["base_ref"] == base
    assert store.spec(request["run_id"])["policy"]["max_repairs"] == 2
