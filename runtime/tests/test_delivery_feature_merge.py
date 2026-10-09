"""Real store/registry with GitHub transport stubs; no native fixture execution."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from test_delivery_feature_execution import feature_service
from test_delivery_store import service as legacy_service

from devflow_temporal import delivery_feature_merge as merger
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import registry
from devflow_temporal.delivery_feature_workflow import publication
from devflow_temporal.delivery_github_contract import encode_manifest, ordered_chunks

service = legacy_service


class Remote:
    def __init__(self, record):
        self.record, self.calls, self.merged, self.closed = record, [], set(), set()
        self.tampered = False

    def api(self, path, *, method="GET", body=None):
        self.calls.append((method, path, body))
        members = self.record["manifest"]["publication"]["members"]
        if "/issues/comments/" in path:
            return {
                "id": 11,
                "node_id": "IC_11",
                "body": encode_manifest(self.record["manifest"]),
                "issue_url": "https://api.github.com/repos/example/fixture/issues/3",
            }
        if "/stacks/" in path:
            return {"number": 42, "pull_requests": [{"number": m["number"]} for m in members]}
        if "/pulls/" in path:
            m = next(m for m in members if m["number"] == int(path.rsplit("/", 1)[-1]))
            merged = m["number"] in self.merged
            return {
                "number": m["number"],
                "html_url": m["url"],
                "draft": False,
                "head": {
                    "sha": "f" * 40 if self.tampered else m["head"],
                    "ref": m["branch"],
                    "repo": {"full_name": "example/fixture"},
                },
                "base": {"ref": m["base_branch"], "repo": {"full_name": "example/fixture"}},
                "state": "closed" if merged else "open",
                "merged": merged,
                "merged_at": "2026-10-09T12:00:00Z" if merged else None,
                "merge_commit_sha": "merge-" + str(m["number"]) if merged else None,
            }
        if "/git/commits/merge-" in path:
            return {"tree": {"sha": "tree-" + path.rsplit("-", 1)[-1]}}
        if "/compare/" in path:
            return {
                "status": "ahead",
                "merge_base_commit": {"sha": path.rsplit("/", 1)[-1].split("...")[0]},
            }
        if "/issues/" in path:
            number = int(path.rsplit("/", 1)[-1])
            if method == "PATCH":
                self.closed.add(number)
            return {
                "node_id": "I_feature" if number == 3 else "I_" + str(number),
                "state": "closed" if number in self.closed else "open",
                "state_reason": "completed" if number in self.closed else None,
            }
        raise AssertionError((path, method, body))


def setup_feature(service, monkeypatch):
    store, request, snapshot = feature_service(service, monkeypatch)
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    value = json.loads(request["accepted_plan"])
    record = {
        "comment_id": 11,
        "comment_node_id": "IC_11",
        "manifest": {
            "version": 1,
            "issue_id": "I_feature",
            "repository_id": "R_fixture",
            "revision": 4,
            "creation_key": "create-feature",
            "plan": value,
            "workstream_issues": {
                "api": {
                    "id": "I_10",
                    "number": 10,
                    "url": "https://github.com/example/fixture/issues/10",
                },
                "ui": {
                    "id": "I_11",
                    "number": 11,
                    "url": "https://github.com/example/fixture/issues/11",
                },
            },
            "publication": {"stack_id": 42, "members": []},
        },
    }
    members = record["manifest"]["publication"]["members"]
    for i, chunk in enumerate(ordered_chunks(value)):
        members.append(
            {
                "chunk_id": chunk["id"],
                "number": 20 + i,
                "url": f"https://github.com/example/fixture/pull/{20 + i}",
                "branch": "feat/" + chunk["id"],
                "head": str(i + 1) * 40,
                "base_branch": members[-1]["branch"] if members else "main",
            }
        )
    shared = registry(spec)
    token = spec["feature_delivery"]["owner"]
    shared.checkpoint(token, "github-record", {"comment_id": 11, "comment_node_id": "IC_11"})
    shared.checkpoint(token, "accepted-plan", {"digest": digest(value)})
    requested = publication(record, complete=True)
    command = {
        "command_id": "merge-command",
        "expected_revision": 5,
        "decision_id": spec["run_id"] + ":merge:4",
        "decision_revision": 4,
        "candidate_revision": 1,
        "answer": "merge",
    }
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET phase='merging',pr_json=? WHERE run_id=?",
            (json.dumps(requested), spec["run_id"]),
        )
        db.execute(
            "INSERT INTO delivery_mutations(command_id,run_id,kind,request_digest,state) "
            "VALUES (?,?,'decision',?,'pending')",
            (command["command_id"], spec["run_id"], digest(command)),
        )
    monkeypatch.setattr(merger, "evidence", lambda *_: ["tree-20", "tree-21", "tree-22"])
    return store, spec, record, requested, command, Remote(record)


def test_one_stack_merge_closes_issues_only_after_tree_and_trunk_readback(service, monkeypatch):
    store, spec, record, requested, command, gh = setup_feature(service, monkeypatch)
    commands = []

    def execute(argv, **kwargs):
        assert not gh.closed
        commands.append(argv)
        gh.merged = {20, 21, 22}
        return SimpleNamespace(returncode=0)

    result = merger.merge(store, spec, requested, command, gh=gh, execute=execute)
    assert result["state"] == "confirmed"
    assert commands == [["gh", "stack", "merge", "42", "--yes", "--squash"]]
    assert gh.closed == {3, 10, 11}
    assert merger.merge(store, spec, requested, command, gh=gh, execute=execute) == result
    assert len(commands) == 1


def test_lost_merge_response_is_read_back_without_repeating_or_revalidating_new_base(
    service, monkeypatch
):
    store, spec, record, requested, command, gh = setup_feature(service, monkeypatch)

    def lost_response(*args, **kwargs):
        gh.merged = {20, 21, 22}
        raise TimeoutError("response lost after acceptance")

    with pytest.raises(TimeoutError):
        merger.merge(store, spec, requested, command, gh=gh, execute=lost_response)
    assert gh.closed == set()
    monkeypatch.setattr(merger, "evidence", lambda *_: pytest.fail("must use original intent"))
    result = merger.merge(
        store,
        spec,
        requested,
        command,
        gh=gh,
        execute=lambda *_args, **_kwargs: pytest.fail("must not resubmit"),
    )
    assert result["state"] == "confirmed"
    assert gh.closed == {3, 10, 11}


def test_queued_or_partially_completed_merge_retains_ownership_and_open_issues(
    service, monkeypatch
):
    store, spec, record, requested, command, gh = setup_feature(service, monkeypatch)
    result = merger.merge(
        store,
        spec,
        requested,
        command,
        gh=gh,
        execute=lambda *_args, **_kwargs: SimpleNamespace(returncode=0),
    )
    assert result["state"] == "pending"
    gh.merged.add(20)
    result = merger.merge(
        store,
        spec,
        requested,
        command,
        gh=gh,
        execute=lambda *_args, **_kwargs: pytest.fail("must not resubmit"),
    )
    assert result["state"] == "pending"
    assert not gh.closed
    with pytest.raises(OwnershipConflict, match="awaiting readback"):
        registry(spec).stop(spec["feature_delivery"]["owner"], "stop", {})


@pytest.mark.parametrize("change", ["authority", "plan", "head", "partial"])
def test_changed_authority_plan_or_head_cannot_merge(service, monkeypatch, change):
    store, spec, record, requested, command, gh = setup_feature(service, monkeypatch)
    if change == "authority":
        command = {**command, "command_id": "not-authenticated"}
    elif change == "plan":
        record["manifest"]["plan"]["scope"] = "Different feature"
    elif change == "head":
        gh.tampered = True
    else:
        gh.merged.add(20)
    with pytest.raises(OwnershipConflict):
        merger.merge(
            store,
            spec,
            requested,
            command,
            gh=gh,
            execute=lambda *_args, **_kwargs: pytest.fail("must not merge"),
        )
    assert not gh.closed
    with registry(spec).connect() as db:
        assert db.execute("SELECT COUNT(*) FROM execution_effects").fetchone()[0] == 0


def test_unrelated_merge_commit_does_not_close_feature(service, monkeypatch):
    store, spec, record, requested, command, gh = setup_feature(service, monkeypatch)
    original = gh.api

    def api(path, **kwargs):
        if "/compare/" in path:
            return {"status": "diverged", "merge_base_commit": {"sha": "other"}}
        return original(path, **kwargs)

    gh.api = api
    gh.merged = {20, 21, 22}
    with pytest.raises(OwnershipConflict, match="target branch"):
        merger.merge(store, spec, requested, command, gh=gh)
    assert not gh.closed
