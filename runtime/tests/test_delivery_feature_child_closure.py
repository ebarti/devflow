"""Exercise merge and projection obligations for a child with several required chunks."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from test_delivery_feature_merge import setup_feature
from test_delivery_store import service as legacy_service

from devflow_temporal import delivery_feature_merge as merger
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import registry
from devflow_temporal.delivery_features import transition

service = legacy_service


def test_child_closure_intents_name_only_its_required_confirmed_chunks(service, monkeypatch):
    store, spec, record, requested, command, gh = setup_feature(service, monkeypatch)
    commands = []

    def execute(argv, **_kwargs):
        assert not gh.closed
        commands.append(argv)
        gh.merged = {20, 21, 22}
        return SimpleNamespace(returncode=0)

    result = merger.merge(store, spec, requested, command, gh=gh, execute=execute)
    assert result["state"] == "confirmed" and result["stack_id"] == 42
    assert commands == [["gh", "stack", "merge", "42", "--yes", "--squash"]]
    shared = registry(spec)
    with shared.connect() as db:
        requests = [
            json.loads(row[0])
            for row in db.execute(
                "SELECT request_json FROM execution_effects WHERE kind='close_merged_issue'"
            )
        ]
    by_issue = {request["issue"]: request for request in requests}
    assert by_issue["I_10"]["required_chunks"] == ["model", "endpoint"]
    assert [item["number"] for item in by_issue["I_10"]["merges"]] == [20, 22]
    assert by_issue["I_11"]["required_chunks"] == ["client"]
    assert [item["number"] for item in by_issue["I_11"]["merges"]] == [21]
    assert by_issue["I_feature"]["required_chunks"] == ["model", "client", "endpoint"]
    assert merger.merge(store, spec, requested, command, gh=gh, execute=execute) == result
    assert len(commands) == 1


def test_an_early_child_chunk_merge_cannot_close_that_child_or_the_parent(service, monkeypatch):
    store, spec, _record, requested, command, gh = setup_feature(service, monkeypatch)

    def execute(_argv, **_kwargs):
        gh.merged = {20}
        return SimpleNamespace(returncode=0)

    result = merger.merge(store, spec, requested, command, gh=gh, execute=execute)
    assert result["state"] == "pending"
    assert not gh.closed
    assert not any(method == "PATCH" and "/issues/" in path for method, path, _ in gh.calls)
    gh.merged.add(22)
    result = merger.merge(
        store,
        spec,
        requested,
        command,
        gh=gh,
        execute=lambda *_args, **_kwargs: pytest.fail("must read original merge"),
    )
    assert result["state"] == "pending" and not gh.closed


@pytest.mark.parametrize(
    "merged,api_status,ui_status,parent_status",
    [
        ({20}, "Partially merged", "Queued", "Partially merged"),
        ({20, 22}, "Merged", "Queued", "Partially merged"),
        ({20, 21, 22}, "Merged", "Merged", "Merged"),
    ],
)
def test_child_status_uses_all_required_pr_merges_not_parent_or_worker_proofs(
    service,
    monkeypatch,
    merged,
    api_status,
    ui_status,
    parent_status,
):
    store, spec, record, _requested, _command, _gh = setup_feature(service, monkeypatch)
    with store._connect() as db:
        for member in record["manifest"]["publication"]["members"]:
            observation = {
                "url": member["url"],
                "head": member["head"],
                "state": "MERGED" if member["number"] in merged else "OPEN",
            }
            db.execute(
                "INSERT INTO delivery_pr_observations VALUES (?,?,?,?,NULL)",
                (
                    member["url"],
                    json.dumps(observation),
                    "2026-10-11T12:00:00Z",
                    "2026-10-12T12:00:00Z",
                ),
            )
        transition(db, store.config, spec["run_id"])
    state = store.feature(spec["run_id"])
    assert state["status"] == parent_status
    children = {stream["id"]: stream for stream in state["workstreams"]}
    assert children["api"]["status"] == api_status
    assert children["api"]["required_chunks"] == ["model", "endpoint"]
    assert children["api"]["issue_id"] == "I_10"
    assert {item["url"] for item in children["api"]["pull_requests"]} == {
        "https://github.com/example/fixture/pull/20",
        "https://github.com/example/fixture/pull/22",
    }
    assert children["ui"]["status"] == ui_status


@pytest.mark.parametrize(
    "field,value",
    [
        ("workstream_id", "ui"),
        ("issue_id", "I_11"),
        ("issue_number", 11),
        ("issue_url", "https://github.com/example/fixture/issues/11"),
    ],
)
def test_explicit_child_ownership_mismatch_refuses_merge_before_any_intent(
    service,
    monkeypatch,
    field,
    value,
):
    store, spec, record, requested, command, gh = setup_feature(service, monkeypatch)
    record["manifest"]["publication"]["members"][0].update(
        workstream_id="api",
        issue_id="I_10",
        issue_number=10,
        issue_url="https://github.com/example/fixture/issues/10",
    )
    record["manifest"]["publication"]["members"][0][field] = value
    from devflow_temporal.delivery_feature_workflow import publication

    requested = publication(record, complete=True)
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET pr_json=? WHERE run_id=?",
            (json.dumps(requested), spec["run_id"]),
        )
    monkeypatch.setattr(merger, "current_record", lambda *_args: record)
    with pytest.raises(OwnershipConflict, match="another child"):
        merger.merge(
            store,
            spec,
            requested,
            command,
            gh=gh,
            execute=lambda *_args, **_kwargs: pytest.fail("must not merge"),
        )
    with registry(spec).connect() as db:
        assert db.execute("SELECT 1 FROM execution_effects").fetchone() is None
    assert not gh.closed


def test_historical_all_feature_closure_intents_remain_replayable(service, monkeypatch):
    store, spec, _record, requested, command, gh = setup_feature(service, monkeypatch)
    gh.merged = {20, 21, 22}
    result = merger.merge(store, spec, requested, command, gh=gh)
    from devflow_temporal.contracts import digest

    shared = registry(spec)
    # Model immutable historical v1 requests in this disposable test database.
    # Production never rewrites historical intents or input rows.
    with shared.connect() as db:
        rows = db.execute(
            "SELECT effect_key,request_json FROM execution_effects WHERE kind='close_merged_issue'"
        ).fetchall()
        for row in rows:
            old = {"issue": json.loads(row[1])["issue"], "merges": result["pull_requests"]}
            db.execute(
                "UPDATE execution_effects SET request_json=?,request_digest=? "
                "WHERE issue_id=? AND effect_key=?",
                (
                    json.dumps(old, sort_keys=True, separators=(",", ":")),
                    digest(old),
                    "I_feature",
                    row[0],
                ),
            )
    assert (
        merger.merge(
            store,
            spec,
            requested,
            command,
            gh=gh,
            execute=lambda *_args, **_kwargs: pytest.fail("must not resubmit"),
        )
        == result
    )
