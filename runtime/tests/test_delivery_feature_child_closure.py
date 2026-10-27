"""Exercise merge and projection obligations for a child with several required chunks."""

from __future__ import annotations

import json

import pytest
from test_delivery_feature_merge import previously_submitted, setup_feature
from test_delivery_store import service as legacy_service

from devflow_temporal import delivery_feature_merge as merger
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import registry

service = legacy_service


def test_child_closure_intents_name_only_its_required_confirmed_chunks(service, monkeypatch):
    store, spec, record, requested, command, gh = setup_feature(service, monkeypatch)
    previously_submitted(store, spec, record, requested, command)
    gh.merged = {20, 21, 22}
    commands = []

    def execute(argv, **_kwargs):
        commands.append(argv)
        pytest.fail("must reconcile the retained stack submission")

    result = merger.merge(store, spec, requested, command, gh=gh, execute=execute)
    assert result["state"] == "confirmed" and result["stack_id"] == 42
    assert commands == []
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
    assert commands == []


@pytest.mark.parametrize(
    "field,value",
    [("workstream_id", "ui"), ("issue_url", "https://github.com/example/fixture/issues/11")],
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
