from __future__ import annotations

import json
from copy import deepcopy

import pytest
from test_delivery_feature_merge import setup_feature
from test_delivery_store import service as legacy_service

from devflow_temporal import delivery_feature_publication as publications
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import registry

service = legacy_service


def test_integration_updates_lower_pr_in_place_with_a_new_pass(service, monkeypatch):
    store, spec, record, _, _, gh = setup_feature(service, monkeypatch)
    members = record["manifest"]["publication"]["members"]
    original = deepcopy(members)
    shared, token = registry(spec), spec["feature_delivery"]["owner"]
    shared.checkpoint(token, "integration-pass:1", {
        "number": 1, "target": "f" * 40, "members": original,
    })
    spec["feature_worker"] = {
        "chunk_id": "model", "integration_pass": 1, "previous_publication": original[0],
    }
    spec["branch"], spec["publication_base_ref"] = original[0]["branch"], "main"
    monkeypatch.setattr(publications, "require_execution", lambda *_: None)

    def update(_issue, old, updated, *_):
        record["manifest"] = deepcopy(updated)
        return deepcopy(record)

    gh.update = update
    receipt = {"number": 20, "url": original[0]["url"], "head": "a" * 40}
    assert publications.record_publication(store, spec, receipt, gh) == receipt
    assert record["manifest"]["publication"]["stack_id"] == 42
    assert record["manifest"]["publication"]["members"][0]["head"] == "a" * 40
    assert record["manifest"]["publication"]["members"][1:] == original[1:]
    assert all(method == "GET" for method, _, _ in gh.calls)
    assert publications.record_publication(store, spec, receipt, gh) == receipt
    # The same integration worker can repair its freshly published lower layer
    # after QA, while all older pass evidence and higher PR identities remain.
    with store._connect() as db:
        db.execute("INSERT INTO delivery_effects(effect_key,run_id,kind,request_json,state,"
                   "observed_json,updated_at) VALUES (? ,?,'publish','{}','complete',?,'now')",
                   ("previous-integration-publish", spec["run_id"], json.dumps(receipt)))
    repaired = {**receipt, "head": "b" * 40}
    assert publications.record_publication(store, spec, repaired, gh) == repaired
    assert record["manifest"]["publication"]["members"][0]["head"] == "b" * 40


def record_spec(issue, record, shared, token, *, worker=False):
    spec = {
        "run_id": "original-worker" if worker else "original-run",
        "github_repo": issue["repository"], "issue_url": issue["url"],
        "accepted_plan": json.dumps(record["manifest"]["plan"]),
        "policy": {"allowed_paths": ["model.py", "endpoint.py", "client.py"]},
        "feature_delivery": {"registry": str(shared.path), "owner": token,
                             "snapshot": {"issue": issue, "delivery": record}},
    }
    if worker:
        spec["feature_worker"] = {"chunk_id": "model", "workstream_id": "api"}
    return spec


def test_v2_chunk_publication_records_actual_child_and_rejects_a_different_worker(
    tmp_path, monkeypatch,
):
    from test_delivery_github_plan_records import setup
    from test_delivery_plan_model import v2_plan

    remote, issue, shared, token = setup(tmp_path)
    record = remote.initialize(issue, v2_plan(), shared, token)
    spec = record_spec(issue, record, shared, token, worker=True)
    spec.update(issue_url="https://github.com/owner/repo/issues/2", branch="feat/model",
                publication_base_ref="main")
    receipt = {"number": 1076, "url": "https://github.com/owner/repo/pull/1076", "head": "a" * 40}
    original_api = remote.api

    def api(endpoint, *, method="GET", body=None):
        if endpoint.endswith("/pulls/1076"):
            return {"number": 1076, "html_url": receipt["url"], "draft": False, "merged": False,
                    "head": {"sha": receipt["head"], "ref": "feat/model",
                             "repo": {"full_name": "owner/repo"}},
                    "base": {"ref": "main", "repo": {"full_name": "owner/repo"}}}
        return original_api(endpoint, method=method, body=body)

    remote.api = api
    monkeypatch.setattr(publications, "require_execution", lambda *_: None)
    invalid = deepcopy(spec)
    invalid["feature_worker"]["workstream_id"] = "ui"
    remote.calls.clear()
    with pytest.raises(OwnershipConflict, match="actual child issue"):
        publications.record_publication(None, invalid, receipt, remote)
    assert not any(method != "GET" for method, _ in remote.calls)
    assert publications.record_publication(None, spec, receipt, remote) == receipt
    member = remote.load_record(issue, record)["manifest"]["publication"]["members"][0]
    assert {key: member[key] for key in ("chunk_id", "workstream_id", "issue_id",
                                        "issue_number", "issue_url")} == {
        "chunk_id": "model", "workstream_id": "api", "issue_id": "I_2", "issue_number": 2,
        "issue_url": "https://github.com/owner/repo/issues/2"}
