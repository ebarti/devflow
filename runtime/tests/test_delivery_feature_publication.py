from __future__ import annotations

import json
from copy import deepcopy

import pytest
from test_delivery_feature_execution import feature_service
from test_delivery_feature_merge import Remote, setup_feature
from test_delivery_store import service as legacy_service

from devflow_temporal import delivery_feature_publication as publications
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import registry
from devflow_temporal.delivery_github_contract import GitHubDelivery

service = legacy_service


def test_publication_response_loss_retains_feature_custody_until_binding_is_confirmed(
    service,
    monkeypatch,
):
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    req = {"spec": spec, "iteration": 0, "candidate": {"id": "candidate-a"}}
    token = spec["feature_delivery"]["owner"]
    receipt = {"number": 20, "head": "1" * 40, "url": "https://github.com/example/fixture/pull/20"}

    class Broker:
        publication_may_have_effect = True

        def publish(self, *_):
            raise TimeoutError("creation response lost")

        def reconcile_publish(self, *_args, **_kwargs):
            return receipt

    broker = Broker()
    with pytest.raises(TimeoutError):
        publications.publish(store, broker, req)
    shared = registry(spec)
    with pytest.raises(OwnershipConflict, match="awaiting readback"):
        shared.stop(token, "cannot-release", {})
    monkeypatch.setattr(
        publications,
        "record_publication",
        lambda *_args, **_kw: (_ for _ in ()).throw(TimeoutError("binding response lost")),
    )
    with pytest.raises(TimeoutError):
        publications.publish(store, broker, req)
    with pytest.raises(OwnershipConflict, match="awaiting readback"):
        shared.stop(token, "still-cannot-release", {})
    monkeypatch.setattr(publications, "record_publication", lambda *_args, **_kw: receipt)
    assert publications.publish(store, broker, req) == receipt
    shared.stop(token, "settled", {})


def test_recorded_stack_is_read_by_id_and_never_searched(service, monkeypatch):
    store, spec, record, _, _, gh = setup_feature(service, monkeypatch)
    assert len(publications.live_members(spec, record, gh)) == 3
    assert [call[1] for call in gh.calls] == [
        "repos/example/fixture/stacks/42",
        "repos/example/fixture/pulls/20",
        "repos/example/fixture/pulls/21",
        "repos/example/fixture/pulls/22",
    ]
    spec["feature_worker"] = {"chunk_id": "client"}
    spec["branch"] = "feat/client"
    monkeypatch.setattr(GitHubDelivery, "api", gh.api)
    assert publications.owned_pr_number(spec) == 21


def test_stack_append_uses_exact_bound_stack_and_preserves_plan(service, monkeypatch):
    store, spec, record, _, _, gh = setup_feature(service, monkeypatch)
    final = record["manifest"]["publication"]["members"].pop()
    spec["feature_worker"] = {"chunk_id": final["chunk_id"]}
    spec["branch"], spec["publication_base_ref"] = final["branch"], final["base_branch"]
    monkeypatch.setattr(publications, "require_execution", lambda *_: None)
    original = gh.api
    calls = []
    stack = [20, 21]

    def api(path, *, method="GET", body=None):
        calls.append((method, path))
        if path.endswith("/stacks/42/add"):
            stack.extend(body["pull_requests"])
            return {}
        if path.endswith("/stacks/42"):
            return {"number": 42, "pull_requests": [{"number": number} for number in stack]}
        if path.endswith("/pulls/22"):
            saved = deepcopy(record)
            saved["manifest"]["publication"]["members"].append(final)
            return Remote(saved).api(path)
        return original(path, method=method, body=body)

    def update(_issue, old, updated, *_):
        assert digest(old["manifest"]["plan"]) == digest(updated["plan"])
        record["manifest"] = deepcopy(updated)
        return deepcopy(record)

    gh.api, gh.update = api, update
    receipt = {key: final[key] for key in ("number", "url", "head")}
    assert publications.record_publication(store, spec, receipt, gh) == receipt
    assert stack == [20, 21, 22]
    assert ("POST", "repos/example/fixture/stacks/42/add") in calls
    assert all("?" not in path for _, path in calls)
    assert publications.record_publication(store, spec, receipt, gh) == receipt
    assert stack == [20, 21, 22]


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


def test_old_worker_is_fenced_before_broker_effect_after_exact_plan_adoption(tmp_path):
    from test_delivery_github_plan_records import converted, legacy_publication, setup

    remote, issue, shared, token = setup(tmp_path)
    original = legacy_publication(remote, issue, shared, token)
    worker = record_spec(issue, original, shared, token, worker=True)
    revised = remote.publish_plan_revision(issue, original, converted(original), shared, token,
                                           operation_id="adopted-correction")
    # Remote publication is not local adoption; the old execution cannot accept it.
    with pytest.raises(OwnershipConflict, match="changed after execution acceptance"):
        publications.current_record(worker, remote)
    identity = {"plan_revision": revised["manifest"]["plan_revision"],
                "plan_digest": revised["manifest"]["plan_digest"],
                "comment_id": revised["comment_id"], "comment_node_id": revised["comment_node_id"],
                "workstream_issues": revised["manifest"]["workstream_issues"]}
    shared.checkpoint(token, "plan-revision:adopted:2",
                      {"identity": identity, "plan": revised["manifest"]["plan"]})
    remote.calls.clear()
    with pytest.raises(OwnershipConflict, match="superseded plan revision"):
        publications.current_record(worker, remote)
    assert remote.calls == []

    class Broker:
        def publish(self, *_):
            raise AssertionError("a stale worker must not originate a publication effect")

    with pytest.raises(OwnershipConflict, match="superseded plan revision"):
        publications.publish(None, Broker(), {"spec": worker, "iteration": 4,
                                              "candidate": {"id": "same-original-candidate"}})
    resumed = deepcopy(worker)
    resumed["feature_plan_revision"] = {
        key: identity[key] for key in ("plan_revision", "plan_digest")}
    assert publications.current_record(resumed, remote) == revised


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


def test_current_remote_publication_head_survives_revision_after_admission_snapshot_advanced(
    tmp_path,
):
    from test_delivery_github_plan_records import converted, legacy_publication, setup

    remote, issue, shared, token = setup(tmp_path)
    snapshot_record = legacy_publication(remote, issue, shared, token)
    spec = record_spec(issue, snapshot_record, shared, token)
    advanced = deepcopy(snapshot_record["manifest"])
    advanced["revision"] += 1
    advanced["publication"]["members"][0]["head"] = "b" * 40
    remote.update(issue, snapshot_record, advanced, shared, token)
    current = publications.current_record(spec, remote)
    assert current["manifest"]["publication"]["members"][0]["head"] == "b" * 40
    revised = remote.publish_plan_revision(issue, current, converted(current), shared, token,
                                           operation_id="retain-current-head")
    assert revised["manifest"]["publication"]["members"][0]["head"] == "b" * 40
    assert snapshot_record["manifest"]["publication"]["members"][0]["head"] == "a" * 40
