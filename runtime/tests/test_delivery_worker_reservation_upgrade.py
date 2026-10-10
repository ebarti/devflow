"""Resume interrupted historical reservations without rewriting worker authority."""

from __future__ import annotations

import json
from copy import deepcopy

import pytest
from test_delivery_feature_execution import feature_service
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_configured_resources import implementation_prerequisites
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import (
    register_worker,
    registry,
    worker_key,
    worker_spec,
)
from devflow_temporal.delivery_github_contract import ordered_chunks


def interrupted(service, monkeypatch, kind, *, row):
    old, request = service
    prerequisite = {"id": "python-worker", "kind": "check", "cwd": ".",
                    "argv": ["uv", "sync", "--locked"],
                    "generated_directories": ["worker/.venv"]}
    old.config.raw["repositories"]["fixture"].update(
        prepublish_checks=[prerequisite], baseline_check_ids=[prerequisite["id"]])
    old.config.raw["execution_mode"] = "trusted-local"
    old.config.path.write_text(json.dumps(old.config.raw))
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    parent = store.effective_spec(request["run_id"])
    # Fake providers omit the native mode field; model the admitted trusted mode
    # while keeping all reservations and immutable worker inputs real.
    parent["policy"]["host_sandbox"] = "trusted-local"
    parent["policy_digest"] = digest(parent["policy"])
    plan = json.loads(request["accepted_plan"])
    chunk = ordered_chunks(plan)[0]
    issue = {"url": "https://github.com/example/fixture/issues/10"}
    shared = registry(parent)
    token = parent["feature_delivery"]["owner"]
    seed = {"head": parent["base_sha"], "run_id": "preserved-build"} if kind == "chunk" else None
    if seed:
        shared.checkpoint(token, "build:" + chunk["id"], seed)
    historical = worker_spec(parent, chunk, issue, kind=kind, base_sha=parent["base_sha"],
                             base_branch=parent["publication_base_ref"], seed=seed)
    # The historical constructor omitted the catalogue, but retained all other
    # frozen policy and identity fields. Admit it through the real registry/store.
    historical["policy"].pop("baseline_checks", None)
    historical["policy_digest"] = digest(historical["policy"])
    if row:
        register_worker(store, parent, historical)
    else:
        key = worker_key(historical["run_id"], token)
        shared.reserve_worker(token, key, chunk["workstream_id"])
        shared.checkpoint(token, "worker-input:" + key, historical)
    record = {"manifest": {"plan": plan, "publication": {"members": []},
                           "workstream_issues": {chunk["workstream_id"]: issue}}}
    monkeypatch.setattr(activities, "_feature_context", lambda _: (store, None, shared, token))
    monkeypatch.setattr(activities, "current_record", lambda _: deepcopy(record))
    fetched = []

    def git(_root, *args):
        fetched.append(args)
        return parent["base_sha"] if args == ("rev-parse", "FETCH_HEAD") else ""

    monkeypatch.setattr(activities, "_git", git)
    return store, parent, chunk, historical, shared, fetched


@pytest.mark.parametrize("kind", ["build", "chunk"])
@pytest.mark.parametrize("row", [False, True])
def test_partial_historical_reservation_reuses_exact_sealed_input(service, monkeypatch, kind, row):
    store, parent, chunk, historical, shared, _ = interrupted(service, monkeypatch, kind, row=row)
    issue_id = parent["feature_delivery"]["owner"]["issue_id"]
    before = shared.checkpoints(issue_id)
    assert "baseline_checks" not in historical["policy"]
    assert [c["id"] for c in implementation_prerequisites(historical)] == ["python-worker"]
    original = store.submitted_spec(historical["run_id"]) if row else None
    result = activities.reserve(parent, chunk["id"], kind)
    assert result["spec"] == historical
    assert store.submitted_spec(historical["run_id"]) == historical
    if original:
        assert original == store.submitted_spec(historical["run_id"])
    after = shared.checkpoints(issue_id)
    assert all(after[key] == value for key, value in before.items())
    assert after[f"assignment:{chunk['id']}:{kind}"]["run_id"] == historical["run_id"]
    assert activities.reserve(parent, chunk["id"], kind)["spec"] == historical
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_runs").fetchone()[0] == 2


@pytest.mark.parametrize("change", ["base", "scope", "owner", "seed", "digest"])
def test_partial_reservation_rejects_other_authority_changes(service, monkeypatch, change):
    store, parent, chunk, historical, shared, _ = interrupted(
        service, monkeypatch, "chunk", row=True)
    original_constructor = activities.worker_spec

    def changed(*args, **kwargs):
        child = original_constructor(*args, **kwargs)
        if change == "base":
            child["base_sha"] = "f" * 40
        elif change == "scope":
            child["policy"]["allowed_paths"] = ["unrelated.py"]
            child["policy_digest"] = digest(child["policy"])
        elif change == "owner":
            child["feature_delivery"]["owner"]["generation"] += 1
        elif change == "seed":
            child["feature_worker"]["seed"]["head"] = "f" * 40
        else:
            child["request_digest"] = "f" * 64
        return child

    monkeypatch.setattr(activities, "worker_spec", changed)
    issue_id = parent["feature_delivery"]["owner"]["issue_id"]
    before = shared.checkpoints(issue_id)
    with pytest.raises(OwnershipConflict, match="checkpoint identity already records different work"
                       ):
        activities.reserve(parent, chunk["id"], "chunk")
    assert shared.checkpoints(issue_id) == before
    assert store.submitted_spec(historical["run_id"]) == historical


@pytest.mark.parametrize("drift", ["missing_config", "changed_config", "repository"])
def test_worker_prerequisites_require_original_configuration(service, monkeypatch, drift):
    store, _, _, child, _, _ = interrupted(service, monkeypatch, "build", row=True)
    if drift == "missing_config":
        store.config.path.unlink()
    elif drift == "changed_config":
        raw = json.loads(store.config.path.read_text())
        raw["repositories"]["fixture"]["baseline_check_ids"] = []
        store.config.path.write_text(json.dumps(raw))
    else:
        child["repository_key"] = "unrelated"
    with pytest.raises((OSError, ValueError, KeyError)):
        implementation_prerequisites(child)


def test_worker_prerequisites_intersect_whole_recipes_without_changing_policy(service, monkeypatch):
    _, _, _, child, _, _ = interrupted(service, monkeypatch, "build", row=True)
    frozen = deepcopy(child)
    assert [c["id"] for c in implementation_prerequisites(child)] == ["python-worker"]
    assert child == frozen
    child["policy"]["prepublish_checks"][0]["argv"].append("--different-authority")
    assert implementation_prerequisites(child) == []
    child = deepcopy(frozen)
    child["policy"]["baseline_checks"] = []
    assert implementation_prerequisites(child) == []
    child = deepcopy(frozen)
    child.pop("feature_worker")
    assert implementation_prerequisites(child) == []
