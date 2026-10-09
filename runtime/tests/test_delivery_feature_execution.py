"""Pure Git/SQLite/stub coverage; native and Temporal qualification runs in CI."""

from __future__ import annotations

import asyncio
import json
from copy import deepcopy

import pytest
from test_delivery_github_contract import plan
from test_delivery_store import service as legacy_service

from devflow_temporal import delivery_feature_workflow as protocol
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import (
    register_worker,
    registry,
    require_execution,
    worker_key,
    worker_spec,
)
from devflow_temporal.delivery_features import _status
from devflow_temporal.delivery_github_contract import GitHubDelivery, ordered_chunks
from devflow_temporal.delivery_store import DeliveryStore

service = legacy_service


def feature_service(service, monkeypatch):
    old, request = service
    raw = deepcopy(old.config.raw)
    raw.update(
        feature_delivery_version=1,
        execution_registry=str(old.config.path.parent / "ownership" / "registry.sqlite3"),
    )
    old.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(old.config.path))
    value = plan()
    for stream in value["workstreams"]:
        for chunk in stream["chunks"]:
            chunk["allowed_paths"] = ["README.md"]
    issue = {
        "id": "I_feature",
        "repository_id": "R_fixture",
        "repository": "example/fixture",
        "number": 3,
        "url": request["issue_url"],
        "labels": [],
        "body": "Human scope",
    }
    snapshot = {"issue": issue, "workstreams": [], "delivery": None, "default_branch": "main"}
    monkeypatch.setattr(GitHubDelivery, "snapshot", lambda *_args: deepcopy(snapshot))
    request = {**request, "accepted_plan": json.dumps(value)}
    return store, request, snapshot


def test_admission_claims_remote_identity_and_never_creates_business_features(service, monkeypatch):
    store, request, _ = feature_service(service, monkeypatch)
    assert not store.submit(request)["existing"]
    spec = store.effective_spec(request["run_id"])
    assert spec["feature_delivery"]["owner"]["issue_id"] == "I_feature"
    assert spec["policy"]["max_repairs"] == 10
    assert store.submit(request)["run_id"] == spec["run_id"]
    with registry(spec).connect() as db:
        snapshot = json.loads(
            db.execute("SELECT input_json FROM execution_snapshots").fetchone()[0]
        )
    assert snapshot["issue"]["body"] == "Human scope"


def test_invalid_plan_does_not_leave_an_orphan_execution_claim(service, monkeypatch):
    store, request, _ = feature_service(service, monkeypatch)
    with pytest.raises(ValueError):
        store.submit({**request, "accepted_plan": "not a structured feature plan"})
    from devflow_temporal.delivery_execution_registry import ExecutionRegistry
    from devflow_temporal.delivery_feature_execution import registry_path

    assert ExecutionRegistry(registry_path(store.config)).current("I_feature") is None
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_runs").fetchone()[0] == 0


def test_successor_requires_stopped_owner_and_preserves_cumulative_budget(service, monkeypatch):
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    shared = registry(spec)
    token = spec["feature_delivery"]["owner"]
    shared.repair(token, "cycle-1", "first repair")
    with pytest.raises(OwnershipConflict):
        shared.claim(
            {"issue": spec["feature_delivery"]["snapshot"]["issue"]},
            "run-next",
            str(store.config.tracking_db),
            predecessor=token,
        )
    shared.stop(token, "done-1", {})
    with store._connect() as db:
        store.state.release_work(db, spec["work_id"], "external:devflow:" + spec["run_id"])
    next_request = {
        **request,
        "run_id": "run-next",
        "command_id": "next-command",
        "feature_predecessor": token,
    }
    store.submit(next_request)
    successor = store.effective_spec("run-next")
    assert successor["feature_delivery"]["owner"]["generation"] == 2
    assert shared.budget("I_feature")["used"] == 1
    with pytest.raises(OwnershipConflict):
        require_execution(store, spec)


def test_workers_are_hidden_from_feature_run_list_and_have_narrow_fresh_authority(
    service, monkeypatch
):
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    parent = store.effective_spec(request["run_id"])
    parent["preparation"] = {"old": "proof"}
    parent["policy"]["security_binding_sha256"] = "old"
    chunk = ordered_chunks(json.loads(request["accepted_plan"]))[0]
    child = worker_spec(
        parent,
        chunk,
        {"url": "https://github.com/example/fixture/issues/10"},
        kind="build",
        base_sha=parent["base_sha"],
        base_branch="main",
    )
    assert "preparation" not in child
    assert "security_binding_sha256" not in child["policy"]
    assert child["policy_digest"] == digest(child["policy"])
    assert child["authorized_endpoint"] == "published_unmerged"
    assert child["feature_worker"]["kind"] == "build"
    register_worker(store, parent, child)
    assert [row["id"] for row in store.list_runs_page()["runs"]] == [parent["run_id"]]
    assert {row["run_id"] for row in store.pending_starts()} == {parent["run_id"], child["run_id"]}
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_features").fetchone()[0] == 1
    require_execution(store, child)
    shared = registry(parent)
    token = parent["feature_delivery"]["owner"]
    shared.finish_worker(token, worker_key(child["run_id"], token), {"cleanup": "confirmed"})
    with pytest.raises(OwnershipConflict, match="active execution assignment"):
        require_execution(store, child)


def test_partial_stack_merge_cannot_mark_unfinished_feature_merged():
    row = {
        "request_json": json.dumps({"feature_delivery": {"version": 1}}),
        "pr_json": json.dumps({"scope_complete": False}),
        "phase": "feature_implementation",
        "outcome": None,
    }
    observed = [{"observation": {"state": "MERGED"}}]
    assert _status(row, observed) == "Partially merged"
    row["pr_json"] = json.dumps({"scope_complete": True})
    assert _status(row, observed) == "Merged"


def test_public_continuation_replays_the_same_command_and_retains_ownership(service, monkeypatch):
    from devflow_temporal.delivery_feature_execution import continue_feature

    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    token = spec["feature_delivery"]["owner"]
    registry(spec).stop(token, "stopped", {})
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET phase='cancelled',outcome='cancelled',"
                   "execution_state='terminal' WHERE run_id=?", (spec["run_id"],))
        store.state.release_work(db, spec["work_id"], "external:devflow:" + spec["run_id"])
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_: {"result": {}})
    command = {"command_id": "continue-command", "expected_revision": 1}
    admitted = continue_feature(store, spec["run_id"], command)
    assert continue_feature(store, spec["run_id"], command) == admitted
    child = store.effective_spec(admitted["run_id"])
    assert child["feature_delivery"]["owner"]["generation"] == 2
    assert child["issue_url"] == spec["issue_url"]
    assert child["accepted_plan"] == spec["accepted_plan"]
    with pytest.raises(OwnershipConflict):
        continue_feature(store, spec["run_id"], {**command, "expected_revision": 2})


def test_stop_activity_reconciles_its_own_lost_completion(service, monkeypatch):
    from devflow_temporal.delivery_feature_activities import stop_feature

    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    checkpoint = {"state": {"outcome": "cancelled"}, "completed_chunks": []}
    first = stop_feature(spec, checkpoint)
    assert stop_feature(spec, checkpoint) == first
    with pytest.raises(OwnershipConflict):
        require_execution(store, spec)


def test_legacy_custody_import_fences_cross_database_replacement(service, monkeypatch):
    from devflow_temporal.delivery_feature_migration import migrate

    store, request, snapshot = feature_service(service, monkeypatch)
    # A historical legacy receipt is deliberately not converted into a stack.
    store.config.raw.pop("feature_delivery_version")
    store.config.path.write_text(json.dumps(store.config.raw))
    store.submit(request)
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET outcome='blocked',pr_json=?",
                   (json.dumps({"number": 20, "url": "https://github.com/example/fixture/pull/20"}),))
    assert migrate([store.config])["issues_with_legacy_custody"] == 1
    from devflow_temporal.delivery_execution_registry import ExecutionRegistry
    from devflow_temporal.delivery_feature_execution import registry_path

    shared = ExecutionRegistry(registry_path(store.config))
    with pytest.raises(OwnershipConflict, match="legacy publication custody"):
        shared.claim(snapshot, "replacement-run", str(store.config.path.parent / "other.sqlite3"))


def test_workstream_project_projection_uses_parent_owner_and_bindings(service, monkeypatch):
    from devflow_temporal.delivery_features import transition
    from devflow_temporal.delivery_project_sync import ProjectSynchronizer

    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    shared, token = registry(spec), spec["feature_delivery"]["owner"]
    shared.checkpoint(token, "workstream-issues", {
        "api": {"id": "I_10", "number": 10, "url": "https://github.com/example/fixture/issues/10"},
        "ui": {"id": "I_11", "number": 11, "url": "https://github.com/example/fixture/issues/11"},
    })
    with store._connect() as db:
        transition(db, store.config, spec["run_id"])
    consumer = object.__new__(ProjectSynchronizer)
    consumer.stores = [store]
    selected = consumer.selected()
    assert len(selected) == 3
    assert selected["https://github.com/example/fixture/issues/10"][1]["status"] == "Queued"
    assert all(value["execution_owner"] == token for _, value in selected.values())


class Controller:
    def __init__(self, value):
        self.cancel_requested = False
        self.state = {"checks": {}, "revision": 1, "candidate_revision": 1}
        self.record = {
            "manifest": {
                "revision": 1,
                "plan": value,
                "publication": {"stack_id": None, "members": []},
            }
        }
        self.events = []

    async def _project(self, spec, event, message):
        self.events.append((event, deepcopy(self.state)))

    async def _activity(self, name, request):
        self.events.append((name, deepcopy(request)))
        if name == "delivery_feature_open":
            return {"record": deepcopy(self.record), "checkpoints": {}, "budget": {"used": 0}}
        if name == "delivery_feature_merge":
            assert request["publication"]["scope_complete"]
            return {"state": "confirmed"}
        return {"state": "confirmed"}

    async def _stop(self, spec, message, **kwargs):
        self.state.update(outcome="blocked", phase="blocked", error=message)
        return self.state

    async def _cancelled(self, spec):
        self.state.update(outcome="cancelled", phase="cancelled")
        return self.state


@pytest.mark.asyncio
async def test_independent_builds_overlap_but_integration_follows_the_shared_stack(monkeypatch):
    controller = Controller(plan())
    started, integrated = [], []
    both = asyncio.Event()

    async def worker(_controller, spec, chunk_id, kind, active):
        if kind == "build":
            started.append(chunk_id)
            if "model" in started and "client" in started:
                both.set()
            if chunk_id in {"model", "client"}:
                await both.wait()
        else:
            integrated.append(chunk_id)
            publication = controller.record["manifest"]["publication"]
            publication["members"].append({"chunk_id": chunk_id})
            if len(publication["members"]) > 1:
                publication["stack_id"] = 42
            controller.record["manifest"]["revision"] += 1
        return {
            "outcome": "delivered",
            "record": deepcopy(controller.record),
            "budget": {"used": 0},
        }

    monkeypatch.setattr(protocol, "_worker", worker)
    result = await protocol.coordinate(
        controller, {"run_id": "run-one", "authorized_endpoint": "merged"}
    )
    assert started == ["model", "client", "endpoint"]
    assert integrated == ["model", "client", "endpoint"]
    assert result["phase"] == "merged"
    assert any(name == "delivery_feature_stop" for name, _ in controller.events)


@pytest.mark.asyncio
async def test_cancel_during_build_does_not_start_integration_or_merge(monkeypatch):
    controller = Controller(plan())
    kinds = []

    async def worker(_controller, spec, chunk_id, kind, active):
        kinds.append(kind)
        controller.cancel_requested = True
        return {"outcome": "delivered"}

    monkeypatch.setattr(protocol, "_worker", worker)
    result = await protocol.coordinate(
        controller, {"run_id": "run-one", "authorized_endpoint": "merged"}
    )
    assert result["outcome"] == "cancelled"
    assert "chunk" not in kinds
    assert not any(name == "delivery_feature_merge" for name, _ in controller.events)
