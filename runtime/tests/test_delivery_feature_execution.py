"""Pure Git/SQLite/stub coverage; native and Temporal qualification runs in CI."""

from __future__ import annotations

import asyncio
import json
from copy import deepcopy

import pytest
from test_delivery_execution_registry import installed_runtime as installed_runtime
from test_delivery_github_contract import plan
from test_delivery_store import service as legacy_service

from devflow_temporal import delivery_feature_workflow as protocol
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import (
    registry,
)
from devflow_temporal.delivery_github_contract import GitHubDelivery
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
    for number, stream in enumerate(value["workstreams"], 10):
        stream["issue_number"] = number
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


def test_invalid_plan_does_not_leave_an_orphan_execution_claim(service, monkeypatch):
    store, request, _ = feature_service(service, monkeypatch)
    with pytest.raises(ValueError):
        store.submit({**request, "accepted_plan": "not a structured feature plan"})
    from devflow_temporal.delivery_execution_registry import ExecutionRegistry
    from devflow_temporal.delivery_feature_execution import registry_path

    assert ExecutionRegistry(registry_path(store.config)).current("I_feature") is None
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_runs").fetchone()[0] == 0


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
            return {"state": "confirmed", "pull_requests": [
                {"number": item["number"]} for item in request["publication"]["pull_requests"]
            ]}
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
            publication["members"].append({"chunk_id": chunk_id, "number": len(integrated)})
            if len(publication["members"]) > 1:
                publication["stack_id"] = 42
            controller.record["manifest"]["revision"] += 1
        return {
            "outcome": "delivered",
            "record": deepcopy(controller.record),
            "budget": {"used": 0},
        }

    monkeypatch.setattr(protocol, "_worker", worker)
    # These are legacy protocol stubs, not a Temporal replay environment.
    monkeypatch.setattr(protocol.workflow, "patched", lambda _marker: False)
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
    # These are legacy protocol stubs, not a Temporal replay environment.
    monkeypatch.setattr(protocol.workflow, "patched", lambda _marker: False)
    result = await protocol.coordinate(
        controller, {"run_id": "run-one", "authorized_endpoint": "merged"}
    )
    assert result["outcome"] == "cancelled"
    assert "chunk" not in kinds
    assert not any(name == "delivery_feature_merge" for name, _ in controller.events)
