"""Pure Git/SQLite/stub coverage; native and Temporal qualification runs in CI."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from copy import deepcopy
from pathlib import Path

import pytest
from test_delivery_execution_registry import installed_runtime as installed_runtime
from test_delivery_github_contract import plan
from test_delivery_store import service as legacy_service
from test_delivery_store import _git

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


def test_admission_claims_remote_identity_and_never_creates_business_features(service, monkeypatch):
    store, request, _ = feature_service(service, monkeypatch)
    assert not store.submit(request)["existing"]
    spec = store.effective_spec(request["run_id"])
    assert spec["feature_delivery"]["owner"]["issue_id"] == "I_feature"
    assert "feature_plan_version" not in spec
    assert spec["policy"]["max_repairs"] == 10
    assert store.submit(request)["run_id"] == spec["run_id"]
    with registry(spec).connect() as db:
        snapshot = json.loads(
            db.execute("SELECT input_json FROM execution_snapshots").fetchone()[0]
        )
    assert snapshot["issue"]["body"] == "Human scope"


def test_admission_reads_github_before_holding_the_tracking_write_lock(service, monkeypatch):
    store, request, snapshot = feature_service(service, monkeypatch)
    observed = []

    def github_snapshot(*_args):
        probe = sqlite3.connect(store.config.tracking_db, timeout=0)
        try:
            probe.execute("BEGIN IMMEDIATE")
            probe.rollback()
        except sqlite3.OperationalError as exc:
            raise AssertionError("GitHub read while the tracking write lock was held") from exc
        finally:
            probe.close()
        observed.append(True)
        return deepcopy(snapshot)

    monkeypatch.setattr(GitHubDelivery, "snapshot", github_snapshot)
    assert not store.submit(request)["existing"]
    assert observed == [True]
    spec = store.effective_spec(request["run_id"])
    assert spec["feature_delivery"]["snapshot"]["issue"]["id"] == "I_feature"
    assert spec["feature_delivery"]["owner"]["run_id"] == request["run_id"]


def test_refused_admission_never_reads_github(service, monkeypatch):
    from devflow_temporal.delivery_settings import save_repository_access

    legacy, legacy_request = service
    legacy.submit(legacy_request)  # legacy custody of issue 3 predates the feature run
    store, request, _ = feature_service(service, monkeypatch)
    monkeypatch.setattr(GitHubDelivery, "snapshot",
                        lambda *_args: pytest.fail("GitHub read for a refused admission"))
    with pytest.raises(OwnershipConflict, match="legacy execution/publication custody"):
        store.submit({**request, "run_id": "run-2", "command_id": "command-2",
                      "work_id": "work-2"})
    save_repository_access(store, {"expected_revision": 0, "allowed_repositories": []})
    with pytest.raises(ValueError, match="disabled in Settings"):
        store.submit({**request, "run_id": "run-3", "command_id": "command-3",
                      "work_id": "work-3",
                      "issue_url": "https://github.com/example/fixture/issues/5"})



def test_new_feature_intake_adopts_v2_without_broadening_legacy_authority(service, monkeypatch):
    store, request, _ = feature_service(service, monkeypatch)
    raw = deepcopy(store.config.raw)
    raw["roles"]["intake"] = {"model": "fixture", "effort": "low"}
    store.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(store.config.path))
    request.pop("accepted_plan")
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    assert spec["intake_required"] is True and spec["feature_plan_version"] == 2
    assert spec["policy"]["allowed_paths"] == ["README.md"]
    assert "source_scope" not in spec["policy"]
    assert store.config.public_policy()["feature_plan_versions"] == [1, 2]


def test_explicit_source_scope_is_frozen_independently_from_expected_files(service, monkeypatch):
    store, request, _ = feature_service(service, monkeypatch)
    raw = deepcopy(store.config.raw)
    repository = raw["repositories"]["fixture"]
    repository.pop("allowed_paths")
    scope = {"version": 1, "allowed_roots": ["."], "allowed_files": [],
             "protected_paths": ["protected.txt", ".git", ".codex"]}
    repository["source_scope"] = deepcopy(scope)
    store.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(store.config.path))
    value = json.loads(request["accepted_plan"])
    value.update(version=2, final_gates=[])
    for stream in value["workstreams"]:
        for chunk in stream["chunks"]:
            chunk["expected_paths"] = chunk.pop("allowed_paths")
            chunk["gates"] = []
    request["accepted_plan"] = json.dumps(value)
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    assert spec["policy"]["source_scope"] == scope
    assert spec["policy"]["allowed_paths"] == []
    child = worker_spec(spec, ordered_chunks(value)[0],
                        {"url": "https://github.com/example/fixture/issues/10"},
                        kind="build", base_sha=spec["base_sha"], base_branch="main")
    assert child["policy"]["source_scope"] == scope and child["expected_paths"] == ["README.md"]
    from devflow_temporal.delivery_source_scope import outside_scope

    assert outside_scope(child["policy"], ["src/new.py", "protected.txt"]) == {"protected.txt"}

def test_invalid_plan_does_not_leave_an_orphan_execution_claim(service, monkeypatch):
    store, request, _ = feature_service(service, monkeypatch)
    with pytest.raises(ValueError):
        store.submit({**request, "accepted_plan": "not a structured feature plan"})
    from devflow_temporal.delivery_execution_registry import ExecutionRegistry
    from devflow_temporal.delivery_feature_execution import registry_path

    assert ExecutionRegistry(registry_path(store.config)).current("I_feature") is None
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_runs").fetchone()[0] == 0


@pytest.mark.parametrize("moved_base", [False, True])
def test_successor_requires_stopped_owner_and_preserves_cumulative_budget(
    service, monkeypatch, moved_base,
):
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
    if moved_base:
        source = Path(spec["source_path"])
        (source / "README.md").write_text("Current trunk after retained feature integration\n")
        _git(source, "commit", "-am", "Advance the configured named base")
        assert _git(source, "rev-parse", spec["base_ref"]) != spec["base_sha"]
        # A caller-supplied predecessor never relaxes fresh config admission.
        with pytest.raises(ValueError, match="base ref moved"):
            store.config.admit(next_request)
        with pytest.raises(OwnershipConflict, match="original owner custody"):
            store.submit({**next_request, "feature_predecessor": {**token, "generation": 99}})
    store.submit(next_request)
    successor = store.effective_spec("run-next")
    assert successor["feature_delivery"]["owner"]["generation"] == 2
    assert successor["base_sha"] == spec["base_sha"]
    assert successor["base_ref"] == spec["base_ref"]
    assert successor["accepted_plan"] == spec["accepted_plan"]
    assert successor["policy"] == spec["policy"]
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
    shared.drain(token)
    with pytest.raises(OwnershipConflict, match='active coordinator'):
        require_execution(store, child)
    require_execution(store, child, terminal_settlement=True)
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


def test_supported_continuation_renews_only_a_new_payload_after_safe_handoff(
    service, monkeypatch, installed_runtime,
):
    from devflow_temporal.delivery_feature_execution import continue_feature

    runtime = installed_runtime
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    initial = store.effective_spec(request["run_id"])
    shared = registry(initial)
    for number in range(10):
        shared.repair(initial["feature_delivery"]["owner"], f"repair-{number}", "product repair")
    with store._connect() as db:
        db.execute("UPDATE delivery_dashboard_state SET identity_json=? WHERE run_id=?",
                   (json.dumps({**runtime.baseline, "dirty": False, "local_digest": None}),
                    initial["run_id"]))
    # The service and worker fixtures remain fake; only the coordinator's native
    # continuation selector is exercised here, without launching a native process.
    submitted = store.submitted_spec

    def native_coordinator(run_id):
        spec = submitted(run_id)
        return {**spec, "provider": "codex"} if not spec.get("feature_worker") else spec

    monkeypatch.setattr(store, "submitted_spec", native_coordinator)
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_: {"result": {}})

    def close(spec, key):
        shared.stop(spec["feature_delivery"]["owner"], key, {})
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET phase='cancelled',outcome='cancelled',"
                       "execution_state='terminal' WHERE run_id=?", (spec["run_id"],))
            store.state.release_work(db, spec["work_id"], "external:devflow:" + spec["run_id"])

    close(initial, "same-runtime-stop")
    first = continue_feature(store, initial["run_id"],
                             {"command_id": "same-runtime", "expected_revision": 1})
    assert (shared.budget("I_feature")["used"],
            shared.budget("I_feature")["allowance"]["sequence"]) == (10, 0)
    parent = store.effective_spec(first["run_id"])
    runtime.advance(2)
    command = {"command_id": "new-install", "expected_revision": 1}
    with pytest.raises(OwnershipConflict, match="stopped checkpoint"):
        continue_feature(store, parent["run_id"], command)
    assert shared.budget("I_feature")["used"] == 10
    close(parent, "new-runtime-stop")
    original_config = store.config
    refused = deepcopy(original_config.raw)
    refused["repositories"]["fixture"]["origin_url"] += ".changed"
    original_config.path.write_text(json.dumps(refused))
    store.config = DeliveryConfig.load(original_config.path)
    with pytest.raises(ValueError, match="configured Git origin changed"):
        continue_feature(store, parent["run_id"], {**command, "command_id": "refused-origin"})
    assert shared.budget("I_feature")["used"] == 10
    assert shared.budget("I_feature")["allowance"]["sequence"] == 0
    original_config.path.write_text(json.dumps(original_config.raw))
    store.config = original_config
    second = continue_feature(store, parent["run_id"], command)
    budget = shared.budget("I_feature")
    assert (budget["used"], budget["lifetime_used"], budget["allowance"]["sequence"]) == (0, 10, 1)
    # An exact replay returns before identity/authentication, even if the service
    # was subsequently restarted with a dirty source tree.
    (runtime.root / "README.md").write_text("Uncommitted fixture change\n")
    assert continue_feature(store, parent["run_id"], command) == second
    assert shared.budget("I_feature") == budget
    with pytest.raises(OwnershipConflict, match="safe handoff"):
        continue_feature(store, parent["run_id"], {**command, "command_id": "alternate-command"})
    assert shared.budget("I_feature") == budget
