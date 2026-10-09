"""Recovery boundaries using only Git, SQLite and in-memory remote transports."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest
from test_delivery_feature_execution import Controller, feature_service
from test_delivery_feature_merge import setup_feature
from test_delivery_store import service as legacy_service

from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal import delivery_feature_merge as merger
from devflow_temporal import delivery_feature_publication as publications
from devflow_temporal import delivery_feature_workflow as protocol
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_broker import DeliveryBroker, _git
from devflow_temporal.delivery_execution_registry import ExecutionRegistry, OwnershipConflict
from devflow_temporal.delivery_feature_execution import (
    continue_feature,
    register_worker,
    registry,
    worker_spec,
)
from devflow_temporal.delivery_feature_pass import checkpoint_key, checkpoints
from devflow_temporal.delivery_feature_readback import settle
from devflow_temporal.delivery_github_contract import GitHubDelivery, ordered_chunks

service = legacy_service


@pytest.mark.parametrize("change", ["binding", "parent"])
def test_merge_rechecks_admitted_workstream_identity_and_parent(service, monkeypatch, change):
    store, spec, record, requested, command, gh = setup_feature(service, monkeypatch)
    if change == "binding":
        record["manifest"]["workstream_issues"]["api"] = {
            "id": "I_999", "number": 999, "url": "https://github.com/example/fixture/issues/999",
        }
    else:
        api = gh.api
        monkeypatch.setattr(gh, "api", lambda path, **kw: {"node_id": "I_other_feature"}
                            if path.endswith("/parent") else api(path, **kw))
    with pytest.raises(OwnershipConflict,
                       match="bindings changed|hierarchy changed|accepted issue"):
        merger.merge(store, spec, requested, command, gh=gh,
                     execute=lambda *_args, **_kw: pytest.fail("must stop before merging"))
    assert not gh.closed


def test_seal_build_replays_exact_patch_after_checkpoint_acknowledgement_loss(service, monkeypatch):
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    chunk = ordered_chunks(json.loads(request["accepted_plan"]))[0]
    child = worker_spec(spec, chunk, {"url": "https://github.com/example/fixture/issues/10"},
                        kind="build", base_sha=spec["base_sha"], base_branch="main")
    register_worker(store, spec, child)
    broker = DeliveryBroker(store, child)
    broker.prepare()
    (broker.checkout / "README.md").write_text("Complete implementation checkpoint\n")
    candidate = broker.candidate()
    monkeypatch.setattr(activities, "_context", lambda _: (store, broker))
    original = ExecutionRegistry.checkpoint

    def interrupted(self, token, key, value):
        if key == "build:model":
            raise TimeoutError("checkpoint acknowledgement lost")
        return original(self, token, key, value)

    monkeypatch.setattr(ExecutionRegistry, "checkpoint", interrupted)
    with pytest.raises(TimeoutError):
        activities.seal_build(child, candidate)
    patch = (Path(child["state_dir"]) / "feature-implementation.patch").read_bytes()
    monkeypatch.setattr(ExecutionRegistry, "checkpoint", original)
    saved = activities.seal_build(child, candidate)
    assert activities.seal_build(child, candidate) == saved
    assert Path(saved["path"]).read_bytes() == patch


async def test_lost_plan_write_settles_before_public_successor_is_admitted(service, monkeypatch):
    store, request, snapshot = feature_service(service, monkeypatch)
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    comments, events = [], []

    class LostResponse(GitHubDelivery):
        unavailable = True

        def issue(self, *_):
            return deepcopy(snapshot["issue"])

        def api(self, endpoint, *, method="GET", body=None):
            assert method == "POST" and endpoint.endswith("/issues/3/comments")
            comments.append({"id": 11, "node_id": "IC_11", "body": body["body"]})
            raise TimeoutError("remote comment exists; acknowledgement lost")

        def pages(self, endpoint):
            assert endpoint.endswith("/issues/3/comments")
            if self.unavailable:
                self.unavailable = False
                raise ConnectionError("temporary readback outage")
            return iter(comments)

    gh = LostResponse()
    monkeypatch.setattr(activities, "GitHubDelivery", lambda: gh)

    class RecoveryController(Controller):
        async def _activity(self, name, payload):
            if name == "delivery_feature_open":
                return activities.open_feature(spec)
            if name == "delivery_feature_settle_workers":
                registry(spec).drain(spec["feature_delivery"]["owner"])
                return {"state": "confirmed"}
            if name == "delivery_feature_settle_effects":
                return settle(store, spec, gh=gh)
            if name == "delivery_feature_stop":
                return activities.stop_feature(spec, payload["checkpoint"])
            raise AssertionError(name)

        async def _project(self, spec, event, message):
            events.append(event)
            with store._connect() as db:
                db.execute("UPDATE delivery_runs SET phase=?,outcome=?,revision=? WHERE run_id=?",
                           (self.state["phase"], self.state.get("outcome"),
                            self.state["revision"], spec["run_id"]))

        async def _stop(self, spec, message, **kwargs):
            await super()._stop(spec, message, **kwargs)
            self.state["revision"] += 1
            await self._project(spec, "blocked", message)

    async def local_sleep(_):
        return None

    monkeypatch.setattr(protocol.workflow, "sleep", local_sleep)
    controller = RecoveryController(json.loads(request["accepted_plan"]))
    result = await protocol.coordinate(controller, spec)
    assert len(comments) == 1 and "feature_readback_pending" in events
    assert registry(spec).current("I_feature")["state"] == "stopped"
    assert registry(spec).budget("I_feature")["used"] == 0
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_: {"result": result})
    successor = continue_feature(store, spec["run_id"], {
        "command_id": "continue-original-plan", "expected_revision": result["revision"],
    })
    assert store.effective_spec(successor["run_id"])["feature_delivery"]["owner"]["generation"] == 2


def test_integration_pass_retains_evidence_and_imports_without_force(service, monkeypatch):
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    shared, token = registry(spec), spec["feature_delivery"]["owner"]
    source = Path(spec["source_path"])
    chunk = ordered_chunks(json.loads(request["accepted_plan"]))[0]
    # Obtain the canonical branch from the normal worker derivation.
    original = worker_spec(spec, chunk, {"url": "https://github.com/example/fixture/issues/10"},
                           kind="chunk", base_sha=spec["base_sha"], base_branch="main")
    old_branch = original["branch"]
    _git(source, "checkout", "-b", old_branch)
    (source / "README.md").write_text("The retained feature contribution\n")
    _git(source, "commit", "-am", "feat: original chunk", "--signoff")
    old_head = _git(source, "rev-parse", "HEAD")
    _git(source, "push", "origin", old_branch)
    _git(source, "checkout", "-B", "main", spec["base_sha"])
    (source / "unrelated.txt").write_text("New trunk contribution\n")
    _git(source, "add", "unrelated.txt")
    _git(source, "commit", "-m", "feat: advance trunk", "--signoff")
    target = _git(source, "rev-parse", "HEAD")
    _git(source, "push", "origin", "main")
    member = {"chunk_id": "model", "number": 20, "branch": old_branch, "head": old_head,
              "base_branch": "main", "url": "https://github.com/example/fixture/pull/20"}
    shared.checkpoint(token, "verified:model", {"head": old_head, "run_id": "original"})
    shared.checkpoint(token, "integration-pass:1", {
        "number": 1, "target": target, "members": [member],
    })
    assert "verified:model" not in checkpoints(spec)
    child = worker_spec(spec, chunk, {"url": "https://github.com/example/fixture/issues/10"},
                        kind="chunk", base_sha=target, base_branch="main")
    assert child["branch"] == old_branch and child["local_branch"] != old_branch
    assert child["run_id"] != original["run_id"]
    register_worker(store, spec, child)
    broker = DeliveryBroker(store, child)
    broker.prepare()
    assert (broker.checkout / "README.md").read_text() == "The retained feature contribution\n"
    assert (broker.checkout / "unrelated.txt").read_text() == "New trunk contribution\n"
    _git(broker.checkout, "commit", "-m", "feat: integrate updated target", "--signoff")
    assert _git(broker.checkout, "merge-base", "--is-ancestor", old_head, "HEAD") == ""
    assert _git(broker.checkout, "merge-base", "--is-ancestor", target, "HEAD") == ""
    _git(broker.checkout, "push", "origin", "HEAD:refs/heads/" + old_branch)
    shared.checkpoint(token, checkpoint_key(child, "verified:model"), {"head": "new-proof"})
    assert checkpoints(spec)["verified:model"]["head"] == "new-proof"
    assert shared.checkpoints("I_feature")["verified:model"]["head"] == old_head


async def test_successor_reintegrates_completed_stack_after_target_moves(service, monkeypatch):
    from types import SimpleNamespace

    store, original, record, _, _, gh = setup_feature(service, monkeypatch)
    shared, old_token = registry(original), original["feature_delivery"]["owner"]
    members = record["manifest"]["publication"]["members"]
    specifications = {}
    for member in members:
        run_id = "original-" + member["chunk_id"]
        specifications[run_id] = {"base_sha": original["base_sha"]}
        shared.checkpoint(old_token, "verified:" + member["chunk_id"], {
            "run_id": run_id, "head": member["head"], "number": member["number"],
        })
    shared.stop(old_token, "previous-stopped", {})
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET phase='cancelled',outcome='cancelled' WHERE run_id=?",
                   (original["run_id"],))
        store.state.release_work(db, original["work_id"], "external:devflow:" + original["run_id"])
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_: {"result": {}})
    successor = continue_feature(store, original["run_id"], {
        "command_id": "continue-after-target-moved", "expected_revision": 1,
    })
    spec = store.effective_spec(successor["run_id"])
    assert spec["feature_delivery"]["owner"]["generation"] == 2
    effective = store.effective_spec
    monkeypatch.setattr(store, "effective_spec", lambda run: specifications[run]
                        if run in specifications else effective(run))
    target = "f" * 40
    api = gh.api

    def remote(path, **kwargs):
        value = api(path, **kwargs)
        if "/pulls/" in path:
            value["base"]["sha"] = target
        return value

    monkeypatch.setattr(gh, "api", remote)
    monkeypatch.setattr(publications, "GitHubDelivery", lambda: gh)
    integrations, commands = [], []

    def execute(argv, **kwargs):
        commands.append(argv)
        gh.merged = {20, 21, 22}
        return SimpleNamespace(returncode=0)

    class IntegrationController(Controller):
        async def _project(self, spec, event, message):
            with store._connect() as db:
                db.execute("UPDATE delivery_runs SET phase=?,pr_json=? WHERE run_id=?", (
                    self.state["phase"], json.dumps(self.state["pull_request"]), spec["run_id"],
                ))

        async def _activity(self, name, payload):
            if name == "delivery_feature_open":
                return {"record": deepcopy(record), "checkpoints": checkpoints(spec), "budget": {}}
            if name == "delivery_feature_merge":
                return merger.merge(store, spec, payload["publication"], payload["authorization"],
                                    gh=gh, execute=execute)
            if name == "delivery_feature_begin_integration":
                return activities.begin_integration(spec, payload["target"])
            return {"state": "confirmed"}

    async def worker(controller, spec, chunk_id, kind, active):
        if kind == "chunk":
            integrations.append(chunk_id)
            member = next(item for item in members if item["chunk_id"] == chunk_id)
            member["head"] = "a" * 39 + str(len(integrations))
            record["manifest"]["revision"] += 1
            run_id = "integrated-" + chunk_id
            specifications[run_id] = {"base_sha": target}
            shared.checkpoint(spec["feature_delivery"]["owner"],
                              checkpoint_key(spec, "verified:" + chunk_id), {
                                  "run_id": run_id, "head": member["head"],
                                  "number": member["number"],
                              })
        return {"outcome": "delivered", "record": deepcopy(record), "budget": {}}

    monkeypatch.setattr(protocol, "_worker", worker)
    controller = IntegrationController(record["manifest"]["plan"])

    async def explicit_merge(predicate):
        decision = controller.state["decision"]
        command = {
            "command_id": "merge-pass-" + str(decision["revision"]), "answer": "merge",
            "expected_revision": controller.state["revision"], "decision_id": decision["id"],
            "decision_revision": decision["revision"],
            "candidate_revision": decision["candidate_revision"],
        }
        with store._connect() as db:
            db.execute("INSERT INTO delivery_mutations"
                       "(command_id,run_id,kind,request_digest,state) "
                       "VALUES (?,?,'decision',?,'pending')",
                       (command["command_id"], spec["run_id"], digest(command)))
        controller.feature_merge_authorization = command
        controller.decision_answer = "merge"
        assert predicate()

    monkeypatch.setattr(protocol.workflow, "wait_condition", explicit_merge)
    result = await protocol.coordinate(controller, spec)
    assert result["phase"] == "merged", result.get("error")
    assert integrations == ["model", "client", "endpoint"]
    assert len(commands) == 1 and commands[0][3] == "42"
    assert [member["number"] for member in members] == [20, 21, 22]
    assert shared.checkpoints("I_feature")["verified:model"]["run_id"] == "original-model"
    assert checkpoints(spec)["verified:model"]["run_id"] == "integrated-model"
    assert shared.budget("I_feature")["used"] == 1
