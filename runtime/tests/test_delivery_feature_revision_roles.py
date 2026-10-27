"""Configured revision roles and real activity retry at the durable proposal boundary."""
from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from pathlib import Path

import pytest
from agent_runtime_kit import FilesystemAccess
from test_delivery_feature_revisions import feature as feature
from test_delivery_feature_revisions import proposed
from test_delivery_github_contract import plan as original_plan
from test_delivery_store import service as legacy_service

from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal import delivery_feature_revisions as revisions
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.role_runner import _task

legacy_fixture = legacy_service


@pytest.fixture
def service(legacy_fixture):
    store, request = legacy_fixture
    value = original_plan()
    for number, stream in enumerate(value["workstreams"], 10):
        stream["issue_number"] = number
        for chunk in stream["chunks"]:
            chunk["allowed_paths"] = ["README.md"]
    bindings = {s["id"]: {"number": s["issue_number"]} for s in value["workstreams"]}
    next_plan = proposed({"manifest": {"plan": value, "workstream_issues": bindings}})
    raw = deepcopy(store.config.raw)
    raw["roles"]["intake"] = {"model": "configured-planner", "effort": "medium"}
    raw["roles"]["review"] = {"model": "configured-independent-review", "effort": "high"}
    raw["fake_intake"] = [{"status": "plan", "summary": "Correct evidenced prerequisite",
                           "plan": next_plan}]
    store.config.path.write_text(json.dumps(raw))
    return DeliveryStore(DeliveryConfig.load(store.config.path)), request


def request_for(spec, candidate, role, context=None):
    return {"spec": spec, "role": role, "iteration": 0, "candidate": candidate,
            "workspace": spec["checkout"], "resume_session": None, "findings": [],
            **({"revision_context": context} if context else {})}


def test_revision_uses_configured_read_only_roles_and_sealed_evidence(feature):
    store, spec, _, record, diagnostic = feature
    context = revisions.begin_revision(store, spec, diagnostic)
    candidate = DeliveryBroker(store, spec).candidate()
    intake = request_for(spec, candidate, "intake", context)
    trusted = revisions.authenticate_revision_role(store, intake)
    intake["revision_context"] = trusted
    task = _task(intake)
    assert task.model == "configured-planner" and task.reasoning_effort == "medium"
    assert task.permissions.filesystem == FilesystemAccess.READ_ONLY
    assert "smallest justified correction" in task.goal
    assert "trusted_evidence" in task.goal and "later prerequisite" in task.goal
    assert "planning_defect" in task.output_schema["properties"]["diagnostic"]["anyOf"][0][
        "properties"]["kind"]["enum"]
    value = proposed(record)
    revisions.record_proposal(store, spec, context["revision_id"], value)
    review = request_for(spec, candidate, "review", {**context, "proposed_plan": value})
    review["revision_context"] = {
        **revisions.authenticate_revision_role(store, review), "proposed_plan": value,
    }
    task = _task(review)
    assert task.model == "configured-independent-review" and task.reasoning_effort == "high"
    assert task.permissions.filesystem == FilesystemAccess.READ_ONLY
    assert "original closure obligation" in task.goal
    assert "reviewed_plan_sha256" in task.output_schema["required"]
    assert "start agents" in task.goal
    native = deepcopy(review)
    native["spec"]["provider"] = "codex"
    native["spec"]["policy"]["host_sandbox"] = "trusted-local"
    assert _task(native).permissions.filesystem == FilesystemAccess.READ_ONLY


async def test_required_revision_approval_survives_cleared_decision_and_lost_proposal_completion(
    tmp_path, service, monkeypatch,
):
    import hashlib
    import shutil

    from temporalio import activity
    from temporalio.testing import WorkflowEnvironment
    from temporalio.worker import Replayer, Worker
    from test_delivery_feature_execution import feature_service

    from devflow_temporal.delivery_activities import delivery_prepare, delivery_project
    from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
    from devflow_temporal.delivery_feature_execution import registry
    from devflow_temporal.delivery_github_contract import GitHubDelivery
    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    store, request, snapshot = feature_service(service, monkeypatch)
    old = json.loads(request.pop("accepted_plan"))
    request["plan_approval"] = "required"
    record = {"comment_id": 99, "comment_node_id": "IC_99", "manifest": {
        "version": 1, "revision": 1, "plan": old,
        "workstream_issues": {s["id"]: {
            "id": "I_" + s["id"], "number": s["issue_number"],
            "url": "https://github.com/example/fixture/issues/" + str(s["issue_number"]),
        } for s in old["workstreams"]},
        "publication": {"stack_id": 42, "members": []},
    }}
    snapshot["delivery"] = record
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    assert not spec["intake_required"] and spec["plan_approval"] == "required"
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    evidence = Path(spec["state_dir"]) / "planning-evidence.json"
    evidence.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    evidence.write_text('{"finding":"a later prerequisite is required too early"}')
    diagnostic = {"version": 1, "kind": "planning_defect", "category": "dependency",
                  "chunk_id": "model", "plan_revision": 1, "plan_sha256": digest(old),
                  "candidate_id": broker.candidate()["id"], "detail": "Gate prerequisite misplaced",
                  "evidence": [{"path": str(evidence),
                                "sha256": hashlib.sha256(evidence.read_bytes()).hexdigest()}]}
    monkeypatch.setattr(GitHubDelivery, "initialize", lambda *_a: deepcopy(record))
    monkeypatch.setattr(GitHubDelivery, "workstreams", lambda *_a: deepcopy(record))
    for module in ("delivery_feature_activities", "delivery_feature_publication"):
        monkeypatch.setattr("devflow_temporal." + module + ".live_members",
                            lambda *_a, **_k: [])
    monkeypatch.setattr("devflow_temporal.delivery_feature_activities.observe_members",
                        lambda *_a, **_k: ([], []))
    monkeypatch.setattr("devflow_temporal.delivery_feature_publication.current_record",
                        lambda *_a: deepcopy(record))
    approval_at_adoption = []
    def publish(_self, _issue, _old, candidate_plan, *_a, **_kw):
        with store._connect() as db:
            row = db.execute("SELECT decision_json FROM delivery_runs WHERE run_id=?",
                             (spec["run_id"],)).fetchone()
            approval_at_adoption.append(json.loads(row[0]))
        record["manifest"].update(version=2, revision=2, plan_revision=2, plan=candidate_plan)
        return deepcopy(record)
    monkeypatch.setattr(GitHubDelivery, "publish_plan_revision", publish)
    calls = []
    def stub(name):
        @activity.defn(name=name)
        async def handler(payload):
            calls.append((name, deepcopy(payload)))
            parent = payload["spec"]
            if name == "delivery_feature_reserve":
                key = payload["kind"] + ":" + payload["chunk_id"]
                return {"spec": {"run_id": key}, "workflow_id": key, "completed": True}
            if name == "delivery_feature_finish_worker":
                kind, key = payload["child_id"].split(":")
                if key == "model" and not parent.get("feature_plan_revision"):
                    return {"outcome": "blocked", "planning_defect": diagnostic}
                if kind == "chunk":
                    members = record["manifest"]["publication"]["members"]
                    number = len(members) + 20
                    members.append({"chunk_id": key, "number": number,
                                    "url": "https://github.com/example/fixture/pull/" + str(number),
                                    "head": parent["base_sha"], "branch": "feat/" + key,
                                    "base": "main"})
                return {"outcome": "delivered", "record": deepcopy(record),
                        "budget": registry(parent).budget("I_feature")}
            if name == "delivery_feature_settle_workers":
                registry(parent).drain(parent["feature_delivery"]["owner"])
            if name == "delivery_feature_merge":
                return {"state": "confirmed",
                        "pull_requests": payload["publication"]["pull_requests"]}
            return {"state": "confirmed"}
        return handler
    proposal_completions = 0
    @activity.defn(name="delivery_feature_revision_propose")
    async def lose_proposal_completion(payload):
        nonlocal proposal_completions
        proposal_completions += 1
        result = await activities.delivery_feature_revision_propose(payload)
        if proposal_completions == 1:
            raise ConnectionError("proposal persisted; native completion acknowledgement lost")
        return result
    handlers = [delivery_prepare, delivery_project, activities.delivery_feature_open,
                activities.delivery_feature_revision_request,
                activities.delivery_feature_revision_begin, lose_proposal_completion,
                activities.delivery_feature_revision_review,
                activities.delivery_feature_revision_adopt,
                activities.delivery_feature_revision_reject]
    handlers += [stub(name) for name in (
        "delivery_feature_reserve", "delivery_feature_finish_worker", "delivery_feature_merge",
        "delivery_feature_settle_workers", "delivery_feature_settle_effects",
        "delivery_feature_stop",
    )]
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "required-approval-temporal.sqlite3"),
        data_converter=DELIVERY_DATA_CONVERTER,
    ) as environment:
        async with Worker(environment.client, task_queue="required-revision",
                          workflows=[DeliveryWorkflow], activities=handlers):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue="required-revision")
            for decision_kind, answer in (("plan_revision", "proceed"), ("merge", "merge")):
                for _ in range(600):
                    state = await handle.query(DeliveryWorkflow.status)
                    if (state.get("decision") or {}).get("kind") == decision_kind:
                        break
                    assert state.get("outcome") != "blocked", state
                    await asyncio.sleep(0.05)
                pending = state["decision"]
                command = {"command_id": "approve-fixture-" + decision_kind, "answer": answer,
                           "expected_revision": state["revision"], "decision_id": pending["id"],
                           "decision_revision": pending["revision"],
                           "candidate_revision": pending["candidate_revision"]}
                # This fake-provider fixture journals the same command receipt the
                # public native-only API saves before delivering a Temporal update.
                with store._connect() as db:
                    db.execute("INSERT INTO delivery_mutations(command_id,run_id,kind,"
                               "request_digest,state,decision_id,decision_revision) "
                               "VALUES (?,?,'decision',?,'pending',?,?)",
                               (command["command_id"], spec["run_id"], digest(command),
                                command["decision_id"], command["decision_revision"]))
                updated = await handle.execute_update(DeliveryWorkflow.decision, command)
                store.finish_mutation(command["command_id"], updated)
            result = await asyncio.wait_for(handle.result(), 40)
            history = await handle.fetch_history()
    assert result["phase"] == "merged", result.get("error")
    assert proposal_completions == 2 and approval_at_adoption == [None]
    assert result["checks"]["repair_budget"]["used"] == 1
    assert result["checks"]["plan_revision"]["identity"]["plan_revision"] == 2
    with store._connect() as db:
        attempts = [dict(row) for row in db.execute("SELECT * FROM delivery_attempts")]
        events = [json.loads(row[0]) for row in db.execute(
            "SELECT payload_json FROM delivery_events WHERE run_id=? "
            "AND type='feature_revision_approval_pending'", (spec["run_id"],))]
    assert [a["role"] for a in attempts] == ["intake", "review"]
    assert attempts[0]["session_id"] != attempts[1]["session_id"]
    assert events[0]["decision"]["plan_digest"] == digest(record["manifest"]["plan"])
    assert "delivery_feature_worker_result" not in [name for name, _ in calls]
    assert len(history.events) < 350
    await Replayer(workflows=[DeliveryWorkflow],
                   data_converter=DELIVERY_DATA_CONVERTER).replay_workflow(history)
