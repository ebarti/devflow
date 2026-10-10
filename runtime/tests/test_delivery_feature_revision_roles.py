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
from devflow_temporal.delivery_feature_revision_roles import (
    assessment_schema,
    canonical_plan,
    revision_output,
    validate_diagnostic,
)
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.role_runner import _task, feature_intake_schema

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


def test_fresh_v2_intake_schema_does_not_change_historical_plan_contract(feature):
    store, spec, _, _, _ = feature
    candidate = DeliveryBroker(store, spec).candidate()
    old_task = _task(request_for(spec, candidate, "intake"))
    old_schema = feature_intake_schema()["properties"]["plan"]
    assert "steps" in old_schema["required"] and "verification" in old_schema["required"]
    old_chunk = old_schema["properties"]["workstreams"]["items"]["properties"]["chunks"]["items"]
    assert "allowed_paths" in old_chunk["required"]
    assert "planning_defect" not in old_task.output_schema["properties"]
    fresh = {**spec, "feature_plan_version": 2}
    task = _task(request_for(fresh, candidate, "intake"))
    schema = task.output_schema["properties"]["plan"]
    chunk = schema["properties"]["workstreams"]["items"]["properties"]["chunks"]["items"]
    assert {"version", "final_gates"} <= set(schema["required"])
    assert {"expected_paths", "gates"} <= set(chunk["required"])
    assert "allowed_paths" not in chunk["properties"]
    assert "future chunk's not-yet-created test" in task.goal
    assert "complete final-feature coverage" in task.goal
    assert "Execution authority" in task.goal


def test_review_binds_exact_proposal_and_rejects_unsupported_diagnostics(feature):
    _, spec, _, record, diagnostic = feature
    value = proposed(record)
    request = {"spec": spec, "role": "review", "revision_context": {"proposed_plan": value}}
    passing = {"status": "pass", "summary": "Acceptance and final gates retained", "findings": [],
               "reviewed_plan_sha256": digest(value)}
    assert revision_output(request, passing) == passing
    with pytest.raises(ValueError, match="exact proposal"):
        revision_output(request, {**passing, "reviewed_plan_sha256": "0" * 64})
    with pytest.raises(ValueError, match="exact proposal"):
        revision_output(request, {**passing, "findings": ["missing final gate"]})
    with pytest.raises(ValueError, match="exact structured"):
        validate_diagnostic({"detail": "a test failed"})
    with pytest.raises(ValueError, match="another candidate"):
        validate_diagnostic(diagnostic, candidate_id="a-different-worker")
    base = {"properties": {"status": {}}, "required": ["status"]}
    assert "planning_defect" in assessment_schema(base)["required"]
    assert base["required"] == ["status"]
    # Provider strict nullable fields normalize before digest/review binding.
    nullable = deepcopy(value)
    nullable["final_gates"] = [{"stage": "checks", "recipe_id": "current", "selectors": [],
                                 "reason": None}]
    normalized = canonical_plan(nullable)
    assert "reason" not in normalized["final_gates"][0]
    assert nullable["final_gates"][0]["reason"] is None


async def test_persisted_proposal_lost_completion_reuses_finished_native_attempt(
    feature, monkeypatch,
):
    store, spec, shared, _, diagnostic = feature
    context = revisions.begin_revision(store, spec, diagnostic)
    candidate = DeliveryBroker(store, spec).candidate()
    request = {"spec": spec, "context": context, "candidate": candidate}
    first = await asyncio.wait_for(activities.delivery_feature_revision_propose(request), 20)
    assert first["status"] == "plan" and first["cleanup"] == "confirmed", first
    with store._connect() as db:
        before = [dict(row) for row in db.execute("SELECT * FROM delivery_attempts")]
    assert len(before) == 1 and before[0]["state"] == "finished"
    saved_result = json.loads(Path(before[0]["result_path"]).read_text())
    assert saved_result["session_id"] == first["session_id"]

    async def duplicate_launch(*_args, **_kwargs):
        raise AssertionError("persisted native attempt must never be relaunched")

    with monkeypatch.context() as retry:
        retry.setattr("devflow_temporal.supervisor.asyncio.create_subprocess_exec",
                      duplicate_launch)
        second = await activities.delivery_feature_revision_propose(deepcopy(request))
    assert second == first
    assert shared.budget(spec["feature_delivery"]["owner"]["issue_id"])["used"] == 1
    with store._connect() as db:
        assert [dict(row) for row in db.execute("SELECT * FROM delivery_attempts")] == before
    review = await asyncio.wait_for(activities.delivery_feature_revision_review({
        **request, "context": {**context, "proposed_plan": first["plan"]},
        "proposal_session_id": first["session_id"],
    }), 20)
    assert review["status"] == "pass" and review["cleanup"] == "confirmed"
    assert review["candidate_id"] == candidate["id"]
    assert review["reviewed_plan_sha256"] == digest(first["plan"])
    assert review["session_id"] != first["session_id"]
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 2
    assert DeliveryBroker(store, spec).candidate() == candidate


def test_unaffected_cancelled_parallel_worker_reserves_original_session_after_adoption(
    feature, monkeypatch,
):
    from test_delivery_store import _git

    from devflow_temporal.contracts import canonical_json
    from devflow_temporal.delivery_feature_execution import (
        register_worker,
        require_execution,
        worker_key,
        worker_spec,
    )
    from devflow_temporal.delivery_github_contract import ordered_chunks

    store, spec, shared, record, diagnostic = feature
    token = spec["feature_delivery"]["owner"]
    chunk = next(c for c in ordered_chunks(record["manifest"]["plan"]) if c["id"] == "client")
    child = worker_spec(spec, chunk, record["manifest"]["workstream_issues"]["ui"],
                        kind="build", base_sha=spec["base_sha"], base_branch="main")
    register_worker(store, spec, child)
    broker = DeliveryBroker(store, child)
    broker.prepare()
    (broker.checkout / "README.md").write_text("Preserved incomplete client work\n")
    _git(broker.checkout, "add", "README.md")
    _git(broker.checkout, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
         "commit", "-qm", "Incomplete client fixture")
    candidate = broker.candidate()
    native_result = {"status": "findings", "summary": "Continue preserved client behavior",
                     "findings": ["client behavior needs repair"], "cleanup": "confirmed",
                     "session_id": "original-parallel-client-session"}
    state = {"run_id": child["run_id"], "phase": "cancelled", "execution_state": "terminal",
             "outcome": "cancelled", "cleanup": "confirmed", "revision": 7, "iteration": 0,
             "candidate": candidate, "candidate_revision": 1, "pull_request": None,
             "decision": None, "error": "Stopped at original revision checkpoint", "usage": {},
             "findings": ["client behavior needs repair"], "checks": {}, "tracker": {},
             "roles": [{**native_result, "role": "implement", "iteration": 0,
                        "candidate": candidate}]}
    store.project(child["run_id"], phase="cancelled", execution_state="terminal",
                  event_type="cancelled", message=state["error"], candidate=candidate,
                  checks={}, iteration=0, protocol_revision=7, outcome="cancelled",
                  cleanup="confirmed", error=state["error"])
    with store._connect() as db:
        db.execute("INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,"
                   "state,session_id,result_json,cleanup) VALUES(?,?, 'implement',0,?,'finished',"
                   "?,?, 'confirmed')", ("client-original", child["run_id"], candidate["id"],
                                         native_result["session_id"],
                                         canonical_json(native_result)))
        store.state.release_work(db, child["work_id"], "external:devflow:" + child["run_id"])
    shared.finish_worker(token, worker_key(child["run_id"], token), {"cleanup": "confirmed"})
    shared.checkpoint(token, "assignment:client:build", {
        "chunk_id": "client", "kind": "build", "run_id": child["run_id"],
        "store_path": str(store.config.tracking_db)})
    closed = {"workflow_id": "delivery-" + child["run_id"], "execution_run_id": "original-closed",
              "request_digest": child["request_digest"], "recovery_digest": None, "result": state}
    monkeypatch.setattr(DeliveryStore, "_completed_temporal_result",
                        lambda *_a, **_k: deepcopy(closed))
    monkeypatch.setattr(DeliveryBroker, "_existing_pr", lambda *_a, **_k: None)
    monkeypatch.setattr(DeliveryStore, "_ensure_no_remote_pr", lambda *_a: None)
    context = revisions.begin_revision(store, spec, diagnostic)
    value = proposed(record)
    revisions.record_proposal(store, spec, context["revision_id"], value)
    class Publisher:
        def publish_plan_revision(self, _issue, previous, plan, *_a, **_k):
            result = deepcopy(previous)
            result["manifest"].update(version=2, plan_revision=2, revision=2, plan=plan)
            return result
    review = {"status": "pass", "cleanup": "confirmed", "findings": [],
              "reviewed_plan_sha256": digest(value), "candidate_id": context["candidate_id"]}
    adopted = revisions.adopt_revision(store, spec, context["revision_id"], value, review,
                                      gh=Publisher())
    assert "client" not in adopted["affected_chunks"]
    published = {**record, "manifest": {**record["manifest"], "version": 2,
                                       "plan_revision": 2, "revision": 2, "plan": value}}
    monkeypatch.setattr(activities, "current_record", lambda *_a: deepcopy(published))
    # Real reserve dispatch must refresh the stale identity before attempting a
    # normal cancelled-worker continuation. No worker/session replacement occurs.
    reserved = activities.reserve(adopted["spec"], "client", "build")
    resumed = reserved["spec"]
    assert reserved["resumed"] and not reserved["completed"]
    for key in ("run_id", "work_id", "branch", "checkout", "state_dir", "base_sha",
                "accepted_plan"):
        assert resumed[key] == child[key]
    assert resumed["feature_plan_revision"] == adopted["spec"]["feature_plan_revision"]
    assert resumed["policy"]["allowed_paths"] == child["policy"]["allowed_paths"]
    require_execution(store, resumed)
    with pytest.raises(revisions.OwnershipConflict, match="superseded feature plan"):
        require_execution(store, child)
    input_key = "worker-input:" + worker_key(child["run_id"], token)
    saved_inputs = shared.checkpoints(token["issue_id"])
    assert saved_inputs[input_key] == child
    assert saved_inputs[input_key + ":revision:2"] == resumed
    assert revisions.resume_revision_worker(store, adopted["spec"], child)["spec"] == resumed
    with store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id=?",
                                         (child["run_id"],)).fetchone()[0])
        assert db.execute("SELECT COUNT(*) FROM delivery_runs").fetchone()[0] == 2
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 1
    assert recovery["session_id"] == native_result["session_id"]
    assert recovery["execution_candidate"]["id"] == candidate["id"]
    assert recovery["custody_ref"]
    assert len(json.dumps(recovery)) < 48_000
    assert shared.budget(token["issue_id"])["used"] == 1


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
        monkeypatch.setattr("devflow_temporal." + module + ".live_members", lambda *_a: [])
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
