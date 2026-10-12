"""Stack-merge evidence of a chunk resumed without a fresh implementation."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest
from test_delivery_feature_execution import feature_service
from test_delivery_feature_revisions import feature as feature
from test_delivery_feature_revisions import proposed
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal import delivery_feature_merge as merger
from devflow_temporal import delivery_feature_revisions as revisions
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_feature_execution import (
    register_worker,
    registry,
    worker_key,
    worker_spec,
)
from devflow_temporal.delivery_feature_pass import checkpoint_key
from devflow_temporal.delivery_github_contract import ordered_chunks
from devflow_temporal.delivery_resources import write_private
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.supervisor import DeliverySupervisor

LOST = "merge implementation lost its controller-bound native output"


def codex_receipt(store, spec, *, source, output, session="original-implementation"):
    """The supervisor's durable receipt for a codex implementation run under ``spec``."""
    request = {"spec": spec, "role": "implement", "iteration": 0,
               "candidate": {"id": source["id"]}, "workspace": spec["checkout"]}
    key = DeliverySupervisor._job_key(request)
    root = Path(spec["state_dir"]) / "attempts" / key
    assessment = {"status": "pass", "session_id": session, "findings": []}
    outcome = {"state": "finished", "monitoring_complete": True,
               "cleanup": "observed-native-confirmed", "journal": str(root / "native-process.json")}
    saved = {**assessment, "cleanup": "confirmed", "process_cleanup": "observed-native-confirmed",
             "resource_cleanup": "pending_workflow_finalization", "native_process": outcome}
    journal = {"phase": "finished", "owned": {"123": {"identity": "observed"}},
               "intent": {"run_id": spec["run_id"], "policy_digest": spec["policy_digest"],
                          "cwd": spec["checkout"]}, "result": outcome,
               "provider_session": {"role": "implement", "iteration": 0, "session_id": session,
                                    "output_candidate": {key: output[key] for key in
                                                         ("id", "head", "content_sha256")},
                                    "result_digest": digest(saved)}}
    for name, value in (("request.json", request), ("result.json", assessment),
                        ("native-process.json", journal)):
        write_private(root / name, value)
    with store._connect() as db:
        db.execute("INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,"
                   "state,session_id,result_json,cleanup,result_path) "
                   "VALUES (?,?,'implement',0,?,'finished',?,?,'confirmed',?)",
                   (key, spec["run_id"], source["id"], session, canonical_json(saved),
                    str(root / "result.json")))
    return {**saved, "role": "implement", "iteration": 0, "candidate": output,
            "input_candidate_id": source["id"], "provider": spec["provider"]}


def cancelled(store, monkeypatch, parent, child, role, output):
    """Close the worker's execution as cancelled after its implementation passed."""
    state = {"run_id": child["run_id"], "phase": "cancelled", "execution_state": "terminal",
             "outcome": "cancelled", "cleanup": "confirmed", "revision": 7, "iteration": 0,
             "candidate": output, "candidate_revision": 1, "pull_request": None,
             "decision": None, "error": None, "usage": {}, "findings": [], "checks": {},
             "tracker": {}, "roles": [role]}
    store.project(child["run_id"], phase="cancelled", execution_state="terminal",
                  event_type="cancelled", message="Feature coordinator stopped",
                  candidate=output, checks={}, iteration=0, protocol_revision=7,
                  outcome="cancelled", cleanup="confirmed", error=None)
    with store._connect() as db:
        store.state.release_work(db, child["work_id"], "external:devflow:" + child["run_id"])
    token = parent["feature_delivery"]["owner"]
    registry(parent).finish_worker(token, worker_key(child["run_id"], token),
                                   {"cleanup": "confirmed"})
    closed = {"workflow_id": "delivery-" + child["run_id"], "execution_run_id": "closed",
              "request_digest": child["request_digest"], "recovery_digest": None, "result": state}
    monkeypatch.setattr(DeliveryStore, "_completed_temporal_result",
                        lambda *_a, **_k: deepcopy(closed))
    monkeypatch.setattr(DeliveryBroker, "_existing_pr", lambda *_a, **_k: None)
    monkeypatch.setattr(DeliveryStore, "_ensure_no_remote_pr", lambda *_a: None)


def merge_evidence(monkeypatch, store, parent, child, output):
    """Deliver the resumed execution's gates and read the real stack-merge evidence."""
    pr = {"number": 21, "url": "https://github.com/example/fixture/pull/21", "state": "OPEN",
          "head": output["head"], "base": child["base_sha"]}
    checks = {"prepublish": {"state": "passed", "candidate_id": output["id"]}}
    with store._connect() as db:
        db.execute("INSERT INTO delivery_effects(effect_key,run_id,kind,request_json,state,"
                   "observed_json,updated_at) VALUES (?,?,'publish',?,'complete',?,?)",
                   (f"publish:{child['run_id']}:0", child["run_id"],
                    canonical_json({"iteration": 0, "input_candidate_id": output["id"]}),
                    canonical_json(pr), "2026-10-12T00:00:00+00:00"))
    store.project(child["run_id"], phase="delivered", execution_state="terminal",
                  event_type="delivered", message="Chunk verified", candidate=output,
                  pull_request=pr, checks=checks, iteration=0, protocol_revision=20,
                  outcome="delivered", cleanup="confirmed", error=None)
    chunk_id = child["feature_worker"]["chunk_id"]
    registry(parent).checkpoint(parent["feature_delivery"]["owner"],
                                checkpoint_key(parent, "verified:" + chunk_id), {
        "run_id": child["run_id"], "head": pr["head"], "number": pr["number"],
        "store_path": str(store.config.tracking_db), "checks_digest": digest(checks)})
    plan = json.loads(parent["accepted_plan"])
    plan["workstreams"] = [{**stream, "chunks": [{**chunk, "depends_on": []} for chunk in
                                                 stream["chunks"] if chunk["id"] == chunk_id]}
                           for stream in plan["workstreams"]
                           if any(chunk["id"] == chunk_id for chunk in stream["chunks"])]
    member = {"chunk_id": chunk_id, "number": pr["number"], "head": pr["head"],
              "base_branch": "main"}
    record = {"manifest": {"plan": plan, "publication": {"members": [member]}}}
    seen = {}

    def gates(_spec, _candidate, _pr, _checks, attempts, _publication):
        seen["output"] = [item for item in attempts
                          if item["role"] == "implement"][-1]["controller_output_candidate"]

    class Broker:
        # GitHub CI and base enforcement are outside the implementation binding.
        def __init__(self, *_args):
            pass

        def live_ci(self, *_args):
            pass

        def require_base_enforcement(self):
            pass

    class GitHub:
        def api(self, path, **_kwargs):
            return {"tree": {"sha": "tree-" + path.rsplit("/", 1)[-1][:8]}}

    # The codex-only merge authority and finalized resources are covered by the
    # merge gate tests; this reads the real implementation binding.
    monkeypatch.setattr(merger, "require_merge_gates", gates)
    monkeypatch.setattr(merger, "observe_finalized_resources", lambda _spec: None)
    monkeypatch.setattr(merger, "MergeBroker", Broker)
    trees = merger.evidence(store, parent, record, [{"merged": False}], GitHub())
    return trees, seen.get("output")


def implemented_chunk(store, parent, chunk, issue):
    child = worker_spec(parent, chunk, issue, kind="chunk", base_sha=parent["base_sha"],
                        base_branch="main")
    register_worker(store, parent, child)
    broker = DeliveryBroker(store, child)
    source = broker.prepare()["candidate"]
    (broker.checkout / "README.md").write_text("Complete chunk implementation\n")
    return child, source, broker.candidate()


@pytest.mark.parametrize("ran_under", ["predecessor", "foreign"])
def test_interrupted_chunk_merges_on_the_implementation_its_predecessor_ran(
    service, monkeypatch, ran_under,
):
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    parent = store.effective_spec(request["run_id"])
    chunk = ordered_chunks(json.loads(request["accepted_plan"]))[0]
    child, source, output = implemented_chunk(
        store, parent, chunk, {"url": "https://github.com/example/fixture/issues/10"})
    # A spec this run never executed under cannot bless an implementation.
    ran = child if ran_under == "predecessor" else {**child, "policy_digest": "0" * 64}
    role = codex_receipt(store, ran, source=source, output=output)
    cancelled(store, monkeypatch, parent, child, role, output)
    # Codex session custody gives the resumed execution its own role-home generation.
    monkeypatch.setattr("devflow_temporal.delivery_session_custody.implementation_custody",
                        lambda *_a: {"generation": "resume-generation"})
    resumed = activities.resume_worker(store, parent, child, {
        "error": None, "checks_json": "{}"})["spec"]
    assert resumed["implementation_role_home_generation"] == "resume-generation"
    assert resumed != child
    if ran_under == "foreign":
        with pytest.raises(ValueError, match=LOST):
            merge_evidence(monkeypatch, store, parent, child, output)
        return
    trees, bound = merge_evidence(monkeypatch, store, parent, child, output)
    assert trees == ["tree-" + output["head"][:8]]
    assert bound == {key: output[key] for key in ("id", "head", "content_sha256")}


def test_revision_gate_only_resume_merges_on_its_original_implementation(feature, monkeypatch):
    store, spec, shared, record, diagnostic = feature
    chunk = next(item for item in ordered_chunks(record["manifest"]["plan"])
                 if item["id"] == "client")
    child, source, output = implemented_chunk(store, spec, chunk,
                                              record["manifest"]["workstream_issues"]["ui"])
    role = codex_receipt(store, child, source=source, output=output)
    cancelled(store, monkeypatch, spec, child, role, output)
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
    resumed = revisions.resume_revision_worker(store, adopted["spec"], child)["spec"]
    with store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id=?",
                                         (child["run_id"],)).fetchone()[0])
    # A gate-only admission: no implementer runs again under the adopted plan.
    assert recovery["resume_stage"] == "checks" and recovery["feature_plan_revision"]
    assert resumed["feature_plan_revision"] != child.get("feature_plan_revision")
    trees, bound = merge_evidence(monkeypatch, store, adopted["spec"], child, output)
    assert trees == ["tree-" + output["head"][:8]]
    assert bound == {key: output[key] for key in ("id", "head", "content_sha256")}
