"""Compose Git imports with implementation admission, publication, and recovery."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from test_delivery_feature_execution import feature_service
from test_delivery_store import service as legacy_service

from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal import delivery_feature_publication as publications
from devflow_temporal.delivery_activities import _role_result, delivery_repair_preflight
from devflow_temporal.delivery_broker import DeliveryBroker, _git
from devflow_temporal.delivery_feature_execution import (
    register_worker,
    registry,
    worker_key,
    worker_spec,
)
from devflow_temporal.delivery_github_contract import ordered_chunks

service = legacy_service


def integration(service, monkeypatch):
    store, supplied, _ = feature_service(service, monkeypatch)
    store.submit(supplied)
    parent = store.effective_spec(supplied["run_id"])
    chunk = ordered_chunks(json.loads(supplied["accepted_plan"]))[0]
    issue = {"url": "https://github.com/example/fixture/issues/10"}
    original = worker_spec(parent, chunk, issue, kind="chunk",
                           base_sha=parent["base_sha"], base_branch="main")
    source = Path(parent["source_path"])
    _git(source, "checkout", "-b", original["branch"])
    (source / "README.md").write_text("Retained complete chunk contribution\n")
    _git(source, "commit", "-am", "feat: original chunk", "--signoff")
    old_head = _git(source, "rev-parse", "HEAD")
    _git(source, "push", "origin", original["branch"])
    _git(source, "checkout", "-B", "main", parent["base_sha"])
    (source / "unrelated.txt").write_text("Independent trunk change\n")
    _git(source, "add", "unrelated.txt")
    _git(source, "commit", "-m", "feat: advance trunk", "--signoff")
    target = _git(source, "rev-parse", "HEAD")
    _git(source, "push", "origin", "main")
    member = {"chunk_id": chunk["id"], "number": 20, "branch": original["branch"],
              "head": old_head, "base_branch": "main",
              "url": "https://github.com/example/fixture/pull/20"}
    registry(parent).checkpoint(parent["feature_delivery"]["owner"], "integration-pass:1",
                                {"number": 1, "target": target, "members": [member]})
    child = worker_spec(parent, chunk, issue, kind="chunk", base_sha=target, base_branch="main")
    register_worker(store, parent, child)
    broker = DeliveryBroker(store, child)
    prepared = broker.prepare()
    assert _git(broker.checkout, "branch", "--show-current") == child["local_branch"]
    assert (broker.checkout / "README.md").read_text() == "Retained complete chunk contribution\n"
    assert (broker.checkout / "unrelated.txt").read_text() == "Independent trunk change\n"
    return store, parent, child, broker, prepared["candidate"], member, target


def role_result(child, broker, candidate, *, iteration=0):
    # Provider receipt is modeled; Git admission and publication are real.
    return _role_result({"spec": child, "role": "implement", "iteration": iteration,
                         "candidate": candidate, "findings": [], "resume_session": None},
                        broker, broker.checkout, None,
                        {"status": "pass", "session_id": "modeled-implementation",
                         "cleanup": "confirmed", "findings": [],
                         "summary": "Imported complete chunk needs no further source edits"})


def test_clean_reintegration_qualifies_and_keeps_both_parents_on_same_pr(service, monkeypatch):
    _, _, child, broker, candidate, member, target = integration(service, monkeypatch)
    result = role_result(child, broker, candidate)
    assert result["status"] == "pass" and result["candidate"] == candidate
    monkeypatch.setattr(broker, "_publication_base_ref", lambda **_: "main")

    def owned(**_):
        tip = _git(broker.source, "ls-remote", "origin", "refs/heads/" + child["branch"]).split()[0]
        return {"number": 20, "url": member["url"], "state": "OPEN", "headRefOid": tip,
                "baseRefName": "main", "title": "feat: original chunk"}

    monkeypatch.setattr(broker, "_existing_pr", owned)
    receipt = broker.publish(0, result["candidate"])
    assert _git(broker.checkout, "merge-base", "--is-ancestor",
                member["head"], receipt["head"]) == ""
    assert _git(broker.checkout, "merge-base", "--is-ancestor", target, receipt["head"]) == ""
    assert len(_git(broker.checkout, "show", "-s", "--format=%P", receipt["head"]).split()) == 2
    assert receipt["number"] == 20


@pytest.mark.parametrize("invalid", ["later_iteration", "missing_receipt", "unrecorded_edits"])
def test_import_exception_does_not_qualify_unrelated_unchanged_candidates(
    service, monkeypatch, invalid,
):
    store, _, child, broker, candidate, _, _ = integration(service, monkeypatch)
    if invalid == "missing_receipt":
        with store._connect() as db:
            db.execute("DELETE FROM delivery_effects WHERE effect_key=?",
                       ("prepare:" + child["run_id"],))
    elif invalid == "unrecorded_edits":
        (broker.checkout / "README.md").write_text("Edits made after the preserved import\n")
        candidate = broker.candidate()
    result = role_result(child, broker, candidate,
                         iteration=1 if invalid == "later_iteration" else 0)
    assert result["status"] == "blocked"
    assert "implementer produced no candidate change" in result["findings"]


def test_initial_chunk_import_can_pass_without_rewriting_its_sealed_build(service, monkeypatch):
    store, supplied, _ = feature_service(service, monkeypatch)
    store.submit(supplied)
    parent = store.effective_spec(supplied["run_id"])
    chunk = ordered_chunks(json.loads(supplied["accepted_plan"]))[0]
    issue = {"url": "https://github.com/example/fixture/issues/10"}
    build = worker_spec(parent, chunk, issue, kind="build",
                        base_sha=parent["base_sha"], base_branch="main")
    register_worker(store, parent, build)
    builder = DeliveryBroker(store, build)
    builder.prepare()
    (builder.checkout / "README.md").write_text("The complete independently built chunk\n")
    monkeypatch.setattr(activities, "_context", lambda _: (store, builder))
    seed = activities.seal_build(build, builder.candidate())
    token = parent["feature_delivery"]["owner"]
    registry(parent).finish_worker(token, worker_key(build["run_id"], token),
                                   {"cleanup": "confirmed"})
    with store._connect() as db:
        store.state.release_work(db, build["work_id"], "external:devflow:" + build["run_id"])
    child = worker_spec(parent, chunk, issue, kind="chunk", seed=seed,
                        base_sha=parent["base_sha"], base_branch="main")
    register_worker(store, parent, child)
    broker = DeliveryBroker(store, child)
    candidate = broker.prepare()["candidate"]
    assert role_result(child, broker, candidate)["status"] == "pass"


@pytest.mark.parametrize("drift", [None, "local_branch", "remote_head", "closed_pr", "binding"])
@pytest.mark.parametrize("preparation_failed", [False, True])
def test_stopped_integration_resumes_only_its_frozen_local_and_remote_custody(
    service, monkeypatch, drift, preparation_failed,
):
    from test_delivery_gate_retry import preparation_stop

    from devflow_temporal.delivery_gate_retry import readback as gate_readback
    from devflow_temporal.delivery_stopped_resume import readback

    store, parent, child, broker, candidate, member, _ = integration(service, monkeypatch)
    record = {"manifest": {"publication": {"members": [member] if drift != "binding" else []}}}
    monkeypatch.setattr(publications, "current_record", lambda *_: record)
    monkeypatch.setattr(publications, "live_members", lambda *_: [
        {"number": member["number"], "state": "closed" if drift == "closed_pr" else "open",
         "merged": False}])
    implementation = {"role": "implement", "iteration": 0, "status": "pass",
                      "session_id": "modeled-implementation", "cleanup": "confirmed",
                      "candidate": candidate}
    state = {"run_id": child["run_id"], "phase": "blocked", "outcome": "blocked",
             "execution_state": "blocked", "cleanup": "confirmed", "revision": 9,
             "iteration": 0, "candidate": candidate, "pull_request": None,
             "roles": [implementation], "checks": {}, "error": "modeled gate failure"}
    store.project(child["run_id"], phase="blocked", execution_state="blocked", outcome="blocked",
                  cleanup="confirmed", event_type="blocked", message=state["error"],
                  candidate=candidate, pull_request=None, checks={}, iteration=0,
                  protocol_revision=9, error=state["error"])
    if preparation_failed:
        state["checks"] = {"prepublish": {"state": "failed", "source_unchanged": True,
                                         "candidate_id": candidate["id"], "results": [
                                             {"passed": False, "cleanup": "confirmed"}]}}
        preparation_stop(store, state)
        monkeypatch.setattr("devflow_temporal.delivery_gate_retry._stopped_cleanup", lambda _: {})
        monkeypatch.setattr(DeliveryBroker, "_existing_pr", lambda *_a, **_k: {
            "number": member["number"], "state": "OPEN", "isDraft": False,
            "headRefOid": member["head"]})
    with store._connect() as db:
        store.state.release_work(db, child["work_id"], "external:devflow:" + child["run_id"])
        db.execute("INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,state,"
                   "session_id,result_json,cleanup) "
                   "VALUES (?,?,'implement',0,?,'finished',?,?,'confirmed')",
                   ("modeled-finished", child["run_id"], candidate["id"],
                    implementation["session_id"],
                    json.dumps({"status": "pass", "session_id": implementation["session_id"],
                                "cleanup": "confirmed"})))
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                              (child["run_id"],)).fetchone())
    closed = {"workflow_id": row["workflow_id"], "execution_run_id": "modeled-closed",
              "request_digest": child["request_digest"], "recovery_digest": None, "result": state}
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_a, **_k: closed)
    if drift == "local_branch":
        _git(broker.checkout, "branch", "-m", "feat/unrelated-local-owner")
        error = ("retained integration publication identity changed" if preparation_failed
                 else "stopped checkout branch or origin changed")
        with pytest.raises(ValueError, match=error):
            activities.resume_worker(store, parent, child, row)
    elif drift is not None:
        if drift == "remote_head":
            _git(broker.source, "checkout", member["branch"])
            (broker.source / "README.md").write_text("Another author advanced the PR\n")
            _git(broker.source, "commit", "-am", "feat: external update", "--signoff")
            _git(broker.source, "push", "origin", member["branch"])
        with pytest.raises(ValueError, match="retained integration"):
            activities.resume_worker(store, parent, child, row)
    else:
        resumed = activities.resume_worker(store, parent, child, row)
        assert resumed["resumed"]
        assert resumed["spec"]["local_branch"] == child["local_branch"]
        assert resumed["spec"]["branch"] == child["branch"]
        assert resumed["spec"]["run_id"] == child["run_id"]
        with store._connect() as db:
            recovery = json.loads(db.execute(
                "SELECT recovery_json FROM delivery_runs WHERE run_id=?",
                (child["run_id"],)).fetchone()[0])
        if preparation_failed:
            assert recovery["kind"] == "prepublication_gate_retry"
            assert recovery["command"]["additional_iterations"] == 0
            assert recovery["state"]["iteration"] == 0
            assert gate_readback(store, resumed["spec"], recovery) is None
        else:
            assert readback(store, resumed["spec"], recovery)["state"] == "confirmed"
        _git(broker.checkout, "branch", "-m", "feat/changed-after-admission")
        with pytest.raises(ValueError, match="retained integration publication identity changed"):
            if preparation_failed:
                gate_readback(store, resumed["spec"], recovery)
            else:
                asyncio.run(delivery_repair_preflight({"spec": resumed["spec"],
                                                      "recovery": recovery}))
