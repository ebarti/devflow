from __future__ import annotations

import json
from copy import deepcopy
from datetime import timedelta

import pytest
from test_delivery_store import _git, submit_historical_admission
from test_delivery_store import service as service

from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_project_sync import GitHub
from devflow_temporal.delivery_publication_validation import KIND, readback
from devflow_temporal.delivery_resources import _gate_path
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture
def publication(service, monkeypatch, tmp_path):
    store, submitted = service
    repository = store.config.raw["repositories"]["fixture"]
    repository["allowed_paths"].append("docs/validation.md")
    repository["prepublish_checks"] = [{"id": "diff", "argv": ["git", "diff", "--check"],
                                       "cwd": ".", "kind": "check", "timeout_seconds": 30}]
    repository["checks"] = repository["prepublish_checks"]
    store.config.path.write_text(json.dumps(store.config.raw))
    submit_historical_admission(store, submitted, monkeypatch)
    spec = store.spec("run-1")
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    (broker.checkout / "docs").mkdir()
    (broker.checkout / "docs/validation.md").write_text("Accepted design.\nExecution history.\n")
    _git(broker.checkout, "add", "docs/validation.md")
    _git(broker.checkout, "commit", "--signoff", "-qm", "docs: accepted design")
    old = broker.candidate()
    _git(broker.checkout, "push", "origin", "HEAD:refs/heads/feat/fixture")
    pr = {"number": 7, "url": "https://github.com/example/fixture/pull/7", "state": "OPEN",
          "head": old["head"], "base": spec["base_sha"], "candidate": old}
    broker._effect("publish:run-1:0", "publish", {"input_candidate_id": old["id"]})
    broker._finish_effect("publish:run-1:0", pr)
    implementation = {"role": "implement", "iteration": 0, "status": "pass",
                      "session_id": "original-session", "cleanup": "confirmed", "candidate": old}
    checks = {key: {"state": "passed", "candidate_id": old["id"]}
              for key in ("prepublish", "local", "review", "qa")}
    state = {"run_id": "run-1", "revision": 9, "iteration": 0, "phase": "delivered",
             "execution_state": "terminal", "outcome": "delivered", "cleanup": "confirmed",
             "candidate": old, "pull_request": pr, "candidate_revision": 2,
             "roles": [implementation], "checks": checks, "usage": {}, "findings": [],
             "tracker": {}, "error": None}
    store.project("run-1", phase="delivered", execution_state="terminal", event_type="delivered",
                  message="Original delivery", candidate=old, pull_request=pr, checks=checks,
                  iteration=0, protocol_revision=9, outcome="delivered", cleanup="confirmed")
    with store._connect() as db:
        db.execute("INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,"
                   "state,session_id,result_json,cleanup) VALUES "
                   "('original','run-1','implement',0,?,'finished',?,?,'confirmed')",
                   (old["id"], "original-session", canonical_json(implementation)))
        store.state.release_work(db, spec["work_id"], "external:devflow:run-1")
        revision = db.execute("SELECT revision FROM delivery_runs WHERE run_id='run-1'").fetchone()
        revision = revision[0]
    closed = {"workflow_id": "delivery-run-1", "execution_run_id": "authentic-closure",
              "request_digest": spec["request_digest"], "recovery_digest": None, "result": state}
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_args, **_kw: closed)
    editor = tmp_path / "editor"
    import subprocess

    subprocess.run(["git", "clone", "-q", "--branch", "feat/fixture", spec["origin_url"],
                    str(editor)], check=True)
    _git(editor, "config", "user.name", "Fixture")
    _git(editor, "config", "user.email", "fixture@example.invalid")
    (editor / "docs/validation.md").write_text("Accepted design.\n")
    _git(editor, "add", "docs/validation.md")
    _git(editor, "commit", "--signoff", "-qm", "docs: keep execution history in run evidence")
    _git(editor, "push", "origin", "HEAD:refs/heads/feat/fixture")
    head = _git(editor, "rev-parse", "HEAD")
    remote = {"number": 7, "url": pr["url"], "state": "OPEN", "isDraft": False,
              "headRefOid": head, "headRefName": spec["branch"], "baseRefName": "main",
              "labels": []}
    comparison = {"status": "ahead", "merge_base_commit": {"sha": old["head"]},
                  "files": [{"filename": "docs/validation.md", "status": "modified"}]}
    issue = {"url": spec["issue_url"], "state": "OPEN", "stateReason": None,
             "closedAt": None, "labels": []}

    def command(_self, *args, **_kw):
        if args[0] == "pr":
            return deepcopy(remote)
        if args[0] == "issue":
            return deepcopy(issue)
        if args[0] == "api":
            return deepcopy(comparison)
        raise AssertionError(args)

    monkeypatch.setattr(GitHub, "command", command)
    payload = {"continuation_kind": KIND, "command_id": "validate-existing-pr",
               "expected_revision": 9, "expected_projection_revision": revision,
               "expected_candidate_id": old["id"], "expected_pr_number": 7,
               "expected_pr_head": head, "additional_iterations": 0}
    return store, broker, payload, state, remote, comparison, issue


def test_validation_preserves_pr_scope_checkpoint_and_original_attempt(publication):
    store, broker, payload, state, *_ = publication
    original = deepcopy(store.spec("run-1"))
    before = _git(broker.checkout, "rev-parse", "HEAD")
    assert store.repair_admission_preflight("run-1", payload)["head"] == payload["expected_pr_head"]
    assert _git(broker.checkout, "rev-parse", "HEAD") == before
    response = store.continue_repair("run-1", payload)
    assert response["pr_number"] == 7 and response["additional_iterations"] == 0
    assert store.continue_repair("run-1", payload) == response
    effective = store.effective_spec("run-1")
    assert effective["policy"] == original["policy"]
    assert effective["accepted_plan"] == original["accepted_plan"]
    assert _git(broker.checkout, "rev-parse", "HEAD") == payload["expected_pr_head"]
    assert "publication-validation-" in str(_gate_path(effective, "review", 0))
    with store._connect() as db:
        row = db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone()
        recovery = json.loads(row["recovery_json"])
        attempt = json.loads(db.execute("SELECT result_json FROM delivery_attempts").fetchone()[0])
        assert db.execute("SELECT count(*) FROM delivery_attempts").fetchone()[0] == 1
        assert not db.execute("SELECT 1 FROM delivery_repair_grants").fetchone()
    assert attempt == state["roles"][0]
    assert recovery["state"] == state
    assert readback(store, effective, recovery)["head"] == payload["expected_pr_head"]
    assert DeliveryBroker(store, effective).publication_effect_key(0) != "publish:run-1:0"


@pytest.mark.parametrize("drift", ["hold_issue", "hold_pr", "closed_pr", "head", "code",
                                  "ancestry", "revision", "candidate", "budget", "dirty"])
def test_validation_rejects_wrong_authority_before_admission(publication, drift):
    store, broker, payload, _state, remote, comparison, issue = publication
    if drift == "hold_issue":
        issue["labels"] = [{"name": "status: deferred"}]
    elif drift == "hold_pr":
        remote["labels"] = [{"name": "do not merge yet"}]
    elif drift == "closed_pr":
        remote["state"] = "CLOSED"
    elif drift == "head":
        remote["headRefOid"] = "f" * 40
    elif drift == "code":
        comparison["files"][0]["filename"] = "apps/api/src/server.ts"
    elif drift == "ancestry":
        comparison["merge_base_commit"]["sha"] = "f" * 40
    elif drift == "revision":
        payload["expected_projection_revision"] += 1
    elif drift == "candidate":
        payload["expected_candidate_id"] = "f" * 64
    elif drift == "budget":
        payload["additional_iterations"] = 1
    else:
        (broker.checkout / "README.md").write_text("Unrelated retained edit\n")
    old = _git(broker.checkout, "rev-parse", "HEAD")
    with pytest.raises(ValueError):
        store.continue_repair("run-1", payload)
    assert _git(broker.checkout, "rev-parse", "HEAD") == old
    with store._connect() as db:
        assert not db.execute("SELECT 1 FROM delivery_commands WHERE command_id=?",
                              (payload["command_id"],)).fetchone()


@pytest.mark.asyncio
async def test_controller_requires_fresh_native_acceptance_on_same_iteration(
    publication, monkeypatch,
):
    store, _broker, payload, state, *_ = publication
    store.continue_repair("run-1", payload)
    spec = store.effective_spec("run-1")
    with store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
    workflow = DeliveryWorkflow()
    workflow.cancel_requested = False
    observed = {}

    async def activity(name, request, **_kw):
        if name == "delivery_publication_validation_readback":
            return readback(store, spec, recovery)
        if name == "delivery_tracker_start":
            return {"state": "consistent"}
        raise AssertionError(name)

    async def project(*_args, **_kwargs):
        return None

    async def iterations(actual, **kwargs):
        observed.update(kwargs)
        assert actual == spec
        assert workflow.state["candidate"]["head"] == payload["expected_pr_head"]
        assert workflow.state["pull_request"]["number"] == 7
        assert not workflow.state["checks"]
        return workflow.state

    monkeypatch.setattr(workflow, "_activity", activity)
    monkeypatch.setattr(workflow, "_project", project)
    monkeypatch.setattr(workflow, "_run_iterations", iterations)
    await workflow._resume_publication_validation(spec, recovery)
    assert observed["start_iteration"] == state["iteration"]
    assert observed["authorized_max_iteration"] == state["iteration"]
    assert observed["prior_implementer_session"] == "original-session"
    assert observed["recovery"] is None


@pytest.mark.asyncio
async def test_real_workflow_revalidates_same_pr_with_fresh_acceptance(publication, monkeypatch):
    from temporalio import activity
    from temporalio.testing import WorkflowEnvironment
    from temporalio.worker import Worker

    from devflow_temporal.delivery_activities import (
        delivery_checks,
        delivery_ci,
        delivery_precheck,
        delivery_project,
        delivery_publication_validation_readback,
        delivery_publish,
        delivery_tracker,
        delivery_tracker_start,
    )

    store, _broker, payload, _state, remote, *_ = publication
    queued = store.continue_repair("run-1", payload)
    spec = store.effective_spec("run-1")
    with store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
    monkeypatch.setattr(DeliveryBroker, "_existing_pr", lambda *_a, **_kw: deepcopy(remote))
    monkeypatch.setattr(DeliveryBroker, "_publication_base_ref", lambda _self: "main")

    async def ci(_self, pr, **_kwargs):
        assert pr["number"] == 7 and pr["head"] == payload["expected_pr_head"]
        return {"state": "passed", "head": pr["head"], "checks": {"fixture": "SUCCESS"}}

    monkeypatch.setattr(DeliveryBroker, "checks", ci)

    @activity.defn(name="delivery_role")
    async def assess(request):
        return {"role": request["role"], "iteration": request["iteration"],
                "status": "pass", "session_id": request.get("resume_session")
                or request["role"] + "-independent", "cleanup": "confirmed", "findings": [],
                "input_candidate_id": request["candidate"]["id"],
                "candidate": request["candidate"], "summary": "Current documentation assessed"}

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client, task_queue="publication-validation-real", workflows=[DeliveryWorkflow],
            activities=[delivery_publication_validation_readback, delivery_tracker_start,
                        delivery_precheck, delivery_checks, delivery_publish, delivery_ci,
                        delivery_tracker, delivery_project, assess],
        ):
            handle = await env.client.start_workflow(
                DeliveryWorkflow.run, args=[spec, recovery], id=queued["workflow_id"],
                task_queue="publication-validation-real", execution_timeout=timedelta(seconds=45),
            )
            result = await handle.result()
            history = await handle.fetch_history()
    assert result["outcome"] == "delivered", {
        "error": result.get("error"), "findings": result.get("findings"),
        "checks": {key: {"state": value.get("state"), "results": value.get("results")}
                   for key, value in result.get("checks", {}).items()},
    }
    assert result["pull_request"]["number"] == 7
    assert result["pull_request"]["head"] == payload["expected_pr_head"]
    assert result["iteration"] == recovery["state"]["iteration"]
    fresh = result["roles"][len(recovery["state"]["roles"]):]
    assert [role["role"] for role in fresh] == ["implement", "review", "verify"]
    assert fresh[0]["session_id"] == "original-session"
    scheduled = [event.activity_task_scheduled_event_attributes.activity_type.name
                 for event in history.events
                 if event.HasField("activity_task_scheduled_event_attributes")]
    assert "delivery_prepare" not in scheduled and "delivery_intake" not in scheduled
    with store._connect() as db:
        publications = db.execute("SELECT observed_json FROM delivery_effects WHERE kind='publish'"
                                  " AND state='complete'").fetchall()
    assert {json.loads(row[0])["number"] for row in publications} == {7}
    assert {json.loads(row[0])["head"] for row in publications} == {
        recovery["state"]["pull_request"]["head"], payload["expected_pr_head"],
    }
