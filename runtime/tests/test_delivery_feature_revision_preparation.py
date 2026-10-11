"""Public recovery of an unstarted plan reservation after finalized preparation."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from copy import deepcopy
from pathlib import Path

import pytest
from test_delivery_feature_review_context import git
from test_delivery_feature_revision_subject import feature as feature
from test_delivery_feature_revision_subject import (
    findings_reconsideration as findings_reconsideration,
)
from test_delivery_feature_revision_subject import intake_fixture as intake_fixture
from test_delivery_feature_revision_subject import native_configuration as native_configuration
from test_delivery_feature_revision_subject import service as service
from test_delivery_feature_revisions import (
    _boundary_snapshot,
    _closed_revision_fixture,
    _complete_native_role_fixture,
    _revision_diagnostic_for,
    _stop_revision_fixture,
)

from devflow_temporal import delivery_activities as activities
from devflow_temporal import delivery_feature_execution as execution
from devflow_temporal import delivery_feature_review_context as review_context
from devflow_temporal import delivery_feature_revisions as revisions
from devflow_temporal import delivery_policy_recovery as policy_recovery
from devflow_temporal import delivery_stopped_resume as stopped_resume
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_preparation import prepare_authority
from devflow_temporal.delivery_resources import RunResources, read_private, write_private


@pytest.fixture
def pending_preparation(findings_reconsideration, monkeypatch):
    fixture, command, runtime, child, seal, observe, files = findings_reconsideration
    store, original, shared, plan, diagnostic, _, prior, _, rejection, parent_row = fixture
    admitted = revisions.revise_feature_plan(store, original["run_id"], command)
    spec = prepare_authority(store, store.effective_spec(admitted["run_id"]))
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    candidate = broker.candidate()
    removed = Path(spec["state_dir"]) / "gates/0/baseline/.venv"
    # Owned finalization/baseline metadata models the cache failure. No baseline
    # or provider is invoked. Current Git candidate/work/cleanup reads are real.
    resources = RunResources(spec)
    manifest = read_private(resources.manifest)
    if not manifest["processes"]:
        # Cached native preparation may have no new journal. This isolated typed
        # process record supplies the owned-PID refusal seam, never a provider.
        journal = resources.root / "isolated-preparation-process.json"
        write_private(journal, {"phase": "finished", "monitoring_complete": True,
                               "owned": {"12345": {"identity": "isolated-preparation"}},
                               "ports": []})
        manifest["processes"] = [str(journal)]
    finalization = {
        "state": "confirmed",
        "process_cleanup": "observed-native-confirmed",
        "resource_cleanup": "confirmed",
        "roots": [{"path": str(removed), "state": "removed"}],
        "processes": [{"journal": name} for name in manifest["processes"]],
    }
    receipt_path = resources.root / "finalization.json"
    write_private(receipt_path, finalization)
    manifest["finalization"] = finalization
    write_private(resources.manifest, manifest)
    checks = {
        "baseline": {
            "state": "failed",
            "base_sha": spec["base_sha"],
            "baseline_candidate": candidate,
            "feature_unchanged": True,
            "source_unchanged": True,
            "results": [{"id": "locked-environment", "passed": False, "cleanup": "confirmed"}],
        }
    }
    checks["resource_cleanup"] = {
        **finalization,
        "receipt": str(receipt_path),
        "receipt_sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
    }
    store.project(
        spec["run_id"],
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message="Preparation failed before revision begin",
        candidate=candidate,
        checks=checks,
        iteration=0,
        protocol_revision=4,
        outcome="blocked",
        cleanup="confirmed",
    )
    row = _stop_revision_fixture(store, spec, shared)
    closed = {
        **_closed_revision_fixture(store, spec["run_id"]),
        "result": {
            "run_id": spec["run_id"],
            "roles": [],
            "iteration": 0,
            "phase": "blocked",
            "outcome": "blocked",
            "cleanup": "confirmed",
            "candidate": candidate,
            "pull_request": None,
            "decision": None,
            "revision": row["protocol_revision"],
            "checks": checks,
        },
    }
    # Exact completed Temporal transport is an isolated fixture. The admitted
    # retained worker's full native progress/custody remains the existing fixture.
    monkeypatch.setattr(
        "devflow_temporal.delivery_feature_closure.closed_coordinator",
        lambda s, run: (
            deepcopy(closed) if run == spec["run_id"] else _closed_revision_fixture(s, run)
        ),
    )
    monkeypatch.setattr(store, "_ensure_no_remote_pr", lambda *_a: None)
    root = review_context._protected_root()
    source = root / review_context.PREFIX / "delivery_feature_revisions.py"
    source.write_text(source.read_text() + "\n# Installed preparation transport repair\n")
    git(root, "add", ".")
    git(root, "commit", "-qm", "Add preparation transport without changing derivation")
    refreshed = review_context.current_runtime_context()
    assert refreshed["revision"] != runtime["revision"]
    assert refreshed["context_sha256"] == runtime["context_sha256"]
    request = {"command_id": "continue-unstarted-reservation", "expected_revision": row["revision"]}
    return {
        "store": store,
        "original": original,
        "shared": shared,
        "plan": plan,
        "diagnostic": diagnostic,
        "prior": prior,
        "rejection": rejection,
        "parent_row": parent_row,
        "admitted": admitted,
        "spec": spec,
        "row": row,
        "request": request,
        "runtime": refreshed,
        "old_runtime": runtime,
        "child": child,
        "seal": seal,
        "observe": observe,
        "files": files,
        "closed": closed,
        "removed": removed,
    }


def _row(store, run_id):
    with store._connect() as db:
        return dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone())


@pytest.mark.skipif(sys.platform != "darwin", reason="native configuration preparation required")
def test_public_retry_recovers_partial_claim_and_same_revision_then_charges_seven_and_eight(
    pending_preparation,
    monkeypatch,
):
    p = pending_preparation
    store, shared, spec, request = (p[k] for k in ("store", "shared", "spec", "request"))
    issue_id = spec["feature_delivery"]["owner"]["issue_id"]
    original_values = shared.checkpoints(issue_id)
    raw_spec = deepcopy(store.submitted_spec(spec["run_id"]))
    historical_readback = revisions.local_revision_readback(spec)
    real_admit = execution.admit

    def lost_local_insert(*args, **kwargs):
        real_admit(*args, **kwargs)
        raise ConnectionError("shared claim committed before local successor insert")

    monkeypatch.setattr(execution, "admit", lost_local_insert)
    with pytest.raises(ConnectionError, match="before local successor insert"):
        execution.continue_feature(store, spec["run_id"], request)
    successor_id = shared.current(issue_id)["run_id"]
    with store._connect() as db:
        assert not db.execute(
            "SELECT 1 FROM delivery_runs WHERE run_id=?", (successor_id,)
        ).fetchone()
    assert shared.budget(issue_id)["used"] == 6
    assert _row(store, spec["run_id"]) == p["row"]
    monkeypatch.setattr(execution, "admit", real_admit)
    response = execution.continue_feature(store, spec["run_id"], request)
    assert response == execution.continue_feature(store, spec["run_id"], request)
    assert response["run_id"] == successor_id
    successor = prepare_authority(store, store.effective_spec(successor_id))
    assert successor["feature_plan_revision_request"] == spec["feature_plan_revision_request"]
    assert successor["feature_delivery"]["owner"]["generation"] == (
        spec["feature_delivery"]["owner"]["generation"] + 1
    )
    assert successor["checkout"] != spec["checkout"] and successor["state_dir"] != spec["state_dir"]
    assert successor["plan_approval"] == spec["plan_approval"]
    assert shared.budget(issue_id)["used"] == 6
    readback = revisions.local_revision_readback(successor)
    assert readback["revision_id"] == p["admitted"]["revision_id"]
    assert readback["phase"] == "requested"
    assert readback["reconsideration"]["current_runtime"] == p["runtime"]
    assert revisions.local_revision_readback(spec) == historical_readback
    with store._connect() as db:
        assert (
            db.execute(
                "SELECT count(*) FROM delivery_outbox WHERE run_id=?", (successor_id,)
            ).fetchone()[0]
            == 1
        )
    broker = DeliveryBroker(store, successor)
    broker.prepare()
    context = revisions.begin_revision(store, successor)
    assert context == revisions.begin_revision(store, successor)
    assert context["revision_id"] == p["admitted"]["revision_id"]
    assert context["budget"]["used"] == 7
    assert context["reconsideration"]["current_runtime"] == p["runtime"]
    assert (
        spec["feature_plan_revision_request"]["reconsideration"]["current_runtime"]
        == p["old_runtime"]
    )
    diagnostic = _revision_diagnostic_for(successor, p["diagnostic"], broker.candidate())
    native = {
        "dirty": False,
        "local_digest": None,
        **{k: p["runtime"][k] for k in ("revision", "runtime_payload_sha256")},
    }
    # Completed-role proofs are explicitly synthetic controller receipts; the
    # production native authentication/result/runtime hooks consume them.
    intake = _complete_native_role_fixture(
        store, successor, context, p["plan"], diagnostic, "intake", native
    )
    proposal = revisions.record_proposal(
        store, successor, context["revision_id"], p["plan"], diagnostic=diagnostic
    )
    assert proposal == revisions.record_proposal(
        store, successor, context["revision_id"], p["plan"], diagnostic=diagnostic
    )
    review = _complete_native_role_fixture(
        store, successor, context, p["plan"], diagnostic, "review", native, status="pass"
    )

    class Publisher:
        def publish_plan_revision(self, _issue, record, value, *_a, **_k):
            result = deepcopy(record)
            result["manifest"].update(version=2, revision=2, plan_revision=2, plan=value)
            return result

    adopted = revisions.adopt_revision(
        store,
        successor,
        context["revision_id"],
        p["plan"],
        {
            **review,
            "candidate_id": context["candidate_id"],
            "proposal_session_id": intake["session_id"],
        },
        gh=Publisher(),
    )
    assert adopted["budget"]["used"] == 7
    assert "feature_plan_revision_preparation_retry" not in adopted["spec"]
    monkeypatch.setattr(stopped_resume, "snapshot", lambda s, run, **_kw: p["observe"](s, run))
    monkeypatch.setattr(stopped_resume, "prepare_runtime", lambda value, *_: deepcopy(value))
    assignment = revisions.resume_revision_worker(store, adopted["spec"], p["child"])
    assert assignment == revisions.resume_revision_worker(store, adopted["spec"], p["child"])
    resumed = assignment["spec"]
    execution.require_execution(store, resumed)
    with pytest.raises(OwnershipConflict):
        execution.require_execution(store, p["child"])
    envelope = json.loads(_row(store, resumed["run_id"])["recovery_json"])
    assert envelope["execution_candidate"] == p["seal"]["candidate"]
    assert envelope["state"]["candidate"] == p["seal"]["state"]["candidate"]
    assert envelope["session_id"] == "retained-original-session" and "resume_stage" not in envelope
    assert envelope["command"]["expected_iteration"] == 4
    assert envelope["command"]["additional_iterations"] == 3
    worker_broker = DeliveryBroker(store, resumed)

    class FinishedFixtureRole:
        async def run(self, request):
            assert request["iteration"] == 5 and request["resume_session"] == envelope["session_id"]
            assert "Original unresolved product finding" in request["findings"]
            return {
                "status": "findings",
                "findings": request["findings"],
                "session_id": request["resume_session"],
                "cleanup": "confirmed",
            }

    monkeypatch.setattr(activities, "_context", lambda _spec: (store, worker_broker))
    monkeypatch.setattr(
        activities, "_role_context", lambda req, *_a: (req, worker_broker.checkout, None, None)
    )
    monkeypatch.setattr(activities, "get_supervisor", lambda _: FinishedFixtureRole())
    role_request = {
        "spec": resumed,
        "role": "implement",
        "iteration": 5,
        "candidate": envelope["execution_candidate"],
        "resume_session": envelope["session_id"],
        "findings": ["Original unresolved product finding"],
    }
    asyncio.run(activities._run_role(role_request))
    asyncio.run(activities._run_role(role_request))
    assert shared.budget(issue_id)["used"] == 8
    assert revisions.PREFIX + "repair-alias:" + resumed["run_id"] + ":5" not in (
        shared.checkpoints(issue_id)
    )
    assert _row(store, spec["run_id"]) == p["row"]
    assert _row(store, p["original"]["run_id"]) == p["parent_row"]
    assert store.submitted_spec(spec["run_id"]) == raw_spec
    values = shared.checkpoints(issue_id)
    assert all(values[key] == value for key, value in original_values.items())
    assert values[revisions.PREFIX + "rejected:" + p["prior"]["revision_id"]] == p["rejection"]


@pytest.mark.skipif(sys.platform != "darwin", reason="native configuration preparation required")
def test_retry_refuses_coordinator_drift_before_receipt_and_at_begin(
    pending_preparation, monkeypatch
):
    p = pending_preparation
    store, spec, shared, request = (p[k] for k in ("store", "spec", "shared", "request"))
    issue_id = spec["feature_delivery"]["owner"]["issue_id"]
    old = _boundary_snapshot(store, shared, issue_id)

    def refused():
        with pytest.raises((ValueError, OwnershipConflict)):
            execution.continue_feature(store, spec["run_id"], request)
        assert _boundary_snapshot(store, shared, issue_id) == old

    p["removed"].mkdir(parents=True)
    refused()
    p["removed"].rmdir()
    git(
        Path(spec["checkout"]),
        "remote",
        "set-url",
        "origin",
        str(Path(spec["source_path"]) / "foreign"),
    )
    refused()
    git(Path(spec["checkout"]), "remote", "set-url", "origin", spec["origin_url"])
    manifest = read_private(RunResources(spec, read_only=True).manifest)
    journal = read_private(Path(manifest["processes"][0]))
    pid, identity = next(iter(journal["owned"].items()))
    with monkeypatch.context() as live:
        live.setattr(
            policy_recovery,
            "process_table",
            lambda: {int(pid): {"identity": identity["identity"], "stat": "S"}},
        )
        refused()
    config = Path(spec["config_path"])
    raw = config.read_bytes()
    config.write_text(json.dumps({**store.config.raw, "max_repairs": 9}))
    refused()
    config.write_bytes(raw)
    p["closed"]["result"]["roles"] = [{"role": "intake"}]
    refused()
    p["closed"]["result"]["roles"] = []
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET iteration=1 WHERE run_id=?", (spec["run_id"],))
    modified = _boundary_snapshot(store, shared, issue_id)
    with pytest.raises(OwnershipConflict, match="unstarted"):
        execution.continue_feature(store, spec["run_id"], request)
    assert _boundary_snapshot(store, shared, issue_id) == modified
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET iteration=0 WHERE run_id=?", (spec["run_id"],))
    response = execution.continue_feature(store, spec["run_id"], request)
    successor = prepare_authority(store, store.effective_spec(response["run_id"]))
    broker = DeliveryBroker(store, successor)
    broker.prepare()
    p["removed"].mkdir()
    with pytest.raises(ValueError, match="recreated"):
        revisions.begin_revision(store, successor)
    p["removed"].rmdir()
    assert shared.budget(issue_id)["used"] == 6
    key = revisions._preparation_retry_key(p["admitted"]["revision_id"])
    with shared.connect() as db:
        saved = dict(
            db.execute(
                "SELECT * FROM execution_checkpoints WHERE issue_id=? AND checkpoint_key=?",
                (issue_id, key),
            ).fetchone()
        )
        db.execute(
            "UPDATE execution_checkpoints SET content_digest=? "
            "WHERE issue_id=? AND checkpoint_key=?",
            ("f" * 64, issue_id, key),
        )
    with pytest.raises(OwnershipConflict):
        revisions.begin_revision(store, successor)
    with shared.connect() as db:
        db.execute(
            "UPDATE execution_checkpoints SET content_digest=? "
            "WHERE issue_id=? AND checkpoint_key=?",
            (saved["content_digest"], issue_id, key),
        )
    assert shared.budget(issue_id)["used"] == 6
    context = revisions.begin_revision(store, successor)
    with pytest.raises(OwnershipConflict):
        execution.continue_feature(
            store, spec["run_id"], {**request, "command_id": "different-retry"}
        )
    with pytest.raises(OwnershipConflict):
        execution.continue_feature(
            store,
            successor["run_id"],
            {
                "command_id": "retry-after-begin",
                "expected_revision": _row(store, successor["run_id"])["revision"],
            },
        )
    # Runtime/result provenance is freshly measured, never copied from the old
    # reservation. A finished role under that old runtime cannot supply the plan.
    diagnostic = _revision_diagnostic_for(successor, p["diagnostic"], broker.candidate())
    old_native = {
        "dirty": False,
        "local_digest": None,
        **{k: p["old_runtime"][k] for k in ("revision", "runtime_payload_sha256")},
    }
    _complete_native_role_fixture(
        store, successor, context, p["plan"], diagnostic, "intake", old_native
    )
    with pytest.raises(OwnershipConflict, match="different sealed review context"):
        revisions.record_proposal(
            store, successor, context["revision_id"], p["plan"], diagnostic=diagnostic
        )
    assert shared.budget(issue_id)["used"] == 7


@pytest.mark.skipif(sys.platform != "darwin", reason="native configuration preparation required")
def test_retry_receipt_cannot_authorize_direct_submission_or_another_command(
    pending_preparation,
    monkeypatch,
):
    p = pending_preparation
    store, spec, shared = (p[k] for k in ("store", "spec", "shared"))
    captured = {}

    def no_dispatch(supplied, **kwargs):
        captured.update(supplied=supplied, authority=kwargs)
        raise ConnectionError("receipt persisted before submission")

    real_submit = store.submit
    monkeypatch.setattr(store, "submit", no_dispatch)
    with pytest.raises(ConnectionError, match="before submission"):
        execution.continue_feature(store, spec["run_id"], p["request"])
    monkeypatch.setattr(store, "submit", real_submit)
    issue_id = spec["feature_delivery"]["owner"]["issue_id"]
    old = _boundary_snapshot(store, shared, issue_id)
    with pytest.raises(OwnershipConflict, match="server reservation binding"):
        store.submit(captured["supplied"])
    assert _boundary_snapshot(store, shared, issue_id) == old
    with pytest.raises(OwnershipConflict):
        execution.continue_feature(
            store, spec["run_id"], {**p["request"], "command_id": "same-reservation-other-command"}
        )
    assert _boundary_snapshot(store, shared, issue_id) == old
    with pytest.raises(OwnershipConflict):
        execution.continue_feature(
            store,
            spec["run_id"],
            {**p["request"], "expected_revision": p["request"]["expected_revision"] - 1},
        )
    assert _boundary_snapshot(store, shared, issue_id) == old
    response = execution.continue_feature(store, spec["run_id"], p["request"])
    assert response == execution.continue_feature(store, spec["run_id"], p["request"])
    assert shared.budget(issue_id)["used"] == 6
