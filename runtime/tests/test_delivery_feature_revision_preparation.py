"""Public recovery of an unstarted plan reservation after finalized preparation."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from copy import deepcopy
from pathlib import Path

import pytest
from test_delivery_execution_registry import installed_runtime as installed_runtime
from test_delivery_execution_registry import registry as registry
from test_delivery_execution_registry import snapshot as snapshot
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
from devflow_temporal import delivery_stopped_resume as stopped_resume
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_preparation import prepare_authority
from devflow_temporal.delivery_resources import RunResources, read_private, write_private


def _failed_preparation(store, spec, shared):
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
    return row, closed, removed


@pytest.fixture
def pending_preparation(findings_reconsideration, monkeypatch):
    fixture, command, runtime, child, seal, observe, files = findings_reconsideration
    store, original, shared, plan, diagnostic, _, prior, _, rejection, parent_row = fixture
    admitted = revisions.revise_feature_plan(store, original["run_id"], command)
    spec = prepare_authority(store, store.effective_spec(admitted["run_id"]))
    row, closed, removed = _failed_preparation(store, spec, shared)
    closures = {spec["run_id"]: closed}
    # Exact completed Temporal transport is an isolated fixture. The admitted
    # retained worker's full native progress/custody remains the existing fixture.
    monkeypatch.setattr(
        "devflow_temporal.delivery_feature_closure.closed_coordinator",
        lambda s, run: (
            deepcopy(closures[run]) if run in closures else _closed_revision_fixture(s, run)
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
        "closures": closures,
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
    # A second real preparation/checkout finishes before any role or debit. Its
    # closed Temporal transport remains a fixture, with the same exact observer.
    first_successor = successor
    second_row, second_closed, _ = _failed_preparation(store, successor, shared)
    p["closures"][successor_id] = second_closed
    second_request = {"command_id": "continue-second-preparation",
                      "expected_revision": second_row["revision"]}
    first_receipt = deepcopy(shared.checkpoints(issue_id)[
        revisions._preparation_retry_key(p["admitted"]["revision_id"])])
    monkeypatch.setattr(execution, "admit", lost_local_insert)
    with pytest.raises(ConnectionError, match="before local successor insert"):
        execution.continue_feature(store, successor_id, second_request)
    second_successor_id = shared.current(issue_id)["run_id"]
    assert second_successor_id != successor_id
    assert shared.budget(issue_id)["used"] == 6
    monkeypatch.setattr(execution, "admit", real_admit)
    second_response = execution.continue_feature(store, successor_id, second_request)
    assert second_response == execution.continue_feature(store, successor_id, second_request)
    assert response == execution.continue_feature(store, spec["run_id"], request)
    assert second_response["run_id"] == second_successor_id
    successor = prepare_authority(store, store.effective_spec(second_successor_id))
    assert successor["feature_plan_revision_request"] == spec["feature_plan_revision_request"]
    assert successor["plan_approval"] == first_successor["plan_approval"]
    key = successor["feature_plan_revision_preparation_retry"]["checkpoint_key"]
    values = shared.checkpoints(issue_id)
    assert values[key]["version"] == 2 and values[key]["sequence"] == 2
    assert values[key]["previous"] == first_successor["feature_plan_revision_preparation_retry"]
    assert values[revisions._preparation_retry_key(p["admitted"]["revision_id"])] == first_receipt
    with pytest.raises(OwnershipConflict):
        execution.continue_feature(store, first_successor["run_id"], {
            **second_request, "command_id": "branch-from-old-preparation"})
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
    assert _row(store, first_successor["run_id"]) == second_row
    assert _row(store, p["original"]["run_id"]) == p["parent_row"]
    assert store.submitted_spec(spec["run_id"]) == raw_spec
    values = shared.checkpoints(issue_id)
    assert all(values[key] == value for key, value in original_values.items())
    assert values[revisions.PREFIX + "rejected:" + p["prior"]["revision_id"]] == p["rejection"]


@pytest.mark.skipif(sys.platform != "darwin", reason="native configuration preparation required")
def test_three_preparation_links_are_bounded_and_each_ancestor_remains_authenticated(
    pending_preparation, monkeypatch,
):
    p = pending_preparation
    store, shared, spec = (p[k] for k in ("store", "shared", "spec"))
    issue_id = spec["feature_delivery"]["owner"]["issue_id"]
    original = deepcopy(spec["feature_plan_revision_request"])
    observed = []
    live = revisions._preparation_observation
    monkeypatch.setattr(revisions, "_preparation_observation",
                        lambda *a, **k: observed.append(a[1]["run_id"]) or live(*a, **k))
    commands = []
    observations = []
    removed = p["removed"]
    for number in range(1, 4):
        row = _row(store, spec["run_id"])
        request = {"command_id": "bounded-preparation-" + str(number),
                   "expected_revision": row["revision"]}
        response = execution.continue_feature(store, spec["run_id"], request)
        commands.append((spec["run_id"], request, response))
        spec = prepare_authority(store, store.effective_spec(response["run_id"]))
        observations.append(len(observed))
        observed.clear()
        assert spec["feature_plan_revision_request"] == original
        assert shared.budget(issue_id)["used"] == 6
        if number < 3:
            _, closed, removed = _failed_preparation(store, spec, shared)
            p["closures"][spec["run_id"]] = closed
    # Sealed ancestors are never re-observed live, so intake does not grow with depth.
    assert observations[1] == observations[2]
    assert all(execution.continue_feature(store, run, request) == response
               for run, request, response in commands)
    chain = [(key, value) for key, value in shared.checkpoints(issue_id).items()
             if key.startswith(revisions._preparation_retry_key(original["revision_id"]))]
    assert len(chain) == 3 and chain[0][1]["version"] == 1
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    key, first = chain[0]
    # Even recomputing a tampered ancestor's DB digest cannot alter its sealed
    # successor reference. No owner, outbox, receipt or debit can be gained.
    changed = {**first, "successor_id": "run-feature-unrelated"}
    with shared.connect() as db:
        db.execute("UPDATE execution_checkpoints SET content_json=?,content_digest=? "
                   "WHERE issue_id=? AND checkpoint_key=?",
                   (canonical_json(changed), digest(changed), issue_id, key))
    boundary = _boundary_snapshot(store, shared, issue_id)
    with pytest.raises(OwnershipConflict):
        revisions.begin_revision(store, spec)
    assert _boundary_snapshot(store, shared, issue_id) == boundary
    with shared.connect() as db:
        db.execute("UPDATE execution_checkpoints SET content_json=?,content_digest=? "
                   "WHERE issue_id=? AND checkpoint_key=?",
                   (canonical_json(first), digest(first), issue_id, key))
    # Recreating the immediate predecessor's finalized resource is also a refusal
    # at the eventual planning consumer, independently of digest authentication.
    removed.mkdir(parents=True)
    with pytest.raises(ValueError, match="recreated"):
        revisions.begin_revision(store, spec)
    removed.rmdir()
    row, closed, _ = _failed_preparation(store, spec, shared)
    p["closures"][spec["run_id"]] = closed
    runs, recorded, current, _, _ = _boundary_snapshot(store, shared, issue_id)
    fourth = {"command_id": "fourth-preparation-refused", "expected_revision": row["revision"]}
    with pytest.raises(OwnershipConflict, match="limit reached; revision "
                       + original["revision_id"] + " is closed; replay continue-feature "
                       + fourth["command_id"]):
        execution.continue_feature(store, spec["run_id"], fourth)
    # The exhausted reservation ends in a terminal receipt; nothing else moved.
    terminal = shared.checkpoints(issue_id)[revisions.PREFIX + "rejected:"
                                            + original["revision_id"]]
    assert terminal["closed_by"] == spec["feature_delivery"]["owner"]
    assert terminal["repair_key"] is None
    assert "limit reached after 3 continuations" in terminal["reason"]
    assert _boundary_snapshot(store, shared, issue_id)[:3] == (runs, recorded, current)
    assert shared.budget(issue_id)["used"] == 6
    # The exact replay continues the feature without the closed reservation.
    response = execution.continue_feature(store, spec["run_id"], fourth)
    plain = store.effective_spec(response["run_id"])
    assert "feature_plan_revision_request" not in plain
    assert revisions.revision_request(plain) is None
    assert shared.budget(issue_id)["used"] == 6



def test_preparation_proof_survives_receipt_and_renewal_but_never_a_new_charge(
    registry, snapshot, installed_runtime, monkeypatch,
):
    shared = registry
    owner = shared.claim(snapshot, 'preparation-owner', '/synthetic/tracking.sqlite3')
    for number in range(5):
        shared.repair(owner, 'old-' + str(number), 'literal historical charge')
    shared.stop(owner, 'closed-preparation', {})
    original = {'feature_delivery': {'owner': owner, 'registry': str(shared.path)}}
    original_budget = shared.budget(owner['issue_id'])
    context = {**installed_runtime.baseline, 'context_sha256': 'b' * 64, 'guards': {}}
    receipt = {'current_runtime': deepcopy(context), 'original_repair_budget': original_budget}
    public = {'reconsideration': deepcopy(receipt), 'request': {'reconsideration': {}},
              'record': {}, 'predecessor_closure': {}, 'unresolved_prior_findings': []}
    before = deepcopy(public)
    monkeypatch.setattr(revisions, '_current_runtime_selectors',
                        lambda selectors: (context, selectors))
    monkeypatch.setattr(review_context, 'compatible_external_guards', lambda *_: True)
    monkeypatch.setattr(revisions, '_reconsideration_proof', lambda *_: (
        {**deepcopy(receipt), 'current_runtime': deepcopy(context),
         'original_repair_budget': shared.budget(owner['issue_id'])}, []))
    def refresh():
        return revisions._refresh_preparation_reconsideration(None, original, public, {})[0]

    assert refresh() == receipt
    shared.renew_repair_allowance(owner, installed_runtime.baseline)
    assert refresh() == receipt
    context.update(installed_runtime.advance(2))
    shared.renew_repair_allowance(owner, installed_runtime.baseline)
    assert refresh()['original_repair_budget'] == original_budget
    assert shared.require_unspent_budget(owner['issue_id'], original_budget)['used'] == 0
    assert public == before
    current = shared.claim(snapshot, 'preparation-successor', owner['store_path'],
                           predecessor=owner)
    shared.repair(current, 'new-unrelated-charge', 'a real charge invalidates the reservation')
    with pytest.raises(OwnershipConflict, match='unspent planning budget'):
        refresh()
    assert public == before
