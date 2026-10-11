"""Consolidated revision boundaries, reusing native delivery/ownership fixtures."""

from __future__ import annotations

import hashlib
import json
import sys
from copy import deepcopy
from pathlib import Path

import pytest
from test_delivery_feature_execution import feature_service
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_revisions as revisions
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import continue_feature, registry
from devflow_temporal.delivery_feature_migration import migrate
from devflow_temporal.delivery_feature_revision_roles import revision_diff
from devflow_temporal.delivery_plan_model import migrate_plan_v1, ordered_chunks
from devflow_temporal.delivery_preparation import prepare_authority
from devflow_temporal.delivery_resources import read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.supervisor import DeliverySupervisor


@pytest.fixture
def feature(service, monkeypatch):
    store, request, snapshot = feature_service(service, monkeypatch)
    request["plan_approval"] = "automatic"
    plan = json.loads(request["accepted_plan"])
    bindings = {
        s["id"]: {
            "id": "child-" + s["id"],
            "number": s["issue_number"],
            "url": "https://github.com/example/fixture/issues/" + str(s["issue_number"]),
        }
        for s in plan["workstreams"]
    }
    record = {
        "comment_id": 99,
        "comment_node_id": "IC_99",
        "manifest": {
            "version": 1,
            "revision": 1,
            "plan": plan,
            "workstream_issues": bindings,
            "publication": {"stack_id": None, "members": []},
        },
    }
    snapshot["delivery"] = record
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    path = Path(spec["state_dir"]) / "planning-evidence.json"
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_text('{"finding":"a later prerequisite is required too early"}')
    diagnostic = {
        "version": 1,
        "kind": "planning_defect",
        "category": "dependency",
        "chunk_id": "model",
        "plan_revision": 1,
        "plan_sha256": digest(plan),
        "candidate_id": broker.candidate()["id"],
        "detail": "Gate prerequisite is misplaced.",
        "evidence": [{"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}],
    }
    monkeypatch.setattr(
        "devflow_temporal.delivery_feature_publication.current_record",
        lambda *_a, **_k: deepcopy(record),
    )
    monkeypatch.setattr(
        "devflow_temporal.delivery_feature_publication.live_members", lambda *_a: []
    )
    return store, spec, registry(spec), record, diagnostic


def proposed(record):
    value = migrate_plan_v1(
        record["manifest"]["plan"],
        record["manifest"]["workstream_issues"],
        {c["id"]: [] for c in ordered_chunks(record["manifest"]["plan"])},
        [],
    )
    value["workstreams"][0]["chunks"][0]["verification"].append("Verify current prerequisites")
    return value


def passing(context, value):
    return {
        "status": "pass",
        "cleanup": "confirmed",
        "findings": [],
        "reviewed_plan_sha256": digest(value),
        "candidate_id": context["candidate_id"],
    }


def test_stopped_public_revision_preserves_original_row_and_four_cycles(feature, monkeypatch):
    store, spec, shared, _, _ = feature
    owner = spec["feature_delivery"]["owner"]
    for number in range(4):
        shared.repair(owner, str(number), "Prior cycle " + str(number))
    shared.stop(owner, "stopped", {})
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET phase='blocked',outcome='blocked',cleanup='confirmed' "
            "WHERE run_id=?",
            (spec["run_id"],),
        )
        store.state.release_work(db, spec["work_id"], "external:devflow:" + spec["run_id"])
        frozen = dict(
            db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (spec["run_id"],)).fetchone()
        )
    monkeypatch.setattr(
        "devflow_temporal.delivery_feature_closure.closed_coordinator",
        lambda *_a: {"result": {"cleanup": "confirmed"}, "workflow_id": "closed"},
    )
    request = {
        "command_id": "revise-1",
        "expected_revision": frozen["revision"],
        "reason": "Investigate the misplaced gate.",
    }
    receipt = revisions.revise_feature_plan(store, spec["run_id"], request)
    assert receipt["repair_budget"]["used"] == 4
    assert revisions.revise_feature_plan(store, spec["run_id"], request) == receipt
    with pytest.raises(OwnershipConflict, match="different inputs"):
        revisions.revise_feature_plan(
            store, spec["run_id"], {**request, "reason": "Changed request"}
        )
    successor = store.effective_spec(receipt["run_id"])
    DeliveryBroker(store, successor).prepare()
    context = revisions.begin_revision(store, successor)
    assert context["diagnostic"] is None and context["budget"]["used"] == 5
    assert revisions.begin_revision(store, successor) == context
    assert context["custody_evidence"]
    with store._connect() as db:
        assert (
            dict(
                db.execute(
                    "SELECT * FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
                ).fetchone()
            )
            == frozen
        )


def test_uncertain_remote_commit_restarts_without_duplicate_effect_debit_or_lost_proof(feature):
    store, spec, shared, record, diagnostic = feature
    owner = spec["feature_delivery"]["owner"]
    shared.checkpoint(owner, "verified:client", {"head": "unaffected"})
    shared.checkpoint(owner, "verified:model", {"head": "affected"})
    context = revisions.begin_revision(store, spec, diagnostic)
    value = proposed(record)
    changed_source = deepcopy(value)
    changed_source["workstreams"][0]["chunks"][0]["steps"].append("Correct model content")
    _, _, implementation = revisions._admit_plan(
        spec, record["manifest"]["plan"], changed_source, context["old_identity"]
    )
    assert implementation == ["endpoint", "model"]
    revisions.record_proposal(store, spec, context["revision_id"], value)

    class Publisher:
        published = False
        calls = 0

        def publish_plan_revision(self, issue, old, plan, journal, token, *, operation_id):
            self.calls += 1
            journal.intent(token, "commit", "github_plan_revision", {"operation_id": operation_id})
            if not self.published:
                self.published = True
                raise ConnectionError("response lost after remote commit")
            journal.finish_effect(token, "commit", {"revision": 2})
            result = deepcopy(old)
            result["manifest"].update(plan=plan, plan_revision=2, version=2, revision=2)
            return result

    publisher = Publisher()
    with pytest.raises(ConnectionError):
        revisions.adopt_revision(
            store, spec, context["revision_id"], value, passing(context, value), gh=publisher
        )
    with pytest.raises(OwnershipConflict, match="unsettled external effects"):
        revisions.reject_revision(
            store, spec, context["revision_id"], "Cannot release unknown effect"
        )
    from devflow_temporal.delivery_store import DeliveryStore

    restarted = DeliveryStore(store.config)
    result = revisions.adopt_revision(
        restarted, spec, context["revision_id"], value, passing(context, value), gh=publisher
    )
    assert result["budget"]["used"] == 1 and publisher.calls == 2
    assert result["checkpoints"]["verified:client"] == {"head": "unaffected"}
    assert "verified:model" not in result["checkpoints"]
    assert store.submitted_spec(spec["run_id"]) == spec
    assert store.effective_spec(spec["run_id"]) == result["spec"]
    assert (
        revisions.adopt_revision(
            store, spec, context["revision_id"], value, passing(context, value), gh=publisher
        )
        == result
    )
    assert revisions.revision_request(result["spec"]) is None


def test_future_split_preserves_started_custody_and_rejected_attempts_share_limit(feature):
    store, spec, shared, record, diagnostic = feature
    value = proposed(record)
    original = value["workstreams"][0]["chunks"][1]
    precursor = deepcopy(original)
    precursor.update(id="precursor", title="Prerequisite", depends_on=["model", "client"])
    original["depends_on"].append("precursor")
    value["workstreams"][0]["chunks"].insert(1, precursor)
    diagnostic.update(chunk_id="endpoint", category="decomposition")
    context = revisions.begin_revision(store, spec, diagnostic, command_id="split-future")
    receipt = revisions.record_proposal(store, spec, context["revision_id"], value)
    assert "precursor" in receipt["affected_chunks"]
    assert "endpoint" in receipt["implementation_chunks"]
    revisions.reject_revision(
        store, spec, context["revision_id"], "First proposal needs refinement"
    )
    shared.checkpoint(
        spec["feature_delivery"]["owner"],
        "assignment:endpoint:chunk",
        {
            "chunk_id": "endpoint",
            "kind": "chunk",
            "run_id": "original-worker",
            "store_path": str(store.config.tracking_db),
        },
    )
    context = revisions.begin_revision(store, spec, diagnostic, command_id="split-started")
    with pytest.raises(OwnershipConflict, match="started chunk"):
        revisions.record_proposal(store, spec, context["revision_id"], value)
    revisions.reject_revision(
        store, spec, context["revision_id"], "Started chunk cannot change prerequisites"
    )
    for number in range(8):
        context = revisions.begin_revision(
            store, spec, diagnostic, command_id="rejected-" + str(number)
        )
        revisions.reject_revision(
            store, spec, context["revision_id"], "Independent review rejected it"
        )
    assert shared.budget(spec["feature_delivery"]["owner"]["issue_id"])["used"] == 10
    with pytest.raises(OwnershipConflict, match="repair limit exhausted"):
        revisions.begin_revision(store, spec, diagnostic, command_id="exhausted")


def _stop_revision_fixture(store, spec, shared):
    shared.stop(spec["feature_delivery"]["owner"], "isolated closure", {})
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET phase='blocked',outcome='blocked',cleanup='confirmed' "
            "WHERE run_id=?",
            (spec["run_id"],),
        )
        store.state.release_work(db, spec["work_id"], "external:devflow:" + spec["run_id"])
        return dict(
            db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (spec["run_id"],)).fetchone()
        )


def _closed_revision_fixture(store, run_id):
    spec = store.effective_spec(run_id)
    return {
        "result": {"cleanup": "confirmed"},
        "workflow_id": store.active_workflow_id(run_id),
        "execution_run_id": "closed-" + run_id,
        "closed_at": "2099-01-01T00:00:00+00:00",
        "request_digest": spec["request_digest"],
        "recovery_digest": None,
    }


def _revision_diagnostic_for(spec, diagnostic, candidate):
    path = Path(spec["state_dir"]) / "isolated-planning-evidence.json"
    path.write_text('{"finding":"a prerequisite is assigned before its owning chunk"}')
    return {
        **deepcopy(diagnostic),
        "candidate_id": candidate["id"],
        "evidence": [{"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}],
    }


@pytest.fixture
def _native_rejected_prelaunch(native_configuration, monkeypatch):
    config, request = native_configuration
    config.raw["max_repairs"] = 10
    config.path.write_text(json.dumps(config.raw))
    config = type(config).load(config.path)
    private_registry = config.path.parent / "ownership/registry.sqlite3"
    assert private_registry.is_relative_to(config.path.parent)
    monkeypatch.setattr(
        "devflow_temporal.delivery_feature_execution.registry_path", lambda cfg: private_registry
    )
    monkeypatch.setattr(
        "devflow_temporal.delivery_feature_migration.registry_path", lambda cfg: private_registry
    )
    empty = DeliveryStore(config)
    migrate([config])
    store, original, shared, record, diagnostic = feature.__wrapped__((empty, request), monkeypatch)
    original = prepare_authority(store, original)
    owner = original["feature_delivery"]["owner"]
    for n in range(4):
        shared.repair(owner, str(n), "Preserved product cycle " + str(n))
    row = _stop_revision_fixture(store, original, shared)
    monkeypatch.setattr(
        "devflow_temporal.delivery_feature_closure.closed_coordinator", _closed_revision_fixture
    )
    command = {
        "command_id": "first-plan-correction",
        "expected_revision": row["revision"],
        "reason": "Investigate the misplaced gate.",
    }
    admission = revisions.revise_feature_plan(store, original["run_id"], command)
    spec = prepare_authority(store, store.effective_spec(admission["run_id"]))
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    candidate = broker.candidate()
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET candidate_json=? WHERE run_id=?",
            (canonical_json(candidate), spec["run_id"]),
        )
    context = revisions.begin_revision(store, spec)
    assert context["budget"]["used"] == 5
    diagnostic = _revision_diagnostic_for(spec, diagnostic, candidate)
    value = proposed(record)
    value["final_gates"] = [
        {"stage": stage, "recipe_id": "native-check", "selectors": []}
        for stage in ("checks", "prepublish_checks")
    ]
    ordered_chunks(value)[-1]["gates"] = deepcopy(value["final_gates"])
    supervisor = DeliverySupervisor(store, capacity=1)
    intake = {
        "spec": spec,
        "role": "intake",
        "iteration": 0,
        "candidate": candidate,
        "resume_session": None,
        "revision_context": context,
    }
    intake["revision_context"] = revisions.authenticate_revision_role(store, intake)
    intake_key, _ = supervisor._claim(intake)
    intake_folder = Path(spec["state_dir"]) / "attempts" / intake_key
    intake.update(
        findings=[],
        native_authorized=True,
        role_evidence_key=intake_key,
        result_path=str(intake_folder / "result.json"),
        start_path=str(intake_folder / "start.json"),
        workspace=spec["checkout"],
    )
    write_private(intake_folder / "request.json", intake)
    result = {
        "status": "plan",
        "cleanup": "confirmed",
        "finish_reason": "done",
        "plan": value,
        "diagnostic": diagnostic,
        "session_id": "original-intake",
        "findings": [],
    }
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_attempts SET state='finished',cleanup='confirmed',session_id=?,"
            "result_json=?,finished_at=? WHERE job_key=?",
            ("original-intake", canonical_json(result), "2026-10-10T20:00:00+00:00", intake_key),
        )
    proposal = revisions.record_proposal(
        store, spec, context["revision_id"], value, diagnostic=diagnostic
    )
    review = {
        "spec": spec,
        "role": "review",
        "iteration": 0,
        "candidate": candidate,
        "resume_session": None,
        "revision_context": {**context, "proposed_plan": value},
    }
    review["revision_context"] = {
        **revisions.authenticate_revision_role(store, review),
        "proposed_plan": value,
    }
    key, _ = supervisor._claim(review)
    folder = Path(spec["state_dir"]) / "attempts" / key
    review.update(
        findings=[],
        native_authorized=True,
        role_evidence_key=key,
        result_path=str(folder / "result.json"),
        start_path=str(folder / "start.json"),
        workspace=str(Path(spec["state_dir"]) / "gates" / "0" / "review"),
    )
    review["review_diff"] = revision_diff(broker, review)
    review["review_diff"].pop("candidate_id", None)
    write_private(folder / "request.json", review)
    with store._connect() as db:
        expected = dict(
            db.execute("SELECT * FROM delivery_attempts WHERE job_key=?", (key,)).fetchone()
        )
    failure = supervisor._native_failure(
        review, key, expected, "product patch shape rejected plan comparison", launch_absent=True
    )
    assert failure["finish_reason"] == "prelaunch" and failure["cleanup"] == "confirmed"
    rejection = revisions.reject_revision(store, spec, context["revision_id"], failure["summary"])
    row = _stop_revision_fixture(store, spec, shared)
    return store, spec, shared, value, diagnostic, candidate, context, proposal, rejection, row


@pytest.mark.skipif(sys.platform != "darwin", reason="actual native macOS preparation required")
def test_plain_public_continuation_preserves_five_but_does_not_retry_review(
    _native_rejected_prelaunch,
):
    store, original, shared, _, _, _, context, _, _, row = _native_rejected_prelaunch
    result = continue_feature(
        store,
        original["run_id"],
        {"command_id": "ordinary-continue", "expected_revision": row["revision"]},
    )
    successor = store.effective_spec(result["run_id"])
    assert shared.budget(original["feature_delivery"]["owner"]["issue_id"])["used"] == 5
    assert successor["accepted_plan"] == original["accepted_plan"]
    assert revisions.revision_request(successor) is None
    assert revisions.revision_request(original) is None
    with pytest.raises(ValueError, match="already closed"):
        revisions.record_proposal(
            store,
            original,
            context["revision_id"],
            proposed(
                {
                    "manifest": {
                        "plan": json.loads(original["accepted_plan"]),
                        "workstream_issues": context["old_identity"]["workstream_issues"],
                    }
                }
            ),
        )
    with store._connect() as db:
        assert (
            dict(
                db.execute(
                    "SELECT * FROM delivery_runs WHERE run_id=?", (original["run_id"],)
                ).fetchone()
            )
            == row
        )


@pytest.mark.skipif(sys.platform != "darwin", reason="actual native macOS preparation required")
def test_new_public_correction_can_reuse_unreviewed_proposal_at_six_without_reopening(
    _native_rejected_prelaunch,
):
    store, original, shared, value, diagnostic, _, prior, proposal, rejection, row = (
        _native_rejected_prelaunch
    )
    old_receipts = shared.checkpoints(original["feature_delivery"]["owner"]["issue_id"])
    with store._connect() as db:
        attempts = [dict(r) for r in db.execute("SELECT * FROM delivery_attempts ORDER BY job_key")]
    command = {
        "command_id": "new-plan-correction",
        "expected_revision": row["revision"],
        "reason": "The independent review failed before launching; "
        "reassess the same bounded gate correction.",
    }
    admission = revisions.revise_feature_plan(store, original["run_id"], command)
    assert admission["repair_budget"]["used"] == 5
    assert revisions.revise_feature_plan(store, original["run_id"], command) == admission
    successor = prepare_authority(store, store.effective_spec(admission["run_id"]))
    broker = DeliveryBroker(store, successor)
    broker.prepare()
    context = revisions.begin_revision(store, successor)
    assert context["budget"]["used"] == 6
    assert context["old_identity"] == prior["old_identity"]
    diagnostic = _revision_diagnostic_for(successor, diagnostic, broker.candidate())
    # Reconstruct failures in this private fixture only; restoration below
    # proves the source rows/receipts still match their original custody.
    with store._connect() as db:
        review_row = dict(
            db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=? AND role='review'",
                (original["run_id"],),
            ).fetchone()
        )
    request_path = Path(review_row["result_path"]).parent / "request.json"
    old_request = read_private(request_path)
    old_bytes = request_path.read_bytes()
    old_result = json.loads(review_row["result_json"])

    def blocked():
        with pytest.raises(ValueError, match="nonprogressing repeated proposal"):
            revisions.record_proposal(
                store, successor, context["revision_id"], value, diagnostic=diagnostic
            )

    for replacement in (
        {**old_result, "status": "findings", "finish_reason": "done", "session_id": "reviewed"},
        {**old_result, "finish_reason": "recovery_unknown", "cleanup": "unknown"},
        {key: item for key, item in old_result.items() if key != "finish_reason"},
    ):
        with store._connect() as db:
            db.execute(
                "UPDATE delivery_attempts SET result_json=? WHERE job_key=?",
                (canonical_json(replacement), review_row["job_key"]),
            )
        blocked()
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_attempts SET result_json=? WHERE job_key=?",
            (review_row["result_json"], review_row["job_key"]),
        )
    replacements = []
    for key, replacement in (
        ("workspace", successor["checkout"]),
        ("native_authorized", False),
        ("result_path", str(request_path.parent / "other-result.json")),
        ("start_path", str(request_path.parent / "other-start.json")),
        ("spec", successor),
        ("candidate", {**old_request["candidate"], "content_sha256": "0" * 64}),
        ("revision_context", {**old_request["revision_context"], "old_identity": {}}),
        ("review_diff", {**old_request["review_diff"], "sha256": "0" * 64}),
        ("review_diff", {**old_request["review_diff"], "path": str(request_path)}),
    ):
        replacements.append({**deepcopy(old_request), key: replacement})
    for replacement in replacements:
        write_private(request_path, replacement)
        blocked()
    write_private(request_path, old_request)
    assert request_path.read_bytes() == old_bytes
    request_path.rename(request_path.with_suffix(".retained"))
    blocked()
    request_path.with_suffix(".retained").rename(request_path)
    launch = request_path.parent / "launch.json"
    write_private(launch, {"unexpected": "launch evidence"})
    blocked()
    launch.unlink()
    comparison_path = Path(old_request["review_diff"]["path"])
    old_comparison = read_private(comparison_path)
    write_private(comparison_path, {**old_comparison, "diff": "altered comparison"})
    blocked()
    write_private(comparison_path, old_comparison)
    canonical_comparison = comparison_path.read_bytes()
    for altered in (
        json.dumps(old_comparison, separators=(",", ":")).encode(),
        canonical_comparison.replace(b"{\n", b'{\n  "version": 0,\n', 1),
    ):
        comparison_path.write_bytes(altered)
        replacement = deepcopy(old_request)
        replacement["review_diff"]["sha256"] = hashlib.sha256(altered).hexdigest()
        write_private(request_path, replacement)
        blocked()
    comparison_path.write_bytes(canonical_comparison)
    write_private(request_path, old_request)
    begin_key = revisions.PREFIX + "begin:" + prior["revision_id"]
    with shared.connect() as db:
        saved_begin = dict(
            db.execute(
                "SELECT * FROM execution_checkpoints WHERE issue_id=? AND checkpoint_key=?",
                (prior["owner"]["issue_id"], begin_key),
            ).fetchone()
        )
        db.execute(
            "UPDATE execution_checkpoints SET content_digest=? "
            "WHERE issue_id=? AND checkpoint_key=?",
            ("0" * 64, prior["owner"]["issue_id"], begin_key),
        )
    blocked()
    with shared.connect() as db:
        db.execute(
            "UPDATE execution_checkpoints SET content_digest=? "
            "WHERE issue_id=? AND checkpoint_key=?",
            (saved_begin["content_digest"], prior["owner"]["issue_id"], begin_key),
        )
    original_begin = json.loads(saved_begin["content_json"])
    for replacement in (
        {**original_begin, "phase": "proposed"},
        {**original_begin, "revision_id": context["revision_id"]},
    ):
        with shared.connect() as db:
            db.execute(
                "UPDATE execution_checkpoints SET content_json=?,content_digest=? "
                "WHERE issue_id=? AND checkpoint_key=?",
                (
                    canonical_json(replacement),
                    digest(replacement),
                    prior["owner"]["issue_id"],
                    begin_key,
                ),
            )
        blocked()
    with shared.connect() as db:
        db.execute(
            "UPDATE execution_checkpoints SET content_json=?,content_digest=? "
            "WHERE issue_id=? AND checkpoint_key=?",
            (
                saved_begin["content_json"],
                saved_begin["content_digest"],
                prior["owner"]["issue_id"],
                begin_key,
            ),
        )
    assert shared.budget(original["feature_delivery"]["owner"]["issue_id"])["used"] == 6
    admitted = revisions.record_proposal(
        store, successor, context["revision_id"], value, diagnostic=diagnostic
    )
    assert admitted["proposal_digest"] == proposal["proposal_digest"]
    assert (
        revisions.record_proposal(
            store, successor, context["revision_id"], value, diagnostic=diagnostic
        )
        == admitted
    )
    assert shared.budget(original["feature_delivery"]["owner"]["issue_id"])["used"] == 6
    assert revisions.revision_request(original) is None
    with store._connect() as db:
        assert (
            dict(
                db.execute(
                    "SELECT * FROM delivery_runs WHERE run_id=?", (original["run_id"],)
                ).fetchone()
            )
            == row
        )
        assert [
            dict(r) for r in db.execute("SELECT * FROM delivery_attempts ORDER BY job_key")
        ] == attempts
    values = shared.checkpoints(original["feature_delivery"]["owner"]["issue_id"])
    assert all(values[key] == item for key, item in old_receipts.items())
    assert values[revisions.PREFIX + "rejected:" + prior["revision_id"]] == rejection
