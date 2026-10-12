"""Consolidated revision boundaries, reusing native delivery/ownership fixtures."""

from __future__ import annotations

import hashlib
import json
import sys
from copy import deepcopy
from pathlib import Path

import pytest
from test_delivery_feature_execution import feature_service
from test_delivery_feature_review_context import context_repository, git
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_review_context as review_context
from devflow_temporal import delivery_feature_revisions as revisions
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import (
    continue_feature,
    register_worker,
    registry,
    worker_key,
    worker_spec,
)
from devflow_temporal.delivery_feature_migration import migrate
from devflow_temporal.delivery_feature_revision_roles import revision_diff
from devflow_temporal.delivery_github_contract import PARENT_INDEX_LIMIT, encode_manifest
from devflow_temporal.delivery_plan_model import migrate_plan_v1, ordered_chunks
from devflow_temporal.delivery_preparation import prepare_authority
from devflow_temporal.delivery_resources import read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.supervisor import DeliverySupervisor


@pytest.fixture
def feature(service, monkeypatch):
    return _feature(service, monkeypatch)


def _feature(service, monkeypatch, *, index_bytes=None):
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
    if index_bytes:
        # Grow the accepted plan until its parent index comment is this large.
        # Each item lands in the body twice: as a bullet and inside the embedded JSON.
        while (short := index_bytes - len(encode_manifest(record["manifest"]).encode())) > 80:
            plan["acceptance"].append("Requirement " + "x" * max(1, min(4000, short // 2 - 40)))
        request["accepted_plan"] = json.dumps(plan)
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


def test_parent_index_without_room_for_its_stack_refuses_revision_before_any_spend(
    service, monkeypatch,
):
    # The recorded index fits GitHub today but not once every chunk's PR is recorded.
    store, spec, shared, record, diagnostic = _feature(service, monkeypatch,
                                                      index_bytes=PARENT_INDEX_LIMIT - 200)
    issue_id = spec["feature_delivery"]["owner"]["issue_id"]
    before = shared.budget(issue_id)
    assert len(encode_manifest(record["manifest"]).encode()) <= PARENT_INDEX_LIMIT
    with pytest.raises(ValueError, match=r"parent index cannot record every chunk's pull "
                                         r"request: \d+ of 60000 bytes once its 3 unpublished"):
        revisions.begin_revision(store, spec, diagnostic)
    # No planning-correction debit, no sealed attempt, so no planner or review turn follows.
    assert shared.budget(issue_id) == before
    assert not any(key.startswith("plan-revision:") for key in shared.checkpoints(issue_id))
    with shared.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM execution_repairs").fetchone()[0] == 0


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


def _native_rejected_fixture(native_configuration, monkeypatch, *, prior_cycles=4,
                             retain_worker=False):
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
    if retain_worker:
        kind = retain_worker if isinstance(retain_worker, str) else "build"
        chunk = ordered_chunks(json.loads(original["accepted_plan"]))[0]
        child = worker_spec(original, chunk, {"url": "https://github.com/example/fixture/issues/10"},
                            kind=kind, base_sha=original["base_sha"], base_branch="main")
        register_worker(store, original, child)
        shared.checkpoint(owner, "assignment:" + chunk["id"] + ":" + kind, {
            "run_id": child["run_id"], "chunk_id": chunk["id"], "kind": kind,
            "store_path": str(store.config.tracking_db),
        })
        broker = DeliveryBroker(store, child)
        broker.prepare()
        candidate = broker.candidate()
        with store._connect() as db:
            db.execute(
                "UPDATE delivery_runs SET outcome='blocked',phase='blocked',cleanup='confirmed',"
                "candidate_json=? WHERE run_id=?", (canonical_json(candidate), child["run_id"]),
            )
            db.execute(
                "INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,state,"
                "result_path,session_id,result_json,cleanup,finished_at) "
                "VALUES (?,?, 'implement',1,?,'finished',?,?,'{}','confirmed',?)",
                (digest(child), child["run_id"], candidate["id"],
                 str(Path(child["state_dir"]) / "synthetic-result.json"),
                 "retained-original-session", "2026-10-10T19:00:00+00:00"),
            )
        shared.finish_worker(owner, worker_key(child["run_id"], owner), {"cleanup": "confirmed"})
        # This existing control fixture has a synthetic finished build rather
        # than a Temporal/native worker closure. Native progress and full readonly
        # observation are qualified separately in test_delivery_feature_revision_subject.
        from devflow_temporal import delivery_stopped_resume

        def observed_build(_store, run_id):
            observed = store.effective_spec(run_id)
            with store._connect() as db:
                row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                                      (run_id,)).fetchone())
                attempts = [dict(value) for value in db.execute(
                    "SELECT * FROM delivery_attempts WHERE run_id=? ORDER BY job_key", (run_id,))]
            current = DeliveryBroker(store, observed).candidate()
            return {"predecessor_spec": observed, "row": row, "attempts": attempts,
                    "state": {"candidate": json.loads(row["candidate_json"]), "roles": []},
                    "candidate": current, "effects": [], "session_id": "retained-original-session",
                    "cleanup_digest": "synthetic-finished-build", "work_binding": {}}

        monkeypatch.setattr(delivery_stopped_resume, "review_snapshot", observed_build)
    for n in range(prior_cycles):
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
    assert context["budget"]["used"] == prior_cycles + 1
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


@pytest.fixture
def _native_rejected_prelaunch(native_configuration, monkeypatch):
    return _native_rejected_fixture(native_configuration, monkeypatch)


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


def _complete_native_role_fixture(store, spec, context, plan, diagnostic, role, runtime,
                                  *, status=None, session=None):
    """Synthetic controller-owned proofs; actual NativeProcess rehearsal is separate."""
    broker = DeliveryBroker(store, spec)
    candidate = broker.candidate()
    proposal = registry(spec).checkpoints(context["owner"]["issue_id"]).get(
        revisions.PREFIX + "proposal:" + context["revision_id"],
    )
    enriched = revisions._role_context(store, spec, context, proposal if role == "review" else None)
    if role == "review":
        enriched["proposed_plan"] = plan
    request = {"spec": spec, "role": role, "iteration": 0, "candidate": candidate,
               "resume_session": None, "revision_context": enriched}
    key = DeliverySupervisor._job_key(request)
    with store._connect() as db:
        existing = db.execute("SELECT * FROM delivery_attempts WHERE job_key=?", (key,)).fetchone()
    if not existing:
        key, _ = DeliverySupervisor(store, capacity=1)._claim(request)
    folder = Path(spec["state_dir"]) / "attempts" / key
    request.update(findings=[], native_authorized=True, role_evidence_key=key,
                   result_path=str(folder / "result.json"), start_path=str(folder / "start.json"),
                   workspace=spec["checkout"] if role == "intake" else
                   str(Path(spec["state_dir"]) / "gates" / "0" / "review"))
    if role == "review":
        request["review_diff"] = revision_diff(broker, request)
    write_private(folder / "request.json", request)
    session = session or spec["run_id"] + ":" + role
    raw = {"status": status or ("plan" if role == "intake" else "findings"),
           "session_id": session, "finish_reason": "done", "summary": "Isolated native receipt",
           "findings": ["Retained source authority must survive derivation."]
           if role == "review" and status != "pass" else []}
    if role == "intake":
        raw.update(plan=plan, diagnostic=diagnostic)
    else:
        raw["reviewed_plan_sha256"] = digest(plan)
    write_private(folder / "result.json", raw)
    native = {"state": "finished", "cleanup": "observed-native-confirmed", "exit_code": 0,
              "monitoring_complete": True, "stdio_drained": True, "cancelled": False,
              "timed_out": False, "journal": str(folder / "native-process.json"),
              "runtime_identity": deepcopy(runtime)}
    result = {**raw, "cleanup": "confirmed", "native_process": native}
    journal = {"phase": "finished", "owned": {"12345": {"identity": "isolated-native-process"}},
               "intent": {"run_id": spec["run_id"], "policy_digest": spec["policy_digest"],
                          "cwd": request["workspace"],
                          "argv": [sys.executable, "-I", "-m", "devflow_temporal.role_runner",
                                   str(folder / "request.json")]},
               "runtime_identity": deepcopy(runtime), "result": native,
               "provider_session": {"session_id": session, "resumed_from": None, "role": role,
                                    "iteration": 0, "result_digest": digest(result)}}
    write_private(folder / "native-process.json", journal)
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_attempts SET state='finished',cleanup='confirmed',pid=12345,"
            "process_identity='isolated-native-process',session_id=?,result_json=?,finished_at=? "
            "WHERE job_key=?",
            (session, canonical_json(result), "2026-10-10T20:00:00+00:00", key),
        )
    return result


def _genuine_fixture(native_configuration, monkeypatch, tmp_path, *, retain_kind="build",
                     prior_cycles=5):
    values = _native_rejected_fixture(native_configuration, monkeypatch, prior_cycles=prior_cycles,
                                      retain_worker=retain_kind)
    store, spec, shared, plan, diagnostic, _, context, _, _, row = values
    root, sources, historical, current = context_repository(tmp_path, monkeypatch)
    for role in ("intake", "review"):
        _complete_native_role_fixture(store, spec, context, plan, diagnostic, role, historical)
    selectors = {"version": 1, "kind": "derivation_context_change",
                 "rejected_revision_id": context["revision_id"], "proposal_sha256": digest(plan),
                 "expected_runtime_revision": current["revision"],
                 "expected_runtime_payload_sha256": current["runtime_payload_sha256"]}
    command = {"command_id": "fresh-context-review", "expected_revision": row["revision"],
               "reason": "Fresh independent review after a relevant controller repair.",
               "reconsideration": selectors}
    return values, command, root, sources, historical, current


@pytest.fixture
def _genuine_reconsideration(native_configuration, monkeypatch, tmp_path):
    return _genuine_fixture(native_configuration, monkeypatch, tmp_path)


@pytest.mark.skipif(sys.platform != "darwin", reason="actual native macOS preparation required")
def test_public_reconsideration_preserves_six_and_needs_fresh_native_pass(
    _genuine_reconsideration, monkeypatch,
):
    fixture, command, _, _, _, current = _genuine_reconsideration
    store, original, shared, plan, diagnostic, _, prior, _, rejection, row = fixture
    old = shared.checkpoints(prior["owner"]["issue_id"])
    real_continue = continue_feature
    def lost_response(*args, **kwargs):
        real_continue(*args, **kwargs)
        raise ConnectionError("owner/outbox succeeded; outer response lost")
    monkeypatch.setattr("devflow_temporal.delivery_feature_execution.continue_feature",
                        lost_response)
    with pytest.raises(ConnectionError, match="outer response lost"):
        revisions.revise_feature_plan(store, original["run_id"], command)
    monkeypatch.setattr("devflow_temporal.delivery_feature_execution.continue_feature",
                        real_continue)
    response = revisions.revise_feature_plan(store, original["run_id"], command)
    assert response["repair_budget"]["used"] == 6
    worker = response["reconsideration"]["worker_summaries"][0]
    assert worker["retained_authority_preserved"]
    assert worker["session_ids"] == ["retained-original-session"]
    assert revisions.revise_feature_plan(store, original["run_id"], command) == response
    with pytest.raises(OwnershipConflict):
        revisions.revise_feature_plan(store, original["run_id"],
                                     {**command, "command_id": "another-context-command"})
    successor = prepare_authority(store, store.effective_spec(response["run_id"]))
    broker = DeliveryBroker(store, successor)
    broker.prepare()
    context = revisions.begin_revision(store, successor)
    assert context["budget"]["used"] == 7 and revisions.begin_revision(store, successor) == context
    assert context["unresolved_prior_findings"] == [
        "Retained source authority must survive derivation."]
    diagnostic = _revision_diagnostic_for(successor, diagnostic, broker.candidate())
    with pytest.raises(ValueError, match="exact finished supervised role"):
        revisions.record_proposal(store, successor, context["revision_id"], plan,
                                  diagnostic=diagnostic)
    runtime = {"dirty": False, "local_digest": None, **{k: current[k] for k in (
        "revision", "runtime_payload_sha256")}}
    intake = _complete_native_role_fixture(
        store, successor, context, plan, diagnostic, "intake", runtime,
    )
    proposal = revisions.record_proposal(store, successor, context["revision_id"], plan,
                                         diagnostic=diagnostic)
    assert revisions.record_proposal(store, successor, context["revision_id"], plan,
                                     diagnostic=diagnostic) == proposal
    with pytest.raises(ValueError, match="exact finished supervised role"):
        revisions._review_authorized(store, successor, context, proposal, {
            **passing(context, plan), "session_id": "forged",
            "proposal_session_id": intake["session_id"],
        })
    review = _complete_native_role_fixture(
        store, successor, context, plan, diagnostic, "review", runtime, status="pass",
    )
    authorized = {**review, "candidate_id": context["candidate_id"],
                  "proposal_session_id": intake["session_id"]}
    class Publisher:
        def publish_plan_revision(self, issue, old_record, value, journal, token, *, operation_id):
            result = deepcopy(old_record)
            result["manifest"].update(plan=value, plan_revision=2, version=2, revision=2)
            return result
    result = revisions.adopt_revision(store, successor, context["revision_id"], plan,
                                      authorized, gh=Publisher())
    assert result["budget"]["used"] == 7 and result["plan_identity"]["plan_revision"] == 2
    assert (result["plan_identity"]["workstream_issues"]
            == prior["old_identity"]["workstream_issues"])
    values = shared.checkpoints(prior["owner"]["issue_id"])
    assert all(values[key] == value for key, value in old.items())
    assert values[revisions.PREFIX + "rejected:" + prior["revision_id"]] == rejection
    with store._connect() as db:
        assert dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                               (original["run_id"],)).fetchone()) == row


def _boundary_snapshot(store, shared, issue_id):
    with store._connect() as db:
        runs = [dict(row) for row in db.execute("SELECT * FROM delivery_runs ORDER BY run_id")]
        commands = [dict(row) for row in db.execute("SELECT * FROM delivery_commands")]
    return (runs, commands, shared.current(issue_id), shared.checkpoints(issue_id),
            shared.budget(issue_id))


@pytest.mark.skipif(sys.platform != "darwin", reason="actual native macOS preparation required")
def test_reconsideration_refuses_missing_stale_or_widened_proof_before_reservation(
    _genuine_reconsideration, monkeypatch,
):
    fixture, command, _, _, _, _ = _genuine_reconsideration
    store, original, shared, _, _, _, prior, _, _, _ = fixture
    issue_id = prior["owner"]["issue_id"]
    initial = _boundary_snapshot(store, shared, issue_id)
    with store._connect() as db:
        review = dict(db.execute(
            "SELECT * FROM delivery_attempts WHERE run_id=? AND role='review'",
            (original["run_id"],),
        ).fetchone())
    path = Path(review["result_path"]).parent / "request.json"
    request = read_private(path)
    raw = path.read_bytes()
    def refused(expected=command):
        with pytest.raises(ValueError):
            revisions.revise_feature_plan(store, original["run_id"], expected)
        assert _boundary_snapshot(store, shared, issue_id) == initial
    for key, value in (
        ("native_authorized", False), ("workspace", original["checkout"]),
        ("result_path", str(path.parent / "caller-result.json")),
        ("revision_context", {**request["revision_context"], "candidate_id": "0" * 64}),
    ):
        write_private(path, {**request, key: value})
        refused()
    path.write_bytes(raw)
    path.rename(path.with_suffix(".retained"))
    refused()
    path.with_suffix(".retained").rename(path)
    for fields in (
        {"expected_runtime_revision": "0" * 40},
        {"proposal_sha256": "0" * 64}, {"source_root": "/caller/runtime"},
    ):
        refused({**command, "reconsideration": {**command["reconsideration"], **fields}})
    canonical = read_private(Path(request["review_diff"]["path"]))
    comparison = Path(request["review_diff"]["path"])
    comparison.write_text(json.dumps(canonical, separators=(",", ":")))
    altered = deepcopy(request)
    altered["review_diff"]["sha256"] = hashlib.sha256(comparison.read_bytes()).hexdigest()
    write_private(path, altered)
    refused()
    write_private(comparison, canonical)
    path.write_bytes(raw)
    monkeypatch.setattr("devflow_temporal.delivery_feature_closure.closed_coordinator",
                        lambda s, r: {**_closed_revision_fixture(s, r),
                                      "recovery_digest": "unbound"})
    refused()
    monkeypatch.setattr("devflow_temporal.delivery_feature_closure.closed_coordinator",
                        _closed_revision_fixture)
    from devflow_temporal.delivery_feature_execution import revised_worker_spec
    def widening(*args, **kwargs):
        result = revised_worker_spec(*args, **kwargs)
        result["policy"]["allowed_paths"].append("unapproved.py")
        return result
    monkeypatch.setattr("devflow_temporal.delivery_feature_execution.revised_worker_spec", widening)
    refused()
    def non_gate_drift(*args, **kwargs):
        result = revised_worker_spec(*args, **kwargs)
        result["policy"]["max_repairs"] += 1
        return result
    monkeypatch.setattr("devflow_temporal.delivery_feature_execution.revised_worker_spec",
                        non_gate_drift)
    refused()
    monkeypatch.setattr("devflow_temporal.delivery_feature_execution.revised_worker_spec",
                        revised_worker_spec)
    response = revisions.revise_feature_plan(store, original["run_id"], command)
    assert response["repair_budget"]["used"] == 6


@pytest.mark.skipif(sys.platform != "darwin", reason="actual native macOS preparation required")
def test_context_dedup_and_completed_native_runtime_cannot_be_replaced_by_new_commit_or_pass(
    _genuine_reconsideration,
):
    fixture, command, root, _, historical, current = _genuine_reconsideration
    store, original, shared, plan, diagnostic, _, prior, _, _, _ = fixture
    response = revisions.revise_feature_plan(store, original["run_id"], command)
    execution_source = root / review_context.PREFIX / "delivery_feature_execution.py"
    execution_source.write_text(execution_source.read_text() + "\n# unrelated runtime update\n")
    git(root, "add", ".")
    git(root, "commit", "-qm", "Comment-only runtime change")
    cosmetic = review_context.current_runtime_context()
    assert cosmetic["context_sha256"] == current["context_sha256"]
    new_command = {**command, "command_id": "commit-cannot-enable-repeat", "reconsideration": {
        **command["reconsideration"], "expected_runtime_revision": cosmetic["revision"],
        "expected_runtime_payload_sha256": cosmetic["runtime_payload_sha256"],
    }}
    before = _boundary_snapshot(store, shared, prior["owner"]["issue_id"])
    with pytest.raises(OwnershipConflict, match="already reserved"):
        revisions.revise_feature_plan(store, original["run_id"], new_command)
    assert _boundary_snapshot(store, shared, prior["owner"]["issue_id"]) == before
    successor = prepare_authority(store, store.effective_spec(response["run_id"]))
    broker = DeliveryBroker(store, successor)
    broker.prepare()
    with pytest.raises(OwnershipConflict, match="runtime expectation is stale"):
        revisions.begin_revision(store, successor)
    assert shared.budget(prior["owner"]["issue_id"])["used"] == 6
    git(root, "reset", "--hard", current["revision"])
    context = revisions.begin_revision(store, successor)
    diagnostic = _revision_diagnostic_for(successor, diagnostic, broker.candidate())
    _complete_native_role_fixture(store, successor, context, plan, diagnostic, "intake", historical)
    with pytest.raises(OwnershipConflict, match="completed fresh native role"):
        revisions.record_proposal(store, successor, context["revision_id"], plan,
                                  diagnostic=diagnostic)
    runtime = {"dirty": False, "local_digest": None, **{k: current[k] for k in (
        "revision", "runtime_payload_sha256")}}
    intake = _complete_native_role_fixture(
        store, successor, context, plan, diagnostic, "intake", runtime,
    )
    proposal = revisions.record_proposal(store, successor, context["revision_id"], plan,
                                         diagnostic=diagnostic)
    review = _complete_native_role_fixture(
        store, successor, context, plan, diagnostic, "review", historical, status="pass",
    )
    with pytest.raises(OwnershipConflict, match="completed fresh native role"):
        revisions._review_authorized(store, successor, context, proposal, {
            **review, "candidate_id": context["candidate_id"],
            "proposal_session_id": intake["session_id"],
        })
    _complete_native_role_fixture(store, successor, context, plan, diagnostic, "review", runtime)
    revisions.reject_revision(store, successor, context["revision_id"], "Fresh genuine findings")
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET candidate_json=? WHERE run_id=?",
                   (canonical_json(broker.candidate()), successor["run_id"]))
    row = _stop_revision_fixture(store, successor, shared)
    latest = {**command, "command_id": "same-context-already-reviewed",
              "expected_revision": row["revision"], "reconsideration": {
                  **command["reconsideration"], "rejected_revision_id": context["revision_id"],
              }}
    before = _boundary_snapshot(store, shared, prior["owner"]["issue_id"])
    with pytest.raises(ValueError, match="changed relevant derivation context"):
        revisions.revise_feature_plan(store, successor["run_id"], latest)
    with pytest.raises(ValueError, match="stopped owner's exact genuine rejection"):
        revisions.revise_feature_plan(store, successor["run_id"], {
            **latest, "reconsideration": command["reconsideration"],
        })
    assert _boundary_snapshot(store, shared, prior["owner"]["issue_id"]) == before
    assert shared.budget(prior["owner"]["issue_id"])["used"] == 7
