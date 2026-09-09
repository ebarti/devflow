from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

import pytest
from helpers import NOW, H, Scenario, authority, record, workflow_snapshot

from devflow.application.commands import WorkflowService
from devflow.domain.rules import scope_hash
from devflow.errors import WorkflowError


def test_editorial_delivers_without_artificial_peer_gate(tmp_path):
    s = Scenario(tmp_path)
    s.candidate()
    s.check()
    assert s.deliver()["lifecycle"] == "done"
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "done"


def test_operation_id_replay_and_conflicting_payload(tmp_path):
    s = Scenario(tmp_path)
    request = {
        "operation_id": "stable-operation",
        "work_id": s.work_id,
        "expected_revision": s.state["revision"],
    }
    first = s.service.execute("work.reconcile", request)
    assert s.service.execute("work.reconcile", request) == first
    with pytest.raises(WorkflowError, match="different payload"):
        s.service.execute("work.reconcile", {**request, "extra": True})
    with pytest.raises(WorkflowError, match="current revision"):
        s.service.execute("work.reconcile", {**request, "operation_id": "stale-operation"})


def test_concurrent_start_has_exactly_one_claim(tmp_path):
    s = Scenario(tmp_path, start=False)
    a = record(
        "attempt",
        attempt_id="attempt-1",
        work_id=s.work_id,
        scope_hash=s.state["scope_hash"],
        authority_id="auth-1",
        host_id="synthetic-host",
        owner_task_id="synthetic-owner",
        phase="implement",
        blocker=None,
        workflow_snapshot_id="snapshot-1",
        model_policy_snapshot_id="snapshot-1",
        revision=1,
        started_at=NOW,
        status="active",
    )

    def start(index):
        service = WorkflowService(tmp_path)
        try:
            return service.execute(
                "work.start",
                {
                    "operation_id": f"concurrent-{index}",
                    "work_id": s.work_id,
                    "expected_revision": 1,
                    "record": {**a, "attempt_id": f"attempt-{index}"},
                    "workflow_snapshot": workflow_snapshot(),
                },
            )["lifecycle"]
        except WorkflowError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(start, [1, 2]))
    assert sorted(results) == ["active", "stale_revision"]
    with s.service.store.connect() as db:
        assert db.execute("SELECT count(*) FROM claims").fetchone()[0] == 1


def test_repository_serializes_different_outcomes(tmp_path):
    Scenario(tmp_path)
    second = Scenario(tmp_path, work_id="synthetic-second", start=False)
    with pytest.raises(WorkflowError, match="active outcome"):
        second.start()
    assert second.state["lifecycle"] == "ready"
    assert second.state["attempt"] is None


def test_scope_amendment_invalidates_proof_preserves_history(tmp_path):
    s = Scenario(tmp_path)
    c = s.candidate()
    s.check()
    amended = deepcopy(s.contract)
    amended["scope_revision"] = 2
    amended["acceptance"][0]["expected"] = "New observable acceptance"
    with pytest.raises(WorkflowError, match="delta"):
        s.call("work.amend", record=amended, authority=authority(amended, "auth-2"))
    s.call(
        "work.amend",
        record=amended,
        authority=authority(amended, "auth-2"),
        approved_delta="synthetic:approved-user-delta",
    )
    assert s.state["candidate_id"] is None
    assert s.state["scope_hash"] == scope_hash(amended)
    assert s.state["records"]["candidate:candidate-1"] == c
    assert s.state["records"]["check_evidence:check-1"]


def test_owner_cannot_produce_independent_gate(tmp_path):
    s = Scenario(tmp_path, tier=1)
    s.candidate()
    s.check()
    a = s.assignment()
    a["task_id"] = "synthetic-owner"
    with pytest.raises(WorkflowError, match="Owner cannot"):
        s.call("assignment.record", record=a)
    gate = record(
        "gate_result",
        gate_id="fake-gate",
        assignment_id=a["assignment_id"],
        producer_task_id="synthetic-owner",
        role="review",
        candidate_id=s.state["candidate_id"],
        scope_hash=s.state["scope_hash"],
        workflow_hash=H,
        status="PASS",
        evidence_ids=["check-1"],
        finding_ids=[],
        blocking_finding_ids=[],
        limitations=[],
        completed_at=NOW,
        fix_verification_ids=[],
    )
    with pytest.raises(WorkflowError, match="independent task"):
        s.call("gate.record", record=gate)


def test_changed_candidate_rejects_old_gate_and_evidence(tmp_path):
    s = Scenario(tmp_path, tier=1)
    s.candidate()
    old = s.check()
    s.assignment()
    gate = s.gate()
    s.candidate("candidate-2", tree="c" * 40)
    with pytest.raises(WorkflowError, match="current candidate"):
        s.call("gate.record", record={**gate, "gate_id": "stale-gate"})
    with pytest.raises(WorkflowError, match="current candidate"):
        s.call("check.record", record={**old, "evidence_id": "stale-check"})
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "run_check"


def test_zero_assertions_setup_only_is_not_pass(tmp_path):
    s = Scenario(tmp_path)
    s.candidate()
    with pytest.raises(WorkflowError, match="assertions or observations"):
        s.check(assertions=0)


def test_high_fix_verified_in_same_transaction_before_gate_local_publication_pending(tmp_path):
    s = Scenario(tmp_path, tier=1)
    s.candidate()
    s.check()
    s.assignment()
    s.finding()
    with pytest.raises(WorkflowError, match="technical fix"):
        s.gate()
    s.candidate("candidate-2", tree="c" * 40)
    s.check("check-2")
    s.assignment()
    s.call(
        "finding.fix",
        finding_id="finding-1",
        candidate_id="candidate-2",
        fix_reference="synthetic:fix-commit",
        observation={
            "candidate_id": "candidate-2",
            "head_sha": "c" * 40,
            "fix_reference": "synthetic:fix-commit",
            "fix_contained": True,
            "independent_readback": True,
        },
    )
    fix = record(
        "fix_verification",
        verification_id="fix-1",
        finding_id="finding-1",
        candidate_id="candidate-2",
        assignment_id="assignment-review",
        producer_task_id="synthetic-review",
        evidence_ids=["check-2"],
        result="verified",
        verified_at=NOW,
    )
    s.gate(evidence_ids=["check-2"], fix_verifications=[fix])
    assert s.state["findings"]["finding-1"]["disposition"] == "verified_fixed"
    assert s.deliver()["lifecycle"] == "done"
    assert s.state["findings"]["finding-1"]["publication"] == "pending_pr"
    assert s.state["findings"]["finding-1"]["closure"] == "not_due"


def test_fix_then_bad_gate_rolls_back_atomically(tmp_path):
    s = Scenario(tmp_path, tier=1)
    s.candidate()
    s.check()
    s.assignment()
    s.finding()
    s.call(
        "finding.fix",
        finding_id="finding-1",
        candidate_id="candidate-1",
        fix_reference="synthetic:fix-commit",
        observation={
            "candidate_id": "candidate-1",
            "head_sha": "b" * 40,
            "fix_reference": "synthetic:fix-commit",
            "fix_contained": True,
            "independent_readback": True,
        },
    )
    before = s.state
    fix = record(
        "fix_verification",
        verification_id="fix-1",
        finding_id="finding-1",
        candidate_id="candidate-1",
        assignment_id="assignment-review",
        producer_task_id="synthetic-review",
        evidence_ids=["check-1"],
        result="verified",
        verified_at=NOW,
    )
    with pytest.raises(WorkflowError):
        s.gate(evidence_ids=["missing-evidence"], fix_verifications=[fix])
    assert s.state == before


def test_pending_launch_response_resumes_without_duplicate_action(tmp_path):
    s = Scenario(tmp_path, tier=1)
    s.candidate()
    action = s.call("action.prepare", operation="launch_role", payload={"role": "review"})["action"]
    s.confirm(action, status="ambiguous", external_id=None)
    resumed = WorkflowService(tmp_path)
    assert resumed.next(s.work_id)["actions"][0]["kind"] == "reconcile_action"
    again = s.call("action.prepare", operation="launch_role", payload={"role": "review"})["action"]
    assert again["action_id"] == action["action_id"]
    assert again["status"] == "ambiguous"


def test_receipt_alone_cannot_mark_done(tmp_path):
    s = Scenario(tmp_path)
    s.candidate()
    s.check()
    action = s.call("action.prepare", operation="publish_status", payload={"status": "success"})[
        "action"
    ]
    s.confirm(action)
    assert s.state["lifecycle"] == "active"
    assert s.state["delivery_id"] is None


def test_missing_and_corrupt_artifact_rejected_without_mutation(tmp_path):
    s = Scenario(tmp_path)
    s.candidate()
    evidence = s.check()
    path = tmp_path / "artifacts" / evidence["artifact_hash"]
    path.unlink()
    before = s.state
    with pytest.raises(WorkflowError, match="unavailable"):
        s.call("work.reconcile")
    assert s.state == before
    path.write_bytes(b"tampered")
    path.chmod(0o600)
    with pytest.raises(WorkflowError, match="does not match"):
        s.call("work.reconcile")


def test_gate_order_uses_admission_order_not_lexicographic_id(tmp_path):
    s = Scenario(tmp_path, tier=1)
    s.candidate()
    s.check()
    s.assignment()
    s.gate(identity="z-pass")
    s.gate(identity="a-fail", status="FAIL")
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "implement"


def test_qa_static_only_does_not_establish_product_pass(tmp_path):
    s = Scenario(tmp_path, tier=2)
    s.candidate()
    s.check(assertions=0, observations=["Synthetic static formatting checked"])
    s.assignment("qa")
    with pytest.raises(WorkflowError, match="QA needs"):
        s.gate("qa")


def test_interrupted_dispatch_requires_readonly_reconciliation_even_with_new_action_id(tmp_path):
    s = Scenario(tmp_path, tier=1)
    s.candidate()
    action = s.call("action.prepare", operation="launch_role", payload={"role": "review"})["action"]
    s.call("action.begin", action_id=action["action_id"])
    resumed = WorkflowService(tmp_path)
    assert resumed.next(s.work_id)["actions"][0]["kind"] == "reconcile_action"
    with pytest.raises(WorkflowError, match="must be reconciled"):
        s.call("action.begin", action_id=action["action_id"])
    repeated = s.call(
        "action.prepare",
        operation="launch_role",
        action_id="attempted-new-action",
        payload={"role": "review"},
    )["action"]
    assert repeated["action_id"] == action["action_id"]
    assert repeated["status"] == "dispatched"
    s.confirm(action, external_id="synthetic-found-original-task")
    assert len([a for a in s.state["actions"].values() if a["operation"] == "launch_role"]) == 1


def test_late_defect_event_keeps_original_delivery_and_unknown_origin(tmp_path):
    s = Scenario(tmp_path)
    s.candidate()
    s.check()
    s.deliver()
    delivery = s.state["records"]["delivery:delivery-1"]
    event = record(
        "outcome_event",
        event_id="late-user-defect",
        work_id=s.work_id,
        attempt_id=s.state["attempt"]["attempt_id"],
        candidate_id="candidate-1",
        event_kind="defect_confirmed",
        occurred_at=NOW,
        source_reference="synthetic:user-report",
        details={
            "defect_id": "synthetic-defect",
            "detector": "user",
            "stage": "post_exposure",
            "invariant": "I02",
            "severity": "high",
            "origin_candidate_id": None,
            "attribution": "unknown",
            "first_report": True,
            "known_before_user_encounter": False,
        },
    )
    s.call("outcome.record", record=event)
    assert s.state["lifecycle"] == "done"
    assert s.state["records"]["delivery:delivery-1"] == delivery
    assert (
        s.state["records"]["outcome_event:late-user-defect"]["details"]["origin_candidate_id"]
        is None
    )
    assert s.state["phase_history"][-1]["ended_at"] is not None


def test_ready_rejects_missing_authority_unknown_dependency_and_revocation(tmp_path):
    from helpers import contract

    service = WorkflowService(tmp_path)
    c = contract()
    request = {
        "work_id": c["work_id"],
        "operation_id": "ready-op",
        "expected_revision": 0,
        "record": c,
    }
    with pytest.raises(WorkflowError):
        service.execute("work.ready", request)
    c["dependencies"] = ["unknown-dependency"]
    with pytest.raises(WorkflowError, match="dependency"):
        service.execute("work.ready", {**request, "authority": authority(c)})
    c["dependencies"] = []
    auth = authority(c)
    auth["revoked"] = True
    with pytest.raises(WorkflowError, match="authority"):
        service.execute("work.ready", {**request, "authority": auth})


def test_later_check_failure_supersedes_pass_and_old_import_cannot_revive_it(tmp_path):
    s = Scenario(tmp_path, tier=1)
    s.candidate()
    passed = s.check()
    s.assignment()
    gate = s.gate()
    s.check("later-failure", status="FAIL")
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "run_check"
    s.call("check.record", record=passed)
    s.call("gate.record", record=gate)
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "run_check"
    with pytest.raises(WorkflowError, match="incomplete"):
        s.call("deliver")


def test_old_gate_reimport_does_not_replace_later_failed_gate(tmp_path):
    s = Scenario(tmp_path, tier=1)
    s.candidate()
    s.check()
    s.assignment()
    passed = s.gate()
    s.gate(identity="failure", status="FAIL")
    s.call("gate.record", record=passed)
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "implement"


def test_historic_candidate_id_cannot_rollback_current_snapshot(tmp_path):
    s = Scenario(tmp_path)
    old = s.candidate()
    s.candidate("candidate-2", tree="c" * 40)
    with pytest.raises(WorkflowError, match="Historical candidate"):
        s.call("candidate.record", record=old)
    assert s.state["candidate_id"] == "candidate-2"


def test_usage_exact_decimal_roundtrip_after_delivery_and_unknown_segment_rejected(tmp_path):
    from decimal import Decimal

    s = Scenario(tmp_path)
    s.candidate()
    s.check()
    s.deliver()
    segment = record(
        "execution_segment",
        segment_id="segment-1",
        attempt_id=s.state["attempt"]["attempt_id"],
        task_id="synthetic-owner",
        role="owner",
        model_id="synthetic-model",
        reasoning_effort="synthetic-effort",
        service_tier="synthetic-tier",
        workflow_hash=H,
        model_policy_hash=H,
        started_at=NOW,
        ended_at=NOW,
        source_reference="synthetic:host-settings",
    )
    s.call("segment.record", record=segment)
    usage = record(
        "usage",
        response_id="response-1",
        task_id="synthetic-owner",
        segment_id="segment-1",
        recorded_at=NOW,
        uncached_input_tokens=1,
        cache_read_tokens=2,
        cache_write_tokens=0,
        output_tokens=3,
        reasoning_output_tokens=1,
        ccusage_version="synthetic",
        price_snapshot_id="synthetic-price",
        api_equivalent_usd=Decimal("0.12345678901234567890123456789"),
        estimated_codex_credits=None,
        allocations=[{"work_id": s.work_id, "weight": Decimal("1")}],
        attribution_status="complete",
        pricing_status="complete",
    )
    s.call("usage.record", record=usage)
    assert (
        s.state["records"]["usage:response-1"]["api_equivalent_usd"] == usage["api_equivalent_usd"]
    )
    assert s.state["lifecycle"] == "done"
    with pytest.raises(WorkflowError, match="Unknown execution_segment"):
        s.call(
            "usage.record", record={**usage, "response_id": "response-2", "segment_id": "unknown"}
        )


def test_cancel_is_possible_when_historical_artifact_is_missing(tmp_path):
    s = Scenario(tmp_path)
    s.candidate()
    evidence = s.check()
    (tmp_path / "artifacts" / evidence["artifact_hash"]).unlink()
    s.call("work.cancel", authority_reference="synthetic:explicit-user-cancel")
    assert s.state["lifecycle"] == "canceled"
    with s.service.store.connect() as db:
        assert db.execute("SELECT count(*) FROM claims").fetchone()[0] == 0


@pytest.mark.parametrize(
    "status,next_kind", [("FAIL", "implement"), ("BLOCKED", "request_user_action")]
)
def test_finished_nonpassing_gate_routes_to_action_then_reuses_same_peer(
    tmp_path, status, next_kind
):
    s = Scenario(tmp_path, tier=1)
    s.candidate()
    s.check()
    assignment = s.assignment()
    s.gate(identity="nonpassing", status=status)
    next_action = s.service.next(s.work_id)["actions"][0]
    assert next_action["kind"] == next_kind
    assert next_action["rerun"]["reuse_task_id"] == assignment["task_id"]
    assert next_action["rerun"]["operation"] == "send_role"
    # Repair/check admission permits the same peer to evaluate the next gate.
    s.check("after-repair")
    rerun = s.service.next(s.work_id)["actions"][0]
    assert rerun["kind"] == "launch_role"
    assert rerun["operation"] == "send_role"
    assert rerun["reuse_task_id"] == assignment["task_id"]
    s.assignment()
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "wait_roles"
    s.gate(identity="repaired", evidence_ids=["after-repair"])
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "deliver"


def test_missing_release_pin_rejected_before_active_claim(tmp_path):
    s = Scenario(tmp_path, start=False)
    from helpers import workflow_snapshot

    snapshot = workflow_snapshot()
    snapshot.pop("package_revision")
    attempt = record(
        "attempt",
        attempt_id="attempt-1",
        work_id=s.work_id,
        scope_hash=s.state["scope_hash"],
        authority_id="auth-1",
        host_id="synthetic-host",
        owner_task_id="synthetic-owner",
        phase="implement",
        blocker=None,
        workflow_snapshot_id="snapshot-1",
        model_policy_snapshot_id="snapshot-1",
        revision=s.state["revision"],
        started_at=NOW,
        status="active",
    )
    with pytest.raises(WorkflowError, match="package_revision"):
        s.call("work.start", record=attempt, workflow_snapshot=snapshot)
    assert s.state["lifecycle"] == "ready"
    with s.service.store.connect() as database:
        assert database.execute("SELECT count(*) FROM claims").fetchone()[0] == 0
