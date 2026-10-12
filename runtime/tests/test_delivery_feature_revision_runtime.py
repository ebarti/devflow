"""Completed roles use a currently qualified retry proof without rewriting its receipt."""
from __future__ import annotations

from contextlib import nullcontext
from copy import deepcopy
from types import SimpleNamespace

import pytest
from test_delivery_feature_review_context import context_repository, git

from devflow_temporal import delivery_feature_review_context as runtime
from devflow_temporal import delivery_feature_revisions as revisions
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_execution_registry import OwnershipConflict


@pytest.fixture
def completed_retry(tmp_path, monkeypatch):
    root, _, _, sealed_runtime = context_repository(tmp_path, monkeypatch)
    source = root / runtime.PREFIX / "delivery_feature_activities.py"
    source.write_text(source.read_text() + "\n# Activity liveness deployment\n")
    git(root, "add", ".")
    git(root, "commit", "-qm", "Activity-only deployment with unchanged review context")
    current_runtime = runtime.current_runtime_context()
    assert sealed_runtime["context_sha256"] == current_runtime["context_sha256"]
    assert sealed_runtime["guards"] == current_runtime["guards"]
    assert sealed_runtime["runtime_payload_sha256"] != current_runtime["runtime_payload_sha256"]
    owner = {"issue_id": "owned-issue", "generation": 2, "run_id": "retry-run"}
    candidate = {"id": "exact-candidate", "head": "head", "base_sha": "base",
                 "content_sha256": "content", "environment_digest": "environment",
                 "policy_digest": "policy"}
    findings = ["Original genuine review finding remains unresolved"]
    receipt = {
        "current_runtime": sealed_runtime, "context_key": "unchanged-semantic-key",
        "current_worker_subject_sha256": "unchanged-subject",
        "input_digests": {"retained_worker_inputs": "unchanged-authority"},
        "product_candidate": runtime.candidate_subject(candidate),
        "unresolved_prior_findings_sha256": digest(findings),
        "original_repair_budget": {"used": 6, "maximum": 10},
    }
    selectors = {"expected_runtime_revision": sealed_runtime["revision"],
                 "expected_runtime_payload_sha256": sealed_runtime["runtime_payload_sha256"]}
    public = {"reconsideration": receipt, "unresolved_prior_findings": findings,
              "predecessor_run_id": "original", "owner": {"generation": 1},
              "record": "exact-record", "predecessor_closure": "exact-closure",
              "request": {"reconsideration": selectors}}
    context = {"revision_id": "revision-fixture", "owner": owner,
               "reconsideration": receipt, "unresolved_prior_findings": findings}
    spec = {"run_id": "retry-run", "feature_plan_revision_request": public,
            "plan_approval": "required"}
    values = {revisions.PREFIX + "request:revision-fixture": public,
              revisions.PREFIX + "begin:revision-fixture": {"repair_key": "paid-planning"}}
    fresh = {**deepcopy(receipt), "current_runtime": current_runtime,
             "original_repair_budget": {"used": 7, "maximum": 10}}
    h = SimpleNamespace(spec=spec, context=context, receipt=receipt, public=public,
                        values=values, proof=fresh, candidate=candidate, active_owner=owner,
                        current_runtime=current_runtime, sealed_runtime=sealed_runtime,
                        saved_calls=0, has_retry=True)
    h.store = SimpleNamespace(effective_spec=lambda name: {"run_id": name})
    database = SimpleNamespace(execute=lambda *_a: SimpleNamespace(fetchone=lambda: (2,)))

    def settlement(_db, token):
        if token != h.active_owner:
            raise OwnershipConflict("current settlement owner changed")

    shared = SimpleNamespace(connect=lambda: nullcontext(database), require_settlement=settlement)

    def custody(_store, supplied, original):
        assert original == {"owner": public["owner"]}
        if supplied["plan_approval"] != "required":
            raise OwnershipConflict("original approval binding changed")

    def prove(_store, original, expected, record, closure, observed, *, planning_paid):
        assert original == {"run_id": "original"}
        assert record == public["record"] and closure == public["predecessor_closure"]
        assert observed == values and planning_paid
        assert expected["expected_runtime_revision"] == (
            current_runtime["revision"] if h.has_retry else sealed_runtime["revision"])
        return deepcopy(h.proof), deepcopy(findings)

    def saved(*_a):
        h.saved_calls += 1
        return {"status": "plan", "session_id": "fresh-session", "cleanup": "confirmed"}, {
            "classification": "native", "runtime_identity": deepcopy(h.native_identity)}

    h.native_identity = {"dirty": False, "local_digest": None,
                         **{k: current_runtime[k] for k in ("revision", "runtime_payload_sha256")}}
    monkeypatch.setattr(revisions, "_registry", lambda _spec: shared)
    monkeypatch.setattr(revisions, "_values", lambda _spec: values)
    monkeypatch.setattr(revisions, "_preparation_retry", lambda *_a: (
        {"reconsideration": receipt, "selectors": selectors} if h.has_retry else None))
    monkeypatch.setattr(revisions, "_checkpoint_proof", lambda *_a: None)
    monkeypatch.setattr(revisions, "authenticate_adopted_plan_custody", custody)
    monkeypatch.setattr(revisions, "_reconsideration_proof", prove)
    monkeypatch.setattr(revisions, "_candidate", lambda *_a: candidate)
    monkeypatch.setattr(revisions, "_saved_revision_role", saved)
    return h


def test_completed_fresh_role_matches_current_qualified_proof_without_rewriting_history(
    completed_retry,
):
    h = completed_retry
    original = deepcopy((h.receipt, h.public, h.context, h.values))
    for role in ("intake", "review"):
        result = revisions._completed_reconsideration_role(h.store, h.spec, h.context, None, role)
        assert result["session_id"] == "fresh-session" and result["cleanup"] == "confirmed"
    fresh = revisions._recheck_reconsideration(h.store, h.spec, h.context)
    assert fresh["current_runtime"] == h.current_runtime
    assert fresh["original_repair_budget"] == h.receipt["original_repair_budget"]
    assert (h.receipt, h.public, h.context, h.values) == original
    # The old native source is fully authenticated and context-equivalent, but
    # it must never pass the exact *current* completed-runtime comparison.
    h.native_identity.update({k: h.sealed_runtime[k]
                              for k in ("revision", "runtime_payload_sha256")})
    with pytest.raises(OwnershipConflict, match="different sealed review context"):
        revisions._completed_reconsideration_role(h.store, h.spec, h.context, None, "intake")


def test_current_retry_proof_still_refuses_guard_subject_authority_approval_and_owner_drift(
    completed_retry,
):
    h = completed_retry
    original_proof = deepcopy(h.proof)
    for group, field in (
        ("current_runtime", "context_sha256"),
        ("current_runtime", "guards"),
        ("input_digests", "retained_worker_inputs"),
        (None, "current_worker_subject_sha256"),
    ):
        h.proof = deepcopy(original_proof)
        (h.proof[group] if group else h.proof)[field] = "changed"
        with pytest.raises(OwnershipConflict, match="derived subject changed"):
            revisions._completed_reconsideration_role(h.store, h.spec, h.context, None, "intake")
    h.proof = original_proof
    h.spec["plan_approval"] = "automatic"
    with pytest.raises(OwnershipConflict, match="approval binding changed"):
        revisions._completed_reconsideration_role(h.store, h.spec, h.context, None, "intake")
    h.spec["plan_approval"] = "required"
    h.active_owner = {**h.active_owner, "generation": 3}
    with pytest.raises(OwnershipConflict, match="settlement owner changed"):
        revisions._completed_reconsideration_role(h.store, h.spec, h.context, None, "intake")
    h.active_owner = h.context["owner"]
    h.has_retry = False
    with pytest.raises(OwnershipConflict, match="derived subject changed"):
        revisions._completed_reconsideration_role(h.store, h.spec, h.context, None, "intake")
    assert h.saved_calls == 0
