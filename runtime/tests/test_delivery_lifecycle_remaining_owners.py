from copy import deepcopy

import pytest
from test_delivery_publication_validation import publication as publication
from test_delivery_publication_validation import service as service

from devflow_temporal.delivery_lifecycle import RecoveryRefused, assess_transition
from devflow_temporal.delivery_publication_validation import KIND


def _database(store):
    with store._connect() as db:
        return list(db.iterdump())


def test_public_observation_is_read_only_and_matches_preflight(publication, monkeypatch):
    store, broker, payload, *_ = publication
    before_db = _database(store)
    before_head = broker.candidate()
    read = store._completed_temporal_result
    def closed(*args, **kwargs):
        assert kwargs.get("preserve") is False
        return read(*args, **kwargs)
    monkeypatch.setattr(store, "_completed_temporal_result", closed)
    observed = assess_transition(store, "run-1", KIND, payload)
    direct = store.repair_admission_preflight("run-1", payload)["lifecycle_assessment"]
    assert observed == direct
    assert observed["eligible"] is True
    assert observed["candidate"]["id"] == payload["expected_candidate_id"]
    assert before_db == _database(store)
    assert before_head == broker.candidate()


@pytest.mark.parametrize(("field", "replacement", "code"), [
    ("expected_revision", 10, "revision_changed"),
    ("expected_projection_revision", 999, "projection_revision_changed"),
    ("expected_candidate_id", "f" * 64, "candidate_changed"),
    ("expected_pr_number", 8, "publication_changed"),
    ("expected_pr_head", "f" * 40, "candidate_head_changed"),
])
def test_public_stale_preflight_and_execution_agree(publication, field, replacement, code):
    store, broker, payload, *_ = publication
    payload = {**payload, field: replacement}
    before = _database(store)
    observed = assess_transition(store, "run-1", KIND, payload)
    assert observed["eligible"] is False
    assert code in {item["code"] for item in observed["blockers"]}
    with pytest.raises(RecoveryRefused) as refusal:
        store.continue_repair("run-1", payload)
    assert code in {item["code"] for item in refusal.value.assessment["blockers"]}
    assert before == _database(store)


def test_public_observation_reports_unknown_owner_evidence(publication, monkeypatch):
    store, _, payload, *_ = publication
    def unavailable(*_args, **_kwargs):
        raise OSError("unreadable")
    monkeypatch.setattr(store, "_completed_temporal_result", unavailable)
    observed = assess_transition(store, "run-1", KIND, payload)
    assert observed["eligible"] is False
    assert any(b["owner"] == "publication_validation" and
               b["code"] == "publication_validation_proof_unavailable" and
               b["missing_or_unknown_fact"] and b["next_action"]
               for b in observed["blockers"])


def test_resource_closure_public_assessment_and_stale_refusal(publication, monkeypatch, tmp_path):
    from devflow_temporal import delivery_resource_closure as closure

    store = publication[0]
    payload = {"continuation_kind": closure.KIND, "command_id": "closure-check",
               "expected_revision": 7, "expected_iteration": 4,
               "expected_candidate_id": "a" * 64, "expected_pr_number": 8,
               "expected_pr_head": "b" * 40, "additional_iterations": 0,
               "authority_path": "/authority", "authority_sha256": "c" * 64,
               "controller_path": "/controller", "controller_sha256": "d" * 64}
    seal = {"state": {"iteration": 4, "candidate": {"id": "a" * 64, "head": "b" * 40},
                      "pull_request": {"number": 8}},
            "original_row": {"protocol_revision": 7}, "spec": {"state_dir": str(tmp_path)}}
    observations = []
    def snapshot(_store, _run_id, _payload, *, observation=False):
        observations.append(observation)
        return deepcopy(seal), {"required": False}
    monkeypatch.setattr(closure, "_snapshot", snapshot)
    monkeypatch.setattr(closure, "_ancestors", lambda *a, **k: None)
    monkeypatch.setattr(closure, "private_directory", lambda *_: pytest.fail("unexpected write"))
    before = _database(store)
    observed = assess_transition(store, "run-1", closure.KIND, payload)
    assert observed["eligible"] is True
    assert all(observations)
    stale = {**payload, "expected_revision": 8}
    observed = assess_transition(store, "run-1", closure.KIND, stale)
    assert "revision_changed" in {b["code"] for b in observed["blockers"]}
    with pytest.raises(RecoveryRefused) as refused:
        store.continue_repair("run-1", stale)
    assert "revision_changed" in {b["code"] for b in refused.value.assessment["blockers"]}
    assert before == _database(store)


def test_retired_adjudication_public_assessment_matches_execution(publication):
    from devflow_temporal import delivery_investigation_adjudication as adjudication

    store = publication[0]
    payload = {"continuation_kind": adjudication.KIND, "command_id": "retired-check",
               "expected_revision": 7, "expected_iteration": 4,
               "expected_candidate_id": "a" * 64, "expected_pr_number": 8,
               "expected_pr_head": "b" * 40, "additional_iterations": 0,
               "authority_path": "/authority", "authority_sha256": "c" * 64,
               "controller_path": "/controller", "controller_sha256": "d" * 64}
    observed = assess_transition(store, "run-1", adjudication.KIND, payload)
    assert observed["eligible"] is False
    assert "adjudication_admission_retired" in {b["code"] for b in observed["blockers"]}
    with pytest.raises(RecoveryRefused) as refused:
        store.continue_repair("run-1", payload)
    assert refused.value.assessment == observed
