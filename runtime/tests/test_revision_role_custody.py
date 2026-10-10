"""Revision roles retain distinct attempts without manufacturing launch authority."""
from __future__ import annotations

from copy import deepcopy

import pytest
from test_delivery_store import service as service
from test_delivery_store import submit_historical_admission

from devflow_temporal import delivery_feature_revisions as revisions
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.supervisor import DeliverySupervisor


def test_revision_attempts_do_not_reuse_intake_or_another_proposal(service, monkeypatch):
    store, submission = service
    submit_historical_admission(store, submission, monkeypatch)
    spec = store.spec("run-1")
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    request = {"spec": spec, "role": "intake", "iteration": 0,
               "candidate": broker.candidate(), "resume_session": None}
    supervisor = DeliverySupervisor(store, capacity=1)
    old_key, _ = supervisor._claim(request)
    admitted = []

    def authenticate(_store, value):
        context = value["revision_context"]
        admitted.append(context["revision_id"])
        return {"revision_id": context["revision_id"],
                "proposed_plan_sha256": digest(context["proposed_plan"])
                if value["role"] == "review" else None}

    monkeypatch.setattr(revisions, "authenticate_revision_role", authenticate)
    one = {**request, "revision_context": {"revision_id": "revision-one"}}
    two = {**request, "revision_context": {"revision_id": "revision-two"}}
    first, _ = supervisor._claim(one)
    repeat, _ = supervisor._claim(deepcopy(one))
    second, _ = supervisor._claim(two)
    reviewed, _ = supervisor._claim({
        **request, "role": "review",
        "revision_context": {"revision_id": "revision-one", "proposed_plan": {"scope": "A"}},
    })
    assert first == repeat
    assert len({old_key, first, second, reviewed}) == 4
    assert admitted == ["revision-one", "revision-one", "revision-two", "revision-one"]
    with store._connect() as db:
        rows = list(db.execute("SELECT job_key,state FROM delivery_attempts WHERE run_id='run-1'"))
    assert {row["job_key"] for row in rows} == {old_key, first, second, reviewed}
    assert all(row["state"] == "queued" for row in rows)


def test_unadmitted_revision_does_not_reserve_attempt_or_create_folder(service, monkeypatch):
    store, submission = service
    submit_historical_admission(store, submission, monkeypatch)
    spec = store.spec("run-1")
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    request = {"spec": spec, "role": "intake", "iteration": 0,
               "candidate": broker.candidate(),
               "revision_context": {"revision_id": "invented-revision"}}

    def reject(*_args):
        raise ValueError("revision has no charged durable admission")

    monkeypatch.setattr(revisions, "authenticate_revision_role", reject)
    supervisor = DeliverySupervisor(store, capacity=1)
    key = supervisor._job_key(request)
    with pytest.raises(ValueError, match="no charged durable admission"):
        supervisor._claim(request)
    assert not (broker.state_dir / "attempts" / key).exists()
    with store._connect() as db:
        attempt = db.execute("SELECT 1 FROM delivery_attempts WHERE job_key=?", (key,)).fetchone()
        assert attempt is None


def test_revision_context_cannot_namespace_a_product_writer():
    request = {"spec": {"run_id": "run-1", "policy_digest": "p"},
               "role": "implement", "iteration": 0, "candidate": {"id": "candidate"},
               "revision_context": {"revision_id": "revision-one"}}
    with pytest.raises(ValueError, match="separate read-only attempt"):
        DeliverySupervisor._job_key(request)
