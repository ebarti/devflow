"""Revision attempts end in a terminal receipt instead of wedging later generations."""

from __future__ import annotations

from copy import deepcopy

import pytest
from test_delivery_feature_revisions import _stop_revision_fixture, passing, proposed
from test_delivery_feature_revisions import feature as feature
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_revisions as revisions
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_execution_registry import OwnershipConflict

PREFIX = revisions.PREFIX


def _published(record, value):
    published = deepcopy(record)
    published["manifest"].update(plan=value, plan_revision=2, version=2, revision=2)
    return published


def _sealed(store, spec, record, diagnostic):
    context = revisions.begin_revision(store, spec, diagnostic)
    value = proposed(record)
    revisions.record_proposal(store, spec, context["revision_id"], value)
    return context, value


def test_refused_adoption_rejects_the_attempt_and_frees_the_next_investigation(feature):
    store, spec, shared, record, diagnostic = feature
    owner = spec["feature_delivery"]["owner"]
    context, value = _sealed(store, spec, record, diagnostic)
    another = {**diagnostic, "detail": "A second evidenced planning defect."}
    with pytest.raises(OwnershipConflict, match="already has different evidence"):
        revisions.begin_revision(store, spec, another)

    class Publisher:
        def publish_plan_revision(self, *_a, **_k):
            raise AssertionError("a refused adoption must not publish")

        load_record = publish_plan_revision

    changed = deepcopy(value)
    changed["scope"] = "A plan the review never sealed"
    result = revisions.settle_adoption(
        store, spec, context["revision_id"], changed, passing(context, value), gh=Publisher()
    )
    assert result["state"] == "rejected"
    assert result["reason"] == "adoption differs from its sealed correction proposal"
    rejected = shared.checkpoints(owner["issue_id"])[PREFIX + "rejected:" + context["revision_id"]]
    assert rejected == result["plan_revision"]
    assert rejected["repair_key"] == context["repair_key"]
    assert result["budget"]["used"] == 1 == shared.budget(owner["issue_id"])["used"]
    assert revisions.revision_request(spec) is None
    second = revisions.begin_revision(store, spec, another)
    assert second["revision_id"] != context["revision_id"]
    assert second["budget"]["used"] == 2


def test_lost_publication_response_completes_adoption_from_github_readback(feature):
    store, spec, shared, record, diagnostic = feature
    context, value = _sealed(store, spec, record, diagnostic)
    published = _published(record, value)

    class Publisher:
        calls = 0

        def publish_plan_revision(self, issue, old, plan, journal, token, *, operation_id):
            self.calls += 1
            if self.calls == 1:
                journal.intent(token, "commit", "github_plan_revision",
                               {"operation_id": operation_id})
                raise ConnectionError("response lost after remote commit")
            journal.finish_effect(token, "commit", {"revision": 2})
            return deepcopy(published)

        def load_record(self, issue, reference):
            assert reference == {"comment_id": 99, "comment_node_id": "IC_99"}
            return deepcopy(published)

    publisher = Publisher()
    result = revisions.settle_adoption(
        store, spec, context["revision_id"], value, passing(context, value), gh=publisher
    )
    assert result["state"] == "adopted" and publisher.calls == 2
    assert result["plan_identity"]["plan_revision"] == 2
    assert result["budget"]["used"] == 1
    assert shared.effect(spec["feature_delivery"]["owner"]["issue_id"], "commit")["state"] == (
        "complete"
    )


def test_publication_refused_before_any_remote_change_rejects(feature):
    store, spec, shared, record, diagnostic = feature
    context, value = _sealed(store, spec, record, diagnostic)

    class Publisher:
        def publish_plan_revision(self, *_a, **_k):
            raise OwnershipConflict("GitHub delivery record changed before plan revision")

        def load_record(self, issue, reference):
            return deepcopy(record)

    result = revisions.settle_adoption(
        store, spec, context["revision_id"], value, passing(context, value), gh=Publisher()
    )
    assert result["state"] == "rejected"
    assert result["reason"] == "GitHub delivery record changed before plan revision"
    assert revisions.revision_request(spec) is None


def test_uncertain_publication_stays_open_until_its_readback_settles(feature):
    store, spec, shared, record, diagnostic = feature
    owner = spec["feature_delivery"]["owner"]
    context, value = _sealed(store, spec, record, diagnostic)
    published = _published(record, value)

    class Publisher:
        remote = None
        readable = False

        def publish_plan_revision(self, issue, old, plan, journal, token, *, operation_id):
            journal.intent(token, "commit", "github_plan_revision", {"operation_id": operation_id})
            if self.remote is None:
                raise ConnectionError("response lost before the commit was confirmed")
            journal.finish_effect(token, "commit", {"revision": 2})
            return deepcopy(self.remote)

        def load_record(self, issue, reference):
            if not self.readable:
                raise OSError("GitHub unavailable")
            return deepcopy(self.remote or record)

    publisher = Publisher()
    review = passing(context, value)
    for _ in range(2):
        with pytest.raises(ConnectionError, match="before the commit was confirmed"):
            revisions.settle_adoption(
                store, spec, context["revision_id"], value, review, gh=publisher
            )
        publisher.readable = True
    values = shared.checkpoints(owner["issue_id"])
    assert PREFIX + "rejected:" + context["revision_id"] not in values
    assert shared.effect(owner["issue_id"], "commit")["state"] == "pending"
    with pytest.raises(OwnershipConflict, match="already has different evidence"):
        revisions.begin_revision(store, spec, {**diagnostic, "detail": "Another defect."})

    publisher.remote = published
    result = revisions.settle_adoption(
        store, spec, context["revision_id"], value, review, gh=publisher
    )
    assert result["state"] == "adopted" and result["budget"]["used"] == 1


def test_foreign_remote_revision_keeps_the_attempt_uncertain(feature):
    store, spec, shared, record, diagnostic = feature
    context, value = _sealed(store, spec, record, diagnostic)
    foreign = deepcopy(record)
    foreign["manifest"].update(plan={**value, "scope": "edited elsewhere"}, plan_revision=2)

    class Publisher:
        def publish_plan_revision(self, *_a, **_k):
            raise ConnectionError("response lost")

        def load_record(self, issue, reference):
            return deepcopy(foreign)

    with pytest.raises(ConnectionError):
        revisions.settle_adoption(
            store, spec, context["revision_id"], value, passing(context, value), gh=Publisher()
        )
    issue_id = spec["feature_delivery"]["owner"]["issue_id"]
    assert PREFIX + "rejected:" + context["revision_id"] not in shared.checkpoints(issue_id)


def test_later_generation_closes_a_stopped_investigation_and_keeps_its_debit(
    feature, monkeypatch,
):
    store, spec, shared, record, diagnostic = feature
    owner = spec["feature_delivery"]["owner"]
    stale = revisions.begin_revision(store, spec, diagnostic)
    assert stale["budget"]["used"] == 1
    frozen = _stop_revision_fixture(store, spec, shared)
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
    successor = store.effective_spec(receipt["run_id"])
    DeliveryBroker(store, successor).prepare()
    context = revisions.begin_revision(store, successor)
    assert context["revision_id"] == receipt["revision_id"] != stale["revision_id"]
    assert context["budget"]["used"] == 2
    closed = shared.checkpoints(owner["issue_id"])[PREFIX + "rejected:" + stale["revision_id"]]
    assert closed["closed_by"] == successor["feature_delivery"]["owner"]
    assert closed["repair_key"] == stale["repair_key"]
    assert closed["old_identity"] == stale["old_identity"]
    assert "stopped with generation 1" in closed["reason"]
    assert revisions.begin_revision(store, successor) == context
    assert shared.budget(owner["issue_id"])["used"] == 2
