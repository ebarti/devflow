"""Revision attempts end in a terminal receipt instead of wedging later generations."""

from __future__ import annotations

from copy import deepcopy

import pytest
from test_delivery_feature_revisions import _stop_revision_fixture, passing, proposed
from test_delivery_feature_revisions import feature as feature
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_execution as execution
from devflow_temporal import delivery_feature_revisions as revisions
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
    # The lost response is a transport failure: it propagates so the activity retry
    # resumes the same operation; nothing is rejected in between.
    with pytest.raises(ConnectionError, match="response lost after remote commit"):
        revisions.settle_adoption(
            store, spec, context["revision_id"], value, passing(context, value), gh=publisher
        )
    issue_id = spec["feature_delivery"]["owner"]["issue_id"]
    assert PREFIX + "rejected:" + context["revision_id"] not in shared.checkpoints(issue_id)
    result = revisions.settle_adoption(
        store, spec, context["revision_id"], value, passing(context, value), gh=publisher
    )
    assert result["state"] == "adopted" and publisher.calls == 2
    assert result["plan_identity"]["plan_revision"] == 2
    assert result["budget"]["used"] == 1
    assert shared.effect(spec["feature_delivery"]["owner"]["issue_id"], "commit")["state"] == (
        "complete"
    )


def _stopped_feature(store, spec, shared, monkeypatch):
    frozen = _stop_revision_fixture(store, spec, shared)
    monkeypatch.setattr(
        "devflow_temporal.delivery_feature_closure.closed_coordinator",
        lambda *_a: {"result": {"cleanup": "confirmed"}, "workflow_id": "closed"},
    )
    return frozen


def _revise(store, spec, revision, command_id="revise-1"):
    return revisions.revise_feature_plan(store, spec["run_id"], {
        "command_id": command_id,
        "expected_revision": revision,
        "reason": "Investigate the misplaced gate.",
    })


@pytest.mark.parametrize("remedy", ["replay", "abandon"])
def test_orphaned_reservation_is_replayable_or_abandonable_from_its_run(
    feature, monkeypatch, remedy,
):
    store, spec, shared, record, diagnostic = feature
    owner = spec["feature_delivery"]["owner"]
    frozen = _stopped_feature(store, spec, shared, monkeypatch)
    real = execution.continue_feature

    def failing(*_args, **_kwargs):
        raise RuntimeError("continuation predecessor Temporal closure is unavailable (x)")

    monkeypatch.setattr(execution, "continue_feature", failing)
    with pytest.raises(RuntimeError, match="unavailable"):
        _revise(store, spec, frozen["revision"])
    monkeypatch.setattr(execution, "continue_feature", real)
    with shared.connect() as db:
        reserved = shared._pending_revision(db, owner["issue_id"])
    with store._connect() as db:
        assert db.execute("SELECT 1 FROM delivery_runs WHERE run_id=?",
                          (reserved["successor_id"],)).fetchone() is None
    plain = {"command_id": "continue-1", "expected_revision": frozen["revision"]}
    with pytest.raises(OwnershipConflict, match="reserved by command revise-1 on run "
                       + spec["run_id"] + " for successor " + reserved["successor_id"]
                       + "; replay that revise-feature-plan command to admit the successor"):
        execution.continue_feature(store, spec["run_id"], plain)
    if remedy == "replay":
        replay = _revise(store, spec, frozen["revision"])
        assert replay["run_id"] == reserved["successor_id"]
        assert revisions.revision_request(store.effective_spec(replay["run_id"])) == reserved
        return
    request = {**plain, "command_id": "abandon-1", "abandon_revision": reserved["revision_id"]}
    response = execution.continue_feature(store, spec["run_id"], request)
    closed = shared.checkpoints(owner["issue_id"])[PREFIX + "rejected:" + reserved["revision_id"]]
    assert closed["closed_by"] == owner and closed["repair_key"] is None
    assert revisions.revision_request(store.effective_spec(response["run_id"])) is None
    assert execution.continue_feature(store, spec["run_id"], request) == response
