"""Consolidated revision boundaries, reusing native delivery/ownership fixtures."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest
from test_delivery_feature_execution import feature_service
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_revisions as revisions
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import registry
from devflow_temporal.delivery_plan_model import migrate_plan_v1, ordered_chunks


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
