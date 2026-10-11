"""Revision attempts end in a terminal receipt instead of wedging later generations."""

from __future__ import annotations

import hashlib
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_delivery_feature_revisions import _stop_revision_fixture, passing, proposed
from test_delivery_feature_revisions import feature as feature
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_execution as execution
from devflow_temporal import delivery_feature_revisions as revisions
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_execution_registry import ExecutionRegistry, OwnershipConflict
from devflow_temporal.delivery_store import DeliveryStore

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


def test_new_revision_command_closes_the_stopped_attempt_and_keeps_its_debit(
    feature, monkeypatch,
):
    store, spec, shared, record, diagnostic = feature
    owner = spec["feature_delivery"]["owner"]
    stale = revisions.begin_revision(store, spec, diagnostic)
    assert stale["budget"]["used"] == 1
    frozen = _stopped_feature(store, spec, shared, monkeypatch)
    receipt = _revise(store, spec, frozen["revision"])
    closed = shared.checkpoints(owner["issue_id"])[PREFIX + "rejected:" + stale["revision_id"]]
    assert closed["closed_by"] == owner
    assert closed["repair_key"] == stale["repair_key"]
    assert closed["old_identity"] == stale["old_identity"]
    assert closed["reason"] == "superseded by revision command revise-1"
    successor = store.effective_spec(receipt["run_id"])
    DeliveryBroker(store, successor).prepare()
    context = revisions.begin_revision(store, successor)
    assert context["revision_id"] == receipt["revision_id"] != stale["revision_id"]
    assert context["budget"]["used"] == 2 == shared.budget(owner["issue_id"])["used"]
    assert revisions.begin_revision(store, successor) == context


def test_later_generation_closes_a_legacy_stranded_investigation_at_its_next_begin(
    feature, monkeypatch,
):
    store, spec, shared, record, diagnostic = feature
    owner = spec["feature_delivery"]["owner"]
    frozen = _stopped_feature(store, spec, shared, monkeypatch)
    receipt = _revise(store, spec, frozen["revision"])
    successor = store.effective_spec(receipt["run_id"])
    DeliveryBroker(store, successor).prepare()
    current = successor["feature_delivery"]["owner"]
    # A begin: left open by the stopped generation before this fix existed.
    stale = {
        "revision_id": "revision-stranded", "phase": "investigating", "owner": owner,
        "old_identity": revisions.adopted_plan_identity(successor, shared),
        "repair_key": "plan-revision:revision-stranded", "diagnostic": diagnostic,
    }
    shared.revision_checkpoint(current, PREFIX + "begin:revision-stranded", stale)
    context = revisions.begin_revision(store, successor)
    closed = shared.checkpoints(owner["issue_id"])[PREFIX + "rejected:revision-stranded"]
    assert closed["closed_by"] == current and closed["repair_key"] == stale["repair_key"]
    assert "stopped with generation 1 before adoption; closed by generation 2" in closed["reason"]
    assert context["revision_id"] == receipt["revision_id"]
    assert revisions.begin_revision(store, successor) == context


def test_continuation_closes_a_stopped_investigation_once_and_then_proceeds(
    feature, monkeypatch,
):
    store, spec, shared, record, diagnostic = feature
    owner = spec["feature_delivery"]["owner"]
    stale = revisions.begin_revision(store, spec, diagnostic)
    frozen = _stopped_feature(store, spec, shared, monkeypatch)
    request = {"command_id": "continue-1", "expected_revision": frozen["revision"]}
    with pytest.raises(OwnershipConflict, match="now closed; replay continue-feature continue-1"):
        execution.continue_feature(store, spec["run_id"], request)
    closed = shared.checkpoints(owner["issue_id"])[PREFIX + "rejected:" + stale["revision_id"]]
    assert closed["closed_by"] == owner and closed["repair_key"] == stale["repair_key"]
    assert "closed by continuation command continue-1" in closed["reason"]
    assert shared.budget(owner["issue_id"])["used"] == 1
    response = execution.continue_feature(store, spec["run_id"], request)
    successor = store.effective_spec(response["run_id"])
    assert successor["feature_delivery"]["owner"]["generation"] == 2
    assert revisions.revision_request(successor) is None
    assert execution.continue_feature(store, spec["run_id"], request) == response


def test_unstarted_reservation_names_itself_and_can_be_abandoned_explicitly(
    feature, monkeypatch,
):
    store, spec, shared, record, diagnostic = feature
    owner = spec["feature_delivery"]["owner"]
    frozen = _stopped_feature(store, spec, shared, monkeypatch)
    receipt = _revise(store, spec, frozen["revision"])
    successor = store.effective_spec(receipt["run_id"])
    stopped = _stop_revision_fixture(store, successor, shared)
    reserved = successor["feature_delivery"]["owner"]
    with pytest.raises(OwnershipConflict, match="reserved by command revise-1 on run "
                       + spec["run_id"] + " for successor " + receipt["run_id"]
                       + "; replay that revise-feature-plan command") as custody:
        shared.claim(spec["feature_delivery"]["snapshot"], "run-feature-other",
                     str(store.config.tracking_db), predecessor=reserved)
    # Refusals lead with the non-destructive remedy; abandonment is named only as a
    # permanent owner decision, never as the retry an orchestrator should follow.
    assert "would permanently reject the pending revision" in str(custody.value)
    with pytest.raises(OwnershipConflict, match="another revision command in custody: revision "
                       + receipt["revision_id"] + " reserved by command revise-1") as held:
        _revise(store, successor, stopped["revision"], command_id="revise-2")
    assert "replay that revise-feature-plan command" in str(held.value)
    assert "would permanently reject the pending revision" in str(held.value)
    with pytest.raises(ValueError, match="requires a command ID"):
        execution.continue_feature(store, successor["run_id"], {
            "command_id": "abandon-0", "expected_revision": stopped["revision"],
            "abandon_revision": ""})
    with pytest.raises(OwnershipConflict, match="no reserved revision revision-unknown; its "
                       "recorded reservation is " + receipt["revision_id"]):
        execution.continue_feature(store, successor["run_id"], {
            "command_id": "abandon-1", "expected_revision": stopped["revision"],
            "abandon_revision": "revision-unknown"})
    request = {"command_id": "abandon-2", "expected_revision": stopped["revision"],
               "abandon_revision": receipt["revision_id"]}
    response = execution.continue_feature(store, successor["run_id"], request)
    closed = shared.checkpoints(owner["issue_id"])[PREFIX + "rejected:" + receipt["revision_id"]]
    assert closed["closed_by"] == reserved and closed["repair_key"] is None
    assert closed["reason"] == "abandoned by continuation command abandon-2"
    assert shared.budget(owner["issue_id"])["used"] == 0
    third = store.effective_spec(response["run_id"])
    assert third["feature_delivery"]["owner"]["generation"] == 3
    assert revisions.revision_request(third) is None
    assert execution.continue_feature(store, successor["run_id"], request) == response
    plain = _stop_revision_fixture(store, third, shared)
    with pytest.raises(OwnershipConflict, match="no reserved revision revision-unknown to abandon"):
        execution.continue_feature(store, third["run_id"], {
            "command_id": "abandon-3", "expected_revision": plain["revision"],
            "abandon_revision": "revision-unknown"})
    # Naming the reservation this feature already closed is idempotent, not a refusal.
    again = execution.continue_feature(store, third["run_id"], {
        "command_id": "abandon-4", "expected_revision": plain["revision"],
        "abandon_revision": receipt["revision_id"]})
    assert store.effective_spec(again["run_id"])["feature_delivery"]["owner"]["generation"] == 4


def test_remote_pull_request_lookup_failure_is_retryable_not_a_refusal(monkeypatch):
    from devflow_temporal import delivery_store

    def run(argv, **_kwargs):
        return SimpleNamespace(returncode=returncode, stdout=stdout, stderr="gh: offline")

    monkeypatch.setattr(delivery_store.subprocess, "run", run)
    returncode, stdout = 1, ""
    with pytest.raises(RuntimeError, match="lookup failed \\(1\\): gh: offline"):
        DeliveryStore._ensure_no_remote_pr("example/fixture", "feat/df-1")
    returncode, stdout = 0, "[]"
    assert DeliveryStore._ensure_no_remote_pr("example/fixture", "feat/df-1") is None
    stdout = '[{"number": 7}]'
    with pytest.raises(ValueError, match="has a remote pull request"):
        DeliveryStore._ensure_no_remote_pr("example/fixture", "feat/df-1")


def _bound_worker_defect(store, spec, shared, record, diagnostic):
    """A finished worker whose product repair at iteration 1 bound a planning defect."""
    from devflow_temporal.delivery_feature_execution import (
        register_worker,
        worker_key,
        worker_spec,
    )
    from devflow_temporal.delivery_github_contract import ordered_chunks

    owner = spec["feature_delivery"]["owner"]
    plan = record["manifest"]["plan"]
    chunk = next(item for item in ordered_chunks(plan) if item["id"] == "model")
    stream = next(s for s in plan["workstreams"] if any(c["id"] == "model" for c in s["chunks"]))
    child = worker_spec(spec, chunk, record["manifest"]["workstream_issues"][stream["id"]],
                        kind="build", base_sha=spec["base_sha"], base_branch="main")
    register_worker(store, spec, child)
    broker = DeliveryBroker(store, child)
    broker.prepare()
    candidate = broker.candidate()
    evidence = Path(child["state_dir"]) / "planning-evidence.json"
    evidence.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    evidence.write_text('{"finding":"the model layer depends on an unbuilt endpoint"}')
    defect = {**diagnostic, "candidate_id": candidate["id"], "evidence": [
        {"path": str(evidence), "sha256": hashlib.sha256(evidence.read_bytes()).hexdigest()}]}
    shared.repair(owner, child["run_id"] + ":1", "Product repair at worker iteration 1")
    binding = revisions.planning_defect_repair_binding(child, child["run_id"], 1, defect)
    assert binding["repair_key"] == child["run_id"] + ":1"
    store.project(child["run_id"], phase="blocked", execution_state="blocked",
                  event_type="blocked", message="Planning defect diagnosed", candidate=candidate,
                  checks={"planning_defect_repair": binding}, iteration=1, protocol_revision=2,
                  outcome="blocked", cleanup="confirmed")
    shared.checkpoint(owner, "assignment:model:build", {
        "chunk_id": "model", "kind": "build", "run_id": child["run_id"],
        "store_path": str(store.config.tracking_db)})
    shared.finish_worker(owner, worker_key(child["run_id"], owner), {"cleanup": "confirmed"})
    return child, defect, binding


def test_worker_paid_planning_defect_is_charged_once_across_a_continuation(
    feature, monkeypatch,
):
    from devflow_temporal.contracts import digest

    store, spec, shared, record, diagnostic = feature
    owner = spec["feature_delivery"]["owner"]
    child, defect, binding = _bound_worker_defect(store, spec, shared, record, diagnostic)
    assert shared.budget(owner["issue_id"])["used"] == 1

    frozen = _stopped_feature(store, spec, shared, monkeypatch)
    response = execution.continue_feature(store, spec["run_id"], {
        "command_id": "continue-1", "expected_revision": frozen["revision"]})
    successor = store.effective_spec(response["run_id"])
    DeliveryBroker(store, successor).prepare()
    context = revisions.begin_revision(store, successor, defect)
    assert context["repair_key"] == child["run_id"] + ":1"
    assert context["paid_worker_run_id"] == child["run_id"]
    assert context["budget"]["used"] == 1 == shared.budget(owner["issue_id"])["used"]
    assert revisions.begin_revision(store, successor, defect) == context
    # The binding names the debit and the defect, never the observing generation.
    assert binding == {
        "revision_id": "revision-" + digest({"worker_run_id": child["run_id"],
                                             "repair_key": child["run_id"] + ":1",
                                             "diagnostic": defect})[:24],
        "repair_key": child["run_id"] + ":1",
        "diagnostic_sha256": digest(defect),
    }


def test_repair_alias_written_by_an_earlier_generation_keeps_its_single_debit(tmp_path):
    shared = ExecutionRegistry(tmp_path / "control" / "execution.sqlite3")
    issue = {"id": "I_feature", "repository_id": "R_fixture", "url": "https://example.invalid/1"}
    first = shared.claim({"issue": issue}, "run-1", str(tmp_path / "local.sqlite3"))
    shared.repair(first, "plan-revision:revision-1", "Planning correction revision-1")
    shared.revision_checkpoint(first, "plan-revision:repair-alias:worker:5", {
        "revision_id": "revision-1", "repair_key": "plan-revision:revision-1",
        "generation": 1, "reason": "Product repair at worker iteration 5"})
    shared.stop(first, "stopped", {})
    second = shared.claim({"issue": issue}, "run-2", str(tmp_path / "local.sqlite3"),
                          predecessor=first)
    assert shared.repair(second, "worker:5", "Product repair at worker iteration 5")["used"] == 1
    with pytest.raises(OwnershipConflict, match="alias lost its exact cumulative debit"):
        shared.repair(second, "worker:5", "Product repair at worker iteration 6")
    assert shared.budget(issue["id"])["used"] == 1


def test_worker_debit_backs_at_most_one_attempt(feature, monkeypatch):
    store, spec, shared, record, diagnostic = feature
    owner = spec["feature_delivery"]["owner"]
    child, defect, _ = _bound_worker_defect(store, spec, shared, record, diagnostic)
    first = revisions.begin_revision(store, spec, defect)
    assert first["repair_key"] == child["run_id"] + ":1"
    assert shared.budget(owner["issue_id"])["used"] == 1
    revisions.reject_revision(store, spec, first["revision_id"],
                              "independent plan review rejected the correction")
    frozen = _stopped_feature(store, spec, shared, monkeypatch)
    response = execution.continue_feature(store, spec["run_id"], {
        "command_id": "continue-1", "expected_revision": frozen["revision"]})
    successor = store.effective_spec(response["run_id"])
    DeliveryBroker(store, successor).prepare()
    second = revisions.begin_revision(store, successor, defect)
    assert second["revision_id"] != first["revision_id"]
    # The worker's cycle already backed the first attempt; the next one pays anew.
    assert second["repair_key"] == "plan-revision:" + second["revision_id"]
    assert second["paid_worker_run_id"] is None
    assert second["budget"]["used"] == 2 == shared.budget(owner["issue_id"])["used"]


def test_transient_publication_failure_propagates_for_the_activity_retry(feature):
    from devflow_temporal.delivery_github_contract import GitHubContractError

    store, spec, shared, record, diagnostic = feature
    issue_id = spec["feature_delivery"]["owner"]["issue_id"]
    context, value = _sealed(store, spec, record, diagnostic)
    published = _published(record, value)

    class Flaky:
        healthy = False

        def publish_plan_revision(self, issue, old, plan, journal, token, *, operation_id):
            if not self.healthy:
                raise GitHubContractError("gh: HTTP 502: Bad Gateway", 502)
            journal.intent(token, "commit", "github_plan_revision", {"operation_id": operation_id})
            journal.finish_effect(token, "commit", {"revision": 2})
            return deepcopy(published)

        def load_record(self, issue, reference):
            return deepcopy(record)

    gh = Flaky()
    review = passing(context, value)
    with pytest.raises(GitHubContractError, match="HTTP 502"):
        revisions.settle_adoption(store, spec, context["revision_id"], value, review, gh=gh)
    assert PREFIX + "rejected:" + context["revision_id"] not in shared.checkpoints(issue_id)
    gh.healthy = True  # the activity's next retry attempt
    result = revisions.settle_adoption(store, spec, context["revision_id"], value, review, gh=gh)
    assert result["state"] == "adopted" and result["budget"]["used"] == 1


def _staging_publisher(record, failure):
    """Mirror _stage_locked's order: competing check, stage, child plans, then a failure."""
    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_github_plans import _ensure_no_competing_effects, _stage_key

    class Staging:
        def publish_plan_revision(self, issue, old, plan, journal, token, *, operation_id):
            _ensure_no_competing_effects(journal, token, operation_id)
            journal.checkpoint(token, _stage_key(operation_id), {
                "operation_id": operation_id, "before_record": old,
                "target_plan_revision": 2, "plan_digest": digest(plan)})
            key = "github-child-plan:" + digest({"operation_id": operation_id, "stream": "api"})
            journal.intent(token, key, "github_child_plan", {
                "operation_id": operation_id, "child": {"id": "child-api", "number": 10}})
            journal.finish_effect(token, key, {"comment_id": 7, "comment_node_id": "IC_7"})
            raise failure

        def load_record(self, issue, reference):
            return deepcopy(record)

    return Staging()


def test_closed_staged_attempt_releases_custody_for_the_next_correction(feature):
    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_github_contract import GitHubContractError
    from devflow_temporal.delivery_github_plans import _ensure_no_competing_effects, _stage_key

    store, spec, shared, record, diagnostic = feature
    owner = spec["feature_delivery"]["owner"]
    context, value = _sealed(store, spec, record, diagnostic)
    staging = _staging_publisher(
        record, GitHubContractError("gh: HTTP 502 while hydrating staged child plans", 502))
    with pytest.raises(GitHubContractError):
        revisions.settle_adoption(
            store, spec, context["revision_id"], value, passing(context, value), gh=staging)
    values = shared.checkpoints(owner["issue_id"])
    assert PREFIX + "rejected:" + context["revision_id"] not in values
    assert _stage_key(context["revision_id"]) in values
    # The attempt ends (independent review refused it); its staged custody is released
    # with a ledger of the child plans it already wrote.
    revisions.reject_revision(store, spec, context["revision_id"],
                              "independent plan review rejected the correction")
    ledger = shared.checkpoints(owner["issue_id"])[
        PREFIX + "publication-released:" + context["revision_id"]]
    assert ledger["stage_key"] == _stage_key(context["revision_id"])
    assert ledger["released_by"] == owner and ledger["target_plan_revision"] == 2
    assert ledger["children"] == [{
        "effect_key": "github-child-plan:" + digest(
            {"operation_id": context["revision_id"], "stream": "api"}),
        "state": "complete", "child": {"id": "child-api", "number": 10},
        "reference": {"comment_id": 7, "comment_node_id": "IC_7"}}]
    another = {**diagnostic, "detail": "A second evidenced planning defect."}
    second = revisions.begin_revision(store, spec, another)
    assert second["budget"]["used"] == 2
    value2 = deepcopy(value)
    value2["workstreams"][0]["chunks"][0]["verification"].append("Verify the second defect")
    revisions.record_proposal(store, spec, second["revision_id"], value2)
    published = _published(record, value2)

    class Clean:
        def publish_plan_revision(self, issue, old, plan, journal, token, *, operation_id):
            _ensure_no_competing_effects(journal, token, operation_id)
            key = "github-plan-revision:" + digest(operation_id)
            journal.intent(token, key, "github_plan_revision", {"operation_id": operation_id})
            journal.finish_effect(token, key, {"plan_revision": 2})
            return deepcopy(published)

        def load_record(self, issue, reference):
            return deepcopy(record)

    result = revisions.settle_adoption(
        store, spec, second["revision_id"], value2, passing(second, value2), gh=Clean())
    assert result["state"] == "adopted" and result["budget"]["used"] == 2


def test_published_attempt_cannot_be_closed_as_rejected(feature):
    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_github_plans import _stage_key

    store, spec, shared, record, diagnostic = feature
    owner = spec["feature_delivery"]["owner"]
    context, value = _sealed(store, spec, record, diagnostic)
    rid = context["revision_id"]
    shared.checkpoint(owner, _stage_key(rid), {"operation_id": rid, "target_plan_revision": 2})
    key = "github-plan-revision:" + digest(rid)
    shared.intent(owner, key, "github_plan_revision", {"operation_id": rid})
    shared.finish_effect(owner, key, {"plan_revision": 2})
    with pytest.raises(OwnershipConflict, match="already published its plan revision"):
        revisions.reject_revision(store, spec, rid, "operator closed the attempt")
    assert PREFIX + "rejected:" + rid not in shared.checkpoints(owner["issue_id"])


def test_abandon_replays_exactly_after_a_failure_past_the_closure(feature, monkeypatch):
    store, spec, shared, record, diagnostic = feature
    owner = spec["feature_delivery"]["owner"]
    frozen = _stopped_feature(store, spec, shared, monkeypatch)
    receipt = _revise(store, spec, frozen["revision"])
    successor = store.effective_spec(receipt["run_id"])
    stopped = _stop_revision_fixture(store, successor, shared)
    closed = {"result": {"cleanup": "confirmed"}, "workflow_id": "closed"}
    calls = []

    def flaky(*_args):
        calls.append(True)
        if len(calls) == 1:
            raise RuntimeError("continuation predecessor Temporal closure is unavailable (x)")
        return closed

    monkeypatch.setattr("devflow_temporal.delivery_feature_closure.closed_coordinator", flaky)
    request = {"command_id": "abandon-1", "expected_revision": stopped["revision"],
               "abandon_revision": receipt["revision_id"]}
    with pytest.raises(RuntimeError, match="unavailable"):
        execution.continue_feature(store, successor["run_id"], request)
    assert PREFIX + "rejected:" + receipt["revision_id"] in shared.checkpoints(owner["issue_id"])
    response = execution.continue_feature(store, successor["run_id"], request)
    assert store.effective_spec(response["run_id"])["feature_delivery"]["owner"]["generation"] == 3
    assert execution.continue_feature(store, successor["run_id"], request) == response


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
