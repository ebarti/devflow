"""Synthetic completed PR continuations through domain, transactions and the actual CLI."""

from copy import deepcopy
from datetime import UTC, datetime

import pytest
from domain.helpers import NOW, Scenario, record
from test_subagent_lifecycle import Subagents, staged_gate

from devflow.application.commands import WorkflowService
from devflow.domain.rules import implementation_required, next_actions, transition
from devflow.errors import WorkflowError
from devflow.validation import digest, validate_record

REPOSITORY = "github:fixture/repo"


def complete_pr(s, delivery_id="delivery-1"):
    state = s.state
    c = state["records"]["candidate:" + state["candidate_id"]]
    refs = {
        "head_ref": "feature",
        "base_ref": s.contract["endpoint"]["target"],
        "title": "Synthetic outcome",
        "body": "Synthetic proof",
    }
    action = s.call("deliver", expected_remote_state=refs)["action"]
    observation = {
        "action_id": action["action_id"],
        "payload_hash": action["payload_hash"],
        "candidate_id": c["candidate_id"],
        "head_sha": c["head_sha"],
        "tree_sha": c["tree_sha"],
        "endpoint": s.contract["endpoint"],
        "verified": True,
        "independent_readback": True,
        "refs": refs,
        "base_ref": refs["base_ref"],
        "head_ref": "feature",
        "pr_number": 1,
        "node_id": "PR-1",
        "action_marker": "<!-- devflow-pr:original-publication -->",
    }
    receipt = s.confirm(action, external_id="1", observation=observation)
    delivery = record(
        "delivery",
        delivery_id=delivery_id,
        work_id=s.work_id,
        attempt_id=state["attempt"]["attempt_id"],
        candidate_id=c["candidate_id"],
        authority_id=state["authority"]["authority_id"],
        endpoint=s.contract["endpoint"],
        gate_ids=action["payload"]["gate_ids"],
        action_id=action["action_id"],
        receipt_id=receipt["receipt_id"],
        observed_result="Synthetic PR readback",
        delivered_at=NOW,
        merge_binding=None,
        resulting_merge=None,
        status="verified",
    )
    s.call("deliver", record=delivery, observation=observation)
    return observation


def finished(tmp_path, tier=0):
    s = Subagents(
        tmp_path, tier=tier, endpoint="pr", repository=REPOSITORY, package_version="0.5.2"
    )
    worker = s.start_role()
    s.capture(worker)
    s.complete(worker)
    s.check(scenario_ids=["A01"])
    if tier:
        reviewer = s.start_role("review")
        s.cli("gate record", record=staged_gate(s, reviewer))
    s.call(
        "usage.account",
        status="unknown",
        source_reference="synthetic:original-period",
        limitations=["Synthetic fixture without host usage"],
    )
    complete_pr(s)
    return s


def reopen_request(state, entry="implement"):
    old = state["actions"][state["records"]["delivery:" + state["delivery_id"]]["action_id"]][
        "observation"
    ]
    request = {
        "operation_id": "reopen-1",
        "work_id": state["work_id"],
        "expected_revision": state["revision"],
        "record": deepcopy(state["contract"]),
        "user_request": {
            "reference": "conversation:new-request",
            "summary": "Repair conflicts in this same PR and verify it",
            "allowed_operations": ["edit", "check", "create_tasks", "publish_pr"],
        },
        "continuation": {
            "entry_phase": entry,
            "prior_delivery_id": state["delivery_id"],
            "owner_task_id": state["attempt"]["owner_task_id"],
            "host_id": state["attempt"]["host_id"],
            "pr_number": old["pr_number"],
            "head_ref": old["head_ref"],
            "base_ref": old["base_ref"],
            "expected_head": old["head_sha"],
        },
    }
    if entry == "deliver":
        c = state["records"]["candidate:" + state["candidate_id"]]
        request["reuse_candidate"] = {
            k: c[k]
            for k in ("candidate_id", "head_sha", "tree_sha", "dependency_hash", "environment_hash")
        }
    observation = {
        "repository": REPOSITORY,
        "pr_number": old["pr_number"],
        "node_id": old["node_id"],
        "head_ref": old["head_ref"],
        "base_ref": old["base_ref"],
        "head_sha": old["head_sha"],
        "state": "open",
        "draft": False,
        "action_marker": old["action_marker"],
    }
    return request, observation


def reopen_service(s):
    return WorkflowService(s.service.store.root, repository=REPOSITORY)


def test_reopen_is_atomic_durable_and_preserves_original_history(tmp_path):
    s = finished(tmp_path)
    before = s.state
    request, observation = reopen_request(before)
    # Parent stack merges legitimately retarget the same PR.
    request["record"]["endpoint"]["target"] = "new-target"
    request["record"]["scope_revision"] += 1
    request["continuation"]["base_ref"] = observation["base_ref"] = "new-target"
    service = reopen_service(s)
    result = service.execute("work.reopen", request, continuation_observation=observation)
    assert result["lifecycle"] == "active" and result["phase"] == "implement"
    after = service.snapshot(s.work_id)
    assert all(after["records"][k] == v for k, v in before["records"].items())
    assert after["actions"] == before["actions"]
    assert after["receipts"] == before["receipts"]
    assert after["assignments"] == before["assignments"]
    assert after["attempt"]["attempt_id"] == before["attempt"]["attempt_id"]
    assert after["attempt"]["entry_phase"] == before["attempt"]["entry_phase"]
    assert after["candidate_id"] is after["delivery_id"] is after["accounting_id"] is None
    assert after["check_ids"] == after["gate_ids"] == {}
    continuation = result["continuation"]
    validate_record(continuation)
    assert continuation["prior_delivery_id"] == before["delivery_id"]
    assert continuation["prior_admission_id"] == before["admission_id"]
    with service.store.connect() as db:
        claim = db.execute("SELECT work_id,attempt_id,host_id FROM claims").fetchone()
        assert tuple(claim) == (
            s.work_id,
            before["attempt"]["attempt_id"],
            before["attempt"]["host_id"],
        )
    restarted = reopen_service(s)
    assert restarted.execute("work.reopen", request, continuation_observation=None) == result
    assert restarted.snapshot(s.work_id) == after
    with pytest.raises(WorkflowError) as exc:
        restarted.execute(
            "work.reopen",
            request | {"operation_id": "stale-reopen"},
            continuation_observation=observation,
        )
    assert exc.value.code == "stale_revision"


def test_competing_repository_claim_rolls_back_every_record(tmp_path):
    s = finished(tmp_path)
    Scenario(s.service.store.root, endpoint="pr", repository=REPOSITORY, work_id="competing-work")
    before = s.state
    request, observation = reopen_request(before)
    with pytest.raises(WorkflowError) as exc:
        reopen_service(s).execute("work.reopen", request, continuation_observation=observation)
    assert exc.value.code == "already_claimed"
    assert s.state == before
    with s.service.store.connect() as db:
        assert not db.execute(
            "SELECT 1 FROM records WHERE record_key LIKE 'work_continuation:%'"
        ).fetchall()


@pytest.mark.parametrize(
    "mutation,error",
    [
        (lambda r, o: r.pop("user_request"), "user_request_required"),
        (
            lambda r, o: r["user_request"].update(allowed_operations=["publish_pr"]),
            "missing_authority",
        ),
        (
            lambda r, o: r["continuation"].update(owner_task_id="another-owner"),
            "continuation_owner",
        ),
        (lambda r, o: r["continuation"].update(host_id="another-host"), "continuation_owner"),
        (lambda r, o: r["continuation"].update(pr_number=2), "continuation_pr"),
        (lambda r, o: o.update(node_id="PR-other"), "continuation_pr"),
        (lambda r, o: o.update(repository="github:another/repo"), "continuation_pr"),
        (lambda r, o: o.update(head_ref="other-source"), "continuation_pr"),
        (lambda r, o: o.update(head_sha="c" * 40), "continuation_pr"),
        (lambda r, o: o.update(base_ref="other-target"), "continuation_pr"),
        (lambda r, o: o.update(state="closed"), "continuation_pr"),
        (
            lambda r, o: r["record"]["acceptance"][0].update(expected="different outcome"),
            "continuation_scope",
        ),
        (
            lambda r, o: r["record"]["source"].update(stable_id="another-issue"),
            "continuation_scope",
        ),
        (lambda r, o: r["record"]["endpoint"].update(kind="merge"), "continuation_scope"),
    ],
)
def test_invalid_continuations_leave_the_completed_outcome_unchanged(tmp_path, mutation, error):
    s = finished(tmp_path)
    before = s.state
    request, observation = reopen_request(before)
    mutation(request, observation)
    with pytest.raises(WorkflowError) as exc:
        reopen_service(s).execute("work.reopen", request, continuation_observation=observation)
    assert exc.value.code == error
    assert s.state == before


@pytest.mark.parametrize("entry", ["implement", "deliver"])
@pytest.mark.parametrize("status", ["prepared", "dispatched", "ambiguous", "pending_setup"])
def test_unresolved_actions_cannot_hide_behind_done(tmp_path, entry, status):
    s = finished(tmp_path)
    state = s.state
    next(a for a in state["actions"].values() if a["operation"] == "prepare_workspace")[
        "status"
    ] = status
    before = deepcopy(state)
    request, observation = reopen_request(state, entry)
    with pytest.raises(WorkflowError) as exc:
        transition(
            state,
            "work.reopen",
            request,
            datetime.now(UTC),
            repository=REPOSITORY,
            continuation_observation=observation,
        )
    assert exc.value.code == "reconcile_required"
    assert state == before


def test_delivery_reuses_exact_candidate_without_invented_implementation(tmp_path):
    s = finished(tmp_path, tier=1)
    before = s.state
    request, observation = reopen_request(before, "deliver")
    request["user_request"]["allowed_operations"] = ["publish_pr"]
    service = reopen_service(s)
    result = service.execute("work.reopen", request, continuation_observation=observation)
    state = service.snapshot(s.work_id)
    assert result["phase"] == "deliver"
    assert state["candidate_id"] == before["candidate_id"]
    assert state["gate_ids"] == before["gate_ids"] and state["check_ids"] == before["check_ids"]
    assert state["assignments"] == before["assignments"]
    assert state["attempt"]["entry_phase"] == "implement"
    assert implementation_required(state)  # The actual completed original worker satisfies reuse.
    assert not any(
        a["kind"] in {"implement", "run_check", "launch_role"} for a in next_actions(state)
    )


@pytest.mark.parametrize(
    "change",
    [
        "head_sha",
        "tree_sha",
        "dependency_hash",
        "environment_hash",
        "workflow",
        "context",
        "target",
    ],
)
def test_deliver_rejects_changed_inputs_without_reusing_pass(tmp_path, change):
    s = finished(tmp_path)
    before = s.state
    request, observation = reopen_request(before, "deliver")
    if change == "workflow":
        snap = deepcopy(before["records"]["workflow_snapshot:snapshot-1"])
        snap.update(snapshot_id="new-snapshot", workflow_hash=digest("changed workflow"))
        request["workflow_snapshot"] = snap
    elif change in {"context", "target"}:
        request["record"]["scope_revision"] += 1
        if change == "context":
            request["record"]["context"].append(
                {"summary": "Changed input", "reference": "synthetic:change"}
            )
        else:
            request["record"]["endpoint"]["target"] = "retargeted"
            request["continuation"]["base_ref"] = observation["base_ref"] = "retargeted"
    else:
        request["reuse_candidate"][change] = "c" * (40 if "sha" in change else 64)
    with pytest.raises(WorkflowError) as exc:
        reopen_service(s).execute("work.reopen", request, continuation_observation=observation)
    assert exc.value.code == "continuation_reuse"
    assert s.state == before


@pytest.mark.parametrize("status", ["running", "interrupted", "completed"])
def test_unfinished_producer_ingestion_blocks_even_when_lifecycle_is_done(tmp_path, status):
    s = finished(tmp_path)
    state = s.state
    state["assignments"]["pending-worker"] = {"role": "implementation_worker", "status": status}
    request, observation = reopen_request(state)
    with pytest.raises(WorkflowError) as exc:
        transition(
            state,
            "work.reopen",
            request,
            datetime.now(UTC),
            repository=REPOSITORY,
            continuation_observation=observation,
        )
    assert exc.value.code == "producer_result_required"


@pytest.mark.parametrize(
    "kind,lifecycle",
    [("local", "done"), ("merge", "done"), ("release", "done"), ("pr", "canceled")],
)
def test_command_is_not_general_terminal_reactivation(tmp_path, kind, lifecycle):
    s = finished(tmp_path)
    state = s.state
    state["lifecycle"] = lifecycle
    state["contract"]["endpoint"]["kind"] = kind
    request, observation = reopen_request(state)
    with pytest.raises(WorkflowError) as exc:
        transition(
            state,
            "work.reopen",
            request,
            datetime.now(UTC),
            repository=REPOSITORY,
            continuation_observation=observation,
        )
    assert exc.value.code == "invalid_state"


@pytest.mark.parametrize("value", [None, [], "continue"])
def test_malformed_continuation_is_a_structured_request_error(tmp_path, value):
    s = finished(tmp_path)
    request, observation = reopen_request(s.state)
    request["continuation"] = value
    before = s.state
    with pytest.raises(WorkflowError) as exc:
        reopen_service(s).execute("work.reopen", request, continuation_observation=observation)
    assert exc.value.code == "invalid_request"
    assert s.state == before


@pytest.mark.parametrize("version", ["0.4.0", "0.5.2"])
def test_missing_independent_result_cannot_be_hidden_by_completed_assignment(tmp_path, version):
    s = finished(tmp_path, tier=1)
    state = s.state
    state["records"]["workflow_snapshot:snapshot-1"]["package_version"] = version
    gate_id = state["gate_ids"]["review"]
    del state["records"]["gate_result:" + gate_id]
    request, observation = reopen_request(state)
    with pytest.raises(WorkflowError) as exc:
        transition(state, "work.reopen", request, datetime.now(UTC), repository=REPOSITORY,
                   continuation_observation=observation)
    assert exc.value.code == "gate_result_required"
