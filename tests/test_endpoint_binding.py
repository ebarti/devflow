"""Synthetic endpoint adapters prove targets are bounded before external mutation."""

from copy import deepcopy
from pathlib import Path

import pytest
from domain.helpers import NOW, SHA, H, Scenario, record

from devflow.errors import WorkflowError
from devflow.execution import dispatch_action
from devflow.validation import canonical_json


class Remote:
    def __init__(self, wrong=False, failures=None):
        self.wrong = wrong
        self.failures = list(failures or [])
        self.writes = []
        self.reads = []

    def _result(self, kind, kwargs):
        if kind == "pr":
            return {
                "status": "published",
                "pr_number": 1,
                "head_sha": SHA,
                "base_ref": "other" if self.wrong else kwargs["base_ref"],
            }
        if kind == "merge":
            return {
                "status": "verified",
                "commit_sha": "e" * 40,
                "tree_sha": SHA,
                "target_ref": "other" if self.wrong else kwargs["target_ref"],
                "target_ancestry_verified": True,
            }
        return {
            "status": "published",
            "release_id": 1,
            "commit_sha": SHA,
            "tag": "v2-NOT-authorized" if self.wrong else kwargs["tag"],
        }

    def _write(self, kind, kwargs):
        if self.failures:
            raise WorkflowError(
                "github_rejected",
                "Synthetic permission rejection",
                {"no_mutation": self.failures.pop(0)},
            )
        self.writes.append((kind, kwargs))
        return self._result(kind, kwargs)

    def publish_pr(self, **kwargs):
        return self._write("pr", kwargs)

    def reconcile_pr(self, **kwargs):
        self.reads.append(kwargs)
        return self._result("pr", kwargs)

    def publish_release(self, **kwargs):
        return self._write("release", kwargs)

    def reconcile_release(self, **kwargs):
        if self.failures:
            raise WorkflowError(
                "github_rejected", "Synthetic read rejection", {"no_mutation": self.failures.pop(0)}
            )
        return self._result("release", kwargs)

    def deliver(self, number, **kwargs):
        return self._write("merge", kwargs)


class Git:
    def __init__(self, path, wrong=False):
        self.path, self.wrong = path, wrong

    def observe(self):
        return {
            "head_sha": SHA,
            "tree_sha": SHA,
            "clean": True,
            "path": "/synthetic/OTHER" if self.wrong else str(Path(self.path).resolve()),
        }

    def identity(self):
        return "github:synthetic/repository"


def scenario(tmp_path, kind):
    s = Scenario(tmp_path, endpoint=kind, repository="github:synthetic/repository")
    s.candidate()
    s.check()
    return s


def preparation(kind):
    refs = {}
    fields = {}
    if kind == "pr":
        refs = {
            "base_ref": "main",
            "head_ref": "feature",
            "title": "Synthetic",
            "body": "Synthetic",
        }
    elif kind == "release":
        refs = {"tag": "v1-approved", "title": "Synthetic", "notes": "Synthetic"}
    elif kind == "merge":
        refs = {"target_ref": "main", "target_sha": SHA}
        fields["merge_binding"] = {
            "source_heads": [
                {
                    "pr_reference": "https://github.com/synthetic/repository/pull/1",
                    "source_ref": "feature",
                    "head_sha": SHA,
                }
            ],
            "target_ref": "main",
            "target_sha": SHA,
            "expected_integrated_tree": SHA,
            "protection_snapshot_hash": H,
            "merge_method": "squash",
        }
    return {"expected_remote_state": refs, **fields}


def dispatch(s, action, remote=None, wrong_local=False, repository="/synthetic/worktree"):
    return dispatch_action(
        s.service,
        {
            "operation_id": f"dispatch-{next(s.sequence)}",
            "work_id": s.work_id,
            "expected_revision": s.state["revision"],
            "action_id": action["action_id"],
        },
        repository=repository,
        github_factory=lambda *args: remote,
        git_factory=lambda path: Git(path, wrong=wrong_local),
    )


def delivery(s, action, result, status="verified"):
    observation = result["observation"]
    return record(
        "delivery",
        delivery_id=f"delivery-{status}",
        work_id=s.work_id,
        attempt_id=s.state["attempt"]["attempt_id"],
        candidate_id=s.state["candidate_id"],
        authority_id=s.state["authority"]["authority_id"],
        endpoint=s.contract["endpoint"],
        gate_ids=action["payload"]["gate_ids"],
        action_id=action["action_id"],
        receipt_id=result["receipt_id"],
        observed_result="Synthetic actual target readback",
        delivered_at=NOW,
        merge_binding=action["payload"].get("merge_binding"),
        resulting_merge=observation.get("resulting_merge"),
        status=status,
    )


@pytest.mark.parametrize("kind", ["local", "pr", "merge", "release"])
def test_malicious_target_rejected_before_admission_or_mutation(tmp_path, kind):
    s = scenario(tmp_path, kind)
    fields = preparation(kind)
    field = {"local": "path", "pr": "base_ref", "merge": "target_ref", "release": "tag"}[kind]
    fields["expected_remote_state"][field] = (
        "/synthetic/OTHER" if kind == "local" else "NOT-authorized"
    )
    if kind == "merge":
        fields["merge_binding"]["target_ref"] = "NOT-authorized"
    before = s.state
    with pytest.raises(WorkflowError, match="accepted|redirects"):
        s.call("deliver", **fields)
    assert s.state == before


@pytest.mark.parametrize("kind", ["local", "pr", "merge", "release"])
def test_false_adapter_target_readback_is_exposed_and_never_done(tmp_path, kind):
    s = scenario(tmp_path, kind)
    action = s.call("deliver", **preparation(kind))["action"]
    result = dispatch(s, action, Remote(wrong=True), wrong_local=kind == "local")
    observation = result["observation"]
    assert observation["status"] == "exposed_unverified"
    assert observation["verified"] is False
    assert observation["endpoint"]["target"] != s.contract["endpoint"]["target"]
    with pytest.raises(WorkflowError):
        s.call("deliver", record=delivery(s, action, result), observation=observation)
    s.call(
        "deliver", record=delivery(s, action, result, "exposed_unverified"), observation=observation
    )
    assert s.state["lifecycle"] == "active"
    assert s.state["blocker"]["code"] == "exposed_unverified"


@pytest.mark.parametrize("kind", ["local", "pr", "merge", "release"])
def test_exact_target_succeeds_normally(tmp_path, kind):
    s = scenario(tmp_path, kind)
    action = s.call("deliver", **preparation(kind))["action"]
    result = dispatch(s, action, Remote())
    assert result["observation"]["verified"] is True
    assert (
        s.call("deliver", record=delivery(s, action, result), observation=result["observation"])[
            "lifecycle"
        ]
        == "done"
    )


def test_actual_checkout_argument_cannot_redirect_local_delivery(tmp_path):
    s = scenario(tmp_path, "local")
    action = s.call("deliver")["action"]
    with pytest.raises(WorkflowError, match="Dispatch checkout"):
        dispatch(s, action, repository="/synthetic/OTHER")
    assert s.state["actions"][action["action_id"]]["status"] == "prepared"


def test_conflicting_refs_and_payload_cannot_override_approved_pr_base(tmp_path):
    s = scenario(tmp_path, "pr")
    with pytest.raises(WorkflowError, match="conflicting"):
        s.call(
            "action.prepare",
            operation="publish_pr",
            payload={"base_ref": "other"},
            expected_remote_state={"base_ref": "main"},
        )
    with pytest.raises(WorkflowError, match="accepted target"):
        s.call("action.prepare", operation="publish_pr", payload={"base_ref": "other"})


@pytest.mark.parametrize("kind", ["pr", "merge", "release"])
def test_legacy_prepared_target_revalidated_before_dispatch(tmp_path, kind):
    s = scenario(tmp_path, kind)
    action = s.call("deliver", **preparation(kind))["action"]
    # Simulate a persisted record admitted by the old target-blind runtime.
    state = deepcopy(s.state)
    stored = state["actions"][action["action_id"]]
    stored["expected_remote_state"][
        {"pr": "base_ref", "merge": "target_ref", "release": "tag"}[kind]
    ] = "other"
    with s.service.store.transaction() as db:
        db.execute("UPDATE works SET state=? WHERE work_id=?", (canonical_json(state), s.work_id))
    remote = Remote()
    with pytest.raises(WorkflowError, match="accepted target"):
        dispatch(s, action, remote)
    assert remote.writes == []


def test_early_pr_reuses_original_correlation_with_fresh_terminal_proof(tmp_path):
    s = Scenario(tmp_path, endpoint="pr", repository="github:synthetic/repository")
    s.candidate()
    fields = preparation("pr")
    early = s.call("action.prepare", operation="publish_pr", payload={}, **fields)["action"]
    remote = Remote()
    published = dispatch(s, early, remote)
    s.check()
    # A publication receipt by itself cannot be repurposed as terminal proof.
    forged = dict(early)
    forged["payload"] = {**early["payload"], "gate_ids": []}
    with pytest.raises(WorkflowError):
        s.call(
            "deliver", record=delivery(s, forged, published), observation=published["observation"]
        )
    terminal = s.call("deliver", **fields)["action"]
    assert terminal["action_id"] != early["action_id"]
    assert terminal["payload"]["publication_action_id"] == early["action_id"]
    result = dispatch(s, terminal, remote)
    assert len(remote.writes) == 1
    assert remote.reads[-1]["action_id"] == early["action_id"]
    assert (
        s.call("deliver", record=delivery(s, terminal, result), observation=result["observation"])[
            "lifecycle"
        ]
        == "done"
    )


def test_definite_failed_action_can_be_explicitly_retried_with_receipts_retained(tmp_path):
    s = scenario(tmp_path, "release")
    action = s.call("deliver", **preparation("release"))["action"]
    remote = Remote(failures=[True])
    failed = dispatch(s, action, remote)
    assert failed["status"] == "failed"
    assert remote.writes == []
    with pytest.raises(WorkflowError, match="action retry"):
        dispatch(s, action, remote)
    assert len(s.state["actions"][action["action_id"]]["receipts"]) == 1
    s.call("action.retry", action_id=action["action_id"])
    completed = dispatch(s, action, remote)
    assert completed["status"] == "confirmed"
    assert len(remote.writes) == 1
    assert len(s.state["actions"][action["action_id"]]["receipts"]) == 2
    assert (
        s.call(
            "deliver", record=delivery(s, action, completed), observation=completed["observation"]
        )["lifecycle"]
        == "done"
    )


def test_earlier_uncertainty_forbids_retry_after_later_definite_read_failure(tmp_path):
    s = scenario(tmp_path, "release")
    action = s.call("deliver", **preparation("release"))["action"]
    remote = Remote(failures=[False, True])
    assert dispatch(s, action, remote)["status"] == "ambiguous"
    with pytest.raises(WorkflowError):
        s.call("action.retry", action_id=action["action_id"])
    assert dispatch(s, action, remote)["status"] == "ambiguous"
    with pytest.raises(WorkflowError, match="Only a definitely failed"):
        s.call("action.retry", action_id=action["action_id"])
    assert remote.writes == []
