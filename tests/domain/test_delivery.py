import pytest
from helpers import NOW, SHA, H, Scenario, record

from devflow.errors import WorkflowError


def prepared_delivery(s, *, status="verified", observation_delta=None, record_delta=None):
    state = s.state
    candidate = state["records"]["candidate:" + state["candidate_id"]]
    refs = (
        {"head_sha": candidate["head_sha"], "target_ref": "main", "target_sha": SHA}
        if s.contract["endpoint"]["kind"] == "merge"
        else {}
    )
    binding = None
    actual = None
    extra = {}
    if s.contract["endpoint"]["kind"] == "merge":
        binding = {
            "source_heads": [
                {
                    "pr_reference": "synthetic:pr:1",
                    "source_ref": "synthetic-feature",
                    "head_sha": candidate["head_sha"],
                }
            ],
            "target_ref": "main",
            "target_sha": SHA,
            "expected_integrated_tree": candidate["tree_sha"],
            "protection_snapshot_hash": H,
            "merge_method": "squash",
        }
        actual = {
            "commit_sha": "e" * 40,
            "tree_sha": candidate["tree_sha"],
            "target_ancestry_verified": True,
        }
        extra = {"merge_binding": binding}
    action = s.call(
        "deliver", candidate_id=candidate["candidate_id"], expected_remote_state=refs, **extra
    )["action"]
    s.call("action.begin", action_id=action["action_id"])
    observation = {
        "action_id": action["action_id"],
        "payload_hash": action["payload_hash"],
        "candidate_id": candidate["candidate_id"],
        "head_sha": candidate["head_sha"],
        "tree_sha": candidate["tree_sha"],
        "endpoint": s.contract["endpoint"],
        {"local": "path", "pr": "base_ref", "merge": "target_ref", "release": "tag"}[
            s.contract["endpoint"]["kind"]
        ]: s.contract["endpoint"]["target"],
        "verified": status == "verified",
        "independent_readback": True,
        "refs": refs,
        "protection_verified": True,
        "required_checks_verified": True,
        "resulting_merge": actual,
    }
    observation.update(observation_delta or {})
    receipt = s.confirm(action, observation=observation)
    delivery = record(
        "delivery",
        delivery_id="delivery-1",
        work_id=s.work_id,
        attempt_id=state["attempt"]["attempt_id"],
        candidate_id=candidate["candidate_id"],
        authority_id=state["authority"]["authority_id"],
        endpoint=s.contract["endpoint"],
        gate_ids=action["payload"]["gate_ids"],
        action_id=action["action_id"],
        receipt_id=receipt["receipt_id"],
        observed_result="Synthetic independently observed endpoint",
        delivered_at=NOW,
        merge_binding=binding,
        resulting_merge=actual,
        status=status,
    )
    delivery.update(record_delta or {})
    return delivery, observation


def test_merge_requires_matching_independent_integrated_tree(tmp_path):
    s = Scenario(tmp_path, endpoint="merge")
    s.candidate()
    s.check()
    delivery, observation = prepared_delivery(s)
    assert s.call("deliver", record=delivery, observation=observation)["lifecycle"] == "done"


@pytest.mark.parametrize(
    "delta,error",
    [
        ({"head_sha": "d" * 40}, "prepared candidate"),
        ({"payload_hash": "d" * 64}, "prepared candidate"),
        (
            {"refs": {"head_sha": SHA, "target_ref": "main", "target_sha": "d" * 40}},
            "expected refs",
        ),
        ({"protection_verified": False}, "Protected fresh"),
        ({"required_checks_verified": False}, "Protected fresh"),
        (
            {
                "resulting_merge": {
                    "commit_sha": "e" * 40,
                    "tree_sha": "d" * 40,
                    "target_ancestry_verified": True,
                }
            },
            "matching independent",
        ),
    ],
)
def test_remote_races_and_mismatched_receipts_never_complete(tmp_path, delta, error):
    s = Scenario(tmp_path, endpoint="merge")
    s.candidate()
    s.check()
    delivery, observation = prepared_delivery(s, observation_delta=delta)
    with pytest.raises(WorkflowError, match=error):
        s.call("deliver", record=delivery, observation=observation)
    assert s.state["lifecycle"] == "active"
    assert s.state["delivery_id"] is None


def test_changed_result_cannot_replace_receipt_observation(tmp_path):
    s = Scenario(tmp_path)
    s.candidate()
    s.check()
    delivery, observation = prepared_delivery(s)
    changed = {**observation, "unrecorded_claim": "invented"}
    with pytest.raises(WorkflowError, match="persisted with its receipt"):
        s.call("deliver", record=delivery, observation=changed)


@pytest.mark.parametrize("status", ["queued", "exposed_unverified"])
def test_nonverified_delivery_is_durable_and_never_done(tmp_path, status):
    s = Scenario(tmp_path, endpoint="merge")
    s.candidate()
    s.check()
    delivery, observation = prepared_delivery(s, status=status)
    s.call("deliver", record=delivery, observation=observation)
    assert s.state["lifecycle"] == "active"
    assert s.state["blocker"]["code"] == status
    assert s.state["records"]["delivery:delivery-1"]["status"] == status


def test_endpoint_mutation_cannot_bypass_delivery_preparation(tmp_path):
    s = Scenario(tmp_path, endpoint="merge")
    with pytest.raises(WorkflowError, match="through deliver"):
        s.call("action.prepare", operation="merge", payload={})
    with pytest.raises(WorkflowError, match="candidate"):
        s.call("deliver")


def test_high_finding_added_after_preparation_blocks_dispatch(tmp_path):
    s = Scenario(tmp_path)
    s.candidate()
    s.check()
    action = s.call("deliver")["action"]
    s.finding()
    with pytest.raises(WorkflowError, match="proof changed"):
        s.call("action.begin", action_id=action["action_id"])
    assert s.state["actions"][action["action_id"]]["status"] == "prepared"


def test_remote_endpoint_blocks_unpublished_low_finding(tmp_path):
    s = Scenario(tmp_path, endpoint="pr")
    s.candidate()
    s.check()
    s.finding(severity="low")
    with pytest.raises(WorkflowError, match="incomplete"):
        s.call("deliver")
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "publish_findings"


def test_publication_requires_matching_readback_and_retains_distinct_finding(tmp_path):
    s = Scenario(tmp_path, endpoint="pr")
    s.candidate()
    s.check()
    s.finding(severity="low")
    action = s.call(
        "action.prepare", operation="publish_finding", payload={"finding_id": "finding-1"}
    )["action"]
    s.confirm(
        action,
        observation={
            "independent_readback": True,
            "finding_id": "finding-1",
            "pr_reference": "synthetic:pr:1",
            "thread_id": "synthetic-thread",
            "comment_id": "synthetic-comment",
        },
    )
    s.call("finding.publish", finding_id="finding-1", action_id=action["action_id"])
    assert s.state["findings"]["finding-1"]["publication"] == "published"
    assert s.state["findings"]["finding-1"]["disposition"] == "open"
    assert s.state["records"]["finding:finding-1"]["publication"] == "pending_pr"
