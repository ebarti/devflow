"""Real domain and GitHub adapter; controlled server transport performs no network I/O."""

import pytest
from adapters.test_github import HEAD, TARGET, TREE, Server
from domain.helpers import NOW, H, Scenario, record

from devflow.adapters.github import GitHubRepository
from devflow.errors import WorkflowError
from devflow.execution import dispatch_action, proof_binding


class ObservedGit:
    def __init__(self, path):
        self.path = str(path)

    def observe(self):
        return {"head_sha": HEAD, "tree_sha": TREE, "clean": True, "path": self.path}

    def identity(self):
        return "github:fixture/repo"


def prepared(tmp_path, server):
    scenario = Scenario(tmp_path, endpoint="merge", repository="github:fixture/repo")
    candidate = record(
        "candidate", candidate_id="synthetic-candidate", attempt_id=scenario.state["attempt"]["attempt_id"],
        scope_hash=scenario.state["scope_hash"], repository=scenario.repository,
        base_sha=TARGET, head_sha=HEAD, tree_sha=TREE, clean=True,
        dependency_hash=H, environment_hash=H, created_at=NOW,
    )
    scenario.call("candidate.record", record=candidate)
    scenario.check()
    github = GitHubRepository("fixture", "repo", server.runner)
    server.statuses[0]["description"] = "devflow:" + proof_binding(scenario.state)
    binding = {
        "source_heads": [{"pr_reference": "https://github.com/fixture/repo/pull/1",
                          "source_ref": "feature", "head_sha": HEAD}],
        "target_ref": "main", "target_sha": TARGET, "expected_integrated_tree": TREE,
        "protection_snapshot_hash": github.protection_snapshot("main")["hash"],
        "merge_method": "squash",
    }
    action = scenario.call(
        "deliver", merge_binding=binding,
        expected_remote_state={"target_ref": "main", "target_sha": TARGET},
    )["action"]
    return scenario, action


def dispatch(scenario, action, server):
    return dispatch_action(scenario.service, {
        "operation_id": f"synthetic-dispatch-{next(scenario.sequence)}",
        "work_id": scenario.work_id, "expected_revision": scenario.state["revision"],
        "action_id": action["action_id"],
    }, repository="/synthetic/checkout", git_factory=ObservedGit,
        github_factory=lambda owner, name: GitHubRepository(owner, name, server.runner))


@pytest.mark.parametrize("blocker", ["ci", "protection"])
def test_definite_preflight_blocker_can_retry_after_actual_prerequisite_recovers(tmp_path, blocker):
    server = Server()
    scenario, action = prepared(tmp_path, server)
    if blocker == "ci":
        server.statuses[1]["state"] = "pending"
    else:
        server.protection["enforce_admins"]["enabled"] = False
    result = dispatch(scenario, action, server)
    assert result["status"] == "failed"
    assert result["observation"]["no_mutation"] is True
    assert not server.writes
    with pytest.raises(WorkflowError, match="action retry"):
        dispatch(scenario, action, server)
    server.statuses[1]["state"] = "success"
    server.protection["enforce_admins"]["enabled"] = True
    scenario.call("action.retry", action_id=action["action_id"])
    result = dispatch(scenario, action, server)
    assert result["status"] == "confirmed" and result["observation"]["verified"] is True
    assert server.merge_count == 1
    receipts = scenario.state["actions"][action["action_id"]]["receipts"]
    assert [scenario.state["receipts"][identity]["status"] for identity in receipts] == [
        "failed", "confirmed"
    ]


def test_post_write_readback_rejection_remains_ambiguous_until_observed(tmp_path):
    class DeniedReadback(Server):
        deny = True

        def runner(self, argv, **kwargs):
            method = argv[argv.index("--method") + 1]
            if self.merge_count and method == "GET" and self.deny:
                raise WorkflowError("github_forbidden", "Synthetic read denied", {"http_status": 403})
            return super().runner(argv, **kwargs)

    server = DeniedReadback()
    scenario, action = prepared(tmp_path, server)
    result = dispatch(scenario, action, server)
    assert result["status"] == "ambiguous"
    assert result["observation"]["no_mutation"] is False
    assert server.merge_count == 1
    # A second invocation cannot erase possible prior effects just because its read failed.
    assert dispatch(scenario, action, server)["status"] == "ambiguous"
    with pytest.raises(WorkflowError, match="Only a definitely failed"):
        scenario.call("action.retry", action_id=action["action_id"])
    server.deny = False
    result = dispatch(scenario, action, server)
    assert result["status"] == "confirmed" and server.merge_count == 1


def test_unavailable_intake_can_reconcile_an_uncertain_write_without_redispatch(tmp_path):
    from devflow.application.commands import WorkflowService

    class LostReadback(Server):
        deny = True

        def runner(self, argv, **kwargs):
            method = argv[argv.index("--method") + 1]
            if self.merge_count and method == "GET" and self.deny:
                raise WorkflowError("github_forbidden", "Synthetic read denied", {"http_status": 403})
            return super().runner(argv, **kwargs)

    server = LostReadback()
    scenario, action = prepared(tmp_path, server)
    assert dispatch(scenario, action, server)["status"] == "ambiguous"
    scenario.service = WorkflowService(scenario.service.store.root)
    server.deny = False
    assert dispatch(scenario, action, server)["status"] == "confirmed"
    assert server.merge_count == 1
    assert scenario.service.next(scenario.work_id)["actions"][-1]["reason"] == "trusted_intake_unavailable"
