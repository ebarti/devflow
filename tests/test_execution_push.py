"""Journal recovery exercises the real push adapter against an owned bare remote."""
import subprocess

import pytest
from adapters.test_git import git
from adapters.test_git import repo as repository_fixture
from domain.helpers import NOW, H, Scenario, record

from devflow.adapters.git import GitRepository
from devflow.errors import WorkflowError
from devflow.execution import dispatch_action

repo = repository_fixture


def prepare(tmp_path, repo):
    bare = tmp_path / "synthetic-remote.git"
    bare.mkdir()
    git(bare, "init", "--bare")
    git(repo.path, "remote", "add", "origin", str(bare))
    git(repo.path, "switch", "-c", "task/publish")
    scenario = Scenario(tmp_path / "state", endpoint="pr", repository="github:fixture/repo")
    scenario.call("candidate.record", record=record("candidate", candidate_id="candidate-push",
        attempt_id=scenario.state["attempt"]["attempt_id"], scope_hash=scenario.state["scope_hash"],
        repository=scenario.repository, base_sha=repo.resolve("HEAD"), head_sha=repo.resolve("HEAD"),
        tree_sha=repo.resolve("HEAD", "tree"), clean=True, dependency_hash=H,
        environment_hash=H, created_at=NOW))
    action = scenario.call("action.prepare", operation="push_branch",
        payload={"head_ref": "task/publish"},
        expected_remote_state={"head_sha": repo.resolve("HEAD"), "remote_head_sha": None})["action"]
    return scenario, action, bare


class PushTransport:
    pushes = 0
    deny_preflight = False
    deny_readback = False

    def runner(self, argv, **kwargs):
        if (self.deny_preflight and "remote" in argv
                or self.deny_readback and self.pushes and "ls-remote" in argv):
            raise subprocess.TimeoutExpired(argv, 120)
        result = subprocess.run(argv, **kwargs)
        if "push" in argv:
            self.pushes += 1
            raise subprocess.TimeoutExpired(argv, 120)
        return result

    def factory(self, path):
        class SyntheticGit(GitRepository):
            def identity(self):
                return "github:fixture/repo"
        return SyntheticGit(path, self.runner)


def dispatch(scenario, action, repo, transport):
    def no_github(*args):
        raise AssertionError("Branch push must use native Git only")
    return dispatch_action(scenario.service, {
        "operation_id": f"dispatch-{next(scenario.sequence)}", "work_id": scenario.work_id,
        "action_id": action["action_id"], "expected_revision": scenario.state["revision"],
    }, repository=repo.path, git_factory=transport.factory, github_factory=no_github)


def test_lost_push_and_readback_resume_same_action_and_preserve_receipts(tmp_path, repo):
    scenario, action, bare = prepare(tmp_path, repo)
    transport = PushTransport()
    transport.deny_readback = True
    first = dispatch(scenario, action, repo, transport)
    assert first["status"] == "ambiguous"
    assert first["observation"]["no_mutation"] is False
    assert dispatch(scenario, action, repo, transport)["status"] == "ambiguous"
    with pytest.raises(WorkflowError, match="Only a definitely failed"):
        scenario.call("action.retry", action_id=action["action_id"])
    transport.deny_readback = False
    result = dispatch(scenario, action, repo, transport)
    assert result["status"] == "confirmed"
    assert result["observation"]["remote_head_sha"] == git(bare, "rev-parse", "task/publish")
    assert dispatch(scenario, action, repo, transport)["status"] == "confirmed"
    assert transport.pushes == 1
    ids = scenario.state["actions"][action["action_id"]]["receipts"]
    assert [scenario.state["receipts"][id]["status"] for id in ids] == [
        "ambiguous", "ambiguous", "confirmed"]


def test_definite_push_preflight_failure_retries_original_action(tmp_path, repo):
    scenario, action, _ = prepare(tmp_path, repo)
    transport = PushTransport()
    transport.deny_preflight = True
    result = dispatch(scenario, action, repo, transport)
    assert result["status"] == "failed" and result["observation"]["no_mutation"] is True
    assert transport.pushes == 0
    transport.deny_preflight = False
    scenario.call("action.retry", action_id=action["action_id"])
    assert dispatch(scenario, action, repo, transport)["status"] == "confirmed"
    ids = scenario.state["actions"][action["action_id"]]["receipts"]
    assert [scenario.state["receipts"][id]["status"] for id in ids] == ["failed", "confirmed"]


@pytest.mark.parametrize("payload,refs", [
    ({"head_ref": "main"}, {"head_sha": "source", "remote_head_sha": None}),
    ({"head_ref": "task/publish", "remote": "other"},
     {"head_sha": "source", "remote_head_sha": None}),
    ({"head_ref": "task/publish"}, {"head_sha": "source"}),
    ({"head_ref": "task/publish"}, {"head_sha": "a" * 40, "remote_head_sha": None}),
])
def test_push_intent_rejects_unbound_or_redirected_source(tmp_path, repo, payload, refs):
    scenario, _, _ = prepare(tmp_path, repo)
    if refs.get("head_sha") == "source":
        refs = {**refs, "head_sha": repo.resolve("HEAD")}
    with pytest.raises(WorkflowError) as error:
        scenario.call("action.prepare", operation="push_branch", payload=payload,
                      expected_remote_state=refs)
    assert error.value.code == "endpoint_target_mismatch"
