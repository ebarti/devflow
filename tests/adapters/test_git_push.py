"""Real bare remotes demonstrate exact-ref push, lease races and lost receipts."""
import subprocess

import pytest
from adapters.test_git import git
from adapters.test_git import repo as repository_fixture

from devflow.adapters.git import GitRepository
from devflow.errors import WorkflowError

repo = repository_fixture


@pytest.fixture
def remote(repo, tmp_path):
    bare = tmp_path / "synthetic-remote.git"
    bare.mkdir()
    git(bare, "init", "--bare")
    git(repo.path, "remote", "add", "origin", str(bare))
    git(repo.path, "switch", "-c", "task/publish")
    return bare


def push(repo, old=None):
    return repo.push_branch(head_ref="task/publish", expected_head=repo.resolve("HEAD"),
                            remote_head_sha=old)


def test_create_then_fast_forward_has_independent_readback(repo, remote):
    first = repo.resolve("HEAD")
    assert push(repo)["remote_head_sha"] == first
    git(repo.path, "commit", "--allow-empty", "-m", "fixture: update")
    result = push(repo, first)
    assert result["remote_head_sha"] == git(remote, "rev-parse", "refs/heads/task/publish")
    assert result["independent_readback"] is True
    assert git(remote, "for-each-ref", "--format=%(refname)") == "refs/heads/task/publish"


@pytest.mark.parametrize("stale", [None, "a" * 40])
def test_stale_expected_remote_blocks_without_mutation(repo, remote, stale):
    push(repo)
    original = repo.resolve("HEAD")
    git(repo.path, "commit", "--allow-empty", "-m", "fixture: later")
    fresh = GitRepository(repo.path)
    with pytest.raises(WorkflowError) as error:
        push(fresh, stale)
    assert error.value.code == "stale_remote"
    assert fresh._mutation_may_have_applied is False
    assert git(remote, "rev-parse", "refs/heads/task/publish") == original


def test_missing_remote_expected_existing_blocks(repo, remote):
    with pytest.raises(WorkflowError, match="expected old head"):
        push(repo, repo.resolve("HEAD"))
    assert git(remote, "for-each-ref") == ""


def test_non_fast_forward_never_overwrites_other_history(repo, remote):
    base = repo.resolve("HEAD")
    git(repo.path, "commit", "--allow-empty", "-m", "fixture: foreign history")
    foreign = repo.resolve("HEAD")
    push(repo)
    git(repo.path, "reset", "--hard", base)  # Only this owned synthetic fixture.
    git(repo.path, "commit", "--allow-empty", "-m", "fixture: divergent history")
    with pytest.raises(WorkflowError) as error:
        push(repo, foreign)
    assert error.value.code == "non_fast_forward"
    assert git(remote, "rev-parse", "refs/heads/task/publish") == foreign


def test_lease_rejects_remote_created_between_preflight_and_push(repo, remote):
    original = repo.resolve("HEAD")
    git(repo.path, "commit", "--allow-empty", "-m", "fixture: local update")
    calls = []

    def runner(argv, **kwargs):
        if "push" in argv:
            git(repo.path, "push", str(remote), original + ":refs/heads/task/publish")
            calls.append(argv)
        return subprocess.run(argv, **kwargs)

    repository = GitRepository(repo.path, runner)
    with pytest.raises(WorkflowError) as error:
        push(repository)
    assert error.value.code == "push_rejected"
    assert error.value.details["no_mutation"] is True
    assert "--force-with-lease=refs/heads/task/publish:" in calls[0]
    assert git(remote, "rev-parse", "refs/heads/task/publish") == original


@pytest.mark.parametrize("applied", [False, True])
def test_lost_push_response_reconciliation_never_duplicates(repo, remote, applied):
    pushes = []

    def runner(argv, **kwargs):
        if "push" in argv:
            pushes.append(argv)
            if applied:
                subprocess.run(argv, **kwargs)
            raise subprocess.TimeoutExpired(argv, 120)
        return subprocess.run(argv, **kwargs)

    repository = GitRepository(repo.path, runner)
    if applied:
        assert push(repository)["status"] == "pushed"
    else:
        with pytest.raises(WorkflowError) as error:
            push(repository)
        assert error.value.code == "ambiguous_git_action"
        assert repository._mutation_may_have_applied is True
    result = repository.reconcile_push(head_ref="task/publish",
        expected_head=repository.resolve("HEAD"), remote_head_sha=None)
    assert (result is not None) is applied
    assert len(pushes) == 1


def test_pushurl_redirection_is_rejected(repo, remote, tmp_path):
    git(repo.path, "config", "remote.origin.pushurl", str(tmp_path / "unrelated.git"))
    with pytest.raises(WorkflowError) as error:
        push(repo)
    assert error.value.code == "push_remote_conflict"
    assert git(remote, "for-each-ref") == ""


def test_source_branch_binding_is_exact(repo, remote):
    with pytest.raises(WorkflowError, match="candidate branch"):
        repo.push_branch(head_ref="task/other", expected_head=repo.resolve("HEAD"),
                         remote_head_sha=None)
    assert git(remote, "for-each-ref") == ""


@pytest.mark.parametrize("rewrite_kind", ["insteadOf", "pushInsteadOf"])
def test_public_origin_rewrite_cannot_publish_to_another_repository(repo, remote, rewrite_kind):
    admitted = "https://github.com/synthetic/authorized.git"
    git(repo.path, "remote", "set-url", "origin", admitted)
    git(repo.path, "config", f"url.{remote.as_uri()}.{rewrite_kind}", admitted)
    assert repo.identity() == "github:synthetic/authorized"
    with pytest.raises(WorkflowError) as error:
        push(repo)
    assert error.value.code == "push_remote_conflict"
    assert repo._mutation_may_have_applied is False
    assert git(remote, "for-each-ref") == ""


def test_explicit_pushurl_cannot_hide_literal_push_rewrite(repo, remote):
    admitted = "https://github.com/synthetic/authorized.git"
    git(repo.path, "remote", "set-url", "origin", admitted)
    git(repo.path, "config", "remote.origin.pushurl", admitted)
    git(repo.path, "config", f"url.{remote.as_uri()}.pushInsteadOf", admitted)
    # A named remote's explicit pushurl suppresses pushInsteadOf. A literal URL
    # does not, so checking only remote get-url would authorize a different push.
    assert git(repo.path, "remote", "get-url", "--push", "origin") == admitted
    with pytest.raises(WorkflowError) as error:
        repo._push_target("task/publish")
    assert error.value.code == "push_remote_conflict"
    assert git(remote, "for-each-ref") == ""


@pytest.mark.parametrize("rewritten", [
    "git@github.com:synthetic/authorized.git",
    "ssh://git@github.com/synthetic/authorized.git",
])
def test_equivalent_ssh_rewrite_keeps_original_url_for_one_expansion(repo, remote, rewritten):
    admitted = "https://github.com/synthetic/authorized.git"
    git(repo.path, "remote", "set-url", "origin", admitted)
    git(repo.path, "config", f"url.{rewritten}.insteadOf", admitted)
    # The expanded URL must not be submitted again: Git would apply this second
    # rewrite and send the actual command to a different repository.
    git(repo.path, "config", f"url.{remote.as_uri()}.insteadOf", rewritten)
    assert git(repo.path, "remote", "get-url", "origin") == rewritten
    assert repo._push_target("task/publish") == ("refs/heads/task/publish", admitted)
