"""Real local Git fixtures; no network or user checkout mutations."""

import subprocess

import pytest

from devflow.adapters.git import GitRepository
from devflow.errors import WorkflowError


def git(path, *args):
    return subprocess.run(
        ["git", "-C", str(path), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    git(source, "init", "-b", "main")
    git(source, "config", "user.name", "Synthetic Fixture")
    git(source, "config", "user.email", "fixture@example.invalid")
    (source / "data.txt").write_text("base\n")
    git(source, "add", ".")
    git(source, "commit", "-m", "fixture: initial")
    return GitRepository(source)


def snapshot(repo, token="owner"):
    return repo.snapshot(
        candidate_id="candidate-1",
        attempt_id="attempt-1",
        scope_hash="a" * 64,
        base_ref="main",
        dependency_hash="b" * 64,
        environment_hash="c" * 64,
        ownership_token=token,
    )


def test_real_owned_worktree_capture_preserves_dirty_canonical(repo, tmp_path):
    (repo.path / "unrelated.txt").write_text("must survive")
    worktree = tmp_path / "task with spaces"
    record = repo.create_worktree(
        worktree, "task/one", "main", action_id="create-1", ownership_token="owner"
    )
    assert record["status"] == "confirmed"
    owned = GitRepository(worktree)
    candidate = snapshot(owned)
    assert candidate["head_sha"] == git(worktree, "rev-parse", "HEAD")
    assert candidate["tree_sha"] == git(worktree, "rev-parse", "HEAD^{tree}")
    assert (repo.path / "unrelated.txt").read_text() == "must survive"
    assert (
        repo.create_worktree(
            worktree, "task/one", "main", action_id="create-1", ownership_token="owner"
        )
        == record
    )
    with pytest.raises(WorkflowError, match="uncommitted"):
        (worktree / "dirty.txt").write_text("preserve")
        snapshot(owned)
    assert (worktree / "dirty.txt").read_text() == "preserve"


def test_existing_path_and_symlink_never_adopted(repo, tmp_path):
    existing = tmp_path / "foreign"
    existing.mkdir()
    (existing / "sentinel").write_text("safe")
    with pytest.raises(WorkflowError):
        repo.create_worktree(
            existing, "task/foreign", "main", action_id="foreign", ownership_token="x"
        )
    link = tmp_path / "linked"
    link.symlink_to(existing, target_is_directory=True)
    with pytest.raises(WorkflowError):
        repo.create_worktree(
            link / "child", "task/link", "main", action_id="link", ownership_token="x"
        )
    assert (existing / "sentinel").read_text() == "safe"


def test_unowned_checkout_and_wrong_token_cannot_freeze(repo, tmp_path):
    with pytest.raises(WorkflowError) as error:
        snapshot(repo)
    assert error.value.code == "workspace_not_owned"
    repo.create_worktree(
        tmp_path / "task", "task/owned", "main", action_id="one", ownership_token="one"
    )
    with pytest.raises(WorkflowError) as error:
        snapshot(GitRepository(tmp_path / "task"), token="two")
    assert error.value.code == "workspace_not_owned"


def test_explicit_registration_checks_branch_cleanliness_and_identity(repo):
    head = repo.resolve("HEAD")
    with pytest.raises(WorkflowError):
        repo.register_checkout(
            action_id="register", ownership_token="owner", expected_head=head, base_ref="main"
        )
    git(repo.path, "switch", "-c", "feat/registered")
    repo.register_checkout(
        action_id="register", ownership_token="owner", expected_head=head, base_ref="main"
    )
    assert snapshot(repo)["head_sha"] == head
    with pytest.raises(WorkflowError):
        repo.register_checkout(
            action_id="other", ownership_token="other", expected_head=head, base_ref="main"
        )


def test_lost_git_response_reconciles_existing_owned_worktree(repo, tmp_path):
    calls = []

    def runner(argv, **kwargs):
        result = subprocess.run(argv, **kwargs)
        if "add" in argv and "worktree" in argv:
            calls.append(argv)
            raise subprocess.TimeoutExpired(argv, 120)
        return result

    repository = GitRepository(repo.path, runner)
    target = tmp_path / "lost"
    with pytest.raises(WorkflowError) as error:
        repository.create_worktree(
            target, "task/lost", "main", action_id="lost", ownership_token="owner"
        )
    assert error.value.code == "git_transport"
    assert (
        repository.create_worktree(
            target, "task/lost", "main", action_id="lost", ownership_token="owner"
        )["status"]
        == "confirmed"
    )
    assert len(calls) == 1


def test_snapshot_detects_head_race(repo, tmp_path):
    path = tmp_path / "racing"
    repo.create_worktree(path, "task/race", "main", action_id="race", ownership_token="owner")
    calls = 0

    def runner(argv, **kwargs):
        nonlocal calls
        if "status" in argv:
            calls += 1
            if calls == 2:
                git(path, "commit", "--allow-empty", "-m", "fixture: concurrent head")
        return subprocess.run(argv, **kwargs)

    with pytest.raises(WorkflowError) as error:
        snapshot(GitRepository(path, runner))
    assert error.value.code == "candidate_race"


def test_identity_shared_across_worktrees_and_credential_url_rejected(repo, tmp_path):
    target = tmp_path / "identity"
    repo.create_worktree(
        target, "task/identity", "main", action_id="identity", ownership_token="owner"
    )
    assert repo.identity() == GitRepository(target).identity()
    git(repo.path, "remote", "add", "origin", "git@github.com:Example/Repository.git")
    assert repo.identity() == GitRepository(target).identity() == "github:example/repository"
    git(
        repo.path,
        "remote",
        "set-url",
        "origin",
        "https://private-token@github.com/Example/Repository.git",
    )
    with pytest.raises(WorkflowError) as error:
        repo.identity()
    assert error.value.code == "credential_remote"
    assert "private-token" not in str(error.value)


def test_argv_never_interprets_branch_or_path_as_shell(repo, tmp_path):
    # '$()' is an invalid branch, but valid path: a shell must never see it.
    target = tmp_path / "$(touch SHOULD_NOT_EXIST)"
    repo.create_worktree(target, "task/argv", "main", action_id="argv", ownership_token="owner")
    assert target.is_dir()
    assert not (repo.path / "SHOULD_NOT_EXIST").exists()


def test_concurrent_actions_cannot_both_own_same_worktree(repo, tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    path = tmp_path / "concurrent"

    def create(identity):
        try:
            return repo.create_worktree(
                path, "task/concurrent", "main", action_id=identity, ownership_token=identity
            )["status"]
        except WorkflowError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(create, ["first", "second"]))
    assert sorted(outcomes) == ["confirmed", "workspace_conflict"]


def test_interrupted_reservation_cannot_be_claimed_by_different_action(repo, tmp_path):
    def unavailable(argv, **kwargs):
        if "worktree" in argv and "add" in argv:
            raise subprocess.TimeoutExpired(argv, 120)
        return subprocess.run(argv, **kwargs)

    path = tmp_path / "reserved"
    with pytest.raises(WorkflowError):
        GitRepository(repo.path, unavailable).create_worktree(
            path, "task/reserved", "main", action_id="first", ownership_token="first"
        )
    assert not path.exists()
    with pytest.raises(WorkflowError) as error:
        repo.create_worktree(
            path, "task/reserved", "main", action_id="second", ownership_token="second"
        )
    assert error.value.code == "workspace_conflict"
    assert (
        repo.create_worktree(
            path, "task/reserved", "main", action_id="first", ownership_token="first"
        )["status"]
        == "confirmed"
    )
