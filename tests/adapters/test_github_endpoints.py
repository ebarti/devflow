"""Synthetic GitHub PR/release publications, including independent readback."""

import copy
import json
import subprocess

import pytest

from devflow.adapters.github import GitHubRepository
from devflow.errors import WorkflowError

HEAD, BASE, TAG_OBJECT, OTHER = (digit * 40 for digit in "1234")


class EndpointServer:
    def __init__(self):
        self.prs, self.release, self.writes = [], None, []
        self.lost = None
        self.source, self.tag = HEAD, HEAD
        self.tag_exists = True
        self.annotated = False
        self.race = None

    def runner(self, argv, **kwargs):
        method, endpoint = argv[argv.index("--method") + 1 : argv.index("--method") + 3]
        body = json.loads(kwargs["input"]) if kwargs.get("input") else None
        result = self.route(method, endpoint.removeprefix("repos/fixture/repo/"), body)
        return subprocess.CompletedProcess(argv, 0, json.dumps(copy.deepcopy(result)), "")

    def route(self, method, path, body):
        if method == "GET":
            if path.startswith("pulls?"):
                return self.prs
            if path == "pulls/1":
                return self.prs[0]
            if path == "git/ref/heads/feature":
                return {"object": {"sha": self.source}}
            if path == "git/ref/heads/main":
                return {"object": {"sha": BASE}}
            if path == "git/ref/tags/v1.0.0":
                if not self.tag_exists:
                    raise WorkflowError("github_rejected", "Not found", {"http_status": 404})
                return {
                    "object": {
                        "type": "tag" if self.annotated else "commit",
                        "sha": TAG_OBJECT if self.annotated else self.tag,
                    }
                }
            if path == "git/tags/" + TAG_OBJECT:
                return {"object": {"type": "commit", "sha": self.tag}}
            if path == "releases/tags/v1.0.0":
                if self.release is None:
                    raise WorkflowError("github_rejected", "Not found", {"http_status": 404})
                return self.release
        if method == "POST" and path == "pulls":
            self.writes.append((path, body))
            if self.race == "source":
                self.source = OTHER
            pr = {
                "number": 1,
                "node_id": "PR-1",
                "html_url": "https://github.com/fixture/repo/pull/1",
                "state": "open",
                "draft": body["draft"],
                "body": body["body"],
                "title": body["title"],
                "head": {
                    "ref": body["head"],
                    "sha": self.source,
                    "repo": {"full_name": "fixture/repo"},
                },
                "base": {"ref": body["base"], "repo": {"full_name": "fixture/repo"}},
            }
            self.prs.append(pr)
            self.maybe_lost("pr")
            return pr
        if method == "POST" and path == "releases":
            self.writes.append((path, body))
            assert body["target_commitish"] == HEAD
            self.release = body | {
                "id": 1,
                "node_id": "RE-1",
                "html_url": "https://github.com/fixture/repo/releases/tag/v1.0.0",
            }
            if self.race == "tag":
                self.tag = OTHER
            self.maybe_lost("release")
            return self.release
        raise AssertionError((method, path, body))

    def maybe_lost(self, operation):
        if self.lost == operation:
            self.lost = None
            raise subprocess.TimeoutExpired(["gh", "api"], 60)


def pr_args():
    return dict(
        head_ref="feature",
        base_ref="main",
        expected_head=HEAD,
        title="feat: synthetic feature",
        body="Synthetic fixture with literal $(never-execute) and `literal` text.",
        action_id="publish-1",
    )


def release_args():
    return dict(
        tag="v1.0.0",
        expected_sha=HEAD,
        title="Synthetic release",
        notes="Synthetic fixture release notes.",
        action_id="release-1",
    )


@pytest.mark.parametrize("lost", [None, "pr"])
def test_regular_pr_publication_lost_response_readback_and_idempotency(lost):
    server = EndpointServer()
    server.lost = lost
    repo = GitHubRepository("fixture", "repo", server.runner)
    result = repo.publish_pr(**pr_args())
    assert result["status"] == "published" and result["draft"] is False
    assert repo.publish_pr(**pr_args()) == result
    assert len(server.writes) == 1
    assert "$(never-execute)" in server.writes[0][1]["body"]


def test_pr_source_race_is_not_claimed_as_admitted_candidate():
    server = EndpointServer()
    server.race = "source"
    repo = GitHubRepository("fixture", "repo", server.runner)
    with pytest.raises(WorkflowError) as error:
        repo.publish_pr(**pr_args())
    assert error.value.code == "stale_head" and len(server.writes) == 1
    with pytest.raises(WorkflowError):
        repo.reconcile_pr(**pr_args())
    assert len(server.writes) == 1


def test_existing_unbound_pr_cannot_be_adopted_or_duplicated():
    server = EndpointServer()
    repo = GitHubRepository("fixture", "repo", server.runner)
    repo.publish_pr(**pr_args())
    server.prs[0]["body"] = "Unrelated user PR"
    with pytest.raises(WorkflowError) as error:
        repo.publish_pr(**pr_args())
    assert error.value.code == "pr_publication_conflict" and len(server.writes) == 1


@pytest.mark.parametrize("annotated", [False, True])
@pytest.mark.parametrize("lost", [None, "release"])
def test_release_existing_tag_lost_response_and_no_tag_or_asset_write(annotated, lost):
    server = EndpointServer()
    server.annotated, server.lost = annotated, lost
    repo = GitHubRepository("fixture", "repo", server.runner)
    result = repo.publish_release(**release_args())
    assert result["status"] == "published" and result["commit_sha"] == HEAD
    assert result["draft"] is False
    assert repo.publish_release(**release_args()) == result
    assert [endpoint for endpoint, _ in server.writes] == ["releases"]
    assert server.writes[0][1]["prerelease"] is False


@pytest.mark.parametrize("failure", ["missing", "moved"])
def test_release_missing_or_different_tag_blocks_before_publication(failure):
    server = EndpointServer()
    if failure == "missing":
        server.tag_exists = False
    else:
        server.tag = OTHER
    with pytest.raises(WorkflowError):
        GitHubRepository("fixture", "repo", server.runner).publish_release(**release_args())
    assert not server.writes


def test_release_tag_race_returns_exposed_unverified_without_moving_tag():
    server = EndpointServer()
    server.race = "tag"
    result = GitHubRepository("fixture", "repo", server.runner).publish_release(**release_args())
    assert result["status"] == "exposed_unverified"
    assert result["commit_sha"] == OTHER and result["expected_sha"] == HEAD
    assert server.tag == OTHER and len(server.writes) == 1


def test_reconciliation_never_publishes_missing_objects():
    server = EndpointServer()
    repo = GitHubRepository("fixture", "repo", server.runner)
    assert repo.reconcile_pr(**pr_args()) is None
    assert repo.reconcile_release(**release_args()) is None
    assert not server.writes


class ContinuedEndpointServer(EndpointServer):
    def route(self, method, path, body):
        if method == "PATCH" and path == "pulls/1":
            self.writes.append((path, body))
            self.prs[0].update(body)
            if self.race == "retarget":
                self.prs[0]["base"]["ref"] = "unexpected"
            self.maybe_lost("update")
            return self.prs[0]
        return super().route(method, path, body)


def continued_pr():
    server = ContinuedEndpointServer()
    repo = GitHubRepository("fixture", "repo", server.runner)
    original = repo.publish_pr(**pr_args())
    binding = repo.observe_continuation(1, original["action_marker"])
    return server, repo, binding


@pytest.mark.parametrize("lost", [None, "update"])
def test_same_pr_update_reconciles_uncertain_response_and_preserves_original_marker(lost):
    server, repo, binding = continued_pr()
    server.lost = lost
    args = dict(binding=binding, expected_head=HEAD, title="fix: continued outcome", body="Updated proof")
    updated = repo.update_continued_pr(**args)
    assert updated["pr_number"] == 1 and updated["action_marker"] == binding["action_marker"]
    assert repo.update_continued_pr(**args, reconcile=True) == updated
    assert repo.update_continued_pr(**args) == updated
    assert len(server.prs) == 1 and len(server.writes) == 2
    assert server.writes[-1] == ("pulls/1", {"title":"fix: continued outcome",
                                           "body":"Updated proof\n\n" + binding["action_marker"]})


@pytest.mark.parametrize("field", ["node_id", "head_ref", "base_ref", "head_sha", "repository", "pr_number"])
def test_continued_pr_mismatch_never_updates_or_creates_replacement(field):
    server, repo, binding = continued_pr()
    if field == "head_sha":
        server.prs[0]["head"]["sha"] = OTHER
    elif field == "pr_number":
        server.prs[0]["number"] = 2
    elif field in {"head_ref", "base_ref"}:
        server.prs[0][field.split("_")[0]]["ref"] = "changed"
    elif field == "repository":
        server.prs[0]["head"]["repo"]["full_name"] = "another/repo"
    else:
        server.prs[0][field] = "changed"
    with pytest.raises(WorkflowError):
        repo.update_continued_pr(binding=binding, expected_head=HEAD, title="New", body="New")
    assert len(server.writes) == 1 and len(server.prs) == 1


def test_continued_pr_target_race_is_unconfirmed_then_read_only_reconciliation_rejects():
    server, repo, binding = continued_pr()
    server.race = "retarget"
    args = dict(binding=binding, expected_head=HEAD, title="New", body="New")
    with pytest.raises(WorkflowError, match="reconciliation"):
        repo.update_continued_pr(**args)
    with pytest.raises(WorkflowError, match="source or target"):
        repo.update_continued_pr(**args, reconcile=True)
    assert len(server.writes) == 2
