import json
import subprocess

import pytest

from devflow.adapters.github import GitHubRepository
from devflow.errors import WorkflowError


@pytest.mark.parametrize("earlier_write", [False, True])
def test_definite_rejection_cannot_erase_an_earlier_mutation(earlier_write):
    responses = (["HTTP/2.0 201 Created\n\n{}"] if earlier_write else []) + [
        "HTTP/2.0 403 Forbidden\n\n{}"
    ]

    def runner(argv, **kwargs):
        response = responses.pop(0)
        return subprocess.CompletedProcess(argv, int("403" in response), response, "")

    github = GitHubRepository("synthetic", "repository", runner)
    if earlier_write:
        github._api("synthetic-first-write", method="POST", payload={})
    with pytest.raises(WorkflowError) as error:
        github._api("synthetic-rejected-write", method="POST", payload={})
    assert error.value.code == "github_forbidden"
    assert error.value.details.get("no_mutation", False) is (not earlier_write)
    assert not responses


def test_read_failure_before_any_write_establishes_nonexecution():
    def runner(argv, **kwargs):
        return subprocess.CompletedProcess(argv, 1, "HTTP/2.0 403 Forbidden\n\n{}", "")

    with pytest.raises(WorkflowError) as error:
        GitHubRepository("synthetic", "repository", runner).pull_request(1)
    assert error.value.details["no_mutation"] is True


def test_partial_graphql_mutation_error_never_claims_nonexecution():
    def runner(argv, **kwargs):
        response = {"data": {"firstMutation": {"id": "created"}},
                    "errors": [{"type": "FORBIDDEN", "message": "second mutation failed"}]}
        return subprocess.CompletedProcess(argv, 0, json.dumps(response), "")

    with pytest.raises(WorkflowError) as error:
        GitHubRepository("synthetic", "repository", runner)._api(
            "graphql", method="POST", payload={"query": "synthetic"}, read_only=False
        )
    assert error.value.code == "github_forbidden"
    assert error.value.details.get("no_mutation", False) is False


def test_uncertain_write_then_rejected_read_still_needs_reconciliation():
    responses = ["HTTP/2.0 503 Unavailable\n\n{}", "HTTP/2.0 403 Forbidden\n\n{}"]

    def runner(argv, **kwargs):
        return subprocess.CompletedProcess(argv, 1, responses.pop(0), "")

    github = GitHubRepository("synthetic", "repository", runner)
    with pytest.raises(WorkflowError, match="uncertain"):
        github._api("synthetic-write", method="POST", payload={})
    with pytest.raises(WorkflowError) as error:
        github.pull_request(1)
    assert error.value.details.get("no_mutation", False) is False
