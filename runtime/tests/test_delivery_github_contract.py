from copy import deepcopy

import pytest

from devflow_temporal.delivery_execution_registry import ExecutionRegistry, OwnershipConflict
from devflow_temporal.delivery_github_contract import (
    GitHubDelivery,
)


def plan():
    def chunk(key, dependencies):
        return {
            "id": key,
            "title": key,
            "scope": "Complete " + key,
            "steps": ["Implement"],
            "verification": ["Run focused tests"],
            "acceptance": ["Works independently on its prerequisites"],
            "allowed_paths": [key + ".py"],
            "depends_on": dependencies,
        }

    return {
        "scope": "A complete feature",
        "acceptance": ["Works together"],
        "workstreams": [
            {
                "id": "api",
                "title": "API",
                "issue_number": 2,
                "acceptance": ["API works"],
                "chunks": [chunk("model", []), chunk("endpoint", ["model", "client"])],
            },
            {
                "id": "ui",
                "title": "UI",
                "issue_number": 3,
                "acceptance": ["UI works"],
                "chunks": [chunk("client", [])],
            },
        ],
    }


ISSUE = {
    "id": "I_1",
    "repository_id": "R_1",
    "repository": "owner/repo",
    "number": 1,
    "url": "https://github.com/owner/repo/issues/1",
    "labels": [],
    "body": "Human scope",
}


def manifest():
    return {
        "version": 1,
        "issue_id": "I_1",
        "repository_id": "R_1",
        "revision": 1,
        "creation_key": "operation-1",
        "plan": plan(),
        "workstream_issues": {},
        "publication": {"stack_id": None, "members": []},
    }


class MemoryGitHub(GitHubDelivery):
    def __init__(self):
        self.saved_issue = deepcopy(ISSUE)
        self.comments = {}
        self.labels = {}
        self.calls = []

    def issue(self, repository, number, repository_id):
        return deepcopy(self.saved_issue)

    def optional(self, endpoint):
        return self.labels.get(endpoint.split("/")[-1])

    def api(self, endpoint, *, method="GET", body=None):
        self.calls.append((method, endpoint))
        if endpoint.endswith("/issues/1/comments") and method == "POST":
            comment = {
                "id": 11,
                "node_id": "IC_11",
                "body": body["body"],
                "issue_url": "https://api.github.com/repos/owner/repo/issues/1",
            }
            self.comments[11] = comment
            return deepcopy(comment)
        if endpoint.endswith("/issues/comments/11"):
            if method == "PATCH":
                self.comments[11]["body"] = body["body"]
            return deepcopy(self.comments[11])
        if endpoint.endswith("/issues/1/labels"):
            self.saved_issue["labels"].extend(body["labels"])
            return []
        if endpoint.endswith("/labels"):
            self.labels[body["name"]] = {"name": body["name"]}
            return body
        if "/labels/" in endpoint:
            return self.labels[endpoint.split("/")[-1]]
        raise AssertionError((method, endpoint))


def test_plan_publication_is_idempotent_and_bound_reads_do_not_discover(tmp_path):
    registry = ExecutionRegistry(tmp_path / "private" / "registry.sqlite3")
    token = registry.claim({"issue": ISSUE}, "run-one", str(tmp_path / "store"))
    gh = MemoryGitHub()
    record = gh.initialize(ISSUE, plan(), registry, token)
    assert gh.saved_issue["body"] == "Human scope"
    gh.calls.clear()
    assert gh.initialize(ISSUE, plan(), registry, token) == record
    assert gh.calls == [("GET", "repos/owner/repo/issues/comments/11")]
    updated = deepcopy(record["manifest"])
    updated["revision"] += 1
    updated["workstream_issues"]["api"] = {
        "id": "I_2",
        "number": 2,
        "url": "https://github.com/owner/repo/issues/2",
    }
    saved = gh.update(ISSUE, record, updated, registry, token)
    assert saved["manifest"] == updated
    assert gh.update(ISSUE, record, updated, registry, token) == saved
    assert gh.saved_issue["body"] == "Human scope"


def test_update_cannot_rewrite_the_accepted_plan(tmp_path):
    registry = ExecutionRegistry(tmp_path / "private" / "registry.sqlite3")
    token = registry.claim({"issue": ISSUE}, "run-one", str(tmp_path / "store"))
    gh = MemoryGitHub()
    record = gh.initialize(ISSUE, plan(), registry, token)
    updated = deepcopy(record["manifest"])
    updated["revision"] += 1
    updated["plan"]["scope"] = "Different feature"
    with pytest.raises(OwnershipConflict, match="business plan"):
        gh.update(ISSUE, record, updated, registry, token)
