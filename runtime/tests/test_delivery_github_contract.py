from copy import deepcopy

import pytest

from devflow_temporal.delivery_execution_registry import ExecutionRegistry, OwnershipConflict
from devflow_temporal.delivery_github_contract import (
    GitHubDelivery,
    decode_manifest,
    encode_manifest,
    ordered_chunks,
    validate_manifest,
    validate_plan,
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


def test_plan_orders_independent_streams_without_forward_dependencies():
    assert [c["id"] for c in ordered_chunks(plan())] == ["model", "client", "endpoint"]
    assert decode_manifest(encode_manifest(manifest()), ISSUE) == manifest()


@pytest.mark.parametrize("change", ["cycle", "unknown", "sequential", "scope", "same_issue"])
def test_invalid_decomposition_cannot_acquire_execution_authority(change):
    value = plan()
    if change == "cycle":
        value["workstreams"][0]["chunks"][0]["depends_on"] = ["endpoint"]
    elif change == "unknown":
        value["workstreams"][1]["chunks"][0]["depends_on"] = ["missing"]
    elif change == "sequential":
        value["workstreams"][0]["chunks"][1]["depends_on"] = []
    elif change == "scope":
        value["workstreams"][0]["chunks"][0]["allowed_paths"] = [".git/config"]
    else:
        value["workstreams"][1]["issue_number"] = 2
    with pytest.raises(ValueError):
        validate_plan(value)


def test_chunk_paths_cannot_expand_service_authority():
    with pytest.raises(ValueError, match="configured source scope"):
        validate_plan(plan(), allowed_paths=["model.py"])


@pytest.mark.parametrize("field,value", [("branch", []), ("head", None), ("url", 1)])
def test_malformed_publication_is_rejected(field, value):
    record = manifest()
    member = {
        "chunk_id": "model",
        "number": 10,
        "url": "https://github.com/owner/repo/pull/10",
        "head": "a" * 40,
        "branch": "feat/model",
        "base_branch": "main",
    }
    member[field] = value
    record["publication"]["members"] = [member]
    with pytest.raises(ValueError):
        validate_manifest(record, ISSUE)


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


def test_conflicting_binding_does_not_choose_the_newest_record():
    gh = MemoryGitHub()
    with pytest.raises(OwnershipConflict, match="ambiguous"):
        gh.bound_record({**ISSUE, "labels": ["devflow-plan-11", "devflow-plan-12"]})


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


@pytest.mark.parametrize("boundary", ["binding", "record"])
def test_lost_binding_or_record_receipt_reconciles_known_comment(tmp_path, monkeypatch, boundary):
    registry = ExecutionRegistry(tmp_path / "private" / "registry.sqlite3")
    token = registry.claim({"issue": ISSUE}, "run-one", str(tmp_path / "store"))
    gh = MemoryGitHub()
    finish = registry.finish_effect
    interrupted = False

    def finish_effect(token, key, result, **kwargs):
        nonlocal interrupted
        prefix = "github-plan-bind:" if boundary == "binding" else "github-record:"
        if key.startswith(prefix) and not interrupted:
            interrupted = True
            raise TimeoutError("response lost before local completion")
        return finish(token, key, result, **kwargs)

    monkeypatch.setattr(registry, "finish_effect", finish_effect)
    if boundary == "binding":
        with pytest.raises(TimeoutError):
            gh.initialize(ISSUE, plan(), registry, token)
    else:
        record = gh.initialize(ISSUE, plan(), registry, token)
        updated = deepcopy(record["manifest"])
        updated["revision"] += 1
        updated["workstream_issues"]["api"] = {
            "id": "I_2", "number": 2, "url": "https://github.com/owner/repo/issues/2",
        }
        with pytest.raises(TimeoutError):
            gh.update(ISSUE, record, updated, registry, token)
    gh.calls.clear()
    saved = gh.initialize(ISSUE, plan(), registry, token)
    assert saved["comment_id"] == 11
    assert gh.calls == [("GET", "repos/owner/repo/issues/comments/11")]
    registry.stop(token, "settled", {})
