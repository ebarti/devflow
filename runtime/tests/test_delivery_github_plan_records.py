"""Remote-operation stubs exercise real immutable registry journals and custody."""

from copy import deepcopy

import pytest
from test_delivery_github_contract import ISSUE, plan

from devflow_temporal.delivery_execution_registry import ExecutionRegistry, OwnershipConflict
from devflow_temporal.delivery_github_contract import (
    GitHubContractError,
    GitHubDelivery,
)
from devflow_temporal.delivery_plan_model import migrate_plan_v1


class PlanRemote(GitHubDelivery):
    def __init__(self):
        self.issues = {}
        for number in (1, 2, 3):
            self.issues[number] = {**deepcopy(ISSUE), "id": "I_" + str(number),
                                   "number": number, "database_id": number,
                                   "url": f"https://github.com/owner/repo/issues/{number}",
                                   "title": "Issue " + str(number), "labels": [],
                                   "body": "Human-owned body " + str(number)}
        self.comments, self.labels, self.calls = {}, {}, []
        self.parents = {2: "I_1", 3: "I_1"}
        self.next_comment, self.next_issue = 100, 4
        self.lost_child, self.lost_parent_patch = False, False

    def issue(self, repository, number, repository_id):
        self.calls.append(("GET", f"repos/{repository}/issues/{number}"))
        return deepcopy(self.issues[number])

    def pages(self, endpoint):
        self.calls.append(("GET", endpoint))
        if endpoint.endswith("/comments"):
            number = int(endpoint.split("/")[-2])
            return iter(deepcopy([raw for raw in self.comments.values()
                                  if raw["issue_url"].endswith("/" + str(number))]))
        if endpoint.endswith("/issues?state=all"):
            return iter([])
        raise AssertionError(endpoint)

    def api(self, endpoint, *, method="GET", body=None):
        self.calls.append((method, endpoint))
        if "/issues/comments/" in endpoint:
            number = int(endpoint.rsplit("/", 1)[-1])
            if method == "PATCH":
                self.comments[number]["body"] = body["body"]
                if self.lost_parent_patch:
                    self.lost_parent_patch = False
                    raise TimeoutError("parent PATCH response lost")
            return deepcopy(self.comments[number])
        if endpoint.endswith("/comments") and method == "POST":
            issue_number = int(endpoint.split("/")[-2])
            number = self.next_comment
            self.next_comment += 1
            raw = {"id": number, "node_id": "IC_" + str(number), "body": body["body"],
                   "issue_url": f"https://api.github.com/repos/owner/repo/issues/{issue_number}"}
            self.comments[number] = raw
            if self.lost_child and issue_number != 1:
                self.lost_child = False
                raise TimeoutError("child POST response lost")
            return deepcopy(raw)
        if endpoint.endswith("/issues") and method == "POST":
            number = self.next_issue
            self.next_issue += 1
            self.issues[number] = {**deepcopy(ISSUE), "id": "I_" + str(number),
                                   "number": number, "database_id": number, "labels": [],
                                   "url": f"https://github.com/owner/repo/issues/{number}",
                                   "title": body["title"], "body": body["body"]}
            return {"number": number}
        if endpoint.endswith("/issues/1/sub_issues") and method == "POST":
            self.parents[body["sub_issue_id"]] = "I_1"
            return {}
        if endpoint.endswith("/parent"):
            number = int(endpoint.split("/")[-2])
            if number not in self.parents:
                raise GitHubContractError("absent parent", 404)
            return {"node_id": self.parents[number]}
        if "/labels/" in endpoint:
            label = self.labels.get(endpoint.rsplit("/", 1)[-1])
            if label is None:
                raise GitHubContractError("absent label", 404)
            return deepcopy(label)
        if endpoint.endswith("/issues/1/labels"):
            self.issues[1]["labels"] = list(set(self.issues[1]["labels"] + body["labels"]))
            return []
        if endpoint.endswith("/labels"):
            self.labels[body["name"]] = body
            return deepcopy(body)
        raise AssertionError((method, endpoint))


def setup(tmp_path):
    remote = PlanRemote()
    issue = remote.issues[1]
    registry = ExecutionRegistry(tmp_path / "private" / "registry.sqlite3")
    token = registry.claim({"issue": issue}, "original-run", str(tmp_path / "store"))
    return remote, issue, registry, token


def legacy_publication(remote, issue, registry, token):
    record = remote.initialize(issue, plan(), registry, token)
    record = remote.workstreams(issue, record, registry, token)
    manifest = deepcopy(record["manifest"])
    manifest["revision"] += 1
    manifest["publication"]["members"] = [{"chunk_id": "model", "number": 1076,
                                           "url": "https://github.com/owner/repo/pull/1076",
                                           "branch": "feat/original", "head": "a" * 40,
                                           "base_branch": "main"}]
    return remote.update(issue, record, manifest, registry, token)


def converted(record):
    gates = {chunk["id"]: [] for stream in record["manifest"]["plan"]["workstreams"]
             for chunk in stream["chunks"]}
    return migrate_plan_v1(record["manifest"]["plan"], record["manifest"]["workstream_issues"],
                           gates, [])


@pytest.mark.parametrize("change", ["outcome", "command_reuse"])
def test_unsafe_revision_is_rejected_before_new_remote_mutation(tmp_path, change):
    remote, issue, registry, token = setup(tmp_path)
    original = legacy_publication(remote, issue, registry, token)
    revised = converted(original)
    operation_id = "proposal-one"
    if change == "outcome":
        revised["scope"] = "Different feature"
    elif change == "acceptance":
        revised["acceptance"] = ["Weaker success condition"]
    elif change == "child":
        revised["workstreams"][0]["issue_number"] = 3
    elif change == "published_order":
        revised["workstreams"][0]["chunks"][0]["depends_on"] = ["client"]
    else:
        remote.stage_plan_revision(issue, original, revised, registry, token,
                                   operation_id=operation_id)
        revised["workstreams"][0]["chunks"][0]["steps"] = ["A different correction"]
    remote.calls.clear()
    with pytest.raises((ValueError, OwnershipConflict)):
        remote.publish_plan_revision(issue, original, revised, registry, token,
                                       operation_id=operation_id)
    assert not any(method != "GET" for method, _ in remote.calls)


@pytest.mark.parametrize(
    "unreadable_record", ["first_child", "parent_v1"],
)
def test_unreadable_record_preflight_takes_no_custody_and_allows_bounded_correction(
    tmp_path, unreadable_record,
):
    from devflow_temporal.delivery_github_contract import MARKER, MARKER_V2
    from devflow_temporal.delivery_github_plans import CHILD_MARKER
    from devflow_temporal.delivery_plan_model import validate_plan

    remote, issue, registry, token = setup(tmp_path)
    original = legacy_publication(remote, issue, registry, token)
    unreadable = converted(original)
    if unreadable_record.startswith("parent_"):
        marker = MARKER_V2 if unreadable_record == "parent_v2" else MARKER
        unreadable["acceptance"].append("Document the " + marker + " record contract")
    else:
        index = 0 if unreadable_record == "first_child" else 1
        unreadable["workstreams"][index]["chunks"][0]["steps"].append(
            "Document the " + CHILD_MARKER + " record contract")
    validate_plan(unreadable)
    remote.calls.clear()
    with pytest.raises(ValueError, match="unique.*record"):
        remote.publish_plan_revision(issue, original, unreadable, registry, token,
                                     operation_id="unreadable-proposal")
    assert not any(method != "GET" for method, _ in remote.calls)
    assert not any(key.startswith("github-plan-stage:")
                   for key in registry.checkpoints(issue["id"]))
    with registry.connect() as db:
        assert not list(db.execute("SELECT kind FROM execution_effects WHERE state='pending'"))
    assert remote.load_record(issue, original)["manifest"]["version"] == 1
    bounded = converted(original)
    bounded["workstreams"][0]["chunks"][0]["steps"].append("Document the record contract")
    result = remote.publish_plan_revision(issue, original, bounded, registry, token,
                                         operation_id="readable-next-proposal")
    assert result["manifest"]["version"] == 2
    assert result["manifest"]["plan_revision"] == 2
    assert result["manifest"]["publication"]["members"][0]["number"] == 1076
