"""Remote-operation stubs exercise real immutable registry journals and custody."""

from copy import deepcopy

import pytest
from test_delivery_github_contract import ISSUE, plan
from test_delivery_plan_model import v2_plan

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_execution_registry import ExecutionRegistry, OwnershipConflict
from devflow_temporal.delivery_github_contract import (
    GitHubContractError,
    GitHubDelivery,
    decode_manifest,
    wire_manifest,
)
from devflow_temporal.delivery_github_plans import child_plan_links, decode_child_plan
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


def test_initial_v2_children_own_detail_and_hydration_uses_only_exact_references(tmp_path):
    remote, issue, registry, token = setup(tmp_path)
    supplied = v2_plan()
    supplied["workstreams"][0]["issue_number"] = None
    before_bodies = {number: child["body"] for number, child in remote.issues.items()}
    record = remote.initialize(issue, supplied, registry, token)
    parent = remote.comments[record["comment_id"]]["body"]
    raw = decode_manifest(parent, issue)
    assert raw == record["wire_index"]
    assert "steps" not in raw["plan"]["workstreams"][0]["chunks"][0]
    assert "expected_paths" not in raw["plan"]["workstreams"][0]["chunks"][0]
    links = child_plan_links(record)
    for stream in record["manifest"]["plan"]["workstreams"]:
        ref = links[stream["id"]]
        child_body = remote.comments[ref["comment_id"]]["body"]
        assert "Implementation steps:" in child_body and "Expected files:" in child_body
        assert decode_child_plan(child_body)["workstream"] == stream
        assert ref["digest"] == digest(child_body)
    assert {number: remote.issues[number]["body"] for number in before_bodies} == before_bodies
    assert record["manifest"]["plan"]["workstreams"][0]["issue_number"] == 4
    assert supplied["workstreams"][0]["issue_number"] is None
    remote.calls.clear()
    assert remote.load_record(issue, record) == record
    assert all(not endpoint.endswith("/comments") for _, endpoint in remote.calls)
    assert not any(method != "GET" for method, _ in remote.calls)


@pytest.mark.parametrize("tamper", ["body", "comment_node", "comment_issue", "child_id", "parent"])
def test_exact_child_digest_identity_and_hierarchy_are_required(tmp_path, tamper):
    remote, issue, registry, token = setup(tmp_path)
    record = remote.initialize(issue, v2_plan(), registry, token)
    ref = record["manifest"]["workstream_plans"]["api"]
    if tamper == "body":
        comment = remote.comments[ref["comment_id"]]
        comment["body"] = "Changed human text\n" + comment["body"]
    elif tamper == "comment_node":
        remote.comments[ref["comment_id"]]["node_id"] = "IC_different"
    elif tamper == "comment_issue":
        remote.comments[ref["comment_id"]]["issue_url"] = "https://api.github.com/repos/owner/repo/issues/3"
    elif tamper == "child_id":
        remote.issues[2]["id"] = "I_replacement"
    else:
        remote.parents[2] = "I_other"
    with pytest.raises(OwnershipConflict):
        remote.load_record(issue, record)
    assert not any(method == "PATCH" for method, _ in remote.calls)


def test_plan_only_migration_keeps_parent_children_pr_head_and_cumulative_budget(tmp_path):
    remote, issue, registry, token = setup(tmp_path)
    original = legacy_publication(remote, issue, registry, token)
    historical = deepcopy(original)
    for number in range(4):
        registry.repair(token, "original-cycle-" + str(number), "Product correction " + str(number))
    budget = registry.budget(issue["id"])
    bodies = {number: child["body"] for number, child in remote.issues.items()}
    migrated = remote.publish_plan_revision(issue, original, converted(original), registry, token,
                                             operation_id="migration-one")
    assert original == historical
    assert migrated["comment_id"] == original["comment_id"]
    assert migrated["comment_node_id"] == original["comment_node_id"]
    assert migrated["manifest"]["plan_revision"] == 2
    assert migrated["manifest"]["workstream_issues"] == original["manifest"]["workstream_issues"]
    member = migrated["manifest"]["publication"]["members"][0]
    historical_member = original["manifest"]["publication"]["members"][0]
    assert all(member[key] == value for key, value in historical_member.items())
    owned = ("workstream_id", "issue_id", "issue_number", "issue_url")
    assert {key: member[key] for key in owned} == {
        "workstream_id": "api", "issue_id": "I_2", "issue_number": 2,
        "issue_url": "https://github.com/owner/repo/issues/2"}
    assert registry.budget(issue["id"]) == budget == {
        "used": 4, "maximum": 10, "learning_required": False}
    assert {number: child["body"] for number, child in remote.issues.items()} == bodies
    assert remote.publish_plan_revision(issue, original, converted(original), registry, token,
                                         operation_id="migration-one") == migrated


@pytest.mark.parametrize("boundary", ["child_create", "parent_patch", "local_commit"])
def test_revision_response_loss_replays_exact_operation_without_duplicate_children(
    tmp_path, monkeypatch, boundary,
):
    remote, issue, registry, token = setup(tmp_path)
    original = legacy_publication(remote, issue, registry, token)
    old_body = remote.comments[original["comment_id"]]["body"]
    if boundary == "child_create":
        remote.lost_child = True
    elif boundary == "parent_patch":
        remote.lost_parent_patch = True
    else:
        original_finish = registry.finish_effect
        lost = False

        def finish(token, key, result, **kwargs):
            nonlocal lost
            if key.startswith("github-plan-revision:") and not lost:
                lost = True
                raise TimeoutError("remote revision committed before local receipt")
            return original_finish(token, key, result, **kwargs)

        monkeypatch.setattr(registry, "finish_effect", finish)
    revised = converted(original)
    with pytest.raises(TimeoutError):
        remote.publish_plan_revision(issue, original, revised, registry, token,
                                     operation_id="original-operation")
    if boundary == "child_create":
        assert remote.comments[original["comment_id"]]["body"] == old_body
    with pytest.raises(OwnershipConflict, match="awaiting readback"):
        registry.stop(token, "unsafe-stop", {})
    result = remote.publish_plan_revision(issue, original, revised, registry, token,
                                           operation_id="original-operation")
    assert result["manifest"]["plan_revision"] == 2
    child_posts = [endpoint for method, endpoint in remote.calls
                   if method == "POST" and endpoint.endswith("/comments")
                   and not endpoint.endswith("/issues/1/comments")]
    assert sorted(child_posts) == ["repos/owner/repo/issues/2/comments",
                                   "repos/owner/repo/issues/3/comments"]
    registry.stop(token, "settled", {})


@pytest.mark.parametrize("change", ["outcome", "acceptance", "child", "published_order",
                                    "command_reuse"])
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


def test_stale_parent_revision_and_conflicting_label_do_not_create_child_plans(tmp_path):
    remote, issue, registry, token = setup(tmp_path)
    original = legacy_publication(remote, issue, registry, token)
    changed = deepcopy(original["manifest"])
    changed["revision"] += 1
    remote.update(issue, original, changed, registry, token)
    remote.calls.clear()
    with pytest.raises(OwnershipConflict, match="changed before plan revision"):
        remote.publish_plan_revision(issue, original, converted(original), registry, token,
                                       operation_id="stale")
    assert not any(method != "GET" for method, _ in remote.calls)
    remote.issues[1]["labels"] = ["devflow-plan-999"]
    with pytest.raises(OwnershipConflict, match="binding changed"):
        remote.publish_plan_revision(issue, original, converted(original), registry, token,
                                       operation_id="wrong-pointer")


def test_partial_staging_settles_original_child_effect_without_parent_adoption(tmp_path):
    remote, issue, registry, token = setup(tmp_path)
    original = legacy_publication(remote, issue, registry, token)
    remote.lost_child = True
    with pytest.raises(TimeoutError):
        remote.publish_plan_revision(issue, original, converted(original), registry, token,
                                       operation_id="partial")
    with registry.connect() as db:
        entry = dict(db.execute("SELECT * FROM execution_effects WHERE state='pending'").fetchone())
    result = remote.settle_plan_effect(issue, entry, registry, token)
    assert result["comment_id"] in remote.comments
    assert remote.load_record(issue, original)["manifest"]["version"] == 1
    assert registry.effect(issue["id"], entry["effect_key"])["state"] == "complete"


def test_publication_update_keeps_child_comments_and_plan_revision_immutable(tmp_path):
    remote, issue, registry, token = setup(tmp_path)
    record = remote.initialize(issue, v2_plan(), registry, token)
    immutable = deepcopy(record["manifest"]["workstream_plans"])
    update = deepcopy(record["manifest"])
    update["revision"] += 1
    saved = remote.update(issue, record, update, registry, token)
    assert saved["manifest"]["plan_revision"] == record["manifest"]["plan_revision"]
    assert saved["manifest"]["workstream_plans"] == immutable
    assert saved["wire_index"] == wire_manifest(update)
    unsafe = deepcopy(saved["manifest"])
    unsafe["revision"] += 1
    unsafe["plan_revision"] += 1
    with pytest.raises(OwnershipConflict, match="business plan"):
        remote.update(issue, saved, unsafe, registry, token)


def test_v2_revision_retains_prior_child_comments_and_added_acceptance(tmp_path):
    remote, issue, registry, token = setup(tmp_path)
    original = remote.initialize(issue, v2_plan(), registry, token)
    historical_comments = deepcopy(remote.comments)
    revised = deepcopy(original["manifest"]["plan"])
    revised["acceptance"].append("Additional verification remains required")
    revised["workstreams"][0]["chunks"][0]["expected_paths"].append("additional-model.py")
    current = remote.publish_plan_revision(issue, original, revised, registry, token,
                                          operation_id="v2-correction")
    assert current["manifest"]["plan_revision"] == 2
    assert current["manifest"]["plan"]["acceptance"] == revised["acceptance"]
    for ref in original["manifest"]["workstream_plans"].values():
        assert remote.comments[ref["comment_id"]] == historical_comments[ref["comment_id"]]
    assert current["manifest"]["workstream_plans"] != original["manifest"]["workstream_plans"]
    remote.calls.clear()
    assert remote.publish_plan_revision(issue, current, revised, registry, token,
                                         operation_id="v2-correction") == current
    assert not any(method != "GET" for method, _ in remote.calls)


def test_another_operation_cannot_take_over_staged_child_records(tmp_path):
    remote, issue, registry, token = setup(tmp_path)
    original = legacy_publication(remote, issue, registry, token)
    revised = converted(original)
    remote.stage_plan_revision(issue, original, revised, registry, token, operation_id="first")
    remote.calls.clear()
    with pytest.raises(OwnershipConflict, match="retains publication custody"):
        remote.publish_plan_revision(issue, original, revised, registry, token,
                                     operation_id="competing")
    assert not any(method != "GET" for method, _ in remote.calls)


def test_parent_pointer_change_during_staging_prevents_commit(tmp_path):
    remote, issue, registry, token = setup(tmp_path)
    original = legacy_publication(remote, issue, registry, token)
    old_body = remote.comments[original["comment_id"]]["body"]
    original_api = remote.api

    def api(endpoint, *, method="GET", body=None):
        result = original_api(endpoint, method=method, body=body)
        if method == "POST" and endpoint.endswith("/issues/3/comments"):
            remote.issues[1]["labels"] = ["devflow-plan-999"]
        return result

    remote.api = api
    with pytest.raises(OwnershipConflict, match="binding changed"):
        remote.publish_plan_revision(issue, original, converted(original), registry, token,
                                     operation_id="pointer-race")
    assert remote.comments[original["comment_id"]]["body"] == old_body
    assert not any(method == "PATCH" for method, _ in remote.calls[-10:])


def test_future_chunk_split_preserves_existing_child_and_published_prefix(tmp_path):
    remote, issue, registry, token = setup(tmp_path)
    original = legacy_publication(remote, issue, registry, token)
    revised = converted(original)
    stream = revised["workstreams"][0]
    future = deepcopy(stream["chunks"][1])
    future.update(id="future_support", title="Complete future support", depends_on=["model"],
                  expected_paths=["future_support.py"])
    stream["chunks"].insert(1, future)
    stream["chunks"][2]["depends_on"].append("future_support")
    migrated = remote.publish_plan_revision(issue, original, revised, registry, token,
                                             operation_id="future-split")
    assert [chunk["id"] for chunk in migrated["manifest"]["plan"]["workstreams"][0]["chunks"]] == [
        "model", "future_support", "endpoint"]
    assert migrated["manifest"]["workstream_issues"] == original["manifest"]["workstream_issues"]
    assert migrated["manifest"]["publication"]["members"][0]["number"] == 1076
    unsafe = deepcopy(migrated["manifest"]["plan"])
    unsafe["workstreams"][0]["chunks"].pop(0)
    unsafe["workstreams"][0]["chunks"][0]["depends_on"] = []
    unsafe["workstreams"][0]["chunks"][1]["depends_on"] = ["future_support", "client"]
    remote.calls.clear()
    with pytest.raises(OwnershipConflict, match="stable chunk ownership"):
        remote.publish_plan_revision(issue, migrated, unsafe, registry, token,
                                     operation_id="deleted-history")
    assert not any(method != "GET" for method, _ in remote.calls)


def test_initial_nullable_v2_adopts_resolved_definition_without_a_repair_debit(tmp_path):
    import json

    from devflow_temporal.delivery_feature_publication import current_record
    from devflow_temporal.delivery_feature_revisions import record_initial_plan_adoption

    remote, issue, registry, token = setup(tmp_path)
    supplied = v2_plan()
    supplied["workstreams"][0]["issue_number"] = None
    spec = {"run_id": "original-run", "accepted_plan": json.dumps(supplied),
            "feature_delivery": {"owner": token, "registry": str(registry.path),
                                 "snapshot": {"issue": issue, "delivery": None}}}
    record = remote.initialize(issue, supplied, registry, token)
    receipt = record_initial_plan_adoption(spec, record, registry)
    assert receipt["identity"]["plan_digest"] == digest(record["manifest"]["plan"])
    assert receipt["plan"]["workstreams"][0]["issue_number"] == 4
    assert registry.budget(issue["id"])["used"] == 0
    assert current_record(spec, remote) == record
    assert spec["accepted_plan"] == json.dumps(supplied)


@pytest.mark.parametrize("oversized_record", ["first_child", "later_child", "parent_index"])
def test_unpublishable_record_preflight_releases_no_custody_and_allows_bounded_correction(
    tmp_path, oversized_record,
):
    from devflow_temporal.delivery_plan_model import validate_plan

    remote, issue, registry, token = setup(tmp_path)
    original = legacy_publication(remote, issue, registry, token)
    oversized = converted(original)
    if oversized_record == "parent_index":
        oversized["acceptance"].extend("Additional requirement " + str(index) + " " + "x" * 4096
                                       for index in range(16))
    else:
        index = 0 if oversized_record == "first_child" else 1
        oversized["workstreams"][index]["chunks"][0]["steps"] = [
            "Step " + str(number) + " " + "x" * 4096 for number in range(16)]
    validate_plan(oversized)
    remote.calls.clear()
    with pytest.raises(ValueError, match="GitHub comment limit"):
        remote.publish_plan_revision(issue, original, oversized, registry, token,
                                     operation_id="oversized-proposal")
    assert not any(method != "GET" for method, _ in remote.calls)
    assert not any(key.startswith("github-plan-stage:")
                   for key in registry.checkpoints(issue["id"]))
    with registry.connect() as db:
        assert not list(db.execute("SELECT kind FROM execution_effects WHERE state='pending'"))
    assert remote.load_record(issue, original)["manifest"]["version"] == 1
    bounded = converted(original)
    bounded["workstreams"][0]["chunks"][0]["steps"].append("Apply bounded correction")
    result = remote.publish_plan_revision(issue, original, bounded, registry, token,
                                         operation_id="bounded-next-proposal")
    assert result["manifest"]["version"] == 2
    assert result["manifest"]["plan_revision"] == 2
    assert result["manifest"]["publication"]["members"][0]["number"] == 1076


@pytest.mark.parametrize(
    "unreadable_record", ["first_child", "later_child", "parent_v2", "parent_v1"],
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


@pytest.mark.parametrize(
    "weakening", ["removed_recipe", "dropped_selector", "narrowed_whole", "replaced_with_frozen"],
)
def test_revision_cannot_drop_final_verification_before_publication(tmp_path, weakening):
    remote, issue, registry, token = setup(tmp_path)
    supplied = v2_plan()
    selectors = [] if weakening == "narrowed_whole" else [
        "tests/test_model.py::test_a", "tests/test_model.py::test_b"]
    supplied["final_gates"] = [{"stage": "checks", "recipe_id": "unit",
                                "selectors": selectors}]
    original = remote.initialize(issue, supplied, registry, token)
    revised = deepcopy(original["manifest"]["plan"])
    if weakening == "removed_recipe":
        revised["final_gates"] = []
    elif weakening == "dropped_selector":
        revised["final_gates"][0]["selectors"].pop()
    elif weakening == "replaced_with_frozen":
        revised["final_gates"][0]["selectors"] = []
    else:
        revised["final_gates"][0]["selectors"] = ["tests/test_model.py::test_a"]
    remote.calls.clear()
    with pytest.raises(OwnershipConflict, match="final verification"):
        remote.publish_plan_revision(issue, original, revised, registry, token,
                                     operation_id="weakened-final-gates")
    assert not any(method != "GET" for method, _ in remote.calls)
    assert not any(key.startswith("github-plan-stage:")
                   for key in registry.checkpoints(issue["id"]))
    stronger = deepcopy(original["manifest"]["plan"])
    if selectors:
        stronger["final_gates"][0]["selectors"].append("tests/test_model.py::test_c")
    stronger["final_gates"].append({"stage": "prepublish_checks", "recipe_id": "integration",
                                    "selectors": []})
    adopted = remote.publish_plan_revision(issue, original, stronger, registry, token,
                                          operation_id="added-final-verification")
    assert adopted["manifest"]["plan"]["final_gates"] == stronger["final_gates"]
    assert adopted["manifest"]["plan_revision"] == 2



@pytest.mark.parametrize(
    "rejected_record", ["bound_parent_marker", "nullable_parent_marker",
                        "nullable_later_child_marker", "nullable_parent_overflow"],
)
def test_initial_v2_admission_precedes_all_writes_and_corrected_intake_can_stop(
    tmp_path, rejected_record,
):
    from devflow_temporal.delivery_github_contract import MARKER_V2
    from devflow_temporal.delivery_github_plans import CHILD_MARKER
    from devflow_temporal.delivery_plan_model import validate_plan

    remote, issue, registry, token = setup(tmp_path)
    bounded = v2_plan()
    if rejected_record.startswith("nullable"):
        for stream in bounded["workstreams"]:
            stream["issue_number"] = None
    rejected = deepcopy(bounded)
    if rejected_record == "nullable_later_child_marker":
        rejected["workstreams"][1]["chunks"][0]["steps"].append(
            "Document the " + CHILD_MARKER + " record contract")
    elif rejected_record == "nullable_parent_overflow":
        rejected["acceptance"].extend("Additional requirement " + str(index) + " " + "x" * 4096
                                      for index in range(16))
    else:
        rejected["acceptance"].append("Document the " + MARKER_V2 + " record contract")
    validate_plan(rejected)
    remote.calls.clear()
    with pytest.raises(ValueError):
        remote.initialize(issue, rejected, registry, token)
    assert not any(method != "GET" for method, _ in remote.calls)
    assert remote.comments == {}
    assert remote.issues[1]["labels"] == []
    with registry.connect() as db:
        assert not list(db.execute("SELECT kind FROM execution_effects"))
    corrected = remote.initialize(issue, bounded, registry, token)
    assert corrected["manifest"]["version"] == 2
    assert remote.load_record(issue, corrected) == corrected
    registry.stop(token, "corrected-intake-stop", {})


@pytest.mark.parametrize("response_loss", ["child_create", "parent_create", "local_receipt"])
def test_initial_v2_uncertain_effects_recover_original_comments_before_stop(
    tmp_path, response_loss,
):
    from devflow_temporal.delivery_execution_registry import UnresolvedEffect

    remote, issue, registry, token = setup(tmp_path)
    supplied = v2_plan()
    lost = False
    original_api = remote.api
    original_finish = registry.finish_effect

    def api(endpoint, *, method="GET", body=None):
        nonlocal lost
        result = original_api(endpoint, method=method, body=body)
        target = "/issues/2/comments" if response_loss == "child_create" else "/issues/1/comments"
        if response_loss != "local_receipt" and method == "POST" and endpoint.endswith(target) \
                and not lost:
            lost = True
            raise TimeoutError("original initial POST response lost")
        return result

    def finish(owner, key, result):
        nonlocal lost
        if response_loss == "local_receipt" and key.startswith("github-plan:") and not lost:
            lost = True
            raise TimeoutError("original initial receipt commit lost")
        return original_finish(owner, key, result)

    remote.api = api
    registry.finish_effect = finish
    with pytest.raises(TimeoutError):
        remote.initialize(issue, supplied, registry, token)
    existing_comments = deepcopy(remote.comments)
    with pytest.raises(UnresolvedEffect):
        registry.stop(token, "unsafe-initial-stop", {})
    corrected = remote.initialize(issue, supplied, registry, token)
    assert all(remote.comments[key] == value for key, value in existing_comments.items())
    assert len({raw["body"] for raw in remote.comments.values()}) == len(remote.comments)
    assert remote.load_record(issue, corrected) == corrected
    registry.stop(token, "settled-initial-stop", {})
