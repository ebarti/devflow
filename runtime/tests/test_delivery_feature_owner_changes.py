"""Owner-side changes to a published feature stack and refused stack merges."""

from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace

import pytest
from test_delivery_feature_merge import Remote, previously_submitted, setup_feature
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal import delivery_feature_merge as merger
from devflow_temporal import delivery_feature_publication as publications
from devflow_temporal.delivery_execution_registry import OwnershipConflict, UnresolvedEffect
from devflow_temporal.delivery_feature_execution import register_worker, registry, worker_spec
from devflow_temporal.delivery_github_contract import ordered_chunks


class Owner(Remote):
    """GitHub after the owner edited, drafted, closed or merged recorded layers."""

    def __init__(self, record):
        super().__init__(record)
        self.drafts, self.closed_prs, self.retargeted, self.moved = set(), set(), set(), {}
        self.trunk, self.trees, self.dropped = "1" * 40, {}, False

    def initialize(self, *_args):
        return self.record

    def workstreams(self, _issue, record, *_args):
        return record

    def api(self, path, *, method="GET", body=None):
        if path.endswith("/git/ref/heads/main"):
            return {"object": {"sha": self.trunk}}
        if "/git/commits/" in path and "/git/commits/merge-" not in path:
            return {"tree": {"sha": self.trees[path.rsplit("/", 1)[-1]]}}
        raw = super().api(path, method=method, body=body)
        if "/stacks/" in path and self.dropped:
            raw = {**raw, "pull_requests": [item for item in raw["pull_requests"]
                                            if item["number"] not in self.merged]}
        if "/pulls/" in path:
            number = raw["number"]
            raw = {**raw, "draft": number in self.drafts,
                   "head": {**raw["head"], "sha": self.moved.get(number, raw["head"]["sha"])},
                   "base": {**raw["base"], "ref": ("main" if number in self.retargeted
                                                   else raw["base"]["ref"])}}
            if number in self.closed_prs:
                raw["state"] = "closed"
        return raw


def merged_all(gh, commands):
    def execute(argv, **_kwargs):
        commands.append(argv)
        gh.merged = {20, 21, 22}
        return SimpleNamespace(returncode=0, stderr="")

    return execute


def merge_effect(spec):
    with registry(spec).connect() as db:
        rows = db.execute("SELECT state,result_json FROM execution_effects "
                          "WHERE kind='feature_merge'").fetchall()
    return [{"state": row[0], "result": json.loads(row[1] or "null")} for row in rows]


def refuse(*_args, **_kwargs):
    pytest.fail("the stack must not be submitted")


def test_draft_layer_waits_without_recording_a_merge_intent(service, monkeypatch):
    store, spec, record, requested, command, _ = setup_feature(service, monkeypatch)
    gh, commands = Owner(record), []
    gh.drafts.add(21)
    assert merger.merge(store, spec, requested, command, gh=gh, execute=refuse) == {
        "state": "pending", "reason": "draft_pull_request", "pull_requests": [21]}
    assert merge_effect(spec) == []
    gh.drafts.clear()
    with pytest.raises(merger.StackMergeConditionUnavailable):
        merger.merge(store, spec, requested, command, gh=gh, execute=refuse)
    assert commands == [] and merge_effect(spec) == []


@pytest.mark.parametrize("dropped", [False, True], ids=["stack-kept", "stack-dropped"])
def test_owner_merged_lower_prefix_is_retained_without_submitting_the_remainder(
    service, monkeypatch, dropped,
):
    store, spec, record, requested, command, _ = setup_feature(service, monkeypatch)
    gh, commands = Owner(record), []
    gh.merged.add(20)
    gh.retargeted.add(21)
    gh.dropped = dropped
    with pytest.raises(merger.StackMergeConditionUnavailable):
        merger.merge(store, spec, requested, command, gh=gh, execute=refuse)
    assert commands == [] and gh.merged == {20}
    assert gh.closed == set() and merge_effect(spec) == []


@pytest.mark.parametrize("trunk_tree", ["verified-base", "moved"])
def test_owner_merged_prefix_target_validation_retains_its_verified_base_tree_guard(
    service, monkeypatch, trunk_tree,
):
    store, spec, record, requested, command, _ = setup_feature(service, monkeypatch)
    members = record["manifest"]["publication"]["members"]
    chunk = ordered_chunks(record["manifest"]["plan"])[1]
    # The real worker admission records the base this layer was verified on.
    child = worker_spec(spec, chunk, {"url": "https://github.com/example/fixture/issues/11"},
                        kind="build", base_sha=spec["base_sha"], base_branch="main")
    register_worker(store, spec, child)
    registry(spec).checkpoint(spec["feature_delivery"]["owner"], "verified:" + chunk["id"],
                              {"run_id": child["run_id"], "head": members[1]["head"],
                               "number": 21})
    gh = Owner(record)
    gh.merged.add(20)
    gh.retargeted.add(21)
    # The owner's squash merge of #20 is a new trunk commit.
    gh.trunk = "a" * 40
    gh.trees = {spec["base_sha"]: "verified-base", gh.trunk: trunk_tree}
    if trunk_tree == "moved":
        with pytest.raises(OwnershipConflict,
                           match="feature PRs #20 were merged by hand and main has moved since"):
            merger._moved_target(store, spec, members,
                                 publications.live_members(spec, record, gh), gh)
        assert merge_effect(spec) == []
    else:
        assert merger._moved_target(store, spec, members,
                                    publications.live_members(spec, record, gh), gh) is None
    # Target validation remains available for reconciliation; it cannot establish
    # a server-side condition for a new native-stack submission.
    with pytest.raises(merger.StackMergeConditionUnavailable):
        merger.merge(store, spec, requested, command, gh=gh, execute=refuse)


@pytest.mark.parametrize("change", ["closed", "merged_above_open"])
def test_owner_change_to_a_layer_stops_merge_naming_it(service, monkeypatch, change):
    store, spec, record, requested, command, _ = setup_feature(service, monkeypatch)
    gh = Owner(record)
    name = r"feature PR #21 for chunk \S+ "
    if change == "closed":
        gh.closed_prs.add(21)
        expected = name + "was closed without merging"
    elif change == "moved":
        gh.moved[21] = "f" * 40
        expected = name + "moved from 222222222222 to ffffffffffff; its verified evidence"
    elif change == "retargeted":
        gh.retargeted.add(21)
        expected = name + r"was retargeted from feat/\S+ to main"
    else:
        gh.merged.add(21)
        expected = name + "was merged before the stack layers below it"
    with pytest.raises(OwnershipConflict, match=expected):
        merger.merge(store, spec, requested, command, gh=gh, execute=refuse)
    assert merge_effect(spec) == []


def test_layer_readback_ignores_owner_changes_above_that_layer(service, monkeypatch):
    _, spec, record, *_ = setup_feature(service, monkeypatch)
    members = record["manifest"]["publication"]["members"]
    gh = Owner(record)
    gh.moved[22] = "f" * 40
    live = publications.live_members(spec, record, gh, through=members[1]["chunk_id"])
    assert [raw["number"] for raw in live] == [20, 21]
    with pytest.raises(OwnershipConflict, match="feature PR #22 for chunk"):
        publications.live_members(spec, record, gh)


def test_open_feature_routes_a_changed_layer_to_its_own_worker(service, monkeypatch):
    _, spec, record, *_ = setup_feature(service, monkeypatch)
    members = record["manifest"]["publication"]["members"]
    token = spec["feature_delivery"]["owner"]
    for member in members:
        registry(spec).checkpoint(token, "verified:" + member["chunk_id"], {
            "run_id": "verified-" + member["chunk_id"], "head": member["head"],
            "number": member["number"]})
    gh = Owner(record)
    gh.drafts.add(20)
    gh.closed_prs.add(21)
    monkeypatch.setattr(activities, "GitHubDelivery", lambda: gh)
    opened = activities.open_feature(spec)
    # The draft stays verified; the closed layer and the layer above it are re-run,
    # so the closed layer's own worker reports the closure.
    assert ["verified:" + member["chunk_id"] in opened["checkpoints"]
            for member in members] == [True, False, False]


@pytest.mark.parametrize("merged_prefix", [False, True])
@pytest.mark.parametrize("stderr", [
    'Post "https://api.github.com/graphql": EOF',
    "GraphQL: Pull request #21 is not mergeable\n",
])
def test_nonzero_merge_response_stays_pending_until_remote_outcome(
    service, monkeypatch, merged_prefix, stderr,
):
    store, spec, record, requested, command, gh = setup_feature(service, monkeypatch)
    previously_submitted(store, spec, record, requested, command,
                         state="unknown", exit_code=1, stderr=stderr)
    if merged_prefix:
        gh.merged.add(20)
    first = merger.merge(store, spec, requested, command, gh=gh, execute=refuse)
    assert first["state"] == "pending"
    for settlement in (False, True):
        observed = merger.merge(store, spec, requested, command, gh=gh,
                                execute=refuse, settlement=settlement)
        assert observed["state"] == "pending"
    assert merge_effect(spec)[0]["state"] == "pending"
    assert merge_effect(spec)[0]["result"]["submissions"][0]["state"] == "unknown"
    # Delayed successful readback settles the original submission without another merge.
    gh.merged.update({20, 21, 22})
    with pytest.raises(UnresolvedEffect, match="no original intent"):
        merger.merge(store, spec, requested, command, gh=gh, execute=refuse, settlement=True)
    assert gh.closed == set()
    assert not any(method == "PATCH" for method, _path, _body in gh.calls)
    # The normal owner may now originate closure. Later settlement only reads
    # those existing effects and still cannot issue another stack submission.
    settled = merger.merge(store, spec, requested, command, gh=gh, execute=refuse)
    assert settled["state"] == "confirmed"
    assert merger.merge(store, spec, requested, command, gh=gh, execute=refuse,
                        settlement=True)["state"] == "confirmed"


@pytest.mark.parametrize("failure", ["timeout", "missing_cli"])
def test_unknown_submission_is_read_back_but_unstarted_one_is_resubmitted(
    service, monkeypatch, failure,
):
    store, spec, record, requested, command, gh = setup_feature(service, monkeypatch, single=True)

    def first(argv, **_kwargs):
        if failure == "timeout":
            raise subprocess.TimeoutExpired(argv, 540)
        raise FileNotFoundError(2, "No such file or directory", "gh")

    with pytest.raises((subprocess.TimeoutExpired, FileNotFoundError)):
        merger.merge(store, spec, requested, command, gh=gh, execute=first)
    commands = []
    result = merger.merge(store, spec, requested, command, gh=gh,
                          execute=merged_all(gh, commands))
    if failure == "timeout":
        # An attempt without an outcome may have reached GitHub; it is never repeated.
        assert (result["state"], result["reason"], commands) == (
            "pending", "original_merge_readback", [])
        assert merge_effect(spec)[0]["result"] == {
            "submissions": [{"attempt": 1, "state": "started"}]}
    else:
        assert result["state"] == "confirmed" and len(commands) == 1
