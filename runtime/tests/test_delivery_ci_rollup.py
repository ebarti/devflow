"""Real normal CI activity + complete GitHub-shaped adapter observations.

Only the external HTTP transport and activity context are owned fixtures. The
broker, GraphQL pagination, identity checks and CI activity execute normally.
"""

from __future__ import annotations

import copy
import json

import pytest

from devflow_temporal import delivery_activities, delivery_broker, delivery_ci_inventory_io as io

HEAD = "a" * 40


def check(identity, conclusion="SUCCESS", *, name="Build", status="COMPLETED"):
    return {"__typename": "CheckRun", "id": identity, "name": name,
            "status": status, "conclusion": conclusion,
            "detailsUrl": "https://github.com/example/fixture/actions/runs/10/job/" + identity}


def response(nodes, *, more=False, cursor=None, head=HEAD):
    value = {"data": {"repository": {"pullRequest": {
        "number": 1, "state": "OPEN", "isDraft": False, "headRefName": "owned",
        "headRefOid": head,
        "commits": {"nodes": [{"commit": {"oid": head, "statusCheckRollup": {
            "contexts": {"nodes": nodes, "pageInfo": {
                "hasNextPage": more, "endCursor": cursor}}}}}]}}}}}
    raw = json.dumps(value).encode()
    return b"HTTP/2.0 200 OK\r\nContent-Length: " + str(len(raw)).encode() + b"\r\n\r\n" + raw


async def activity(monkeypatch, pages):
    commands = []

    def transport(command, _limit, _remaining):
        commands.append(command)
        return pages[min(len(commands) - 1, len(pages) - 1)]

    reader = io.GitHubRead
    monkeypatch.setattr(io, "GitHubRead", lambda **kwargs: reader(**kwargs, runner=transport))
    broker = object.__new__(delivery_broker.DeliveryBroker)
    spec = {"github_repo": "example/fixture", "run_id": "owned-ci",
            "policy": {"required_ci": ["Build"], "ci_wait_seconds": 0}}
    broker.spec = spec
    monkeypatch.setattr(delivery_activities, "_context", lambda _spec: (None, broker))
    monkeypatch.setattr(delivery_activities.activity, "heartbeat", lambda *_: None)
    result = await delivery_activities.delivery_ci({
        "spec": spec, "pull_request": {"number": 1, "head": HEAD}})
    return result, commands


@pytest.mark.asyncio
@pytest.mark.parametrize("conclusion", ["FAILURE", "SUCCESS", None])
@pytest.mark.parametrize("reverse", [False, True])
async def test_required_duplicate_identity_refuses_every_order(monkeypatch, conclusion, reverse):
    nodes = [check("job-1", conclusion, status="IN_PROGRESS" if conclusion is None else "COMPLETED"),
             check("job-2")]
    if reverse:
        nodes.reverse()
    original = copy.deepcopy(nodes)
    result, commands = await activity(monkeypatch, [response(nodes)])
    assert result["state"] == "failed"
    assert result["ambiguous"] == ["Build"]
    assert result["observations"] == original
    assert "Build" not in result["checks"]
    assert result["next_action"] == "resolve_ambiguous_required_check_identity"
    assert len(commands) == 1


@pytest.mark.asyncio
async def test_duplicate_on_later_page_cannot_hide_behind_first_success(monkeypatch):
    first = [check("build-1")] + [check(str(i), name="Other-" + str(i)) for i in range(99)]
    result, commands = await activity(monkeypatch, [
        response(first, more=True, cursor="owned-page-two"), response([check("build-2", "FAILURE")])])
    assert result["state"] == "failed" and result["ambiguous"] == ["Build"]
    assert len(result["observations"]) == 101
    assert len(commands) == 2 and "cursor=owned-page-two" in commands[1]


@pytest.mark.asyncio
async def test_unique_complete_success_only_satisfies_delivery_policy(monkeypatch):
    result, _ = await activity(monkeypatch, [response([check("only-build")])])
    assert result["state"] == "passed" and result["head"] == HEAD
    assert result["checks"]["Build"]["id"] == "only-build"
    states = result["ci_assessment"]
    assert states["execution"]["state"] == "observed"
    assert states["execution"]["complete"] is True
    assert states["delivery_policy"]["state"] == "passed"
    assert states["pr_eligibility"]["state"] == "unverified"
    assert states["merge_readiness"]["state"] == "unverified"


@pytest.mark.asyncio
async def test_status_and_check_name_collision_is_also_ambiguous(monkeypatch):
    nodes = [check("only-build"), {"__typename": "StatusContext", "id": "status-build",
                                   "context": "Build", "state": "SUCCESS"}]
    result, _ = await activity(monkeypatch, [response(nodes)])
    assert result["state"] == "failed" and result["ambiguous"] == ["Build"]
    assert result["observations"] == nodes


@pytest.mark.asyncio
@pytest.mark.parametrize("conclusion", ["FAILURE", "CANCELLED", "TIMED_OUT", "SKIPPED",
                                         "NEUTRAL", "ACTION_REQUIRED", "STALE", "STARTUP_FAILURE"])
async def test_completed_non_success_is_an_explicit_failed_gate(monkeypatch, conclusion):
    result, _ = await activity(monkeypatch, [response([check("only-build", conclusion)])])
    assert result["state"] == "failed" and result["failed"] == ["Build"]


@pytest.mark.asyncio
async def test_in_progress_success_is_not_completed_evidence(monkeypatch):
    result, _ = await activity(monkeypatch, [response([check("only-build", status="IN_PROGRESS")])])
    assert result["state"] == "pending"


@pytest.mark.asyncio
async def test_completed_without_a_known_conclusion_remains_unknown(monkeypatch):
    result, _ = await activity(monkeypatch, [response([check("only-build", None)])])
    assert result["state"] == "pending"
    assert result["checks"]["Build"]["conclusion"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["missing", "truncated", "repeat", "read_failure", "head"])
async def test_incomplete_or_changed_observations_never_pass(monkeypatch, mode):
    pages = {
        "missing": [response([])],
        "truncated": [response([check("only-build")], more=True)],
        "repeat": [response([check("only-build")], more=True, cursor="repeat"),
                   response([check("only-build")])],
        "read_failure": [b"HTTP/2.0 503 Unavailable\r\n\r\n{}"],
        "head": [response([check("only-build")], head="b" * 40)],
    }
    result, _ = await activity(monkeypatch, pages[mode])
    assert result["state"] == ("stale" if mode == "head" else "pending")
    if mode not in {"missing", "head"}:
        assert result["ci_assessment"]["execution"]["state"] == "unknown"

