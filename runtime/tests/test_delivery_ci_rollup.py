"""Real normal CI activity + complete GitHub-shaped adapter observations.

Only the external HTTP transport and activity context are owned fixtures. The
broker, GraphQL pagination, identity checks and CI activity execute normally.
"""

from __future__ import annotations

import json

import pytest

from devflow_temporal import delivery_activities, delivery_broker
from devflow_temporal import delivery_ci_inventory_io as io

HEAD = "a" * 40


class Clock:
    def __init__(self):
        self.seconds = 0
        self.delays = []

    def time(self):
        return self.seconds

    async def sleep(self, seconds):
        self.delays.append(seconds)
        self.seconds += seconds


def check(identity, conclusion="SUCCESS", *, name="Build", status="COMPLETED", run=10):
    return {"__typename": "CheckRun", "id": identity, "name": name,
            "status": status, "conclusion": conclusion,
            "detailsUrl": f"https://github.com/example/fixture/actions/runs/{run}/job/" + identity}


def response(nodes, *, more=False, cursor=None, head=HEAD, total=None, null_rollup=False):
    value = {"data": {"repository": {"pullRequest": {
        "number": 1, "state": "OPEN", "isDraft": False, "headRefName": "owned",
        "headRefOid": head,
        "commits": {"nodes": [{"commit": {"oid": head, "statusCheckRollup": {
            "contexts": {"nodes": nodes,
                         "totalCount": len(nodes) + int(more) if total is None else total,
                         "pageInfo": {
                "hasNextPage": more, "endCursor": cursor}}}}}]}}}}}
    if null_rollup:
        commit = value["data"]["repository"]["pullRequest"]["commits"]["nodes"][0]["commit"]
        commit["statusCheckRollup"] = None
    raw = json.dumps(value).encode()
    return b"HTTP/2.0 200 OK\r\nContent-Length: " + str(len(raw)).encode() + b"\r\n\r\n" + raw


async def activity(monkeypatch, pages, *, legacy=False, clock=None, cycle=False):
    commands = []

    def transport(command, _limit, _remaining):
        commands.append(command)
        index = ((len(commands) - 1) % len(pages) if cycle
                 else min(len(commands) - 1, len(pages) - 1))
        return pages[index]

    reader = io.GitHubRead
    monkeypatch.setattr(io, "GitHubRead", lambda **kwargs: reader(**kwargs, runner=transport))
    broker = object.__new__(delivery_broker.DeliveryBroker)
    spec = {"github_repo": "example/fixture", "run_id": "owned-ci",
            "policy": {"required_ci": ["Build"]}}
    if not legacy:
        spec["policy"]["ci_wait_seconds"] = 0
    if clock is not None:
        async def off_loop(function, *args, **kwargs):
            return function(*args, **kwargs)

        monkeypatch.setattr(delivery_broker.asyncio, "get_running_loop", lambda: clock)
        monkeypatch.setattr(delivery_broker.asyncio, "sleep", clock.sleep)
        monkeypatch.setattr(delivery_broker.asyncio, "to_thread", off_loop)
    broker.spec = spec
    monkeypatch.setattr(delivery_activities, "_context", lambda _spec: (None, broker))
    monkeypatch.setattr(delivery_activities.activity, "heartbeat", lambda *_: None)
    result = await delivery_activities.delivery_ci({
        "spec": spec, "pull_request": {"number": 1, "head": HEAD}})
    return result, commands


@pytest.mark.asyncio
async def test_duplicate_on_later_page_cannot_hide_behind_first_success(monkeypatch):
    first = [check("build-1")] + [check(str(i), name="Other-" + str(i)) for i in range(99)]
    result, commands = await activity(monkeypatch, [
        response(first, more=True, cursor="owned-page-two", total=101),
        response([check("build-2", "FAILURE")], total=101)])
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
    assert states["execution"]["read_complete"] is True
    assert states["delivery_policy"]["state"] == "passed"
    assert states["pr_eligibility"]["state"] == "unverified"
    assert states["merge_readiness"]["state"] == "unverified"


@pytest.mark.asyncio
async def test_completed_without_a_known_conclusion_remains_unknown(monkeypatch):
    result, _ = await activity(monkeypatch, [response([check("only-build", None)])])
    assert result["state"] == "pending"
    assert result["checks"]["Build"]["conclusion"] is None


@pytest.mark.asyncio
async def test_legacy_lost_readback_preserves_prior_observation_without_promoting_it(monkeypatch):
    clock = Clock()
    previous = check("only-build", None, status="IN_PROGRESS")
    result, _ = await activity(monkeypatch, [
        response([previous]), b"HTTP/2.0 503 Unavailable\r\n\r\n{}"],
        legacy=True, clock=clock)
    assert result["state"] == "pending" and clock.seconds == 1200
    assert set(clock.delays) == {15}
    assert result["observations"] == [previous]
    assert result["ci_assessment"]["execution"]["state"] == "unknown"
    assert result["ci_assessment"]["execution"]["read_complete"] is False
