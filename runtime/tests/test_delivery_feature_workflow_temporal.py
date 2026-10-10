"""Hosted Temporal qualification of feature coordination and explicit merge control."""

from __future__ import annotations

import asyncio
import shutil
from copy import deepcopy

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from test_delivery_github_contract import plan

from devflow_temporal.delivery_workflow import DeliveryWorkflow


class FeatureActivities:
    def __init__(self):
        self.record = {
            "manifest": {
                "revision": 1,
                "plan": plan(),
                "publication": {"stack_id": None, "members": []},
            }
        }
        self.builds = set()
        self.parallel = asyncio.Event()
        self.ready = asyncio.Event()
        self.calls = []

    def handlers(self):
        def stub(name):
            @activity.defn(name=name)
            async def execute(payload):
                self.calls.append((name, deepcopy(payload)))
                if name == "delivery_feature_revision_request":
                    return None
                if name == "delivery_prepare":
                    return {"candidate": {"id": "parent", "head": "parent-head"}}
                if name == "delivery_feature_open":
                    return {"record": deepcopy(self.record), "checkpoints": {}, "budget": {}}
                if name == "delivery_feature_reserve":
                    chunk, kind = payload["chunk_id"], payload["kind"]
                    if kind == "build":
                        self.builds.add(chunk)
                        if {"model", "client"} <= self.builds:
                            self.parallel.set()
                        if chunk in {"model", "client"}:
                            await asyncio.wait_for(self.parallel.wait(), 20)
                    return {
                        "spec": {"run_id": kind + ":" + chunk},
                        "workflow_id": kind + ":" + chunk,
                        "completed": True,
                    }
                if name == "delivery_feature_finish_worker":
                    kind, chunk = payload["child_id"].split(":")
                    if kind == "chunk":
                        publication = self.record["manifest"]["publication"]
                        publication["members"].append({
                            "chunk_id": chunk, "number": len(publication["members"]) + 1,
                        })
                        if len(publication["members"]) > 1:
                            publication["stack_id"] = 42
                        self.record["manifest"]["revision"] += 1
                    return {
                        "record": deepcopy(self.record), "outcome": "delivered", "budget": {},
                    }
                if name == "delivery_project" and payload["event_type"] == "feature_ready":
                    self.ready.set()
                if name == "delivery_feature_merge":
                    return {"state": "confirmed", "pull_requests": [
                        {"number": item["number"]}
                        for item in payload["publication"]["pull_requests"]
                    ]}
                return {"state": "confirmed"}

            return execute

        return [stub(name) for name in (
            "delivery_feature_revision_request",
            "delivery_prepare", "delivery_project", "delivery_feature_open",
            "delivery_feature_reserve", "delivery_feature_finish_worker",
            "delivery_feature_merge", "delivery_feature_settle_workers", "delivery_feature_stop",
            "delivery_feature_settle_effects",
        )]


@pytest.mark.parametrize("action", ["merge", "cancel"])
async def test_feature_waits_for_explicit_control_and_replays_its_history(tmp_path, action):
    fixtures = FeatureActivities()
    spec = {
        "run_id": "feature-temporal-" + action,
        "provider": "fake",
        "authorized_endpoint": "published_unmerged",
        "feature_delivery": {"version": 1},
        "policy": {"max_repairs": 10},
    }
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "owned-temporal.sqlite3"),
    ) as environment:
        async with Worker(
            environment.client, task_queue=spec["run_id"], workflows=[DeliveryWorkflow],
            activities=fixtures.handlers(),
        ):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue=spec["run_id"],
            )
            await asyncio.wait_for(fixtures.ready.wait(), 40)
            state = await handle.query(DeliveryWorkflow.status)
            assert state["phase"] == "awaiting_merge"
            assert state["decision"]["kind"] == "merge"
            assert not any(name == "delivery_feature_merge" for name, _ in fixtures.calls)
            if action == "merge":
                decision = state["decision"]
                command = {
                    "command_id": "explicit-merge", "answer": "merge",
                    "expected_revision": state["revision"],
                    "decision_id": decision["id"], "decision_revision": decision["revision"],
                    "candidate_revision": decision["candidate_revision"],
                }
                await handle.execute_update(DeliveryWorkflow.decision, command)
            else:
                await handle.execute_update(DeliveryWorkflow.cancel, {
                    "command_id": "explicit-stop", "expected_revision": state["revision"],
                    "reason": "Preserve this stack for a successor",
                })
            result = await asyncio.wait_for(handle.result(), 30)
            history = await handle.fetch_history()
    merges = [payload for name, payload in fixtures.calls if name == "delivery_feature_merge"]
    if action == "merge":
        assert result["phase"] == "merged"
        assert len(merges) == 1 and merges[0]["authorization"] == command
    else:
        assert result["outcome"] == "cancelled"
        assert not merges
    assert [p["child_id"] for n, p in fixtures.calls
            if n == "delivery_feature_finish_worker" and p["child_id"].startswith("chunk:")] == [
        "chunk:model", "chunk:client", "chunk:endpoint",
    ]
    assert any(name == "delivery_feature_settle_workers" for name, _ in fixtures.calls)
    assert fixtures.calls[-1][0] == "delivery_feature_stop"
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
