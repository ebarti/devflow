"""Histories recorded before the feature-revision marker replay on their recorded path."""

from __future__ import annotations

import asyncio
import shutil
from copy import deepcopy

import pytest
from temporalio import workflow
from temporalio.converter import DataConverter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from test_delivery_feature_revision_temporal import RevisionActivities

from devflow_temporal.delivery_workflow import DeliveryWorkflow

MARKER = "delivery-feature-revision-wedges-v1"


class RejectedAdoption(RevisionActivities):
    """The adopt activity proves nothing reached GitHub and closes the attempt itself."""

    async def execute(self, name, payload):
        if name != "delivery_feature_revision_adopt":
            return await super().execute(name, payload)
        self.calls.append((name, deepcopy(payload)))
        receipt = {
            "revision_id": "revision-original", "phase": "rejected",
            "reason": "adoption differs from its sealed correction proposal",
            "repair_key": "plan-revision:original", "budget": self.budget(),
        }
        return {"state": "rejected", "plan_revision": receipt, "budget": self.budget(),
                "reason": receipt["reason"]}


async def _record(tmp_path, run_id, *, marked):
    fixtures = RejectedAdoption("automatic")
    spec = {"run_id": run_id, "provider": "fake", "authorized_endpoint": "merged",
            "plan_approval": "automatic",
            "feature_delivery": {"version": 1, "owner": {"issue_id": "I_original"}},
            "policy": {"max_repairs": 10}}
    original = workflow.patched
    with pytest.MonkeyPatch.context() as patch:
        if not marked:
            # Record exactly as the previous deployment did: no marker command.
            patch.setattr("devflow_temporal.delivery_workflow.workflow.patched",
                          lambda name: False if name == MARKER else original(name))
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal"),
            dev_server_database_filename=str(tmp_path / (run_id + ".sqlite3")),
        ) as environment:
            async with Worker(environment.client, task_queue=run_id,
                              workflows=[DeliveryWorkflow], activities=fixtures.handlers()):
                handle = await environment.client.start_workflow(
                    DeliveryWorkflow.run, spec, id=run_id, task_queue=run_id)
                result = await asyncio.wait_for(handle.result(), 40)
                history = await handle.fetch_history()
    return fixtures, result, history


async def _markers(history):
    """Recorded patch markers, in order."""
    markers = []
    for event in history.events:
        if not event.HasField("marker_recorded_event_attributes"):
            continue
        details = event.marker_recorded_event_attributes.details
        for key in ("patch-data", "patch_id"):
            if key in details:
                value = (await DataConverter.default.decode(details[key].payloads))[0]
                markers.append(value["id"] if isinstance(value, dict) else value)
    return markers


async def _replayed_flag(history):
    """Replay with the marker observed; the instance attribute must agree with it."""
    observed = []
    original = workflow.patched

    def patched(name):
        result = original(name)
        if name == MARKER:
            observed.append((workflow.instance(), result))
        return result

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("devflow_temporal.delivery_workflow.workflow.patched", patched)
        await Replayer(workflows=[DeliveryWorkflow],
                       workflow_runner=UnsandboxedWorkflowRunner()).replay_workflow(history)
    (instance, result), = observed
    assert instance.feature_revision_v1 is result
    return result


@pytest.mark.asyncio
async def test_history_recorded_without_the_marker_replays_its_recorded_path(tmp_path):
    fixtures, result, history = await _record(tmp_path, "revision-premarker", marked=False)
    names = [name for name, _ in fixtures.calls]
    assert names.count("delivery_feature_revision_adopt") == 1
    assert "delivery_feature_revision_reject" not in names
    # The previous deployment treated the activity's rejection as an unconfirmed
    # adoption: custody unknown, the attempt left at its proposed stage.
    assert result["outcome"] == "blocked" and result["cleanup"] == "unknown"
    assert result["checks"]["plan_revision"]["phase"] == "proposed"
    assert MARKER not in await _markers(history)
    assert await _replayed_flag(history) is False


@pytest.mark.asyncio
async def test_marked_history_records_the_activity_rejection_and_replays(tmp_path):
    fixtures, result, history = await _record(tmp_path, "revision-marked", marked=True)
    names = [name for name, _ in fixtures.calls]
    assert names.count("delivery_feature_revision_adopt") == 1
    assert "delivery_feature_revision_reject" not in names
    assert result["outcome"] == "blocked" and result["cleanup"] != "unknown"
    assert result["checks"]["plan_revision"]["phase"] == "rejected"
    assert result["checks"]["repair_budget"] == fixtures.budget()
    assert MARKER in await _markers(history)
    assert await _replayed_flag(history) is True
