"""Every recorded delivery history remains executable after workflow changes."""

from pathlib import Path

import pytest
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.mark.asyncio
@pytest.mark.parametrize("path", sorted((Path(__file__).parent / "fixtures").glob("*.json")),
                         ids=lambda path: path.stem)
async def test_recorded_delivery_history_replays(path):
    history = WorkflowHistory.from_json("delivery-replay", path.read_text())
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
