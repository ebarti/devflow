from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import pytest
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_store import service as service
from test_delivery_terminal_recovery import ControlledTrackerCheckpoint

from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture
def clock(monkeypatch):
    now = [datetime(2026, 10, 6, tzinfo=UTC)]
    waits = []

    async def wait(predicate, *, timeout):
        if predicate():
            return
        waits.append(timeout.total_seconds())
        now[0] += timeout
        await asyncio.sleep(0)
        raise TimeoutError

    async def sleep(duration):
        now[0] += duration
        await asyncio.sleep(0)

    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.now', lambda: now[0])
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.wait_condition', wait)
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.sleep', sleep)
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.patched', lambda _: True)
    return now, waits


def spec():
    return {'run_id': 'tracker-fixture', 'provider': 'fake', 'tracker_retry_version': 1,
            'terminal_tracker_version': 1, 'resource_cleanup_version': 1,
            'policy': {'tracker_retry_seconds': 60, 'max_repairs': 2}}


@pytest.mark.asyncio
async def test_initial_ownership_conflict_stops_without_retry_or_feature_work(clock):
    controller = DeliveryWorkflow()
    calls = []

    async def execute(name, request, **_):
        calls.append(name)
        if name == 'delivery_prepare':
            return {'candidate': {'id': 'unchanged'}}
        if name == 'delivery_tracker_start':
            return {'state': 'pending', 'retryable': False, 'reason': 'foreign ownership'}
        if name == 'delivery_terminal_tracker':
            return {'state': 'pending', 'retryable': False, 'reason': 'foreign ownership'}
        if name == 'delivery_finalize_resources':
            return {'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
                    'resource_cleanup': 'confirmed'}
        return {}

    controller._activity = execute
    result = await controller.run(spec())
    assert result['outcome'] != 'delivered'
    assert calls.count('delivery_tracker_start') == 1
    assert calls.count('delivery_terminal_tracker') == 1
    assert not any(n == 'delivery_role' for n in calls)
    assert clock[1] == []


@pytest.mark.asyncio
async def test_actual_previous_history_replays_unchanged():
    history = Path(__file__).parent / 'fixtures' / 'intake-required-history.json'
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
        WorkflowHistory.from_json('prior-intake', history.read_text()))


@pytest.mark.asyncio
async def test_actual_previous_terminal_history_replays_unchanged():
    history = Path(__file__).parent / 'fixtures' / 'tracker' / 'c04-terminal-previous-history.json'
    await Replayer(workflows=[ControlledTrackerCheckpoint],
                   workflow_runner=UnsandboxedWorkflowRunner()).replay_workflow(
        WorkflowHistory.from_json('prior-terminal', history.read_text()))
