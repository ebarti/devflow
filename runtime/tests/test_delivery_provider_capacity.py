"""Capacity is a wait in the same admitted role, not a failed repair attempt."""
from __future__ import annotations

import os

import pytest
from test_delivery_provider_retry import harness

from devflow_temporal.delivery_provider_capacity import CapacityWait, clock_credit
from devflow_temporal.delivery_resources import read_private, write_private


@pytest.mark.asyncio
async def test_long_capacity_outage_has_capped_backoff_and_no_new_session(tmp_path, monkeypatch):
    runtime, task, observation, calls, clients, control = harness(
        tmp_path, monkeypatch, ["serverOverloaded"] * 70, resume=True)
    result = await runtime.run(task)
    assert result.is_success and len(calls) == 71 and len(clients) == 1
    assert clients[0].resumed == ["owned-thread"]
    assert control["backoffs"] == [1, 2, 4, 8, 16, 32] + [60] * 64
    assert result.usage.total_tokens == 994
    assert read_private(observation.path)["turns"][-1]["turn"]["status"] == "completed"


def clock_fixture(tmp_path):
    clock = CapacityWait(tmp_path, "run", os.getpid(), "original birth")
    clock.value.update(credited_seconds=3, wait_started=105, wait_seconds=60)
    write_private(clock.path, clock.value)
    return clock


@pytest.mark.parametrize("key,value", [("run_id", "foreign"), ("wait_seconds", 1000000)])
def test_monitor_refuses_foreign_or_unbounded_capacity_credit(tmp_path, key, value):
    clock = clock_fixture(tmp_path)
    clock.value[key] = value
    write_private(clock.path, clock.value)
    with pytest.raises(ValueError, match="capacity wait"):
        clock_credit(tmp_path, "run", os.getpid(), "original birth", 100, 110)
