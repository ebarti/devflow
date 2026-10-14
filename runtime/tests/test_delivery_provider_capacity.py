"""Capacity is a wait in the same admitted role, not a failed repair attempt."""
from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_delivery_provider_retry import harness

from devflow_temporal import delivery_native_threads
from devflow_temporal.delivery_provider_capacity import CapacityWait, clock_credit
from devflow_temporal.delivery_resources import read_private, write_private


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", ["serverOverloaded", {"httpConnectionFailed": {"httpStatusCode": 503}}]
)
async def test_capacity_retries_past_the_transport_attempt_limit(tmp_path, monkeypatch, error):
    runtime, task, observation, calls, clients, _ = harness(
        tmp_path, monkeypatch, [error] * 8)
    result = await runtime.run(task)
    assert result.is_success and result.session_id == "owned-thread"
    assert len(calls) == 9 and len(clients) == 1
    assert all(call == calls[0] for call in calls)
    assert len(observation.data["turns"]) == 9
    assert result.usage.total_tokens == 126


@pytest.mark.asyncio
async def test_exact_model_at_capacity_error_is_not_treated_as_a_rate_limit(tmp_path, monkeypatch):
    runtime, task, observation, calls, _, _ = harness(
        tmp_path, monkeypatch, ["serverOverloaded"] * 12,
        failure_message="Selected model is at capacity. Please try a different model.")
    result = await runtime.run(task)
    assert result.is_success and len(calls) == 13
    assert all(record["turn"]["error"]["codexErrorInfo"] == "serverOverloaded"
               for record in observation.data["turns"][:-1])


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


@pytest.mark.asyncio
async def test_capacity_wait_exceeding_active_role_time_does_not_expire(tmp_path, monkeypatch):
    runtime, task, observation, calls, _, control = harness(
        tmp_path, monkeypatch, ["serverOverloaded"])
    observation.timeout = .2

    async def wait(seconds):
        control["backoffs"].append(seconds)
        # Longer than the entire active-work allowance, shorter than the 1s backoff.
        await asyncio.sleep(.3)

    monkeypatch.setattr(delivery_native_threads, "asyncio",
                        SimpleNamespace(sleep=wait, timeout=asyncio.timeout))
    result = await runtime.run(replace(task, deadline=None))
    assert result.is_success and len(calls) == 2
    clock = read_private(observation.capacity_wait.path)
    assert clock["credited_seconds"] >= .3 and clock["wait_started"] is None


@pytest.mark.asyncio
async def test_generic_transient_wait_still_expires_active_work_limit(tmp_path, monkeypatch):
    runtime, task, observation, calls, _, _ = harness(
        tmp_path, monkeypatch, ["internalServerError"], hold_backoff=True)
    observation.timeout = .15
    with pytest.raises(TimeoutError):
        await runtime.run(replace(task, deadline=None))
    assert len(calls) == 1 and not observation.capacity_wait.path.exists()


@pytest.mark.asyncio
async def test_cancel_native_capacity_wait_prevents_the_next_turn(tmp_path, monkeypatch):
    runtime, task, observation, calls, _, control = harness(
        tmp_path, monkeypatch, ["serverOverloaded"], hold_backoff=True)
    task = replace(task, deadline=None)
    running = asyncio.create_task(runtime.run(task))
    await asyncio.wait_for(control["completed"].wait(), timeout=1)
    await runtime.cancel(task.task_id)
    with pytest.raises(asyncio.CancelledError):
        await running
    assert len(calls) == 1
    assert read_private(observation.capacity_wait.path)["wait_started"] is None


def clock_fixture(tmp_path):
    clock = CapacityWait(tmp_path, "run", os.getpid(), "original birth")
    clock.value.update(credited_seconds=3, wait_started=105, wait_seconds=60)
    write_private(clock.path, clock.value)
    return clock


def test_monitor_credit_is_identity_bound_and_an_abandoned_wait_is_finite(tmp_path):
    clock_fixture(tmp_path)
    assert clock_credit(tmp_path, "run", os.getpid(), "original birth", 100, 110) == 8
    assert clock_credit(tmp_path, "run", os.getpid(), "original birth", 100, 200) == 63
    assert clock_credit(tmp_path, "run", os.getpid(), "original birth", 100, 1000) == 63


@pytest.mark.parametrize("key,value", [("run_id", "foreign"), ("wait_seconds", 1000000)])
def test_monitor_refuses_foreign_or_unbounded_capacity_credit(tmp_path, key, value):
    clock = clock_fixture(tmp_path)
    clock.value[key] = value
    write_private(clock.path, clock.value)
    with pytest.raises(ValueError, match="capacity wait"):
        clock_credit(tmp_path, "run", os.getpid(), "original birth", 100, 110)
