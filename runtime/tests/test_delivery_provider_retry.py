"""Typed SDK turn failures through the native observation and real kit adapter."""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from functools import wraps
from types import SimpleNamespace

import pytest
from agent_runtime_kit import AgentTask, SessionResumeState
from agent_runtime_kit._errors import AgentTaskTimeoutError
from agent_runtime_kit.adapters import CodexAgentRuntime
from openai_codex import (
    ApprovalMode,
    AsyncCodex,
    CodexConfig,
    Sandbox,
    ServerBusyError,
    TransportClosedError,
)
from openai_codex._run import _collect_async_turn_result
from openai_codex.api import AsyncThread
from openai_codex.generated.v2_all import (
    AgentMessageThreadItem,
    CodexErrorInfo,
    CollabAgentToolCallThreadItem,
    ItemCompletedNotification,
    ThreadItem,
    ThreadTokenUsage,
    ThreadTokenUsageUpdatedNotification,
    TokenUsageBreakdown,
    Turn,
    TurnCompletedNotification,
    TurnError,
)
from openai_codex.models import Notification
from test_delivery_store import service as service

from devflow_temporal import delivery_native_threads
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_resources import read_private, write_private
from devflow_temporal.role_runner import ASSESSMENT_SCHEMA


def usage(multiplier=1):
    return TokenUsageBreakdown(inputTokens=10 * multiplier, cachedInputTokens=2 * multiplier,
                              outputTokens=4 * multiplier, reasoningOutputTokens=1 * multiplier,
                              totalTokens=14 * multiplier)


def harness(tmp_path, monkeypatch, errors, *, legacy=False, completed=True,
            collaboration=False, foreign=False, interrupted=False, max_attempts=3,
            resume=False, missing_usage=False, assessment=None):
    folder = tmp_path / "attempt"
    folder.mkdir()
    write_private(folder / "native-process.json",
                  {"owned": {str(os.getpid()): {"identity": "controlled identity"}}})
    policy = {} if legacy else {"provider_max_attempts": max_attempts}
    observation = delivery_native_threads.NativeThreadObservation({
        "result_path": str(folder / "result.json"), "role": "implement", "iteration": 0,
        "spec": {"run_id": "fixture", "policy": policy},
        "resume_session": "owned-thread" if resume else None,
    })
    calls, clients = [], []
    control = {"completed": asyncio.Event(), "closed": [], "exited": []}

    class Handle:
        def __init__(self, sequence, error):
            self.id, self.error, self.sequence = f"turn-{sequence}", error, sequence
            self.thread_id = "owned-thread"

        async def stream(self):
            try:
                async for event in self.events():
                    yield event
            finally:
                control["closed"].append(self.id)
                control["completed"].set()

        async def events(self):
            item = ThreadItem(AgentMessageThreadItem(
                id="message", type="agentMessage", text=json.dumps(assessment or {
                    "status": "pass", "summary": "Verified fixture", "findings": []}),
                phase="final_answer"))
            yield Notification("item/completed", ItemCompletedNotification(
                item=item, threadId="foreign" if foreign else self.thread_id,
                turnId=self.id, completedAtMs=0))
            if collaboration and self.sequence == 1:
                yield Notification("item/completed", ItemCompletedNotification(
                    threadId=self.thread_id, turnId=self.id, completedAtMs=0,
                    item=ThreadItem(CollabAgentToolCallThreadItem(
                        id="spawn", type="collabAgentToolCall", agentsStates={},
                        tool="spawnAgent", status="completed", senderThreadId=self.thread_id,
                        receiverThreadIds=["child"]))))
            if not missing_usage or self.sequence != 1:
                yield Notification("thread/tokenUsage/updated", ThreadTokenUsageUpdatedNotification(
                    threadId="owned-thread", turnId=self.id,
                    tokenUsage=ThreadTokenUsage(last=usage(), total=usage(self.sequence))))
            if completed:
                yield Notification("turn/completed", TurnCompletedNotification(
                    threadId="owned-thread", turn=Turn(
                        id=self.id, status=("interrupted" if interrupted else
                                            "failed" if self.error else "completed"),
                        items=[item], itemsView="full",
                        error=TurnError(message="Controlled provider failure",
                                        codexErrorInfo=CodexErrorInfo(self.error))
                        if self.error else None)))

        async def run(self):
            # Exercise the pinned SDK's real message-erasing failed-turn collector.
            return await _collect_async_turn_result(self.stream(), turn_id=self.id)

    class Thread:
        id = "owned-thread"

        @wraps(AsyncThread.turn)
        async def turn(self, *args, **kwargs):
            error = errors[len(calls)] if len(calls) < len(errors) else None
            calls.append({"thread": self.id, "args": args, "kwargs": kwargs})
            if isinstance(error, BaseException):
                raise error
            return Handle(len(calls), error)

        @wraps(AsyncThread.run)
        async def run(self, *args, **kwargs):
            return await (await self.turn(*args, **kwargs)).run()

    class Client:
        def __init__(self, **_kwargs):
            self.threads = [Thread.id] if resume else []
            self.resumed = []
            clients.append(self)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            control["exited"].append(self)

        @wraps(AsyncCodex.thread_start)
        async def thread_start(self, **_kwargs):
            self.threads.append(Thread.id)
            return Thread()

        @wraps(AsyncCodex.thread_resume)
        async def thread_resume(self, thread_id, **_kwargs):
            assert thread_id == Thread.id
            self.resumed.append(thread_id)
            return Thread()

        async def thread_list(self, **_kwargs):
            return SimpleNamespace(data=[SimpleNamespace(id=t) for t in self.threads],
                                   next_cursor=None)

    monkeypatch.setattr(delivery_native_threads, "AsyncCodex", Client)
    runtime = CodexAgentRuntime(codex_cls=observation.codex_class(), config_cls=CodexConfig,
                               sandbox_cls=Sandbox, approval_mode_cls=ApprovalMode)
    task = AgentTask(goal="Assess the fixture", working_directory=tmp_path, model="fixture",
                     reasoning_effort="high", deadline=datetime.now(UTC) + timedelta(minutes=1),
                     resume_from=SessionResumeState(session_id="owned-thread") if resume else None,
                     output_schema=ASSESSMENT_SCHEMA)
    return runtime, task, observation, calls, clients, control


@pytest.mark.asyncio
@pytest.mark.parametrize("error", ["rateLimitExceeded", "serverOverloaded",
                                  {"httpConnectionFailed": {"httpStatusCode": 503}},
                                  {"responseStreamDisconnected": {"httpStatusCode": None}}])
async def test_completed_provider_transient_retries_same_thread_and_retains_original(
        tmp_path, monkeypatch, error):
    runtime, task, observation, calls, clients, _control = harness(tmp_path, monkeypatch, [error])
    result = await runtime.run(task)
    assert result.is_success and result.session_id == "owned-thread"
    assert len(clients) == 1 and len(calls) == 2
    assert calls[0] == calls[1]
    assert result.usage.total_tokens == 28
    saved = read_private(observation.path)
    assert saved["state"] == "confirmed"
    assert saved["turns"][0]["turn"]["error"]["codexErrorInfo"] == error
    assert saved["turns"][0]["items"][0]["id"] == "message"
    assert saved["turns"][0]["usage"]["last"]["totalTokens"] == 14
    assert saved["turns"][1]["turn"]["status"] == "completed"


@pytest.mark.asyncio
async def test_transient_exhaustion_is_finite_with_all_original_turns(tmp_path, monkeypatch):
    runtime, task, observation, calls, *_ = harness(
        tmp_path, monkeypatch, ["rateLimitExceeded"] * 4)
    result = await runtime.run(task)
    assert not result.is_success and result.finish_reason == "failed"
    assert len(calls) == 3 and len(read_private(observation.path)["turns"]) == 3
    assert result.usage.total_tokens == 42


@pytest.mark.asyncio
@pytest.mark.parametrize("error", ["usageLimitExceeded", "sessionBudgetExceeded", "unauthorized",
                                  "badRequest", "sandboxError", "other",
                                  {"httpConnectionFailed": {"httpStatusCode": 401}}])
async def test_terminal_provider_failure_never_starts_another_turn(tmp_path, monkeypatch, error):
    runtime, task, observation, calls, *_ = harness(tmp_path, monkeypatch, [error])
    result = await runtime.run(task)
    assert not result.is_success and len(calls) == 1
    assert read_private(observation.path)["turns"][0]["turn"]["status"] == "failed"


@pytest.mark.asyncio
async def test_unknown_completion_is_not_a_transient(tmp_path, monkeypatch):
    runtime, task, observation, calls, _clients, control = harness(
        tmp_path, monkeypatch, ["rateLimitExceeded"], completed=False)
    with pytest.raises(RuntimeError, match="completed event not received"):
        await runtime.run(task)
    assert len(calls) == 1
    saved = read_private(observation.path)["turns"]
    assert saved[0]["turn"] is None and saved[0]["items"][0]["id"] == "message"
    assert saved[0]["usage"]["last"]["totalTokens"] == 14
    assert control["closed"] == ["turn-1"] and len(control["exited"]) == 1


@pytest.mark.asyncio
async def test_legacy_frozen_policy_does_not_gain_retry_turns(tmp_path, monkeypatch):
    runtime, task, _observation, calls, *_ = harness(
        tmp_path, monkeypatch, ["rateLimitExceeded"], legacy=True)
    with pytest.raises(RuntimeError, match="Controlled provider failure"):
        await runtime.run(task)
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_typed_rejected_start_retries_with_original_error(tmp_path, monkeypatch):
    rejected = ServerBusyError(-32000, "Controlled overload",
                               {"codexErrorInfo": "server_overloaded"})
    runtime, task, observation, calls, clients, _control = harness(
        tmp_path, monkeypatch, [rejected])
    result = await runtime.run(task)
    assert result.is_success and result.session_id == "owned-thread"
    assert len(clients) == 1 and len(calls) == 2 and calls[0] == calls[1]
    assert result.usage.total_tokens == 14
    saved = read_private(observation.path)["turns"]
    assert saved[0]["start_error"]["code"] == -32000
    assert saved[0]["start_error"]["data"] == rejected.data


@pytest.mark.asyncio
async def test_rpc_and_completed_failures_share_one_attempt_budget(tmp_path, monkeypatch):
    rejected = ServerBusyError(-32000, "Controlled overload")
    runtime, task, observation, calls, *_ = harness(
        tmp_path, monkeypatch, [rejected, "rateLimitExceeded", "rateLimitExceeded"])
    result = await runtime.run(task)
    assert not result.is_success and len(calls) == 3
    assert len(read_private(observation.path)["turns"]) == 3
    assert result.usage.total_tokens == 28


@pytest.mark.asyncio
async def test_rejected_start_exhaustion_never_extends_attempt_budget(tmp_path, monkeypatch):
    rejected = ServerBusyError(-32000, "Controlled overload")
    runtime, task, observation, calls, *_ = harness(tmp_path, monkeypatch, [rejected] * 4)
    with pytest.raises(ServerBusyError):
        await runtime.run(task)
    assert len(calls) == 3 and len(read_private(observation.path)["turns"]) == 3


@pytest.mark.asyncio
async def test_unknown_start_outcome_never_replays(tmp_path, monkeypatch):
    runtime, task, observation, calls, *_ = harness(
        tmp_path, monkeypatch, [TransportClosedError("Controlled lost transport")])
    with pytest.raises(TransportClosedError):
        await runtime.run(task)
    assert len(calls) == 1
    saved = read_private(observation.path)
    assert saved["turns"][0]["start_error"]["type"] == "TransportClosedError"


@pytest.mark.asyncio
async def test_retry_preserves_original_resumed_session(tmp_path, monkeypatch):
    runtime, task, observation, calls, clients, _control = harness(
        tmp_path, monkeypatch, ["rateLimitExceeded"], resume=True)
    result = await runtime.run(task)
    assert result.is_success and result.session_id == "owned-thread"
    assert len(clients) == 1 and clients[0].resumed == ["owned-thread"]
    assert len(calls) == 2 and calls[0] == calls[1]
    assert read_private(observation.path)["resumed_from"] == "owned-thread"


@pytest.mark.asyncio
async def test_explicit_single_attempt_retains_failed_completion(tmp_path, monkeypatch):
    runtime, task, observation, calls, *_ = harness(
        tmp_path, monkeypatch, ["rateLimitExceeded"], max_attempts=1)
    result = await runtime.run(task)
    assert not result.is_success and len(calls) == 1
    assert read_private(observation.path)["turns"][0]["turn"]["status"] == "failed"


@pytest.mark.asyncio
async def test_missing_usage_remains_unknown_after_successful_retry(tmp_path, monkeypatch):
    runtime, task, observation, calls, *_ = harness(
        tmp_path, monkeypatch, ["rateLimitExceeded"], missing_usage=True)
    result = await runtime.run(task)
    assert result.is_success and len(calls) == 2 and result.usage.total_tokens is None
    assert read_private(observation.path)["turns"][0]["usage"] is None


@pytest.mark.asyncio
async def test_completed_assessment_findings_are_preserved_without_retry(tmp_path, monkeypatch):
    assessment = {"status": "findings", "summary": "Controlled defect", "findings": ["Unresolved"]}
    runtime, task, _observation, calls, *_ = harness(
        tmp_path, monkeypatch, [], assessment=assessment)
    result = await runtime.run(task)
    assert result.parsed_output == assessment and len(calls) == 1


@pytest.mark.asyncio
async def test_collaboration_in_failed_turn_blocks_retry(tmp_path, monkeypatch):
    runtime, task, observation, calls, *_ = harness(
        tmp_path, monkeypatch, ["rateLimitExceeded"], collaboration=True)
    result = await runtime.run(task)
    assert not result.is_success and len(calls) == 1
    saved = read_private(observation.path)
    assert saved["state"] == "blocked" and saved["new_child_thread_ids"] == ["child"]
    assert saved["turns"][0]["items"][1]["tool"] == "spawnAgent"


@pytest.mark.asyncio
async def test_foreign_thread_cannot_supply_completion_or_retry(tmp_path, monkeypatch):
    runtime, task, _observation, calls, *_ = harness(
        tmp_path, monkeypatch, ["rateLimitExceeded"], foreign=True)
    with pytest.raises(ValueError, match="provider thread"):
        await runtime.run(task)
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_interrupted_completion_is_not_retried(tmp_path, monkeypatch):
    runtime, task, _observation, calls, *_ = harness(
        tmp_path, monkeypatch, ["rateLimitExceeded"], interrupted=True)
    result = await runtime.run(task)
    assert not result.is_success and len(calls) == 1


@pytest.mark.asyncio
async def test_backoff_obeys_original_deadline(tmp_path, monkeypatch):
    runtime, task, observation, calls, clients, control = harness(
        tmp_path, monkeypatch, ["rateLimitExceeded"])
    deadline = datetime.now(UTC) + timedelta(seconds=.15)
    with pytest.raises(AgentTaskTimeoutError):
        await runtime.run(replace(task, deadline=deadline))
    assert len(calls) == 1 and len(clients) == 1
    assert len(read_private(observation.path)["turns"]) == 1
    assert control["closed"] == ["turn-1"] and len(control["exited"]) == 1


@pytest.mark.asyncio
async def test_application_cancellation_during_backoff_never_retries(tmp_path, monkeypatch):
    runtime, task, observation, calls, clients, control = harness(
        tmp_path, monkeypatch, ["rateLimitExceeded"])
    running = asyncio.create_task(runtime.run(task))
    await asyncio.wait_for(control["completed"].wait(), timeout=1)
    await runtime.cancel(task.task_id)
    with pytest.raises(asyncio.CancelledError):
        await running
    assert len(calls) == 1 and len(clients) == 1
    assert len(read_private(observation.path)["turns"]) == 1
    assert control["closed"] == ["turn-1"] and len(control["exited"]) == 1


@pytest.mark.parametrize("limit", [0, 4, True, "3", None])
def test_invalid_provider_attempt_limit_is_rejected_at_configuration(service, limit):
    store, _request = service
    path = store.config.path
    path.write_text(json.dumps({**store.config.raw, "provider_max_attempts": limit}))
    with pytest.raises(ValueError, match="provider_max_attempts"):
        DeliveryConfig.load(path)


def test_provider_attempt_limit_is_frozen_and_cannot_be_supplied_per_run(service):
    store, request = service
    spec = store.config.admit(request)
    assert spec["policy"]["provider_max_attempts"] == 3
    assert store.config.public_policy()["provider_max_attempts"] == 3
    store.config.raw["provider_max_attempts"] = 1
    assert spec["policy"]["provider_max_attempts"] == 3
    assert store.config.admit(request)["policy"]["provider_max_attempts"] == 1
    with pytest.raises(ValueError, match="submit fields"):
        store.config.admit({**request, "provider_max_attempts": 3})
