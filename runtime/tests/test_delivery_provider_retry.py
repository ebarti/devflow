"""Typed SDK turn failures through the native observation and real kit adapter."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from functools import wraps
from types import SimpleNamespace

import pytest
from agent_runtime_kit import AgentTask, SessionResumeState
from agent_runtime_kit.adapters import CodexAgentRuntime
from openai_codex import (
    ApprovalMode,
    AsyncCodex,
    CodexConfig,
    Sandbox,
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
from devflow_temporal.bridge import ASSESSMENT_SCHEMA
from devflow_temporal.delivery_resources import read_private, write_private


def usage(multiplier=1):
    return TokenUsageBreakdown(inputTokens=10 * multiplier, cachedInputTokens=2 * multiplier,
                              outputTokens=4 * multiplier, reasoningOutputTokens=1 * multiplier,
                              totalTokens=14 * multiplier)


def harness(tmp_path, monkeypatch, errors, *, legacy=False, completed=True,
            collaboration=False, foreign=False, interrupted=False, max_attempts=3,
            resume=False, missing_usage=False, assessment=None, hold_backoff=False,
            failure_message="Controlled provider failure"):
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
    control = {"completed": asyncio.Event(), "closed": [], "exited": [], "backoffs": []}
    real_sleep = asyncio.sleep

    async def controlled_backoff(seconds):
        assert seconds in (1, 2, 4, 8, 16, 32, 60) and type(seconds) is int
        control["backoffs"].append(seconds)
        if hold_backoff:
            await asyncio.Event().wait()
        else:
            await real_sleep(0)

    # Intercept only this module's backoff, preserving the kit/event loop clocks.
    monkeypatch.setattr(delivery_native_threads, "asyncio",
                        SimpleNamespace(sleep=controlled_backoff, timeout=asyncio.timeout))

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
                        error=TurnError(message=failure_message,
                                        codexErrorInfo=CodexErrorInfo(self.error))
                        if self.error else None)))

        async def run(self):
            # Exercise the pinned SDK's real message-erasing failed-turn collector.
            return await _collect_async_turn_result(self.stream(), turn_id=self.id)

    class Thread:
        id = "owned-thread"

        def __init__(self):
            self.attempts = 0
            self.backoff_start = len(control["backoffs"])

        @wraps(AsyncThread.turn)
        async def turn(self, *args, **kwargs):
            assert len(control["backoffs"][self.backoff_start:]) == self.attempts
            self.attempts += 1
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
            self.call_start = len(calls)
            self.backoff_start = len(control["backoffs"])

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            control["exited"].append(self)
            if not hold_backoff:
                assert len(control["backoffs"][self.backoff_start:]) == (
                    len(calls) - self.call_start - 1)

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
async def test_unknown_start_outcome_never_replays(tmp_path, monkeypatch):
    runtime, task, observation, calls, *_ = harness(
        tmp_path, monkeypatch, [TransportClosedError("Controlled lost transport")])
    with pytest.raises(TransportClosedError):
        await runtime.run(task)
    assert len(calls) == 1
    saved = read_private(observation.path)
    assert saved["turns"][0]["start_error"]["type"] == "TransportClosedError"


@pytest.mark.asyncio
async def test_bounded_reference_keeps_actual_managed_resume_smoke_compatible(
    tmp_path, monkeypatch,
):
    import importlib.util
    import uuid
    from pathlib import Path

    from devflow_temporal import delivery_broker, delivery_config, delivery_store, supervisor

    marker = 'remembered-marker'
    runtime, task, observation, *_ = harness(
        tmp_path, monkeypatch, [], resume=True,
        assessment={'status': 'pass', 'summary': marker, 'findings': []})
    path = Path(__file__).resolve().parents[1] / 'scripts/smoke_preparation.py'
    module = importlib.util.spec_from_file_location('bounded_resume_smoke', path)
    smoke = importlib.util.module_from_spec(module)
    module.loader.exec_module(smoke)
    candidate = {'head': 'controlled-source', 'id': 'controlled-candidate'}
    spec = {'checkout': str(tmp_path), 'policy': {}}
    monkeypatch.setattr(delivery_config.DeliveryConfig, 'load', lambda _path: None)
    monkeypatch.setattr(delivery_store, 'DeliveryStore',
                        lambda _config: SimpleNamespace(spec=lambda _run: spec))
    monkeypatch.setattr(delivery_broker, 'DeliveryBroker',
                        lambda *_args: SimpleNamespace(candidate=lambda: candidate))
    monkeypatch.setattr(uuid, 'uuid4', lambda: SimpleNamespace(hex=marker))
    results = {}

    class ManagedSupervisor:
        def __init__(self, *_args, **_kwargs):
            pass

        async def run(self, request):
            key = request['iteration']
            if key not in results:
                result = await runtime.run(task)
                assert result.is_success and result.session_id == 'owned-thread'
                results[key] = {
                    'status': 'pass', 'cleanup': 'confirmed', 'session_id': result.session_id,
                    'summary': result.parsed_output['summary'],
                    'native_thread_observation': observation.reference(),
                    'requested_model': 'gpt-6.1-sol', 'requested_effort': 'max',
                    'usage': {'total_tokens': result.usage.total_tokens},
                }
            return results[key]

    monkeypatch.setattr(supervisor, 'DeliverySupervisor', ManagedSupervisor)
    summary = await smoke.managed_resume_qa(tmp_path / 'unused.json', tmp_path)
    assert summary['same_session_id'] == 'owned-thread'
    assert summary['builtin_collaboration_items'] == 0
    assert summary['new_child_provider_threads'] == []
    assert summary['prior_context_recalled'] and summary['duplicate_request_reused_receipt']
