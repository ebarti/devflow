"""Typed SDK turn failures through the native observation and real kit adapter."""

from __future__ import annotations

import asyncio
import hashlib
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
        assert seconds in (1, 2) and type(seconds) is int
        control["backoffs"].append(seconds)
        if hold_backoff:
            await asyncio.Event().wait()
        else:
            await real_sleep(0)

    # Intercept only this module's backoff, preserving the kit/event loop clocks.
    monkeypatch.setattr(delivery_native_threads, "asyncio",
                        SimpleNamespace(sleep=controlled_backoff))

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
            assert control["backoffs"][self.backoff_start:] == [
                2 ** attempt for attempt in range(self.attempts)]
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
                assert control["backoffs"][self.backoff_start:] == [
                    2 ** attempt for attempt in range(len(calls) - self.call_start - 1)]

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
        tmp_path, monkeypatch, ["rateLimitExceeded"], hold_backoff=True)
    deadline = datetime.now(UTC) + timedelta(seconds=.15)
    with pytest.raises(AgentTaskTimeoutError):
        await runtime.run(replace(task, deadline=deadline))
    assert len(calls) == 1 and len(clients) == 1
    assert len(read_private(observation.path)["turns"]) == 1
    assert control["closed"] == ["turn-1"] and len(control["exited"]) == 1
    assert control["backoffs"] == [1]


@pytest.mark.asyncio
async def test_application_cancellation_during_backoff_never_retries(tmp_path, monkeypatch):
    runtime, task, observation, calls, clients, control = harness(
        tmp_path, monkeypatch, ["rateLimitExceeded"], hold_backoff=True)
    running = asyncio.create_task(runtime.run(task))
    await asyncio.wait_for(control["completed"].wait(), timeout=1)
    await runtime.cancel(task.task_id)
    with pytest.raises(asyncio.CancelledError):
        await running
    assert len(calls) == 1 and len(clients) == 1
    assert len(read_private(observation.path)["turns"]) == 1
    assert control["closed"] == ["turn-1"] and len(control["exited"]) == 1
    assert control["backoffs"] == [1]


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


@pytest.mark.asyncio
async def test_large_completed_item_stays_private_with_bounded_reference(tmp_path, monkeypatch):
    _runtime, _task, observation, *_ = harness(tmp_path, monkeypatch, [])
    observation.started("owned-thread")
    output = "x" * 1048576
    item = ThreadItem.model_validate({
        "type": "commandExecution", "id": "cmd", "command": "pytest -vv",
        "commandActions": [], "cwd": "/tmp", "status": "completed",
        "aggregatedOutput": output,
    })

    class Handle:
        id, thread_id = "turn-1", "owned-thread"

        async def stream(self):
            yield Notification("item/completed", ItemCompletedNotification(
                item=item, threadId=self.thread_id, turnId=self.id, completedAtMs=0))
            yield Notification("turn/completed", TurnCompletedNotification(
                threadId=self.thread_id, turn=Turn(id=self.id, status="completed", items=[])))

    await observation.collect(Handle())
    reference = observation.reference()
    assert len(json.dumps(reference).encode()) < 64 * 1024
    assert output not in json.dumps(reference)
    assert read_private(observation.path)["turns"][0]["items"][0]["aggregatedOutput"] == output
    assert reference["sha256"] == hashlib.sha256(observation.path.read_bytes()).hexdigest()


def test_reference_bounds_arbitrary_provider_fields_and_turn_count(tmp_path, monkeypatch):
    _runtime, _task, observation, *_ = harness(tmp_path, monkeypatch, [])
    oversized = "\u0000" * 1024
    record = {"thread_id": oversized, "turn_id": oversized,
              "turn": {"status": "failed", "error": {
                  "message": oversized, "codexErrorInfo": {"httpConnectionFailed": {
                      "httpStatusCode": 503, "message": oversized}}}},
              "start_error": {"type": oversized, "message": oversized, "data": oversized},
              "items": [{"output": oversized}],
              **{key: oversized for key in ("id", "type", "tool", "status", "senderThreadId")},
              "receiverThreadIds": [oversized] * 100}
    observation.data.update({"turns": [record] * 100, "parent_thread_id": oversized,
                             "resumed_from": oversized, "collaboration_items": [record] * 100,
                             "thread_inventory_before": [oversized] * 200,
                             "thread_inventory_after": [oversized] * 200,
                             "new_child_thread_ids": [oversized] * 200})
    write_private(observation.path, observation.data)
    reference = observation.reference()
    assert len(json.dumps(reference).encode()) < 64 * 1024
    assert reference["turn_count"] == 100
    assert len(reference["turns"]) <= observation.max_attempts


@pytest.mark.asyncio
@pytest.mark.parametrize("error", ["unauthorized", "contextWindowExceeded",
                                  "rateLimitExceeded"])
@pytest.mark.parametrize("role", ["implement", "intake", "verify"])
async def test_failed_role_exposes_bounded_typed_provider_reason(
        tmp_path, monkeypatch, error, role):
    from devflow_temporal import role_runner

    runtime, task, observation, *_ = harness(
        tmp_path, monkeypatch, [error] * 3,
        max_attempts=3 if error == "rateLimitExceeded" else 1,
        failure_message="Controlled provider failure " + "x" * 16384)
    monkeypatch.setattr(delivery_native_threads, "NativeThreadObservation", lambda _r: observation)
    monkeypatch.setattr(role_runner, "CodexAgentRuntime", lambda **_kw: runtime)
    monkeypatch.setattr(role_runner, "_task", lambda _r: task)
    binary = tmp_path / "codex"
    binary.write_bytes(b"fixture binary")
    request = {"role": role, "qa_evidence": {"sha256": "fixture"}, "spec": {"policy": {
        "codex_bin": str(binary),
        "codex_bin_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "execution_backend": "native-macos", "config_overrides": [], "host_sandbox": {},
    }}}
    result = await role_runner._run_codex(request)
    assert result["status"] == "blocked"
    assert error in " ".join(result["findings"])
    assert "Controlled provider failure" in " ".join(result["findings"])
    assert len(" ".join(result["findings"])) < 2048
    assert len(json.dumps(result["native_thread_observation"]).encode()) < 64 * 1024


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


@pytest.mark.asyncio
async def test_collaboration_reference_is_bounded_without_restoring_transcripts(
    tmp_path, monkeypatch,
):
    runtime, task, observation, *_ = harness(
        tmp_path, monkeypatch, ['rateLimitExceeded'], collaboration=True)
    result = await runtime.run(task)
    assert not result.is_success
    reference = observation.reference()
    assert reference['state'] == 'blocked'
    assert reference['new_child_thread_ids'] == ['child']
    assert reference['collaboration_items'][0]['receiverThreadIds'] == ['child']
    assert reference['collaboration_items'][0]['senderThreadId'] == 'owned-thread'
    oversized = '\u0000' * 1024
    item = {key: oversized for key in ['id', 'type', 'tool', 'status', 'senderThreadId',
                                     'prompt', 'output', 'message', 'agentsStates']}
    item['receiverThreadIds'] = [oversized] * 100
    observation.data['collaboration_items'] = [item] * 100
    observation.data['new_child_thread_ids'] = [oversized] * 200
    write_private(observation.path, observation.data)
    bounded = observation.reference()
    assert len(json.dumps(bounded).encode()) < 64 * 1024
    assert len(bounded['collaboration_items']) <= 8
    assert len(bounded['new_child_thread_ids']) <= 16
    assert bounded['collaboration_items_count'] == 100
    assert bounded['new_child_thread_ids_count'] == 200
    assert all('output' not in entry and 'prompt' not in entry
               for entry in bounded['collaboration_items'])
    assert read_private(observation.path)['collaboration_items'][0]['output'] == oversized
