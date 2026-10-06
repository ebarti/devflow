"""Observe native SDK thread results before the kit's filtered tool translation."""

from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import replace
from functools import wraps
from pathlib import Path

from openai_codex import AsyncCodex, CodexError, ServerBusyError, TurnResult
from openai_codex.api import AsyncThread
from openai_codex.generated.v2_all import (
    AgentMessageThreadItem,
    ItemCompletedNotification,
    MessagePhase,
    ThreadSourceKind,
    ThreadTokenUsageUpdatedNotification,
    TurnCompletedNotification,
    TurnStatus,
)

from .delivery_resources import read_private, write_private


def collaboration_items(items) -> list[dict]:
    found = []
    for wrapped in items:
        item = getattr(wrapped, "root", wrapped)
        value = item if isinstance(item, dict) else item.model_dump(mode="json", by_alias=True)
        if value.get("type") == "collabAgentToolCall":
            # Prompts and response contents are unnecessary for this ownership evidence.
            found.append(
                {
                    key: value.get(key)
                    for key in (
                        "id",
                        "type",
                        "tool",
                        "status",
                        "senderThreadId",
                        "receiverThreadIds",
                    )
                }
            )
    return found


class NativeThreadObservation:
    def __init__(self, request: dict):
        folder = Path(request["result_path"]).parent
        self.path = folder / "native-thread-observation.json"
        identity = read_private(folder / "native-process.json")["owned"][str(os.getpid())]
        policy = request["spec"].get("policy", {})
        self.legacy = "provider_max_attempts" not in policy
        self.max_attempts = policy.get("provider_max_attempts", 1)
        if type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 3:
            raise ValueError("invalid frozen provider attempt limit")
        self.data = {
            "schema": "devflow-native-kit-threads-v1",
            "run_id": request["spec"]["run_id"],
            "role": request["role"],
            "iteration": request["iteration"],
            "pid": os.getpid(),
            "start_identity": identity["identity"],
            "resumed_from": request.get("resume_session"),
            "state": "unknown",
            "parent_thread_id": None,
            "raw_turn_items": None,
            "collaboration_items": [],
            "thread_inventory_before": None,
            "thread_inventory_after": None,
            "new_child_thread_ids": None,
            "observation_source": "SDK typed TurnResult and isolated CODEX_HOME thread_list",
        }
        if not self.legacy:
            self.data["turns"] = []
        write_private(self.path, self.data)

    async def inventory(self, codex) -> list[str]:
        found = set()
        # Explicitly include subagents and archived children. Default thread_list
        # filters to interactive sources and cannot establish this observation.
        for archived in (False, True):
            response = await codex.thread_list(
                archived=archived,
                limit=100,
                source_kinds=list(ThreadSourceKind),
                use_state_db_only=False,
            )
            if response.next_cursor:
                raise ValueError("native thread inventory exceeds its bounded observation")
            found.update(thread.id for thread in response.data)
        return sorted(found)

    async def begin(self, codex) -> None:
        self.data["thread_inventory_before"] = await self.inventory(codex)
        write_private(self.path, self.data)

    def started(self, thread_id: str) -> None:
        prior = self.data["resumed_from"]
        if prior and prior != thread_id:
            raise ValueError("native controller resume changed its provider thread")
        self.data["parent_thread_id"] = thread_id
        write_private(self.path, self.data)  # durable before the provider turn

    async def completed(self, codex, result) -> None:
        items = list(result.items)
        self.data["raw_turn_items"] = (self.data["raw_turn_items"] or 0) + len(items)
        self.data["collaboration_items"].extend(collaboration_items(items))
        if self.data["collaboration_items"]:
            self.data["state"] = "blocked"
            self.data["new_child_thread_ids"] = sorted(
                {
                    child
                    for item in self.data["collaboration_items"]
                    for child in item["receiverThreadIds"] or []
                    if child != self.data["parent_thread_id"]
                }
            )
        write_private(self.path, self.data)
        await self.observe_inventory(codex)

    async def observe_inventory(self, codex) -> None:
        after = await self.inventory(codex)
        self.data["thread_inventory_after"] = after
        before = self.data["thread_inventory_before"]
        parent = self.data["parent_thread_id"]
        children = set(after) - set(before) - {parent}
        children.update(
            child
            for item in self.data["collaboration_items"]
            for child in item["receiverThreadIds"] or []
            if child != parent
        )
        self.data["new_child_thread_ids"] = sorted(children)
        self.data["state"] = (
            "confirmed"
            if parent in after and not children and not self.data["collaboration_items"]
            else "blocked"
        )
        write_private(self.path, self.data)

    async def collect(self, handle):
        """Retain typed failed completion before the SDK convenience API erases it."""
        items, usage, turn = [], None, None
        stream = handle.stream()
        record = {"thread_id": handle.thread_id, "turn_id": handle.id,
                  "turn": None, "items": [], "usage": None}
        self.data["turns"].append(record)
        self.data["state"] = "unknown"
        write_private(self.path, self.data)
        try:
            if handle.thread_id != self.data["parent_thread_id"]:
                raise ValueError("turn handle changed its provider thread")
            async for event in stream:
                payload = event.payload
                if not isinstance(payload, (ItemCompletedNotification,
                                            ThreadTokenUsageUpdatedNotification,
                                            TurnCompletedNotification)):
                    continue
                if payload.thread_id != handle.thread_id:
                    raise ValueError("notification changed its provider thread")
                if isinstance(payload, ItemCompletedNotification) and payload.turn_id == handle.id:
                    items.append(payload.item)
                elif (isinstance(payload, ThreadTokenUsageUpdatedNotification)
                      and payload.turn_id == handle.id):
                    usage = payload.token_usage
                elif (isinstance(payload, TurnCompletedNotification)
                      and payload.turn.id == handle.id):
                    turn = payload.turn
        finally:
            record.update({
                "turn": turn.model_dump(mode="json", by_alias=True) if turn else None,
                "items": [item.model_dump(mode="json", by_alias=True) for item in items],
                "usage": usage.model_dump(mode="json", by_alias=True) if usage else None,
            })
            write_private(self.path, self.data)
            await stream.aclose()
        if turn is None:
            raise RuntimeError("turn completed event not received")
        if turn.status not in {TurnStatus.completed, TurnStatus.failed, TurnStatus.interrupted}:
            raise ValueError("provider completion has no terminal status")
        messages = [item.root for item in items if isinstance(item.root, AgentMessageThreadItem)]
        final = next((item.text for item in reversed(messages)
                      if item.phase == MessagePhase.final_answer), None)
        if final is None:
            final = next((item.text for item in reversed(messages) if item.phase is None), None)
        return TurnResult(id=turn.id, status=turn.status, error=turn.error,
                          started_at=turn.started_at, completed_at=turn.completed_at,
                          duration_ms=turn.duration_ms, final_response=final,
                          items=items, usage=usage)

    @staticmethod
    def retryable(result):
        if result.status != TurnStatus.failed or not result.error:
            return False
        info = result.error.codex_error_info
        value = info.model_dump(mode="json", by_alias=True) if info else None
        if isinstance(value, str):
            return value in {"rateLimitExceeded", "serverOverloaded", "internalServerError",
                             "flexUnavailable"}
        if not isinstance(value, dict):
            return False
        for kind in ("httpConnectionFailed", "responseStreamConnectionFailed",
                     "responseStreamDisconnected", "responseTooManyFailedAttempts"):
            if kind in value:
                code = value[kind].get("httpStatusCode")
                return code is None or code in {408, 429} or 500 <= code <= 599
        return False
    def reference(self) -> dict:
        return {
            **self.data,
            "path": str(self.path),
            "sha256": hashlib.sha256(self.path.read_bytes()).hexdigest(),
        }

    def codex_class(self):
        observer = self

        class ObservedThread:
            def __init__(self, codex, thread):
                self.codex, self.thread, self.id = codex, thread, thread.id

            @wraps(AsyncThread.run)
            async def run(self, *args, **kwargs):
                if observer.legacy:
                    result = await self.thread.run(*args, **kwargs)
                    await observer.completed(self.codex, result)
                    return result
                results = []
                for attempt in range(observer.max_attempts):
                    try:
                        handle = await self.thread.turn(*args, **kwargs)
                    except CodexError as exc:
                        # An explicit RPC rejection has no unknown accepted turn.
                        # Lost transport or missing completion is never replayed.
                        observer.data["turns"].append({"start_error": {
                            "type": type(exc).__name__, "code": getattr(exc, "code", None),
                            "message": str(exc), "data": getattr(exc, "data", None)}})
                        write_private(observer.path, observer.data)
                        if not isinstance(exc, ServerBusyError):
                            raise
                        await observer.observe_inventory(self.codex)
                        if (observer.data["state"] != "confirmed"
                                or attempt + 1 == observer.max_attempts):
                            raise
                        await asyncio.sleep(2 ** attempt)
                        continue
                    result = await observer.collect(handle)
                    await observer.completed(self.codex, result)
                    results.append(result)
                    if (observer.data["state"] != "confirmed" or not observer.retryable(result)
                            or attempt + 1 == observer.max_attempts):
                        break
                    await asyncio.sleep(2 ** attempt)
                # The kit consumes per-turn `last`, not cumulative thread usage.
                # Preserve all tool effects and account for every bounded attempt.
                usage = None
                if all(result.usage is not None for result in results):
                    last = results[-1].usage.last.model_dump()
                    total = {key: sum(result.usage.last.model_dump()[key] for result in results)
                             if all(result.usage.last.model_dump()[key] is not None
                                    for result in results) else None for key in last}
                    usage = results[-1].usage.model_copy(update={
                        "last": results[-1].usage.last.model_validate(total)})
                return replace(results[-1], usage=usage,
                               items=[item for result in results for item in result.items])

        class ObservedCodex(AsyncCodex):
            @wraps(AsyncCodex.thread_start)
            async def thread_start(self, *args, **kwargs):
                await observer.begin(self)
                thread = await super().thread_start(*args, **kwargs)
                observer.started(thread.id)
                return ObservedThread(self, thread)

            @wraps(AsyncCodex.thread_resume)
            async def thread_resume(self, *args, **kwargs):
                await observer.begin(self)
                thread = await super().thread_resume(*args, **kwargs)
                observer.started(thread.id)
                return ObservedThread(self, thread)

        return ObservedCodex
