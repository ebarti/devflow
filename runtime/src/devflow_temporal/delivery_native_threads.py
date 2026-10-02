"""Observe native SDK thread results before the kit's filtered tool translation."""

from __future__ import annotations

import hashlib
import os
from functools import wraps
from pathlib import Path

from openai_codex import AsyncCodex
from openai_codex.api import AsyncThread
from openai_codex.generated.v2_all import ThreadSourceKind

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
        self.data["raw_turn_items"] = len(items)
        self.data["collaboration_items"] = collaboration_items(items)
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
                result = await self.thread.run(*args, **kwargs)
                await observer.completed(self.codex, result)
                return result

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
