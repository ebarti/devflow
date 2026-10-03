"""Submission-only capture of a frozen callback destination."""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID


def thread_uuid(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("origin_thread_id must be a canonical UUID")
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise ValueError("origin_thread_id must be a canonical UUID") from exc
    if str(parsed) != value or not parsed.int:
        raise ValueError("origin_thread_id must be a canonical UUID")
    return value


def bind_origin(supplied: dict, observed: str | None) -> dict:
    """Freeze per-call identity; an explicit destination cannot disagree with it."""
    explicit = thread_uuid(supplied["origin_thread_id"]) if "origin_thread_id" in supplied else None
    if observed is not None:
        observed = thread_uuid(observed)
    if explicit is not None and observed is not None and explicit != observed:
        raise ValueError("origin_thread_id disagrees with the originating thread")
    return {
        **supplied, **({"origin_thread_id": explicit or observed} if explicit or observed else {})
    }


def metadata_origin(metadata: dict[str, Any] | None) -> str | None:
    """Read MCP request metadata, never the server process's startup environment."""
    metadata = metadata or {}
    values = [metadata[key] for key in ("threadId", "openai/threadId", "openai/thread_id")
              if key in metadata]
    if "x-codex-turn-metadata" in metadata:
        turn = metadata["x-codex-turn-metadata"]
        if isinstance(turn, str):
            try:
                turn = json.loads(turn)
            except ValueError as exc:
                raise ValueError("invalid x-codex-turn-metadata") from exc
        if not isinstance(turn, dict):
            raise ValueError("invalid x-codex-turn-metadata")
        values.extend(turn[key] for key in ("thread_id", "threadId") if key in turn)
    origins = {thread_uuid(value) for value in values}
    if len(origins) > 1:
        raise ValueError("originating thread metadata disagrees")
    return next(iter(origins), None)
