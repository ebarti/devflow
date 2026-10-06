"""A public change summary is separate from immutable execution instructions."""

from __future__ import annotations

import re


def conventional(subject: str) -> bool:
    return bool(re.fullmatch(r"[a-z][a-z0-9-]*(?:\([^()\r\n]+\))?!?: \S.*", subject))


def publication_summary(goal: str, supplied: str | None = None) -> str:
    """Admit one concise subject, never shorten a detailed execution prompt."""
    value = goal if supplied is None else supplied
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value.splitlines()) != 1
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
        or re.search(r"[.!?]\s+\S", value)
        or len(value.strip()) > 120
    ):
        raise ValueError(
            "publication_summary must be one concise change summary (at most 120 characters); "
            "provide it separately for a detailed goal"
        )
    summary = value.strip()
    if supplied is not None and not conventional(summary):
        raise ValueError("publication_summary must use Conventional Commit syntax")
    if not conventional(summary):
        summary = "chore: " + summary
    if len(summary) > 120:
        raise ValueError("publication_summary exceeds 120 characters including its type")
    return summary
