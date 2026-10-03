"""Versioned, deterministic shape validation for unresolved blocking questions."""

from __future__ import annotations

from typing import Any

BLOCKER_SCHEMA = {
    "type": "object",
    "properties": {
        "unknown": {"type": "string"},
        "evidence_checked": {"type": "array", "items": {"type": "string"}},
        "why_no_safe_default": {"type": "string"},
    },
    "required": ["unknown", "evidence_checked", "why_no_safe_default"],
    "additionalProperties": False,
}


def _text(value: Any, limit: int) -> bool:
    return isinstance(value, str) and bool(value.strip()) and len(value) <= limit


def valid_blocker(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == {"unknown", "evidence_checked", "why_no_safe_default"}
        and _text(value["unknown"], 4000)
        and _text(value["why_no_safe_default"], 4000)
        and isinstance(value["evidence_checked"], list)
        and 1 <= len(value["evidence_checked"]) <= 8
        and all(_text(item, 1000) for item in value["evidence_checked"])
    )


def valid_blocking_questions(questions: Any) -> bool:
    return (
        isinstance(questions, list) and 1 <= len(questions) <= 5
        and all(
            isinstance(q, dict) and set(q) == {"id", "prompt", "options", "blocker"}
            and _text(q["id"], 128) and _text(q["prompt"], 4000)
            and isinstance(q["options"], list) and len(q["options"]) <= 8
            and all(_text(option, 1000) for option in q["options"])
            and valid_blocker(q["blocker"])
            for q in questions
        )
        and len({q["id"] for q in questions}) == len(questions)
    )
