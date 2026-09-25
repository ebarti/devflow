"""Versioned JSON contracts exchanged with Temporal and the local CLI."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
ROLE_NAMES = ("implement", "review", "verify")


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def public_inputs(spec: dict[str, Any]) -> dict[str, Any]:
    """Inputs whose equality decides whether a repeated start is identical."""
    return {
        key: value
        for key, value in spec.items()
        if key not in {"initial_candidate", "input_digest", "preflight"}
    }


def validate_spec(spec: dict[str, Any]) -> None:
    if spec.get("version") != 1:
        raise ValueError("unsupported run contract version")
    run_id = spec.get("run_id")
    if not isinstance(run_id, str) or not RUN_ID_RE.fullmatch(run_id):
        raise ValueError("run ID must be 1-128 letters, digits, dots, underscores or hyphens")
    if not isinstance(spec.get("goal"), str) or not spec["goal"].strip():
        raise ValueError("goal must be non-empty")
    if spec.get("provider") not in ("fake", "codex"):
        raise ValueError("provider must be fake or codex")
    for key in ("repo", "state_dir"):
        if not isinstance(spec.get(key), str) or not spec[key].startswith("/"):
            raise ValueError(f"{key} must be an absolute path")
    if spec["provider"] == "codex" and (
        not isinstance(spec.get("model"), str)
        or not spec["model"].strip()
        or not isinstance(spec.get("effort"), str)
        or not spec["effort"].strip()
    ):
        raise ValueError("real provider requires explicit model and effort")
    if spec.get("fake_finding") not in (None, "review", "verify"):
        raise ValueError("fake_finding must be review or verify")
    if spec.get("fake_change") not in (None, "review", "verify"):
        raise ValueError("fake_change must be review or verify")
    if spec["provider"] != "fake" and (
        spec.get("fake_finding") is not None or spec.get("fake_change") is not None
    ):
        raise ValueError("fake scenarios are only supported with the fake provider")
    if not isinstance(spec.get("require_decision"), bool):
        raise ValueError("require_decision must be boolean")
    if not isinstance(spec.get("initial_candidate"), dict):
        raise ValueError("initial candidate is required")
