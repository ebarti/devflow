"""Schedule identical broad validation once, without synthesizing cached evidence."""
from __future__ import annotations

from pathlib import PurePosixPath

from .contracts import canonical_json


def _signature(check):
    value = {key: item for key, item in check.items() if key != "id"}
    value.setdefault("cwd", ".")
    value.setdefault("timeout_seconds", 600)
    return canonical_json(value)


def _preparation(check):
    argv = check["argv"]
    return ("/store" in argv or bool(check.get("native_addon_prerequisite"))
            or any(token in {"install", "sync"} for token in argv))


def prepublication(spec):
    """Return checks still needed before publication and exact deferred ID bindings.

    A build-only worker has no postpublication gate. Dependency setup and Git
    history checks remain where requested. Unknown unique commands may depend on
    earlier build output, so their presence conservatively keeps the whole list.
    """
    checks = list(spec["policy"].get("prepublish_checks", []))
    if (spec.get("validation_schedule_version") != 1
            or spec.get("feature_worker", {}).get("kind") == "build"):
        return checks, {}
    post = {_signature(c): c for c in spec["policy"].get("checks", [])}
    deferred, retained = {}, []
    for check in checks:
        match = post.get(_signature(check))
        history = PurePosixPath(check["argv"][0]).name == "git"
        if match and not history and not _preparation(check):
            deferred[check["id"]] = match["id"]
        else:
            retained.append(check)
            if not history and not _preparation(check):
                return checks, {}
    return retained, deferred


def require_deferred_checks(spec, prechecked, checked):
    """A deferred requirement is met only by its actual passing later command."""
    _retained, expected = prepublication(spec)
    if (prechecked.get("deferred_to_checks") or {}) != expected:
        raise ValueError("prepublication deferral differs from frozen validation inputs")
    completed = {r["id"]: r for r in checked.get("results", [])}
    recipes = {r["id"]: r for r in spec["policy"].get("checks", [])}
    for ident in expected.values():
        result = completed.get(ident, {})
        if result.get("passed") is not True or result.get("argv") != recipes[ident]["argv"]:
            raise ValueError("deferred validation has no passing execution: " + ident)
