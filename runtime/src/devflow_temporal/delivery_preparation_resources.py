"""Canonical run ownership for the existing finite native measurement probes."""

from __future__ import annotations

import json
import re
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from pathlib import Path

from .contracts import digest
from .delivery_execution_config import load_execution_config
from .delivery_runtime_location import private_bytes

FIELD = "native_preparation_resource_owner"
_LEGACY = ContextVar("native_preparation_resource_owner", default=None)


def _authority(intent):
    value = {
        key: item
        for key, item in intent.items()
        if key not in {"state", "preparation_attempts", "effective_spec"}
    }
    identities = (
        ("command_digest", "seal_digest") if "command_digest" in value else ("binding_sha256",)
    )
    if not all(
        isinstance(value.get(key), str) and re.fullmatch(r"[0-9a-f]{64}", value[key])
        for key in identities
    ) or ("binding_sha256" in value and "command_digest" in value):
        raise ValueError("native preparation has no exact owning intent")
    return digest(value)


def binding(canonical, intent, path, index):
    """Only _prepare derives this finite resource context before measurement."""
    return {
        "canonical_state_dir": canonical["state_dir"],
        "canonical_spec_sha256": digest(canonical),
        "intent_path": str(path),
        "intent_authority_sha256": _authority(intent),
        "attempt_index": index,
    }


def canonical_owner(spec):
    """Authenticate a known probe, never normalize an arbitrary namespace."""
    supplied = spec.get(FIELD)
    legacy = _LEGACY.get()
    if supplied is None and legacy is not None and legacy[0] == digest(spec):
        supplied = legacy[1]
    if supplied is None:
        return None
    if spec.get("native_diagnostic_resource_only"):
        raise ValueError("native preparation cannot use a different resource purpose")
    if (
        not isinstance(supplied, dict)
        or set(supplied)
        != {
            "canonical_state_dir",
            "canonical_spec_sha256",
            "intent_path",
            "intent_authority_sha256",
            "attempt_index",
        }
        or type(supplied["attempt_index"]) is not int
        or supplied["attempt_index"] not in (0, 1)
    ):
        raise ValueError("native preparation resource owner is malformed")
    parent = deepcopy(spec)
    parent.pop(FIELD, None)
    parent["state_dir"] = supplied["canonical_state_dir"]
    if digest(parent) != supplied["canonical_spec_sha256"]:
        raise ValueError("native preparation canonical parent changed")
    config = load_execution_config(parent)
    if str(config.tracking_db) != parent["policy"]["tracking_db"]:
        raise ValueError("native preparation changed its canonical database")
    state, path = Path(spec["state_dir"]), Path(supplied["intent_path"])
    canonical = Path(parent["state_dir"])
    renewal_paths = {
        canonical / "native-preparation-renewal/preparation.json",
        canonical / "technical-successor/native-generation/preparation.json",
        canonical / "resource-closure/native-generation/preparation.json",
    }
    renewal = path in renewal_paths
    if (
        not path.is_absolute()
        or path.resolve(strict=True) != path
        or not path.is_relative_to(canonical)
        or (path.name != "intent.json" and not renewal)
        or state != path.parent / "probes" / str(supplied["attempt_index"]) / spec["run_id"]
        or ".." in state.parts
        or state.resolve() != state
    ):
        raise ValueError("native preparation left its exact finite resource generation")
    raw = private_bytes(path)
    intent = json.loads(raw)
    authority_raw = None
    authority_path = path.parent / "authority.json"
    if renewal:
        authority_raw = private_bytes(authority_path)
        authority = json.loads(authority_raw)
        if not isinstance(intent, dict) or digest(authority) != intent.get("binding_sha256"):
            raise ValueError("native renewal lost its exact preparation authority")
    attempts = intent.get("preparation_attempts") if isinstance(intent, dict) else None
    if (
        not isinstance(attempts, list)
        or not 1 <= len(attempts) <= 2
        or supplied["attempt_index"] >= len(attempts)
        or not isinstance(attempts[supplied["attempt_index"]], dict)
        or attempts[supplied["attempt_index"]].get("spec") != spec
        or attempts[supplied["attempt_index"]].get("state")
        not in {"started", "stopped", "interrupted"}
        or _authority(intent) != supplied["intent_authority_sha256"]
    ):
        raise ValueError("native preparation lost its exact recorded resource attempt")
    from .delivery_store import tracking_read_connection

    with tracking_read_connection(config.tracking_db) as db:
        db.execute("PRAGMA query_only=ON")
        row = db.execute(
            "SELECT request_json,request_digest,work_id FROM delivery_runs WHERE run_id=?",
            (spec["run_id"],),
        ).fetchone()
        if renewal:
            try:
                anchor = db.execute(
                    "SELECT binding_sha256,parent_sha256 FROM delivery_native_renewal_anchors "
                    "WHERE run_id=? AND namespace=?",
                    (spec["run_id"], str(path.parent.relative_to(canonical))),
                ).fetchone()
            except sqlite3.OperationalError as exc:
                raise ValueError("native renewal has no durable probe authority") from exc
            if anchor != (digest(authority), digest(parent)):
                raise ValueError("native renewal lost its durable probe authority")
    original = json.loads(row[0]) if row else None
    if (
        not isinstance(original, dict)
        or original.get("state_dir") != str(canonical)
        or row[1] != parent["request_digest"]
        or row[2] != parent["work_id"]
        or original.get("policy", {}).get("tracking_db") != str(config.tracking_db)
        or any(
            original.get(key) != parent.get(key)
            for key in (
                "run_id",
                "work_id",
                "request_digest",
                "provider",
                "source_path",
                "checkout",
                "origin_url",
                "repository_key",
                "github_repo",
                "authorized_endpoint",
            )
        )
        or private_bytes(path) != raw
        or (renewal and private_bytes(authority_path) != authority_raw)
    ):
        raise ValueError("native preparation lost its canonical admitted run")
    return parent


@contextmanager
def resource_scope(spec, canonical_state, intent, path, index, *,
                   renewal_binding=None, renewal_parent=None):
    """Re-observe old attempts without changing their recorded specs or journals."""
    parent = deepcopy(spec)
    parent.pop(FIELD, None)
    parent["state_dir"] = canonical_state
    expected = binding(parent, intent, path, index)
    if FIELD in spec and spec[FIELD] != expected:
        raise ValueError("native preparation changed its recorded canonical parent")
    if renewal_binding is not None and parent != renewal_parent:
        raise ValueError("native renewal probe changed its exact effective parent")
    token = _LEGACY.set((digest(spec), expected, deepcopy(renewal_binding)))
    try:
        parent = canonical_owner(spec)
        if parent is None:
            raise ValueError("native preparation lacks its authenticated resource parent")
        from .delivery_native_diagnostic import require_resource_idle
        from .delivery_resources import RunResources

        # The diagnostic ACK holds a shared lease on precisely this original
        # registry. Hold its existing exclusive lease through probe measurement
        # and finalization, without changing the original manifest. A prevalidated
        # ACK cannot race a separate probe registry into an active diagnostic.
        registry = RunResources(parent, read_only=True)
        require_resource_idle(parent)
        with registry.locked(read_only=True, exclusive=True):
            require_resource_idle(parent)
            yield parent
    finally:
        _LEGACY.reset(token)
