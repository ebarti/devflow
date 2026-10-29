"""Authenticate an unlaunched gate readiness refusal for bounded reassessment.

The immutable refusal is historical evidence, not a readiness pass or a launch
permission. The admitted continuation must probe again before any required gate.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from .contracts import digest
from .delivery_runtime_location import private_bytes

RETRY_LIMIT = 2
SLOTS = {"prepublish": 100, "local": 101, "browser_qa": 102}


def probe_attempt(spec, candidate, slot):
    """Reserve disjoint readiness evidence names for the two retry generations."""
    if slot not in SLOTS.values():
        raise ValueError("unsupported gate readiness slot")
    if spec.get("gate_retry_stage") == "readiness":
        generation = spec.get("gate_retry_generation")
        if type(generation) is not int or not 1 <= generation <= RETRY_LIMIT:
            raise ValueError("readiness retry generation exceeds its finite bound")
        # Existing hash-derived attempts are in [10, 900009].
        return 900010 + (generation - 1) * 3 + slot - 100
    generation = int(digest({"spec": spec, "candidate": candidate})[:5], 16) % 300000
    return 10 + generation * 3 + slot - 100


def failure(readiness, stage, candidate_id, cleanup):
    state = readiness.get("state")
    classification = ("environment_prelaunch" if state == "failed" and cleanup == "confirmed"
                      else "cancelled" if state == "cancelled" else "unknown")
    return {"version": 1, "class": classification, "source": "execution_readiness",
            "stage": stage, "candidate_id": candidate_id,
            "receipt_sha256": readiness.get("receipt_sha256"), "launched": False}


def possible(state):
    """Selection hint only. `observe` authenticates the result and its receipt."""
    checks = state.get("checks") or {}
    if not isinstance(checks, dict):
        return None
    found = [stage for stage in SLOTS if isinstance(checks.get(stage), dict)
             and checks[stage].get("failure_kind") == "readiness_preparation"
             and checks[stage].get("launched") is False]
    return found[0] if len(found) == 1 else None


def observe(spec, state):
    """Bind a completed controller result to its original private failure receipt."""
    stage = possible(state)
    checks, candidate = state.get("checks") or {}, state.get("candidate") or {}
    roles = state.get("roles")
    if (not isinstance(roles, list) or any(not isinstance(role, dict) for role in roles)):
        raise ValueError("readiness retry lacks its complete role history")
    if (spec.get("readiness_version") != 1 or stage is None
            or type(state.get("iteration")) is not int or state["iteration"] < 0
            or not isinstance(candidate, dict) or not candidate.get("id")):
        raise ValueError("readiness retry requires an exact unlaunched gate refusal")
    expected_error = {"prepublish": "execution readiness prevented prepublication",
                      "local": "execution readiness prevented local checks",
                      "browser_qa": "execution readiness prevented browser QA"}[stage]
    if state.get("error") != expected_error:
        raise ValueError("readiness retry lost its exact controller stop")
    result = checks[stage]
    receipt = result.get("readiness")
    if (not isinstance(receipt, dict) or result.get("state") != "failed"
            or result.get("cleanup") != "confirmed" or result.get("source_unchanged") is not True
            or result.get("candidate_id") != candidate["id"]
            or checks.get("readiness") != receipt or receipt.get("version") != 1
            or receipt.get("stage") != "plan" or receipt.get("state") != "failed"
            or receipt.get("iteration") != state["iteration"]
            or receipt.get("attempt") != probe_attempt(spec, candidate, SLOTS[stage])):
        raise ValueError("readiness retry lacks a confirmed candidate-bound refusal")
    provenance = failure(receipt, stage, candidate["id"], "confirmed")
    if result.get("failure_class") not in (None, provenance):
        raise ValueError("readiness failure provenance changed")
    # A failed or unassessed product gate remains authoritative. Never replace
    # pending findings with a same-candidate retry.
    for name in ("prepublish", "local", "browser_qa", "review", "qa", "ci"):
        item = checks.get(name)
        if name != stage and isinstance(item, dict) and item.get("state") != "passed":
            raise ValueError("readiness retry cannot replace another gate verdict")
    if any(role.get("iteration") == state["iteration"]
           and role.get("role") in {"review", "verify"}
           and (role.get("status") != "pass" or role.get("findings"))
           for role in roles):
        raise ValueError("readiness retry cannot replace an independent product finding")
    pending = state.get("repair_findings") or {}
    if not isinstance(pending, dict):
        raise ValueError("readiness retry lacks its pending finding history")
    if pending.get("iteration") == state["iteration"] and pending.get("findings"):
        raise ValueError("readiness retry cannot discard pending product findings")
    subject = receipt.get("binding") or {}
    observations = receipt.get("observations")
    if (not isinstance(subject, dict) or subject.get("version") != 1
            or subject.get("spec_sha256") != digest(spec) or subject.get("candidate") != candidate
            or not isinstance(subject.get("requirements_sha256"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", subject["requirements_sha256"])
            or type(subject.get("resource_generation")) is not int
            or subject["resource_generation"] < 0 or not isinstance(subject.get("resources"), dict)
            or not isinstance(observations, dict) or not observations
            or not any(isinstance(item, dict) and item.get("state") == "failed"
                       for item in observations.values())
            or any(not isinstance(item, dict) or not isinstance(item.get("state"), str)
                   or item.get("state") not in {"passed", "failed"}
                   or item.get("cleanup") != "confirmed" or item.get("candidate") != candidate
                   or item.get("requirement_sha256") != subject["requirements_sha256"]
                   for item in observations.values())):
        raise ValueError("readiness retry lacks complete confirmed failure observations")
    path = (Path(spec["state_dir"]) / "readiness" / "receipts" / "plan"
            / str(state["iteration"]) / f"{receipt['attempt']}.json")
    if receipt.get("receipt") != str(path):
        raise ValueError("readiness failure receipt left its owned run")
    raw = private_bytes(path, maximum=8 * 1024 * 1024)
    if (hashlib.sha256(raw).hexdigest() != receipt.get("receipt_sha256")
            or json.loads(raw) != {key: value for key, value in receipt.items()
                                   if key not in {"receipt", "receipt_sha256"}}):
        raise ValueError("readiness failure receipt changed")
    return {"stage": stage, "candidate_id": candidate["id"], "attempt": receipt["attempt"],
            "receipt": str(path), "receipt_sha256": receipt["receipt_sha256"],
            "requirements_sha256": subject["requirements_sha256"], "failure_class": provenance}
