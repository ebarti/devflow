"""Versioned read model of existing state owners; this never grants execution."""
from __future__ import annotations

import json
from typing import Any

from .contracts import digest


def recorded(row: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    """Describe persisted evidence without reconstructing an environment proof.

    A digest names recorded bytes; it is deliberately not a new authorization
    token. Recovery and launch continue to authenticate their owning stores.
    """
    controls = json.loads(row["checks_json"] or "{}").get("delivery_limits_control")
    feature = spec.get("feature_delivery") or {}
    policy = spec["policy"]
    return {
        "version": 1,
        "authorization": {
            "version": 1, "owner": "immutable_admission",
            "request_digest": row["request_digest"], "admission_sha256": digest(spec),
            "policy_digest": spec["policy_digest"],
            "admitted_plan_sha256": digest(spec.get("accepted_plan")),
            "accepted_plan_sha256": digest(
                row["accepted_plan_text"] if row["accepted_plan_text"] is not None
                else spec.get("accepted_plan")),
            "accepted_plan_owner": "accepted_plan_record",
            "authorized_endpoint": spec["authorized_endpoint"],
            "budget": {key: policy[key] for key in (
                "max_attempts", "max_repairs", "delivery_limits") if key in policy},
        },
        "execution": {
            "version": 1, "owner": "execution_registry" if feature else "workflow",
            "workflow_id": row["workflow_id"],
            "recorded_feature_owner": feature.get("owner"),
            "protocol_revision": row["protocol_revision"],
            "verification": "recorded",
        },
        "environment": {
            "version": 1, "owner": "execution_configuration_and_preparation",
            "config_digest": spec.get("config_digest"),
            "policy_digest": spec["policy_digest"],
            "verification": "not_reconstructed",
        },
        "compatible_transitions": {
            "version": 1, "owner": "authenticated_recovery_receipt",
            "recovery_sha256": digest(json.loads(row["recovery_json"]))
            if row["recovery_json"] else None,
            "verification": "recorded",
        },
        "operational_control": {
            "version": 1, "owner": "accepted_control_receipt",
            "delivery_limits_control": controls,
            "lineage": spec.get("delivery_control_lineage"),
        },
        "effects": {"version": 1, "owner": "effect_journal_and_executor_lease"},
        "projection": {
            "version": 1, "owner": "derived_delivery_projection",
            "revision": row["revision"], "protocol_revision": row["protocol_revision"],
        },
    }


def gate_specs(spec: dict[str, Any]) -> list[tuple[str, str, str]]:
    gates = [
        ("prepare", "Prepare", "tracker_start"),
        ("prepublish", "Before PR checks", "candidate_ready"),
        ("publish", "Publish", "published"),
        ("local_checks", "Local checks",
         "browser_qa_started" if spec["policy"].get("browser_qa") else "ci_wait"),
    ]
    if spec["policy"].get("browser_qa"):
        gates.append(("browser_qa", "Browser / API QA", "browser_qa_passed"))
    return [*gates, ("required_ci", "Required CI", "tracker_started"),
            ("tracker", "Tracker", "delivered")]
