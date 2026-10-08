"""Baseline diagnosis never substitutes for passing final candidate checks."""
from __future__ import annotations

import re
from typing import Any


def repairable_baseline(spec: dict[str, Any], result: dict[str, Any]) -> bool:
    """Allow only fully observed source regressions under fresh raw-goal authority."""
    candidate = result.get("baseline_candidate")
    policy = spec.get("policy")
    if (type(spec.get("baseline_checks_version")) is not int
            or spec["baseline_checks_version"] != 2 or spec.get("intake_required") is not True
            or result.get("state") != "failed" or result.get("source_unchanged") is not True
            or result.get("feature_unchanged") is not True
            or not isinstance(spec.get("base_sha"), str) or not spec["base_sha"]
            or result.get("base_sha") != spec["base_sha"]
            or not isinstance(candidate, dict) or candidate.get("head") != spec["base_sha"]
            or not isinstance(candidate.get("id"), str) or not candidate["id"]
            or result.get("candidate_id") != candidate["id"] or not isinstance(policy, dict)):
        return False
    recipes = policy.get("baseline_checks")
    if (not isinstance(recipes, list) or not recipes
            or any(not isinstance(recipe, dict) or not isinstance(recipe.get("id"), str)
                   or not recipe["id"] for recipe in recipes)):
        return False
    required = {recipe["id"] for recipe in recipes}
    if len(required) != len(recipes):
        return False
    # Both prepublication and postpublication keep every original recipe exactly.
    for stage in ("prepublish_checks", "checks"):
        checks = policy.get(stage)
        if (not isinstance(checks, list)
                or any(checks.count(recipe) != 1 for recipe in recipes)):
            return False
    rows = result.get("results")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        return False
    # The broker stops at its first failed command. Later baseline recipes are
    # unexecuted diagnostics, but remain mandatory on the final candidate.
    observed = [row.get("id") for row in rows if row.get("id") in required]
    if not observed or observed != [recipe["id"] for recipe in recipes[:len(observed)]]:
        return False
    failures = [row for row in rows if row.get("passed") is not True]
    if (len(failures) != 1 or rows[-1] is not failures[0]
            or failures[0].get("id") not in required):
        return False
    for row in rows:
        native = row.get("native_process")
        if (type(row.get("passed")) is not bool or type(row.get("exit_code")) is not int
                or (row["exit_code"] != 0 if row["passed"] else row["exit_code"] <= 0)
                or row.get("cleanup") != "confirmed" or row.get("failure_kind") is not None
                or row.get("launched") is False or row.get("rejected_output") is not False
                or row.get("rejection_causes") != []
                or not isinstance(row.get("log"), str) or not row["log"]
                or not isinstance(row.get("log_sha256"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", row["log_sha256"])
                or not isinstance(native, dict) or native.get("state") != "finished"
                or type(native.get("exit_code")) is not int
                or native["exit_code"] != row["exit_code"]
                or native.get("timed_out") is not False or native.get("cancelled") is not False
                or native.get("monitoring_complete") is not True
                or native.get("stdio_drained") is not True
                or native.get("cleanup") != "observed-native-confirmed"):
            return False
        if not row["passed"] and (type(row.get("test_count")) is not int
                                  or row["test_count"] <= 0):
            return False
    return True
