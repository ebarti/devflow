"""Bounded ccusage execution and exact, supplied-snapshot token pricing."""
from __future__ import annotations

import json
import re
import subprocess
from datetime import datetime
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path

from devflow.errors import WorkflowError
from devflow.validation import canonical_json as decimal_json_dumps  # noqa: F401

PARTITIONS = (
    "uncached_input_tokens", "cache_read_tokens", "cache_write_tokens", "output_tokens"
)
NORMALIZATION_VERSION = "native-response-v1"


def decimal(value):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise WorkflowError("invalid_number", "Expected a finite nonnegative decimal") from exc
    if not result.is_finite() or result < 0:
        raise WorkflowError("invalid_number", "Expected a finite nonnegative decimal")
    return result


def price_usage(tokens, snapshot, *, model_id, service_tier, recorded_at):
    """Rates are independent USD/credit prices per million disjoint tokens.

    A snapshot selects the actual model, tier, effective interval and context band.
    No current-model fallback or USD-to-credit assumption is permitted.
    """
    unknown = {"price_snapshot_id": None, "api_equivalent_usd": None,
               "estimated_codex_credits": None, "pricing_status": "missing_rate"}
    if snapshot is None:
        return unknown
    if not snapshot.get("snapshot_id") or not snapshot.get("rate_version"):
        raise WorkflowError("invalid_price", "Snapshot identity and rate version are required")
    def instant(value):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise WorkflowError("invalid_price", "Price timestamps need a timezone")
        return parsed

    input_total = sum(tokens[key] for key in PARTITIONS[:3])
    if (snapshot.get("model_id") != model_id or snapshot.get("service_tier") != service_tier
            or not snapshot.get("effective_from")
            or instant(recorded_at) < instant(snapshot["effective_from"])
            or (snapshot.get("effective_until") and instant(recorded_at) >= instant(snapshot["effective_until"]))
            or input_total < snapshot.get("min_input_tokens", 0)
            or input_total > snapshot.get("max_input_tokens", float("inf"))):
        return unknown
    result = dict(unknown, price_snapshot_id=snapshot["snapshot_id"])
    with localcontext() as context:
        context.prec = 50
        for field, unit in (("api_equivalent_usd", "usd_per_million"),
                            ("estimated_codex_credits", "credits_per_million")):
            rates = snapshot.get(unit, {})
            if any(tokens[key] and rates.get(key) is None for key in PARTITIONS):
                continue
            result[field] = sum((Decimal(tokens[key]) * decimal(rates.get(key, 0) or 0)
                                 for key in PARTITIONS), Decimal(0)) / Decimal(1_000_000)
    if all(result[key] is not None for key in ("api_equivalent_usd", "estimated_codex_credits")):
        result["pricing_status"] = "complete"
    return result


class CcusageAdapter:
    """Never defaults to an account-wide data root; callers provide a scoped copy."""

    def __init__(self, runner=subprocess.run):
        self.runner = runner

    def collect(self, *, data_root, report="session"):
        import os

        root = Path(data_root).resolve(strict=True)
        # CODEX_HOME is a comma-separated list in ccusage, not an opaque path.
        if not root.is_dir() or "," in str(root) or report not in {"session", "daily"}:
            raise WorkflowError("usage_scope", "A scoped directory and supported report are required")
        env = dict(os.environ, CODEX_HOME=str(root))

        def run(args):
            result = self.runner(args, env=env, text=True, capture_output=True, check=False)
            if result.returncode:
                raise WorkflowError("ccusage_failed", "ccusage failed; private output was suppressed")
            return result.stdout

        def resolved_version():
            output = run(["npx", "ccusage@latest", "--version"]).strip()
            # The native 20.x collector reports "ccusage 20.0.20"; older
            # supported wrappers may print bare SemVer. Both name one version.
            match = re.fullmatch(r"(?:ccusage[ \t]+)?(\d+\.\d+\.\d+(?:[-+][A-Za-z0-9.-]+)?)", output)
            if not match:
                raise WorkflowError("ccusage_version", "Unrecognized resolved ccusage version")
            return match.group(1)

        version = resolved_version()
        raw = run(["npx", "ccusage@latest", "codex", report, "--json"])
        if resolved_version() != version:
            raise WorkflowError("ccusage_version", "Latest ccusage version changed during collection")
        try:
            summary = json.loads(raw, parse_float=Decimal)
        except ValueError as exc:
            raise WorkflowError("ccusage_format", "ccusage did not return valid JSON") from exc
        if not isinstance(summary, dict):
            raise WorkflowError("ccusage_format", "Expected a ccusage JSON object")
        return {"ccusage_version": version, "normalization_version": NORMALIZATION_VERSION,
                "summary": summary, "attribution": "requires_response_records"}


def reconcile_usage(records, summary):
    """Reconcile one identical scoped population; never attribute summary rows.

    Native ccusage 20.x names its disjoint uncached input `inputTokens` and its
    cache partitions `cacheReadTokens`/`cacheCreationTokens`. The separately
    recognized legacy shape uses inclusive input and `cachedInputTokens`.
    Upstream omissions, including unsupported cache-write counters, stay mismatches.
    """
    totals = summary.get("totals", {})
    native_fields = ("inputTokens", "cacheReadTokens", "cacheCreationTokens", "outputTokens",
                     "reasoningOutputTokens", "totalTokens")
    legacy_fields = ("inputTokens", "cachedInputTokens", "outputTokens", "reasoningOutputTokens")
    if not isinstance(totals, dict):
        return {"status": "unsupported_format", "reason": "Missing supported ccusage totals"}
    if all(key in totals for key in native_fields):
        summary_format = "ccusage-native-disjoint-v20"
    elif all(key in totals for key in legacy_fields):
        summary_format = "ccusage-legacy-inclusive"
    else:
        return {"status": "unsupported_format", "reason": "Missing supported ccusage totals"}
    unique = {}
    for row in records:
        key = row["response_id"]
        if key in unique and unique[key] != row:
            raise WorkflowError("usage_conflict", "Conflicting response during reconciliation")
        unique[key] = row
    partitions = {key: sum(row[key] for row in unique.values()) for key in PARTITIONS}
    reasoning = sum(row["reasoning_output_tokens"] for row in unique.values())
    if summary_format == "ccusage-native-disjoint-v20":
        observed = {
            "inputTokens": partitions["uncached_input_tokens"],
            "cacheReadTokens": partitions["cache_read_tokens"],
            "cacheCreationTokens": partitions["cache_write_tokens"],
            "outputTokens": partitions["output_tokens"],
            "reasoningOutputTokens": reasoning,
            "totalTokens": sum(partitions.values()),
        }
    else:
        observed = {
            "inputTokens": sum(partitions[key] for key in PARTITIONS[:3]),
            "cachedInputTokens": partitions["cache_read_tokens"],
            "cacheWriteInputTokens": partitions["cache_write_tokens"],
            "outputTokens": partitions["output_tokens"],
            "reasoningOutputTokens": reasoning,
        }
        if observed["cacheWriteInputTokens"] and "cacheWriteInputTokens" not in totals:
            return {"status": "unsupported_format", "reason": "ccusage did not expose cache-write totals"}
    differences = {key: observed[key] - decimal(totals.get(key, 0)) for key in observed}
    return {"status": "matched" if not any(differences.values()) else "mismatch",
            "summary_format": summary_format,
            "differences": differences, "unique_responses": len(unique)}
