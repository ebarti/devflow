"""Explicit-source native response ingestion. No transcript discovery or model calls.

Supported: Codex token_usage_record perresponse events (2026-09 observed shape),
OpenAI response.completed usage, and devflow.response.v1 normalized exports.
Cumulative turn counters and replayed parent history never add another charge.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal

from devflow.adapters.usage import PARTITIONS, decimal, price_usage
from devflow.errors import WorkflowError

TOKEN_FIELDS = (*PARTITIONS, "reasoning_output_tokens")


def response_key(task_id, native_response_id):
    return hashlib.sha256(json.dumps([task_id, native_response_id]).encode()).hexdigest()


def _count(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise WorkflowError("invalid_usage", "Token counts must be nonnegative integers")
    return value


def _tokens(usage):
    required = ("input_tokens", "output_tokens")
    if any(key not in usage for key in required):
        raise WorkflowError("unsupported_usage", "Missing perresponse input/output counters")
    total_input = _count(usage["input_tokens"])
    cached = _count(usage.get("cached_input_tokens", 0))
    written = _count(usage.get("cache_write_input_tokens", 0))
    output = _count(usage["output_tokens"])
    reasoning = _count(usage.get("reasoning_output_tokens", 0))
    if cached + written > total_input or reasoning > output:
        raise WorkflowError("invalid_usage", "Token partitions exceed their inclusive counters")
    if "total_tokens" in usage and _count(usage["total_tokens"]) != total_input + output:
        raise WorkflowError("invalid_usage", "Inclusive token total does not reconcile")
    return dict(zip(TOKEN_FIELDS, (total_input - cached - written, cached, written,
                                   output, reasoning), strict=True))


def _timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError
        return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    except (AttributeError, ValueError) as exc:
        raise WorkflowError("invalid_usage", "A timezone-aware response timestamp is required") from exc


def _normalize(event):
    kind = event.get("type")
    if kind == "token_usage_record":
        payload = event["payload"]
        return (payload["thread_id"], payload["response_id"], event["timestamp"],
                _tokens(payload["usage"]), payload.get("model"), payload.get("service_tier"))
    if kind == "response.completed":
        response = event["response"]
        usage = response["usage"]
        normalized = dict(usage,
                          cached_input_tokens=usage.get("input_tokens_details", {}).get(
                              "cached_tokens", 0),
                          cache_write_input_tokens=usage.get("input_tokens_details", {}).get(
                              "cache_write_tokens", 0),
                          reasoning_output_tokens=usage.get("output_tokens_details", {}).get(
                              "reasoning_tokens", 0))
        return (event["task_id"], response["id"], event["timestamp"], _tokens(normalized),
                response.get("model"), response.get("service_tier"))
    if kind == "devflow.response.v1":
        tokens = {key: _count(event["tokens"][key]) for key in TOKEN_FIELDS}
        if tokens["reasoning_output_tokens"] > tokens["output_tokens"]:
            raise WorkflowError("invalid_usage", "Reasoning is a subset of output")
        return (event["task_id"], event["response_id"], event["timestamp"], tokens,
                event.get("model_id"), event.get("service_tier"))
    # Known transcript metadata carries no response charges.
    if kind in {"session_meta", "turn_context", "event_msg", "response_item"}:
        return None
    raise WorkflowError("unsupported_usage", "Unsupported response event format")


def import_responses(events, *, ccusage_version, assignments=None, segments=None,
                     prices=None, existing=()):
    """Return deterministic upserts keyed by globally unique response_id.

    assignments[response_key(task, response)] = {segment_id, attempt_id, allocations}.
    segments[segment_id] carries task_id, attempt_id, actual model_id/service_tier.
    prices[response_key(...)] is the exact chosen price snapshot. Existing records
    may be replayed verbatim; changed usage or attribution needs explicit correction.
    """
    assignments, segments, prices = assignments or {}, segments or {}, prices or {}
    if not ccusage_version:
        raise WorkflowError("ccusage_version", "Resolved ccusage version is required")
    records = {}
    for record in existing:
        key = record["response_id"]
        if key in records and records[key] != record:
            raise WorkflowError("usage_conflict", "Existing response identities conflict")
        records[key] = record
    cumulative_only = False
    response_events = 0
    for event in events:
        if (event.get("type") == "event_msg"
                and event.get("payload", {}).get("type") == "token_count"):
            cumulative_only = True
        try:
            parsed = _normalize(event)
        except (KeyError, TypeError) as exc:
            raise WorkflowError("unsupported_usage", "Incomplete supported response event") from exc
        if parsed is None:
            continue
        response_events += 1
        task_id, native_id, timestamp, tokens, model, tier = parsed
        if not isinstance(task_id, str) or not task_id or not isinstance(native_id, str) or not native_id:
            raise WorkflowError("invalid_usage", "Native task and response identities are required")
        key = response_key(task_id, native_id)
        timestamp = _timestamp(timestamp)
        assignment = assignments.get(key, {})
        segment_id = assignment.get("segment_id")
        if segment_id:
            segment = segments.get(segment_id)
            if (not segment or segment.get("task_id") != task_id
                    or not assignment.get("attempt_id")
                    or segment.get("attempt_id") != assignment["attempt_id"]):
                raise WorkflowError("usage_assignment", "Response segment/attempt/task does not match")
            if ((model and model != segment.get("model_id"))
                    or (tier and tier != segment.get("service_tier"))):
                raise WorkflowError("usage_assignment", "Actual response model/tier disagrees with segment")
            start, end = segment.get("started_at"), segment.get("ended_at")
            if ((start and timestamp < _timestamp(start))
                    or (end and timestamp >= _timestamp(end))):
                raise WorkflowError("usage_assignment", "Response falls outside the assigned segment")
            model, tier = segment.get("model_id"), segment.get("service_tier")
        allocations = []
        seen_work = set()
        for allocation in assignment.get("allocations", []):
            work = allocation["work_id"]
            weight = decimal(allocation["weight"])
            if not work or work in seen_work or weight > 1:
                raise WorkflowError("usage_assignment", "Duplicate or invalid work allocation")
            allocations.append({"work_id": work, "weight": weight})
            seen_work.add(work)
        weight = sum((row["weight"] for row in allocations), Decimal(0))
        if weight > 1 or (allocations and not segment_id):
            raise WorkflowError("usage_assignment", "Allocation exceeds one or lacks a segment")
        record = {"schema_version": 1, "record_type": "usage", "response_id": key,
                  "task_id": task_id, "segment_id": segment_id, "recorded_at": timestamp,
                  **tokens, "ccusage_version": ccusage_version,
                  **price_usage(tokens, prices.get(key), model_id=model, service_tier=tier,
                                recorded_at=timestamp),
                  "allocations": sorted(allocations, key=lambda row: row["work_id"]),
                  "attribution_status": ("complete" if weight == 1 else
                                         "partial" if weight else "unallocated")}
        previous = records.get(key)
        if previous is not None and previous != record:
            raise WorkflowError("usage_conflict", "A response replay changed usage or provenance")
        records[key] = record
    if cumulative_only and not response_events:
        raise WorkflowError("unsupported_usage", "Cumulative counters lack unique response identities")
    return sorted(records.values(), key=lambda row: (row["recorded_at"], row["response_id"]))
