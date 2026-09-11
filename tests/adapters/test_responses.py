import copy
from decimal import Decimal

import pytest

from devflow.adapters.responses import import_responses, response_key
from devflow.errors import WorkflowError


def event(response="response-1", task="synthetic-task", **usage):
    return {"type": "token_usage_record", "timestamp": "2026-09-01T00:00:00Z",
            "payload": {"thread_id": task, "response_id": response, "turn_id": "turn-1",
                        "usage": {"input_tokens": 100, "cached_input_tokens": 20,
                                  "cache_write_input_tokens": 10, "output_tokens": 40,
                                  "reasoning_output_tokens": 30, "total_tokens": 140, **usage},
                        "turn_token_usage": {"input_tokens": 100000, "output_tokens": 900000}}}


def assignment(response="response-1", work="work-1", weight="1", segment="segment-1"):
    key = response_key("synthetic-task", response)
    return {key: {"segment_id": segment, "attempt_id": "attempt-1",
                  "allocations": [{"work_id": work, "weight": weight}]}}


def segment():
    return {"segment-1": {"task_id": "synthetic-task", "attempt_id": "attempt-1",
                          "model_id": "synthetic-model", "service_tier": "synthetic-tier"}}


def test_native_replay_ignores_cumulative_turn_counts_and_parent_replay():
    records = import_responses([event(), event(), event("response-2")], ccusage_version="99.1.0")
    assert len(records) == 2
    assert records[0]["uncached_input_tokens"] == 70
    assert records[0]["output_tokens"] == 40
    assert records[0]["pricing_status"] == "missing_rate"
    assert records[0]["api_equivalent_usd"] is None
    assert import_responses([event()], existing=records, ccusage_version="99.1.0") == records


def test_task_reused_across_outcomes_requires_explicit_response_allocations():
    assignments = assignment() | assignment("response-2", "work-2", ".75")
    records = import_responses([event(), event("response-2")], ccusage_version="99.1.0",
                               assignments=assignments, segments=segment())
    by_id = {row["response_id"]: row for row in records}
    assert by_id[response_key("synthetic-task", "response-1")]["allocations"] == [
        {"work_id": "work-1", "weight": Decimal(1)}]
    assert by_id[response_key("synthetic-task", "response-2")]["attribution_status"] == "partial"


def test_response_namespace_includes_task_and_rejects_changed_counter():
    assert len(import_responses([event(), event(task="another-task")],
                                ccusage_version="99.1.0")) == 2
    with pytest.raises(WorkflowError, match="replay changed"):
        import_responses([event(), event(reasoning_output_tokens=29)], ccusage_version="99.1.0")


@pytest.mark.parametrize("mutation", ["attempt", "overallocate", "duplicate"])
def test_invalid_allocation_fails(mutation):
    assignments = assignment()
    selected = next(iter(assignments.values()))
    if mutation == "attempt":
        selected["attempt_id"] = "another-attempt"
    elif mutation == "overallocate":
        selected["allocations"][0]["weight"] = "1.1"
    else:
        selected["allocations"] *= 2
    with pytest.raises(WorkflowError):
        import_responses([event()], ccusage_version="99.1.0", assignments=assignments,
                         segments=segment())


def test_actual_model_must_match_segment():
    native = event()
    native["payload"]["model"] = "different-model"
    with pytest.raises(WorkflowError, match="model/tier"):
        import_responses([native], ccusage_version="99.1.0", assignments=assignment(),
                         segments=segment())


def test_later_observed_tier_is_preserved_when_startup_tier_was_unknown(monkeypatch):
    from devflow.adapters import responses

    native = event()
    native["payload"].update(model="synthetic-model", service_tier="synthetic-observed-tier")
    segments = segment()
    segments["segment-1"]["service_tier"] = None
    observed = []
    original = responses.price_usage

    def priced(*args, **kwargs):
        observed.append((kwargs["model_id"], kwargs["service_tier"]))
        return original(*args, **kwargs)

    monkeypatch.setattr(responses, "price_usage", priced)
    responses.import_responses([native], ccusage_version="99.1.0", assignments=assignment(),
                               segments=segments)
    responses.import_responses([event()], ccusage_version="99.1.0", assignments=assignment(),
                               segments=segments)
    assert observed == [("synthetic-model", "synthetic-observed-tier"), ("synthetic-model", None)]


def test_openai_response_completed_and_unsupported_summary():
    response = {"type": "response.completed", "task_id": "synthetic-task",
                "timestamp": "2026-09-01T00:00:00Z", "response": {"id": "r",
                "usage": {"input_tokens": 10, "input_tokens_details": {"cached_tokens": 3},
                          "output_tokens": 5, "output_tokens_details": {"reasoning_tokens": 4}}}}
    record = import_responses([response], ccusage_version="99.1.0")[0]
    assert record["uncached_input_tokens"] == 7
    assert record["reasoning_output_tokens"] == 4
    with pytest.raises(WorkflowError, match="Unsupported"):
        import_responses([{"sessions": [], "total_tokens": 500}], ccusage_version="99.1.0")


def test_invalid_native_partitions_rejected():
    with pytest.raises(WorkflowError, match="partitions"):
        import_responses([event(cached_input_tokens=100)], ccusage_version="99.1.0")
    malformed = copy.deepcopy(event())
    del malformed["payload"]["response_id"]
    with pytest.raises(WorkflowError, match="Incomplete"):
        import_responses([malformed], ccusage_version="99.1.0")


def test_cumulative_only_native_log_is_explicitly_unsupported():
    with pytest.raises(WorkflowError, match="Cumulative counters"):
        import_responses([{"type": "event_msg", "payload": {"type": "token_count",
                          "info": {"total_token_usage": {"input_tokens": 100}}}}],
                         ccusage_version="99.1.0")
