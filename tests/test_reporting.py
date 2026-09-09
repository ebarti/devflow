from decimal import Decimal

from devflow.adapters.responses import import_responses
from devflow.reporting import usage_report


def test_portfolio_deduplicates_repairs_and_preserves_unknown_remainder():
    events = [{"type": "devflow.response.v1", "task_id": "synthetic-task",
               "response_id": "r", "timestamp": "2026-09-01T00:00:00Z",
               "tokens": {"uncached_input_tokens": 10, "cache_read_tokens": 2,
                          "cache_write_tokens": 3, "output_tokens": 5,
                          "reasoning_output_tokens": 4}}]
    row = import_responses(events, ccusage_version="99.1.0")[0]
    row["allocations"] = [{"work_id": "original", "weight": Decimal(".25")},
                          {"work_id": "repair", "weight": Decimal(".5")}]
    report = usage_report([row, row], cutoff="2026-09-02T00:00:00Z",
                          observation_window="30-day")
    assert report["unique_responses"] == 1
    assert report["portfolio"]["total_tokens"] == 20  # reasoning already included
    assert report["unallocated"]["total_tokens"] == 5
    assert report["work_allocations"]["repair"]["total_tokens"] == 10
    assert report["portfolio"]["api_equivalent_usd"] is None
    assert report["portfolio"]["known_api_equivalent_usd"] == 0
    assert report["missing_data_count"] == 1
    assert usage_report([row], cutoff="2026-08-01T00:00:00Z")["unique_responses"] == 0
