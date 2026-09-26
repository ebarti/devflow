"""Regression for colored Vitest output from an actual contained JobCtrl check."""

import re

from devflow_temporal.delivery_output import visible_output


def test_colored_vitest_summary_retains_test_count_and_failure_words():
    raw = (
        "2026-09-26T20:15:03.483160375Z \x1b[2m      Tests \x1b[22m "
        "\x1b[1m\x1b[32m1 passed\x1b[39m\x1b[22m\x1b[90m (1)\x1b[39m\n"
    )
    assert re.findall(r"(?m)Tests\s+(\d+)\s+passed", raw) == []
    assert re.findall(r"(?m)Tests\s+(\d+)\s+passed", visible_output(raw)) == ["1"]
    colored_failure = "\x1b[31m1 failed\x1b[0m\n"
    assert re.search(r"(?m)\bfailed\b", visible_output(colored_failure))
    assert "\x1b[32m" in raw  # Immutable evidence keeps the original bytes.


def test_timestamped_colored_browser_failure_cannot_hide_from_anchored_reject_pattern():
    raw = (
        "2026-09-26T20:15:03.483160375Z \x1b[32m2 passed\x1b[0m\n"
        "2026-09-26T20:15:03.483160376Z \x1b[31m 1 failed\x1b[0m\n"
    )
    failure = r"(?m)^\s*\d+\s+(?:failed|skipped|flaky|did not run)\b"
    assert not re.search(failure, raw)
    parsed = visible_output(raw)
    assert re.findall(r"(?m)(\d+) passed", parsed) == ["2"]
    assert re.search(failure, parsed)
    assert raw.startswith("2026-09-26T")
