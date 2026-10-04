"""Regression for colored Vitest output from an actual contained JobCtrl check."""

import json
import re

from test_delivery_store import service as service

from devflow_temporal.delivery_output import observed_test_count, rejection_causes, visible_output
from devflow_temporal.delivery_workflow import _broker_findings


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


def test_playwright_notice_does_not_inflate_the_single_runner_test_count():
    actual_two = (
        "2026-09-26T20:38:53.395069592Z   2 passed (11.6s)\n"
        "2026-09-26T20:38:53.395541092Z ::notice title=🎭 Playwright "
        "Run Summary::  2 passed (11.6s)\n"
    )
    assert observed_test_count(actual_two, r"(?m)(\d+) passed") == 2
    actual_one_with_misleading_notice = (
        "2026-09-26T20:38:53.395069592Z   1 passed (11.6s)\n"
        "2026-09-26T20:38:53.395541092Z ::notice title=🎭 Playwright "
        "Run Summary::  2 passed (11.6s)\n"
    )
    assert observed_test_count(actual_one_with_misleading_notice, r"(?m)(\d+) passed") == 1
    assert observed_test_count(actual_one_with_misleading_notice, r"(?m)(\d+) passed") < 2


def test_passing_test_titles_are_not_failure_outcomes_but_real_outcomes_still_reject():
    pattern = r"(?m)\b(?:skipped|flaky|failed)\b"
    title = "  ✓  2 [chromium] › test.ts:189:1 › failed requests leave edits available (1.5s)\n"
    output = title + "  5 passed (14.0s)\n"
    # Historical diagnostics retain the original rejected match.
    assert rejection_causes(output, [pattern])[0]["match"] == "failed"
    assert rejection_causes(output, [pattern], test_results=True) == []
    for outcome in ("failed", "skipped", "flaky"):
        log = output + f"  1 {outcome}\n"
        cause = rejection_causes(log, [pattern], test_results=True)[0]
        assert cause["match"] == outcome
        assert cause["span"][0] >= len(output)
    # Explicit non-outcome prohibitions must still reject a passing title.
    assert rejection_causes(output, ["requests"], test_results=True)


def test_other_test_reporters_and_failed_test_lines_keep_honest_outcomes():
    pattern = r"(?m)\b(?:skipped|flaky|failed)\b"
    for prefix in ("✓ ", "✔ ", "√ ", "ok 12 - "):
        assert rejection_causes(prefix + "handles skipped and flaky requests\n", [pattern],
                                test_results=True) == []
    for prefix in ("✘ ", "not ok 12 - ", "WARNING: "):
        assert rejection_causes(prefix + "failed requests\n", [pattern], test_results=True)


def test_passing_title_rejection_at_original_offset_survives_repair_prompt_truncation():
    title = "✓ required-bullet coaching handles failed requests with bounded feedback"
    output = "x" * 1021 + title + "\n5 passed (6.4s)\n"
    pattern = r"(?m)\b(?:skipped|flaky|failed)\b"
    result = {
        "state": "failed",
        "candidate_id": "current-candidate",
        "exit_code": 0,
        "test_count": 5,
        "rejected_output": True,
        "diagnostic": output,
        "log": "/owned/run/browser-qa/3/browser-qa.log",
        "log_sha256": "a" * 64,
        "rejection_causes": rejection_causes(output, [pattern]),
    }
    findings = _broker_findings("browser_qa", result, iteration=3)
    summary = json.loads(findings[0].split(": ", 1)[1])
    assert title not in summary["diagnostic"]
    assert title in summary["rejection_causes"][0]["context"]
    assert summary["rejection_causes"][0]["pattern"] == pattern
    assert summary["rejection_causes"][0]["match"] == "failed"
    assert summary["log"] == result["log"] and summary["log_sha256"] == result["log_sha256"]
    assert summary["state"] == "failed" and summary["rejected_output"] is True


def test_large_multiline_rejection_keeps_match_span_and_bounded_context():
    output = "noise\n" * 10000 + "START\n" + "bad\n" * 10000 + "END\n"
    pattern = r"(?s)START.*END"
    cause = rejection_causes(output, [pattern])[0]
    assert cause["pattern"] == pattern and cause["span"][0] == 60000
    assert cause["match"].startswith("START\n")
    assert len(cause["context"]) <= 768 and len(cause["match"]) <= 512
    assert cause["match_length"] > len(cause["match"])
    assert cause["output_sha256"] and cause["pattern_sha256"]
    assert rejection_causes("5 passed\n", [r"(?m)^\s*\d+\s+failed\b"]) == []


def test_historical_hashed_full_log_enrichment_keeps_failure_and_owning_prompt_cause(service):
    from pathlib import Path

    import pytest

    from devflow_temporal.delivery_repair import failed_gate_diagnostics
    from devflow_temporal.delivery_resources import private_directory
    from devflow_temporal.role_runner import _task

    store, request = service
    store.submit(request)
    spec = store.spec("run-1")
    spec["policy"]["browser_qa"] = {"reject_regex": r"(?m)\b(?:skipped|flaky|failed)\b"}
    path = Path(spec["state_dir"]) / "browser-qa/4/browser-qa.log"
    private_directory(path.parent)
    title = "stale results, version conflicts, and failed requests leave manual edits available"
    path.write_text("x" * 1021 + "\n✓ " + title + "\n5 passed\n")
    path.chmod(0o600)
    import hashlib

    receipt = {
        "state": "failed",
        "cleanup": "confirmed",
        "exit_code": 0,
        "test_count": 5,
        "rejected_output": True,
        "diagnostic": path.read_text()[:2000],
        "log": str(path),
        "log_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    original = json.loads(json.dumps(receipt))
    state = {
        "iteration": 4,
        "error": "repair limit exhausted",
        "roles": [],
        "checks": {"browser_qa": receipt},
    }
    findings = failed_gate_diagnostics(state, spec)
    task = _task(
        {
            "spec": spec,
            "role": "implement",
            "iteration": 5,
            "candidate": {"id": "a" * 64, "head": "b" * 40},
            "workspace": spec["checkout"],
            "findings": findings,
        }
    )
    assert title in task.goal
    parsed = json.loads(findings[0].split(": ", 1)[1])
    assert parsed["rejection_causes"][0]["pattern"] == spec["policy"]["browser_qa"]["reject_regex"]
    assert str(path) in task.goal and receipt["log_sha256"] in task.goal
    assert receipt == original and receipt["state"] == "failed"
    path.write_text("modified historical evidence")
    with pytest.raises(ValueError, match="changed"):
        failed_gate_diagnostics(state, spec)
