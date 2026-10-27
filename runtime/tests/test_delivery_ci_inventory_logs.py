"""Literal report parsing and source-framed execution, never semantic coverage."""

import copy

import pytest
from ci_inventory_fixture import fixture

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_ci_inventory_contract import workflow_plan
from devflow_temporal.delivery_ci_inventory_logs import executed_log, report


@pytest.mark.parametrize(
    "kind,text,expected",
    [
        (
            "pytest",
            "5544 passed, 19 skipped, 75 warnings in 10.2s",
            {"passed": 5544, "skipped": 19, "warnings": 75},
        ),
        ("vitest", " Tests 2 passed | 1 skipped (3)", [{"passed": 2, "skipped": 1}]),
        ("jest", "Tests: 4 passed, 4 total", {"passed": 4}),
        (
            "playwright",
            "1 flaky\n6 passed (10.0s)\n##[notice] 1 flaky\n6 passed (10.0s)",
            {"flaky": 1, "passed": 6, "repeated_notice_blocks": 1},
        ),
    ],
)
def test_terminal_formats_preserve_actual_cases_and_source_permitted_skips(kind, text, expected):
    assert report(kind, text.splitlines()) == expected


@pytest.mark.parametrize(
    "drift",
    [
        "source_command",
        "input_group",
        "declared_env",
        "action",
        "checkout",
        "step",
        "post",
        "report",
    ],
)
def test_exact_hash_alone_cannot_hide_a_structural_execution_mismatch(drift):
    case = fixture()
    pin = copy.deepcopy(case.profile["jobs"][case.profile["required_ci"][0]])
    w = case.profile["workflows"][0]
    plan = workflow_plan(
        case.sources[w["source"]],
        repository=case.profile["subject"]["repository"],
        branch=case.profile["subject"]["branch"],
        inputs={},
    )[0]
    job = copy.deepcopy(
        case.data["repos/example/fixture/actions/runs/2000/attempts/1/jobs"]["jobs"][0]
    )
    raw = case.data["repos/example/fixture/actions/jobs/7000/logs"]
    if drift == "source_command":
        plan["steps"][1]["source"]["run"] = "python -m pytest another.py"
    elif drift == "input_group":
        raw = raw.replace(b"with:\n", b"env:\n")
    elif drift == "declared_env":
        raw = raw.replace(b"OWNED_CI: 1", b"OWNED_CI: 0")
    elif drift == "action":
        raw = raw.replace(b"(SHA:" + b"b" * 40 + b")", b"(SHA:" + b"8" * 40 + b")")
    elif drift == "checkout":
        raw = raw.replace(b"\n" + b"a" * 40 + b"\n", b"\n" + b"8" * 40 + b"\n")
    elif drift == "step":
        job["steps"][2]["conclusion"] = "skipped"
    elif drift == "post":
        job["steps"].pop(3)
    else:
        raw = raw.replace(b"2 passed", b"0 passed")
    # Even an independently supplied raw hash cannot mint a different parser,
    # selector, interpreter or completed step/report. Production additionally
    # requires the immutable compiled profile, so this is a lower-level test.
    import hashlib

    pin["log_sha256"] = hashlib.sha256(raw).hexdigest()
    pin["log_bytes"] = len(raw)
    pin["steps_sha256"] = digest(job["steps"])
    with pytest.raises(ValueError):
        executed_log(raw, job, plan, pin, head="a" * 40)
