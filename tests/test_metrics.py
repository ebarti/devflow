from copy import deepcopy
from decimal import Decimal

import pytest

from devflow.errors import WorkflowError
from devflow.metrics import quality_report


def event(event_id, kind, day, details=None):
    return {
        "record_type": "outcome_event",
        "event_id": event_id,
        "event_kind": kind,
        "occurred_at": f"2026-08-{day:02d}T00:00:00Z",
        "details": details or {},
    }


def defect(identity, *, detector="user", stage="review", attribution="confirmed", known=False):
    return {
        "defect_id": identity,
        "detector": detector,
        "stage": stage,
        "attribution": attribution,
        "origin_candidate_id": "C-old" if attribution == "confirmed" else None,
        "severity": "high",
        "first_report": not known,
        "known_before_user_encounter": known,
    }


def work(events, *, identity="W-1", kind="feature"):
    return {
        "work_id": identity,
        "contract": {"kind": kind},
        "records": {e["event_id"]: e for e in events},
        "phase_history": [],
    }


def test_user_review_escape_counts_before_merge_and_cannot_dilute_with_editorial():
    item = work(
        [
            event("H", "first_ready_handoff", 1),
            event("D", "defect_confirmed", 2, defect("F-1")),
            event("again", "defect_confirmed", 3, defect("F-1")),
        ]
    )
    editorial = work([event("H2", "first_ready_handoff", 1)], identity="W-2", kind="editorial")
    report = quality_report([item, editorial], cutoff="2026-09-09T00:00:00Z")
    assert report["confirmed_defects"] == 1
    assert report["mature_implementation_items"] == 1
    assert report["user_found_after_handoff_rate_30_days"] == Decimal(100)
    assert report["works"][0]["post_exposure_first_discoveries"] == 0


def test_internal_catch_unknown_origin_and_known_user_encounter_stay_distinct():
    item = work(
        [
            event("I", "defect_confirmed", 1, defect("F-1", detector="qa", stage="before_handoff")),
            event("H", "first_ready_handoff", 2),
            event("U", "defect_confirmed", 3, defect("F-2", attribution="unknown")),
            event("K", "defect_confirmed", 4, defect("F-1", known=True)),
        ]
    )
    report = quality_report([item], cutoff="2026-09-09T00:00:00Z")
    assert report["confirmed_defects"] == 2
    assert report["user_first_discoveries"] == 1
    assert report["known_defects_encountered_by_user"] == 1
    assert report["unknown_origin_defects"] == 1
    assert report["user_found_after_handoff_rate_30_days"] == 0


def test_immature_window_is_undefined_and_future_observation_excluded():
    item = work(
        [
            event("H", "first_ready_handoff", 1),
            event("F", "defect_confirmed", 20, defect("F-later")),
        ]
    )
    report = quality_report([item], cutoff="2026-08-10T00:00:00Z")
    assert report["mature_implementation_items"] == 0
    assert report["user_found_after_handoff_rate_30_days"] is None
    assert report["confirmed_defects"] == 0


def test_parallel_verification_does_not_double_elapsed_time():
    item = work([])
    item["phase_history"] = [
        {
            "phase": "verify",
            "started_at": "2026-08-01T00:00:00Z",
            "ended_at": "2026-08-01T00:20:00Z",
        },
        {
            "phase": "verify",
            "started_at": "2026-08-01T00:10:00Z",
            "ended_at": "2026-08-01T00:30:00Z",
        },
    ]
    report = quality_report([item], cutoff="2026-09-09T00:00:00Z")
    assert report["works"][0]["phase_elapsed_seconds"]["verify"] == 1800
    assert report["execution_lead_time"]["p50_seconds"] is None
    with pytest.raises(WorkflowError, match="each work outcome once"):
        quality_report([item, deepcopy(item)], cutoff="2026-09-09T00:00:00Z")
