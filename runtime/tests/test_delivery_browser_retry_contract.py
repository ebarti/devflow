"""Browser setup accounting may stay deferred only with a bounded usable retry."""

import json
from copy import deepcopy

import pytest
from test_delivery_gate_retry import stopped as stopped
from test_delivery_metadata_recovery import published as published
from test_delivery_store import service as service

from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_repair import failed_gate_diagnostics


@pytest.fixture(autouse=True)
def modern_budget(monkeypatch):
    # Exercise the existing fixed budget, not the legacy repair-extension path.
    monkeypatch.setattr(
        "test_delivery_metadata_recovery.submit_historical_admission",
        lambda store, request, _patch: store.submit(request),
    )


def browser_setup_stop(stopped):
    store, broker, state, closed, request = stopped
    state["roles"] = [role for role in state["roles"] if role["role"] != "verify"]
    state["checks"] = {
        "browser_qa": {
            "state": "failed",
            "candidate_id": state["candidate"]["id"],
            "source_unchanged": True,
            "cleanup": "confirmed",
            "exit_code": 1,
            "test_count": 0,
            "rejected_output": False,
            "diagnostic": "pnpm install failed before browser tests: registry unavailable",
        }
    }
    with store._connect() as db:
        db.execute("DELETE FROM delivery_attempts WHERE job_key='old-qa'")
        db.execute(
            "UPDATE delivery_runs SET checks_json=? WHERE run_id='run-1'",
            (canonical_json(state["checks"]),),
        )
    return store, broker, state, request


def test_browser_setup_reason_and_bounded_retry_survive_exhausted_budget(stopped):
    store, broker, state, request = browser_setup_stop(stopped)
    original = deepcopy(state)
    spec = store.spec("run-1")
    assert spec["retry_budget_version"] == 1
    findings = failed_gate_diagnostics(state, spec)
    assert "pnpm install failed before browser tests" in findings[0]
    assert "browser_qa" in findings[0]
    before = broker.candidate()
    assert store.repair_admission_preflight("run-1", request)["additional_iterations"] == 0
    admitted = store.continue_repair("run-1", request)
    assert admitted["implementation_authority"] is False
    assert store.continue_repair("run-1", request) == admitted
    assert broker.candidate() == before
    assert store.effective_spec("run-1")["policy"]["max_repairs"] == spec["policy"]["max_repairs"]
    with store._connect() as db:
        recovery = json.loads(
            db.execute("SELECT recovery_json FROM delivery_gate_admissions").fetchone()[0]
        )
        assert recovery["state"] == original
        assert not db.execute("SELECT 1 FROM delivery_repair_grants").fetchone()
    with pytest.raises(ValueError):
        store.continue_repair("run-1", {**request, "command_id": "another-browser-retry"})


@pytest.mark.parametrize("drift", ["candidate_id", "source_unchanged", "cleanup", "projection"])
def test_browser_retry_requires_the_same_closed_candidate_and_cleanup(stopped, drift):
    store, _, state, request = browser_setup_stop(stopped)
    browser = state["checks"]["browser_qa"]
    if drift == "candidate_id":
        browser[drift] = "another-candidate"
    elif drift == "source_unchanged":
        browser[drift] = False
    elif drift == "cleanup":
        browser[drift] = "unknown"
    else:
        browser["diagnostic"] = "changed after projection"
    if drift != "projection":
        with store._connect() as db:
            db.execute(
                "UPDATE delivery_runs SET checks_json=? WHERE run_id='run-1'",
                (canonical_json(state["checks"]),),
            )
    with pytest.raises(ValueError, match="closed, finalized failed gate"):
        store.continue_repair("run-1", request)
