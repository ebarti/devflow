"""One immutable candidate yields one repair batch across independent assessments."""
from __future__ import annotations

import asyncio
from copy import deepcopy

import pytest
from temporalio import workflow

from devflow_temporal.delivery_workflow import DeliveryWorkflow


def exercise(monkeypatch, *, enabled=True, failed="all", unsafe=None):
    candidate = {"id": "candidate", "head": "head"}
    spec = {"run_id": "batch", "provider": "fake", "assessment_batch_version": int(enabled),
            "policy": {"max_repairs": 1, "browser_qa": {"id": "browser"}}}
    flow = DeliveryWorkflow()
    flow.post_publish_v1 = True
    flow.state = {"run_id": "batch", "phase": "published", "execution_state": "running",
                  "revision": 1, "iteration": 0, "candidate_revision": 1,
                  "candidate": candidate, "pull_request": {"number": 1, "candidate": candidate},
                  "checks": {}, "roles": [], "usage": {}, "findings": [], "cleanup": "confirmed"}
    calls, repairs = [], []
    monkeypatch.setattr(workflow, "patched", lambda _name: True)
    monkeypatch.setattr(flow, "_planning_request", lambda _spec: {})

    async def project(*_args, **_kwargs):
        pass

    async def execute(name, body, **_kwargs):
        stage = body.get("role", {"delivery_checks": "checks",
                                  "delivery_browser_qa": "browser"}.get(name, name))
        calls.append(stage)
        if stage == "implement":
            assert body["iteration"] == 1
            assert body["resume_session"] == "original-author"
            repairs.append(deepcopy(body["findings"]))
            return {"status": "blocked", "candidate": candidate, "session_id": "original-author"}
        if stage == "checks":
            passed = failed not in {"all", "checks"}
            return {"state": "passed" if passed else "failed", "candidate_id": candidate["id"],
                    "cleanup": "unknown" if unsafe == "cleanup" else "confirmed",
                    "source_unchanged": unsafe != "source", "results": [
                        {"id": "unit", "argv": ["pytest"], "passed": passed,
                         "diagnostic": "local finding", "exit_code": int(not passed)}]}
        if stage == "browser":
            return {"state": "failed" if failed in {"all", "browser"} else "passed",
                    "cleanup": "confirmed", "exit_code": 1, "diagnostic": "browser finding",
                    "receipt": "qa.json", "receipt_sha256": "qa-hash",
                    "log": "qa.log", "log_sha256": "log-hash"}
        assert stage in {"review", "verify"}
        if failed in {'prelaunch', 'review-prelaunch'} and (
                failed == 'prelaunch' or stage == 'review'):
            return {'status': 'blocked', 'candidate': candidate, 'session_id': None,
                    'cleanup': 'confirmed', 'finish_reason': 'prelaunch',
                    'findings': ['controller evidence could not be prepared']}
        return {"status": "findings" if failed in {"all", stage} else "pass",
                "candidate": ({"id": "changed"} if unsafe == "review-candidate"
                              and stage == "review" else candidate),
                "session_id": stage, "cleanup": "confirmed", "summary": stage,
                "findings": [stage + " finding"]}

    monkeypatch.setattr(flow, "_project", project)
    monkeypatch.setattr(flow, "_activity", execute)
    state = asyncio.run(flow._run_iterations(
        spec, start_iteration=0, prior_implementer_session="original-author",
        repair_findings=[], continuation=None, recovery=None, published_checkpoint=True,
    ))
    return calls, repairs, state


@pytest.mark.parametrize("failed", ["all", "checks", "browser", "review", "verify"])
def test_every_independent_assessment_finishes_before_single_repair(monkeypatch, failed):
    calls, repairs, state = exercise(monkeypatch, failed=failed)
    assert calls == ["checks", "browser", "review", "verify", "implement"]
    assert len(repairs) == 1
    if failed == "all":
        batch = repairs[0]
        assert len(batch) == 4
        assert "local_checks" in batch[0] and "browser_qa" in batch[1]
        assert batch[2:] == ["review finding", "verify finding"]
        assert state["repair_findings"] == {"iteration": 0, "findings": batch}


@pytest.mark.parametrize("unsafe", ["cleanup", "source", "review-candidate"])
def test_unsafe_custody_stops_collection_without_a_repair_debit(monkeypatch, unsafe):
    calls, repairs, state = exercise(monkeypatch, unsafe=unsafe)
    assert not repairs
    assert state["outcome"] == "blocked"
    assert calls == (["checks", "browser", "review"] if unsafe == "review-candidate"
                     else ["checks"])


def test_recorded_legacy_input_keeps_first_failure_order(monkeypatch):
    calls, repairs, _state = exercise(monkeypatch, enabled=False)
    assert calls == ["checks", "implement"]
    assert len(repairs[0]) == 1


@pytest.mark.parametrize('failed', ['prelaunch', 'review-prelaunch'])
def test_prelaunch_failures_finish_independent_collection_without_a_coding_turn(monkeypatch, failed):
    calls, repairs, state = exercise(monkeypatch, failed=failed)
    assert calls == ['checks', 'browser', 'review', 'verify']
    assert repairs == []
    assert state['outcome'] == 'blocked'
    assert state['error'] == 'independent assessment launch failed before provider'
