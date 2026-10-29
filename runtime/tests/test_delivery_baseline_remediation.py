"""Pure contract and workflow scheduling probes; no native roles or servers."""
from __future__ import annotations

from copy import deepcopy

import pytest
from test_delivery_intake import intake_fixture as intake_fixture

from devflow_temporal.delivery_workflow import DeliveryWorkflow


def evidence():
    recipe = {"id": "scripts", "kind": "check", "argv": ["pnpm", "scripts:test"]}
    spec = {
        "run_id": "fixture", "provider": "codex", "base_sha": "immutable-base",
        "baseline_checks_version": 2, "intake_required": True, "plan_approval": "automatic",
        "command_id": "submit", "request_digest": "request", "policy_digest": "policy",
        "authorized_endpoint": "published_unmerged", "policy": {
            "baseline_checks": [recipe], "prepublish_checks": [recipe], "checks": [recipe],
            "max_repairs": 0,
        },
    }
    result = {
        "state": "failed", "base_sha": spec["base_sha"], "source_unchanged": True,
        "feature_unchanged": True, "candidate_id": "baseline",
        "baseline_candidate": {"id": "baseline", "head": spec["base_sha"]},
        "results": [{
            "id": "scripts", "passed": False, "exit_code": 1, "test_count": 162,
            "cleanup": "confirmed", "rejected_output": False, "rejection_causes": [],
            "log": "/evidence/scripts.log", "log_sha256": "a" * 64,
            "native_process": {
                "state": "finished", "exit_code": 1, "timed_out": False, "cancelled": False,
                "monitoring_complete": True, "stdio_drained": True,
                "cleanup": "observed-native-confirmed",
            },
        }],
    }
    return spec, result


class ProbeWorkflow(DeliveryWorkflow):
    def __init__(self, baseline, *, final_failure="prepublish", cancel=False):
        super().__init__()
        self.baseline = baseline
        self.final_failure = final_failure
        self.cancel_after_baseline = cancel
        self.calls = []
        self.events = []

    async def _project(self, spec, event, _label):
        self.events.append(event)

    async def _start_tracker(self, spec):
        return {"state": "consistent"}

    async def _stop(self, spec, reason, **_kwargs):
        self.state.update(outcome="blocked", error=reason)
        return self.state

    async def _cancelled(self, spec):
        self.state.update(outcome="cancelled")
        return self.state

    async def _activity(self, name, payload):
        self.calls.append((name, deepcopy(payload)))
        candidate = {"id": "feature", "head": "feature-head"}
        if name == "delivery_prepare":
            return {"candidate": candidate}
        if name == "delivery_baseline_checks":
            self.cancel_requested = self.cancel_after_baseline
            return self.baseline
        if name == "delivery_intake":
            assert payload["intake"]["baseline_findings"]
            return {"status": "plan", "cleanup": "confirmed", "plan": {
                "scope": "Implement issue and repair recorded regression",
                "steps": ["Repair owning code"], "verification": ["Run original checks"],
                "acceptance": ["Issue complete, every check passed"],
            }}
        if name == "delivery_accept_plan":
            return {**payload["spec"], "accepted_plan": "Native intake plan"}
        if name == "delivery_role":
            assert payload["role"] == "implement", "failed checks must prevent review/QA"
            assert payload["findings"]
            return {"status": "pass", "candidate": candidate, "session_id": "original"}
        if name == "delivery_precheck":
            return {"state": "failed" if self.final_failure == "prepublish" else "passed",
                    "cleanup": "confirmed", "results": [{"id": "scripts", "passed": False}]}
        if name == "delivery_publish":
            assert self.final_failure == "local"
            return {"candidate": candidate, "head": candidate["head"], "number": 1}
        if name == "delivery_checks":
            return {"state": "failed", "cleanup": "confirmed",
                    "results": [{"id": "scripts", "passed": False}]}
        raise AssertionError(f"unexpected effect: {name}")


@pytest.mark.asyncio
@pytest.mark.parametrize("final_failure", ["prepublish", "local"])
async def test_real_workflow_diagnoses_then_still_requires_final_checks(monkeypatch, final_failure):
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", lambda _: True)
    spec, baseline = evidence()
    probe = ProbeWorkflow(baseline, final_failure=final_failure)
    state = await probe.run(spec)
    calls = [name for name, _ in probe.calls]
    assert calls[:4] == ["delivery_prepare", "delivery_baseline_checks", "delivery_intake",
                         "delivery_accept_plan"]
    last = "delivery_precheck" if final_failure == "prepublish" else "delivery_checks"
    assert calls[-1] == last
    assert state["outcome"] == "blocked"
    assert state["checks"]["baseline"] == baseline
    assert "baseline_diagnosed" in probe.events and "baseline_passed" not in probe.events
    assert state["iteration"] == 0
    assert "delivery_ci" not in calls
    if final_failure == "prepublish":
        assert "delivery_publish" not in calls


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ["legacy", "cancel"])
async def test_real_workflow_retains_legacy_unsafe_and_cancel_stops(scenario):
    spec, baseline = evidence()
    if scenario == "legacy":
        spec["baseline_checks_version"] = 1
    if scenario == "timeout":
        baseline["results"][0]["native_process"]["timed_out"] = True
    probe = ProbeWorkflow(baseline, cancel=scenario == "cancel")
    state = await probe.run(spec)
    assert [name for name, _ in probe.calls] == ["delivery_prepare", "delivery_baseline_checks"]
    assert state["outcome"] == ("cancelled" if scenario == "cancel" else "blocked")
    assert state["roles"] == []
    assert state["checks"]["baseline"] == baseline
