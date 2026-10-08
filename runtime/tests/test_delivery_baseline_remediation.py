"""Pure contract and workflow scheduling probes; no native roles or servers."""
from __future__ import annotations

import hashlib
import json
from copy import deepcopy

import pytest
from test_delivery_intake import intake_fixture as intake_fixture

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_baseline_contract import repairable_baseline
from devflow_temporal.delivery_config import DeliveryConfig, scope_amended_spec
from devflow_temporal.delivery_resources import _gate_path
from devflow_temporal.delivery_store import DeliveryStore
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


def test_only_fresh_intake_authority_can_diagnose_observed_failure():
    spec, result = evidence()
    assert repairable_baseline(spec, result)
    original = deepcopy((spec, result))
    for key, value in (("baseline_checks_version", 1), ("baseline_checks_version", True),
                       ("intake_required", False)):
        changed = {**spec, key: value}
        assert not repairable_baseline(changed, result)
    assert (spec, result) == original


@pytest.mark.parametrize("stage", ["prepublish_checks", "checks"])
@pytest.mark.parametrize("change", ["remove", "lower", "duplicate"])
def test_original_recipe_is_mandatory_at_both_final_boundaries(stage, change):
    spec, result = evidence()
    if change == "remove":
        spec["policy"][stage] = []
    elif change == "lower":
        spec["policy"][stage] = [{**spec["policy"][stage][0], "argv": ["true"]}]
    else:
        spec["policy"][stage] *= 2
    assert not repairable_baseline(spec, result)


@pytest.mark.parametrize("field,value", [
    ("cleanup", "unknown"), ("passed", 0), ("exit_code", -9), ("exit_code", True),
    ("exit_code", 0), ("test_count", None), ("test_count", 0), ("test_count", True),
    ("failure_kind", "preparation"), ("evidence_failure", "unowned artifact"),
    ("launched", False), ("rejected_output", True),
    ("rejection_causes", [{}]), ("log", ""), ("log_sha256", "unauthenticated"),
    ("argv", [1]), ("argv", "pnpm scripts:test"),
])
def test_uncertain_or_prerequisite_failure_blocks(field, value):
    spec, result = evidence()
    result["results"][0][field] = value
    assert not repairable_baseline(spec, result)


@pytest.mark.parametrize("field,value", [
    ("state", "running"), ("exit_code", 0), ("exit_code", True),
    ("timed_out", True), ("cancelled", True), ("monitoring_complete", 1),
    ("stdio_drained", False), ("cleanup", "unknown"),
])
def test_process_receipt_must_confirm_actual_finished_cleanup(field, value):
    spec, result = evidence()
    result["results"][0]["native_process"][field] = value
    assert not repairable_baseline(spec, result)


@pytest.mark.parametrize("field,value", [
    ("source_unchanged", False), ("feature_unchanged", False), ("base_sha", "other"),
    ("candidate_id", "other"), ("baseline_candidate", None), ("results", None),
    ("results", [{}]), ("results", []),
])
def test_candidate_or_evidence_mismatch_blocks(field, value):
    spec, result = evidence()
    result[field] = value
    assert not repairable_baseline(spec, result)


def test_duplicate_missing_and_extra_failed_commands_block():
    spec, result = evidence()
    result["results"] *= 2
    assert not repairable_baseline(spec, result)
    spec, result = evidence()
    result["results"].append({**deepcopy(result["results"][0]), "id": "installer"})
    assert not repairable_baseline(spec, result)
    spec, result = evidence()
    result["results"][0]["id"] = "other"
    assert not repairable_baseline(spec, result)



def test_fail_fast_observation_preserves_unexecuted_final_recipes():
    spec, result = evidence()
    later = {"id": "docs", "kind": "check", "argv": ["pnpm", "docs:build"]}
    for stage in ("baseline_checks", "prepublish_checks", "checks"):
        spec["policy"][stage] = [*spec["policy"][stage], later]
    assert repairable_baseline(spec, result)
    assert "docs" not in {row["id"] for row in result["results"]}
    assert later in spec["policy"]["checks"]
    # An observed command after an omitted earlier one is not a fail-fast prefix.
    for stage in ("baseline_checks", "prepublish_checks", "checks"):
        spec["policy"][stage].reverse()
    assert not repairable_baseline(spec, result)


@pytest.mark.parametrize("argv", [
    ["corepack", "pnpm", "install", "--offline"],
    ["uv", "--project", "worker", "sync", "--locked"],
    ["uv", "run", "python", "-m", "playwright", "install", "chromium"],
])
def test_executed_prerequisite_cannot_be_a_feature_repair(argv):
    spec, result = evidence()
    for stage in ("baseline_checks", "prepublish_checks", "checks"):
        spec["policy"][stage][0]["argv"] = argv
    result["results"][0]["argv"] = argv
    assert not repairable_baseline(spec, result)

def test_passed_prerequisites_must_also_have_observed_cleanup():
    spec, result = evidence()
    prerequisite = deepcopy(result["results"][0])
    prerequisite.update(id="install", passed=True, exit_code=0, test_count=None)
    prerequisite["native_process"]["exit_code"] = 0
    result["results"].insert(0, prerequisite)
    assert repairable_baseline(spec, result)
    prerequisite["native_process"]["monitoring_complete"] = False
    assert not repairable_baseline(spec, result)


@pytest.mark.parametrize("policy", [None, [], {"baseline_checks": [None]},
                                     {"baseline_checks": [{"id": []}]}])
def test_malformed_policy_fails_closed(policy):
    spec, result = evidence()
    spec["policy"] = policy
    assert not repairable_baseline(spec, result)


def test_raw_admission_uses_new_behavior_without_rewriting_legacy(intake_fixture):
    path, request = intake_fixture
    config = DeliveryConfig.load(path)
    recipe = evidence()[0]["policy"]["baseline_checks"][0]
    config.raw["repositories"]["fixture"].update(
        baseline_check_ids=[recipe["id"]], prepublish_checks=[recipe], checks=[recipe],
    )
    path.write_text(json.dumps(config.raw))
    config = DeliveryConfig.load(path)
    store = DeliveryStore(config)
    store.submit(request)
    fresh = store.spec(request["run_id"])
    legacy = config.admit({**request, "accepted_plan": "Already accepted plan"})
    assert fresh["baseline_checks_version"] == 2
    assert legacy["baseline_checks_version"] == 1
    assert fresh["policy"]["baseline_checks"] == fresh["policy"]["checks"]
    assert _gate_path(fresh, "baseline", 0).name == "baseline"
    with pytest.raises(ValueError, match="baseline"):
        _gate_path(fresh, "baseline", 1)


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
@pytest.mark.parametrize("scenario", ["legacy", "timeout", "cancel"])
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


@pytest.mark.parametrize("version", [None, 1, 2])
def test_scope_delta_never_upgrades_frozen_baseline_behavior(intake_fixture, version):
    path, request = intake_fixture
    raw = json.loads(path.read_text())
    recipe = evidence()[0]["policy"]["baseline_checks"][0]
    raw["repositories"]["fixture"].update(
        baseline_check_ids=[recipe["id"]], prepublish_checks=[recipe], checks=[recipe],
    )
    path.write_text(json.dumps(raw))
    config = DeliveryConfig.load(path)
    config.state_root.mkdir(mode=0o700, parents=True)
    original = config.admit(request)
    original["accepted_plan"] = "Original accepted raw-goal plan"
    original["request_digest"] = digest(request)
    if version is None:
        original.pop("baseline_checks_version")
    else:
        original["baseline_checks_version"] = version
    updated = deepcopy(raw)
    updated["repositories"]["fixture"]["allowed_paths"].append("tests/fixture.py")
    amendment = config.state_root / "amendment.json"
    amendment.write_text(json.dumps(updated))
    amendment.chmod(0o600)
    before = deepcopy(original)
    effective = scope_amended_spec(
        original, amendment, hashlib.sha256(amendment.read_bytes()).hexdigest(),
        ["tests/fixture.py"],
    )
    assert effective.get("baseline_checks_version") == version
    assert ("baseline_checks_version" in effective) == (version is not None)
    assert effective["request_digest"] == original["request_digest"]
    assert effective["accepted_plan"] == original["accepted_plan"]
    assert original == before
