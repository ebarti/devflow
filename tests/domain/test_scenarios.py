import pytest
from helpers import NOW, Scenario, record

from devflow.errors import WorkflowError

NAMED = "foreign-directory-sentinel"


def test_all_acceptance_covered_does_not_prove_a_named_qa_scenario(tmp_path):
    s = Scenario(tmp_path, tier=2, scenarios=[NAMED])
    s.candidate()
    s.check()
    s.assignment("qa")
    with pytest.raises(WorkflowError, match="every required scenario"):
        s.gate("qa")
    assert s.state["gate_ids"] == {}


def test_explicit_executed_named_scenario_satisfies_qa_and_delivery(tmp_path):
    s = Scenario(tmp_path, tier=2, scenarios=[NAMED])
    s.candidate()
    s.check(scenario_ids=[NAMED])
    for role in ("review", "qa"):
        s.assignment(role)
        s.gate(role)
    assert s.deliver()["lifecycle"] == "done"


def test_independent_manual_observation_can_prove_work_specific_scenario(tmp_path):
    s = Scenario(tmp_path, tier=2, scenarios=[NAMED])
    s.candidate()
    s.check()
    s.assignment("review")
    s.gate("review")
    s.assignment("qa")
    observation = record(
        "observation_evidence",
        evidence_id="qa-observation",
        candidate_id="candidate-1",
        producer_task_id="synthetic-qa",
        acceptance_ids=["A01"],
        scenario_ids=[NAMED],
        source_references=["synthetic:owned-sentinel-fixture"],
        observations=["The unrelated synthetic sentinel remained after rejected cleanup"],
        artifact_hash=s.service.put_artifact(b"Synthetic independent QA sentinel observation"),
        observed_at=NOW,
    )
    s.call("evidence.record", record=observation)
    s.gate("qa", evidence_ids=["check-1", "qa-observation"])
    assert s.deliver()["lifecycle"] == "done"


def test_delivery_without_qa_still_requires_named_scenario_execution(tmp_path):
    s = Scenario(tmp_path, scenarios=[NAMED])
    s.candidate()
    s.check()
    next_action = s.service.next(s.work_id)["actions"][0]
    assert next_action["kind"] == "run_check"
    assert next_action["scenario_ids"] == [NAMED]
    with pytest.raises(WorkflowError, match="incomplete"):
        s.call("deliver")
    assert not any(a["operation"] == "local_delivery" for a in s.state["actions"].values())


def test_later_failed_or_narrower_check_cannot_reuse_old_scenario_coverage(tmp_path):
    s = Scenario(tmp_path, scenarios=[NAMED])
    s.candidate()
    old = s.check(scenario_ids=[NAMED])
    s.check("later-failure", status="FAIL", scenario_ids=[NAMED])
    with pytest.raises(WorkflowError, match="incomplete"):
        s.call("deliver")
    # Even after another check passes every acceptance, the old named scenario
    # cannot be taken from a superseded result for the same recipe.
    s.check("later-narrow-pass", scenario_ids=[])
    s.call("check.record", record=old)
    assert s.service.next(s.work_id)["actions"][0]["scenario_ids"] == [NAMED]
    with pytest.raises(WorkflowError, match="incomplete"):
        s.call("deliver")


def test_qa_cannot_cite_superseded_scenario_evidence(tmp_path):
    s = Scenario(tmp_path, tier=2, scenarios=[NAMED])
    s.candidate()
    s.check(scenario_ids=[NAMED])
    s.check("new-check", scenario_ids=[])
    s.assignment("qa")
    with pytest.raises(WorkflowError, match="superseded"):
        s.gate("qa", evidence_ids=["check-1"])
