"""Synthetic GitHub readbacks exercise the real deferral adapter and durable store."""

import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from domain.helpers import contract
from test_subagent_lifecycle import staged_candidate

from devflow.adapters.github import GitHubRepository
from devflow.deferrals import defer_finding
from devflow.errors import WorkflowError
from devflow.validation import canonical_json

URL = "https://github.com/synthetic/fixture/issues/23"
API = "https://api.github.com/repos/synthetic/fixture"


def issue(**changes):
    return {"number": 23, "id": 23001, "node_id": "I_synthetic23", "state": "open",
            "html_url": URL, "url": API + "/issues/23", "repository_url": API,
            "body": "SYNTHETIC PRIVATE ISSUE BODY", "title": "SYNTHETIC PRIVATE TITLE", **changes}


@pytest.fixture
def scenario(tmp_path):
    s, _ = staged_candidate(tmp_path, tier=0)
    s.finding(severity="medium")
    return s


def request(s, **changes):
    return {"work_id": s.work_id, "operation_id": f"defer-{next(s.sequence)}",
            "expected_revision": s.state["revision"], "finding_id": "finding-1",
            "followup_reference": URL, "rationale": "Synthetic bounded follow-up", **changes}


def transport(data, calls, *, status=200):
    def runner(argv, **kwargs):
        calls.append(argv)
        return SimpleNamespace(returncode=0 if status == 200 else 1,
                               stdout=f"HTTP/2 {status}\n\n" + json.dumps(data))

    return lambda owner, name: GitHubRepository(owner, name, runner=runner, sleep=lambda _: None)


def test_open_issue_readback_uses_existing_transport_and_only_retains_allowlisted_identity(scenario):
    s, calls = scenario, []
    defer_finding(s.service, request(s), github_factory=transport(issue(), calls))
    assert len(calls) == 1
    assert calls[0][:6] == ["gh", "api", "--include", "--method", "GET", "repos/synthetic/fixture/issues/23"]
    finding = s.state["findings"]["finding-1"]
    assert finding["disposition"] == "deferred"
    observation = finding["followup_observation"]
    assert observation["issue_node_id"] == "I_synthetic23" and observation["issue_id"] == 23001
    assert observation["reference"] == URL and observation["state"] == "open"
    persisted = canonical_json(s.state)
    assert "SYNTHETIC PRIVATE ISSUE BODY" not in persisted
    assert "SYNTHETIC PRIVATE TITLE" not in persisted
    assert any(r.get("followup_observation") == observation for key, r in s.state["records"].items()
               if key.startswith("finding_event:"))


@pytest.mark.parametrize("data", [
    issue(state="closed"), issue(pull_request={"url": "synthetic:pull-request"}),
    issue(number=24), issue(html_url=URL + "0"),
    issue(url=API + "/issues/24"), issue(repository_url=API + "-other"),
    issue(node_id=None), issue(id=None),
])
def test_issue_deferral_rejects_closed_pr_unrelated_or_unstable_readback(scenario, data):
    s, calls = scenario, []
    before = s.state["revision"]
    with pytest.raises(WorkflowError, match="exact open GitHub issue"):
        defer_finding(s.service, request(s), github_factory=transport(data, calls))
    assert s.state["revision"] == before and s.state["findings"]["finding-1"]["disposition"] == "open"


def test_missing_issue_does_not_create_a_deferral(scenario):
    s, calls = scenario, []
    with pytest.raises(WorkflowError):
        defer_finding(s.service, request(s), github_factory=transport({"message": "Not Found"}, calls, status=404))
    assert len(calls) == 1 and s.state["findings"]["finding-1"]["disposition"] == "open"


@pytest.mark.parametrize("reference", [
    "https://example.com/synthetic/fixture/issues/23", "http://github.com/synthetic/fixture/issues/23",
    "https://github.com/synthetic/fixture/pull/23", URL + "?token=private", URL + "#comment",
])
def test_unsupported_reference_never_reaches_transport(scenario, reference):
    def forbidden(*args):
        pytest.fail("Unsupported reference must not reach any transport")

    with pytest.raises(WorkflowError, match="canonical GitHub issue"):
        defer_finding(scenario.service, request(scenario, followup_reference=reference), github_factory=forbidden)


def test_forged_request_observation_cannot_replace_adapter_readback(scenario):
    s = scenario
    payload = request(s, followup_observation={"kind": "github_issue", "state": "open"})
    with pytest.raises(WorkflowError, match="adapter, not request JSON"):
        defer_finding(s.service, payload)
    # Calling the service directly cannot make user JSON into the separate adapter observation.
    with pytest.raises(WorkflowError, match="exact open GitHub issue"):
        s.service.execute("finding.defer", payload)
    assert s.state["findings"]["finding-1"]["disposition"] == "open"


def test_domain_rechecks_adapter_observation_binding(scenario):
    s, calls = scenario, []
    defer_finding(s.service, request(s), github_factory=transport(issue(), calls))
    observation = s.state["findings"]["finding-1"]["followup_observation"]
    with pytest.raises(WorkflowError, match="exact open GitHub issue"):
        s.service.execute("finding.defer", request(s),
                          deferral_observation=observation | {"issue_number": 24})
    with pytest.raises(WorkflowError, match="stable identity"):
        defer_finding(s.service, request(s), github_factory=transport(issue(node_id="I_replaced"), calls))


def add_followup(s):
    s.call("work.ready", work_id="synthetic-followup", expected_revision=0,
           record=contract("synthetic-followup"), user_request={
               "reference": "synthetic:follow-up-request", "summary": "Synthetic follow-up",
               "allowed_operations": ["edit", "check", "create_tasks"],
           })


def test_known_unresolved_work_is_a_durable_followup_without_network(scenario):
    s = scenario
    add_followup(s)
    with patch("devflow.adapters.github.GitHubRepository.issue", side_effect=AssertionError("No issue read needed")):
        s.cli("finding defer", finding_id="finding-1", related_work_id="synthetic-followup",
              rationale="Owned unresolved follow-up")
    finding = s.state["findings"]["finding-1"]
    assert finding["followup_reference"] == "work:synthetic-followup"
    assert finding["followup_observation"]["lifecycle"] == "ready"


@pytest.mark.parametrize("identity,lifecycle", [
    ("missing", None), ("synthetic-subagents", None),
    ("synthetic-followup", "canceled"), ("synthetic-followup", "done"),
])
def test_missing_self_canceled_or_completed_work_cannot_be_followup(scenario, identity, lifecycle):
    s = scenario
    if lifecycle:
        add_followup(s)
        # Synthetic archived state isolates admission from unrelated completion machinery.
        with s.service.store.transaction() as db:
            row = db.execute("SELECT state FROM works WHERE work_id=?", (identity,)).fetchone()
            state = json.loads(row[0])
            state["lifecycle"] = lifecycle
            db.execute("UPDATE works SET state=? WHERE work_id=?", (canonical_json(state), identity))
    with pytest.raises(WorkflowError, match="must exist, remain unresolved"):
        s.call("finding.defer", finding_id="finding-1", related_work_id=identity,
               rationale="Synthetic follow-up")
    assert s.state["findings"]["finding-1"]["disposition"] == "open"


def test_cli_performs_the_readback_before_recording_an_issue_deferral(scenario):
    with patch("devflow.adapters.github.GitHubRepository.issue", return_value=issue()) as observed:
        scenario.cli("finding defer", finding_id="finding-1", followup_reference=URL,
                     rationale="Synthetic CLI follow-up")
    observed.assert_called_once_with(23)
    assert scenario.state["findings"]["finding-1"]["followup_observation"]["issue_node_id"] == "I_synthetic23"


def test_identical_operation_replays_saved_readback_without_refetching(scenario):
    s, calls = scenario, []
    payload = request(s)
    original = defer_finding(s.service, payload, github_factory=transport(issue(), calls))
    assert defer_finding(s.service, payload, github_factory=transport(issue(state="closed"), calls)) == original
    assert len(calls) == 1
    with pytest.raises(WorkflowError, match="different payload"):
        defer_finding(s.service, payload | {"rationale": "Changed retry"},
                      github_factory=transport(issue(), calls))
