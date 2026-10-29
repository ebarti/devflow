"""Strict neutral public-boundary graphs; no real GitHub execution/eligibility."""

import copy

import pytest
from ci_inventory_fixture import approve, fixture

from devflow_temporal.delivery_ci_inventory_io import GitHubRead
from devflow_temporal.delivery_ci_inventory_observation import collect


def observed(case):
    reader = GitHubRead(
        repositories={p["repository"] for p in case.profile["sources"]}, runner=case.transport
    )
    result = collect(
        reader,
        case.profile,
        required=case.profile["required_ci"],
        expected_tree=case.profile["subject"]["tree"],
    )
    return result, reader


def test_complete_original_inventory_preserves_canonical_omission_and_report_skips(monkeypatch):
    case = fixture()
    approve(monkeypatch, case)
    result, reader = observed(case)
    assert result["state"] == "passed" and len(result["checks"]) == 11
    assert result["canonical_missing_names"] == case.profile["required_ci"]
    assert result["PR_ruleset_eligibility"] is False
    assert result["merge_context_verified"] is False
    assert len(result["workflows"]) == 5 and len(result["jobs"]) == 11
    assert all(x["reports"][0]["report"]["skipped"] == 1 for x in result["jobs"])
    assert all(x["historical_wire_sha256"] is None for x in result["jobs"])
    assert any(b"\x1b[32m" in x["raw"] for x in reader.observations)
    commands = reader.observations
    assert all(x["http"]["status"] == 200 for x in commands)
    assert all(
        "--allow-escape-sequences" in cmd for cmd in case.calls if any("/logs" in v for v in cmd)
    )
    assert all(
        "dispatches" not in " ".join(cmd) and "rerun" not in " ".join(cmd) for cmd in case.calls
    )


@pytest.mark.parametrize("drift", ["run", "job", "suite", "artifact", "check", "canonical"])
def test_mutation_after_observation_is_revalidated_before_success(monkeypatch, drift):
    case = fixture()
    approve(monkeypatch, case)
    changed = []

    def after_last_log(command, case):
        if changed or not any("/actions/jobs/7010/logs" in x for x in command):
            return
        changed.append(True)
        repo, head = case.profile["subject"]["repository"], case.profile["subject"]["head"]
        if drift == "run":
            case.data[f"repos/{repo}/actions/runs/2000"]["run_attempt"] = 2
        elif drift == "job":
            case.data[f"repos/{repo}/actions/runs/2000/attempts/1/jobs"]["jobs"][0]["steps"][2][
                "conclusion"
            ] = "failure"
        elif drift == "suite":
            case.data[f"repos/{repo}/check-suites/3000"]["conclusion"] = "failure"
        elif drift == "artifact":
            case.data[f"repos/{repo}/actions/runs/2000/artifacts"]["artifacts"] = [{"id": 999}]
        elif drift == "check":
            case.data[f"repos/{repo}/commits/{head}/check-runs"]["check_runs"][0]["app"]["id"] = 999
        else:
            case.canonical["name"] = case.profile["required_ci"][0]

    case.callback = after_last_log
    with pytest.raises(ValueError):
        observed(case)
    assert changed


def test_successful_ordinary_execution_inventory_cannot_satisfy_merge_context(
    monkeypatch, tmp_path
):
    from devflow_temporal.delivery_merge import MergeBroker

    case = fixture()
    approve(monkeypatch, case)
    result, _ = observed(case)
    assert result["state"] == "passed" and result["merge_context_verified"] is False
    repo, head = case.profile["subject"]["repository"], case.profile["subject"]["head"]
    base, hosted = "6" * 40, "7" * 40
    broker = object.__new__(MergeBroker)
    broker.spec = {
        "github_repo": repo,
        "base_sha": base,
        "policy": {"required_ci": case.profile["required_ci"]},
    }
    broker.root = tmp_path / "merge"

    def api(endpoint):
        if endpoint == f"repos/{repo}/git/commits/{hosted}":
            return {
                "sha": hosted,
                "tree": {"sha": case.profile["subject"]["tree"]},
                "parents": [{"sha": base}, {"sha": head}],
            }
        if endpoint.endswith("/check-runs?per_page=100"):
            value = copy.deepcopy(case.data[f"repos/{repo}/commits/{head}/check-runs"])
            value["total_count"] = len(value["check_runs"])
            return value
        if "/actions/jobs/" in endpoint:
            identity = int(endpoint.rsplit("/", 1)[1])
            return next(
                j
                for value in case.data.values()
                if isinstance(value, dict)
                for j in value.get("jobs", [])
                if j["id"] == identity
            )
        return copy.deepcopy(case.data[endpoint])

    broker.api = api
    with pytest.raises(ValueError, match="different head, trigger or attempt"):
        broker.live_ci(
            {"head": head},
            {"state": "open", "merged": False, "base": {"sha": base}, "merge_commit_sha": hosted},
            case.profile["subject"]["tree"],
        )
    assert not broker.root.exists()
