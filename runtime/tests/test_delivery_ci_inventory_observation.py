"""Strict neutral public-boundary graphs; no real GitHub execution/eligibility."""

import copy

import pytest
from ci_inventory_fixture import approve, fixture

from devflow_temporal import delivery_ci_inventory_contract as contract
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


def test_caller_cannot_supply_a_different_profile_or_format():
    case = fixture()
    with pytest.raises(ValueError, match="reviewed"):
        contract.approved_profile(case.profile)
    assert contract.APPROVED_PROFILE_SHA256 != "0" * 64


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


@pytest.mark.parametrize(
    "drift",
    [
        "actor",
        "repository",
        "pr_head",
        "draft",
        "run_head",
        "run_actor",
        "event",
        "attempt",
        "extra_run",
        "missing_run",
        "workflow_path",
        "suite_app",
        "check_app",
        "check_head",
        "check_name",
        "duplicate_check",
        "missing_check",
        "check_job_url",
        "job_attempt",
        "job_checkout",
        "job_status",
        "job_skipped",
        "extra_job",
        "missing_job",
        "step_status",
        "unknown_step",
        "raw_byte",
        "missing_report",
        "source_blob",
        "tree_truncated",
        "artifact_extra",
        "job_interpreter",
    ],
)
def test_foreign_incomplete_or_drifted_public_authority_never_passes(monkeypatch, drift):
    case = fixture()
    approve(monkeypatch, case)
    s, data = case.profile["subject"], case.data
    repo, head = s["repository"], s["head"]
    run = data[f"repos/{repo}/actions/runs/2000"]
    checks = data[f"repos/{repo}/commits/{head}/check-runs"]["check_runs"]
    jobs = data[f"repos/{repo}/actions/runs/2000/attempts/1/jobs"]["jobs"]
    if drift == "actor":
        data["user"]["id"] = 999
    elif drift == "repository":
        data["repos/" + repo]["id"] = 999
    elif drift == "pr_head":
        data[f"repos/{repo}/pulls/10"]["head"]["sha"] = "7" * 40
    elif drift == "draft":
        data[f"repos/{repo}/pulls/10"]["draft"] = True
    elif drift == "run_head":
        run["head_sha"] = "7" * 40
    elif drift == "run_actor":
        run["triggering_actor"] = {"id": 999, "login": "other"}
    elif drift == "event":
        run["event"] = "pull_request"
    elif drift == "attempt":
        run["run_attempt"] = 2
    elif drift == "extra_run":
        data[f"repos/{repo}/actions/runs"]["workflow_runs"].append({**run, "id": 999})
    elif drift == "missing_run":
        data[f"repos/{repo}/actions/runs"]["workflow_runs"].pop(0)
    elif drift == "workflow_path":
        data[f"repos/{repo}/actions/workflows/1500"]["path"] = "other"
    elif drift == "suite_app":
        data[f"repos/{repo}/check-suites/3000"]["app"]["id"] = 999
    elif drift == "check_app":
        checks[0]["app"]["id"] = 999
    elif drift == "check_head":
        checks[0]["head_sha"] = "7" * 40
    elif drift == "check_name":
        checks[0]["name"] = "unselected"
    elif drift == "duplicate_check":
        checks.append({**checks[0], "id": 999})
    elif drift == "missing_check":
        checks.pop(0)
    elif drift == "check_job_url":
        checks[0]["details_url"] = "https://example.test/foreign"
    elif drift == "job_attempt":
        jobs[0]["run_attempt"] = 2
    elif drift == "job_checkout":
        jobs[0]["head_sha"] = "7" * 40
    elif drift == "job_status":
        jobs[0]["status"] = "in_progress"
    elif drift == "job_skipped":
        jobs[0]["conclusion"] = "skipped"
    elif drift == "extra_job":
        jobs.append({**jobs[0], "id": 999, "name": "foreign"})
    elif drift == "missing_job":
        jobs.clear()
    elif drift == "step_status":
        jobs[0]["steps"][2]["conclusion"] = "skipped"
    elif drift == "unknown_step":
        jobs[0]["steps"].append({"number": 99, "name": "foreign"})
    elif drift in {"raw_byte", "missing_report", "job_interpreter"}:
        key = f"repos/{repo}/actions/jobs/7000/logs"
        data[key] = data[key].replace(b"2 passed", b"0 passed")
    elif drift == "source_blob":
        key = next(k for k in data if "/contents/" in k)
        data[key] += b"// foreign change\n"
    elif drift == "tree_truncated":
        next(v for k, v in data.items() if "/git/trees/" in k)["truncated"] = True
    else:
        data[f"repos/{repo}/actions/runs/2000/artifacts"]["artifacts"].append({"id": 999})
    with pytest.raises((ValueError, AssertionError)):
        observed(case)


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


def test_unrelated_social_metadata_does_not_change_execution_authority(monkeypatch):
    case = fixture()
    approve(monkeypatch, case)

    def refreshed(command, case):
        for value in case.data.values():
            if isinstance(value, dict) and isinstance(value.get("repository"), dict):
                value["repository"]["stargazers_count"] = len(case.calls)

    case.callback = refreshed
    assert observed(case)[0]["state"] == "passed"


def test_profile_mutation_is_inert_even_with_self_consistent_new_hashes(monkeypatch):
    case = fixture()
    approve(monkeypatch, case)
    other = copy.deepcopy(case.profile)
    other["jobs"][case.profile["required_ci"][0]]["reports"][0]["observed"] = {"passed": 0}
    case.profile = other
    with pytest.raises(ValueError, match="reviewed"):
        observed(case)


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
