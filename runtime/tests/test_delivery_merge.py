"""Pure merge gates and controlled remote readback; no native/server fixtures."""
from __future__ import annotations

import json
import sqlite3
from copy import deepcopy
from datetime import UTC, datetime

import pytest
from test_delivery_intake import intake_fixture as intake_fixture

from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_merge import MergeBroker, require_merge_gates
from devflow_temporal.delivery_workflow import DeliveryWorkflow


def gates():
    candidate = {"id": "published", "head": "h", "base_sha": "b",
                 "policy_digest": "policy", "content_sha256": "contents"}
    spec = {"run_id": "fresh", "provider": "codex", "authorized_endpoint": "merged",
            "merge_version": 1, "resource_cleanup_version": 1, "terminal_tracker_version": 1,
            "base_sha": "b", "policy_digest": "policy", "github_repo": "owner/repo",
            "branch": "feat/fresh", "publication_base_ref": "main",
            "issue_url": "https://github.com/owner/repo/issues/7",
            "policy": {"required_ci": ["test"], "tracker_retry_seconds": 600}}
    pr = {"number": 1, "head": "h", "url": "https://github.com/owner/repo/pull/1"}
    checks = {stage: {"state": "passed", "candidate_id": "published"}
              for stage in ("local", "review", "qa")}
    checks.update(prepublish={"state": "passed", "candidate_id": "source"},
                  ci={"state": "passed", "head": "h"}, resource_cleanup={
                      "state": "confirmed", "process_cleanup": "observed-native-confirmed",
                      "resource_cleanup": "confirmed"})
    attempts = [{"role": role, "candidate_id": "published", "state": "finished",
                 "cleanup": "confirmed", "session_id": role,
                 "controller_output_candidate": {"id": "source", "head": "source-head",
                                                 "content_sha256": "contents"},
                 "result_json": json.dumps({"status": "pass", "session_id": role})}
                for role in ("implement", "review", "verify")]
    return spec, candidate, pr, checks, attempts, {"input_candidate_id": "source"}


def test_publication_commit_can_change_head_without_changing_accepted_source():
    values = gates()
    original = deepcopy(values)
    require_merge_gates(*values)
    assert values == original


def test_real_admission_selects_merge_explicitly_without_a_second_repo_permission(intake_fixture):
    from test_delivery_intake import _git

    path, request = intake_fixture
    cfg = DeliveryConfig.load(path)
    source = cfg.raw["repositories"]["fixture"]["source_path"]
    from pathlib import Path

    source = Path(source)
    _git(source, "push", "origin", "HEAD:refs/heads/main")
    _git(source, "fetch", "origin", "main")
    cfg.raw["provider"] = "codex"
    cfg.raw["repositories"]["fixture"].update(
        base_ref="origin/main", assignee="owner", project_url="https://github.com/users/owner/projects/1",
        required_ci=["test"], prepublish_checks=[{"id": "readme", "argv": ["true"]}],
        checks=[{"id": "readme", "argv": ["true"]}],
    )
    request["base_ref"] = "origin/main"
    # Tests only construct immutable authority. No service, role or native check starts.
    unmerged = cfg.admit(request)
    merged = cfg.admit({**request, "authorized_endpoint": "merged"})
    assert "merge_version" not in unmerged
    assert merged["merge_version"] == 1 and merged["authorized_endpoint"] == "merged"
    assert merged["policy"] == unmerged["policy"]
    assert merged["policy_digest"] == unmerged["policy_digest"]
    assert merged["intake_required"] is True
    assert cfg.public_policy()["authorized_endpoints"] == ["published_unmerged", "merged"]


@pytest.mark.parametrize("change", ["fake", "recovery", "supersedes", "unknown_endpoint"])
def test_merge_cannot_broaden_a_legacy_recovery_or_simulation(intake_fixture, change):
    path, request = intake_fixture
    cfg = DeliveryConfig.load(path)
    request["authorized_endpoint"] = "merged"
    if change != "fake":
        cfg.raw["provider"] = "codex"
    if change == "recovery":
        request["recovery_key"] = "existing"
    elif change == "supersedes":
        request["supersedes_run_id"] = "run-previous"
    elif change == "unknown_endpoint":
        request["authorized_endpoint"] = "deployed"
    with pytest.raises(ValueError):
        cfg.admit(request)


@pytest.mark.parametrize("stage", ["prepublish", "local", "review", "qa", "ci"])
@pytest.mark.parametrize("failure", ["failed", "pending", "unknown"])
def test_any_unpassed_original_gate_prevents_merge(stage, failure):
    spec, candidate, pr, checks, attempts, publication = gates()
    checks[stage]["state"] = failure
    with pytest.raises(ValueError):
        require_merge_gates(spec, candidate, pr, checks, attempts, publication)


@pytest.mark.parametrize("change", ["legacy", "false_marker", "source", "head", "base",
                                   "policy", "ci_head", "cleanup", "unfinished",
                                   "shared_session", "missing_review", "changed_implementation",
                                   "publication", "stale_qa"])
def test_changed_authority_candidate_or_native_custody_prevents_merge(change):
    spec, candidate, pr, checks, attempts, publication = gates()
    if change == "legacy":
        spec["authorized_endpoint"] = "published_unmerged"
    elif change == "false_marker":
        spec["merge_version"] = True
    elif change == "source":
        candidate["content_sha256"] = "changed"
    elif change in {"head", "base", "policy"}:
        candidate[{"head": "head", "base": "base_sha", "policy": "policy_digest"}[change]] = "other"
    elif change == "ci_head":
        checks["ci"]["head"] = "other"
    elif change == "cleanup":
        checks["resource_cleanup"]["resource_cleanup"] = "unknown"
    elif change == "unfinished":
        attempts[0]["state"] = "running"
    elif change == "shared_session":
        attempts[1]["session_id"] = attempts[0]["session_id"]
    elif change == "missing_review":
        attempts.pop(1)
    elif change == "changed_implementation":
        attempts[0]["controller_output_candidate"]["content_sha256"] = "other"
    elif change == "publication":
        publication["input_candidate_id"] = "other"
    else:
        checks["qa"]["candidate_id"] = "stale"
    with pytest.raises(ValueError):
        require_merge_gates(spec, candidate, pr, checks, attempts, publication)


@pytest.mark.parametrize("field,value", [("state", "unknown"), ("cleanup", "unknown"),
                                       ("candidate_id", "other")])
def test_configured_browser_gate_is_mandatory(field, value):
    spec, candidate, pr, checks, attempts, publication = gates()
    spec["policy"]["browser_qa"] = {"configured": True}
    checks["browser_qa"] = {"state": "passed", "cleanup": "confirmed", "candidate_id": "published"}
    checks["browser_qa"][field] = value
    with pytest.raises(ValueError):
        require_merge_gates(spec, candidate, pr, checks, attempts, publication)


class Store:
    def __init__(self, path):
        self.path = path
        with self._connect() as db:
            db.execute("CREATE TABLE delivery_effects(effect_key PRIMARY KEY,run_id,kind,"
                       "request_json,state,observed_json,updated_at)")

    def _connect(self):
        db = sqlite3.connect(self.path)
        db.row_factory = sqlite3.Row
        return db


def test_unknown_remote_effect_preserves_one_intent_and_does_not_regrant_mutation(tmp_path):
    spec = gates()[0]
    spec["state_dir"] = str(tmp_path)
    store = Store(tmp_path / "state.sqlite3")
    first = MergeBroker(store, spec)
    assert first.intent("owned", "merge", {"head": "h"}) is True
    retry = MergeBroker(store, spec)
    assert retry.intent("owned", "merge", {"head": "h"}) is False
    with pytest.raises(ValueError, match="another request"):
        retry.intent("owned", "merge", {"head": "different"})
    with store._connect() as db:
        assert db.execute("SELECT count(*) FROM delivery_effects").fetchone()[0] == 1
        assert db.execute("SELECT state FROM delivery_effects").fetchone()[0] == "pending"


class Projection(DeliveryWorkflow):
    def __init__(self, cleanup, merge):
        super().__init__()
        self.cleanup, self.merge = cleanup, merge
        self.calls = []
        self.state = {"phase": "delivered", "execution_state": "terminal", "outcome": "delivered",
                      "cleanup": "none", "error": None, "checks": {}, "revision": 1,
                      "iteration": 0, "candidate": gates()[1], "pull_request": gates()[2]}

    async def _activity(self, name, request):
        self.calls.append((name, deepcopy(request)))
        if name == "delivery_finalize_resources":
            return self.cleanup
        if name == "delivery_merge":
            return self.merge
        if name == "delivery_terminal_tracker":
            return {"state": "consistent"}
        if name == "delivery_project":
            return {}
        raise AssertionError(name)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["merged", "unmerged", "bad_cleanup", "unknown_merge"])
async def test_actual_workflow_orders_cleanup_merge_and_done(monkeypatch, mode):
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", lambda _: True)
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.now",
                        lambda: datetime(2026, 10, 8, tzinfo=UTC))
    spec = gates()[0]
    cleanup = deepcopy(gates()[3]["resource_cleanup"])
    merged = {"state": "confirmed"}
    if mode == "unmerged":
        spec.pop("merge_version")
        spec["authorized_endpoint"] = "published_unmerged"
    elif mode == "bad_cleanup":
        cleanup["state"] = "unknown"
    elif mode == "unknown_merge":
        merged["state"] = "unknown"
    probe = Projection(cleanup, merged)
    await probe._project(spec, "delivered", "terminal")
    names = [name for name, _ in probe.calls]
    assert names[0] == "delivery_finalize_resources"
    assert ("delivery_merge" in names) == (mode in {"merged", "unknown_merge"})
    if "delivery_merge" in names:
        assert names.index("delivery_merge") < names.index("delivery_terminal_tracker")
    status = next(r["status"] for n, r in probe.calls if n == "delivery_terminal_tracker")
    if mode == "unknown_merge":
        terminal = next(r for n, r in probe.calls if n == "delivery_terminal_tracker")
        assert terminal["release"] is False
    assert status == {"merged": "done", "unmerged": "in-review",
                      "bad_cleanup": "blocked", "unknown_merge": "blocked"}[mode]
    assert probe.state["outcome"] == ("delivered" if mode in {"merged", "unmerged"} else "blocked")


def test_squash_readback_rejects_different_merged_tree_or_base(tmp_path):
    spec, _, pr, _, _, _ = gates()
    spec["state_dir"] = str(tmp_path)
    broker = MergeBroker(None, spec)
    found = {"merged": True, "state": "closed", "merged_at": "now", "merge_commit_sha": "m"}
    for tree, parent in [("other", "b"), ("t", "advanced")]:
        broker.api = lambda *_, tree=tree, parent=parent, **__: {
            "sha": "m", "tree": {"sha": tree}, "parents": [{"sha": parent}],
            "status": "diverged"}
        with pytest.raises(ValueError, match="verified tree or base"):
            broker.merged(pr, found, "t")


def test_changed_base_is_denied_before_any_merge_effect(tmp_path):
    spec, _, pr, _, _, _ = gates()
    spec["state_dir"] = str(tmp_path)
    broker = MergeBroker(None, spec)
    found = {"state": "open", "merged": False, "base": {"sha": "advanced"}}
    broker.api = lambda *_, **__: pytest.fail("moved base must block before GitHub effects")
    with pytest.raises(ValueError, match="upstream moved"):
        broker.live_ci(pr, found, "t")


def test_owned_issue_closure_is_not_replayed_after_unknown_response(tmp_path):
    spec, _, _, _, _, _ = gates()
    spec["state_dir"] = str(tmp_path)
    store = Store(tmp_path / "state.sqlite3")
    broker = MergeBroker(store, spec)
    request = {"issue": spec["issue_url"], "merged_commit": "m"}
    assert broker.intent("merge-issue:fresh", "merge_issue", request)
    calls = []
    def api(*args, **kwargs):
        calls.append(kwargs.get("method", "GET"))
        return {"state": "open", "html_url": spec["issue_url"]}
    broker.api = api
    with pytest.raises(ValueError, match="no mutation replay"):
        broker.close_issue({"merged_commit": "m"})
    assert calls == ["GET"]


def test_already_closed_issue_reconciles_lost_effect_without_another_patch(tmp_path):
    spec, _, _, _, _, _ = gates()
    spec["state_dir"] = str(tmp_path)
    store = Store(tmp_path / "state.sqlite3")
    broker = MergeBroker(store, spec)
    assert broker.intent("merge-issue:fresh", "merge_issue", {
        "issue": spec["issue_url"], "merged_commit": "m"})
    calls = []
    def api(*args, **kwargs):
        calls.append(kwargs.get("method", "GET"))
        return {"state": "closed", "state_reason": "completed", "html_url": spec["issue_url"]}
    broker.api = api
    assert broker.close_issue({"merged_commit": "m"})["issue_state"] == "CLOSED"
    assert calls == ["GET"]


class Hosted(MergeBroker):
    def __init__(self, spec):
        super().__init__(None, spec)
        self.log_reads = 0
        self.hosted = {"sha": "hosted", "tree": {"sha": "t"},
                       "parents": [{"sha": "b"}, {"sha": "h"}]}
        self.check = {"id": 10, "name": "test", "head_sha": "h", "status": "completed",
                      "conclusion": "success", "details_url":
                          "https://github.com/owner/repo/actions/runs/20/job/30"}
        self.run = {"id": 20, "event": "pull_request", "run_attempt": 1, "head_sha": "h"}
        self.job = {"id": 30, "name": "test", "run_id": 20, "run_attempt": 1,
                    "head_sha": "h", "status": "completed", "conclusion": "success"}
        self.log = "checkout hosted\r\nMerge h into b\r\nfull command stream\r\n"

    def api(self, endpoint, **kwargs):
        if "/git/commits/" in endpoint:
            return self.hosted
        if "/check-runs" in endpoint:
            return {"total_count": 1, "check_runs": [self.check]}
        if "/actions/runs/" in endpoint:
            return self.run
        if endpoint.endswith("/logs"):
            self.log_reads += 1
            return self.log
        if "/actions/jobs/" in endpoint:
            return self.job
        raise AssertionError(endpoint)


def test_normal_ci_authenticates_the_full_hosted_tree_and_reuses_complete_log(tmp_path):
    from devflow_temporal.delivery_resources import read_private

    spec, _, pr, _, _, _ = gates()
    spec["state_dir"] = str(tmp_path)
    broker = Hosted(spec)
    found = {"state": "open", "merged": False, "base": {"sha": "b"},
             "merge_commit_sha": "hosted"}
    first = broker.live_ci(pr, found, "t")
    second = broker.live_ci(pr, found, "t")
    assert first == second
    assert broker.log_reads == 1
    assert read_private(broker.root / "job-30-log.json")["stdout"] == broker.log


@pytest.mark.parametrize("change", ["hosted_tree", "hosted_parents", "check_head", "failed",
                                   "foreign_job", "manual_trigger", "rerun", "job_head",
                                   "different_checkout_log"])
def test_remote_ci_mismatch_cannot_bless_a_merge(tmp_path, change):
    spec, _, pr, _, _, _ = gates()
    spec["state_dir"] = str(tmp_path)
    broker = Hosted(spec)
    found = {"state": "open", "merged": False, "base": {"sha": "b"},
             "merge_commit_sha": "hosted"}
    if change == "hosted_tree":
        broker.hosted["tree"]["sha"] = "other"
    elif change == "hosted_parents":
        broker.hosted["parents"][0]["sha"] = "other"
    elif change == "check_head":
        broker.check["head_sha"] = "other"
    elif change == "failed":
        broker.check["conclusion"] = "failure"
    elif change == "foreign_job":
        broker.check["details_url"] = "https://github.com/foreign/repo/actions/runs/20/job/30"
    elif change == "manual_trigger":
        broker.run["event"] = "workflow_dispatch"
    elif change == "rerun":
        broker.run["run_attempt"] = 2
    elif change == "job_head":
        broker.job["head_sha"] = "other"
    else:
        broker.log = "checkout other\nMerge h into b\n"
    with pytest.raises(ValueError):
        broker.live_ci(pr, found, "t")


@pytest.mark.asyncio
async def test_terminal_reconciliation_never_schedules_merge_or_changes_confirmed_outcome():
    probe = Projection(gates()[3]["resource_cleanup"], {"state": "unknown"})
    probe.terminal_reconciliation_only = True
    await probe._project(gates()[0], "delivered", "readback only")
    assert [n for n, _ in probe.calls] == ["delivery_project"]
    assert probe.state["outcome"] == "delivered"


def native_receipt(tmp_path):
    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_resources import write_private
    from devflow_temporal.supervisor import DeliverySupervisor

    spec = gates()[0]
    spec.update(state_dir=str(tmp_path / "fresh"), checkout=str(tmp_path / "source"))
    request = {"spec": spec, "role": "implement", "iteration": 0,
               "candidate": {"id": "input"}, "workspace": spec["checkout"]}
    key = DeliverySupervisor._job_key(request)
    root = tmp_path / "fresh" / "attempts" / key
    path = root / "native-process.json"
    assessment = {"status": "pass", "session_id": "implement", "findings": []}
    outcome = {"state": "finished", "monitoring_complete": True,
               "cleanup": "observed-native-confirmed", "journal": str(path)}
    saved = {**assessment, "cleanup": "confirmed", "process_cleanup": "observed-native-confirmed",
             "resource_cleanup": "pending_workflow_finalization", "native_process": outcome}
    output = {"id": "source", "head": "source-head", "content_sha256": "contents"}
    journal = {"phase": "finished", "owned": {"123": {"identity": "observed"}},
               "intent": {"run_id": spec["run_id"], "policy_digest": spec["policy_digest"],
                          "cwd": spec["checkout"]}, "result": outcome,
               "provider_session": {"role": "implement", "iteration": 0,
                                    "session_id": "implement", "output_candidate": output,
                                    "result_digest": digest(saved)}}
    for name, value in (("request.json", request), ("result.json", assessment),
                        ("native-process.json", journal)):
        write_private(root / name, value)
    attempt = {**gates()[4][0], "job_key": key, "iteration": 0, "candidate_id": "input",
               "result_path": str(root / "result.json"), "result_json": json.dumps(saved)}
    attempt.pop("controller_output_candidate")
    return spec, attempt, path, journal


def test_actual_native_saved_receipt_uses_separate_supervisor_output_binding(tmp_path):
    from devflow_temporal.delivery_merge import native_implementation_output

    spec, attempt, _, _ = native_receipt(tmp_path)
    assert "candidate" not in json.loads(attempt["result_json"])
    output = native_implementation_output(spec, attempt)
    values = gates()
    values[4][0] = {**attempt, "controller_output_candidate": output}
    require_merge_gates(*values)


@pytest.mark.parametrize("change", ["receipt", "session", "output", "request", "run", "policy",
                                   "iteration", "result", "role", "cleanup"])
def test_changed_native_receipt_or_controller_binding_cannot_bless_implementation(tmp_path, change):
    from devflow_temporal.delivery_merge import native_implementation_output
    from devflow_temporal.delivery_resources import write_private

    spec, attempt, path, journal = native_receipt(tmp_path)
    if change == "receipt":
        write_private(path.with_name("result.json"), {"status": "pass", "session_id": "other"})
    elif change == "request":
        write_private(path.with_name("request.json"), {"spec": spec, "role": "review"})
    elif change in {"session", "output", "iteration", "role"}:
        journal["provider_session"][{"session": "session_id", "output": "output_candidate"}.get(
            change, change)] = "changed"
    elif change in {"run", "policy"}:
        journal["intent"][{"run": "run_id", "policy": "policy_digest"}[change]] = "changed"
    elif change == "cleanup":
        journal["phase"] = "unknown"
    else:
        journal["provider_session"]["result_digest"] = "changed"
    write_private(path, journal)
    with pytest.raises(ValueError):
        native_implementation_output(spec, attempt)


def strict_rules():
    params = {"strict_required_status_checks_policy": True,
              "required_status_checks": [{"context": "test", "integration_id": 15368}]}
    rule = {"type": "required_status_checks", "parameters": params, "ruleset_id": 42,
            "ruleset_source_type": "Repository", "ruleset_source": "owner/repo"}
    detail = {"id": 42, "target": "branch", "enforcement": "active",
              "source_type": "Repository", "source": "owner/repo", "bypass_actors": [],
              "rules": [{"type": "required_status_checks", "parameters": params}]}
    return rule, detail


@pytest.mark.parametrize("change", ["loose", "missing_checks", "disabled", "bypass", "exempt",
                                   "pull_request_bypass", "hidden_bypass", "changed_rules",
                                   "foreign"])
def test_merge_refuses_unenforced_or_bypassable_github_base_checks(tmp_path, change):
    spec = gates()[0]
    spec["state_dir"] = str(tmp_path)
    broker = MergeBroker(None, spec)
    rule, detail = strict_rules()
    if change == "loose":
        rule["parameters"]["strict_required_status_checks_policy"] = False
    elif change == "missing_checks":
        rule["parameters"]["required_status_checks"] = []
    elif change == "disabled":
        detail["enforcement"] = "evaluate"
    elif change in {"bypass", "exempt", "pull_request_bypass"}:
        detail["bypass_actors"] = [{"actor_type": "RepositoryRole", "actor_id": 5,
                                    "bypass_mode": {"bypass": "always", "exempt": "exempt",
                                                    "pull_request_bypass": "pull_request"}[change]}]
    elif change == "hidden_bypass":
        detail.pop("bypass_actors")
    elif change == "foreign":
        rule["ruleset_source"] = "foreign/repo"
    else:
        detail["rules"] = []
    broker.api = lambda endpoint, **_: detail if "/rulesets/" in endpoint else [rule]
    with pytest.raises(ValueError, match="active strict GitHub checks"):
        broker.require_base_enforcement()


def test_strict_main_only_ruleset_with_no_bypass_is_a_server_enforcement_receipt(tmp_path):
    spec = gates()[0]
    spec["state_dir"] = str(tmp_path)
    broker = MergeBroker(None, spec)
    rule, detail = strict_rules()
    broker.api = lambda endpoint, **_: detail if "/rulesets/" in endpoint else [rule]
    assert broker.require_base_enforcement() == {
        "ruleset_id": 42, "required_ci": ["test"], "strict": True, "bypass_actors": []}


def test_contained_base_advance_records_actual_parent_without_changing_frozen_base(tmp_path):
    spec, _, pr, _, _, _ = gates()
    spec["state_dir"] = str(tmp_path)
    broker = MergeBroker(None, spec)
    calls = []
    def api(endpoint, **_):
        calls.append(endpoint)
        if "/git/commits/" in endpoint:
            return {"sha": "m", "tree": {"sha": "t"}, "parents": [{"sha": "c"}]}
        before = endpoint.split("/compare/", 1)[1].split("...", 1)[0]
        return {"status": "ahead", "base_commit": {"sha": before},
                "merge_base_commit": {"sha": before}}
    broker.api = api
    found = {"merged": True, "state": "closed", "merged_at": "now", "merge_commit_sha": "m"}
    result = broker.merged(pr, found, "t")
    assert result["base"] == spec["base_sha"] == "b"
    assert result["actual_merge_base"] == "c"
    assert calls[-2:] == ["repos/owner/repo/compare/b...c", "repos/owner/repo/compare/c...h"]


@pytest.mark.parametrize("boundary", ["cancelled", "deadline"])
def test_lost_activity_ownership_prevents_external_writes(tmp_path, monkeypatch, boundary):
    from threading import Event

    spec = gates()[0]
    spec["state_dir"] = str(tmp_path)
    cancelled = Event()
    broker = MergeBroker(None, spec, cancelled)
    if boundary == "cancelled":
        cancelled.set()
    else:
        broker.deadline = 0
    monkeypatch.setattr("devflow_temporal.delivery_merge.subprocess.run",
                        lambda *_, **__: pytest.fail("expired ownership cannot send requests"))
    with pytest.raises(RuntimeError, match="no longer owns"):
        broker.api("repos/owner/repo/issues/7", method="PATCH", body={"state": "closed"})


def concurrent_base_probe():
    import tempfile
    from pathlib import Path
    from unittest.mock import patch

    import devflow_temporal.delivery_merge as merge_module
    from devflow_temporal.contracts import canonical_json

    spec, candidate, pr, checks, attempts, publication = gates()
    with tempfile.TemporaryDirectory(prefix="devflow-merge-pure-race-", dir="/private/tmp") as tmp:
        spec["state_dir"] = tmp
        store = Store(Path(tmp) / "fake.sqlite3")
        saved = deepcopy(checks)
        saved.pop("resource_cleanup")
        with store._connect() as db:
            db.execute("CREATE TABLE delivery_runs("
                       "run_id,iteration,candidate_json,pr_json,checks_json)")
            db.execute("CREATE TABLE delivery_attempts(run_id,role,candidate_id,state,cleanup,"
                       "session_id,result_json,iteration)")
            db.execute("INSERT INTO delivery_runs VALUES(?,?,?,?,?)", (
                spec["run_id"], 0, canonical_json(candidate), canonical_json(pr),
                canonical_json(saved)))
            for attempt in attempts:
                db.execute("INSERT INTO delivery_attempts VALUES(?,?,?,?,?,?,?,?)", (
                    spec["run_id"], *(attempt[k] for k in (
                        "role", "candidate_id", "state", "cleanup", "session_id", "result_json")),
                    0))
            db.execute("INSERT INTO delivery_effects VALUES(?,?,?,?,?,?,?)", (
                "publish:fresh:0", "fresh", "publish", canonical_json(publication), "complete",
                canonical_json(pr), "2026-10-08T00:00:00Z"))
        hosted = Hosted(spec)
        remote = {"base": "b", "merged": False, "put_calls": [], "calls": []}

        def fake_api(self, endpoint, *, method="GET", body=None, raw=False):
            remote["calls"].append({"endpoint": endpoint, "method": method})
            if endpoint == "repos/owner/repo/pulls/1":
                return {"number": 1, "html_url": pr["url"], "draft": False,
                        "head": {"sha": "h", "ref": spec["branch"],
                                 "repo": {"full_name": "owner/repo"}},
                        "base": {"sha": remote["base"], "ref": "main",
                                 "repo": {"full_name": "owner/repo"}},
                        "merged": remote["merged"],
                        "state": "closed" if remote["merged"] else "open",
                        "merged_at": "2026-10-08T00:01:00Z" if remote["merged"] else None,
                        "merge_commit_sha": "actual-merge" if remote["merged"] else "hosted"}
            if endpoint == "repos/owner/repo/git/commits/h":
                return {"sha": "h", "tree": {"sha": "t"}, "parents": [{"sha": "b"}]}
            if endpoint == "repos/owner/repo/git/commits/actual-merge":
                return {"sha": "actual-merge", "tree": {"sha": "untested-tree"},
                        "parents": [{"sha": "advanced"}]}
            if endpoint == "repos/owner/repo/pulls/1/merge" and method == "PUT":
                remote["put_calls"].append({"body": deepcopy(body),
                                            "base_at_write": remote["base"]})
                remote["merged"] = True
                return {"merged": True, "sha": "actual-merge"}
            # The original candidate does not inspect these unprotected-repository
            # observations. Their explicit absence of strict enforcement is the
            # publication trigger and can be rejected by an owning repair.
            if endpoint == "repos/owner/repo/branches/main/protection":
                return {"required_status_checks": {"strict": False, "contexts": ["test"]},
                        "enforce_admins": {"enabled": True}}
            if endpoint.startswith("repos/owner/repo/rules/branches/main"):
                return []
            if endpoint.startswith("repos/owner/repo/rulesets"):
                return []
            if endpoint == "user":
                return {"login": "owner", "id": 1}
            if endpoint.endswith("/logs"):
                # A second delivery advances main after every old-base check
                # has succeeded, while the first delivery's head stays fixed.
                remote["base"] = "advanced"
            return deepcopy(hosted.api(endpoint, method=method, body=body, raw=raw))

        error = None
        with patch.object(merge_module.MergeBroker, "api", fake_api), \
                patch.object(merge_module, "observe_finalized_resources",
                             lambda _: {"state": "confirmed"}), \
                patch("devflow_temporal.delivery_policy_recovery.work_binding", lambda *_: None), \
                patch.object(merge_module, "native_implementation_output", lambda *_: {
                    "id": "source", "head": "source-head", "content_sha256": "contents"}):
            try:
                merge_module.merge_verified(store, spec, {
                    "candidate": candidate, "pull_request": pr, "checks": checks})
            except (ValueError, RuntimeError) as exc:
                error = str(exc)
        with store._connect() as db:
            effect = db.execute("SELECT state FROM delivery_effects "
                                "WHERE effect_key='merge:fresh'").fetchone()
        return {"probe": "actual-entry-concurrent-base-change",
                "status": "RED" if remote["put_calls"] else "PASS",
                "original_base": "b", "base_after_ci": remote["base"],
                "put_calls": remote["put_calls"], "simulated_merge_applied": remote["merged"],
                "merge_effect_state": effect["state"] if effect else None, "error": error,
                "remote_calls": remote["calls"]}


def test_actual_merge_entry_rejects_intervening_base_advance_without_server_enforcement():
    proof = concurrent_base_probe()
    assert proof["base_after_ci"] == "advanced"
    assert proof["put_calls"] == []
    assert proof["simulated_merge_applied"] is False
    assert proof["merge_effect_state"] is None
    assert "active strict GitHub checks" in proof["error"]
