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
                 "result_json": json.dumps({"status": "pass", "candidate": {
                     "id": "source", "content_sha256": "contents"}})}
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
        attempts[0]["result_json"] = json.dumps({"status": "pass", "candidate": {
            "id": "source", "content_sha256": "other"}})
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
            "sha": "m", "tree": {"sha": tree}, "parents": [{"sha": parent}]}
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
