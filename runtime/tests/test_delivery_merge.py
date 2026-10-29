"""Pure merge gates and controlled remote readback; no native/server fixtures."""
from __future__ import annotations

import json
import sqlite3
from copy import deepcopy

import pytest
from test_delivery_intake import intake_fixture as intake_fixture

from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_merge import MergeBroker, require_merge_gates


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
        readiness={"version": 1, "generation": 0, "browser_engines": [],
                   "temporal_checks": []},
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


@pytest.mark.parametrize("stage,failure", [("prepublish", "failed"), ("ci", "unknown")])
def test_any_unpassed_original_gate_prevents_merge(stage, failure):
    spec, candidate, pr, checks, attempts, publication = gates()
    checks[stage]["state"] = failure
    with pytest.raises(ValueError):
        require_merge_gates(spec, candidate, pr, checks, attempts, publication)


@pytest.mark.parametrize("change", ["legacy", "stale_qa"])
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
