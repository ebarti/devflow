"""Real Git combinations and durable update intents with a controlled GitHub transport."""
from __future__ import annotations

import json
from copy import deepcopy

import pytest
from test_delivery_merge import Store, gates
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal.delivery_merge import MergeBroker
from devflow_temporal.delivery_merge_reconciliation import reconcile


class Remote:
    def __init__(self, repo, base):
        self.repo, self.original, self.target = repo, base, base
        self.head = _git(repo, "rev-parse", "HEAD")
        self.updates, self.lost, self.mergeable = 0, False, True
        self.commits = {}

    def commit(self, head):
        return {"sha": head, "tree": {"sha": _git(self.repo, "rev-parse", head + "^{tree}")},
                "parents": [{"sha": p} for p in _git(
                    self.repo, "show", "-s", "--format=%P", head).split()]}

    def raw(self):
        tree = _git(self.repo, "merge-tree", "--write-tree", self.target, self.head)
        hosted = _git(self.repo, "commit-tree", tree, "-p", self.target, "-p", self.head,
                      "-m", "Hosted test merge")
        return {"number": 1, "html_url": "https://github.com/owner/repo/pull/1",
                "head": {"sha": self.head, "ref": "feat/fresh",
                         "repo": {"full_name": "owner/repo"}},
                "base": {"sha": self.target, "ref": "main",
                         "repo": {"full_name": "owner/repo"}},
                "draft": False, "merged": False, "state": "open",
                "mergeable": self.mergeable, "merge_commit_sha": hosted}

    def api(self, path, *, method="GET", body=None):
        if "/git/commits/" in path:
            return self.commit(path.rsplit("/", 1)[-1])
        if path.endswith("/git/ref/heads/main"):
            return {"object": {"sha": self.target}}
        if "/compare/" in path:
            before, after = path.rsplit("/", 1)[-1].split("...")
            return {"status": "ahead", "merge_base_commit": {
                "sha": _git(self.repo, "merge-base", before, after)}}
        if path.endswith("/update-branch"):
            assert method == "PUT" and body == {"expected_head_sha": self.head}
            tree = self.raw()["merge_commit_sha"] + "^{tree}"
            self.head = _git(self.repo, "commit-tree", tree, "-p", self.head,
                             "-p", self.target, "-m", "Merge main into feature")
            self.updates += 1
            if self.lost:
                raise TimeoutError("accepted update response lost")
            return {"message": "Updating pull request branch"}
        raise AssertionError((path, method, body))


def fixture(service, tmp_path):
    from pathlib import Path

    original_store, _ = service
    repo = Path(original_store.config.raw["repositories"]["fixture"]["source_path"])
    base = _git(repo, "rev-parse", "HEAD")
    (repo / "feature.txt").write_text("Reviewed feature\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "feat: accepted feature")
    feature_head = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "--detach", base)
    (repo / "main.txt").write_text("Independent main update\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "chore: advance main")
    target = _git(repo, "rev-parse", "HEAD")
    spec, _, pr, *_ = gates()
    spec.update(base_sha=base, merge_reconciliation_version=1, state_dir=str(tmp_path))
    pr["head"] = feature_head
    remote = Remote(repo, base)
    remote.head, remote.target = feature_head, target
    store = Store(tmp_path / "effects.sqlite3")
    broker = MergeBroker(store, spec)
    broker.api = remote.api
    return broker, pr, remote


@pytest.mark.parametrize("lost_response", [False, True])
def test_moved_main_combines_real_git_once_and_preserves_reviewed_source(
    service, tmp_path, lost_response,
):
    broker, pr, remote = fixture(service, tmp_path)
    old_pr, old_spec = deepcopy(pr), deepcopy(broker.spec)
    remote.lost = lost_response
    if lost_response:
        with pytest.raises(TimeoutError):
            reconcile(broker, pr, remote.raw())
    else:
        assert reconcile(broker, pr, remote.raw())["state"] == "pending"
    result = reconcile(broker, pr, remote.raw())
    assert result["state"] == "ready" and result["base"] == remote.target
    assert result["pr"]["head"] != pr["head"] and remote.updates == 1
    head = result["pr"]["head"]
    assert _git(remote.repo, "show", head + ":feature.txt") == "Reviewed feature"
    assert _git(remote.repo, "show", head + ":main.txt") == "Independent main update"
    assert reconcile(broker, pr, remote.raw()) == result
    assert pr == old_pr and broker.spec == old_spec
    with broker.store._connect() as db:
        saved = db.execute("SELECT * FROM delivery_effects").fetchall()
    assert len(saved) == 1 and saved[0]["state"] == "complete"
    assert json.loads(saved[0]["observed_json"])["head"] == head


@pytest.mark.parametrize("change", ["unknown_head", "bad_tree", "wrong_parent"])
def test_reconciliation_cannot_launder_foreign_source(service, tmp_path, change):
    broker, pr, remote = fixture(service, tmp_path)
    if change != "unknown_head":
        reconcile(broker, pr, remote.raw())
    parent = remote.original if change == "wrong_parent" else remote.target
    tree = remote.original + "^{tree}" if change == "bad_tree" else remote.head + "^{tree}"
    remote.head = _git(remote.repo, "commit-tree", tree, "-p", pr["head"], "-p", parent,
                       "-m", "Unowned rewrite")
    with pytest.raises(ValueError, match="unowned head|exact mechanical"):
        reconcile(broker, pr, remote.raw())
    assert remote.updates == (0 if change == "unknown_head" else 1)


def test_upstream_conflict_names_decision_without_mutating_or_rebuilding(service, tmp_path):
    broker, pr, remote = fixture(service, tmp_path)
    remote.mergeable = False
    result = reconcile(broker, pr, remote.raw())
    assert result["state"] == "decision_required"
    assert result["reason"] == "conflicting_upstream_changes"
    assert "opposing product direction" in result["instruction"]
    assert remote.updates == 0
    with broker.store._connect() as db:
        assert not db.execute("SELECT 1 FROM delivery_effects").fetchall()


def test_ordinary_followup_routes_textual_conflicts_to_resolution_instead_of_a_user_decision(
    service, tmp_path,
):
    broker, pr, remote = fixture(service, tmp_path)
    remote.mergeable = False
    result = reconcile(broker, pr, remote.raw(), resolve_conflicts=True)
    assert result == {"state": "resolve_conflicts", "target": remote.target,
                      "original_head": pr["head"], "previous_head": pr["head"]}
    assert remote.updates == 0


def test_unknown_original_update_is_read_back_never_resubmitted(service, tmp_path):
    broker, pr, remote = fixture(service, tmp_path)
    update = remote.api

    def lost_before_response(path, **kwargs):
        if path.endswith("/update-branch"):
            raise TimeoutError("outcome unknown")
        return update(path, **kwargs)

    broker.api = lost_before_response
    with pytest.raises(TimeoutError):
        reconcile(broker, pr, remote.raw())
    broker.api = lambda *args, **kwargs: pytest.fail("original update must only be observed")
    result = reconcile(broker, pr, remote.raw())
    assert result == {"state": "pending", "reason": "original_target_update_readback"}
    assert remote.updates == 0


@pytest.mark.asyncio
async def test_current_target_wait_never_reopens_completed_chunk_workers(monkeypatch):
    from test_delivery_feature_execution import Controller
    from test_delivery_github_contract import plan

    from devflow_temporal import delivery_feature_workflow as protocol

    controller = Controller(plan())
    chunks = [c for s in controller.record["manifest"]["plan"]["workstreams"] for c in s["chunks"]]
    members = [{"chunk_id": c["id"], "number": i + 20} for i, c in enumerate(chunks)]
    controller.record["manifest"]["publication"] = {"stack_id": 42, "members": members}
    calls = []
    original = controller._activity

    async def dispatch(name, request):
        calls.append(name)
        if name == "delivery_feature_open":
            return {"record": deepcopy(controller.record), "budget": {"used": 0},
                    "checkpoints": {"verified:" + c["id"]: {} for c in chunks}}
        if name == "delivery_feature_merge" and calls.count(name) == 1:
            return {"state": "pending", "reason": "target_update_submitted"}
        assert name != "delivery_feature_begin_integration"
        return await original(name, request)

    async def no_wait(_):
        return

    async def no_worker(*_):
        pytest.fail("a main movement must not replay completed chunk workers")

    controller._activity = dispatch
    monkeypatch.setattr(protocol.workflow, "patched", lambda _: False)
    monkeypatch.setattr(protocol.workflow, "sleep", no_wait)
    monkeypatch.setattr(protocol, "_worker", no_worker)
    result = await protocol.coordinate(controller, {
        "run_id": "original-feature", "authorized_endpoint": "merged",
        "merge_reconciliation_version": 1,
    })
    assert result["phase"] == "merged" and calls.count("delivery_feature_merge") == 2
    assert result["checks"]["repair_budget"] == {"used": 0}


def test_current_target_ci_waits_but_never_accepts_stale_checkout(tmp_path):
    from test_delivery_merge import Hosted

    from devflow_temporal.delivery_merge_reconciliation import MergeChecksPending

    spec, _, pr, *_ = gates()
    spec.update(state_dir=str(tmp_path), merge_reconciliation_version=1)
    broker = Hosted(spec)
    api = broker.api
    broker.api = lambda path, **kwargs: ({"object": {"sha": "b"}}
        if path.endswith("/git/ref/heads/main") else api(path, **kwargs))
    found = {"state": "open", "merged": False, "base": {"sha": "b"},
             "merge_commit_sha": "hosted"}
    broker.check["status"] = "in_progress"
    with pytest.raises(MergeChecksPending):
        broker.live_ci(pr, found, "t")
    broker.check["status"] = "completed"
    broker.log = "checkout stale\nMerge h into old-main\n"
    with pytest.raises(ValueError, match="checkout log"):
        broker.live_ci(pr, found, "t")
