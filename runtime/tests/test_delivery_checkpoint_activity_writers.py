"""Public activity entry points retain delayed real filesystem and CLI effects."""

from __future__ import annotations

import asyncio
import json
import sys
import threading
from copy import deepcopy
from pathlib import Path

import pytest
from test_delivery_checkpoints import git
from test_delivery_checkpoints import stopped as stopped
from test_delivery_feature_execution import feature_service
from test_delivery_store import service as service

from devflow_temporal import delivery_activities as activities
from devflow_temporal import delivery_feature_activities as features
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_checkpoints import create_checkpoint
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_github_contract import GitHubDelivery
from devflow_temporal.delivery_resources import RunResources
from devflow_temporal.delivery_store import DeliveryStore


@pytest.mark.asyncio
async def test_cancelled_prepare_body_blocks_checkpoint_until_real_worktree_creation_finishes(
    service, monkeypatch,
):
    old, request = service
    raw = deepcopy(old.config.raw)
    raw["workspace_custody_version"] = 1
    old.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(old.config.path))
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    checkout = Path(spec["checkout"])
    checkout.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    resources = RunResources(spec)
    resources.register(checkout, "checkout")
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = DeliveryBroker.prepare

    def delayed(broker):
        entered.set()  # _context has completed; the real Git side effect is still ahead.
        try:
            assert release.wait(10)
            return original(broker)
        finally:
            finished.set()

    monkeypatch.setattr(DeliveryBroker, "prepare", delayed)
    task = asyncio.create_task(activities.delivery_prepare({"spec": spec}))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET execution_state='blocked',phase='blocked',"
                       "outcome='blocked' WHERE run_id=?", (spec["run_id"],))
            revision = db.execute("SELECT revision FROM delivery_runs WHERE run_id=?",
                                  (spec["run_id"],)).fetchone()[0]
        with pytest.raises(ValueError, match="writer.*owns"):
            create_checkpoint(store, spec["run_id"], "cannot-ack-prepare", revision)
        assert not checkout.exists()
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 5)
    assert checkout.is_dir()
    resources.created(checkout)
    assert create_checkpoint(store, spec["run_id"], "after-prepare", revision)["resumable"]


@pytest.mark.asyncio
async def test_cancelled_role_context_keeps_real_gate_checkout_excluded(stopped, monkeypatch):
    store, spec, broker, row = stopped
    # The independent gate consumes a completed clean candidate. This commit
    # belongs only to the disposable fixture, before exercising the runtime.
    git(broker.checkout, "add", "--all")
    git(broker.checkout, "commit", "-qm", "Owned completed role candidate")
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = DeliveryBroker.gate_checkout
    created = []
    failures = []

    def delayed(owner, *args):
        entered.set()
        try:
            assert release.wait(10)
            result = original(owner, *args)
            created.append(result)
            return result
        except Exception as exc:
            failures.append(repr(exc))
            raise
        finally:
            finished.set()

    monkeypatch.setattr(DeliveryBroker, "gate_checkout", delayed)
    request = {"spec": spec, "role": "review", "iteration": 1,
               "candidate": broker.candidate()}
    task = asyncio.create_task(activities.delivery_role(request))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(ValueError, match="writer.*owns"):
            create_checkpoint(store, spec["run_id"], "cannot-ack-role", row["revision"])
        assert not created
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 5)
    assert len(created) == 1 and created[0].is_dir(), failures


@pytest.mark.asyncio
async def test_cancelled_feature_open_cannot_post_after_checkpoint_ack(
    service, monkeypatch, tmp_path,
):
    old, supplied = service
    raw = deepcopy(old.config.raw)
    raw["workspace_custody_version"] = 1
    old.config.path.write_text(json.dumps(raw))
    configured = DeliveryStore(DeliveryConfig.load(old.config.path))
    store, supplied, snapshot = feature_service((configured, supplied), monkeypatch)
    store.submit(supplied)
    spec = store.effective_spec(supplied["run_id"])
    broker = DeliveryBroker(store, spec)
    broker.checkout.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    resources = RunResources(spec)
    resources.register(broker.checkout, "checkout")
    broker.prepare()
    resources.created(broker.checkout)
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    external = tmp_path / "external-posts.jsonl"
    command = tmp_path / "owned-gh"
    command.write_text("#!" + sys.executable + "\n" + """import json,os,sys
from pathlib import Path
method=sys.argv[sys.argv.index('--method')+1]
if method != 'POST': raise SystemExit('unexpected owned CLI operation')
body=json.load(sys.stdin)
with Path(os.environ['OWNED_FEATURE_POSTS']).open('a') as out:
    out.write(json.dumps({'method':method,'body':body})+'\\n')
print(json.dumps({'unconfirmed_fixture_response':True}))
""")
    command.chmod(0o700)
    monkeypatch.setenv("DEVFLOW_GH", str(command))
    monkeypatch.setenv("OWNED_FEATURE_POSTS", str(external))
    monkeypatch.setattr(GitHubDelivery, "issue", lambda *_a: deepcopy(snapshot["issue"]))
    original = GitHubDelivery.initialize

    def delayed(owner, *args):
        entered.set()
        try:
            assert release.wait(10)
            # Real intent and CLI POST; the malformed reply remains unknown.
            return original(owner, *args)
        finally:
            finished.set()

    monkeypatch.setattr(GitHubDelivery, "initialize", delayed)
    task = asyncio.create_task(features.delivery_feature_open({"spec": spec}))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET execution_state='blocked',phase='blocked',"
                       "outcome='blocked' WHERE run_id=?", (spec["run_id"],))
            revision = db.execute("SELECT revision FROM delivery_runs WHERE run_id=?",
                                  (spec["run_id"],)).fetchone()[0]
        with pytest.raises(ValueError, match="writer.*owns"):
            create_checkpoint(store, spec["run_id"], "cannot-ack-feature-post", revision)
        assert not external.exists()
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 5)
    posts = [json.loads(line) for line in external.read_text().splitlines()]
    assert len(posts) == 1 and posts[0]["method"] == "POST"
    with store._connect() as db:
        assert db.execute(
            "SELECT 1 FROM delivery_commands WHERE "
            "command_id='checkpoint-create:cannot-ack-feature-post'").fetchone() is None
