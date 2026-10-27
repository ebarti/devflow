"""Feature checkpoint ACK projects from its already-held query-only registry."""

from __future__ import annotations

import json
import shutil
import threading
from contextlib import contextmanager
from copy import deepcopy

import pytest
from test_delivery_checkpoints import durable_fence
from test_delivery_feature_execution import feature_service
from test_delivery_store import service as service

from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_checkpoint_registry_view import (
    CheckpointRegistryView,
    PreparedRegistryProjection,
)
from devflow_temporal.delivery_checkpoints import create_checkpoint, restore_checkpoint
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_execution_registry import ExecutionRegistry
from devflow_temporal.delivery_feature_execution import (
    register_worker,
    registry,
    worker_spec,
)
from devflow_temporal.delivery_github_contract import ordered_chunks
from devflow_temporal.delivery_registry_epoch import observe, ordered_writer_fences
from devflow_temporal.delivery_resources import RunResources
from devflow_temporal.delivery_store import DeliveryStore


def _checkout(store, spec):
    broker = DeliveryBroker(store, spec)
    broker.checkout.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    resources = RunResources(spec)
    resources.register(broker.checkout, "checkout")
    broker.prepare()
    resources.created(broker.checkout)
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET phase='blocked',execution_state='blocked',"
                   "outcome='blocked' WHERE run_id=?", (spec["run_id"],))
        revision = db.execute("SELECT revision FROM delivery_runs WHERE run_id=?",
                              (spec["run_id"],)).fetchone()[0]
    return broker, revision


def test_feature_create_restore_and_worker_parent_projection_reuse_fenced_registry(
    service, monkeypatch,
):
    old, request = service
    raw = deepcopy(old.config.raw)
    raw["workspace_custody_version"] = 1
    old.config.path.write_text(json.dumps(raw))
    configured = DeliveryStore(DeliveryConfig.load(old.config.path))
    store, request, _ = feature_service((configured, request), monkeypatch)
    store.submit(request)
    parent = store.effective_spec(request["run_id"])
    broker, revision = _checkout(store, parent)
    guarded = threading.Event()
    original = ExecutionRegistry.__init__

    def constructor(owner, *args, **kwargs):
        assert not guarded.is_set(), "projection reopened/bootstraped its held registry"
        return original(owner, *args, **kwargs)

    monkeypatch.setattr(ExecutionRegistry, "__init__", constructor)
    original_budget = ExecutionRegistry._budget
    original_pending = ExecutionRegistry._pending_revision

    def budget(db, issue_id):
        assert not guarded.is_set(), "projection parsed or hashed repair history under its fence"
        return original_budget(db, issue_id)

    def pending_revision(db, issue_id):
        assert not guarded.is_set(), "projection scanned revision history under its fence"
        return original_pending(db, issue_id)

    monkeypatch.setattr(ExecutionRegistry, "_budget", staticmethod(budget))
    monkeypatch.setattr(ExecutionRegistry, "_pending_revision", staticmethod(pending_revision))

    @contextmanager
    def admission():
        guarded.set()
        try:
            yield
        finally:
            guarded.clear()

    def cycle(spec, owner, revision, suffix, **control):
        shared = registry(spec)
        with shared.connect() as db:
            before = [tuple(row) for row in db.execute(
                "SELECT * FROM execution_authority_epochs ORDER BY kind,key")]
        saved = create_checkpoint(store, spec["run_id"], "save-" + suffix, revision,
                                  admission_context=admission, **control)
        shutil.rmtree(owner.checkout)
        restored = restore_checkpoint(store, spec["run_id"], saved["checkpoint_id"],
                                          "restore-" + suffix, revision,
                                          admission_context=admission, **control)
        assert restored["restored"] and owner.checkout.is_dir()
        with shared.connect() as db:
            assert [tuple(row) for row in db.execute(
                "SELECT * FROM execution_authority_epochs ORDER BY kind,key")] == before
        return saved

    cycle(parent, broker, revision, "parent")
    chunk = ordered_chunks(json.loads(parent["accepted_plan"]))[0]
    child = worker_spec(parent, chunk, {"url": "https://github.com/example/fixture/issues/10"},
                        kind="build", base_sha=parent["base_sha"], base_branch="main")
    register_worker(store, parent, child)
    child_broker, child_revision = _checkout(store, child)
    # This fixture's durable pause receipt permits retaining the still-owned
    # worker assignment; it does not invent a completed native worker verdict.
    cycle(child, child_broker, child_revision, "worker",
          fence=durable_fence(store, child), checkpoint_context={"owned_fixture_cursor": 1})


def test_registry_projection_view_cannot_write_close_or_outlive_its_fence(service, monkeypatch):
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    shared = registry(spec)
    with observe(shared.path, spec["feature_delivery"]["owner"]["issue_id"],
                 spec["issue_url"].rstrip("/").casefold(),
                 str(store.config.tracking_db)) as observation:
        prepared = PreparedRegistryProjection(spec, observation)
        with ordered_writer_fences([observation]) as fences:
            db = fences.connection(shared.path)
            view = CheckpointRegistryView(db, spec, prepared)
            assert view.current(spec["feature_delivery"]["owner"]["issue_id"])
            with view.connect() as query:
                with pytest.raises(ValueError, match="queries only"):
                    query.execute("UPDATE execution_claims SET state='stopped'")
                with pytest.raises(ValueError, match="queries only"):
                    query.execute("COMMIT")
            assert db.in_transaction
    with pytest.raises(ValueError, match="closed|held transaction"):
        view.current(spec["feature_delivery"]["owner"]["issue_id"])
