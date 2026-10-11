"""Configuration edits preserve admitted authority and the owning transport."""

from __future__ import annotations

import copy
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from test_delivery_store import service as source_service

from devflow_temporal.delivery_activities import _context, delivery_project
from devflow_temporal.delivery_api import DeliveryService
from devflow_temporal.delivery_config import DeliveryConfig, scope_amended_spec
from devflow_temporal.delivery_execution_config import (
    load_execution_config,
    preserve_execution_config,
)
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.supervisor import get_supervisor


@pytest.fixture
def service(tmp_path):
    return source_service.__wrapped__(tmp_path)


def edit_configuration(store, change):
    raw = copy.deepcopy(store.config.raw)
    change(raw)
    store.config.path.write_text(json.dumps(raw))
    return raw


def snapshot(store, spec):
    return store.config.state_root / "configuration-snapshots" / (spec["config_digest"] + ".json")


@pytest.mark.asyncio
async def test_capacity_edit_keeps_admitted_dispatch_activity_and_projection(service, monkeypatch):
    store, request = service
    store.submit(request)
    original = copy.deepcopy(store.effective_spec(request["run_id"]))
    frozen = snapshot(store, original).read_bytes()
    scheduler = get_supervisor(store)
    assert scheduler.capacity == 2
    edit_configuration(store, lambda raw: raw.update(capacity=3))
    assert store.owns_execution(original)
    assert get_supervisor(store) is scheduler
    assert scheduler.capacity == 3
    active, broker = _context(original)
    assert active.config.raw == store.config.raw
    assert broker.spec == original
    assert original["policy"]["capacity"] == 2
    assert active.config.path == store.config.path
    assert snapshot(store, original).read_bytes() == frozen

    service = object.__new__(DeliveryService)
    service.config, service.store = store.config, store
    selected = []

    async def own_transport():
        selected.append(True)
        raise RuntimeError("own transport selected")

    monkeypatch.setattr(service, "healthy_client", own_transport)
    with pytest.raises(RuntimeError, match="own transport selected"):
        await service.dispatch_once()
    assert selected == [True]
    observed = await delivery_project({
        "spec": original, "phase": "blocked", "execution_state": "blocked",
        "event_type": "blocked", "message": "fixture terminal projection",
        "outcome": "blocked", "key": "capacity-drift-projection",
    })
    assert observed["phase"] == "blocked"
    assert store.submitted_spec(request["run_id"]) == original
    assert store.detail(request["run_id"])["outcome"] == "blocked"


def test_stale_new_admission_rejects_but_command_and_run_replays_remain_exact(service):
    store, request = service
    accepted = store.submit(request)
    edit_configuration(store, lambda raw: raw.update(capacity=3))
    assert store.submit(request) == accepted
    assert store.submit({**request, "command_id": "replay-command"})["existing"] is True
    new = {**request, "run_id": "run-2", "command_id": "command-2", "work_id": "work-2",
           "issue_url": "https://github.com/example/fixture/issues/4", "branch": "feat/new"}
    with pytest.raises(ValueError, match="reload the service before new admission"):
        store.submit(new)
    with store._connect() as db:
        assert db.execute("SELECT 1 FROM delivery_runs WHERE run_id='run-2'").fetchone() is None
        assert db.execute("SELECT 1 FROM delivery_outbox WHERE run_id='run-2'").fetchone() is None
    current = DeliveryStore(DeliveryConfig.load(store.config.path))
    assert current.submit(new)["existing"] is False
    fresh = current.effective_spec("run-2")
    assert fresh["policy"]["capacity"] == 3
    assert current.owns_execution(fresh)
    assert current.owns_execution(store.effective_spec(request["run_id"]))


@pytest.mark.parametrize("field,value", [
    ("temporal_address", "127.0.0.1:19099"), ("temporal_namespace", "different"),
    ("queue", "different-queue"), ("dashboard_url", "http://127.0.0.1:19098"),
])
def test_another_service_transport_cannot_claim_original_execution(service, field, value):
    store, request = service
    store.submit(request)
    original = store.effective_spec(request["run_id"])
    edit_configuration(store, lambda raw: raw.update({field: value}))
    other = DeliveryStore(DeliveryConfig.load(store.config.path))
    assert not other.owns_execution(original)
    assert store.owns_execution(original)
    assert load_execution_config(original).raw == store.config.raw


def test_live_role_scope_and_command_changes_do_not_enter_admitted_execution(service):
    store, request = service
    store.submit(request)
    spec = copy.deepcopy(store.effective_spec(request["run_id"]))

    def alter(raw):
        raw["roles"]["implement"]["model"] = "a-different-model"
        raw["repositories"]["fixture"]["allowed_paths"].append("other.txt")
        raw["codex_bin"] = "/usr/bin/true"
        raw["max_repairs"] = 100

    edit_configuration(store, alter)
    execution, _ = _context(spec)
    assert execution.config.raw == store.config.raw
    assert execution.config.raw["codex_bin"] == "/usr/bin/false"
    assert execution.effective_spec(request["run_id"])["policy"] == spec["policy"]
    assert execution.effective_spec(request["run_id"])["policy"]["max_repairs"] == 2
    tampered = copy.deepcopy(spec)
    tampered["policy"]["allowed_paths"].append("other.txt")
    assert not store.owns_execution(tampered)
    with pytest.raises(ValueError, match="policy changed"):
        load_execution_config(tampered)


@pytest.mark.parametrize("tamper", ["content", "symlink", "mode"])
def test_tampered_snapshot_never_falls_back_or_overwrites(service, tamper):
    store, request = service
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    path = snapshot(store, spec)
    if tamper == "content":
        raw = json.loads(path.read_bytes())
        raw["capacity"] = 4
        path.write_text(json.dumps(raw))
    elif tamper == "symlink":
        path.unlink()
        path.symlink_to(store.config.path)
    else:
        path.chmod(0o644)
    before = path.lstat().st_ino, path.lstat().st_mode
    assert not store.owns_execution(spec)
    with pytest.raises(ValueError):
        load_execution_config(spec)
    with pytest.raises(ValueError):
        preserve_execution_config(store.config)
    assert (path.lstat().st_ino, path.lstat().st_mode) == before


def test_legacy_exact_live_config_reads_without_creating_a_snapshot(service):
    store, request = service
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    path = snapshot(store, spec)
    path.unlink()
    folder = path.parent
    folder.rmdir()
    before = set(store.config.state_root.rglob("*"))
    assert load_execution_config(spec).raw == store.config.raw
    assert store.owns_execution(spec)
    assert set(store.config.state_root.rglob("*")) == before
    edit_configuration(store, lambda raw: raw.update(capacity=3))
    assert not store.owns_execution(spec)
    with pytest.raises(ValueError, match="unavailable or changed"):
        load_execution_config(spec)
    assert not folder.exists()


def test_snapshot_is_private_and_replay_preserves_original_bytes(service):
    store, request = service
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    path = snapshot(store, spec)
    before = path.read_bytes(), path.stat().st_ino
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    assert path.stat().st_nlink == 1
    preserve_execution_config(store.config)
    assert (path.read_bytes(), path.stat().st_ino) == before
    # Read-only ownership remains valid even if the mutable source disappears.
    store.config.path.unlink()
    assert load_execution_config(spec).path == store.config.path
    assert store.owns_execution(spec)


def test_new_boundary_marker_is_not_injected_into_a_legacy_amendment(service, monkeypatch):
    store, request = service
    store.submit(request)
    original = store.effective_spec(request["run_id"])
    assert original["workflow_failure_boundary_version"] == 1
    original.pop("workflow_failure_boundary_version")
    from devflow_temporal import delivery_preparation

    monkeypatch.setattr(delivery_preparation, "require_native_execution", lambda spec: None)
    amended = copy.deepcopy(store.config.raw)
    amended["repositories"]["fixture"]["allowed_paths"].append("additional.txt")
    path = store.config.state_root / "amended.json"
    path.write_text(json.dumps(amended))
    path.chmod(0o600)
    effective = scope_amended_spec(original, path, hashlib.sha256(path.read_bytes()).hexdigest(),
                                   ["additional.txt"])
    assert "workflow_failure_boundary_version" not in effective
    original["workflow_failure_boundary_version"] = 1
    effective = scope_amended_spec(original, path, hashlib.sha256(path.read_bytes()).hexdigest(),
                                   ["additional.txt"])
    assert effective["workflow_failure_boundary_version"] == 1


def test_snapshot_publication_is_complete_for_readers_and_concurrent_initializers(service,
                                                                                monkeypatch):
    store, request = service
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    snapshot(store, spec).unlink()
    from devflow_temporal import delivery_execution_config as configuration

    link = configuration.os.link
    observations = []

    def observe_link(stage, path, **kwargs):
        link(stage, path, **kwargs)
        observations.append(load_execution_config(spec).raw)

    monkeypatch.setattr(configuration.os, "link", observe_link)
    with ThreadPoolExecutor(max_workers=4) as workers:
        list(workers.map(lambda _: preserve_execution_config(store.config), range(8)))
    assert observations and all(raw == store.config.raw for raw in observations)
    assert snapshot(store, spec).stat().st_nlink == 1
    assert not list(snapshot(store, spec).parent.glob(".configuration-*"))
