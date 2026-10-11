"""Configuration edits preserve admitted authority and the owning transport."""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import httpx
import pytest
from temporalio.service import RPCError, RPCStatusCode
from test_delivery_store import service as source_service

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_activities import _context, delivery_project
from devflow_temporal.delivery_api import DeliveryService, create_app
from devflow_temporal.delivery_config import DeliveryConfig, scope_amended_spec
from devflow_temporal.delivery_execution_config import (
    load_execution_config,
    preserve_execution_config,
    transport_digest,
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
    ("queue", "different-queue"),
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


@pytest.mark.parametrize("tamper", ["content", "symlink", "hardlink"])
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
        os.link(path, path.with_name("unrelated"))
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


def _dispatcher(store, started):
    """A DeliveryService on this store whose Temporal client only records starts."""
    service = object.__new__(DeliveryService)
    service.config, service.store = store.config, store

    async def missing():
        raise RPCError("not found", RPCStatusCode.NOT_FOUND, b"")

    class Temporal:
        def get_workflow_handle(self, _workflow_id):
            return SimpleNamespace(describe=missing)

        async def start_workflow(self, _workflow, **kwargs):
            started.append(kwargs["id"])

    async def healthy():
        return Temporal()

    service.healthy_client = healthy
    return service


def _second(request):
    return {**request, "run_id": "run-2", "command_id": "command-2", "work_id": "work-2",
            "issue_url": "https://github.com/example/fixture/issues/4", "branch": "feat/new"}


@pytest.mark.asyncio
async def test_dashboard_address_change_keeps_admitted_runs_owned_and_dispatchable(service):
    store, request = service
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    edit_configuration(store, lambda raw: raw.update(dashboard_url="http://127.0.0.1:19098"))
    moved = DeliveryStore(DeliveryConfig.load(store.config.path))
    assert moved.execution_ownership(spec) is None
    assert moved.owns_execution(spec)
    assert load_execution_config(spec).dashboard_url == store.config.dashboard_url
    started = []
    await _dispatcher(moved, started).dispatch_once()
    assert started == ["delivery-" + request["run_id"]]
    dispatch = moved.detail(request["run_id"])["dispatch"]
    assert (dispatch["state"], dispatch["last_error"]) == ("sent", None)
    assert moved.submit(_second(request))["existing"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("cause", ["transport", "snapshot"])
async def test_owner_sees_once_why_its_own_queued_run_is_not_dispatched(service, cause):
    store, request = service
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    if cause == "transport":
        edit_configuration(store, lambda raw: raw.update(queue="moved-queue"))
        expected = "admitted transport differs from this service in queue"
    else:
        # Upgraded, then edited before any start: the admitted bytes are gone.
        snapshot(store, spec).unlink()
        edit_configuration(store, lambda raw: raw.update(capacity=3))
        expected = "restart` once to seed the snapshot"
    current = DeliveryStore(DeliveryConfig.load(store.config.path))
    started = []
    dispatcher = _dispatcher(current, started)
    for _ in range(3):
        await dispatcher.dispatch_once()
    assert started == []
    detail = current.detail(request["run_id"])
    assert detail["phase"] == "accepted"
    assert detail["dispatch"]["state"] == "pending"
    assert expected in detail["dispatch"]["last_error"]
    blocked = [event for event in detail["events"] if event["type"] == "dispatch_blocked"]
    assert len(blocked) == 1 and expected in blocked[0]["message"]
    assert [row["run_id"] for row in current.pending_starts()] == [request["run_id"]]
    if cause == "transport":
        # The admitting transport still owns the run; nothing was acknowledged.
        assert store.owns_execution(spec)
        return
    stranded = current.seed_execution_snapshots()
    assert [item["run_id"] for item in stranded["unavailable"]] == [request["run_id"]]
    assert expected in stranded["unavailable"][0]["reason"]
    # The named remedy: restore the admitted bytes, start once, then edit again.
    store.config.path.write_text(json.dumps(store.config.raw))
    seeded = DeliveryStore(DeliveryConfig.load(store.config.path)).seed_execution_snapshots()
    assert seeded == {"published": spec["config_digest"], "unavailable": []}
    assert snapshot(store, spec).exists()
    edit_configuration(store, lambda raw: raw.update(capacity=3))
    await dispatcher.dispatch_once()
    assert started == ["delivery-" + request["run_id"]]
    assert current.detail(request["run_id"])["dispatch"]["last_error"] is None


@pytest.mark.parametrize("drift", ["directory", "file", "readonly-directory"])
def test_benign_snapshot_mode_drift_heals_in_place(service, drift):
    store, request = service
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    path = snapshot(store, spec)
    inode = path.stat().st_ino
    if drift == "directory":
        path.parent.chmod(0o755)
    elif drift == "file":
        path.chmod(0o644)
    else:
        path.parent.chmod(0o500)
    # Every surface that builds a store or reads the snapshot keeps working...
    healed = DeliveryStore(DeliveryConfig.load(store.config.path))
    assert healed.owns_execution(spec)
    assert load_execution_config(spec).raw == store.config.raw
    assert healed.submit(_second(request))["existing"] is False
    # ...and the drift is repaired in place rather than republished.
    assert (path.stat().st_mode & 0o777, path.parent.stat().st_mode & 0o777) == (0o600, 0o700)
    assert path.stat().st_ino == inode


def test_tampered_snapshot_degrades_only_admission_and_its_runs(service, caplog):
    store, request = service
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    path = snapshot(store, spec)
    raw = json.loads(path.read_bytes())
    raw["capacity"] = 4
    path.write_text(json.dumps(raw))
    with caplog.at_level(logging.WARNING):
        degraded = DeliveryStore(DeliveryConfig.load(store.config.path))
    assert "snapshot publication failed" in caplog.text
    # Read surfaces still answer; admission under the tampered identity and the
    # runs that need it refuse with the named condition.
    assert degraded.detail(request["run_id"])["phase"] == "accepted"
    with pytest.raises(ValueError, match="immutable admitted configuration changed"):
        degraded.submit(_second(request))
    assert not degraded.owns_execution(spec)
    assert path.stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
@pytest.mark.parametrize("problem", ["deleted", "invalid", "edited"])
async def test_unusable_live_configuration_is_a_conflict_that_names_restart(service, problem):
    store, request = service
    app = create_app(store.config.path)
    origin = store.config.dashboard_url.rstrip("/")
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url=origin) as browser:
        csrf = (await browser.get("/api/session")).json()["csrf_token"]
        headers = {"Origin": origin, "X-Devflow-CSRF": csrf}
        if problem == "deleted":
            store.config.path.unlink()
        elif problem == "invalid":
            store.config.path.write_text("{not json")
        else:
            edit_configuration(store, lambda raw: raw.update(capacity=3))
        response = await browser.post("/api/runs", json=request, headers=headers)
        assert response.status_code == 409
        detail = response.json()["detail"]
        assert f"devflow-delivery --config {store.config.path} restart" in detail
        condition = {"deleted": "unavailable", "invalid": "invalid", "edited": "changed"}
        assert condition[problem] in detail
        # Reads keep answering from the loaded configuration, whose identity the
        # lifecycle controller compares with the file to reload only the API.
        info = (await browser.get("/api/service")).json()
        assert info["config_digest"] == digest(store.config.raw)
        assert info["transport_digest"] == transport_digest(store.config)
        assert (await browser.get("/api/runs")).status_code == 200
