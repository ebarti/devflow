"""Known finite measurement resources retain the canonical diagnostic fence."""

from __future__ import annotations

import copy
import fcntl
import json
import os
import threading
from importlib.metadata import distribution
from pathlib import Path

import pytest
from test_delivery_store import service as service

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_native_diagnostic import ERROR, require_resource_idle
from devflow_temporal.delivery_native_renewal import _seal_probe_anchor
from devflow_temporal.delivery_policy_recovery import _prepare
from devflow_temporal.delivery_preparation import PreparationError, require_native_execution
from devflow_temporal.delivery_preparation_resources import (
    FIELD,
    binding,
    canonical_owner,
    resource_scope,
)
from devflow_temporal.delivery_resources import RunResources, read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore


@pytest.fixture
def measurement_owner(service):
    old, request = service
    raw = copy.deepcopy(old.config.raw)
    raw.update(
        provider="codex",
        execution_mode="trusted-local",
        codex_bin=str(distribution("openai-codex-cli-bin").locate_file("codex_cli_bin/bin/codex")),
    )
    raw["repositories"]["fixture"].update(
        prepublish_checks=[{"id": "prepublish", "argv": ["/usr/bin/true"]}],
        checks=[{"id": "local", "argv": ["/usr/bin/true"]}],
        required_ci=["owned-ci"],
        project_url="https://github.com/users/example/projects/1",
        assignee="example",
    )
    old.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(old.config.path))
    store.submit(request)
    spec = store.submitted_spec("run-1")
    registry = RunResources(spec)
    registry.scratch("preparation", "owned-initial-generation")
    assert registry.finalize("blocked")["state"] == "confirmed"
    path = Path(spec["state_dir"]) / "owned-runtime-preparation/intent.json"
    intent = {
        "command_digest": digest("owned-exact-command"),
        "seal_digest": digest({"spec": spec}),
        "preparation_attempts": [],
    }
    write_private(path, intent)
    return store, spec, registry, intent, path


def probe_for(owner, *, legacy=False):
    _store, original, _registry, intent, path = owner
    probe = copy.deepcopy(original)
    probe["state_dir"] = str(path.parent / "probes/0" / original["run_id"])
    if not legacy:
        probe[FIELD] = binding(original, intent, path, 0)
    intent["preparation_attempts"] = [{"spec": probe, "state": "started", "cleanup": None}]
    write_private(path, intent)
    return probe


def active(store, spec):
    # Opaque ACTIVE diagnostic metadata is the unit boundary here. Actual public
    # ACK, intent/lease generation and execution are covered by diagnostic tests.
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_native_diagnostics VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                "0" * 64,
                "opaque-held-diagnostic",
                spec["run_id"],
                spec["work_id"],
                store.state.issue_resource(spec["issue_url"]),
                "1" * 64,
                "{}",
                "2" * 64,
                "unknown",
                "{}",
                store.state.now(),
            ),
        )


def test_uncached_real_native_measurement_keeps_canonical_rows_and_resources(measurement_owner):
    store, original, registry, intent, path = measurement_owner
    before = {p: p.read_bytes() for p in (registry.manifest, registry.root / "finalization.json")}
    with store._connect() as db:
        original_row = dict(db.execute("SELECT * FROM delivery_runs").fetchone())
    effective = _prepare(original, store.config, intent, path)
    assert "preparation" in effective and FIELD not in effective
    saved = read_private(path)
    assert saved["state"] == "prepared" and len(saved["preparation_attempts"]) == 1
    attempt = saved["preparation_attempts"][0]
    assert attempt["state"] == "stopped" and attempt["cleanup"]["state"] == "confirmed"
    probe = attempt["spec"]
    assert canonical_owner(probe)["state_dir"] == original["state_dir"]
    journals = list(Path(probe["state_dir"]).glob("native-preparation/*/*/native-process.json"))
    assert len(journals) == 2
    for item in journals:
        value = read_private(item)
        assert value["phase"] == "finished" and value["monitoring_complete"] is True
        assert value["result"]["exit_code"] == 0
        assert value["result"]["cleanup"] == "observed-native-confirmed"
        assert value["runtime_identity"]["dirty"] is False
    assert all(p.read_bytes() == old for p, old in before.items())
    with store._connect() as db:
        assert dict(db.execute("SELECT * FROM delivery_runs").fetchone()) == original_row
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 0
    with pytest.raises(PreparationError, match="measurement resource metadata"):
        require_native_execution(probe)


@pytest.mark.parametrize("legacy", [False, True])
def test_exact_new_and_retained_probe_context_use_original_run_fence(measurement_owner, legacy):
    store, original, registry, intent, path = measurement_owner
    probe = probe_for(measurement_owner, legacy=legacy)
    original_intent = path.read_bytes()
    before = registry.manifest.read_bytes()
    if legacy:
        with pytest.raises(ValueError, match="admitted configuration identity"):
            require_resource_idle(probe)
    with resource_scope(probe, original["state_dir"], intent, path, 0):
        assert RunResources(probe).finalize("blocked")["state"] == "confirmed"
    assert path.read_bytes() == original_intent and registry.manifest.read_bytes() == before
    active(store, original)
    with pytest.raises(ValueError, match=ERROR):
        with resource_scope(probe, original["state_dir"], intent, path, 0):
            pytest.fail("active diagnostic must fence either finite probe form")


@pytest.mark.parametrize(
    "defect",
    [
        "missing_marker",
        "parent_digest",
        "foreign_database",
        "foreign_request",
        "coherent_foreign_database",
        "coherent_foreign_request",
        "different_purpose",
        "index",
        "state_path",
        "intent_path",
        "missing_intent",
        "intent_spec",
        "intent_authority",
        "intent_state",
        "intent_symlink",
        "intent_link",
        "intent_fifo",
    ],
)
def test_resource_fence_refuses_unknown_or_changed_probe_context(measurement_owner, defect):
    _store, _original, _registry, _intent, path = measurement_owner
    probe = copy.deepcopy(probe_for(measurement_owner))
    if defect == "missing_marker":
        probe.pop(FIELD)
    elif defect == "parent_digest":
        probe[FIELD]["canonical_spec_sha256"] = "0" * 64
    elif defect == "foreign_database":
        probe["policy"]["tracking_db"] += "-foreign"
        probe["policy_digest"] = digest(probe["policy"])
    elif defect == "foreign_request":
        probe["request_digest"] = "0" * 64
    elif defect in {"coherent_foreign_database", "coherent_foreign_request"}:
        if defect == "coherent_foreign_database":
            probe["policy"]["tracking_db"] += "-foreign"
            probe["policy_digest"] = digest(probe["policy"])
        else:
            probe["request_digest"] = "0" * 64
        parent = {key: value for key, value in probe.items() if key != FIELD}
        parent["state_dir"] = probe[FIELD]["canonical_state_dir"]
        probe[FIELD]["canonical_spec_sha256"] = digest(parent)
        intent = read_private(path)
        intent["preparation_attempts"][0]["spec"] = probe
        write_private(path, intent)
    elif defect == "different_purpose":
        probe["native_diagnostic_resource_only"] = True
    elif defect == "index":
        probe[FIELD]["attempt_index"] = 2
    elif defect == "state_path":
        probe["state_dir"] = str(path.parent / "unregistered" / probe["run_id"])
    elif defect == "intent_path":
        probe[FIELD]["intent_path"] = str(path.with_name("another-intent.json"))
    elif defect == "missing_intent":
        path.unlink()
    elif defect.startswith("intent_"):
        value = read_private(path)
        if defect == "intent_spec":
            value["preparation_attempts"][0]["spec"]["request_digest"] = "0" * 64
            write_private(path, value)
        elif defect == "intent_authority":
            value["command_digest"] = "0" * 64
            write_private(path, value)
        elif defect == "intent_state":
            value["preparation_attempts"][0]["state"] = "unknown"
            write_private(path, value)
        elif defect == "intent_symlink":
            held = path.with_suffix(".held")
            path.rename(held)
            path.symlink_to(held)
        elif defect == "intent_link":
            os.link(path, path.with_suffix(".linked"))
        else:
            path.unlink()
            os.mkfifo(path, mode=0o600)
    with pytest.raises((ValueError, OSError)):
        require_resource_idle(probe)


def test_waiting_probe_rechecks_active_diagnostic_after_original_exclusive_lease(
    measurement_owner,
    monkeypatch,
):
    store, original, registry, intent, path = measurement_owner
    probe = probe_for(measurement_owner)
    descriptor = os.open(registry.root / "ownership.lock", os.O_RDONLY)
    fcntl.flock(descriptor, fcntl.LOCK_SH)
    identity = os.fstat(descriptor)
    waiting = threading.Event()
    attempted = []
    real_flock = fcntl.flock

    def observe(fd, operation):
        current = os.fstat(fd)
        if operation == fcntl.LOCK_EX and (current.st_dev, current.st_ino) == (
            identity.st_dev,
            identity.st_ino,
        ):
            waiting.set()
        return real_flock(fd, operation)

    monkeypatch.setattr(fcntl, "flock", observe)

    def prepare():
        try:
            with resource_scope(probe, original["state_dir"], intent, path, 0):
                attempted.append("UNSAFE measurement admitted")
        except ValueError as exc:
            attempted.append(str(exc))

    worker = threading.Thread(target=prepare)
    worker.start()
    try:
        assert waiting.wait(5)
        active(store, original)
    finally:
        os.close(descriptor)
    worker.join(5)
    assert not worker.is_alive() and attempted == [ERROR]
    assert not Path(probe["state_dir"]).exists()


def test_probe_exclusive_parent_lease_serializes_the_diagnostic_ack_observation(measurement_owner):
    _store, original, registry, intent, path = measurement_owner
    probe = probe_for(measurement_owner)
    entered, finished = threading.Event(), threading.Event()

    def ack_observer():
        entered.set()
        with RunResources(original, read_only=True).locked(read_only=True):
            finished.set()

    with resource_scope(probe, original["state_dir"], intent, path, 0):
        worker = threading.Thread(target=ack_observer)
        worker.start()
        assert entered.wait(5)
        assert not finished.wait(0.1)
        assert registry.manifest.is_file()
    worker.join(5)
    assert finished.is_set() and not worker.is_alive()


@pytest.mark.parametrize("namespace", [
    "native-preparation-renewal",
    "technical-successor/native-generation",
    "resource-closure/native-generation",
])
@pytest.mark.parametrize("defect", [None, "missing_anchor", "wrong_namespace",
                                    "authority_changed", "intent_changed"])
def test_renewal_probe_requires_exact_durable_namespace_and_authority(
    measurement_owner, namespace, defect,
):
    store, original, registry, _, _ = measurement_owner
    root = Path(original["state_dir"]) / namespace
    if defect == "wrong_namespace":
        root = root.parent / "foreign-generation"
    path = root / "preparation.json"
    authority = {"original_spec": original, "command_digest": digest("command")}
    write_private(root / "authority.json", authority)
    intent = {"binding_sha256": digest(authority), "preparation_attempts": []}
    owner = (store, original, registry, intent, path)
    if defect != "missing_anchor":
        _seal_probe_anchor(store.config, original, namespace, authority, original)
    probe = probe_for(owner)
    if defect == "authority_changed":
        write_private(root / "authority.json", {**authority, "command_digest": digest("changed")})
    elif defect == "intent_changed":
        intent["binding_sha256"] = digest("changed")
        write_private(path, intent)
    if defect:
        with pytest.raises(ValueError):
            require_resource_idle(probe)
        with pytest.raises(ValueError):
            with resource_scope(probe, original["state_dir"], intent, path, 0,
                                renewal_binding=authority, renewal_parent=original):
                pytest.fail("changed renewal authority was admitted")
    else:
        assert canonical_owner(probe) == original
        require_resource_idle(probe)
        with resource_scope(probe, original["state_dir"], intent, path, 0,
                            renewal_binding=authority, renewal_parent=original):
            assert RunResources(probe).finalize("blocked")["state"] == "confirmed"


@pytest.mark.parametrize("namespace", [
    "native-preparation-renewal", "technical-successor/native-generation",
    "resource-closure/native-generation",
])
@pytest.mark.parametrize("defect", ["coherent_authority", "coherent_parent"])
def test_renewal_probe_refuses_coherent_record_substitution(measurement_owner, namespace, defect):
    store, original, registry, _, _ = measurement_owner
    root = Path(original["state_dir"]) / namespace
    path = root / "preparation.json"
    authority = {"original_spec": original, "command_digest": digest("original")}
    write_private(root / "authority.json", authority)
    _seal_probe_anchor(store.config, original, namespace, authority, original)
    intent = {"binding_sha256": digest(authority), "preparation_attempts": []}
    probe = probe_for((store, original, registry, intent, path))
    parent = copy.deepcopy(original)
    if defect == "coherent_authority":
        changed = {**authority, "command_digest": digest("foreign")}
        write_private(root / "authority.json", changed)
        intent["binding_sha256"] = digest(changed)
    else:
        parent["policy"]["max_repairs"] += 1
        parent["policy_digest"] = digest(parent["policy"])
        probe["policy"] = parent["policy"]
        probe["policy_digest"] = parent["policy_digest"]
    probe[FIELD] = binding(parent, intent, path, 0)
    intent["preparation_attempts"][0]["spec"] = probe
    write_private(path, intent)
    with pytest.raises(ValueError):
        with resource_scope(probe, original["state_dir"], intent, path, 0,
                            renewal_binding=authority, renewal_parent=original):
            pytest.fail("coherent substituted renewal was admitted")


def test_renewal_probe_anchor_is_insert_once(measurement_owner):
    store, original, _, _, _ = measurement_owner
    namespace = "native-preparation-renewal"
    authority = {"original_spec": original, "command_digest": digest("original")}
    _seal_probe_anchor(store.config, original, namespace, authority, original)
    _seal_probe_anchor(store.config, original, namespace, authority, original)
    with pytest.raises(ValueError, match="conflicts with its durable probe authority"):
        _seal_probe_anchor(store.config, original, namespace,
                           {**authority, "command_digest": digest("foreign")}, original)
    with store._connect() as db:
        rows = db.execute("SELECT binding_sha256,parent_sha256 FROM "
                          "delivery_native_renewal_anchors WHERE run_id=? AND namespace=?",
                          (original["run_id"], namespace)).fetchall()
    assert [tuple(row) for row in rows] == [(digest(authority), digest(original))]


def test_renewal_anchor_schema_migrates_existing_tracking_store(measurement_owner):
    store, original, _, _, _ = measurement_owner
    with store._connect() as db:
        db.execute("DROP TABLE delivery_native_renewal_anchors")
    upgraded = DeliveryStore(DeliveryConfig.load(store.config.path))
    with upgraded._connect() as db:
        assert db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='delivery_native_renewal_anchors'"
        ).fetchone()
        row = db.execute(
            "SELECT request_digest,work_id FROM delivery_runs WHERE run_id=?",
            (original["run_id"],),
        ).fetchone()
    assert tuple(row) == (original["request_digest"], original["work_id"])


def test_renewal_probe_refuses_when_anchor_table_is_absent(measurement_owner):
    store, original, registry, _, _ = measurement_owner
    namespace = "native-preparation-renewal"
    root = Path(original["state_dir"]) / namespace
    path = root / "preparation.json"
    authority = {"original_spec": original, "command_digest": digest("original")}
    write_private(root / "authority.json", authority)
    _seal_probe_anchor(store.config, original, namespace, authority, original)
    intent = {"binding_sha256": digest(authority), "preparation_attempts": []}
    probe = probe_for((store, original, registry, intent, path))
    with store._connect() as db:
        db.execute("DROP TABLE delivery_native_renewal_anchors")
    with pytest.raises(ValueError, match="native renewal has no durable probe authority"):
        canonical_owner(probe)
