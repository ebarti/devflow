"""Known finite measurement resources retain the canonical diagnostic fence."""

from __future__ import annotations

import copy
import json
import threading
from importlib.metadata import distribution
from pathlib import Path

import pytest
from test_delivery_store import service as service

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_config import DeliveryConfig
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
        readiness={"version": 1, "generation": 0, "browser_engines": [],
                   "temporal_checks": []},
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
