"""Preparation measurement journals bind to the digest they were measured with."""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest
from runtime_location_scenario import artifact_holder_failure_snapshot
from test_delivery_api import api_fixture as api_fixture
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_native import native_store as native_store
from test_delivery_store import service as service
from test_native_interrupted_gate import native_gates as native_gates

from devflow_temporal import delivery_resources as resources
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_preparation import prepare_authority
from devflow_temporal.delivery_resources import (
    RunResources,
    observe_completed_resources,
    private_directory,
    read_private,
    write_private,
)

pytestmark = pytest.mark.skipif(sys.platform != "darwin",
                                reason="native macOS execution authority")


def _measurement_journals(spec) -> list[Path]:
    state = Path(spec["state_dir"])
    manifest = read_private(RunResources(spec).manifest)
    return [Path(raw) for raw in manifest["processes"]
            if Path(raw).is_relative_to(state / "native-preparation")]


def _clone_measurement(spec, source: Path, name: str, **intent) -> Path:
    """Register a measurement journal of another run or spec next to the real ones."""
    folder = source.parent.parent / name
    private_directory(folder)
    journal = read_private(source)
    journal["intent"].update(intent)
    write_private(folder / "native-process.json", journal)
    write_private(folder / "launch.json", read_private(source.with_name("launch.json")))
    (folder / "native-process.lock").touch(mode=0o600)
    RunResources(spec).process(folder / "native-process.json")
    return folder / "native-process.json"


def _assert_measured_binding(store, spec):
    submitted = store.submitted_spec(spec["run_id"])
    assert spec["preparation"]["cache_reused"] is False
    assert submitted["policy_digest"] != spec["policy_digest"]
    measured = _measurement_journals(spec)
    assert measured
    assert all(read_private(path)["intent"]["policy_digest"] == submitted["policy_digest"]
               for path in measured)
    # The sealed record names the digest those journals were measured with.
    assert spec["preparation"].get("measured_policy_digest") == submitted["policy_digest"]
    execution = store.intake_execution_spec(spec["run_id"])
    assert execution["preparation"].get("measured_policy_digest") == submitted["policy_digest"]
    return execution, measured


@pytest.mark.asyncio
@pytest.mark.parametrize("foreign", [None, "run"])
async def test_trusted_local_measurement_journals_bind_to_the_measured_digest(
        native_gates, foreign):
    store, spec = native_gates.store, native_gates.spec
    result = await asyncio.get_running_loop().run_in_executor(
        None, native_gates.broker.run_checks, 0, native_gates.candidate)
    assert result["state"] == "passed", result
    execution, measured = _assert_measured_binding(store, spec)
    assert RunResources(spec).finalize("blocked")["outcome"] == "blocked"
    # Measurement journals (submitted digest) and gate journals (prepared digest)
    # authenticate together, under either spec view.
    assert observe_completed_resources(execution, [])["journal_sha256"]
    assert observe_completed_resources(spec, [])["journal_sha256"]
    if foreign is None:
        return
    # The same journal content claiming another spec's digest, or another run,
    # is still refused once registered and finalized alongside the real ones.
    _clone_measurement(spec, measured[0], f"foreign-{foreign}", **(
        {"policy_digest": spec["policy_digest"]} if foreign == "policy"
        else {"run_id": "run-elsewhere"}))
    assert RunResources(spec).finalize("blocked")["outcome"] == "blocked"
    with pytest.raises(ValueError, match="launch binding changed"):
        observe_completed_resources(execution, [])


def test_native_profile_measurement_journals_bind_to_the_measured_digest(native_store):
    store, request = native_store
    prepared = prepare_authority(store, store.submitted_spec(request["run_id"]))
    try:
        broker = DeliveryBroker(store, prepared)
        candidate = broker.prepare()["candidate"]
        assert broker.run_prechecks(0, candidate)["state"] == "passed"
        execution, _ = _assert_measured_binding(store, prepared)
        assert RunResources(prepared).finalize("blocked")["outcome"] == "blocked"
        try:
            assert observe_completed_resources(execution, [])["journal_sha256"]
        except ValueError as exc:
            if str(exc) == "predecessor has a live root user or unreadable lease":
                try:
                    facts = artifact_holder_failure_snapshot(
                        exc, resources._observe_locked_resources, root_observation=True)
                    if facts is not None:
                        exc.add_note("native preparation root observation: "
                                     + json.dumps(facts, sort_keys=True))
                except BaseException:
                    pass  # Diagnostic failure must preserve the original refusal.
            raise
    finally:
        assert RunResources(prepared).finalize("blocked")["resource_cleanup"] == "confirmed"
