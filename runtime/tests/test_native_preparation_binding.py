"""Preparation measurement journals bind to the digest they were measured with."""
from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_automatic_retry import stopped
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_native import native_store as native_store
from test_delivery_store import service as service
from test_native_interrupted_gate import native_gates as native_gates

from devflow_temporal import delivery_automatic_retry as retry
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
    assert spec["preparation"]["measured_policy_digest"] == submitted["policy_digest"]
    measured = _measurement_journals(spec)
    assert measured
    assert all(read_private(path)["intent"]["policy_digest"] == submitted["policy_digest"]
               for path in measured)
    execution = store.intake_execution_spec(spec["run_id"])
    assert execution["preparation"]["measured_policy_digest"] == submitted["policy_digest"]
    return execution, measured


@pytest.mark.asyncio
@pytest.mark.parametrize("foreign", [None, "policy", "run"])
async def test_trusted_local_measurement_journals_bind_to_the_measured_digest(
        native_gates, foreign):
    store, spec = native_gates.store, native_gates.spec
    result = await asyncio.get_running_loop().run_in_executor(
        None, native_gates.broker.run_checks, 0, native_gates.candidate)
    assert result["state"] == "passed", result
    execution, measured = _assert_measured_binding(store, spec)
    if foreign == "policy":
        _clone_measurement(spec, measured[0], "foreign-policy", policy_digest=spec["policy_digest"])
    if foreign == "run":
        _clone_measurement(spec, measured[0], "foreign-run", run_id="run-elsewhere")
    assert RunResources(spec).finalize("blocked")["outcome"] == "blocked"
    if foreign is None:
        # Measurement journals (submitted digest) and gate journals (prepared
        # digest) now authenticate together, under either spec view.
        assert observe_completed_resources(execution, [])["journal_sha256"]
        assert observe_completed_resources(spec, [])["journal_sha256"]
        return
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
        assert observe_completed_resources(execution, [])["journal_sha256"]
    finally:
        assert RunResources(prepared).finalize("blocked")["resource_cleanup"] == "confirmed"


def test_automatic_retry_skip_is_logged_and_recorded(service, monkeypatch, caplog):
    store, request, _closed = stopped(service, monkeypatch)

    # Any fresh observation the retry depends on may fail; the base readback is
    # the one this closed fixture run reaches.
    def unconfirmed(*_args, **_kwargs):
        raise ValueError("original completed provider binding is unconfirmed")

    monkeypatch.setattr(retry, "fresh_unpublished_base", unconfirmed)
    with caplog.at_level(logging.WARNING, logger=retry.__name__):
        assert retry.retry_once(store) == []
    assert any(record.levelno == logging.WARNING and request["run_id"] in record.getMessage()
               and "binding is unconfirmed" in record.getMessage() for record in caplog.records)

    def skipped():
        return [event["payload"]["reason"] for event in store.events(request["run_id"])
                if event["type"] == "automatic_retry_skipped"]

    assert skipped() == ["ValueError: original completed provider binding is unconfirmed"]
    # The same reason on the next pass is not recorded again; a new one is.
    store._automatic_retry_waits = {}
    assert retry.retry_once(store) == []
    assert len(skipped()) == 1

    def unavailable(*_args, **_kwargs):
        raise OSError("lsof unavailable")

    monkeypatch.setattr(retry, "fresh_unpublished_base", unavailable)
    store._automatic_retry_waits = {}
    assert retry.retry_once(store) == []
    assert skipped()[-1] == "OSError: lsof unavailable" and len(skipped()) == 2
    with store._connect() as db:
        assert db.execute("SELECT outcome FROM delivery_runs WHERE run_id=?",
                          (request["run_id"],)).fetchone()[0] == "blocked"
