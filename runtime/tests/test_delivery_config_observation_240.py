"""Observations must not repair or publish execution configuration."""

import asyncio
import stat

import pytest
from test_delivery_store import service as source_service

from devflow_temporal.delivery_execution_config import (
    load_execution_config,
    observation_mode,
    preserve_execution_config,
)


@pytest.fixture
def service(tmp_path):
    return source_service.__wrapped__(tmp_path)


@pytest.mark.parametrize("target", ["root", "folder", "snapshot"])
def test_observation_refuses_mode_drift_without_changing_it(service, target):
    store, request = service
    store.submit(request)
    spec = store.submitted_spec(request["run_id"])
    root = store.config.state_root
    folder = root / "configuration-snapshots"
    snapshot = folder / (spec["config_digest"] + ".json")
    path = {"root": root, "folder": folder, "snapshot": snapshot}[target]
    drift = 0o644 if target == "snapshot" else 0o755
    path.chmod(drift)
    with observation_mode():
        with pytest.raises(ValueError, match="permissions"):
            load_execution_config(spec)
    assert stat.S_IMODE(path.stat().st_mode) == drift
    load_execution_config(spec)
    assert stat.S_IMODE(path.stat().st_mode) == (0o600 if target == "snapshot" else 0o700)


@pytest.mark.asyncio
async def test_observation_context_propagates_to_thread_and_blocks_publication(service):
    store, request = service
    store.submit(request)
    spec = store.submitted_spec(request["run_id"])
    path = store.config.state_root / "configuration-snapshots" / (spec["config_digest"] + ".json")
    before = path.read_bytes()
    path.chmod(0o644)
    with observation_mode():
        with pytest.raises(ValueError, match="permissions"):
            await asyncio.to_thread(load_execution_config, spec)
        with pytest.raises(ValueError, match="cannot publish"):
            preserve_execution_config(store.config)
    assert stat.S_IMODE(path.stat().st_mode) == 0o644
    assert path.read_bytes() == before
    load_execution_config(spec)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
