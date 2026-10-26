from contextlib import contextmanager
from pathlib import Path

import pytest
from test_delivery_preparation_resource_fence import (
    measurement_owner as measurement_owner,
)
from test_delivery_preparation_resource_fence import probe_for
from test_delivery_store import service as service

from devflow_temporal import (
    delivery_filesystem_custody,
    delivery_orphans,
    delivery_resources,
    delivery_store,
)
from devflow_temporal.delivery_execution_config import observation_mode, observation_scope
from devflow_temporal.delivery_preparation_resources import canonical_owner
from devflow_temporal.delivery_resources import write_private


def test_nested_custody_readers_share_one_tracking_snapshot(measurement_owner, monkeypatch):
    store, spec, registry, _intent, _path = measurement_owner
    probe = probe_for(measurement_owner)
    path = Path(spec["state_dir"]) / "attempts" / "missing-attempt" / "native-process.json"
    write_private(path, {"intent": {"policy_digest": spec["policy_digest"]},
                         "provider_session": {}})
    before = store.config.tracking_db.read_bytes()
    real = delivery_store.tracking_read_connection
    snapshots = []

    @contextmanager
    def observed(tracking):
        with real(tracking) as db:
            snapshots.append(observation_scope().resources[("tracking", str(tracking))])
            yield db

    monkeypatch.setattr(delivery_store, "tracking_read_connection", observed)
    with observation_mode():
        assert canonical_owner(probe)["state_dir"] == spec["state_dir"]
        assert delivery_resources._gate_roots(spec)[0] == Path(spec["state_dir"])
        assert delivery_filesystem_custody._acknowledged(spec) is None

        def roots(value, *, execution_specs, **_kwargs):
            execution_specs[Path(value["state_dir"])] = value
            return Path(value["state_dir"]), {Path(value["state_dir"])}

        monkeypatch.setattr(delivery_orphans, "_gate_roots", roots)
        monkeypatch.setattr(delivery_orphans, "_launch_binding", lambda *_: None)
        with pytest.raises(ValueError, match="no durable original attempt"):
            delivery_orphans._observe_registered_process(spec, path)
        monkeypatch.setattr(delivery_resources, "_gate_roots", roots)
        with pytest.raises(ValueError, match="no durable original attempt"):
            with registry._closed_journals({"processes": [str(path)]}, [path]):
                pass
        assert len(snapshots) == 5 and len({id(value) for value in snapshots}) == 1
    assert store.config.tracking_db.read_bytes() == before
