"""Configuration edits preserve admitted authority and the owning transport."""

from __future__ import annotations

import copy
import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from test_delivery_store import service as source_service

from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_execution_config import (
    load_execution_config,
    preserve_execution_config,
)
from devflow_temporal.delivery_store import DeliveryStore


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
