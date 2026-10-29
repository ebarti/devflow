from __future__ import annotations

from contextlib import ExitStack

import pytest
from test_delivery_checkpoints import stopped as stopped
from test_delivery_store import service as service

from devflow_temporal.delivery_checkpoints import (
    _locked,
    _read_acknowledged,
    _root,
    create_checkpoint,
)
from devflow_temporal.delivery_resources import RunResources
from devflow_temporal.delivery_retained_checkpoint import (
    REVALIDATED,
    RetainedCheckpointCustody,
    _query,
)
from devflow_temporal.delivery_writer_custody import CheckpointWriters


@pytest.fixture
def retained(stopped):
    store, spec, broker, row = stopped
    receipt = create_checkpoint(store, spec["run_id"], "retained-unit", row["revision"])
    with ExitStack() as stack:
        stack.enter_context(_locked(spec))
        resources = stack.enter_context(RunResources(spec, read_only=True).locked(
            read_only=True, exclusive=True))
        stack.enter_context(CheckpointWriters(store, spec))
        with store._connect() as db:
            value, _ = _read_acknowledged(db, spec, receipt["checkpoint_id"])
        yield spec, broker, resources, value, _root(spec) / receipt["checkpoint_id"]


def test_source_only_ack_is_idempotent_and_never_grants_transition_authority(stopped):
    store, spec, broker, row = stopped
    checkpoint = create_checkpoint(store, spec["run_id"], "retained-create", row["revision"])
    original = broker.checkout.stat().st_ino
    with RetainedCheckpointCustody(store, spec["run_id"], checkpoint["checkpoint_id"],
                                  row["revision"]) as owner:
        receipt = owner.acknowledge("retained-bind")
        assert receipt["copied"] is False and receipt["execution_started"] is False
        with pytest.raises(ValueError, match="fresh custody scope"):
            owner.acknowledge("retained-bind")
    with pytest.raises(ValueError, match="live custody owner"):
        owner.acknowledge("expired")
    with RetainedCheckpointCustody(store, spec["run_id"], checkpoint["checkpoint_id"],
                                  row["revision"]) as retry:
        assert retry.acknowledge("retained-bind") == receipt
        with pytest.raises(TypeError):
            retry.acknowledge("retained-bind", sql_transition=lambda *_: pytest.fail("replayed"))
    assert broker.checkout.stat().st_ino == original
    with store._connect() as db:
        assert db.execute("SELECT count(*) FROM delivery_events WHERE run_id=? AND type=?",
                          (spec["run_id"], REVALIDATED)).fetchone()[0] == 1
    assert not list(broker.checkout.parent.glob(".checkpoint-restore-*"))


@pytest.mark.asyncio
async def test_public_retained_route_revalidates_without_copy_or_activation(stopped):
    import httpx

    from devflow_temporal.delivery_api import create_app

    store, spec, broker, row = stopped
    checkpoint = create_checkpoint(store, spec["run_id"], "retained-http", row["revision"])
    inode = broker.checkout.stat().st_ino
    app = create_app(store.config.path)
    url = store.config.dashboard_url.rstrip("/")
    path = ("/api/runs/" + spec["run_id"] + "/checkpoints/" + checkpoint["checkpoint_id"]
            + "/verify-retained")
    command = {"command_id": "retained-http-command", "expected_revision": row["revision"]}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=url) as client:
        assert (await client.post(path, json=command)).status_code == 403
        session = (await client.get("/api/session")).json()
        headers = {"origin": url, "x-devflow-csrf": session["csrf_token"]}
        first = await client.post(path, json=command, headers=headers)
        assert first.status_code == 200, first.text
        assert first.json()["retained_verified"] and not first.json()["execution_started"]
        assert first.json()["copied"] is False
        assert (await client.post(path, json=command, headers=headers)).json() == first.json()
        _query(broker.checkout, "update-index", "--assume-unchanged", "README.md")
        refused = await client.post(path, json=command, headers=headers)
        assert refused.status_code == 409 and "semantic" in refused.text
    assert broker.checkout.stat().st_ino == inode
