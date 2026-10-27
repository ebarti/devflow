"""Late retained-command admission is rechecked after real writer custody.

The merge row injection models its already-authorized tracking commit; it does
not claim native merge gate or Temporal admission qualification. The checkpoint
entry point, filesystem owner, writer locks and command ACK are real.
"""

from __future__ import annotations

import pytest
from test_delivery_checkpoints import stopped as stopped
from test_delivery_store import service as service

from devflow_temporal import delivery_checkpoints as checkpoints
from devflow_temporal.delivery_interrupted_recovery import installation_recovery_lease


def test_merge_admitted_after_preflight_cannot_be_acknowledged_as_stopped(stopped, monkeypatch):
    store, spec, _broker, row = stopped
    with installation_recovery_lease(store.config):
        pass  # Initialize only this fake fixture's owned installation witness.
    original = checkpoints._controller_binding

    def concurrent_admission(*args, **kwargs):
        closed = original(*args, **kwargs)
        with store._connect() as db:
            db.execute("INSERT INTO delivery_merge_requests "
                       "(run_id,command_id,grant_json,workflow_id,phase,created_at,updated_at) "
                       "VALUES (?,'late-owned-merge','{}','late-owned-sdk','queued','now','now')",
                       (spec["run_id"],))
        return closed

    monkeypatch.setattr(checkpoints, "_controller_binding", concurrent_admission)
    with pytest.raises(ValueError, match="active merge needs its authenticated pause controller"):
        checkpoints.create_checkpoint(store, spec["run_id"], "late-merge", row["revision"])
    with store._connect() as db:
        assert db.execute("SELECT 1 FROM delivery_commands WHERE "
                          "command_id='checkpoint-create:late-merge'").fetchone() is None
