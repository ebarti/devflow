"""Merge-native preparation and actual supervisor prelaunch producer contracts.

GitHub/delivered history and the installed-location rejection are controlled
boundaries. The original supervisor/attempt-owner/absence/merge admission code
and owned Git workspace are real. No provider may start here.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from test_delivery_merge_followup import delivered as delivered
from test_delivery_store import service as service

from devflow_temporal import delivery_merge_followup as followup
from devflow_temporal import delivery_native_process, delivery_preparation
from devflow_temporal.candidate import candidate_for
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_merge import MergeBroker
from devflow_temporal.supervisor import DeliverySupervisor


@pytest.mark.asyncio
async def test_actual_empty_prelaunch_continues_original_merge_without_fabricating_request(
    delivered,
    monkeypatch,
):
    d = delivered
    grant = followup.admit(d.store, "run-1", d.command)
    broker = MergeBroker(d.store, d.spec)
    workspace = Path(d.spec["source_path"])
    context = {"resolution_id": "owned-empty-prelaunch"}
    effect = f"merge-resolution:run-1:{context['resolution_id']}:implement"
    request = {
        "spec": d.spec,
        "role": "implement",
        "iteration": 0,
        "candidate": candidate_for(workspace),
        "workspace": str(workspace),
        "merge_context": {**context, "effect_key": effect},
    }
    key = DeliverySupervisor._job_key(request)
    broker.intent(
        effect,
        "merge_resolution",
        {
            "merge_authorization": grant["grant_sha256"],
            "spec_sha256": digest(d.spec),
            "candidate": request["candidate"],
            "workspace": request["workspace"],
            "merge_context": request["merge_context"],
            "job_key": key,
        },
    )
    calls = []

    def reject_location(_spec):
        calls.append("verify")
        raise ValueError("historical installed program location must be prepared before launch")

    def forbidden(*_args, **_kwargs):
        pytest.fail("the empty-prelaunch control must not enter capacity or create a provider")

    monkeypatch.setattr(delivery_preparation, "verify_prepared_spec", reject_location)
    monkeypatch.setattr(delivery_native_process, "NativeProcess", forbidden)
    monkeypatch.setattr(DeliverySupervisor, "_enter_capacity", forbidden)
    result = await DeliverySupervisor(d.store, capacity=2).run(request)
    assert calls == ["verify"]
    assert result["status"] == "blocked" and result["finish_reason"] == "prelaunch"
    assert result["cleanup"] == "confirmed" and result["session_id"] is None
    assert "process_cleanup" not in result
    folder = Path(d.spec["state_dir"]) / "attempts" / key
    assert list(folder.iterdir()) == []
    with d.store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_attempts WHERE job_key=?", (key,)).fetchone())
        original_grant = followup.current(db, "run-1")["grant_json"]
    assert row["state"] == "finished" and row["cleanup"] == "confirmed"
    assert all(
        row[name] is None for name in ("started_at", "pid", "process_identity", "session_id")
    )
    followup.failed(
        d.store,
        d.spec,
        grant["grant_sha256"],
        "RuntimeError: merge resolution native cleanup remains unconfirmed",
    )
    with pytest.raises(ValueError, match="settled effects"):
        followup.preflight(d.store, "run-1")
    before = copy.deepcopy(d.before)
    command = {**d.command, "command_id": "continue-real-empty-prelaunch"}
    continued = followup.admit(d.store, "run-1", command)
    assert continued["grant_sha256"] == grant["grant_sha256"]
    assert followup.admit(d.store, "run-1", command) == continued
    assert list(folder.iterdir()) == []  # No invented request/result/native receipt.
    with d.store._connect() as db:
        assert (
            dict(db.execute("SELECT * FROM delivery_attempts WHERE job_key=?", (key,)).fetchone())
            == row
        )
        assert followup.current(db, "run-1")["grant_json"] == original_grant
        saved = db.execute(
            "SELECT state,observed_json FROM delivery_effects WHERE effect_key=?", (effect,)
        ).fetchone()
        assert saved["state"] == "complete"
        assert json.loads(saved["observed_json"])["cleanup"] == "confirmed_prelaunch_absent"
    assert d.before == before
