"""Merge-native preparation and actual supervisor prelaunch producer contracts.

GitHub/delivered history and the installed-location rejection are controlled
boundaries. The original supervisor/attempt-owner/absence/merge admission code
and owned Git workspace are real. No provider may start here.
"""

from __future__ import annotations

import copy
import json
import os
import shutil
from pathlib import Path

import pytest
from merge_activity_test_helper import activity_call, activity_call_async, selected_activity
from test_delivery_merge_followup import delivered as delivered
from test_delivery_merge_resolution import conflicted as conflicted
from test_delivery_store import service as service

from devflow_temporal import delivery_merge_followup as followup
from devflow_temporal import delivery_merge_native_preparation as native
from devflow_temporal import delivery_merge_resolution as resolution
from devflow_temporal import delivery_native_process, delivery_preparation
from devflow_temporal.candidate import candidate_for
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_merge import MergeBroker
from devflow_temporal.supervisor import DeliverySupervisor

REAL_PREPARE = native.prepare


@pytest.fixture
def relocated_conflict(conflicted, monkeypatch):
    """Unit boundary only: location ACK and native measurement are controlled."""
    from devflow_temporal import delivery_gate_retry as gates
    from devflow_temporal import delivery_runtime_location as location
    from devflow_temporal.contracts import canonical_json
    from devflow_temporal.delivery_resources import write_private

    c = conflicted
    c.preparations = []
    monkeypatch.setattr(native, "prepare", REAL_PREPARE)
    monkeypatch.setattr(location, "require_preparation_location", lambda _old, _binary: None)

    def prepare(old, root, command, seal):
        c.preparations.append((copy.deepcopy(old), root, command, seal))
        execution = copy.deepcopy(old)
        execution["policy"]["codex_bin"] = "/owned/prepared/bundled-codex"
        execution["policy"]["native_identity"] = {"controlled_measurement": True}
        execution["policy_digest"] = digest(execution["policy"])
        write_private(
            root / "runtime-preparation/intent.json",
            {
                "command_digest": command,
                "seal_digest": seal,
                "state": "prepared",
                "preparation_attempts": [],
                "effective_spec": execution,
            },
        )
        return execution

    def verify(execution):
        assert execution["policy"]["codex_bin"] == "/owned/prepared/bundled-codex"

    monkeypatch.setattr(gates, "prepare_runtime", prepare)
    monkeypatch.setattr(delivery_preparation, "verify_prepared_spec", verify)
    c.original_bytes = canonical_json(c.d.spec)
    return c


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "reference",
        "receipt",
        "intent",
        "grant",
        "source",
        "policy",
        "workspace_replacement",
        "parent_evidence",
    ],
)
async def test_derived_role_rejects_unbound_preparation_or_current_drift(
    relocated_conflict,
    change,
):
    c = relocated_conflict
    await activity_call_async(
        resolution.resolve, c.d.store, c.d.spec, c.receipt["grant_sha256"], c.conflict
    )
    request = copy.deepcopy(c.calls[0])
    if change == "reference":
        request[native.FIELD]["sha256"] = "0" * 64
    elif change == "receipt":
        Path(request[native.FIELD]["path"]).write_text("{}")
    elif change == "intent":
        path = Path(request[native.FIELD]["path"]).parent / "runtime-preparation/intent.json"
        path.write_text("{}")
    elif change == "grant":
        request["merge_context"]["effect_key"] = "foreign"
    elif change == "source":
        (Path(request["workspace"]) / "conflict.txt").write_text("unowned change")
    elif change == "workspace_replacement":
        path = Path(request["workspace"])
        held = path.with_name(path.name + "-held")
        path.rename(held)
        shutil.copytree(held, path, symlinks=True)
    elif change == "parent_evidence":
        Path(request["merge_context"]["evidence_path"]).write_text("{}")
    else:
        request["spec"]["policy"]["max_repairs"] += 1
    with selected_activity(c.d.store, c.d.spec), pytest.raises((ValueError, KeyError)):
        resolution.authenticate(request, c.d.store)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "defect",
    [
        None,
        "owner_missing",
        "owner_busy",
        "launch",
        "unknown",
        "started",
        "session",
        "foreign_grant",
        "unexpected_file",
    ],
)
async def test_actual_empty_prelaunch_continues_original_merge_without_fabricating_request(
    delivered,
    monkeypatch,
    defect,
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
    with selected_activity(d.store, d.spec):
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
    activity_call(followup.failed,
        d.store,
        d.spec,
        grant["grant_sha256"],
        "RuntimeError: merge resolution native cleanup remains unconfirmed",
    )
    held_owner = None
    if defect == "owner_missing":
        DeliverySupervisor._owner_lock_path(d.spec, key).unlink()
    elif defect == "owner_busy":
        held_owner = DeliverySupervisor._owner_lock(d.spec, key)
        assert held_owner is not None
    elif defect == "session":
        with d.store._connect() as db:
            db.execute("UPDATE delivery_attempts SET session_id='foreign' WHERE job_key=?", (key,))
    elif defect == "foreign_grant":
        with d.store._connect() as db:
            bound = json.loads(
                db.execute(
                    "SELECT request_json FROM delivery_effects WHERE effect_key=?", (effect,)
                ).fetchone()[0]
            )
            bound["merge_authorization"] = "0" * 64
            db.execute(
                "UPDATE delivery_effects SET request_json=? WHERE effect_key=?",
                (json.dumps(bound), effect),
            )
    elif defect == "unexpected_file":
        (folder / "native-environment.json").write_text("{}")
    elif defect == "launch":
        from devflow_temporal.delivery_resources import write_private

        write_private(folder / "launch.json", {"never": "authority"})
    elif defect in {"unknown", "started"}:
        with d.store._connect() as db:
            if defect == "unknown":
                db.execute("UPDATE delivery_attempts SET state='unknown' WHERE job_key=?", (key,))
            else:
                db.execute(
                    "UPDATE delivery_attempts SET started_at='2026-10-26T00:00:00Z' "
                    "WHERE job_key=?",
                    (key,),
                )
    names = {
        str(p.relative_to(Path(d.spec["state_dir"]))) for p in Path(d.spec["state_dir"]).rglob("*")
    }
    assert not os.path.lexists(resolution.scratch_path(request))
    with d.store._connect() as db:
        before_commands = [dict(r) for r in db.execute("SELECT * FROM delivery_commands")]
        before_effects = [dict(r) for r in db.execute("SELECT * FROM delivery_effects")]
    if defect:
        try:
            with pytest.raises(ValueError):
                followup.preflight(d.store, "run-1")
        finally:
            if held_owner is not None:
                os.close(held_owner)
    else:
        assert followup.preflight(d.store, "run-1")["phase"] == "available"
    assert {
        str(p.relative_to(Path(d.spec["state_dir"]))) for p in Path(d.spec["state_dir"]).rglob("*")
    } == names
    assert not os.path.lexists(resolution.scratch_path(request))
    with d.store._connect() as db:
        assert [dict(r) for r in db.execute("SELECT * FROM delivery_commands")] == before_commands
        assert [dict(r) for r in db.execute("SELECT * FROM delivery_effects")] == before_effects
    if defect:
        return
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
