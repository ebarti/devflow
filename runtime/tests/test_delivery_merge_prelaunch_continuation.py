"""Actual typed derived prelaunch producer and effect-free continuation readers.

The existing unit fixture controls historical delivery, GitHub, location ACK and
native measurement. The supervisor claim/owner/prelaunch result is real; a
tripwire forbids capacity or provider launch. Persisted request variants are
explicitly authored legacy metadata, never reconstructed original evidence.
"""

from __future__ import annotations

import json
import os
import shutil
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from merge_activity_test_helper import activity_call, role_call
from test_delivery_merge_followup import delivered as delivered
from test_delivery_merge_native_preparation import relocated_conflict as relocated_conflict
from test_delivery_merge_resolution import conflicted as conflicted
from test_delivery_store import service as service

from devflow_temporal import delivery_merge_followup as followup
from devflow_temporal import delivery_merge_native_preparation as native
from devflow_temporal import delivery_merge_resolution as resolution
from devflow_temporal import delivery_native_process, delivery_preparation, supervisor
from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_merge import MergeBroker
from devflow_temporal.delivery_resources import write_private


async def blocked(c, monkeypatch, role="implement", *, persisted=False):
    broker = MergeBroker(c.d.store, c.d.spec)
    _, workspace, context = resolution._prepare(broker, c.pr, c.conflict, c.receipt["grant_sha256"])
    requests = []
    original_run = supervisor.DeliverySupervisor._run_native

    async def fail_after_claim(self, request, job_key):
        requests.append(request)

        def reject(_spec):
            raise ValueError("owned verification refusal after claim and before capacity")

        monkeypatch.setattr(delivery_preparation, "verify_prepared_spec", reject)
        return await original_run(self, request, job_key)

    def forbidden(*_args, **_kwargs):
        pytest.fail("prelaunch continuation must not start capacity or provider work")

    monkeypatch.setattr(supervisor.DeliverySupervisor, "_run_native", fail_after_claim)
    monkeypatch.setattr(supervisor.DeliverySupervisor, "_enter_capacity", forbidden)
    monkeypatch.setattr(delivery_native_process, "NativeProcess", forbidden)
    monkeypatch.setattr(
        supervisor, "get_supervisor", lambda _: supervisor.DeliverySupervisor(c.d.store, capacity=2)
    )
    with pytest.raises(RuntimeError, match="blocked before provider launch"):
        await role_call(resolution._role,
            broker, c.d.spec, workspace, context, c.receipt["grant_sha256"], role
        )
    request = requests[0]
    key = supervisor.DeliverySupervisor._job_key(request)
    assert supervisor.DeliverySupervisor._job_key({**request, "spec": c.d.spec}) != key
    folder = Path(c.d.spec["state_dir"]) / "attempts" / key
    assert list(folder.iterdir()) == []
    with c.d.store._connect() as db:
        attempt = dict(
            db.execute("SELECT * FROM delivery_attempts WHERE job_key=?", (key,)).fetchone()
        )
        effect = dict(
            db.execute(
                "SELECT * FROM delivery_effects WHERE effect_key=?",
                (request["merge_context"]["effect_key"],),
            ).fetchone()
        )
        original_row = dict(
            db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone()
        )
        original_grant = followup.current(db, "run-1")["grant_json"]
    result = json.loads(attempt["result_json"])
    assert attempt["state"] == "finished" and attempt["cleanup"] == "confirmed"
    assert all(attempt[k] is None for k in ("started_at", "pid", "process_identity", "session_id"))
    assert result["finish_reason"] == "prelaunch" and result["cleanup"] == "confirmed"
    assert result["session_id"] is None and "process_cleanup" not in result
    if persisted:
        write_private(folder / "request.json", request)
    activity_call(followup.failed,
        c.d.store,
        c.d.spec,
        c.receipt["grant_sha256"],
        "RuntimeError: merge native role was blocked before provider launch",
    )
    return SimpleNamespace(
        c=c,
        request=request,
        folder=folder,
        key=key,
        attempt=attempt,
        effect=effect,
        original_row=original_row,
        grant=original_grant,
    )


def names(spec):
    root = Path(spec["state_dir"])
    return {str(p.relative_to(root)) for p in root.rglob("*")}


@pytest.mark.asyncio
@pytest.mark.parametrize("persisted", [False, True])
@pytest.mark.parametrize("role", ["implement", "review"])
async def test_exact_derived_prelaunch_continues_original_grant(
    relocated_conflict, monkeypatch, persisted, role
):
    b = await blocked(relocated_conflict, monkeypatch, role, persisted=persisted)
    d = b.c.d
    before = names(d.spec)
    assert not os.path.lexists(resolution.scratch_path(b.request))
    with d.store._connect() as db:
        commands = [dict(r) for r in db.execute("SELECT * FROM delivery_commands")]
        effects = [dict(r) for r in db.execute("SELECT * FROM delivery_effects")]
    assert followup.preflight(d.store, "run-1")["phase"] == "available"
    assert names(d.spec) == before
    with d.store._connect() as db:
        assert [dict(r) for r in db.execute("SELECT * FROM delivery_commands")] == commands
        assert [dict(r) for r in db.execute("SELECT * FROM delivery_effects")] == effects
    command = {**d.command, "command_id": "continue-exact-derived-prelaunch"}
    admitted = followup.admit(d.store, "run-1", command)
    assert admitted["grant_sha256"] == b.c.receipt["grant_sha256"]
    assert followup.admit(d.store, "run-1", command) == admitted
    assert names(d.spec) == before
    with d.store._connect() as db:
        assert (
            dict(db.execute("SELECT * FROM delivery_attempts WHERE job_key=?", (b.key,)).fetchone())
            == b.attempt
        )
        assert (
            dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
            == b.original_row
        )
        assert followup.current(db, "run-1")["grant_json"] == b.grant
        effect = dict(
            db.execute(
                "SELECT * FROM delivery_effects WHERE effect_key=?", (b.effect["effect_key"],)
            ).fetchone()
        )
        assert effect["state"] == "complete" and effect["request_json"] == b.effect["request_json"]
        assert json.loads(effect["observed_json"])["cleanup"] == "confirmed_prelaunch_absent"
    assert canonical_json(d.spec) == b.c.original_bytes


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "execution_digest",
        "reference",
        "receipt",
        "intent",
        "workflow",
        "workspace",
        "parent_evidence",
        "session",
        "foreign_original_spec",
    ],
)
async def test_derived_prelaunch_requires_complete_original_and_execution_authority(
    relocated_conflict, monkeypatch, change
):
    b = await blocked(relocated_conflict, monkeypatch)
    d = b.c.d
    bound = json.loads(b.effect["request_json"])
    if change in {"execution_digest", "reference", "foreign_original_spec"}:
        if change == "execution_digest":
            bound["execution_spec_sha256"] = "0" * 64
        elif change == "reference":
            bound[native.FIELD]["sha256"] = "0" * 64
        else:
            bound["spec_sha256"] = "0" * 64
        with d.store._connect() as db:
            db.execute(
                "UPDATE delivery_effects SET request_json=? WHERE effect_key=?",
                (canonical_json(bound), b.effect["effect_key"]),
            )
    elif change == "receipt":
        Path(bound[native.FIELD]["path"]).write_text("{}")
    elif change == "intent":
        (Path(bound[native.FIELD]["path"]).parent / "runtime-preparation/intent.json").write_text(
            "{}"
        )
    elif change == "workflow":
        with d.store._connect() as db:
            db.execute("UPDATE delivery_merge_requests SET workflow_id='foreign'")
    elif change == "workspace":
        path = Path(b.request["workspace"])
        held = path.with_name(path.name + "-held")
        path.rename(held)
        shutil.copytree(held, path, symlinks=True)
    elif change == "parent_evidence":
        Path(bound["merge_context"]["evidence_path"]).write_text("{}")
    else:
        with d.store._connect() as db:
            db.execute(
                "UPDATE delivery_attempts SET session_id='foreign' WHERE job_key=?", (b.key,)
            )
    before = names(d.spec)
    with pytest.raises((ValueError, KeyError)):
        followup.preflight(d.store, "run-1")
    assert names(d.spec) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("late", [False, True])
async def test_prelaunch_never_acquires_cleanup_of_an_unbound_scratch(
    relocated_conflict, monkeypatch, late
):
    b = await blocked(relocated_conflict, monkeypatch, persisted=True)
    d = b.c.d
    scratch = resolution.scratch_path(b.request)
    assert not os.path.lexists(scratch)
    observe = native.prelaunch_receipts

    def create():
        resolution.scratch(b.request)
        (scratch / "keep.txt").write_text("owned scratch must survive refused admission")

    if late:

        @contextmanager
        def changed(*args, **kwargs):
            with observe(*args, **kwargs) as proofs:
                create()  # An external generation appears after observation.
                yield proofs

        monkeypatch.setattr(native, "prelaunch_receipts", changed)
    else:
        create()
    try:
        with pytest.raises(ValueError, match="scratch|custody"):
            followup.preflight(d.store, "run-1")
        assert (scratch / "keep.txt").read_text() == "owned scratch must survive refused admission"
        with pytest.raises(ValueError, match="scratch|custody"):
            followup.admit(d.store, "run-1", {**d.command, "command_id": "refuse-unbound-scratch"})
        assert (scratch / "keep.txt").read_text() == "owned scratch must survive refused admission"
    finally:
        shutil.rmtree(scratch)  # Only this test's authored scratch, after its assertions.


@pytest.mark.asyncio
async def test_claim_drift_after_prelaunch_observation_cannot_settle_effect(
    relocated_conflict, monkeypatch
):
    b = await blocked(relocated_conflict, monkeypatch, persisted=True)
    d = b.c.d
    observe = native.prelaunch_receipts

    @contextmanager
    def changed(*args, **kwargs):
        with observe(*args, **kwargs) as proofs:
            with d.store._connect() as db:
                d.store.state.release_work(db, d.spec["work_id"], "external:devflow:run-1")
                d.store.state.claim_work(db, d.spec["work_id"], "external:other")
            yield proofs

    monkeypatch.setattr(native, "prelaunch_receipts", changed)
    before = names(d.spec)
    with pytest.raises(ValueError, match="current issue owner|claim resource differs"):
        followup.admit(d.store, "run-1", {**d.command, "command_id": "refuse-after-claim-drift"})
    assert names(d.spec) == before
    with d.store._connect() as db:
        assert (
            dict(
                db.execute(
                    "SELECT * FROM delivery_effects WHERE effect_key=?", (b.effect["effect_key"],)
                ).fetchone()
            )
            == b.effect
        )
