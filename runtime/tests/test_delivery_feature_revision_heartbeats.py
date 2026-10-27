"""Revision authentication, execution and durable persistence share one live activity."""
from __future__ import annotations

import asyncio
import threading
from copy import deepcopy
from types import SimpleNamespace

import pytest
from temporalio.testing import ActivityEnvironment

from devflow_temporal import delivery_activities as roles
from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal import delivery_feature_revisions as revisions
from devflow_temporal.delivery_execution_registry import OwnershipConflict


@pytest.fixture
def revision_activity(monkeypatch):
    request = {
        "spec": {"run_id": "isolated-revision", "provider": "codex"},
        "candidate": {"id": "exact-candidate", "head": "unchanged-head"},
        "context": {"revision_id": "revision-isolated", "proposed_plan": {"version": 2},
                    "budget": {"used": 7, "maximum": 10}},
        "proposal_session_id": "original-proposal-session",
    }
    h = SimpleNamespace(request=request, phase="initial", beats=[], payloads=[], saved=[],
                        auth_entered=threading.Event(), auth_release=threading.Event(),
                        auth_done=threading.Event(), role_entered=asyncio.Event(),
                        role_release=asyncio.Event(), role_cancelled=False,
                        save_entered=threading.Event(), save_release=threading.Event(),
                        save_done=threading.Event(), store=object(), candidate=request["candidate"])
    environment = ActivityEnvironment()
    environment.on_heartbeat = lambda *details: h.beats.append((h.phase, deepcopy(details)))
    h.environment = environment
    monkeypatch.setattr(activities, "_context", lambda _spec: (
        h.store, SimpleNamespace(candidate=lambda: deepcopy(h.candidate))))

    def authenticate(store, payload):
        assert store is h.store
        assert payload == {
            "spec": request["spec"], "role": payload["role"], "iteration": 0,
            "candidate": request["candidate"], "resume_session": None,
            "revision_context": request["context"], "findings": [],
        }
        h.phase = "authentication"
        h.auth_entered.set()
        try:
            assert h.auth_release.wait(3), "fixture authentication was not released"
            return {"revision_id": request["context"]["revision_id"],
                    "budget": deepcopy(request["context"]["budget"]),
                    "trusted_evidence": [{"path": "sealed", "sha256": "unchanged"}]}
        finally:
            h.auth_done.set()

    async def execute(payload):
        h.payloads.append(deepcopy(payload))
        h.phase = "role"
        h.role_entered.set()
        try:
            await h.role_release.wait()
        except asyncio.CancelledError:
            h.role_cancelled = True
            raise
        return {"status": "plan" if payload["role"] == "intake" else "pass",
                "cleanup": "confirmed", "session_id": "fresh-independent-session",
                "candidate": deepcopy(request["candidate"]), "plan": {"version": 2},
                "diagnostic": {"kind": "sealed-planning-defect"}}

    def save(store, spec, revision_id, plan, *, diagnostic):
        assert store is h.store and spec == request["spec"]
        assert revision_id == request["context"]["revision_id"]
        h.phase = "persistence"
        h.save_entered.set()
        try:
            assert h.save_release.wait(3), "fixture persistence was not released"
            receipt = {"revision_id": revision_id, "plan": plan, "diagnostic": diagnostic}
            h.saved.append(receipt)
            return deepcopy(receipt)
        finally:
            h.save_done.set()

    monkeypatch.setattr(revisions, "authenticate_revision_role", authenticate)
    monkeypatch.setattr(revisions, "record_proposal", save)
    monkeypatch.setattr(roles, "_run_intake", execute)
    monkeypatch.setattr(roles, "_run_role", execute)
    # Accelerate only the existing five-second cadence. The production wrapper,
    # real SDK heartbeat context and cancellation handling are not replaced.
    wait = asyncio.wait

    async def accelerated_wait(futures, *, timeout=None, **kwargs):
        return await wait(futures, timeout=0.01 if timeout == 5 else timeout, **kwargs)

    monkeypatch.setattr(roles.asyncio, "wait", accelerated_wait)
    return h


async def phase_heartbeats(h, phase):
    async def observed():
        while sum(name == phase for name, _ in h.beats) < 2:
            await asyncio.sleep(0.002)
    await asyncio.wait_for(observed(), 1)


async def finish(h, task):
    h.environment.cancel()
    task.cancel()
    h.auth_release.set()
    h.role_release.set()
    h.save_release.set()
    await asyncio.gather(task, return_exceptions=True)
    if h.auth_entered.is_set():
        assert await asyncio.to_thread(h.auth_done.wait, 1)
    if h.save_entered.is_set():
        assert await asyncio.to_thread(h.save_done.wait, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["propose", "review"])
async def test_revision_heartbeats_cover_delayed_authentication_and_persistence(
    revision_activity, role,
):
    h = revision_activity
    original = deepcopy(h.request)
    function = getattr(activities, "delivery_feature_revision_" + role)
    task = asyncio.create_task(h.environment.run(function, h.request))
    try:
        assert await asyncio.to_thread(h.auth_entered.wait, 1)
        await phase_heartbeats(h, "authentication")
        assert h.payloads == [] and h.saved == []
        h.auth_release.set()
        await asyncio.wait_for(h.role_entered.wait(), 1)
        await phase_heartbeats(h, "role")
        payload = h.payloads[0]
        assert payload["role"] == ("intake" if role == "propose" else "review")
        assert payload["iteration"] == 0 and payload["resume_session"] is None
        assert payload["candidate"] == original["candidate"]
        assert payload["revision_context"]["budget"] == {"used": 7, "maximum": 10}
        assert payload["revision_context"]["trusted_evidence"][0]["path"] == "sealed"
        h.role_release.set()
        if role == "propose":
            assert await asyncio.to_thread(h.save_entered.wait, 1)
            await phase_heartbeats(h, "persistence")
            h.save_release.set()
        result = await asyncio.wait_for(task, 1)
        assert result["status"] == ("plan" if role == "propose" else "pass")
        assert (result["proposal_receipt"] == h.saved[0]) if role == "propose" else (
            result["proposal_session_id"] == original["proposal_session_id"]
            and result["candidate_id"] == original["candidate"]["id"])
        stage = "feature_revision_intake" if role == "propose" else "feature_revision_review"
        assert all(details == ({"run_id": original["spec"]["run_id"], "stage": stage},)
                   for _, details in h.beats)
        assert h.request == original
    finally:
        await finish(h, task)


@pytest.mark.asyncio
async def test_revision_role_cancel_and_candidate_refusal_keep_existing_checks(revision_activity):
    h = revision_activity
    h.candidate = {"id": "drifted-candidate"}
    with pytest.raises(OwnershipConflict, match="candidate changed"):
        await h.environment.run(activities.delivery_feature_revision_propose, h.request)
    assert not h.auth_entered.is_set() and h.payloads == [] and h.saved == []
    h.candidate = h.request["candidate"]
    task = asyncio.create_task(h.environment.run(activities.delivery_feature_revision_propose,
                                               h.request))
    try:
        assert await asyncio.to_thread(h.auth_entered.wait, 1)
        h.auth_release.set()
        await asyncio.wait_for(h.role_entered.wait(), 1)
        h.environment.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert h.role_cancelled and len(h.payloads) == 1 and h.saved == []
    finally:
        await finish(h, task)
