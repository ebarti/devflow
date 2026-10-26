"""Exact SDK generations, unfinished threads, and canonical merge closure custody."""
from __future__ import annotations

import asyncio
import copy
import json
from contextvars import copy_context
from dataclasses import replace
from threading import Event
from types import SimpleNamespace

import httpx
import pytest
from temporalio import activity
from temporalio.testing import ActivityEnvironment
from test_delivery_merge_followup import delivered as delivered
from test_delivery_store import service as service

from devflow_temporal import delivery_merge_execution as execution
from devflow_temporal import delivery_merge_followup as followup
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_merge import MergeBroker

_REAL_BROKER_API = MergeBroker.api


def sdk_environment(d, grant, **overrides):
    env = ActivityEnvironment()
    env.info = replace(env.info, workflow_id=grant["workflow_id"],
                       workflow_run_id="sdk-original-run", workflow_type="DevflowMergeWorkflow",
                       namespace=d.store.config.raw.get("temporal_namespace", "default"),
                       task_queue=d.store.config.queue, **overrides)
    return env


def test_sdk_origin_binds_exact_run_and_fences_late_progress_effects(delivered, monkeypatch):
    d = delivered
    grant = followup.admit(d.store, "run-1", d.command)
    monkeypatch.setattr(MergeBroker, "api", _REAL_BROKER_API)
    request = {"spec": d.spec, "merge_authorization": grant["grant_sha256"]}
    broker = MergeBroker(d.store, {**d.spec, "base_sha": "derived-target"})

    def capture():
        with execution.activity_scope(d.store, request):
            broker.intent("owned-effect", "owned-test", {"owned": True})
            return copy_context()

    with d.store._connect() as db:
        execution.bind_execution(db, d.spec, grant["grant_sha256"], {
            "workflow_id": grant["workflow_id"], "execution_run_id": "sdk-original-run"})
    origin = sdk_environment(d, grant).run(capture)
    with d.store._connect() as db:
        saved = dict(db.execute("SELECT * FROM delivery_merge_executions").fetchone())
        assert saved == {"workflow_id": grant["workflow_id"],
                         "execution_run_id": "sdk-original-run",
                         "run_id": "run-1", "spec_sha256": digest(d.spec),
                         "grant_sha256": grant["grant_sha256"]}
    foreign = sdk_environment(d, grant)
    foreign.info = replace(foreign.info, workflow_run_id="reset-execution")
    with pytest.raises(ValueError, match="original SDK binding"):
        foreign.run(capture)
    with d.store._connect() as db:
        db.execute("UPDATE delivery_merge_requests SET workflow_id='authorized-successor'")
        original = followup.current(db, "run-1")
    subprocess_calls = []
    monkeypatch.setattr("devflow_temporal.delivery_merge.subprocess.run",
                        lambda *a, **k: subprocess_calls.append((a, k)))
    for operation in (
        lambda: followup.record(d.store, d.spec, "merged", {"state": "confirmed"}),
        lambda: broker.intent("late-effect", "owned-test", {}),
        lambda: broker.finish("owned-effect", {"state": "confirmed"}),
        lambda: broker.api("repos/owned/fixture/pulls/1/merge", method="PUT", body={}),
    ):
        with pytest.raises(ValueError, match="generation changed"):
            origin.run(operation)
    with d.store._connect() as db:
        assert followup.current(db, "run-1") == original
        assert db.execute("SELECT state FROM delivery_effects WHERE effect_key='owned-effect'"
                          ).fetchone()[0] == "pending"
        assert not db.execute("SELECT 1 FROM delivery_effects "
                              "WHERE effect_key='late-effect'").fetchone()
    assert subprocess_calls == []


def test_sdk_context_without_verified_start_is_not_an_effect_grant(delivered):
    d = delivered
    grant = followup.admit(d.store, "run-1", d.command)

    def probe():
        with execution.activity_scope(d.store, {
            "spec": d.spec, "merge_authorization": grant["grant_sha256"]}):
            pytest.fail("unbound SDK context reached the effect boundary")

    with pytest.raises(ValueError, match="original SDK binding"):
        sdk_environment(d, grant).run(probe)
    with d.store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_merge_executions").fetchone()[0] == 0


@pytest.mark.parametrize("field,value", [("namespace", "foreign"),
    ("workflow_type", "DeliveryWorkflow"), ("task_queue", "foreign"),
    ("workflow_run_id", "")])
def test_sdk_scope_refuses_foreign_execution_fields(delivered, field, value):
    d = delivered
    grant = followup.admit(d.store, "run-1", d.command)
    env = sdk_environment(d, grant)
    env.info = replace(env.info, **{field: value})

    def probe():
        with execution.activity_scope(d.store, {
            "spec": d.spec, "merge_authorization": grant["grant_sha256"]}):
            pytest.fail("foreign SDK origin reached the effect boundary")

    with pytest.raises(ValueError, match="SDK execution identity"):
        env.run(probe)
    with d.store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_merge_executions").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_cancelled_waiter_preserves_unfinished_thread_lease(delivered):
    d = delivered
    grant = followup.admit(d.store, "run-1", d.command)
    entered, release, finished = Event(), Event(), Event()
    with d.store._connect() as db:
        execution.bind_execution(db, d.spec, grant["grant_sha256"], {
            "workflow_id": grant["workflow_id"], "execution_run_id": "sdk-original-run"})

    def body():
        entered.set()
        try:
            assert release.wait(10)
        finally:
            finished.set()

    async def probe():
        with execution.activity_scope(d.store, {
            "spec": d.spec, "merge_authorization": grant["grant_sha256"]}):
            task = asyncio.create_task(execution.to_thread(d.store, d.spec, body))
            assert await asyncio.to_thread(entered.wait, 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    try:
        await sdk_environment(d, grant).run(probe)
        with pytest.raises(ValueError, match="still owns its effect lease"):
            with execution.existing_lease(d.store.config, exclusive=True):
                pytest.fail("unfinished body lost its effect lease")
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 5)
    # The body signals just before its lease finally closes; wait for that
    # observed release, rather than racing its descriptor cleanup.
    for _ in range(100):
        try:
            with execution.existing_lease(d.store.config, exclusive=True):
                break
        except ValueError:
            await asyncio.sleep(0.01)
    else:
        pytest.fail("finished body did not release its effect lease")


@pytest.mark.asyncio
async def test_public_lost_start_reply_reconciles_exact_completed_history_and_replays(
    service, monkeypatch,
):
    from temporal_test_server import local_temporal
    from temporalio.worker import Replayer, Worker

    from devflow_temporal.delivery_api import create_app
    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_merge_workflow import MergeWorkflow
    from devflow_temporal.delivery_store import DeliveryStore

    release, authenticated, complete = asyncio.Event(), asyncio.Event(), asyncio.Event()
    result = {"state": "decision_required", "reason": "conflicting_upstream_changes"}

    async with local_temporal() as environment:
        old, submitted = service
        raw = copy.deepcopy(old.config.raw)
        raw["temporal_address"] = environment.client.service_client.config.target_host
        old.config.path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(old.config.path))
        d = delivered.__wrapped__((store, submitted), monkeypatch)
        monkeypatch.setattr(DeliveryStore, "_completed_temporal_result",
                            lambda *_args, **_kwargs: copy.deepcopy(d.closed))

        @activity.defn(name="delivery_merge_followup")
        async def controlled_followup(request):
            await release.wait()
            await execution.authenticate_activity(d.store, request)
            with execution.activity_scope(d.store, request):
                authenticated.set()
                await complete.wait()
                followup.record(d.store, request["spec"], "decision_required", result)
                return result

        host = d.store.config.dashboard_url
        async with httpx.AsyncClient(transport=httpx.ASGITransport(
            app=create_app(d.store.config.path)), base_url=host) as public:
            token = (await public.get("/api/session")).json()["csrf_token"]
            response = await public.post("/api/runs/run-1/merge", json=d.command,
                headers={"Origin": host, "X-Devflow-CSRF": token})
            assert response.status_code == 200, response.text
            admitted = response.json()
        service = SimpleNamespace(store=d.store, config=d.store.config)

        class LostStartReply:
            def get_workflow_handle(self, *args, **kwargs):
                return environment.client.get_workflow_handle(*args, **kwargs)

            async def start_workflow(self, *args, **kwargs):
                await environment.client.start_workflow(*args, **kwargs)
                raise RuntimeError("owned fault: start accepted, response lost")

        async with Worker(environment.client, task_queue=d.store.config.queue,
                          workflows=[MergeWorkflow], activities=[controlled_followup]):
            await followup.dispatch(service, LostStartReply())
            handle = environment.client.get_workflow_handle(admitted["workflow_id"])
            description = await handle.describe()
            with d.store._connect() as db:
                selected = followup.current(db, "run-1")
                assert selected["phase"] == "queued" and selected["dispatched"] == 0
            with pytest.raises(ValueError, match="not authentically closed"):
                await execution._closed(d.store, d.spec, selected)
            # The SDK activity arrives before the dispatcher can save its ACK.
            release.set()
            await asyncio.wait_for(authenticated.wait(), 10)
            await followup.dispatch(service, environment.client)
            with d.store._connect() as db:
                selected = followup.current(db, "run-1")
                bound = dict(db.execute("SELECT * FROM delivery_merge_executions").fetchone())
                assert selected["dispatched"] == 1
                assert bound["execution_run_id"] == description.run_id
            complete.set()
            assert await asyncio.wait_for(handle.result(), 15) == result
            history = await handle.fetch_history()
            await Replayer(workflows=[MergeWorkflow]).replay_workflow(history)
        with d.store._connect() as db:
            selected = followup.current(db, "run-1")
            assert dict(db.execute("SELECT * FROM delivery_runs").fetchone()) == d.before
        closed = await execution._closed(d.store, d.spec, selected)
        assert closed["execution_run_id"] == description.run_id
        assert closed["result_sha256"] == digest(result)
        assert closed["grant_sha256"] == admitted["grant_sha256"]
        assert closed["event_count"] == len(history.events)
        for changed in ({**selected, "result_json": '{"state":"merged"}'},
                        {**selected, "grant_json": '{}'}):
            with pytest.raises(ValueError):
                await execution._closed(d.store, d.spec, changed)
        with d.store._connect() as db:
            db.execute("UPDATE delivery_merge_executions SET execution_run_id='foreign-reset-run'")
        with pytest.raises(ValueError, match="not authentically closed"):
            await execution._closed(d.store, d.spec, selected)
