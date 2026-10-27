"""A lost start response may be reconciled after the exact SDK run closes."""
import asyncio
import copy
import json
import threading
from types import SimpleNamespace

import pytest
from temporal_test_server import local_temporal
from temporalio import workflow
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_store import service as service

from devflow_temporal.delivery_api import _dispatch_start
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore


@workflow.defn(name="DevflowDeliveryWorkflow")
class ImmediateDelivery:
    @workflow.run
    async def run(self, spec, recovery=None):
        return "complete"


def _controlled_cutover(monkeypatch):
    # The owned fixture controls only the installation namespace cutover.
    from contextlib import nullcontext

    from devflow_temporal import delivery_pause_context, delivery_pause_namespace

    monkeypatch.setattr(delivery_pause_context, "require_intake_dispatcher_cutover",
                        lambda _config: None)
    monkeypatch.setattr(delivery_pause_namespace, "intake_namespace_scope",
                        lambda _config: nullcontext())


class LostReply:
    def __init__(self, client):
        self.client = client

    def get_workflow_handle(self, *args, **kwargs):
        return self.client.get_workflow_handle(*args, **kwargs)

    async def start_workflow(self, *args, **kwargs):
        await self.client.start_workflow(*args, **kwargs)
        raise RuntimeError("owned lost start response")


@pytest.mark.asyncio
async def test_closed_original_reconciles_lost_start_without_live_pause_actor(service, monkeypatch):
    async with local_temporal() as env:
        old, request = service
        raw = copy.deepcopy(old.config.raw)
        raw["temporal_address"] = env.client.service_client.config.target_host
        old.config.path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(old.config.path))
        original = DeliveryConfig.admit

        def admit(config, *args, **kwargs):
            return {**original(config, *args, **kwargs), "pause_intake_version": 1}

        monkeypatch.setattr(DeliveryConfig, "admit", admit)
        _controlled_cutover(monkeypatch)
        store.submit(request)
        # The separate public intake test exercises the actual explicit resume.
        with store._connect() as db:
            db.execute("UPDATE delivery_pause_control SET state='running' WHERE root_id=?",
                       (request["run_id"],))
        spec = store.effective_spec(request["run_id"])
        dispatcher = SimpleNamespace(store=store, config=store.config)
        async with Worker(env.client, task_queue=store.config.queue,
                          workflow_runner=UnsandboxedWorkflowRunner(),
                          workflows=[ImmediateDelivery]):
            await _dispatch_start(
                dispatcher, LostReply(env.client), store.pending_starts()[0], spec)
            handle = env.client.get_workflow_handle("delivery-" + spec["run_id"])
            assert await asyncio.wait_for(handle.result(), 15) == "complete"
            assert store.pending_starts()
            with store._connect() as db:
                before = dict(db.execute("SELECT phase,execution_state,outcome FROM delivery_runs "
                                         "WHERE run_id=?", (spec["run_id"],)).fetchone())
            await _dispatch_start(dispatcher, env.client, store.pending_starts()[0], spec)
            assert not store.pending_starts()
            from devflow_temporal.delivery_closed_projection import reconcile_completed_projections

            await reconcile_completed_projections(store, env.client)
            assert not store.pending_starts()
            with store._connect() as db:
                after = dict(db.execute("SELECT phase,execution_state,outcome FROM delivery_runs "
                                        "WHERE run_id=?", (spec["run_id"],)).fetchone())
                assert after == before
                assert after["execution_state"] != "running"
                assert after["outcome"] is None
                root = db.execute("SELECT * FROM delivery_pause_control WHERE root_id=?",
                                  (spec["run_id"],)).fetchone()
                assert root["execution_run_id"] == ""
                assert db.execute("SELECT COUNT(*) FROM delivery_pause_start_receipts "
                                  "WHERE root_id=?", (spec["run_id"],)).fetchone()[0] == 1
                assert db.execute("SELECT COUNT(*) FROM delivery_pause_steps WHERE root_id=? "
                                  "AND step='start'", (spec["run_id"],)).fetchone()[0] == 1
                authority = db.execute("SELECT execution_run_id FROM delivery_execution_authority "
                                       "WHERE run_id=?", (spec["run_id"],)).fetchone()
                assert authority[0] == (await handle.describe()).run_id


@workflow.defn(name="DevflowDeliveryWorkflow")
class CloseOnSignal:
    def __init__(self):
        self.done = False

    @workflow.signal
    def finish(self):
        self.done = True

    @workflow.run
    async def run(self, spec, recovery=None):
        await workflow.wait_condition(lambda: self.done)
        return "complete"


@pytest.mark.asyncio
async def test_close_after_running_auth_does_not_create_a_running_actor(service, monkeypatch):
    async with local_temporal() as env:
        old, request = service
        raw = copy.deepcopy(old.config.raw)
        raw["temporal_address"] = env.client.service_client.config.target_host
        old.config.path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(old.config.path))
        original = DeliveryConfig.admit

        def admit(config, *args, **kwargs):
            return {**original(config, *args, **kwargs), "pause_intake_version": 1}

        monkeypatch.setattr(DeliveryConfig, "admit", admit)
        from devflow_temporal import delivery_pause_dispatch as dispatch

        _controlled_cutover(monkeypatch)
        store.submit(request)
        # The separate public intake test exercises the actual explicit resume.
        with store._connect() as db:
            db.execute("UPDATE delivery_pause_control SET state='running' WHERE root_id=?",
                       (request["run_id"],))
        spec = store.effective_spec(request["run_id"])
        dispatcher = SimpleNamespace(store=store, config=store.config)
        entered, release = threading.Event(), threading.Event()
        original_check = dispatch._acknowledge_selected_start

        def barrier(*args):
            entered.set()
            if not release.wait(15):
                raise TimeoutError("fixture barrier timed out")
            return original_check(*args)

        monkeypatch.setattr(dispatch, "_acknowledge_selected_start", barrier)
        async with Worker(env.client, task_queue=store.config.queue,
                          workflow_runner=UnsandboxedWorkflowRunner(),
                          workflows=[CloseOnSignal]):
            await _dispatch_start(
                dispatcher, LostReply(env.client), store.pending_starts()[0], spec)
            handle = env.client.get_workflow_handle("delivery-" + spec["run_id"])
            task = asyncio.create_task(
                _dispatch_start(dispatcher, env.client, store.pending_starts()[0], spec))
            try:
                assert await asyncio.to_thread(entered.wait, 15)
                await handle.signal(CloseOnSignal.finish)
                assert await asyncio.wait_for(handle.result(), 15) == "complete"
            finally:
                release.set()
            await asyncio.wait_for(task, 15)
            assert not store.pending_starts()
            with store._connect() as db:
                row = db.execute("SELECT phase,execution_state,outcome FROM delivery_runs "
                                 "WHERE run_id=?", (spec["run_id"],)).fetchone()
                assert row["execution_state"] != "running" and row["outcome"] is None
                assert db.execute("SELECT COUNT(*) FROM delivery_pause_start_receipts "
                                  "WHERE root_id=?", (spec["run_id"],)).fetchone()[0] == 1
