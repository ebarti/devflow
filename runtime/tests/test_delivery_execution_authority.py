"""Original-start binding and transaction-local handoff fencing."""

import asyncio
import copy
import json
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from temporal_test_server import local_temporal
from temporalio import activity, workflow
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.workflowservice.v1 import ResetWorkflowExecutionRequest
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_store import service as service

from devflow_temporal import delivery_execution_authority as authority
from devflow_temporal.delivery_api import _dispatch_start
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore


@workflow.defn(name="DevflowDeliveryWorkflow")
class ControlledDelivery:
    @workflow.run
    async def run(self, spec, recovery=None):
        return await workflow.execute_activity(
            "authority_probe", {"spec": spec}, start_to_close_timeout=timedelta(seconds=30)
        )


class LostReply:
    def __init__(self, client):
        self.client = client

    def get_workflow_handle(self, *args, **kwargs):
        return self.client.get_workflow_handle(*args, **kwargs)

    async def start_workflow(self, *args, **kwargs):
        await self.client.start_workflow(*args, **kwargs)
        raise RuntimeError("owned lost start reply")


@pytest.mark.asyncio
async def test_public_dispatch_lost_ack_activity_first_and_successor_fence(service):
    async with local_temporal() as env:
        old, request = service
        raw = copy.deepcopy(old.config.raw)
        raw["temporal_address"] = env.client.service_client.config.target_host
        old.config.path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(old.config.path))
        store.submit(request)
        spec = store.effective_spec(request["run_id"])
        got, release = asyncio.Event(), asyncio.Event()
        proofs = []

        @activity.defn(name="authority_probe")
        async def probe(request):
            proof = await authority.authenticate_activity(store, request["spec"])
            proofs.append(proof)
            got.set()
            await release.wait()
            with store._connect() as db:
                authority.require_current(db, request["spec"], proof)
            return "ok"

        service = SimpleNamespace(store=store, config=store.config)
        async with Worker(
            env.client,
            task_queue=store.config.queue,
            workflow_runner=UnsandboxedWorkflowRunner(),
            workflows=[ControlledDelivery],
            activities=[probe],
        ):
            item = store.pending_starts()[0]
            await _dispatch_start(service, LostReply(env.client), item, spec)
            await asyncio.wait_for(got.wait(), 10)
            bound = authority.read_reservation(store, spec["run_id"], "delivery-" + spec["run_id"])
            assert bound["execution_run_id"] == proofs[0]["execution_run_id"]
            await _dispatch_start(service, env.client, store.pending_starts()[0], spec)
            assert not store.pending_starts()
            with store._connect() as db:
                authority.require_current(db, spec, proofs[0])
                db.execute(
                    "UPDATE delivery_runs SET workflow_id='new-owner' WHERE run_id=?",
                    (spec["run_id"],),
                )
            with store._connect() as db, pytest.raises(ValueError, match="owner"):
                authority.require_current(db, spec, proofs[0])
            release.set()


@pytest.mark.asyncio
async def test_real_reset_cannot_bind_copied_start(service):
    async with local_temporal() as env:
        old, request = service
        raw = copy.deepcopy(old.config.raw)
        raw["temporal_address"] = env.client.service_client.config.target_host
        old.config.path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(old.config.path))
        store.submit(request)
        spec = store.effective_spec(request["run_id"])
        entered, release = asyncio.Event(), asyncio.Event()
        refused = []

        @activity.defn(name="authority_probe")
        async def probe(request):
            entered.set()
            await release.wait()
            try:
                await authority.authenticate_activity(store, request["spec"])
            except ValueError as exc:
                refused.append(str(exc))
                return "refused"
            return "bound"

        service = SimpleNamespace(store=store, config=store.config)
        async with Worker(
            env.client,
            task_queue=store.config.queue,
            workflow_runner=UnsandboxedWorkflowRunner(),
            workflows=[ControlledDelivery],
            activities=[probe],
        ):
            await _dispatch_start(service, LostReply(env.client), store.pending_starts()[0], spec)
            await asyncio.wait_for(entered.wait(), 10)
            handle = env.client.get_workflow_handle("delivery-" + spec["run_id"])
            desc = await handle.describe()
            history = await handle.fetch_history()
            event = next(
                e.event_id
                for e in history.events
                if e.HasField("workflow_task_completed_event_attributes")
            )
            reset = await env.client.workflow_service.reset_workflow_execution(
                ResetWorkflowExecutionRequest(
                    namespace="default",
                    workflow_execution=WorkflowExecution(workflow_id=desc.id, run_id=desc.run_id),
                    reason="owned reset proof",
                    workflow_task_finish_event_id=event,
                    request_id=str(uuid4()),
                )
            )
            latest = await handle.describe()
            assert latest.run_id == reset.run_id != desc.run_id
            await _dispatch_start(service, env.client, store.pending_starts()[0], spec)
            with store._connect() as db:
                assert (
                    db.execute(
                        "SELECT execution_run_id FROM delivery_execution_authority"
                    ).fetchone()[0]
                    is None
                )
            release.set()
            result = await asyncio.wait_for(
                env.client.get_workflow_handle(desc.id, run_id=reset.run_id).result(), 15
            )
            assert result == "refused" and any("reset or foreign start" in text for text in refused)


@pytest.mark.asyncio
async def test_real_delivery_launch_interceptor_and_history_replay(service):
    from temporalio.worker import Replayer

    from devflow_temporal.delivery_activities import DatabaseContentionInterceptor
    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    async with local_temporal() as env:
        old, request = service
        raw = copy.deepcopy(old.config.raw)
        raw["temporal_address"] = env.client.service_client.config.target_host
        old.config.path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(old.config.path))
        store.submit(request)
        spec = store.effective_spec(request["run_id"])
        original = copy.deepcopy(store.submitted_spec(request["run_id"]))
        got, release = asyncio.Event(), asyncio.Event()
        proofs = []

        @activity.defn(name="delivery_project")
        async def project(request):
            proofs.append(authority.activity_authority())
            got.set()
            await release.wait()

        service = SimpleNamespace(store=store, config=store.config)
        async with Worker(
            env.client,
            task_queue=store.config.queue,
            workflow_runner=UnsandboxedWorkflowRunner(),
            workflows=[DeliveryWorkflow],
            activities=[project],
            interceptors=[DatabaseContentionInterceptor()],
        ):
            await _dispatch_start(service, env.client, store.pending_starts()[0], spec)
            await asyncio.wait_for(got.wait(), 10)
            handle = env.client.get_workflow_handle("delivery-" + spec["run_id"])
            with store._connect() as db:
                authority.require_current(db, spec, proofs[0])
            assert store.submitted_spec(spec["run_id"]) == original
            assert spec.get("delivery_limits") == original.get("delivery_limits")
            history = await handle.fetch_history()
            await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
            await handle.terminate(reason="owned test teardown")
            release.set()


def test_concurrent_authenticated_bindings_have_one_winner(service):
    from concurrent.futures import ThreadPoolExecutor

    store, request = service
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    reservation = authority.reserve(store, store.pending_starts()[0], spec)

    def bind(run):
        try:
            return authority.bind(store, reservation, run)["execution_run_id"]
        except ValueError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(bind, ("first", "second")))
    assert sum(value is not None for value in results) == 1


def test_stale_activity_cannot_project_or_mutate_effect_journal(service):
    from devflow_temporal.delivery_broker import DeliveryBroker

    store, request = service
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    reservation = authority.reserve(store, store.pending_starts()[0], spec)
    proof = authority.bind(store, reservation, "original-sdk")
    broker = DeliveryBroker(store, spec)
    token = authority._ACTIVITY_AUTHORITY.set(proof)
    try:
        broker._effect("owned-test", "test", {"input": "frozen"})
        with store._connect() as db:
            db.execute(
                "UPDATE delivery_runs SET workflow_id='authorized-successor' WHERE run_id=?",
                (spec["run_id"],),
            )
        with pytest.raises(ValueError, match="owner"):
            store.project(
                spec["run_id"],
                phase="preparing",
                execution_state="running",
                event_type="late",
                message="stale",
            )
        with pytest.raises(ValueError, match="owner"):
            broker._effect("late-test", "test", {})
        with pytest.raises(ValueError, match="owner"):
            broker._finish_effect("owned-test", {"state": "complete"})
        with store._connect() as db:
            assert (
                db.execute(
                    "SELECT state FROM delivery_effects WHERE effect_key='owned-test'"
                ).fetchone()[0]
                == "pending"
            )
            assert not db.execute(
                "SELECT 1 FROM delivery_effects WHERE effect_key='late-test'"
            ).fetchone()
    finally:
        authority._ACTIVITY_AUTHORITY.reset(token)


def test_successor_or_missing_work_claim_refuses_final_handoff(service):
    store, request = service
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    reservation = authority.reserve(store, store.pending_starts()[0], spec)
    proof = authority.bind(store, reservation, "original-sdk")
    with store._connect() as db:
        db.execute(
            "UPDATE claims SET owner='external:devflow:successor' WHERE work_id=?",
            (spec["work_id"],),
        )
    with store._connect() as db, pytest.raises(ValueError, match="work claim"):
        authority.require_current(db, spec, proof)
    with store._connect() as db:
        db.execute("DELETE FROM claims WHERE work_id=?", (spec["work_id"],))
    with store._connect() as db, pytest.raises(ValueError, match="work claim"):
        authority.require_current(db, spec, proof)


@pytest.mark.asyncio
async def test_lost_ack_reconciles_completed_original_start(service):
    async with local_temporal() as env:
        old, request = service
        raw = copy.deepcopy(old.config.raw)
        raw["temporal_address"] = env.client.service_client.config.target_host
        old.config.path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(old.config.path))
        store.submit(request)
        spec = store.effective_spec(request["run_id"])

        @activity.defn(name="authority_probe")
        async def probe(request):
            await authority.authenticate_activity(store, request["spec"])
            return "complete"

        service = SimpleNamespace(store=store, config=store.config)
        async with Worker(
            env.client,
            task_queue=store.config.queue,
            workflow_runner=UnsandboxedWorkflowRunner(),
            workflows=[ControlledDelivery],
            activities=[probe],
        ):
            await _dispatch_start(service, LostReply(env.client), store.pending_starts()[0], spec)
            handle = env.client.get_workflow_handle("delivery-" + spec["run_id"])
            assert await asyncio.wait_for(handle.result(), 15) == "complete"
            await _dispatch_start(service, env.client, store.pending_starts()[0], spec)
            assert not store.pending_starts()
