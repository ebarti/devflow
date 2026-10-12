"""Real Temporal closed-controller recovery using private stores and fixture activities."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from temporal_test_server import local_temporal
from temporalio import activity, workflow
from temporalio.client import WorkflowExecutionStatus, WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_store import service as service

from devflow_temporal.delivery_closed_projection import reconcile_completed_projections
from devflow_temporal.delivery_closures import closure_facts, sealed_closure
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
from devflow_temporal.delivery_interrupted_recovery import INTENT, RESULT


@workflow.defn(name="DevflowDeliveryWorkflow")
class InterruptedController:
    def __init__(self):
        self.released = False
        self.ready = False

    @workflow.query
    def ready_to_wait(self):
        return self.ready

    @workflow.signal
    async def release(self):
        self.released = True

    @workflow.run
    async def run(self, spec):
        await workflow.execute_activity(
            "retained_work", spec, start_to_close_timeout=timedelta(seconds=10)
        )
        self.ready = True
        if spec["goal"] == "FAILED":
            await workflow.execute_activity(
                "interrupted_finish",
                spec,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
        else:
            await workflow.wait_condition(lambda: self.released)
        return {
            "run_id": spec["run_id"],
            "outcome": "blocked",
            "iteration": 2,
            "checks": {},
            "cleanup": "confirmed",
        }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,legacy,lose_response",
    [
        ("FAILED", False, False),
        ("FAILED", True, True),
        ("FAILED", True, False),
        ("TIMED_OUT", True, False),
        ("TERMINATED", True, False),
    ],
)
async def test_closed_controller_resumes_without_repeating_work_or_budget(
    service, monkeypatch, status, legacy, lose_response
):
    store, request = service
    work_calls, finish_calls = [], []
    broken = True
    reached = asyncio.Event()

    @activity.defn(name="retained_work")
    async def retained_work(spec):
        work_calls.append(spec["run_id"])
        store.project(
            spec["run_id"],
            phase="implementing",
            execution_state="running",
            iteration=2,
            protocol_revision=1,
            event_type="implementing",
            message="Private fixture completed two charged iterations",
        )
        reached.set()
        return {"iteration": 2, "session_id": "retained-session"}

    @activity.defn(name="interrupted_finish")
    async def interrupted_finish(spec):
        finish_calls.append(spec["run_id"])
        if broken:
            raise ApplicationError(
                "Private controller activity transport failure", non_retryable=True
            )
        return {"state": "confirmed"}

    async with local_temporal() as environment:
        from devflow_temporal.delivery_config import DeliveryConfig
        from devflow_temporal.delivery_store import DeliveryStore

        raw = json.loads(store.config.path.read_text())
        raw["temporal_address"] = environment.client.service_client.config.target_host
        store.config.path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(store.config.path))
        if legacy:
            admit = DeliveryConfig.admit

            def old_admit(config, supplied):
                spec = admit(config, supplied)
                spec.pop("workflow_failure_boundary_version", None)
                return spec

            monkeypatch.setattr(DeliveryConfig, "admit", old_admit)
        store.submit({**request, "goal": status})
        store.mark_start("run-1", accepted=True)
        spec = store.spec("run-1")
        client = environment.client
        # Use the production payload converter, including its retained-history codec.
        from temporalio.client import Client

        client = Client(
            client.service_client,
            namespace=client.namespace,
            data_converter=DELIVERY_DATA_CONVERTER,
        )
        async with Worker(
            client,
            task_queue="interrupted-fixture",
            workflows=[InterruptedController],
            activities=[retained_work, interrupted_finish],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            handle = await client.start_workflow(
                InterruptedController.run,
                spec,
                id="delivery-run-1",
                task_queue="interrupted-fixture",
                memo={"request_digest": spec["request_digest"]},
                execution_timeout=timedelta(seconds=3) if status == "TIMED_OUT" else None,
            )
            await asyncio.wait_for(reached.wait(), 10)
            if status == "TERMINATED":
                # Terminate after Temporal has recorded the completed activity,
                # not merely after the fixture's external side effect occurred.
                for _ in range(100):
                    if await handle.query(InterruptedController.ready_to_wait):
                        break
                    await asyncio.sleep(0.01)
                else:
                    raise AssertionError("fixture did not record its completed activity")
                await handle.terminate("Private interruption fixture")
            with pytest.raises(WorkflowFailureError):
                await asyncio.wait_for(handle.result(), 15)
            description = await handle.describe()
            assert description.status == WorkflowExecutionStatus[status]
            with store._connect() as db:
                before = dict(
                    db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone()
                )
                grants = [tuple(row) for row in db.execute("SELECT * FROM delivery_repair_grants")]
                # Old maintenance used to permanently ignore this closed/legacy row.
                db.execute(
                    "INSERT INTO delivery_closed_projections "
                    "(run_id,workflow_id,state,attempts,revision,updated_at) "
                    "VALUES ('run-1','delivery-run-1','closed',0,0,'old')"
                )
            extra_recoveries = 0
            if status == "FAILED" and legacy and not lose_response:
                # Repeated server resets retain the original start event while
                # replacing the reset fork. Preserve all closures and cap a
                # persistently broken controller independently of product repairs.
                for attempt in range(3):
                    observed = await reconcile_completed_projections(
                        store, client, now=datetime.now(UTC) + timedelta(hours=attempt + 1)
                    )
                    assert observed[0]["state"] == "execution_recovered"
                    current = client.get_workflow_handle("delivery-run-1")
                    with pytest.raises(WorkflowFailureError):
                        await current.result()
                    description = await current.describe()
                    extra_recoveries += 1
                refused = await reconcile_completed_projections(
                    store, client, now=datetime.now(UTC) + timedelta(hours=5)
                )
                assert refused[0]["state"] == "unproven"
                assert "exhausted three attempts" in refused[0]["reason"]
                # A repaired deployment provides a new measured controller
                # payload; it does not add/refund any application repair turns.
                monkeypatch.setattr(
                    "devflow_temporal.delivery_interrupted_recovery.payload_digest",
                    lambda _: "repaired-fixture-runtime",
                )
            broken = False
            if lose_response:
                original = client.workflow_service.reset_workflow_execution

                async def lost(*args, **kwargs):
                    await original(*args, **kwargs)
                    raise RuntimeError("Private lost reset response after server accepted it")

                monkeypatch.setattr(client.workflow_service, "reset_workflow_execution", lost)
            observed = await reconcile_completed_projections(
                store, client, now=datetime.now(UTC) + timedelta(hours=6)
            )
            assert observed[0]["state"] == ("unproven" if lose_response else "execution_recovered")
            if lose_response:
                observed = await reconcile_completed_projections(
                    store, client, now=datetime.now(UTC) + timedelta(hours=8)
                )
                assert observed[0]["state"] == "execution_recovered"
            current = client.get_workflow_handle("delivery-run-1")
            new_description = await current.describe()
            assert new_description.run_id != description.run_id
            if status != "FAILED":
                await current.signal("release")
            result = await asyncio.wait_for(current.result(), 15)
            assert result["iteration"] == 2
            assert work_calls == ["run-1"]
            assert len(finish_calls) == (2 + extra_recoveries if status == "FAILED" else 0)
            facts = await closure_facts(await current.fetch_history())
            assert facts["execution_run_id"] == new_description.run_id
            old = await sealed_closure(store, "delivery-run-1", execution_run_id=description.run_id)
            assert old["execution_run_id"] == description.run_id and old["status"] == status
            from devflow_temporal.delivery_closures import seal_closed_executions

            # Current retention sealing follows the recovered execution, never
            # its older already-sealed failure.
            await seal_closed_executions(store, client)
            latest = await sealed_closure(store, "delivery-run-1")
            assert latest["execution_run_id"] == new_description.run_id
            assert latest["status"] == "COMPLETED" and latest["result"] == result
            with store._connect() as db:
                after = dict(
                    db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone()
                )
                assert all(
                    after[key] == before[key]
                    for key in (
                        "iteration",
                        "request_json",
                        "request_digest",
                        "recovery_json",
                        "candidate_json",
                        "accepted_plan_text",
                    )
                )
                assert [
                    tuple(row) for row in db.execute("SELECT * FROM delivery_repair_grants")
                ] == grants
                for kind in (INTENT, RESULT):
                    assert (
                        db.execute(
                            "SELECT COUNT(*) FROM delivery_events WHERE type=?", (kind,)
                        ).fetchone()[0]
                        == 1 + extra_recoveries
                    )


@pytest.mark.parametrize(
    "obstacle", ["cancel", "mutation", "claim", "attempt", "successor", "changed-row", "retired"]
)
def test_interrupted_recovery_respects_existing_authority(service, monkeypatch, obstacle):
    from devflow_temporal.delivery_interrupted_recovery import _guard

    store, request = service
    store.submit(request)
    spec = store.effective_spec("run-1")
    with store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
    _guard(store, row, spec)
    with store._connect() as db:
        if obstacle in {"cancel", "mutation"}:
            db.execute(
                "INSERT INTO delivery_mutations "
                "(command_id,run_id,kind,request_digest,state) VALUES (?,?,?,?,?)",
                (
                    "fixture-control",
                    "run-1",
                    "cancel" if obstacle == "cancel" else "steer",
                    "fixture",
                    "complete" if obstacle == "cancel" else "unknown",
                ),
            )
        elif obstacle == "claim":
            store.state.release_work(db, spec["work_id"], "external:devflow:run-1")
        elif obstacle == "attempt":
            db.execute(
                "INSERT INTO delivery_attempts (job_key,run_id,role,iteration,"
                "candidate_id,state,cleanup) VALUES "
                "('uncertain','run-1','implement',2,'candidate','unknown','unknown')"
            )
        elif obstacle == "changed-row":
            db.execute("UPDATE delivery_runs SET iteration=iteration+1 WHERE run_id='run-1'")
        elif obstacle == "successor":
            # Clone a private fixture row with a newer admission identity.
            other = dict(row)
            other["run_id"] = "newer-run"
            other["request_digest"] = "newer-request"
            keys = list(other)
            db.execute(
                "INSERT INTO delivery_runs ("
                + ",".join(keys)
                + ") VALUES ("
                + ",".join("?" for _ in keys)
                + ")",
                tuple(other.values()),
            )
    if obstacle == "retired":
        monkeypatch.setattr(
            "devflow_temporal.delivery_interrupted_recovery.execution_retired", lambda _: True
        )
    with pytest.raises(ValueError):
        _guard(store, row, spec)
    assert not [event for event in store.events("run-1") if event["type"] in (INTENT, RESULT)]


@pytest.mark.asyncio
async def test_execution_that_expired_before_first_worker_task_can_start(service):
    from temporalio.client import Client

    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_store import DeliveryStore

    store, request = service
    calls = []

    @activity.defn(name="retained_work")
    async def retained_work(spec):
        calls.append(spec["run_id"])
        return {}

    async with local_temporal() as environment:
        raw = json.loads(store.config.path.read_text())
        raw["temporal_address"] = environment.client.service_client.config.target_host
        store.config.path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(store.config.path))
        store.submit({**request, "goal": "queued"})
        store.mark_start("run-1", accepted=True)
        spec = store.spec("run-1")
        client = Client(
            environment.client.service_client,
            namespace="default",
            data_converter=DELIVERY_DATA_CONVERTER,
        )
        handle = await client.start_workflow(
            InterruptedController.run,
            spec,
            id="delivery-run-1",
            task_queue="previously-unpolled",
            memo={"request_digest": spec["request_digest"]},
            execution_timeout=timedelta(seconds=5),
        )
        with pytest.raises(WorkflowFailureError):
            await handle.result()
        history = await handle.fetch_history()
        assert not any(
            event.HasField("workflow_task_started_event_attributes") for event in history.events
        )
        async with Worker(
            client,
            task_queue="previously-unpolled",
            workflows=[InterruptedController],
            activities=[retained_work],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            observed = await reconcile_completed_projections(store, client)
            assert observed[0]["state"] == "execution_recovered"
            current = client.get_workflow_handle("delivery-run-1")
            await current.signal("release")
            assert (await current.result())["iteration"] == 2
            assert calls == ["run-1"]
            assert store.detail("run-1")["iteration"] == 0  # The fixture did not charge repairs.
