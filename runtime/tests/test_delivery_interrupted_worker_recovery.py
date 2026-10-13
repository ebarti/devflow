"""Interrupted-worker recovery with real registry custody and private Temporal history."""

from __future__ import annotations

import asyncio
import json
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_feature_execution import feature_service
from test_delivery_interrupted_recovery import InterruptedController
from test_delivery_store import service as legacy_service

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import (
    register_worker,
    registry,
    worker_key,
    worker_spec,
)
from devflow_temporal.delivery_interrupted_recovery import (
    INTENT,
    RESULT,
    _guard,
    recover_interrupted,
)
from devflow_temporal.delivery_plan_model import migrate_plan_v1, ordered_chunks
from devflow_temporal.delivery_store import DeliveryStore


def _row(store, run_id):
    with store._connect() as db:
        return dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone())


@contextmanager
def _reattached(monkeypatch, temporal_address=None):
    with TemporaryDirectory(prefix="devflow-interrupted-worker-") as directory:
        service = legacy_service.__wrapped__(Path(directory))
        if temporal_address:
            original_store, request = service
            raw = dict(original_store.config.raw)
            raw["temporal_address"] = temporal_address
            original_store.config.path.write_text(json.dumps(raw))
            service = DeliveryStore(DeliveryConfig.load(original_store.config.path)), request
        store, request, snapshot = feature_service(service, monkeypatch)
        legacy = json.loads(request["accepted_plan"])
        bindings = {
            stream["id"]: {
                "id": "I_" + stream["id"],
                "number": stream["issue_number"],
                "url": "https://github.com/example/fixture/issues/" + str(stream["issue_number"]),
            }
            for stream in legacy["workstreams"]
        }
        plan = migrate_plan_v1(legacy, bindings, {c["id"]: [] for c in ordered_chunks(legacy)}, [])
        plan["workstreams"][0]["chunks"][0]["title"] = "FAILED"
        store.submit({**request, "accepted_plan": json.dumps(plan)})
        parent = store.effective_spec(request["run_id"])
        shared = registry(parent)
        original = parent["feature_delivery"]["owner"]
        identity = {"plan_revision": 2, "plan_digest": digest(plan)}
        # Synthetic GitHub read-back seam; registry ownership and assignments are real.
        shared.checkpoint(
            original,
            "plan-revision:adopted:fixture-2",
            {
                "identity": identity,
                "plan": plan,
            },
        )
        parent["feature_plan_revision"] = identity
        child = worker_spec(
            parent,
            ordered_chunks(plan)[0],
            bindings[legacy["workstreams"][0]["id"]],
            kind="build",
            base_sha=parent["base_sha"],
            base_branch="main",
        )
        register_worker(store, parent, child)
        for number in (1, 2):
            shared.repair(original, "paid-cycle-" + str(number), "Private fixture repair")
        shared.finish_worker(
            original, worker_key(child["run_id"], original), {"cleanup": "confirmed"}
        )
        shared.stop(original, "fixture-stopped", {})
        current = shared.claim(
            snapshot, "coordinator-next", str(store.config.tracking_db), predecessor=original
        )
        successor = deepcopy(parent)
        successor["feature_delivery"]["owner"] = current
        successor["run_id"] = current["run_id"]
        # Explicit successor assignment keeps the stored worker input unchanged.
        register_worker(store, successor, child)
        yield store, parent, child, current


@pytest.fixture
def reattached(monkeypatch):
    with _reattached(monkeypatch) as fixture:
        yield fixture


@pytest.fixture
async def reattached_temporal(monkeypatch):
    async with local_temporal() as environment:
        with _reattached(
            monkeypatch, environment.client.service_client.config.target_host
        ) as fixture:
            client = Client(
                environment.client.service_client,
                namespace="default",
                data_converter=DELIVERY_DATA_CONVERTER,
            )
            yield fixture, client


@pytest.mark.parametrize(
    "obstacle,message",
    [
        ("missing", "active execution assignment"),
        ("finished", "active execution assignment"),
        ("generation", "active execution assignment"),
        ("plan", "superseded feature plan"),
        ("workflow", "changed during recovery"),
        ("claim", "work claim changed"),
        ("attempt", "native attempt cleanup"),
    ],
)
def test_reattached_worker_recovery_preserves_fences(reattached, obstacle, message):
    store, parent, child, current = reattached
    row = _row(store, child["run_id"])
    _guard(store, row, child)
    shared = registry(child)
    key = worker_key(child["run_id"], current)
    if obstacle == "finished":
        shared.finish_worker(current, key, {"cleanup": "confirmed"})
    elif obstacle == "plan":
        plan = json.loads(parent["accepted_plan"])
        plan["scope"] = "Superseding fixture plan"
        shared.checkpoint(
            current,
            "plan-revision:adopted:fixture-3",
            {"identity": {"plan_revision": 3, "plan_digest": digest(plan)}, "plan": plan},
        )
    elif obstacle in {"missing", "generation"}:
        with shared.connect() as db:
            if obstacle == "missing":
                db.execute("DELETE FROM execution_workers WHERE worker_key=?", (key,))
            else:
                db.execute(
                    "UPDATE execution_workers SET generation=generation-1 WHERE worker_key=?",
                    (key,),
                )
    else:
        with store._connect() as db:
            if obstacle == "workflow":
                db.execute(
                    "UPDATE delivery_runs SET workflow_id='superseding-workflow' WHERE run_id=?",
                    (child["run_id"],),
                )
            elif obstacle == "claim":
                store.state.release_work(
                    db, child["work_id"], "external:devflow:" + child["run_id"]
                )
            else:
                db.execute(
                    "INSERT INTO delivery_attempts (job_key,run_id,role,iteration,"
                    "candidate_id,state,cleanup) VALUES (?,?,'implement',2,'c',"
                    "'unknown','unknown')",
                    ("unfinished", child["run_id"]),
                )
    with pytest.raises((ValueError, OwnershipConflict), match=message):
        _guard(store, row, child)
    assert not [
        event for event in store.events(child["run_id"]) if event["type"] in (INTENT, RESULT)
    ]


def test_reattachment_does_not_authorize_stale_coordinator(reattached):
    store, parent, _, _ = reattached
    with pytest.raises(OwnershipConflict, match="execution ownership changed"):
        _guard(store, _row(store, parent["run_id"]), parent)


@pytest.mark.asyncio
async def test_reattached_worker_resumes_authenticated_history_and_keeps_budget(
    reattached_temporal,
    monkeypatch,
):
    (store, _, child, current), client = reattached_temporal
    calls = []
    broken = True

    @activity.defn(name="retained_work")
    async def retained_work(spec):
        calls.append(spec["run_id"])
        store.project(
            spec["run_id"],
            phase="implementing",
            execution_state="running",
            iteration=2,
            protocol_revision=1,
            event_type="implementing",
            message="Two retained iterations",
        )
        return {"iteration": 2}

    @activity.defn(name="interrupted_finish")
    async def interrupted_finish(spec):
        if broken:
            raise ApplicationError("Private fixture transport failure", non_retryable=True)
        return {"state": "confirmed"}

    async with Worker(
        client,
        task_queue="reattached-fixture",
        workflows=[InterruptedController],
        activities=[retained_work, interrupted_finish],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ):
        handle = await client.start_workflow(
            InterruptedController.run,
            child,
            id=store.active_workflow_id(child["run_id"]),
            task_queue="reattached-fixture",
            memo={"request_digest": child["request_digest"]},
        )
        with pytest.raises(WorkflowFailureError):
            await asyncio.wait_for(handle.result(), 15)
        description = await handle.describe()
        row = _row(store, child["run_id"])
        budget = registry(child).budget(current["issue_id"])
        history = deepcopy(await handle.fetch_history())
        bad = (await DELIVERY_DATA_CONVERTER.encode(["unrelated-admission"]))[0]
        history.events[0].workflow_execution_started_event_attributes.memo.fields[
            "request_digest"
        ].CopyFrom(bad)

        async def unrelated_history():
            return history

        with monkeypatch.context() as patch:
            patch.setattr(
                client,
                "get_workflow_handle",
                lambda *_a, **_kw: SimpleNamespace(
                    fetch_history=unrelated_history,
                ),
            )
            with pytest.raises(ValueError, match="history does not authenticate"):
                await recover_interrupted(store, client, row, child, description)
        assert not [e for e in store.events(child["run_id"]) if e["type"] in (INTENT, RESULT)]
        broken = False
        result = await recover_interrupted(store, client, row, child, description)
        assert result["state"] == "execution_recovered"
        recovered = client.get_workflow_handle(description.id, run_id=result["execution_run_id"])
        assert (await asyncio.wait_for(recovered.result(), 15))["iteration"] == 2
        assert calls == [child["run_id"]]
        after = _row(store, child["run_id"])
        assert all(
            after[key] == row[key]
            for key in (
                "request_json",
                "request_digest",
                "recovery_json",
                "iteration",
                "protocol_revision",
            )
        )
        assert (
            registry(child).budget(current["issue_id"])
            == budget
            == {
                "maximum": 10,
                "used": 2,
                "learning_required": False,
            }
        )
        assert (
            store.spec(child["run_id"])["feature_delivery"]["owner"]["generation"]
            < current["generation"]
        )
