"""Accepted operator controls survive a fresh execution without changing authority."""

from __future__ import annotations

import asyncio
import copy
import json

import httpx
import pytest
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker
from test_delivery_automatic_retry import stopped
from test_delivery_store import service as service

from devflow_temporal import delivery_automatic_retry as retry
from devflow_temporal.delivery_activities import delivery_finalize_resources, delivery_project
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_control_lineage import (
    control_from_spec,
)
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow


def disabled_stop(service, monkeypatch):
    store, request = service
    raw = copy.deepcopy(store.config.raw)
    raw["delivery_timeout_seconds"] = 4
    store.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(store.config.path))
    store, request, closed = stopped((store, request), monkeypatch)
    control = {
        "disabled": True,
        "command_id": "accepted-disable",
        "reason": "Continue retained work",
        "recorded_at": "2026-10-26T00:00:00+00:00",
        "protocol_revision": 2,
    }
    store.record_delivery_limits_disabled(
        request["run_id"],
        {"revision": 2, "checks": {"delivery_limits_control": control}},
        control["command_id"],
    )
    closed["result"]["checks"]["delivery_limits_control"] = copy.deepcopy(control)
    return store, request, closed, control


@pytest.mark.parametrize(
    "conflict", ["missing_event", "changed_event", "closed_control", "predecessor_changed"]
)
def test_automatic_control_uncertainty_refuses_successor(service, monkeypatch, conflict):
    store, request, closed, _ = disabled_stop(service, monkeypatch)
    if conflict == "closed_control":
        closed["result"]["checks"]["delivery_limits_control"]["reason"] = "Unaccepted change"
    elif conflict == "predecessor_changed":
        original_submit = store.submit

        def submit(value, **kwargs):
            kwargs["_automatic"]["row"]["request_json"] = "{}"
            return original_submit(value, **kwargs)

        monkeypatch.setattr(store, "submit", submit)
    else:
        with store._connect() as db:
            if conflict == "missing_event":
                db.execute("DELETE FROM delivery_events WHERE type='delivery_limits_disabled'")
            else:
                db.execute(
                    "UPDATE delivery_events SET payload_json='{}' "
                    "WHERE type='delivery_limits_disabled'"
                )
    assert retry.retry_once(store) == []
    assert len(store.list_runs()) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("capacity_change", [False, True],
                         ids=["original-capacity", "new-capacity"])
async def test_public_disable_survives_closed_retry_and_service_restart(
    service, tmp_path, monkeypatch, capacity_change
):
    # First principles: the accepted update belongs to the logical delivery;
    # replacing its execution must preserve its receipt and original frozen clock.
    original, request = service
    entered, release, successor_entered, successor_release = (asyncio.Event() for _ in range(4))
    launches = []
    original_admit = DeliveryConfig.admit

    def resource_aware_admit(self, value, **kwargs):
        return {**original_admit(self, value, **kwargs), "resource_cleanup_version": 1}

    monkeypatch.setattr(DeliveryConfig, "admit", resource_aware_admit)

    @activity.defn(name="delivery_prepare")
    async def preparation(payload):
        launches.append(payload["spec"]["run_id"])
        if len(launches) == 1:
            entered.set()
            await release.wait()
        else:
            successor_entered.set()
            await successor_release.wait()
        raise ConnectionError("Owned transport failure after accepted timer control")

    async with local_temporal(
        dev_server_database_filename=str(tmp_path / "temporal.sqlite3")
    ) as env:
        target = env.client.service_client.config.target_host
        raw = copy.deepcopy(original.config.raw)
        raw.update(temporal_address=target, delivery_timeout_seconds=4)
        original.config.path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(original.config.path))
        store.submit(request)
        spec = store.spec(request["run_id"])
        DeliveryBroker(store, spec).prepare()
        client = await Client.connect(target, data_converter=DELIVERY_DATA_CONVERTER)
        app = create_app(store.config.path)
        api = app.state.delivery
        async with Worker(
            client,
            task_queue=store.config.queue,
            workflows=[DeliveryWorkflow],
            activities=[preparation, delivery_project, delivery_finalize_resources],
        ):
            await api.dispatch_once()
            handle = client.get_workflow_handle("delivery-" + spec["run_id"])
            await asyncio.wait_for(entered.wait(), 10)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url=store.config.dashboard_url
            ) as browser:
                login = await browser.get("/api/session")
                state = await handle.query("status")
                response = await browser.post(
                    "/api/runs/" + spec["run_id"] + "/disable-limits",
                    headers={
                        "Origin": store.config.dashboard_url,
                        "X-Devflow-CSRF": login.json()["csrf_token"],
                    },
                    json={
                        "command_id": "disable-clock",
                        "expected_revision": state["revision"],
                        "reason": "Owned fixture operator disables delivery timer",
                    },
                )
                assert response.status_code == 200, response.text
            # Reconstruct the service store after the accepted receipt, and again
            # after successor admission: neither process-local state is authority.
            if capacity_change:
                current = json.loads(store.config.path.read_text())
                current["capacity"] = 1
                store.config.path.write_text(json.dumps(current))
            api.store = store = DeliveryStore(DeliveryConfig.load(store.config.path))
            await asyncio.sleep(4.5)
            active = await handle.query("status")
            assert active["outcome"] is None
            assert active["checks"]["delivery_limits_control"]["disabled"] is True
            assert "delivery_limit" not in active["checks"]
            release.set()
            before = await asyncio.wait_for(handle.result(), 15)
            assert before["outcome"] == "blocked" and before["cleanup"] == "confirmed"
            control = before["checks"]["delivery_limits_control"]
            with store._connect() as db:
                store.state.release_work(
                    db, request["work_id"], "external:devflow:" + spec["run_id"]
                )
            monkeypatch.setattr(retry, "fresh_unpublished_base", lambda *_: spec["base_sha"])
            outcomes = await asyncio.gather(
                *(asyncio.to_thread(retry.retry_once, store) for _ in range(2))
            )
            admitted = [value for values in outcomes for value in values]
            assert len(admitted) == 1
            successor = store.spec(admitted[0]["run_id"])
            assert control_from_spec(successor) == control
            assert successor["delivery_admitted_at"] == spec["delivery_admitted_at"]
            assert successor["policy"] == spec["policy"]
            assert len(store.pending_starts()) == 1
            assert store.pending_starts()[0]["recovery_json"] is None
            api.store = store = DeliveryStore(DeliveryConfig.load(store.config.path))
            await api.dispatch_once()
            successor_handle = client.get_workflow_handle("delivery-" + successor["run_id"])
            await asyncio.wait_for(successor_entered.wait(), 10)
            successor_release.set()
            after = await asyncio.wait_for(successor_handle.result(), 15)
            assert launches == [spec["run_id"], successor["run_id"]]
            assert after["outcome"] == "blocked"
            assert after["checks"]["delivery_limits_control"] == control
            assert after["checks"]["delivery_clock"]["admitted_at"] == spec["delivery_admitted_at"]
            assert "delivery_limit" not in after["checks"]
            assert store.spec(spec["run_id"]) == spec
