"""Real active work survives disabling a delivery limit through the public API."""
from __future__ import annotations

import asyncio
import json
import shutil
from datetime import timedelta

import httpx
import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from test_delivery_store import service as service

from devflow_temporal import delivery_limits
from devflow_temporal.delivery_activities import delivery_finalize_resources, delivery_project
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow


def test_stage_based_no_progress_limit_is_refused_before_claiming_work(service):
    store, request = service
    raw = json.loads(store.config.path.read_text())
    raw["no_progress_timeout_seconds"] = 1800
    store.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(store.config.path))
    public = store.config.public_policy()
    assert public["delivery_limits"] == {}
    assert public["unsupported_delivery_limits"] == ["no_progress_timeout_seconds"]
    with pytest.raises(ValueError, match="no-progress.*not supported"):
        store.submit(request)
    assert not store.list_runs()


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["delivery_timeout_seconds", "no_progress_timeout_seconds"])
async def test_public_disable_keeps_same_active_execution_past_its_old_deadline(
    service, tmp_path, monkeypatch, key,
):
    store, request = service
    started, release, stopped = asyncio.Event(), asyncio.Event(), asyncio.Event()
    launches = []
    original_admit = DeliveryConfig.admit

    def legacy_admission(self, value):
        # Authenticate an old frozen policy, including the retired no-progress setting.
        return {**original_admit(self, value),
                "resource_cleanup_version": 1}

    monkeypatch.setattr(DeliveryConfig, "admit", legacy_admission)
    monkeypatch.setattr(delivery_limits, "require_supported_admission_limits", lambda _: None)

    @activity.defn(name="delivery_prepare")
    async def productive(_payload):
        launches.append("same activity")
        started.set()
        try:
            while not release.is_set():
                with dirty.open("a") as stream:
                    stream.write("completed owned work\n")
                activity.heartbeat("owned work completed")
                await asyncio.sleep(0.1)
            return {}
        finally:
            stopped.set()

    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "live-limits.sqlite3"),
    ) as env:
        raw = json.loads(store.config.path.read_text())
        raw[key] = 4
        store.config.path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(store.config.path))
        store.submit(request)
        spec = store.spec(request["run_id"])
        broker = DeliveryBroker(store, spec)
        broker.prepare()
        dirty = broker.checkout / "README.md"
        app = create_app(store.config.path)
        api = app.state.delivery
        api.store = store

        async def client():
            return env.client

        monkeypatch.setattr(api, "client", client)
        async with Worker(
            env.client, task_queue="live-limits", workflows=[DeliveryWorkflow],
            activities=[productive, delivery_finalize_resources, delivery_project],
            default_heartbeat_throttle_interval=timedelta(seconds=1),
        ):
            handle = await env.client.start_workflow(
                DeliveryWorkflow.run, spec, id="delivery-" + spec["run_id"],
                task_queue="live-limits",
            )
            store.mark_start(spec["run_id"], accepted=True)
            before = await handle.describe()
            try:
                await asyncio.wait_for(started.wait(), 10)
                transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://127.0.0.1:18770",
                ) as browser:
                    login = await browser.get("/api/session")
                    headers = {"Origin": "http://127.0.0.1:18770",
                               "X-Devflow-CSRF": login.json()["csrf_token"]}
                    state = await handle.query("status")
                    payload = {"command_id": "disable-clock",
                               "expected_revision": state["revision"],
                               "reason": "User removed the delivery limits"}
                    url = f"/api/runs/{spec['run_id']}/disable-limits"
                    denied = await browser.post(url, json=payload)
                    assert denied.status_code == 403
                    stale = await browser.post(
                        url, json={**payload, "command_id": "stale-clock", "expected_revision": -1},
                        headers=headers,
                    )
                    assert stale.status_code == 409
                    finish = store.finish_mutation
                    lost = []

                    def lose_first_ack(command_id, response):
                        if command_id == "disable-clock" and not lost:
                            lost.append(command_id)
                            raise RuntimeError("fixture lost acknowledgement after workflow update")
                        return finish(command_id, response)

                    monkeypatch.setattr(store, "finish_mutation", lose_first_ack)
                    uncertain = await browser.post(url, json=payload, headers=headers)
                    assert uncertain.status_code == 500
                    assert (await handle.query("status"))["checks"]["delivery_limits_control"][
                        "disabled"] is True
                    response = await browser.post(url, json=payload, headers=headers)
                    assert response.status_code == 200, response.text
                    assert response.json()["delivery_limits_disabled"] is True
                    replay = await browser.post(url, json=payload, headers=headers)
                    assert replay.json() == response.json()
                    assert (await handle.query("status"))["revision"] == state["revision"] + 1
                    saved = store.detail(spec["run_id"])["checks"]["delivery_limits_control"]
                    assert saved["disabled"] is True and saved["command_id"] == "disable-clock"
                    initial_lines = len(dirty.read_text().splitlines())
                    await asyncio.sleep(4.5)
                    assert not stopped.is_set() and launches == ["same activity"]
                    assert len(dirty.read_text().splitlines()) > initial_lines
                    assert (await handle.describe()).run_id == before.run_id
                    current = await handle.query("status")
                    assert not current.get("outcome") and "delivery_limit" not in current["checks"]
                    cancel = await browser.post(
                        f"/api/runs/{spec['run_id']}/cancel",
                        json={"command_id": "ordinary-cancel",
                              "expected_revision": current["revision"],
                              "reason": "Finish owned fixture after live disable proof"},
                        headers=headers,
                    )
                    assert cancel.status_code == 200
                    release.set()
                    result = await asyncio.wait_for(handle.result(), 10)
                    assert result["outcome"] == "cancelled"
                    assert "time limit reached" not in (result.get("error") or "")
                    assert result["checks"]["delivery_limits_control"]["disabled"] is True
                    denied = await browser.post(
                        url, json={**payload, "command_id": "after-terminal",
                                   "expected_revision": result["revision"]}, headers=headers,
                    )
                    assert denied.status_code == 409
                    assert (await browser.post(url, json=payload, headers=headers)).json() == (
                        response.json())
                    history = await handle.fetch_history()
            finally:
                release.set()
                if not (await handle.describe()).close_time:
                    current = await handle.query("status")
                    if not current.get("outcome"):
                        await handle.execute_update("cancel", {
                            "command_id": "fixture-cleanup",
                            "expected_revision": current["revision"],
                            "reason": "Settle failed fixture",
                        })
                    await asyncio.wait_for(handle.result(), 10)
        await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
