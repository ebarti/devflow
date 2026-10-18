"""Real active work survives disabling a delivery limit through the public API."""
from __future__ import annotations

import asyncio
import json
import shutil
from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from test_delivery_store import service as service

from devflow_temporal import delivery_control, delivery_limits
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


def test_new_runs_have_no_implicit_delivery_limit(service):
    store, request = service
    store.submit(request)
    spec = store.spec(request["run_id"])
    assert "delivery_limits_version" not in spec
    assert not delivery_limits.policy(spec["policy"])


def test_control_receipt_preserves_concurrent_phase_checks_and_question(service):
    store, request = service
    run = request["run_id"]
    store.submit(request)
    question = {"id": "pending-question", "revision": 1, "kind": "question"}
    store.project(run, phase="waiting_question", execution_state="waiting",
                  event_type="question", message="Newer controller work", protocol_revision=7,
                  checks={"local": {"state": "passed"}}, decision=question)
    receipt = {"disabled": True, "command_id": "earlier-control", "protocol_revision": 5,
               "reason": "User request", "recorded_at": "2026-10-18T17:00:00+00:00"}
    result = {"revision": 5, "phase": "implement", "checks": {"delivery_limits_control": receipt}}
    store.record_delivery_limits_disabled(run, result, "earlier-control")
    current = store.detail(run)
    assert current["phase"] == "waiting_question" and current["decisions"] == [question]
    assert current["protocol_revision"] == 7
    assert current["checks"] == {"local": {"state": "passed"}, "delivery_limits_control": receipt}
    store.record_delivery_limits_disabled(run, result, "earlier-control")
    assert len([e for e in store.events(run) if e["type"] == "delivery_limits_disabled"]) == 1
    store.project(run, phase="waiting_question", execution_state="waiting",
                  event_type="another-check", message="Projection from an earlier snapshot",
                  protocol_revision=4, checks={"ci": {"state": "pending"}}, decision=question)
    assert store.detail(run)["checks"]["delivery_limits_control"] == receipt
    assert store.detail(run)["protocol_revision"] == 7


def test_existing_legacy_admission_receipt_still_replays(service, monkeypatch):
    store, request = service
    raw = json.loads(store.config.path.read_text())
    raw["no_progress_timeout_seconds"] = 1800
    store.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(store.config.path))
    with monkeypatch.context() as old:
        old.setattr(delivery_limits, "require_supported_admission_limits", lambda _: None)
        receipt = store.submit(request)
    original = store.spec(request["run_id"])
    assert store.submit(request) == receipt
    assert store.spec(request["run_id"]) == original
    assert len(store.list_runs()) == 1


def test_live_control_cli_never_invokes_service_lifecycle(service, monkeypatch, capsys, tmp_path):
    store, _ = service
    payload = {"command_id": "disable-live", "expected_revision": 3, "reason": "user request"}
    request = tmp_path / "disable.json"
    request.write_text(json.dumps(payload))
    calls = []

    def forbidden(*_args, **_kwargs):
        pytest.fail("live control must not start or reload a service")

    monkeypatch.setattr(delivery_control, "_config", forbidden)
    monkeypatch.setattr(delivery_control, "api_client", forbidden)
    monkeypatch.setattr(delivery_control, "read_only_client", lambda path: SimpleNamespace(
        disable_delivery_limits=lambda run, value: calls.append((path, run, value)) or {
            "delivery_limits_disabled": True,
        },
    ))
    monkeypatch.setattr("sys.argv", [
        "devflow-delivery", "--config", str(store.config.path), "disable-limits",
        "--id", "same-run", "--request", str(request),
    ])
    delivery_control.main()
    assert calls == [(store.config.path, "same-run", payload)]
    assert json.loads(capsys.readouterr().out)["delivery_limits_disabled"] is True


@pytest.mark.asyncio
async def test_disabled_control_survives_recovery_without_resetting_original_clock(monkeypatch):
    async def wait_condition(predicate, **_kwargs):
        assert predicate()

    monkeypatch.setattr(delivery_limits.workflow, "wait_condition", wait_condition)
    clock = {"admitted_at": "2000-01-01T00:00:00+00:00",
             "last_progress_at": "2000-01-01T00:00:00+00:00", "signature": "old"}
    recovery = {"state": {"checks": {"delivery_limits_control": {"disabled": True},
                                      "delivery_clock": clock}}}
    controller = SimpleNamespace(state={"phase": "running"}, cancel_requested=False,
                                 delivery_limits_closing=False)
    spec = {"delivery_limits_version": 1, "delivery_admitted_at": clock["admitted_at"],
            "policy": {"delivery_timeout_seconds": 1}}

    async def operation(actual_spec, actual_recovery):
        assert actual_spec is spec and actual_recovery is recovery
        assert controller.delivery_limits_disabled
        assert delivery_limits.remaining(controller) is None
        assert controller.delivery_clock == clock
        await asyncio.sleep(0)
        return "same retained work"

    assert await delivery_limits.run(controller, spec, recovery, operation) == "same retained work"


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
