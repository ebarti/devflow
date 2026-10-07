from __future__ import annotations

import asyncio
import copy
import json
import shutil
import sys
from pathlib import Path

import httpx
import pytest
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.worker import Replayer, Worker
from test_delivery_store import service as service

from devflow_temporal.delivery_activities import (
    delivery_baseline_checks,
    delivery_prepare,
    delivery_project,
)
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_baseline import run_baseline_checks
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_resources import RunResources, _gate_path
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow


def configure(store, *, passes=False):
    raw = copy.deepcopy(store.config.raw)
    recipe = {
        "id": "shared-regression", "kind": "check", "cwd": ".",
        "argv": [sys.executable, "-c", "import sys; print('shared baseline'); "
                 + ("sys.exit(0)" if passes else "sys.exit(1)")],
    }
    raw["repositories"]["fixture"].update(
        prepublish_checks=[recipe], checks=[recipe], baseline_check_ids=[recipe["id"]]
    )
    store.config.path.write_text(json.dumps(raw))
    return DeliveryStore(DeliveryConfig.load(store.config.path))


@pytest.mark.parametrize("ids", [["missing"], ["shared-regression", "shared-regression"], None])
def test_baseline_admission_rejects_missing_duplicate_or_invalid_recipe(service, ids):
    store, request = service
    store = configure(store)
    raw = store.config.raw
    raw["repositories"]["fixture"]["baseline_check_ids"] = ids
    store.config.path.write_text(json.dumps(raw))
    with pytest.raises(ValueError, match="baseline"):
        DeliveryConfig.load(store.config.path).admit(request)


@pytest.mark.parametrize("passes", [False, True])
def test_baseline_executes_on_clean_base_and_preserves_feature_source(service, passes):
    store, request = service
    store = configure(store, passes=passes)
    store.submit(request)
    spec = store.spec(request["run_id"])
    assert spec["baseline_checks_version"] == 1
    assert spec["policy"]["baseline_checks"] == spec["policy"]["prepublish_checks"]
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    # A recovered/changed feature must not contaminate baseline measurement.
    (broker.checkout / "README.md").write_text("Feature source must remain here\n")
    before = broker.candidate()
    result = run_baseline_checks(broker)
    assert result["state"] == ("passed" if passes else "failed")
    assert result["base_sha"] == spec["base_sha"]
    assert result["baseline_candidate"]["head"] == spec["base_sha"]
    assert result["feature_unchanged"] and result["source_unchanged"]
    assert broker.candidate() == before
    assert result["baseline_candidate"]["id"] != before["id"]
    log = Path(result["results"][0]["log"])
    assert log.read_text().strip() == "shared baseline"
    path = _gate_path(spec, "baseline", 0)
    assert (path / "README.md").read_text() == "Test repository\n"
    (path / "README.md").write_text("Contaminated baseline\n")
    with pytest.raises(ValueError, match="clean"):
        run_baseline_checks(broker)


def test_legacy_specs_cannot_allocate_a_baseline_checkout(service):
    store, request = service
    store.submit(request)
    spec = store.spec(request["run_id"])
    assert "baseline_checks_version" not in spec
    assert "baseline_checks" not in spec["policy"]
    with pytest.raises(ValueError, match="admission"):
        _gate_path(spec, "baseline", 0)
    with pytest.raises(ValueError, match="resource"):
        RunResources(spec).register(Path(spec["state_dir"]) / "gates/0/baseline", "gate")


@pytest.mark.asyncio
@pytest.mark.parametrize("passes", [False, True])
async def test_two_public_concurrent_runs_measure_baseline_before_roles_and_replay(
    service, tmp_path, passes,
):
    store, request = service
    store = configure(store, passes=passes)
    app = create_app(store.config.path)
    store = app.state.delivery.store
    role_calls = []

    @activity.defn(name="delivery_role")
    async def unexpected_role(body):
        role_calls.append(body)
        assert passes, "Broken shared baseline must not spend a feature turn"
        return {"status": "blocked", "candidate": body["candidate"], "cleanup": "confirmed"}

    @activity.defn(name="delivery_tracker_start")
    async def tracker_start(_body):
        assert passes
        return {"state": "consistent"}

    async with local_temporal(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "baseline-temporal.sqlite3"),
    ) as environment:
        async def client():
            return environment.client

        app.state.delivery.client = client
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 10001)),
            base_url=store.config.dashboard_url,
        ) as api:
            session = (await api.get("/api/session")).json()
            headers = {
                "Origin": store.config.dashboard_url, "X-Devflow-CSRF": session["csrf_token"],
            }
            requests = [request, {**request, "command_id": "command-2", "run_id": "run-2",
                                 "work_id": "work-2", "branch": "feat/fixture2",
                                 "issue_url": "https://github.com/example/fixture/issues/4"}]
            for body in requests:
                submitted = await api.post("/api/runs", json=body, headers=headers)
                assert submitted.status_code == 200, submitted.text
            async with Worker(
                environment.client, task_queue="baseline-fixture", workflows=[DeliveryWorkflow],
                activities=[delivery_prepare, delivery_project, delivery_baseline_checks,
                            unexpected_role, tracker_start],
            ):
                handles = [await environment.client.start_workflow(
                    DeliveryWorkflow.run, store.spec(body["run_id"]),
                    id="delivery-" + body["run_id"], task_queue="baseline-fixture",
                ) for body in requests]
                results = await asyncio.wait_for(asyncio.gather(*[h.result() for h in handles]), 30)
                assert len(role_calls) == (2 if passes else 0)
                for body, result, handle in zip(requests, results, handles, strict=True):
                    assert result["outcome"] == "blocked"
                    assert result["iteration"] == 0
                    if not passes:
                        assert result["roles"] == []
                        assert result["error"] == (
                            "project baseline failed before feature work: shared-regression"
                        )
                    else:
                        assert len(result["roles"]) == 1
                    expected = "passed" if passes else "failed"
                    assert result["checks"]["baseline"]["state"] == expected
                    detail = (await api.get("/api/runs/" + body["run_id"])).json()["run"]
                    assert detail["checks"]["baseline"]["base_sha"] == (
                        store.spec(body["run_id"])["base_sha"]
                    )
                    assert not detail["intake"]
                    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
                        await handle.fetch_history()
                    )
