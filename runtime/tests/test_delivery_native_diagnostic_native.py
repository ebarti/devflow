"""Real native diagnostic with a neutral committed high-count test subject.

Provider/GitHub/closed Temporal observations and dependency acquisition are the
controlled seams. The public ACK, slot owner, original native spec, scoped
resources, official tools, browser/SDK controls, full Node/xdist/JUnit recipe,
finalizer and acknowledgement-loss replay are real. Product gates remain failed.
"""

from __future__ import annotations

import asyncio
import copy
import json
import os
import time

import httpx
import pytest
from test_delivery_browser_mode_recovery import restore_native_observers
from test_delivery_native_diagnostic import stopped_diagnostic as stopped_diagnostic
from test_delivery_store import service as service
from test_delivery_temporal_prerequisites import owned_sdk_environment

from devflow_temporal import delivery_native_diagnostic as diagnostic
from devflow_temporal import delivery_native_diagnostic_execution as execution
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_resources import RunResources, read_private


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["pass_ackloss", "missing_browser", "short"])
async def test_real_public_diagnostic_runs_same_native_readiness_and_complete_frozen_recipe(
    stopped_diagnostic,
    monkeypatch,
    tmp_path,
    mode,
):
    case = stopped_diagnostic
    restore_native_observers(monkeypatch)
    if mode == "missing_browser":
        home = tmp_path / "empty-controller-home"
        home.mkdir(mode=0o700)
        monkeypatch.setenv("HOME", str(home))
    original_files = {
        p: p.read_bytes()
        for p in (
            case.registry.manifest,
            case.registry.state / "resources/finalization.json",
            case.folder / "request.json",
            case.folder / "result.json",
            case.folder / "native-process.json",
        )
    }
    before_state = copy.deepcopy(case.state)
    with case.store._connect() as db:
        before_rows = diagnostic._rows(db, "run-1")
    setup = DeliveryBroker.run_implementation_preparation

    def prepared_fixture(broker, iteration, candidate):
        if broker.diagnostic_context is None:
            return setup(broker, iteration, candidate)
        # Locked SDK/browser dependency acquisition is owned test setup; no
        # command/cwd/timeout/count/JUnit/environment owner is replaced.
        owned_sdk_environment(broker)
        return {
            "state": "passed",
            "cleanup": "confirmed",
            "candidate_id": candidate["id"],
            "fixture_dependency_acquisition": True,
        }

    monkeypatch.setattr(DeliveryBroker, "run_implementation_preparation", prepared_fixture)
    original_ack = execution._ack_complete
    lost = []

    def lose_one_ack(*args):
        if mode == "pass_ackloss" and not lost:
            lost.append(True)
            raise RuntimeError("owned crash after immutable diagnostic receipt fsync")
        return original_ack(*args)

    monkeypatch.setattr(execution, "_ack_complete", lose_one_ack)
    app = create_app(case.store.config.path)
    host = case.store.config.dashboard_url
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url=host) as client:
        headers = {
            "Origin": host,
            "X-Devflow-CSRF": (await client.get("/api/session")).json()["csrf_token"],
        }
        path = "/api/runs/run-1/"
        response = await client.post(
            path + "native-diagnostic-preflight", json=case.diagnostic_request
        )
        assert response.status_code == 200, response.text
        command = {
            **case.diagnostic_request,
            "preflight_key": response.json()["preflight_key"],
            "acknowledgement": diagnostic.ACK,
        }
        started = time.monotonic()
        response = await client.post(path + "ack-native-diagnostic", json=command, headers=headers)
        assert response.status_code == 200, response.text
        assert response.json()["state"] == "queued"
        assert time.monotonic() - started < 30  # POST never waits for a 2700s gate.
        await diagnostic.dispatch_once(app.state.delivery)
        task = next(iter(app.state.delivery.native_diagnostic_tasks.values()))
        await asyncio.wait_for(asyncio.shield(task), timeout=300)
        response = await client.get(path + "native-diagnostics/" + command["command_id"])
        assert response.status_code == 200, response.text
        observed = response.json()
        if output := os.environ.get("DEVFLOW_TEST_NATIVE_DIAGNOSTIC_RESULT"):
            with open(output + "." + mode + ".initial.json", "x") as stream:
                json.dump(observed, stream, indent=2)
        assert observed["state"] == ("unknown" if mode == "pass_ackloss" else "complete"), observed
        # Replay acknowledges the SAME completed receipt; it never executes the
        # worker recipe twice or acquires a new diagnostic/resource generation.
        response = await client.post(path + "ack-native-diagnostic", json=command, headers=headers)
        assert response.status_code == 200, response.text
        value = response.json()
        assert value["state"] == "complete", value
        receipt = value["receipt"]
        assert receipt["result"]["state"] == ("passed" if mode == "pass_ackloss" else "failed"), (
            receipt
        )
        readiness = receipt["result"]["readiness"]
        assert readiness["state"] == ("failed" if mode == "missing_browser" else "passed")
        assert readiness["acceptance_gate"] is False
        assert len(readiness["junit"]["test_cases"]) == 4
        assert receipt["cleanup"]["resources"]["state"] == "confirmed"
        assert receipt["delivery_acceptance"] is False and receipt["provider_turns"] == 0
        root = diagnostic._root(case.spec, value["identity"])
        if mode == "missing_browser":
            assert receipt["result"]["stage"] == "readiness"
            assert not (root / "run-1/checks/3/worker-regressions/native").exists()
        else:
            assert receipt["result"]["stage"] == "frozen_check"
            check = receipt["result"]["check"]
            assert check["state"] == ("passed" if mode == "pass_ackloss" else "failed")
            count = 1012 if mode == "pass_ackloss" else 12
            assert check["results"][0]["test_count"] == count
            assert check["results"][0]["argv"] == diagnostic._check(case.spec)["argv"]
            assert check["results"][0]["junit"]["passed"] == count
            assert not check["results"][0]["junit"]["errors"]
            assert not check["results"][0]["junit"]["failures"]
            native_journal = read_private(
                root / "run-1/checks/3/worker-regressions/native/native-process.json"
            )
            assert native_journal["intent"]["argv"] == diagnostic._check(case.spec)["argv"]
            assert native_journal["intent"]["timeout"] == 2700
            assert native_journal["monitoring_complete"] is True
        assert len(
            list(root.glob("run-1/checks/3/worker-regressions/native*/native-process.json"))
        ) == (0 if mode == "missing_browser" else 1)
        assert not (root / "run-1/checks/3/worker-regressions/native-retry").exists()
        replay = await client.post(path + "ack-native-diagnostic", json=command, headers=headers)
        assert replay.status_code == 200 and replay.json() == value
        (tmp_path / "native-diagnostic-result.json").write_text(json.dumps(value, indent=2) + "\n")
        if output := os.environ.get("DEVFLOW_TEST_NATIVE_DIAGNOSTIC_RESULT"):
            with open(output + "." + mode + ".json", "x") as stream:
                json.dump(value, stream, indent=2)
    with case.store._connect() as db:
        assert diagnostic._rows(db, "run-1") == before_rows
        assert case.store.state.claim_for(db, case.spec["work_id"]) is None
    assert case.state == before_state and case.state["roles"][-1]["status"] == "blocked"
    assert all(p.read_bytes() == raw for p, raw in original_files.items())
    assert (
        RunResources(case.spec, read_only=True).manifest.read_bytes()
        == original_files[case.registry.manifest]
    )
