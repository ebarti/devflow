"""Actual public ACK/CI-only dispatch, SDK worker, native custody and replay.

All remote GitHub facts are neutral authored boundary data, not a claim of
GitHub eligibility or real hosted tests. The original provider passes/closed
descriptor and tracker effects are controlled. Current original framework
Python/SDK, -I native monitors, source/resource/claim/config owners, public
preflight/ACK/finite admission, CI activity, workflow/finalizer and replay are
real. No model role, publication, merge or replacement native gate is invoked.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
from test_delivery_ci_inventory import ci_case, public
from test_delivery_store import service as service

from devflow_temporal import delivery_ci_inventory as owner
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore


@pytest.mark.asyncio
async def test_real_public_inventory_then_normal_CI_worker_preserves_original_verdicts(
    service,
    monkeypatch,
    tmp_path,
):
    from temporal_test_server import local_temporal
    from temporalio import activity
    from temporalio.worker import Replayer, Worker

    from devflow_temporal.delivery_activities import (
        delivery_ci,
        delivery_finalize_resources,
        delivery_gates_readback,
        delivery_project,
    )
    from devflow_temporal.delivery_api import create_app
    from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
    from devflow_temporal.delivery_feature_review_context import current_runtime_identity
    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    real_closed = DeliveryStore._completed_temporal_result
    async with local_temporal() as environment:
        initial, submitted = service
        config = copy.deepcopy(initial.config.raw)
        config["temporal_address"] = environment.client.service_client.config.target_host
        initial.config.path.write_text(json.dumps(config))
        initial = DeliveryStore(DeliveryConfig.load(initial.config.path))
        c = ci_case.__wrapped__((initial, submitted), monkeypatch)
        old = copy.deepcopy(c.closed)
        original_native = {}
        for stage in ("local", "prepublish"):
            for value in old["result"]["checks"][stage]["results"]:
                path = Path(value["native_process"]["journal"])
                assert path.is_relative_to(Path(c.case.spec["state_dir"]))
                original_native[path] = path.read_bytes()
        # Identical configured recipes may share the existing native receipt.
        # Authenticate every declared reference, rather than invent a launch count.
        assert original_native
        program = current_runtime_identity()
        identities = []
        for p, raw in original_native.items():
            value = json.loads(raw)
            assert value["runtime_identity"]["revision"] == program["revision"]
            assert value["runtime_identity"]["dirty"] is False
            assert (
                value["runtime_identity"]["runtime_payload_sha256"]
                == program["runtime_payload_sha256"]
            )
            identities.append(
                {
                    "path": str(p),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "runtime_identity": value["runtime_identity"],
                }
            )
        async with public(c) as (client, headers):
            pre = await client.post("/api/runs/run-1/ci-inventory-preflight", json=c.command)
            assert pre.status_code == 200, pre.text
            ack = {
                **c.command,
                "preflight_key": pre.json()["preflight_key"],
                "acknowledgement": owner.ACK,
            }
            response = await client.post(
                "/api/runs/run-1/ack-ci-inventory", json=ack, headers=headers
            )
            assert response.status_code == 200, response.text
            acknowledgement = response.json()
            pre = await client.post("/api/runs/run-1/repair-admission-preflight", json=c.gate)
            assert pre.status_code == 200, pre.text
            response = await client.post(
                "/api/runs/run-1/continue-repair", json=c.gate, headers=headers
            )
            assert response.status_code == 200, response.text
            admitted = response.json()
        current = c.store.effective_spec("run-1")
        with c.store._connect() as db:
            recovery = json.loads(
                db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0]
            )
            assert not db.execute("SELECT 1 FROM delivery_repair_grants").fetchone()
            assert db.execute("SELECT COUNT(*) FROM delivery_ci_inventories").fetchone()[0] == 1
        assert recovery["state"] == old["result"]
        assert current["policy"]["max_repairs"] == c.case.spec["policy"]["max_repairs"]
        assert recovery["command"]["additional_iterations"] == 0
        assert admitted["implementation_authority"] is False
        assert admitted["claim_identity"]
        assert recovery["seal"][owner.FIELD]["acknowledgement"] == acknowledgement["receipt"]

        # Historical metadata never acquires current physical source authority.
        checkout = Path(current["checkout"])
        retained = checkout.with_name(checkout.name + "-held")
        checkout.rename(retained)
        try:
            assert owner.archive(c.store, "run-1")[0] == acknowledgement
            with pytest.raises((ValueError, OSError, subprocess.CalledProcessError)) as refused:
                await delivery_gates_readback({"spec": current, "recovery": recovery})
            if isinstance(refused.value, subprocess.CalledProcessError):
                assert refused.value.returncode == 128
                assert refused.value.cmd == ["git", "-C", str(checkout), "rev-parse", "HEAD"]
            checkout.symlink_to(retained, target_is_directory=True)
            with pytest.raises((ValueError, OSError)):
                await delivery_gates_readback({"spec": current, "recovery": recovery})
            checkout.unlink()
            shutil.copytree(retained, checkout, symlinks=True)
            with pytest.raises(ValueError):
                await delivery_gates_readback({"spec": current, "recovery": recovery})
            shutil.rmtree(checkout)
        finally:
            if checkout.is_symlink():
                checkout.unlink()
            elif checkout.exists():
                assert retained.exists()
                shutil.rmtree(checkout)
            retained.rename(checkout)
        assert await delivery_gates_readback({"spec": current, "recovery": recovery})

        calls = []

        @activity.defn(name="delivery_tracker_start")
        async def tracker_start(_body):
            calls.append("tracker_start")
            return {"state": "consistent"}

        @activity.defn(name="delivery_terminal_tracker")
        async def terminal_tracker(body):
            calls.append("terminal_tracker")
            assert body["release"] is True
            with c.store._connect() as db:
                c.store.state.release_work(db, current["work_id"], "external:devflow:run-1")
            return {"state": "consistent"}

        @activity.defn(name="delivery_tracker")
        async def tracker(_body):
            return {"state": "consistent"}

        # A role/publish/check activity is deliberately not registered. Any
        # unintended model turn or substitute gate makes this real workflow fail.
        app = create_app(c.store.config.path)
        service_owner = app.state.delivery
        sdk = await service_owner.healthy_client()
        async with Worker(
            sdk,
            task_queue=c.store.config.queue,
            workflows=[DeliveryWorkflow],
            activities=[
                delivery_gates_readback,
                delivery_project,
                delivery_ci,
                delivery_finalize_resources,
                tracker_start,
                terminal_tracker,
                tracker,
            ],
        ):
            await service_owner.dispatch_once()
            handle = sdk.get_workflow_handle(admitted["workflow_id"])
            result = await asyncio.wait_for(handle.result(), timeout=120)
            assert result["outcome"] == "delivered", result
            assert result["iteration"] == 3 and result["roles"] == old["result"]["roles"]
            for field in ("prepublish", "local", "review", "qa"):
                assert result["checks"][field] == old["result"]["checks"][field]
            ci = result["checks"]["ci"]
            assert ci["state"] == "passed" and len(ci["checks"]) == 11
            assert ci["execution_inventory"]["PR_ruleset_eligibility"] is False
            assert ci["execution_inventory"]["merge_context_verified"] is False
            assert ci["ci_assessment"]["execution"]["state"] == "observed"
            assert ci["ci_assessment"]["delivery_policy"]["state"] == "passed"
            assert ci["ci_assessment"]["pr_eligibility"]["state"] == "not_established"
            assert ci["ci_assessment"]["merge_readiness"]["state"] == "unverified"
            owner.validate_result(c.store, "run-1", ci)
            assert result["checks"]["resource_cleanup"]["state"] == "confirmed"
            assert calls == ["tracker_start", "terminal_tracker"]
            assert old["result"]["checks"]["ci"]["state"] == "pending"
            assert c.closed == old and all(
                p.read_bytes() == raw for p, raw in original_native.items()
            )
            with c.store._connect() as db:
                assert not db.execute("SELECT 1 FROM delivery_repair_grants").fetchone()
                assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 1
                assert (
                    db.execute("SELECT COUNT(*) FROM delivery_ci_inventory_results").fetchone()[0]
                    == 1
                )
                assert c.store.state.claim_for(db, current["work_id"]) is None
            await Replayer(
                workflows=[DeliveryWorkflow], data_converter=DELIVERY_DATA_CONVERTER
            ).replay_workflow(await handle.fetch_history())
            # Current actual SDK closure/result is read through the real reader;
            # the old historical descriptor remains the explicitly controlled seam.
            closed = await asyncio.to_thread(
                real_closed, c.store, "run-1", workflow_id=admitted["workflow_id"]
            )
            assert closed["result"]["checks"]["ci"] == ci
            # Normal public readers authenticate the separate ACK/result again;
            # they expose JSON receipts, never terminal-control log bodies.
            async with public(c) as (client, _headers):
                detail = await client.get("/api/runs/run-1")
                assert detail.status_code == 200, detail.text
                assert detail.json()["run"]["checks"]["ci"]["ci_assessment"] == ci["ci_assessment"]
                evidence_ids = {item["id"] for item in detail.json()["evidence"]}
                assert {
                    "ordinary-ci-inventory-acknowledgement",
                    "ordinary-ci-execution-observation",
                } <= evidence_ids
                listed = await client.get("/api/runs?limit=50")
                assert listed.status_code == 200, listed.text
                assert len(listed.json()["runs"]) == 1
                for evidence_id in sorted(
                    evidence_ids
                    & {
                        "ordinary-ci-inventory-acknowledgement",
                        "ordinary-ci-execution-observation",
                    }
                ):
                    response = await client.get(f"/api/runs/run-1/evidence/{evidence_id}")
                    assert response.status_code == 200, response.text
                    assert "\x1b" not in response.json()["text"]
                assert (await client.get("/api/runs/run-1/ci-inventory")).json() == acknowledgement
            if output := os.environ.get("DEVFLOW_TEST_CI_INVENTORY_NATIVE_RESULT"):
                with open(output, "x") as stream:
                    json.dump(
                        {
                            "program": program,
                            "old_closed": old,
                            "ack": acknowledgement,
                            "admission": admitted,
                            "recovery": recovery,
                            "result": result,
                            "sdk_closed_workflow_id": closed["workflow_id"],
                            "native_monitor_identities": identities,
                            "qualification": __doc__,
                        },
                        stream,
                        indent=2,
                    )
