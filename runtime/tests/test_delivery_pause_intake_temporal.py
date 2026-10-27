"""Real SDK no-start proof with an unserved, owned Temporal workflow."""
import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
from temporal_test_server import local_temporal
from temporalio.client import Client
from test_delivery_store import service as service

from devflow_temporal import delivery_pause as pause
from devflow_temporal import delivery_pause_intake as intake
from devflow_temporal.delivery_api import _dispatch_start
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore


@pytest.mark.asyncio
async def test_exact_sdk_no_start_pause_then_explicit_same_run_dispatch(service, monkeypatch):
    old, request = service
    async with local_temporal() as environment:
        raw = json.loads(old.config.path.read_text())
        raw["workspace_custody_version"] = 1
        raw["temporal_address"] = environment.client.service_client.config.target_host
        old.config.path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(old.config.path))
        original = DeliveryConfig.admit

        def admit(config, *args, **kwargs):
            spec = original(config, *args, **kwargs)
            spec["pause_intake_version"] = 1
            return spec

        monkeypatch.setattr(DeliveryConfig, "admit", admit)
        store.submit(request)
        with store._connect() as db:
            db.execute("UPDATE delivery_pause_control SET state='running', revision=0, epoch=0 "
                       "WHERE root_id=?", (request["run_id"],))
        from contextlib import nullcontext

        from devflow_temporal import delivery_pause_namespace

        monkeypatch.setattr(delivery_pause_namespace, 'intake_namespace_scope',
                            lambda *_: nullcontext())
        spec = store.effective_spec(request["run_id"])
        root = spec["run_id"]
        with store._connect() as db:
            revision = db.execute("SELECT revision FROM delivery_runs WHERE run_id=?",
                                  (root,)).fetchone()[0]
        pause.request_pause(store, root, "sdk-intake-pause", 0, intake_only=True)
        pause.begin_settlement(store, root, 1)
        other = await Client.connect(store.config.temporal_address, namespace="wrong-namespace")
        with pytest.raises(ValueError, match="endpoint or namespace changed"):
            await intake.seal_with_sdk(store, root, 1, revision, client=other)
        receipt = await intake.seal_with_sdk(store, root, 1, revision,
                                             client=environment.client)
        # A lost acknowledgement repeats the exact proof and returns the same receipt.
        assert await intake.seal_with_sdk(store, root, 1, revision,
                                          client=environment.client) == receipt
        with store._connect() as db:
            stored = db.execute("SELECT payload_json FROM delivery_pause_intake_checkpoints "
                                "WHERE root_id=? AND epoch=1", (root,)).fetchone()
            no_start = json.loads(stored[0])["no_start"]
            assert no_start["state"] == "not_found" and no_start["namespace_id"]
            assert no_start["workflow_id"] == "delivery-" + root
        paused = pause.publish_paused(store, root, 1)
        # The independently owned base remains usable after the source is gone.
        shutil.rmtree(Path(spec["source_path"]))
        with pytest.raises(ValueError, match="resume control state"):
            await intake.activate_with_sdk(store, root, 1, revision, client=environment.client)
        pause.begin_resume(store, root, "sdk-intake-resume", paused["revision"], 1)
        await intake.activate_with_sdk(store, root, 1, revision, client=environment.client)
        item = store.pending_starts()[0]
        await _dispatch_start(SimpleNamespace(store=store, config=store.config),
                              environment.client, item, spec)
        description = await environment.client.get_workflow_handle("delivery-" + root).describe()
        with store._connect() as db:
            control = pause._row(db, root)
            receipt = db.execute("SELECT execution_run_id FROM delivery_pause_start_receipts "
                                 "WHERE root_id=?", (root,)).fetchone()
            assert receipt[0] == description.run_id
            assert not control["execution_run_id"]
            assert db.execute("SELECT state FROM delivery_outbox WHERE run_id=?",
                              (root,)).fetchone()[0] == "sent"
            claim = store.state.claim_for(db, request["work_id"])
            assert claim["owner"] == "external:devflow:" + root
            assert db.execute("SELECT COUNT(*) FROM delivery_pause_steps WHERE root_id=? "
                              "AND step='start'", (root,)).fetchone()[0] == 1


@pytest.mark.asyncio
async def test_sdk_no_start_proof_refuses_an_existing_selected_workflow(service, monkeypatch):
    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    old, request = service
    async with local_temporal() as environment:
        raw = json.loads(old.config.path.read_text())
        raw["workspace_custody_version"] = 1
        raw["temporal_address"] = environment.client.service_client.config.target_host
        old.config.path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(old.config.path))
        original = DeliveryConfig.admit

        def admit(config, *args, **kwargs):
            spec = original(config, *args, **kwargs)
            spec["pause_intake_version"] = 1
            return spec

        monkeypatch.setattr(DeliveryConfig, "admit", admit)
        store.submit(request)
        with store._connect() as db:
            db.execute("UPDATE delivery_pause_control SET state='running', revision=0, epoch=0 "
                       "WHERE root_id=?", (request["run_id"],))
        from contextlib import nullcontext

        from devflow_temporal import delivery_pause_namespace

        monkeypatch.setattr(delivery_pause_namespace, 'intake_namespace_scope',
                            lambda *_: nullcontext())
        spec = store.effective_spec(request["run_id"])
        root = spec["run_id"]
        with store._connect() as db:
            revision = db.execute("SELECT revision FROM delivery_runs WHERE run_id=?",
                                  (root,)).fetchone()[0]
        pause.request_pause(store, root, "pause-before-observation", 0, intake_only=True)
        pause.begin_settlement(store, root, 1)
        # No worker is connected: this owned foreign execution cannot run a role.
        await environment.client.start_workflow(
            DeliveryWorkflow.run, args=[spec, None], id="delivery-" + root,
            task_queue=store.config.queue)
        with pytest.raises(ValueError, match="already exists"):
            await intake.seal_with_sdk(store, root, 1, revision, client=environment.client)
        with store._connect() as db:
            assert pause._row(db, root)["checkpoint_json"] is None
            claim = store.state.claim_for(db, request["work_id"])
            assert claim["owner"] == "external:devflow:" + root
