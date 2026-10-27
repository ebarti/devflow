"""Owned Git and SQLite tests for a never-started logical pause checkpoint."""
import json
import shutil
from pathlib import Path

import pytest
from test_delivery_store import service as service

from devflow_temporal import delivery_pause as pause
from devflow_temporal import delivery_pause_intake as intake
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_pause_dispatch_lease import dispatch_lease
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workspace_custody import source_for


def controlled_no_start(store):
    """The real SDK observation is exercised in the separate Temporal fixture."""
    def validate(db, spec, control):
        row = db.execute("""SELECT workflow_id,request_digest,request_json,recovery_json
            FROM delivery_runs WHERE run_id=?""", (spec["run_id"],)).fetchone()
        selected = {"root_id": spec["run_id"], "epoch": control["epoch"],
                    "workflow_id": row["workflow_id"] or "delivery-" + spec["run_id"],
                    "request_digest": row["request_digest"], "request_json": row["request_json"],
                    "recovery_json": row["recovery_json"]}
        return {"schema": "devflow-sdk-no-start-v1", "state": "not_found",
                "root_id": selected["root_id"], "epoch": selected["epoch"],
                "workflow_id": selected["workflow_id"], "selection_sha256": digest(selected),
                "endpoint": store.config.temporal_address,
                "namespace": store.config.raw.get("temporal_namespace", "default"),
                "namespace_id": "controlled-namespace"}
    return validate


@pytest.fixture
def unstarted(service, monkeypatch):
    old, request = service
    config = json.loads(old.config.path.read_text())
    config["workspace_custody_version"] = 1
    old.config.path.write_text(json.dumps(config))
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
                   "WHERE root_id=?",
                   (request["run_id"],))
    spec = store.effective_spec(request["run_id"])
    with store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                              (spec["run_id"],)).fetchone())
    return store, spec, row


def test_unstarted_checkpoint_preserves_claim_outbox_and_owned_source(unstarted):
    store, spec, row = unstarted
    root = spec["run_id"]
    pause.request_pause(store, root, "pause-intake", 0, intake_only=True)
    pause.begin_settlement(store, root, 1)
    with pytest.raises(ValueError, match="no-start outcome is not proven"):
        intake.seal_unstarted(store, root, 1, row["revision"], prove_unstarted=lambda *_: False)
    with store._connect() as db:
        assert pause._row(db, root)["checkpoint_json"] is None
    receipt = intake.seal_unstarted(store, root, 1, row["revision"],
                                    prove_unstarted=controlled_no_start(store))
    assert receipt["kind"] == "unstarted_intake" and receipt["resumable"] is True
    assert not Path(spec["checkout"]).exists()
    owned_source = source_for(spec)
    shutil.rmtree(Path(spec["source_path"]))
    assert source_for(spec) == owned_source
    paused = pause.publish_paused(store, root, 1)
    item = store.pending_starts()[0]
    with pytest.raises(pause.PauseDeferred):
        with dispatch_lease(store, spec, item):
            pytest.fail("paused intake dispatched without explicit resume")
    with pytest.raises(ValueError, match="state"):
        intake.activate_unstarted(store, root, 1, row["revision"],
                                  prove_unstarted=controlled_no_start(store))
    pause.begin_resume(store, root, "resume-intake", paused["revision"], 1)
    active = intake.activate_unstarted(store, root, 1, row["revision"],
                                       prove_unstarted=controlled_no_start(store))
    assert active["state"] == "running" and active["epoch"] == 1
    with store._connect() as db:
        actual = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (root,)).fetchone()
        assert dict(actual) == row
        outbox = db.execute("SELECT state FROM delivery_outbox WHERE run_id=?", (root,)).fetchone()
        assert outbox[0] == "pending"
        assert store.state.claim_for(db, row["work_id"])["owner"] == "external:devflow:" + root
        assert db.execute("SELECT COUNT(*) FROM delivery_pause_actors WHERE root_id=?",
                          (root,)).fetchone()[0] == 0


def test_unstarted_checkpoint_refuses_any_admitted_start(unstarted):
    store, spec, row = unstarted
    root = spec["run_id"]
    with pause.start_admission(store, root, "delivery-" + root, 1, epoch=0,
                               validate=lambda *_: None):
        pass
    with pytest.raises(ValueError, match="after start admission"):
        pause.request_pause(store, root, "pause-after-start", 0, intake_only=True)
    with store._connect() as db:
        assert pause._row(db, root)["checkpoint_json"] is None


def test_intake_resume_refuses_changed_outbox_or_boolean_revision(unstarted):
    store, spec, row = unstarted
    root = spec["run_id"]
    pause.request_pause(store, root, "pause-intake", 0, intake_only=True)
    pause.begin_settlement(store, root, 1)
    with pytest.raises(ValueError, match="run revision"):
        intake.seal_unstarted(store, root, 1, True, prove_unstarted=controlled_no_start(store))
    intake.seal_unstarted(store, root, 1, row["revision"],
                          prove_unstarted=controlled_no_start(store))
    paused = pause.publish_paused(store, root, 1)
    pause.begin_resume(store, root, "resume-intake", paused["revision"], 1)
    with store._connect() as db:
        db.execute("UPDATE delivery_outbox SET state='sent' WHERE run_id=?", (root,))
    with pytest.raises(ValueError, match="outbox"):
        intake.activate_unstarted(store, root, 1, row["revision"],
                                  prove_unstarted=controlled_no_start(store))
    with store._connect() as db:
        assert pause._row(db, root)["state"] == "resuming"
        assert store.state.claim_for(db, row["work_id"])["owner"] == "external:devflow:" + root
