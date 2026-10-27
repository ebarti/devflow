"""Restore the actual published v1 source format without inventing old flags."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import httpx
import pytest
from test_delivery_checkpoints import (
    stopped as stopped,
)
from test_delivery_checkpoints import (
    test_public_restore_process_loss_reserves_and_preserves_unknown as public_restore_loss_case,
)
from test_delivery_store import service as service

from devflow_temporal import delivery_checkpoints as checkpoints
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_checkpoint_index import observe_index
from devflow_temporal.delivery_retained_checkpoint import RetainedCheckpointCustody

_CURRENT_SOURCE_PRODUCER = checkpoints._candidate_snapshot


@pytest.fixture
def legacy_v1(stopped, monkeypatch):
    # This is the exact pre-change source producer, not an edited new manifest or
    # rewritten ACK. The ordinary create path seals and acknowledges its output.
    path = Path(__file__).parent / "fixtures/checkpoint_v1/source_producer.py"
    module = dict(vars(checkpoints))
    exec(compile(path.read_text(), str(path), "exec"), module)
    monkeypatch.setattr(checkpoints, "_candidate_snapshot", module["_candidate_snapshot"])
    return stopped


def test_original_v1_restore_receipt_replays_without_inferred_original_flags(
    legacy_v1, monkeypatch,
):
    store, spec, broker, row = legacy_v1
    saved = checkpoints.create_checkpoint(store, spec["run_id"], "v1-old-receipt", row["revision"])
    path = Path(__file__).parent / "fixtures/checkpoint_v1/source_producer.py"
    module = dict(vars(checkpoints))
    exec(compile(path.read_text(), str(path), "exec"), module)
    shutil.rmtree(broker.checkout)
    with monkeypatch.context() as old_runtime:
        old_runtime.setattr(checkpoints, "_finish_restore", module["_finish_restore"])
        original = checkpoints.restore_checkpoint(store, spec["run_id"], saved["checkpoint_id"],
                                                  "old-v1-restore", row["revision"])
    assert "index_reconstruction" not in original and not original["execution_started"]
    assert checkpoints.restore_checkpoint(store, spec["run_id"], saved["checkpoint_id"],
                                          "old-v1-restore", row["revision"]) == original
    inode = broker.checkout.stat().st_ino
    # Corrupt prior success is an unknown receipt, never a reason to replace the
    # retained workspace or invent a new original-index baseline.
    with store._connect() as db:
        corrupt = {**original, "index_reconstruction": None}
        db.execute("UPDATE delivery_commands SET response_json=? WHERE command_id=?",
                   (json.dumps(corrupt), "checkpoint-restore:old-v1-restore"))
    with pytest.raises(ValueError, match="unknown index reconstruction"):
        checkpoints.restore_checkpoint(store, spec["run_id"], saved["checkpoint_id"],
                                       "old-v1-restore", row["revision"])
    assert broker.checkout.stat().st_ino == inode


def test_ambiguous_v1_flags_preserve_prepared_source_without_fabricated_ack(legacy_v1):
    store, spec, broker, row = legacy_v1
    checkpoints._git(broker.checkout, "update-index", "--assume-unchanged", "README.md")
    before = (broker.checkout / "README.md").read_bytes()
    saved = checkpoints.create_checkpoint(store, spec["run_id"], "ambiguous-v1", row["revision"])
    manifest = checkpoints._root(spec) / saved["checkpoint_id"] / "manifest.json"
    original = manifest.read_bytes()
    value = json.loads(original)
    archived = manifest.parent / value["source"]["files"]["README.md"]["artifact"]
    shutil.rmtree(broker.checkout)
    for _retry in range(2):
        with pytest.raises(ValueError, match="restored file changed"):
            checkpoints.restore_checkpoint(store, spec["run_id"], saved["checkpoint_id"],
                                           "ambiguous-restore", row["revision"])
        stages = list(broker.checkout.parent.glob(".checkpoint-restore-*"))
        assert len(stages) == 1
        assert (stages[0] / "README.md").exists() and archived.read_bytes() == before
    assert manifest.read_bytes() == original and not broker.checkout.exists()
    with store._connect() as db:
        assert not db.execute("SELECT 1 FROM delivery_commands WHERE command_id=?",
                              ("checkpoint-restore:ambiguous-restore",)).fetchone()


@pytest.mark.asyncio
async def test_public_v1_restore_reconstructs_recorded_source_without_rewriting_ack(
    legacy_v1, monkeypatch,
):
    store, spec, broker, row = legacy_v1
    saved = checkpoints.create_checkpoint(store, spec["run_id"], "actual-v1", row["revision"])
    manifest = checkpoints._root(spec) / saved["checkpoint_id"] / "manifest.json"
    original = manifest.read_bytes()
    assert "index_semantics" not in json.loads(original)["source"]
    staged = checkpoints._git(broker.checkout, "diff", "--cached", "--binary", "--full-index")
    assert checkpoints.create_checkpoint(store, spec["run_id"], "actual-v1",
                                         row["revision"]) == saved
    with pytest.raises(ValueError, match="baseline"):
        with RetainedCheckpointCustody(store, spec["run_id"], saved["checkpoint_id"],
                                      row["revision"]) as owner:
            owner.acknowledge("legacy-no-copy")
    shutil.rmtree(Path(spec["source_path"]))
    shutil.rmtree(broker.checkout)
    url = store.config.dashboard_url.rstrip("/")
    path = "/api/runs/" + spec["run_id"] + "/checkpoints/" + saved["checkpoint_id"]
    command = {"command_id": "v1-public-restore", "expected_revision": row["revision"]}
    app = create_app(store.config.path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=url) as client:
        session = (await client.get("/api/session")).json()
        headers = {"origin": url, "x-devflow-csrf": session["csrf_token"]}
        read = await client.get(path)
        assert read.status_code == 200 and read.json()["acknowledged_resumable"]
        assert not read.json()["resumable"] and read.json()["legacy_index_recovery"]
        restored = await client.post(path + "/restore", headers=headers, json=command)
        assert restored.status_code == 200, restored.text
        receipt = restored.json()
        assert receipt["index_reconstruction"] == {
            "schema": "devflow-legacy-index-reconstruction-v1",
            "original_index_flags": "not_recorded",
            "restored_index_semantics": observe_index(broker.checkout, checkpoints._git),
        }
        assert not receipt["execution_started"]
    # A new app/store process view reacquires all custody and the public command
    # replays the one durable reconstruction rather than allocating again.
    app = create_app(store.config.path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=url) as client:
        session = (await client.get("/api/session")).json()
        headers = {"origin": url, "x-devflow-csrf": session["csrf_token"]}
        replay = await client.post(path + "/restore", headers=headers, json=command)
        assert replay.status_code == 200 and replay.json() == receipt
        created_replay = await client.post("/api/runs/" + spec["run_id"] + "/checkpoints",
            headers=headers, json={"command_id": "actual-v1", "expected_revision": row["revision"]})
        assert created_replay.status_code == 200 and created_replay.json() == saved
        monkeypatch.setattr(checkpoints, "_candidate_snapshot", _CURRENT_SOURCE_PRODUCER)
        fresh = await client.post("/api/runs/" + spec["run_id"] + "/checkpoints", headers=headers,
            json={"command_id": "fresh-after-v1-recovery", "expected_revision": row["revision"]})
        assert fresh.status_code == 200, fresh.text
        fresh_path = "/api/runs/" + spec["run_id"] + "/checkpoints/" + fresh.json()["checkpoint_id"]
        verified = await client.post(fresh_path + "/verify-retained", headers=headers,
            json={"command_id": "fresh-source-verified", "expected_revision": row["revision"]})
        assert verified.status_code == 200, verified.text
        assert verified.json()["retained_verified"] and not verified.json()["execution_started"]
        checkpoints._git(broker.checkout, "update-index", "--assume-unchanged", "README.md")
        refused = await client.post(path + "/restore", headers=headers, json=command)
        assert refused.status_code == 409
    assert manifest.read_bytes() == original
    assert checkpoints._git(broker.checkout, "diff", "--cached", "--binary",
                            "--full-index") == staged
    assert (broker.checkout / "owned-untracked.txt").read_text() == "recover me\n"
    with store._connect() as db:
        assert db.execute("SELECT count(*) FROM delivery_events WHERE run_id=? AND type=?",
                          (spec["run_id"], checkpoints.RESTORED)).fetchone()[0] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["before_mkdir", "after_mkdir", "git_active"])
async def test_actual_v1_public_restore_process_loss_preserves_unknown(legacy_v1, boundary):
    # Reuse the existing joined, owned process-death test through its public
    # endpoint. It requests no provider/native or external publication work.
    await public_restore_loss_case(legacy_v1, boundary)
