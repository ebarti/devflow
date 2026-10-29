"""A public source restore permits only proven missing child allocations."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import httpx
import pytest
from test_delivery_api import api_fixture as api_fixture

from devflow_temporal.candidate import candidate_for
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_resources import RunResources, read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore


def git(path, *args):
    return subprocess.check_output(["git", "-C", str(path), *args], text=True).strip()


@pytest.fixture
async def restored_allocation(api_fixture, request):
    preservation = getattr(request, "param", None)
    path, supplied = api_fixture
    raw = json.loads(path.read_text())
    raw["workspace_custody_version"] = 1
    source = Path(raw["repositories"]["fixture"]["source_path"])
    (source / ".gitignore").write_text("node_modules/\n")
    git(source, "add", ".gitignore")
    git(source, "commit", "-qm", "Fixture ignored dependency output")
    raw["repositories"]["fixture"]["expected_base_sha"] = git(source, "rev-parse", "HEAD")
    path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(supplied)
    spec = store.effective_spec(supplied["run_id"])
    broker = DeliveryBroker(store, spec)
    broker.checkout.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    resources = RunResources(spec)
    resources.register(broker.checkout, "checkout")
    broker.prepare()
    resources.created(broker.checkout)
    dependency = broker.checkout / "node_modules"
    resources.register(dependency, "generated")
    dependency.mkdir(mode=0o700)
    (dependency / "owned-fixture.txt").write_text("regenerable dependency bytes\n")
    resources.created(dependency)
    if preservation == "unproven":
        current = read_private(resources.manifest)
        current["roots"][str(dependency)]["generated_output_preservation"] = {
            "ownership": "unproven", "disposable": False,
        }
        write_private(resources.manifest, current)
    original = read_private(resources.manifest)
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET execution_state='blocked',phase='blocked',"
            "outcome='blocked',usage_json=? WHERE run_id=?",
            (json.dumps({"tokens": 731, "wall_seconds": 19}), spec["run_id"]),
        )
        row = dict(db.execute(
            "SELECT * FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
        ).fetchone())
    before = candidate_for(broker.checkout)
    app = create_app(path)
    url = store.config.dashboard_url
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=url) as client:
        session = (await client.get("/api/session")).json()
        headers = {"origin": url, "x-devflow-csrf": session["csrf_token"]}
        route = "/api/runs/" + spec["run_id"] + "/checkpoints"
        saved = await client.post(route, headers=headers, json={
            "command_id": "save-generated", "expected_revision": row["revision"],
        })
        if preservation == "unproven":
            assert saved.status_code == 409, saved.text
            yield (
                store, spec, broker, resources, dependency, original, None,
                saved.json(), client, route,
            )
            assert read_private(resources.manifest) == original
            assert candidate_for(broker.checkout) == before
            return
        assert saved.status_code == 200, saved.text
        checkpoint = saved.json()
        archive = Path(spec["state_dir"]) / "checkpoints" / checkpoint["checkpoint_id"]
        archived_manifest = (archive / "state/resources/manifest.json").read_bytes()
        shutil.rmtree(source)
        shutil.rmtree(broker.checkout)
        restored = await client.post(route + "/" + checkpoint["checkpoint_id"] + "/restore",
                                    headers=headers, json={
                                        "command_id": "restore-generated",
                                        "expected_revision": row["revision"],
                                    })
        assert restored.status_code == 200, restored.text
        assert restored.json()["execution_started"] is False
        assert candidate_for(broker.checkout) == before
        assert not dependency.exists()
        assert read_private(resources.manifest) == original
        yield (
            store, spec, broker, resources, dependency, original, archive, checkpoint, client, route
        )
        readback = await client.get(route + "/" + checkpoint["checkpoint_id"])
        assert readback.status_code == 200, readback.text
        assert readback.json()["manifest_sha256"] == checkpoint["manifest_sha256"]
        assert (archive / "state/resources/manifest.json").read_bytes() == archived_manifest


async def test_restored_checkout_regenerates_acknowledged_missing_dependencies(restored_allocation):
    store, spec, broker, resources, dependency, original, *_ = restored_allocation
    before = candidate_for(broker.checkout)
    previous = original["roots"][str(dependency)]
    resources.register(dependency, "generated")
    allocated = read_private(resources.manifest)["roots"][str(dependency)]
    assert allocated["identity"] is None
    assert allocated["generation"] == previous.get("generation", 0) + 1
    proof = allocated["checkpoint_restore_allocation"]
    assert proof["previous_identity"] == previous["identity"]
    dependency.mkdir(mode=0o700)
    resources.created(dependency)
    current = read_private(resources.manifest)["roots"][str(dependency)]
    assert current["identity"] != previous["identity"]
    assert current["checkpoint_restore_allocation"] == proof
    assert candidate_for(broker.checkout) == before
    with store._connect() as db:
        assert json.loads(db.execute("SELECT usage_json FROM delivery_runs WHERE run_id=?",
                                     (spec["run_id"],)).fetchone()[0])["tokens"] == 731


@pytest.mark.parametrize("restored_allocation", ["unproven"], indirect=True)
async def test_checkpoint_refuses_unproven_generated_output_before_claiming_resumable(
    restored_allocation,
):
    store, spec, broker, resources, dependency, original, archive, refusal, *_ = (
        restored_allocation
    )
    assert "needs custody reconciliation" in refusal["detail"]
    assert "checkpoint_id" not in refusal and archive is None
    assert (dependency / "owned-fixture.txt").read_bytes() == b"regenerable dependency bytes\n"
    assert Path(spec["source_path"]).is_dir() and broker.checkout.is_dir()
    assert read_private(resources.manifest) == original
    assert original["roots"][str(dependency)]["generated_output_preservation"] == {
        "ownership": "unproven", "disposable": False,
    }
    assert not list((Path(spec["state_dir"]) / "checkpoints").glob("*/manifest.json"))
    with store._connect() as db:
        assert db.execute(
            "SELECT 1 FROM delivery_commands WHERE run_id=? AND command_id LIKE 'checkpoint-%'",
            (spec["run_id"],),
        ).fetchone() is None
