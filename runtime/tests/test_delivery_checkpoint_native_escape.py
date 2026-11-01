"""An observed native monitor cannot prove every descendant has settled."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from temporal_test_server import local_temporal
from temporalio import workflow
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_api import api_fixture as api_fixture
from test_native_interrupted_gate import native_gates

from devflow_temporal.delivery_api import _dispatch_start, create_app
from devflow_temporal.delivery_native_process import NativeProcess, process_table
from devflow_temporal.delivery_resources import read_private
from devflow_temporal.delivery_writer_custody import to_thread


@workflow.defn(name="DevflowDeliveryWorkflow")
class NativeCheckpointOwner:
    @workflow.run
    async def run(self, spec, recovery=None):
        return {"run_id": spec["run_id"], "execution_state": "blocked",
                "outcome": "blocked", "iteration": 0}


@pytest.mark.asyncio
async def test_public_native_checkpoint_refuses_escaped_post_go_descendant(
    api_fixture, tmp_path,
):
    async with local_temporal() as env:
        config_path, _request = api_fixture
        raw = json.loads(config_path.read_text())
        raw["temporal_address"] = env.client.service_client.config.target_host
        config_path.write_text(json.dumps(raw))
        fixture = native_gates.__wrapped__(api_fixture, tmp_path)
        g = next(fixture)
        ready, release = tmp_path / "escaped-ready.json", tmp_path / "escaped-release"
        actor = None
        try:
            # The owned native body exits after a fast double fork. Its child
            # closes inherited witnesses and stdio, reparents, and keeps no
            # checkout cwd or open checkout file for lsof to observe.
            code = """import json,os,sys,time
from pathlib import Path
from devflow_temporal.delivery_native_process import process_table
if os.fork(): os._exit(0)
os.setsid()
if os.fork(): os._exit(0)
os.closerange(0,1024)
os.chdir('/')
pid=os.getpid()
Path(sys.argv[1]).write_text(json.dumps({'pid':pid,'identity':process_table()[pid]['identity']}))
while not Path(sys.argv[2]).exists(): time.sleep(.02)
"""
            folder = Path(g.spec["state_dir"]) / "owned-opaque-check"
            process = NativeProcess(
                g.spec, folder, argv=[sys.executable, "-I", "-c", code, str(ready), str(release)],
                cwd=g.broker.checkout, timeout=20,
                environment={"PATH": os.environ["PATH"],
                             "HOME": str(tmp_path / "owned-provider-home")})
            observed = await to_thread(g.spec, process.run)
            deadline = time.monotonic() + 10
            while not ready.exists():
                assert time.monotonic() < deadline
                await asyncio.sleep(.02)
            actor = json.loads(ready.read_text())
            journal = read_private(folder / "native-process.json")
            assert observed["cleanup"] == "observed-native-confirmed"
            assert journal["monitoring_complete"] is True
            assert str(actor["pid"]) not in journal["owned"]
            assert process_table()[actor["pid"]]["identity"] == actor["identity"]

            # The exact original SDK execution closes; no mocked controller
            # response or caller-provided custody assertion is used.
            submitted = g.store.submitted_spec(g.spec["run_id"])
            async with Worker(
                env.client, task_queue=g.store.config.queue,
                workflow_runner=UnsandboxedWorkflowRunner(), workflows=[NativeCheckpointOwner],
            ):
                await _dispatch_start(SimpleNamespace(store=g.store, config=g.store.config),
                                      env.client, g.store.pending_starts()[0], submitted)
                assert (await env.client.get_workflow_handle(
                    "delivery-" + g.spec["run_id"]).result())["outcome"] == "blocked"
            with g.store._connect() as db:
                db.execute("UPDATE delivery_runs SET phase='blocked',execution_state='blocked',"
                           "outcome='blocked' WHERE run_id=?", (g.spec["run_id"],))
                revision = db.execute("SELECT revision FROM delivery_runs WHERE run_id=?",
                                      (g.spec["run_id"],)).fetchone()[0]
            url = g.store.config.dashboard_url.rstrip("/")
            app = create_app(g.store.config.path)
            path = "/api/runs/" + g.spec["run_id"] + "/checkpoints"
            command = {"command_id": "escaped-native", "expected_revision": revision}
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url=url,
            ) as client:
                session = (await client.get("/api/session")).json()
                headers = {"origin": url, "x-devflow-csrf": session["csrf_token"]}
                response = await client.post(path, json=command, headers=headers)
                assert response.status_code == 409, response.text
                assert response.json()["detail"]["code"] == "native_checkpoint_custody_unknown"
                assert process_table()[actor["pid"]]["identity"] == actor["identity"]

                # Even later disappearance cannot manufacture complete closure.
                release.touch()
                deadline = time.monotonic() + 10
                while True:
                    current = process_table().get(actor["pid"], {})
                    if (current.get("identity") != actor["identity"]
                            or current.get("stat", "Z").startswith("Z")):
                        break
                    assert time.monotonic() < deadline
                    await asyncio.sleep(.02)
                retry = await client.post(path, json=command, headers=headers)
                assert retry.status_code == 409
                assert retry.json()["detail"]["code"] == "native_checkpoint_custody_unknown"
            with g.store._connect() as db:
                assert db.execute("SELECT 1 FROM delivery_commands WHERE command_id=?",
                                  ("checkpoint-create:escaped-native",)).fetchone() is None
            assert not list((Path(g.spec["state_dir"]) / "checkpoints").glob("*/manifest.json"))
        finally:
            release.touch()
            if actor:
                current = process_table().get(actor["pid"], {})
                if (current.get("identity") == actor["identity"]
                        and not current.get("stat", "Z").startswith("Z")):
                    try:
                        os.kill(actor["pid"], signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                deadline = time.monotonic() + 5
                while True:
                    current = process_table().get(actor["pid"], {})
                    if (current.get("identity") != actor["identity"]
                            or current.get("stat", "Z").startswith("Z")):
                        break
                    assert time.monotonic() < deadline
                    await asyncio.sleep(.02)
            next(fixture, None)
