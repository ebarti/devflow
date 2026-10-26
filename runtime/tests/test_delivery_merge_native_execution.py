"""Real native preparation/supervisor/public merge workflow and SDK replay.

Historical delivery/closed proof and location-ACK are controlled boundaries.
Native measurement executes the installed locked CLI --version and framework
probe. Both merge roles use real current -I monitors and an authored neutral
provider adapter instead of a model. GitHub, current-target CI and ruleset
facts use the existing controlled transport. No real hosted acceptance claim.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import sys
from importlib.metadata import distribution
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from test_delivery_merge_followup import delivered, git_remote
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_merge_followup as followup
from devflow_temporal import delivery_merge_resolution as resolution
from devflow_temporal import delivery_native_process as process_module
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_resources import RunResources, read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore


@pytest.mark.asyncio
async def test_public_merge_uses_real_prepared_roles_then_original_workflow_and_replay(
    service,
    monkeypatch,
    tmp_path,
):
    from temporal_test_server import local_temporal
    from temporalio.worker import Replayer, Worker

    from devflow_temporal import delivery_runtime_location as location
    from devflow_temporal.delivery_activities import (
        delivery_merge_followup,
        delivery_merge_followup_failed,
        delivery_resolve_merge_conflicts,
    )
    from devflow_temporal.delivery_api import create_app
    from devflow_temporal.delivery_feature_review_context import current_runtime_identity
    from devflow_temporal.delivery_merge_workflow import MergeWorkflow

    binary = Path(distribution("openai-codex-cli-bin").locate_file("codex_cli_bin/bin/codex"))
    auth = tmp_path / "owned-synthetic-auth.json"
    auth.write_text('{"tokens":{"access_token":"owned-synthetic-never-used"}}')
    auth.chmod(0o600)

    def native_spec(spec):
        from devflow_temporal.delivery_native_guard import NATIVE_OVERRIDES
        from devflow_temporal.runtime_dependencies import locked_dependency_identity

        spec["policy"].update(
            codex_bin="/owned/removed-runtime/bin/codex",
            codex_bin_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
            codex_auth_path=str(auth),
            host_sandbox="trusted-local",
            runtime_dependencies=locked_dependency_identity(),
            config_overrides=NATIVE_OVERRIDES,
        )

    async with local_temporal() as environment:
        old, submitted = service
        raw = copy.deepcopy(old.config.raw)
        raw["temporal_address"] = environment.client.service_client.config.target_host
        old.config.path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(old.config.path))
        d = delivered.__wrapped__((store, submitted), monkeypatch, native_spec=native_spec)
        remote = git_remote(d, monkeypatch, conflict=True)
        remote.ci_ready = True
        repo = Path(d.spec["source_path"])
        bare = tmp_path / "owned-remote.git"
        _git(tmp_path, "clone", "--bare", str(repo), str(bare))
        _git(bare, "update-ref", "refs/heads/" + d.spec["branch"], remote.head)
        original_git = resolution._git

        def transport(workspace, *args):
            if args[:3] == ("remote", "set-url", "origin"):
                args = (*args[:3], str(bare))
            result = original_git(workspace, *args)
            if args and args[0] == "push":
                remote.head = _git(bare, "rev-parse", "refs/heads/" + d.spec["branch"])
                _git(repo, "fetch", str(bare), remote.head)
            return result

        monkeypatch.setattr(resolution, "_git", transport)

        def location_ack(original, selected):
            assert original == d.spec
            assert selected == binary.resolve()

        monkeypatch.setattr(location, "require_preparation_location", location_ack)
        monkeypatch.setattr(
            "devflow_temporal.delivery_activities._context", lambda _spec: (d.store, None)
        )
        monkeypatch.setattr(
            DeliveryStore,
            "_completed_temporal_result",
            lambda *_args, **_kwargs: copy.deepcopy(d.closed),
        )
        registry = RunResources(d.spec)
        with registry.locked() as manifest:
            write_private(registry.manifest, manifest)
        original_files = {
            p: p.read_bytes()
            for p in (
                registry.manifest,
                d.root / "request.json",
                d.root / "result.json",
                d.root / "native-process.json",
            )
        }
        with d.store._connect() as db:
            original_row = dict(db.execute("SELECT * FROM delivery_runs").fetchone())

        adapter = tmp_path / "owned-provider-adapter.py"
        adapter.write_text("""import json,os,sys
from pathlib import Path
r=json.loads(Path(sys.argv[1]).read_text())
w=Path(r['workspace'])
if r['role']=='implement':
    (w/'conflict.txt').write_text('Reviewed source\\nNew main work\\n')
else:
    assert (w/'conflict.txt').read_text()=='Reviewed source\\nNew main work\\n'
session='owned-native-'+r['role']
sessions=Path(os.environ['CODEX_HOME'])/'sessions'
sessions.mkdir(mode=0o700,parents=True,exist_ok=True)
(sessions/(session+'.jsonl')).write_text('Owned neutral provider boundary\\n')
Path(r['result_path']).write_text(json.dumps({'status':'pass','summary':'Owned neutral merge',
    'findings':[],'merge_decision':None,'session_id':session,'finish_reason':'done'}))
""")
        adapter.chmod(0o600)
        real_process = process_module.NativeProcess
        role_journals = []

        def native_process(spec, folder, **kwargs):
            argv = kwargs.get("argv", [])
            if "devflow_temporal.role_runner" in argv:
                kwargs["argv"] = [sys.executable, "-I", str(adapter), argv[-1]]
                role_journals.append(Path(folder) / "native-process.json")
            return real_process(spec, folder, **kwargs)

        monkeypatch.setattr(process_module, "NativeProcess", native_process)
        host = d.store.config.dashboard_url
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(d.store.config.path)), base_url=host
        ) as client:
            token = (await client.get("/api/session")).json()["csrf_token"]
            response = await client.get("/api/runs/run-1/merge-preflight")
            assert response.status_code == 200, response.text
            response = await client.post(
                "/api/runs/run-1/merge",
                json=d.command,
                headers={"Origin": host, "X-Devflow-CSRF": token},
            )
            assert response.status_code == 200, response.text
            admitted = response.json()
        async with Worker(
            environment.client,
            task_queue=d.store.config.queue,
            workflows=[MergeWorkflow],
            activities=[
                delivery_merge_followup,
                delivery_merge_followup_failed,
                delivery_resolve_merge_conflicts,
            ],
        ):
            await followup.dispatch(
                SimpleNamespace(store=d.store, config=d.store.config), environment.client
            )
            handle = environment.client.get_workflow_handle(admitted["workflow_id"])
            result = await asyncio.wait_for(handle.result(), timeout=120)
            assert result["state"] == "confirmed", result
            await Replayer(workflows=[MergeWorkflow]).replay_workflow(await handle.fetch_history())
        assert remote.merge_calls == 1 and remote.issue_state == "closed"
        assert len(role_journals) == 2
        program = current_runtime_identity()
        monitors = []
        for path in role_journals:
            journal = read_private(path)
            assert journal["phase"] == "finished" and journal["monitoring_complete"] is True
            assert journal["runtime_identity"]["revision"] == program["revision"]
            assert (
                journal["runtime_identity"]["runtime_payload_sha256"]
                == program["runtime_payload_sha256"]
            )
            assert journal["runtime_identity"]["dirty"] is False
            assert journal["result"]["cleanup"] == "observed-native-confirmed"
            monitors.append(
                {
                    "path": str(path),
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    "runtime_identity": journal["runtime_identity"],
                }
            )
        assert all(path.read_bytes() == content for path, content in original_files.items())
        with d.store._connect() as db:
            assert dict(db.execute("SELECT * FROM delivery_runs").fetchone()) == original_row
            assert followup.current(db, "run-1")["phase"] == "merged"
        if output := os.environ.get("DEVFLOW_TEST_MERGE_NATIVE_RESULT"):
            with open(output, "x") as stream:
                json.dump(
                    {
                        "program": program,
                        "qualification": __doc__,
                        "admission": admitted,
                        "result": result,
                        "monitors": monitors,
                        "adapter_sha256": hashlib.sha256(adapter.read_bytes()).hexdigest(),
                        "grant": admitted["grant_sha256"],
                    },
                    stream,
                    indent=2,
                )
