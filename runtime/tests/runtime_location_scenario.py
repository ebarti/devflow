"""Owned subprocess harness for the public permanent-installation contract."""

from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import importlib.util
import json
import os
import shutil
import socket
import subprocess
import sys
from contextlib import closing
from datetime import UTC, datetime, timedelta
from importlib.metadata import distribution
from pathlib import Path
from types import SimpleNamespace

import pytest

import devflow_temporal

SOURCE = Path(devflow_temporal.__file__).resolve().parents[3]
PLUGIN_SOURCE = "27aa49e188d9a8b788169f69a066dc913061fb94"
PLUGIN_BASELINE = "824f00f9cb664e6e61285589bf3c258e651d6653"
sys.path.insert(0, str(SOURCE / "runtime/tests"))

from devflow_temporal.contracts import canonical_json, digest  # noqa: E402
from devflow_temporal.delivery_broker import DeliveryBroker  # noqa: E402
from devflow_temporal.delivery_config import DeliveryConfig  # noqa: E402
from devflow_temporal.delivery_resources import (  # noqa: E402
    RunResources,
    read_private,
    write_private,
)
from devflow_temporal.delivery_store import DeliveryStore  # noqa: E402


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def ports():
    with socket.socket() as one, socket.socket() as two:
        one.bind(("127.0.0.1", 0))
        two.bind(("127.0.0.1", 0))
        return one.getsockname()[1], two.getsockname()[1]


def fixture_service(root):
    from test_delivery_store import service

    store, request = service.__wrapped__(root)
    home = Path.home()
    compat = root / "compat/devflow"
    compat.mkdir(parents=True)
    (compat / "scripts").symlink_to(SOURCE / "skills/devflow/scripts")
    retained = root / "durable-references"
    retained.mkdir()
    (compat / "references").symlink_to(retained)
    logical = home / ".agents/skills/devflow"
    logical.parent.mkdir(parents=True, exist_ok=True)
    logical.unlink(missing_ok=True)
    logical.symlink_to(compat)
    raw = copy.deepcopy(store.config.raw)
    raw.update(
        helpers_dir=str(logical / "scripts"),
        capacity=6,
        temporal_address=os.environ["DEVFLOW_TEST_LOCATION_TEMPORAL_ADDRESS"],
        execution_registry=str(home / ".local/state/devflow/execution-ownership/registry.sqlite3"),
    )
    # A real service may retain different repository/browser authorities under
    # one physical installation. Freeze both declarations BEFORE any admission.
    browser_cache = home / ".cache/ms-playwright"
    browser_cache.mkdir(mode=0o700, parents=True, exist_ok=True)
    assert browser_cache.resolve(strict=True) == browser_cache and browser_cache.is_dir()
    web_port, api_port = ports()
    browser_repository = copy.deepcopy(raw["repositories"]["fixture"])
    browser_repository.update(
        expected_base_sha=None,
        allowed_paths=[],
        checks=[{"id": "local", "argv": ["/usr/bin/true"]}],
        prepublish_checks=[{"id": "prepublish", "argv": ["/usr/bin/true"]}],
        required_ci=["ci"],
        project_url="https://github.com/users/example/projects/1",
        assignee="example",
        source_scope={
            "version": 1,
            "allowed_roots": ["apps/api/src"],
            "allowed_files": [],
            "protected_paths": [".git", ".github", ".agents", ".codex"],
        },
        browser_qa={
            "id": "owned-browser-scope",
            "argv": [str(Path(sys.executable).resolve()), "-c", "print('1 passed')"],
            "cwd": ".",
            "ports": {"WEB_PORT": web_port, "API_PORT": api_port},
            "env": {"JOBCTRL_E2E_ISOLATED": "1"},
            "read_roots": [str(browser_cache)],
            "artifact_paths": [],
            "test_count_regex": r"(\d+) passed",
            "min_tests": 1,
            "timeout_seconds": 30,
        },
    )
    raw["repositories"]["fixture-browser"] = browser_repository
    store.config.path.write_text(json.dumps(raw))
    store.config.path.chmod(0o600)
    return DeliveryStore(DeliveryConfig.load(store.config.path)), request


async def seal_closed(store, spec, state, *, stale=None):
    from temporalio.api.common.v1 import Memo, Payloads, WorkflowType
    from temporalio.api.history.v1 import (
        HistoryEvent,
        WorkflowExecutionCompletedEventAttributes,
        WorkflowExecutionStartedEventAttributes,
    )
    from temporalio.client import WorkflowHistory

    from devflow_temporal.delivery_closures import closure_facts, seal
    from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER

    events = [
        HistoryEvent(
            event_id=1,
            workflow_execution_started_event_attributes=WorkflowExecutionStartedEventAttributes(
                workflow_type=WorkflowType(name="DevflowDeliveryWorkflow"),
                workflow_id="delivery-" + spec["run_id"],
                original_execution_run_id="owned-closed-" + spec["run_id"],
                input=Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode([spec, None])),
                memo=Memo(
                    fields={
                        "request_digest": (
                            await DELIVERY_DATA_CONVERTER.encode([spec["request_digest"]])
                        )[0]
                    }
                ),
            ),
        )
    ]
    if stale:
        from temporalio.api.common.v1 import ActivityType
        from temporalio.api.history.v1 import (
            ActivityTaskFailedEventAttributes,
            ActivityTaskScheduledEventAttributes,
        )

        projection, state = stale
        events.extend(HistoryEvent(event_id=index) for index in range(2, 10))
        events.append(
            HistoryEvent(
                event_id=10,
                activity_task_scheduled_event_attributes=ActivityTaskScheduledEventAttributes(
                    activity_type=ActivityType(name="delivery_project"),
                    input=Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode([projection])),
                ),
            )
        )
        events.append(HistoryEvent(event_id=11))
        events.append(
            HistoryEvent(
                event_id=12,
                activity_task_failed_event_attributes=ActivityTaskFailedEventAttributes(
                    scheduled_event_id=10
                ),
            )
        )
    end = HistoryEvent(
        event_id=len(events) + 1,
        workflow_execution_completed_event_attributes=WorkflowExecutionCompletedEventAttributes(
            result=Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode([state]))
        ),
    )
    end.event_time.FromDatetime(datetime.now(UTC) + timedelta(seconds=1))
    events.append(end)
    history = WorkflowHistory("delivery-" + spec["run_id"], events)
    facts = await closure_facts(history)
    seal(store, history, facts, run_id=spec["run_id"], source="temporal")


HOST_CLI = """#!/usr/bin/env python3
import json,os,pathlib,shutil,sys
home=pathlib.Path(os.environ['CODEX_HOME']);path=home/'public-inventory.json'
value=json.loads(path.read_text());args=sys.argv[1:]
plugin={'pluginId':'devflow@devflow-local','enabled':True,'installed':True,'name':'devflow',
 'version':'0.1.0','marketplaceName':'devflow-local','installPolicy':'AVAILABLE',
 'authPolicy':'ON_INSTALL','source':{'source':'local',
 'path':str(pathlib.Path(value['marketplace'] or '/absent')/'plugins/devflow')},
 'marketplaceSource':{'sourceType':'local','source':value['marketplace']}}
cache=home/'plugins/cache/devflow-local/devflow/0.1.0'
server=None
if value['installed']:
 raw=json.loads((cache/'.mcp.json').read_text())['mcpServers']['devflow-local-delivery']
 server={'name':'devflow-local-delivery','enabled':True,'transport':raw}
if args==['mcp','list','--json']: print(json.dumps([server] if server else []))
elif args==['mcp','get','devflow-local-delivery','--json']: print(json.dumps(server))
elif args==['plugin','list','--json']:
 print(json.dumps({'installed':[plugin] if value['installed'] else []}))
elif args==['plugin','marketplace','list']:
 print('MARKETPLACE ROOT\\ndevflow-local '+value['marketplace'] if value['marketplace'] else
       'No plugin marketplaces in scope.')
else:
 value['calls'].append(args)
 if value.get('hold'):
  path.write_text(json.dumps(value));sys.exit(0)
 if args[:2]==['plugin','remove']: value['installed']=False
 elif args[:3]==['plugin','marketplace','remove']: value['marketplace']=None
 elif args[:3]==['plugin','marketplace','add']: value['marketplace']=args[3]
 elif args[:2]==['plugin','add']:
  cache=home/'plugins/cache/devflow-local/devflow/0.1.0'
  if cache.exists():shutil.rmtree(cache)
  shutil.copytree(pathlib.Path(value['marketplace'])/'plugins/devflow',cache)
  value['installed']=True
 else: raise RuntimeError(args)
 path.write_text(json.dumps(value));path.chmod(0o600)
 print('{}' if args[-1]=='--json' else 'ok')
"""


def registrations(root, config):
    home = Path.home()
    source = SOURCE / "runtime/desktop"
    sys.path.insert(0, str(source))
    import package_plugin

    old_market = root / "original-marketplace"
    files = package_plugin.package_files(SOURCE / "runtime", config.path, "codex")
    # The program advanced after this reviewed package was installed. Keep its
    # genuine historical Git skill bytes rather than synthesizing arbitrary skew.
    files["skills/devflow-local-delivery/SKILL.md"] = subprocess.check_output(
        [
            "git",
            "-C",
            str(SOURCE),
            "show",
            PLUGIN_SOURCE + ":runtime/desktop/devflow-local-delivery/SKILL.md",
        ]
    )
    for name, raw in files.items():
        path = old_market / "plugins/devflow" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        path.chmod(0o600)
    catalog = old_market / ".agents/plugins/marketplace.json"
    catalog.parent.mkdir(parents=True)
    catalog.write_bytes(
        package_plugin.json_bytes(
            {
                "name": "devflow-local",
                "interface": {"displayName": "Devflow Local"},
                "plugins": [package_plugin.ENTRY],
            }
        )
    )
    write_private(
        root / "plugin-origin.json",
        {
            "schema": "devflow.original-plugin-package/v1",
            "source_revision": PLUGIN_SOURCE,
            "marketplace": str(old_market),
            "catalog_sha256": hashlib.sha256(catalog.read_bytes()).hexdigest(),
            "files_sha256": {name: hashlib.sha256(raw).hexdigest() for name, raw in files.items()},
        },
    )
    codex = home / ".codex"
    codex.mkdir(parents=True, exist_ok=True)
    (codex / "config.toml").write_text("")
    cache = codex / "plugins/cache/devflow-local/devflow/0.1.0"
    if cache.exists():
        shutil.rmtree(cache)
    shutil.copytree(old_market / "plugins/devflow", cache)
    write_private(
        codex / "public-inventory.json",
        {"installed": True, "marketplace": str(old_market), "calls": []},
    )
    cli = root / "public-host-cli"
    cli.write_text(HOST_CLI)
    cli.chmod(0o755)
    from shlex import quote

    activated = {}
    for name in ("devflow", "devflow-delivery", "devflow-delivery-mcp"):
        path = home / ".local/share/devflow/delivery-launchers/owned/entries" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        entry = "devflow-delivery" if name == "devflow" else name
        path.write_text(
            "#!/bin/sh\n# Canonical delivery entry; devflow aliases devflow-delivery.\nexec "
            + quote(str(SOURCE / "runtime/.venv/bin" / entry))
            + " --config "
            + quote(str(config.path))
            + ' "$@"\n'
        )
        path.chmod(0o755)
        link = home / ".local/bin" / name
        link.parent.mkdir(parents=True, exist_ok=True)
        link.unlink(missing_ok=True)
        link.symlink_to(path)
        activated[name] = {
            "path": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    marker = path.parent.parent / "activation-manifest.json"
    write_private(
        marker,
        {
            "revision": git(SOURCE, "rev-parse", "HEAD"),
            "tree": git(SOURCE, "rev-parse", "HEAD^{tree}"),
            "config": {
                "owner": 5,
                "original": str(root / "retained-original-policy.json"),
                "config_path": str(config.path),
                "sha256": "5" * 64,
            },
            "entries": activated,
        },
    )
    path.parent.parent.parent.chmod(0o700)
    return cli


def auxiliary(store, index):
    """Second real admission proves a multi-run ACK rather than a one-row stub."""
    from test_delivery_blocked_scope import prepared_spec

    from devflow_temporal.delivery_native_preparation import bind_native_spec, native_identity

    run_id = "run-" + str(index)
    request = {
        "command_id": "retained-command-" + str(index),
        "run_id": run_id,
        "work_id": "work-" + str(index),
        "issue_url": "https://github.com/example/fixture/issues/" + str(index + 2),
        "repository_key": "fixture-browser" if index != 3 else "fixture",
        "goal": "Owned second stopped delivery",
        "accepted_plan": "Retain the original source.",
        "base_ref": "HEAD",
        "branch": "feat/retained-" + str(index),
        "authorized_endpoint": "published_unmerged",
    }
    store.submit(request)
    store.mark_start(run_id, accepted=True)
    spec = store.effective_spec(run_id)
    DeliveryBroker(store, spec).prepare()
    identity = native_identity(spec)
    proof = (
        Path(spec["state_dir"]).parents[1] / "preparation-native" / digest(identity) / "proof.json"
    )
    spec = (
        bind_native_spec(spec, identity, proof, reused=True)
        if proof.exists()
        else prepared_spec(spec)
    )
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET request_json=? WHERE run_id=?", (canonical_json(spec), run_id)
        )
    receipt = RunResources(spec).finalize("blocked")
    state = {
        "run_id": run_id,
        "phase": "blocked",
        "execution_state": "blocked",
        "outcome": "blocked",
        "revision": 1,
        "iteration": 0,
        "candidate": None,
        "pull_request": None,
        "roles": [],
        "cleanup": "confirmed",
        "tracker": None,
        "checks": {"resource_cleanup": receipt},
        "error": "Owned stopped fixture",
        "usage": {},
    }
    store.project(
        run_id,
        phase="blocked",
        execution_state="blocked",
        outcome="blocked",
        event_type="blocked",
        message=state["error"],
        protocol_revision=1,
        cleanup="confirmed",
        checks=state["checks"],
        error=state["error"],
    )
    asyncio.run(seal_closed(store, spec, state))


def seed(root, kind):
    from importlib.util import module_from_spec, spec_from_file_location

    from test_delivery_amendment_capacity import cut_operational_capacity

    # The retained runtime owns the production and scenario helpers. Its old
    # test fixture assumed a private sibling and a fixed historical receipt;
    # load only the current portable fixture adapter for synthetic setup.
    fixture_directory = Path(__file__).resolve().parent
    sys.path.append(str(fixture_directory))
    module_spec = spec_from_file_location(
        "portable_temporal_case_adapter",
        fixture_directory / "test_delivery_temporal_prerequisites.py",
    )
    fixture_module = module_from_spec(module_spec)
    module_spec.loader.exec_module(fixture_module)
    temporal_case = fixture_module.temporal_case

    root.mkdir(parents=True)
    patch = pytest.MonkeyPatch()
    # The synthetic browser command uses the retained stable base interpreter;
    # it must not accidentally freeze the disposable fixture's venv shebang.
    saved = sys.executable
    sys.executable = str(Path(saved).resolve())
    try:
        case = temporal_case(
            fixture_service(root),
            patch,
            minimum=3,
            amendment="browser" if kind == "browser" else "scope",
            ports=ports(),
        )
    finally:
        sys.executable = saved
    for index in range(2, 7):
        auxiliary(case.store, index)
    cut_operational_capacity(case)
    if kind.startswith("stale"):
        from test_delivery_blocked_scope import rewrite_assessment, rewrite_observation

        raw = copy.deepcopy(case.raw)
        raw.update(status="pass", findings=[], summary="Owned finished implementation")
        rewrite_assessment(case, raw)
        observation = read_private(case.folder / "native-thread-observation.json")
        for name in (observation["turns"][-1]["items"], observation["turns"][-1]["turn"]["items"]):
            name[0]["text"] = json.dumps(
                {key: raw[key] for key in ("status", "summary", "findings")}
            )
        rewrite_observation(case, observation)
        case.state["candidate"] = case.partial
        if kind == "stale1169":
            git(case.broker.checkout, "add", case.api)
            git(case.broker.checkout, "commit", "-qm", "Owned finished published implementation")
            git(case.broker.checkout, "push", "origin", "HEAD:refs/heads/" + case.spec["branch"])
            current = case.broker.candidate()
            case.partial = current
            case.state["candidate"] = current
            case.state["roles"][-1]["candidate"] = current
            case.journal["provider_session"]["output_candidate"] = {
                key: current[key] for key in ("id", "head", "content_sha256")
            }
            write_private(case.folder / "native-process.json", case.journal)
            case.publication.update(head=current["head"], candidate=current)
            case.state["pull_request"] = case.publication
        case.state["checks"]["local"] = {
            "state": "failed",
            "cleanup": "confirmed",
            "candidate_id": case.state["candidate"]["id"],
            "source_unchanged": True,
            "results": [
                {
                    "id": case.check["id"],
                    "passed": False,
                    "timed_out": True,
                    "exit_code": None,
                    "cleanup": "confirmed",
                    "junit": {"exists": False},
                }
            ],
        }
    # Real native journals retain the stopped monitor in the final owned set.
    # The older role-only fixture did not need the finalizer's exact PID
    # inventory; this owner scenario deliberately exercises that real reader.
    registry = RunResources(case.spec)
    for raw in read_private(registry.manifest)["processes"]:
        journal = read_private(Path(raw))
        monitor = journal.get("monitor")
        if monitor:
            journal["owned"][str(monitor["pid"])] = monitor
            write_private(Path(raw), journal)
    case.journal = read_private(case.folder / "native-process.json")
    finalization = registry.finalize("blocked")
    assert finalization["state"] == "confirmed"
    case.state["checks"]["resource_cleanup"] = finalization
    case.store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        outcome="blocked",
        event_type="blocked",
        message=case.state["error"],
        candidate=case.state["candidate"],
        pull_request=case.publication,
        checks=case.state["checks"],
        iteration=3,
        protocol_revision=40,
        cleanup="confirmed",
        error=case.state["error"],
        tracker=case.state["tracker"],
    )
    case.store.config.path.chmod(0o600)
    stale = None
    if kind.startswith("stale"):
        from temporalio import workflow
        from test_delivery_controller_failure_boundary import _projection_error

        from devflow_temporal.delivery_workflow import DeliveryWorkflow

        case.command = {
            key: value
            for key, value in case.command.items()
            if key not in {"added_paths", "amended_config_path", "amended_config_sha256"}
        }
        controller = DeliveryWorkflow()
        controller.controller_failure_boundary = True
        controller.state = copy.deepcopy(case.state)
        controller.state.update(
            revision=47 if kind == "stale1169" else 38,
            cleanup="unknown",
            phase="blocked",
            execution_state="blocked",
            outcome="blocked",
            error="terminal tracker synchronization conflicted",
        )
        controller.state["checks"]["resource_cleanup"] = {
            "state": "unknown",
            "process_cleanup": "unknown",
            "resource_cleanup": "unknown",
            "reason": "ValueError",
        }
        requests = []

        async def failed(name, request, **_kwargs):
            if name == "delivery_finalize_resources":
                raise ValueError("Owned unavailable aggregate finalization")
            if name == "delivery_terminal_tracker":
                return {"state": "conflicted", "retryable": False}
            assert name == "delivery_project"
            requests.append(copy.deepcopy(request))
            raise _projection_error()

        patch.setattr(workflow, "patched", lambda *_: True)
        patch.setattr(workflow, "now", lambda: datetime.now(UTC))
        controller._activity = failed
        asyncio.run(controller._project(case.spec, "blocked", controller.state["error"]))
        case.state = controller.state
        case.command["expected_revision"] = case.state["revision"]
        case.command.update(
            expected_candidate_id=case.partial["id"], expected_candidate_head=case.partial["head"]
        )
        with case.store._connect() as db:
            case.store.state.claim_work(
                db, case.spec["work_id"], "external:devflow:run-1", "Owned retained terminal claim"
            )
            db.execute(
                "UPDATE delivery_runs SET phase=?,execution_state='running',outcome=NULL,"
                "protocol_revision=?,cleanup='unknown',error=NULL WHERE run_id='run-1'",
                ("checks" if kind == "stale1169" else "repair", case.state["revision"] - 1),
            )
        stale = requests[-1], case.state
    asyncio.run(seal_closed(case.store, case.spec, case.state, stale=stale))
    cli = registrations(root, case.store.config)
    write_private(case.store.config.state_root / "service-stopped.json", {"command": "stop"})
    data = json.loads(
        json.dumps(
            {key: value for key, value in vars(case).items() if key not in {"store", "broker"}},
            default=str,
        )
    )
    data.update(
        owner=str(case.store.config.path),
        old_root=str(SOURCE),
        cli=str(cli),
        kind=kind,
        plugin_origin=read_private(root / "plugin-origin.json"),
    )
    write_private(root / "case.json", data)
    for source in (SOURCE / "skills/devflow/scripts").iterdir():
        if source.suffix not in {".py", ".sql"}:
            continue
        target = root / "loaded-old-projector" / source.name
        target.parent.mkdir(exist_ok=True)
        target.write_bytes(source.read_bytes())
    patch.undo()


def control(config, operation, request=None, *, success=True):
    argv = [
        sys.executable,
        "-I",
        "-m",
        "devflow_temporal.delivery_control",
        "--config",
        str(config.path),
        operation,
    ]
    if request:
        argv += ["--request", str(request)]
    result = subprocess.run(argv, capture_output=True, text=True, timeout=180)
    if success:
        assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-6000:]
        return json.loads(result.stdout)
    assert result.returncode != 0, "An unacknowledged/changed installation was accepted"
    return result.stderr


def restored(root):
    value = read_private(root / "case.json")
    for key in (
        "folder",
        "home",
        "owner_path",
        "private_path",
        "old_browser",
        "old_report",
        "provenance_path",
    ):
        if key in value:
            value[key] = Path(value[key])
    return SimpleNamespace(**value)


def original_rows(config):
    import sqlite3

    with closing(sqlite3.connect(config.tracking_db)) as db, db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute("SELECT * FROM delivery_runs ORDER BY run_id")]


def scoped_external_boundaries(case, patch):
    patch.setattr(
        DeliveryBroker,
        "_existing_pr",
        lambda *_: {
            "number": 12,
            "headRefOid": case.publication["head"],
            "url": case.publication["url"],
        },
    )
    from devflow_temporal import delivery_automatic_retry

    def tracker(spec, status, *, release=False, **_kwargs):
        with case.store._connect() as db:
            if release:
                case.store.state.release_work(
                    db, spec["work_id"], "external:devflow:" + spec["run_id"]
                )
        return {
            "state": "recorded" if spec.get("project_sync_version") == 1 else "consistent",
            "pending": False,
            "scope": "local",
            "project_sync": "asynchronous",
            "status": status,
            "claim_released": release,
        }

    patch.setattr(delivery_automatic_retry, "_tracker_sync", tracker)


def assert_cohort_native_authority(config):
    from devflow_temporal import delivery_runtime_installation as installation
    from devflow_temporal import delivery_runtime_location as location

    receipt = location._acknowledged(config)
    plugin = receipt["registrations"]["plugin"]
    assert plugin["origin"]["request"]["source_revision"] == PLUGIN_SOURCE
    assert {name: raw for name, raw in plugin["before_files"].items() if name != ".mcp.json"} == {
        name: raw for name, raw in plugin["after_files"].items() if name != ".mcp.json"
    }
    old_skill = base64.b64decode(plugin["before_files"]["skills/devflow-local-delivery/SKILL.md"])
    assert old_skill != (SOURCE / "runtime/desktop/devflow-local-delivery/SKILL.md").read_bytes()
    assert receipt["destination"]["identity_kind"] == installation.PHYSICAL_DESTINATION
    assert not installation.RUN_NATIVE_SCOPE & receipt["destination"]["identity"].keys()
    scopes = []
    binary = receipt["destination"]["identity"]["codex_bin"]
    for item in receipt["runs"]:
        full = installation.scoped_identity(receipt["destination"], item)
        frozen = item["original_spec"]["policy"]["native_identity"]
        assert {key: full[key] for key in installation.RUN_NATIVE_SCOPE} == {
            key: frozen[key] for key in installation.RUN_NATIVE_SCOPE
        }
        # The actual normal preparation consumer must honor BOTH scope shapes.
        location.require_preparation_location(item["original_spec"], binary)
        scopes.append(full["browser_read_roots"])
    assert len(scopes) == 6 and len({digest(value) for value in scopes}) >= 2
    return scopes


def assert_original_plugin_bytes(case):
    from devflow_temporal import delivery_runtime_registrations as registration

    witness = case.plugin_origin
    for folder in (
        Path(witness["marketplace"]) / "plugins/devflow",
        Path.home() / ".codex/plugins/cache/devflow-local/devflow/0.1.0",
    ):
        files = registration._package_tree(folder, registration.PLUGIN_FILES)["files"]
        assert {
            name: hashlib.sha256(base64.b64decode(raw)).hexdigest()
            for name, raw in files.items()
        } == witness["files_sha256"]


def crash_apply(root, kind):
    """Kill an owned public invocation at exact durable/effect boundaries."""
    import argparse

    from devflow_temporal import delivery_control
    from devflow_temporal import delivery_runtime_installation as installation
    from devflow_temporal import delivery_runtime_registrations as registrations
    from devflow_temporal import delivery_runtime_service_guard as service_guard

    case = restored(root)
    if kind == "guard":
        service_guard.census = lambda *_: os._exit(73)
    elif kind == "intent":
        original = installation._pointer

        def pointer(path, value, **kwargs):
            if path.name == "pending.json":
                os._exit(74)
            return original(path, value, **kwargs)

        installation._pointer = pointer
    elif kind == "swap":
        original = registrations._exchange

        def exchange(left, right):
            original(left, right)
            if Path(right).name == "devflow":
                os._exit(71)

        registrations._exchange = exchange
    else:
        original = installation._pointer

        def pointer(path, value, **kwargs):
            if path.name == "current.json":
                os._exit(72)
            return original(path, value, **kwargs)

        installation._pointer = pointer
    delivery_control._run(
        argparse.Namespace(
            config=case.owner,
            command="adopt-runtime-location",
            request=Path(case.owner).parent
            / "service-state/operator-inputs/permanent-request.json",
        ),
        argparse.ArgumentParser(),
    )


def cohort_faults(root, config, request, rows):
    import sqlite3

    from devflow_temporal import delivery_runtime_location as location

    with closing(sqlite3.connect(config.tracking_db)) as db, db:
        db.execute(
            "CREATE TRIGGER owned_partial_intent BEFORE INSERT ON delivery_commands "
            "WHEN NEW.run_id='run-6' AND NEW.command_id LIKE 'runtime-location-intent-%' "
            "BEGIN SELECT RAISE(ABORT,'Owned cohort crash'); END"
        )
    control(config, "adopt-runtime-location", request, success=False)
    assert original_rows(config) == rows
    with closing(sqlite3.connect(config.tracking_db)) as db, db:
        assert (
            db.execute(
                "SELECT COUNT(*) FROM delivery_events WHERE type IN (?,?)",
                (location.PENDING, location.KIND),
            ).fetchone()[0]
            == 0
        )
        db.execute("DROP TRIGGER owned_partial_intent")
    # File-only staged evidence did not install a guard or fence the owner.
    assert not (config.state_root / "runtime-installations/pending.json").exists()
    assert (config.state_root / "service.lock").stat().st_nlink == 1
    with pytest.raises(ValueError, match="helper|scripts|missing|does not exist"):
        DeliveryStore(config)
    # An extra descriptor in this observer process is foreign to the exact
    # installer's FD. PENDING+guard may survive; every relocation preimage stays.
    extra = os.open(config.state_root / "service.lock", os.O_RDONLY | os.O_NOFOLLOW)
    try:
        error = control(config, "adopt-runtime-location", request, success=False)
        assert "foreign service-lock FDs" in error, error
        pending = config.state_root / "runtime-installations/pending.json"
        receipt = json.loads(location.receipt_bytes(Path(read_private(pending)["receipt"])))
        from devflow_temporal.delivery_runtime_registrations import _state

        assert all(_state(item) == "before" for item in receipt["installed_objects"])
        assert original_rows(config) == rows
        observation = read_private(
            Path(read_private(pending)["receipt"]).with_name("service-guard-observation.json")
        )
        assert observation["relocation_started"] is False
        assert observation["stopped_asserted"] is False
        assert observation["legacy_lifecycle_may_act_through_prevalidated_fd"] is True
    finally:
        os.close(extra)
    stopped = config.state_root / "service-stopped.json"
    marker = stopped.read_bytes()
    stopped.unlink()  # Model the demonstrated old prevalidated lifecycle effect.
    try:
        error = control(config, "adopt-runtime-location", request, success=False)
        assert "service-stopped.json" in error, error
        assert all(_state(item) == "before" for item in receipt["installed_objects"])
        assert original_rows(config) == rows
    finally:
        stopped.write_bytes(marker)
        stopped.chmod(0o600)
    # The exact immutable receipt and six-row pending transaction survive a
    # process death immediately after the first actual atomic file exchange.
    for mode, code in (("guard", 73), ("swap", 71), ("ack", 72)):
        result = subprocess.run(
            [sys.executable, "-I", __file__, "crash", str(root), mode],
            capture_output=True,
            text=True,
            timeout=180,
        )
        assert result.returncode == code, result.stdout[-1000:] + result.stderr[-5000:]
        assert original_rows(config) == rows
        pending = config.state_root / "runtime-installations/pending.json"
        assert pending.is_file()
        assert (config.state_root / "service.lock").stat().st_nlink == 2
        with closing(sqlite3.connect(config.tracking_db)) as db, db:
            assert (
                db.execute(
                    "SELECT COUNT(*) FROM delivery_events WHERE type=?", (location.PENDING,)
                ).fetchone()[0]
                == 6
            )
            assert db.execute(
                "SELECT COUNT(*) FROM delivery_events WHERE type=?", (location.KIND,)
            ).fetchone()[0] == (6 if mode == "ack" else 0)
        if mode == "guard":
            receipt = json.loads(location.receipt_bytes(Path(read_private(pending)["receipt"])))
            from devflow_temporal.delivery_runtime_registrations import _state

            assert all(_state(item) == "before" for item in receipt["installed_objects"])
        if mode == "swap":
            pending.unlink()  # File loss cannot erase the durable pending fence.
        with pytest.raises(ValueError, match="installation is incomplete"):
            DeliveryStore(DeliveryConfig.load(config.path))


def reject_partial_membership(config):
    import sqlite3

    from devflow_temporal import delivery_runtime_location as location

    receipt = location._acknowledged(config)
    key = location.command_id(receipt, "run-6")
    with closing(sqlite3.connect(config.tracking_db)) as db, db:
        saved = db.execute("SELECT * FROM delivery_commands WHERE command_id=?", (key,)).fetchone()
        db.execute("DELETE FROM delivery_commands WHERE command_id=?", (key,))
    try:
        assert location._acknowledged(config) is None
        with pytest.raises(ValueError, match="not completely acknowledged"):
            DeliveryStore(config)
    finally:
        with closing(sqlite3.connect(config.tracking_db)) as db, db:
            db.execute("INSERT INTO delivery_commands VALUES (?,?,?,?)", saved)


def pending_rollback(root, case, config, request, rows, before):
    """Actual public rollback before FINAL ACK, not a simulated completed state."""
    import sqlite3

    from devflow_temporal import delivery_runtime_location as location
    from devflow_temporal.delivery_native_renewal import _old_proof
    from devflow_temporal.delivery_runtime_registrations import _state

    unlinked = subprocess.run(
        [sys.executable, "-I", __file__, "crash", str(root), "intent"],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert unlinked.returncode == 74, unlinked.stdout + unlinked.stderr
    original_inode = (config.state_root / "service.lock").stat().st_ino
    assert (config.state_root / "service.lock").stat().st_nlink == 1
    control(config, "stop")  # Authentic PENDING before first link is still stoppable.
    assert (config.state_root / "service.lock").stat().st_ino == original_inode
    assert (config.state_root / "service.lock").stat().st_nlink == 1
    crash = subprocess.run(
        [sys.executable, "-I", __file__, "crash", str(root), "guard"],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert crash.returncode == 73, crash.stdout + crash.stderr
    reference = read_private(config.state_root / "runtime-installations/pending.json")
    receipt = json.loads(location.receipt_bytes(Path(reference["receipt"])))
    assert all(_state(item) == "before" for item in receipt["installed_objects"])
    with closing(sqlite3.connect(config.tracking_db)) as db, db:
        assert (
            db.execute(
                "SELECT COUNT(*) FROM delivery_events WHERE type=?", (location.PENDING,)
            ).fetchone()[0]
            == 6
        )
        assert (
            db.execute(
                "SELECT COUNT(*) FROM delivery_events WHERE type=?", (location.KIND,)
            ).fetchone()[0]
            == 0
        )
    result = control(config, "rollback-runtime-location", request)
    assert result["state"] == "rolled_back" and result["legacy_runtime_ready"] is False
    assert config.path.read_bytes() == before and original_rows(config) == rows
    assert location._acknowledged(config) is None
    assert location.historical_proof(case.spec) is False
    _old_proof(case.spec)  # Original evidence remains authentic data, never launch permission.
    calls = read_private(Path.home() / ".codex/public-inventory.json")["calls"]
    error = control(config, "adopt-runtime-location", request, success=False)
    assert "rolled back" in error, error
    assert control(config, "rollback-runtime-location", request)["state"] == "rolled_back"
    control(config, "stop")
    assert_original_plugin_bytes(case)
    assert read_private(Path.home() / ".codex/public-inventory.json")["calls"] == calls
    assert all(_state(item) == "before" for item in receipt["installed_objects"])
    assert (
        original_rows(config) == rows
        and not (config.state_root / "runtime-installations/current.json").exists()
    )
    with closing(sqlite3.connect(config.tracking_db)) as db, db:
        assert (
            db.execute(
                "SELECT COUNT(*) FROM delivery_events WHERE type=?", (location.KIND,)
            ).fetchone()[0]
            == 0
        )
        assert (
            db.execute(
                "SELECT COUNT(*) FROM delivery_events WHERE type=?", (location.REVERSED,)
            ).fetchone()[0]
            == 6
        )
    print(
        json.dumps(
            {
                **result,
                "ack_row_unchanged": True,
                "original_proof_preserved": True,
                "original_package_bytes": True,
                "legacy_guard_retained": (config.state_root / "service.lock").stat().st_nlink == 2,
                "pending_only": True,
                "location_permission": False,
            }
        )
    )


def reject_proposal_drift(config, request, supplied, proposal, binary):
    """Public bootstrap rejects semantic changes and an equal-byte unbundled CLI."""
    original = proposal.read_bytes()
    equal = proposal.with_name("unbundled-codex")
    shutil.copyfile(binary, equal)
    equal.chmod(0o700)
    assert (
        hashlib.sha256(equal.read_bytes()).hexdigest()
        == hashlib.sha256(binary.read_bytes()).hexdigest()
    )
    roles = json.loads(original)["roles"]
    roles["implement"]["model"] = "foreign-model"
    try:
        for key, value in (
            ("capacity", 6),
            ("temporal_address", "127.0.0.1:19999"),
            ("roles", roles),
            ("codex_bin", str(equal)),
        ):
            changed = json.loads(original)
            changed[key] = value
            write_private(proposal, changed)
            attempt = {
                **supplied,
                "owner_after_sha256": hashlib.sha256(proposal.read_bytes()).hexdigest(),
            }
            write_private(request, attempt)
            error = control(config, "runtime-location-preflight", request, success=False)
            assert "may change only" in error, error
    finally:
        proposal.write_bytes(original)
        proposal.chmod(0o600)
        equal.unlink()
        write_private(request, supplied)


def fresh_admission(case):
    from devflow_temporal.delivery_native_preparation import verify_native_spec
    from devflow_temporal.delivery_preparation import prepare_authority

    request = {
        "command_id": "fresh-permanent-registration",
        "run_id": "fresh-permanent-run",
        "work_id": "fresh-permanent-work",
        "issue_url": "https://github.com/example/fixture/issues/99",
        "repository_key": "fixture",
        "goal": "Owned fresh permanent runtime registration",
        "accepted_plan": "Retain the original scope and all gates.",
        "base_ref": "HEAD",
        "branch": "feat/fresh-permanent-registration",
        "authorized_endpoint": "published_unmerged",
    }
    result = case.store.submit(request)
    assert case.store.submit(request) == result
    spec = prepare_authority(case.store, case.store.effective_spec(request["run_id"]))
    verify_native_spec(spec)
    assert spec["policy"]["codex_bin"] == case.store.config.raw["codex_bin"]
    assert spec["policy"]["capacity"] == 2
    assert Path(spec["policy"]["codex_bin"]).is_relative_to(SOURCE / "runtime/.venv")
    assert not str(spec["policy"]["codex_bin"]).startswith(case.old_root)
    DeliveryBroker(case.store, spec).prepare()
    assert RunResources(spec).finalize("blocked")["state"] == "confirmed"
    return {"state": "prepared", "native_identity": "actual bundled permanent runtime"}


def reject_retained_custody_drift(case):
    """Real public preflight rereads raw proof, canonical owner, candidate and claim."""
    proof = Path(case.spec["preparation"]["environment"]["path"])
    owner = case.store.config.path
    source = Path(case.spec["checkout"]) / case.api
    for path, changed in (
        (proof, proof.read_bytes() + b" "),
        (owner, json.dumps({**case.store.config.raw, "capacity": 3}).encode()),
        (source, source.read_bytes() + b"\nOwned changed retained candidate\n"),
    ):
        original, mode = path.read_bytes(), path.stat().st_mode & 0o777
        try:
            path.write_bytes(changed)
            with pytest.raises((ValueError, OSError)):
                case.store.repair_admission_preflight("run-1", case.command)
        finally:
            path.write_bytes(original)
            path.chmod(mode)
    work = case.spec["work_id"]
    with case.store._connect() as db:
        assert case.store.state.claim_for(db, work) is None
        case.store.state.claim_work(db, work, "external:owned-foreign-fixture")
    try:
        with pytest.raises(ValueError):
            case.store.repair_admission_preflight("run-1", case.command)
    finally:
        with case.store._connect() as db:
            case.store.state.release_work(db, work, "external:owned-foreign-fixture")


def reject_installed_custody_drift(case, current):
    """Changed artifacts are observed by real strict readers; never executed."""
    from devflow_temporal.delivery_preparation import PreparationError, verify_prepared_spec
    from devflow_temporal.delivery_runtime_location import historical_proof

    registry = RunResources(current)
    for root in (SOURCE, SOURCE.parent):
        with pytest.raises(ValueError, match="cannot own the installed runtime"):
            registry.register(root, "generated")
    link = Path(current["state_dir"]) / "installed-link"
    link.symlink_to(SOURCE, target_is_directory=True)
    try:
        with pytest.raises(ValueError, match="cannot own the installed runtime"):
            registry.register(link, "generated")
    finally:
        link.unlink()
    foreign = copy.deepcopy(case.spec)
    foreign["run_id"] = "unregistered-foreign-run"
    with pytest.raises(ValueError):
        historical_proof(foreign)
    binary = Path(current["policy"]["codex_bin"])
    extra = binary.with_name("unacknowledged-hardlink")
    os.link(binary, extra)
    try:
        with pytest.raises((ValueError, OSError)):
            verify_prepared_spec(current)
    finally:
        extra.unlink()
    mode = binary.stat().st_mode & 0o777
    binary.chmod(mode | 0o200)
    fd = os.open(binary, os.O_RDWR | os.O_NOFOLLOW)
    original = None
    try:
        offset = os.fstat(fd).st_size - 1
        original = os.pread(fd, 1, offset)
        os.pwrite(fd, bytes([original[0] ^ 1]), offset)
        with pytest.raises((ValueError, PreparationError)):
            verify_prepared_spec(current)
    finally:
        if original is not None:
            os.pwrite(fd, original, offset)
            os.fsync(fd)
        os.close(fd)
        binary.chmod(mode)
    metadata = Path(distribution("openai-codex")._path) / "METADATA"
    original = metadata.read_bytes()
    try:
        changed = original.replace(
            ("Version: " + distribution("openai-codex").version).encode(), b"Version: 999.0.0"
        )
        assert changed != original
        metadata.write_bytes(changed)
        with pytest.raises((ValueError, PreparationError)):
            verify_prepared_spec(current)
    finally:
        metadata.write_bytes(original)
    # The imported program cannot gain another owning directory through a
    # path alias, even when all contents and the underlying inode are retained.
    moved = SOURCE.with_name("displaced-runtime-fixture")
    SOURCE.rename(moved)
    SOURCE.symlink_to(moved, target_is_directory=True)
    try:
        with pytest.raises((ValueError, OSError)):
            verify_prepared_spec(current)
    finally:
        SOURCE.unlink()
        moved.rename(SOURCE)
    verify_prepared_spec(current)


def reject_active_guard_drift(case, current, request):
    """Real postadmission authority must not fall back or heal a lost active guard."""
    from devflow_temporal import delivery_runtime_location as location
    from devflow_temporal.delivery_control import _deadline, _lifecycle_lock
    from devflow_temporal.delivery_preparation import verify_prepared_spec
    from devflow_temporal.delivery_runtime_service_guard import GuardCustodyError

    config = DeliveryConfig.load(case.store.config.path)
    receipt = location._acknowledged(config)
    path, link = Path(receipt["service_guard"]["path"]), Path(receipt["service_guard"]["link"])
    rows = original_rows(config)
    platform = Path.home() / ".codex/public-inventory.json"
    calls = read_private(platform)["calls"]
    for kind in ("missing-guard", "replaced-lock", "missing-lock"):
        if kind == "missing-guard":
            link.unlink()
        else:
            path.unlink()
            if kind == "replaced-lock":
                path.write_bytes(b"")
                path.chmod(0o600)
        try:
            with pytest.raises((GuardCustodyError, ValueError, OSError)):
                with _lifecycle_lock(config, _deadline(config)):
                    pytest.fail("Known active guard loss reached ordinary lifecycle")
            with pytest.raises(GuardCustodyError):
                location.require_ready(config)
            with pytest.raises(GuardCustodyError):
                with case.store._connect():
                    pytest.fail("An existing API Store retained writes after guard loss")
            with pytest.raises(GuardCustodyError):
                verify_prepared_spec(current)
            readonly = DeliveryStore(config, read_only=True)
            assert readonly.submitted_spec("run-1")["request_digest"] == case.spec["request_digest"]
            assert location.historical_proof(case.spec)
            control(config, "adopt-runtime-location", request, success=False)
            control(config, "rollback-runtime-location", request, success=False)
            assert original_rows(config) == rows and read_private(platform)["calls"] == calls
            if kind == "missing-guard":
                assert not link.exists()  # Replaying an ACK cannot silently heal custody.
            elif kind == "missing-lock":
                assert not path.exists()  # The legacy O_CREAT path was not taken.
        finally:
            if kind == "missing-guard":
                os.link(path, link)
            else:
                path.unlink(missing_ok=True)
                os.link(link, path)
    verify_prepared_spec(current)
    with case.store._connect() as db:
        assert db.execute("PRAGMA query_only").fetchone()[0] == 0


def owned_probe_failure_diagnostic(result, check, state_dir):
    """Read authenticated owned JUnit and expose only fixed probe metadata."""
    import re
    import xml.etree.ElementTree as ET

    exception_types = {
        "AssertionError", "RuntimeError", "TimeoutError", "CancelledError", "OSError",
        "ValueError", "TypeError", "ImportError", "ModuleNotFoundError", "ConnectionError",
        "RPCError", "ApplicationError", "ActivityError", "ParseError",
    }
    probes = {
        "test_real_six_hour_time_skipping", "test_real_local_activity",
        "test_real_same_workflow_after_worker_restart",
    }
    try:
        from devflow_temporal.delivery_check_evidence import _retained_junit

        reference = check.get("artifacts")
        if not reference:
            return {"state": "missing"}
        candidate_id = (result.get("candidate") or {}).get("id")
        if not isinstance(candidate_id, str) or not re.fullmatch(r"[0-9a-f]{64}", candidate_id):
            return {"state": "unavailable", "reason": "candidate_unknown"}
        if (type(reference.get("bytes")) is not int or reference["bytes"] > 262144
                or type(reference.get("count")) is not int or reference["count"] > 16):
            return {"state": "unavailable", "reason": "report_bound"}
        content = _retained_junit(reference, candidate_id, Path(state_dir))
        if (len(content) > 262144 or b"<!DOCTYPE" in content.upper()
                or b"<!ENTITY" in content.upper()):
            return {"state": "unavailable", "reason": "report_bound"}
        root = ET.fromstring(content)
        cases = list(root.iter("testcase"))
        if root.tag not in {"testsuite", "testsuites"} or len(cases) > 16:
            return {"state": "unavailable", "reason": "report_shape"}
        failed, unknown = [], 0
        counts = {"passed": 0, "failure": 0, "error": 0, "skipped": 0}
        for case in cases:
            kind = next((tag for tag in ("failure", "error", "skipped")
                         if case.find(tag) is not None), "passed")
            counts[kind] += 1
            if kind not in {"failure", "error"}:
                continue
            name = case.get("name")
            if name not in probes:
                unknown += 1
                continue
            verdict = case.find(kind)
            text = (verdict.get("message", "") + "\n" + (verdict.text or ""))[:262144]
            exception = verdict.get("type", "").rsplit(".", 1)[-1]
            if exception not in exception_types:
                match = re.search(
                    r"(?:^|\n)(?:E\s+)?(" + "|".join(sorted(exception_types)) + r"):", text,
                )
                exception = match.group(1) if match else "other"
            lowered = text.lower()
            startup_timeout = (
                "failed starting" in lowered
                and ("temporal dev server" in lowered or "temporal test server" in lowered)
                and ("timed out" in lowered or "timeout" in lowered)
            )
            failed.append({"probe": name, "outcome": kind, "exception_type": exception,
                           "known_startup_timeout": startup_timeout})
        return {"state": "available", "counts": counts, "failed_cases": failed[:3],
                "unknown_failed_case_count": min(unknown, 16)}
    except Exception as exc:
        category = type(exc).__name__
        return {"state": "unavailable",
                "exception_type": category if category in exception_types else "other"}


def worker_failure_diagnostic(result, calls, native_checks, *, state_dir=None):
    """Report bounded, allowlisted gate facts without copying untrusted output."""
    def choice(value, allowed):
        return value if isinstance(value, str) and value in allowed else "other"

    def count(value):
        return min(len(value), 1000) if isinstance(value, (list, tuple)) else 0

    def number(value):
        return value if type(value) is int and -2**31 <= value < 2**31 else None

    def cleanup_summary(value):
        import re

        summary = {"state": "other", "refusal_category": "other"}
        if not isinstance(value, dict):
            return summary
        summary["state"] = choice(value.get("state"), {
            "removed", "already_absent", "failed_unknown",
        })
        reason = value.get("reason")
        if (summary["state"] != "failed_unknown" or not isinstance(reason, str)
                or len(reason) > 300):
            return summary
        categories = {
            "disposable check artifact root was replaced": "root_replaced",
            "disposable check output lost an original native journal": "journal_changed",
            "registered native processes or ports remain active": "native_actor_or_port_live",
        }
        if reason in categories:
            summary["refusal_category"] = categories[reason]
            return summary
        holders = re.fullmatch(
            r"disposable check artifact root has a live holder "
            r"\(count=([1-9][0-9]{0,6}), pids=([1-9][0-9]{0,9}(?:,[1-9][0-9]{0,9}){0,7})\)",
            reason,
        )
        if holders:
            count = int(holders.group(1))
            pids = [int(value) for value in holders.group(2).split(",")]
            if (count <= 1_000_000 and len(pids) == min(count, 8)
                    and len(set(pids)) == len(pids) and all(pid < 2**31 for pid in pids)):
                summary.update(refusal_category="live_holder", holder_count=count,
                               holder_pids=pids)
            return summary
        inspection = re.fullmatch(
            r"disposable check artifact root holder inspection failed "
            r"\(returncode=(-?[1-9][0-9]{0,2})\)", reason,
        )
        if inspection:
            returncode = int(inspection.group(1))
            if -255 <= returncode <= 255 and returncode != 1:
                summary.update(refusal_category="inspection_failed",
                               inspection_returncode=returncode)
        return summary

    def check_summary(check):
        summary = {
            "id": choice(check.get("id"), {"worker-regressions", "owned-python-layout"}),
            "passed": check.get("passed") if type(check.get("passed")) is bool else None,
            "exit_code": number(check.get("exit_code")),
            "test_count": number(check.get("test_count")),
            "cleanup": choice(check.get("cleanup"), {"confirmed", "unknown"}),
            "evidence_failure_present": bool(check.get("evidence_failure")),
            "rejection_count": count(check.get("rejection_causes")),
            "diagnostic_present": bool(check.get("diagnostic")),
        }
        if "artifact_cleanup" in check:
            summary["artifact_cleanup"] = cleanup_summary(check["artifact_cleanup"])
        if state_dir is not None and check.get("id") == "worker-regressions":
            summary["inner_junit"] = owned_probe_failure_diagnostic(result, check, state_dir)
        return summary

    return {
        "outcome": choice(result.get("outcome"), {"delivered", "blocked", "cancelled"}),
        "error_category": choice(result.get("error"), {"repair limit exhausted"}),
        "finding_count": count(result.get("findings")),
        "stages": {
            stage: {"state": choice(value.get("state"), {"passed", "failed", "unknown",
                                                        "confirmed", "pending"}),
                    "finding_count": count(value.get("findings"))}
            for stage in ("prepublish", "local", "browser_qa", "review", "qa", "ci",
                          "resource_cleanup")
            if isinstance(value := (result.get("checks") or {}).get(stage), dict)
        },
        "calls": [choice(call, {"implement", "review", "verify", "precheck", "publish",
                               "checks", "ci", "terminal_tracker"}) for call in calls[-12:]],
        "roles": [{"role": choice(role.get("role"), {"implement", "review", "verify"}),
                   "status": choice(role.get("status"), {"pass", "findings", "blocked"}),
                   "finding_count": count(role.get("findings"))}
                  for role in result.get("roles", [])[-4:]],
        "checks": [{"state": choice(check.get("state"), {"passed", "failed", "unknown"}),
                    "source_unchanged": (check.get("source_unchanged")
                                         if type(check.get("source_unchanged")) is bool else None),
                    "diagnostic_present": bool(check.get("diagnostic")),
                    "results": [check_summary(item) for item in (check.get("results") or [])[-2:]]}
                   for check in (native_checks[:1] + native_checks[-1:]
                                 if len(native_checks) > 1 else native_checks)],
    }

def assert_native_checks_passed(result, calls, native_checks, *, state_dir=None):
    assert native_checks and native_checks[0].get("state") == "passed", json.dumps(
        worker_failure_diagnostic(result, calls, native_checks, state_dir=state_dir), sort_keys=True
    )


def ci_failure_diagnostics():
    return (os.environ.get("DEVFLOW_CI_FAILURE_DIAGNOSTICS") == "runner-custody-v1"
            and os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REPOSITORY") == "ebarti/devflow")


def holder_executable_observation(pid, sampled_birth):
    """One bounded kernel lookup; output is a label, never identity/authority."""
    import re

    if not ci_failure_diagnostics():
        return {"state": "disabled"}
    unavailable = {"state": "unavailable", "sample_birth_revalidated": None}
    if (type(pid) is not int or not 0 < pid < 2**31 or not isinstance(sampled_birth, str)
            or not 0 < len(sampled_birth) <= 128):
        return unavailable
    # SDK proc_bsdinfo layout: 12 uint32s, comm[16], name[32], six 32-bit
    # values, then high-resolution start seconds/useconds. No names are read.
    script = r'''
import ctypes,json,os,sys,time
value=json.load(sys.stdin)
class BSD(ctypes.Structure):
    _fields_=[("head",ctypes.c_uint32*12),("labels",ctypes.c_char*48),
              ("tail",ctypes.c_uint32*6),("sec",ctypes.c_uint64),("usec",ctypes.c_uint64)]
facts={"state":"unavailable","sample_birth_revalidated":None}
try:
    library=ctypes.CDLL("/usr/lib/libproc.dylib")
    library.proc_pidinfo.argtypes=[ctypes.c_int,ctypes.c_int,ctypes.c_uint64,ctypes.c_void_p,ctypes.c_int]
    library.proc_pidpath.argtypes=[ctypes.c_int,ctypes.c_void_p,ctypes.c_uint32]
    def birth():
        record=BSD()
        count=library.proc_pidinfo(value["pid"],3,0,ctypes.byref(record),ctypes.sizeof(record))
        if count!=ctypes.sizeof(record):
            raise ValueError()
        if record.head[3]!=value["pid"] or not record.sec:
            raise ValueError()
        return record.sec,record.usec
    before=birth()
    stamp=time.strptime(" ".join(value["birth"].split()),"%a %b %d %H:%M:%S %Y")
    expected=int(time.mktime(stamp))
    buffer=ctypes.create_string_buffer(4096)
    length=library.proc_pidpath(value["pid"],buffer,len(buffer))
    after=birth()
    if before!=after or before[0]!=expected:
        facts["sample_birth_revalidated"]=False
    elif 0<length<len(buffer):
        image=buffer.value.decode("utf-8")
        if not os.path.isabs(image):
            raise ValueError()
        name=os.path.basename(image)
        if 0<len(name)<=64 and all(32<=ord(char)<127 for char in name) and name not in {".",".."}:
            facts={"state":"observed","basename":name,"sample_birth_revalidated":True}
except BaseException:
    pass
sys.stdout.write(json.dumps(facts))
'''
    try:
        result = subprocess.run([sys.executable, "-I", "-B", "-c", script],
            input=json.dumps({"pid": pid, "birth": sampled_birth}), text=True,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=5)
        if result.returncode != 0 or len(result.stdout) > 512:
            return unavailable
        value = json.loads(result.stdout)
        if value == unavailable or value == {**unavailable, "sample_birth_revalidated": False}:
            return value
        if (isinstance(value, dict) and set(value) == {
                "state", "basename", "sample_birth_revalidated"}
                and value["state"] == "observed" and value["sample_birth_revalidated"] is True
                and isinstance(value["basename"], str)
                and re.fullmatch(r"[ -~]{1,64}", value["basename"])
                and "/" not in value["basename"] and value["basename"] not in {".", ".."}):
            return value
    except Exception:
        pass
    return unavailable


def artifact_holder_failure_snapshot(exc, original, registry=None, path=None, entry=None,
                                    manifest=None, journals=None, *, root_observation=False,
                                    checkpoint_spec=None):
    """Post-refusal observations are not atomic holder identity or cleanup authority."""
    import re

    from devflow_temporal.delivery_native_process import process_table

    checkpoint = checkpoint_spec is not None
    if checkpoint:
        from devflow_temporal.delivery_checkpoints import _openers

        if (str(exc) != "checkpoint workspace still has an open actor or unknown writer"
                or original is not getattr(_openers, "_original_custody_guard", _openers)
                or not ci_failure_diagnostics()):
            return None
    elif root_observation:
        from devflow_temporal import delivery_resources as resources

        observer = resources._observe_locked_resources
        if (str(exc) != "predecessor has a live root user or unreadable lease"
                or original is not getattr(observer, "_original_custody_guard", observer)):
            return None
    elif not re.fullmatch(
        r"disposable check artifact root has a live holder "
        r"\(count=[1-9][0-9]{0,6}, pids=[1-9][0-9]{0,9}(?:,[1-9][0-9]{0,9}){0,7}\)",
        str(exc),
    ):
        return None
    facts = {"semantics": "post_refusal_non_atomic", "state": "unavailable"}
    trace = exc.__traceback__
    while trace is not None and trace.tb_frame.f_code is not original.__code__:
        trace = trace.tb_next
    if trace is None:
        return facts
    values = trace.tb_frame.f_locals
    if checkpoint:
        path, users = values.get("checkout"), values.get("observed")
        if not isinstance(checkpoint_spec, dict) or not isinstance(path, Path):
            return facts
        homes = {Path(checkpoint_spec["state_dir"]) / "role-homes" / role
                 for role in ("implement", "review", "verify")}
        if (path != Path(checkpoint_spec["checkout"]) and path not in homes
                or not isinstance(users, subprocess.CompletedProcess)
                or users.args != ["/usr/sbin/lsof", "-nP", "-F", "p", "+D", str(path)]):
            return facts
        facts["root_kind"] = "checkout" if path not in homes else "role_home"
    elif root_observation:
        registry, path, entry, manifest, journals = (
            values.get(key) for key in ("registry", "path", "entry", "manifest", "journals"))
        spec, users = values.get("spec"), values.get("observed")
        if (not isinstance(registry, RunResources) or not isinstance(spec, dict)
                or registry.spec is not spec or not isinstance(manifest, dict)
                or not isinstance(manifest.get("roots"), dict) or not isinstance(entry, dict)
                or not isinstance(path, Path) or not path.is_absolute()
                or manifest["roots"].get(str(path)) is not entry
                or registry.state != Path(spec.get("state_dir", ""))
                or manifest.get("run_id") != spec.get("run_id")
                or not isinstance(journals, dict) or len(journals) > 64
                or not isinstance(manifest.get("processes"), list)
                or len(manifest["processes"]) > 64
                or any(not isinstance(raw, str) for raw in manifest["processes"])
                or set(journals) != set(manifest.get("processes", []))
                or any(not isinstance(raw, str) or not isinstance(sha, str)
                       or not re.fullmatch(r"[0-9a-f]{64}", sha)
                       for raw, sha in journals.items())
                or not isinstance(users, subprocess.CompletedProcess)
                or users.args != ["/usr/sbin/lsof", "-nP", "-F", "p", "+D", str(path)]):
            return facts
        kind = entry.get("kind")
        facts["root_kind"] = (kind if isinstance(kind, str) and kind in {"checkout", "gate"}
                              else "other")
    if checkpoint or root_observation:
        facts.update(original_holder_count=None,
                     inspection_returncode=users.returncode
                     if type(users.returncode) is int and -255 <= users.returncode <= 255
                     else None,
                     stderr_present=bool(users.stderr) if isinstance(users.stderr, str) else None)
        if (type(users.returncode) is not int or users.returncode not in (0, 1)
                or not isinstance(users.stdout, str) or len(users.stdout) > 16384):
            return facts
    else:
        users, observed = values.get("users"), values.get("observed")
        if (values.get("self") is not registry or values.get("path") != path
            or values.get("entry") is not entry or values.get("manifest") is not manifest
            or values.get("journals") is not journals or entry.get("kind") != "check-artifacts"
            or not isinstance(path, Path) or not path.is_relative_to(registry.state)
            or path.name != "pytest-artifacts" or not isinstance(users, subprocess.CompletedProcess)
            or users.args != ["/usr/sbin/lsof", "-nP", "-F", "p", "+D", str(path)]
            or type(users.returncode) is not int or users.returncode not in (0, 1)
            or not isinstance(users.stdout, str) or len(users.stdout) > 16384
            or not isinstance(observed, list) or not 1 <= len(observed) <= 2):
            return facts
    lines = users.stdout.splitlines()
    if not lines or not all(
        re.fullmatch(r"p[1-9][0-9]{0,9}", line) and int(line[1:]) < 2**31
        or re.fullmatch(r"f(?:[0-9]{1,6}[rwu]?|cwd|rtd|txt|mem|DEL)"
                        if root_observation or checkpoint
                        else r"f(?:[0-9]{1,6}|cwd|rtd|txt|mem|DEL)", line)
        for line in lines
    ):
        return facts
    holders = [int(line[1:]) for line in lines if line.startswith("p")]
    if not holders:
        return facts
    expected = ("disposable check artifact root has a live holder "
                f"(count={len(holders)}, pids={','.join(map(str, holders[:8]))})")
    if (not (root_observation or checkpoint) and str(exc) != expected
            or len(set(holders)) != len(holders)):
        return facts
    facts["original_holder_count"] = len(holders)
    holders = holders[:8]
    census, census_truncated, owner = {}, set(), None
    for line in lines:
        if line.startswith("p"):
            owner = int(line[1:])
        elif owner is None:
            return facts
        elif owner in holders:
            value = line[1:]
            if (root_observation or checkpoint) and re.fullmatch(r"[0-9]{1,6}[rwu]", value):
                value = value[:-1]
            rows = census.setdefault(owner, [])
            if len(rows) < 8:
                rows.append({"category": "numeric" if value.isdecimal() else value,
                             "fd": int(value) if value.isdecimal() else None})
            else:
                census_truncated.add(owner)
    fixture_pid = os.getpid()
    births, birth_inventory_complete = {}, not (root_observation or checkpoint)
    if checkpoint:
        pass  # No original actor inventory is yielded by this guard.
    elif root_observation:
        raw, sha, journal = (values.get(key) for key in (
            "journal_bytes", "journal_sha256", "value"))
        try:
            if (isinstance(raw, bytes) and len(raw) <= 262144 and isinstance(sha, str)
                    and sha in journals.values() and hashlib.sha256(raw).hexdigest() == sha
                    and isinstance(journal, dict) and json.loads(raw) == journal):
                actors = dict(journal.get("owned", {}))
                monitor = journal.get("monitor", {})
                if type(monitor.get("pid")) is int:
                    actors[str(monitor["pid"])] = monitor
                for pid in holders:
                    identity = actors.get(str(pid), {}).get("identity")
                    if isinstance(identity, str) and 0 < len(identity) <= 128:
                        births.setdefault(pid, set()).add(identity)
                birth_inventory_complete = len(journals) == 1
        except (ValueError, TypeError, AttributeError):
            births = {}
    else:
        allowed = {path.parent / name / "native-process.json"
                   for name in ("native", "native-retry")}
        if (not isinstance(journals, (list, tuple)) or not 1 <= len(journals) <= 2
                or any(not isinstance(item, dict) or not isinstance(item.get("journal"), str)
                       or not isinstance(item.get("sha256"), str)
                       or Path(item["journal"]) not in allowed
                       or not re.fullmatch(r"[0-9a-f]{64}", item["sha256"])
                       for item in observed)
                or set(map(Path, journals)) != {Path(item["journal"]) for item in observed}):
            return facts
        births = {}
        for item in observed:
            try:
                raw = read_private(Path(item["journal"]), raw=True)
                if len(raw) > 262144 or hashlib.sha256(raw).hexdigest() != item["sha256"]:
                    continue
                journal = json.loads(raw)
                actors = dict(journal.get("owned", {}))
                monitor = journal.get("monitor", {})
                if type(monitor.get("pid")) is int:
                    actors[str(monitor["pid"])] = monitor
                for pid in holders:
                    identity = actors.get(str(pid), {}).get("identity")
                    if isinstance(identity, str) and 0 < len(identity) <= 128:
                        births.setdefault(pid, set()).add(identity)
            except (OSError, ValueError, TypeError, AttributeError):
                continue
    try:
        table = process_table()
        process_state = "available" if isinstance(table, dict) else "unavailable"
        if not isinstance(table, dict):
            table = {}
    except Exception:
        table, process_state = {}, "unavailable"
    descriptors, commands, truncated = {}, {}, set()
    fd_state = "unavailable"
    extended = ci_failure_diagnostics()

    def reported_role(value):
        reported = Path(value)
        if (not reported.is_absolute() or ".." in reported.parts
                or not reported.is_relative_to(path)):
            return "unknown"
        relative = reported.relative_to(path)
        if path.name == "pytest-artifacts" and relative.parts == ("junit.xml",):
            return "reported_owned_junit_report"
        if facts.get("root_kind") == "role_home":
            if relative.parts == ("codex", "state_1.sqlite"):
                return "reported_session_database"
            if (len(relative.parts) == 3 and relative.parts[:2] == ("codex", "sessions")
                    and relative.suffix == ".jsonl"):
                return "reported_session_transcript"
        return "reported_other_within_root"

    try:
        result = subprocess.run(
            ["/usr/sbin/lsof", "-nP", "-a", "-p", ",".join(map(str, holders)),
             "-F", "0pftcan" if extended else "pftc", "+D", str(path)],
            capture_output=True, text=True, timeout=5,
        )
        if (type(result.returncode) is int and result.returncode in (0, 1)
                and isinstance(result.stdout, str) and len(result.stdout) <= 32768):
            fields = ([field.lstrip("\n") for field in result.stdout.split("\0")
                       if field.strip("\n")]
                      if extended else result.stdout.splitlines())
            if len(fields) > 1024:
                raise ValueError("bounded descriptor field count exceeded")
            pid, row = None, None

            def finish_row():
                if row is not None and row["category"] != "unknown" and row["type"] is not None:
                    if len(descriptors[pid]) < 8:
                        descriptors[pid].append(dict(row))
                    else:
                        truncated.add(pid)

            for line in fields:
                if re.fullmatch(r"p[1-9][0-9]{0,9}", line) and int(line[1:]) in holders:
                    finish_row()
                    pid, row = int(line[1:]), None
                    descriptors.setdefault(pid, [])
                elif line.startswith("c") and pid is not None:
                    value = line[1:]
                    family = {"node": "node", "temporal": "temporal", "java": "java",
                              "temporal-test-server": "temporal_test_server",
                              "temporal_test_server": "temporal_test_server"}.get(value)
                    if re.fullmatch(r"(?:python|Python)(?:[1-9][0-9]?(?:\.[0-9]{1,2}){0,2})?",
                                    value):
                        family = "python"
                    commands[pid] = family or ("other" if 0 < len(value) <= 64 else None)
                elif line.startswith("f") and pid is not None:
                    finish_row()
                    value = line[1:]
                    numeric = int(value) if re.fullmatch(r"[0-9]{1,6}", value) else None
                    numeric = numeric if numeric is not None and numeric <= 1048575 else None
                    category = "numeric" if numeric is not None else (
                        value if value in {"cwd", "rtd", "txt", "mem", "DEL"} else "unknown")
                    row = {"category": category, "fd": numeric, "type": None}
                    if extended:
                        row.update(access="unknown", file_role="unknown")
                elif line.startswith("t") and row is not None:
                    if row["type"] is not None:
                        raise ValueError("duplicate bounded descriptor type")
                    row["type"] = line[1:] if line[1:] in {
                        "REG", "DIR", "CHR", "FIFO", "IPv4", "IPv6", "unix", "KQUEUE", "PIPE"
                    } else None
                elif extended and line.startswith("a") and row is not None:
                    row["access"] = {"r": "read", "w": "write", "u": "read_write"}.get(
                        line[1:], "unknown")
                elif extended and line.startswith("n") and row is not None:
                    row["file_role"] = reported_role(line[1:])
                else:
                    raise ValueError("unrecognized bounded descriptor record")
            finish_row()
            fd_state = "observed" if any(descriptors.values()) else "not_observed"
    except Exception:
        descriptors, commands, fd_state = {}, {}, "unavailable"

    def pid_number(value):
        return value if type(value) is int and 0 <= value < 2**31 else None

    rows = []
    categories = {"R": "running", "S": "sleeping", "I": "idle", "T": "stopped",
                  "t": "stopped", "Z": "zombie", "U": "uninterruptible", "D": "uninterruptible"}
    for pid in holders:
        current = table.get(pid)
        current = current if isinstance(current, dict) else {}
        identity, status = current.get("identity"), current.get("stat")
        identity = identity if isinstance(identity, str) and 0 < len(identity) <= 128 else None
        status = status if isinstance(status, str) and re.fullmatch(
            r"[RSDTtZIXU][<N+LsEesl-]{0,8}", status) else None
        rows.append({"pid": pid, "ppid": pid_number(current.get("ppid")),
                     "pgid": pid_number(current.get("pgid")),
                     "original_census_descriptors": census.get(pid) or None,
                     "original_census_sample_truncated": pid in census_truncated
                     if pid in census else None,
                     "parent_is_fixture": current["ppid"] == fixture_pid
                     if pid_number(current.get("ppid")) is not None else None,
                     "process_state": categories.get(status[0]) if status else None,
                     "command_family": commands.get(pid),
                     "recorded_birth_match": (identity in births[pid]
                                              if birth_inventory_complete or identity in births[pid]
                                              else None)
                     if identity is not None and pid in births else None,
                     "descriptors": descriptors.get(pid) or None,
                     "descriptor_sample_truncated": pid in truncated
                     if pid in descriptors else None})
    facts.update(state="snapshot", fixture_pid=fixture_pid,
                 process_snapshot=process_state, fd_snapshot=fd_state,
                 descriptor_sample_limit=8, holders=rows)
    if extended:
        first = table.get(holders[0])
        facts["executable_observation"] = holder_executable_observation(
            holders[0], first.get("identity") if isinstance(first, dict) else None)
        facts["descriptor_role_semantics"] = "reported_path_only_not_original_inode"
    return facts


def observe_artifact_holder_failures(patch):
    original_dispose = RunResources._dispose_check_artifacts

    def dispose_with_failure_snapshot(registry, path, entry, manifest, journals):
        try:
            return original_dispose(registry, path, entry, manifest, journals)
        except ValueError as exc:
            try:
                snapshot = artifact_holder_failure_snapshot(
                    exc, original_dispose, registry, path, entry, manifest, journals)
                if snapshot is not None:
                    print(json.dumps({"scope_artifact_holder_snapshot": snapshot}, sort_keys=True),
                          file=sys.stderr, flush=True)
            except BaseException:
                pass  # Diagnostic failure must preserve the original refusal and traceback.
            raise

    patch.setattr(RunResources, "_dispose_check_artifacts", dispose_with_failure_snapshot)


def observe_resource_holder_failures(patch):
    if not ci_failure_diagnostics():
        return
    from devflow_temporal import delivery_resources as resources

    original = resources._observe_locked_resources

    def observe(*args, **kwargs):
        try:
            return original(*args, **kwargs)
        except ValueError as exc:
            try:
                snapshot = artifact_holder_failure_snapshot(exc, original, root_observation=True)
                if snapshot is not None:
                    print(json.dumps({"resource_root_holder_snapshot": snapshot}, sort_keys=True),
                          file=sys.stderr, flush=True)
            except BaseException:
                pass
            raise

    observe._original_custody_guard = original
    patch.setattr(resources, "_observe_locked_resources", observe)


def observe_checkpoint_holder_failures(patch, spec):
    if not ci_failure_diagnostics():
        return
    from devflow_temporal import delivery_checkpoints as checkpoints

    original = checkpoints._openers

    def openers(checkout):
        try:
            return original(checkout)
        except ValueError as exc:
            try:
                snapshot = artifact_holder_failure_snapshot(exc, original, checkpoint_spec=spec)
                if snapshot is not None:
                    print(json.dumps({"checkpoint_holder_snapshot": snapshot}, sort_keys=True),
                          file=sys.stderr, flush=True)
            except BaseException:
                pass
            raise

    openers._original_custody_guard = original
    patch.setattr(checkpoints, "_openers", openers)


async def actual_worker(case, current, recovery, response, patch):
    """Actual workflow, readback, native checks/browser/finalization and replay.

    Provider verdicts and GitHub/ProjectSync are controlled external boundaries.
    The unchanged owner/preparation/resource/process/check/recipe consumers run.
    """
    from temporalio import activity
    from temporalio.client import Client
    from temporalio.worker import Replayer, Worker
    from test_delivery_temporal_prerequisites import owned_sdk_environment

    from devflow_temporal.delivery_activities import (
        delivery_browser_qa,
        delivery_checks,
        delivery_finalize_resources,
        delivery_project,
        delivery_repair_preflight,
    )
    from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    observe_artifact_holder_failures(patch)
    original_checks = DeliveryBroker._run_check_list
    native_checks = []

    def checks_with_dependencies(self, checkout, checks, *args, **kwargs):
        if any(value["id"] == case.check["id"] for value in checks):
            owned_sdk_environment(self, checkout)
        return original_checks(self, checkout, checks, *args, **kwargs)

    patch.setattr(DeliveryBroker, "_run_check_list", checks_with_dependencies)
    calls = []

    @activity.defn(name="delivery_role")
    async def role(body):
        calls.append(body["role"])
        assert body["iteration"] == 4
        if body["role"] == "implement":
            assert body["resume_session"] == recovery["session_id"]
            path = Path(current["checkout"]) / case.api
            path.write_text(path.read_text() + "\nAuthorized owned fixture iteration4\n")
            git(Path(current["checkout"]), "add", case.api)
            git(Path(current["checkout"]), "commit", "-qm", "Owned finite fixture implementation")
        return {
            "role": body["role"],
            "iteration": 4,
            "status": "pass",
            "cleanup": "confirmed",
            "findings": [],
            "session_id": recovery["session_id"] if body["role"] == "implement" else body["role"],
            "candidate": DeliveryBroker(case.store, body["spec"]).candidate(),
        }

    @activity.defn(name="delivery_tracker_start")
    async def tracker_start(_body):
        return {"state": "consistent"}

    @activity.defn(name="delivery_precheck")
    async def precheck(body):
        calls.append("precheck")
        return {"state": "passed", "candidate_id": body["candidate"]["id"], "cleanup": "confirmed"}

    @activity.defn(name="delivery_publish")
    async def publish(body):
        calls.append("publish")
        git(Path(current["checkout"]), "push", "origin", "HEAD:refs/heads/" + current["branch"])
        case.publication.update(head=body["candidate"]["head"], candidate=body["candidate"])
        return copy.deepcopy(case.publication)

    @activity.defn(name="delivery_checks")
    async def checks(body):
        calls.append("checks")
        result = await delivery_checks(body)
        native_checks.append(result)
        return result

    @activity.defn(name="delivery_ci")
    async def ci(body):
        calls.append("ci")
        return {"state": "passed", "head": body["pull_request"]["head"]}

    @activity.defn(name="delivery_terminal_tracker")
    async def terminal_tracker(body):
        calls.append("terminal_tracker")
        if body["release"]:
            with case.store._connect() as db:
                case.store.state.release_work(db, current["work_id"], "external:devflow:run-1")
        return {"state": "consistent"}

    client = await Client.connect(
        case.store.config.temporal_address, data_converter=DELIVERY_DATA_CONVERTER
    )
    queue = "owned-location-" + case.kind
    case.store.mark_start("run-1", accepted=True)
    async with Worker(
        client,
        task_queue=queue,
        workflows=[DeliveryWorkflow],
        activities=[
            delivery_repair_preflight,
            delivery_project,
            delivery_finalize_resources,
            delivery_browser_qa,
            role,
            tracker_start,
            precheck,
            publish,
            checks,
            ci,
            terminal_tracker,
        ],
    ):
        handle = await client.start_workflow(
            DeliveryWorkflow.run,
            args=[current, recovery],
            id=response["workflow_id"],
            task_queue=queue,
        )
        result = await asyncio.wait_for(handle.result(), 150)
        assert result["outcome"] == "delivered", json.dumps(
            worker_failure_diagnostic(result, calls, native_checks,
                                      state_dir=current["state_dir"]), sort_keys=True
        )
        assert result["iteration"] == 4 and result["roles"][0] == recovery["state"]["roles"][0]
        assert result["cleanup"] == "confirmed"
        assert_native_checks_passed(result, calls, native_checks, state_dir=current["state_dir"])
        worker = next(
            value for value in native_checks[0]["results"] if value["id"] == case.check["id"]
        )
        assert worker["junit"]["passed"] == 3 and worker["passed"] is True
        if case.kind == "browser":
            qa = result["checks"]["browser_qa"]
            assert qa["test_count"] == 2
            assert [item["state"] for item in qa["browser_qa_amendment"]["cases"]] == [
                "passed",
                "passed",
            ]
        assert "review" in calls and "verify" in calls and "ci" in calls
        history = await handle.fetch_history()
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
    return {"native_junit_passed": 3, "workflow_replay": "passed", "iteration": 4}


def exercise(root, kind):
    from test_delivery_temporal_prerequisites import native_check_ready

    from devflow_temporal import delivery_native_preparation as native
    from devflow_temporal import delivery_runtime_location as location
    from devflow_temporal.delivery_activities import _context, delivery_repair_preflight
    from devflow_temporal.delivery_closures import sealed_closure
    from devflow_temporal.delivery_resources import _gate_roots
    from devflow_temporal.payload import payload_digest
    from devflow_temporal.supervisor import get_supervisor

    case = restored(root)
    config = DeliveryConfig.load(Path(case.owner))
    assert not Path(case.old_root).exists()
    assert SOURCE == Path.home() / ".local/share/devflow/developer-workflow"
    assert Path(sys.prefix) == SOURCE / "runtime/.venv"
    patch = pytest.MonkeyPatch()
    scoped_external_boundaries(case, patch)
    # Old measurements are still impossible to launch under the strict owner.
    with pytest.raises((ValueError, OSError)):
        native.verify_native_spec(case.spec)
    with pytest.raises(ValueError, match="helper is missing"):
        DeliveryStore(config)
    control(config, "stop")  # Supported lifecycle remains reachable without old helpers.
    binary = Path(distribution("openai-codex-cli-bin").locate_file("codex_cli_bin/bin/codex"))
    before = config.path.read_bytes()
    after = copy.deepcopy(config.raw)
    after["codex_bin"] = str(binary)
    proposal = config.state_root / "operator-inputs/permanent-owner.json"
    write_private(proposal, after)
    supplied = {
        "command_id": "owned-permanent-" + kind,
        "owner_before_sha256": hashlib.sha256(before).hexdigest(),
        "owner_after_path": str(proposal),
        "owner_after_sha256": hashlib.sha256(proposal.read_bytes()).hexdigest(),
        "destination": {
            "root": str(SOURCE),
            "revision": git(SOURCE, "rev-parse", "HEAD"),
            "tree": git(SOURCE, "rev-parse", "HEAD^{tree}"),
            "payload_sha256": payload_digest(SOURCE / "runtime/src/devflow_temporal"),
        },
        "runs": ["run-" + str(index) for index in range(1, 7)],
        "host_home": str(Path.home()),
        "host_cli": case.cli,
        "host_cli_sha256": hashlib.sha256(Path(case.cli).read_bytes()).hexdigest(),
        "baseline_revision": PLUGIN_BASELINE,
        "plugin_origin": case.plugin_origin,
        "answer": location.ANSWER,
    }
    request = config.state_root / "operator-inputs/permanent-request.json"
    write_private(request, supplied)
    if kind == "scope":
        write_private(
            request, {key: value for key, value in supplied.items() if key != "plugin_origin"}
        )
        assert "installed plugin differs" in control(
            config, "runtime-location-preflight", request, success=False
        )
        write_private(request, supplied)
        reject_proposal_drift(config, request, supplied, proposal, binary)
    rows = original_rows(config)
    preflight = control(config, "runtime-location-preflight", request)
    assert original_rows(config) == rows and config.path.read_bytes() == before
    assert (
        preflight["authority"]["destination"]["identity"]["python_sha256"]
        == (case.spec["policy"]["native_identity"]["python_sha256"])
    )
    supplied["expected_evidence_sha256"] = preflight["expected_evidence_sha256"]
    write_private(request, supplied)
    if kind == "pendingrollback":
        pending_rollback(root, case, config, request, rows, before)
        patch.undo()
        return
    if kind == "scope":
        cohort_faults(root, config, request, rows)
    # The old ProjectSync module can write unrelated tracker data even while the
    # new controller sees the pending fence; it has never learned this fence.
    import threading
    import time

    errors, compatible = [], []
    stop = threading.Event()

    def old_projector():
        try:
            sys.path.insert(0, str(root / "loaded-old-projector"))
            spec = importlib.util.spec_from_file_location(
                "owned_old_projector", root / "loaded-old-projector/state.py"
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            pending = config.state_root / "runtime-installations/pending.json"
            for _ in range(3000):
                if stop.is_set():
                    return
                if pending.exists():
                    with module.connect(config.tracking_db) as db:
                        count = db.execute(
                            "SELECT COUNT(*) FROM delivery_events WHERE type=?", (location.PENDING,)
                        ).fetchone()[0]
                        if count == 6:
                            with pytest.raises(ValueError, match="installation is incomplete"):
                                DeliveryStore(config)
                            module.record(
                                db,
                                "work",
                                {
                                    "id": "old-project-sync-only",
                                    "title": "Owned tracker observation",
                                },
                            )
                            module.update(
                                db,
                                "work",
                                {
                                    "id": "old-project-sync-only",
                                    "title": "Owned refreshed tracker observation",
                                },
                                "owned-refresh",
                            )
                            assert module.metrics(db)
                            compatible.append(True)
                            return
                time.sleep(0.01)
            raise AssertionError("The old projector did not observe the pending cohort")
        except BaseException as exc:
            errors.append(repr(exc))

    thread = threading.Thread(target=old_projector)
    thread.start()
    try:
        result = control(config, "adopt-runtime-location", request)
    finally:
        stop.set()
        thread.join(timeout=10)
    assert compatible and not errors, errors
    assert result["state"] == "acknowledged" and original_rows(config) == rows
    assert location._acknowledged(DeliveryConfig.load(config.path))
    assert_cohort_native_authority(DeliveryConfig.load(config.path))
    reject_partial_membership(DeliveryConfig.load(config.path))
    control(config, "adopt-runtime-location", request)
    case.store = DeliveryStore(DeliveryConfig.load(config.path))
    if kind == "scope":
        import httpx

        from devflow_temporal.delivery_api import create_app

        async def public_read():
            app = create_app(config.path)
            transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
            async with httpx.AsyncClient(
                transport=transport, base_url=case.store.config.dashboard_url, timeout=30,
            ) as browser:
                started = time.monotonic()
                response = await asyncio.wait_for(browser.get("/api/runs/run-1"), timeout=30)
            assert response.status_code == 200, response.text[:1000]
            assert response.json()["run"]["run_id"] == "run-1"
            return {"seconds": time.monotonic() - started, "bytes": len(response.content)}

        print(json.dumps({"actual_native_post_ack_public_get": asyncio.run(public_read())}))
    from devflow_temporal.delivery_control import _deadline, _lifecycle_lock

    with _lifecycle_lock(case.store.config, _deadline(case.store.config)):
        assert (config.state_root / "service.lock").stat().st_nlink == 2
    control(case.store.config, "stop")
    case.broker = DeliveryBroker(case.store, case.spec)
    assert location.historical_proof(case.spec)
    with pytest.raises((ValueError, OSError)):
        native.verify_native_spec(case.spec)
    if kind == "rollback":
        result = control(case.store.config, "rollback-runtime-location", request)
        restored_owner = DeliveryConfig.load(config.path)
        assert restored_owner.path.read_bytes() == before
        assert original_rows(restored_owner) == rows
        assert location._acknowledged(restored_owner) is None
        assert location.historical_proof(case.spec)
        control(restored_owner, "stop")
        assert_original_plugin_bytes(case)
        error = control(restored_owner, "adopt-runtime-location", request, success=False)
        assert "rolled back" in error, error
        print(
            json.dumps(
                {
                    **result,
                    "ack_row_unchanged": True,
                    "original_proof_preserved": True,
                    "original_package_bytes": True,
                    "historical_proof_readable": True,
                    "legacy_guard_retained": (config.state_root / "service.lock").stat().st_nlink
                    == 2,
                }
            )
        )
        patch.undo()
        return
    if kind == "scope":
        reject_retained_custody_drift(case)
    if kind.startswith("stale"):
        from temporalio.client import Client

        from devflow_temporal.delivery_closed_projection import reconcile_completed_projections
        from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
        from devflow_temporal.delivery_orphans import reconcile_attempt

        async def maintenance():
            client = await Client.connect(
                config.temporal_address, data_converter=DELIVERY_DATA_CONVERTER
            )
            return await reconcile_completed_projections(case.store, client)

        observed = asyncio.run(maintenance())
        assert any(
            item["run_id"] == "run-1" and item["state"] == "reconciled" for item in observed
        ), observed
        reconciled = case.store.detail("run-1")
        assert reconciled["phase"] == "blocked" and reconciled["cleanup"] == "unknown"
        assert reconciled["checks"] == case.state["checks"]
        assert (
            reconcile_attempt(case.store, run_id="run-1", role="cleanup", iteration=3)["cleanup"]
            == "confirmed"
        )
        case.command["expected_revision"] = case.store.detail("run-1")["protocol_revision"]
        assert case.store.detail("run-1")["checks"]["local"]["state"] == "failed"
        original = asyncio.run(sealed_closure(case.store, "delivery-run-1"))
        assert original["result"] == case.state and original["result"]["cleanup"] == "unknown"
    outcome = case.store.repair_admission_preflight("run-1", case.command)
    assert outcome["authorized_through_iteration"] == 4
    response = case.store.continue_repair("run-1", case.command)
    assert case.store.continue_repair("run-1", case.command) == response
    current = case.store.effective_spec("run-1")
    with case.store._connect() as db:
        recovery = json.loads(
            db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0]
        )
    assert (
        recovery["maximum_iteration"] == 4 and recovery["session_id"] == case.result["session_id"]
    )
    assert current["policy"]["capacity"] == 6
    assert current["policy"]["codex_bin"] == str(binary)
    native.verify_native_spec(current)
    owner, broker = _context(current)
    assert get_supervisor(owner).capacity == 2
    bindings = {}
    _gate_roots(current, execution_specs=bindings)
    assert current in bindings.values() and case.spec in bindings.values()
    assert asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery})) == {
        "state": "confirmed"
    }
    if kind == "scope":
        import httpx

        from devflow_temporal.delivery_api import create_app

        async def admitted_public_reads():
            app = create_app(config.path)
            transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
            async with httpx.AsyncClient(
                transport=transport, base_url=case.store.config.dashboard_url, timeout=30,
            ) as browser:
                started = time.monotonic()
                detail, listing = await asyncio.wait_for(asyncio.gather(
                    browser.get("/api/runs/run-1"), browser.get("/api/runs"),
                ), timeout=30)
            assert detail.status_code == listing.status_code == 200
            assert detail.json()["run"]["iteration"] == 3
            assert len(listing.json()["runs"]) == 6
            return {"seconds": time.monotonic() - started, "provider": current["provider"],
                    "detail_bytes": len(detail.content), "default_page_rows": 6}

        print(json.dumps({
            "actual_native_post_admission_public_reads": asyncio.run(admitted_public_reads()),
        }))
    if kind == "scope":
        reject_installed_custody_drift(case, current)
        reject_active_guard_drift(case, current, request)
    # The actual launch owner consumes the current candidate, claim, manifest,
    # recipe, independent pinned tool receipt and fresh installed native proof.
    if kind in {"scope", "browser"}:
        worker_result = asyncio.run(actual_worker(case, current, recovery, response, patch))
    else:
        worker_result = None
    broker = native_check_ready(case, current) if worker_result is None else None
    from devflow_temporal.delivery_sandbox import prepare_native_check
    from devflow_temporal.delivery_temporal_prerequisites import check_launch

    if broker is not None:
        evidence = broker.evidence_dir / "checks/4"
        try:
            _profile, environment = prepare_native_check(
                current, broker.checkout, evidence, case.check
            )
            process = SimpleNamespace(
                spec=current,
                folder=evidence / case.check["id"] / "native",
                argv=case.check["argv"],
                cwd=broker.checkout,
                environment=environment,
                timeout=2700,
            )
            check_launch(process)
        finally:
            assert RunResources(current).finalize("blocked")["state"] == "confirmed"
    control(DeliveryConfig.load(config.path), "rollback-runtime-location", request, success=False)
    assert case.store.continue_repair("run-1", case.command) == response
    fresh = fresh_admission(case) if kind == "scope" else None
    print(
        json.dumps(
            {
                "old_runtime_absent": True,
                "ack_row_unchanged": True,
                "cohort_size": 6,
                "strict_bundle": True,
                "finite_maximum": 4,
                "native_preparation": "actual",
                "capacity": 2,
                "frozen_capacity": 6,
                "kind": kind,
                "source": str(SOURCE),
                "worker": worker_result,
                "fresh_registration": fresh,
            }
        )
    )
    patch.undo()


if __name__ == "__main__":
    phase, raw_root, scenario = sys.argv[1:]
    selected = {"seed": seed, "exercise": exercise, "crash": crash_apply}[phase]
    selected(Path(raw_root), scenario)
