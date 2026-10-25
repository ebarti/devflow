"""Owned subprocess harness for the public permanent-installation contract."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib.util
import json
import os
import shutil
import socket
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from importlib.metadata import distribution
from pathlib import Path
from types import SimpleNamespace

import pytest

import devflow_temporal

SOURCE = Path(devflow_temporal.__file__).resolve().parents[3]
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
plugin={'pluginId':'devflow@devflow-local','enabled':True}
if args==['mcp','list','--json']: print('[]')
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
        "repository_key": "fixture",
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
    from test_delivery_amendment_capacity import cut_operational_capacity
    from test_delivery_temporal_prerequisites import temporal_case

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
    data.update(owner=str(case.store.config.path), old_root=str(SOURCE), cli=str(cli), kind=kind)
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

    with sqlite3.connect(config.tracking_db) as db:
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

    with sqlite3.connect(config.tracking_db) as db:
        db.execute(
            "CREATE TRIGGER owned_partial_intent BEFORE INSERT ON delivery_commands "
            "WHEN NEW.run_id='run-6' AND NEW.command_id LIKE 'runtime-location-intent-%' "
            "BEGIN SELECT RAISE(ABORT,'Owned cohort crash'); END"
        )
    control(config, "adopt-runtime-location", request, success=False)
    assert original_rows(config) == rows
    with sqlite3.connect(config.tracking_db) as db:
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
        with sqlite3.connect(config.tracking_db) as db:
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
    with sqlite3.connect(config.tracking_db) as db:
        saved = db.execute("SELECT * FROM delivery_commands WHERE command_id=?", (key,)).fetchone()
        db.execute("DELETE FROM delivery_commands WHERE command_id=?", (key,))
    try:
        assert location._acknowledged(config) is None
        with pytest.raises(ValueError, match="not completely acknowledged"):
            DeliveryStore(config)
    finally:
        with sqlite3.connect(config.tracking_db) as db:
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
    with sqlite3.connect(config.tracking_db) as db:
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
    assert read_private(Path.home() / ".codex/public-inventory.json")["calls"] == calls
    assert all(_state(item) == "before" for item in receipt["installed_objects"])
    assert (
        original_rows(config) == rows
        and not (config.state_root / "runtime-installations/current.json").exists()
    )
    with sqlite3.connect(config.tracking_db) as db:
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
        assert result["outcome"] == "delivered", result.get("error")
        assert result["iteration"] == 4 and result["roles"][0] == recovery["state"]["roles"][0]
        assert result["cleanup"] == "confirmed"
        assert native_checks and native_checks[0]["state"] == "passed", native_checks
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
        "baseline_revision": "1ebfc71ea5522ee74dbf6cd820f01649e3197bca",
        "answer": location.ANSWER,
    }
    request = config.state_root / "operator-inputs/permanent-request.json"
    write_private(request, supplied)
    if kind == "scope":
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
    reject_partial_membership(DeliveryConfig.load(config.path))
    control(config, "adopt-runtime-location", request)
    case.store = DeliveryStore(DeliveryConfig.load(config.path))
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
        error = control(restored_owner, "adopt-runtime-location", request, success=False)
        assert "rolled back" in error, error
        print(
            json.dumps(
                {
                    **result,
                    "ack_row_unchanged": True,
                    "original_proof_preserved": True,
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
