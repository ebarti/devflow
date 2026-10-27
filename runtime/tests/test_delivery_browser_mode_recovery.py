"""Closed browser setup amendments use the actual config/resource/native launch owners."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import socket
import sys
from pathlib import Path

import pytest
from test_delivery_blocked_scope import rewrite_assessment, rewrite_observation, scope_case
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_browser_mode_recovery as mode
from devflow_temporal import delivery_native_process as native
from devflow_temporal import delivery_policy_recovery as policy
from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_activities import delivery_repair_preflight
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_gate_retry import prepare_runtime
from devflow_temporal.delivery_resources import (
    RunResources,
    private_directory,
    read_private,
    write_private,
)

CASES = [
    ("offer-outcomes.spec.ts", "Offer history composition"),
    ("outreach.spec.ts", "Outreach reviewed action"),
]


def junit(*, skipped=True, missing=False):
    cases = CASES[:1] if missing else CASES
    return (
        "<testsuites>"
        + "".join(
            f'<testsuite name="{file}"><testcase classname="{file}" name="{name}" time=".01">'
            + ("<skipped/>" if skipped and index == 0 else "")
            + "</testcase></testsuite>"
            for index, (file, name) in enumerate(cases)
        )
        + "</testsuites>"
    )


GATE = """import json,os,pathlib,socket,time
root=pathlib.Path(os.environ['PLAYWRIGHT_JUNIT_OUTPUT_FILE']).parent
root.mkdir(parents=True,exist_ok=True)
mode=os.environ['JOBCTRL_E2E_ISOLATED']
home=pathlib.Path(os.environ['HOME'])
workspace=home/'owned-synthetic-workspace'
workspace.mkdir(parents=True)
(workspace/'config.json').write_text('{"provider":"fake","actions":"fake-transport"}')
cases=[('offer-outcomes.spec.ts','Offer history composition'),
       ('outreach.spec.ts','Outreach reviewed action')]
items=[]
for index,(file,name) in enumerate(cases):
    skipped=mode=='1' and index==0
    opening=f'<testsuite name="{file}"><testcase classname="{file}" name="{name}" time=".01">'
    items.append(opening+('<skipped/>' if skipped else '')+'</testcase></testsuite>')
pathlib.Path(os.environ['PLAYWRIGHT_JUNIT_OUTPUT_FILE']).write_text('<testsuites>'+''.join(items)+'</testsuites>')
seen={'mode':mode,'home':str(home),'tmp':os.environ['TMPDIR'],'workspace':str(workspace),
      'fake_transport':True,'fake_dispatch':True,
      'secrets':[key for key in ('OPENAI_API_KEY','GH_TOKEN','JOBCTRL_DIR','JOBCTRL_DB_PATH')
                 if key in os.environ]}
(root/'observed-launch.json').write_text(json.dumps(seen))
sockets=[]
for key in ('QA_API_PORT','QA_WEB_PORT'):
    sock=socket.socket();sock.bind(('127.0.0.1',int(os.environ[key])));sock.listen();sockets.append(sock)
time.sleep(.7)
print('1 skipped\\n1 passed' if mode=='1' else '2 passed',flush=True)
"""


REAL_TABLE, REAL_LISTENERS = native.process_table, native.listeners


def browser_case(service, monkeypatch, *, ports=None, extra_sources=None, checks=None):
    qa = {
        "id": "owned-browser",
        "argv": [
            sys.executable,
            "apps/web/e2e/fixtures/gate.py",
            "playwright",
            "test",
            "e2e/tests/outreach.spec.ts",
            "e2e/tests/offer-outcomes.spec.ts",
        ],
        "ports": dict(zip(("QA_API_PORT", "QA_WEB_PORT"), ports or (34127, 34128), strict=True)),
        "timeout_seconds": 30,
        "cwd": ".",
        "artifact_paths": ["dist/playwright-report"],
        "env": {"JOBCTRL_E2E_ISOLATED": "1"},
        "read_roots": [],
        "test_count_regex": r"(\d+) passed",
        "min_tests": 1,
        "reject_regex": r"(?m)^\s*\d+\s+(?:skipped|flaky|failed)\b",
    }
    sources = {
        ".gitignore": "dist/\n",
        "apps/web/e2e/playwright.config.ts": "Owned workspace and fake action dispatch\n",
        "apps/api/src/main.ts": "Owned API fixture entry\n",
        "apps/api/src/e2e-dispatch.ts": "Deterministic fake action dispatch\n",
        "apps/api/test/qa-seed.ts": "Owned synthetic data and fake transport\n",
        **{
            "apps/web/e2e/fixtures/" + name: "Reviewed owned synthetic fixture\n"
            for name in ("owned-workspace.cjs", "seed-workspace.cjs", "start-api.cjs")
        },
        "apps/web/e2e/fixtures/gate.py": GATE,
    }
    case = scope_case(
        service,
        monkeypatch,
        browser_qa=qa,
        extra_sources={**sources, **(extra_sources or {})},
        checks=checks,
    )
    raw = copy.deepcopy(case.raw)
    raw.update(status="pass", summary="Completed prior implementation", findings=[])
    rewrite_assessment(case, raw)
    observation = read_private(case.folder / "native-thread-observation.json")
    text = json.dumps({key: raw[key] for key in ("status", "summary", "findings")})
    observation["turns"][-1]["items"][0]["text"] = text
    observation["turns"][-1]["turn"]["items"][0]["text"] = text
    rewrite_observation(case, observation)
    _git(case.broker.checkout, "add", case.api)
    _git(case.broker.checkout, "commit", "-qm", "Publish browser fixture candidate")
    _git(case.broker.checkout, "push", "origin", "HEAD:refs/heads/" + case.spec["branch"])
    current = case.broker.candidate()
    case.publication.update(head=current["head"], candidate=current)
    case.state.update(candidate=current, error="repair limit exhausted")
    folder = Path(case.spec["state_dir"]) / "browser-qa/3"
    native_folder = folder / "native"
    private_directory(native_folder)
    report = folder / "artifacts/0/junit.xml"
    private_directory(report.parent)
    report.write_text(junit())
    report.chmod(0o600)
    log = folder / "browser-qa.log"
    log.write_text("1 skipped\n1 passed\n")
    log.chmod(0o600)
    effective = [*qa["argv"], "--reporter=line,junit", "--output=owned-report"]
    launch = {
        "argv": effective,
        "environment": {**qa["env"], **{key: str(value) for key, value in qa["ports"].items()}},
    }
    process = {
        "journal": str(native_folder / "native-process.json"),
        "state": "finished",
        "exit_code": 0,
        "cancelled": False,
        "timed_out": False,
        "cleanup": "observed-native-confirmed",
        "monitoring_complete": True,
        "stdio_drained": True,
    }
    journal = {
        "intent": {
            "run_id": case.spec["run_id"],
            "policy_digest": case.spec["policy_digest"],
            "argv": effective,
            "environment_sha256": digest(launch["environment"]),
            "cwd": case.spec["checkout"],
            "timeout": qa["timeout_seconds"],
            "ports": list(qa["ports"].values()),
        },
        "owned": {"17027": {"identity": "opaque-browser"}},
        "monitor": {"pid": 17029, "identity": "opaque-browser-monitor"},
        "ports": list(qa["ports"].values()),
        "phase": "finished",
        "monitoring_complete": True,
        "result": process,
    }
    write_private(native_folder / "native-process.json", journal)
    write_private(native_folder / "launch.json", launch)
    for name in ("native-process.lock", "native-monitor.lock"):
        (native_folder / name).touch(mode=0o600)
    body = {
        "iteration": 3,
        "candidate_id": current["id"],
        "policy_digest": case.spec["policy_digest"],
        "qa_config_sha256": digest(qa),
        "ports": qa["ports"],
        "argv": qa["argv"],
        "effective_argv": effective,
        "state": "failed",
        "cleanup": "confirmed",
        "process_cleanup": "observed-native-confirmed",
        "source_unchanged": True,
        "rejected_output": True,
        "test_count": 1,
        "native_process": process,
        "cwd": case.spec["checkout"],
        "log": str(log),
        "log_sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
        "artifacts": [
            {
                "path": str(report),
                "sha256": hashlib.sha256(report.read_bytes()).hexdigest(),
                "size": report.stat().st_size,
            }
        ],
    }
    receipt = folder / "receipt.json"
    write_private(receipt, body)
    observed = {
        **body,
        "receipt": str(receipt),
        "receipt_sha256": hashlib.sha256(receipt.read_bytes()).hexdigest(),
    }
    effect_request = {
        key: body[key]
        for key in (
            "iteration",
            "candidate_id",
            "policy_digest",
            "qa_config_sha256",
            "ports",
            "argv",
        )
    }
    key = "browser_qa:run-1:3"
    assert case.broker._effect(key, "browser_qa", effect_request) is None
    case.broker._finish_effect(key, observed)
    case.state["checks"]["browser_qa"] = {
        **body,
        "receipt": str(receipt),
        "receipt_sha256": hashlib.sha256(receipt.read_bytes()).hexdigest(),
    }
    resources = RunResources(case.spec)
    resources.process(native_folder / "native-process.json")
    old_reports = []
    for iteration in range(3):
        previous = folder.parent / str(iteration)
        private_directory(previous / "native")
        previous_report = previous / "artifacts/0/junit.xml"
        private_directory(previous_report.parent)
        previous_report.write_bytes(report.read_bytes())
        previous_report.chmod(0o600)
        previous_log = previous / "browser-qa.log"
        previous_log.write_bytes(log.read_bytes())
        previous_log.chmod(0o600)
        previous_process = {**process, "journal": str(previous / "native/native-process.json")}
        previous_journal = {**journal, "result": previous_process}
        write_private(previous / "native/native-process.json", previous_journal)
        write_private(previous / "native/launch.json", launch)
        for name in ("native-process.lock", "native-monitor.lock"):
            (previous / "native" / name).touch(mode=0o600)
        previous_body = {
            **body,
            "iteration": iteration,
            "log": str(previous_log),
            "native_process": previous_process,
            "artifacts": [{**body["artifacts"][0], "path": str(previous_report)}],
        }
        previous_receipt = previous / "receipt.json"
        write_private(previous_receipt, previous_body)
        previous_observed = {
            **previous_body,
            "receipt": str(previous_receipt),
            "receipt_sha256": hashlib.sha256(previous_receipt.read_bytes()).hexdigest(),
        }
        previous_request = {**effect_request, "iteration": iteration}
        key = f"browser_qa:run-1:{iteration}"
        assert case.broker._effect(key, "browser_qa", previous_request) is None
        case.broker._finish_effect(key, previous_observed)
        resources.process(previous / "native/native-process.json")
        old_reports.append(previous_report)
    final = resources.finalize("blocked")
    assert final["state"] == "confirmed"
    case.state["checks"]["resource_cleanup"] = final
    case.store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        outcome="blocked",
        event_type="blocked",
        message=case.state["error"],
        error=case.state["error"],
        candidate=current,
        pull_request=case.publication,
        checks=case.state["checks"],
        iteration=3,
        protocol_revision=40,
        tracker=case.state["tracker"],
        cleanup="confirmed",
    )
    config = copy.deepcopy(case.store.config.raw)
    replacement = copy.deepcopy(qa)
    replacement["env"]["JOBCTRL_E2E_ISOLATED"] = "0"
    config["repositories"]["fixture"]["browser_qa"] = replacement
    config_path = case.store.config.state_root / "browser-mode-amended.json"
    recipe_path = case.store.config.state_root / "browser-mode-recipe.json"
    write_private(config_path, config)
    write_private(recipe_path, replacement)
    case.command = {
        key: value for key, value in case.command.items() if key not in resume.SCOPE_FIELDS
    }
    case.command.update(
        command_id="browser-mode-iteration4",
        expected_candidate_id=current["id"],
        expected_candidate_head=current["head"],
        browser_qa_amendment={
            "expected_recipe_sha256": digest(qa),
            "recipe_path": str(recipe_path),
            "recipe_sha256": hashlib.sha256(recipe_path.read_bytes()).hexdigest(),
            "amended_config_path": str(config_path),
            "amended_config_sha256": hashlib.sha256(config_path.read_bytes()).hexdigest(),
        },
    )
    # Actual preparation owner with an already verified canonical proof cache;
    # no equality/source/resource/custody or preparation function is stubbed.
    monkeypatch.setattr(resume, "prepare_runtime", prepare_runtime)
    case.qa, case.old_browser, case.old_report = qa, receipt, report
    case.historical_reports = old_reports
    return case


@pytest.fixture
def stopped_browser(service, monkeypatch):
    return browser_case(service, monkeypatch)


def admitted(case):
    response = case.store.continue_repair("run-1", case.command)
    current = case.store.effective_spec("run-1")
    with case.store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
    return response, current, recovery


def rewrite_browser(case, body):
    write_private(case.old_browser, body)
    case.state["checks"]["browser_qa"] = {
        **body,
        "receipt": str(case.old_browser),
        "receipt_sha256": hashlib.sha256(case.old_browser.read_bytes()).hexdigest(),
    }
    with case.store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET checks_json=?", (canonical_json(case.state["checks"]),)
        )
        db.execute(
            "UPDATE delivery_effects SET observed_json=? WHERE effect_key='browser_qa:run-1:3'",
            (canonical_json(case.state["checks"]["browser_qa"]),),
        )


def rewrite_proposal(case, recipe, *, config_extra=None):
    delta = case.command[mode.FIELD]
    path = Path(delta["amended_config_path"])
    config = read_private(path)
    config["repositories"]["fixture"]["browser_qa"] = recipe
    if config_extra:
        config_extra(config)
    write_private(path, config)
    recipe_path = Path(delta["recipe_path"])
    write_private(recipe_path, recipe)
    delta.update(
        recipe_sha256=hashlib.sha256(recipe_path.read_bytes()).hexdigest(),
        amended_config_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
    )


@pytest.mark.parametrize(
    "defect",
    [
        "extra_env",
        "argv",
        "port",
        "timeout",
        "minimum",
        "reject",
        "artifacts",
        "mode",
        "model",
        "scope",
        "transport",
        "recipe_hash",
        "old_recipe_hash",
        "public_recipe",
        "missing_old_hash",
        "junit_tamper",
        "old_receipt_tamper",
        "native_unknown",
        "old_launch_mode",
        "missing_case",
        "all_original_pass",
        "claim",
        "unknown_role",
        "pending_effect",
        "ceiling",
    ],
)
def test_public_browser_amendment_refuses_unrelated_or_unproven_authority(stopped_browser, defect):
    case = stopped_browser
    delta = case.command[mode.FIELD]
    recipe = read_private(Path(delta["recipe_path"]))
    if defect in {"extra_env", "argv", "port", "timeout", "minimum", "reject", "artifacts", "mode"}:
        if defect == "extra_env":
            recipe["env"]["OTHER_SETTING"] = "unapproved"
        elif defect == "argv":
            recipe["argv"].append("--list")
        elif defect == "port":
            recipe["ports"]["QA_API_PORT"] += 1
        elif defect == "timeout":
            recipe["timeout_seconds"] += 1
        elif defect == "minimum":
            recipe["min_tests"] = 0
        elif defect == "reject":
            recipe["reject_regex"] = ""
        elif defect == "artifacts":
            recipe["artifact_paths"] = []
        else:
            recipe["env"]["JOBCTRL_E2E_ISOLATED"] = "1"
        rewrite_proposal(case, recipe)
    elif defect in {"model", "scope", "transport"}:

        def unrelated(config):
            if defect == "model":
                config["roles"]["verify"]["model"] = "unapproved"
            elif defect == "scope":
                config["repositories"]["fixture"]["source_scope"]["allowed_roots"].append(
                    "apps/web"
                )
            else:
                config["queue"] = "foreign-queue"

        rewrite_proposal(case, recipe, config_extra=unrelated)
    elif defect == "recipe_hash":
        delta["recipe_sha256"] = "0" * 64
    elif defect == "old_recipe_hash":
        delta["expected_recipe_sha256"] = "0" * 64
    elif defect == "public_recipe":
        Path(delta["recipe_path"]).chmod(0o644)
    elif defect == "missing_old_hash":
        del case.state["checks"]["browser_qa"]["receipt_sha256"]
        with case.store._connect() as db:
            db.execute(
                "UPDATE delivery_runs SET checks_json=?", (canonical_json(case.state["checks"]),)
            )
    elif defect == "junit_tamper":
        case.old_report.write_text(junit(skipped=False))
    elif defect == "old_receipt_tamper":
        case.old_browser.write_bytes(case.old_browser.read_bytes() + b"\n")
    elif defect in {"native_unknown", "old_launch_mode"}:
        folder = case.old_browser.parent / "native"
        path = folder / ("native-process.json" if defect == "native_unknown" else "launch.json")
        value = read_private(path)
        if defect == "native_unknown":
            value["monitoring_complete"] = False
        else:
            value["environment"]["JOBCTRL_E2E_ISOLATED"] = "0"
            journal = read_private(folder / "native-process.json")
            journal["intent"]["environment_sha256"] = digest(value["environment"])
            write_private(folder / "native-process.json", journal)
        write_private(path, value)
    elif defect in {"missing_case", "all_original_pass"}:
        case.old_report.write_text(
            junit(skipped=defect != "all_original_pass", missing=defect == "missing_case")
        )
        body = read_private(case.old_browser)
        body["artifacts"][0].update(
            size=case.old_report.stat().st_size,
            sha256=hashlib.sha256(case.old_report.read_bytes()).hexdigest(),
        )
        rewrite_browser(case, body)
    elif defect == "claim":
        with case.store._connect() as db:
            case.store.state.claim_work(db, case.spec["work_id"], "external:other", "fixture")
    elif defect == "unknown_role":
        case.state["roles"].append(
            {
                "role": "verify",
                "iteration": 3,
                "status": "recovery_unknown",
                "cleanup": "unknown",
                "finish_reason": "recovery_unknown",
            }
        )
    elif defect == "pending_effect":
        with case.store._connect() as db:
            db.execute(
                "INSERT INTO delivery_effects (effect_key,run_id,kind,state,request_json,"
                "updated_at) VALUES ('unknown','run-1','publish','pending','{}','')"
            )
    else:
        case.command["expected_iteration"] = 10
    with pytest.raises(ValueError):
        case.store.repair_admission_preflight("run-1", case.command)
    with case.store._connect() as db:
        assert not db.execute(
            "SELECT 1 FROM delivery_commands WHERE command_id=?",
            (case.command["command_id"],),
        ).fetchone()


def owned_ports():
    guards = [socket.socket(), socket.socket()]
    try:
        for guard in guards:
            guard.bind(("127.0.0.1", 0))
        return [guard.getsockname()[1] for guard in guards]
    finally:
        for guard in guards:
            guard.close()


def restore_native_observers(monkeypatch):
    monkeypatch.setattr(native, "process_table", REAL_TABLE)
    monkeypatch.setattr(native, "listeners", REAL_LISTENERS)
    monkeypatch.setattr(policy, "process_table", REAL_TABLE)
    monkeypatch.setattr(policy, "listeners", REAL_LISTENERS)


async def browser_timeout_diagnostics(handle, store, evidence_dir):
    """Inspect only bounded lifecycle facts, never inputs, logs or environment."""
    import os
    import sqlite3
    import stat
    from datetime import timedelta

    diagnostics = {"history": [], "history_truncated": False}
    try:
        async with asyncio.timeout(5):
            history = handle.fetch_history_events(page_size=100, rpc_timeout=timedelta(seconds=2))
            total = 0
            for _ in range(5):
                await history.fetch_next_page()
                page = history.current_page or []
                total += sum(event.ByteSize() for event in page)
                if total > 8 * 1024 * 1024:
                    diagnostics["history_truncated"] = True
                    break
                for event in page:
                    name = event.WhichOneof("attributes") or "unknown"
                    entry = {"id": event.event_id, "type": name,
                             "time": event.event_time.ToDatetime().isoformat()}
                    if name == "activity_task_scheduled_event_attributes":
                        attrs = event.activity_task_scheduled_event_attributes
                        activity_name = attrs.activity_type.name
                        if (activity_name.startswith("delivery_")
                                and activity_name.replace("_", "").isalnum()):
                            entry["activity"] = activity_name[:100]
                    elif name in {
                        "activity_task_started_event_attributes",
                        "activity_task_completed_event_attributes",
                        "activity_task_failed_event_attributes",
                        "activity_task_timed_out_event_attributes",
                    }:
                        entry["scheduled_id"] = getattr(event, name).scheduled_event_id
                    diagnostics["history"].append(entry)
                    diagnostics["history"] = diagnostics["history"][-20:]
                if history.next_page_token is None:
                    break
            else:
                diagnostics["history_truncated"] = True
    except Exception as exc:
        diagnostics["history_error"] = type(exc).__name__

    try:
        uri = store.config.tracking_db.as_uri() + "?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=1) as db:
            row = db.execute(
                "SELECT phase,execution_state,iteration,outcome,cleanup FROM delivery_runs "
                "WHERE run_id=?", ("run-1",)
            ).fetchone()
        allowed = {"queued", "running", "blocked", "cancelled", "delivered", "terminal",
                   "confirmed", "unknown", "none", "cancelling", "finalizing",
                   "preparing", "repair_preflight", "tracker_start", "implement", "repair",
                   "prepublish_checks", "publishing", "checks", "browser_qa", "review",
                   "verify", "waiting_ci", "tracker", "waiting_tracker", "merging"}
        if row:
            diagnostics["projection"] = {
                key: (value if value in allowed else None)
                for key, value in zip(("phase", "execution_state", "outcome", "cleanup"),
                                      (row[0], row[1], row[3], row[4]), strict=True)
            }
            diagnostics["projection"]["iteration"] = row[2] if type(row[2]) is int else None
    except Exception as exc:
        diagnostics["projection_error"] = type(exc).__name__

    path = Path(evidence_dir) / "browser-qa/4/native/native-process.json"
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
                    or info.st_size > 1024 * 1024):
                raise ValueError("unsafe native journal")
            with os.fdopen(fd, "rb", closefd=False) as source:
                journal = json.loads(source.read(1024 * 1024 + 1))
        finally:
            os.close(fd)
        result = journal.get("result") or {}
        diagnostics["browser_native"] = {
            "phase": journal.get("phase") if journal.get("phase") in {
                "allocated", "authorized", "finished"} else None,
            "monitoring_complete": journal.get("monitoring_complete") is True,
            "cleanup": result.get("cleanup") if result.get("cleanup") in {
                "observed-native-confirmed", "unknown"} else None,
            "timed_out": result.get("timed_out") is True,
        }
    except FileNotFoundError:
        diagnostics["browser_native"] = "absent"
    except Exception as exc:
        diagnostics["browser_native_error"] = type(exc).__name__
    return diagnostics


@pytest.mark.asyncio
async def test_public_browser_amendment_real_preparation_worker_native_gate_and_replay(
    service, monkeypatch
):
    from temporal_test_server import local_temporal
    from temporalio import activity
    from temporalio.worker import Replayer, Worker

    from devflow_temporal.delivery_activities import (
        delivery_browser_qa,
        delivery_finalize_resources,
        delivery_project,
    )
    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    async with local_temporal() as environment:
        initial, request = service
        config = copy.deepcopy(initial.config.raw)
        config["temporal_address"] = environment.client.service_client.config.target_host
        initial.config.path.write_text(json.dumps(config))
        case = browser_case(
            (type(initial)(DeliveryConfig.load(initial.config.path)), request),
            monkeypatch,
            ports=owned_ports(),
        )
        store = case.store
        assert (
            store.repair_admission_preflight("run-1", case.command)["authorized_through_iteration"]
            == 4
        )
        response, current, recovery = admitted(case)
        restore_native_observers(monkeypatch)
        evidence_dir = DeliveryBroker(store, current).evidence_dir
        before = copy.deepcopy(recovery["state"])
        store.mark_start("run-1", accepted=True)
        calls = []

        @activity.defn(name="delivery_role")
        async def role(body):
            calls.append(body["role"])
            assert body["iteration"] == 4
            if body["role"] == "implement":
                assert body["resume_session"] == recovery["session_id"]
                assert body["spec"]["policy"]["browser_qa"]["env"]["JOBCTRL_E2E_ISOLATED"] == "0"
            else:
                assert body["role"] == "review" or body.get("qa_evidence")
            return {
                "role": body["role"],
                "iteration": 4,
                "status": "pass",
                "cleanup": "confirmed",
                "findings": [],
                "session_id": recovery["session_id"]
                if body["role"] == "implement"
                else body["role"],
                "candidate": DeliveryBroker(store, body["spec"]).candidate(),
            }

        @activity.defn(name="delivery_tracker_start")
        async def tracker_start(_body):
            return {"state": "consistent"}

        @activity.defn(name="delivery_precheck")
        async def precheck(body):
            calls.append("precheck")
            return {
                "state": "passed",
                "candidate_id": body["candidate"]["id"],
                "cleanup": "confirmed",
            }

        @activity.defn(name="delivery_publish")
        async def publish(body):
            calls.append("publish")
            return {**case.publication, "candidate": body["candidate"]}

        @activity.defn(name="delivery_checks")
        async def checks(body):
            calls.append("checks")
            if getattr(case, "actual_checks", None):
                return await case.actual_checks(body)
            return {
                "state": "passed",
                "candidate_id": body["candidate"]["id"],
                "cleanup": "confirmed",
            }

        @activity.defn(name="delivery_ci")
        async def ci(body):
            calls.append("ci")
            return {"state": "passed", "head": body["pull_request"]["head"]}

        @activity.defn(name="delivery_terminal_tracker")
        async def terminal_tracker(body):
            calls.append("terminal_tracker")
            if body["release"]:
                with store._connect() as db:
                    store.state.release_work(db, current["work_id"], "external:devflow:run-1")
            return {"state": "consistent"}

        async with Worker(
            environment.client,
            task_queue="browser-mode-recovery",
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
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run,
                args=[current, recovery],
                id=response["workflow_id"],
                task_queue="browser-mode-recovery",
            )
            try:
                result = await asyncio.wait_for(
                    handle.result(), getattr(case, "owner_test_timeout", 45)
                )
            except TimeoutError as exc:
                diagnostics = await browser_timeout_diagnostics(handle, store, evidence_dir)
                raise AssertionError(
                    "Browser amendment workflow timed out; bounded lifecycle facts: "
                    + json.dumps(diagnostics, sort_keys=True)
                ) from exc
            assert result["outcome"] == "delivered", result.get("error")
            assert result["iteration"] == 4 and result["roles"][0] == before["roles"][0]
            assert result["checks"]["browser_qa"]["test_count"] == 2
            assert [
                item["state"] for item in result["checks"]["browser_qa"][mode.FIELD]["cases"]
            ] == ["passed", "passed"]
            assert calls == [
                "implement",
                "precheck",
                "publish",
                "checks",
                "review",
                "verify",
                "ci",
                "terminal_tracker",
            ]
            assert recovery["state"]["checks"]["browser_qa"]["state"] == "failed"
            assert result["checks"]["resource_cleanup"]["state"] == "confirmed"
            await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
                await handle.fetch_history()
            )
            # Delivered cleanup can retire the candidate; historical authority
            # remains readable without claiming that absent source is launchable.
            assert not Path(current["checkout"]).exists()
            assert store.effective_spec("run-1") == current
            with pytest.raises(ValueError):
                mode.launch_authority(current, Path(current["checkout"]))
