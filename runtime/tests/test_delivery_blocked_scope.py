"""Native completed blocked assessments retain their verdict across explicit scope grants."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import subprocess
import sys
from importlib.metadata import distribution
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_native_preparation as preparation
from devflow_temporal import delivery_native_process as native
from devflow_temporal import delivery_policy_recovery as policy
from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_activities import delivery_repair_preflight
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_native_threads import NativeThreadObservation
from devflow_temporal.delivery_resources import (
    RunResources,
    private_directory,
    read_private,
    write_private,
)
from devflow_temporal.delivery_role_evidence import allocate, seal
from devflow_temporal.delivery_sandbox import _native_role_home
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import _initial_controller_state
from devflow_temporal.supervisor import DeliverySupervisor

ADDED = [
    "apps/web/src/contexts/apply/hooks/useApplyReviewSession.test.ts",
    "apps/web/src/contexts/apply/hooks/useApplyReviewSession.ts",
]
SESSION = "00000000-0000-4000-8000-000000000168"


def prepared_spec(spec):
    # Measurement execution is the synthetic external boundary. Canonical proof
    # bytes, host identity, policy binding and every verification reader are real.
    identity = preparation.native_identity(spec)
    root = Path(spec["state_dir"])
    measured = root / "native-preparation/fixture"
    private_directory(measured)
    observed = measured / "observed.json"
    write_private(
        observed,
        {"mode": "trusted-local", "host_read": True, "owned_tmp_write": True, "managed_depth": "1"},
    )
    paths = {"observed": observed}
    for key, value in {
        "path_control": "codex-cli " + identity["runtime_dependencies"]["codex_cli_version"],
        "log": "Owned synthetic external measurement\n",
    }.items():
        path = measured / (key + ".log")
        path.write_text(value)
        path.chmod(0o600)
        paths[key] = path
    proof = {
        "schema": preparation.SCHEMA,
        "identity": identity,
        "fingerprint": digest(identity),
        "measurement": {
            "process_cleanup": "observed-native-confirmed",
            "exit_code": 0,
            **{
                key: {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                for key, path in paths.items()
            },
        },
    }
    cached = root.parents[1] / "preparation-native" / digest(identity) / "proof.json"
    write_private(cached, proof)
    return preparation.bind_native_spec(spec, identity, cached, reused=True)


def scope_case(
    service, monkeypatch, *, browser_qa=None, extra_sources=None, checks=None, named_base=False,
    prepublish_checks=None, required_ci=None,
):
    owner, submitted = service
    source = owner.config.path.parent / "source"
    api = "apps/api/src/approval.ts"
    for name in [api, *ADDED]:
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("Original tracked source\n")
    for name, content in (extra_sources or {}).items():
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    _git(source, "add", ".")
    _git(source, "commit", "-qm", "Track caller and API fixtures")
    if named_base:
        _git(source, "branch", "-M", "main")
        _git(source, "push", "origin", "main")
        submitted = {**submitted, "base_ref": "refs/remotes/origin/main"}
    config = copy.deepcopy(owner.config.raw)
    config.update(
        provider="codex",
        execution_mode="trusted-local",
        max_repairs=10,
        codex_bin=str(distribution("openai-codex-cli-bin").locate_file("codex_cli_bin/bin/codex")),
    )
    repository = config["repositories"]["fixture"]
    if named_base:
        repository["base_ref"] = submitted["base_ref"]
    repository.update(
        expected_base_sha=_git(source, "rev-parse", "HEAD"),
        allowed_paths=[],
        checks=[{"id": "local", "argv": ["/usr/bin/true"]}],
        prepublish_checks=(prepublish_checks if prepublish_checks is not None else
                           [{"id": "prepublish", "argv": ["/usr/bin/true"]}]),
        required_ci=required_ci if required_ci is not None else ["ci"],
        project_url="https://github.com/users/example/projects/1",
        assignee="example",
        source_scope={
            "version": 1,
            "allowed_roots": ["apps/api/src"],
            "allowed_files": [],
            "protected_paths": [".git", ".github", ".agents", ".codex"],
        },
    )
    if checks is not None:
        repository["checks"] = checks
    if browser_qa is not None:
        repository["browser_qa"] = browser_qa
    owner.config.path.write_text(json.dumps(config))
    store = DeliveryStore(DeliveryConfig.load(owner.config.path))
    store.submit(submitted)
    store.mark_start("run-1", accepted=True)
    spec = store.effective_spec("run-1")
    DeliveryBroker(store, spec).prepare()
    spec.update(provider="codex", retry_budget_version=1)
    spec["policy"].update(
        execution_backend="native-macos", host_sandbox="trusted-local", max_repairs=10
    )
    spec["policy_digest"] = digest(spec["policy"])
    spec = prepared_spec(spec)
    broker = DeliveryBroker(store, spec)
    (broker.checkout / api).write_text("Previously published implementation\n")
    _git(broker.checkout, "add", api)
    _git(broker.checkout, "commit", "-qm", "Publish prior implementation fixture")
    _git(broker.checkout, "push", "origin", "HEAD:refs/heads/" + spec["branch"])
    before = broker.candidate()
    (broker.checkout / api).write_text("Retained uncommitted API repair\n")
    partial = broker.candidate()
    publication = {
        "number": 12,
        "head": before["head"],
        "state": "OPEN",
        "base": spec["base_sha"],
        "url": "https://github.com/example/fixture/pull/12",
        "candidate": before,
    }
    request = {
        "spec": spec,
        "role": "implement",
        "iteration": 3,
        "candidate": before,
        "workspace": spec["checkout"],
        "resume_session": SESSION,
        "native_authorized": True,
    }
    key = DeliverySupervisor._job_key(request)
    folder = Path(spec["state_dir"]) / "attempts" / key
    private_directory(folder)
    request.update(result_path=str(folder / "result.json"), start_path=str(folder / "start.json"))
    request = allocate(request, key)
    Path(request["artifact_directory"], "handoff.md").write_text(
        "Caller update requires explicit scope\n"
    )
    assessment = {
        "status": "blocked",
        "summary": "API repair retained; caller edit authority is absent.",
        "findings": ["Caller and its focused test require the two omitted web files."],
    }
    message = {
        "id": "final-assessment",
        "type": "agentMessage",
        "phase": "final_answer",
        "text": json.dumps(assessment),
    }
    observation = {
        "schema": "devflow-native-kit-threads-v1",
        "run_id": "run-1",
        "role": "implement",
        "iteration": 3,
        "pid": 17017,
        "start_identity": "opaque-role",
        "resumed_from": SESSION,
        "state": "confirmed",
        "parent_thread_id": SESSION,
        "observation_source": "SDK typed TurnResult and isolated CODEX_HOME thread_list",
        "raw_turn_items": 1,
        "collaboration_items": [],
        "thread_inventory_before": [SESSION],
        "thread_inventory_after": [SESSION],
        "new_child_thread_ids": [],
        "turns": [
            {
                "thread_id": SESSION,
                "turn_id": "completed-turn",
                "items": [message],
                "turn": {
                    "id": "completed-turn",
                    "status": "completed",
                    "error": None,
                    "items": [message],
                },
            }
        ],
    }
    observation_path = folder / "native-thread-observation.json"
    write_private(observation_path, observation)
    reference = NativeThreadObservation.reference(
        SimpleNamespace(data=observation, path=observation_path, max_attempts=1)
    )
    raw = {
        **assessment,
        "finish_reason": "done",
        "session_id": SESSION,
        "usage": None,
        "native_thread_observation": reference,
    }
    home = _native_role_home(request)
    private_directory(home / "codex/sessions")
    (home / "codex/sessions" / (SESSION + ".jsonl")).write_text("Original session state\n")
    launch = {"argv": ["opaque-native-role"], "environment": {"CODEX_HOME": str(home / "codex")}}
    outcome = {
        "journal": str(folder / "native-process.json"),
        "state": "finished",
        "exit_code": 0,
        "cleanup": "observed-native-confirmed",
        "monitoring_complete": True,
        "stdio_drained": True,
        "cancelled": False,
        "timed_out": False,
    }
    result = {
        **raw,
        **seal(request),
        "cleanup": "confirmed",
        "process_cleanup": "observed-native-confirmed",
        "resource_cleanup": "pending_workflow_finalization",
        "native_process": outcome,
    }
    journal = {
        "intent": {
            "run_id": "run-1",
            "policy_digest": spec["policy_digest"],
            "argv": launch["argv"],
            "environment_sha256": digest(launch["environment"]),
            "cwd": spec["checkout"],
            "timeout": 10,
            "ports": [],
        },
        "phase": "finished",
        "ports": [],
        "monitoring_complete": True,
        # NativeProcess finishes with the monitor in its complete owned set;
        # finalization receipts and the retained observer bind that same set.
        "owned": {
            "17017": {"identity": "opaque-role"},
            "17019": {"pid": 17019, "identity": "opaque-monitor"},
        },
        "monitor": {"pid": 17019, "identity": "opaque-monitor"},
        "result": outcome,
        "provider_session": {
            "role": "implement",
            "iteration": 3,
            "session_id": SESSION,
            "resumed_from": SESSION,
            "result_digest": digest(result),
            "output_candidate": {k: partial[k] for k in ("id", "head", "content_sha256")},
        },
    }
    (folder / "process.log").touch(mode=0o600)
    write_private(folder / "result.json", raw)
    write_private(folder / "request.json", request)
    write_private(folder / "start.json", {"pid": 17017, "started_at": "fixture"})
    write_private(folder / "launch.json", launch)
    write_private(folder / "native-process.json", journal)
    for name in ("native-process.lock", "native-monitor.lock"):
        folder.joinpath(name).touch(mode=0o600)
    monkeypatch.setattr(native, "process_table", lambda: {})
    monkeypatch.setattr(native, "listeners", lambda _: set())
    monkeypatch.setattr(policy, "process_table", lambda: {})
    monkeypatch.setattr(policy, "listeners", lambda _: set())
    registry = RunResources(spec)
    registry.process(folder / "native-process.json")
    receipt = registry.finalize("blocked")
    assert receipt["state"] == "confirmed"
    role = {
        **result,
        "role": "implement",
        "iteration": 3,
        "input_candidate_id": before["id"],
        "candidate": partial,
    }
    state = {
        **_initial_controller_state(spec),
        "run_id": "run-1",
        "phase": "blocked",
        "execution_state": "blocked",
        "outcome": "blocked",
        "revision": 40,
        "iteration": 3,
        "candidate": before,
        "pull_request": publication,
        "roles": [role],
        "checks": {"resource_cleanup": receipt},
        "cleanup": "confirmed",
        "usage": {},
        "error": "implementer did not establish a pass",
        "tracker": {"state": "consistent"},
    }
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET request_json=?,accepted_plan_text=?,workflow_id=?",
            (canonical_json(spec), spec["accepted_plan"], "delivery-run-1"),
        )
        db.execute(
            "INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,"
            "state,session_id,result_json,cleanup,pid,process_identity,result_path) VALUES "
            "(?,'run-1','implement',3,?,'finished',?,?,'confirmed',17017,'opaque-role',?)",
            (key, before["id"], SESSION, canonical_json(result), str(folder / "result.json")),
        )
        store.state.release_work(db, spec["work_id"], "external:devflow:run-1")
    store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        outcome="blocked",
        event_type="blocked",
        message=state["error"],
        candidate=before,
        pull_request=publication,
        checks=state["checks"],
        iteration=3,
        protocol_revision=40,
        error=state["error"],
        tracker=state["tracker"],
        cleanup="confirmed",
    )
    closed = {
        "workflow_id": "delivery-run-1",
        "execution_run_id": "closed-implementation-3",
        "closed_at": "2026-10-24T16:04:46+00:00",
        "request_digest": spec["request_digest"],
        "recovery_digest": None,
        "result": state,
    }
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_a, **_k: closed)
    monkeypatch.setattr(
        DeliveryBroker,
        "_existing_pr",
        lambda *_: {"number": 12, "headRefOid": publication["head"], "url": publication["url"]},
    )
    monkeypatch.setattr(resume, "prepare_runtime", lambda value, *_: copy.deepcopy(value))
    amended = copy.deepcopy(store.config.raw)
    amended["repositories"]["fixture"]["source_scope"]["allowed_files"] += ADDED
    path = store.config.state_root / "explicit-caller-scope.json"
    write_private(path, amended)
    command = {
        "continuation_kind": resume.KIND,
        "command_id": "scope-blocked-iteration4",
        "expected_revision": 40,
        "expected_iteration": 3,
        "expected_candidate_id": partial["id"],
        "expected_candidate_head": partial["head"],
        "additional_iterations": 1,
        "added_paths": ADDED,
        "amended_config_path": str(path),
        "amended_config_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    return SimpleNamespace(
        store=store,
        spec=spec,
        broker=broker,
        state=state,
        raw=raw,
        result=result,
        request=request,
        journal=journal,
        folder=folder,
        command=command,
        home=home,
        publication=publication,
        partial=partial,
        input=before,
        api=api,
    )


@pytest.fixture
def blocked_scope(service, monkeypatch):
    return scope_case(service, monkeypatch)


def test_native_blocked_scope_public_admission_and_real_readback_preserve_assessment(blocked_scope):
    case = blocked_scope
    original = copy.deepcopy(case.state)
    assert (case.folder / "process.log").stat().st_size == 0
    before = {p: p.read_bytes() for p in case.folder.iterdir() if p.is_file()}
    result = case.store.repair_admission_preflight("run-1", case.command)
    assert result["authorized_through_iteration"] == 4
    assert case.state == original
    response = case.store.continue_repair("run-1", case.command)
    assert case.store.continue_repair("run-1", case.command) == response
    current = case.store.effective_spec("run-1")
    with case.store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
        assert not db.execute("SELECT 1 FROM delivery_repair_grants").fetchone()
        assert db.execute("SELECT COUNT(*) FROM delivery_runs").fetchone()[0] == 1
    assert recovery["state"] == original and recovery["state"]["roles"][-1]["status"] == "blocked"
    assert recovery["blocked_scope_finding"]["input_candidate_id"] == case.input["id"]
    assert recovery["blocked_scope_finding"]["output_candidate"] == case.partial
    assert case.input["id"] != case.partial["id"]
    assert current["policy"]["source_scope"]["allowed_roots"] == ["apps/api/src"]
    assert current["policy"]["source_scope"]["allowed_files"] == ADDED
    assert current["policy"]["max_repairs"] == 10
    assert recovery["session_id"] == SESSION
    assert asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery})) == {
        "state": "confirmed"
    }
    assert {p: p.read_bytes() for p in before} == before


def admitted(case):
    response = case.store.continue_repair("run-1", case.command)
    current = case.store.effective_spec("run-1")
    with case.store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
    return response, current, recovery


def rewrite_assessment(case, raw):
    """Construct a coherent alternative native producer, not just a stale checksum."""
    case.raw = copy.deepcopy(raw)
    enrichment = {
        key: copy.deepcopy(case.result[key])
        for key in (
            "cleanup",
            "process_cleanup",
            "resource_cleanup",
            "native_process",
            "role_artifacts",
        )
    }
    case.result = {**case.raw, **enrichment}
    case.state["roles"][-1] = {
        **case.result,
        "role": "implement",
        "iteration": 3,
        "candidate": case.partial,
        "input_candidate_id": case.input["id"],
    }
    case.journal["provider_session"]["result_digest"] = digest(case.result)
    write_private(case.folder / "result.json", case.raw)
    write_private(case.folder / "native-process.json", case.journal)
    with case.store._connect() as db:
        db.execute(
            "UPDATE delivery_attempts SET result_json=?,session_id=?",
            (canonical_json(case.result), case.result.get("session_id")),
        )


def rewrite_observation(case, observation):
    path = case.folder / "native-thread-observation.json"
    write_private(path, observation)
    raw = copy.deepcopy(case.raw)
    raw["native_thread_observation"] = NativeThreadObservation.reference(
        SimpleNamespace(data=observation, path=path, max_attempts=1)
    )
    rewrite_assessment(case, raw)


@pytest.mark.parametrize(
    "defect",
    [
        "missing_result",
        "public_result",
        "symlink_request",
        "oversized_request",
        "result_path",
        "start_path",
        "request_input",
        "request_input_bytes",
        "request_spec",
        "output_input",
        "metadata_digest",
        "home",
        "raw_pass",
        "raw_findings",
        "missing_session",
        "interrupted",
        "empty_findings",
        "unknown",
        "typed_pass",
        "typed_nonfinal",
        "typed_incomplete",
        "typed_error",
        "new_child",
        "prelaunch",
    ],
)
def test_blocked_scope_public_preflight_refuses_unproven_native_assessment(blocked_scope, defect):
    case = blocked_scope
    if defect == "missing_result":
        (case.folder / "result.json").unlink()
    elif defect == "public_result":
        (case.folder / "result.json").chmod(0o644)
    elif defect == "symlink_request":
        path = case.folder / "request.json"
        copy_path = case.folder / "request-copy.json"
        path.rename(copy_path)
        path.symlink_to(copy_path)
    elif defect == "oversized_request":
        (case.folder / "request.json").write_bytes(b" " * (4 * 1024 * 1024 + 1))
    elif defect in {
        "result_path",
        "start_path",
        "request_input",
        "request_input_bytes",
        "request_spec",
    }:
        request = copy.deepcopy(case.request)
        if defect in {"result_path", "start_path"}:
            request[defect] = str(case.folder / "foreign.json")
        elif defect == "request_input":
            request["candidate"] = case.partial
        elif defect == "request_input_bytes":
            request["candidate"]["content_sha256"] = digest("unbound input bytes")
        else:
            request["spec"]["policy"]["source_scope"]["allowed_files"] = ADDED
        write_private(case.folder / "request.json", request)
    elif defect in {"output_input", "metadata_digest", "prelaunch"}:
        journal = copy.deepcopy(case.journal)
        if defect == "output_input":
            journal["provider_session"]["output_candidate"] = {
                key: case.input[key] for key in ("id", "head", "content_sha256")
            }
        elif defect == "metadata_digest":
            journal["provider_session"]["result_digest"] = digest("foreign result")
        else:
            journal["phase"] = "authorized"
            journal["monitoring_complete"] = False
        write_private(case.folder / "native-process.json", journal)
    elif defect == "home":
        launch = read_private(case.folder / "launch.json")
        launch["environment"]["CODEX_HOME"] = str(case.home.parent / "foreign")
        case.journal["intent"]["environment_sha256"] = digest(launch["environment"])
        write_private(case.folder / "launch.json", launch)
        write_private(case.folder / "native-process.json", case.journal)
    elif defect in {"raw_pass", "raw_findings"}:
        # The closed and supervisor assessments still say blocked. A controller
        # rewrite cannot authenticate a different original provider verdict/text.
        raw = copy.deepcopy(case.raw)
        if defect == "raw_pass":
            raw["status"] = "pass"
        else:
            raw["findings"] = ["Different assessment from the provider"]
        write_private(case.folder / "result.json", raw)
    elif defect in {"missing_session", "interrupted", "empty_findings", "unknown"}:
        raw = copy.deepcopy(case.raw)
        if defect == "missing_session":
            raw["session_id"] = None
        elif defect == "interrupted":
            raw["finish_reason"] = "interrupted"
        elif defect == "empty_findings":
            raw["findings"] = []
        else:
            raw.update(status="recovery_unknown", finish_reason="recovery_unknown")
        rewrite_assessment(case, raw)
    else:
        observation = read_private(case.folder / "native-thread-observation.json")
        turn = observation["turns"][-1]
        if defect == "typed_pass":
            value = json.loads(turn["items"][0]["text"])
            value.update(status="pass", findings=[])
            turn["items"][0]["text"] = json.dumps(value)
        elif defect == "typed_nonfinal":
            turn["items"][0]["phase"] = "commentary"
        elif defect == "typed_incomplete":
            turn["turn"]["status"] = "interrupted"
        elif defect == "typed_error":
            turn["turn"]["error"] = {"message": "Uncertain completion"}
        else:
            observation["new_child_thread_ids"] = ["unowned-child"]
            observation["state"] = "blocked"
        # Refresh the reference, original raw assessment, enriched digest and
        # closed role together: this reaches the typed native completion owner.
        rewrite_observation(case, observation)
    with pytest.raises((ValueError, OSError)):
        case.store.repair_admission_preflight("run-1", case.command)
    with case.store._connect() as db:
        assert not db.execute(
            "SELECT 1 FROM delivery_commands WHERE command_id=?", (case.command["command_id"],)
        ).fetchone()
        assert case.store.state.claim_for(db, case.spec["work_id"]) is None
        assert db.execute("SELECT COUNT(*) FROM delivery_runs").fetchone()[0] == 1


@pytest.mark.parametrize(
    "name",
    [
        "request.json",
        "result.json",
        "native-process.json",
        "launch.json",
        "start.json",
        "native-thread-observation.json",
    ],
)
def test_admitted_worker_reauthenticates_every_original_native_byte(blocked_scope, name):
    case = blocked_scope
    _response, current, recovery = admitted(case)
    path = case.folder / name
    path.write_bytes(path.read_bytes() + b"\n")
    with pytest.raises(ValueError):
        asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery}))
    assert case.state["roles"][-1]["status"] == "blocked"
    with case.store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 1


@pytest.mark.parametrize("defect", ["source", "config", "added_file", "allowance"])
def test_public_scope_resume_preserves_source_config_file_and_finite_budget(blocked_scope, defect):
    case = blocked_scope
    if defect == "source":
        (case.broker.checkout / case.api).write_text("Changed after operator's candidate\n")
    elif defect == "config":
        path = Path(case.command["amended_config_path"])
        value = read_private(path)
        value["roles"]["implement"]["model"] = "foreign-model"
        write_private(path, value)
        case.command["amended_config_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    elif defect == "added_file":
        (case.broker.checkout / ADDED[0]).write_text(
            "Unreviewed caller edit before scope authority\n"
        )
    else:
        case.command["additional_iterations"] = 8
    with pytest.raises(ValueError):
        case.store.repair_admission_preflight("run-1", case.command)


def test_scope_admission_rechecks_native_bytes_after_runtime_preparation(
    blocked_scope, monkeypatch
):
    case = blocked_scope

    def prepare(spec, *_):
        path = case.folder / "start.json"
        path.write_bytes(path.read_bytes() + b"\n")
        return copy.deepcopy(spec)

    monkeypatch.setattr(resume, "prepare_runtime", prepare)
    with pytest.raises(ValueError, match="changed during runtime preparation"):
        case.store.continue_repair("run-1", case.command)
    with case.store._connect() as db:
        assert not db.execute(
            "SELECT 1 FROM delivery_commands WHERE command_id=?", (case.command["command_id"],)
        ).fetchone()
        assert case.store.state.claim_for(db, case.spec["work_id"]) is None


def test_scope_worker_cannot_drop_the_new_native_provenance(blocked_scope):
    case = blocked_scope
    response, current, recovery = admitted(case)
    del recovery["blocked_scope_finding"]
    write_private(
        Path(current["state_dir"]) / resume.namespace(recovery) / "admission.json", recovery
    )
    response["admission_sha256"] = digest(recovery)
    with case.store._connect() as db:
        db.execute("UPDATE delivery_runs SET recovery_json=?", (canonical_json(recovery),))
        db.execute(
            "UPDATE delivery_commands SET response_json=? WHERE command_id=?",
            (canonical_json(response), case.command["command_id"]),
        )
    with pytest.raises(ValueError, match="authenticated native receipt"):
        asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery}))


@pytest.mark.asyncio
async def test_public_native_blocked_scope_runs_worker_same_session_and_all_normal_gates(
    service, monkeypatch
):
    from temporal_test_server import local_temporal
    from temporalio import activity
    from temporalio.worker import Replayer, Worker

    from devflow_temporal.delivery_activities import delivery_finalize_resources, delivery_project
    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    async with local_temporal() as environment:
        initial, submitted = service
        config = copy.deepcopy(initial.config.raw)
        config["temporal_address"] = environment.client.service_client.config.target_host
        initial.config.path.write_text(json.dumps(config))
        store = DeliveryStore(DeliveryConfig.load(initial.config.path))
        case = scope_case((store, submitted), monkeypatch)
        # Real completed SDK streams can retain an earlier terminal-labelled
        # item. The final typed item is the producer's structured assessment.
        observation = read_private(case.folder / "native-thread-observation.json")
        final = observation["turns"][-1]["items"][-1]
        earlier = {**final, "id": "earlier-terminal-item", "text": "Earlier native observation"}
        observation["turns"][-1]["items"].insert(0, earlier)
        observation["turns"][-1]["turn"]["items"].insert(0, copy.deepcopy(earlier))
        observation["raw_turn_items"] = 2
        rewrite_observation(case, observation)
        store = case.store
        assert (
            store.repair_admission_preflight("run-1", case.command)["authorized_through_iteration"]
            == 4
        )
        response, current, recovery = admitted(case)
        original_role = copy.deepcopy(case.state["roles"][-1])
        original_raw = (case.folder / "result.json").read_bytes()
        store.mark_start("run-1", accepted=True)
        calls = []

        @activity.defn(name="delivery_role")
        async def role(body):
            calls.append(body["role"])
            assert body["iteration"] == 4
            if body["role"] == "implement":
                assert body["resume_session"] == SESSION
                assert case.raw["findings"][0] in body["findings"]
                assert (
                    body["spec"]["implementation_role_home_generation"]
                    == recovery["session_custody"]["generation"]
                )
                assert body["spec"]["policy"]["source_scope"]["allowed_files"] == ADDED
                assert body["spec"]["policy"]["max_repairs"] == 10
                for name in ADDED:
                    (case.broker.checkout / name).write_text("Authorized caller repair\n")
                # Native session state may evolve only after its guarded initial
                # readback. Original static assessment receipts remain unchanged.
                transcript = case.home / "codex/sessions" / (SESSION + ".jsonl")
                with transcript.open("a") as stream:
                    stream.write("Synthetic same-session iteration4\n")
            return {
                "role": body["role"],
                "iteration": 4,
                "status": "pass",
                "cleanup": "confirmed",
                "findings": [],
                "finish_reason": "done",
                "session_id": SESSION if body["role"] == "implement" else body["role"],
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
            broker = DeliveryBroker(store, body["spec"])
            _git(broker.checkout, "add", case.api, *ADDED)
            _git(broker.checkout, "commit", "-qm", "Publish authorized fixture repair")
            _git(broker.checkout, "push", "origin", "HEAD:refs/heads/" + current["branch"])
            candidate = broker.candidate()
            case.publication.update(head=candidate["head"], candidate=candidate)
            return copy.deepcopy(case.publication)

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

        @activity.defn(name="delivery_tracker")
        async def tracker(body):
            calls.append("tracker")
            if body.get("release"):
                with store._connect() as db:
                    store.state.release_work(db, current["work_id"], "external:devflow:run-1")
            return {"state": "consistent"}

        @activity.defn(name="delivery_terminal_tracker")
        async def terminal_tracker(body):
            calls.append("terminal_tracker")
            if body["release"]:
                with store._connect() as db:
                    store.state.release_work(db, current["work_id"], "external:devflow:run-1")
            return {"state": "consistent"}

        # Only provider execution, dependency measurement, GitHub and check
        # results are synthetic. The real activity context, resource/gate-root
        # ownership, raw receipt authentication, worker/finalizer and replay run.
        async with Worker(
            environment.client,
            task_queue="blocked-native-scope",
            workflows=[DeliveryWorkflow],
            activities=[
                delivery_repair_preflight,
                delivery_project,
                delivery_finalize_resources,
                role,
                tracker_start,
                precheck,
                publish,
                checks,
                ci,
                tracker,
                terminal_tracker,
            ],
        ):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run,
                args=[current, recovery],
                id=response["workflow_id"],
                task_queue="blocked-native-scope",
            )
            result = await asyncio.wait_for(
                handle.result(), getattr(case, "owner_test_timeout", 30)
            )
            assert result["outcome"] == "delivered", (result.get("error"), calls, result["checks"])
            assert result["iteration"] == 4 and result["roles"][0] == original_role
            assert result["roles"][0]["status"] == "blocked"
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
            assert recovery["maximum_iteration"] == 4
            assert result["checks"]["resource_cleanup"]["state"] == "confirmed"
            assert (case.folder / "result.json").read_bytes() == original_raw
            with store._connect() as db:
                assert store.state.claim_for(db, current["work_id"]) is None
                assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 1
            await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
                await handle.fetch_history()
            )


@pytest.mark.parametrize("target", ["source", "configuration"])
def test_worker_rejects_postadmission_source_or_configuration_drift(blocked_scope, target):
    case = blocked_scope
    _response, current, recovery = admitted(case)
    if target == "source":
        (case.broker.checkout / ADDED[0]).write_text("Changed before guarded provider launch\n")
    else:
        path = Path(case.command["amended_config_path"])
        path.write_bytes(path.read_bytes() + b"\n")
    with pytest.raises(ValueError):
        asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery}))
    assert case.state["roles"][-1]["status"] == "blocked"
    with case.store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 1


@pytest.mark.parametrize("stage", ["preflight", "worker"])
def test_fifo_native_receipt_refuses_public_and_worker_readback_without_blocking(
    blocked_scope, stage
):
    case = blocked_scope
    body = {
        "config": str(case.store.config.path),
        "command": case.command,
        "closed": case.store._completed_temporal_result("run-1"),
        "publication": case.publication,
    }
    if stage == "worker":
        _response, current, recovery = admitted(case)
        body.update(spec=current, recovery=recovery)
    path = case.folder / "start.json"
    path.unlink()
    os.mkfifo(path, 0o600)
    payload = case.store.config.state_root / "fifo-probe.json"
    write_private(payload, body)
    script = """
import asyncio,json,sys
from pathlib import Path
from devflow_temporal import delivery_native_process as native
from devflow_temporal import delivery_policy_recovery as policy
from devflow_temporal.delivery_activities import delivery_repair_preflight
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_store import DeliveryStore
value=json.loads(Path(sys.argv[1]).read_bytes())
store=DeliveryStore(DeliveryConfig.load(Path(value['config'])))
store._completed_temporal_result=lambda *_a,**_kw:value['closed']
DeliveryBroker._existing_pr=lambda *_a,**_kw:{'number':value['publication']['number'],
    'headRefOid':value['publication']['head'],'url':value['publication']['url']}
native.process_table=policy.process_table=lambda:{}
native.listeners=policy.listeners=lambda _port:set()
try:
    if 'recovery' in value:
        asyncio.run(delivery_repair_preflight({'spec':value['spec'],'recovery':value['recovery']}))
    else:
        store.repair_admission_preflight('run-1',value['command'])
except ValueError as exc:
    assert str(exc)=='blocked scope finding lacks its private native receipts', str(exc)
    print('FIFO refused by real recovery owner')
else:
    raise AssertionError('FIFO was admitted')
"""
    # subprocess.run kills and reaps a blocked child on timeout, making the
    # regression deterministic without leaving a waiter in this pytest process.
    observed = subprocess.run(
        [sys.executable, "-c", script, str(payload)],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert observed.returncode == 0, observed.stderr
    assert observed.stdout.strip() == "FIFO refused by real recovery owner"
    with case.store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 1
