"""Public native quarantine recovery with real resource/admission/gate-root owners."""

from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_delivery_gate_retry import unpublished
from test_delivery_store import service as service

from devflow_temporal import delivery_native_process as native
from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_activities import delivery_repair_preflight
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_orphans import reconcile_attempt
from devflow_temporal.delivery_resources import (
    RunResources,
    _identity,
    read_private,
    write_private,
)
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.supervisor import DeliverySupervisor


def native_case(service, monkeypatch, *, preserve_existing_roots=False):
    store, _old_broker, state, _ = unpublished.__wrapped__(service, monkeypatch)
    spec = copy.deepcopy(store.effective_spec("run-1"))
    spec.update(
        provider="codex",
        automatic_retry_version=1,
        retry_budget_version=1,
        resource_cleanup_version=1,
        terminal_tracker_version=1,
    )
    spec["policy"].update(
        execution_backend="native-macos", host_sandbox="trusted-local", max_repairs=10
    )
    spec["policy_digest"] = digest(spec["policy"])
    broker = DeliveryBroker(store, spec)
    partial = broker.candidate()
    before = {**partial, "id": digest("original-input"), "content_sha256": digest("before-turn")}
    state.update(
        revision=10,
        iteration=0,
        cleanup="unknown",
        candidate=before,
        tracker=None,
        error="implementer did not establish a pass",
    )
    role = {
        "role": "implement",
        "iteration": 0,
        "status": "recovery_unknown",
        "finish_reason": "recovery_unknown",
        "cleanup": "unknown",
        "session_id": None,
        "candidate": partial,
        "input_candidate_id": before["id"],
    }
    state["roles"] = [role]
    state["checks"] = {
        "failure": {"classification": "terminal", "stage": "implement", "reason": state["error"]},
        "resource_cleanup": {
            "state": "unknown",
            "resource_cleanup": "unknown",
            "process_cleanup": "unknown",
            "reason": "ActivityError",
        },
    }
    request = {
        "spec": spec,
        "role": "implement",
        "iteration": 0,
        "candidate": before,
        "workspace": spec["checkout"],
        "resume_session": None,
    }
    key = DeliverySupervisor._job_key(request)
    folder = Path(spec["state_dir"]) / "attempts" / key
    journal_path = folder / "native-process.json"
    request.update(result_path=str(folder / "result.json"), start_path=str(folder / "start.json"))
    launch = {"argv": ["opaque-provider"], "environment": {"SYNTHETIC": "true"}}
    intent = {
        "run_id": spec["run_id"],
        "policy_digest": spec["policy_digest"],
        "argv": launch["argv"],
        "environment_sha256": digest(launch["environment"]),
        "cwd": spec["checkout"],
        "timeout": 10,
        "ports": [],
    }
    owned = {
        "17017": {"identity": "opaque-provider"},
        "17019": {"pid": 17019, "identity": "opaque-monitor"},
    }
    proof = {
        "version": 1,
        "kind": "interrupted_monitor",
        "previous_phase": "authorized",
        "intent_sha256": digest(intent),
        "owned_sha256": digest(owned),
    }
    outcome = {
        "journal": str(journal_path),
        "cleanup": "observed-native-confirmed",
        "interrupted_monitor": True,
        "provider_outcome": "unknown",
        "monitoring_complete": False,
        "cleanup_reconciliation": proof,
    }
    result = {
        "status": "recovery_unknown",
        "finish_reason": "interrupted_monitor",
        "cleanup": "confirmed",
        "process_cleanup": "observed-native-confirmed",
        "session_id": None,
        "native_process": outcome,
    }
    journal = {
        "phase": "finished",
        "monitoring_complete": False,
        "owned": owned,
        "monitor": {"pid": 17019, "identity": "opaque-monitor"},
        "ports": [],
        "intent": intent,
        "cleanup_reconciliation": proof,
        "result": outcome,
        "provider_session": {
            "role": "implement",
            "iteration": 0,
            "session_id": None,
            "resumed_from": None,
            "result_digest": digest(result),
            "output_candidate": {k: partial[k] for k in ("id", "head", "content_sha256")},
        },
    }
    write_private(journal_path, journal)
    write_private(folder / "request.json", request)
    write_private(folder / "launch.json", launch)
    for name in ("native-process.lock", "native-monitor.lock"):
        folder.joinpath(name).touch(mode=0o600)
    state_dir = Path(spec["state_dir"])
    gate = state_dir / "gates/0/review"
    registry = RunResources(spec)
    with registry.locked() as manifest:
        manifest.update(
            schema="devflow-run-resources-v1",
            run_id="run-1",
            state_identity=_identity(state_dir),
            processes=[str(journal_path)],
            roots={
                **(manifest["roots"] if preserve_existing_roots else {}),
                str(gate): {
                    "kind": "gate",
                    "identity": None,
                    "state": "removed",
                    "receipt": {"path": str(gate), "kind": "gate", "state": "already_absent"},
                }
            },
        )
        write_private(registry.manifest, manifest)
    write_private(
        registry.root / "finalization.json",
        {
            "state": "unknown",
            "resource_cleanup": "unknown",
            "process_cleanup": "unknown",
            "outcome": "blocked",
            "roots": [],
            "processes": [
                {
                    "journal": str(journal_path),
                    "cleanup": "unknown",
                    "monitoring_complete": False,
                    "observed_pids": [17017, 17019],
                    "owned_ports_clear": True,
                }
            ],
        },
    )
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET request_json=?,accepted_plan_text=?,protocol_revision=10,"
            "iteration=0,candidate_json=?,checks_json=?,cleanup=?,error=?,tracker_json=?",
            (
                canonical_json(spec),
                spec["accepted_plan"],
                canonical_json(before),
                canonical_json(state["checks"]),
                "unknown",
                state["error"],
                "null",
            ),
        )
        db.execute("UPDATE delivery_outbox SET state='sent'")
        db.execute("DELETE FROM delivery_attempts")
        db.execute(
            "INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,state,"
            "cleanup,session_id,pid,process_identity,result_path,result_json) VALUES "
            "(?,'run-1','implement',0,?,'finished','confirmed',NULL,17017,'opaque-provider',?,?)",
            (key, before["id"], request["result_path"], canonical_json(result)),
        )
        store.state.claim_work(db, spec["work_id"], "external:devflow:run-1", "fixture")
    closed = store._completed_temporal_result("run-1")
    closed["closed_at"] = "2026-10-24T12:00:00+00:00"
    monkeypatch.setattr(native, "process_table", lambda: {})
    monkeypatch.setattr(native, "listeners", lambda _: set())
    monkeypatch.setattr(DeliveryBroker, "_read_owned_pr", lambda *_: None)
    monkeypatch.setattr(DeliveryStore, "_ensure_no_remote_pr", staticmethod(lambda *_: None))

    # External preparation is opaque, but preserve its actual measured-policy
    # transition so the never-started retry must authenticate policy-rebound source.
    def prepare(value, _root, command_digest, _seal_digest):
        execution = copy.deepcopy(value)
        execution["policy"]["native_identity"] = {"runtime_payload_sha256": digest(command_digest)}
        execution["policy_digest"] = digest(execution["policy"])
        return execution

    monkeypatch.setattr(resume, "prepare_runtime", prepare)

    def tracker(_spec, status, *, release, terminal, reason):
        with store._connect() as db:
            store.state.release_work(db, spec["work_id"], "external:devflow:run-1")
        return {
            "state": "recorded",
            "pending": False,
            "scope": "local",
            "project_sync": "asynchronous",
            "status": status,
            "claim_released": release,
        }

    monkeypatch.setattr("devflow_temporal.delivery_automatic_retry._tracker_sync", tracker)
    teardown = native.reconcile_process(journal_path, spec=spec)
    assert teardown["cleanup"] == "observed-native-confirmed", teardown
    command = {
        "continuation_kind": resume.KIND,
        "command_id": "first-finite-iteration",
        "expected_revision": 10,
        "expected_iteration": 0,
        "expected_candidate_id": partial["id"],
        "expected_candidate_head": partial["head"],
        "additional_iterations": 1,
    }
    return SimpleNamespace(
        store=store,
        broker=broker,
        state=state,
        spec=spec,
        closed=closed,
        command=command,
        registry=registry,
        journal=journal_path,
        gate=gate,
    )


@pytest.fixture
def native_quarantine(service, monkeypatch):
    return native_case(service, monkeypatch)


def admitted(case):
    assert reconcile_attempt(case.store, run_id="run-1", role="cleanup", iteration=0)["changed"]
    assert case.store.repair_admission_preflight("run-1", case.command)["preflight"]
    response = case.store.continue_repair("run-1", case.command)
    with case.store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
    return response, case.store.effective_spec("run-1"), recovery


def failed_preflight(case, response, execution, recovery):
    """The observed second closure stays at iteration0/protocol12 with no new attempt."""
    case.store.mark_start("run-1", accepted=True)
    later = copy.deepcopy(recovery["state"])
    later.update(
        revision=12,
        iteration=0,
        phase="blocked",
        outcome="blocked",
        execution_state="blocked",
        cleanup="unknown",
        candidate=recovery["execution_candidate"],
        error="repair authority preflight failed: ActivityError",
    )
    later["tracker"] = {
        "state": "recorded",
        "pending": False,
        "scope": "local",
        "project_sync": "asynchronous",
        "status": "blocked",
        "claim_released": False,
    }
    later["checks"]["failure"] = {
        "classification": "terminal",
        "stage": "repair_preflight",
        "reason": later["error"],
    }
    # The real terminal finalizer ran with unknown cleanup after the old worker refusal.
    later["checks"]["resource_cleanup"] = RunResources(execution).finalize(
        "blocked", uncertain=True
    )
    case.store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        outcome="blocked",
        cleanup="unknown",
        candidate=later["candidate"],
        pull_request=None,
        checks=later["checks"],
        tracker=later.get("tracker"),
        iteration=0,
        protocol_revision=12,
        error=later["error"],
        event_type="blocked",
        message=later["error"],
    )
    with case.store._connect() as db:
        db.execute("UPDATE delivery_runs SET revision=18")
    case.closed.update(
        workflow_id=response["workflow_id"],
        execution_run_id="closed-preflight",
        closed_at="2026-10-24T13:25:00+00:00",
        recovery_digest=digest(recovery),
        result=later,
    )
    return later


def test_actual_never_started_second_closure_reuses_existing_iteration_and_maximum(
    native_quarantine,
):
    case = native_quarantine
    response, execution, recovery = admitted(case)
    original = copy.deepcopy(recovery["state"])
    later = failed_preflight(case, response, execution, recovery)
    assert later["iteration"] == 0 and later["revision"] == 12
    assert later["candidate"]["policy_digest"] != later["roles"][0]["candidate"]["policy_digest"]
    with case.store._connect() as db:
        assert db.execute("SELECT revision FROM delivery_runs").fetchone()[0] == 18
    assert reconcile_attempt(case.store, run_id="run-1", role="cleanup", iteration=0)["changed"]
    command = {
        **case.command,
        "command_id": "retry-original-unused-iteration",
        "expected_revision": 12,
        "additional_iterations": 0,
    }
    assert (
        case.store.repair_admission_preflight("run-1", command)["authorized_through_iteration"]
        == recovery["maximum_iteration"]
        == 1
    )
    second_response = case.store.continue_repair("run-1", command)
    with case.store._connect() as db:
        second = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
        assert not db.execute("SELECT 1 FROM delivery_attempts WHERE iteration=1").fetchone()
    second_spec = case.store.effective_spec("run-1")
    assert second["state"] == later
    assert recovery["state"] == original
    assert second["command"]["additional_iterations"] == 0
    assert second["pending_repair"]["iteration"] == second["maximum_iteration"] == 1
    assert second_spec["role_home_generation"] == execution["role_home_generation"]
    assert (
        second["session_id"] is None and second_response["workflow_id"] != response["workflow_id"]
    )
    assert asyncio.run(delivery_repair_preflight({"spec": second_spec, "recovery": second})) == {
        "state": "confirmed"
    }
    assert read_private(case.journal)["result"]["provider_outcome"] == "unknown"


@pytest.mark.asyncio
async def test_public_second_closure_reentry_runs_real_worker_readback_and_new_iteration(
    service,
    monkeypatch,
):
    from temporal_test_server import local_temporal
    from temporalio import activity
    from temporalio.worker import Replayer, Worker

    from devflow_temporal.delivery_activities import delivery_finalize_resources, delivery_project
    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    async with local_temporal() as environment:
        initial, submitted = service
        config = copy.deepcopy(initial.config.raw)
        config["temporal_address"] = environment.client.service_client.config.target_host
        initial.config.path.write_text(json.dumps(config))
        store = DeliveryStore(DeliveryConfig.load(initial.config.path))
        case = native_case((store, submitted), monkeypatch)
        response, execution, recovery = admitted(case)
        original_role = copy.deepcopy(recovery["state"]["roles"][0])
        later = failed_preflight(case, response, execution, recovery)
        assert reconcile_attempt(store, run_id="run-1", role="cleanup", iteration=0)["changed"]
        command = {
            **case.command,
            "command_id": "worker-reentry-same-unused-turn",
            "expected_revision": 12,
            "additional_iterations": 0,
        }
        assert (
            store.repair_admission_preflight("run-1", command)["authorized_through_iteration"] == 1
        )
        response = store.continue_repair("run-1", command)
        with store._connect() as db:
            second = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
        current = store.effective_spec("run-1")
        store.mark_start("run-1", accepted=True)
        calls = []

        @activity.defn(name="delivery_role")
        async def role(body):
            calls.append(body["role"])
            assert body["iteration"] == 1
            if body["role"] == "implement":
                assert body["resume_session"] is None and body["findings"] == []
                assert body["spec"]["role_home_generation"] == execution["role_home_generation"]
                (case.broker.checkout / "README.md").write_text(
                    "New independently completed turn\n"
                )
            return {
                "role": body["role"],
                "iteration": 1,
                "status": "pass",
                "cleanup": "confirmed",
                "session_id": "fresh-session" if body["role"] == "implement" else body["role"],
                "candidate": DeliveryBroker(store, body["spec"]).candidate(),
                "findings": [],
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
            return {
                "number": 7,
                "head": body["candidate"]["head"],
                "candidate": body["candidate"],
                "state": "OPEN",
            }

        @activity.defn(name="delivery_checks")
        async def checks(body):
            calls.append("checks")
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
            assert body["release"] is True
            with store._connect() as db:
                store.state.release_work(db, current["work_id"], "external:devflow:run-1")
            return {"state": "consistent"}

        # Only external provider/GitHub/gate-result boundaries are synthetic. The
        # actual repair activity, DB/config/admission/resource owners and finalizer run.
        async with Worker(
            environment.client,
            task_queue="quarantined-authority",
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
                args=[current, second],
                id=response["workflow_id"],
                task_queue="quarantined-authority",
            )
            result = await asyncio.wait_for(handle.result(), 30)
            assert result["outcome"] == "delivered", result.get("error")
            assert result["iteration"] == 1 and result["roles"][0] == original_role
            assert second["state"] == later and second["maximum_iteration"] == 1
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
            assert result["checks"]["resource_cleanup"]["state"] == "confirmed"
            with store._connect() as db:
                assert store.state.claim_for(db, current["work_id"]) is None
            assert read_private(case.journal)["result"]["provider_outcome"] == "unknown"
            await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
                await handle.fetch_history()
            )
