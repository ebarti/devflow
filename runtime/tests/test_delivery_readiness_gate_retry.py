"""Public same-candidate retry of a real, retained readiness refusal.

The Temporal closure, implementation and failed dependency are controlled
fixtures. Readiness receipt creation, Git/SQLite ownership and HTTP admission
are real; no provider or production installation is used.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path

import httpx
import pytest
from test_delivery_gate_retry import unpublished as unpublished
from test_delivery_metadata_recovery import published as published
from test_delivery_store import _git
from test_delivery_store import service as base_service

from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_activities import _check_readiness
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_gate_retry import KIND, readback
from devflow_temporal.delivery_resources import RunResources
from devflow_temporal.delivery_store import DeliveryStore


@pytest.fixture
def service(tmp_path):
    store, request = base_service.__wrapped__(tmp_path)
    source = Path(store.config.raw["repositories"]["fixture"]["source_path"])
    (source / "test_retry_gate.py").write_text(
        "import unittest\n"
        "from pathlib import Path\n"
        "class Gate(unittest.TestCase):\n"
        "    def test_candidate(self):\n"
        "        self.assertIn('Owned unpublished candidate', Path('README.md').read_text())\n"
        "    def test_absent(self):\n"
        "        self.assertIn('an absent requirement', Path('README.md').read_text())\n"
    )
    _git(source, "add", "test_retry_gate.py")
    _git(source, "commit", "-qm", "Add fixture check")
    config = deepcopy(store.config.raw)
    config["repositories"]["fixture"]["expected_base_sha"] = _git(source, "rev-parse", "HEAD")
    store.config.path.write_text(json.dumps(config))
    return DeliveryStore(DeliveryConfig.load(store.config.path)), request


@pytest.fixture
def stopped_readiness(unpublished, monkeypatch, request):
    store, _, state, command = unpublished
    spec = deepcopy(store.spec("run-1"))
    spec["readiness_version"] = 1
    spec["policy"]["prepublish_checks"] = [
        {
            "id": "diff",
            "argv": ["git", "diff", "--check"],
            "cwd": ".",
            "kind": "check",
            "timeout_seconds": 30,
        }
    ]
    selector = (
        "test_retry_gate.Gate.test_absent"
        if getattr(request, "param", "") == "assertion"
        else "test_retry_gate.Gate.test_candidate"
    )
    spec["policy"]["checks"] = [
        {
            "id": "assert-readme",
            "argv": ["python3", "-m", "unittest", selector],
            "cwd": ".",
            "kind": "check",
            "timeout_seconds": 30,
        }
    ]
    spec["policy"]["readiness"] = {
        "version": 1,
        "generation": 0,
        "browser_engines": [],
        "temporal_checks": [],
    }
    browser_missing = getattr(request, "param", "") == "missing-browser"
    if browser_missing:
        spec["policy"]["browser_qa"] = {"argv": ["playwright", "test"], "ports": {}}
        spec["policy"]["readiness"]["browser_engines"] = ["chromium"]
        monkeypatch.setattr(RunResources, "browser_scratch", lambda self: Path(spec["state_dir"]))
        monkeypatch.setattr(
            "devflow_temporal.delivery_sandbox.prepare_browser_qa",
            lambda *_args, **_kwargs: (None, {"PATH": str(Path(spec["state_dir"]) / "no-node")}),
        )
    spec["policy_digest"] = digest(spec["policy"])
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET request_json=? WHERE run_id=?",
            (canonical_json(spec), "run-1"),
        )
    broker = DeliveryBroker(store, spec)
    resources = RunResources(spec)
    staged = broker.checkout.with_name("staged-candidate")
    broker.checkout.rename(staged)
    resources.register(broker.checkout, "checkout")
    staged.rename(broker.checkout)
    resources.created(broker.checkout)
    candidate = broker.candidate()
    state["candidate"] = candidate
    state["roles"][0]["candidate"] = candidate
    with store._connect() as db:
        role_result = {
            k: v
            for k, v in state["roles"][0].items()
            if k not in {"role", "iteration", "candidate"}
        }
        db.execute(
            "UPDATE delivery_attempts SET result_json=? WHERE job_key='original'",
            (canonical_json(role_result),),
        )
    command["expected_candidate_id"] = candidate["id"]
    monkeypatch.setattr(
        "devflow_temporal.delivery_preparation.verify_prepared_spec", lambda *_: None
    )
    monkeypatch.setattr(
        "devflow_temporal.delivery_configured_resources.implementation_prerequisites", lambda *_: []
    )
    monkeypatch.setattr(
        "devflow_temporal.delivery_plan_checks.planned_checks", lambda *_a, **_k: []
    )
    monkeypatch.setattr(
        DeliveryBroker,
        "run_implementation_preparation",
        lambda self, *_a, **_k: {
            "state": "failed",
            "results": [{"id": "dependency", "passed": False, "cleanup": "confirmed"}],
            "candidate_id": candidate["id"],
            "source_unchanged": True,
        },
    )

    def failure(execution, current):
        broker = DeliveryBroker(store, execution)
        result = _check_readiness(
            broker, {"spec": execution, "candidate": current["candidate"], "iteration": 0}, 100
        )
        assert result["failure_class"]["class"] == "environment_prelaunch"
        assert result["readiness"]["state"] == "failed"
        current["checks"] = {"prepublish": result, "readiness": result["readiness"]}
        current["error"] = "execution readiness prevented prepublication"
        return result

    original = failure(spec, state)
    store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message=state["error"],
        candidate=candidate,
        checks=state["checks"],
        iteration=0,
        protocol_revision=9,
        outcome="blocked",
        cleanup="confirmed",
        error=state["error"],
    )
    closed = {
        "workflow_id": "delivery-run-1",
        "execution_run_id": "closed-original",
        "request_digest": spec["request_digest"],
        "recovery_digest": None,
        "result": state,
    }
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_a, **_k: closed)
    monkeypatch.setattr(DeliveryStore, "_completed_temporal_result", lambda *_a, **_k: closed)
    return store, broker, state, command, original, closed, failure


@asynccontextmanager
async def client_for(store):
    app = create_app(store.config.path)
    host = store.config.dashboard_url
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 8001)), base_url=host
    )
    async with client:
        headers = {
            "Origin": host,
            "X-Devflow-CSRF": (await client.get("/api/session")).json()["csrf_token"],
        }
        yield client, headers


@pytest.mark.asyncio
async def test_public_retry_preserves_receipts_and_bounds_repeated_failures(stopped_readiness):
    store, broker, state, command, original, closed, failure = stopped_readiness
    candidate = deepcopy(state["candidate"])
    retained = Path(original["readiness"]["receipt"]).read_bytes()
    with store._connect() as db:
        initial_attempts = [dict(x) for x in db.execute("SELECT * FROM delivery_attempts")]
        initial_effects = [dict(x) for x in db.execute("SELECT * FROM delivery_effects")]
    attempts = [original["readiness"]["attempt"]]
    async with client_for(store) as (client, headers):
        for generation in (1, 2):
            preflight = await client.post(
                "/api/runs/run-1/repair-admission-preflight", json=command
            )
            assert preflight.status_code == 200, preflight.text
            result = await client.post(
                "/api/runs/run-1/continue-repair", json=command, headers=headers
            )
            assert result.status_code == 200, result.text
            admitted = result.json()
            assert admitted["implementation_authority"] is False
            assert (
                admitted["additional_iterations"] == 0
                and admitted["candidate_id"] == candidate["id"]
            )
            assert (
                await client.post("/api/runs/run-1/continue-repair", json=command, headers=headers)
            ).json() == admitted
            execution = store.effective_spec("run-1")
            with store._connect() as db:
                recovery = json.loads(
                    db.execute(
                        "SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'"
                    ).fetchone()[0]
                )
                assert [
                    dict(x) for x in db.execute("SELECT * FROM delivery_attempts")
                ] == initial_attempts
                assert [
                    dict(x) for x in db.execute("SELECT * FROM delivery_effects")
                ] == initial_effects
                assert not db.execute("SELECT 1 FROM delivery_repair_grants").fetchone()
            assert execution["gate_retry_generation"] == generation
            assert execution["gate_retry_stage"] == "readiness"
            assert recovery["state"]["checks"]["prepublish"]["readiness"]["state"] == "failed"
            assert readback(store, execution, recovery) is None
            if generation == 1:
                old_path = Path(original["readiness"]["receipt"])
                old_path.write_bytes(retained + b" ")
                with pytest.raises(ValueError, match="receipt changed"):
                    readback(store, execution, recovery)
                old_path.write_bytes(retained)
            assert broker.candidate() == candidate
            current = deepcopy(recovery["state"])
            current.update(
                revision=9 + generation,
                phase="blocked",
                execution_state="blocked",
                outcome="blocked",
                cleanup="confirmed",
                candidate=recovery["candidate"],
            )
            failed_again = failure(execution, current)
            attempts.append(failed_again["readiness"]["attempt"])
            store.project(
                "run-1",
                phase="blocked",
                execution_state="blocked",
                event_type="blocked",
                message=current["error"],
                candidate=current["candidate"],
                checks=current["checks"],
                iteration=0,
                protocol_revision=current["revision"],
                outcome="blocked",
                cleanup="confirmed",
                error=current["error"],
            )
            with store._connect() as db:
                store.state.release_work(db, execution["work_id"], "external:devflow:run-1")
            closed.update(
                workflow_id=admitted["workflow_id"],
                execution_run_id=f"closed-{generation}",
                recovery_digest=digest(recovery),
                result=current,
            )
            command = {
                **command,
                "command_id": f"retry-readiness-{generation + 1}",
                "expected_revision": current["revision"],
            }
        refusal = await client.post("/api/runs/run-1/repair-admission-preflight", json=command)
        assert refusal.status_code == 409
        assert "readiness_retry_limit_exhausted" in refusal.text
    assert len(set(attempts)) == 3
    assert Path(original["readiness"]["receipt"]).read_bytes() == retained
    with store._connect() as db:
        assert [dict(x) for x in db.execute("SELECT * FROM delivery_attempts")] == initial_attempts
        assert [dict(x) for x in db.execute("SELECT * FROM delivery_effects")] == initial_effects
        assert not db.execute("SELECT 1 FROM delivery_repair_grants").fetchone()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["receipt", "source", "effect", "product", "cleanup"])
async def test_public_retry_refuses_changed_or_unsettled_evidence(stopped_readiness, drift):
    store, broker, state, command, original, _, _ = stopped_readiness
    if drift == "receipt":
        path = Path(original["readiness"]["receipt"])
        path.write_bytes(path.read_bytes() + b" ")
    elif drift == "source":
        (broker.checkout / "README.md").write_text("Changed candidate\n")
    elif drift == "effect":
        with store._connect() as db:
            db.execute(
                "INSERT INTO delivery_effects VALUES ('pending','run-1','publish',"
                "'{}','pending',NULL,'now')"
            )
    elif drift == "product":
        state["roles"].append(
            {
                "role": "review",
                "iteration": 0,
                "status": "findings",
                "findings": ["Real product defect"],
                "cleanup": "confirmed",
            }
        )
    else:
        state["cleanup"] = "unknown"
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET cleanup='unknown' WHERE run_id='run-1'")
    async with client_for(store) as (client, _):
        response = await client.post("/api/runs/run-1/repair-admission-preflight", json=command)
        assert response.status_code == 409, response.text
    with store._connect() as db:
        assert not db.execute("SELECT 1 FROM delivery_gate_admissions").fetchone()


@pytest.mark.asyncio
@pytest.mark.parametrize("stopped_readiness", ["passing", "assertion"], indirect=True)
async def test_retry_workflow_runs_the_frozen_gates_without_implementation(
    stopped_readiness, monkeypatch
):
    import asyncio

    from temporal_test_server import local_temporal
    from temporalio import activity
    from temporalio.worker import Worker

    from devflow_temporal.delivery_activities import (
        delivery_checks,
        delivery_gates_readback,
        delivery_precheck,
        delivery_project,
    )
    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    store, broker, state, command, original, _, _ = stopped_readiness
    old_receipt = Path(original["readiness"]["receipt"]).read_bytes()
    async with client_for(store) as (client, headers):
        response = await client.post(
            "/api/runs/run-1/continue-repair", json=command, headers=headers
        )
        assert response.status_code == 200, response.text
        admitted = response.json()
    spec = store.effective_spec("run-1")
    with store._connect() as db:
        recovery = json.loads(
            db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0]
        )
    store.mark_start("run-1", accepted=True)
    monkeypatch.setattr(
        DeliveryBroker,
        "run_implementation_preparation",
        lambda self, _i, candidate, **_k: {
            "state": "passed",
            "results": [],
            "candidate_id": candidate["id"],
            "source_unchanged": True,
        },
    )
    monkeypatch.setattr(
        "devflow_temporal.delivery_readiness.probe_tools",
        lambda _broker, requirements, candidate, **_kw: {
            "state": "passed",
            "cleanup": "confirmed",
            "component": "tools",
            "candidate": candidate,
            "requirement_sha256": digest(requirements),
            "items": [],
        },
    )
    monkeypatch.setattr("devflow_temporal.delivery_readiness_verify.tools", lambda *_a: None)
    actual_gate_checkout = DeliveryBroker.gate_checkout

    def gate_checkout(self, role, iteration, candidate):
        path = actual_gate_checkout(self, role, iteration, candidate)
        resources = self.run_resources()
        with resources.locked(read_only=True) as manifest:
            registered = str(path) in manifest["roots"]
        if not registered:
            staged = path.with_name(path.name + "-staged")
            path.rename(staged)
            resources.register(path, "gate")
            staged.rename(path)
            resources.created(path, head=candidate["head"])
        return path

    monkeypatch.setattr(DeliveryBroker, "gate_checkout", gate_checkout)
    calls = []

    @activity.defn(name="delivery_publish")
    async def publish(body):
        calls.append("publish")
        assert body["candidate"]["id"] == state["candidate"]["id"]
        _git(broker.checkout, "add", "README.md")
        _git(broker.checkout, "commit", "-qm", "Publish fixture")
        current = DeliveryBroker(store, body["spec"]).candidate()
        return {
            "number": 7,
            "url": "https://example.invalid/pull/7",
            "state": "OPEN",
            "head": current["head"],
            "candidate": current,
        }

    @activity.defn(name="delivery_role")
    async def role(body):
        calls.append(body["role"])
        assert body["role"] in {"review", "verify"}
        return {
            "role": body["role"],
            "iteration": body["iteration"],
            "status": "pass",
            "session_id": "independent-" + body["role"],
            "candidate": body["candidate"],
            "cleanup": "confirmed",
            "findings": [],
        }

    @activity.defn(name="delivery_tracker_start")
    async def start(_body):
        return {"state": "consistent"}

    @activity.defn(name="delivery_tracker")
    async def tracker(_body):
        return {"state": "consistent"}

    @activity.defn(name="delivery_ci")
    async def ci(_body):
        calls.append("ci")
        return {"state": "passed"}

    async with local_temporal() as env:
        async with Worker(
            env.client,
            task_queue="issue-243-gates",
            workflows=[DeliveryWorkflow],
            activities=[
                delivery_gates_readback,
                delivery_project,
                delivery_precheck,
                delivery_checks,
                publish,
                role,
                start,
                tracker,
                ci,
            ],
        ):
            handle = await env.client.start_workflow(
                DeliveryWorkflow.run,
                args=[spec, recovery],
                id=admitted["workflow_id"],
                task_queue="issue-243-gates",
            )
            result = await asyncio.wait_for(handle.result(), 60)
    if spec["policy"]["checks"][0]["argv"][-1].endswith("test_absent"):
        assert result["outcome"] == "blocked"
        assert result["checks"]["local"]["state"] == "failed"
        assert result["checks"]["local"]["results"][0]["exit_code"] != 0
        assert any("local_checks" in finding for finding in result["findings"])
        assert calls == ["publish"]
    else:
        assert result["outcome"] == "delivered", result
        assert calls == ["publish", "review", "verify", "ci"]
    assert result["iteration"] == 0
    assert result["roles"][0] == state["roles"][0]
    assert (
        result["checks"]["prepublish"]["results"][0]["argv"]
        == spec["policy"]["prepublish_checks"][0]["argv"]
    )
    assert result["checks"]["local"]["results"][0]["argv"] == spec["policy"]["checks"][0]["argv"]
    assert recovery["original_spec"]["policy"]["checks"] == spec["policy"]["checks"]
    assert Path(original["readiness"]["receipt"]).read_bytes() == old_receipt
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 1
        assert not db.execute("SELECT 1 FROM delivery_repair_grants").fetchone()


@pytest.mark.asyncio
async def test_public_native_retry_reports_missing_descendant_settlement(stopped_readiness):
    """A plausible native-shaped checkpoint still cannot bypass missing settlement proof."""
    import hashlib

    from devflow_temporal.delivery_readiness_gate_retry import failure as classify
    from devflow_temporal.delivery_readiness_gate_retry import probe_attempt
    from devflow_temporal.delivery_resources import write_private

    store, _, state, command, result, closed, _ = stopped_readiness
    spec = deepcopy(store.spec("run-1"))
    spec["provider"] = "codex"
    spec["policy"]["host_sandbox"] = "trusted-local"
    spec["policy"]["execution_backend"] = "native-macos"
    spec["policy_digest"] = digest(spec["policy"])
    candidate = deepcopy(state["candidate"])
    candidate["policy_digest"] = spec["policy_digest"]
    state["candidate"] = candidate
    state["roles"][0]["candidate"] = candidate
    receipt = result["readiness"]
    receipt["binding"]["spec_sha256"] = digest(spec)
    receipt["binding"]["candidate"] = candidate
    for observation in receipt["observations"].values():
        observation["candidate"] = candidate
    receipt["attempt"] = probe_attempt(spec, candidate, 100)
    path = (
        Path(spec["state_dir"])
        / "readiness"
        / "receipts"
        / "plan"
        / "0"
        / f"{receipt['attempt']}.json"
    )
    receipt["receipt"] = str(path)
    write_private(
        path,
        {key: value for key, value in receipt.items() if key not in {"receipt", "receipt_sha256"}},
    )
    receipt["receipt_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    result["candidate_id"] = candidate["id"]
    result["failure_class"] = classify(receipt, "prepublish", candidate["id"], "confirmed")
    with store._connect() as db:
        role_result = {
            k: v
            for k, v in state["roles"][0].items()
            if k not in {"role", "iteration", "candidate"}
        }
        db.execute(
            "UPDATE delivery_attempts SET result_json=? WHERE job_key='original'",
            (canonical_json(role_result),),
        )
        db.execute(
            "UPDATE delivery_runs SET request_json=?,candidate_json=?,checks_json=? "
            "WHERE run_id='run-1'",
            (canonical_json(spec), canonical_json(candidate), canonical_json(state["checks"])),
        )
    closed["result"] = state
    async with client_for(store) as (client, headers):
        assessment = await client.post(
            "/api/runs/run-1/assess-transition",
            json={"kind": command["continuation_kind"], "request": command},
        )
        assert assessment.status_code == 200, assessment.text
        data = assessment.json()
        assert data["eligible"] is False
        assert any(
            item["code"] == "native_descendant_settlement_unknown" and item["satisfied"] is None
            for item in data["blockers"]
        ), data
        denied = await client.post("/api/runs/run-1/continue-repair", json=command, headers=headers)
        assert denied.status_code == 409, denied.text
        assert any(
            item["code"] == "native_descendant_settlement_unknown"
            for item in denied.json()["detail"]["blockers"]
        )
    with store._connect() as db:
        assert not db.execute("SELECT 1 FROM delivery_gate_admissions").fetchone()


@pytest.mark.asyncio
async def test_public_published_readiness_retains_candidate_and_full_selector(
    published, monkeypatch
):
    store, _, state, closed, _, _ = published
    spec = deepcopy(store.spec("run-1"))
    spec["readiness_version"] = 1
    spec["resource_cleanup_version"] = 1
    spec["policy"]["readiness"] = {
        "version": 1,
        "generation": 0,
        "browser_engines": [],
        "temporal_checks": [],
    }
    spec["policy_digest"] = digest(spec["policy"])
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET request_json=? WHERE run_id=?",
            (canonical_json(spec), "run-1"),
        )
    broker = DeliveryBroker(store, spec)
    resources = RunResources(spec)
    staged = broker.checkout.with_name("staged-published-candidate")
    broker.checkout.rename(staged)
    resources.register(broker.checkout, "checkout")
    staged.rename(broker.checkout)
    resources.created(broker.checkout)
    candidate = broker.candidate()
    state["candidate"] = candidate
    state["pull_request"]["candidate"] = candidate
    state["roles"][-1]["candidate"]["policy_digest"] = spec["policy_digest"]
    with store._connect() as db:
        role_result = {
            k: v
            for k, v in state["roles"][-1].items()
            if k not in {"role", "iteration", "candidate", "input_candidate_id"}
        }
        db.execute(
            "UPDATE delivery_attempts SET result_json=? WHERE job_key='implementation-1'",
            (canonical_json(role_result),),
        )
        store.state.release_work(db, spec["work_id"], "external:devflow:run-1")
    monkeypatch.setattr(
        "devflow_temporal.delivery_preparation.verify_prepared_spec", lambda *_: None
    )
    monkeypatch.setattr(
        "devflow_temporal.delivery_configured_resources.implementation_prerequisites", lambda *_: []
    )
    monkeypatch.setattr(
        "devflow_temporal.delivery_plan_checks.planned_checks", lambda *_a, **_k: []
    )
    monkeypatch.setattr(
        DeliveryBroker,
        "run_implementation_preparation",
        lambda self, *_a, **_k: {
            "state": "failed",
            "results": [{"id": "dependency", "passed": False, "cleanup": "confirmed"}],
            "candidate_id": candidate["id"],
            "source_unchanged": True,
        },
    )
    readiness = _check_readiness(
        broker, {"spec": spec, "candidate": candidate, "iteration": 1}, 101
    )
    assert readiness["failure_class"]["class"] == "environment_prelaunch"
    checks = {
        "prepublish": {
            "state": "passed",
            "candidate_id": candidate["id"],
            "source_unchanged": True,
        },
        "local": readiness,
        "readiness": readiness["readiness"],
    }
    state.update(
        checks=checks, cleanup="confirmed", error="execution readiness prevented local checks"
    )
    store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message=state["error"],
        candidate=candidate,
        pull_request=state["pull_request"],
        checks=checks,
        iteration=1,
        protocol_revision=13,
        outcome="blocked",
        cleanup="confirmed",
        error=state["error"],
    )
    closed["result"] = state
    monkeypatch.setattr(DeliveryStore, "_completed_temporal_result", lambda *_a, **_k: closed)
    monkeypatch.setattr("devflow_temporal.delivery_gate_retry._stopped_cleanup", lambda *a, **k: {})
    monkeypatch.setattr("devflow_temporal.delivery_gate_retry.preserve_resources", lambda *_: None)
    command = {
        "continuation_kind": KIND,
        "command_id": "published-readiness-retry",
        "expected_revision": 13,
        "expected_iteration": 1,
        "expected_candidate_id": candidate["id"],
        "expected_pr_number": 7,
        "expected_pr_head": candidate["head"],
        "additional_iterations": 0,
    }
    with store._connect() as db:
        effects = [
            dict(row) for row in db.execute("SELECT * FROM delivery_effects ORDER BY effect_key")
        ]
        attempts = [
            dict(row) for row in db.execute("SELECT * FROM delivery_attempts ORDER BY job_key")
        ]
    async with client_for(store) as (client, headers):
        narrower = await client.post(
            "/api/runs/run-1/repair-admission-preflight",
            json={**command, "verification_test_paths": ["test_retry_gate.py"]},
        )
        assert narrower.status_code == 409 and "frozen test selectors" in narrower.text
        preflight = await client.post("/api/runs/run-1/repair-admission-preflight", json=command)
        assert preflight.status_code == 200, preflight.text
        response = await client.post(
            "/api/runs/run-1/continue-repair", json=command, headers=headers
        )
        assert response.status_code == 200, response.text
        assert response.json()["implementation_authority"] is False
    execution = store.effective_spec("run-1")
    with store._connect() as db:
        recovery = json.loads(
            db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0]
        )
        assert [
            dict(row) for row in db.execute("SELECT * FROM delivery_effects ORDER BY effect_key")
        ] == effects
        assert [
            dict(row) for row in db.execute("SELECT * FROM delivery_attempts ORDER BY job_key")
        ] == attempts
        assert not db.execute("SELECT 1 FROM delivery_repair_grants").fetchone()
    assert recovery["publication"] == state["pull_request"]
    assert recovery["state"]["checks"]["local"] == readiness
    assert execution["policy"]["checks"] == spec["policy"]["checks"]
    assert readback(store, execution, recovery) == state["pull_request"]


@pytest.mark.asyncio
@pytest.mark.parametrize("stopped_readiness", ["missing-browser"], indirect=True)
async def test_public_retry_retains_missing_browser_runtime_observation(stopped_readiness):
    store, _, state, command, original, _, _ = stopped_readiness
    observation = original["readiness"]["observations"]["browser"]
    assert observation["state"] == "failed" and observation["launched"] is False
    assert observation["reason"] == "node unavailable in browser environment"
    async with client_for(store) as (client, headers):
        response = await client.post(
            "/api/runs/run-1/continue-repair", json=command, headers=headers
        )
        assert response.status_code == 200, response.text
        assert response.json()["implementation_authority"] is False
    with store._connect() as db:
        recovery = json.loads(
            db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0]
        )
        assert not db.execute("SELECT 1 FROM delivery_repair_grants").fetchone()
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 1
    assert recovery["state"]["checks"]["prepublish"]["readiness"] == original["readiness"]
    assert recovery["candidate"] == state["candidate"]
