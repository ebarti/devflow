from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path

import pytest
from test_delivery_store import service as service

from devflow_temporal import delivery_gates_admission as gates
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_resources import RunResources, private_directory
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture
def stopped(service, monkeypatch):
    store, request = service
    store.submit(request)
    spec = store.spec("run-1")
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    (broker.checkout / "README.md").write_text("Original dirty investigation\n")
    before = broker.candidate()
    (broker.checkout / "README.md").write_text("Measured stopped investigation\n")
    after = broker.candidate()
    session = "original-session"
    home = broker.state_dir / "role-homes/implement/codex/sessions"
    private_directory(home)
    transcript = home / (session + ".jsonl")
    transcript.write_text(
        json.dumps(
            {
                "status": "pass",
                "summary": "Measured outcome",
                "findings": [{"severity": "medium", "detail": "Existing outcome"}],
            }
        )
    )
    transcript.chmod(0o600)
    result = {
        "status": None,
        "summary": "Measured outcome",
        "findings": ["pass assessment contained findings"],
        "session_id": session,
    }
    folder = broker.state_dir / "attempts/authentic-shape"
    private_directory(folder)
    path = folder / "result.json"
    path.write_text(canonical_json(result))
    path.chmod(0o600)
    role = {
        **result,
        "role": "implement",
        "iteration": 4,
        "provider": "codex",
        "cleanup": "confirmed",
        "input_candidate_id": before["id"],
        "candidate": after,
    }
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,"
            "state,session_id,result_json,result_path,cleanup) VALUES "
            "('authentic-shape','run-1','implement',4,?,'finished',?,?,?,'confirmed')",
            (before["id"], session, canonical_json(result), str(path)),
        )
        store.state.release_work(db, "work-1", "external:devflow:run-1")
    resources = RunResources(spec)
    resources.scratch("check", "original")
    receipt = resources.finalize("blocked")
    state = {
        "run_id": "run-1",
        "phase": "blocked",
        "execution_state": "blocked",
        "outcome": "blocked",
        "error": "implementer did not establish a pass",
        "cleanup": "none",
        "revision": 14,
        "iteration": 4,
        "candidate": before,
        "pull_request": None,
        "roles": [role],
        "checks": {"resource_cleanup": receipt},
        "candidate_revision": 1,
        "findings": result["findings"],
        "usage": {},
        "tracker": {},
    }
    store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message=state["error"],
        candidate=before,
        checks=state["checks"],
        iteration=4,
        protocol_revision=14,
        outcome="blocked",
        cleanup="none",
        error=state["error"],
    )
    closed = {
        "workflow_id": "delivery-run-1",
        "execution_run_id": "closed-run",
        "request_digest": spec["request_digest"],
        "recovery_digest": None,
        "closed_at": "2026-10-03T21:34:00Z",
        "result": state,
    }
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_a, **_kw: closed)
    monkeypatch.setattr(DeliveryBroker, "_existing_pr", lambda *_a, **_kw: None)
    custody = store.config.state_root / "custody.json"
    custody.write_text(json.dumps({"before": before, "after": after, "authentic_shape": True}))
    custody.chmod(0o600)
    plan = store.config.state_root / "accepted-plan.json"
    plan.write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "accepted_plan": {
                    "revision": 1,
                    "content": spec["accepted_plan"],
                    "digest": digest(spec["accepted_plan"]),
                },
            }
        )
    )
    plan.chmod(0o600)
    semantic = {
        "decision_owner": "main task",
        "new_user_approval_required": False,
        "scope_unchanged": spec["policy"]["allowed_paths"],
        "accepted_plan_readback": str(plan),
        "accepted_plan_readback_sha256": hashlib.sha256(plan.read_bytes()).hexdigest(),
    }
    sempath = store.config.state_root / "semantic.json"
    sempath.write_text(json.dumps(semantic))
    sempath.chmod(0o600)
    authority = {
        "decision_owner": "main task",
        "new_user_approval_required": False,
        "scope": {
            "run_id": "run-1",
            "work_id": "work-1",
            "original_session": session,
            "iteration": 4,
            "max_commands": 1,
            "current_after_candidate": after,
            "frozen_input_candidate_id": before["id"],
            "allowed_paths": spec["policy"]["allowed_paths"],
            "authority_sha256": hashlib.sha256(sempath.read_bytes()).hexdigest(),
        },
        "custody_proof": {
            "path": str(custody),
            "sha256": hashlib.sha256(custody.read_bytes()).hexdigest(),
        },
    }
    authpath = store.config.state_root / "gates-authority.json"
    authpath.write_text(json.dumps(authority))
    authpath.chmod(0o600)
    observed = store.gates_only_preflight("run-1")
    command = {
        "command_id": "gates-1",
        "precheck_sha256": observed["precheck_sha256"],
        "authority_path": str(authpath),
        "authority_sha256": hashlib.sha256(authpath.read_bytes()).hexdigest(),
        "semantic_path": str(sempath),
        "semantic_sha256": hashlib.sha256(sempath.read_bytes()).hexdigest(),
    }
    return store, broker, closed, command


def test_gates_only_preserves_rejected_result_input_after_custody_and_has_no_extra_turn(stopped):
    store, broker, closed, command = stopped
    with store._connect() as db:
        attempts = list(db.execute("SELECT * FROM delivery_attempts"))
        original = db.execute("SELECT request_json FROM delivery_runs").fetchone()[0]
    response = store.admit_gates_only("run-1", command)
    assert response["iteration"] == 4 and response["implementation_authority"] is False
    assert store.admit_gates_only("run-1", command) == response
    with store._connect() as db:
        row = db.execute("SELECT * FROM delivery_runs").fetchone()
        recovery = json.loads(row["recovery_json"])
        assert row["request_json"] == original
        assert list(db.execute("SELECT * FROM delivery_attempts")) == attempts
        assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0
        assert store.state.claim_for(db, "work-1")["owner"] == "external:devflow:run-1"
    assert recovery["state"] == closed["result"]
    assert recovery["candidate"] == closed["result"]["roles"][0]["candidate"]
    assert recovery["candidate"]["id"] != closed["result"]["candidate"]["id"]
    assert gates.readback(store, broker.spec, recovery)["implementation_authority"] is False
    with pytest.raises(ValueError, match="already received"):
        store.admit_gates_only("run-1", {**command, "command_id": "gates-2"})
    with pytest.raises(ValueError, match="different inputs"):
        store.admit_gates_only("run-1", {**command, "precheck_sha256": "0" * 64})


def test_historical_native_attempt_container_and_controller_enrichment_are_read_only(stopped):
    store, broker, _closed, _command = stopped
    container = broker.state_dir / 'attempts'
    container.chmod(0o755)  # Supervisor.mkdir(parents=True) historical container contract.
    with store._connect() as db:
        attempt = dict(db.execute('SELECT * FROM delivery_attempts').fetchone())
        raw = json.loads(Path(attempt['result_path']).read_bytes())
        saved = {**raw, 'cleanup': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
                 'resource_cleanup': 'pending_workflow_finalization', 'native_process': {
                     'state': 'finished', 'monitoring_complete': True,
                     'cleanup': 'observed-native-confirmed', 'exit_code': 0,
                     'journal': str(Path(attempt['result_path']).with_name('native-process.json')),
                 }}
        db.execute('UPDATE delivery_attempts SET result_json=?', (canonical_json(saved),))
    path = Path(attempt['result_path'])
    before = path.read_bytes()
    assert gates.preflight(store, 'run-1')['iteration'] == 4
    gates._assessment_receipt({**broker.spec, 'provider': 'codex'},
                             {**attempt, 'result_json': canonical_json(saved)}, before)
    assert path.read_bytes() == before and container.stat().st_mode & 0o777 == 0o755
    for changed in ({**saved, 'summary': 'Changed assessment'},
                    {**saved, 'process_cleanup': 'unknown'},
                    {**saved, 'native_process': {
                        **saved['native_process'], 'monitoring_complete': 1}},
                    {**saved, 'unrecognized': True}):
        with pytest.raises(ValueError):
            gates._assessment_receipt({**broker.spec, 'provider': 'codex'},
                                     {**attempt, 'result_json': canonical_json(changed)}, before)


@pytest.mark.parametrize('change', ['container-write', 'leaf-public', 'result-public',
                                    'hardlink', 'symlink', 'wrong-path', 'missing-leaf'])
def test_historical_native_receipt_reader_rejects_unrecognized_custody_without_creation(
    stopped, change,
):
    store, broker, _closed, _command = stopped
    with store._connect() as db:
        attempt = dict(db.execute('SELECT * FROM delivery_attempts').fetchone())
    path = Path(attempt['result_path'])
    if change == 'container-write':
        path.parent.parent.chmod(0o775)
    elif change == 'leaf-public':
        path.parent.chmod(0o755)
    elif change == 'result-public':
        path.chmod(0o644)
    elif change == 'hardlink':
        os.link(path, path.with_name('foreign-link.json'))
    elif change == 'symlink':
        moved = path.with_name('retained.json')
        path.rename(moved)
        path.symlink_to(moved.name)
    elif change == 'wrong-path':
        attempt['result_path'] = str(path.with_name('retained.json'))
    else:
        attempt['job_key'] = 'absent-leaf'
        attempt['result_path'] = str(broker.state_dir / 'attempts/absent-leaf/result.json')
    with pytest.raises((ValueError, OSError)):
        gates._native_result_bytes(broker.spec, attempt)
    assert not (broker.state_dir / 'attempts/absent-leaf').exists()


@pytest.mark.parametrize(
    "change",
    [
        "source",
        "frozen",
        "after",
        "role-input",
        "session",
        "raw",
        "cleanup",
        "active",
        "issue",
        "authority",
        "precheck",
        "accepted-plan",
    ],
)
def test_gates_only_admission_refuses_changed_provenance_without_claim_or_roles(stopped, change):
    store, broker, closed, command = stopped
    if change == "source":
        (broker.checkout / "README.md").write_text("Different source")
    elif change == "frozen":
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET candidate_json=NULL")
    elif change == "after":
        closed["result"]["roles"][0]["candidate"]["id"] = "a" * 64
    elif change == "role-input":
        closed["result"]["roles"][0]["input_candidate_id"] = "a" * 64
    elif change == "session":
        next((broker.state_dir / "role-homes/implement/codex/sessions").glob("*")).write_text(
            "changed"
        )
    elif change == "raw":
        (broker.state_dir / "attempts/authentic-shape/result.json").write_text("{}")
    elif change == "cleanup":
        (broker.state_dir / "resources/finalization.json").write_text("{}")
    elif change == "active":
        with store._connect() as db:
            db.execute("UPDATE delivery_attempts SET state='running'")
    elif change == "issue":
        with store._connect() as db:
            db.execute("UPDATE works SET issue='https://github.com/example/fixture/issues/999'")
    elif change == "authority":
        Path(command["authority_path"]).write_text("{}")
    elif change == "accepted-plan":
        (store.config.state_root / "accepted-plan.json").write_text("{}")
    else:
        command["precheck_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        store.admit_gates_only("run-1", command)
    with store._connect() as db:
        assert store.state.claim_for(db, "work-1") is None
        assert db.execute("SELECT COUNT(*) FROM delivery_gate_admissions").fetchone()[0] == 0


@pytest.mark.parametrize("failed", [False, True])
def test_same_iteration_workflow_skips_implementation_and_cannot_spend_another_turn(
    stopped, monkeypatch, failed
):
    store, broker, _closed, command = stopped
    store.admit_gates_only("run-1", command)
    with store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
    flow = DeliveryWorkflow()
    called = []

    async def project(*_args):
        pass

    async def execute(name, request, **_kw):
        called.append(name)
        if name == "delivery_role":
            assert request["role"] in {"review", "verify"}
            return {
                "status": "pass",
                "role": request["role"],
                "cleanup": "confirmed",
                "session_id": "independent",
                "candidate": flow.state["candidate"],
            }
        if name in {"delivery_tracker_start", "delivery_tracker"}:
            return {"state": "consistent"}
        if name == "delivery_publish":
            candidate = {**flow.state["candidate"], "head": "b" * 40, "id": "b" * 64}
            return {
                "state": "OPEN",
                "head": "b" * 40,
                "candidate": candidate,
                "number": 7,
                "url": "https://example.invalid/pull/7",
            }
        if name == "delivery_precheck" and failed:
            return {"state": "failed", "cleanup": "confirmed", "results": []}
        return {"state": "passed", "cleanup": "confirmed"}

    monkeypatch.setattr(flow, "_project", project)
    monkeypatch.setattr(flow, "_activity", execute)
    result = asyncio.run(flow._resume_gates_only(broker.spec, recovery))
    assert result["iteration"] == 4
    assert result["outcome"] == ("blocked" if failed else "delivered")
    assert called.count("delivery_role") == (0 if failed else 2)
    assert result["roles"][0]["status"] is None  # Rejected original assessment stays rejected.
