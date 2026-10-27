from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from test_delivery_store import service as service
from test_delivery_store import submit_historical_admission

from devflow_temporal import delivery_gates_admission as gates
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_resources import RunResources, private_directory


@pytest.fixture
def stopped(service, monkeypatch):
    store, request = service
    submit_historical_admission(store, request, monkeypatch)
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


def test_gates_only_controller_patch_uses_authenticated_consumer_namespace(stopped):
    from devflow_temporal.delivery_activities import _context

    store, original, _closed, command = stopped
    store.admit_gates_only('run-1', command)
    spec = store.effective_spec('run-1')
    _store, broker = _context(spec)
    candidate = broker.candidate()
    # The controller publishes the admitted dirty after-candidate before review.
    _git = gates._git
    _git(broker.checkout, 'add', 'README.md')
    _git(broker.checkout, 'commit', '-qm', 'test: published admitted candidate')
    candidate = broker.candidate()
    path = broker.gate_checkout('review', 4, candidate)
    diff = broker.gate_diff('review', 4, candidate)
    request = {'spec': spec, 'role': 'review', 'iteration': 4,
               'candidate': candidate, 'workspace': str(path), 'review_diff': diff}
    assert path == original.state_dir / 'gates-admission/evidence/gates/4/review'
    from devflow_temporal.delivery_sandbox import _review_diff_path

    RunResources(spec)._allowed(path, 'gate')
    assert _review_diff_path(request) == Path(diff['path'])
    assert Path(diff['path']).parent == (
        original.state_dir / 'gates-admission/evidence/gate-evidence/4/review'
    )
    assert not (original.state_dir / 'gate-evidence/4/review').exists()
    assert broker.state_dir == original.state_dir


@pytest.mark.parametrize(
    "change",
    ["source", "accepted-plan"],
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
