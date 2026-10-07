from __future__ import annotations

import copy
import hashlib
import json

import pytest
from test_delivery_metadata_recovery import published, restore_admitted_metadata
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_metadata_recovery import evidence_applicability
from devflow_temporal.delivery_title_repair import validate_source


@pytest.fixture
def title_repair(service, monkeypatch):
    store, request = service
    source = store.config.raw["repositories"]["fixture"]["source_path"]
    from pathlib import Path

    target = Path(source) / "browser.spec.ts"
    title = "stale results, version conflicts, and failed requests leave manual edits available"
    target.write_text(
        "".join(
            f'test("{title if i == 4 else f"case {i}"}", async () => {{\n'
            "  expect(conflict).toBe(true);\n});\n"
            for i in range(5)
        )
    )
    _git(source, "add", "browser.spec.ts")
    _git(source, "commit", "-qm", "test: base browser fixture")
    cfg = store.config.raw
    cfg["max_repairs"] = 3
    cfg["repositories"]["fixture"]["allowed_paths"].append("browser.spec.ts")
    cfg["repositories"]["fixture"]["expected_base_sha"] = _git(source, "rev-parse", "HEAD")
    cfg["repositories"]["fixture"]["browser_qa"] = {
        "id": "browser",
        "argv": ["true"],
        "cwd": ".",
        "timeout_seconds": 60,
        "env": {},
        "artifact_paths": [],
        "ports": {"PORT": 18798},
        "test_count_regex": r"(?m)(\d+) passed",
        "min_tests": 5,
        "reject_regex": r"(?m)\b(?:skipped|flaky|failed)\b",
    }
    store.config.path.write_text(json.dumps(cfg))
    store, broker, state, closed, metadata_command, _title = published.__wrapped__(
        (store, request), monkeypatch
    )
    for role in state["roles"]:
        role["iteration"] += 3
    state["iteration"] = 4
    review = {
        "role": "review",
        "iteration": 4,
        "candidate": state["candidate"],
        "status": "pass",
        "cleanup": "confirmed",
        "session_id": "review-session",
    }
    state["roles"].append(review)
    state["checks"]["review"] = {"state": "passed", "candidate_id": state["candidate"]["id"]}
    with store._connect() as db:
        for role in state["roles"]:
            if role["role"] == "implement":
                db.execute(
                    "UPDATE delivery_attempts SET iteration=?,result_json=? WHERE job_key=?",
                    (
                        role["iteration"],
                        canonical_json(role),
                        f"implementation-{role['iteration'] - 3}",
                    ),
                )
        for effect in db.execute("SELECT * FROM delivery_effects").fetchall():
            request_body = json.loads(effect["request_json"])
            if effect["kind"] == "publish":
                request_body["iteration"] += 3
                db.execute(
                    "UPDATE delivery_effects SET request_json=? WHERE effect_key=?",
                    (canonical_json(request_body), effect["effect_key"]),
                )
        db.execute(
            "INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,"
            "state,session_id,result_json,cleanup) VALUES ('review','run-1','review',4,?,"
            "'finished','review-session',?,'confirmed')",
            (state["candidate"]["id"], canonical_json(review)),
        )
    store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message="original",
        candidate=state["candidate"],
        pull_request=state["pull_request"],
        checks=state["checks"],
        iteration=4,
        protocol_revision=13,
        outcome="blocked",
        cleanup="none",
        error="repair limit exhausted",
    )
    restore_admitted_metadata(store, broker, state, closed, metadata_command, _title)
    with store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
    original_closed = copy.deepcopy(closed)
    new_state = copy.deepcopy(state)
    new_state.update(
        candidate=recovery["candidate"],
        pull_request=recovery["publication"],
        revision=20,
        cleanup="confirmed",
    )
    log = broker.state_dir / "metadata-reconciliation/evidence/browser-qa/4/browser-qa.log"
    from devflow_temporal.delivery_resources import private_directory

    private_directory(log.parent)
    log.write_text("x" * 1021 + "\n✓ " + title + "\n5 passed\n")
    log.chmod(0o600)
    browser = {
        "state": "failed",
        "cleanup": "confirmed",
        "exit_code": 0,
        "test_count": 5,
        "rejected_output": True,
        "log": str(log),
        "log_sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
        "candidate_id": recovery["candidate"]["id"],
        "diagnostic": log.read_text(),
    }
    new_state["checks"] = {**evidence_applicability(recovery), "browser_qa": browser}
    store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message="fresh browser rejection",
        candidate=new_state["candidate"],
        pull_request=new_state["pull_request"],
        checks=new_state["checks"],
        iteration=4,
        protocol_revision=20,
        outcome="blocked",
        cleanup="confirmed",
        error="repair limit exhausted",
    )
    closed.clear()
    closed.update(
        {
            **original_closed,
            "workflow_id": "delivery-run-1-metadata-1",
            "execution_run_id": "metadata-closed",
            "recovery_digest": digest(recovery),
            "result": new_state,
        }
    )
    with store._connect() as db:
        store.state.release_work(db, "work-1", "external:devflow:run-1")
    authority = {
        "decision_owner": "main task",
        "new_user_approval_required": False,
        "scope": {
            "run_id": "run-1",
            "work_id": "work-1",
            "session_id": "original-session",
            "base": broker.spec["base_sha"],
            "known_old_head": recovery["old_head"],
            "additional_iterations": 1,
            "authorized_through_iteration": 5,
            "max_new_grants": 1,
            "max_new_implementation_turns": 1,
            "actual_target": "browser.spec.ts:13 title-only correction",
        },
    }
    path = store.config.state_root / "title-authority.json"
    path.write_text(json.dumps(authority))
    path.chmod(0o600)
    command = {
        "command_id": "title-1",
        "expected_revision": 20,
        "expected_iteration": 4,
        "expected_candidate_id": new_state["candidate"]["id"],
        "expected_pr_number": 7,
        "expected_pr_head": new_state["candidate"]["head"],
        "additional_iterations": 1,
        "authority_path": str(path),
        "authority_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    return store, broker, command, closed, recovery


def test_metadata_first_existing_repair_preflight_one_grant_reacquires_and_preserves_history(
    title_repair,
):
    store, broker, command, _closed, predecessor = title_repair
    with store._connect() as db:
        original = db.execute("SELECT request_json FROM delivery_runs").fetchone()[0]
        attempts = list(db.execute("SELECT * FROM delivery_attempts"))
    observed = store.repair_admission_preflight("run-1", command)
    assert observed["preflight"] and "failed" in str(observed["diagnostics"])
    with store._connect() as db:
        assert store.state.claim_for(db, "work-1") is None
        assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0
    response = store.continue_repair("run-1", command)
    assert response["authorized_through_iteration"] == 5
    assert store.continue_repair("run-1", command) == response
    with store._connect() as db:
        row = db.execute("SELECT * FROM delivery_runs").fetchone()
        recovery = json.loads(row["recovery_json"])
        assert row["request_json"] == original
        assert list(db.execute("SELECT * FROM delivery_attempts")) == attempts
        assert store.state.claim_for(db, "work-1")["owner"] == "external:devflow:run-1"
    assert recovery["original_recovery"] == predecessor
    assert store.effective_spec("run-1") == broker.spec
    store.repair_preflight(broker.spec, recovery)
    constraint = recovery["title_constraint"]
    validate_source(broker.spec, constraint, completed=False)
    target = broker.checkout / constraint["path"]
    target.write_text(target.read_text().replace("failed requests", "request errors"))
    validate_source(broker.spec, constraint, completed=True)
    with pytest.raises(ValueError, match="one repair grant"):
        store.continue_repair("run-1", {**command, "command_id": "title-2"})


@pytest.mark.parametrize(
    "change",
    ["assertion", "other-file", "unchanged", "bad-title", "another-title", "head", "authority"],
)
def test_literal_only_controller_rejects_source_or_authority_expansion(title_repair, change):
    store, broker, command, _closed, _predecessor = title_repair
    store.continue_repair("run-1", command)
    with store._connect() as db:
        constraint = json.loads(
            db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0]
        )["title_constraint"]
    target = broker.checkout / constraint["path"]
    text = target.read_text().replace("failed requests", "request errors")
    if change == "assertion":
        text = text.replace("toBe(true)", "toBe(false)", 1)
    elif change == "other-file":
        (broker.checkout / "README.md").write_text("Unauthorized change")
    elif change == "unchanged":
        text = target.read_text()
    elif change == "bad-title":
        text = text.replace("request errors", "failed requests again")
    elif change == "another-title":
        text = text.replace("case 0", "unrelated case")
    elif change == "head":
        _git(broker.checkout, "commit", "--allow-empty", "-qm", "test: unadmitted commit")
    else:
        from pathlib import Path

        Path(command["authority_path"]).write_text("{}")
    target.write_text(text)
    with pytest.raises(ValueError):
        validate_source(broker.spec, constraint, completed=True)


@pytest.mark.parametrize(
    "change", ["grant", "session", "lineage", "candidate", "claim", "cleanup", "log", "pattern"]
)
def test_cause_specific_admission_rejects_unproven_or_changed_authority_without_claim(
    title_repair, change
):
    store, broker, command, closed, _predecessor = title_repair
    if change == "grant":
        command["additional_iterations"] = 2
    elif change == "session":
        closed["result"]["roles"][0]["session_id"] = "foreign"
    elif change == "lineage":
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET recovery_json=NULL")
        closed["recovery_digest"] = None
    elif change == "candidate":
        (broker.checkout / "browser.spec.ts").write_text("Changed source")
    elif change == "claim":
        with store._connect() as db:
            store.state.claim_work(db, "work-1", "external:foreign", "foreign")
    elif change == "cleanup":
        closed["result"]["cleanup"] = "unknown"
    elif change == "log":
        from pathlib import Path

        Path(closed["result"]["checks"]["browser_qa"]["log"]).write_text("changed")
    else:
        closed["result"]["checks"]["browser_qa"]["rejected_output"] = False
    with pytest.raises(ValueError):
        store.continue_repair("run-1", command)
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0
        claim = store.state.claim_for(db, "work-1")
        assert claim is None or claim["owner"] == "external:foreign"
