from __future__ import annotations

import copy
import hashlib
import json

import httpx
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


@pytest.fixture
def admitted_title_repair(title_repair):
    """Existing row/payload fixture, not a new admission or recorded workflow history."""
    from pathlib import Path

    from devflow_temporal.delivery_repair import confirmed_native_cleanup

    store, broker, command, closed, predecessor = title_repair
    original = (broker.checkout / "browser.spec.ts").read_text()
    title = "stale results, version conflicts, and failed requests leave manual edits available"
    start = original.index(title)
    constraint = {
        "authority_path": command["authority_path"],
        "authority_sha256": command["authority_sha256"],
        "authority": json.loads(Path(command["authority_path"]).read_text()),
        "path": "browser.spec.ts", "original": original,
        "sha256": hashlib.sha256(original.encode()).hexdigest(),
        "start": start, "end": start + len(title), "title": title,
        "pattern": broker.spec["policy"]["browser_qa"]["reject_regex"],
        "candidate": closed["result"]["candidate"],
        "browser_receipt_sha256": digest(closed["result"]["checks"]["browser_qa"]),
        "new_implementation_turns": 1, "maximum_iteration": 5,
    }
    recovery = {
        "kind": "repair_continuation",
        "predecessor_workflow_id": closed["workflow_id"],
        "predecessor_execution_run_id": closed["execution_run_id"],
        "predecessor_closed_at": closed["closed_at"],
        "predecessor_result_digest": digest(closed["result"]),
        "state": closed["result"], "candidate": closed["result"]["candidate"],
        "pull_request": closed["result"]["pull_request"],
        "session_id": "original-session", "findings": {"browser_qa": "rejected title"},
        "additional_iterations": 1, "maximum_iteration": 5,
        "original_recovery": predecessor, "effective_spec": broker.spec,
        "title_constraint": constraint,
        "cleanup_digest": confirmed_native_cleanup(broker.spec),
    }
    response = {
        "run_id": "run-1", "dashboard_url": f"{store.config.dashboard_url}/runs/run-1",
        "phase": "repair_continuation_queued",
        "workflow_id": "delivery-run-1-repair-continuation-1",
        "authorized_through_iteration": 5, "existing": False,
    }
    with store._connect() as db:
        db.execute("INSERT INTO delivery_commands VALUES (?,?,?,?)",
                   (command["command_id"], "run-1", digest({"run_id": "run-1", **command}),
                    canonical_json(response)))
        db.execute("INSERT INTO delivery_repair_grants VALUES (?,?,?,?,?,?,?,?)",
                   ("run-1", command["command_id"], closed["workflow_id"],
                    closed["execution_run_id"], digest(closed["result"]),
                    1, 5, closed["closed_at"]))
        db.execute("UPDATE delivery_runs SET phase='repair_continuation_queued', "
                   "execution_state='queued',outcome=NULL,error=NULL,workflow_id=?,recovery_json=?",
                   (response["workflow_id"], canonical_json(recovery)))
        store.state.claim_work(db, "work-1", "external:devflow:run-1", store.config.dashboard_url)
    return store, broker, command, recovery, response


def test_previously_admitted_title_repair_remains_readable_and_strict(admitted_title_repair):
    store, broker, command, recovery, response = admitted_title_repair
    with store._connect() as db:
        before = list(db.iterdump())
    assert store.continue_repair("run-1", command) == response
    assert store.repair_admission_preflight("run-1", command) == response
    with store._connect() as db:
        assert list(db.iterdump()) == before
    assert store.effective_spec("run-1") == broker.spec
    store.repair_preflight(broker.spec, recovery)
    constraint = recovery["title_constraint"]
    validate_source(broker.spec, constraint, completed=False)
    target = broker.checkout / constraint["path"]
    target.write_text(target.read_text().replace("failed requests", "request errors"))
    validate_source(broker.spec, constraint, completed=True)
    with pytest.raises(ValueError, match="fields do not match"):
        store.continue_repair("run-1", {**command, "command_id": "title-2"})


@pytest.mark.parametrize("change", ["digest", "run"])
def test_previously_admitted_title_command_cannot_be_rebound(admitted_title_repair, change):
    store, _broker, command, _recovery, _response = admitted_title_repair
    if change == "digest":
        command["additional_iterations"] = 2
    else:
        with store._connect() as db:
            row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
            row["run_id"] = "foreign"
            db.execute(f"INSERT INTO delivery_runs ({','.join(row)}) "
                       f"VALUES ({','.join('?' for _ in row)})", tuple(row.values()))
            db.execute("UPDATE delivery_commands SET run_id='foreign' WHERE command_id='title-1'")
    with pytest.raises(ValueError, match="different inputs"):
        store.continue_repair("run-1", command)


@pytest.mark.parametrize(
    "change",
    ["assertion", "other-file", "unchanged", "bad-title", "another-title", "head", "authority"],
)
def test_literal_only_controller_rejects_source_or_authority_expansion(
    admitted_title_repair, change,
):
    _store, broker, command, recovery, _response = admitted_title_repair
    constraint = recovery["title_constraint"]
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


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ["repair-admission-preflight", "continue-repair"])
async def test_public_title_repair_new_admissions_are_retired_without_effects(
    title_repair, endpoint,
):
    from devflow_temporal.delivery_api import create_app

    store, broker, command, _closed, _predecessor = title_repair
    def snapshot():
        with store._connect() as db:
            return {
                table: [tuple(row) for row in db.execute(f"SELECT * FROM {table}")]
                for table in ("delivery_runs", "delivery_attempts", "delivery_commands",
                              "delivery_repair_grants", "delivery_outbox", "delivery_effects",
                              "delivery_events", "claims")
            }

    before = snapshot()
    files = {str(path): path.read_bytes() for path in broker.state_dir.rglob("*")
             if path.is_file()}
    source = _git(broker.checkout, "status", "--porcelain", "--untracked-files=all")
    app = create_app(store.config.path)
    app.state.delivery.store = store
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport,
                                base_url=store.config.dashboard_url) as browser:
        session = await browser.get("/api/session")
        response = await browser.post(f"/api/runs/run-1/{endpoint}", json=command,
                                      headers={"Origin": store.config.dashboard_url,
                                               "X-Devflow-CSRF": session.json()["csrf_token"]})
    assert response.status_code == 409, response.text
    assert "fields do not match the contract" in response.json()["detail"]
    assert snapshot() == before
    assert {str(path): path.read_bytes() for path in broker.state_dir.rglob("*")
            if path.is_file()} == files
    assert _git(broker.checkout, "status", "--porcelain", "--untracked-files=all") == source
