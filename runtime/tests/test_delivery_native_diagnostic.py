"""A separately acknowledged native diagnostic cannot advance a blocked delivery.

The completed provider and GitHub/Temporal observations are neutral owned fixture
boundaries. Raw typed result, admitted request, resource/candidate/proof readers,
SQLite fences, public HTTP commands and native check execution remain real.
"""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import httpx
import pytest
from test_delivery_blocked_scope import scope_case
from test_delivery_store import _git
from test_delivery_store import service as service
from test_delivery_temporal_prerequisites import (
    CHECK,
    DEPENDENCY,
    PROBE,
    PROVENANCE,
    PROVENANCE_SHA,
    STAGE,
)

from devflow_temporal import delivery_native_diagnostic as diagnostic
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_native_diagnostic_execution import DiagnosticContext
from devflow_temporal.delivery_resources import RunResources, read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore


def owned_controls(source):
    """Neutral source, not real provider output or a copied JobCtrl gate result."""
    base = "workers/automation/tests/"
    output = {
        base + "temporal_env.py": source.joinpath("tests/temporal_env.py").read_text(),
        base + "_owned_temporal_controls.py": PROBE,
        base + "conftest.py": ("from _owned_temporal_controls import observe_existing_paths\n"),
        base + "test_linkedin_guest_dialog.py": """import os
from playwright.sync_api import sync_playwright

def test_offered_guest_dialog_is_closed_before_description_capture():
    assert os.environ['PLAYWRIGHT_BROWSERS_PATH']
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True)
        try:
            page=browser.new_page()
            page.set_content('<dialog open><button onclick="this.parentNode.close()">Close</button>'
                             '</dialog><main>Owned synthetic description</main>')
            page.get_by_role('button',name='Close').click()
            assert not page.locator('dialog').is_visible()
            assert page.locator('main').inner_text()=='Owned synthetic description'
        finally:
            browser.close()
""",
        # A mechanical high-count check exercises the complete unmodified
        # wrapper/JUnit/minimum. This is a synthetic framework test subject,
        # never a claim that 1000 real product tests passed.
        base + "test_owned_invariants.py": """import hashlib,json
import pytest
@pytest.mark.parametrize('value',range(1008))
def test_complete_structural_inventory(value):
    raw=json.dumps({'number':value,'unicode':'é','items':[value,None,True]},
                   sort_keys=True,ensure_ascii=False).encode()
    assert json.loads(raw)['number']==value
    assert len(hashlib.sha256(raw).hexdigest())==64
    assert raw!=raw+b' '
""",
    }
    for module, name, control in (
        (
            "test_saved_posting_availability_runtime.py",
            "test_real_temporal_activity_and_registered_workflow_record_current_evidence",
            "test_real_six_hour_time_skipping",
        ),
        (
            "test_workflow_apply.py",
            "test_apply_workflow_returns_ok_when_apply_main_succeeds",
            "test_real_local_activity",
        ),
        (
            "test_workflow_durability_probe.py",
            "test_durability_probe_resumes_same_run_after_worker_crash",
            "test_real_same_workflow_after_worker_restart",
        ),
    ):
        output[base + module] = (
            "import pytest\nimport _owned_temporal_controls as control\n"
            f"@pytest.mark.asyncio\nasync def {name}():\n    await control.{control}()\n"
        )
    return output


@pytest.fixture
def stopped_diagnostic(service, monkeypatch):
    assert digest(CHECK) == diagnostic.CHECK_SHA256
    source = STAGE / "immutable-source/workers/automation"
    extra = {
        ".gitignore": "node_modules/\nworkers/automation/.venv/\n__pycache__/\n.pytest_cache/\n"
    }
    for name in ("pyproject.toml", "uv.lock"):
        extra["workers/automation/" + name] = source.joinpath(name).read_text()
    extra.update(owned_controls(source))
    case = scope_case(
        service,
        monkeypatch,
        extra_sources=extra,
        checks=[DEPENDENCY, CHECK],
        prepublish_checks=[DEPENDENCY, CHECK],
    )
    # Complete the earlier source changes, retaining distinct authenticated
    # input/output checkpoints and the original genuinely blocked assessment.
    _git(case.broker.checkout, "add", case.api)
    _git(case.broker.checkout, "commit", "-qm", "Retain owned diagnostic candidate")
    _git(case.broker.checkout, "push", "origin", "HEAD:refs/heads/" + case.spec["branch"])
    candidate = case.broker.candidate()
    case.journal["provider_session"]["output_candidate"] = {
        k: candidate[k] for k in ("id", "head", "content_sha256")
    }
    write_private(case.folder / "native-process.json", case.journal)
    case.state["roles"][-1]["candidate"] = candidate
    case.publication.update(head=candidate["head"], candidate=candidate)
    registry = RunResources(case.spec)
    # A neutral earlier failed native full-check receipt, not a captured real
    # provider/check output. All its schema, paths and hashes are owned evidence.
    check_folder = Path(case.spec["state_dir"]) / "checks/3/worker-regressions/native"
    check_folder.mkdir(parents=True, mode=0o700)
    log = check_folder / "process.log"
    log.write_text("Owned previous full check did not finish within its fixed deadline\n")
    log.chmod(0o600)
    outcome = {
        "state": "finished",
        "journal": str(check_folder / "native-process.json"),
        "log": str(log),
        "exit_code": -15,
        "cleanup": "observed-native-confirmed",
        "monitoring_complete": True,
        "stdio_drained": True,
        "timed_out": True,
        "cancelled": False,
    }
    journal = {
        "phase": "finished",
        "monitoring_complete": True,
        "ports": [],
        "owned": {"17031": {"identity": "owned-prior-check"}},
        "result": outcome,
        "intent": {
            "run_id": "run-1",
            "policy_digest": case.spec["policy_digest"],
            "argv": CHECK["argv"],
            "cwd": str(case.broker.checkout),
            "environment_sha256": digest({"CI": "1"}),
            "timeout": 2700,
            "ports": [],
        },
    }
    write_private(check_folder / "native-process.json", journal)
    (check_folder / "native-process.lock").touch(mode=0o600)
    registry.process(check_folder / "native-process.json")
    finalization = registry.finalize("blocked")
    failed = {
        "id": CHECK["id"],
        "argv": CHECK["argv"],
        "cwd": str(case.broker.checkout),
        "passed": False,
        "log": str(log),
        "log_sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
        "native_process": outcome,
    }
    case.state["checks"].update(
        resource_cleanup=finalization,
        local={"state": "failed", "candidate_id": candidate["id"], "results": [failed]},
    )
    case.store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        outcome="blocked",
        event_type="blocked",
        message=case.state["error"],
        candidate=case.input,
        pull_request=case.publication,
        checks=case.state["checks"],
        iteration=3,
        protocol_revision=40,
        error=case.state["error"],
        tracker=case.state["tracker"],
        cleanup="confirmed",
    )
    closed = {
        "workflow_id": "delivery-run-1",
        "execution_run_id": "owned-closed-3",
        "closed_at": "2026-10-25T12:00:00+00:00",
        "request_digest": case.spec["request_digest"],
        "recovery_digest": None,
        "result": case.state,
    }
    monkeypatch.setattr(DeliveryStore, "_completed_temporal_result", lambda *_a, **_k: closed)
    original = case.store.config.state_root / "operator-inputs/official-tools.json"
    original.parent.mkdir(mode=0o700)
    original.write_bytes(PROVENANCE.read_bytes())
    original.chmod(0o600)
    assert hashlib.sha256(original.read_bytes()).hexdigest() == PROVENANCE_SHA
    with case.store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs").fetchone())
    case.diagnostic_request = {
        "command_id": "owned-finite-check",
        "expected_revision": 40,
        "expected_projection_revision": row["revision"],
        "expected_iteration": 3,
        "expected_candidate_id": candidate["id"],
        "expected_candidate_head": candidate["head"],
        "expected_content_sha256": candidate["content_sha256"],
        "expected_pr_number": 12,
        "expected_pr_head": candidate["head"],
        "check_id": CHECK["id"],
        "expected_check_sha256": digest(CHECK),
        diagnostic.TEMPORAL: {
            "check_id": CHECK["id"],
            "python_project": "workers/automation",
            "provenance_path": str(original),
            "provenance_sha256": PROVENANCE_SHA,
        },
    }
    case.closed, case.candidate, case.registry = closed, candidate, registry
    return case


def ack(case):
    request = copy.deepcopy(case.diagnostic_request)
    preflight = diagnostic.preflight(case.store, "run-1", request)
    request.update(preflight_key=preflight["preflight_key"], acknowledgement=diagnostic.ACK)
    return diagnostic.acknowledge(case.store, "run-1", request), request


def running_context(case):
    response, request = ack(case)
    with case.store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_native_diagnostics").fetchone())
        db.execute("UPDATE delivery_native_diagnostics SET state='running'")
    return DiagnosticContext(DeliveryBroker(case.store, case.spec), row), row, request, response


def test_preflight_and_ack_leave_every_delivery_row_and_original_resource_byte_unchanged(
    stopped_diagnostic,
):
    case = stopped_diagnostic
    with case.store._connect() as db:
        before = diagnostic._rows(db, "run-1")
    files = {
        p: p.read_bytes()
        for p in (case.registry.manifest, case.registry.state / "resources/finalization.json")
    }
    original = copy.deepcopy(case.state)
    response, request = ack(case)
    assert response["state"] == "queued" and response["delivery_acceptance"] is False
    assert diagnostic.acknowledge(case.store, "run-1", request) == response
    assert case.store.effective_spec("run-1") == case.spec
    assert case.state == original
    assert all(p.read_bytes() == b for p, b in files.items())
    with case.store._connect() as db:
        assert diagnostic._rows(db, "run-1") == before
        assert case.store.state.claim_for(db, case.spec["work_id"]) is None
    assert len(case.store.evidence_index("run-1")) > 0


@pytest.mark.parametrize(
    "change",
    [
        "model",
        "candidate",
        "revision",
        "check",
        "ack",
        "extra",
        "tools",
        "missing_tools",
        "raw",
        "log",
        "live_port",
    ],
)
def test_preflight_never_loosens_original_authority(stopped_diagnostic, monkeypatch, change):
    case = stopped_diagnostic
    request = copy.deepcopy(case.diagnostic_request)
    if change == "model":
        value = json.loads(case.store.config.path.read_text())
        value["roles"]["implement"]["model"] = "foreign"
        case.store.config.path.write_text(json.dumps(value))
    elif change == "candidate":
        request["expected_content_sha256"] = "0" * 64
    elif change == "revision":
        request["expected_revision"] -= 1
    elif change == "check":
        request["expected_check_sha256"] = digest("changed recipe")
    elif change == "ack":
        request["acknowledgement"] = "ordinary acceptance"
    elif change == "extra":
        request["argv"] = ["/usr/bin/true"]
    elif change == "tools":
        request[diagnostic.TEMPORAL]["provenance_sha256"] = digest("foreign tools")
    elif change == "missing_tools":
        del request[diagnostic.TEMPORAL]
    elif change == "raw":
        raw = read_private(case.folder / "result.json")
        raw["status"] = "pass"
        write_private(case.folder / "result.json", raw)
    elif change == "log":
        path = Path(case.spec["state_dir"]) / "checks/3/worker-regressions/native/process.log"
        path.write_text("Changed failed gate bytes\n")
    else:
        from devflow_temporal import delivery_native_process as native

        journal = read_private(case.folder / "native-process.json")
        journal["ports"] = [19687]
        write_private(case.folder / "native-process.json", journal)
        monkeypatch.setattr(native, "listeners", lambda _port: {19001})
    with pytest.raises((ValueError, OSError, RuntimeError)):
        diagnostic.preflight(case.store, "run-1", request)


@pytest.mark.parametrize("action", ["row", "claim", "attempt", "effect", "resource"])
def test_ack_fences_original_writers_but_does_not_modify_them(stopped_diagnostic, action):
    case = stopped_diagnostic
    ack(case)
    with pytest.raises(ValueError, match=diagnostic.ERROR):
        if action == "resource":
            RunResources(case.spec).finalize("blocked")
            return
        with case.store._connect() as db:
            if action == "row":
                db.execute("UPDATE delivery_runs SET iteration=4 WHERE run_id='run-1'")
            elif action == "claim":
                case.store.state.claim_work(db, case.spec["work_id"], "other-owner")
            elif action == "attempt":
                db.execute("UPDATE delivery_attempts SET state='running' WHERE run_id='run-1'")
            else:
                db.execute(
                    "INSERT INTO delivery_effects VALUES "
                    "('other','run-1','publish','{}','pending',NULL,'now')"
                )


def test_fresh_command_or_alternative_provenance_copy_does_not_mint_another_check(
    stopped_diagnostic,
):
    case = stopped_diagnostic
    ack(case)
    request = copy.deepcopy(case.diagnostic_request)
    request["command_id"] = "fresh-id"
    old = Path(request[diagnostic.TEMPORAL]["provenance_path"])
    path = old.with_name("another-exact-copy.json")
    path.write_bytes(old.read_bytes())
    path.chmod(0o600)
    request[diagnostic.TEMPORAL]["provenance_path"] = str(path)
    with pytest.raises(ValueError, match="already acknowledged"):
        diagnostic.preflight(case.store, "run-1", request)
    with case.store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_native_diagnostics").fetchone()[0] == 1


@pytest.mark.parametrize("drift", ["source", "owner", "receipt"])
def test_stale_preflight_cannot_publish_an_acknowledgement(stopped_diagnostic, drift):
    case = stopped_diagnostic
    request = copy.deepcopy(case.diagnostic_request)
    preflight = diagnostic.preflight(case.store, "run-1", request)
    request.update(preflight_key=preflight["preflight_key"], acknowledgement=diagnostic.ACK)
    if drift == "source":
        (case.broker.checkout / case.api).write_text("Later source changes\n")
    elif drift == "owner":
        config = json.loads(case.store.config.path.read_text())
        config["capacity"] = 1
        case.store.config.path.write_text(json.dumps(config))
    else:
        path = case.registry.state / "resources/finalization.json"
        value = read_private(path)
        value["outcome"] = "delivered"
        write_private(path, value)
    with pytest.raises((ValueError, RuntimeError, OSError)):
        diagnostic.acknowledge(case.store, "run-1", request)
    with case.store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_native_diagnostics").fetchone()[0] == 0


def test_file_only_intent_is_inert_and_exact_acknowledgement_can_resume(
    stopped_diagnostic, monkeypatch
):
    case = stopped_diagnostic
    original = diagnostic._immutable

    def fail_after_file(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("owned crash before diagnostic DB acknowledgement")

    monkeypatch.setattr(diagnostic, "_immutable", fail_after_file)
    with pytest.raises(RuntimeError, match="owned crash"):
        ack(case)
    # No table acknowledgement means neither SQL nor file-only normal owners
    # gain/lose authority. Replay retains the same immutable intent bytes.
    assert not diagnostic.active(case.store, "run-1")
    registry = RunResources(case.spec)
    with registry.locked(read_only=True):
        pass
    monkeypatch.setattr(diagnostic, "_immutable", original)
    assert ack(case)[0]["state"] == "queued"


@pytest.mark.parametrize("drift", ["source", "symlink", "replaced", "intent", "failed_log"])
def test_current_authority_is_fresh_after_ack_and_before_any_native_launch(
    stopped_diagnostic,
    drift,
):
    case = stopped_diagnostic
    context, _row, _request, _response = running_context(case)
    if drift == "intent":
        value = read_private(context.root / "intent.json")
        value["command"]["check_id"] = "other"
        write_private(context.root / "intent.json", value)
        with pytest.raises(ValueError):
            DiagnosticContext(case.broker, _row)
        return
    context.create_subject()
    if drift in ("symlink", "replaced"):
        path = context.checkout
        saved = path.with_name("saved-original")
        path.rename(saved)
        if drift == "symlink":
            path.symlink_to(saved, target_is_directory=True)
        else:
            path.mkdir()
        with pytest.raises((ValueError, OSError)):
            context.subject()
        return
    if drift == "source":
        (case.broker.checkout / case.api).write_text("Changed original source\n")
    else:
        path = Path(case.spec["state_dir"]) / "checks/3/worker-regressions/native/process.log"
        path.write_text("Changed original failed check\n")
    with pytest.raises(ValueError):
        context.guard()


def test_unrelated_tracker_metadata_can_refresh_while_authority_fields_are_fenced(
    stopped_diagnostic,
):
    case = stopped_diagnostic
    ack(case)
    with case.store._connect() as db:
        db.execute(
            "UPDATE works SET issue=issue,repository=repository WHERE id=?", (case.spec["work_id"],)
        )
    with pytest.raises(ValueError, match=diagnostic.ERROR), case.store._connect() as db:
        db.execute("UPDATE works SET issue='https://github.com/example/fixture/issues/99'")


@pytest.mark.asyncio
async def test_public_http_preflight_ack_and_status_are_separate_from_delivery_acceptance(
    stopped_diagnostic,
):
    case = stopped_diagnostic
    app = create_app(case.store.config.path)
    host = case.store.config.dashboard_url
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url=host) as client:
        csrf = (await client.get("/api/session")).json()["csrf_token"]
        path = "/api/runs/run-1/"
        response = await client.post(
            path + "native-diagnostic-preflight", json=case.diagnostic_request
        )
        assert response.status_code == 200, response.text
        request = {
            **case.diagnostic_request,
            "preflight_key": response.json()["preflight_key"],
            "acknowledgement": diagnostic.ACK,
        }
        response = await client.post(
            path + "ack-native-diagnostic",
            json=request,
            headers={"Origin": host, "X-Devflow-CSRF": csrf},
        )
        assert response.status_code == 200, response.text
        assert response.json()["state"] == "queued"
        response = await client.get(path + "native-diagnostics/" + request["command_id"])
        assert response.status_code == 200 and response.json()["state"] == "queued"
