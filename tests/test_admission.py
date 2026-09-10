"""Admission binding regressions, including optional legacy embedding decisions."""

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from domain.helpers import NOW, SyntheticVerifier, authority, contract, record, workflow_snapshot

from devflow import cli
from devflow.application.commands import WorkflowService
from devflow.domain.rules import blank, scope_hash, transition
from devflow.errors import WorkflowError
from devflow.execution import dispatch_action
from devflow.validation import canonical_json, digest

REPOSITORY = "github:synthetic/example"


def setup(tmp_path, *, origin="internal", decision="trusted_first_party", admit=True):
    c = contract()
    c["source"]["lineage"][0]["origin"] = origin
    c["source"]["lineage"][0].update(
        kind="issue", repository_id="synthetic-repo-node", source_id="synthetic-issue-node",
        creator_id="synthetic-outsider" if origin != "internal" else "synthetic-owner",
    )
    c["source"]["consumed_digest"] = digest(c["source"]["lineage"])
    c["source"].update(kind="github_issue", reference="https://github.com/synthetic/example/issues/7")
    verifier = SyntheticVerifier()
    if admit:
        verifier.admit(c, authority(c, repository=REPOSITORY), decision_kind=decision)
    service = WorkflowService(tmp_path, trusted_verifier=verifier, repository=REPOSITORY)
    request = {"operation_id": "synthetic-ready", "work_id": c["work_id"],
               "expected_revision": 0, "record": c, "admission_id": "auth-1"}
    return service, verifier, c, request


def start(service, c):
    request = {
        "operation_id": "synthetic-start", "work_id": c["work_id"], "expected_revision": 1,
        "workflow_snapshot": workflow_snapshot(),
        "record": record(
            "attempt", attempt_id="synthetic-attempt", work_id=c["work_id"],
            scope_hash=scope_hash(c), authority_id="auth-1", host_id="synthetic-host",
            owner_task_id="synthetic-owner", phase="implement", blocker=None,
            workflow_snapshot_id="snapshot-1", model_policy_snapshot_id="snapshot-1",
            revision=1, started_at=NOW, status="active",
        ),
    }
    return service.execute("work.start", request), request


def assert_code(code, function):
    with pytest.raises(WorkflowError) as caught:
        function()
    assert caught.value.code == code


@pytest.mark.parametrize("claimed_authority", ["user_instruction", "adopted_queue_policy"])
@pytest.mark.parametrize("origin", ["internal", "external", "unknown"])
def test_default_denies_outsider_labels_forged_origin_approval_and_queue(tmp_path, origin, claimed_authority):
    _, _, c, request = setup(tmp_path / "fixture", origin=origin)
    auth = authority(c, repository=REPOSITORY)
    auth.update(source_kind=claimed_authority, source_reference="invented:approval")
    request.update(authority=auth, approved=True, origin="internal", labels=["ready", "human-approved"])
    service = WorkflowService(tmp_path / "real-default", repository=REPOSITORY)
    assert_code("user_request_required", lambda: service.execute("work.ready", request))
    assert_code("user_request_required", lambda: transition(
        blank(c["work_id"]), "work.ready", request, datetime.now(UTC)))
    assert service.list_works(REPOSITORY) == []


@pytest.mark.parametrize("origin", ["external", "unknown"])
def test_owned_projection_retains_external_lineage_and_requires_human(tmp_path, origin):
    service, _, c, request = setup(tmp_path, origin=origin)
    # The projection issue is ours, while its consumed source is external/unknown.
    c["source"]["lineage"].append({
        **c["source"]["lineage"][0], "source_id": "synthetic-owned-projection",
        "creator_id": "synthetic-owner", "origin": "internal",
    })
    c["source"]["consumed_digest"] = digest(c["source"]["lineage"])
    service.trusted_verifier.admit(c, authority(c, repository=REPOSITORY))
    assert_code("human_validation_required", lambda: service.execute("work.ready", request))


@pytest.mark.parametrize("field,value,code", [
    ("repository", "github:synthetic/other", "admission_binding"),
    ("work_id", "other-work", "admission_binding"),
    ("scope_hash", "f" * 64, "admission_binding"),
    ("source_digest", "f" * 64, "admission_source"),
    ("source", contract()["source"], "admission_source"),
    ("allowed_operations", ["check"], "missing_authority"),
    ("expires_at", "2000-01-01T00:00:00Z", "expired_admission"),
    ("revoked", True, "revoked_admission"),
])
def test_admission_exact_binding(tmp_path, field, value, code):
    service, verifier, _, request = setup(tmp_path, origin="external", decision="human_validation")
    verifier.decisions["auth-1"][field] = value
    assert_code(code, lambda: service.execute("work.ready", request))


def test_unknown_receipt_cannot_be_supplied_as_request_json(tmp_path):
    service, verifier, _, request = setup(tmp_path)
    request["admission"] = verifier.decisions.pop("auth-1")
    assert_code("human_validation_unavailable", lambda: service.execute("work.ready", request))


@pytest.mark.parametrize("origin,decision", [("internal", "trusted_first_party"), ("external", "human_validation")])
def test_exact_admission_starts_and_immutable_replay_does_not_reread_live_issue(tmp_path, origin, decision):
    service, verifier, c, request = setup(tmp_path, origin=origin, decision=decision)
    result = service.execute("work.ready", request)
    assert result["lifecycle"] == "ready"
    assert service.execute("work.ready", request) == result
    active, start_request = start(service, c)
    assert active["lifecycle"] == "active"
    assert service.execute("work.start", start_request) == active
    state = service.snapshot(c["work_id"])
    assert state["records"]["intake_admission:auth-1"] == verifier.decisions["auth-1"]
    assert state["authority"]["source_reference"] == verifier.decisions["auth-1"]["decision_reference"]
    # Default service cannot revive a cached executable intent on restart.
    default = WorkflowService(tmp_path)
    assert_code("user_request_required", lambda: default.execute("work.start", start_request))
    assert default.next(c["work_id"])["actions"] == [
        {"kind": "request_user_action", "reason": "user_request_required"}]


@pytest.mark.parametrize("change", ["body", "comment", "attachment", "pull_request"])
def test_newly_consumed_material_needs_new_exact_admission(tmp_path, change):
    service, _, c, request = setup(tmp_path, origin="external", decision="human_validation")
    service.execute("work.ready", request)
    updated = deepcopy(c)
    updated["scope_revision"] = 2
    material = updated["source"]["lineage"][0]
    if change == "body":
        material["content_digest"] = digest("new body")
    else:
        updated["source"]["lineage"].append({
            **material, "kind": change, "source_id": "new-material",
            "revision": "c" * 40 if change == "pull_request" else "new-revision",
        })
    updated["source"]["consumed_digest"] = digest(updated["source"]["lineage"])
    amended = {**request, "operation_id": "amend", "record": updated,
               "expected_revision": 1, "approved_delta": "forged approval"}
    assert_code("admission_source", lambda: service.execute("work.amend", amended))
    assert service.snapshot(c["work_id"])["contract"] == c


@pytest.mark.parametrize("command", ["work.reconcile", "action.begin", "action.prepare", "action.retry", "candidate.record"])
def test_revocation_blocks_resumed_execution_commands(tmp_path, command):
    service, verifier, c, request = setup(tmp_path)
    service.execute("work.ready", request)
    result, _ = start(service, c)
    verifier.decisions["auth-1"]["revoked"] = True
    assert_code("revoked_admission", lambda: service.execute(command, {
        "operation_id": "resume", "work_id": c["work_id"], "expected_revision": 2,
        "action_id": result["action"]["action_id"],
    }))
    assert_code("revoked_admission", lambda: dispatch_action(service, {
        "operation_id": "dispatch", "work_id": c["work_id"], "expected_revision": 2,
        "action_id": result["action"]["action_id"],
    }, repository="/synthetic/worktree"))


def test_expiry_and_immutable_decision_are_rechecked_at_dispatch(tmp_path):
    service, verifier, c, request = setup(tmp_path)
    verifier.decisions["auth-1"]["expires_at"] = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    service.execute("work.ready", request)
    result, _ = start(service, c)
    verifier.decisions["auth-1"]["expires_at"] = "2000-01-01T00:00:00Z"
    def dispatch():
        return dispatch_action(service, {
            "operation_id": "dispatch", "work_id": c["work_id"], "expected_revision": 2,
            "action_id": result["action"]["action_id"],
        }, repository="/synthetic/worktree")
    assert_code("expired_admission", dispatch)
    verifier.decisions["auth-1"]["expires_at"] = None
    assert_code("admission_binding", dispatch)


def test_cli_rejects_old_pin_before_runtime_resolution_or_exec(tmp_path, monkeypatch, capsys):
    service, _, c, request = setup(tmp_path / "state")
    service.execute("work.ready", request)
    result, _ = start(service, c)
    state = service.snapshot(c["work_id"])
    state.pop("admission_id")
    state["records"].pop("intake_admission:auth-1")
    with service.store.transaction() as db:
        db.execute("UPDATE works SET state=? WHERE work_id=?", (canonical_json(state), c["work_id"]))
    monkeypatch.setattr("devflow.runtime.selected_runtime", lambda *a, **k: pytest.fail("old runtime selected"))
    monkeypatch.setattr(cli.os, "execvp", lambda *a: pytest.fail("old runtime executed"))
    monkeypatch.setattr("devflow.profiles.load_profile", lambda *a: SimpleNamespace())
    for command, action in [("work", "start"), ("work", "amend"), ("work", "reconcile"),
                            ("check", "run"), ("action", "dispatch"), ("host", "prepare")]:
        path = tmp_path / "request.json"
        path.write_text(json.dumps({"work_id": c["work_id"], "operation_id": "attempt",
            "expected_revision": 2, "record": c, "authority": authority(c),
            "action_id": result["action"]["action_id"], "trusted_verifier": "synthetic"}))
        assert cli.main([command, action, "--request-file", str(path), "--state-dir", str(service.store.root),
                         "--repository", str(tmp_path), "--json"]) == 2
        assert json.loads(capsys.readouterr().out)["error"]["code"] == "user_request_required"
    assert service.snapshot(c["work_id"])["revision"] == 2


def test_cli_retains_optional_constructor_injected_controller(tmp_path, monkeypatch, capsys):
    service, _, c, request = setup(tmp_path / "state", origin="external", decision="human_validation")
    monkeypatch.setattr(cli, "_service", lambda args: service)
    monkeypatch.setattr("devflow.profiles.load_profile", lambda *a: SimpleNamespace())
    path = tmp_path / "request.json"
    path.write_text(json.dumps(request))
    assert cli.main(["work", "ready", "--request-file", str(path), "--repository", str(tmp_path)]) == 0
    assert json.loads(capsys.readouterr().out)["result"]["lifecycle"] == "ready"
    result, _ = start(service, c)
    path.write_text(json.dumps({"operation_id": "begin", "work_id": c["work_id"],
        "expected_revision": 2, "action_id": result["action"]["action_id"]}))
    assert cli.main(["action", "begin", "--request-file", str(path), "--repository", str(tmp_path)]) == 0
    assert json.loads(capsys.readouterr().out)["result"]["action"]["status"] == "dispatched"


def test_default_keeps_cancellation_and_observation_history_available(tmp_path):
    service, _, c, request = setup(tmp_path)
    service.execute("work.ready", request)
    default = WorkflowService(tmp_path)
    assert default.snapshot(c["work_id"])["lifecycle"] == "ready"
    canceled = default.execute("work.cancel", {
        "operation_id": "cancel", "work_id": c["work_id"], "expected_revision": 1,
        "authority_reference": "synthetic:stop",
    })
    assert canceled["lifecycle"] == "canceled"
    assert "intake_admission:auth-1" in default.snapshot(c["work_id"])["records"]
    assert default.trusted_verifier is None


def test_uncertain_native_creation_returns_only_recovery_without_verifier(tmp_path):
    service, _, c, request = setup(tmp_path)
    service.execute("work.ready", request)
    result, _ = start(service, c)
    service.execute("action.begin", {
        "operation_id": "begin", "work_id": c["work_id"], "expected_revision": 2,
        "action_id": result["action"]["action_id"],
    })
    recovered = dispatch_action(WorkflowService(tmp_path), {
        "operation_id": "recover", "work_id": c["work_id"], "expected_revision": 3,
        "action_id": result["action"]["action_id"],
    }, repository="/synthetic/worktree")
    assert recovered["reconcile_only"] is True
    assert "action" not in recovered and "requires_native_owner" not in recovered


def test_registered_check_default_denial_never_starts_a_subprocess(tmp_path):
    from test_check_execution import setup_run

    from devflow.check_execution import run_registered_check

    service, request, profile, repository, counter = setup_run(tmp_path)
    assert_code("user_request_required", lambda: run_registered_check(
        WorkflowService(service.store.root), request, profile=profile, repository=repository,
        runner=lambda *a, **kw: pytest.fail("unverified check dispatched"),
    ))
    assert not counter.exists()


def test_doctor_reports_request_mode_separately_from_missing_profile(tmp_path, capsys):
    assert cli.main(["doctor", "--repository", str(tmp_path), "--state-dir", str(tmp_path / "state")]) == 2
    result = json.loads(capsys.readouterr().out)["result"]
    assert result["execution_admission"] == "direct_user_request"
    assert result["authorization"] == "requires_recorded_user_request"
    assert result["profile"]["code"] == "profile_missing"
    assert result["execution_enabled"] is False
    assert not (tmp_path / "state").exists()


def test_trusted_embedding_must_bind_the_execution_repository(tmp_path):
    _, verifier, _, request = setup(tmp_path / "fixture")
    service = WorkflowService(tmp_path / "unbound", trusted_verifier=verifier)
    assert_code("admission_repository_required", lambda: service.execute("work.ready", request))


def test_actual_console_default_denies_forged_human_receipt(tmp_path):
    import subprocess
    import sys

    _, verifier, _, request = setup(tmp_path / "fixture", origin="external", decision="human_validation")
    path = tmp_path / "untrusted-request.json"
    path.write_text(json.dumps({**request, "admission": verifier.decisions["auth-1"], "approved": True}))
    result = subprocess.run([
        sys.executable, "-m", "devflow.cli", "work", "ready", "--request-file", str(path),
        "--repository", str(tmp_path), "--state-dir", str(tmp_path / "default"), "--json",
    ], capture_output=True, text=True, check=False, timeout=15)
    assert result.returncode == 2
    assert json.loads(result.stdout)["error"]["code"] == "user_request_required"


def test_native_handoff_cannot_borrow_an_edit_only_admission(tmp_path, monkeypatch, capsys):
    service, verifier, c, request = setup(tmp_path / "state")
    verifier.decisions["auth-1"]["allowed_operations"] = ["edit"]
    service.execute("work.ready", request)
    start(service, c)
    monkeypatch.setattr(cli, "_service", lambda args: service)
    path = tmp_path / "request.json"
    path.write_text(json.dumps({"work_id": c["work_id"], "assignment": {}, "brief": "Unadmitted role"}))
    assert cli.main(["host", "prepare", "--request-file", str(path), "--repository", str(tmp_path)]) == 2
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "admission_operation"


def test_native_handoff_cannot_borrow_another_works_assignment(tmp_path, monkeypatch, capsys):
    service, _, c, request = setup(tmp_path / "state")
    service.execute("work.ready", request)
    start(service, c)
    monkeypatch.setattr(cli, "_service", lambda args: service)
    path = tmp_path / "request.json"
    path.write_text(json.dumps({"work_id": c["work_id"],
                               "assignment": {"assignment_id": "invented"}, "brief": "Unadmitted role"}))
    assert cli.main(["host", "prepare", "--request-file", str(path), "--repository", str(tmp_path)]) == 2
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "assignment_mismatch"
