import json
import signal
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from devflow.adapters.git import GitRepository
from devflow.application.commands import WorkflowService
from devflow.check_execution import run_registered_check
from devflow.domain.rules import scope_hash
from devflow.errors import WorkflowError
from devflow.profiles import load_profile


def setup_run(tmp_path, *, kill_parent=False):
    repository = tmp_path / "repo"
    repository.mkdir()
    for args in (
        ["init", "-b", "synthetic"],
        ["config", "user.name", "Synthetic Fixture"],
        ["config", "user.email", "synthetic@example.invalid"],
    ):
        subprocess.run(["git", "-C", str(repository), *args], check=True, capture_output=True)
    counter = tmp_path / "runner-invocations.txt"
    script = f"from pathlib import Path; p=Path({str(counter)!r}); p.write_text(p.read_text()+'run\\n' if p.exists() else 'run\\n')"
    if kill_parent:
        script += "; import os, signal; os.kill(os.getppid(), signal.SIGKILL)"
    profile_dir = repository / ".devflow"
    profile_dir.mkdir()
    identity = GitRepository(repository).identity()
    (profile_dir / "repository.toml").write_text(
        "schema_version=1\n[repository]\nid="
        + json.dumps(identity)
        + '\ndefault_branch="synthetic"\n'
    )
    (profile_dir / "workflow.lock").write_text(
        'schema_version=1\nversion="0.1.0"\nrevision="' + "a" * 40 + '"\n'
    )
    (profile_dir / "checks.toml").write_text(
        'schema_version=1\n[checks.synthetic]\nkind="static"\ndescription="Synthetic run count"\nargv='
        + json.dumps([sys.executable, "-c", script])
        + "\n"
    )
    subprocess.run(["git", "-C", str(repository), "add", "."], check=True, capture_output=True)
    subprocess.run(
        ["git", "-C", str(repository), "commit", "-m", "test: synthetic check fixture"],
        check=True,
        capture_output=True,
    )
    profile = load_profile(repository)
    service = WorkflowService(tmp_path / "state")
    contract = json.loads(
        (Path(__file__).parents[1] / "docs/design/work-contract.example.json").read_text()
    )
    contract.update(
        work_id="synthetic-work",
        risk={"tier": 0, "reason": "Synthetic static command"},
        endpoint={"kind": "local", "target": str(repository.resolve())},
    )
    contract["verification"] = {
        "recipes": ["synthetic"],
        "scenarios": [],
        "documentation_owners": [],
    }
    stamp = datetime.now(UTC).isoformat()
    auth = {
        "schema_version": 1,
        "record_type": "authority",
        "authority_id": "auth-1",
        "source_kind": "user_instruction",
        "source_reference": "synthetic:user-request",
        "work_id": contract["work_id"],
        "scope_hash": scope_hash(contract),
        "repository": identity,
        "allowed_operations": ["edit", "check"],
        "expires_at": None,
        "revoked": False,
    }
    service.execute(
        "work.ready",
        {
            "operation_id": "ready",
            "work_id": contract["work_id"],
            "expected_revision": 0,
            "record": contract,
            "authority": auth,
        },
    )
    snapshot = {
        "schema_version": 1,
        "record_type": "workflow_snapshot",
        "snapshot_id": "snapshot-1",
        "package_version": "0.1.0",
        "package_revision": "a" * 40,
        "workflow_hash": "a" * 64,
        "model_policy_hash": "a" * 64,
        "instruction_sources": [{"reference": "synthetic:instructions", "hash": "a" * 64}],
        "repository_profile_reference": "sha256:" + profile.fingerprint,
        "effective_settings_reference": "synthetic:settings",
        "captured_at": stamp,
    }
    attempt = {
        "schema_version": 1,
        "record_type": "attempt",
        "attempt_id": "attempt-1",
        "work_id": contract["work_id"],
        "scope_hash": scope_hash(contract),
        "authority_id": "auth-1",
        "host_id": "synthetic-host",
        "owner_task_id": "synthetic-owner",
        "phase": "implement",
        "blocker": None,
        "workflow_snapshot_id": "snapshot-1",
        "model_policy_snapshot_id": "snapshot-1",
        "revision": 1,
        "started_at": stamp,
        "status": "active",
    }
    service.execute(
        "work.start",
        {
            "operation_id": "start",
            "work_id": contract["work_id"],
            "expected_revision": 1,
            "record": attempt,
            "workflow_snapshot": snapshot,
        },
    )
    observed = GitRepository(repository).observe()
    candidate = {
        "schema_version": 1,
        "record_type": "candidate",
        "candidate_id": "candidate-1",
        "attempt_id": "attempt-1",
        "scope_hash": scope_hash(contract),
        "repository": identity,
        "base_sha": observed["head_sha"],
        "head_sha": observed["head_sha"],
        "tree_sha": observed["tree_sha"],
        "clean": True,
        "dependency_hash": "a" * 64,
        "environment_hash": "b" * 64,
        "created_at": stamp,
    }
    service.execute(
        "candidate.record",
        {
            "operation_id": "candidate",
            "work_id": contract["work_id"],
            "expected_revision": 2,
            "record": candidate,
        },
    )
    request = {
        "operation_id": "logical-check",
        "work_id": contract["work_id"],
        "expected_revision": 3,
        "recipe_id": "synthetic",
        "acceptance_ids": [a["id"] for a in contract["acceptance"]],
    }
    return service, request, profile, repository, counter


def test_completed_replay_does_not_run_the_real_check_again(tmp_path):
    service, request, profile, repository, counter = setup_run(tmp_path)
    first = run_registered_check(service, request, profile=profile, repository=repository)
    assert first["evidence"]["execution_status"] == "PASS"
    assert counter.read_text().splitlines() == ["run"]
    second = run_registered_check(service, request, profile=profile, repository=repository)
    assert second == first
    assert counter.read_text().splitlines() == ["run"]
    assert (
        len(
            [
                r
                for r in service.snapshot(request["work_id"])["records"].values()
                if r["record_type"] == "check_evidence"
            ]
        )
        == 1
    )


def test_reused_request_id_with_changed_payload_rejected_before_runner(tmp_path):
    service, request, profile, repository, counter = setup_run(tmp_path)
    run_registered_check(service, request, profile=profile, repository=repository)
    with pytest.raises(WorkflowError, match="different check request"):
        run_registered_check(
            service, {**request, "acceptance_ids": []}, profile=profile, repository=repository
        )
    assert counter.read_text().splitlines() == ["run"]


def test_stale_initial_revision_does_not_run_or_prepare(tmp_path):
    service, request, profile, repository, counter = setup_run(tmp_path)
    with pytest.raises(WorkflowError, match="before check admission"):
        run_registered_check(
            service, {**request, "expected_revision": 2}, profile=profile, repository=repository
        )
    assert not counter.exists()
    assert service.store.operation(request["operation_id"]) is None


def test_real_subprocess_death_leaves_dispatched_check_and_refuses_rerun(tmp_path):
    service, request, profile, repository, counter = setup_run(tmp_path, kill_parent=True)
    request_path = tmp_path / "request.json"
    request_path.write_text(json.dumps(request))
    script = """
import json, sys
from pathlib import Path
from devflow.application.commands import WorkflowService
from devflow.check_execution import run_registered_check
from devflow.profiles import load_profile
repository=Path(sys.argv[1])
run_registered_check(WorkflowService(Path(sys.argv[2])),json.loads(Path(sys.argv[3]).read_text()),profile=load_profile(repository),repository=repository)
"""
    child = subprocess.run(
        [sys.executable, "-c", script, str(repository), str(service.store.root), str(request_path)],
        capture_output=True,
        timeout=15,
    )
    assert child.returncode == -signal.SIGKILL
    assert counter.read_text().splitlines() == ["run"]
    state = service.snapshot(request["work_id"])
    check_action = next(a for a in state["actions"].values() if a["operation"] == "run_check")
    assert check_action["status"] == "dispatched"
    with pytest.raises(WorkflowError, match="requires reconciliation"):
        run_registered_check(service, request, profile=profile, repository=repository)
    assert counter.read_text().splitlines() == ["run"]
    assert not any(
        r["record_type"] == "check_evidence"
        for r in service.snapshot(request["work_id"])["records"].values()
    )


def test_concurrent_revision_advance_retains_draft_without_accepting_stale_proof(tmp_path):
    service, request, profile, repository, counter = setup_run(tmp_path)

    def runner(*args, **kwargs):
        completed = subprocess.run(*args, **kwargs)
        state = service.snapshot(request["work_id"])
        service.execute(
            "work.reconcile",
            {
                "operation_id": "concurrent-writer",
                "work_id": request["work_id"],
                "expected_revision": state["revision"],
            },
        )
        return completed

    with pytest.raises(WorkflowError, match="current revision"):
        run_registered_check(
            service, request, profile=profile, repository=repository, runner=runner
        )
    assert counter.read_text().splitlines() == ["run"]
    drafts = list((service.store.root / "check-runs").glob("*.result.json"))
    assert len(drafts) == 1
    draft = json.loads(drafts[0].read_text())
    service.store.require_artifact(draft["record"]["artifact_hash"])
    assert not any(
        r["record_type"] == "check_evidence"
        for r in service.snapshot(request["work_id"])["records"].values()
    )
    with pytest.raises(WorkflowError, match="requires reconciliation"):
        run_registered_check(service, request, profile=profile, repository=repository)
    assert counter.read_text().splitlines() == ["run"]


def test_check_that_changes_git_candidate_is_recorded_blocked(tmp_path):
    service, request, profile, repository, counter = setup_run(tmp_path)

    def runner(*args, **kwargs):
        completed = subprocess.run(*args, **kwargs)
        (repository / "unexpected-drift.txt").write_text("Synthetic unexpected check output")
        return completed

    result = run_registered_check(
        service, request, profile=profile, repository=repository, runner=runner
    )
    assert result["evidence"]["execution_status"] == "BLOCKED"
    assert any("candidate_drift" in text for text in result["evidence"]["observations"])
    assert counter.read_text().splitlines() == ["run"]
    action = next(
        a
        for a in service.snapshot(request["work_id"])["actions"].values()
        if a["operation"] == "run_check"
    )
    assert action["status"] == "confirmed"
    assert action["observation"]["checkout_verified"] is False


def test_concurrent_identical_requests_share_one_completed_execution(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    service, request, profile, repository, counter = setup_run(tmp_path)

    def invoke(_):
        return run_registered_check(
            WorkflowService(service.store.root), request, profile=profile, repository=repository
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(invoke, [1, 2]))
    assert results[0] == results[1]
    assert counter.read_text().splitlines() == ["run"]


def test_result_draft_flush_failure_preserves_previous_result(tmp_path, monkeypatch):
    import devflow.check_execution as execution

    previous = execution._write_draft(tmp_path, "synthetic", {"result": "retained"})

    def fail(_):
        raise OSError("synthetic result flush failure")

    monkeypatch.setattr(execution, "flush_descriptor", fail)
    with pytest.raises(OSError, match="result flush failure"):
        execution._write_draft(tmp_path, "synthetic", {"result": "replacement"})
    assert json.loads(previous.read_text()) == {"result": "retained"}
    assert list(tmp_path.iterdir()) == [previous]


def test_sigkill_after_result_draft_publication_leaves_recoverable_json(tmp_path):
    code = '''
import os, signal, sys
from pathlib import Path
from devflow.check_execution import _write_draft
_write_draft(Path(sys.argv[1]), "synthetic", {"result": "retained"})
os.kill(os.getpid(), signal.SIGKILL)
'''
    result = subprocess.run([sys.executable, "-c", code, str(tmp_path)], timeout=30)
    assert result.returncode == -signal.SIGKILL
    assert json.loads((tmp_path / "synthetic.result.json").read_text()) == {"result": "retained"}
