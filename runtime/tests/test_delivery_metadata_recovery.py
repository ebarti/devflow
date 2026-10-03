from __future__ import annotations

import asyncio
import hashlib
import json
import os
from datetime import timedelta

import pytest
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_metadata_recovery as metadata
from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture
def published(service, monkeypatch):
    store, request = service
    request["goal"] = "refactor(profile): share pure production and demo coaching policy"
    repository = store.config.raw["repositories"]["fixture"]
    repository["prepublish_checks"] = [
        {
            "id": "diff",
            "argv": ["git", "diff", "--check"],
            "cwd": ".",
            "kind": "check",
            "timeout_seconds": 30,
        }
    ]
    repository["checks"] = repository["prepublish_checks"]
    store.config.path.write_text(json.dumps(store.config.raw))
    store.submit(request)
    spec = store.spec("run-1")
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    broker.state_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    roles = []
    for iteration in range(2):
        (broker.checkout / "README.md").write_text(f"Owned content {iteration}\n")
        candidate = broker.candidate()
        role = {
            "role": "implement",
            "iteration": iteration,
            "status": "pass",
            "session_id": "original-session",
            "cleanup": "confirmed",
            "input_candidate_id": candidate["id"],
            "candidate": candidate,
        }
        roles.append(role)
        key = f"publish:run-1:{iteration}"
        broker._effect(
            key, "publish", {"iteration": iteration, "input_candidate_id": candidate["id"]}
        )
        _git(broker.checkout, "add", "README.md")
        _git(broker.checkout, "commit", "-qm", "Implement " + request["goal"])
        after = broker.candidate()
        pr = {
            "number": 7,
            "url": "https://example.invalid/pull/7",
            "state": "OPEN",
            "head": after["head"],
            "base": spec["base_sha"],
            "candidate": after,
        }
        broker._finish_effect(key, pr)
        with store._connect() as db:
            db.execute(
                "INSERT INTO delivery_attempts "
                "(job_key,run_id,role,iteration,candidate_id,state,session_id,"
                "result_json,cleanup) VALUES (?,?,?,?,?,'finished',?,?,'confirmed')",
                (
                    f"implementation-{iteration}",
                    "run-1",
                    "implement",
                    iteration,
                    candidate["id"],
                    "original-session",
                    canonical_json(role),
                ),
            )
    _git(broker.checkout, "push", "origin", "HEAD:refs/heads/feat/fixture")
    state = {
        "run_id": "run-1",
        "revision": 13,
        "iteration": 1,
        "phase": "blocked",
        "execution_state": "blocked",
        "outcome": "blocked",
        "cleanup": "none",
        "error": "repair limit exhausted",
        "candidate": after,
        "pull_request": pr,
        "candidate_revision": 4,
        "roles": roles,
        "checks": {},
        "usage": {},
        "findings": ["Historical browser rejection"],
        "tracker": {},
    }
    store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message=state["error"],
        candidate=after,
        pull_request=pr,
        checks={},
        iteration=1,
        protocol_revision=13,
        outcome="blocked",
        cleanup="none",
        error=state["error"],
    )
    closed = {
        "workflow_id": "delivery-run-1",
        "execution_run_id": "authentic-closed-run",
        "closed_at": "2026-10-03T20:00:00Z",
        "request_digest": spec["request_digest"],
        "recovery_digest": None,
        "result": state,
    }
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_args, **_kw: closed)
    title = {"value": request["goal"]}

    def existing(self, **_kwargs):
        remote = _git(self.source, "ls-remote", "origin", "refs/heads/feat/fixture").split()[0]
        return {
            "number": 7,
            "url": pr["url"],
            "state": "OPEN",
            "isDraft": False,
            "headRefName": spec["branch"],
            "baseRefName": "HEAD",
            "headRefOid": remote,
            "title": title["value"],
        }

    monkeypatch.setattr(DeliveryBroker, "_existing_pr", existing)
    original_run = metadata._run

    def run(argv, **kw):
        if argv[:3] == ["gh", "pr", "edit"]:
            title["value"] = argv[argv.index("--title") + 1]
            return ""
        return original_run(argv, **kw)

    monkeypatch.setattr(metadata, "_run", run)
    authority = {
        "decision_owner": "main task",
        "new_user_approval_required": False,
        "authority_source": "Existing authorized original publication repair",
        "scope": {
            "eligible_original_run_ids": ["run-1", "run-2"],
            "repository": "github.com/example/fixture",
            "known_base": spec["base_sha"],
            "max_reconciliation_commands_per_original_run": 1,
            "exact_duplicate_and_known_interrupted_effect_resume_allowed": True,
        },
    }
    path = store.config.state_root / "metadata-authority.json"
    path.write_text(json.dumps(authority))
    path.chmod(0o600)
    command = {
        "command_id": "metadata-1",
        "expected_revision": 13,
        "expected_candidate_id": after["id"],
        "expected_head": after["head"],
        "expected_pr_number": 7,
        "expected_signer": "Delivery Test <delivery@example.invalid>",
        "authority_path": str(path),
        "authority_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    return store, broker, state, closed, command, title


def test_public_metadata_reconciliation_preserves_each_tree_author_and_original_history(published):
    store, broker, state, _closed, command, title = published
    with store._connect() as db:
        attempts = list(db.execute("SELECT * FROM delivery_attempts"))
        effects = list(db.execute("SELECT * FROM delivery_effects"))
    response = store.reconcile_published_metadata("run-1", command)
    assert response["phase"] == "metadata_validation_queued"
    assert response == store.reconcile_published_metadata("run-1", command)
    assert response["head"] != state["candidate"]["head"]
    grant = metadata.read_private(broker.state_dir / "metadata-reconciliation/intent.json")
    assert len(grant["mapping"]) == 2
    for item in grant["mapping"]:
        for field in ("%T", "%an <%ae>", "%at", "%ai"):
            assert _git(broker.checkout, "show", "-s", "--format=" + field, item["old"]) == (
                _git(broker.checkout, "show", "-s", "--format=" + field, item["new"])
            )
        assert (
            _git(broker.checkout, "cat-file", "commit", item["old"]) + "\n"
            == (item["original_object"])
        )
        assert (
            _git(broker.checkout, "show", "-s", "--format=%s", item["new"]) == broker.spec["goal"]
        )
    broker._validate_publication_commits()
    assert title["value"] == broker.spec["goal"]
    assert (
        _git(broker.checkout, "rev-parse", "refs/devflow/metadata/run-1/original")
        == (state["candidate"]["head"])
    )
    assert broker.candidate()["content_sha256"] == state["candidate"]["content_sha256"]
    with store._connect() as db:
        assert list(db.execute("SELECT * FROM delivery_attempts")) == attempts
        assert list(db.execute("SELECT * FROM delivery_effects")) == effects
        assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
    assert recovery["state"] == state
    assert store.effective_spec("run-1") == broker.spec
    assert metadata.validation_readback(store, broker.spec, recovery)["head"] == response["head"]


@pytest.mark.parametrize("window", ["objects", "local", "push", "title", "receipt"])
def test_same_command_resumes_known_interrupted_metadata_effects(published, monkeypatch, window):
    store, broker, state, _closed, command, title = published
    seen = []
    original_git, original_run, original_immutable = (
        metadata._git,
        metadata._run,
        metadata._immutable,
    )

    def git(path, *args):
        result = original_git(path, *args)
        target = (
            (
                window == "objects"
                and args[:2] == ("update-ref", "refs/devflow/metadata/run-1/rewritten")
            )
            or (window == "local" and args[:2] == ("update-ref", "refs/heads/feat/fixture"))
            or (window == "push" and args[0] == "push")
        )
        if target and not seen:
            seen.append(True)
            raise RuntimeError("uncertain metadata completion")
        return result

    def run(argv, **kw):
        result = original_run(argv, **kw)
        if window == "title" and argv[:3] == ["gh", "pr", "edit"] and not seen:
            seen.append(True)
            raise RuntimeError("uncertain metadata completion")
        return result

    def immutable(path, value):
        result = original_immutable(path, value)
        if window == "receipt" and path.name == "publication.json" and not seen:
            seen.append(True)
            raise RuntimeError("uncertain metadata completion")
        return result

    if window == "title":
        title["value"] = "Implement legacy PR title"
    monkeypatch.setattr(metadata, "_git", git)
    monkeypatch.setattr(metadata, "_run", run)
    monkeypatch.setattr(metadata, "_immutable", immutable)
    with pytest.raises(RuntimeError, match="uncertain"):
        store.reconcile_published_metadata("run-1", command)
    response = store.reconcile_published_metadata("run-1", command)
    assert seen and response == store.reconcile_published_metadata("run-1", command)
    assert (
        _git(broker.checkout, "rev-parse", "refs/devflow/metadata/run-1/original")
        == (state["candidate"]["head"])
    )
    assert (
        len(_git(broker.checkout, "rev-list", broker.spec["base_sha"] + "..HEAD").splitlines()) == 2
    )


@pytest.mark.parametrize(
    "drift",
    [
        "active",
        "candidate",
        "remote",
        "pr",
        "signer",
        "authority",
        "foreign_commit",
        "changed_receipt",
        "merged",
        "issue",
    ],
)
def test_metadata_precheck_rejects_drift_without_rewrite(published, monkeypatch, drift):
    store, broker, state, closed, command, _title = published
    if drift == "active":
        closed["result"]["outcome"] = None
    elif drift == "candidate":
        (broker.checkout / "README.md").write_text("Unauthorized later edit\n")
    elif drift == "remote":
        _git(
            broker.source,
            "push",
            "--force",
            "origin",
            broker.spec["base_sha"] + ":refs/heads/feat/fixture",
        )
    elif drift == "pr":
        command["expected_pr_number"] = 8
    elif drift == "signer":
        command["expected_signer"] = "Other <other@example.invalid>"
    elif drift == "authority":
        command["authority_sha256"] = "a" * 64
    elif drift == "foreign_commit":
        _git(broker.checkout, "commit", "--allow-empty", "-qm", "Foreign commit")
        candidate = broker.candidate()
        state["candidate"] = candidate
        command.update(expected_head=candidate["head"], expected_candidate_id=candidate["id"])
    elif drift == "changed_receipt":
        with store._connect() as db:
            db.execute("UPDATE delivery_effects SET observed_json='{}' WHERE kind='publish'")
    elif drift == "merged":
        original = DeliveryBroker._existing_pr

        def existing(self, **kw):
            value = original(self, **kw)
            value["state"] = "MERGED"
            return value

        monkeypatch.setattr(DeliveryBroker, "_existing_pr", existing)
    elif drift == "issue":
        with store._connect() as db:
            db.execute("UPDATE works SET issue='https://github.com/example/fixture/issues/999'")
    old_head = _git(broker.checkout, "rev-parse", "HEAD")
    with pytest.raises((ValueError, KeyError)):
        store.reconcile_published_metadata("run-1", command)
    assert _git(broker.checkout, "rev-parse", "HEAD") == old_head
    assert not _git(broker.checkout, "for-each-ref", "refs/devflow/metadata")
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_metadata_recoveries").fetchone()[0] == 0


@pytest.mark.parametrize(
    "field,value", [("command_id", 1), ("expected_revision", True), ("expected_pr_number", 7.0)]
)
def test_metadata_request_rejects_untyped_identity_without_effects(published, field, value):
    store, broker, _state, _closed, command, _title = published
    with pytest.raises(ValueError, match="fields or identity"):
        store.reconcile_published_metadata("run-1", {**command, field: value})
    assert not _git(broker.checkout, "for-each-ref", "refs/devflow/metadata")


def test_metadata_effect_guard_rejects_numeric_type_change_in_sealed_attempt(published):
    store, broker, _state, _closed, command, _title = published
    grant = metadata._snapshot(store, "run-1", command)
    grant["attempts"][0]["iteration"] = float(grant["attempts"][0]["iteration"])
    with pytest.raises(ValueError, match="stopped run history changed"):
        metadata._guard(store, grant)
    assert not _git(broker.checkout, "for-each-ref", "refs/devflow/metadata")


def test_explicit_command_bound_and_immutable_staged_receipt_recovery(published, monkeypatch):
    store, broker, _state, _closed, command, _title = published
    root = broker.state_dir / "metadata-reconciliation"
    root.mkdir(mode=0o700)
    value = {"bounded": "receipt"}
    path = root / "controlled.json"
    original_unlink = metadata.Path.unlink
    stopped = []

    def unlink(self, *args, **kwargs):
        if self.name.startswith(".stage-controlled") and not stopped:
            stopped.append(True)
            raise RuntimeError("interrupted staged link cleanup")
        return original_unlink(self, *args, **kwargs)

    monkeypatch.setattr(metadata.Path, "unlink", unlink)
    with pytest.raises(RuntimeError, match="interrupted"):
        metadata._immutable(path, value)
    assert path.stat().st_nlink == 2
    metadata._immutable(path, value)
    assert path.stat().st_nlink == 1
    os.link(path, root / "foreign-hardlink")
    with pytest.raises(ValueError):
        metadata._immutable(path, value)
    store.reconcile_published_metadata("run-1", command)
    with pytest.raises(ValueError, match="already received"):
        store.reconcile_published_metadata("run-1", {**command, "command_id": "metadata-2"})
    with pytest.raises(ValueError, match="different inputs"):
        store.reconcile_published_metadata("run-1", {**command, "expected_revision": 14})


@pytest.mark.parametrize("failed_gate", [False, True])
def test_metadata_workflow_runs_deterministic_gates_without_any_provider_turn(
    published, monkeypatch, failed_gate
):
    store, broker, _state, _closed, command, _title = published
    store.reconcile_published_metadata("run-1", command)
    with store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
    flow = DeliveryWorkflow()
    called = []

    async def project(*_args):
        return None

    async def execute(name, request, **kwargs):
        called.append(name)
        assert name != "delivery_role"
        if name == "delivery_tracker_start":
            return {"state": "consistent"}
        return {"state": "failed" if failed_gate and name == "delivery_checks" else "passed"}

    monkeypatch.setattr(flow, "_project", project)
    monkeypatch.setattr(flow, "_activity", execute)
    result = asyncio.run(flow._resume_metadata(broker.spec, recovery))
    assert result["roles"] == recovery["state"]["roles"]
    assert result["outcome"] == "blocked"
    assert result["iteration"] == 1
    assert called == [
        "delivery_metadata_readback",
        "delivery_tracker_start",
        "delivery_precheck",
        "delivery_checks",
    ]
    assert result["error"] == (
        "repair limit exhausted"
        if failed_gate
        else "metadata reconciled; independent source assessment remains incomplete"
    )


def test_pending_intent_survives_interrupted_admission_without_recapturing_mapping(
    published, monkeypatch
):
    store, broker, _state, _closed, command, _title = published
    root = broker.state_dir / "metadata-reconciliation"
    root.mkdir(mode=0o700)
    original = metadata._snapshot(store, "run-1", command)
    metadata._immutable(root / "intent.json", original)
    monkeypatch.setattr(
        metadata, "_snapshot", lambda *_a: pytest.fail("must retain pending intent")
    )
    response = store.reconcile_published_metadata("run-1", command)
    assert response["head"] == original["new_head"]
    assert metadata.read_private(root / "intent.json") == original


@pytest.mark.asyncio
async def test_real_temporal_metadata_successor_uses_production_activities_and_zero_provider_roles(
    published,
):
    from temporalio.testing import WorkflowEnvironment
    from temporalio.worker import Worker

    from devflow_temporal.delivery_activities import (
        delivery_checks,
        delivery_metadata_readback,
        delivery_precheck,
        delivery_project,
        delivery_tracker_start,
    )

    store, broker, _state, _closed, command, _title = published
    response = store.reconcile_published_metadata("run-1", command)
    with store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
        before = db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0]
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(
            environment.client,
            task_queue="metadata-real",
            workflows=[DeliveryWorkflow],
            activities=[
                delivery_metadata_readback,
                delivery_tracker_start,
                delivery_precheck,
                delivery_checks,
                delivery_project,
            ],
        ):
            result = await environment.client.execute_workflow(
                DeliveryWorkflow.run,
                args=[broker.spec, recovery],
                id=response["workflow_id"],
                task_queue="metadata-real",
                execution_timeout=timedelta(seconds=45),
            )
    assert result["outcome"] == "blocked"
    assert (
        result["error"] == "metadata reconciled; independent source assessment remains incomplete"
    )
    assert result["checks"]["local"]["state"] == "passed"
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == before
    assert store.detail("run-1")["metadata_reconciliation"]["provider_turns"] == 0
