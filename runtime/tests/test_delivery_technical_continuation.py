from __future__ import annotations

import asyncio
import fcntl
import hashlib
import sys
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import pytest
from temporalio import workflow
from temporalio.exceptions import ApplicationError
from test_delivery_gates_admission import stopped as stopped
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_native_renewal import payload_update as payload_update
from test_delivery_resources import spec as resource_spec
from test_delivery_store import service as service
from test_delivery_store import submit_historical_admission

from devflow_temporal import delivery_native_renewal as renewal
from devflow_temporal import delivery_technical_continuation as technical
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_resources import RunResources, read_private, write_private
from devflow_temporal.delivery_workflow import DeliveryWorkflow


def request():
    return {
        "continuation_kind": "accepted_technical_successor",
        "command_id": "technical-1",
        "expected_revision": 27,
        "expected_iteration": 4,
        "expected_candidate_id": "a" * 64,
        "expected_pr_number": 1047,
        "expected_pr_head": "b" * 40,
        "additional_iterations": 0,
        "authority_path": "/absent/authority.json",
        "authority_sha256": "c" * 64,
        "expected_source_revision": "d" * 40,
    }


@pytest.mark.skipif(sys.platform != "darwin", reason="actual native process/lease observation")
@pytest.mark.parametrize("adverse", [None, "unknown", "lease", "actor-inventory", "journal-alias"])
def test_unknown_closure_observes_real_closed_native_actor_without_normalizing_history(
    tmp_path,
    adverse,
):
    from devflow_temporal.delivery_native_process import NativeProcess

    spec = resource_spec(tmp_path)
    registry = RunResources(spec)
    scratch = registry.scratch("check", "retained")
    actor = NativeProcess(
        spec,
        Path(spec["state_dir"]) / "check-actor",
        argv=[sys.executable, "-c", "print('owned check')"],
        cwd=scratch,
        environment={"PATH": "/usr/bin:/bin"},
        timeout=5,
    )
    assert actor.run()["cleanup"] == "observed-native-confirmed"
    receipt = registry.finalize("blocked", uncertain=True)
    assert receipt["state"] == "unknown"
    before = {p.name: p.read_bytes() for p in (registry.manifest, Path(receipt["receipt"]))}
    lock = None
    try:
        if adverse == "lease":
            lock = (actor.folder / "native-process.lock").open("rb")
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        elif adverse == "actor-inventory":
            value = read_private(Path(receipt["receipt"]))
            value["processes"][0]["observed_pids"] = []
            write_private(Path(receipt["receipt"]), value)
        elif adverse == "journal-alias":
            original = actor.journal.with_name("original.json")
            actor.journal.rename(original)
            actor.journal.symlink_to(original)
        if adverse:
            with pytest.raises(ValueError):
                technical._observe_resources(spec, unknown_allowed=adverse != "unknown")
        else:
            observed = technical._observe_resources(spec, unknown_allowed=True)
            assert len(observed["journal_sha256"]) == 1 and scratch.exists()
            assert {
                p.name: p.read_bytes() for p in (registry.manifest, Path(receipt["receipt"]))
            } == before

    finally:
        if lock:
            lock.close()




@pytest.mark.parametrize(
    "change",
    ["missing", "wrong-hash", "unknown-kind", "grant", "bool", "extra-root", "partial-packet"],
)
def test_public_technical_preflight_refuses_before_intent_claim_archive_or_effect(service, change):
    store, submitted = service
    store.submit(submitted)
    payload = request()
    if change == "missing":
        payload.pop("authority_path")
    elif change == "wrong-hash":
        authority = store.config.state_root / "technical-authority.json"
        write_private(authority, {"decision_owner": "main task"})
        payload["authority_path"] = str(authority)
    elif change == "unknown-kind":
        payload["continuation_kind"] = "ordinary"
    elif change == "grant":
        payload["additional_iterations"] = 1
    elif change == "bool":
        payload["additional_iterations"] = False
    elif change == "extra-root":
        payload["evidence_root"] = str(store.config.state_root / "foreign")
    else:
        payload["prospective_path"] = "/absent/prospective.json"
    with store._connect() as db:
        claim = store.state.claim_for(db, "work-1")
        commands = list(db.execute("SELECT * FROM delivery_commands"))
    for operation in (store.repair_admission_preflight, store.continue_repair):
        with pytest.raises((ValueError, FileNotFoundError)):
            operation("run-1", payload)
    assert not (Path(store.spec("run-1")["state_dir"]) / "technical-successor").exists()
    with store._connect() as db:
        assert store.state.claim_for(db, "work-1") == claim
        assert list(db.execute("SELECT * FROM delivery_commands")) == commands
        assert db.execute("SELECT COUNT(*) FROM delivery_technical_successors").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM delivery_effects").fetchone()[0] == 0


@pytest.mark.parametrize("stage", ["review", "checks"])
@pytest.mark.parametrize("point", ["preflight", "role"])
def test_inherited_confirmed_blocked_checkpoint_does_not_freeze_fresh_technical_cancel(
    service,
    monkeypatch,
    stage,
    point,
):
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", lambda _: True)
    store, submitted = service
    submit_historical_admission(store, submitted, monkeypatch)
    spec = store.spec("run-1")
    candidate = {"id": "a" * 64, "head": "b" * 40}
    published = {"number": 7, "head": candidate["head"], "candidate": candidate}
    checkpoint = {
        "event": "blocked",
        "phase": "blocked",
        "execution_state": "blocked",
        "outcome": "blocked",
        "status": "blocked",
        "release": True,
        "state": "confirmed",
        "waiting": False,
        "attempts": 1,
        "cycles": 1,
    }
    state = {
        "run_id": "run-1",
        "iteration": 4,
        "revision": 27,
        "candidate_revision": 1,
        "candidate": candidate,
        "pull_request": published,
        "roles": [],
        "findings": [],
        "usage": {},
        "tracker": {},
        "cleanup": "confirmed",
        "outcome": "blocked",
        "phase": "blocked",
        "checks": {"prepublish": {"state": "passed"}, "terminal_tracker_checkpoint": checkpoint},
    }
    recovery = {
        "kind": "accepted_technical_successor",
        "execution_spec": spec,
        "maximum_iteration": 4,
        "command": {"additional_iterations": 0},
        "state": state,
        "resume_stage": stage,
        "candidate": candidate,
        "publication": published,
        "session_id": "original",
    }
    frozen = deepcopy(recovery)
    flow = DeliveryWorkflow()
    cancelled = []

    async def wait(predicate, **_kw):
        assert predicate()

    async def project(*_a, **_kw):
        return None

    async def execute(name, body, **_kw):
        if not cancelled and name == (
            "delivery_technical_readback" if point == "preflight" else "delivery_role"
        ):
            assert flow.state["outcome"] is None
            result = await flow.cancel(
                {
                    "expected_revision": flow.state["revision"],
                    "reason": "cancel fresh owning continuation",
                }
            )
            assert result["phase"] == "cancelling"
            cancelled.append(name)
        if name == "delivery_tracker_start":
            return {"state": "consistent"}
        if name == "delivery_role":
            return {
                "role": body["role"],
                "iteration": 4,
                "status": "pass",
                "candidate": candidate,
                "cleanup": "confirmed",
                "session_id": "independent",
                "findings": [],
            }
        return {"state": "passed", "cleanup": "confirmed"}

    monkeypatch.setattr(workflow, "wait_condition", wait)
    monkeypatch.setattr(flow, "_project", project)
    monkeypatch.setattr(flow, "_activity", execute)
    result = asyncio.run(flow.run(spec, recovery))
    assert cancelled and result["outcome"] == "cancelled"
    assert "terminal_tracker_checkpoint" not in result["checks"]
    if stage == "review":
        assert result["checks"]["prepublish"] == frozen["state"]["checks"]["prepublish"]
    else:
        assert result["checks"]["prepublish"] == {"state": "passed", "cleanup": "confirmed"}
    assert recovery == frozen


def test_new_owning_terminal_checkpoint_still_freezes_cancellation(service, monkeypatch):
    store, submitted = service
    store.submit(submitted)
    spec = {**store.spec("run-1"), "terminal_tracker_version": 1}
    flow = DeliveryWorkflow()
    flow.state = {
        "phase": "delivered",
        "execution_state": "terminal",
        "outcome": "delivered",
        "revision": 30,
        "iteration": 4,
        "checks": {},
        "roles": [],
        "cleanup": "confirmed",
    }

    async def wait(predicate, **_kw):
        assert predicate()

    async def execute(*_a, **_kw):
        return {
            "state": "confirmed",
            "process_cleanup": "observed-native-confirmed",
            "resource_cleanup": "confirmed",
        }

    async def pending(_spec, checkpoint):
        assert checkpoint["event"] == "delivered" and checkpoint["status"] == "in-review"
        flow.state.update(phase="waiting_tracker", execution_state="waiting_tracker", outcome=None)
        with pytest.raises(ApplicationError, match="terminal transition is frozen"):
            await flow.cancel({"expected_revision": flow.state["revision"], "reason": "too late"})
        return False

    monkeypatch.setattr(workflow, "now", lambda: datetime(2026, 10, 4, tzinfo=UTC))
    monkeypatch.setattr(workflow, "patched", lambda _name: True)
    monkeypatch.setattr(workflow, "wait_condition", wait)
    monkeypatch.setattr(flow, "_activity", execute)
    monkeypatch.setattr(flow, "_finish_terminal_tracker", pending)
    asyncio.run(flow._project(spec, "delivered", "fresh terminal transition"))
    assert flow.state["checks"]["terminal_tracker_checkpoint"]["event"] == "delivered"
    assert flow.cancel_requested is False


@pytest.mark.parametrize("stage", ["review", "checks"])
@pytest.mark.parametrize("failure", [None, "review", "browser"])
def test_published_technical_checkpoint_never_implements_or_republishes(
    service,
    monkeypatch,
    stage,
    failure,
):
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", lambda _: True)
    store, submitted = service
    submit_historical_admission(store, submitted, monkeypatch)
    spec = store.spec("run-1")
    spec["policy"]["browser_qa"] = {"argv": ["fixture"]}
    candidate = {"id": "a" * 64, "head": "b" * 40}
    publication = {"head": candidate["head"], "candidate": candidate, "number": 7}
    old_checks = {"prepublish": {"state": "passed", "candidate_id": candidate["id"]}}
    state = {
        "run_id": "run-1",
        "iteration": 4,
        "revision": 27,
        "candidate_revision": 1,
        "candidate": candidate,
        "pull_request": publication,
        "roles": [],
        "checks": old_checks,
        "tracker": {},
        "usage": {},
        "findings": [],
        "outcome": "blocked",
        "phase": "blocked",
        "cleanup": "confirmed",
    }
    recovery = {
        "kind": "accepted_technical_successor",
        "execution_spec": spec,
        "maximum_iteration": 4,
        "command": {"additional_iterations": 0},
        "state": state,
        "resume_stage": stage,
        "candidate": candidate,
        "publication": publication,
        "session_id": "original",
    }
    flow, calls = DeliveryWorkflow(), []

    async def project(*_a):
        return None

    async def execute(name, body, **_kw):
        calls.append((name, body))
        assert name not in {"delivery_publish", "delivery_prepare"}
        if name == "delivery_role":
            assert body["role"] in {"review", "verify"}
            assert body["iteration"] == 4
            return {
                "role": body["role"],
                "iteration": 4,
                "status": "fail" if failure == body["role"] else "pass",
                "candidate": candidate,
                "cleanup": "confirmed",
                "session_id": "independent",
                "findings": ["fresh finding"] if failure == body["role"] else [],
            }
        if name in {"delivery_tracker_start", "delivery_tracker"}:
            return {"state": "consistent"}
        if name == "delivery_browser_qa":
            return {
                "state": "failed" if failure == "browser" else "passed",
                "cleanup": "confirmed",
                "receipt": "receipt",
                "receipt_sha256": "a" * 64,
                "log": "log",
                "log_sha256": "b" * 64,
                "results": [],
            }
        if name == "delivery_ci":
            assert body["pull_request"] == publication
        return {"state": "passed", "cleanup": "confirmed"}

    monkeypatch.setattr(flow, "_project", project)
    monkeypatch.setattr(flow, "_activity", execute)
    result = asyncio.run(flow.run(spec, recovery))
    assert result["iteration"] == 4
    assert result["outcome"] == ("blocked" if failure else "delivered")
    assert all(body.get("role") != "implement" for _name, body in calls)
    assert sum(name == "delivery_precheck" for name, _body in calls) == (stage == "checks")
    assert result["pull_request"] == publication
    if stage == "review":
        assert result["checks"]["prepublish"] == old_checks["prepublish"]


def test_one_native_child_preserves_consumed_generation_and_explicit_base_lineage(
    payload_update,
    monkeypatch,
):
    store, original, payload, _old, package = payload_update
    predecessor_spec, predecessor_reference = renewal.renew(original, payload, digest(payload))
    predecessor_bytes = Path(predecessor_reference["path"]).read_bytes()
    before_receipt = read_private(Path(predecessor_reference["path"]))
    (package / "__init__.py").write_text(
        (package / "__init__.py").read_text() + "\n# Second owned controlled payload.\n"
    )
    controlled = package.parents[2]
    renewal._git(controlled, "add", ".")
    renewal._git(controlled, "commit", "-qm", "test: technical native child")
    authority = {
        "trigger_bindings": {
            "consumed_native_renewal_authority": {
                "path": payload["preparation_authority_path"],
                "sha256": payload["preparation_authority_sha256"],
            }
        },
        "installed_predecessor": {
            "source": before_receipt["source_revision"],
            "config_sha256": before_receipt["config_sha256"],
        },
    }
    predecessor = {
        "original_spec": original,
        "spec": predecessor_spec,
        "recovery": {
            "execution_spec": predecessor_spec,
            "native_preparation_renewal": predecessor_reference,
        },
    }
    next_payload = {
        **request(),
        "expected_source_revision": renewal._git(controlled, "rev-parse", "HEAD"),
        "preparation_authority_path": payload["preparation_authority_path"],
        "preparation_authority_sha256": payload["preparation_authority_sha256"],
    }
    assert technical.native_predecessor(predecessor, authority) == before_receipt
    observed = renewal.readiness(predecessor_spec, next_payload)
    observed["authority"] = authority
    proposed = deepcopy(predecessor_spec)
    proposed["base_sha"] = (
        "e" * 40
    )  # Explicit project applicability amendment; native identity is unchanged.
    child, child_reference = renewal.renew(
        proposed,
        next_payload,
        digest(next_payload),
        technical={"readiness": observed, "predecessor": predecessor},
    )
    assert Path(predecessor_reference["path"]).read_bytes() == predecessor_bytes
    assert child_reference["path"] != predecessor_reference["path"]
    assert "/technical-successor/native-generation/" in child_reference["path"]
    assert child["base_sha"] == proposed["base_sha"]
    assert (
        child["preparation"]["security_binding_sha256"]
        != (predecessor_spec["preparation"]["security_binding_sha256"])
    )
    assert renewal.renew(
        proposed,
        next_payload,
        digest(next_payload),
        technical={"readiness": observed, "predecessor": predecessor},
    ) == (child, child_reference)
    recovery = {
        "execution_spec": child,
        "native_preparation_renewal": child_reference,
        "native_predecessor": predecessor,
        "authority": authority,
    }
    assert renewal.effective_spec(proposed, recovery, technical=True) == child
    with pytest.raises(ValueError, match="exact owned path"):
        renewal.effective_spec(proposed, recovery)
    changed = deepcopy(recovery)
    changed["native_predecessor"]["spec"]["base_sha"] = "f" * 40
    with pytest.raises(ValueError, match="predecessor changed"):
        renewal.effective_spec(proposed, changed, technical=True)
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0
