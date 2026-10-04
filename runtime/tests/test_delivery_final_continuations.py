"""Controller tails retain raw failed history; fixture seams never claim native execution."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import pytest
from temporalio import workflow
from test_delivery_store import service as service

from devflow_temporal import delivery_investigation_adjudication as adjudication
from devflow_temporal import delivery_native_renewal as renewal
from devflow_temporal import delivery_resource_closure as closure
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_resources import RunResources
from devflow_temporal.delivery_workflow import DeliveryWorkflow


def request(kind):
    return {
        "continuation_kind": kind,
        "command_id": "final-1",
        "expected_revision": 41,
        "expected_iteration": 4,
        "expected_candidate_id": "a" * 64,
        "expected_pr_number": 7,
        "expected_pr_head": "b" * 40,
        "additional_iterations": 0,
        "authority_path": "/private/authority.json",
        "authority_sha256": "c" * 64,
        "controller_path": "/private/controller.json",
        "controller_sha256": "d" * 64,
    }


@pytest.mark.parametrize("argv", [
    ["corepack", "pnpm", "install", "--offline", "--frozen-lockfile"],
    ["uv", "--project", "worker", "sync", "--locked"],
    ["uv", "run", "python", "-m", "playwright", "install", "chromium"],
])
def test_dependency_failure_retains_checkpoint_without_spending_a_coding_round(
    stopped_tail, monkeypatch, argv,
):
    _, spec, state, *_ = stopped_tail
    flow = DeliveryWorkflow()
    flow.state = deepcopy(state)
    flow.state.update(iteration=0, cleanup="none", phase="prepublish_checks")
    calls = []

    async def execute(name, body, **kwargs):
        calls.append(name)
        assert name == "delivery_precheck"
        return {"state": "failed", "cleanup": "confirmed", "results": [
            {"id": "dependencies", "argv": argv, "passed": False, "exit_code": 1},
        ]}

    async def project(*args):
        pass

    async def stop(_spec, reason):
        flow.state["error"] = reason
        return flow.state

    monkeypatch.setattr(flow, "_activity", execute)
    monkeypatch.setattr(flow, "_project", project)
    monkeypatch.setattr(flow, "_stop", stop)
    result = asyncio.run(flow._run_iterations(
        spec, start_iteration=0, prior_implementer_session="preserved", repair_findings=[],
        continuation=None, recovery=None, resume_prechecks=True,
    ))
    assert result["iteration"] == 0
    assert result["error"].startswith("environment preparation failed: dependencies")
    assert calls == ["delivery_precheck"]
    assert result["candidate"] == state["candidate"]


def test_source_type_errors_are_not_misclassified_as_dependency_preparation():
    from devflow_temporal.delivery_workflow import _preparation_failure
    result = {"results": [
        {"id": "api-check", "argv": ["corepack", "pnpm", "api:check"], "passed": False},
    ]}
    assert _preparation_failure(result) is None


@pytest.mark.parametrize("kind", [adjudication.KIND, closure.KIND])
@pytest.mark.parametrize("bad", ["missing", "grant", "boolean", "iteration", "root", "child"])
def test_public_discriminator_refuses_before_ordinary_grant_or_any_effect(
    service, monkeypatch, kind, bad
):
    store, submitted = service
    store.submit(submitted)
    payload = request(kind)
    if bad == "missing":
        payload.pop("controller_path")
    elif bad == "grant":
        payload["additional_iterations"] = 1
    elif bad == "boolean":
        payload["additional_iterations"] = False
    elif bad == "iteration":
        payload["expected_iteration"] = 5
    elif bad == "root":
        payload["evidence_root"] = "/foreign"
    else:
        monkeypatch.setenv("DEVFLOW_MANAGED_DEPTH", "1")
    with store._connect() as db:
        claim = store.state.claim_for(db, "work-1")
        commands = list(db.execute("SELECT * FROM delivery_commands"))
    for operation in (store.repair_admission_preflight, store.continue_repair):
        with pytest.raises(ValueError):
            operation("run-1", payload)
    with store._connect() as db:
        assert store.state.claim_for(db, "work-1") == claim
        assert list(db.execute("SELECT * FROM delivery_commands")) == commands
        assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM delivery_resource_closures").fetchone()[0] == 0
    root = Path(store.spec("run-1")["state_dir"])
    assert not (root / "resource-closure").exists()
    assert not (root / "investigation-adjudication").exists()


@pytest.mark.parametrize("bad", ["hash", "alias", "parent", "hardlink", "size", "mode", "owner"])
def test_hash_bound_evidence_reader_refuses_before_writes(tmp_path, monkeypatch, bad):
    file = tmp_path / "receipt.json"
    file.write_bytes(b'{"retained":"raw findings"}')
    file.chmod(0o600)
    sha = hashlib.sha256(file.read_bytes()).hexdigest()
    target = file
    limit = 1024
    if bad == "hash":
        sha = "f" * 64
    elif bad == "alias":
        target = tmp_path / "alias"
        target.symlink_to(file)
    elif bad == "parent":
        alias = tmp_path / "parent"
        alias.symlink_to(tmp_path)
        target = alias / file.name
    elif bad == "hardlink":
        os.link(file, tmp_path / "other")
    elif bad == "size":
        limit = 1
    elif bad == "mode":
        file.chmod(0o644)
    else:
        monkeypatch.setattr(adjudication.os, "getuid", lambda: file.stat().st_uid + 1)
    before = file.read_bytes()
    with pytest.raises(ValueError):
        adjudication._bytes(target, sha, limit=limit)
    assert file.read_bytes() == before


@pytest.fixture
def stopped_tail(service):
    store, submitted = service
    store.submit(submitted)
    spec = store.spec("run-1")
    spec["resource_cleanup_version"] = 1
    spec["terminal_tracker_version"] = 1
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    candidate = broker.candidate()
    publication = {
        "number": 7,
        "head": candidate["head"],
        "candidate": candidate,
        "url": "https://example.invalid/pull/7",
        "state": "OPEN",
    }
    final = RunResources(spec).finalize("blocked", uncertain=True)
    state = {
        "run_id": "run-1",
        "phase": "blocked",
        "execution_state": "blocked",
        "outcome": "blocked",
        "error": "repair limit exhausted",
        "cleanup": "unknown",
        "iteration": 4,
        "revision": 41,
        "candidate_revision": 9,
        "candidate": candidate,
        "pull_request": publication,
        "roles": [
            {
                "role": "verify",
                "iteration": 4,
                "status": "findings",
                "cleanup": "confirmed",
                "findings": ["Medium receipt", "Medium layout", "Medium orphans"],
            }
        ],
        "checks": {
            "resource_cleanup": final,
            "terminal_tracker_checkpoint": {"state": "confirmed"},
            "qa": {"state": "failed"},
            "browser_qa": {"state": "failed", "test_count": 5},
        },
        "findings": ["Medium receipt", "Medium layout", "Medium orphans"],
        "tracker": {},
        "usage": {},
    }
    store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message=state["error"],
        candidate=candidate,
        pull_request=publication,
        checks=state["checks"],
        iteration=4,
        protocol_revision=41,
        outcome="blocked",
        cleanup="unknown",
        error=state["error"],
    )
    with store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs").fetchone())
        claim = store.state.claim_for(db, "work-1")
    resource_root = Path(spec["state_dir"]) / "resources"
    resources = {
        name + "_sha256": hashlib.sha256((resource_root / file).read_bytes()).hexdigest()
        for name, file in [("manifest", "manifest.json"), ("finalization", "finalization.json")]
    }
    resources.update(journal_sha256={}, roots={})
    return store, spec, state, row, claim, resources


def seal_for(stopped_tail, kind):
    store, spec, state, row, claim, resources = stopped_tail
    payload = request(kind)
    payload.update(
        expected_candidate_id=state["candidate"]["id"], expected_pr_head=state["candidate"]["head"]
    )
    seal = {
        "kind": kind,
        "command": payload,
        "spec": spec,
        "execution_spec": spec,
        "authority": {"raw_qa": {"findings": state["findings"]}},
        "controller": {},
        "original_row": row,
        "original_recovery": None,
        "state": state,
        "resources": resources,
        "claim": claim,
        "parent": {},
        "candidate": state["candidate"],
        "publication": state["pull_request"],
        "maximum_iteration": 4,
    }
    return store, payload, seal


@pytest.mark.parametrize("kind", [adjudication.KIND, closure.KIND])
@pytest.mark.parametrize("failure", ["preflight", "locked-recheck", "lost-intent", "none"])
def test_controller_admission_is_atomic_replayable_and_preserves_original_bytes(
    stopped_tail,
    monkeypatch,
    kind,
    failure,
):
    store, payload, seal = seal_for(stopped_tail, kind)
    module = closure if kind == closure.KIND else adjudication
    if kind == adjudication.KIND:
        with store._connect() as db:
            store.state.release_work(db, "work-1", "external:devflow:run-1")
        seal["claim"] = None
    calls = []

    def snapshot(*_args):
        calls.append("preflight")
        locked_call = 3 if kind == closure.KIND else 2
        if failure == "preflight" or (failure == "locked-recheck" and len(calls) == locked_call):
            raise ValueError("fresh authority/head/source/claim observation refused")
        value = deepcopy(seal)
        return (value, {"required": False}) if kind == closure.KIND else value

    monkeypatch.setattr(module, "_snapshot", snapshot)
    monkeypatch.setattr(module, "_authority", lambda *_a, **_kw: ({}, {}, {}))
    monkeypatch.setattr(module, "readback", lambda *_a: {"state": "observed"})
    # Native preparation is deliberately excluded in this atomic journal fixture.
    native_calls = []
    monkeypatch.setattr(
        renewal,
        "renew",
        lambda spec, *_a, **_kw: (native_calls.append("readiness-false") or spec, None),
    )
    root = Path(seal["spec"]["state_dir"]) / (
        "resource-closure" if kind == closure.KIND else "investigation-adjudication"
    )
    original = {
        n: (Path(seal["spec"]["state_dir"]) / "resources" / n).read_bytes()
        for n in ("manifest.json", "finalization.json")
    }
    immutable = module._immutable
    if failure == "lost-intent":

        def interrupted(path, *args, **kwargs):
            immutable(path, *args, **kwargs)
            if path.name == "intent.json":
                raise RuntimeError("lost after immutable publication before durable commit")

        monkeypatch.setattr(module, "_immutable", interrupted)
    if failure in {"preflight", "locked-recheck", "lost-intent"}:
        with pytest.raises((ValueError, RuntimeError)):
            store.continue_repair("run-1", payload)
        with store._connect() as db:
            assert store.state.claim_for(db, "work-1") == seal["claim"]
            assert db.execute("SELECT COUNT(*) FROM delivery_resource_closures").fetchone()[0] == 0
            assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0
            assert db.execute("SELECT COUNT(*) FROM delivery_commands").fetchone()[0] == 1
        assert not native_calls
        if failure != "lost-intent":
            assert not (root / "intent.json").exists()
            return
        retained = (root / "intent.json").read_bytes()
        monkeypatch.setattr(module, "_immutable", immutable)
    else:
        preview = store.repair_admission_preflight("run-1", payload)
        assert preview["preflight"] and preview["additional_iterations"] == 0
        assert not (root / "intent.json").exists()
    first = store.continue_repair("run-1", payload)
    assert first["additional_iterations"] == 0 and not first["existing"]
    if failure == "lost-intent":
        assert (root / "intent.json").read_bytes() == retained
    second = store.continue_repair("run-1", payload)
    assert second["existing"] and second["workflow_id"] == first["workflow_id"]
    for name, raw in original.items():
        assert (Path(seal["spec"]["state_dir"]) / "resources" / name).read_bytes() == raw
        assert (root / "predecessor-resources" / name).read_bytes() == raw
    with store._connect() as db:
        assert store.state.claim_for(db, "work-1")["owner"] == "external:devflow:run-1"
        assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM delivery_commands").fetchone()[0] == 2
    assert len(native_calls) == (1 if kind == closure.KIND else 0)


@pytest.mark.parametrize("kind", [adjudication.KIND, closure.KIND])
@pytest.mark.parametrize("ci_state", ["passed", "failed"])
def test_real_controller_tail_preserves_raw_history_and_never_codes_or_publishes(
    stopped_tail,
    monkeypatch,
    kind,
    ci_state,
):
    _, payload, recovery = seal_for(stopped_tail, kind)
    recovery["source_applicability"] = {
        "before": recovery["candidate"],
        "after": recovery["candidate"],
    }
    old = deepcopy(recovery["state"])
    flow, calls = DeliveryWorkflow(), []
    monkeypatch.setattr(workflow, "now", lambda: datetime.now(UTC))
    monkeypatch.setattr(workflow, "patched", lambda _name: True)

    async def execute(name, body, **_kwargs):
        calls.append((name, body))
        assert name in (
            adjudication.ACTIVITIES
            if kind == adjudication.KIND
            else {
                "delivery_resource_closure_readback",
                "delivery_project",
                "delivery_finalize_resources",
                "delivery_terminal_tracker",
                "delivery_checks", "delivery_browser_qa", "delivery_role", "delivery_ci",
            }
        )
        if name == "delivery_adjudication_readback":
            return {"state": "adjudicated", "raw_status": "findings"}
        if name == "delivery_resource_closure_readback":
            return {"state": "observed", "current_payload_verified": True}
        if name == "delivery_ci":
            return {"state": ci_state, "head": recovery["candidate"]["head"]}
        if name == "delivery_checks":
            return {"state": "passed", "cleanup": "confirmed", "results": []}
        if name == "delivery_browser_qa":
            return {"state": "passed", "cleanup": "confirmed", "test_count": 5,
                    "receipt": "new-receipt", "receipt_sha256": "a" * 64,
                    "log": "new-log", "log_sha256": "b" * 64}
        if name == "delivery_role":
            assert body["role"] == "verify" and body["iteration"] == 4
            return {"status": "pass", "candidate": recovery["candidate"],
                    "session_id": "independent-new-qa", "cleanup": "confirmed"}
        if name == "delivery_finalize_resources":
            assert body["uncertain"] is False
            return {
                "state": "confirmed",
                "resource_cleanup": "confirmed",
                "process_cleanup": "observed-native-confirmed",
            }
        if name == "delivery_terminal_tracker":
            assert body["release"] is True
            return {"state": "consistent"}
        return {}

    monkeypatch.setattr(workflow, "execute_activity", execute)
    result = asyncio.run(
        flow._resume_adjudication(recovery["spec"], recovery)
        if kind == adjudication.KIND
        else flow._resume_resource_closure(recovery["spec"], recovery)
    )
    assert result["outcome"] == (
        "delivered" if ci_state == "passed" else "blocked"
    )
    assert result["cleanup"] == "confirmed"
    assert result["roles"][:len(old["roles"])] == old["roles"]
    assert result["findings"] == old["findings"]
    if kind == adjudication.KIND:
        assert result["checks"]["qa"] == old["checks"]["qa"]
    else:
        assert result["checks"]["qa"]["state"] == "passed"
        assert result["iteration"] == 4
    assert recovery["state"] == old
    assert sum(n == "delivery_finalize_resources" for n, _ in calls) == 1
    assert sum(n == "delivery_terminal_tracker" for n, _ in calls) == 1
    assert sum(n == "delivery_ci" for n, _ in calls) == 1
    for forbidden in (
        "delivery_prepare",
        "delivery_publish",
        "delivery_precheck",
        "delivery_tracker_start",
    ):
        with pytest.raises(ValueError):
            asyncio.run(flow._activity(forbidden, {"spec": recovery["spec"]}))
    with pytest.raises(ValueError):
        asyncio.run(flow._activity("delivery_role", {
            "spec": recovery["spec"], "role": "implement", "iteration": 4,
        }))


def test_supplemental_spec_cannot_alias_or_skip_its_immediate_parent(tmp_path):
    original = {"state_dir": str(tmp_path), "policy": {}}
    with pytest.raises(ValueError, match="exact owned path"):
        renewal.effective_spec(
            original,
            {
                "native_preparation_renewal": {
                    "path": str(tmp_path / "native-preparation-renewal/generation.json"),
                    "sha256": "a" * 64,
                }
            },
            supplemental=True,
        )


@pytest.mark.parametrize("broader_change", [False, True])
def test_controller_effort_amendment_preserves_frozen_non_role_authority(
    stopped_tail, monkeypatch, broader_change,
):
    store, spec, *_ = stopped_tail
    from devflow_temporal import delivery_native_process
    from devflow_temporal import payload as runtime_payload
    from devflow_temporal.contracts import digest
    frozen = deepcopy(store.config.raw)
    active = deepcopy(frozen)
    active["roles"]["verify"] = {"model": "gpt-6.1-sol", "effort": "high"}
    if broader_change:
        active["capacity"] = frozen.get("capacity", 2) + 1
    active_path = str(store.config.state_root / "active-config.json")
    processes_path = store.config.state_root / "service-processes.json"
    processes = {"config_path": active_path, "processes": {
        name: {"pid": pid, "identity": name} for pid, name in enumerate(
            ("api", "worker", "temporal"), start=100,
        )}}
    table = {p["pid"]: {"identity": p["identity"], "stat": "S"}
             for p in processes["processes"].values()}
    controller = {
        "kind": "root_installed_controller_final_tail_readback", "owner": "root",
        "run_id": spec["run_id"], "authority_sha256": "a" * 64,
        "installed_source_root": str(Path(adjudication.__file__).resolve().parents[3]),
        "source_revision": "b" * 40, "source_tree": "c" * 40,
        "runtime_payload_sha256": "d" * 64,
        "config_path": spec["config_path"], "config_digest": digest(frozen),
        "published_head": "e" * 40, "published_tree": "c" * 40,
        "source_review": "PASS", "required_ci": "SUCCESS",
        "service_manifest_path": str(processes_path),
        "active_config": {"path": active_path, "roles": active["roles"], "sha256": "f" * 64},
    }
    monkeypatch.setattr(adjudication, "reference", lambda *_: controller)
    blobs = {spec["config_path"]: frozen, active_path: active, str(processes_path): processes}
    monkeypatch.setattr(adjudication, "_bytes", lambda p, *_: json.dumps(blobs[str(p)]).encode())
    monkeypatch.setattr(adjudication, "_git", lambda _p, *a:
                        "" if a[0] == "status" else "c" * 40 if a[-1].endswith("^{tree}")
                        else "b" * 40)
    monkeypatch.setattr(delivery_native_process, "process_table", lambda: table)
    monkeypatch.setattr(runtime_payload, "payload_digest", lambda _: "d" * 64)
    command = {"controller_path": "/controller", "controller_sha256": "0" * 64,
               "authority_sha256": "a" * 64}
    if broader_change:
        with pytest.raises(ValueError, match="non-role"):
            adjudication._controller(store, spec, command)
    else:
        assert adjudication._controller(store, spec, command) == controller
        assert store.config.raw == frozen
