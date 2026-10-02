"""Admission, measured cache integrity, crash/replay and preparation cancellation."""

from __future__ import annotations

import asyncio
import copy
import json
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from test_delivery_intake import intake_fixture as intake_fixture

from devflow_temporal import delivery_preparation as preparation
from devflow_temporal.delivery_activities import delivery_prepare, delivery_project
from devflow_temporal.delivery_config import DeliveryConfig, scope_amended_spec
from devflow_temporal.delivery_container import Bind, OwnedContainer
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow


def test_foreign_docker_context_is_rejected_before_any_daemon_action(monkeypatch):
    monkeypatch.setattr(
        preparation,
        "_docker_json",
        lambda *_args: [{"Endpoints": {"docker": {"Host": "ssh://remote.invalid"}}}],
    )
    monkeypatch.setattr(
        preparation,
        "_docker",
        lambda *_args, **_kwargs: pytest.fail("foreign daemon must not be inspected or started"),
    )
    with pytest.raises(preparation.PreparationError, match="local Docker Desktop"):
        preparation._engine({"docker_bin": "/configured/docker"}, start=True)


@pytest.mark.parametrize("start", [False, True])
def test_only_preparation_can_start_known_local_desktop(monkeypatch, start):
    monkeypatch.setattr(preparation, "_local_endpoint", lambda _container: "unix:///owned.sock")
    calls = []
    original_is_dir = Path.is_dir
    monkeypatch.setattr(
        Path,
        "is_dir",
        lambda path: True if str(path) == "/Applications/Docker.app" else original_is_dir(path),
    )
    identity = {
        "ID": "owned-engine",
        "ServerVersion": "29.4.3",
        "KernelVersion": "test",
        "OperatingSystem": "Docker Desktop",
        "Architecture": "aarch64",
        "SecurityOptions": ["seccomp"],
    }

    def docker(_binary, *argv, **kwargs):
        calls.append((argv, kwargs.get("timeout")))
        if argv[0] == "desktop":
            return subprocess.CompletedProcess(argv, 0, b"", b"")
        return subprocess.CompletedProcess(
            argv, 1 if len(calls) == 1 else 0, json.dumps(identity).encode(), b""
        )

    monkeypatch.setattr(preparation, "_docker", docker)
    container = {"docker_bin": "/configured/docker", "docker_bin_sha256": "a" * 64}
    if start:
        assert preparation._engine(container, start=True)["ID"] == "owned-engine"
        assert calls == [
            (("info", "--format", "{{json .}}"), 10),
            (("desktop", "start", "--timeout", "90"), 95),
            (("info", "--format", "{{json .}}"), 10),
        ]
    else:
        with pytest.raises(preparation.PreparationError, match="unavailable"):
            preparation._engine(container, start=False)
        assert len(calls) == 1


@pytest.fixture
def real_store(intake_fixture):
    path, request = intake_fixture
    configured = json.loads(path.read_text())
    configured["provider"] = "codex"
    configured.pop("fake_intake")
    configured["roles"] = {
        role: {"model": "gpt-6.1-sol", "effort": "max"} for role in configured["roles"]
    }
    configured["container"] = {"docker_bin": "/usr/local/bin/docker"}
    configured["repositories"]["fixture"].update(
        {
            "prepublish_checks": [{"id": "precheck", "argv": ["/usr/bin/true"]}],
            "checks": [{"id": "check", "argv": ["/usr/bin/true"]}],
            "required_ci": ["test"],
            "project_url": "https://github.com/users/example/projects/1",
            "assignee": "example",
        }
    )
    path.write_text(json.dumps(configured))
    return DeliveryStore(DeliveryConfig.load(path)), request


def _observed(mode, ports):
    fields = (
        "host_credential_read",
        "state_read",
        "state_write",
        "outside_write",
        "docker_socket_read",
        "unrelated_host_connect",
    )
    parent = {key: "PermissionError:1" for key in fields}
    parent.update({"allowed_write": True, "workspace_write": "ALLOWED", "git_read": "ALLOWED"})
    if mode.startswith("role"):
        parent.update(
            {
                "copied_auth_read": "PermissionError:1",
                "loopback": "PermissionError:1",
                "git_read": "PermissionError:1",
                "temporary_write": "PermissionError:1",
                "slash_tmp_write": "PermissionError:1",
            }
        )
        if mode == "role-read":
            parent["workspace_write"] = "PermissionError:1"
    else:
        parent.update(
            {
                "unrelated_port_bind": "PermissionError:1",
                "unrelated_port_connect": "PermissionError:1",
            }
        )
        if mode == "browser-qa":
            parent["owned_ports"] = ports
    return {**parent, "child": dict(parent), "child_returncode": 0}


@pytest.fixture
def measured_environment(monkeypatch):
    state = {
        "payload": "a" * 64,
        "profile": "b" * 64,
        "image": "sha256:" + "c" * 64,
        "engine": {"ID": "owned-engine", "version": "test"},
        "calls": [],
    }

    def launch(_spec):
        return {
            "docker_bin": "/usr/local/bin/docker",
            "docker_bin_sha256": "d" * 64,
            "runtime_payload_sha256": state["payload"],
            "seccomp_sha256": state["profile"],
            "memory": "2g",
            "cpus": "2",
            "pids_limit": 256,
        }

    def identity(container, *, source):
        if container["runtime_payload_sha256"] != state["payload"]:
            raise ValueError("trusted runtime payload changed")
        if container["seccomp_sha256"] != state["profile"]:
            raise ValueError("trusted seccomp profile changed")
        return dict(container)

    def measure(_spec, root, fingerprint, identity):
        state["calls"].append(fingerprint)
        time.sleep(0.02)
        proof = {
            "schema": preparation.SCHEMA,
            "fingerprint": fingerprint,
            "identity": identity,
            "measurements": {},
            "detached": {},
        }
        for group, modes in (
            ("measurements", ("role-write", "role-read", "check", "browser-qa")),
            ("detached", ("role-write", "check", "browser-qa")),
        ):
            for mode in modes:
                folder = root / "probes" / fingerprint / group / mode
                preparation._directory(folder)
                preparation._write(folder / "log", {"actual_fixture_log": mode})
                preparation._write(
                    folder / "intent",
                    {
                        "image_id": identity["container"]["image_id"],
                        "seccomp_sha256": identity["container"]["seccomp_sha256"],
                    },
                )
                item = {
                    "exit_code": 0,
                    "cleanup": "confirmed",
                    "container_id": "e" * 64,
                    "log": preparation._reference(folder / "log"),
                    "intent": preparation._reference(folder / "intent"),
                }
                if group == "measurements":
                    preparation._write(
                        folder / "observed", _observed(mode, identity["browser_ports"])
                    )
                    item["observed"] = preparation._reference(folder / "observed")
                else:
                    item.update(
                        {
                            "replay_container_id": "e" * 64,
                            "heartbeat_before": "3",
                            "heartbeat_after": "3",
                        }
                    )
                proof[group][mode] = item
        return proof

    monkeypatch.setattr(preparation, "_launch_policy", launch)
    monkeypatch.setattr(preparation, "_container_identity", identity)
    monkeypatch.setattr(
        preparation, "_engine", lambda _container, *, start: copy.deepcopy(state["engine"])
    )
    monkeypatch.setattr(
        preparation,
        "_resolve_image",
        lambda container, root: {**container, "image_id": state["image"]},
    )
    monkeypatch.setattr(preparation, "measure_environment", measure)
    return state


def _submit(store, request, number):
    value = {
        **request,
        "run_id": f"run-{number}",
        "work_id": f"work-{number}",
        "command_id": f"submit-{number}",
        "branch": f"feat/fixture-{number}",
        "issue_url": f"https://github.com/example/fixture/issues/{number}",
    }
    assert store.submit(value)["phase"] == "preparing"
    return store.submitted_spec(value["run_id"])


def test_two_run_ids_share_measured_environment_and_have_distinct_binding(
    real_store, measured_environment
):
    store, request = real_store
    first = _submit(store, request, 1)
    second = _submit(store, request, 2)
    first_bytes = store.submitted_spec(first["run_id"])
    prepared = preparation.prepare_authority(store, first)
    other = preparation.prepare_authority(store, second)
    assert len(measured_environment["calls"]) == 1
    assert prepared["preparation"]["fingerprint"] == other["preparation"]["fingerprint"]
    assert not prepared["preparation"]["cache_reused"] and other["preparation"]["cache_reused"]
    assert (
        prepared["preparation"]["security_binding_sha256"]
        != other["preparation"]["security_binding_sha256"]
    )
    assert store.submitted_spec(first["run_id"]) == first_bytes
    assert store.effective_spec(first["run_id"]) == prepared
    assert preparation.prepare_authority(store, first) == prepared
    assert store.detail(first["run_id"])["preparation"] == prepared["preparation"]


def test_concurrent_preparation_freezes_one_result(real_store, measured_environment):
    store, request = real_store
    spec = _submit(store, request, 1)
    with ThreadPoolExecutor(max_workers=2) as workers:
        results = list(workers.map(lambda _: preparation.prepare_authority(store, spec), range(2)))
    assert results[0] == results[1] == store.prepared_spec(spec["run_id"])
    assert len(measured_environment["calls"]) == 1
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_preparations").fetchone()[0] == 1


def test_restart_after_measurement_before_freeze_reuses_verified_evidence(
    real_store, measured_environment, monkeypatch
):
    store, request = real_store
    spec = _submit(store, request, 1)

    def crash(*_args):
        raise RuntimeError("simulated crash before durable freeze")

    monkeypatch.setattr(store, "freeze_preparation", crash)
    with pytest.raises(RuntimeError, match="simulated crash"):
        preparation.prepare_authority(store, spec)
    assert store.prepared_spec(spec["run_id"]) is None
    restarted = DeliveryStore(DeliveryConfig.load(store.config.path))
    prepared = preparation.prepare_authority(restarted, spec)
    assert prepared["preparation"]["cache_reused"]
    assert len(measured_environment["calls"]) == 1


@pytest.mark.parametrize(
    "change", ["branch", "allowed_paths", "proof", "observed", "payload", "profile"]
)
def test_tampered_or_stale_preparation_cannot_construct_container(
    real_store, measured_environment, change
):
    store, request = real_store
    effective = preparation.prepare_authority(store, _submit(store, request, 1))
    if change == "branch":
        effective["branch"] = "feat/escalation"
    elif change == "allowed_paths":
        effective["policy"]["allowed_paths"].append("secret.txt")
    elif change == "payload":
        measured_environment["payload"] = "f" * 64
    elif change == "profile":
        measured_environment["profile"] = "f" * 64
    else:
        path = Path(effective["preparation"]["environment"]["path"])
        if change == "observed":
            proof = json.loads(path.read_bytes())
            path = Path(proof["measurements"]["role-write"]["observed"]["path"])
        path.write_text("{}")
    with pytest.raises((preparation.PreparationError, ValueError)):
        OwnedContainer(
            effective,
            kind="role",
            identity={"role": "intake"},
            evidence_dir=Path(effective["state_dir"]) / "attempt/container",
            binds=(Bind(Path(effective["checkout"]), "/work", True),),
            command=("/usr/bin/true",),
            cwd="/work",
            environment={},
            network="bridge",
            timeout_seconds=30,
        )


@pytest.mark.parametrize("changed", ["payload", "profile", "image", "engine"])
def test_changed_execution_environment_invalidates_cache(real_store, measured_environment, changed):
    store, request = real_store
    first = preparation.prepare_authority(store, _submit(store, request, 1))
    measured_environment[changed] = (
        {"ID": "new-engine", "version": "test"}
        if changed == "engine"
        else "sha256:" + "f" * 64
        if changed == "image"
        else "f" * 64
    )
    second = preparation.prepare_authority(store, _submit(store, request, 2))
    assert first["preparation"]["fingerprint"] != second["preparation"]["fingerprint"]
    assert len(measured_environment["calls"]) == 2
    assert not second["preparation"]["cache_reused"]


def test_failed_measurement_never_freezes_launch_authority(
    real_store, measured_environment, monkeypatch
):
    store, request = real_store
    spec = _submit(store, request, 1)

    def failed(*_args):
        raise preparation.PreparationError("a child credential read was allowed")

    monkeypatch.setattr(preparation, "measure_environment", failed)
    with pytest.raises(preparation.PreparationError, match="child credential"):
        preparation.prepare_authority(store, spec)
    assert store.prepared_spec(spec["run_id"]) is None
    with pytest.raises(preparation.PreparationError, match="not frozen"):
        preparation.verify_prepared_spec(spec)


def test_missing_child_denial_or_cleanup_cannot_be_sealed(real_store, measured_environment):
    store, request = real_store
    prepared = preparation.prepare_authority(store, _submit(store, request, 1))
    path = Path(prepared["preparation"]["environment"]["path"])
    proof = json.loads(path.read_bytes())
    root = Path(prepared["state_dir"]).parents[1] / "preparation"
    observed = Path(proof["measurements"]["role-write"]["observed"]["path"])
    value = json.loads(observed.read_bytes())
    del value["child"]["copied_auth_read"]
    preparation._write(observed, value)
    proof["measurements"]["role-write"]["observed"] = preparation._reference(observed)
    with pytest.raises(preparation.PreparationError, match="did not pass"):
        preparation.validate_environment(proof, root, proof["identity"])
    proof["measurements"]["role-write"]["cleanup"] = "unknown"
    with pytest.raises(preparation.PreparationError, match="cleanup measurement"):
        preparation.validate_environment(proof, root, proof["identity"])


def test_explicit_scope_amendment_rebinds_verified_environment_without_manual_proof(
    real_store, measured_environment
):
    store, request = real_store
    request = {**request, "accepted_plan": "An already accepted fixture plan"}
    original = preparation.prepare_authority(store, _submit(store, request, 1))
    raw = copy.deepcopy(store.config.raw)
    raw["repositories"]["fixture"]["allowed_paths"].append("fixture-test.py")
    config_path = store.config.state_root / "amended.json"
    preparation._write(config_path, raw)
    effective = scope_amended_spec(
        original, config_path, preparation._hash(config_path), ["fixture-test.py"]
    )
    assert effective["preparation"]["fingerprint"] == original["preparation"]["fingerprint"]
    assert (
        effective["policy"]["security_binding_sha256"]
        != original["policy"]["security_binding_sha256"]
    )
    assert store.submitted_spec(original["run_id"])["policy"]["allowed_paths"] == ["README.md"]
    preparation.verify_prepared_spec(effective)


def test_legacy_rows_remain_readable_and_are_not_rewritten(intake_fixture):
    path, request = intake_fixture
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(request)
    with store._connect() as db:
        before = dict(db.execute("SELECT * FROM delivery_runs").fetchone())
    DeliveryStore(DeliveryConfig.load(path))
    assert store.effective_spec(request["run_id"]) == store.spec(request["run_id"])
    with store._connect() as db:
        assert dict(db.execute("SELECT * FROM delivery_runs").fetchone()) == before
        assert db.execute("SELECT COUNT(*) FROM delivery_preparations").fetchone()[0] == 0


def test_prepared_minimal_container_preserves_post_role_successor_authority(
    real_store, measured_environment, tmp_path, monkeypatch
):
    from test_delivery_store import (
        test_post_role_continuation_carries_sealed_candidate_and_session_without_auth as exercise,
    )

    store, request = real_store
    original_submit = DeliveryStore.submit

    def submitted_and_prepared(current, value, *args, **kwargs):
        result = original_submit(current, value, *args, **kwargs)
        preparation.prepare_authority(current, current.submitted_spec(value["run_id"]))
        return result

    monkeypatch.setattr(DeliveryStore, "submit", submitted_and_prepared)
    exercise(
        (store, {**request, "accepted_plan": "Make one bounded edit and test it."}),
        tmp_path,
        monkeypatch,
        False,
    )
    assert store.submitted_spec("run-1")["policy"]["container"] == {
        "docker_bin": "/usr/local/bin/docker"
    }
    assert store.spec("run-1")["preparation"]
    assert store.spec("run-2")["preparation"]


@pytest.mark.asyncio
async def test_permanent_probe_failure_is_not_retried_by_temporal(
    real_store, measured_environment, tmp_path, monkeypatch
):
    store, request = real_store
    submitted = _submit(store, request, 1)
    calls = []

    def failed_probe(*_args):
        calls.append("measured")
        raise preparation.PreparationError("fixture native child denial failed")

    monkeypatch.setattr(preparation, "measure_environment", failed_probe)
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "preparation-failure.sqlite3"),
    ) as environment:
        async with Worker(
            environment.client,
            task_queue="preparation-failure",
            workflows=[DeliveryWorkflow],
            activities=[delivery_project, delivery_prepare],
        ):
            result = await asyncio.wait_for(
                environment.client.execute_workflow(
                    DeliveryWorkflow.run,
                    submitted,
                    id="permanent-preparation-failure",
                    task_queue="preparation-failure",
                ),
                15,
            )
    assert result["outcome"] == "blocked"
    assert "fixture native child denial failed" in result["error"]
    assert calls == ["measured"]
    assert store.prepared_spec(submitted["run_id"]) is None


@pytest.mark.asyncio
async def test_cancellation_during_preparation_never_starts_intake(tmp_path):
    started, finish = asyncio.Event(), asyncio.Event()
    calls = []

    @activity.defn(name="delivery_project")
    async def project_stub(_request):
        return {}

    @activity.defn(name="delivery_prepare")
    async def prepare_stub(request):
        started.set()
        await finish.wait()
        return {"candidate": {"id": "fixture", "head": "head"}, "spec": request["spec"]}

    @activity.defn(name="delivery_intake")
    async def intake_stub(_request):
        calls.append("intake")
        raise AssertionError("cancelled preparation must never start intake")

    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "preparation-cancel.sqlite3"),
    ) as environment:
        async with Worker(
            environment.client,
            task_queue="preparation-cancel",
            workflows=[DeliveryWorkflow],
            activities=[project_stub, prepare_stub, intake_stub],
        ):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run,
                {"run_id": "cancel-fixture", "intake_required": True},
                id="preparation-cancel-fixture",
                task_queue="preparation-cancel",
            )
            await asyncio.wait_for(started.wait(), 10)
            status = await handle.query("status")
            await handle.execute_update(
                "cancel", {"expected_revision": status["revision"], "reason": "stop"}
            )
            finish.set()
            result = await asyncio.wait_for(handle.result(), 10)
            assert result["outcome"] == "cancelled" and calls == []


@pytest.mark.asyncio
async def test_original_preparation_projection_remains_valid_after_atomic_freeze(
    real_store, measured_environment
):
    store, request = real_store
    submitted = _submit(store, request, 1)
    preparation.prepare_authority(store, submitted)
    await delivery_project(
        {
            "spec": submitted,
            "phase": "blocked",
            "execution_state": "blocked",
            "event_type": "blocked",
            "message": "Git preparation failed",
            "iteration": 0,
            "protocol_revision": 2,
            "outcome": "blocked",
        }
    )
    assert store.detail(submitted["run_id"])["error"] is None
    assert store.detail(submitted["run_id"])["outcome"] == "blocked"


@pytest.mark.parametrize("changed", ["repository_key", "authorized_endpoint", "base_ref", "branch"])
def test_invalid_public_authority_still_rejected_before_claim(real_store, changed):
    store, request = real_store
    with pytest.raises(ValueError):
        store.submit({**request, changed: "unconfigured"})
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_runs").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM claims").fetchone()[0] == 0
