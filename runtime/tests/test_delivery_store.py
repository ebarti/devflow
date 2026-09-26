from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib.metadata
import json
import shutil
import socket
import subprocess
from pathlib import Path

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowUpdateFailedError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from devflow_temporal.delivery_activities import delivery_prepare, delivery_project, delivery_role
from devflow_temporal.delivery_api import DeliveryService
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_broker import _git as broker_git
from devflow_temporal.delivery_config import (
    BOUNDARY_DENIAL_FIELDS,
    DeliveryConfig,
    _boundary_probe_passed,
    _browser_qa_probe_passed,
    _installed_codex_binary,
    security_binding,
)
from devflow_temporal.delivery_sandbox import validate_network_domain
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow
from devflow_temporal.role_runner import _task
from devflow_temporal.supervisor import DeliverySupervisor


def _git(path: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(path), *args], check=True, capture_output=True, text=True
    )
    return completed.stdout.strip()


@pytest.fixture
def service(tmp_path: Path) -> tuple[DeliveryStore, dict]:
    source = tmp_path / "source"
    source.mkdir()
    (source / "README.md").write_text("Test repository\n")
    _git(source, "init", "-q")
    _git(source, "config", "user.name", "Delivery Test")
    _git(source, "config", "user.email", "delivery@example.invalid")
    _git(source, "add", "README.md")
    _git(source, "commit", "-qm", "Fixture")
    origin = tmp_path / "origin.git"
    subprocess.run(["git", "init", "--bare", "-q", str(origin)], check=True)
    _git(source, "remote", "add", "origin", str(origin))
    root = Path(__file__).resolve().parents[2]
    configuration = {
        "version": 1,
        "tracking_db": str(tmp_path / "workflow.sqlite3"),
        "state_root": str(tmp_path / "service-state"),
        "helpers_dir": str(root / "skills" / "devflow" / "scripts"),
        "codex_bin": "/usr/bin/false",
        "provider": "fake",
        "repositories": {
            "fixture": {
                "source_path": str(source),
                "origin_url": str(origin),
                "github_repo": "example/fixture",
                "base_ref": "HEAD",
                "expected_base_sha": _git(source, "rev-parse", "HEAD"),
                "allowed_paths": ["README.md"],
            }
        },
        "roles": {
            "implement": {"model": "fixture", "effort": "low"},
            "review": {"model": "fixture", "effort": "low"},
            "verify": {"model": "fixture", "effort": "low"},
        },
    }
    path = tmp_path / "service.json"
    path.write_text(json.dumps(configuration))
    request = {
        "command_id": "command-1",
        "run_id": "run-1",
        "work_id": "work-1",
        "issue_url": "https://github.com/example/fixture/issues/3",
        "repository_key": "fixture",
        "goal": "Change a fixture",
        "accepted_plan": "Make one bounded edit and test it.",
        "base_ref": "HEAD",
        "branch": "feat/fixture",
        "authorized_endpoint": "published_unmerged",
    }
    return DeliveryStore(DeliveryConfig.load(path)), request


def test_submit_claim_and_temporal_outbox_are_atomic_and_idempotent(service):
    store, request = service
    first = store.submit(request)
    assert first["phase"] == "accepted"
    assert first["existing"] is False
    assert len(store.pending_starts()) == 1
    with store._connect() as db:
        claim = store.state.claim_for(db, request["work_id"])
        assert claim["owner"] == "external:devflow:run-1"
    assert store.submit(request) == first
    replay = store.submit({**request, "command_id": "command-2"})
    assert replay["existing"] is True
    assert len(store.pending_starts()) == 1

    with pytest.raises(ValueError, match="different inputs"):
        store.submit({**request, "goal": "A different change"})
    with pytest.raises(ValueError, match="different inputs"):
        store.submit({**request, "command_id": "command-3", "goal": "A different change"})


def test_projection_event_is_idempotent_and_detail_is_factual(service):
    store, request = service
    store.submit(request)
    store.mark_start("run-1", accepted=True)
    store.project(
        "run-1",
        phase="implement",
        execution_state="running",
        event_type="role_started",
        message="Implementer started",
        key="implement:0",
    )
    before = store.detail("run-1")
    store.project(
        "run-1",
        phase="implement",
        execution_state="running",
        event_type="role_started",
        message="Implementer started",
        key="implement:0",
    )
    after = store.detail("run-1")
    assert after["revision"] == before["revision"]
    assert len(after["events"]) == len(before["events"])
    assert after["roles"] == []
    assert after["pull_request"] is None
    assert after["usage"] == {}


def test_phase_gates_do_not_reuse_previous_repair_iteration(service):
    store, request = service
    store.submit(request)
    for event in ("tracker_start", "candidate_ready", "published", "ci_wait"):
        store.project(
            "run-1",
            phase=event,
            execution_state="running",
            event_type=event,
            message=event,
            iteration=0,
            key=f"{event}:0",
        )
    first = {gate["id"]: gate["state"] for gate in store.detail("run-1")["phase_gates"]}
    assert all(
        first[name] == "completed" for name in ("prepare", "prepublish", "publish", "local_checks")
    )
    store.project(
        "run-1",
        phase="repair",
        execution_state="running",
        event_type="role_started",
        message="Repair started",
        iteration=1,
        key="role_started:1",
    )
    repaired = {gate["id"]: gate["state"] for gate in store.detail("run-1")["phase_gates"]}
    assert repaired["prepare"] == "completed"
    assert all(repaired[name] == "pending" for name in ("prepublish", "publish", "local_checks"))
    store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message="prepublication repair limit exhausted",
        checks={"prepublish": {"state": "failed", "results": []}},
        iteration=1,
        outcome="blocked",
        key="blocked:1",
    )
    terminal = {gate["id"]: gate["state"] for gate in store.detail("run-1")["phase_gates"]}
    assert terminal["prepublish"] == "failed"
    assert all(terminal[name] == "pending" for name in ("publish", "local_checks"))


def test_browser_gate_projects_current_candidate_failure_and_resets_on_repair(service):
    original, request = service
    config = json.loads(original.config.path.read_text())
    config["repositories"]["fixture"]["browser_qa"] = {"id": "fixture-browser"}
    original.config.path.write_text(json.dumps(config))
    store = DeliveryStore(DeliveryConfig.load(original.config.path))
    store.submit(request)
    store.project(
        "run-1",
        phase="browser_qa",
        execution_state="running",
        event_type="findings",
        message="Browser fixture failed",
        checks={"local": {"state": "passed"}, "browser_qa": {"state": "failed"}},
        iteration=0,
        key="browser-failed:0",
    )
    first = {gate["id"]: gate["state"] for gate in store.detail("run-1")["phase_gates"]}
    assert first["local_checks"] == "completed"
    assert first["browser_qa"] == "failed"
    store.project(
        "run-1",
        phase="repair",
        execution_state="running",
        event_type="role_started",
        message="Repair started",
        checks={},
        iteration=1,
        key="repair:1",
    )
    repaired = {gate["id"]: gate["state"] for gate in store.detail("run-1")["phase_gates"]}
    assert repaired["local_checks"] == repaired["browser_qa"] == "pending"


def test_blocked_pre_role_run_can_transfer_claim_to_explicit_successor(service):
    store, request = service
    store.submit(request)
    successor = {
        **request,
        "command_id": "submit-successor",
        "run_id": "run-2",
        "branch": "feat/fixture-2",
        "supersedes_run_id": "run-1",
    }
    with pytest.raises(ValueError, match="only a blocked pre-role"):
        store.submit(successor)
    store.mark_start("run-1", accepted=True)
    store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message="fixture preparation failed",
        outcome="blocked",
    )
    assert store.submit(successor)["run_id"] == "run-2"
    with store._connect() as db:
        claim = store.state.claim_for(db, request["work_id"])
        old_session = db.execute(
            "SELECT closed_at FROM runtime_sessions WHERE id=?", ("external:devflow:run-1",)
        ).fetchone()
    assert claim["owner"] == "external:devflow:run-2"
    assert old_session["closed_at"]
    assert store.detail("run-1")["outcome"] == "blocked"


def test_check_network_domains_reject_local_destinations():
    for domain in ("127.0.0.1", "::1", "localhost", "metadata.localhost", "*.example.com"):
        with pytest.raises(ValueError):
            validate_network_domain(domain)
    assert validate_network_domain("REGISTRY.NPMJS.ORG") == "registry.npmjs.org"


def test_security_attestation_binding_rejects_changed_workspace_and_check_authority(service):
    store, request = service
    repository = copy.deepcopy(store.config.raw["repositories"]["fixture"])
    repository["prepublish_checks"] = [
        {"id": "install", "argv": ["corepack", "pnpm", "install"], "network_domains": []}
    ]
    policy = {
        "roles": {"implement": {"model": "gpt-6-sol", "effort": "max"}},
        "toolchain_roots": ["/opt/toolchain"],
        "package_manager_cache": "/opt/cache",
        "codex_auth_path": "/private/auth.json",
        "checks": [{"id": "test", "argv": ["corepack", "pnpm", "test"]}],
    }
    base = {
        "supplied": request,
        "repository": repository,
        "source": Path(repository["source_path"]),
        "origin": repository["origin_url"],
        "base_sha": repository["expected_base_sha"],
        "state_dir": store.config.state_root / "runs" / request["run_id"],
        "checkout": store.config.state_root / "checkouts" / request["run_id"],
        "policy": policy,
    }
    expected = security_binding(**base)
    variants = []
    for field, value in (
        ("source", Path(repository["source_path"]).parent / "other"),
        ("checkout", store.config.state_root / "elsewhere"),
        ("state_dir", store.config.state_root / "other-state"),
        ("origin", "https://example.invalid/other.git"),
    ):
        changed = copy.deepcopy(base)
        changed[field] = value
        variants.append(changed)
    changed_repo = copy.deepcopy(base)
    changed_repo["repository"]["prepublish_checks"][0]["network_domains"] = ["registry.npmjs.org"]
    variants.append(changed_repo)
    changed_policy = copy.deepcopy(base)
    changed_policy["policy"]["toolchain_roots"] = ["/opt/other"]
    variants.append(changed_policy)
    changed_browser = copy.deepcopy(base)
    changed_browser["policy"]["browser_qa"] = {
        "ports": {"JOBCTRL_E2E_API_PORT": 18871, "JOBCTRL_E2E_WEB_PORT": 18872}
    }
    variants.append(changed_browser)
    changed_model = copy.deepcopy(base)
    changed_model["policy"]["roles"]["implement"]["effort"] = "high"
    variants.append(changed_model)
    assert all(security_binding(**variant) != expected for variant in variants)


def test_browser_attestation_requires_positive_fixture_and_child_denials():
    denied = {
        key: "PermissionError:1"
        for key in (
            "host_credential_read",
            "state_read",
            "outside_write",
            "slash_tmp_read",
            "slash_tmp_write",
            "private_tmp_read",
            "private_tmp_write",
            "unrelated_port_connect",
            "unrelated_port_bind",
            "unrelated_host_connect",
        )
    }
    observed = {
        **denied,
        "browser_api_sqlite": True,
        "owned_listeners": True,
        "cleanup": "confirmed",
        "test_count": 2,
        "allowed_scratch_write": "ALLOWED",
        "child": {**denied, "allowed_scratch_write": "ALLOWED"},
    }
    assert _browser_qa_probe_passed(observed)
    for change in (
        {"owned_listeners": False},
        {"cleanup": "unknown"},
        {"test_count": 0},
        {"unrelated_port_connect": "ALLOWED"},
        {"child": {**observed["child"], "host_credential_read": "ALLOWED"}},
    ):
        assert not _browser_qa_probe_passed({**observed, **change})


def test_verify_task_requires_hash_of_broker_executed_qa_receipt():
    digest = "a" * 64
    spec = {
        "run_id": "qa-run",
        "provider": "codex",
        "goal": "Verify feature",
        "accepted_plan": "Inspect browser and API results",
        "policy": {
            "roles": {"verify": {"model": "gpt-6-sol", "effort": "max"}},
            "allowed_paths": ["README.md"],
            "host_sandbox": "native-profile",
        },
    }
    request = {
        "spec": spec,
        "role": "verify",
        "iteration": 0,
        "candidate": {"id": "candidate", "head": "head"},
        "workspace": "/owned/checkout",
        "qa_evidence": {
            "path": "/owned/receipt.json",
            "log": "/owned/browser-qa.log",
            "sha256": digest,
        },
    }
    task = _task(request)
    assert "broker, not you, executed" in task.goal
    assert task.output_schema["properties"]["qa_receipt_sha256"]["type"] == "string"
    assert "qa_receipt_sha256" in task.output_schema["required"]


def test_boundary_attestation_requires_both_tmp_aliases_in_parent_and_child():
    observed = {
        "allowed_write": True,
        "child_returncode": 0,
        "child": {"allowed_write": True},
    }
    for result in (observed, observed["child"]):
        result.update({field: "PermissionError:1" for field in BOUNDARY_DENIAL_FIELDS})
    assert _boundary_probe_passed(observed)
    for scope in (observed, observed["child"]):
        for field in ("slash_tmp_read", "slash_tmp_write", "private_tmp_read", "private_tmp_write"):
            changed = copy.deepcopy(observed)
            target = changed["child"] if scope is observed["child"] else changed
            target.pop(field)
            assert not _boundary_probe_passed(changed)


def test_real_provider_requires_the_installed_tested_sdk_and_binary(monkeypatch):
    binary = _installed_codex_binary()
    assert binary.is_file()
    assert binary.name == "codex"
    assert not binary.is_symlink()
    original = importlib.metadata.version

    def changed_version(name):
        return "0.154.0" if name == "openai-codex" else original(name)

    monkeypatch.setattr(importlib.metadata, "version", changed_version)
    with pytest.raises(ValueError, match="SDK is not the tested version"):
        _installed_codex_binary()


def test_admission_rejects_tracked_project_codex_config(service):
    original, request = service
    configuration = json.loads(original.config.path.read_text())
    repository = configuration["repositories"]["fixture"]
    source = Path(repository["source_path"])
    (source / ".codex").mkdir()
    (source / ".codex" / "config.toml").write_text('sandbox_mode = "danger-full-access"\n')
    _git(source, "add", ".codex/config.toml")
    _git(source, "commit", "-qm", "Project permissions fixture")
    repository["expected_base_sha"] = _git(source, "rev-parse", "HEAD")
    original.config.path.write_text(json.dumps(configuration))
    with pytest.raises(ValueError, match="project Codex configuration"):
        DeliveryConfig.load(original.config.path).admit(request)


@pytest.mark.skipif(not Path("/usr/bin/sandbox-exec").is_file(), reason="macOS required")
def test_real_admission_rejects_unsafe_check_domains_and_script_wrapper(service, tmp_path):
    original, request = service
    configuration = json.loads(original.config.path.read_text())
    configuration["provider"] = "codex"
    configuration["codex_bin"] = str(_installed_codex_binary())
    repository = configuration["repositories"]["fixture"]
    repository.update(
        {
            "prepublish_checks": [
                {"id": "install", "argv": ["/usr/bin/true"], "network_domains": ["127.0.0.1"]}
            ],
            "checks": [{"id": "test", "argv": ["/usr/bin/true"]}],
            "required_ci": ["test"],
            "project_url": "https://github.com/orgs/example/projects/1",
            "assignee": "example",
        }
    )
    original.config.path.write_text(json.dumps(configuration))
    with pytest.raises(ValueError, match="network domain must not be an IP"):
        DeliveryConfig.load(original.config.path).admit(request)

    repository["prepublish_checks"][0]["network_domains"] = ["registry.npmjs.org"]
    wrapper = tmp_path / "codex-wrapper"
    wrapper.write_text("#!/bin/sh\nexec /usr/bin/true\n")
    wrapper.chmod(0o700)
    configuration["codex_bin"] = str(wrapper)
    original.config.path.write_text(json.dumps(configuration))
    with pytest.raises(ValueError, match="tested installed Codex CLI binary"):
        DeliveryConfig.load(original.config.path).admit(request)


def test_check_cwd_accepts_canonical_path_behind_checkout_alias(service, tmp_path):
    store, request = service
    store.submit(request)
    broker = DeliveryBroker(store, store.spec("run-1"))
    broker.prepare()
    alias = tmp_path / "checkout-alias"
    alias.symlink_to(broker.checkout, target_is_directory=True)
    result = broker._run_check_list(
        alias,
        [{"id": "cwd", "argv": ["/usr/bin/python3", "-c", "print('ok')"], "cwd": "."}],
        broker.state_dir / "alias-check",
        broker.candidate(),
    )
    assert result["state"] == "passed"
    assert result["results"][0]["cwd"] == str(broker.checkout.resolve())


def test_gate_diff_is_bound_to_base_head_and_rejects_tampering(service):
    store, request = service
    store.submit(request)
    broker = DeliveryBroker(store, store.spec("run-1"))
    broker.prepare()
    (broker.checkout / "README.md").write_text("Reviewed feature\n")
    _git(broker.checkout, "add", "README.md")
    _git(
        broker.checkout,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "-qm",
        "Feature",
    )
    candidate = broker.candidate()
    broker.gate_checkout("review", 0, candidate)
    evidence = broker.gate_diff("review", 0, candidate)
    patch = Path(evidence["path"])
    assert evidence["base_sha"] == store.spec("run-1")["base_sha"]
    assert evidence["head"] == candidate["head"]
    assert b"+Reviewed feature" in patch.read_bytes()
    assert hashlib.sha256(patch.read_bytes()).hexdigest() == evidence["sha256"]
    assert broker.gate_diff("review", 0, candidate) == evidence
    patch.write_text("tampered\n")
    with pytest.raises(RuntimeError, match="changed across attempts"):
        broker.gate_diff("review", 0, candidate)


@pytest.mark.asyncio
async def test_delivery_review_role_receives_a_bound_diff_in_its_gate_checkout(service):
    store, request = service
    store.submit(request)
    spec = store.spec("run-1")
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    (broker.checkout / "README.md").write_text("Reviewed feature\n")
    _git(broker.checkout, "add", "README.md")
    _git(
        broker.checkout,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "-qm",
        "Feature",
    )
    candidate = broker.candidate()
    assessed = await delivery_role(
        {"spec": spec, "role": "review", "iteration": 0, "candidate": candidate}
    )
    assert assessed["status"] == "pass"
    assert assessed["candidate"] == candidate
    artifact = broker.gate_diff("review", 0, candidate)
    assert Path(artifact["path"]).is_file()
    assert artifact["candidate_id"] == candidate["id"]


def test_broker_git_push_does_not_invoke_repository_hook(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--bare", "-q", str(remote)], check=True)
    _git(source, "init", "-q")
    _git(source, "config", "user.name", "Fixture")
    _git(source, "config", "user.email", "fixture@example.invalid")
    _git(source, "remote", "add", "origin", str(remote))
    (source / "README.md").write_text("Fixture\n")
    _git(source, "add", "README.md")
    _git(source, "commit", "-qm", "Fixture")
    sentinel = tmp_path / "outside.txt"
    sentinel.write_text("SAFE\n")
    hook = source / ".git" / "hooks" / "pre-push"
    hook.write_text(f'#!/bin/sh\nprintf BREACH > "{sentinel}"\n')
    hook.chmod(0o700)

    broker_git(source, "push", "origin", "HEAD:refs/heads/fixture")

    assert sentinel.read_text() == "SAFE\n"


@pytest.mark.skipif(not Path("/usr/bin/sandbox-exec").is_file(), reason="macOS required")
def test_real_check_command_cannot_escape_native_profile(service, tmp_path):
    binary = _installed_codex_binary()
    store, request = service
    store.submit(request)
    spec = store.spec("run-1")
    spec["provider"] = "codex"
    spec["policy"].update(
        {
            "host_sandbox": "native-profile",
            "codex_bin": str(binary),
            "codex_bin_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        }
    )
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    Path(_git(broker.checkout, "rev-parse", "--git-path", "info/exclude")).write_text(
        "allowed.txt\n"
    )
    outside = tmp_path / "outside.txt"
    outside.write_text("SAFE\n")
    credential = tmp_path / "credential.txt"
    credential.write_text("CANARY\n")
    state_secret = broker.state_dir / "controller.txt"
    state_secret.parent.mkdir(parents=True, exist_ok=True)
    state_secret.write_text("STATE\n")
    probe = (
        "import json,pathlib,sys,subprocess,socket\n"
        "paths=list(map(pathlib.Path,sys.argv[1:5]));port=int(sys.argv[5])\n"
        "out={}\n"
        "for name,path,write in zip(('owned','outside','credential','controller'),paths,"
        "(True,True,False,False)):\n"
        " try:\n"
        "  path.write_text('BREACH\\n') if write else path.read_text()\n"
        "  out[name]=True\n"
        " except Exception as exc: out[name]=type(exc).__name__\n"
        "try:\n"
        " s=socket.create_connection(('127.0.0.1',port),timeout=1);s.sendall(b'BREACH');s.close()\n"
        " out['loopback']=True\n"
        "except Exception as exc: out['loopback']=type(exc).__name__\n"
        "if len(sys.argv)<8:\n"
        " child=subprocess.run([sys.executable,'-c',sys.argv[6],*sys.argv[1:7],'child'],"
        "capture_output=True,text=True,timeout=10)\n"
        " out['child']=json.loads(child.stdout)\n"
        "print(json.dumps(out,sort_keys=True))\n"
    )
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(1)
    try:
        check = {
            "id": "security",
            "argv": [
                "/usr/bin/python3",
                "-c",
                probe,
                str(broker.checkout / "allowed.txt"),
                str(outside),
                str(credential),
                str(state_secret),
                str(listener.getsockname()[1]),
                probe,
            ],
            "cwd": ".",
        }
        result = broker._run_check_list(
            broker.checkout, [check], broker.state_dir / "security-check", broker.candidate()
        )
        try:
            received = listener.accept()[0].recv(100)
        except TimeoutError:
            received = None
    finally:
        listener.close()
    assert result["state"] == "passed"
    lines = Path(result["results"][0]["log"]).read_text().splitlines()
    observed = json.loads(next(line for line in lines if line.startswith('{"child":')))
    for item in (observed, observed["child"]):
        assert item["owned"] is True
        assert item["outside"] is not True
        assert item["credential"] is not True
        assert item["controller"] is not True
        assert item["loopback"] is not True
    assert outside.read_text() == "SAFE\n"
    assert received is None


@pytest.mark.asyncio
@pytest.mark.skipif(not Path("/usr/bin/sandbox-exec").is_file(), reason="macOS Seatbelt required")
async def test_supervisor_launches_one_sandboxed_fake_role_and_replays_receipt(service):
    store, request = service
    store.submit(request)
    spec = store.spec(request["run_id"])
    broker = DeliveryBroker(store, spec)
    candidate = broker.prepare()["candidate"]
    supervisor = DeliverySupervisor(store, capacity=1)
    role_request = {
        "spec": spec,
        "role": "implement",
        "iteration": 0,
        "candidate": candidate,
        "findings": [],
        "resume_session": None,
        "workspace": str(broker.checkout),
    }
    first = await supervisor.run(role_request)
    assert first["status"] == "pass"
    assert first["session_id"] == "fake:run-1:implement"
    assert (broker.checkout / "devflow-fake-change.txt").is_file()
    assert await supervisor.run(role_request) == first
    with store._connect() as db:
        attempts = db.execute("SELECT state,cleanup FROM delivery_attempts").fetchall()
    assert [tuple(row) for row in attempts] == [("finished", "confirmed")]


@pytest.mark.asyncio
@pytest.mark.skipif(
    not Path("/usr/bin/sandbox-exec").is_file() or not shutil.which("temporal"),
    reason="macOS Seatbelt and local Temporal CLI required",
)
async def test_real_temporal_finding_repairs_same_session_with_new_gates(service):
    original_store, request = service
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    config = json.loads(original_store.config.path.read_text())
    config.update(
        {
            "temporal_address": f"127.0.0.1:{port}",
            "queue": "delivery-real-temporal-test",
            "max_repairs": 1,
            "fake_findings": {"review": [0]},
        }
    )
    config["repositories"]["fixture"]["allowed_paths"].append("devflow-fake-change.txt")
    original_store.config.path.write_text(json.dumps(config))
    service_runtime = DeliveryService(original_store.config.path)
    server = await asyncio.create_subprocess_exec(
        "temporal",
        "server",
        "start-dev",
        "--headless",
        "--ip",
        "127.0.0.1",
        "--port",
        str(port),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        client = None
        for _ in range(100):
            try:
                client = await Client.connect(f"127.0.0.1:{port}")
                break
            except Exception as exc:
                if server.returncode is not None:
                    raise RuntimeError("Temporal dev server exited during test startup") from exc
                await asyncio.sleep(0.1)
        assert client is not None

        @activity.defn(name="delivery_publish")
        async def publish_stub(payload):
            broker = DeliveryBroker(service_runtime.store, payload["spec"])
            _git(broker.checkout, "add", "devflow-fake-change.txt")
            _git(broker.checkout, "commit", "-qm", f"fake candidate {payload['iteration']}")
            candidate = broker.candidate()
            return {
                "number": 1,
                "url": "https://example.invalid/pull/1",
                "state": "OPEN",
                "head": candidate["head"],
                "base": payload["spec"]["base_sha"],
                "candidate": candidate,
            }

        @activity.defn(name="delivery_checks")
        async def checks_stub(payload):
            return {"state": "passed", "candidate_id": payload["candidate"]["id"]}

        @activity.defn(name="delivery_precheck")
        async def precheck_stub(payload):
            return {"state": "passed", "candidate_id": payload["candidate"]["id"]}

        @activity.defn(name="delivery_ci")
        async def ci_stub(payload):
            return {"state": "passed", "head": payload["pull_request"]["head"]}

        @activity.defn(name="delivery_tracker")
        async def tracker_stub(_payload):
            return {"state": "consistent", "observed": {"fixture": True}}

        @activity.defn(name="delivery_tracker_start")
        async def tracker_start_stub(_payload):
            return {"state": "consistent", "observed": {"fixture": True}}

        async with Worker(
            client,
            task_queue=config["queue"],
            workflows=[DeliveryWorkflow],
            activities=[
                delivery_project,
                delivery_prepare,
                delivery_role,
                publish_stub,
                precheck_stub,
                checks_stub,
                ci_stub,
                tracker_start_stub,
                tracker_stub,
            ],
        ):
            service_runtime.store.submit(request)
            await service_runtime.dispatch_once()
            result = await asyncio.wait_for(
                client.get_workflow_handle("delivery-run-1").result(), timeout=30
            )
        assert result["outcome"] == "delivered"
        assert result["iteration"] == 1
        detail = service_runtime.store.detail("run-1")
        assert detail["phase"] == "delivered"
        assert [role["role"] for role in detail["roles"]] == [
            "implement",
            "review",
            "implement",
            "review",
            "verify",
        ]
        assert detail["roles"][0]["session_id"] == detail["roles"][2]["session_id"]
        assert detail["pull_request"]["number"] == 1
        assert detail["pull_request"]["head"] == detail["candidate"]["head"]
        assert detail["checks"]["review"]["state"] == "passed"
        assert detail["checks"]["qa"]["state"] == "passed"
        assert detail["checks"]["ci"]["state"] == "passed"
    finally:
        server.terminate()
        await server.wait()


@pytest.mark.asyncio
@pytest.mark.parametrize("qa_state", ["passed", "failed", "unknown"])
async def test_managed_browser_qa_precedes_independent_verify_and_blocks_failure(qa_state):
    calls = []
    candidate = {"id": "candidate-1", "head": "head-1"}
    spec = {
        "run_id": f"browser-qa-{qa_state}",
        "provider": "fake",
        "policy": {"max_repairs": 0, "browser_qa": {"id": "owned-browser"}},
    }

    @activity.defn(name="delivery_project")
    async def project_stub(_payload):
        return {"revision": 1}

    @activity.defn(name="delivery_prepare")
    async def prepare_stub(_payload):
        return {"candidate": candidate}

    @activity.defn(name="delivery_tracker_start")
    async def start_stub(_payload):
        return {"state": "consistent"}

    @activity.defn(name="delivery_role")
    async def role_stub(payload):
        role = payload["role"]
        calls.append(role)
        if role == "verify":
            assert payload["qa_evidence"] == {
                "path": "/owned/receipt.json",
                "sha256": "a" * 64,
                "log": "/owned/browser-qa.log",
                "log_sha256": "b" * 64,
                "candidate_id": candidate["id"],
                "iteration": 0,
            }
        return {"status": "pass", "session_id": f"fake:{role}", "candidate": candidate}

    @activity.defn(name="delivery_precheck")
    async def precheck_stub(_payload):
        return {"state": "passed"}

    @activity.defn(name="delivery_publish")
    async def publish_stub(_payload):
        return {"candidate": candidate, "head": candidate["head"]}

    @activity.defn(name="delivery_browser_qa")
    async def browser_stub(_payload):
        calls.append("browser_qa")
        return {
            "state": qa_state,
            "cleanup": "unknown" if qa_state == "unknown" else "confirmed",
            "receipt": "/owned/receipt.json",
            "receipt_sha256": "a" * 64,
            "log": "/owned/browser-qa.log",
            "log_sha256": "b" * 64,
        }

    @activity.defn(name="delivery_checks")
    async def checks_stub(_payload):
        calls.append("local_checks")
        return {"state": "passed"}

    @activity.defn(name="delivery_ci")
    async def ci_stub(_payload):
        return {"state": "passed"}

    @activity.defn(name="delivery_tracker")
    async def tracker_stub(_payload):
        return {"state": "consistent"}

    async with await WorkflowEnvironment.start_local() as environment:
        async with Worker(
            environment.client,
            task_queue=f"browser-qa-{qa_state}",
            workflows=[DeliveryWorkflow],
            activities=[
                project_stub,
                prepare_stub,
                start_stub,
                role_stub,
                precheck_stub,
                publish_stub,
                browser_stub,
                checks_stub,
                ci_stub,
                tracker_stub,
            ],
        ):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run,
                spec,
                id=spec["run_id"],
                task_queue=spec["run_id"],
            )
            result = await handle.result()
    assert calls[:4] == ["implement", "review", "local_checks", "browser_qa"]
    assert ("verify" in calls) is (qa_state == "passed")
    assert result["outcome"] == ("delivered" if qa_state == "passed" else "blocked")
    if qa_state == "unknown":
        assert result["cleanup"] == "unknown"


@pytest.mark.asyncio
async def test_managed_decision_wait_survives_worker_restart(service, tmp_path):
    original, request = service
    config = json.loads(original.config.path.read_text())
    config["repositories"]["fixture"]["initial_decision_prompt"] = "Proceed with this run?"
    original.config.path.write_text(json.dumps(config))
    store = DeliveryStore(DeliveryConfig.load(original.config.path))
    store.submit(request)
    admitted = store.spec(request["run_id"])
    calls = []

    @activity.defn(name="delivery_tracker_start")
    async def tracker_start_stub(_payload):
        return {"state": "consistent"}

    @activity.defn(name="delivery_role")
    async def role_stub(payload):
        calls.append(payload["role"])
        return {"status": "blocked", "candidate": payload["candidate"]}

    activities = [delivery_project, delivery_prepare, tracker_start_stub, role_stub]
    async with await WorkflowEnvironment.start_local(
        dev_server_database_filename=str(tmp_path / "decision-temporal.sqlite3")
    ) as environment:
        queue = "managed-decision-restart"
        async with Worker(
            environment.client,
            task_queue=queue,
            workflows=[DeliveryWorkflow],
            activities=activities,
        ):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, admitted, id="delivery-run-1", task_queue=queue
            )
            store.mark_start(request["run_id"], accepted=True)
            for _ in range(50):
                if store.detail(request["run_id"])["decisions"]:
                    break
                await asyncio.sleep(0.05)
            waiting = store.detail(request["run_id"])
            assert waiting["decisions"][0]["id"] == "run-1:initial"
            assert waiting["decisions"][0]["candidate_revision"] == 1
            assert calls == []
        async with Worker(
            environment.client,
            task_queue=queue,
            workflows=[DeliveryWorkflow],
            activities=activities,
        ):
            current = await handle.query("status")
            assert current["phase"] == "waiting_decision"
            with pytest.raises(WorkflowUpdateFailedError):
                await handle.execute_update(
                    "decision",
                    {
                        "expected_revision": current["revision"],
                        "decision_id": "wrong",
                        "decision_revision": 1,
                        "candidate_revision": 1,
                        "answer": "proceed",
                    },
                )
            accepted = await handle.execute_update(
                "decision",
                {
                    "expected_revision": current["revision"],
                    "decision_id": "run-1:initial",
                    "decision_revision": 1,
                    "candidate_revision": 1,
                    "answer": "proceed",
                },
            )
            assert accepted["decision"] is None
            final = await asyncio.wait_for(handle.result(), 15)
        assert final["outcome"] == "blocked"
        assert calls == ["implement"]
        assert store.detail(request["run_id"])["decisions"] == []


@pytest.mark.asyncio
async def test_managed_cancel_wins_over_overlapping_failed_precheck(service, tmp_path):
    original, request = service
    config = json.loads(original.config.path.read_text())
    config["repositories"]["fixture"]["allowed_paths"].append("devflow-fake-change.txt")
    original.config.path.write_text(json.dumps(config))
    store = DeliveryStore(DeliveryConfig.load(original.config.path))
    store.submit(request)
    entered = asyncio.Event()
    release = asyncio.Event()

    @activity.defn(name="delivery_tracker_start")
    async def tracker_start_stub(_payload):
        return {"state": "consistent"}

    @activity.defn(name="delivery_precheck")
    async def failing_precheck(_payload):
        entered.set()
        await release.wait()
        return {"state": "failed", "results": []}

    @activity.defn(name="delivery_role")
    async def role_stub(payload):
        return {
            "status": "pass",
            "summary": "Fixture role completed",
            "findings": [],
            "candidate": payload["candidate"],
            "session_id": "fake:implement",
            "usage": None,
        }

    activities = [
        delivery_project,
        delivery_prepare,
        role_stub,
        tracker_start_stub,
        failing_precheck,
    ]
    async with await WorkflowEnvironment.start_local(
        dev_server_database_filename=str(tmp_path / "cancel-temporal.sqlite3")
    ) as environment:
        async with Worker(
            environment.client,
            task_queue="managed-cancel-race",
            workflows=[DeliveryWorkflow],
            activities=activities,
        ):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run,
                store.spec(request["run_id"]),
                id="delivery-run-1",
                task_queue="managed-cancel-race",
            )
            store.mark_start(request["run_id"], accepted=True)
            await asyncio.wait_for(entered.wait(), 15)
            current = await handle.query("status")
            assert current["phase"] == "prepublish_checks"
            accepted = await handle.execute_update(
                "cancel", {"expected_revision": current["revision"], "reason": "stop now"}
            )
            assert accepted["execution_state"] == "cancelling"
            release.set()
            final = await asyncio.wait_for(handle.result(), 15)
        assert final["outcome"] == "cancelled"
        assert final["cleanup"] == "confirmed_after_role_boundary"
        detail = store.detail(request["run_id"])
        assert detail["outcome"] == "cancelled"
        assert detail["phase"] == "cancelled"
        assert detail["checks"]["prepublish"]["state"] == "failed"
        assert (
            next(gate for gate in detail["phase_gates"] if gate["id"] == "prepublish")["state"]
            == "failed"
        )


@pytest.mark.asyncio
async def test_cancelled_role_with_unknown_teardown_retains_unknown_cleanup(service, tmp_path):
    original, request = service
    store = original
    store.submit(request)
    entered = asyncio.Event()
    release = asyncio.Event()

    @activity.defn(name="delivery_tracker_start")
    async def tracker_start_stub(_payload):
        return {"state": "consistent"}

    @activity.defn(name="delivery_role")
    async def ambiguous_role(payload):
        entered.set()
        await release.wait()
        with store._connect() as db:
            db.execute(
                """INSERT INTO delivery_attempts
                   (job_key,run_id,role,iteration,candidate_id,state,cleanup)
                   VALUES (?,?,?,0,?,'unknown','unknown')""",
                ("unknown-attempt", request["run_id"], "implement", payload["candidate"]["id"]),
            )
        return {
            "status": "recovery_unknown",
            "summary": "Child exited without a final receipt",
            "findings": ["outcome ambiguous"],
            "candidate": payload["candidate"],
            "session_id": None,
            "cleanup": "unknown",
            "finish_reason": "recovery_unknown",
        }

    async with await WorkflowEnvironment.start_local(
        dev_server_database_filename=str(tmp_path / "unknown-cancel.sqlite3")
    ) as environment:
        async with Worker(
            environment.client,
            task_queue="unknown-cancel",
            workflows=[DeliveryWorkflow],
            activities=[delivery_project, delivery_prepare, tracker_start_stub, ambiguous_role],
        ):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run,
                store.spec(request["run_id"]),
                id="delivery-run-1",
                task_queue="unknown-cancel",
            )
            store.mark_start(request["run_id"], accepted=True)
            await asyncio.wait_for(entered.wait(), 15)
            status = await handle.query("status")
            accepted = await handle.execute_update(
                "cancel", {"expected_revision": status["revision"], "reason": "stop"}
            )
            assert accepted["execution_state"] == "cancelling"
            release.set()
            final = await asyncio.wait_for(handle.result(), 15)
    assert final["outcome"] == "cancelled"
    assert final["cleanup"] == "unknown"
    detail = store.detail(request["run_id"])
    assert detail["cleanup"] == "unknown"
    assert detail["capacity"]["active"] == 1
