"""Synthetic execution-prerequisite regressions; never launch native processes."""

import json
from pathlib import Path

import pytest
from test_delivery_native_addon import addon as addon

from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_resources import RunResources, read_private
from devflow_temporal.delivery_sandbox import _native_env


def test_short_scratch_is_owned_bounded_distinct_and_replayable(tmp_path, monkeypatch):
    state = tmp_path / ("long-service-name-" * 10) / "run-test"
    state.mkdir(parents=True)
    spec = {"state_dir": str(state), "run_id": state.name}
    resources = RunResources(spec)
    actual_root = resources._execution_scratch_root()
    assert len(str(actual_root / ("a" * 16) / "tsx-501" / "9999999.pipe").encode()) < 103
    root = tmp_path / "short"
    monkeypatch.setattr(RunResources, "_execution_scratch_root", lambda self: root)
    first = resources.execution_scratch("checks", "checks/1/api-regressions")
    second = resources.execution_scratch("checks", "checks/2/api-regressions")
    assert first != second and first.parent == second.parent == root
    assert resources.execution_scratch("checks", "checks/1/api-regressions") == first
    record = read_private(resources.manifest)["roots"][str(root)]
    assert (
        record["kind"] == "execution-scratch" and record["identity"]["inode"] == root.stat().st_ino
    )
    for value in ("../escape", "/foreign"):
        with pytest.raises(ValueError):
            resources.execution_scratch("checks", value)
    with pytest.raises(ValueError):
        resources.register(root.with_name("foreign"), "execution-scratch")


@pytest.mark.parametrize("existing", ["directory", "symlink"])
def test_short_scratch_rejects_preexisting_foreign_roots(tmp_path, monkeypatch, existing):
    state = tmp_path / "run-test"
    state.mkdir()
    root = tmp_path / "short"
    if existing == "directory":
        root.mkdir()
    else:
        root.symlink_to(state, target_is_directory=True)
    monkeypatch.setattr(RunResources, "_execution_scratch_root", lambda self: root)
    with pytest.raises(ValueError):
        RunResources({"state_dir": str(state), "run_id": state.name}).execution_scratch(
            "checks", "api"
        )


def test_all_temp_environment_aliases_use_owned_root_and_do_not_inherit_credentials(monkeypatch):
    monkeypatch.setenv("TMPDIR", "/foreign/long/temp")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "synthetic-secret")
    scratch = Path("/private/tmp/dftmp-controlled/0123456789abcdef")
    env = _native_env(Path("/durable/home"), Path("/durable/codex"), scratch)
    assert all(env[key] == str(scratch) for key in ["TMPDIR", "TMP", "TEMP"])
    assert "AWS_SECRET_ACCESS_KEY" not in env


def test_native_projects_are_from_frozen_lock_not_plan_test_names(addon):
    from devflow_temporal.delivery_native_dependencies import frozen_native_projects

    spec, checkout, _, _, _, _ = addon
    spec["accepted_plan"] = json.dumps({"verification": ["Run the entire API regression gate"]})
    assert frozen_native_projects(spec, checkout) == ["apps/api"]
    lock = checkout / "pnpm-lock.yaml"
    lock.write_text(lock.read_text() + "\n# candidate drift\n")
    with pytest.raises(ValueError, match="differs from the admitted Git base"):
        frozen_native_projects(spec, checkout)


def test_broker_validates_native_owners_without_planned_vitest(addon, tmp_path, monkeypatch):
    from devflow_temporal import delivery_native_dependencies as deps

    spec, checkout, _, _, _, _ = addon
    broker = object.__new__(DeliveryBroker)
    broker.spec = {**spec, "provider": "codex"}
    monkeypatch.setattr(
        "devflow_temporal.delivery_preparation.require_native_execution", lambda *_: None
    )
    observed = []
    original = deps.native_addon_authority

    def observe(spec, checkout, projects):
        observed.append(projects)
        return original(spec, checkout, projects)

    monkeypatch.setattr(deps, "native_addon_authority", observe)

    class BeforeAnyProcess(Exception):
        pass

    def stop(_):
        raise BeforeAnyProcess

    monkeypatch.setattr(broker, "_ensure_native_dependency_store", stop)
    with pytest.raises(BeforeAnyProcess):
        broker._run_check_list(
            checkout,
            [{"id": "docs-install", "argv": ["corepack", "/store"]}],
            tmp_path / "evidence",
            {"id": "candidate"},
            native_projects=[],
        )
    assert observed == [["apps/api"]]


def test_implementation_prepares_configured_install_and_handoff_without_named_node_tests(
    tmp_path,
    monkeypatch,
):
    from devflow_temporal import delivery_plan_checks

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    state = tmp_path / "run-test"
    state.mkdir()
    install = {"id": "docs-install", "argv": ["corepack", "pnpm", "install", "/store"]}
    broker = object.__new__(DeliveryBroker)
    broker.spec = {
        "state_dir": str(state),
        "run_id": state.name,
        "policy": {"prepublish_checks": [install]},
    }
    broker.checkout, broker.state_dir = checkout, state
    broker.evidence_dir = state
    candidate = {"id": "unchanged"}
    monkeypatch.setattr(broker, "candidate", lambda: candidate)
    monkeypatch.setattr(delivery_plan_checks, "planned_checks", lambda *_a, **_kw: [])
    observed = []

    def check_list(checkout, checks, evidence, candidate, **options):
        observed.extend(checks)
        return {
            "state": "passed",
            "results": [],
            "node_toolchain": {"node_interpreter": {"absolute_path": "/frozen/node"}},
        }

    monkeypatch.setattr(broker, "_run_check_list", check_list)
    result = broker.run_implementation_preparation(1, candidate)
    assert (
        observed == [install]
        and result["node_toolchain"]["node_interpreter"]["absolute_path"] == "/frozen/node"
    )


@pytest.mark.parametrize("role", [False, True])
def test_role_and_check_profiles_get_short_temp_without_moving_durable_home(
    tmp_path, monkeypatch, role
):
    from devflow_temporal import delivery_sandbox as sandbox

    state = tmp_path / "run-test"
    state.mkdir()
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    auth = tmp_path / "synthetic-auth.json"
    auth.write_text("{}")
    auth.chmod(0o600)
    spec = {
        "run_id": state.name,
        "state_dir": str(state),
        "provider": "codex",
        "policy": {"host_sandbox": "trusted-local", "codex_auth_path": str(auth)},
    }
    monkeypatch.setattr(
        "devflow_temporal.delivery_preparation.require_native_execution", lambda *_: None
    )
    monkeypatch.setattr(sandbox, "_protected_native_commands", lambda *_: ())
    short = tmp_path / "short"
    monkeypatch.setattr(RunResources, "_execution_scratch_root", lambda self: short)
    evidence = state / "checks" / "2"
    evidence.mkdir(parents=True)
    if role:
        request = {"spec": spec, "role": "implement", "iteration": 2, "workspace": str(checkout)}
        _, env = sandbox.prepare_native_role(request, state / "attempt")
    else:
        _, env = sandbox.prepare_native_check(spec, checkout, evidence, {"id": "api"})
    assert Path(env["TMPDIR"]).parent == short
    assert all(env[k] == env["TMPDIR"] for k in ["TMP", "TEMP"])
    assert Path(env["HOME"]).is_relative_to(state)
    assert Path(env["CODEX_HOME"]).is_relative_to(state)
    assert (
        read_private(RunResources(spec).manifest)["roots"][str(short)]["kind"]
        == "execution-scratch"
    )


def test_short_scratch_cleanup_preserves_durable_evidence_and_foreign_directories(
    tmp_path, monkeypatch
):
    state = tmp_path / "run-test"
    state.mkdir()
    durable = state / "original-command.log"
    durable.write_text("original failure")
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    sentinel = foreign / "keep.txt"
    sentinel.write_text("KEEP")
    root = tmp_path / "short"
    monkeypatch.setattr(RunResources, "_execution_scratch_root", lambda self: root)
    resources = RunResources({"run_id": state.name, "state_dir": str(state)})
    scratch = resources.execution_scratch("checks", "api")
    (scratch / "temporary.txt").write_text("temporary")
    (scratch / "alias").symlink_to(foreign, target_is_directory=True)
    result = resources.finalize("blocked")
    assert result["state"] == "confirmed" and not root.exists()
    assert durable.read_text() == "original failure"
    assert sentinel.read_text() == "KEEP"
    recreated = resources.execution_scratch("checks", "next-api")
    assert recreated.is_dir()
    assert read_private(resources.manifest)["roots"][str(root)]["generation"] == 1
    resources.finalize("blocked")
