"""Configured Python prerequisites have custody before an intake plan exists."""

from __future__ import annotations

import copy
import json
import os
import shutil
import subprocess
import sys
from importlib.metadata import distribution
from pathlib import Path

import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_resources import spec
from test_delivery_store import _git

from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_configured_resources import implementation_prerequisites
from devflow_temporal.delivery_resources import RunResources, read_private


def test_implementation_preparation_preserves_exact_frozen_recipe_order():
    node = {"id": "node", "argv": ["corepack", "pnpm", "install", "--store-dir", "/store"]}
    python = {"id": "python", "kind": "check", "argv": ["uv", "sync", "--locked"],
              "generated_directories": ["worker/.venv"]}
    consumer = {"id": "consumer", "argv": ["node", "benchmark.mjs"]}
    candidate_only = {**python, "id": "candidate-only"}
    tests = {**python, "id": "tests", "kind": "test"}
    owned = {"policy": {"host_sandbox": "trusted-local",
                        "baseline_checks": [node, python, consumer, tests],
                        "prepublish_checks": [node, consumer, python, candidate_only, tests]}}
    frozen = copy.deepcopy(owned)
    assert implementation_prerequisites(owned) == [node, python]
    assert owned == frozen
    # Matching an ID cannot authorize an altered recipe.
    owned["policy"]["baseline_checks"][1] = {**python, "argv": ["different-command"]}
    assert implementation_prerequisites(owned) == [node]


@pytest.mark.parametrize("declaration", ["worker/.venv", ["worker/../.venv"],
                                         ["/tmp/worker/.venv"], ["worker/output"], [None]])
def test_implementation_preparation_does_not_infer_python_authority(declaration):
    recipe = {"id": "candidate", "argv": ["unrelated-command"],
              "generated_directories": declaration}
    owned = {"policy": {"host_sandbox": "trusted-local", "baseline_checks": [recipe],
                        "prepublish_checks": [recipe]}}
    assert implementation_prerequisites(owned) == []


def configured(tmp_path, kind="gate"):
    owned = spec(tmp_path)
    owned.update(accepted_plan="", baseline_checks_version=2)
    check = {
        "id": "python-prerequisite", "kind": "check", "cwd": ".",
        "argv": [sys.executable, "-c", "print('configured prerequisite')"],
        "generated_directories": ["worker/.venv"],
    }
    owned["policy"].update(host_sandbox="trusted-local", baseline_checks=[check])
    root = (Path(owned["state_dir"]) / "gates/0/baseline" if kind == "gate"
            else Path(owned["checkout"]))
    root.parent.mkdir(parents=True)
    resources = RunResources(owned)
    resources.register(root, kind)
    root.mkdir()
    resources.created(root)
    project = root / "worker"
    project.mkdir()
    (root / ".gitignore").write_text(".venv/\n")
    (project / "pyproject.toml").write_text('[project]\nname="fixture"\nversion="1"\n')
    (project / "uv.lock").write_text("version=1\n")
    _git(root, "init", "-q")
    _git(root, "add", ".")
    broker = object.__new__(DeliveryBroker)
    broker.spec = owned
    return owned, resources, broker, root, project


@pytest.mark.parametrize("kind", ["gate", "checkout"])
@pytest.mark.parametrize("stage", ["baseline_checks", "prepublish_checks", "checks"])
def test_configured_environment_before_intake_survives_plan_and_source_changes(
    tmp_path, kind, stage,
):
    owned, resources, broker, root, project = configured(tmp_path, kind)
    check = owned["policy"].pop("baseline_checks")
    owned["policy"][stage] = check
    environment = project / ".venv"
    assert broker._register_generated(root, ["worker/.venv"]) == [environment]
    environment.mkdir()
    (environment / "package").write_text("owned fixture dependency")
    broker._record_generated([environment])
    custody = read_private(resources.manifest)["roots"][str(environment)]
    assert custody["configured_environment_sha256"]
    assert "accepted_plan_sha256" not in custody
    # Intake happens after baseline. Partial product work may also change metadata.
    owned["accepted_plan"] = json.dumps({"verification": ["Run the API suite"]})
    (project / "uv.lock").unlink()
    assert resources.finalize("blocked")["state"] == "confirmed"
    assert not environment.exists()
    assert resources.finalize("blocked")["state"] == "confirmed"


@pytest.mark.parametrize("change", ["remove", "argv", "directory"])
def test_changed_configured_authority_preserves_environment(tmp_path, change):
    owned, resources, broker, root, project = configured(tmp_path)
    environment = project / ".venv"
    broker._register_generated(root, ["worker/.venv"])
    environment.mkdir()
    (environment / "sentinel").write_text("preserve changed authority")
    broker._record_generated([environment])
    check = owned["policy"]["baseline_checks"][0]
    if change == "remove":
        owned["policy"]["baseline_checks"] = []
    elif change == "argv":
        check["argv"].append("changed")
    else:
        check["generated_directories"] = ["outside/.venv"]
    with pytest.raises(ValueError, match="authority changed"):
        broker._register_generated(root, ["worker/.venv"])
    assert resources.finalize("blocked")["state"] == "unknown"
    assert (environment / "sentinel").read_text() == "preserve changed authority"


@pytest.mark.parametrize("violation", ["foreign", "symlink", "untracked", "metadata_link",
                                        "missing_lock", "undeclared", "profile", "string"])
def test_configured_environment_does_not_adopt_foreign_or_unlocked_paths(tmp_path, violation):
    owned, _resources, broker, root, project = configured(tmp_path)
    environment = project / ".venv"
    sentinel = tmp_path / "sentinel"
    sentinel.mkdir()
    (sentinel / "precious").write_text("SAFE")
    if violation == "foreign":
        environment.mkdir()
    elif violation == "symlink":
        environment.symlink_to(sentinel, target_is_directory=True)
    elif violation == "untracked":
        _git(root, "rm", "--cached", "worker/uv.lock")
    elif violation == "metadata_link":
        (project / "uv.lock").unlink()
        (project / "uv.lock").symlink_to(sentinel / "precious")
    elif violation == "missing_lock":
        (project / "uv.lock").unlink()
    elif violation == "undeclared":
        owned["policy"]["baseline_checks"] = []
    elif violation == "string":
        owned["policy"]["baseline_checks"][0]["generated_directories"] = "worker/.venv"
    else:
        owned["policy"]["host_sandbox"] = "native-profile"
    with pytest.raises(ValueError):
        broker._register_generated(root, ["worker/.venv"])
    assert (sentinel / "precious").read_text() == "SAFE"


@pytest.mark.skipif(sys.platform != "darwin", reason="native macOS execution authority")
@pytest.mark.parametrize("fails", [False, True])
@pytest.mark.parametrize("stage", ["baseline", "implementation"])
def test_real_native_preparation_owns_locked_python_before_consumers(
    api_fixture, tmp_path, fails, stage,
):
    from devflow_temporal.delivery_baseline import run_baseline_checks
    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_preparation import prepare_authority
    from devflow_temporal.delivery_store import DeliveryStore

    config_path, request = api_fixture
    raw = json.loads(config_path.read_text())
    repository = raw["repositories"]["fixture"]
    source = Path(repository["source_path"])
    project = source / "worker"
    project.mkdir()
    (source / ".gitignore").write_text(".venv/\n")
    (project / "pyproject.toml").write_text(
        '[project]\nname="owned-baseline-fixture"\nversion="1"\nrequires-python=">=3.12"\n'
    )
    uv = shutil.which("uv")
    assert uv
    subprocess.run([uv, "lock", "--offline", "--python", sys.executable], cwd=project,
                   env={k: v for k, v in os.environ.items() if k != "UV_EXCLUDE_NEWER"},
                   check=True, capture_output=True)
    _git(source, "add", ".")
    _git(source, "commit", "-qm", "locked Python fixture")
    repository["expected_base_sha"] = _git(source, "rev-parse", "HEAD")
    prerequisite = {
        "id": "python-prerequisite", "kind": "check", "cwd": ".",
        "argv": [uv, "--offline", "--project", "worker", "sync", "--locked",
                 "--python", str(Path(sys.executable).resolve())],
        "generated_directories": ["worker/.venv"],
    }
    consumer = {
        "id": "baseline-consumer", "kind": "check", "cwd": ".",
        "argv": [sys.executable, "-I", "-c",
                 "from pathlib import Path; import sys; "
                 "assert Path('worker/.venv/bin/python').exists(); "
                 "print('locked Python environment consumed'); " + f"sys.exit({int(fails)})"],
    }
    repository.update(baseline_check_ids=[prerequisite["id"], consumer["id"]],
                      prepublish_checks=[prerequisite, consumer], checks=[prerequisite, consumer],
                      required_ci=["test"], project_url="https://github.com/users/example/projects/1",
                      assignee="example")
    auth = tmp_path / "fixture-auth.json"
    auth.write_text('{"OPENAI_API_KEY":"unusable-owned-fixture"}')
    auth.chmod(0o600)
    raw.update(provider="codex", execution_mode="trusted-local", codex_auth_path=str(auth),
               codex_bin=str(distribution("openai-codex-cli-bin").locate_file(
                   "codex_cli_bin/bin/codex")))
    raw["roles"] = {role: {"model": "gpt-6.1-sol", "effort": "high"}
                    for role in ["intake", "implement", "review", "verify"]}
    config_path.write_text(json.dumps(raw))
    if stage == "baseline":
        request.pop("accepted_plan")
    else:
        # Cross-language probes need the worker even without a named pytest file.
        request["accepted_plan"] = json.dumps({"verification": ["Run API acceptance checks"]})
    store = DeliveryStore(DeliveryConfig.load(config_path))
    store.submit(request)
    owned = prepare_authority(store, store.submitted_spec(request["run_id"]))
    if stage == "baseline":
        assert owned["accepted_plan"] == "" and owned["baseline_checks_version"] == 2
    broker = DeliveryBroker(store, owned)
    broker.prepare()
    try:
        if stage == "baseline":
            result = run_baseline_checks(broker)
            environment = broker._gate_path("baseline", 0) / "worker/.venv"
        else:
            candidate = broker.candidate()
            prepared = broker.run_implementation_preparation(0, candidate)
            assert prepared["state"] == "passed"
            assert [r["id"] for r in prepared["results"]] == [prerequisite["id"]]
            environment = broker.checkout / "worker/.venv"
            assert (environment / "bin/python").exists()
            custody = read_private(RunResources(owned).manifest)["roots"][str(environment)]
            assert custody["identity"] and custody["configured_environment_sha256"]
            # Retrying preparation reattaches the same native execution, then the
            # later candidate gates reuse the registered environment normally.
            assert broker.run_implementation_preparation(0, candidate) == prepared
            result = broker._run_check_list(
                broker.checkout, [prerequisite, consumer],
                broker.evidence_dir / "prechecks/0", candidate,
            )
            assert broker.candidate() == candidate
        assert result["state"] == ("failed" if fails else "passed")
        if stage == "baseline":
            assert result["feature_unchanged"]
        assert [r["passed"] for r in result["results"]] == [True, not fails]
        assert all(r["native_process"]["monitoring_complete"] for r in result["results"])
        assert environment.is_dir()
        owned["accepted_plan"] = json.dumps({"verification": ["Run the product suite"]})
    finally:
        assert RunResources(owned).finalize("blocked")["state"] == "confirmed"
    assert not environment.exists()
    assert not (project / ".venv").exists()
