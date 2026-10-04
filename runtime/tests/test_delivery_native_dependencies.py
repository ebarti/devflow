from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import ControlledNativeTerminalFixture, controlled_terminal_tracker
from test_delivery_native import native_configuration as native_configuration

from devflow_temporal.delivery_activities import delivery_finalize_resources, delivery_project
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_native_dependencies import frozen_pnpm_inputs, write_frozen_inputs
from devflow_temporal.delivery_preparation import prepare_authority
from devflow_temporal.delivery_store import DeliveryStore

INTEGRITY = (
    "sha512-41Cifkg6e8TylSpdtTpeLVMqvSBEVzTttHvERD741+pnZ8ANv0004MRL43"
    "QKPDlK9cGvNp6NZWZUBlbGXYxxng=="
)
LOCK = """lockfileVersion: '9.0'
settings:
  autoInstallPeers: true
  excludeLinksFromLockfile: false
importers:
  .:
    dependencies:
      is-number:
        specifier: 7.0.0
        version: 7.0.0
packages:
  is-number@7.0.0:
    resolution:
      integrity: INTEGRITY
    engines: {node: '>=0.12.0'}
snapshots:
  is-number@7.0.0: {}
""".replace("INTEGRITY", INTEGRITY)


def fixture_inputs(source: Path, lock=LOCK):
    source.joinpath("pnpm-lock.yaml").write_text(lock)
    source.joinpath("package.json").write_text(
        json.dumps(
            {
                "name": "owned-native-dependency-fixture",
                "version": "1.0.0",
                "packageManager": "pnpm@10.24.0",
                "dependencies": {"is-number": "7.0.0"},
                "scripts": {
                    "preinstall": (
                        "node -e \"require('fs').writeFileSync('setup-hook-executed','BAD')\""
                    )
                },
            }
        )
    )
    source.joinpath(".pnpmfile.cjs").write_text(
        "module.exports={hooks:{readPackage:p=>{require('fs').writeFileSync('setup-hook-executed',"
        "'BAD');return p}}}"
    )
    source.joinpath(".gitignore").write_text("node_modules/\n")
    subprocess.run(["git", "-C", str(source), "add", "."], check=True)
    subprocess.run(
        ["git", "-C", str(source), "commit", "-qm", "frozen dependency fixture"], check=True
    )
    head = subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True
    ).strip()
    return {"source_path": str(source), "base_sha": head}


def test_frozen_fetch_excludes_setup_and_rejects_candidate_lock_or_manager_drift(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    subprocess.run(
        ["git", "-C", str(source), "config", "user.email", "fixture@example.com"], check=True
    )
    subprocess.run(["git", "-C", str(source), "config", "user.name", "Fixture"], check=True)
    source.joinpath("pnpm-workspace.yaml").write_text(
        "packages: [packages/*]\nonlyBuiltDependencies: [esbuild]\n"
    )
    spec = fixture_inputs(source)
    provenance = {}
    manager, inputs = frozen_pnpm_inputs(spec, source, provenance=provenance)
    staging = tmp_path / "registered-staging"
    staging.mkdir(mode=0o700)
    hashes = write_frozen_inputs(staging, inputs)
    assert manager == "pnpm@10.24.0" and set(hashes) == {
        "pnpm-lock.yaml", "package.json", "pnpm-workspace.yaml"
    }
    assert json.loads((staging / "package.json").read_bytes()) == {"packageManager": manager}
    assert provenance["source_input_hashes"] == {
        name: hashlib.sha256((source / name).read_bytes()).hexdigest() for name in inputs
    }
    assert hashes["package.json"] != provenance["source_input_hashes"]["package.json"]
    assert provenance["transformations"]["package.json"] == {
        "operation": "json-key-allowlist", "keys": ["packageManager"]
    }
    assert hashes["pnpm-lock.yaml"] == provenance["source_input_hashes"]["pnpm-lock.yaml"]
    assert hashes["pnpm-workspace.yaml"] != provenance["source_input_hashes"]["pnpm-workspace.yaml"]
    assert "onlyBuiltDependencies" not in (staging / "pnpm-workspace.yaml").read_text()
    assert provenance["transformations"]["pnpm-workspace.yaml"]["operation"] == "yaml-key-allowlist"
    assert not (staging / ".pnpmfile.cjs").exists()
    source.joinpath("pnpm-lock.yaml").write_text(LOCK + "# candidate drift\n")
    with pytest.raises(ValueError, match="lock differs"):
        frozen_pnpm_inputs(spec, source)
    source.joinpath("pnpm-lock.yaml").write_text(LOCK)
    manifest = json.loads((source / "package.json").read_bytes())
    manifest["packageManager"] = "pnpm@11.10.0"
    source.joinpath("package.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="package manager differs"):
        frozen_pnpm_inputs(spec, source)


@pytest.mark.parametrize(
    "target",
    [
        "https://example.com/is-number.tgz",
        "https://registry.npmjs.org.evil.test/x",
        "https://token@registry.npmjs.org/x",
        "http://registry.npmjs.org/x",
        "file:/outside/x",
    ],
)
def test_frozen_lock_external_sources_rejected_before_launch(tmp_path, target):
    source = tmp_path / "source"
    source.mkdir()
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    subprocess.run(
        ["git", "-C", str(source), "config", "user.email", "fixture@example.com"], check=True
    )
    subprocess.run(["git", "-C", str(source), "config", "user.name", "Fixture"], check=True)
    lock = LOCK.replace("    engines:", f"      tarball: {target}\n    engines:")
    spec = fixture_inputs(source, lock)
    with pytest.raises(ValueError, match="outside the admitted npm registry"):
        frozen_pnpm_inputs(spec, source)


@pytest.mark.skipif(sys.platform != "darwin", reason="actual native macOS PNPM fetch required")
@pytest.mark.asyncio
async def test_fresh_native_registry_fetch_offline_install_check_and_resource_removal(
    native_configuration,
):
    config, request = native_configuration
    repo = config.raw["repositories"]["fixture"]
    source = Path(repo["source_path"])
    frozen = fixture_inputs(source)
    subprocess.run(["git", "-C", str(source), "push", "-q", "origin", "HEAD"], check=True)
    repo["expected_base_sha"] = frozen["base_sha"]
    protected_log = (
        config.state_root / "runs" / request["run_id"] / "checks/0"
        / "registry-dependent-check/native/process.log"
    )
    checks = [
        {
            "id": "install",
            "argv": [
                "corepack",
                "pnpm",
                "install",
                "--offline",
                "--frozen-lockfile",
                "--ignore-scripts",
                "--ignore-pnpmfile",
                "--store-dir",
                "/store",
            ],
        },
        {
            "id": "registry-dependent-check",
            "argv": [
                "node",
                "-e",
                "const a=require('node:assert');const n=require('is-number');"
                "const fs=require('node:fs');fs.fstatSync(0);"
                "a.ok(fs.fstatSync(1).isFIFO());a.ok(fs.fstatSync(2).isFIFO());"
                + f"a.throws(()=>fs.readFileSync({json.dumps(str(protected_log))}),"
                + "e=>['EPERM','EACCES'].includes(e.code));"
                + "a.equal(n(7),true);a.equal(n('bad'),false);console.log('2 passed')",
            ],
            "test_count_regex": r"(\d+) passed",
            "min_tests": 2,
        },
    ]
    repo.update(checks=checks, prepublish_checks=checks)
    config.raw.update(
        toolchain_roots=["/Users/eloibarti/.nvm/versions/node/v22.21.1"],
        package_manager_cache="/Users/eloibarti/.cache/node/corepack",
    )
    config.path.write_text(json.dumps(config.raw))
    store = DeliveryStore(config)
    store.submit(request)
    spec = prepare_authority(store, store.submitted_spec(request["run_id"]))
    broker = DeliveryBroker(store, spec)
    candidate = broker.prepare()["candidate"]
    outcome = "cancelled"
    try:
        checks = broker.run_checks(0, candidate)
        assert checks["state"] == "passed" and checks["results"][1]["test_count"] == 2
        dependency = checks["results"][0]["dependency_preparation"]
        assert dependency["state"] == "passed"
        assert dependency["native_process"]["cleanup"] == "observed-native-confirmed"
        assert dependency["candidate_setup_executed"] is False
        assert dependency["input_provenance"]["source_input_hashes"]["package.json"] == (
            hashlib.sha256((source / "package.json").read_bytes()).hexdigest()
        )
        assert any(Path(dependency["store"]).iterdir())
        gate = Path(checks["results"][0]["cwd"])
        assert (gate / "node_modules/is-number").exists()
        assert not any(Path(spec["state_dir"]).rglob("setup-hook-executed"))
        replay = broker._ensure_native_dependency_store(gate)
        assert replay == dependency  # durable receipt, not another network fetch
        outcome = "delivered"
    finally:
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal"),
        ) as environment:
            async with Worker(
                environment.client,
                task_queue="native-dependency-finalization",
                workflows=[ControlledNativeTerminalFixture],
                workflow_runner=UnsandboxedWorkflowRunner(),
                activities=[delivery_project, delivery_finalize_resources,
                            controlled_terminal_tracker],
            ):
                terminal = await environment.client.execute_workflow(
                    ControlledNativeTerminalFixture.run,
                    {"spec": spec, "outcome": outcome, "uncertain": False},
                    id="native-dependency-finalization",
                    task_queue="native-dependency-finalization",
                )
        cleanup = terminal["checks"]["resource_cleanup"]
    assert cleanup["resource_cleanup"] == "confirmed"
    assert not (Path(spec["state_dir"]) / "transient").exists()
    assert not gate.exists() and Path(dependency["log"]).is_file()
