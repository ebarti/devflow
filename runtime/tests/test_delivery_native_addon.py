"""Deterministic registry/authentication guards; actual native execution is separate."""

import base64
import hashlib
import json

import pytest
import yaml

from devflow_temporal.delivery_native_dependencies import (
    native_addon_authority,
    validate_native_addon,
)


@pytest.fixture
def addon(tmp_path, monkeypatch):
    checkout = tmp_path / "checkout"
    api = checkout / "apps/api"
    api.mkdir(parents=True)
    frozen = {
        "package.json": json.dumps(
            {
                "packageManager": "pnpm@10.24.0",
                "pnpm": {"onlyBuiltDependencies": ["better-sqlite3", "esbuild"]},
            }
        ).encode(),
        "apps/api/package.json": b'{"dependencies":{"better-sqlite3":"^12.9.0"}}',
        "pnpm-lock.yaml": yaml.safe_dump(
            {
                "lockfileVersion": "9.0",
                "importers": {
                    "apps/api": {
                        "dependencies": {
                            "better-sqlite3": {"specifier": "^12.9.0", "version": "12.9.0"}
                        }
                    }
                },
                "packages": {
                    "better-sqlite3@12.9.0": {
                        "resolution": {
                            "integrity": "sha512-" + base64.b64encode(bytes(range(64))).decode()
                        }
                    }
                },
            }
        ).encode(),
    }
    for name, raw in frozen.items():
        (checkout / name).write_bytes(raw)

    def git(argv):
        if "ls-tree" in argv:
            return b"100644 blob fixture\tinput\n" if argv[-1] in frozen else b""
        return frozen[argv[-1].split(":", 1)[1]]

    monkeypatch.setattr(
        "devflow_temporal.delivery_native_dependencies.subprocess.check_output", git
    )
    spec = {"source_path": "/readonly/source", "base_sha": "frozen"}
    authority = native_addon_authority(spec, checkout, ["apps/api"])
    root = checkout / "node_modules/.pnpm/better-sqlite3@12.9.0/node_modules/better-sqlite3"
    root.mkdir(parents=True)
    (api / "node_modules").mkdir()
    (api / "node_modules/better-sqlite3").symlink_to(root, target_is_directory=True)
    files = {
        "package.json": json.dumps(
            {
                "name": "better-sqlite3",
                "version": "12.9.0",
                "scripts": {"install": "prebuild-install || node-gyp rebuild --release"},
            }
        ).encode(),
        "lib.js": b"authentic registry source",
    }
    store = tmp_path / "store"
    index = (
        store / "v10/index/00" / (bytes(range(64)).hex()[:64][2:] + "-better-sqlite3@12.9.0.json")
    )
    index.parent.mkdir(parents=True)
    records = {}
    for name, content in files.items():
        (root / name).write_bytes(content)
        records[name] = {
            "integrity": "sha512-" + base64.b64encode(hashlib.sha512(content).digest()).decode()
        }
    index.write_text(json.dumps({"name": "better-sqlite3", "version": "12.9.0", "files": records}))
    (checkout / "node_modules/.modules.yaml").write_text(
        yaml.safe_dump({"storeDir": str(store / "v10"), "virtualStoreDir": ".pnpm"})
    )
    return spec, checkout, authority, root, store, index


def test_native_addon_is_one_allowed_registry_target_and_all_source_integrities_match(addon):
    spec, checkout, authority, root, store, _ = addon
    assert authority["target"] == "better-sqlite3@12.9.0"
    assert set(authority["metadata"]) == {"package.json", "apps/api/package.json", "pnpm-lock.yaml"}
    assert validate_native_addon(checkout, authority, store) == root
    assert native_addon_authority(spec, checkout, ["apps/api"]) == authority


@pytest.mark.parametrize("metadata", ["package.json", "apps/api/package.json", "pnpm-lock.yaml"])
def test_candidate_manifest_and_lock_drift_refuse_before_build(addon, metadata):
    spec, checkout, *_ = addon
    path = checkout / metadata
    path.write_bytes(path.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="frozen Git base|lock differs"):
        native_addon_authority(spec, checkout, ["apps/api"])


@pytest.mark.parametrize("config", [".npmrc", ".pnpmfile.cjs", "pnpmfile.cjs"])
def test_candidate_package_manager_hooks_are_not_native_authority(addon, config):
    spec, checkout, *_ = addon
    (checkout / config).write_text("candidate hooks")
    with pytest.raises(ValueError, match="setup is not"):
        native_addon_authority(spec, checkout, ["apps/api"])


@pytest.mark.parametrize(
    "drift", ["source", "extra", "alias", "index", "index-alias", "store", "virtual-store"]
)
def test_registry_source_target_and_generated_roots_cannot_be_substituted(addon, drift):
    _, checkout, authority, root, store, index = addon
    if drift == "source":
        (root / "lib.js").write_text("changed")
    elif drift == "extra":
        (root / "extra.js").write_text("unverified")
    elif drift == "alias":
        old = root.with_name("original")
        root.rename(old)
        root.symlink_to(old, target_is_directory=True)
    elif drift == "index":
        index.write_text(index.read_text().replace("12.9.0", "13.0.0"))
    elif drift == "index-alias":
        old = index.with_suffix(".retained")
        index.rename(old)
        index.symlink_to(old)
    else:
        modules = checkout / "node_modules/.modules.yaml"
        data = yaml.safe_load(modules.read_text())
        data["storeDir" if drift == "store" else "virtualStoreDir"] = "/foreign"
        modules.write_text(yaml.safe_dump(data))
    with pytest.raises(ValueError):
        validate_native_addon(checkout, authority, store)


@pytest.mark.parametrize("entry", ["implementation", "gate"])
def test_both_preparation_entries_pass_the_fresh_checkout_native_target(
    addon, monkeypatch, tmp_path, entry
):
    from devflow_temporal.delivery_broker import DeliveryBroker

    _, checkout, _, _, _, _ = addon
    state = tmp_path / "run-fixture"
    spec = {
        "run_id": state.name,
        "state_dir": str(state),
        "provider": "codex",
        "policy": {
            "host_sandbox": "trusted-local",
            "checks": [],
            "prepublish_checks": [
                {
                    "id": "original-install",
                    "argv": [
                        "corepack",
                        "pnpm",
                        "install",
                        "--offline",
                        "--frozen-lockfile",
                        "--ignore-scripts",
                        "--store-dir",
                        "/store",
                    ],
                }
            ],
        },
    }
    planned = [{"id": "planned-vitest-fixture", "cwd": "apps/api", "argv": ["vitest"]}]
    def phase_planner(*_args, preparation=False):
        assert preparation is (entry == "implementation")
        return planned

    monkeypatch.setattr("devflow_temporal.delivery_plan_checks.planned_checks", phase_planner)
    broker = object.__new__(DeliveryBroker)
    broker.spec, broker.checkout, broker.state_dir, broker.evidence_dir = (
        spec,
        checkout,
        state,
        state,
    )
    candidate = {"id": "candidate"}
    broker.candidate = lambda: candidate
    broker.gate_checkout = lambda *a: checkout
    calls = []

    def run(path, checks, folder, current, **kwargs):
        calls.append((path, checks, kwargs))
        return {"state": "passed", "results": [], "candidate_id": current["id"]}

    broker._run_check_list = run
    if entry == "implementation":
        broker.run_implementation_preparation(1, candidate)
        assert calls[0][1] == spec["policy"]["prepublish_checks"]
    else:
        broker.run_checks(1, candidate)
        assert calls[0][1] == planned
    assert calls[0][0] == checkout
    assert calls[0][2]["native_projects"] == ["apps/api"]


def test_node_handoff_preserves_explicit_environment_and_role_shell_guidance(tmp_path):
    from devflow_temporal.delivery_role_evidence import _copy_receipts

    tools = {
        "node_toolchain": {
            "node_interpreter": {
                "absolute_path": "/frozen/bin/node",
                "realpath": "/frozen/bin/node",
                "sha256": "a" * 64,
                "version": "v22.21.1",
                "modules_ABI": "127",
            },
            "corepack": {"absolute_path": "/frozen/bin/corepack", "sha256": "b" * 64},
            "environment": {
                "PATH": "/frozen/bin:/usr/bin",
                "COREPACK_HOME": "/frozen/cache",
                "npm_config_nodedir": "/frozen",
                "npm_config_build_from_source": "true",
            },
        }
    }
    assert _copy_receipts(tools, tmp_path, tmp_path) == tools


@pytest.mark.parametrize("install_exit", [0, 1])
def test_target_prerequisite_runs_after_successful_original_install_before_tests(
    addon, monkeypatch, tmp_path, install_exit
):
    from devflow_temporal import delivery_native_process, delivery_preparation, delivery_sandbox
    from devflow_temporal.delivery_broker import DeliveryBroker

    spec, checkout, _, _, store, _ = addon
    broker = object.__new__(DeliveryBroker)
    broker.spec = {**spec, "provider": "codex", "policy": {}}
    broker.state_dir = tmp_path / "run-fixture"
    broker._ensure_native_dependency_store = lambda _: {"store": str(store)}
    broker._register_generated = lambda *a: []
    broker._record_generated = lambda *a: None
    broker._native_cancelled = lambda: False
    candidate = {"id": "candidate"}
    monkeypatch.setattr("devflow_temporal.delivery_broker.candidate_for", lambda _: candidate)
    monkeypatch.setattr(delivery_preparation, "require_native_execution", lambda _: None)
    monkeypatch.setattr(delivery_preparation, "verify_prepared_spec", lambda _: None)
    monkeypatch.setattr(delivery_sandbox, "prepare_native_check", lambda *a, **k: ("profile", {}))
    monkeypatch.setattr(delivery_sandbox, "native_check_argv", lambda *a: a[-1])
    sequence = []

    class Process:
        def __init__(self, spec, folder, **options):
            self.folder, self.command = folder, options["argv"]

        def run(self):
            sequence.append("install" if "install" in self.command else "test")
            path = self.folder.parent / "controlled.log"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("controlled process output")
            return {
                "cleanup": "observed-native-confirmed",
                "log": str(path),
                "exit_code": install_exit if "install" in self.command else 0,
            }

    monkeypatch.setattr(delivery_native_process, "NativeProcess", Process)

    def prerequisite(*args):
        sequence.append("target-build-and-load")
        return {"state": "passed", "results": []}

    monkeypatch.setattr(
        "devflow_temporal.delivery_native_dependencies.native_addon_authority",
        lambda *a: {"target": "better-sqlite3@12.9.0"},
    )
    broker._prepare_native_addon = prerequisite
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
                "--store-dir",
                "/store",
            ],
        },
        {"id": "test", "argv": ["node", "test"]},
    ]
    result = broker._run_check_list(
        checkout, checks, tmp_path / "evidence", candidate, native_projects=[]
    )
    assert sequence == (
        ["install", "target-build-and-load", "test"] if install_exit == 0 else ["install"]
    )
    assert result["state"] == ("passed" if install_exit == 0 else "failed")


def test_target_only_build_still_requires_frozen_allowlist(addon, monkeypatch):
    from devflow_temporal import delivery_native_dependencies as dependencies

    spec, checkout, *_ = addon
    original_git = dependencies.subprocess.check_output
    raw = json.dumps(
        {"packageManager": "pnpm@10.24.0", "pnpm": {"onlyBuiltDependencies": ["esbuild"]}}
    ).encode()
    (checkout / "package.json").write_bytes(raw)

    def git(argv):
        if "show" in argv and argv[-1].endswith(":package.json"):
            return raw
        return original_git(argv)

    monkeypatch.setattr(dependencies.subprocess, "check_output", git)
    with pytest.raises(ValueError, match="does not permit"):
        native_addon_authority(spec, checkout, ["apps/api"])


@pytest.mark.parametrize(
    "drift", [None, "source", "binary", "tools", "environment", "log", "process",
              "process-exception"]
)
def test_native_receipt_replay_readback_refuses_drift_without_launch(
    addon, tmp_path, monkeypatch, drift
):
    from devflow_temporal import delivery_native_process, delivery_resources
    from devflow_temporal.delivery_broker import DeliveryBroker

    spec, checkout, authority, package, store, _ = addon
    state = tmp_path / "run-fixture"
    tools_root = tmp_path / "toolchain"
    (tools_root / "bin").mkdir(parents=True)
    for name in ("node", "corepack"):
        (tools_root / "bin" / name).write_text("frozen " + name)
    spec.update(
        run_id=state.name,
        state_dir=str(state),
        policy={
            "toolchain_roots": [str(tools_root)],
            "package_manager_cache": str(tmp_path / "cache"),
        },
    )
    resources = delivery_resources.RunResources(spec)
    monkeypatch.setattr(
        "devflow_temporal.delivery_native_dependencies.frozen_native_builder",
        lambda spec, manager: {"absolute_path": "/controlled/node-gyp.js", "sha256": "fixture"},
    )
    delivery_resources.write_private(
        resources.manifest,
        {
            "roots": {
                str(checkout / "node_modules"): {"kind": "generated", "identity": {"fixture": True}}
            }
        },
    )
    monkeypatch.setattr(delivery_resources.RunResources, "register", lambda *a: None)
    def reconcile(_):
        if drift == "process-exception":
            raise RuntimeError("owned fixture readback unavailable")
        return {"cleanup": "unknown" if drift == "process" else "observed-native-confirmed"}

    monkeypatch.setattr(delivery_native_process, "reconcile_process", reconcile)
    broker = object.__new__(DeliveryBroker)
    broker.spec, broker.state_dir = spec, state
    broker.native_cleanup_confirmed = True
    candidate = {"id": "candidate"}
    evidence = state / "checks"
    evidence.mkdir()
    (package / "build/Release").mkdir(parents=True)
    binary = package / "build/Release/better_sqlite3.node"
    binary.write_bytes(b"controlled binary fixture")
    log = evidence / "identity.log"
    log.write_text(
        json.dumps(
            {
                "version": "v22.21.1",
                "modules_ABI": "127",
                "native_binding_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            }
        )
    )
    builds = []

    def checks(*args):
        builds.append(args)
        return {
            "state": "passed",
            "candidate_id": "candidate",
            "results": [
                {
                    "log": str(log),
                    "log_sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
                    "native_process": {"journal": str(evidence / "controlled-journal.json")},
                }
            ],
        }

    broker._run_check_list = checks
    first = broker._prepare_native_addon(
        checkout, ["apps/api"], evidence, candidate, {"store": str(store)}
    )
    original_receipt = (evidence / "native-addon-preparation.json").read_bytes()
    if drift == "source":
        candidate["id"] = "other-candidate"
    elif drift == "binary":
        binary.write_bytes(b"changed")
    elif drift == "tools":
        (tools_root / "bin/node").write_text("changed")
    elif drift == "environment":
        spec["policy"]["package_manager_cache"] = "/other/cache"
    elif drift == "log":
        log.write_text("changed")
    if drift is None:
        assert (
            broker._prepare_native_addon(
                checkout, ["apps/api"], evidence, candidate, {"store": str(store)}
            )
            == first
        )
    else:
        with pytest.raises((ValueError, RuntimeError)):
            broker._prepare_native_addon(
                checkout, ["apps/api"], evidence, candidate, {"store": str(store)}
            )
    assert len(builds) == 1
    assert (evidence / "native-addon-preparation.json").read_bytes() == original_receipt
    assert first["state"] == "passed"  # Controlled unit fixture, not an actual native load.
    if drift in {"process", "process-exception"}:
        assert broker.native_cleanup_confirmed is False
    elif drift is None:
        assert broker.native_cleanup_confirmed is True


def test_candidate_setup_refuses_before_original_install_or_registry_fetch(
    addon, monkeypatch, tmp_path
):
    from devflow_temporal import delivery_preparation
    from devflow_temporal.delivery_broker import CheckPreparationFailure, DeliveryBroker

    spec, checkout, *_ = addon
    spec.update(provider="codex", policy={})
    (checkout / ".pnpmfile.cjs").write_text("candidate lifecycle setup")
    broker = object.__new__(DeliveryBroker)
    broker.spec = spec

    def forbidden(*args):
        pytest.fail("native authority refusal must precede fetch/process effects")

    broker._ensure_native_dependency_store = forbidden
    monkeypatch.setattr(delivery_preparation, "require_native_execution", lambda _: None)
    with pytest.raises(CheckPreparationFailure) as error:
        broker._run_check_list(
            checkout,
            [{"id": "install", "argv": ["/store"]}],
            tmp_path / "evidence",
            {"id": "candidate"},
            native_projects=["apps/api"],
        )
    assert error.value.results == [
        {
            "id": "native-addon-authority",
            "passed": False,
            "cleanup": "confirmed",
            "launched": False,
            "failure_kind": "preparation",
            "diagnostic": str(error.value),
        }
    ]


def test_unverified_dependency_alias_and_lifecycle_launcher_refuse_before_build(addon):
    _, checkout, authority, package, store, _ = addon
    foreign = checkout / 'node_modules/.pnpm/unverified@1.0.0/node_modules/unverified'
    foreign.mkdir(parents=True)
    (foreign / 'bin.js').write_text('unverified executable source')
    (package.parent / 'prebuild-install').symlink_to(foreign, target_is_directory=True)
    (package.parent / '.bin').mkdir()
    launcher = package.parent / '.bin/prebuild-install'
    launcher.write_text('#!/bin/sh\necho unverified-execution\n')
    launcher.chmod(0o700)
    with pytest.raises(ValueError, match='dependency alias'):
        validate_native_addon(checkout, authority, store)


def test_unverified_project_native_dependency_alias_refuses_before_build(addon):
    _, checkout, authority, package, store, _ = addon
    link = checkout / "apps/api/node_modules/better-sqlite3"
    link.unlink()
    foreign = package.with_name("unverified")
    foreign.mkdir()
    link.symlink_to(foreign, target_is_directory=True)
    with pytest.raises(ValueError, match="project alias"):
        validate_native_addon(checkout, authority, store)


def test_authentic_nonempty_dependency_closure_matches_package_index(addon):
    _, checkout, authority, package, store, _ = addon
    target = "prebuild-install@7.1.3"
    digest = bytes(range(1, 65))
    integrity = "sha512-" + base64.b64encode(digest).decode()
    authority["registry_packages"][target] = integrity
    authority["dependency_links"] = {authority["target"]: {"prebuild-install": target}, target: {}}
    dependency = checkout / "node_modules/.pnpm" / target / "node_modules/prebuild-install"
    dependency.mkdir(parents=True)
    content = b'{"name":"prebuild-install","version":"7.1.3"}'
    (dependency / "package.json").write_bytes(content)
    index = store / "v10/index" / digest.hex()[:2] / (digest.hex()[2:64] + "-" + target + ".json")
    index.parent.mkdir(parents=True)
    index.write_text(json.dumps({"name": "prebuild-install", "version": "7.1.3", "files": {
        "package.json": {"integrity": "sha512-" + base64.b64encode(
            hashlib.sha512(content).digest()).decode()}}}))
    link = package.parent / "prebuild-install"
    link.symlink_to(dependency, target_is_directory=True)
    assert validate_native_addon(checkout, authority, store) == package
    link.unlink()
    foreign = dependency.with_name("foreign")
    foreign.mkdir()
    link.symlink_to(foreign, target_is_directory=True)
    with pytest.raises(ValueError, match="dependency alias"):
        validate_native_addon(checkout, authority, store)
