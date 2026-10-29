"""Deterministic registry/authentication guards; actual native execution is separate."""

import base64
import hashlib
import json
import subprocess

import pytest
import yaml
from test_delivery_store import service as service

from devflow_temporal.delivery_broker import _git
from devflow_temporal.delivery_native_dependencies import (
    native_addon_authority,
)


@pytest.fixture
def addon(tmp_path, monkeypatch):
    checkout = tmp_path / "checkout"
    api = checkout / "apps/api"
    api.mkdir(parents=True)
    _git(checkout, "init", "-q")
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

    real_check_output = subprocess.check_output

    def git(argv, *args, **kwargs):
        if "/readonly/source" not in argv:
            return real_check_output(argv, *args, **kwargs)
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


def test_actual_native_owner_manifest_remains_frozen(addon):
    spec, checkout, *_ = addon
    path = checkout / 'apps/api/package.json'
    value = json.loads(path.read_bytes())
    value['scripts'] = {'install': 'unapproved build command'}
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match='frozen Git base'):
        native_addon_authority(spec, checkout, ['apps/api'])


@pytest.mark.parametrize("drift,environment_version", [
    (drift, 3) for drift in (None, "source", "binary", "tools", "environment", "log",
                            "process", "process-exception")
] + [(drift, version) for version in (1, 2) for drift in (None, "environment")])
def test_native_receipt_replay_readback_refuses_drift_without_launch(
    addon, tmp_path, monkeypatch, drift, environment_version
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
    def reconcile(_, *, spec):
        assert spec == broker.spec
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
    marker = evidence / "native-environment.json"
    delivery_resources.write_private(marker, {"version": environment_version})
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
    assert ("/usr/local/go/bin" in first["node_toolchain"]["environment"]["PATH"].split(":")) == (
        environment_version >= 3
    )
    if environment_version == 1:
        marker.unlink()  # Historical addon receipts predate the environment marker.
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
