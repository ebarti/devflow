"""Authenticate a candidate Playwright closure against the controller-owned PNPM store."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import stat
from pathlib import Path

import yaml

from .contracts import digest
from .delivery_native_dependencies import REGISTRY, frozen_pnpm_inputs
from .delivery_resources import RunResources, read_private, same_object
from .delivery_sandbox import native_check_argv


class PackageProofError(ValueError):
    pass


# PNPM creates these executable launchers after unpacking the registry tarball.
# They are never accepted as Playwright modules or used by the readiness probe.
_GENERATED_SHIMS = {
    "@playwright/test": "node_modules/.bin/playwright",
    "playwright": "node_modules/.bin/playwright-core",
}


def _read(path, maximum):
    if path.is_symlink() or path.resolve(strict=True) != path:
        raise PackageProofError("Playwright proof path is not canonical")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= maximum:
            raise PackageProofError("Playwright proof file is not bounded regular data")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            data = stream.read(maximum + 1)
        after, named = os.fstat(fd), path.lstat()
        fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if (
            len(data) != info.st_size
            or any(getattr(info, k) != getattr(after, k) for k in fields)
            or (info.st_dev, info.st_ino) != (named.st_dev, named.st_ino)
        ):
            raise PackageProofError("Playwright proof file changed while read")
        return data
    finally:
        os.close(fd)


def _fetch_authority(spec, checkout, preparation):
    if not isinstance(preparation, dict) or preparation.get("state") != "passed":
        raise PackageProofError("Playwright needs the successful owned frozen dependency fetch")
    receipt = Path(preparation.get("receipt", ""))
    state = Path(spec["state_dir"])
    if (
        not receipt.is_absolute()
        or not receipt.is_relative_to(state)
        or receipt.name != "receipt.json"
    ):
        raise PackageProofError("dependency fetch receipt escaped its run")
    raw = read_private(receipt, raw=True)
    if len(raw) > 4 * 1024 * 1024 or hashlib.sha256(raw).hexdigest() != preparation.get(
        "receipt_sha256"
    ):
        raise PackageProofError("dependency fetch receipt changed")
    saved = json.loads(raw)
    if (saved != {k: v for k, v in preparation.items() if k not in {"receipt", "receipt_sha256"}}
            or saved.get("state") != "passed"
            or saved.get("candidate_setup_executed") is not False):
        raise PackageProofError("dependency fetch observation differs from its receipt")
    request, process = saved.get("request", {}), saved.get("native_process", {})
    if any(saved.get(key) != value for key, value in request.items()):
        raise PackageProofError("dependency fetch receipt differs from its request")
    journal = Path(process.get("journal", ""))
    if (
        not journal.is_absolute()
        or not journal.is_relative_to(receipt.parent)
        or process.get("exit_code") != 0
        or process.get("cleanup") != "observed-native-confirmed"
        or process.get("monitoring_complete") is not True
        or process.get("stdio_drained") is not True
        or any(process.get(k) is not False for k in ("cancelled", "timed_out", "listener_conflict"))
    ):
        raise PackageProofError("dependency fetch lacks a completed native process")
    native = read_private(journal)
    from .payload import payload_digest

    identity = native.get("runtime_identity") or {}
    if (native.get("phase") != "finished" or native.get("result") != process
            or identity.get("source_root") != str(Path(__file__).resolve().parents[3])
            or identity.get("runtime_payload_sha256") != payload_digest(Path(__file__).parent)):
        raise PackageProofError("dependency fetch native journal or runtime changed")
    provenance = {}
    manager, inputs = frozen_pnpm_inputs(spec, checkout, provenance=provenance)
    if (
        request.get("base_sha") != spec["base_sha"]
        or request.get("policy_digest") != spec["policy_digest"]
        or request.get("package_manager") != manager
        or request.get("input_provenance") != provenance
        or request.get("input_hashes")
        != {name: hashlib.sha256(content).hexdigest() for name, content in inputs.items()}
        or request.get("registry") != REGISTRY
    ):
        raise PackageProofError("dependency fetch no longer describes the frozen graph")
    store = Path(request.get("store", ""))
    transient = state / "transient"
    resources = RunResources(spec, read_only=True)
    with resources.locked(read_only=True) as manifest:
        owner = manifest.get("roots", {}).get(str(transient), {})
        if (
            not store.is_absolute()
            or not store.is_relative_to(transient / "dependencies")
            or store.resolve(strict=True) != store
            or not same_object(spec, transient, owner.get("identity"))
            or owner.get("generation", 0) != request.get("generation")
        ):
            raise PackageProofError("dependency store lost its current resource generation")
        info = store.stat()
        if request.get("store_identity") != {"device": info.st_dev, "inode": info.st_ino}:
            raise PackageProofError("dependency store identity changed")
    staging = store.parent
    expected_argv = native_check_argv(
        spec,
        "devflow-check",
        staging,
        [
            "corepack",
            manager,
            "fetch",
            "--frozen-lockfile",
            "--ignore-scripts",
            "--ignore-pnpmfile",
            "--store-dir",
            str(store),
        ],
    )
    from .delivery_sandbox import prepare_native_check

    profile, environment = prepare_native_check(
        spec, staging, receipt.parent, {"id": "pnpm-fetch", "network_domains": [REGISTRY]},
        resources=RunResources(spec),
    )
    environment.update({"COREPACK_ENABLE_NETWORK": "0",
                        "npm_config_registry": "https://" + REGISTRY + "/"})
    intent = native.get("intent", {})
    launch = read_private(journal.parent / "launch.json")
    if (
        profile != "devflow-check"
        or launch != {"argv": expected_argv, "environment": environment}
        or journal.parent.parent != receipt.parent
        or journal.parent.name not in {"process", "process-retry"}
        or intent.get("run_id") != spec["run_id"]
        or intent.get("policy_digest") != spec["policy_digest"]
        or intent.get("argv") != expected_argv
        or launch.get("argv") != expected_argv
        or intent.get("cwd") != str(staging)
        or intent.get("timeout") != 1800
        or intent.get("ports") != []
        or intent.get("environment_sha256") != digest(environment)
    ):
        raise PackageProofError("dependency fetch launch differs from its frozen recipe")
    log = Path(process.get("log", ""))
    if (log != journal.parent / "process.log" or saved.get("log") != str(log)
        or saved.get("log_sha256") != process.get("log_sha256") or hashlib.sha256(
        _read(log, 16 * 1024 * 1024)
    ).hexdigest() != process.get("log_sha256")):
        raise PackageProofError("dependency fetch log changed")
    document = yaml.safe_load(inputs["pnpm-lock.yaml"])
    return store, document


def _package(checkout, store, lock, name, version):
    key = name + "@" + version
    target = name.replace("/", "+") + "@" + version
    package = checkout / "node_modules/.pnpm" / target / "node_modules" / name
    if package.resolve(strict=True) != package or not package.is_dir():
        raise PackageProofError("Playwright package escaped its candidate")
    package_meta = json.loads(_read(package / "package.json", 1024 * 1024))
    if package_meta.get("name") != name or package_meta.get("version") != version:
        raise PackageProofError("Playwright package metadata differs from its locked version")
    resolution = (lock.get("packages") or {}).get(key, {}).get("resolution", {})
    integrity = resolution.get("integrity", "")
    if not isinstance(integrity, str) or not re.fullmatch(
        r"sha(?:256|384|512)-[A-Za-z0-9+/=]+", integrity
    ):
        raise PackageProofError("Playwright package lacks frozen registry integrity")
    try:
        fingerprint = base64.b64decode(integrity.split("-", 1)[1], validate=True).hex()[:64]
    except ValueError as exc:
        raise PackageProofError("Playwright package integrity is malformed") from exc
    index = store / "v10/index" / fingerprint[:2] / (fingerprint[2:] + "-" + target + ".json")
    index_data = _read(index, 10 * 1024 * 1024)
    parsed = json.loads(index_data)
    records = parsed.get("files")
    if (
        parsed.get("name") != name
        or parsed.get("version") != version
        or not isinstance(records, dict)
        or not 1 <= len(records) <= 10000
    ):
        raise PackageProofError("Playwright registry file inventory is malformed")
    total = 0
    for relative, record in records.items():
        if (
            not isinstance(relative, str)
            or not relative
            or Path(relative).is_absolute()
            or ".." in Path(relative).parts
            or not isinstance(record, dict)
        ):
            raise PackageProofError("Playwright package file escaped its registry inventory")
        data = _read(package / relative, 128 * 1024 * 1024)
        total += len(data)
        if total > 512 * 1024 * 1024:
            raise PackageProofError("Playwright package inventory exceeds its byte bound")
        try:
            algorithm, encoded = record["integrity"].split("-", 1)
            expected = base64.b64decode(encoded, validate=True)
        except (KeyError, ValueError, TypeError) as exc:
            raise PackageProofError("Playwright registry file hash is malformed") from exc
        if (
            algorithm not in {"sha256", "sha384", "sha512"}
            or hashlib.new(algorithm, data).digest() != expected
        ):
            raise PackageProofError("Playwright package file differs from its registry inventory")
    generated_shims = {}
    for path in package.rglob("*"):
        if path.is_symlink():
            raise PackageProofError("Playwright package contains an unverified symlink")
        relative = path.relative_to(package).as_posix()
        if path.is_file() and relative not in records:
            if relative != _GENERATED_SHIMS.get(name):
                raise PackageProofError("Playwright package contains unverified source")
            data = _read(path, 64 * 1024)
            if not path.stat().st_mode & 0o111:
                raise PackageProofError("Playwright generated shim is not executable")
            generated_shims[relative] = hashlib.sha256(data).hexdigest()
    return package, {
        "name": name,
        "version": version,
        "integrity": integrity,
        "index_sha256": hashlib.sha256(index_data).hexdigest(),
        "generated_shims": generated_shims,
    }


def authenticate_playwright(spec, checkout, preparation, entry, modules):
    """Verify the loaded Playwright closure; return no grant or gate verdict."""
    checkout = Path(checkout)
    store, lock = _fetch_authority(spec, checkout, preparation)
    entry = Path(entry)
    if (
        not entry.is_absolute()
        or not entry.is_relative_to(checkout)
        or entry.resolve(strict=True) != entry
    ):
        raise PackageProofError("Playwright entry escaped its candidate")
    metadata = next(
        (
            p / "package.json"
            for p in entry.parents
            if p.is_relative_to(checkout) and (p / "package.json").is_file()
        ),
        None,
    )
    if metadata is None:
        raise PackageProofError("Playwright entry lacks package metadata")
    source = json.loads(_read(metadata, 1024 * 1024))
    if source.get("name") not in {"@playwright/test", "playwright"}:
        raise PackageProofError("Playwright entry is not an admitted package")
    version = source.get("version")
    if not isinstance(version, str) or not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise PackageProofError("Playwright version is not an exact supported release")
    names = (["@playwright/test"] if source["name"] == "@playwright/test" else []) + [
        "playwright",
        "playwright-core",
    ]
    packages = [_package(checkout, store, lock, name, version) for name in names]
    roots = [item[0] for item in packages]
    if (not entry.is_relative_to(roots[0])
            or entry.relative_to(roots[0]).as_posix() in packages[0][1]["generated_shims"]):
        raise PackageProofError("Playwright entry differs from its frozen PNPM package")
    if (
        not isinstance(modules, list)
        or not 1 <= len(modules) <= 4096
        or any(not isinstance(raw, str) or not Path(raw).is_absolute() for raw in modules)
        or len(modules) != len(set(modules))
    ):
        raise PackageProofError("Playwright module closure is malformed or unbounded")
    for raw in modules:
        path = Path(raw)
        if path.resolve(strict=True) != path or not any(
            path.is_relative_to(root)
            and path.relative_to(root).as_posix() not in proof["generated_shims"]
            for root, proof in packages
        ):
            raise PackageProofError("Playwright loaded a module outside its authenticated closure")
    return {
        "packages": [value for _, value in packages],
        "modules_sha256": hashlib.sha256(
            json.dumps(sorted(modules), separators=(",", ":")).encode()
        ).hexdigest(),
    }
