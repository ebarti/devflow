"""Public stopped-installation location migration, without rewriting executions."""

from __future__ import annotations

import asyncio
import base64
import csv
import fcntl
import hashlib
import io
import json
import os
import shlex
import stat
import subprocess
import sys
import tomllib
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from contextvars import copy_context
from copy import deepcopy
from importlib.metadata import distributions
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_runtime_authority import (
    INSTALLATION_RECEIPT,
)
from .delivery_runtime_authority import (
    load as load_authority,
)
from .delivery_runtime_authority import (
    publish as publish_authority,
)
from .delivery_runtime_location import (
    ANSWER,
    KIND,
    REVERSED,
    SCHEMA,
    _transport,
    bootstrap,
    command_id,
    private_bytes,
    receipt_bytes,
    source_root,
)

BASE_FIELDS = {
    "command_id",
    "owner_before_sha256",
    "owner_after_path",
    "owner_after_sha256",
    "destination",
    "runs",
    "host_home",
    "host_cli",
    "host_cli_sha256",
    "answer",
    "baseline_revision",
}
MEASURED_PATHS = {"codex_bin", "protected_commands", "runtime_payload_sha256"}
ROW_METADATA = {"revision", "updated_at", "tracker_json"}


def _hash(path):
    path = Path(path)
    if path.resolve(strict=True) != path:
        raise ValueError("installed content has a linked path")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_nlink != 1
            or before.st_mode & 0o022
            or before.st_size > 512 * 1024 * 1024
        ):
            raise ValueError("installed content is not a bounded owned protected file")
        sha = hashlib.sha256()
        with os.fdopen(os.dup(descriptor), "rb") as stream:
            while chunk := stream.read(1024 * 1024):
                sha.update(chunk)
        after, named = os.fstat(descriptor), path.lstat()
        keys = (
            "st_dev",
            "st_ino",
            "st_uid",
            "st_mode",
            "st_nlink",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if any(getattr(before, key) != getattr(after, key) for key in keys) or any(
            getattr(after, key) != getattr(named, key) for key in keys
        ):
            raise ValueError("installed content changed during observation")
        return sha.hexdigest()
    finally:
        os.close(descriptor)


def _directory(path):
    path = Path(path)
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("installed directory is linked or not canonical")
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022:
        raise ValueError("installed directory is not owned and protected")
    volume = None
    if sys.platform == "darwin":
        from .delivery_filesystem_custody import _volume

        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            if (os.fstat(descriptor).st_dev, os.fstat(descriptor).st_ino) != (
                info.st_dev,
                info.st_ino,
            ):
                raise ValueError("installed directory changed during volume observation")
            volume = _volume(descriptor)
        finally:
            os.close(descriptor)
    return {
        "device": info.st_dev,
        "inode": info.st_ino,
        "uid": info.st_uid,
        "mode": stat.S_IMODE(info.st_mode),
        "volume_uuid": volume,
        "birthtime": getattr(info, "st_birthtime", None),
    }


PHYSICAL_DESTINATION = "devflow-shared-physical-installation-v1"
RUN_NATIVE_SCOPE = frozenset({"browser_read_roots", "toolchain_roots", "package_manager_cache"})


def shared_destination(measured):
    """Separate shared installed objects from each run's retained native grants."""
    value = deepcopy(measured)
    if value.get("identity_kind") == PHYSICAL_DESTINATION:
        if RUN_NATIVE_SCOPE & value["identity"].keys():
            raise ValueError("shared installation contains a run's native scope")
        return value
    if "identity_kind" in value or not RUN_NATIVE_SCOPE <= value["identity"].keys():
        raise ValueError("installed measurement lacks its complete per-run native scope")
    value["identity"] = {
        key: item for key, item in value["identity"].items() if key not in RUN_NATIVE_SCOPE
    }
    value["identity_kind"] = PHYSICAL_DESTINATION
    return value


def scoped_identity(shared, item):
    """A shared location never supplies, merges or rewrites a run's native grants."""
    if shared.get("identity_kind") != PHYSICAL_DESTINATION:
        return None  # Previously acknowledged full-identity destinations stay strict.
    original = item["original_spec"]
    frozen = original["policy"]["native_identity"]
    policy = original["policy"]
    scope = {
        "toolchain_roots": policy.get("toolchain_roots", []),
        "package_manager_cache": policy.get("package_manager_cache"),
        "browser_read_roots": (policy.get("browser_qa") or {}).get("read_roots", []),
    }
    if scope != {key: frozen[key] for key in RUN_NATIVE_SCOPE}:
        raise ValueError("installation run scope differs from its frozen native authority")
    expected = {**shared_destination(shared)["identity"], **scope}
    actual = item.get("installed_native_identity")
    if not isinstance(actual, dict) or actual != expected:
        raise ValueError("installation lost its full per-run measured native identity")
    validate_measured_location(original, actual, Path(shared["root"]))
    return actual


def same_destination(expected, actual, *, strict_source=True):
    if expected.get("identity_kind") == PHYSICAL_DESTINATION:
        actual = shared_destination(actual)
    elif "identity_kind" in expected or "identity_kind" in actual:
        return False
    before, current = deepcopy(expected), deepcopy(actual)
    for key in ("root_identity", "environment_identity"):
        old, new = before[key], current[key]
        if old["device"] != new["device"]:
            # Unlike the legacy records, this new acknowledgement actually
            # records volume UUID and birth time. Never infer past continuity.
            if (
                not old.get("volume_uuid")
                or old["volume_uuid"] != new.get("volume_uuid")
                or old.get("birthtime") is None
                or old["birthtime"] != new.get("birthtime")
                or {k: v for k, v in old.items() if k != "device"}
                != {k: v for k, v in new.items() if k != "device"}
            ):
                return False
            new["device"] = old["device"]
    for value in (before, current):
        value["identity"].pop("os_version", None)  # Existing trusted native recorded-only rule.
        if not strict_source:
            # Normal future admissions retain the existing clean committed
            # source-upgrade contract; the exact old cohort grant stays pinned.
            for key in ("revision", "tree", "payload_sha256"):
                value.pop(key)
            value["identity"].pop("runtime_payload_sha256")
    return before == current


def destination_current(receipt, *, strict_source=True):
    """Observe shared objects freshly and authenticate every recorded run grant."""
    from .delivery_native_renewal import _old_proof

    if not same_destination(
        receipt["destination"],
        destination(receipt["runs"][0]["original_spec"]),
        strict_source=strict_source,
    ):
        raise ValueError("acknowledged installed runtime destination changed")
    for item in receipt["runs"]:
        scoped_identity(receipt["destination"], item)
        _old_proof(item["original_spec"])


def _protected(root, binary):
    return sorted(
        str(path)
        for path in {
            binary,
            root / "runtime/src/devflow_temporal",
            root / "runtime/.venv/bin/devflow-delivery",
            root / "runtime/.venv/bin/devflow-delivery-mcp",
            Path("/opt/homebrew/bin/codex"),
            Path("/usr/local/bin/codex"),
        }
    )


def validate_measured_location(original, actual, root):
    from .delivery_native_preparation import recorded_only

    before = original["policy"]["native_identity"]
    old_binary = Path(before["codex_bin"])
    old_root = old_binary.parents[7]
    new_binary = root / "runtime/.venv" / old_binary.relative_to(old_root / "runtime/.venv")
    if (
        original["policy"]["codex_bin"] != str(old_binary)
        or actual["codex_bin"] != str(new_binary)
        or before["protected_commands"] != _protected(old_root, old_binary)
        or actual["protected_commands"] != _protected(root, new_binary)
    ):
        raise ValueError("installed relocation changed a protected command or original SDK layout")
    skip = MEASURED_PATHS | recorded_only(original)
    if {k: v for k, v in before.items() if k not in skip} != {
        k: v for k, v in actual.items() if k not in skip
    }:
        raise ValueError(
            "installed relocation changed Python, dependencies, CLI bytes or native policy"
        )


def _records(prefix):
    """Check installed wheel RECORD bytes; do not trust a version string alone."""
    result = {}
    for installed in distributions(path=[str(prefix)]):
        name = installed.metadata["Name"].lower().replace("_", "-")
        if name == "devflow-temporal":
            continue  # The editable controller has its independent complete Git proof.
        raw = installed.read_text("RECORD")
        if raw is None:
            raise ValueError("installed dependency has no wheel RECORD")
        entries = []
        for relative, encoded, size in csv.reader(io.StringIO(raw)):
            if not encoded:
                continue
            algorithm, wanted = encoded.split("=", 1)
            path = Path(os.path.abspath(installed.locate_file(relative)))
            if (
                algorithm != "sha256"
                or path.resolve(strict=True) != path
                or not path.is_relative_to(prefix.parent.parent.parent)
            ):
                raise ValueError("installed dependency has an external or linked RECORD member")
            info = path.lstat()
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_mode & 0o022
                or str(info.st_size) != size
            ):
                raise ValueError("installed dependency RECORD object changed")
            actual = base64.urlsafe_b64encode(bytes.fromhex(_hash(path))).decode().rstrip("=")
            if actual != wanted:
                raise ValueError("installed dependency differs from its wheel RECORD")
            entries.append([relative, wanted, size])
        if name in result:
            raise ValueError("installed dependency metadata is ambiguous")
        result[name] = {"version": installed.version, "files": entries}
    if (
        not {"agent-runtime-kit", "openai-codex", "openai-codex-cli-bin", "temporalio"}
        <= result.keys()
    ):
        raise ValueError("installed runtime dependency closure is incomplete")
    return result


def destination(original):
    """The actual independently imported runtime, not an equal-hash chosen executable."""
    from .delivery_broker import _git
    from .delivery_native_preparation import native_identity
    from .payload import payload_digest

    root = source_root()
    if root != Path.home().resolve(strict=True) / ".local/share/devflow/developer-workflow":
        raise ValueError("permanent installation must use its owning user's fixed durable runtime")
    if any(part in {".codex-workspaces", "worktrees"} for part in root.parts):
        raise ValueError("permanent runtime cannot be inside managed worktree storage")
    identity = _directory(root)
    git_dir = root / ".git"
    if not git_dir.is_dir() or git_dir.is_symlink():
        raise ValueError("permanent runtime requires its own independent Git directory")
    common = Path(_git(root, "rev-parse", "--git-common-dir"))
    common = common if common.is_absolute() else root / common
    if (
        common.resolve(strict=True) != git_dir
        or (git_dir / "objects/info/alternates").exists()
        or _git(root, "status", "--porcelain", "--untracked-files=all")
        or _git(root, "rev-parse", "--show-toplevel") != str(root)
    ):
        raise ValueError("permanent runtime depends on another checkout or uncommitted source")
    environment = root / "runtime/.venv"
    if Path(sys.prefix) != environment or environment.resolve(strict=True) != environment:
        raise ValueError("permanent runtime is not running its own installed environment")
    environment_identity = _directory(environment)
    site = (
        environment / f"lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages"
    )
    from importlib.metadata import distribution

    binary = Path(distribution("openai-codex-cli-bin").locate_file("codex_cli_bin/bin/codex"))
    if binary != site / "codex_cli_bin/bin/codex" or binary.resolve(strict=True) != binary:
        raise ValueError("permanent runtime is not using its own actual bundled SDK CLI")
    pth = list(site.glob("*.pth"))
    if (
        len(pth) != 1
        or pth[0].name != "_editable_impl_devflow_temporal.pth"
        or pth[0].read_text().strip() != str(root / "runtime/src")
    ):
        raise ValueError("permanent runtime has an unacknowledged external import registration")
    direct = json.loads(distribution("devflow-temporal").read_text("direct_url.json"))
    from urllib.parse import unquote, urlsplit

    if direct.get("dir_info") != {"editable": True} or unquote(
        urlsplit(direct.get("url", "")).path
    ) != str(root / "runtime"):
        raise ValueError("permanent editable controller is registered to a different source")
    for path in [
        environment / "pyvenv.cfg",
        *site.glob("*.pth"),
        *site.glob("*/direct_url.json"),
        environment / "bin/devflow-delivery",
        environment / "bin/devflow-delivery-mcp",
    ]:
        text = path.read_text()
        if any(value in text for value in ("/.codex/worktrees/", "/.codex-workspaces/")):
            raise ValueError("installed entry/import/environment depends on managed chat storage")
    for path in (environment / "bin").iterdir():
        if path.is_symlink():
            # The original resolved base interpreter remains explicitly pinned.
            if (
                path.name.startswith("python")
                and str(path.resolve(strict=True))
                == (original["policy"]["native_identity"]["python"])
            ):
                continue
            raise ValueError("permanent executable registration has an external symlink")
        descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
                raise ValueError("permanent executable registration is not an owned file")
            head = os.read(descriptor, 4096)
            if any(value in head for value in (b"/.codex/worktrees/", b"/.codex-workspaces/")):
                raise ValueError("permanent executable shebang depends on disposable chat code")
        finally:
            os.close(descriptor)
    current = deepcopy(original)
    current["policy"]["codex_bin"] = str(binary)
    actual = native_identity(current)
    validate_measured_location(original, actual, root)
    if original["policy"].get("host_sandbox") != "trusted-local":
        raise ValueError("installed relocation requires the original trusted native policy")
    packages = _records(site)
    locked = tomllib.loads((root / "runtime/uv.lock").read_text())["package"]
    for name, item in packages.items():
        if not any(
            package["name"].replace("_", "-") == name and package["version"] == item["version"]
            for package in locked
        ):
            raise ValueError("installed dependency version differs from the frozen lock")
    from .runtime_dependencies import locked_dependency_identity

    if actual["runtime_dependencies"] != locked_dependency_identity():
        raise ValueError("installed relocation changed the frozen dependency lock")
    # The old helper schema and loader remain byte-identical; the new pointer
    # never appoints a caller-authored state implementation.
    helper_prefix = "skills/devflow/scripts/"
    helper_manifest = {}
    for path in (root / helper_prefix).iterdir():
        if path.is_file() and path.suffix in {".py", ".sql"}:
            helper_manifest[path.name] = _hash(path)
    revision = _git(root, "rev-parse", "HEAD")
    tree = _git(root, "rev-parse", "HEAD^{tree}")
    payload = payload_digest(root / "runtime/src/devflow_temporal")
    if (
        actual["runtime_payload_sha256"] != payload
        or _directory(root) != identity
        or _directory(environment) != environment_identity
        or _git(root, "status", "--porcelain", "--untracked-files=all")
        or _git(root, "rev-parse", "HEAD") != revision
    ):
        raise ValueError("installed source or directory generation changed during observation")
    return {
        "root": str(root),
        "root_identity": identity,
        "revision": revision,
        "tree": tree,
        "payload_sha256": payload,
        "identity": actual,
        "environment_identity": environment_identity,
        "dependencies_sha256": digest(packages),
        "dependency_versions": {name: item["version"] for name, item in packages.items()},
        "helpers": helper_manifest,
    }


def _request(config, supplied):
    if (
        not isinstance(supplied, dict)
        or not BASE_FIELDS
        <= set(supplied)
        <= BASE_FIELDS | {"expected_evidence_sha256", "plugin_origin"}
        or supplied["answer"] != ANSWER
        or not isinstance(supplied["runs"], list)
        or not 1 <= len(supplied["runs"]) <= 16
        or len(set(supplied["runs"])) != len(supplied["runs"])
        or any(not isinstance(run, str) for run in supplied["runs"])
    ):
        raise ValueError("invalid explicit permanent installation request")
    import re

    from .delivery_config import COMMAND_RE, DeliveryConfig
    from .delivery_runtime_registrations import origin_request

    if "plugin_origin" in supplied:
        origin_request(supplied["plugin_origin"])

    if not isinstance(supplied["command_id"], str) or not COMMAND_RE.fullmatch(
        supplied["command_id"]
    ):
        raise ValueError("invalid permanent installation command identity")
    for key in (
        "owner_before_sha256",
        "owner_after_sha256",
        "host_cli_sha256",
        "expected_evidence_sha256",
    ):
        if key in supplied and (
            not isinstance(supplied[key], str) or not re.fullmatch(r"[0-9a-f]{64}", supplied[key])
        ):
            raise ValueError("installation requires exact evidence hashes")
    if (
        not isinstance(supplied["destination"], dict)
        or set(supplied["destination"]) != {"root", "revision", "tree", "payload_sha256"}
        or any(not isinstance(value, str) for value in supplied["destination"].values())
        or not re.fullmatch(r"[0-9a-f]{40}", supplied["destination"]["revision"])
        or not re.fullmatch(r"[0-9a-f]{40}", supplied["destination"]["tree"])
        or not re.fullmatch(r"[0-9a-f]{64}", supplied["destination"]["payload_sha256"])
        or not isinstance(supplied["baseline_revision"], str)
        or not re.fullmatch(r"[0-9a-f]{40}", supplied["baseline_revision"])
    ):
        raise ValueError(
            "permanent destination must bind its exact source root/revision/tree/payload"
        )
    after_path = Path(supplied["owner_after_path"])
    if not after_path.is_relative_to(config.state_root / "operator-inputs"):
        raise ValueError("permanent owner proposal must remain in owned operator inputs")
    before = private_bytes(config.path)
    after = private_bytes(Path(supplied["owner_after_path"]))
    if hashlib.sha256(before).hexdigest() not in {
        supplied["owner_before_sha256"],
        supplied["owner_after_sha256"],
    }:
        raise ValueError("original installation owner preimage changed")
    if hashlib.sha256(after).hexdigest() != supplied["owner_after_sha256"]:
        raise ValueError("proposed permanent owner bytes changed")
    new = DeliveryConfig._from_raw(config.path, json.loads(after))
    return before, after, new


def bootstrap_inputs(config, supplied):
    """Authenticate recovery inputs before any ordinary Store can load missing helpers."""
    from .delivery_broker import _git
    from .delivery_runtime_location import pending_acknowledged
    from .payload import payload_digest

    current, after, proposed = _request(config, supplied)
    root = source_root()
    if supplied["destination"] != {
        "root": str(root),
        "revision": _git(root, "rev-parse", "HEAD"),
        "tree": _git(root, "rev-parse", "HEAD^{tree}"),
        "payload_sha256": payload_digest(root / "runtime/src/devflow_temporal"),
    }:
        raise ValueError("installation command is not executing its exact proposed source")
    if _git(root, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("installation requires clean reviewed source before bootstrap")
    original = current
    folder = config.state_root / "runtime-installations" / supplied["command_id"]
    intent_path = folder / "receipt.json"
    if hashlib.sha256(current).hexdigest() != supplied["owner_before_sha256"]:
        if not intent_path.exists():
            raise ValueError("owner changed before an authenticated installation intent")
        raw = receipt_bytes(intent_path)
        intent = load_authority(
            intent_path, INSTALLATION_RECEIPT, expected_sha256=hashlib.sha256(raw).hexdigest()
        )
        reference = {
            "command_id": supplied["command_id"],
            "receipt": str(intent_path),
            "receipt_sha256": hashlib.sha256(raw).hexdigest(),
        }
        if intent["command"] != supplied or not pending_acknowledged(config, intent, reference):
            raise ValueError("partial owner write has no original pending cohort acknowledgement")
        original = private_bytes(folder / "owner-before.json")
    if hashlib.sha256(original).hexdigest() != supplied["owner_before_sha256"]:
        raise ValueError("original owner byte archive changed")
    before_raw, after_raw = json.loads(original), json.loads(after)
    expected = deepcopy(before_raw)
    expected["codex_bin"] = str(
        Path(sys.prefix)
        / (
            f"lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages/codex_cli_bin/bin/codex"
        )
    )
    if after_raw != expected or before_raw.get("capacity", 2) != 2:
        raise ValueError(
            "permanent owner proposal may change only its acknowledged bundled CLI path"
        )
    for name in ("state.py", "schema.sql"):
        from .delivery_feature_review_context import _git as git_bytes

        old = git_bytes(
            root, "show", supplied["baseline_revision"] + ":skills/devflow/scripts/" + name
        )
        actual = (root / "skills/devflow/scripts" / name).read_bytes()
        if old != actual:
            raise ValueError(
                "permanent installation changed the original shared state helper/schema"
            )
    return {
        "helpers_dir": before_raw["helpers_dir"],
        "before": original,
        "after": after,
        "proposed": proposed,
        "folder": folder,
    }


def _historical_specs(store, spec):
    from .delivery_resources import _gate_roots

    values = {}
    _gate_roots(spec, execution_specs=values)
    values[digest(spec)] = spec
    with store._connect() as db:
        original = json.loads(
            db.execute(
                "SELECT request_json FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
            ).fetchone()[0]
        )
    # Unprepared initial requests carry no measurement, so are never granted a
    # historical native proof by this receipt.
    if original.get("preparation"):
        values[digest(original)] = original
    return list({digest(value): value for value in values.values()}.values())


def _native_observation(spec, leases):
    from .candidate import candidate_for
    from .delivery_filesystem_custody import _opened
    from .delivery_native_process import interrupted_cleanup_proven, listeners, process_table
    from .delivery_resources import RunResources

    registry = RunResources(spec, read_only=True)
    manifest = leases.enter_context(registry.locked(read_only=True))
    manifest_raw = private_bytes(registry.manifest)
    if json.loads(manifest_raw) != manifest:
        raise ValueError("installation resource manifest changed during locked observation")
    hashes, actors, ports = {}, [], set()
    for raw in sorted(manifest["processes"]):
        path = Path(raw)
        if not path.is_relative_to(Path(spec["state_dir"])) or path.name != "native-process.json":
            raise ValueError("installation native journal left its original run")
        for name in ("native-process.lock", "native-monitor.lock"):
            leases.enter_context(_opened(path.with_name(name), lock=True))
        journal_raw = private_bytes(path)
        journal = json.loads(journal_raw)
        result = journal.get("result") or {}
        if (
            journal.get("phase") != "finished"
            or result.get("cleanup") != "observed-native-confirmed"
            or (
                journal.get("monitoring_complete") is not True
                and not interrupted_cleanup_proven(journal)
            )
        ):
            raise ValueError("installation has an unresolved native actor or teardown")
        actors.extend(
            (int(pid), item["identity"]) for pid, item in journal.get("owned", {}).items()
        )
        if journal.get("monitor"):
            actors.append((int(journal["monitor"]["pid"]), journal["monitor"]["identity"]))
        ports.update(journal.get("ports", []))
        hashes[raw] = hashlib.sha256(journal_raw).hexdigest()
        for name in (
            "request.json",
            "result.json",
            "role-result.json",
            "assessment.json",
            "outcome.json",
            "start.json",
            "native-observation.json",
            "native-thread-observation.json",
            "launch.json",
        ):
            sibling = path.with_name(name)
            if os.path.lexists(sibling):
                hashes[str(sibling)] = hashlib.sha256(private_bytes(sibling)).hexdigest()
    table = process_table()
    if any(
        table.get(pid, {}).get("identity") == identity and not table[pid]["stat"].startswith("Z")
        for pid, identity in actors
    ) or any(listeners(port) for port in ports):
        raise ValueError("installation native process identity or port is still live")
    checkout = Path(spec["checkout"])
    return {
        "journals": hashes,
        "actors": actors,
        "ports": sorted(ports),
        "manifest": str(registry.manifest),
        "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
        "candidate": candidate_for(checkout) if checkout.exists() else None,
    }


def _closure(store, row):
    from .delivery_closures import closure_facts, sealed_closure, sealed_record

    workflow = row["workflow_id"] or "delivery-" + row["run_id"]

    async def observe():
        value = await sealed_closure(store, workflow)
        return (value, await closure_facts(value["history"])) if value else (None, None)

    with ThreadPoolExecutor(max_workers=1) as pool:
        # This fixed historical reader must retain the already authenticated
        # public installation owner's scope during PENDING rollback. It grants
        # no ordinary Store or native-launch bypass outside that operation.
        closed, facts = pool.submit(copy_context().run, lambda: asyncio.run(observe())).result(
            timeout=30
        )
    if not closed or closed["status"] != "COMPLETED":
        raise ValueError(
            "installation requires authentic sealed closed history; import it publicly first"
        )
    spec = store.effective_spec(row["run_id"])
    recovery = json.loads(row["recovery_json"]) if row["recovery_json"] else None
    starts = (
        [spec]
        if recovery
        else [store.submitted_spec(row["run_id"]), store.spec(row["run_id"]), spec]
    )
    if (
        closed["request_digest"] != row["request_digest"]
        or closed["recovery_digest"] != (digest(recovery) if recovery else None)
        or len(facts["inputs"]) != 2
        or facts["inputs"][0] not in starts
        or facts["inputs"][1] != recovery
        or closed["result"].get("run_id") != row["run_id"]
    ):
        raise ValueError("installation closed history belongs to another native execution")
    return (
        {key: value for key, value in closed.items() if key != "history"},
        sealed_record(store, workflow, execution_run_id=closed["execution_run_id"]),
    )


def _stopped(config):
    from .delivery_control import _owned, _read_manifest, _recorded_deployment

    # Re-observe on replay too: a prevalidated legacy FD may have acted after
    # an unfinished pre-zero attempt released its exclusive lifecycle lease.
    json.loads(private_bytes(config.state_root / "service-stopped.json"))
    lifecycle = _read_manifest(config) or {"processes": {}}
    if _recorded_deployment(lifecycle) is not None:
        raise ValueError("retained worker deployment requires its existing deployment owner")
    if any(_owned(actor) for actor in lifecycle.get("processes", {}).values()):
        raise ValueError("stop and drain the original delivery actors before installation")


def collect(store, supplied, leases):
    from .delivery_native_renewal import _old_proof

    # Existing execution/closure readers can repair benign directory modes.
    # This public inspection refuses that drift before entering those readers.
    for folder in (
        store.config.state_root,
        store.config.state_root / "closures",
        store.config.state_root / "configuration-snapshots",
    ):
        if _directory(folder)["mode"] != 0o700:
            raise ValueError("installation evidence directory is not already private")
    for path in (store.config.state_root / "configuration-snapshots").glob("*.json"):
        info = path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
        ):
            raise ValueError("installation admitted snapshot is not already a private owned file")
    _stopped(store.config)
    entries, observations, installed = [], {}, None
    for run_id in sorted(supplied["runs"]):
        spec = store.effective_spec(run_id)
        if spec.get("feature_worker") or spec.get("feature_delivery"):
            raise ValueError("this installation cohort requires ordinary original deliveries")
        from .delivery_amendment_capacity import submitted_owner

        _original, owner = submitted_owner(spec)
        if owner.path != store.config.path or not store.owns_execution(spec):
            raise ValueError("installation cohort belongs to another original operational owner")
        histories = _historical_specs(store, spec)
        for previous in histories:
            _old_proof(previous)
        measured = destination(spec)
        physical = shared_destination(measured)
        if installed is None:
            installed = physical
        elif physical != installed:
            raise ValueError("installation cohort has different native runtime authority")
        with store._connect() as db:
            row = dict(
                db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            )
            attempts = [
                dict(r)
                for r in db.execute(
                    "SELECT * FROM delivery_attempts WHERE run_id=? ORDER BY job_key", (run_id,)
                )
            ]
            effects = [
                dict(r)
                for r in db.execute(
                    "SELECT * FROM delivery_effects WHERE run_id=? ORDER BY effect_key", (run_id,)
                )
            ]
            mutations = [
                dict(r)
                for r in db.execute(
                    "SELECT * FROM delivery_mutations WHERE run_id=? ORDER BY command_id", (run_id,)
                )
            ]
            claim = store.state.claim_for(db, spec["work_id"])
            from .delivery_policy_recovery import work_binding

            work = work_binding(store, spec, db)
        if any(attempt["state"] != "finished" for attempt in attempts):
            raise ValueError("installation cohort still has an unfinished native attempt")
        if claim and claim["owner"] != "external:devflow:" + run_id:
            raise ValueError("installation cohort lost its own claim")
        closed, closure_record = _closure(store, row)
        native = _native_observation(spec, leases)
        for raw in (spec["source_path"], spec["checkout"], spec["state_dir"]):
            path, runtime = Path(raw), Path(installed["root"])
            if path == runtime or path in runtime.parents or runtime in path.parents:
                raise ValueError("installed runtime overlaps disposable work or controller data")
        entries.append(
            {
                "run_id": run_id,
                "request_digest": row["request_digest"],
                "request_json_sha256": hashlib.sha256(row["request_json"].encode()).hexdigest(),
                "original_spec": spec,
                "installed_native_identity": measured["identity"],
                "historical_spec_digests": sorted(digest(v) for v in histories),
                "historical_specs": histories,
                "native_identities": sorted(
                    digest(v["policy"]["native_identity"]) for v in histories
                ),
                "row_authority_sha256": digest(
                    {k: v for k, v in row.items() if k not in ROW_METADATA}
                ),
                "attempts_sha256": digest(attempts),
                "effects_sha256": digest(effects),
                "mutations_sha256": digest(mutations),
                "claim": (
                    {key: claim[key] for key in ("resource", "work_id", "owner", "claimed_at")}
                    if claim
                    else None
                ),
                "work_binding": work,
                "closed": closed,
                "closure_record": closure_record,
                "native": native,
            }
        )
        observations[run_id] = {**{key: row[key] for key in ROW_METADATA}, "claim_metadata": claim}
    if supplied["destination"] != {key: installed[key] for key in supplied["destination"]}:
        raise ValueError("explicit installed destination changed")
    return {"runs": entries, "destination": installed}, observations


def _authority(config, supplied, inputs, leases, *, service_descriptor, read_only=True):
    from .delivery_runtime_registrations import observe
    from .delivery_runtime_registrations import registration as pin_registration
    from .delivery_store import DeliveryStore

    with bootstrap(config, inputs):
        store = DeliveryStore(config, read_only=read_only)
        authority, observations = collect(store, supplied, leases)
        registrations = observe(
            config, supplied, authority["runs"][0]["original_spec"], authority["destination"]
        )
        registrations["objects"] = [
            pin_registration(item)
            for item in [*registrations["objects"], _owner_item(config, inputs)]
        ]
    authority.update(
        owner_path=str(config.path),
        owner_before_sha256=supplied["owner_before_sha256"],
        owner_after_sha256=supplied["owner_after_sha256"],
        helpers_dir=inputs["helpers_dir"],
        transport=_transport(config),
        registrations=registrations,
    )
    from .delivery_runtime_service_guard import describe

    authority["service_guard"] = describe(config, supplied["command_id"], service_descriptor)
    with store._connect() as db:
        authority["native_inputs"] = _native_inputs(store, db)
    authority["global_native"] = _global_native(config, authority, leases)
    return store, authority, observations


def _process_views():
    """Native monitors use host-default time; controllers may inherit UTC."""
    values = {}
    for zone in (None, "UTC"):
        environment = dict(os.environ)
        environment.pop("TZ", None)
        if zone:
            environment["TZ"] = zone
        output = subprocess.run(
            ["ps", "-A", "-o", "pid=,stat=,lstart="],
            env=environment,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
        for line in output.splitlines():
            row = line.strip().split(maxsplit=2)
            if len(row) == 3 and row[0].isdigit() and not row[1].startswith("Z"):
                values.setdefault(int(row[0]), set()).add(row[2])
    return values


def _forwarding_adapter(argv, root, owner, interpreters):
    """Only the actual entrypoint position identifies a known legacy adapter."""
    mcp = str(root / "runtime/.venv/bin/devflow-delivery-mcp")
    tail = ["--config", owner]
    if argv == [mcp, *tail]:
        return True
    if not argv or argv[0] not in interpreters:
        return False
    index = 1
    while index < len(argv) and argv[index] in {"-I", "-u", "-B"}:
        index += 1
    return argv[index:] in ([mcp, *tail], ["-m", "devflow_temporal.delivery_mcp", *tail])


def _original_programs_absent(receipt):
    roots = {
        Path(item["original_spec"]["policy"]["codex_bin"]).parents[7] for item in receipt["runs"]
    }
    interpreters = {}
    for root in roots:
        bases = {
            item["original_spec"]["policy"].get("native_identity", {}).get("python")
            for item in receipt["runs"]
            if Path(item["original_spec"]["policy"]["codex_bin"]).parents[7] == root
        } - {None}
        names = {"python", "python3", *(Path(base).name for base in bases)}
        interpreters[root] = bases | {str(root / "runtime/.venv/bin" / name) for name in names}
    output = subprocess.run(
        ["ps", "-A", "-o", "pid=,command="], check=True, capture_output=True, text=True, timeout=5
    ).stdout
    observed = set()
    for line in output.splitlines():
        fields = line.strip().split(maxsplit=1)
        if len(fields) != 2 or not fields[0].isdigit():
            continue
        try:
            argv = shlex.split(fields[1])
        except ValueError:
            if any(str(root) in fields[1] for root in roots):
                raise ValueError(
                    "original runtime process command cannot be authenticated"
                ) from None
            continue
        for root in roots:
            first = Path(argv[0]) if argv else None
            scripts = {
                str(root / "runtime/.venv/bin" / name)
                for name in ("devflow-delivery", "devflow-delivery-mcp", "devflow-temporal")
            }
            if _forwarding_adapter(argv, root, receipt["owner_path"], interpreters[root]):
                continue  # Original EX lease and exact two-name guard fence this adapter.
            if (first and first.is_relative_to(root / "runtime/.venv")) or (
                first
                and first.name.startswith("python")
                and any(arg in scripts for arg in argv[1:3])
            ):
                observed.add(int(fields[0]))
    after = _process_views()
    if any(pid in after for pid in observed):
        raise ValueError("original runtime controller or MCP process is still live; drain it first")


def _global_native(config, receipt, leases):
    """Current actor observation only; unnamed historical custody is not adopted."""
    from .delivery_filesystem_custody import _opened

    known = {
        path
        for item in receipt["runs"]
        for path in item["native"]["journals"]
        if Path(path).name == "native-process.json"
    }
    journals, actors, ports = {}, set(), set()
    paths = sorted(config.state_root.rglob("native-process.json"))
    if len(paths) > 4096:
        raise ValueError("installation exceeds its bounded registered native observation")
    for path in paths:
        raw = private_bytes(path)
        journal = json.loads(raw)
        if journal.get("phase") != "finished":
            raise ValueError("installation has an unfinished historical native monitor")
        if str(path) not in known:
            for name in ("native-process.lock", "native-monitor.lock"):
                lock = path.with_name(name)
                if lock.exists():
                    leases.enter_context(_opened(lock, lock=True))
        journals[str(path)] = hashlib.sha256(raw).hexdigest()
        actors.update(
            (int(pid), item["identity"]) for pid, item in journal.get("owned", {}).items()
        )
        if journal.get("monitor"):
            actors.add((int(journal["monitor"]["pid"]), journal["monitor"]["identity"]))
        ports.update(journal.get("ports", []))
    observation = {
        "journals": journals,
        "actors": sorted(actors),
        "ports": sorted(ports),
        "process_time_views": ["host-default", "UTC"],
    }
    _global_current({**receipt, "global_native": observation})
    return observation


def _global_current(receipt):
    from .delivery_native_process import listeners

    value = receipt["global_native"]
    for raw, expected in value["journals"].items():
        if hashlib.sha256(private_bytes(Path(raw))).hexdigest() != expected:
            raise ValueError("installation historical native actor evidence changed")
    table = _process_views()
    if any(identity in table.get(pid, set()) for pid, identity in value["actors"]) or any(
        listeners(port) for port in value["ports"]
    ):
        raise ValueError("installation has live historical native actors or ports")
    _original_programs_absent(receipt)


def _native_inputs(store, db):
    """Observe inputs, not tracker/status refreshes, across this shared stack."""
    values = {}
    for row in db.execute(
        "SELECT run_id,request_digest,request_json,recovery_json,workflow_id FROM delivery_runs"
    ):
        original = json.loads(row["request_json"])
        if original.get("provider") != "codex" or (
            Path(original["state_dir"]).parent.parent != store.config.state_root
        ):
            continue
        values[row["run_id"]] = {
            "request_digest": row["request_digest"],
            "request_json_sha256": hashlib.sha256(row["request_json"].encode()).hexdigest(),
            "recovery_json_sha256": hashlib.sha256(
                (row["recovery_json"] or "").encode()
            ).hexdigest(),
            "workflow_id": row["workflow_id"],
            "attempt_keys": [
                item[0]
                for item in db.execute(
                    "SELECT job_key FROM delivery_attempts WHERE run_id=? ORDER BY job_key",
                    (row["run_id"],),
                )
            ],
        }
    return values


def preflight(config, supplied):
    from .delivery_interrupted_recovery import installation_recovery_lease

    inputs = bootstrap_inputs(config, supplied)
    from .delivery_runtime_service_guard import installation_lease

    with installation_recovery_lease(config, exclusive=True), ExitStack() as leases:
        descriptor = leases.enter_context(installation_lease(config, supplied))
        _store, authority, observations = _authority(
            config, supplied, inputs, leases, service_descriptor=descriptor
        )
        return {
            "kind": SCHEMA,
            "preflight": True,
            "authority": authority,
            "observations": observations,
            "expected_evidence_sha256": digest(authority),
        }


def _native_current(item):
    from .candidate import candidate_for
    from .delivery_native_process import listeners, process_table

    observation = item["native"]
    if (
        hashlib.sha256(private_bytes(Path(observation["manifest"]))).hexdigest()
        != observation["manifest_sha256"]
    ):
        raise ValueError("installation original resource manifest changed")
    for path, sha in observation["journals"].items():
        if hashlib.sha256(private_bytes(Path(path))).hexdigest() != sha:
            raise ValueError("installation original native journal changed")
    table = process_table()
    if any(
        table.get(pid, {}).get("identity") == identity and not table[pid]["stat"].startswith("Z")
        for pid, identity in observation["actors"]
    ) or any(listeners(p) for p in observation["ports"]):
        raise ValueError("installation native actor or port appeared")
    path = Path(item["original_spec"]["checkout"])
    candidate = candidate_for(path) if path.exists() else None
    if candidate != observation["candidate"]:
        raise ValueError("installation retained source candidate changed")


def _rows_current(store, receipt, db):
    from .delivery_native_renewal import _old_proof
    from .delivery_policy_recovery import work_binding

    _stopped(store.config)
    if _native_inputs(store, db) != receipt["native_inputs"]:
        raise ValueError("installation has a new or changed native execution input")
    _global_current(receipt)
    for item in receipt["runs"]:
        scoped_identity(receipt["destination"], item)
        run_id, spec = item["run_id"], item["original_spec"]
        row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
        if (
            not row
            or digest({k: v for k, v in dict(row).items() if k not in ROW_METADATA})
            != item["row_authority_sha256"]
        ):
            raise ValueError("installation native run authority changed")
        for table, key, column in (
            ("delivery_attempts", "attempts", "job_key"),
            ("delivery_effects", "effects", "effect_key"),
            ("delivery_mutations", "mutations", "command_id"),
        ):
            values = [
                dict(r)
                for r in db.execute(
                    f"SELECT * FROM {table} WHERE run_id=? ORDER BY {column}", (run_id,)
                )
            ]
            if digest(values) != item[key + "_sha256"]:
                raise ValueError("installation native attempt/effect/command authority changed")
        claim = store.state.claim_for(db, spec["work_id"])
        identity = (
            {k: claim[k] for k in ("resource", "work_id", "owner", "claimed_at")} if claim else None
        )
        if identity != item["claim"] or work_binding(store, spec, db) != item["work_binding"]:
            raise ValueError("installation original claim/work binding changed")
        for previous in item["historical_specs"]:
            _old_proof(previous)
        record = item["closure_record"]
        current_record = db.execute(
            "SELECT * FROM delivery_closures WHERE workflow_id=? AND execution_run_id=?",
            (record["workflow_id"], record["execution_run_id"]),
        ).fetchone()
        if not current_record or dict(current_record) != record:
            raise ValueError("installation authentic closure record changed")
        from .delivery_closures import _private_root, _read_sealed

        _read_sealed(_private_root(store), record["history_sha256"])
        _native_current(item)


def _record_cohort(store, receipt, reference, *, pending=False, reverse=False, effect_guard=None):
    """Metadata only. In particular, do not bump a stale closed projection's row."""
    from .delivery_runtime_location import PENDING, intent_command_id

    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        _rows_current(store, receipt, db)
        if effect_guard is not None:
            effect_guard(db)
        for item in receipt["runs"]:
            run_id = item["run_id"]
            key = intent_command_id(receipt, run_id) if pending else command_id(receipt, run_id)
            if reverse:
                key = (
                    "runtime-location-rollback-"
                    + digest([receipt["command"]["command_id"], run_id])[:40]
                )
            response = (
                reference
                if reverse
                else {
                    **reference,
                    "run_id": run_id,
                    "command_id": key,
                    **({} if pending else {"original_request_digest": item["request_digest"]}),
                }
            )
            request_digest = digest(
                {
                    "installation_intent" if pending else "installation": receipt["command"],
                    "run_id": run_id,
                    **({"rollback": True} if reverse else {}),
                }
            )
            existing = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (key,),
            ).fetchone()
            if existing:
                if existing[0] != request_digest or json.loads(existing[1]) != response:
                    raise ValueError("installation command identity already binds different inputs")
                continue
            revision = db.execute(
                "SELECT revision FROM delivery_runs WHERE run_id=?", (run_id,)
            ).fetchone()[0]
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (key, run_id, request_digest, canonical_json(response)),
            )
            db.execute(
                "INSERT INTO delivery_events "
                "(run_id,timestamp,type,message,run_revision,payload_json) "
                "VALUES (?,?,?,?,?,?)",
                (
                    run_id,
                    store.state.now(),
                    REVERSED if reverse else PENDING if pending else KIND,
                    "Operator acknowledged installed program locations; "
                    "original execution unchanged",
                    revision,
                    canonical_json(response),
                ),
            )


def _pointer(path, value, *, previous=None):
    from .delivery_filesystem_custody import _immutable
    from .delivery_runtime_registrations import replace

    raw = (canonical_json(value) + "\n").encode()
    if not os.path.lexists(path):
        _immutable(path, raw)
    elif private_bytes(path) != raw:
        if previous is None or json.loads(private_bytes(path)) != previous:
            raise ValueError("installation pointer belongs to another acknowledged operation")
        replace(
            {
                "path": str(path),
                "type": "file",
                "mode": 0o600,
                "before": base64.b64encode(private_bytes(path)).decode(),
                "after": base64.b64encode(raw).decode(),
            }
        )


def _owner_item(config, inputs):
    return {
        "path": str(config.path),
        "type": "file",
        "mode": 0o600,
        "before": base64.b64encode(inputs["before"]).decode(),
        "after": base64.b64encode(inputs["after"]).decode(),
    }


@contextmanager
def _owner_lease(path):
    """Retain the original FD across our own guarded atomic path replacement."""
    original = private_bytes(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        held, named = os.fstat(descriptor), path.lstat()
        if (held.st_dev, held.st_ino) != (named.st_dev, named.st_ino):
            raise ValueError("installation owner name changed before its retained lease")
        if os.read(descriptor, 4 * 1024 * 1024 + 1) != original:
            raise ValueError("installation original owner FD changed before its lease")
        yield descriptor
        os.lseek(descriptor, 0, os.SEEK_SET)
        if os.read(descriptor, 4 * 1024 * 1024 + 1) != original:
            raise ValueError("installation retained original owner FD changed")
    finally:
        os.close(descriptor)


def apply(config, supplied):
    from .delivery_filesystem_custody import _immutable
    from .delivery_interrupted_recovery import installation_recovery_lease
    from .delivery_runtime_location import _acknowledged, pending_acknowledged, reversed_reference
    from .delivery_runtime_registrations import (
        package,
        plugin_apply,
        replace,
        sealed_objects,
        stage,
        validate,
    )
    from .delivery_store import DeliveryStore

    if "expected_evidence_sha256" not in supplied:
        raise ValueError("installation apply requires the exact public preflight evidence hash")
    inputs = bootstrap_inputs(config, supplied)
    from .delivery_filesystem_custody import _opened

    with (
        installation_recovery_lease(config, exclusive=True),
        ExitStack() as leases,
        bootstrap(config, inputs),
    ):
        from .delivery_runtime_registrations import update_leases

        update_leases(supplied, leases)
        from .delivery_runtime_service_guard import install as install_guard
        from .delivery_runtime_service_guard import installation_lease

        service_descriptor = leases.enter_context(installation_lease(config, supplied))
        leases.enter_context(_owner_lease(config.path))
        leases.enter_context(_opened(Path(supplied["owner_after_path"]), lock=True))
        folder = inputs["folder"]
        current_path = config.state_root / "runtime-installations/current.json"
        if (
            os.path.lexists(current_path)
            and json.loads(private_bytes(current_path)).get("command_id") != supplied["command_id"]
        ):
            raise ValueError("installation already has a different location acknowledgement")
        path = folder / "receipt.json"
        raw = receipt_bytes(path) if path.exists() else None
        receipt = (
            load_authority(
                path, INSTALLATION_RECEIPT, expected_sha256=hashlib.sha256(raw).hexdigest()
            )
            if raw is not None
            else None
        )
        reference = (
            {
                "command_id": supplied["command_id"],
                "receipt": str(path),
                "receipt_sha256": hashlib.sha256(raw).hexdigest(),
            }
            if receipt
            else None
        )
        if receipt and receipt["command"] != supplied:
            raise ValueError("installation command ID already belongs to different authority")
        if receipt:
            sealed_objects(receipt)
            if reversed_reference(config, reference):
                raise ValueError(
                    "this installation location was rolled back; apply cannot reactivate it"
                )
        active = _acknowledged(config, reference=reference) if reference else None
        historical = (
            _acknowledged(config, reference=reference, historical=True) if reference else None
        )
        if historical and not active:
            raise ValueError(
                "this installation location was rolled back; apply cannot reactivate it"
            )
        if active:
            install_guard(config, receipt, reference, service_descriptor)
            destination_current(receipt)
            validate(receipt["registrations"], after=True, objects=receipt["installed_objects"])
            _pointer(config.state_root / "runtime-installations/current.json", reference)
            pending_path = config.state_root / "runtime-installations/pending.json"
            if pending_path.exists() and json.loads(private_bytes(pending_path)) == reference:
                pending_path.unlink()
            return {"state": "acknowledged", "replayed": True, **reference}
        pending = bool(reference and pending_acknowledged(config, receipt, reference))
        if not pending:
            store, authority, observations = _authority(
                config, supplied, inputs, leases, service_descriptor=service_descriptor
            )
            if digest(authority) != supplied["expected_evidence_sha256"]:
                raise ValueError("installation preflight authority changed")
            candidate = {
                "schema": SCHEMA,
                "command": supplied,
                **authority,
                "observations": observations,
                "owner_after_digest": digest(json.loads(inputs["after"])),
            }
            if receipt:
                if canonical_json(
                    {
                        k: v
                        for k, v in receipt.items()
                        if k not in {"observations", "installed_objects"}
                    }
                ) != canonical_json({k: v for k, v in candidate.items() if k != "observations"}):
                    raise ValueError("staged installation receipt has different owner inputs")
            else:
                receipt = candidate
                receipt["installed_objects"] = [
                    stage(item) for item in receipt["registrations"]["objects"]
                ]
            _immutable(folder / "owner-before.json", inputs["before"])
            receipt_sha = publish_authority(path, receipt, INSTALLATION_RECEIPT)
            reference = {
                "command_id": supplied["command_id"],
                "receipt": str(path),
                "receipt_sha256": receipt_sha,
            }
            # Writable Store creation occurs only in this explicit effectful
            # owner, after every read-only preflight; no ordinary bypass flag.
            store = DeliveryStore(config)
            _record_cohort(
                store,
                receipt,
                reference,
                pending=True,
                effect_guard=lambda _db: sealed_objects(receipt),
            )
        else:
            store = DeliveryStore(config)
            for item in receipt["runs"]:
                _native_observation(item["original_spec"], leases)
        _pointer(config.state_root / "runtime-installations/pending.json", reference)
        with store._connect() as connection:
            _rows_current(store, receipt, connection)
        try:
            install_guard(config, receipt, reference, service_descriptor)
        except (ValueError, OSError) as exc:
            from .delivery_resources import write_private
            from .delivery_runtime_registrations import _state

            write_private(
                folder / "service-guard-observation.json",
                {
                    "state": "unfinished",
                    "error": str(exc),
                    "relocation_started": any(
                        _state(item) == "after" for item in receipt["installed_objects"]
                    ),
                    "legacy_lifecycle_may_act_through_prevalidated_fd": True,
                    "stopped_asserted": False,
                    "drained_asserted": False,
                },
            )
            raise

        def guard(db=None):
            from .delivery_runtime_service_guard import census, check

            check(config, receipt["service_guard"], service_descriptor, linked=True)
            census(service_descriptor, receipt["service_guard"])
            sealed_objects(receipt)
            destination_current(receipt)
            actual = hashlib.sha256(private_bytes(config.path)).hexdigest()
            if actual not in {supplied["owner_before_sha256"], supplied["owner_after_sha256"]}:
                raise ValueError("installation operational owner drifted during effects")
            if (
                hashlib.sha256(private_bytes(Path(supplied["owner_after_path"]))).hexdigest()
                != supplied["owner_after_sha256"]
            ):
                raise ValueError("installation owner proposal changed during effects")
            if db is None:
                with store._connect() as connection:
                    _rows_current(store, receipt, connection)
            else:
                _rows_current(store, receipt, db)
            validate(receipt["registrations"], objects=receipt["installed_objects"])

        package(receipt["registrations"]["plugin"])
        plugin_apply(receipt["registrations"]["plugin"], guard)
        for item in receipt["installed_objects"]:
            guard()
            replace(item)
            guard()
        guard()
        validate(receipt["registrations"], after=True, objects=receipt["installed_objects"])
        if hashlib.sha256(private_bytes(config.path)).hexdigest() != supplied["owner_after_sha256"]:
            raise ValueError("installation owner postimage is incomplete")
        _record_cohort(store, receipt, reference, effect_guard=guard)
        _pointer(config.state_root / "runtime-installations/current.json", reference)
        pending_path = config.state_root / "runtime-installations/pending.json"
        if json.loads(private_bytes(pending_path)) != reference:
            raise ValueError("installation pending namespace changed before completion")
        pending_path.unlink()
        return {"state": "acknowledged", "replayed": False, **reference}


def rollback(config, supplied):
    """Reverse only owned pointer effects, before any subsequent native admission.

    A location acknowledgement stays historical. This operation never resumes
    an old runtime, changes an execution, or describes absent code as healthy.
    """
    from .delivery_filesystem_custody import _opened
    from .delivery_interrupted_recovery import installation_recovery_lease
    from .delivery_runtime_location import _acknowledged, pending_acknowledged
    from .delivery_runtime_registrations import plugin_apply, replace, sealed_objects, validate
    from .delivery_store import DeliveryStore

    inputs = bootstrap_inputs(config, supplied)
    path = inputs["folder"] / "receipt.json"
    raw = receipt_bytes(path)
    receipt = load_authority(
        path, INSTALLATION_RECEIPT, expected_sha256=hashlib.sha256(raw).hexdigest()
    )
    reference = {
        "command_id": supplied["command_id"],
        "receipt": str(path),
        "receipt_sha256": hashlib.sha256(raw).hexdigest(),
    }
    if (
        receipt.get("schema") != SCHEMA
        or receipt.get("command") != supplied
        or receipt.get("owner_path") != str(config.path)
    ):
        raise ValueError("rollback has no exact original installation receipt")
    sealed_objects(receipt)
    with (
        installation_recovery_lease(config, exclusive=True),
        ExitStack() as leases,
        bootstrap(config, inputs),
    ):
        from .delivery_runtime_registrations import update_leases

        update_leases(supplied, leases)
        from .delivery_runtime_service_guard import installation_lease

        service_descriptor = leases.enter_context(installation_lease(config, supplied))
        leases.enter_context(_owner_lease(config.path))
        leases.enter_context(_opened(Path(supplied["owner_after_path"]), lock=True))
        current_path = config.state_root / "runtime-installations/current.json"
        pending_path = config.state_root / "runtime-installations/pending.json"
        for pointer in (current_path, pending_path):
            if os.path.lexists(pointer) and json.loads(private_bytes(pointer)) != reference:
                raise ValueError("rollback pointer belongs to another installation")
        acknowledged = _acknowledged(config, reference=reference, historical=True)
        intended = pending_acknowledged(config, receipt, reference)
        if not acknowledged and not intended:
            # A staged file grants no effect authority. It may only be abandoned
            # while every original object and owner still has its beforeimage.
            validate(receipt["registrations"], before=True, objects=receipt["installed_objects"])
            if (
                hashlib.sha256(private_bytes(config.path)).hexdigest()
                != supplied["owner_before_sha256"]
            ):
                raise ValueError("unacknowledged rollback cannot adopt an owner write")
            if pending_path.exists():
                pending_path.unlink()
            return {"state": "abandoned_unacknowledged", **reference, "legacy_runtime_ready": False}
        from .delivery_runtime_service_guard import install as install_guard

        store = DeliveryStore(config)
        with store._connect() as db:
            if _native_inputs(store, db) != receipt["native_inputs"]:
                raise ValueError("rollback is forbidden after another native admission or attempt")
        # Legitimate closed projection and cleanup may have settled since ACK.
        # Observe them honestly, then pin those *current* rows for reverse effects.
        observed, _metadata = collect(store, supplied, leases)
        reverse = {**receipt, "runs": observed["runs"]}
        with store._connect() as db:
            _rows_current(store, reverse, db)
        install_guard(config, receipt, reference, service_descriptor)

        def guard(db=None):
            from .delivery_runtime_service_guard import census, check

            check(config, receipt["service_guard"], service_descriptor, linked=True)
            census(service_descriptor, receipt["service_guard"])
            sealed_objects(receipt)
            destination_current(receipt)
            if hashlib.sha256(private_bytes(config.path)).hexdigest() not in {
                supplied["owner_before_sha256"],
                supplied["owner_after_sha256"],
            }:
                raise ValueError("rollback owner changed; preserving it")
            if db is None:
                with store._connect() as connection:
                    _rows_current(store, reverse, connection)
            else:
                _rows_current(store, reverse, db)
            validate(receipt["registrations"], objects=receipt["installed_objects"])

        _pointer(pending_path, reference)
        guard()
        plugin_apply(receipt["registrations"]["plugin"], guard, reverse=True)
        for item in reversed(receipt["installed_objects"]):
            guard()
            replace(item, reverse=True)
            guard()
        guard()
        validate(receipt["registrations"], before=True, objects=receipt["installed_objects"])
        _record_cohort(store, reverse, reference, reverse=True, effect_guard=guard)
        if json.loads(private_bytes(pending_path)) != reference:
            raise ValueError("rollback pending namespace changed")
        pending_path.unlink()
        return {
            "state": "rolled_back",
            **reference,
            "legacy_runtime_ready": False,
            "historical_acknowledgement_retained": bool(acknowledged),
            "legacy_lifecycle_guard": "retained; retirement requires separate old-authority proof",
        }
