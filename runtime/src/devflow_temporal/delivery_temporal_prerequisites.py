"""Explicit pinned Temporal test tools for one unchanged native check, never host caches."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import stat
import subprocess
import tarfile
import tomllib
from collections import OrderedDict
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from threading import RLock

from .contracts import canonical_json, digest
from .delivery_resources import private_directory, read_private

FIELD = "temporal_test_prerequisites"
MARKER = "temporal_test_preparation"
FIELDS = {FIELD}
INPUTS = {"check_id", "python_project", "provenance_path", "provenance_sha256"}
SDK = "1.26.0"
TOOLS = {
    "test_server": {
        "repository": "temporalio/sdk-java",
        "tag": "v1.32.0",
        "archive": "temporal-test-server_1.32.0_macOS_arm64.tar.gz",
        "archive_sha256": "747259da997351ca19e0ce4b08c09dd87817e24ebf3afcd7ce6b4c220f65664a",
        "member": "temporal-test-server_1.32.0_macOS_arm64/temporal-test-server",
        "binary_sha256": "7c5fee72fbb64466f2ebbe84e101c977d5b1957a58264c33189bfeba03a211bc",
        "binary_size": 55983960,
        "maximum": 64 * 1024 * 1024,
        "filename": "temporal-test-server",
        "environment": "JOBCTRL_TEMPORAL_TEST_SERVER_PATH",
    },
    "cli": {
        "repository": "temporalio/cli",
        "tag": "v1.9.1",
        "archive": "temporal_cli_1.9.1_darwin_arm64.tar.gz",
        "archive_sha256": "41e0425378fcb4fb5766340b97435e20fe47bbff2d7bf644ec2d51f7662b7c56",
        "member": "temporal",
        "binary_sha256": "33c298d41f6eefebf01a28a3511f444480e939e21d3518cb0c12a4abad467c4b",
        "binary_size": 150806194,
        "maximum": 192 * 1024 * 1024,
        "filename": "temporal",
        "environment": "JOBCTRL_TEMPORAL_CLI_PATH",
    },
}
MEASURED = {"native_identity", "environment_proof_sha256", "security_binding_sha256"}


# Only completed content observations are memoized, under exact FD/path metadata.
# Every use still opens, checks type/UID/mode/link and revalidates FD and name;
# writes, replacement, chmod and reboot/device changes invalidate the key.
_LARGE_TOOL_HASHES = frozenset(
    value[key] for value in TOOLS.values() for key in ("archive_sha256", "binary_sha256")
)
_HASHES = OrderedDict()
_ATTESTED = set()
_CACHE_LOCK = RLock()


def _digest_stream(raw, expected, stream, info):
    key = (raw, expected, tuple(_stamp(info).values()))
    memoize = expected in _LARGE_TOOL_HASHES
    present = False
    if memoize:
        with _CACHE_LOCK:
            present = key in _HASHES
            if present:
                _HASHES.move_to_end(key)
    if not present:
        if hashlib.file_digest(stream, "sha256").hexdigest() != expected:
            raise ValueError("Temporal prerequisite executable/archive hash changed")
        if memoize:
            with _CACHE_LOCK:
                _HASHES[key] = True
                if len(_HASHES) > 256:
                    _HASHES.popitem(last=False)


def _stamp(info):
    return {
        key: getattr(info, key)
        for key in (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_uid",
            "st_nlink",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
    }


@contextmanager
def _file(raw, maximum, *, private=False, executable=False):
    path = Path(raw)
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("Temporal prerequisite path is not exact canonical owned data")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_nlink != 1
            or before.st_mode & 0o022
            or not 0 < before.st_size <= maximum
            or (private and stat.S_IMODE(before.st_mode) != 0o600)
            or (executable and not before.st_mode & 0o111)
        ):
            raise ValueError("Temporal prerequisite is not bounded regular owned evidence")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            yield stream, before
        if _stamp(os.fstat(fd)) != _stamp(before) or _stamp(path.lstat()) != _stamp(before):
            raise ValueError("Temporal prerequisite changed during observation")
    finally:
        os.close(fd)


def _reference(raw, expected, *, maximum=4 * 1024 * 1024, private=False):
    with _file(raw, maximum, private=private) as (stream, info):
        content = stream.read(maximum + 1)
        if len(content) != info.st_size or hashlib.sha256(content).hexdigest() != expected:
            raise ValueError("Temporal prerequisite declared evidence hash changed")
    return content


def _hash(raw, expected, *, maximum, executable=False):
    with _file(raw, maximum, executable=executable) as (stream, info):
        _digest_stream(raw, expected, stream, info)
        return {
            "path": raw,
            "sha256": expected,
            "size": info.st_size,
            "mode": stat.S_IMODE(info.st_mode),
            "uid": info.st_uid,
        }


def _project(spec, project, checkout):
    if project != "workers/automation":
        raise ValueError("Temporal prerequisite needs its explicit supported Python project")
    source, metadata = Path(spec["source_path"]), {}
    for name in ("pyproject.toml", "uv.lock"):
        relative = project + "/" + name
        entry = subprocess.check_output(
            ["git", "-C", str(source), "ls-tree", spec["base_sha"], "--", relative], timeout=30
        )
        if not entry.startswith((b"100644 blob ", b"100755 blob ")):
            raise ValueError("Temporal project metadata is not regular frozen Git data")
        raw = subprocess.check_output(
            ["git", "-C", str(source), "show", spec["base_sha"] + ":" + relative], timeout=30
        )
        if len(raw) > 4 * 1024 * 1024:
            raise ValueError("Temporal project metadata exceeds its byte bound")
        expected = hashlib.sha256(raw).hexdigest()
        if checkout is not None:
            _reference(str(checkout / relative), expected)
        metadata[name] = expected
        document = tomllib.loads(raw.decode())
        if name == "uv.lock":
            versions = [
                p.get("version")
                for p in document.get("package", [])
                if p.get("name") == "temporalio"
            ]
            if versions != [SDK]:
                raise ValueError("Temporal prerequisite requires the exact frozen SDK1.26.0")
    return metadata


def _evidence(receipt):
    pending, found = [receipt], {}
    while pending:
        value = pending.pop()
        if isinstance(value, list):
            pending.extend(value)
        elif isinstance(value, dict):
            pending.extend(value.values())
            pairs = [(value.get("path"), value.get("sha256"))]
            for key, expected in value.items():
                if key.endswith("_sha256"):
                    stem = key.removesuffix("_sha256")
                    raw = value.get(stem + "_path", value.get(stem))
                    if stem == "original_custody":
                        raw = value.get("original_custody_receipt")
                    pairs.append((raw, expected))
            for raw, expected in pairs:
                if not isinstance(raw, str) or not raw.startswith("/"):
                    continue  # Content hashes without path declarations are metadata.
                if not isinstance(expected, str) or not re.fullmatch("[0-9a-f]{64}", expected):
                    raise ValueError("Temporal prerequisite declared a malformed evidence hash")
                if raw in found and found[raw]["sha256"] != expected:
                    raise ValueError("Temporal prerequisite evidence declarations conflict")
                if raw not in found:
                    if len(found) >= 64:
                        raise ValueError("Temporal prerequisite provenance inventory is unbounded")
                    found[raw] = _hash(raw, expected, maximum=192 * 1024 * 1024)
    return sorted(found.values(), key=lambda item: item["path"])


def _tools(receipt):
    if (
        receipt.get("schema") != "jobctrl.temporal-test-prerequisites/v1"
        or receipt.get("schema_version") != 1
        or platform.system() != "Darwin"
        or platform.machine() != "arm64"
        or receipt.get("compatibility_facts", {}).get("locked_sdk_version") != SDK
    ):
        raise ValueError("Temporal prerequisite provenance/SDK/platform is unsupported")
    observed = {}
    for key, pinned in TOOLS.items():
        tool = receipt["tools"][key]
        archive, binary = tool["archive"], tool["binary"]
        if (
            tool.get("official_repository") != pinned["repository"]
            or tool.get("pinned_tag") != pinned["tag"]
            or tool.get("asset_name") != pinned["archive"]
            or tool.get("asset_url")
            != f"https://github.com/{pinned['repository']}/releases/download/{pinned['tag']}/{pinned['archive']}"
            or tool.get("published_digest", {}).get("algorithm") != "sha256"
            or tool["published_digest"].get("value") != pinned["archive_sha256"]
            or archive.get("sha256") != pinned["archive_sha256"]
            or tool.get("extraction", {}).get("exact_member") != pinned["member"]
            or binary.get("sha256") != pinned["binary_sha256"]
            or binary.get("platform") != "darwin_arm64"
            or binary.get("size") != pinned["binary_size"]
        ):
            raise ValueError("Temporal prerequisite differs from its pinned official tools")
        archive_ref = _hash(archive["path"], pinned["archive_sha256"], maximum=64 * 1024 * 1024)
        binary_ref = _hash(
            binary["path"], pinned["binary_sha256"], maximum=pinned["maximum"], executable=True
        )
        # Verify the member-to-executable relation; caller-authored filenames or
        # metadata cannot confer publisher provenance. Never extract tar paths.
        relation = (pinned["archive_sha256"], pinned["member"], pinned["binary_sha256"])
        with _CACHE_LOCK:
            attested = relation in _ATTESTED
        if not attested:
            _attest_member(archive["path"], pinned)
            with _CACHE_LOCK:
                _ATTESTED.add(relation)
        if (
            binary_ref["mode"] != int(binary["mode"], 0)
            or binary_ref["uid"] != binary["uid"]
            or archive_ref["size"] != archive["size"]
        ):
            raise ValueError("Temporal prerequisite declared size or custody mode changed")
        observed[key] = {
            "archive": archive_ref,
            "binary": binary_ref,
            "member": pinned["member"],
            "platform": "darwin_arm64",
        }
    return observed


def _attest_member(path, pinned):
    with _file(path, 64 * 1024 * 1024) as (stream, info):
        _digest_stream(path, pinned["archive_sha256"], stream, info)
        stream.seek(0)
        with tarfile.open(fileobj=stream, mode="r|gz") as archive_file:
            matched, total = False, 0
            for member in archive_file:
                total += member.size
                parts = Path(member.name).parts
                if (
                    member.name.startswith("/")
                    or ".." in parts
                    or member.issym()
                    or member.islnk()
                    or not (member.isfile() or member.isdir())
                    or total > 256 * 1024 * 1024
                ):
                    raise ValueError("Temporal official archive has unsafe members")
                if member.name != pinned["member"]:
                    continue
                if matched or not member.isfile() or member.size != pinned["binary_size"]:
                    raise ValueError("Temporal official tool member is not unique regular data")
                data = archive_file.extractfile(member)
                if (
                    data is None
                    or hashlib.file_digest(data, "sha256").hexdigest() != pinned["binary_sha256"]
                ):
                    raise ValueError("Temporal executable differs from its official archive member")
                matched = True
            if not matched:
                raise ValueError("Temporal executable lacks its official archive member")


def proposal(seal, command, *, historical=False, execution=None):
    original = seal["predecessor_spec"]
    if FIELD not in command:
        return original, None
    value = command[FIELD]
    if (
        not isinstance(value, dict)
        or set(value) != INPUTS
        or any(not isinstance(value[key], str) for key in INPUTS)
        or original.get("provider") != "codex"
        or original.get("feature_worker")
        or original.get("feature_delivery")
        or original.get(MARKER)
        or original["policy"].get("host_sandbox") != "trusted-local"
        or not isinstance(value.get("provenance_sha256"), str)
        or not re.fullmatch("[0-9a-f]{64}", value["provenance_sha256"])
    ):
        raise ValueError("Temporal preparation needs one explicit ordinary stopped declaration")
    path = Path(value["provenance_path"])
    root = Path(original["state_dir"]).parents[1]
    if not path.is_absolute() or not path.is_relative_to(root):
        raise ValueError("Temporal prerequisite provenance must be private service-state input")
    receipt = json.loads(_reference(str(path), value["provenance_sha256"], private=True))
    if not isinstance(receipt, dict) or not isinstance(receipt.get("source_facts"), dict):
        raise ValueError("Temporal prerequisite provenance is malformed")
    checks = [
        check for check in original["policy"].get("checks", []) if check["id"] == value["check_id"]
    ]
    if len(checks) != 1 or checks[0].get("kind") != "test":
        raise ValueError("Temporal prerequisite needs one unchanged frozen native test check")
    metadata = _project(
        original, value["python_project"], None if historical else Path(original["checkout"])
    )
    if (
        receipt["source_facts"].get("pyproject_sha256") != metadata["pyproject.toml"]
        or receipt["source_facts"].get("uv_lock_sha256") != metadata["uv.lock"]
    ):
        raise ValueError("Temporal tool compatibility belongs to different locked project metadata")
    try:
        tool_proof, evidence = _tools(receipt), _evidence(receipt)
    except (KeyError, TypeError, AttributeError, tarfile.TarError) as exc:
        raise ValueError("Temporal prerequisite provenance is incomplete or malformed") from exc
    proof = {
        "schema": "devflow-temporal-test-preparation-v1",
        "sdk": SDK,
        "declaration": value,
        "check": checks[0],
        "check_sha256": digest(checks[0]),
        "metadata": metadata,
        "tools": tool_proof,
        "evidence": evidence,
        "provenance": {"path": str(path), "sha256": value["provenance_sha256"]},
    }
    execution = deepcopy(original if execution is None else execution)
    execution[MARKER] = {
        "command_digest": digest({"run_id": original["run_id"], **command}),
        "proof_sha256": digest(proof),
    }
    return execution, proof


def _transition(expected, actual):
    from .delivery_runtime_location import policy_for_comparison

    actual_policy = policy_for_comparison(expected, actual)
    derived = {
        "policy",
        "policy_digest",
        "preparation",
        "role_home_generation",
        "gate_admission",
        "implement_admission",
        "implementation_role_home_generation",
    }
    if {k: v for k, v in actual.items() if k not in derived} != {
        k: v for k, v in expected.items() if k not in derived
    } or {k: v for k, v in actual_policy.items() if k not in MEASURED} != {
        k: v for k, v in expected["policy"].items() if k not in MEASURED
    }:
        raise ValueError("Temporal prerequisite changed frozen execution authority")


def custody(db, spec, recovery):
    marker = spec.get(MARKER)
    if marker is None:
        from .delivery_feature_revision_recovery import retained_recovery

        previous = recovery
        while previous:
            details = retained_recovery(previous)
            if FIELD in details.get("command", {}) or details.get("predecessor_spec", {}).get(
                MARKER
            ):
                raise ValueError("Temporal prerequisite marker was removed from its execution")
            previous = details.get("original_recovery")
        return None
    if not isinstance(marker, dict) or set(marker) != {"command_digest", "proof_sha256"}:
        raise ValueError("Temporal preparation marker is malformed")
    from .delivery_feature_revision_recovery import retained_recovery

    item = recovery
    while item:
        details = retained_recovery(item)
        if details.get("command_digest") == marker["command_digest"]:
            break
        item = details.get("original_recovery")
    else:
        raise ValueError("Temporal prerequisite is absent from admitted execution ancestry")
    command = db.execute(
        "SELECT request_digest,response_json FROM delivery_commands "
        "WHERE command_id=? AND run_id=?",
        (details["command"]["command_id"], spec["run_id"]),
    ).fetchone()
    path = Path(spec["state_dir"]) / "stopped-resumes" / marker["command_digest"] / "admission.json"
    if (
        details.get("kind") != "stopped_delivery_resume"
        or FIELD not in details["command"]
        or not command
        or command[0] != marker["command_digest"]
        or marker["command_digest"] != digest({"run_id": spec["run_id"], **details["command"]})
        or json.loads(command[1]).get("admission_sha256") != digest(item)
        or read_private(path) != item
        or details["predecessor_spec"]["run_id"] != spec["run_id"]
    ):
        raise ValueError("Temporal prerequisite lost immutable command acknowledgement")
    original, command = details["predecessor_spec"], details["command"]
    base = original
    if {"added_paths", "amended_config_path", "amended_config_sha256"} <= command.keys():
        from .delivery_config import scope_receipt_spec

        base = scope_receipt_spec(
            original,
            Path(command["amended_config_path"]),
            command["amended_config_sha256"],
            command["added_paths"],
        )
    elif "browser_qa_amendment" in command:
        from .delivery_browser_mode_recovery import _fixture_custody
        from .delivery_browser_mode_recovery import proposal as browser_proposal

        base, _ = browser_proposal(
            details, command, historical_sources=_fixture_custody(spec, details)
        )
    expected, proof = proposal(details, command, historical=True, execution=base)
    if (
        details.get(FIELD) != proof
        or marker != expected[MARKER]
        or marker["proof_sha256"] != digest(proof)
    ):
        raise ValueError("Temporal prerequisite lost its original provenance")
    _transition(expected, details["execution_spec"])
    return proof


def launch_authority(spec):
    from .delivery_execution_config import load_execution_config
    from .delivery_store import DeliveryStore

    store = DeliveryStore(load_execution_config(spec), read_only=True)
    if store.effective_spec(spec["run_id"]) != spec:
        raise ValueError("Temporal prerequisite differs from current execution")
    with store._connect() as db:
        row = db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
        proof = custody(db, spec, json.loads(row[0]) if row and row[0] else None)
        claim = store.state.claim_for(db, spec["work_id"])
        if not claim or claim["owner"] != "external:devflow:" + spec["run_id"]:
            raise ValueError("Temporal prerequisite launch lost its current own claim")
        from .delivery_policy_recovery import work_binding

        work_binding(store, spec, db)
    return proof


def _copy(source, target, pinned):
    # O_EXCL never replaces an older executable. A complete copy is published by
    # the existing atomic no-replace owner; interrupted temporary files are inert.
    import tempfile

    from .delivery_filesystem_custody import _publish_exclusive

    if os.path.lexists(target):
        return _hash(
            str(target), pinned["binary_sha256"], maximum=pinned["maximum"], executable=True
        )
    fd, raw = tempfile.mkstemp(prefix=".temporal-tool-", dir=target.parent)
    stage = Path(raw)
    try:
        with os.fdopen(fd, "wb") as output:
            with _file(source, pinned["maximum"], executable=True) as (stream, _):
                sha = hashlib.sha256()
                while chunk := stream.read(1024 * 1024):
                    output.write(chunk)
                    sha.update(chunk)
                if sha.hexdigest() != pinned["binary_sha256"]:
                    raise ValueError("Temporal prerequisite source changed during its owned copy")
            output.flush()
            os.fchmod(output.fileno(), 0o500)
            os.fsync(output.fileno())
        try:
            _publish_exclusive(stage, target)
        except FileExistsError:
            pass
        directory = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return _hash(
            str(target), pinned["binary_sha256"], maximum=pinned["maximum"], executable=True
        )
    finally:
        stage.unlink(missing_ok=True)


def _preparation_authority(spec, checkout, evidence, check, registry, manifest):
    """Derive a subject and generation from the admitted resource owner, not a receipt."""
    from .candidate import candidate_for
    from .delivery_broker import _git
    from .delivery_execution_config import load_execution_config
    from .delivery_native_guard import validate_native_turn
    from .delivery_policy_recovery import work_binding
    from .delivery_resources import _gate_evidence_root, _gate_path, _identity, same_object
    from .delivery_sandbox import _native_check_key
    from .delivery_store import DeliveryStore

    root = _gate_evidence_root(spec)
    relative = evidence.relative_to(root)
    if (
        len(relative.parts) != 2
        or relative.parts[0] != "checks"
        or not relative.parts[1].isdecimal()
        or str(int(relative.parts[1])) != relative.parts[1]
    ):
        raise ValueError("Temporal preparation left its exact admitted check evidence")
    iteration = int(relative.parts[1])
    store = DeliveryStore(load_execution_config(spec), read_only=True)
    validate_native_turn(spec, "verify", iteration, store)
    with store._connect() as db:
        db.execute("BEGIN")
        current = db.execute(
            "SELECT candidate_json,iteration FROM delivery_runs WHERE run_id=?",
            (spec["run_id"],),
        ).fetchone()
        claim = store.state.claim_for(db, spec["work_id"])
        if not claim or claim["owner"] != "external:devflow:" + spec["run_id"]:
            raise ValueError("Temporal preparation lost its current own work claim")
        binding = work_binding(store, spec, db)
        claim_identity = {key: claim[key] for key in ("resource", "work_id", "owner", "claimed_at")}
    expected = (
        json.loads(current["candidate_json"]) if current and current["candidate_json"] else None
    )
    if (
        not isinstance(expected, dict)
        or current["iteration"] != iteration
        or expected.get("policy_digest") != spec["policy_digest"]
        or expected.get("base_sha") != spec["base_sha"]
    ):
        raise ValueError("Temporal preparation has no current admitted check candidate")
    if checkout not in {Path(spec["checkout"]), _gate_path(spec, "verify", iteration)}:
        raise ValueError("Temporal preparation has no admitted check subject")
    entry = manifest["roots"].get(str(checkout), {})
    if (
        entry.get("kind") not in {"checkout", "gate"}
        or not entry.get("identity")
        or entry.get("receipt", {}).get("state") in {"removed", "already_absent"}
        or not same_object(spec, checkout, entry["identity"])
    ):
        raise ValueError("Temporal preparation subject has no current registered custody")
    registry._allowed(checkout, entry["kind"])
    if _git(checkout, "rev-parse", "--show-toplevel") != str(checkout):
        raise ValueError("Temporal preparation subject is not its exact Git root")
    candidate = candidate_for(checkout)
    if any(expected.get(key) != value for key, value in candidate.items()):
        raise ValueError("Temporal preparation subject differs from the current run candidate")
    if entry.get("head", candidate["head"]) != candidate["head"]:
        raise ValueError("Temporal preparation gate head changed")
    key = _native_check_key(spec, evidence, check["id"])
    home = Path(spec["state_dir"]) / "transient/checks" / key
    recipe = read_private(home / "codex/native-environment.json")
    if type(recipe.get("version")) is not int or recipe["version"] not in (1, 2, 3):
        raise ValueError("Temporal preparation lost its owned environment recipe")
    short = recipe["version"] >= 2
    parent = registry._execution_scratch_root() if short else Path(spec["state_dir"]) / "transient"
    scratch = parent / digest(["checks", key])[:16] if short else home / "tmp"
    parent_entry = manifest["roots"].get(str(parent), {})
    kind = "execution-scratch" if short else "transient"
    if (
        parent_entry.get("kind") != kind
        or not parent_entry.get("identity")
        or parent_entry.get("receipt", {}).get("state") in {"removed", "already_absent"}
        or not same_object(spec, parent, parent_entry["identity"])
    ):
        raise ValueError("Temporal preparation scratch has no registered owner generation")
    registry._allowed(parent, kind)
    if (
        stat.S_IMODE(parent.lstat().st_mode) != 0o700
        or scratch.resolve(strict=True) != scratch
        or stat.S_IMODE(scratch.lstat().st_mode) != 0o700
    ):
        raise ValueError("Temporal preparation scratch ancestry or mode changed")
    return {
        "key": key,
        "evidence": str(evidence),
        "check_sha256": digest(check),
        "work_binding": binding,
        "claim": claim_identity,
        "subject": {
            "path": str(checkout),
            "kind": entry["kind"],
            "identity": entry["identity"],
            "generation": entry.get("generation", 0),
            "candidate": candidate,
            "admitted_candidate": expected,
        },
        "environment_recipe": recipe,
        "scratch": str(scratch),
        "scratch_identity": _identity(scratch),
        "scratch_owner": {
            "path": str(parent),
            "kind": kind,
            "identity": parent_entry["identity"],
            "generation": parent_entry.get("generation", 0),
        },
    }


def prepare_check(spec, checkout, evidence, check, scratch):
    if spec.get(MARKER) is None:
        return {}
    proof = launch_authority(spec)
    if check["id"] != proof["check"]["id"]:
        return {}
    if (
        check != proof["check"]
        or _project(spec, proof["declaration"]["python_project"], checkout) != proof["metadata"]
    ):
        raise ValueError("Temporal prerequisite native check or metadata changed")
    from .delivery_resources import RunResources, write_private

    registry = RunResources(spec)
    with registry.locked() as manifest:
        authority = _preparation_authority(spec, checkout, evidence, check, registry, manifest)
        if scratch != Path(authority["scratch"]):
            raise ValueError("Temporal preparation received a foreign scratch path")
        directory = scratch / "temporal-tools"
        private_directory(directory)
        environment, copied = {}, {}
        for key, pinned in TOOLS.items():
            target = directory / pinned["filename"]
            copied[key] = _copy(proof["tools"][key]["binary"]["path"], target, pinned)
            environment[pinned["environment"]] = str(target)
        from .delivery_metadata_recovery import _immutable

        request = {
            "run_id": spec["run_id"],
            "preparation": spec[MARKER],
            "policy_digest": spec["policy_digest"],
            "check_sha256": digest(check),
            "owner_authority": authority,
            "checkout": str(checkout),
            "scratch": str(scratch),
            "environment": environment,
            "tools": copied,
            "scratch_identity": {
                "device": scratch.stat().st_dev,
                "inode": scratch.stat().st_ino,
                "uid": scratch.stat().st_uid,
            },
            "tool_identities": {
                key: {
                    "device": Path(item["path"]).stat().st_dev,
                    "inode": Path(item["path"]).stat().st_ino,
                }
                for key, item in copied.items()
            },
        }
        private_directory(evidence / check["id"])
        path = evidence / check["id"] / "temporal-prerequisites.json"
        expected_raw = (canonical_json(request) + "\n").encode()
        expected_sha256 = hashlib.sha256(expected_raw).hexdigest()
        if os.path.lexists(path):
            _reference(str(path), expected_sha256, private=True)
        _immutable(path, request)
        if _preparation_authority(spec, checkout, evidence, check, registry, manifest) != authority:
            raise ValueError("Temporal preparation subject changed during its owned copy")
        _reference(str(path), expected_sha256, private=True)
        seal = {
            "authority": authority,
            "receipt": str(path),
            "receipt_sha256": expected_sha256,
            "preparation": spec[MARKER],
        }
        prior = manifest.setdefault("temporal_test_preparations", {}).get(authority["key"])
        if prior is not None and prior != seal:
            raise ValueError("Temporal preparation conflicts with its owner generation")
        manifest["temporal_test_preparations"][authority["key"]] = seal
        write_private(registry.manifest, manifest)
    return environment


def check_launch(process):
    keys = {tool["environment"] for tool in TOOLS.values()}
    present = keys & process.environment.keys()
    if not present:
        if process.spec.get(MARKER) is not None:
            proof = launch_authority(process.spec)
            if process.folder.parent.name == proof["check"]["id"]:
                raise ValueError("Temporal prerequisite native launch lost its tool environment")
        return
    if present != keys or process.spec.get(MARKER) is None:
        raise ValueError("Temporal native launch has an undeclared tool environment")
    proof = launch_authority(process.spec)
    check = proof["check"]
    from .delivery_resources import RunResources
    from .delivery_sandbox import _native_check_key

    registry = RunResources(process.spec, read_only=True)
    evidence = process.folder.parent.parent
    key = _native_check_key(process.spec, evidence, check["id"])
    with registry.locked(read_only=True) as manifest:
        seal = manifest.get("temporal_test_preparations", {}).get(key)
        if not isinstance(seal, dict) or seal.get("preparation") != process.spec[MARKER]:
            raise ValueError("Temporal preparation has no acknowledged resource owner")
        checkout = Path(seal["authority"]["subject"]["path"])
        authority = _preparation_authority(
            process.spec, checkout, evidence, check, registry, manifest
        )
        if authority != seal.get("authority"):
            raise ValueError("Temporal preparation owner subject or generation changed")
        path = evidence / check["id"] / "temporal-prerequisites.json"
        if seal.get("receipt") != str(path):
            raise ValueError("Temporal preparation owner receipt left its exact check")
        recorded = json.loads(_reference(str(path), seal["receipt_sha256"], private=True))
        _check_owned_launch(process, proof, recorded, authority)
        if (
            _preparation_authority(process.spec, checkout, evidence, check, registry, manifest)
            != authority
        ):
            raise ValueError("Temporal preparation subject changed during launch observation")


def _check_owned_launch(process, proof, recorded, authority):
    keys = {tool["environment"] for tool in TOOLS.values()}
    check = proof["check"]
    environment = {key: process.environment[key] for key in keys}
    checkout = Path(authority["subject"]["path"])
    if (
        recorded.get("run_id") != process.spec["run_id"]
        or recorded.get("checkout") != str(checkout)
        or recorded.get("scratch") != authority["scratch"]
        or recorded.get("scratch_identity") != authority["scratch_identity"]
        or recorded.get("preparation") != process.spec[MARKER]
        or recorded.get("policy_digest") != process.spec["policy_digest"]
        or recorded.get("check_sha256") != proof["check_sha256"]
        or recorded.get("owner_authority") != authority
        or environment != recorded.get("environment")
        or process.folder.name not in {"native", "native-retry"}
        or process.folder.parent.name != check["id"]
        or process.argv != check["argv"]
        or process.cwd != (checkout / check.get("cwd", ".")).resolve(strict=True)
        or process.timeout != check.get("timeout_seconds", 600)
        or process.environment.get("TMPDIR") != recorded.get("scratch")
        or _project(process.spec, proof["declaration"]["python_project"], checkout)
        != proof["metadata"]
    ):
        raise ValueError("Temporal prerequisite native launch binding changed")
    from .delivery_resources import same_object

    if not same_object(process.spec, Path(recorded["scratch"]), recorded.get("scratch_identity")):
        raise ValueError("Temporal prerequisite lost its exact owned scratch generation")
    for key, pinned in TOOLS.items():
        target = Path(recorded["scratch"]) / "temporal-tools" / pinned["filename"]
        info = target.lstat()
        if (
            {"device": info.st_dev, "inode": info.st_ino} != recorded["tool_identities"][key]
            or environment[pinned["environment"]] != str(target)
            or _hash(
                str(target), pinned["binary_sha256"], maximum=pinned["maximum"], executable=True
            )
            != recorded["tools"][key]
        ):
            raise ValueError("Temporal prerequisite owned launch executable changed")
