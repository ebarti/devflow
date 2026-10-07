"""Read retained published-metadata evidence for already-admitted executions."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from copy import deepcopy
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_broker import DeliveryBroker, _git
from .delivery_continuation import session_state_digest
from .delivery_metadata_contract import evidence_applicability as evidence_applicability
from .delivery_policy_recovery import _rows, work_binding
from .delivery_resources import private_directory, read_private


def _immutable(path, value, *, raw=None):
    data = raw if raw is not None else (canonical_json(value) + "\n").encode()
    if canonical_json(json.loads(data)) != canonical_json(value):
        raise ValueError("immutable raw evidence differs from its typed value")
    stage = path.with_name(".stage-" + path.name + "-" + hashlib.sha256(data).hexdigest())

    def staging():
        info = stage.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink not in (1, 2)
        ):
            raise ValueError("metadata journal staging identity is unsafe")
        return info

    if path.exists():
        info = path.lstat()
        if info.st_nlink == 2 and stage.exists():
            staged = staging()
            if (staged.st_dev, staged.st_ino) != (info.st_dev, info.st_ino):
                raise ValueError("metadata journal has an unrelated hardlink")
            if path.read_bytes() != data:
                raise ValueError("metadata staged journal changed")
            stage.unlink()
        if canonical_json(read_private(path)) != canonical_json(value) or path.read_bytes() != data:
            raise ValueError("metadata immutable journal changed")
        return
    if stage.exists():
        info = staging()
        if info.st_nlink != 1 or not data.startswith(stage.read_bytes()):
            raise ValueError("metadata unfinished staging bytes are not attributable")
        stage.unlink()  # Only this hash-named unpublished staging prefix, never a receipt.
    fd = os.open(stage, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.link(stage, path)
    stage.unlink()
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def preserve_resources(root, spec):
    """Before a new resource generation, preserve exact old bytes and hashes."""
    if spec.get("resource_cleanup_version") != 1:
        return
    target = root / "predecessor-resources"
    private_directory(target)
    for name in ("manifest.json", "finalization.json"):
        source = Path(spec["state_dir"]) / "resources" / name
        _immutable(target / name, read_private(source), raw=source.read_bytes())


def _authority(payload, spec):
    reference = Path(payload["authority_path"])
    info = reference.lstat()
    if (
        not reference.is_absolute()
        or reference.is_symlink()
        or not reference.is_file()
        or info.st_uid != os.getuid()
        or info.st_nlink != 1
        or info.st_mode & 0o022
        or info.st_size > 128 * 1024
    ):
        raise ValueError("metadata authority reference is unsafe")
    raw = reference.read_bytes()
    if hashlib.sha256(raw).hexdigest() != payload["authority_sha256"]:
        raise ValueError("metadata authority reference changed")
    value = json.loads(raw)
    scope = value.get("scope", {})
    eligible = scope.get("eligible_original_run_ids", [])
    if (
        value.get("decision_owner") != "main task"
        or value.get("new_user_approval_required") is not False
        or not isinstance(value.get("authority_source"), str)
        or not value["authority_source"]
        or not isinstance(eligible, list)
        or not 1 <= len(eligible) <= 2
        or any(not isinstance(item, str) or not item for item in eligible)
        or len(set(eligible)) != len(eligible)
        or spec["run_id"] not in eligible
        or type(scope.get("max_reconciliation_commands_per_original_run")) is not int
        or scope["max_reconciliation_commands_per_original_run"] != 1
        or scope.get("exact_duplicate_and_known_interrupted_effect_resume_allowed") is not True
        or scope.get("known_base") != spec["base_sha"]
        or scope.get("repository", "").casefold() != "github.com/" + spec["github_repo"].casefold()
    ):
        raise ValueError("metadata authority does not bind this original run")
    return value


def _identity(broker):
    author = _git(broker.checkout, "var", "GIT_AUTHOR_IDENT")
    committer = _git(broker.checkout, "var", "GIT_COMMITTER_IDENT")
    name = author.rsplit(" ", 2)[0]
    if name != committer.rsplit(" ", 2)[0] or not re.fullmatch(r"[^<>\n]+ <[^<>\s]+>", name):
        raise ValueError("existing human publication signer is not recognized")
    return name, committer










def validation_readback(store, spec, recovery):
    if recovery.get('native_preparation_renewal'):
        from .delivery_native_renewal import verify_generation

        verify_generation(recovery['spec'], recovery['native_preparation_renewal'], spec)
    broker = DeliveryBroker(store, spec)
    if canonical_json(broker.candidate()) != canonical_json(recovery["candidate"]):
        raise ValueError("metadata validation candidate changed")
    broker._validate_publication_commits()
    remote = _git(broker.source, "ls-remote", "origin", f"refs/heads/{spec['branch']}")
    found = broker._existing_pr()
    if (
        not remote
        or remote.split()[0] != recovery["new_head"]
        or found is None
        or found["number"] != recovery["publication"]["number"]
        or found["headRefOid"] != recovery["new_head"]
    ):
        raise ValueError("metadata validation remote publication changed")
    with store._connect() as db:
        work_binding(store, spec, db)
        grant = db.execute(
            "SELECT * FROM delivery_metadata_recoveries WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
        if not grant or digest(json.loads(grant["grant_json"])) != recovery["grant_digest"]:
            raise ValueError("metadata validation grant changed")
    root = Path(spec["state_dir"]) / "metadata-reconciliation"
    original = read_private(root / "intent.json")
    if digest(original) != recovery["grant_digest"]:
        raise ValueError("metadata immutable intent changed before validation")
    _authority(original["command"], spec)
    _row, attempts, effects, _claim = _rows(store, spec["run_id"])
    by_key = {item["effect_key"]: item for item in effects}
    if canonical_json(attempts) != canonical_json(original["attempts"]) or any(
        canonical_json(by_key.get(item["effect_key"])) != canonical_json(item)
        for item in original["effects"]
    ):
        raise ValueError("metadata predecessor attempt/publication history changed")
    if (
        spec["provider"] == "codex"
        and session_state_digest(
            Path(spec["state_dir"]) / "role-homes/implement",
            original["session_id"],
        )
        != original["session_sha256"]
    ):
        raise ValueError("metadata original session evidence changed")
    for kind, head in (("original", recovery["old_head"]), ("rewritten", recovery["new_head"])):
        ref = f"refs/devflow/metadata/{spec['run_id']}/{kind}"
        if _git(
            broker.checkout, "for-each-ref", "--format=%(objectname)", ref
        ) != head or canonical_json(read_private(root / (kind + "-ref.json"))) != canonical_json(
            {"ref": ref, "head": head}
        ):
            raise ValueError("metadata preserved object/ref custody changed")
    return deepcopy(recovery["publication"])
