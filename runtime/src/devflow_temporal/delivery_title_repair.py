"""One receipt-bound literal-only repair; no assertion or file-scope expansion."""

from __future__ import annotations

import hashlib
import os
import re
import stat
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_broker import _git
from .delivery_gates_admission import _reference
from .delivery_metadata_recovery import validation_readback
from .delivery_repair import _historical_browser_rejection

_TITLE = re.compile(r"""(?m)^test\((?P<quote>["'])(?P<title>[^"'\\\r\n]+)(?P=quote),""")


def prepare(store, spec, state, previous, supplied, session):
    """Read-only authentication before the existing one-time repair admission."""
    authority = _reference(supplied["authority_path"], supplied["authority_sha256"])
    scope = authority.get("scope", {})
    technical = (isinstance(previous, dict)
                 and previous.get('kind') == 'accepted_technical_successor')
    metadata = previous.get('original_recovery') if technical else previous
    original_base = previous['spec']['base_sha'] if technical else spec['base_sha']
    if technical:
        from .delivery_technical_continuation import readback

        readback(store, spec, previous, require_claim=False)
        integration = previous.get('integration')
        retained_roles = previous['state']['roles']
        if (not integration or integration['original_base'] != original_base
                or integration['main'] != spec['base_sha']
                or state.get('roles', [])[:len(retained_roles)] != retained_roles
                or any(role.get('role') not in {'review', 'verify'}
                       or role.get('iteration') != 4 or role.get('cleanup') != 'confirmed'
                       for role in state.get('roles', [])[len(retained_roles):])):
            raise ValueError('title repair changed its integration or independent gate ancestry')
    if (
        authority.get("decision_owner") != "main task"
        or authority.get("new_user_approval_required") is not False
        or scope.get("run_id") != spec["run_id"]
        or scope.get("work_id") != spec["work_id"]
        or scope.get("session_id") != session
        or scope.get("base") != original_base
        or any(
            type(scope.get(k)) is not int or scope[k] != value
            for k, value in (
                ("additional_iterations", 1),
                ("authorized_through_iteration", 5),
                ("max_new_grants", 1),
                ("max_new_implementation_turns", 1),
            )
        )
        or supplied["additional_iterations"] != 1
        or state["iteration"] != 4
        or spec["policy"]["max_repairs"] != 3
        or not isinstance(previous, dict)
        or not isinstance(metadata, dict)
        or metadata.get("kind") != "published_metadata_recovery"
        or metadata.get("old_head") != scope.get("known_old_head")
        or canonical_json(previous.get("candidate")) != canonical_json(state["candidate"])
        or canonical_json(previous.get("execution_spec", previous.get("spec")))
        != canonical_json(spec)
        or (not technical and canonical_json(state.get("roles"))
            != canonical_json(previous["state"]["roles"]))
    ):
        raise ValueError("title repair does not bind its one authorized effective metadata lineage")
    if not technical:
        validation_readback(store, spec, previous)
    receipt = state.get("checks", {}).get("browser_qa")
    if (
        not isinstance(receipt, dict)
        or receipt.get("state") != "failed"
        or receipt.get("candidate_id") != state["candidate"]["id"]
        or receipt.get("exit_code") != 0
        or receipt.get("test_count") != 5
        or receipt.get("cleanup") != "confirmed"
        or receipt.get("rejected_output") is not True
        or state.get("error") != "repair limit exhausted"
    ):
        raise ValueError("title repair requires its fresh genuine five-case output rejection")
    enriched = _historical_browser_rejection(receipt, spec)
    path = scope.get("actual_target", "").split(":", 1)[0]
    if path not in spec["policy"]["allowed_paths"]:
        raise ValueError("title target is outside the frozen source scope")
    root = Path(spec["checkout"])
    target = root / path
    if target.is_symlink() or not target.resolve().is_relative_to(root.resolve()):
        raise ValueError("title source identity changed")
    info = target.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
        raise ValueError("title source is not an owned regular file")
    raw = target.read_bytes()
    if len(raw) > 256 * 1024 or _git(root, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("title repair requires a bounded clean published source")
    text = raw.decode("utf-8")
    pattern = spec["policy"]["browser_qa"]["reject_regex"]
    matches = [m for m in _TITLE.finditer(text) if re.search(pattern, m["title"])]
    if len(list(_TITLE.finditer(text))) != 5 or len(matches) != 1:
        raise ValueError("title repair requires exactly one rejected title among all five cases")
    match = matches[0]
    if not any(match["title"] in cause["context"] for cause in enriched["rejection_causes"]):
        raise ValueError("fresh rejection does not identify this exact test title")
    return {
        "authority_path": supplied["authority_path"],
        "authority_sha256": supplied["authority_sha256"],
        "authority": authority,
        "path": path,
        "original": text,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "start": match.start("title"),
        "end": match.end("title"),
        "title": match["title"],
        "pattern": pattern,
        "candidate": state["candidate"],
        "browser_receipt_sha256": digest(receipt),
        "new_implementation_turns": 1,
        "maximum_iteration": 5,
    }


def validate_source(spec, constraint, *, completed):
    """Run immediately before and after the one provider turn; never undo its edits."""
    authority = _reference(constraint["authority_path"], constraint["authority_sha256"])
    if canonical_json(authority) != canonical_json(constraint["authority"]):
        raise ValueError("title repair authority changed")
    root = Path(spec["checkout"])
    target = root / constraint["path"]
    if (
        target.is_symlink()
        or not target.resolve().is_relative_to(root.resolve())
        or _git(root, "rev-parse", "HEAD") != constraint["candidate"]["head"]
        or _git(root, "branch", "--show-current") != spec["branch"]
    ):
        raise ValueError("title repair changed source or Git identity")
    info = target.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
        raise ValueError("title source is not an owned regular file")
    raw = target.read_bytes()
    before = constraint["original"]
    if hashlib.sha256(before.encode()).hexdigest() != constraint["sha256"]:
        raise ValueError("title repair frozen source changed")
    if not completed:
        if raw != before.encode() or _git(root, "status", "--porcelain", "--untracked-files=all"):
            raise ValueError("title repair predecessor is no longer clean and exact")
        return
    text = raw.decode("utf-8")
    prefix, suffix = before[: constraint["start"]], before[constraint["end"] :]
    if not text.startswith(prefix) or not text.endswith(suffix):
        raise ValueError("title repair changed assertions or surrounding source")
    title = text[len(prefix) : len(text) - len(suffix)]
    if (
        not title
        or title == constraint["title"]
        or len(title) > 256
        or any(c in title for c in "\"'\\\r\n")
        or re.search(constraint["pattern"], title)
        or set(_git(root, "diff", "--name-only", "HEAD").splitlines()) != {constraint["path"]}
        or _git(root, "ls-files", "--others", "--exclude-standard")
    ):
        raise ValueError("title repair exceeded its single literal-only correction")
