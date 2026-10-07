"""Validate literal-only repairs already admitted by earlier versions."""

from __future__ import annotations

import hashlib
import os
import re
import stat
from pathlib import Path

from .contracts import canonical_json
from .delivery_broker import _git
from .delivery_gates_admission import _reference


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
