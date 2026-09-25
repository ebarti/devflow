"""Content identity and isolated snapshots for small disposable Git repositories."""

from __future__ import annotations

import hashlib
import os
import shutil
import stat
import subprocess
from pathlib import Path
from typing import Any

from .contracts import RUN_ID_RE

SKIP_NAMES = frozenset(
    {".git", ".venv", "node_modules", "__pycache__", ".pytest_cache", ".ruff_cache"}
)


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=True
    )
    return completed.stdout.strip()


def validate_paths(repo: Path, state_dir: Path, *, require_clean: bool) -> None:
    repo = repo.resolve(strict=True)
    state_dir = state_dir.resolve()
    if not repo.is_dir() or _git(repo, "rev-parse", "--show-toplevel") != str(repo):
        raise ValueError("repository must be a Git working-copy root")
    if state_dir == repo or repo in state_dir.parents or state_dir in repo.parents:
        raise ValueError("state directory and repository must be disjoint")
    if require_clean and _git(repo, "status", "--porcelain=v1", "--untracked-files=all"):
        raise ValueError("repository must start clean; use a disposable working copy")


def _files(root: Path) -> list[Path]:
    paths: list[Path] = []
    for base, dirs, files in os.walk(root, followlinks=False):
        dirs[:] = sorted(name for name in dirs if name not in SKIP_NAMES)
        for name in sorted(files):
            if name in SKIP_NAMES:
                continue
            path = Path(base, name)
            mode = path.lstat().st_mode
            if not stat.S_ISREG(mode):
                raise ValueError(f"candidate contains a symlink or special file: {path}")
            paths.append(path)
    return sorted(paths)


def content_hash(root: Path) -> str:
    hasher = hashlib.sha256()
    for path in _files(root):
        relative = path.relative_to(root).as_posix()
        hasher.update(relative.encode("utf-8") + b"\0")
        hasher.update(b"x" if path.stat().st_mode & stat.S_IXUSR else b"-")
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                hasher.update(chunk)
        hasher.update(b"\0")
    return hasher.hexdigest()


def candidate_for(repo: Path, *, head: str | None = None) -> dict[str, str]:
    head = head or _git(repo, "rev-parse", "HEAD")
    content = content_hash(repo)
    return {
        "head": head,
        "content_sha256": content,
        "id": hashlib.sha256(f"{head}:{content}".encode()).hexdigest(),
    }


def assert_candidate(repo: Path, candidate: dict[str, Any], *, git_head: bool = True) -> None:
    current = candidate_for(repo, head=None if git_head else str(candidate["head"]))
    if current["id"] != candidate["id"]:
        raise ValueError("candidate content changed after its identity was recorded")


def snapshot(repo: Path, state_dir: Path, run_id: str, candidate: dict[str, Any]) -> Path:
    if not RUN_ID_RE.fullmatch(run_id):
        raise ValueError("invalid run ID")
    destination = state_dir / "runs" / run_id / "candidate"
    if destination.exists():
        raise ValueError("candidate snapshot already exists; recovery requires inspection")
    destination.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    shutil.copytree(repo, destination, ignore=shutil.ignore_patterns(*SKIP_NAMES))
    assert_candidate(destination, candidate, git_head=False)
    return destination
