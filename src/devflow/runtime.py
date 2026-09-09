"""Select an immutable release from the active attempt or repository lock."""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from urllib.parse import quote

import devflow
from devflow.errors import WorkflowError
from devflow.installation import installed_release


def package_root() -> Path:
    return Path(devflow.__file__).resolve().parents[2]


def active_revision(state_dir: Path, work_id: str | None) -> str | None:
    path = state_dir / "state.sqlite3"
    if not work_id or not path.exists():
        return None
    if path.is_symlink():
        raise WorkflowError("unsafe_state", "State database cannot be a symlink")
    try:
        with sqlite3.connect(f"file:{quote(str(path.absolute()))}?mode=ro", uri=True) as db:
            row = db.execute("SELECT state FROM works WHERE work_id=?", (work_id,)).fetchone()
    except sqlite3.DatabaseError as exc:
        raise WorkflowError(
            "state_unreadable", "Cannot resolve the active attempt's workflow version"
        ) from exc
    if not row:
        return None
    state = json.loads(row[0])
    if state["lifecycle"] != "active":
        return None
    attempt = state["attempt"]
    snapshot = state["records"]["workflow_snapshot:" + attempt["workflow_snapshot_id"]]
    revision = snapshot.get("package_revision")
    if not revision:
        raise WorkflowError(
            "unrecorded_release", "Active attempt has no captured full workflow revision"
        )
    return revision


def runtime_argv(release_root: Path, revision: str) -> list[str]:
    if not re.fullmatch(r"[a-f0-9]{40}", revision):
        raise WorkflowError("workflow_unpinned", "Expected a full workflow Git revision")
    root = release_root.expanduser().absolute()
    release = root / "releases" / revision
    metadata = installed_release(release)
    if metadata["revision"] != revision:
        raise WorkflowError("release_mismatch", "Installed release differs from the requested pin")
    return [
        "env",
        "PYTHONDONTWRITEBYTECODE=1",
        f"UV_PROJECT_ENVIRONMENT={root / 'environments' / revision}",
        "uv",
        "run",
        "--frozen",
        "--project",
        str(release),
        "devflow",
    ]


def selected_runtime(
    profile, *, state_dir: Path, work_id: str | None, release_root: Path
) -> list[str] | None:
    revision = active_revision(state_dir, work_id) or profile.lock["revision"]
    root = package_root()
    if (root / ".devflow-release.json").exists():
        current = installed_release(root)
        if current["revision"] == revision:
            return None
    return runtime_argv(release_root, revision)
