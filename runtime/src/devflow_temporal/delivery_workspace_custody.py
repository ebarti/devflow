"""Independent Git custody for deliveries admitted after workspace ownership v1.

The admitted source path remains authorization provenance. It is not the common
Git directory of a delivery worktree: that directory belongs to the run's durable
state, so removing a setup checkout cannot remove an unfinished delivery's Git
objects. Historical specs retain their original execution path.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import secrets
import shutil
import sqlite3
import stat
import subprocess
import tempfile
from contextlib import closing, contextmanager
from contextvars import ContextVar
from pathlib import Path
from urllib.parse import urlsplit

from .contracts import RUN_ID_RE, digest

_BINDING_FIELDS = (
    "run_id",
    "work_id",
    "issue_url",
    "repository_key",
    "source_path",
    "origin_url",
    "github_repo",
    "base_sha",
    "base_ref",
    "branch",
    "state_dir",
    "checkout",
)
_ADMISSION_STATE = ContextVar("workspace_admission_state", default=None)
_ADMISSION_FD = ContextVar("workspace_admission_fd", default=None)
_ADMISSION_JOURNAL = ContextVar("workspace_admission_journal", default=None)


class WorkspaceAdmissionUnknown(ValueError):
    """A staging Git invocation may still own its provisional allocation."""

    code = "workspace_admission_git_custody_unknown"


def _git(source: Path, *args: str) -> str:
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(
        GIT_NO_REPLACE_OBJECTS="1",
        GIT_GRAFT_FILE=os.devnull,
        GIT_TERMINAL_PROMPT="0",
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull,
    )
    journal = _ADMISSION_JOURNAL.get()
    if journal is not None:
        from .delivery_resources import write_private

        journal[1]["phase"] = "git_active"
        write_private(journal[0], journal[1])
    result = subprocess.run(
        [
            "git",
            "--no-replace-objects",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-C",
            str(source),
            *args,
        ],
        env=environment,
        capture_output=True,
        text=True,
        timeout=120,
        pass_fds=(() if _ADMISSION_FD.get() is None else (_ADMISSION_FD.get(),)),
    )
    if journal is not None and (result.returncode == 0 or (
            result.returncode == 1 and args[:2] == ("config", "--get"))):
        # Successful Git and the expected missing-config result are reaped
        # normally. Other failures retain unknown helper custody.
        journal[1]["phase"] = "preparing"
        write_private(journal[0], journal[1])
    elif journal is not None:
        raise WorkspaceAdmissionUnknown(
            "provisional Git staging failed without confirmed helper closure; retain its "
            "journal and allocation for reconciliation"
        )
    if result.returncode:
        raise ValueError(
            "durable Git custody command failed: "
            + (result.stderr.strip() or result.stdout.strip())[:500]
        )
    return result.stdout.strip()


def _binding(spec: dict) -> dict:
    return {
        **{key: spec.get(key) for key in _BINDING_FIELDS},
        **({"publication_base_ref": spec["publication_base_ref"]}
           if "publication_base_ref" in spec else {}),
    }


def owner_path(spec: dict) -> Path:
    return Path(spec["state_dir"]) / "workspace-owner.git"


def require_durable_locations(config, logical_source: Path) -> None:
    """Run state and tracking authority cannot inherit a managed worktree lifetime."""
    source = logical_source.resolve()
    state, tracking = config.state_root.resolve(), config.tracking_db.resolve()
    if (
        state == source
        or state.is_relative_to(source)
        or source.is_relative_to(state)
        or tracking == source
        or tracking.is_relative_to(source)
    ):
        raise ValueError(
            "durable delivery state and tracking database must be disjoint from source"
        )
    for allocated in (state, tracking.parent):
        parent = allocated
        while not parent.exists():
            parent = parent.parent
        if not parent.is_dir():
            raise ValueError("durable delivery state has a non-directory ancestor")
        try:
            top = _git(parent, "rev-parse", "--show-toplevel")
        except ValueError as exc:
            if "not a git repository" in str(exc).lower():
                continue
            raise ValueError("durable delivery state location could not be verified") from exc
        common = Path(_git(parent, "rev-parse", "--path-format=absolute", "--git-common-dir"))
        if common.resolve() != (Path(top) / ".git").resolve():
            raise ValueError(
                "durable delivery state cannot live inside a disposable linked worktree"
            )


def _identity(path: Path) -> dict:
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("durable Git owner is not a private owned directory")
    return {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid}


def _validate(spec: dict, receipt: dict, owner: Path) -> None:
    if (
        receipt.get("version") != 1
        or receipt.get("binding") != _binding(spec)
        or receipt.get("binding_sha256") != digest(_binding(spec))
    ):
        raise ValueError("durable Git owner receipt changed its admitted source binding")
    if not owner.exists():
        raise ValueError("durable Git owner is absent; restore its authenticated checkpoint")
    if owner.resolve(strict=True) != owner or _identity(owner) != receipt.get("identity"):
        raise ValueError("durable Git owner identity changed")
    common = _git(owner, "rev-parse", "--path-format=absolute", "--git-common-dir")
    if common != str(owner) or _git(owner, "rev-parse", "--is-bare-repository") != "true":
        raise ValueError("durable Git owner does not own its common Git directory")
    if (
        os.path.lexists(owner / "objects/info/alternates")
        or os.path.lexists(owner / "objects/info/http-alternates")
        or os.path.lexists(owner / "info/grafts")
        or _git(owner, "for-each-ref", "--format=%(refname)", "refs/replace/")
    ):
        raise ValueError("durable Git owner has external or replaced Git objects")
    if (
        _git(owner, "cat-file", "-t", spec["base_sha"]) != "commit"
        or _git(owner, "rev-parse", "refs/devflow/base") != spec["base_sha"]
        or _git(owner, "remote", "get-url", "origin") != spec["origin_url"]
    ):
        raise ValueError("durable Git owner lost its admitted base or remote identity")
    publication = spec.get("publication_base_ref")
    if publication is not None:
        reference = "refs/remotes/origin/" + publication
        if (
            _git(owner, "symbolic-ref", "refs/remotes/origin/HEAD") != reference
            or _git(owner, "for-each-ref", "--format=%(symref)", reference)
        ):
            raise ValueError("durable Git owner changed its admitted publication branch")
        try:
            # Explicit fresh-base observation may advance this operational alias.
            # The immutable admitted commit remains refs/devflow/base; changing
            # the alias's ancestry or symbolic branch identity grants no custody.
            _git(owner, "merge-base", "--is-ancestor", spec["base_sha"], reference)
        except ValueError as exc:
            raise ValueError("durable Git owner lost its admitted publication ancestry") from exc
    # Fetch copies object bytes; it must never retain a hard-linked object or a
    # symlink back to the source repository. Check again on every resolver call.
    for base, directories, files in os.walk(owner / "objects", followlinks=False):
        for name in [*directories, *files]:
            path = Path(base) / name
            info = path.lstat()
            if (
                stat.S_ISLNK(info.st_mode)
                or info.st_uid != os.getuid()
                or (stat.S_ISREG(info.st_mode) and info.st_nlink != 1)
            ):
                raise ValueError("durable Git object custody is not independent")


def _sync_tree(root: Path) -> None:
    for base, _directories, files in os.walk(root, topdown=False, followlinks=False):
        for name in files:
            path = Path(base) / name
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        descriptor = os.open(base, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _import_source(spec: dict) -> Path:
    """Import only the authenticated coordinator/predecessor's independent owner."""
    worker = spec.get("feature_worker")
    predecessor = spec.get("feature_predecessor") or {}
    parent_id = (
        worker.get("parent_run_id")
        if worker
        else predecessor.get("run_id") or spec.get("supersedes_run_id")
    )
    if not parent_id:
        return Path(spec["source_path"])
    from .delivery_resources import read_private

    if not isinstance(parent_id, str) or not RUN_ID_RE.fullmatch(parent_id):
        raise ValueError("durable Git custody has an invalid coordinator lineage")
    state = Path(spec["state_dir"]).parent / parent_id
    receipt_path = state / "workspace-owner.json"
    if not os.path.lexists(receipt_path):
        # A new admission may continue an authenticated pre-custody run. Only
        # that run's immutable admission can establish legacy source ownership;
        # missing receipts for new owners never fall back to the setup path.
        from .delivery_execution_config import load_execution_config

        config = load_execution_config(spec)
        with closing(sqlite3.connect(config.tracking_db.as_uri() + "?mode=ro", uri=True)) as db:
            row = db.execute(
                "SELECT request_json FROM delivery_runs WHERE run_id=?", (parent_id,)
            ).fetchone()
        original = json.loads(row[0]) if row else {}
        if (
            not original
            or original.get("workspace_custody_version") == 1
            or original.get("state_dir") != str(state)
            or any(
                original.get(key) != spec.get(key)
                for key in ("source_path", "origin_url", "github_repo", "repository_key")
            )
            or (
                not worker
                and any(original.get(key) != spec.get(key) for key in ("work_id", "issue_url"))
            )
        ):
            raise ValueError("durable Git custody predecessor lost its acknowledged owner")
        return Path(original["source_path"])
    parent = read_private(receipt_path)
    binding = parent.get("binding", {})
    if (
        binding.get("run_id") != parent_id
        or binding.get("state_dir") != str(state)
        or any(
            binding.get(key) != spec.get(key)
            for key in ("source_path", "origin_url", "github_repo", "repository_key")
        )
        or (
            not worker
            and any(binding.get(key) != spec.get(key) for key in ("work_id", "issue_url"))
        )
    ):
        raise ValueError("durable Git custody coordinator changed its admitted repository")
    owner = state / "workspace-owner.git"
    _validate(binding, parent, owner)
    return owner


def ensure_owner(spec: dict, *, _storage_state: Path | None = None) -> Path:
    """Publish or authenticate a run-owned independent Git object database.

    Only the admitted base's ancestry and publication name/email are copied.
    Hooks, credential configuration, working files and unrelated refs are not.
    A partial publication is never treated as a usable owner or silently reset.
    """
    if spec.get("workspace_custody_version") != 1:
        if not isinstance(spec.get("source_path"), str) or not spec["source_path"]:
            raise ValueError("durable Git custody requires an admitted source path")
        return Path(spec["source_path"])
    from .delivery_resources import _ancestors, private_directory, read_private, write_private

    state = _storage_state or Path(spec["state_dir"])
    if not state.is_absolute() or state.resolve() != state:
        raise ValueError("durable Git custody requires canonical run state")
    if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", spec.get("base_sha", "")):
        raise ValueError("durable Git custody requires an admitted base commit")
    origin = urlsplit(spec.get("origin_url", ""))
    if origin.password or (origin.scheme in {"https", "http"} and origin.username) or origin.query:
        raise ValueError("durable Git custody refuses credential-bearing origin URLs")
    private_directory(state)
    _ancestors(state)
    owner = state / "workspace-owner.git"
    receipt_path = state / "workspace-owner.json"
    descriptor = os.open(
        state / "workspace-owner.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600
    )
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
        ):
            raise ValueError("durable Git custody lock is not private")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        if receipt_path.exists() or receipt_path.is_symlink():
            _validate(spec, read_private(receipt_path), owner)
            return owner
        if owner.exists() or owner.is_symlink():
            raise ValueError(
                "durable Git owner publication is incomplete; checkpoint reconciliation required"
            )
        source = _import_source(spec)
        if (
            source.resolve(strict=True) != source
            or (
                source == Path(spec["source_path"])
                and _git(source, "rev-parse", "--show-toplevel") != str(source)
            )
            or _git(source, "remote", "get-url", "origin") != spec["origin_url"]
            or _git(source, "cat-file", "-t", spec["base_sha"]) != "commit"
        ):
            raise ValueError("durable Git custody source differs from its admission")
        temporary = Path(tempfile.mkdtemp(prefix=".workspace-owner-", dir=state))
        try:
            _git(temporary, "init", "--bare", "-q")
            _git(
                temporary,
                "fetch",
                "--no-tags",
                "--no-write-fetch-head",
                str(source),
                spec["base_sha"] + ":refs/devflow/base",
            )
            _git(temporary, "remote", "add", "origin", spec["origin_url"])
            publication = spec.get("publication_base_ref")
            if publication is not None:
                _git(temporary, "check-ref-format", "refs/heads/" + publication)
                publication_ref = "refs/remotes/origin/" + publication
                _git(temporary, "update-ref", publication_ref, spec["base_sha"])
                _git(temporary, "symbolic-ref", "refs/remotes/origin/HEAD", publication_ref)
            reference = _git(source, "rev-parse", "--symbolic-full-name", spec["base_ref"])
            if reference.startswith(("refs/heads/", "refs/remotes/")):
                _git(temporary, "update-ref", reference, spec["base_sha"])
            head = (
                reference
                if reference.startswith("refs/heads/")
                else "refs/heads/devflow-custody-base"
            )
            _git(temporary, "update-ref", head, spec["base_sha"])
            _git(temporary, "symbolic-ref", "HEAD", head)
            for key in ("user.name", "user.email"):
                # This is publication identity, not the repository's arbitrary
                # local configuration (which can contain helpers or secrets).
                try:
                    value = _git(source, "config", "--get", key)
                except ValueError:
                    continue
                if value:
                    _git(temporary, "config", key, value)
            os.chmod(temporary, 0o700)
            _sync_tree(temporary)
            os.rename(temporary, owner)
            from .delivery_writer_custody import initialize_witness

            writer_custody = initialize_witness(state)
            write_private(
                receipt_path,
                {
                    "version": 1,
                    "binding": _binding(spec),
                    "binding_sha256": digest(_binding(spec)),
                    "identity": _identity(owner),
                    "writer_custody": writer_custody,
                },
            )
            _validate(spec, read_private(receipt_path), owner)
            return owner
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)
    finally:
        os.close(descriptor)


def _allocation_identity(path: Path) -> dict:
    info = path.lstat()
    if info.st_uid != os.getuid() or stat.S_ISLNK(info.st_mode):
        raise ValueError("provisional Git owner allocation changed")
    return {"device": info.st_dev, "inode": info.st_ino, "mode": info.st_mode}


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _reconcile_provisional(state: Path, marker: dict, *, admitted: bool) -> None:
    """Remove only the exact journaled allocation, or retain its admitted owner."""
    from .delivery_resources import read_private

    names = ("workspace-owner.git", "workspace-writers.lock", "workspace-owner.json")
    stage_name = marker.get("stage")
    if (marker.get("schema") != "devflow-workspace-admission-v1"
            or not isinstance(stage_name, str)
            or not stage_name.startswith(".workspace-admission-")
            or Path(stage_name).name != stage_name
            or marker.get("phase") not in {"preparing", "git_active", "ready"}):
        raise ValueError("provisional Git owner journal is invalid")
    stage = state / stage_name
    if marker["phase"] == "git_active":
        raise WorkspaceAdmissionUnknown(
            "provisional Git staging custody is unknown; retain its journal and allocation "
            "until the original Git invocation and helpers are reconciled"
        )
    if marker["phase"] == "preparing":
        if admitted or any(os.path.lexists(state / name) for name in names):
            raise ValueError("unpublished Git owner has unexpected admission or allocation")
        if os.path.lexists(stage):
            identity = _allocation_identity(stage)
            if (not stat.S_ISDIR(identity["mode"])
                    or stat.S_IMODE(identity["mode"]) != 0o700):
                raise ValueError("provisional Git owner staging is not private")
            if marker.get("stage_identity") is None:
                # Only an empty mkdir can precede its durable identity record.
                if os.listdir(stage):
                    raise ValueError("unidentified provisional Git owner is not empty")
                stage.rmdir()
            else:
                if identity != marker["stage_identity"]:
                    raise ValueError("provisional Git owner staging identity changed")
                allowed = {"workspace-owner.git", "workspace-owner.json",
                           "workspace-owner.lock", "workspace-writers.lock"}
                for entry in stage.iterdir():
                    if (entry.name not in allowed
                            and not entry.name.startswith((".workspace-owner-",
                                                          "workspace-owner.json."))):
                        raise ValueError("provisional Git owner staging has unrecognized contents")
                for base, dirs, files in os.walk(stage, followlinks=False):
                    for name in (*dirs, *files):
                        info = (Path(base) / name).lstat()
                        if (stat.S_ISLNK(info.st_mode) or info.st_uid != os.getuid()
                                or (stat.S_ISREG(info.st_mode) and info.st_nlink != 1)):
                            raise ValueError("provisional Git owner staging was replaced")
                shutil.rmtree(stage)
        (state / "workspace-admission.json").unlink()
        _sync_directory(state)
        return
    if set(marker.get("objects", {})) != set(names) or marker.get("stage_identity") is None:
        raise ValueError("provisional Git owner journal is invalid")
    if os.path.lexists(stage):
        if _allocation_identity(stage) != marker.get("stage_identity"):
            raise ValueError("provisional Git owner staging identity changed")
        if set(os.listdir(stage)) - {*names, "workspace-owner.lock"}:
            raise ValueError("provisional Git owner staging has unrecognized contents")
    for name in names:
        target, staged = state / name, stage / name
        if os.path.lexists(target) and os.path.lexists(staged):
            raise ValueError("provisional Git owner has duplicate allocation identities")
        for path in (target, staged):
            if os.path.lexists(path) and _allocation_identity(path) != marker["objects"][name]:
                raise ValueError("provisional Git owner allocation identity changed")
        if admitted and not os.path.lexists(target):
            raise ValueError("admitted Git owner publication is incomplete")
    if admitted:
        receipt = read_private(state / "workspace-owner.json")
        if receipt.get("binding") != marker.get("binding"):
            raise ValueError("admitted Git owner differs from provisional journal")
        _validate(marker["binding"], receipt, state / "workspace-owner.git")
    else:
        # A writer witness must be idle even if a DB row is unexpectedly absent.
        witness = state / "workspace-writers.lock"
        if witness.exists():
            fd = os.open(witness, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(fd)
        for name in names:
            target = state / name
            if os.path.lexists(target):
                if name == "workspace-owner.git":
                    shutil.rmtree(target)
                else:
                    target.unlink()
    if os.path.lexists(stage):
        shutil.rmtree(stage)
    (state / "workspace-admission.json").unlink()
    _sync_directory(state)


@contextmanager
def provisional_owner(store, spec: dict):
    """Serialize admission and reconcile a crash without adopting foreign custody."""
    if spec.get("workspace_custody_version") != 1:
        yield
        return
    from .delivery_resources import _ancestors, private_directory, read_private, write_private

    state = Path(spec["state_dir"])
    if not state.is_absolute() or state.resolve() != state or state.name != spec["run_id"]:
        raise ValueError("provisional Git owner requires canonical run state")
    private_directory(state)
    _ancestors(state)
    lock = state / "workspace-owner.lock"
    fd = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
            raise ValueError("durable Git custody lock is not private")
        fcntl.flock(fd, fcntl.LOCK_EX)
        current = lock.lstat()
        if (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino):
            raise ValueError("durable Git custody lock changed")
        marker_path = state / "workspace-admission.json"

        def admitted():
            with closing(sqlite3.connect(store.config.tracking_db.as_uri() + "?mode=ro",
                                         uri=True)) as db:
                return db.execute("SELECT 1 FROM delivery_runs WHERE run_id=?",
                                  (spec["run_id"],)).fetchone() is not None

        if os.path.lexists(marker_path):
            marker = read_private(marker_path)
            if (marker.get("binding", {}).get("run_id") != spec["run_id"]
                    or marker.get("binding", {}).get("state_dir") != str(state)):
                raise ValueError("provisional Git owner journal belongs to another run")
            _reconcile_provisional(state, marker, admitted=admitted())
        if admitted():
            _validate(spec, read_private(state / "workspace-owner.json"), owner_path(spec))
            token = _ADMISSION_STATE.set(state)
            try:
                yield
            finally:
                _ADMISSION_STATE.reset(token)
            return
        if any(os.path.lexists(state / name) for name in (
                "workspace-owner.git", "workspace-owner.json", "workspace-writers.lock")):
            raise ValueError("unjournaled Git owner cannot be adopted by admission")
        stage = state / (".workspace-admission-" + secrets.token_hex(16))
        marker = {"schema": "devflow-workspace-admission-v1", "binding": _binding(spec),
                  "phase": "preparing", "stage": stage.name, "stage_identity": None,
                  "objects": {}}
        try:
            # Journal the allocation name before mkdir and its identity before
            # populating it; both crash windows are reconciled under this lock.
            write_private(marker_path, marker)
            os.mkdir(stage, 0o700)
            _sync_directory(state)
            marker["stage_identity"] = _allocation_identity(stage)
            write_private(marker_path, marker)
            token = _ADMISSION_FD.set(fd)
            journal_token = _ADMISSION_JOURNAL.set((marker_path, marker))
            try:
                ensure_owner(spec, _storage_state=stage)
            finally:
                _ADMISSION_JOURNAL.reset(journal_token)
                _ADMISSION_FD.reset(token)
            names = ("workspace-owner.git", "workspace-writers.lock", "workspace-owner.json")
            marker["objects"] = {name: _allocation_identity(stage / name) for name in names}
            marker["phase"] = "ready"
            write_private(marker_path, marker)
            for name in names:
                os.rename(stage / name, state / name)
                _sync_directory(state)
            token = _ADMISSION_STATE.set(state)
            try:
                yield
            finally:
                _ADMISSION_STATE.reset(token)
        finally:
            if os.path.lexists(marker_path):
                _reconcile_provisional(state, read_private(marker_path), admitted=admitted())
            elif os.path.lexists(stage):
                shutil.rmtree(stage)
    finally:
        os.close(fd)


def source_for(spec: dict) -> Path:
    """Observe an existing owner without creating or repairing custody."""
    if spec.get("workspace_custody_version") != 1:
        if not isinstance(spec.get("source_path"), str) or not spec["source_path"]:
            raise ValueError("durable Git custody requires an admitted source path")
        return Path(spec["source_path"])
    from .delivery_resources import _ancestors, read_private

    state = Path(spec["state_dir"])
    _ancestors(state)
    if _ADMISSION_STATE.get() == state:
        owner = owner_path(spec)
        _validate(spec, read_private(state / "workspace-owner.json"), owner)
        return owner
    lock = state / "workspace-owner.lock"
    try:
        descriptor = os.open(lock, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError as exc:
        raise ValueError("durable Git custody observation lock is unavailable") from exc
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
            raise ValueError("durable Git custody lock is not private")
        fcntl.flock(descriptor, fcntl.LOCK_SH)
        current = lock.lstat()
        if (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino):
            raise ValueError("durable Git custody observation lock changed")
        owner = owner_path(spec)
        _validate(spec, read_private(state / "workspace-owner.json"), owner)
        return owner
    finally:
        os.close(descriptor)
