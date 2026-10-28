"""Retained-workspace verification; never replace source or activate execution.

The manifest covers content and executable bits for source, plus exact mode and
mtime for retained state evidence. It does not cover complete source modes or
source mtimes, so this comparator makes no stronger claim.
"""

from __future__ import annotations

import json
import os
import stat
import threading
from contextlib import ExitStack
from pathlib import Path

from devflow_temporal import delivery_checkpoints as checkpoints
from devflow_temporal.candidate import SKIP_NAMES, _files, candidate_for
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_checkpoint_index import observe_index
from devflow_temporal.delivery_checkpoints import (
    _git,
    _hash,
    _path,
    _source_path_allowed,
)
from devflow_temporal.delivery_filesystem_custody import matches
from devflow_temporal.delivery_resources import RunResources, _identity, read_private
from devflow_temporal.delivery_retained_tracking import (
    RetainedSQLPolicy,
    RetainedTrackingObservation,
)
from devflow_temporal.delivery_store import _now
from devflow_temporal.delivery_workspace_custody import source_for
from devflow_temporal.delivery_writer_custody import CheckpointWriters

REVALIDATED = "delivery_checkpoint_retained_verified"


class RetainedCheckpointCustody:
    """An exact live process/thread owner; receipts never grant activation."""

    def __init__(self, store, run_id, checkpoint_id, expected_revision):
        if type(expected_revision) is not int or expected_revision < 0:
            raise ValueError("retained checkpoint revision must be a nonnegative integer")
        self.store, self.run_id = store, run_id
        self.checkpoint_id, self.expected_revision = checkpoint_id, expected_revision
        self._stack, self._owner = None, None
        self._committed = False

    def __enter__(self):
        if self._stack is not None:
            raise ValueError("retained checkpoint custody cannot be reopened")
        stack = self._stack = ExitStack()
        self._owner = (os.getpid(), threading.get_ident())
        try:
            self.spec = self.store.effective_spec(self.run_id)
            if self.spec.get("feature_delivery"):
                raise ValueError("retained feature custody requires a prepared projection owner")
            if not self.store.owns_execution(self.spec):
                raise ValueError("retained checkpoint service does not own this execution")
            stack.enter_context(checkpoints._locked(self.spec))
            self.registry = RunResources(self.spec, read_only=True)
            self.resources = stack.enter_context(self.registry.locked(
                read_only=True, exclusive=True))
            with self.store._connect() as db:
                self.value, self.checkpoint = checkpoints._read_acknowledged(
                    db, self.spec, self.checkpoint_id, verify_artifacts=False)
            checkpoints._require_supported_native_custody(self.value.get("closed_journals"))
            if not self.checkpoint["resumable"] or self.value.get("resume_blockers"):
                raise ValueError("retained checkpoint requires reconciled effects and a fresh seal")
            if (self.value.get("control") is not None
                    or self.value.get("checkpoint_context_sha256") is not None):
                raise ValueError("retained pause/control verification is unsupported")
            if digest(self.spec) != self.value["spec_sha256"]:
                raise ValueError("retained checkpoint execution authority changed")
            self.policy = RetainedSQLPolicy()
            self.db = stack.enter_context(self.store._connect(write_authorizer=self.policy))
            self.observation = RetainedTrackingObservation(self.store, self.run_id)
            stack.callback(self.observation.close)
            self.frozen = self.observation.frozen
            checkpoints._verify_effects(self.frozen)
            if (not checkpoints._same_snapshot(self.frozen, self.value)
                    or self.observation.claim != self.value["owner"]):
                raise ValueError("retained checkpoint state, effects, owner or budget changed")
            self.journals = checkpoints._closed(
                self.registry, self.resources, self.frozen["delivery_attempts"], stack)
            writer = self.store._checkpoint_writer_context(self.spec)
            if (type(writer) is not CheckpointWriters or writer.store is not self.store
                    or digest(writer.spec) != digest(self.spec) or writer.stack is not None):
                raise ValueError("retained checkpoint requires its concrete writer owner")
            self.writer = stack.enter_context(writer)
            self.writer.verify(self.db)
            controller = checkpoints._controller_binding(
                self.store, self.spec, self.expected_revision, None)
            if not checkpoints._same_controller(controller, self.value.get("controller")):
                raise ValueError("retained checkpoint controller lineage changed")
            self.observation._unchanged()
            return self
        except BaseException:
            self._owner = None
            stack.close()
            raise

    def _live(self):
        if (type(self) is not RetainedCheckpointCustody
                or self._owner != (os.getpid(), threading.get_ident())):
            raise ValueError("retained checkpoint requires its exact live custody owner")
        self.writer.lease.check()

    def _verify_physical(self):
        self._live()
        with self.store._connect() as db:
            value, receipt = checkpoints._read_acknowledged(db, self.spec, self.checkpoint_id)
        if value != self.value or receipt != self.checkpoint:
            raise ValueError("retained checkpoint acknowledged bytes or command changed")
        self.root = checkpoints._root(self.spec) / self.checkpoint_id
        if _hash(read_private(self.registry.manifest, raw=True)) != self.value[
                "resource_manifest_sha256"]:
            raise ValueError("retained checkpoint resource manifest changed")
        checkpoints._openers(Path(self.spec["checkout"]))
        self.physical = verify_retained_source(self.spec, self.value, self.root, self.resources)
        verify_retained_state(self.spec, self.value, self.root)
        inventory = {}
        selected = checkpoints._state_artifacts(
            self.spec, self.frozen, None, inventory, self.journals,
            self.value.get("attempt_settlements"))
        if selected != self.value["state_refs"]:
            raise ValueError("retained checkpoint selected evidence inventory changed")
        self.writer.verify(self.db)
        self.observation._unchanged()

    def _verify_token(self):
        self._live()
        self.writer.verify_token(self.db)
        self.observation.verify_token(self.db)
        row = self.observation.row
        if row["revision"] != self.expected_revision:
            raise ValueError("retained checkpoint run revision changed")
        checkpoints._stopped(self.db, row, None)
        return row

    def acknowledge(self, command_id):
        """Verify once under held leases and record a source-only receipt."""
        self._live()
        if self._committed:
            raise ValueError("retained ACK replay requires a fresh custody scope")
        if not isinstance(command_id, str) or not command_id or len(command_id) > 200:
            raise ValueError("retained checkpoint command is invalid")
        self._verify_physical()
        request = {"kind": REVALIDATED, "run_id": self.run_id,
                   "checkpoint_id": self.checkpoint_id, "command_id": command_id,
                   "expected_revision": self.expected_revision}
        key, request_sha256 = "checkpoint-retained:" + command_id, digest(request)
        response = {**request, "manifest_sha256": self.checkpoint["manifest_sha256"],
                    "physical": self.physical,
                    "retained_verified": True, "copied": False, "execution_started": False}
        response_json = canonical_json(response)
        saved = self.observation.saved_command(key, self.run_id, request_sha256)
        if saved is not None and saved != response:
            raise ValueError("retained checkpoint replay changed its recorded receipt")
        try:
            self.db.execute("BEGIN IMMEDIATE")
            with self.policy.allow("read"):
                row = self._verify_token()
            if saved is None:
                with self.policy.allow("record"):
                    self.db.execute("INSERT INTO delivery_commands VALUES (?,?,?,?)",
                                    (key, self.run_id, request_sha256, response_json))
                    stamp = _now()
                    event = self.db.execute(
                        "INSERT INTO delivery_events "
                        "(run_id,timestamp,type,message,run_revision,payload_json) "
                        "VALUES (?,?,?,?,?,?)", (self.run_id, stamp, REVALIDATED,
                        "Retained delivery workspace verified", self.expected_revision,
                        response_json))
                    self.db.execute("INSERT INTO delivery_phase_events "
                        "(event_sequence,run_id,workflow_id,phase,execution_state,"
                        "iteration,timestamp) "
                        "VALUES (?,?,?,?,?,?,?)", (event.lastrowid, self.run_id,
                        row["workflow_id"] or "delivery-" + self.run_id, row["phase"],
                        row["execution_state"], row["iteration"], stamp))
            with self.policy.allow("read"):
                self.writer.verify_token(self.db)
                self.observation.verify_token(self.db)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        self._committed = True
        return json.loads(response_json)

    def __exit__(self, *args):
        self._owner = None
        return self._stack.__exit__(*args)


def _query(path, *args):
    if args and args[0] == "diff":
        args = ("diff", "--no-ext-diff", "--no-textconv", *args[1:])
    return _git(path, "--no-optional-locks", *args)


def verify_retained_source(spec, value, checkpoint_root, resources):
    """Read-only comparison under an already-held concrete custody owner."""
    if not value["source"].get("index_semantics"):
        raise ValueError("retained checkpoint requires an index semantics baseline")
    if observe_index(Path(spec["checkout"]), _query) != value["source"]["index_semantics"]:
        raise ValueError("retained checkpoint index semantics changed")
    return _verify_recorded_source(spec, value, checkpoint_root, resources)


def verify_restored_source(spec, value, checkpoint_root, resources, reconstruction=None):
    """Verify v1's recorded contract without inventing its unrecorded flags."""
    if not checkpoints._legacy_index_format(value):
        return verify_retained_source(spec, value, checkpoint_root, resources)
    observed = checkpoints._verify_index_baseline(Path(spec["checkout"]), value)
    if reconstruction is not None and reconstruction != observed:
        raise ValueError("legacy reconstructed checkpoint index changed")
    return _verify_recorded_source(spec, value, checkpoint_root, resources)


def _verify_recorded_source(spec, value, checkpoint_root, resources):
    checkout = Path(spec["checkout"])
    entry = resources["roots"].get(str(checkout))
    if (entry is None or entry["kind"] != "checkout"
            or not matches(spec, checkout, entry["identity"])
            or entry["identity"] != value["checkout_identity"]):
        raise ValueError("retained checkpoint lost its checkout resource generation")
    if any(entry.get("generated_output_preservation") for raw, entry in resources["roots"].items()
           if Path(raw).is_relative_to(checkout)):
        raise ValueError("retained checkpoint generated-source custody requires reconciliation")
    source = source_for(spec)
    if Path(_query(checkout, "rev-parse", "--path-format=absolute", "--git-common-dir")
            .decode().strip()) != source:
        raise ValueError("retained checkpoint lost its independent Git owner")
    branch = value["source"]["branch"]
    head = value["source"]["candidate"]["head"]
    if (_query(checkout, "symbolic-ref", "--short", "HEAD").decode().strip() != branch
            or _query(checkout, "rev-parse", "HEAD").decode().strip() != head
            or _query(source, "rev-parse", "refs/heads/" + branch).decode().strip() != head):
        raise ValueError("retained checkpoint branch or HEAD changed")
    registrations = _query(source, "worktree", "list", "--porcelain", "-z").split(b"\0\0")
    expected_registration = (
        b"worktree " + os.fsencode(checkout) + b"\0HEAD " + head.encode()
        + b"\0branch refs/heads/" + branch.encode() + b"\0")
    if not any(item.rstrip(b"\0") == expected_registration.rstrip(b"\0")
               for item in registrations):
        raise ValueError("retained checkpoint worktree registration changed")
    _query(checkout, "diff", "--cached", "--exit-code", value["source"]["index_tree"], "--")
    for name, args in (
        ("staged.patch", ("diff", "--cached", "--binary", "--full-index", "HEAD")),
        ("unstaged.patch", ("diff", "--binary", "--full-index")),
    ):
        if _query(checkout, *args) != read_private(checkpoint_root / name, raw=True):
            raise ValueError("retained checkpoint staged or unstaged source changed")
    generated = [Path(raw) for raw, entry in resources["roots"].items()
                 if entry["kind"] == "generated" and Path(raw).is_relative_to(checkout)
                 and Path(raw).exists() and matches(spec, Path(raw), entry["identity"])]
    ignored = _query(checkout, "ls-files", "--others", "--ignored", "--exclude-standard", "-z")
    for raw in ignored.split(b"\0"):
        if not raw:
            continue
        relative = Path(os.fsdecode(raw))
        if (not _source_path_allowed(relative.as_posix())
                or any(part in SKIP_NAMES for part in relative.parts)
                or any((checkout / relative).is_relative_to(root) for root in generated)):
            continue
        raise ValueError("retained checkpoint has unclassified ignored source: " + str(relative))
    paths = {path.relative_to(checkout).as_posix(): path for path in _files(checkout)}
    if set(paths) != set(value["source"]["files"]):
        raise ValueError("retained checkpoint selected source path set changed")
    for relative, item in value["source"]["files"].items():
        path = paths[relative]
        info = path.lstat()
        if item["kind"] == "symlink":
            if not path.is_symlink() or os.readlink(path) != item["target"]:
                raise ValueError("retained checkpoint source link changed: " + relative)
        elif (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1
              or bool(info.st_mode & stat.S_IXUSR) != item["executable"]
              or path.read_bytes() != read_private(checkpoint_root / item["artifact"], raw=True)):
            raise ValueError(
                "retained checkpoint source bytes or executable bit changed: " + relative)
    if candidate_for(checkout) != value["source"]["candidate"]:
        raise ValueError("retained checkpoint candidate changed")
    return {"checkout_identity": _identity(checkout), "source_owner": str(source),
            "branch": branch, "head": head, "index_tree": value["source"]["index_tree"]}


def verify_retained_state(spec, value, checkpoint_root):
    """Verify every declared evidence/session byte and authenticated absence."""
    for relative, item in value["state_refs"].items():
        target = _path(Path(spec["state_dir"]), relative)
        if item.get("kind") == "absent":
            if os.path.lexists(target):
                raise ValueError("retained checkpoint no-GO absence changed: " + relative)
            continue
        info = target.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
            raise ValueError("retained checkpoint evidence is not an owned file: " + relative)
        raw = target.read_bytes()
        if (raw != read_private(checkpoint_root / item["artifact"], raw=True)
                or _hash(raw) != item["sha256"] or stat.S_IMODE(info.st_mode) != item["mode"]
                or info.st_mtime_ns != item["mtime_ns"]):
            raise ValueError("retained checkpoint original evidence changed: " + relative)
