"""Run-wide admission and lifetime custody for checkpoint-sensitive writers.

The witness is created only with a fresh source owner. Observing a missing old
witness never manufactures evidence that an earlier writer was fenced. A queued
thread owns the same shared open-file description as its actual body and CLI;
cancelling the waiter cannot release a running body's custody.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import os
import sqlite3
import stat
import threading
from contextlib import ExitStack, asynccontextmanager, contextmanager
from contextvars import ContextVar
from functools import wraps
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_resources import _ancestors, read_private

NAME = "workspace-writers.lock"
SCHEMA = "devflow-workspace-writers-v1"
_CURRENT = ContextVar("checkpoint_writer_custody", default=())
_DELIVERY_SCOPE = ContextVar("checkpoint_delivery_scope", default=None)
_READ_ONLY_SCOPE = ContextVar("checkpoint_read_only_scope", default=False)
_READBACK_ACTIVITIES = frozenset({
    "delivery_publication_validation_readback", "delivery_resource_closure_readback",
    "delivery_adjudication_readback", "delivery_technical_readback",
    "delivery_metadata_readback", "delivery_gates_readback",
})
_SETTLEMENT_RECOVERIES = frozenset({
    "published_head_validation", "published_metadata_recovery", "terminal_tracker_recovery",
})


def initialize(db):
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_checkpoint_writer_closures (
        run_id TEXT NOT NULL REFERENCES delivery_runs(run_id),
        owner_kind TEXT NOT NULL, workflow_id TEXT NOT NULL,
        execution_run_id TEXT NOT NULL, binding_json TEXT NOT NULL,
        PRIMARY KEY(run_id,owner_kind,workflow_id,execution_run_id))""")


def _delivery_binding(spec, controller):
    return {"spec_sha256": digest(spec),
            "controller": {key: controller[key] for key in (
                "kind", "closure", "result_sha256")}}


def record_closed(db, spec, controller):
    """Only the checkpoint ACK passes its independently verified SDK closure."""
    if controller is None or controller.get("kind") != "closed_workflow":
        return
    closure = controller["closure"]
    from .delivery_execution_authority import VERSION, require_current

    if spec.get(VERSION) == 1:
        selected = db.execute(
            "SELECT * FROM delivery_execution_authority WHERE workflow_id=?",
            (closure["workflow_id"],)).fetchone()
        if not selected or selected["execution_run_id"] != closure["execution_run_id"]:
            raise ValueError("checkpoint closed writer lost its selected SDK owner")
        # This ACK settles a verified closed actor. A released claim may stay
        # released; an extant claim must still name this exact admitted run.
        require_current(db, spec, {
            "run_id": spec["run_id"], "workflow_id": closure["workflow_id"],
            "execution_run_id": closure["execution_run_id"],
            "request_digest": selected["request_digest"], "generation": selected["generation"],
        }, require_claim=False)
    binding = _delivery_binding(spec, controller)
    identity = (spec["run_id"], "delivery", closure["workflow_id"],
                closure["execution_run_id"])
    prior = db.execute(
        "SELECT binding_json FROM delivery_checkpoint_writer_closures "
        "WHERE run_id=? AND owner_kind=? AND workflow_id=? AND execution_run_id=?",
        identity).fetchone()
    if prior and prior[0] != canonical_json(binding):
        raise ValueError("closed writer execution changed its authenticated closure")
    if not prior:
        db.execute("INSERT INTO delivery_checkpoint_writer_closures VALUES (?,?,?,?,?)",
                   (*identity, canonical_json(binding)))
    merge = controller.get("merge_closure")
    if merge is not None:
        selected = db.execute("SELECT * FROM delivery_merge_executions WHERE workflow_id=?",
                              (merge["workflow_id"],)).fetchone()
        saved = db.execute("SELECT * FROM delivery_merge_requests WHERE run_id=?",
                           (spec["run_id"],)).fetchone()
        if (not selected or not saved or selected["run_id"] != spec["run_id"]
                or selected["execution_run_id"] != merge["execution_run_id"]
                or selected["spec_sha256"] != digest(spec)
                or selected["grant_sha256"] != merge["grant_sha256"]
                or saved["workflow_id"] != merge["workflow_id"]
                or digest(json.loads(saved["grant_json"])) != merge["grant_sha256"]):
            raise ValueError("checkpoint closed merge lost its selected SDK owner")
        identity = (spec["run_id"], "merge", merge["workflow_id"], merge["execution_run_id"])
        serialized = canonical_json({"spec_sha256": digest(spec), "merge_closure": merge})
        prior = db.execute(
            "SELECT binding_json FROM delivery_checkpoint_writer_closures "
            "WHERE run_id=? AND owner_kind=? AND workflow_id=? AND execution_run_id=?",
            identity).fetchone()
        if prior and prior[0] != serialized:
            raise ValueError("closed merge writer changed its authenticated closure")
        if not prior:
            db.execute("INSERT INTO delivery_checkpoint_writer_closures VALUES (?,?,?,?,?)",
                       (*identity, serialized))


def verify_closed(db, spec, controller):
    if controller is None or controller.get("kind") != "closed_workflow":
        return
    closure = controller["closure"]
    prior = db.execute(
        "SELECT binding_json FROM delivery_checkpoint_writer_closures "
        "WHERE run_id=? AND owner_kind='delivery' AND workflow_id=? AND execution_run_id=?",
        (spec["run_id"], closure["workflow_id"], closure["execution_run_id"])).fetchone()
    if prior is None or prior[0] != canonical_json(_delivery_binding(spec, controller)):
        raise ValueError("checkpoint lost its authenticated closed-writer cutoff")
    merge = controller.get("merge_closure")
    if merge is not None:
        prior = db.execute(
            "SELECT binding_json FROM delivery_checkpoint_writer_closures "
            "WHERE run_id=? AND owner_kind='merge' AND workflow_id=? AND execution_run_id=?",
            (spec["run_id"], merge["workflow_id"], merge["execution_run_id"])).fetchone()
        if prior is None or prior[0] != canonical_json({"spec_sha256": digest(spec),
                                                      "merge_closure": merge}):
            raise ValueError("checkpoint lost its authenticated closed-merge writer cutoff")


def _actor():
    from temporalio import activity

    if not activity.in_activity():
        return None
    info = activity.info()
    kind = {"DevflowMergeWorkflow": "merge", "DevflowDeliveryWorkflow": "delivery"}.get(
        info.workflow_type)
    if kind is None or not info.workflow_id or not info.workflow_run_id:
        return None
    return {"owner_kind": kind, "workflow_id": info.workflow_id,
            "execution_run_id": info.workflow_run_id,
            "namespace": info.namespace, "task_queue": info.task_queue}


def require_writer(spec, *, store=None, db=None):
    lease = _current(spec)
    if _READ_ONLY_SCOPE.get() or (lease is not None and lease.read_only):
        raise ValueError("read-only writer custody cannot originate a mutation")
    result = _require_actor(spec, db=db, purpose="mutation")
    if spec.get("workspace_custody_version") == 1 and lease is None:
        raise ValueError("delivery mutation has no authenticated actual-body writer custody")
    return result


def _require_actor(spec, *, db=None, purpose):
    """A closed SDK actor cannot enter later, even after the EX lease is released."""
    if spec.get("workspace_custody_version") != 1:
        return
    if db is None:
        path = Path(spec["policy"]["tracking_db"])
        connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            return _require_actor(spec, db=connection, purpose=purpose)
        finally:
            connection.close()
    if not _table(db, "delivery_checkpoint_writer_closures"):
        raise ValueError("workspace writer admission schema is unavailable")
    closed = [dict(row) for row in db.execute(
        "SELECT * FROM delivery_checkpoint_writer_closures WHERE run_id=?", (spec["run_id"],))]
    if not closed:
        return  # Existing execution authority remains responsible before closure.
    actor = _actor()
    if actor is None:
        raise ValueError("closed writer admission needs an authenticated SDK actor")
    old = any(all(row[key] == actor[key] for key in (
        "owner_kind", "workflow_id", "execution_run_id")) for row in closed)
    if old and purpose != "readback":
        raise ValueError("closed SDK execution cannot admit another writer")
    from .delivery_execution_config import load_execution_config

    config = load_execution_config(spec)
    if (actor["namespace"] != config.raw.get("temporal_namespace", "default")
            or actor["task_queue"] != config.queue):
        raise ValueError("replacement writer SDK transport differs from its execution grant")
    # A different identity is not a grant. The owning execution layer must
    # authenticate it against its selected SDK start, immutable spec and grant.
    if actor["owner_kind"] == "merge":
        from .delivery_merge_execution import identity, require_current

        try:
            origin = identity()
        except ValueError:
            # The selected merge's first read-only context loads the store used
            # by SDK/history authentication. It cannot inherit mutation rights.
            if purpose != "readback":
                raise
            saved = db.execute("SELECT * FROM delivery_merge_requests WHERE run_id=?",
                               (spec["run_id"],)).fetchone()
            selected = db.execute(
                "SELECT * FROM delivery_merge_executions WHERE workflow_id=?",
                (actor["workflow_id"],)).fetchone()
            if (not saved or saved["workflow_id"] != actor["workflow_id"]
                    or json.loads(saved["grant_json"]).get("spec_sha256") != digest(spec)
                    or selected and selected["execution_run_id"] != actor["execution_run_id"]):
                raise ValueError("merge setup differs from its selected original grant") from None
            return
        if any(actor[key] != origin[key] for key in ("workflow_id", "execution_run_id")):
            raise ValueError("replacement merge SDK identity differs from its execution grant")
        require_current(db, spec, required=True)
        return
    from .delivery_execution_authority import activity_authority, require_current

    authority = activity_authority()
    if any(actor[key] != authority[key] for key in ("workflow_id", "execution_run_id")):
        raise ValueError("replacement writer SDK identity differs from its execution grant")
    require_current(db, spec, authority, require_claim=purpose == "mutation")


def _purpose(spec):
    from temporalio import activity

    if spec.get("workspace_custody_version") != 1 or not activity.in_activity():
        return "mutation"
    name = activity.info().activity_type
    if name in _READBACK_ACTIVITIES:
        return "readback"
    path = Path(spec["policy"]["tracking_db"])
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    try:
        row = connection.execute("SELECT recovery_json FROM delivery_runs WHERE run_id=?",
                                 (spec["run_id"],)).fetchone()
        recovery = json.loads(row[0] or "null") if row else None
        settlement = recovery and recovery.get("kind") in _SETTLEMENT_RECOVERIES
        if name == "delivery_reconcile_publish":
            actor = _actor()
            old = connection.execute(
                "SELECT 1 FROM delivery_checkpoint_writer_closures WHERE run_id=? "
                "AND owner_kind=? AND workflow_id=? AND execution_run_id=?",
                (spec["run_id"], actor["owner_kind"], actor["workflow_id"],
                 actor["execution_run_id"])).fetchone() if actor else None
            return "readback" if old or settlement else "mutation"
        if settlement and name == "delivery_project":
            return "settlement"
        if (recovery and recovery.get("kind") == "terminal_tracker_recovery"
                and name in {"delivery_terminal_tracker", "delivery_terminal_preflight"}):
            return "settlement"
        return "mutation"
    finally:
        connection.close()


def require_activity_entry(spec):
    lease = _current(spec)
    actor = _actor()
    if lease is not None and lease.read_only and actor and actor["owner_kind"] == "merge":
        return _require_actor(spec, purpose="readback")
    return _require_actor(spec, purpose=_purpose(spec))


def reconciliation_read_only(spec):
    lease = _current(spec)
    return (_READ_ONLY_SCOPE.get() or bool(lease is not None and lease.read_only)
            or _purpose(spec) == "readback")


def _prepare_admission(spec):
    """Explicit fresh writer admission may initialize custody; observation may not."""
    if spec.get("workspace_custody_version") != 1:
        return
    receipt = Path(spec["state_dir"]) / "workspace-owner.json"
    if os.path.lexists(receipt):
        return  # Existing/legacy witnesses are always authenticated without repair.
    from .delivery_execution_config import load_execution_config
    from .delivery_store import DeliveryStore
    from .delivery_workspace_custody import ensure_owner

    store = DeliveryStore(load_execution_config(spec), read_only=True)
    if spec not in (store.effective_spec(spec["run_id"]), store.submitted_spec(spec["run_id"])):
        raise ValueError("writer bootstrap differs from its admitted execution authority")
    ensure_owner(spec)


def _identity(info):
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1 or info.st_size):
        raise ValueError("workspace writer witness is not its private owned file")
    return {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid}


def initialize_witness(state):
    """Called only while publishing a newly allocated source-owner receipt."""
    path = state / NAME
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        os.fsync(descriptor)
        identity = _identity(os.fstat(descriptor))
    finally:
        os.close(descriptor)
    directory = os.open(state, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return {"schema": SCHEMA, "identity": identity}


def _canonical(spec):
    from .delivery_preparation_resources import canonical_owner

    return canonical_owner(spec) or spec


def _witness(spec):
    from .delivery_workspace_custody import _binding, _validate, owner_path

    spec = _canonical(spec)
    if spec.get("workspace_custody_version") != 1:
        raise ValueError("legacy writer custody is unknown; checkpoint is unavailable")
    state = Path(spec["state_dir"])
    receipt = read_private(state / "workspace-owner.json")
    if receipt.get("binding") != _binding(spec):
        raise ValueError("writer witness changed its admitted source owner")
    _validate(spec, receipt, owner_path(spec))
    witness = receipt.get("writer_custody")
    if not isinstance(witness, dict) or witness.get("schema") != SCHEMA:
        raise ValueError("legacy source owner has no writer-admission witness")
    path = state / NAME
    _ancestors(path)
    if _identity(path.lstat()) != witness.get("identity"):
        raise ValueError("workspace writer witness generation changed")
    return spec, path, witness["identity"]


class Lease:
    def __init__(self, spec, *, exclusive=False, read_only=False):
        self.spec, self.path, self.identity = _witness(spec)
        self.descriptor = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        self.delegated_descriptor = None
        self.exclusive = exclusive
        self.thread = None
        self.read_only = read_only
        self.closed = False
        try:
            self.check()
            fcntl.flock(self.descriptor,
                        (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
            if not exclusive:
                self.delegated_descriptor = os.open(
                    self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
                if _identity(os.fstat(self.delegated_descriptor)) != self.identity:
                    raise ValueError("delegated writer witness generation changed")
                fcntl.flock(self.delegated_descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
            self.check()
            if not exclusive:
                _require_actor(self.spec, purpose="readback" if read_only else "mutation")
        except BlockingIOError as exc:
            self.close()
            raise ValueError(
                "checkpoint-sensitive writer or checkpoint still owns its lease") from exc
        except BaseException:
            self.close()
            raise

    def check(self):
        if self.closed or _identity(os.fstat(self.descriptor)) != self.identity:
            raise ValueError("workspace writer lease lost its original descriptor")
        _ancestors(self.path)
        if _identity(self.path.lstat()) != self.identity:
            raise ValueError("workspace writer witness generation changed")
        if (self.delegated_descriptor is not None
                and _identity(os.fstat(self.delegated_descriptor)) != self.identity):
            raise ValueError("delegated writer descriptor generation changed")

    def close(self):
        if not self.closed:
            self.closed = True
            # No LOCK_UN: a live subprocess may retain this open description.
            os.close(self.descriptor)
            if self.delegated_descriptor is not None:
                os.close(self.delegated_descriptor)


def _current(spec=None):
    identity = (os.getpid(), threading.get_ident())
    for lease in reversed(_CURRENT.get()):
        if (lease.thread == identity and not lease.closed and (spec is None or (
                lease.spec["run_id"] == spec["run_id"]
                and lease.spec["state_dir"] == spec["state_dir"]))):
            lease.check()
            return lease
    return None


@contextmanager
def _activate(lease):
    lease.thread = (os.getpid(), threading.get_ident())
    token = _CURRENT.set((*_CURRENT.get(), lease))
    try:
        lease.check()
        yield lease
    finally:
        _CURRENT.reset(token)


@contextmanager
def shared(spec, *, read_only=False):
    token = _READ_ONLY_SCOPE.set(_READ_ONLY_SCOPE.get() or read_only)
    try:
        with _shared_lease(spec, read_only=read_only) as lease:
            yield lease
    finally:
        _READ_ONLY_SCOPE.reset(token)


@contextmanager
def _shared_lease(spec, *, read_only=False):
    if spec.get("workspace_custody_version") != 1:
        yield None  # Historical execution retains its original gates, never checkpoints.
        return
    owner = _current(spec)
    if owner is not None:
        if not read_only:
            require_writer(owner.spec)
        yield owner
        return
    lease = Lease(spec, read_only=read_only)
    try:
        with _activate(lease):
            yield lease
    finally:
        lease.close()


@asynccontextmanager
async def activity_custody(spec):
    """Cover inline async state/effects as well as each escaped synchronous body."""
    require_activity_entry(spec)
    if (spec.get("workspace_custody_version") == 1
            and not os.path.lexists(Path(spec["state_dir"]) / "workspace-owner.json")):
        await asyncio.to_thread(_prepare_admission, spec)
    purpose = _purpose(spec)
    token = _DELIVERY_SCOPE.set(spec)
    try:
        with shared(spec, read_only=purpose in {"readback", "settlement"}):
            yield
    finally:
        _DELIVERY_SCOPE.reset(token)


def inherited_descriptors():
    identity = (os.getpid(), threading.get_ident())
    descriptors = []
    for lease in _CURRENT.get():
        if lease.thread == identity and not lease.closed:
            lease.check()
            # Never expose the trusted body's own shared open description: an
            # untrusted child can LOCK_UN its inherited description even read-only.
            descriptors.append(lease.delegated_descriptor if not lease.exclusive
                               else lease.descriptor)
    return tuple(dict.fromkeys(descriptors))


def require_current_writer():
    """Compose generic GitHub clients with an already authenticated run lease."""
    if _READ_ONLY_SCOPE.get():
        raise ValueError("read-only writer custody cannot originate a mutation")
    lease = _current()
    if lease is not None:
        require_writer(lease.spec)
        return
    scope = _DELIVERY_SCOPE.get()
    if scope is not None and scope.get("workspace_custody_version") == 1:
        raise ValueError("delivery mutation has no authenticated actual-body writer custody")
    if scope is None:
        from temporalio import activity

        if activity.in_activity() and activity.info().workflow_type in {
            "DevflowDeliveryWorkflow", "DevflowMergeWorkflow"
        }:
            raise ValueError("delivery mutation has no authenticated actual-body writer scope")


def require_tracking_operation(spec, status, release, *, terminal, reason):
    """Only the selected sealed terminal grant can settle its exact tracker intent."""
    lease = _current(spec)
    if lease is None or not (lease.read_only or _READ_ONLY_SCOPE.get()):
        return require_writer(spec)
    from temporalio import activity

    if (not terminal or not activity.in_activity()
            or activity.info().activity_type != "delivery_terminal_tracker"):
        raise ValueError("read-only custody has no terminal tracker settlement capability")
    _require_actor(spec, purpose="settlement")
    path = Path(spec["policy"]["tracking_db"])
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    try:
        row = connection.execute("SELECT recovery_json FROM delivery_runs WHERE run_id=?",
                                 (spec["run_id"],)).fetchone()
        recovery = json.loads(row[0] or "null") if row else None
        checkpoint = ((recovery or {}).get("state") or {}).get("checks", {}).get(
            "terminal_tracker_checkpoint", {})
        if (not recovery or recovery.get("kind") != "terminal_tracker_recovery"
                or recovery.get("spec_sha256") != digest(spec)
                or not isinstance(recovery.get("seal"), dict)
                or not isinstance(recovery.get("command_id"), str)
                or not recovery["command_id"]
                or not isinstance(recovery.get("closed"), dict)
                or not isinstance(recovery["closed"].get("history_sha256"), str)
                or len(recovery["closed"]["history_sha256"]) != 64
                or set(recovery["closed"]["history_sha256"]) - set("0123456789abcdef")
                or (status, release, reason) != (
                    checkpoint.get("status"), checkpoint.get("release"), checkpoint.get("reason"))
                or type(release) is not bool
                or type(checkpoint.get("release")) is not bool
                or status not in {"blocked", "in-review", "done"}):
            raise ValueError(
                "terminal tracker target differs from its exact sealed settlement grant")
    finally:
        connection.close()


def writer_method(function):
    """Keep direct synchronous workspace entry points in the same custody contract."""
    @wraps(function)
    def execute(owner, *args, **kwargs):
        with shared(owner.spec):
            return function(owner, *args, **kwargs)
    return execute


def native_custody(spec):
    """Only a current actual body can create the internal monitor carrier."""
    canonical = _canonical(spec)
    lease = _current(canonical)
    if lease is None:
        return None
    lease.check()
    if lease.exclusive:
        raise ValueError("checkpoint custody cannot authorize a native writer")
    return {"schema": "devflow-native-writer-custody-v1", "run_id": canonical["run_id"],
            "state_dir": canonical["state_dir"], "identity": lease.identity,
            "descriptors": [lease.delegated_descriptor]}


def native_descriptors(spec, carrier):
    """Authenticate inherited read-only descriptors, never open a replacement."""
    if carrier is None:
        return ()
    canonical, _path, identity = _witness(spec)
    if (not isinstance(carrier, dict) or set(carrier) != {
            "schema", "run_id", "state_dir", "identity", "descriptors"}
            or carrier["schema"] != "devflow-native-writer-custody-v1"
            or carrier["run_id"] != canonical["run_id"]
            or carrier["state_dir"] != canonical["state_dir"]
            or carrier["identity"] != identity
            or not isinstance(carrier["descriptors"], list)
            or len(carrier["descriptors"]) != 1):
        raise ValueError("native writer custody carrier changed its admitted run")
    descriptor = carrier["descriptors"][0]
    if (type(descriptor) is not int or descriptor <= 2
            or fcntl.fcntl(descriptor, fcntl.F_GETFL) & os.O_ACCMODE != os.O_RDONLY
            or _identity(os.fstat(descriptor)) != identity):
        raise ValueError("native writer custody descriptor is not its inherited read-only witness")
    return (descriptor,)


def native_binding(spec, carrier):
    if carrier is None:
        return None
    native_descriptors(spec, carrier)
    return {"schema": "devflow-native-writer-binding-v1", "run_id": carrier["run_id"],
            "state_dir": carrier["state_dir"], "identity": carrier["identity"]}


def observed_native_binding(spec):
    """Read an existing writer witness without creating or granting custody."""
    if spec.get("workspace_custody_version") != 1:
        return None
    canonical, path, identity = _witness(spec)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        if (_identity(os.fstat(descriptor)) != identity
                or _identity(path.lstat()) != identity):
            raise ValueError("native writer observation witness changed")
        return {"schema": "devflow-native-writer-binding-v1", "run_id": canonical["run_id"],
                "state_dir": canonical["state_dir"], "identity": identity}
    finally:
        os.close(descriptor)


@contextmanager
def native_guard(spec, carrier):
    """Keep an independent trusted monitor description away from provider tools."""
    if carrier is None:
        yield None
        return
    native_descriptors(spec, carrier)
    _canonical_spec, path, identity = _witness(spec)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        if _identity(os.fstat(descriptor)) != identity:
            raise ValueError("native monitor guard witness changed")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("native monitor cannot enter a checkpoint writer fence") from exc
        yield {"kind": "independent-monitor-shared-v1", "identity": identity}
        if _identity(os.fstat(descriptor)) != identity or _identity(path.lstat()) != identity:
            raise ValueError("native monitor guard witness changed while held")
    finally:
        os.close(descriptor)


async def to_thread(spec, function, *args, read_only=None, **kwargs):
    """Acquire before submission; abandon a cancelled queue entry before it runs."""
    purpose = _purpose(spec)
    if read_only is True:
        purpose = "readback"
    if read_only is None:
        read_only = purpose in {"readback", "settlement"}
    _require_actor(spec, purpose=purpose)
    if spec.get("workspace_custody_version") != 1:
        def legacy():
            token = _DELIVERY_SCOPE.set(spec)
            try:
                return function(*args, **kwargs)
            finally:
                _DELIVERY_SCOPE.reset(token)
        return await asyncio.to_thread(legacy)
    if not os.path.lexists(Path(spec["state_dir"]) / "workspace-owner.json"):
        await asyncio.to_thread(_prepare_admission, spec)
    lease = Lease(spec, read_only=read_only)
    handoff, state = threading.Lock(), {"phase": "queued"}

    def execute():
        with handoff:
            if state["phase"] == "cancelled":
                return None
            state["phase"] = "running"
        try:
            token = _DELIVERY_SCOPE.set(spec)
            try:
                with _activate(lease):
                    _require_actor(spec, purpose=purpose)
                    return function(*args, **kwargs)
            finally:
                _DELIVERY_SCOPE.reset(token)
        finally:
            with handoff:
                state["phase"] = "done"
                lease.close()

    try:
        return await asyncio.to_thread(execute)
    except BaseException:
        with handoff:
            if state["phase"] == "queued":
                state["phase"] = "cancelled"
                lease.close()
        raise


def _table(db, name):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                      (name,)).fetchone() is not None


def _inventory(db, run_id):
    prefix = "publish:" + run_id + ":"
    executors = []
    if _table(db, "delivery_publication_executors"):
        executors = [dict(row) for row in db.execute(
            "SELECT * FROM delivery_publication_executors "
            "WHERE substr(effect_key,1,?)=? ORDER BY effect_key", (len(prefix), prefix))]
    return {
        "publication_executors": executors,
        "merge_requests": [dict(row) for row in db.execute(
            "SELECT * FROM delivery_merge_requests WHERE run_id=? ORDER BY run_id", (run_id,))],
        "merge_executions": [dict(row) for row in db.execute(
            "SELECT * FROM delivery_merge_executions WHERE run_id=? ORDER BY workflow_id",
            (run_id,))] if _table(db, "delivery_merge_executions") else [],
    }


class CheckpointWriters:
    """Exclusive admission, actual bodies and exact existing executor generations."""
    def __init__(self, store, spec, *, additional_context=None, shared_merge_lease=None):
        self.store, self.spec = store, spec
        self.additional_context = additional_context
        self.additional = None
        self.shared_merge_lease = shared_merge_lease
        if shared_merge_lease is not None:
            from .delivery_checkpoint_merge_lease import SharedCheckpointMergeLease

            if type(shared_merge_lease) is not SharedCheckpointMergeLease:
                raise ValueError("checkpoint shared merge lease has no authenticated owner")
        self.stack = None

    def __enter__(self):
        self.stack = ExitStack()
        try:
            self.lease = Lease(self.spec, exclusive=True)
            self.stack.callback(self.lease.close)
            self.stack.enter_context(_activate(self.lease))
            with self.store._connect() as db:
                self.snapshot = _inventory(db, self.spec["run_id"])
            from .delivery_feature_execution import registry_path
            from .delivery_merge_execution import existing_lease

            global_path = registry_path(self.store.config).parent / "installation-recovery.lock"
            if self.snapshot["merge_requests"] or self.snapshot["merge_executions"]:
                if self.shared_merge_lease is not None:
                    self.shared_merge_lease.verify(self.store.config)
                elif not os.path.lexists(global_path):
                    raise ValueError("merge writer custody has no original installation witness")
                else:
                    self.stack.enter_context(existing_lease(self.store.config, exclusive=True))
            self.executor_locks = []
            root = Path(self.spec["state_dir"]) / "publication-executors"
            expected = set()
            for row in self.snapshot["publication_executors"]:
                path = root / (hashlib.sha256(row["effect_key"].encode()).hexdigest() + ".lock")
                expected.add(path.name)
                _ancestors(path)
                descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
                self.stack.callback(os.close, descriptor)
                identity = json.loads(row["lock_json"])
                if (_identity(os.fstat(descriptor)) != identity
                        or _identity(path.lstat()) != identity):
                    raise ValueError("publication writer lost its authenticated lock generation")
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as exc:
                    raise ValueError("publication subprocess still owns its effect lease") from exc
                self.executor_locks.append((path, descriptor, identity))
            if os.path.lexists(root):
                _ancestors(root)
                info = root.lstat()
                if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                        or stat.S_IMODE(info.st_mode) != 0o700
                        or {path.name for path in root.iterdir()} != expected):
                    raise ValueError("publication writer inventory has unbound or missing locks")
            if self.additional_context is not None:
                self.additional = self.stack.enter_context(self.additional_context)
            with self.store._connect() as db:
                self.verify(db)
            return self
        except BaseException:
            self.stack.close()
            raise

    def verify(self, db):
        self.lease.check()
        if self.shared_merge_lease is not None:
            self.shared_merge_lease.verify(self.store.config)
        if _inventory(db, self.spec["run_id"]) != self.snapshot:
            raise ValueError("checkpoint publication or merge writer generation changed")
        for path, descriptor, identity in self.executor_locks:
            if (_identity(os.fstat(descriptor)) != identity
                    or _identity(path.lstat()) != identity):
                raise ValueError("checkpoint publication writer lock changed")
        if self.additional is not None:
            self.additional.verify(db)

    def __exit__(self, *args):
        return self.stack.__exit__(*args)
