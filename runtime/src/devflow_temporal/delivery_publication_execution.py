"""Exclusive publication bodies and durable mutation uncertainty.

An activity waiter is not the owner of its synchronous publication thread.  The
descriptor lease lives with that body (and its subprocesses), while the journal
prevents a replacement body from repeating an uncertain mutation after a crash.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
import stat
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from uuid import uuid4

from .contracts import canonical_json
from .delivery_resources import _ancestors, private_directory

_CURRENT: ContextVar[Execution | None] = ContextVar("publication_execution", default=None)
_MUTATION = ContextVar("publication_mutation", default=None)


class PublicationUncertain(RuntimeError):
    """A prior mutation has no authenticated no-effect or completion result."""

    def __init__(self, operation: str):
        self.operation = operation
        super().__init__("original publication " + operation + " requires readback")


def _identity(info):
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
            or info.st_size != 0):
        raise ValueError("publication executor lock is not its private owned file")
    return {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid}


class Execution:
    def __init__(self, broker, key, request, path, descriptor):
        self.broker, self.key, self.request = broker, key, canonical_json(request)
        self.path, self.descriptor = path, descriptor
        self.identity = _identity(os.fstat(descriptor))
        self.token = uuid4().hex
        self.thread = (os.getpid(), threading.get_ident())
        self.generation = 0
        self.closed = False
        self._initialize()

    def _initialize(self):
        with self.broker.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("""CREATE TABLE IF NOT EXISTS delivery_publication_executors (
                effect_key TEXT PRIMARY KEY, request_json TEXT NOT NULL,
                lock_json TEXT NOT NULL, generation INTEGER NOT NULL,
                owner TEXT NOT NULL, legacy_unknown INTEGER NOT NULL,
                mutations_json TEXT NOT NULL
            )""")
            prior = db.execute(
                "SELECT * FROM delivery_publication_executors WHERE effect_key=?", (self.key,)
            ).fetchone()
            self.replacing = prior is not None
            if prior is None:
                effect = db.execute(
                    "SELECT kind,request_json,state FROM delivery_effects WHERE effect_key=?",
                    (self.key,),
                ).fetchone()
                if effect and (effect["kind"] != "publish"
                               or effect["request_json"] != self.request):
                    raise ValueError("publication executor belongs to another effect")
                legacy = int(effect is not None and effect["state"] != "complete")
                self.generation = 1
                db.execute(
                    "INSERT INTO delivery_publication_executors VALUES (?,?,?,?,?,?,?)",
                    (self.key, self.request, canonical_json(self.identity), self.generation,
                     self.token, legacy, "{}"),
                )
            else:
                if (prior["request_json"] != self.request
                        or json.loads(prior["lock_json"]) != self.identity):
                    raise ValueError("publication executor request or lock generation changed")
                self.generation = prior["generation"] + 1
                mutations = json.loads(prior["mutations_json"])
                for mutation in mutations.values():
                    if mutation["state"] == "started":
                        mutation["state"] = "unknown"
                db.execute(
                    "UPDATE delivery_publication_executors SET generation=?,owner=?,"
                    "mutations_json=? WHERE effect_key=?",
                    (self.generation, self.token, canonical_json(mutations), self.key),
                )

    def check(self, db=None):
        if self.closed:
            raise ValueError("publication executor lease is closed")
        if self.thread != (os.getpid(), threading.get_ident()):
            raise ValueError("publication executor belongs to another execution body")
        _ancestors(self.path)
        if (_identity(os.fstat(self.descriptor)) != self.identity
                or _identity(self.path.lstat()) != self.identity):
            raise ValueError("publication executor lock generation changed")
        if db is None:
            with self.broker.store._connect() as connection:
                return self.check(connection)
        row = db.execute(
            "SELECT * FROM delivery_publication_executors WHERE effect_key=?", (self.key,)
        ).fetchone()
        if (row is None or row["request_json"] != self.request
                or row["generation"] != self.generation or row["owner"] != self.token
                or json.loads(row["lock_json"]) != self.identity):
            raise ValueError("publication executor no longer owns this generation")
        return row

    def state(self, operation):
        row = self.check()
        return json.loads(row["mutations_json"]).get(operation, {}).get("state")

    def _record(self, operation, state):
        with self.broker.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self.check(db)
            mutations = json.loads(row["mutations_json"])
            mutations[operation] = {
                "state": state, "generation": self.generation, "owner": self.token,
            }
            db.execute(
                "UPDATE delivery_publication_executors SET mutations_json=? WHERE effect_key=?",
                (canonical_json(mutations), self.key),
            )

    def observed(self, operation):
        """The caller has authenticated the exact local/remote completion."""
        if self.state(operation) in {"started", "unknown"}:
            self._record(operation, "complete")

    def permitted(self, operation):
        row = self.check()
        return (not row["legacy_unknown"]
                and json.loads(row["mutations_json"]).get(operation, {}).get("state")
                in {None, "not_started"})

    def mutate(self, operation, function):
        self.require(operation)
        self._record(operation, "started")
        token = _MUTATION.set((self, operation))
        try:
            result = function()
        except (FileNotFoundError, PermissionError):
            # Popen did not launch. No CLI exit code or diagnostic text grants this.
            self._record(operation, "not_started")
            raise
        except BaseException:
            self._record(operation, "unknown")
            raise
        finally:
            _MUTATION.reset(token)
        self._record(operation, "complete")
        return result

    def require(self, operation):
        if not self.permitted(operation):
            raise PublicationUncertain(operation)


def current(key=None):
    owner = _CURRENT.get()
    if (owner is not None and owner.thread == (os.getpid(), threading.get_ident())
            and (key is None or owner.key == key)):
        return owner
    return None


def inherited_descriptors():
    owner = current()
    if owner is None:
        return ()
    owner.check()
    return (owner.descriptor,)


def current_mutation():
    """Exact launch boundary for the run-control layer, never a waiter context."""
    mutation = _MUTATION.get()
    return mutation if mutation is not None and mutation[0] is current() else None


@contextmanager
def execution(broker, iteration, candidate):
    key = broker.publication_effect_key(iteration)
    request = {"iteration": iteration, "input_candidate_id": candidate["id"]}
    existing = current(key)
    if existing is not None:
        if existing.request != canonical_json(request):
            raise ValueError("nested publication executor changed its request")
        existing.check()
        yield existing
        return
    root = Path(broker.spec["state_dir"]) / "publication-executors"
    if not root.is_absolute() or root.resolve() != root:
        raise ValueError("publication executor namespace is not canonical")
    private_directory(root)
    if stat.S_IMODE(root.stat().st_mode) != 0o700:
        raise ValueError("publication executor namespace is not private")
    path = root / (hashlib.sha256(key.encode()).hexdigest() + ".lock")
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                     0o600)
        os.fsync(fd)
        directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except FileExistsError:
        fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        _identity(os.fstat(fd))
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield None
            return
        owner = Execution(broker, key, request, path, fd)
        token = _CURRENT.set(owner)
        try:
            owner.check()
            yield owner
        finally:
            _CURRENT.reset(token)
            owner.closed = True
    finally:
        # pass_fds gives a live CLI the same lease if its Python parent dies.
        # A surviving subprocess remains an executor, not an unlocked orphan.
        os.close(fd)


@contextmanager
def observe_execution(broker, iteration, candidate):
    """Hold and authenticate an existing executor lease without creating or recording it.

    Absence of its journal or lock cannot prove that a legacy publisher is idle.
    The descriptor remains locked until the caller finishes remote readback.
    """
    key = broker.publication_effect_key(iteration)
    request = canonical_json({"iteration": iteration, "input_candidate_id": candidate["id"]})
    root = Path(broker.spec["state_dir"]) / "publication-executors"
    path = root / (hashlib.sha256(key.encode()).hexdigest() + ".lock")
    if not root.is_absolute() or root.resolve(strict=True) != root:
        raise ValueError("publication executor namespace is unavailable")
    root_info = root.lstat()
    if (not stat.S_ISDIR(root_info.st_mode) or root_info.st_uid != os.getuid()
            or stat.S_IMODE(root_info.st_mode) != 0o700):
        raise ValueError("publication executor namespace is not private")
    _ancestors(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        identity = _identity(os.fstat(fd))
        if _identity(path.lstat()) != identity:
            raise ValueError("publication executor lock identity changed")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield None
            return
        try:
            with broker.store._connect() as db:
                row = db.execute("SELECT * FROM delivery_publication_executors WHERE effect_key=?",
                                 (key,)).fetchone()
        except sqlite3.OperationalError as exc:
            raise ValueError("publication executor journal is unavailable") from exc
        if (row is None or row["request_json"] != request
                or json.loads(row["lock_json"]) != identity):
            raise ValueError("publication executor evidence is unavailable or changed")
        before = dict(row)
        yield before
        with broker.store._connect() as db:
            row = db.execute("SELECT * FROM delivery_publication_executors WHERE effect_key=?",
                             (key,)).fetchone()
        if (row is None or dict(row) != before or _identity(os.fstat(fd)) != identity
                or _identity(path.lstat()) != identity):
            raise ValueError("publication executor changed during readback")
    finally:
        os.close(fd)
