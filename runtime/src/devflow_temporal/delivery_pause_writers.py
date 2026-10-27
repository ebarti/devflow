"""Acquire and revalidate all durable publication and merge writer leases."""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
from contextlib import ExitStack
from pathlib import Path

from .delivery_publication_execution import _identity
from .delivery_resources import _ancestors


class PauseWriterCustody:
    def __init__(self, store, spec, *, dispatcher_only=False):
        self.store, self.spec = store, spec
        self.dispatcher_only = dispatcher_only
        self.stack = ExitStack()
        self.snapshot = None
        self.descriptors = {}

    def _snapshot(self, db):
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        prefixes = "publish:" + self.spec["run_id"] + ":"
        publications = []
        if not self.dispatcher_only and "delivery_publication_executors" in tables:
            publications = sorted((row["effect_key"], row["lock_json"]) for row in db.execute(
                "SELECT effect_key,lock_json FROM delivery_publication_executors")
                if row["effect_key"].startswith(prefixes))
        dispatch = None
        if "delivery_pause_dispatch_leases" in tables:
            row = db.execute("""SELECT workflow_id,owner_generation,lock_json,generation,owner
                FROM delivery_pause_dispatch_leases WHERE root_id=?""",
                (self.spec["run_id"],)).fetchone()
            dispatch = tuple(row) if row else None
        merge = None
        if not self.dispatcher_only and "delivery_merge_requests" in tables:
            row = db.execute("""SELECT workflow_id,grant_json FROM delivery_merge_requests
                WHERE run_id=?""", (self.spec["run_id"],)).fetchone()
            merge = tuple(row) if row else None
        return publications, merge, dispatch

    def __enter__(self):
        try:
            with self.store._connect() as db:
                self.snapshot = self._snapshot(db)
            publications, merge, dispatch = self.snapshot
            if dispatch is not None:
                from .delivery_pause_dispatch_lease import identity, lock_path

                path = lock_path(self.store, self.spec["run_id"])
                _ancestors(path)
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                self.stack.callback(os.close, fd)
                lock_identity = identity(os.fstat(fd))
                if (lock_identity != json.loads(dispatch[2])
                        or lock_identity != identity(path.lstat())):
                    raise ValueError("pause dispatcher lock identity changed")
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as exc:
                    raise ValueError("pause dispatcher still owns its execution lease") from exc
                if lock_identity != identity(path.lstat()):
                    raise ValueError("pause dispatcher lock changed while acquiring custody")
                self.descriptors["dispatch"] = (fd, path, lock_identity)
            if merge is not None:
                from .delivery_merge_execution import existing_lease

                self.stack.enter_context(existing_lease(self.store.config, exclusive=True))
            root = Path(self.spec["state_dir"]) / "publication-executors"
            for key, encoded in publications:
                path = root / (hashlib.sha256(key.encode()).hexdigest() + ".lock")
                _ancestors(path)
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                self.stack.callback(os.close, fd)
                identity = _identity(os.fstat(fd))
                if identity != json.loads(encoded) or identity != _identity(path.lstat()):
                    raise ValueError("publication writer lock identity changed")
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as exc:
                    raise ValueError("publication executor still owns its writer lease") from exc
                if identity != _identity(path.lstat()):
                    raise ValueError("publication writer lock changed while acquiring custody")
                self.descriptors[key] = (fd, path, identity)
            return self
        except BaseException:
            self.stack.close()
            raise

    def verify(self, db):
        """Call under the root fence to detect any writer added after enumeration."""
        if self.snapshot is None or self._snapshot(db) != self.snapshot:
            raise ValueError("pause writer inventory changed before the admission fence")
        for fd, path, identity in self.descriptors.values():
            _ancestors(path)
            if _identity(os.fstat(fd)) != identity or _identity(path.lstat()) != identity:
                raise ValueError("pause writer lease changed while held")

    def __exit__(self, *args):
        self.stack.close()


def checkpoint_writers(store, spec):
    """Combine run-wide writer custody with the independent dispatcher lease."""
    from .delivery_writer_custody import CheckpointWriters

    return CheckpointWriters(store, spec,
                             additional_context=PauseWriterCustody(
                                 store, spec, dispatcher_only=True))
