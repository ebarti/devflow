"""Retain dispatcher custody alongside the run-wide checkpoint writer leases."""
from __future__ import annotations

import fcntl
import json
import os
from contextlib import ExitStack

from .delivery_pause_dispatch_lease import identity, lock_path
from .delivery_resources import _ancestors


class PauseWriterCustody:
    def __init__(self, store, spec):
        self.store, self.spec = store, spec
        self.stack = ExitStack()
        self.snapshot = None
        self.descriptor = None
        self.entered = False

    def _snapshot(self, db):
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "delivery_pause_dispatch_leases" not in tables:
            return None
        row = db.execute("""SELECT workflow_id,owner_generation,lock_json,generation,owner
            FROM delivery_pause_dispatch_leases WHERE root_id=?""",
            (self.spec["run_id"],)).fetchone()
        return tuple(row) if row else None

    def __enter__(self):
        try:
            with self.store._connect() as db:
                self.snapshot = self._snapshot(db)
            if self.snapshot is not None:
                path = lock_path(self.store, self.spec["run_id"])
                _ancestors(path)
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                self.stack.callback(os.close, fd)
                lock_identity = identity(os.fstat(fd))
                if (lock_identity != json.loads(self.snapshot[2])
                        or lock_identity != identity(path.lstat())):
                    raise ValueError("pause dispatcher lock identity changed")
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as exc:
                    raise ValueError("pause dispatcher still owns its execution lease") from exc
                if lock_identity != identity(path.lstat()):
                    raise ValueError("pause dispatcher lock changed while acquiring custody")
                self.descriptor = (fd, path, lock_identity)
            self.entered = True
            return self
        except BaseException:
            self.stack.close()
            raise

    def verify(self, db):
        """Call under the root fence to detect changes after enumeration."""
        if not self.entered or self._snapshot(db) != self.snapshot:
            raise ValueError("pause writer inventory changed before the admission fence")
        if self.descriptor is not None:
            fd, path, held_identity = self.descriptor
            _ancestors(path)
            if identity(os.fstat(fd)) != held_identity or identity(path.lstat()) != held_identity:
                raise ValueError("pause writer lease changed while held")

    def __exit__(self, *args):
        self.entered = False
        self.stack.close()


def checkpoint_writers(store, spec):
    """Combine run-wide writer custody with the independent dispatcher lease."""
    from .delivery_writer_custody import CheckpointWriters

    return CheckpointWriters(store, spec, additional_context=PauseWriterCustody(store, spec))
