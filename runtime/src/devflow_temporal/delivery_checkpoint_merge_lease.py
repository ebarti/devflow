"""Share the original global merge writer lease across a feature graph.

This object proves only exclusive ownership of the existing installation
witness. It does not prove process settlement, actor quiescence, or checkpoint
eligibility. Callers must retain it while all participant custody is held.
"""

from __future__ import annotations

import fcntl
import os
import stat

from .delivery_feature_execution import registry_path
from .delivery_resources import _ancestors


class SharedCheckpointMergeLease:
    """Hold one authenticated original exclusive merge fence, without creating it."""

    def __init__(self, config):
        self._path = registry_path(config).parent / "installation-recovery.lock"
        self._descriptor = None
        self._identity = None
        self._process = None

    @staticmethod
    def _file_identity(info):
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
        ):
            raise ValueError("checkpoint merge witness is not its private owned file")
        return (info.st_dev, info.st_ino, info.st_uid)

    def __enter__(self):
        if self._descriptor is not None or self._process is not None:
            raise ValueError("checkpoint merge lease cannot be reopened")
        _ancestors(self._path)
        descriptor = os.open(self._path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            identity = self._file_identity(os.fstat(descriptor))
            if self._file_identity(self._path.lstat()) != identity:
                raise ValueError("checkpoint merge witness changed before acquisition")
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ValueError("checkpoint merge writer still owns its lease") from exc
            self._descriptor = descriptor
            self._identity = identity
            self._process = os.getpid()
            self._verify_path()
            return self
        except BaseException:
            self._descriptor = None
            os.close(descriptor)
            raise

    def _verify_path(self):
        if self._descriptor is None or self._process != os.getpid():
            raise ValueError("checkpoint merge lease is not active in this process")
        _ancestors(self._path)
        if (
            self._file_identity(os.fstat(self._descriptor)) != self._identity
            or self._file_identity(self._path.lstat()) != self._identity
        ):
            raise ValueError("checkpoint merge witness changed while held")

    def verify(self, config):
        """Verify the same owned descriptor and witness without reacquiring it."""
        if registry_path(config).parent / "installation-recovery.lock" != self._path:
            raise ValueError("checkpoint merge lease belongs to another installation")
        self._verify_path()
        probe = os.open(self._path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            if self._file_identity(os.fstat(probe)) != self._identity:
                raise ValueError("checkpoint merge witness changed during verification")
            try:
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pass
            else:
                raise ValueError("checkpoint merge exclusive lease is no longer held")
        finally:
            os.close(probe)
        self._verify_path()

    def __exit__(self, *args):
        if self._descriptor is not None:
            descriptor, self._descriptor = self._descriptor, None
            os.close(descriptor)
        # The object stays closed and cannot silently acquire a new generation.
        return False
