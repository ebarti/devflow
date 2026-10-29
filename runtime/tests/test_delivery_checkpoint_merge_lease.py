"""Authentic shared checkpoint merge custody on owned local lock files."""

from __future__ import annotations

import fcntl
import os
from types import SimpleNamespace

import pytest

from devflow_temporal.delivery_checkpoint_merge_lease import SharedCheckpointMergeLease


def witness(tmp_path):
    root = tmp_path / "ownership"
    root.mkdir(mode=0o700)
    path = root / "installation-recovery.lock"
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    os.close(fd)
    return SimpleNamespace(
        raw={"provider": "fake", "execution_registry": str(root / "registry.sqlite3")}
    ), path


def test_shared_lease_preserves_original_exclusive_owner_for_both_consumers(tmp_path):
    config, path = witness(tmp_path)
    lease = SharedCheckpointMergeLease(config)
    with lease as held:
        held.verify(config)
        held.verify(config)  # Two independent checkpoint participants share the same descriptor.
        other = os.open(path, os.O_RDONLY)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(other)
        with pytest.raises(ValueError, match="writer still owns"):
            with SharedCheckpointMergeLease(config):
                pass
    with pytest.raises(ValueError, match="not active"):
        held.verify(config)
    with pytest.raises(ValueError, match="cannot be reopened"):
        with lease:
            pass


def test_shared_lease_refuses_replacement_and_wrong_installation(tmp_path):
    config, path = witness(tmp_path)
    other_dir = tmp_path / "other"
    other_dir.mkdir(mode=0o700)
    other_config = SimpleNamespace(
        raw={"provider": "fake", "execution_registry": str(other_dir / "registry.sqlite3")}
    )
    with SharedCheckpointMergeLease(config) as held:
        with pytest.raises(ValueError, match="another installation"):
            held.verify(other_config)
        old = path.with_name("retained-original")
        path.rename(old)
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        os.close(fd)
        with pytest.raises(ValueError, match="changed while held"):
            held.verify(config)
