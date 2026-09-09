import pytest

from devflow import durability


def test_macos_full_flush_failure_is_not_silently_downgraded(monkeypatch):
    events = []
    monkeypatch.setattr(durability.sys, "platform", "darwin")
    monkeypatch.setattr(durability.fcntl, "F_FULLFSYNC", 51, raising=False)
    monkeypatch.setattr(durability.os, "fsync", lambda fd: events.append(("fsync", fd)))

    def full_flush(fd, operation):
        events.append((operation, fd))
        raise OSError("synthetic drive flush failure")

    monkeypatch.setattr(durability.fcntl, "fcntl", full_flush)
    with pytest.raises(OSError, match="drive flush failure"):
        durability.flush_descriptor(42)
    assert events == [("fsync", 42), (51, 42)]


def test_existing_directory_reuse_does_not_open_unrelated_ancestors(tmp_path, monkeypatch):
    import os
    from pathlib import Path

    parent = tmp_path / "accessible"
    selected = parent / "selected"
    selected.mkdir(parents=True)
    original = durability.os.open
    opened = []

    def limited_open(path, flags, *args, **kwargs):
        path = Path(path)
        if path in parent.parents:
            raise PermissionError("synthetic unrelated ancestor access denied")
        opened.append(path)
        return original(path, flags, *args, **kwargs)

    monkeypatch.setattr(durability.os, "open", limited_open)
    for _ in range(2):
        durability.durable_directory(selected)
    assert opened == [selected, parent, selected, parent]
    assert selected.stat().st_uid == os.getuid()


def test_nested_creation_stops_before_descendants_when_parent_flush_fails(tmp_path, monkeypatch):
    selected = tmp_path / "first" / "second" / "selected"
    first = tmp_path / "first"
    original = durability.flush_directory
    events = []

    def fail_required_parent(path):
        events.append(path)
        if path == tmp_path and first.exists():
            raise OSError("synthetic required namespace flush failed")
        original(path)

    monkeypatch.setattr(durability, "flush_directory", fail_required_parent)
    with pytest.raises(OSError, match="required namespace flush failed"):
        durability.durable_directory(selected)
    assert first.is_dir()
    assert not (first / "second").exists()
    assert events[-2:] == [first, tmp_path]
    monkeypatch.setattr(durability, "flush_directory", original)
    durability.durable_directory(selected)
    assert selected.is_dir()


@pytest.mark.parametrize("interrupted_depth", [1, 2, 3])
def test_nested_mkdir_sigkill_retry_flushes_only_unsettled_boundary(
    tmp_path, monkeypatch, interrupted_depth
):
    import signal
    import subprocess
    import sys

    first = tmp_path / "first"
    selected = first / "second" / "selected"
    components = [first, first / "second", selected]
    interrupted = components[interrupted_depth - 1]
    code = '''
import os, signal, sys
from pathlib import Path
from devflow import durability
first = Path(sys.argv[1])
original = durability.flush_directory
def kill_before_parent_flush(path):
    if path == first.parent and first.exists():
        os.kill(os.getpid(), signal.SIGKILL)
    original(path)
durability.flush_directory = kill_before_parent_flush
durability.durable_directory(Path(sys.argv[2]))
'''
    result = subprocess.run([sys.executable, "-c", code, str(interrupted), str(selected)], timeout=30)
    assert result.returncode == -signal.SIGKILL
    assert interrupted.is_dir()
    for directory in components[interrupted_depth:]:
        assert not directory.exists()
    events = []
    original = durability.flush_directory

    def observe(path):
        events.append(path)
        assert path not in interrupted.parent.parents
        original(path)

    monkeypatch.setattr(durability, "flush_directory", observe)
    durability.durable_directory(selected)
    expected = [interrupted, interrupted.parent]
    for directory in components[interrupted_depth:]:
        expected.extend([directory, directory.parent])
    assert events == expected
    assert selected.is_dir()


def test_unknown_existing_bootstrap_requires_its_immediate_parent(tmp_path, monkeypatch):
    anchor = tmp_path / "accessible"
    anchor.mkdir()
    selected = anchor / "selected"
    original = durability.flush_directory

    def denied_parent(path):
        if path == tmp_path:
            raise PermissionError("synthetic bootstrap parent denied")
        original(path)

    # The helper cannot prove this anchor was not an earlier interrupted mkdir.
    monkeypatch.setattr(durability, "flush_directory", denied_parent)
    with pytest.raises(PermissionError, match="bootstrap parent denied"):
        durability.durable_directory(selected)
    assert not selected.exists()
