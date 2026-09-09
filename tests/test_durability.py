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
