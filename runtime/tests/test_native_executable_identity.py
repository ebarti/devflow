"""Real executable edits and namespace changes invalidate repeated measurements."""

import hashlib
import os

import pytest

from devflow_temporal import delivery_native_preparation as preparation


def test_repeated_identity_reuses_only_unchanged_opened_bytes(tmp_path, monkeypatch):
    path = tmp_path / "owned-executable"
    path.write_bytes(b"first native bytes")
    real_digest = hashlib.file_digest
    measured = []

    def digest(stream, algorithm):
        measured.append(True)
        return real_digest(stream, algorithm)

    monkeypatch.setattr(hashlib, "file_digest", digest)
    first = preparation._executable_hash(path)
    assert preparation._executable_hash(path) == first
    assert len(measured) == 1
    modified_at = path.stat().st_mtime_ns
    path.write_bytes(b"other native bytes")
    os.utime(path, ns=(modified_at, modified_at))
    second = preparation._executable_hash(path)
    assert second != first and len(measured) == 2
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"replacement native")
    replacement.replace(path)
    assert preparation._executable_hash(path) not in {first, second}
    assert len(measured) == 3
    path.chmod(0o700)
    preparation._executable_hash(path)
    assert len(measured) == 4
    path.unlink()
    path.symlink_to(replacement)
    with pytest.raises(OSError):
        preparation._executable_hash(path)


def test_edit_during_measurement_is_refused_and_never_cached(tmp_path, monkeypatch):
    path = tmp_path / "owned-executable"
    path.write_bytes(b"original native bytes")
    real_digest = hashlib.file_digest

    def edit_after_read(stream, algorithm):
        value = real_digest(stream, algorithm)
        path.write_bytes(b"modified native bytes")
        return value

    with monkeypatch.context() as editing:
        editing.setattr(hashlib, "file_digest", edit_after_read)
        with pytest.raises(preparation.PreparationError, match="changed while measured"):
            preparation._executable_hash(path)
    assert preparation._executable_hash(path) == hashlib.sha256(path.read_bytes()).hexdigest()
