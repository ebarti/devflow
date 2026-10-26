"""Closure observations never create or repair storage as a side effect."""
from types import SimpleNamespace

import pytest

from devflow_temporal.delivery_closures import _private_root


def test_readonly_closure_root_never_creates_missing_directory(tmp_path):
    store = SimpleNamespace(config=SimpleNamespace(state_root=tmp_path))
    with pytest.raises(FileNotFoundError):
        _private_root(store, read_only=True)
    assert not (tmp_path / "closures").exists()


def test_readonly_closure_root_does_not_change_unprivate_mode(tmp_path):
    store = SimpleNamespace(config=SimpleNamespace(state_root=tmp_path))
    root = tmp_path / "closures"
    root.mkdir()
    root.chmod(0o755)
    with pytest.raises(ValueError, match="permissions"):
        _private_root(store, read_only=True)
    assert root.stat().st_mode & 0o777 == 0o755
    assert _private_root(store) == root
    assert root.stat().st_mode & 0o777 == 0o700
