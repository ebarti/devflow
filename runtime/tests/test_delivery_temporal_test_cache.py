import hashlib
import json

import pytest

from devflow_temporal.delivery_sandbox import _seed_temporal_test_server


@pytest.fixture
def cached(tmp_path, monkeypatch):
    project, cache, scratch = (tmp_path / name for name in ("project", "cache", "scratch"))
    for path in (project, cache, scratch):
        path.mkdir()
    (project / "uv.lock").write_text('[[package]]\nname = "temporalio"\nversion = "1.26.0"\n')
    binary = cache / "temporal-test-server-sdk-python-1.26.0"
    binary.write_bytes(b"owned test server")
    binary.chmod(0o700)
    monkeypatch.setattr("devflow_temporal.delivery_sandbox.tempfile.gettempdir", lambda: str(cache))
    return project, binary, scratch, {"kind": "test", "argv": ["python", "-m", "pytest"]}


def test_locked_test_server_is_copied_into_private_temp_with_provenance(cached):
    project, binary, scratch, check = cached
    _seed_temporal_test_server(project, check, scratch)
    target = scratch / binary.name
    assert target.read_bytes() == binary.read_bytes()
    assert target.stat().st_mode & 0o777 == 0o700
    receipt = json.loads((scratch / "temporal-test-server-source.json").read_text())
    assert receipt["sha256"] == hashlib.sha256(binary.read_bytes()).hexdigest()
    assert receipt["source"] == str(binary) and receipt["path"] == str(target)
    before = target.stat().st_ino
    _seed_temporal_test_server(project, check, scratch)
    assert target.stat().st_ino == before


@pytest.mark.parametrize("change", ["version", "symlink", "writable", "missing", "not_pytest"])
def test_test_server_cache_does_not_adopt_unmatched_or_unsafe_entries(cached, change):
    project, binary, scratch, check = cached
    if change == "version":
        (project / "uv.lock").write_text('[[package]]\nname = "temporalio"\nversion = "1.27.0"\n')
    elif change == "symlink":
        destination = binary.with_suffix(".real")
        binary.rename(destination)
        binary.symlink_to(destination)
    elif change == "writable":
        binary.chmod(0o777)
    elif change == "missing":
        binary.unlink()
    else:
        check["argv"] = ["python", "--version"]
    _seed_temporal_test_server(project, check, scratch)
    assert not list(scratch.iterdir())
