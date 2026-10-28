"""Actual original-inode locks and legacy before-flock concurrency, without a service."""

from __future__ import annotations

import fcntl
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from devflow_temporal import delivery_runtime_service_guard as guard

ROOT = Path(__file__).resolve().parents[2]
BASE = "1ebfc71ea5522ee74dbf6cd820f01649e3197bca"
pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="macOS inode-wide lsof contract")

CHILD = r"""
import ast, contextlib, errno, fcntl, os, pathlib, stat, sys, time, types
from devflow_temporal.delivery_store import _private_directory
source=pathlib.Path(sys.argv[1]).read_text()
tree=ast.parse(source)
names={'_lifecycle_lock', '_remaining'}
nodes=[node for node in tree.body if isinstance(node,(ast.FunctionDef, ast.AsyncFunctionDef))
       and node.name in names]
assert {node.name for node in nodes} == names
scope=dict(os=os, stat=stat, errno=errno, fcntl=fcntl, time=time,
           _private_directory=_private_directory, contextmanager=contextlib.contextmanager,
           Path=pathlib.Path, DeliveryConfig=object)
exec(compile(ast.Module(body=nodes,type_ignores=[]),sys.argv[1],'exec'),scope)
root=pathlib.Path(sys.argv[2]); paused=sys.argv[3]=='paused'
original=fcntl.flock
if paused:
 def flock(fd, operation):
  (root/'validated').write_text(str(fd))
  while not (root/'resume').exists(): time.sleep(.01)
  return original(fd, operation)
 fcntl.flock=flock
config=types.SimpleNamespace(state_root=root)
try:
 with scope['_lifecycle_lock'](config,time.monotonic()-.01):
  (root/'legacy-acted').write_text('original lifecycle yielded after expired deadline')
except ValueError as exc:
 (root/'refused').write_text(str(exc))
"""


@pytest.fixture
def owned(tmp_path):
    tmp_path.chmod(0o700)
    path = tmp_path / "service.lock"
    path.write_bytes(b"")
    path.chmod(0o600)
    folder = tmp_path / "runtime-installations/owned"
    folder.mkdir(parents=True, mode=0o700)
    fd = os.open(path, os.O_RDWR | os.O_NONBLOCK | os.O_NOFOLLOW)
    try:
        config = SimpleNamespace(
            state_root=tmp_path, tracking_db=tmp_path / "workflow.sqlite3", path=tmp_path / "owner"
        )
        yield config, fd, guard.describe(config, "owned", fd)
    finally:
        os.close(fd)


def legacy(tmp_path):
    source = subprocess.check_output(
        ["git", "-C", str(ROOT), "show", BASE + ":runtime/src/devflow_temporal/delivery_control.py"]
    )
    path = tmp_path / "original-control.py"
    path.write_bytes(source)
    return path


def wait_for(path, child):
    end = time.monotonic() + 5
    while not path.exists():
        assert child.poll() is None, child.communicate()
        assert time.monotonic() < end, str(path)
        time.sleep(0.01)


def test_original_prevalidated_expired_fd_is_counted_and_can_act_after_abort(owned):
    config, fd, proof = owned
    root = config.state_root
    child = subprocess.Popen(
        [sys.executable, "-I", "-c", CHILD, str(legacy(root)), str(root), "paused"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for(root / "validated", child)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        guard.check(config, proof, fd, linked=False)
        os.link(proof["path"], proof["link"])
        guard.check(config, proof, fd, linked=True)
        with pytest.raises(ValueError, match="foreign service-lock FDs") as refused:
            guard.census(fd, proof)
        summary = re.search(r"\(count=(\d+), holders=(\d+:\d+(?:,\d+:\d+){0,7})\)",
                            str(refused.value))
        assert summary is not None and int(summary[1]) >= 1
        if int(summary[1]) <= 8:
            assert f"{child.pid}:{(root / 'validated').read_text()}" in summary[2].split(",")
        assert str(root) not in str(refused.value)
        fresh = subprocess.run(
            [sys.executable, "-I", "-c", CHILD, str(legacy(root)), str(root), "fresh"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        assert fresh.returncode == 0, fresh.stderr
        assert "owned private file" in (root / "refused").read_text()
        assert not (root / "legacy-acted").exists()
        # An unfinished migration preserves program/config preimages, but its
        # released EX cannot prohibit this already validated legacy descriptor.
        fcntl.flock(fd, fcntl.LOCK_UN)
        (root / "resume").touch()
        out, err = child.communicate(timeout=5)
        assert child.returncode == 0, out + err
        assert (root / "legacy-acted").is_file()
        assert Path(proof["link"]).stat().st_nlink == 2
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=5)


def test_unacknowledged_two_name_lock_never_grants_current_lifecycle(owned):
    from devflow_temporal.delivery_control import _lifecycle_lock

    config, fd, proof = owned
    os.link(proof["path"], proof["link"])
    with pytest.raises(ValueError, match="no acknowledged installation authority"):
        with _lifecycle_lock(config, time.monotonic() + 1):
            pytest.fail("A file-only guard cannot authorize lifecycle effects")
