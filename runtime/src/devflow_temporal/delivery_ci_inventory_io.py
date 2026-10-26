"""Bounded read-only GitHub observations and private CI provenance bytes."""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import selectors
import stat
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path

from .delivery_resources import _ancestors, private_directory

MAX_TOTAL = 128 * 1024 * 1024
MAX_JSON = 2 * 1024 * 1024
MAX_SOURCE = 8 * 1024 * 1024
MAX_LOG = 16 * 1024 * 1024
MAX_FILES = 256
MAX_PAGES = 10


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def _stamp(info):
    if info is None:
        return None
    return (info.st_dev, info.st_ino, info.st_uid, info.st_mode, info.st_nlink,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _private(info, limit, *, links=(1,)):
    if (info is None or not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink not in links
            or info.st_size > limit):
        raise ValueError("CI inventory evidence is not a bounded private owned file")


@contextmanager
def owned_bytes(path, limit, *, expected=None):
    """The bytes and decode lifetime share one authenticated descriptor."""
    if type(limit) is not int or not 0 < limit <= MAX_LOG:
        raise ValueError("CI inventory evidence requires its fixed byte bound")
    path = Path(path)
    _ancestors(path)
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("CI inventory evidence path changed")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        _private(before, limit)
        if _stamp(path.lstat()) != _stamp(before):
            raise ValueError("CI inventory evidence name changed")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            raw = stream.read(limit + 1)
        if len(raw) != before.st_size or (expected is not None and sha256(raw) != expected):
            raise ValueError("CI inventory evidence bytes changed")
        yield raw
        _ancestors(path)
        if (_stamp(os.fstat(fd)) != _stamp(before)
                or _stamp(path.lstat()) != _stamp(before)):
            raise ValueError("CI inventory evidence generation changed")
    finally:
        os.close(fd)


def publish(root, raw, *, limit):
    """Publish one hash-named observation; only its exact staging prefix can resume."""
    if (type(limit) is not int or not 0 < limit <= MAX_LOG
            or not isinstance(raw, bytes) or len(raw) > limit):
        raise ValueError("CI inventory observation exceeds its byte bound")
    root = Path(root)
    if not root.is_absolute() or ".." in root.parts or root.resolve() != root:
        raise ValueError("CI inventory observation root is not canonical")
    private_directory(root)
    if root.resolve(strict=True) != root or stat.S_IMODE(root.stat().st_mode) != 0o700:
        raise ValueError("CI inventory observation directory changed")
    folder = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    directory = os.fstat(folder)
    identity = (directory.st_dev, directory.st_ino, directory.st_uid)
    digest = sha256(raw)
    path, stage = root / digest, root / (".stage-" + digest)
    try:
        names = os.listdir(folder)
        if (len(names) > MAX_FILES * 2 or any(not re.fullmatch(
                r"(?:\.stage-)?[0-9a-f]{64}", name) for name in names)):
            raise ValueError("CI inventory observation namespace is incomplete or excessive")
        def observed(name):
            try:
                return os.stat(name, dir_fd=folder, follow_symlinks=False)
            except FileNotFoundError:
                return None

        generations, current_bytes = set(), 0
        for name in names:
            info = observed(name)
            _private(info, MAX_LOG, links=(1, 2))
            if info.st_nlink == 2:
                plain = name.removeprefix(".stage-")
                other = observed(plain if name.startswith(".stage-") else ".stage-" + plain)
                if (other is None or other.st_nlink != 2
                        or (other.st_dev, other.st_ino) != (info.st_dev, info.st_ino)):
                    raise ValueError("CI inventory observation has a foreign hardlink")
            identity_key = (info.st_dev, info.st_ino)
            if identity_key not in generations:
                generations.add(identity_key)
                current_bytes += info.st_size
        existing = observed(digest) or observed(stage.name)
        if current_bytes + len(raw) - (existing.st_size if existing else 0) > MAX_TOTAL:
            raise ValueError("CI inventory retained observation bound exceeded")
        if observed(digest) is None and digest not in {
                name.removeprefix(".stage-") for name in names} and len({
                    name.removeprefix(".stage-") for name in names}) >= MAX_FILES:
            raise ValueError("CI inventory observation file bound exceeded")
        if observed(digest) is not None:
            # Crash after link publication but before removal of this exact
            # staging name. No other hardlink grants evidence authority.
            if observed(stage.name) is not None:
                a, b = observed(digest), observed(stage.name)
                _private(a, limit, links=(2,))
                _private(b, limit, links=(2,))
                if (a.st_dev, a.st_ino) != (b.st_dev, b.st_ino):
                    raise ValueError("CI inventory publication has a foreign staging link")
                os.unlink(stage.name, dir_fd=folder)
            with owned_bytes(path, limit, expected=digest) as observed:
                if observed != raw:
                    raise ValueError("CI inventory immutable observation changed")
        else:
            try:
                fd = os.open(stage.name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
                             | os.O_NONBLOCK | os.O_CLOEXEC, 0o600, dir_fd=folder)
            except FileExistsError:
                fd = os.open(stage.name, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK
                             | os.O_CLOEXEC, dir_fd=folder)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
                info = os.fstat(fd)
                _private(info, limit)
                if _stamp(observed(stage.name)) != _stamp(info):
                    raise ValueError("CI inventory staging generation changed")
                with os.fdopen(fd, "r+b", closefd=False) as stream:
                    prefix = stream.read(limit + 1)
                    if not raw.startswith(prefix):
                        raise ValueError("CI inventory staging bytes have different authority")
                    stream.write(raw[len(prefix):])
                    stream.flush()
                    os.fsync(fd)
                current = os.fstat(fd)
                if _stamp(observed(stage.name)) != _stamp(current) or current.st_size != len(raw):
                    raise ValueError("CI inventory staging name changed before publication")
                os.link(stage.name, path.name, src_dir_fd=folder, dst_dir_fd=folder,
                        follow_symlinks=False)
                os.unlink(stage.name, dir_fd=folder)
            finally:
                os.close(fd)
        now = root.lstat()
        if (now.st_dev, now.st_ino, now.st_uid) != identity:
            raise ValueError("CI inventory observation directory was replaced")
        os.fsync(folder)
        return {"path": str(path), "sha256": digest, "bytes": len(raw)}
    finally:
        os.close(folder)


class GitHubRead:
    """One finite observation, without dispatch/rerun/status-writing capability."""

    def __init__(self, *, repositories, timeout=120, runner=None):
        if (not isinstance(repositories, (set, frozenset)) or not 1 <= len(repositories) <= 32
                or any(not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", value)
                       for value in repositories)):
            raise ValueError("CI inventory requires its exact acknowledged repository set")
        self.repositories = frozenset(repositories)
        self.deadline = time.monotonic() + timeout
        self.total = 0
        self.runner = runner

    def _bytes(self, command, limit):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError("CI inventory total observation deadline exceeded")
        if self.runner is not None:
            # Controlled external boundary for owned tests only. Production
            # callers never accept a runner through a request or declaration.
            raw = self.runner(command, limit, remaining)
            if not isinstance(raw, bytes) or len(raw) > limit:
                raise ValueError("CI inventory response exceeds its byte bound")
            self.total += len(raw)
            if self.total > MAX_TOTAL:
                raise ValueError("CI inventory aggregate observation bound exceeded")
            return raw
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   close_fds=True)
        output, error = bytearray(), bytearray()
        try:
            with selectors.DefaultSelector() as selected:
                for pipe, target in ((process.stdout, output), (process.stderr, error)):
                    os.set_blocking(pipe.fileno(), False)
                    selected.register(pipe, selectors.EVENT_READ, target)
                while selected.get_map():
                    remaining = self.deadline - time.monotonic()
                    if remaining <= 0:
                        raise ValueError("CI inventory total observation deadline exceeded")
                    for event, _ in selected.select(min(1, remaining)):
                        chunk = os.read(event.fd, 65536)
                        if not chunk:
                            selected.unregister(event.fileobj)
                            continue
                        target = event.data
                        maximum = limit if target is output else 65536
                        self.total += len(chunk)
                        if len(target) + len(chunk) > maximum or self.total > MAX_TOTAL:
                            raise ValueError("CI inventory response or aggregate bound exceeded")
                        target.extend(chunk)
            if process.wait(timeout=max(0.01, self.deadline - time.monotonic())) != 0:
                raise ValueError("CI inventory GitHub read failed: "
                                 + error.decode("utf-8", errors="replace")[-500:])
            return bytes(output)
        finally:
            if process.poll() is None:
                process.kill()  # Only this held, direct, read-only child.
                process.wait(timeout=5)
            for pipe in (process.stdout, process.stderr):
                pipe.close()

    def get(self, endpoint, *, limit=MAX_JSON, raw=False):
        if (not isinstance(endpoint, str) or not endpoint.startswith("repos/")
                or not re.fullmatch(r"repos/[A-Za-z0-9_./?=&+-]+", endpoint)
                or ".." in endpoint
                or "/".join(endpoint.split("/")[1:3]) not in self.repositories):
            raise ValueError("CI inventory GitHub endpoint is invalid")
        command = ["gh", "api", "--method", "GET", "--hostname", "github.com", endpoint]
        if raw:
            command += ["-H", "Accept: application/vnd.github.raw+json"]
        content = self._bytes(command, limit)
        return content if raw else json.loads(content)

    def pages(self, endpoint, key, *, head=None):
        if "?" in endpoint or key not in {"check_runs", "workflow_runs", "jobs", "artifacts"}:
            raise ValueError("CI inventory pagination selector changed")
        if head is not None and not re.fullmatch(r"[0-9a-f]{40}", head):
            raise ValueError("CI inventory pagination head changed")
        result, expected = [], None
        for page in range(1, MAX_PAGES + 1):
            query = f"?per_page=100&page={page}" + ("&head_sha=" + head if head else "")
            observed = self.get(endpoint + query)
            if (not isinstance(observed, dict) or type(observed.get("total_count")) is not int
                    or not 0 <= observed["total_count"] <= MAX_PAGES * 100
                    or not isinstance(observed.get(key), list)
                    or len(observed[key]) > 100):
                raise ValueError("CI inventory page is incomplete or unbounded")
            if expected is None:
                expected = observed["total_count"]
            if observed["total_count"] != expected:
                raise ValueError("CI inventory changed during pagination")
            result.extend(observed[key])
            if len(result) == expected:
                identities = [item.get("id") for item in result if isinstance(item, dict)]
                if (len(identities) != expected or any(type(x) is not int or x < 1
                                                     for x in identities)
                        or len(set(identities)) != expected):
                    raise ValueError("CI inventory page identities are duplicated or malformed")
                return result
            if len(observed[key]) < 100 or len(result) > expected:
                raise ValueError("CI inventory pagination is truncated")
        raise ValueError("CI inventory pagination exceeds its complete finite bound")
