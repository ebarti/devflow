"""Best-effort local wakeups; committed SQLite events remain authoritative."""

from __future__ import annotations

import hashlib
import logging
import os
import selectors
import socket
import stat
from contextlib import ExitStack, contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)


def endpoint(database: Path) -> Path:
    # macOS has a short AF_UNIX path limit; the user's TMPDIR can exceed it.
    identity = hashlib.sha256(str(database.resolve()).encode()).hexdigest()[:24]
    return Path("/tmp") / f"devflow-sync-{os.getuid()}" / (identity + ".sock")


def notify(database: Path) -> None:
    """Never wait for the consumer, GitHub, or a delivery acknowledgement."""
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as client:
            client.setblocking(False)
            client.sendto(b"feature_changed", str(endpoint(database)))
    except Exception:
        # Missing service, full queue, or transport failure cannot undo a commit.
        # The next startup/daily catch-up reads the durable events and current state.
        logger.debug("Project sync notification unavailable; committed event retained",
                     exc_info=True)


class Notifications:
    """Bound only while the synchronizer holds every configured owner lock."""

    @contextmanager
    def listen(self, databases: list[Path]):
        with ExitStack() as stack:
            self.selector = stack.enter_context(selectors.DefaultSelector())
            for database in databases:
                path = endpoint(database)
                path.parent.mkdir(mode=0o700, exist_ok=True)
                metadata = path.parent.lstat()
                if (not stat.S_ISDIR(metadata.st_mode)
                        or stat.S_IMODE(metadata.st_mode) != 0o700
                        or metadata.st_uid != os.getuid()):
                    raise ValueError("Project notification directory must be owned and private")
                if path.exists() or path.is_symlink():
                    metadata = path.lstat()
                    if not stat.S_ISSOCK(metadata.st_mode) or metadata.st_uid != os.getuid():
                        raise ValueError("refusing an unowned Project notification endpoint")
                    path.unlink()  # An owned socket left by a stopped/crashed consumer.
                server = stack.enter_context(socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM))
                server.bind(str(path))
                stack.callback(path.unlink, missing_ok=True)
                path.chmod(0o600)
                server.setblocking(False)
                self.selector.register(server, selectors.EVENT_READ)
            yield self

    def wait(self, timeout: float) -> bool:
        # Drain before the next reconciliation, never after it: a commit arriving
        # during reconciliation must cause another wakeup. Contents grant no authority.
        ready = self.selector.select(timeout)
        for key, _ in ready:
            for _ in range(1024):
                try:
                    key.fileobj.recv(4096)
                except BlockingIOError:
                    break
        return bool(ready)
