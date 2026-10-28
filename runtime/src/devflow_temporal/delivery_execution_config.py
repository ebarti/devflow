"""Authenticate admitted execution configuration independently of live operations."""

from __future__ import annotations

import errno
import json
import os
import re
import sqlite3
import stat
import tempfile
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_config import DeliveryConfig

TRANSPORT_FIELDS = ("tracking_db", "state_root", "temporal_address", "temporal_namespace", "queue")

_observation_mode = ContextVar("execution_configuration_observation", default=False)
_observation_scope = ContextVar("execution_configuration_observation_scope", default=None)


class _ObservationScope:
    def __init__(self):
        self.resources = {}
        self.lock = threading.RLock()
        self.closed = False

    def resource(self, key, factory):
        with self.lock:
            if self.closed:
                raise sqlite3.OperationalError("tracking observation scope is closed")
            if key not in self.resources:
                self.resources[key] = factory()
            return self.resources[key]

    def close(self):
        with self.lock:
            if self.closed:
                return
            self.closed = True
            resources = list(self.resources.values())
        failure = None
        for resource in reversed(resources):
            try:
                resource.close()
            except Exception as exc:
                if failure is None:
                    failure = exc
        if failure is not None:
            raise failure


def is_observation_mode() -> bool:
    return _observation_mode.get()


def observation_scope():
    return _observation_scope.get()


@contextmanager
def observation_mode():
    """Share read-only evidence and verify it before the outer observation returns."""
    token = _observation_mode.set(True)
    scope = _observation_scope.get()
    owner = scope is None
    scope_token = None
    if owner:
        scope = _ObservationScope()
        scope_token = _observation_scope.set(scope)
    failed = False
    try:
        yield
    except BaseException:
        failed = True
        raise
    finally:
        try:
            if owner:
                try:
                    scope.close()
                except Exception:
                    if not failed:
                        raise
        finally:
            if scope_token is not None:
                _observation_scope.reset(scope_token)
            _observation_mode.reset(token)


def transport_binding(config: DeliveryConfig) -> tuple:
    """Shared persistence does not grant another service's transport.

    The dashboard address is presentation, not transport: moving its port must
    not disown admitted work or fail pending question callbacks.
    """
    return (
        config.tracking_db.resolve(), config.state_root.resolve(),
        config.temporal_address, config.raw.get("temporal_namespace", "default"),
        config.queue,
    )


def transport_difference(frozen: DeliveryConfig, live: DeliveryConfig) -> list[str]:
    """Names of the transport fields whose admitted value differs from the live service."""
    return [name for name, before, after
            in zip(TRANSPORT_FIELDS, transport_binding(frozen), transport_binding(live),
                   strict=True)
            if before != after]


def transport_digest(config: DeliveryConfig) -> str:
    """Transport identity a running API can advertise without exposing its paths."""
    return digest([str(value) for value in transport_binding(config)])


def _owned_directory(path: Path, *, create: bool = False) -> None:
    """Require an owned private directory, repairing nothing but its mode.

    A backup restore or ``chmod -R`` changes modes without changing ownership
    or content; that drift heals in place. Foreign ownership, symlinks and
    non-directories never do.
    """
    if create:
        if _observation_mode.get():
            raise ValueError("configuration observation cannot create directories")
        path.mkdir(mode=0o700, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError as exc:
        if exc.errno in (errno.ELOOP, errno.ENOTDIR):
            raise ValueError(
                "execution configuration requires an owned private directory") from exc
        raise
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
            raise ValueError("execution configuration requires an owned private directory")
        if stat.S_IMODE(info.st_mode) != 0o700:
            if _observation_mode.get():
                raise ValueError("execution configuration directory permissions are not private")
            os.fchmod(descriptor, 0o700)
            if stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o700:
                raise ValueError("execution configuration requires an owned private directory")
    finally:
        os.close(descriptor)


def _read_snapshot(path: Path, expected: str) -> dict:
    """Authenticate one immutable snapshot by content; only benign mode drift heals."""
    _owned_directory(path.parent.parent)
    _owned_directory(path.parent)
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise ValueError("admitted configuration snapshot is a symlink") from exc
        raise
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_nlink not in (1, 2)):
            raise ValueError("admitted configuration snapshot is not a private owned file")
        if info.st_nlink == 2:
            # A no-replace publication briefly retains its private staging link.
            # Readers may authenticate that exact inode without mutating either
            # name; unrelated hardlinks never qualify as controller publication.
            attributable = False
            for stage in path.parent.glob(".configuration-*"):
                try:
                    staged = stage.lstat()
                except FileNotFoundError:
                    continue
                if (staged.st_dev, staged.st_ino) == (info.st_dev, info.st_ino):
                    attributable = True
                    break
            if not attributable and os.fstat(descriptor).st_nlink != 1:
                raise ValueError("admitted configuration has an unrelated hardlink")
        with os.fdopen(descriptor, "r", closefd=False) as stream:
            raw = json.load(stream)
        if not isinstance(raw, dict) or digest(raw) != expected:
            raise ValueError("immutable admitted configuration changed")
        if stat.S_IMODE(info.st_mode) != 0o600:
            if _observation_mode.get():
                raise ValueError("admitted configuration snapshot permissions are not private")
            # Content-authenticated, owner-owned mode drift is repaired in place.
            # A reader never republishes, replaces or widens an inode.
            os.fchmod(descriptor, 0o600)
            if stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600:
                raise ValueError("admitted configuration snapshot is not a private owned file")
        return raw
    finally:
        os.close(descriptor)


def verify_existing_execution_config(config: DeliveryConfig) -> None:
    """Prove admission can reuse its exact private snapshot without publishing one."""
    with observation_mode():
        DeliveryConfig._from_raw(config.path, config.raw)
        _owned_directory(config.state_root)
        folder = config.state_root / "configuration-snapshots"
        _owned_directory(folder)
        expected = digest(config.raw)
        _read_snapshot(folder / (expected + ".json"), expected)


def preserve_execution_config(config: DeliveryConfig) -> None:
    """Only admission/service initialization publishes immutable private snapshots."""
    if _observation_mode.get():
        raise ValueError("configuration observation cannot publish snapshots")
    # Authenticate the same complete schema as a live config, without rereading a
    # mutable path after a service has already loaded it.
    DeliveryConfig._from_raw(config.path, config.raw)
    _owned_directory(config.state_root)
    folder = config.state_root / "configuration-snapshots"
    _owned_directory(folder, create=True)
    expected = digest(config.raw)
    path = folder / (expected + ".json")
    if path.exists() or path.is_symlink():
        _read_snapshot(path, expected)
        return
    data = (canonical_json(config.raw) + "\n").encode()
    descriptor, name = tempfile.mkstemp(prefix=".configuration-", dir=folder)
    stage = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        # link publishes a completely written file without replacing any earlier
        # snapshot. Competing authenticated admissions may publish identical data.
        try:
            os.link(stage, path, follow_symlinks=False)
        except FileExistsError:
            pass
        stage.unlink()
        _read_snapshot(path, expected)
        directory = os.open(folder, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if stage.exists():
            stage.unlink()


def unavailable_configuration(expected: str, source: Path, state_root: Path) -> str:
    """Name the missing admitted snapshot and the one action that seeds it."""
    return (
        "admitted execution configuration is unavailable or changed: snapshot "
        f"{expected} is absent from {state_root / 'configuration-snapshots'} and {source} "
        "no longer matches it; restore that configuration file, run "
        f"`devflow-delivery --config {source} restart` once to seed the snapshot, "
        "then apply the edit again"
    )


def load_execution_config(spec: dict) -> DeliveryConfig:
    """Read exact admitted authority; never write or reconstruct historic inputs."""
    if digest(spec["policy"]) != spec["policy_digest"]:
        raise ValueError("admitted execution policy changed")
    expected = spec["config_digest"]
    state = Path(spec["state_dir"])
    if (not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected)
            or not state.is_absolute() or state.parent.name != "runs"
            or state.name != spec["run_id"] or ".." in state.parts):
        raise ValueError("admitted configuration identity is invalid")
    source = Path(spec["config_path"])
    if not source.is_absolute():
        raise ValueError("admitted configuration source must be absolute")
    root = state.parent.parent
    path = root / "configuration-snapshots" / (expected + ".json")
    if path.exists() or path.is_symlink():
        raw = _read_snapshot(path, expected)
        validated = DeliveryConfig._from_raw(source, raw)
        config = DeliveryConfig(path=source, raw=validated.raw)
    else:
        # Legacy runs are usable only while an authentic matching original still
        # exists. Loading never seeds a snapshot or edits a historic spec.
        try:
            config = DeliveryConfig.load(source)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise ValueError(unavailable_configuration(expected, source, root)) from exc
        if digest(config.raw) != expected:
            raise ValueError(unavailable_configuration(expected, source, root))
    if (digest(config.raw) != expected
            or config.state_root / "runs" / spec["run_id"] != state):
        raise ValueError("admitted execution configuration is unavailable or changed")
    return config


def operational_config(config: DeliveryConfig) -> DeliveryConfig:
    """Only operational controls may read a current compatible service config."""
    try:
        live = DeliveryConfig.load(config.path)
    except (OSError, ValueError, KeyError, TypeError):
        return config
    return live if transport_binding(live) == transport_binding(config) else config
