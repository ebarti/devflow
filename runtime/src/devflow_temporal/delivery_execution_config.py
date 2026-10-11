"""Authenticate admitted execution configuration independently of live operations."""

from __future__ import annotations

import errno
import json
import os
import re
import stat
import tempfile
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_config import DeliveryConfig


def transport_binding(config: DeliveryConfig) -> tuple:
    """Shared persistence does not grant another service's transport."""
    return (
        config.tracking_db.resolve(), config.state_root.resolve(),
        config.temporal_address, config.raw.get("temporal_namespace", "default"),
        config.queue, config.dashboard_url.rstrip("/"),
    )


def _owned_directory(path: Path, *, create: bool = False) -> None:
    if create:
        path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o700):
        raise ValueError("execution configuration requires an owned private directory")


def _read_snapshot(path: Path) -> dict:
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
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink not in (1, 2)):
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
            return json.load(stream)
    finally:
        os.close(descriptor)


def preserve_execution_config(config: DeliveryConfig) -> None:
    """Only admission/service initialization publishes immutable private snapshots."""
    # Authenticate the same complete schema as a live config, without rereading a
    # mutable path after a service has already loaded it.
    DeliveryConfig._from_raw(config.path, config.raw)
    _owned_directory(config.state_root)
    folder = config.state_root / "configuration-snapshots"
    _owned_directory(folder, create=True)
    path = folder / (digest(config.raw) + ".json")
    if path.exists() or path.is_symlink():
        if _read_snapshot(path) != config.raw:
            raise ValueError("immutable admitted configuration changed")
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
        if _read_snapshot(path) != config.raw:
            raise ValueError("immutable admitted configuration changed")
        directory = os.open(folder, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if stage.exists():
            stage.unlink()


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
    path = state.parent.parent / "configuration-snapshots" / (expected + ".json")
    if path.exists() or path.is_symlink():
        raw = _read_snapshot(path)
        validated = DeliveryConfig._from_raw(source, raw)
        config = DeliveryConfig(path=source, raw=validated.raw)
    else:
        # Legacy runs are usable only while an authentic matching original still
        # exists. Loading never seeds a snapshot or edits a historic spec.
        config = DeliveryConfig.load(source)
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
