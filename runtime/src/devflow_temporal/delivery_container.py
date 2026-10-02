"""One durable Docker PID boundary for every candidate-controlled process.

The broker creates a deterministic, labelled container before authorizing its
start. A retry observes that container; it never launches a second copy after
an ambiguous start. Docker's private PID namespace is the teardown proof:
process-group sampling alone cannot account for detached descendants.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .contracts import canonical_json, digest


class ContainerUnknown(RuntimeError):
    """The broker cannot prove an execution outcome or namespace teardown."""


@dataclass(frozen=True)
class Bind:
    source: Path
    target: str
    readonly: bool = False


@dataclass(frozen=True)
class ContainerResult:
    container_id: str
    name: str
    exit_code: int
    log: Path
    log_sha256: str
    cleanup: str
    image_id: str
    seccomp_sha256: str


def _private_dir(path: Path) -> None:
    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    info = path.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or stat.S_IMODE(info.st_mode) != 0o700
        or info.st_uid != os.getuid()
    ):
        raise ValueError("container evidence directory is not private and owned")


def _write_once(path: Path, value: bytes) -> None:
    if path.exists() or path.is_symlink():
        info = path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_uid != os.getuid()
            or path.read_bytes() != value
        ):
            raise ContainerUnknown("durable container evidence changed")
        return
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(value)


def _sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _docker(
    binary: str, *argv: str, expected_sha256: str, timeout: int = 30
) -> subprocess.CompletedProcess[bytes]:
    try:
        executable = Path(binary).resolve(strict=True)
        if (
            not executable.is_file()
            or not os.access(executable, os.X_OK)
            or _sha256(executable) != expected_sha256
        ):
            raise ContainerUnknown("admitted Docker CLI changed before an operation")
    except OSError as exc:
        raise ContainerUnknown("admitted Docker CLI is unavailable") from exc
    try:
        return subprocess.run(
            [str(executable), *argv], capture_output=True, check=False, timeout=timeout
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ContainerUnknown("Docker daemon or CLI did not provide a bounded response") from exc


def _docker_checked(
    binary: str, *argv: str, expected_sha256: str, timeout: int = 30
) -> bytes:
    completed = _docker(binary, *argv, expected_sha256=expected_sha256, timeout=timeout)
    if completed.returncode:
        raise ContainerUnknown(
            "Docker operation failed: "
            + (completed.stderr or completed.stdout).decode("utf-8", errors="replace")[:500]
        )
    return completed.stdout


def dependency_volume(spec: dict[str, Any], lock_sha256: str) -> str:
    """Create or verify the one private, labelled package cache for a run."""

    policy = spec["policy"]["container"]
    binary = str(policy["docker_bin"])
    binary_sha256 = str(policy["docker_bin_sha256"])
    name = (
        "devflow-"
        + digest({"run_id": spec["run_id"], "policy": spec["policy_digest"], "lock": lock_sha256})[
            :32
        ]
    )
    labels = {
        "devflow.owner": "temporal-delivery",
        "devflow.policy": spec["policy_digest"],
        "devflow.purpose": "dependencies",
        "devflow.lock": lock_sha256,
    }
    command = ["volume", "create"]
    for key, value in sorted(labels.items()):
        command.extend(("--label", f"{key}={value}"))
    command.append(name)
    observed_name = _docker_checked(
        binary, *command, expected_sha256=binary_sha256
    ).decode().strip()
    if observed_name != name:
        raise ContainerUnknown("Docker returned a different dependency volume")
    inspected = _docker_checked(
        binary, "volume", "inspect", name, expected_sha256=binary_sha256
    )
    try:
        values = json.loads(inspected)
        volume = values[0]
    except (ValueError, IndexError, KeyError, TypeError) as exc:
        raise ContainerUnknown("Docker dependency volume inspection is malformed") from exc
    if (
        len(values) != 1
        or volume.get("Name") != name
        or volume.get("Driver") != "local"
        or any(volume.get("Labels", {}).get(key) != value for key, value in labels.items())
    ):
        raise ContainerUnknown("Docker dependency volume has a different owner or lock")
    return name


class OwnedContainer:
    """A single container whose identity, config and start are journalled."""

    def __init__(
        self,
        spec: dict[str, Any],
        *,
        kind: str,
        identity: dict[str, Any],
        evidence_dir: Path,
        binds: tuple[Bind, ...],
        command: tuple[str, ...],
        cwd: str,
        environment: dict[str, str],
        network: str,
        timeout_seconds: int,
        volume_mounts: tuple[tuple[str, str, bool], ...] = (),
    ) -> None:
        if spec.get("preparation_version") == 1 and not spec.get("preparation"):
            raise ValueError("runtime preparation has not frozen this execution authority")
        if network not in {"none", "bridge"} or not command or timeout_seconds < 1:
            raise ValueError("unsupported container execution settings")
        policy = spec["policy"].get("container")
        if not isinstance(policy, dict):
            raise ValueError("real execution requires an admitted container policy")
        self.spec = spec
        self.policy = policy
        self.binary = str(policy["docker_bin"])
        self.binary_sha256 = str(policy["docker_bin_sha256"])
        self.image_id = str(policy["image_id"])
        self.seccomp = Path(policy["seccomp_profile"])
        if _sha256(self.seccomp) != policy["seccomp_sha256"]:
            raise ContainerUnknown("container seccomp profile changed after admission")
        self.kind = kind
        self.identity = identity
        self.evidence_dir = evidence_dir
        _private_dir(evidence_dir)
        self.name = "devflow-" + digest({"run_id": spec["run_id"], "kind": kind, **identity})[:32]
        self.labels = {
            "devflow.owner": "temporal-delivery",
            "devflow.run_id": spec["run_id"],
            "devflow.kind": kind,
            "devflow.identity": digest(identity),
            "devflow.policy": spec["policy_digest"],
        }
        self.binds = binds
        self.command = command
        self.cwd = cwd
        self.environment = environment
        self.network = network
        self.timeout_seconds = timeout_seconds
        self.volume_mounts = volume_mounts
        self.intent = {
            "name": self.name,
            "image_id": self.image_id,
            "seccomp_sha256": policy["seccomp_sha256"],
            "labels": self.labels,
            "binds": [
                {"source": str(b.source), "target": b.target, "readonly": b.readonly} for b in binds
            ],
            "volumes": volume_mounts,
            "command": command,
            "cwd": cwd,
            "environment": environment,
            "network": network,
            "timeout_seconds": timeout_seconds,
        }
        _write_once(
            evidence_dir / "container-intent.json",
            (canonical_json(self.intent) + "\n").encode(),
        )

    def _inspect(self) -> dict[str, Any] | None:
        result = _docker(
            self.binary, "inspect", self.name, expected_sha256=self.binary_sha256
        )
        if result.returncode:
            if (
                b"no such object" in result.stderr.lower()
                or b"no such container" in result.stderr.lower()
            ):
                return None
            raise ContainerUnknown("Docker could not inspect the owned container")
        try:
            values = json.loads(result.stdout)
            if len(values) != 1 or not isinstance(values[0], dict):
                raise ValueError
            return values[0]
        except (json.JSONDecodeError, ValueError) as exc:
            raise ContainerUnknown("Docker returned an invalid container inspection") from exc

    def _validate_bind(self, bind: Bind) -> str:
        if (
            not bind.target.startswith("/")
            or ".." in Path(bind.target).parts
            or bind.target.split("/")[1]
            not in {"work", "rolehome", "attempt", "recovery", "evidence", "qa", "deps", "gitmeta"}
        ):
            raise ValueError("container bind target is invalid")
        if (bind.target == "/gitmeta" or bind.target == "/work/.git") and not bind.readonly:
            raise ValueError("container Git metadata must be read-only")
        raw_root = Path(self.spec["state_dir"]).parent.parent.absolute()
        raw_source = bind.source.absolute()
        try:
            relative_source = raw_source.relative_to(raw_root)
        except ValueError as exc:
            raise ValueError("container bind source is outside the owned state root") from exc
        current = raw_root
        for component in relative_source.parts:
            current /= component
            info = current.lstat()
            if stat.S_ISLNK(info.st_mode) or info.st_uid != os.getuid():
                raise ValueError("container bind has an unowned or linked source component")
        source = bind.source.resolve(strict=True)
        state_root = Path(self.spec["state_dir"]).parent.parent.resolve(strict=True)
        run_state = Path(self.spec["state_dir"]).resolve(strict=True)
        checkout = Path(self.spec["checkout"]).resolve(strict=True)
        if (
            source in {state_root, run_state, state_root / "checkouts", state_root / "runs"}
            or state_root not in source.parents
            or not (source == checkout or checkout in source.parents or run_state in source.parents)
        ):
            raise ValueError("container bind source escaped the owned state root")
        for path in (state_root, *reversed(source.relative_to(state_root).parents)):
            current = state_root if path == state_root else state_root / path
            info = current.lstat()
            if stat.S_ISLNK(info.st_mode) or info.st_uid != os.getuid():
                raise ValueError("container bind has an unowned or linked ancestor")
        info = source.lstat()
        if stat.S_ISLNK(info.st_mode) or info.st_uid != os.getuid():
            raise ValueError("container bind source is not owned or is a symlink")
        return f"type=bind,src={source},dst={bind.target}" + (",readonly" if bind.readonly else "")

    def _validate_volume(self, name: str, target: str, readonly: bool) -> None:
        if (
            not re.fullmatch(r"devflow-[a-z0-9-]{1,80}", name)
            or not target.startswith("/")
            or ".." in Path(target).parts
            or target.split("/")[1] not in {"store", "deps"}
        ):
            raise ValueError("container volume name or target is invalid")
        observed = _docker_checked(
            self.binary, "volume", "inspect", name,
            expected_sha256=self.binary_sha256,
        )
        try:
            values = json.loads(observed)
            volume = values[0]
        except (ValueError, IndexError, KeyError, TypeError) as exc:
            raise ContainerUnknown("Docker volume inspection is malformed") from exc
        if (
            len(values) != 1
            or volume.get("Name") != name
            or volume.get("Driver") != "local"
            or volume.get("Labels", {}).get("devflow.owner") != "temporal-delivery"
            or volume.get("Labels", {}).get("devflow.policy") != self.spec["policy_digest"]
            or volume.get("Labels", {}).get("devflow.purpose") != "dependencies"
        ):
            raise ContainerUnknown("Docker volume identity is not owned by this policy")

    def _create_argv(self) -> list[str]:
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", self.image_id):
            raise ValueError("container image must be an exact content ID")
        options = [
            "create",
            "--name",
            self.name,
            "--network",
            self.network,
            "--ipc",
            "private",
            "--init",
            "--read-only",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--security-opt",
            f"seccomp={self.seccomp}",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "--memory",
            str(self.policy.get("memory", "2g")),
            "--cpus",
            str(self.policy.get("cpus", "2")),
            "--pids-limit",
            str(self.policy.get("pids_limit", 256)),
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,size=128m",
            "--workdir",
            self.cwd,
        ]
        for key, value in sorted(self.labels.items()):
            options.extend(("--label", f"{key}={value}"))
        for bind in self.binds:
            options.extend(("--mount", self._validate_bind(bind)))
        for name, target, readonly in self.volume_mounts:
            self._validate_volume(name, target, readonly)
            options.extend(
                (
                    "--mount",
                    f"type=volume,src={name},dst={target}" + (",readonly" if readonly else ""),
                )
            )
        for key, value in sorted(self.environment.items()):
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) or "\x00" in value:
                raise ValueError("container environment is invalid")
            options.extend(("--env", f"{key}={value}"))
        options.append(self.image_id)
        options.extend(self.command)
        return options

    def _validate_inspection(self, inspected: dict[str, Any]) -> str:
        container_id = inspected.get("Id")
        config = inspected.get("Config") or {}
        host = inspected.get("HostConfig") or {}
        security = host.get("SecurityOpt") or []
        seccomp_values = [
            value.removeprefix("seccomp=") for value in security if value.startswith("seccomp=")
        ]
        try:
            seccomp_matches = len(seccomp_values) == 1 and json.loads(
                seccomp_values[0]
            ) == json.loads(self.seccomp.read_text())
        except (OSError, json.JSONDecodeError):
            seccomp_matches = False
        if (
            not isinstance(container_id, str)
            or not re.fullmatch(r"[0-9a-f]{64}", container_id)
            or inspected.get("Image") != self.image_id
            or config.get("User") != f"{os.getuid()}:{os.getgid()}"
            or config.get("WorkingDir") != self.cwd
            or config.get("Cmd") != list(self.command)
            or any(config.get("Labels", {}).get(key) != value for key, value in self.labels.items())
            or host.get("NetworkMode") != self.network
            or not host.get("ReadonlyRootfs")
            or host.get("Privileged")
            or host.get("PidMode") not in {"", "private"}
            or host.get("IpcMode") != "private"
            or host.get("Init") is not True
            or "ALL" not in host.get("CapDrop", [])
            or "no-new-privileges" not in security
            or not seccomp_matches
            or host.get("PortBindings") not in ({}, None)
            or host.get("PublishAllPorts")
        ):
            raise ContainerUnknown("Docker container identity or authority changed")
        actual_env = dict(value.split("=", 1) for value in config.get("Env", []) if "=" in value)
        if any(actual_env.get(key) != value for key, value in self.environment.items()):
            raise ContainerUnknown("Docker container environment changed")
        mounts = {
            (mount.get("Source"), mount.get("Destination"), mount.get("RW"))
            for mount in inspected.get("Mounts", [])
        }
        for bind in self.binds:
            expected = (str(bind.source.resolve(strict=True)), bind.target, not bind.readonly)
            if expected not in mounts:
                raise ContainerUnknown("Docker bind mount changed from its admitted source")
        for name, target, readonly in self.volume_mounts:
            matching = [
                mount for mount in inspected.get("Mounts", []) if mount.get("Destination") == target
            ]
            if (
                len(matching) != 1
                or matching[0].get("Type") != "volume"
                or matching[0].get("Name") != name
                or matching[0].get("RW") is not (not readonly)
            ):
                raise ContainerUnknown("Docker volume mount changed from its admitted identity")
            self._validate_volume(name, target, readonly)
        expected_targets = {bind.target for bind in self.binds} | {
            target for _, target, _ in self.volume_mounts
        }
        if {mount.get("Destination") for mount in inspected.get("Mounts", [])} != expected_targets:
            raise ContainerUnknown("Docker has an extra or missing mount")
        return container_id

    def _ensure(self) -> tuple[str, dict[str, Any]]:
        inspected = self._inspect()
        start = self.evidence_dir / "container-start-authorized.json"
        if inspected is None:
            if start.exists() or start.is_symlink():
                raise ContainerUnknown("started container disappeared before cleanup proof")
            created = (
                _docker_checked(
                    self.binary, *self._create_argv(),
                    expected_sha256=self.binary_sha256, timeout=90,
                ).decode().strip()
            )
            if not re.fullmatch(r"[0-9a-f]{64}", created):
                raise ContainerUnknown("Docker did not return a complete container ID")
            inspected = self._inspect()
            if inspected is None:
                raise ContainerUnknown("created Docker container disappeared")
        container_id = self._validate_inspection(inspected)
        _write_once(
            self.evidence_dir / "container-id.json",
            (canonical_json({"container_id": container_id, "name": self.name}) + "\n").encode(),
        )
        return container_id, inspected

    def run(self) -> ContainerResult:
        container_id, inspected = self._ensure()
        state = inspected.get("State") or {}
        status = state.get("Status")
        if status == "created":
            _write_once(
                self.evidence_dir / "container-start-authorized.json",
                (canonical_json({"container_id": container_id, "name": self.name}) + "\n").encode(),
            )
            _docker_checked(
                self.binary, "start", container_id,
                expected_sha256=self.binary_sha256, timeout=60,
            )
        elif status not in {"running", "exited"}:
            raise ContainerUnknown("owned container is neither created, running nor exited")
        if status != "exited":
            try:
                _docker_checked(
                    self.binary, "wait", container_id,
                    expected_sha256=self.binary_sha256, timeout=self.timeout_seconds,
                )
            except ContainerUnknown:
                _docker_checked(
                    self.binary, "stop", "--time", "2", container_id,
                    expected_sha256=self.binary_sha256, timeout=20,
                )
        inspected = self._inspect()
        if inspected is None or self._validate_inspection(inspected) != container_id:
            raise ContainerUnknown("Docker lost the owned container during execution")
        final = inspected.get("State") or {}
        if final.get("Status") != "exited" or final.get("Running") or final.get("Pid") != 0:
            raise ContainerUnknown("Docker has not confirmed private PID namespace teardown")
        code = final.get("ExitCode")
        if type(code) is not int:
            raise ContainerUnknown("Docker did not provide an exit code")
        logs = _docker(
            self.binary, "logs", "--timestamps", container_id,
            expected_sha256=self.binary_sha256, timeout=30,
        )
        if logs.returncode:
            raise ContainerUnknown("Docker could not read both container output streams")
        log_bytes = logs.stdout + (b"\n[stderr]\n" + logs.stderr if logs.stderr else b"")
        if len(log_bytes) > 20 * 1024 * 1024:
            raise ContainerUnknown("container log exceeded its bounded evidence size")
        log = self.evidence_dir / "container.log"
        _write_once(log, log_bytes)
        return ContainerResult(
            container_id=container_id,
            name=self.name,
            exit_code=code,
            log=log,
            log_sha256=_sha256(log),
            cleanup="confirmed",
            image_id=self.image_id,
            seccomp_sha256=self.policy["seccomp_sha256"],
        )
