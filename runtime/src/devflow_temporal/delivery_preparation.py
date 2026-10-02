"""Runtime-owned image readiness, measured environment evidence and run binding."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import stat
import subprocess
import time
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any

from .contracts import canonical_json, digest
from .delivery_config import (
    REQUIRED_CODEX_VERSION,
    _contained_denied,
    _contained_probe_passed,
    _container_identity,
)
from .delivery_container import Bind, OwnedContainer, _docker
from .delivery_sandbox import prepare_native_role
from .payload import payload_digest

SCHEMA = "devflow-prepared-environment-v1"
KIT_REVISION = "d9ed6e186ce028d0db3b044ce959a94f409510c5"
CODEX_BINARY = "/opt/devflow-venv/lib/python3.12/site-packages/codex_cli_bin/bin/codex"
CODEX_BINARY_SHA256 = "9cbc3cdcc18ca336523ffa7d64207a1ae1f5991f823081d0a37bcb3a748de093"
PACKAGE = Path(__file__).resolve().parent
RUNTIME = PACKAGE.parents[1]
SECCOMP = RUNTIME / "docker/moby-56be731-codex-bwrap-seccomp.json"
LAUNCHER = RUNTIME / "docker/landlock_exec.py"


class PreparationError(RuntimeError):
    """Preparation could not establish its actual execution boundary."""


def _hash(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _directory(path: Path) -> None:
    if not path.parent.exists():
        _directory(path.parent)
    path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_mode & 0o777 != 0o700
    ):
        raise PreparationError("preparation directory must be private and owned")


def _private_bytes(path: Path, root: Path) -> bytes:
    _directory(root)
    if not path.is_absolute() or not path.is_relative_to(root):
        raise PreparationError("preparation evidence escaped its owned directory")
    current = root
    for part in path.relative_to(root).parts[:-1]:
        current /= part
        _directory(current)
    info = path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_nlink != 1
    ):
        raise PreparationError("preparation evidence must be an owned private regular file")
    return path.read_bytes()


def _write(path: Path, value: Any) -> None:
    _directory(path.parent)
    content = (canonical_json(value) + "\n").encode()
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def _lock(root: Path):
    _directory(root)
    descriptor = os.open(root / "preparation.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
        ):
            raise PreparationError("preparation lock is not private and owned")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


def _docker_json(container: dict, *argv: str) -> Any:
    result = _docker(
        container["docker_bin"],
        *argv,
        expected_sha256=container["docker_bin_sha256"],
    )
    if result.returncode:
        raise PreparationError("Docker readback failed; inspect the owned preparation evidence")
    try:
        return json.loads(result.stdout)
    except ValueError as exc:
        raise PreparationError("Docker returned malformed preparation readback") from exc


def _local_endpoint(container: dict) -> str:
    context = _docker_json(container, "context", "inspect")
    try:
        endpoint = context[0]["Endpoints"]["docker"]["Host"]
    except (KeyError, TypeError, IndexError) as exc:
        raise PreparationError("Docker context has no known local endpoint") from exc
    allowed = {
        "unix://" + str(Path.home() / ".docker/run/docker.sock"),
        "unix:///var/run/docker.sock",
    }
    if endpoint not in allowed or os.environ.get("DOCKER_HOST", endpoint) not in allowed:
        raise PreparationError("automatic preparation requires the configured local Docker Desktop")
    # DOCKER_HOST overrides context selection for actual daemon operations.
    return os.environ.get("DOCKER_HOST", endpoint)


def _engine(container: dict, *, start: bool) -> dict:
    endpoint = _local_endpoint(container)
    result = _docker(
        container["docker_bin"],
        "info",
        "--format",
        "{{json .}}",
        expected_sha256=container["docker_bin_sha256"],
        timeout=10,
    )
    if result.returncode and start:
        if not Path("/Applications/Docker.app").is_dir():
            raise PreparationError("install Docker Desktop before preparing a contained delivery")
        result = _docker(
            container["docker_bin"],
            "desktop",
            "start",
            "--timeout",
            "90",
            expected_sha256=container["docker_bin_sha256"],
            timeout=95,
        )
        if result.returncode:
            raise PreparationError("Docker Desktop did not start within 90 seconds")
        result = _docker(
            container["docker_bin"],
            "info",
            "--format",
            "{{json .}}",
            expected_sha256=container["docker_bin_sha256"],
            timeout=10,
        )
    if result.returncode:
        raise PreparationError("the local Docker Desktop daemon is unavailable")
    try:
        info = json.loads(result.stdout)
    except ValueError as exc:
        raise PreparationError("Docker Desktop returned malformed engine identity") from exc
    if info.get("OperatingSystem") != "Docker Desktop" or info.get("Architecture") != "aarch64":
        raise PreparationError("Docker daemon is outside the tested local Desktop boundary")
    fields = (
        "ID",
        "ServerVersion",
        "KernelVersion",
        "OperatingSystem",
        "Architecture",
        "SecurityOptions",
    )
    if any(not info.get(field) for field in fields):
        raise PreparationError("Docker engine identity is incomplete")
    return {"endpoint": endpoint, **{field: info[field] for field in fields}}


def _launch_policy(spec: dict) -> dict:
    configured = spec["policy"]["container"]
    binary = Path(configured["docker_bin"]).resolve(strict=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise PreparationError("configured Docker executable is unavailable")
    lock = Path(spec["source_path"]) / "pnpm-lock.yaml"
    if lock.is_symlink() or not lock.is_file():
        raise PreparationError("configured repository has no regular pinned package lock")
    lock_sha = _hash(lock)
    if configured.get("pnpm_lock_sha256", lock_sha) != lock_sha:
        raise PreparationError("configured repository package lock changed")
    base_lock = subprocess.run(
        ["git", "-C", spec["source_path"], "show", spec["base_sha"] + ":pnpm-lock.yaml"],
        capture_output=True,
        check=False,
        timeout=30,
    )
    if base_lock.returncode or hashlib.sha256(base_lock.stdout).hexdigest() != lock_sha:
        raise PreparationError("package lock differs from the admitted Git base")
    return {
        "docker_bin": configured["docker_bin"],
        "docker_bin_sha256": _hash(binary),
        "platform": "linux/arm64",
        "seccomp_profile": str(SECCOMP),
        "seccomp_sha256": _hash(SECCOMP),
        "role_runner_sha256": _hash(PACKAGE / "role_runner.py"),
        "runtime_payload_sha256": payload_digest(PACKAGE, LAUNCHER),
        "codex_bin": CODEX_BINARY,
        "codex_bin_sha256": CODEX_BINARY_SHA256,
        "pnpm_lock_sha256": lock_sha,
        **{
            key: configured.get(key, default)
            for key, default in (("memory", "2g"), ("cpus", "2"), ("pids_limit", 256))
        },
        **(
            {"prefetch_timeout_seconds": configured["prefetch_timeout_seconds"]}
            if "prefetch_timeout_seconds" in configured
            else {}
        ),
    }


def _resolve_image(container: dict, root: Path) -> dict:
    expected = {
        "devflow.role_runner_sha256": container["role_runner_sha256"],
        "devflow.runtime_payload_sha256": container["runtime_payload_sha256"],
        "devflow.codex_bin_sha256": CODEX_BINARY_SHA256,
        "devflow.kit_revision": KIT_REVISION,
        "devflow.codex_cli_version": REQUIRED_CODEX_VERSION,
        "devflow.dockerfile_sha256": _hash(RUNTIME / "docker/Dockerfile"),
    }
    _directory(root / "images")
    folder = root / "images" / digest(expected)
    _directory(folder)
    record = folder / "image.json"
    image_id = None
    if record.exists() or record.is_symlink():
        saved = json.loads(_private_bytes(record, root))
        if saved.get("labels") != expected:
            raise PreparationError("owned image build record changed")
        image_id = saved.get("image_id")
        inspected = _docker(
            container["docker_bin"],
            "image",
            "inspect",
            image_id,
            expected_sha256=container["docker_bin_sha256"],
        )
        if inspected.returncode:
            image_id = None
    if image_id is None:
        # Send only the reviewed payload to Docker, never the target repository
        # or unrelated files in the development checkout.
        context = folder / "context"
        _directory(context)
        package_target = context / "runtime/src/devflow_temporal"
        _directory(package_target)
        for source in PACKAGE.rglob("*.py"):
            target = package_target / source.relative_to(PACKAGE)
            _directory(target.parent)
            if target.is_symlink():
                raise PreparationError("owned image context was replaced with a linked file")
            shutil.copyfile(source, target)
        launcher = context / "runtime/docker/landlock_exec.py"
        _directory(launcher.parent)
        if launcher.is_symlink():
            raise PreparationError("owned image launcher was replaced with a linked file")
        shutil.copyfile(LAUNCHER, launcher)
        tag = "devflow-prepared:" + digest(expected)
        argv = ["build", "--platform", "linux/arm64"]
        for key, value in (
            ("ROLE_RUNNER_SHA256", container["role_runner_sha256"]),
            ("RUNTIME_PAYLOAD_SHA256", container["runtime_payload_sha256"]),
            ("CODEX_BIN_SHA256", CODEX_BINARY_SHA256),
            ("DOCKERFILE_SHA256", expected["devflow.dockerfile_sha256"]),
        ):
            argv.extend(("--build-arg", f"{key}={value}"))
        argv.extend(("-f", str(RUNTIME / "docker/Dockerfile"), "-t", tag, str(context)))
        built = _docker(
            container["docker_bin"],
            *argv,
            expected_sha256=container["docker_bin_sha256"],
            timeout=1800,
        )
        _write(
            folder / "build-output.json",
            {
                "exit_code": built.returncode,
                "stdout": built.stdout.decode(errors="replace")[-20000:],
                "stderr": built.stderr.decode(errors="replace")[-20000:],
            },
        )
        if built.returncode:
            excerpt = (built.stderr or built.stdout).decode(errors="replace")[-500:].strip()
            raise PreparationError(
                f"pinned runtime image build failed: {excerpt}; evidence: {folder}"
            )
        image_id = _docker_json(container, "image", "inspect", tag)[0]["Id"]
    values = _docker_json(container, "image", "inspect", image_id)
    image = values[0]
    labels = image.get("Config", {}).get("Labels") or {}
    if (
        len(values) != 1
        or image.get("Id") != image_id
        or image.get("Os") != "linux"
        or image.get("Architecture") != "arm64"
        or any(labels.get(key) != value for key, value in expected.items())
    ):
        raise PreparationError("runtime image differs from its pinned build and executable policy")
    _write(record, {"image_id": image_id, "labels": expected})
    return {
        **container,
        "image_id": image_id,
        "dockerfile_sha256": expected["devflow.dockerfile_sha256"],
    }


def environment_identity(spec: dict, container: dict, engine: dict) -> dict:
    """Canonical container paths make this identity independent of task IDs."""

    return {
        "schema": SCHEMA,
        "container": _container_identity(container, source=Path(spec["source_path"])),
        "dockerfile_sha256": _hash(RUNTIME / "docker/Dockerfile"),
        "engine": engine,
        "roles": spec["policy"]["roles"],
        "config_overrides": spec["policy"]["config_overrides"],
        "limits": {key: container[key] for key in ("memory", "cpus", "pids_limit")},
        "browser_ports": sorted(
            (spec["policy"].get("browser_qa") or {})
            .get("ports", {"api": 18931, "web": 18932})
            .values()
        ),
    }


def _boundary_passed(mode: str, observed: dict, ports: list[int]) -> bool:
    role = mode.startswith("role")
    if not _contained_probe_passed(observed, role=role):
        return False
    for value in (observed, observed["child"]):
        if role:
            if not _contained_denied(value.get("git_read")):
                return False
            if any(
                not _contained_denied(value.get(key))
                for key in ("temporary_write", "slash_tmp_write")
            ):
                return False
            if mode == "role-read" and not _contained_denied(value.get("workspace_write")):
                return False
            if mode == "role-write" and value.get("workspace_write") != "ALLOWED":
                return False
        elif value.get("workspace_write") != "ALLOWED" or value.get("git_read") != "ALLOWED":
            return False
        if mode == "browser-qa" and value.get("owned_ports") != ports:
            return False
    return True


def _reference(path: Path) -> dict:
    return {"path": str(path), "sha256": _hash(path)}


def _probe_container(
    spec: dict, mode: str, folder: Path, command: tuple[str, ...]
) -> OwnedContainer:
    role = mode.startswith("role")
    checkout, state = Path(spec["checkout"]), Path(spec["state_dir"])
    if role:
        request = {
            "spec": spec,
            "role": "intake" if mode == "role-read" else "implement",
            "iteration": 0,
            "workspace": str(checkout),
        }
        _, env = prepare_native_role(request, folder, containerized=True)
        home = state / "role-homes" / request["role"]
        if mode == "role-read":
            home /= "0"
        binds = (
            Bind(checkout, "/work", mode == "role-read"),
            Bind(home, "/rolehome"),
            Bind(folder, "/attempt"),
            Bind(state / "recovery", "/recovery", True),
        )
        command = (CODEX_BINARY, "sandbox", "-P", "devflow-role", "-C", "/work", "--", *command)
    else:
        env = {
            "HOME": "/tmp",
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "PYTHONPATH": "/opt/devflow-runtime/src",
            "PYTHONDONTWRITEBYTECODE": "1",
            "GIT_DIR": "/gitmeta",
            "GIT_WORK_TREE": "/work",
            "GIT_OPTIONAL_LOCKS": "0",
        }
        binds = (
            Bind(checkout, "/work"),
            Bind(checkout / ".git", "/work/.git", True),
            Bind(state / "gitmeta", "/gitmeta", True),
        )
        ports = sorted(
            (spec["policy"].get("browser_qa") or {})
            .get("ports", {"api": 18931, "web": 18932})
            .values()
        )
        command = (
            "/usr/bin/python3",
            "/opt/devflow/landlock_exec.py",
            *(
                item
                for port in ports
                if mode == "browser-qa"
                for item in ("--allow-port", str(port))
            ),
            "--",
            *command,
        )
    return OwnedContainer(
        spec,
        kind="role" if role else mode,
        identity={"probe": mode, "stage": folder.name},
        evidence_dir=folder / "container",
        binds=binds,
        command=command,
        cwd="/work",
        environment=env,
        network="bridge" if role else "none",
        timeout_seconds=90,
    )


def measure_environment(spec: dict, root: Path, fingerprint: str, identity: dict) -> dict:
    """Use production launch/profile primitives against fixed disposable canaries."""

    _directory(root / "probes")
    probe_root = root / "probes" / fingerprint
    state, checkout = probe_root / "runs/probe", probe_root / "checkouts/probe"
    protected = probe_root / "runs/protected"
    for folder in (probe_root, state, checkout, protected, state / "recovery", state / "gitmeta"):
        _directory(folder)
    for name in ("credential", "state", "outside"):
        _write(protected / name, "SAFE")
    _write(checkout / ".git", "SAFE")
    probe = {
        **spec,
        "run_id": "probe-" + fingerprint[:24],
        "state_dir": str(state),
        "checkout": str(checkout),
        "policy": deepcopy(spec["policy"]),
    }
    # This fixed controller-created fixture is the producer of preparation,
    # never a public submitted run or a candidate role that can skip it.
    probe.pop("preparation_version", None)
    probe.pop("preparation", None)
    probe["policy"]["host_sandbox"] = "native-profile"
    probe["policy_digest"] = digest(probe["policy"])
    ports = identity["browser_ports"]
    measurements = {}
    for mode in ("role-write", "role-read", "check", "browser-qa"):
        folder = state / mode / "boundary"
        _directory(folder)
        _write(folder / "protected-state", "SAFE")
        output = "/rolehome/boundary.json" if mode.startswith("role") else f"/work/{mode}.json"
        command = (
            "/usr/bin/python3",
            "/opt/devflow-runtime/src/devflow_temporal/preparation_probe.py",
            mode,
            "--protected",
            str(protected),
            "--output",
            output,
            *(item for port in ports if mode == "browser-qa" for item in ("--port", str(port))),
        )
        execution = _probe_container(probe, mode, folder, command)
        outcome = execution.run()
        if mode.startswith("role"):
            home = state / "role-homes" / ("intake/0" if mode == "role-read" else "implement")
            observed_path = home / "boundary.json"
        else:
            observed_path = checkout / f"{mode}.json"
        if outcome.exit_code != 0 or outcome.cleanup != "confirmed" or not observed_path.is_file():
            excerpt = outcome.log.read_text(errors="replace")[-500:].strip()
            raise PreparationError(f"{mode} boundary probe failed: {excerpt}; evidence: {folder}")
        observed = json.loads(observed_path.read_bytes())
        if not _boundary_passed(mode, observed, ports):
            raise PreparationError(
                f"{mode} parent/child boundary denial failed: "
                f"{canonical_json(observed)[:500]}; evidence: {folder}"
            )
        snapshot = folder / "observed.json"
        _write(snapshot, observed)
        measurements[mode] = {
            "exit_code": outcome.exit_code,
            "cleanup": outcome.cleanup,
            "container_id": outcome.container_id,
            "observed": _reference(snapshot),
            "log": _reference(outcome.log),
            "intent": _reference(folder / "container/container-intent.json"),
        }
        if _private_bytes(folder / "protected-state", root) != b'"SAFE"\n':
            raise PreparationError("role probe changed protected controller state")
    detached = {}
    for mode in ("role-write", "check", "browser-qa"):
        folder = state / mode / "detached"
        _directory(folder)
        heartbeat = "/rolehome/heartbeat" if mode.startswith("role") else f"/work/{mode}-heartbeat"
        child = (
            "import pathlib,time\n"
            + f"p=pathlib.Path({heartbeat!r})\n"
            + "for n in range(300):\n p.write_text(str(n))\n time.sleep(0.1)\n"
        )
        launcher = (
            "import subprocess,time\n"
            + f"subprocess.Popen(['/usr/bin/python3','-c',{child!r}],start_new_session=True,"
            + "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)\n"
            + "time.sleep(0.4)\n"
        )
        execution = _probe_container(probe, mode, folder, ("/usr/bin/python3", "-c", launcher))
        outcome = execution.run()
        heartbeat_path = (
            state / "role-homes/implement/heartbeat"
            if mode.startswith("role")
            else checkout / f"{mode}-heartbeat"
        )
        if outcome.exit_code != 0 or outcome.cleanup != "confirmed" or not heartbeat_path.is_file():
            raise PreparationError(f"{mode} detached cleanup probe failed; evidence: {folder}")
        stopped = heartbeat_path.read_bytes()
        time.sleep(0.4)
        if heartbeat_path.read_bytes() != stopped:
            raise PreparationError(f"{mode} detached descendant survived namespace teardown")
        replay = execution.run()
        if replay.container_id != outcome.container_id or replay.log_sha256 != outcome.log_sha256:
            raise PreparationError("detached probe replay did not observe its original execution")
        detached[mode] = {
            "exit_code": outcome.exit_code,
            "cleanup": outcome.cleanup,
            "container_id": outcome.container_id,
            "replay_container_id": replay.container_id,
            "heartbeat_before": stopped.decode(),
            "heartbeat_after": heartbeat_path.read_text(),
            "log": _reference(outcome.log),
            "intent": _reference(folder / "container/container-intent.json"),
        }
    if any(
        _private_bytes(protected / name, root) != b'"SAFE"\n'
        for name in ("credential", "state", "outside")
    ):
        raise PreparationError("boundary probe changed a protected host canary")
    return {
        "schema": SCHEMA,
        "fingerprint": fingerprint,
        "identity": identity,
        "measurements": measurements,
        "detached": detached,
    }


def validate_environment(proof: dict, root: Path, identity: dict) -> None:
    if (
        proof.get("schema") != SCHEMA
        or proof.get("identity") != identity
        or proof.get("fingerprint") != digest(identity)
        or set(proof.get("measurements", {})) != {"role-write", "role-read", "check", "browser-qa"}
        or set(proof.get("detached", {})) != {"role-write", "check", "browser-qa"}
    ):
        raise PreparationError("measured preparation proof is stale or incomplete")
    for group in ("measurements", "detached"):
        for mode, item in proof[group].items():
            if (
                item.get("exit_code") != 0
                or item.get("cleanup") != "confirmed"
                or not item.get("container_id")
            ):
                raise PreparationError(
                    "preparation has no successful namespace cleanup measurement"
                )
            for key in ("log", "intent", *(("observed",) if group == "measurements" else ())):
                reference = item[key]
                content = _private_bytes(Path(reference["path"]), root)
                if hashlib.sha256(content).hexdigest() != reference["sha256"]:
                    raise PreparationError("measured preparation evidence changed")
            intent = json.loads(_private_bytes(Path(item["intent"]["path"]), root))
            if (
                intent.get("image_id") != identity["container"]["image_id"]
                or intent.get("seccomp_sha256") != identity["container"]["seccomp_sha256"]
            ):
                raise PreparationError("preparation evidence belongs to a different launch policy")
            if group == "measurements":
                observed = json.loads(_private_bytes(Path(item["observed"]["path"]), root))
                if not _boundary_passed(mode, observed, identity["browser_ports"]):
                    raise PreparationError("measured preparation boundary did not pass")
            elif (
                item.get("replay_container_id") != item["container_id"]
                or not item.get("heartbeat_before")
                or item.get("heartbeat_before") != item.get("heartbeat_after")
            ):
                raise PreparationError(
                    "preparation detached cleanup or replay evidence is incomplete"
                )


def run_binding(spec: dict) -> str:
    policy = {
        key: value
        for key, value in spec["policy"].items()
        if key not in {"security_binding_sha256", "environment_proof_sha256"}
    }
    keys = (
        "run_id",
        "work_id",
        "repository_key",
        "source_path",
        "origin_url",
        "github_repo",
        "base_sha",
        "base_ref",
        "branch",
        "state_dir",
        "checkout",
        "authorized_endpoint",
        "config_digest",
    )
    return digest({**{key: spec[key] for key in keys}, "policy": policy})


def bind_prepared_spec(
    spec: dict, container: dict, proof_path: Path, proof: dict, *, reused: bool
) -> dict:
    effective = deepcopy(spec)
    effective["policy"].update(
        {
            "container": container,
            "host_sandbox": "native-profile",
            "kit_revision": KIT_REVISION,
            "environment_proof_sha256": _hash(proof_path),
        }
    )
    effective["policy"]["security_binding_sha256"] = run_binding(effective)
    effective["policy_digest"] = digest(effective["policy"])
    effective["preparation"] = {
        "schema": SCHEMA,
        "fingerprint": proof["fingerprint"],
        "environment": _reference(proof_path),
        "security_binding_sha256": effective["policy"]["security_binding_sha256"],
        "cache_reused": reused,
    }
    return effective


def verify_prepared_spec(spec: dict) -> None:
    if spec.get("preparation_version") != 1:
        return  # Existing recorded runs retain their original attestation contract.
    prepared = spec.get("preparation")
    if not isinstance(prepared, dict) or prepared.get("schema") != SCHEMA:
        raise PreparationError("runtime preparation has not frozen this execution authority")
    container = spec["policy"]["container"]
    identity = environment_identity(spec, container, _engine(container, start=False))
    root = Path(spec["state_dir"]).parents[1] / "preparation"
    reference = prepared["environment"]
    path = Path(reference["path"])
    if path != root / "environments" / digest(identity) / "proof.json":
        raise PreparationError("prepared environment is outside its canonical owned cache")
    content = _private_bytes(path, root)
    if (
        hashlib.sha256(content).hexdigest() != reference["sha256"]
        or reference["sha256"] != spec["policy"].get("environment_proof_sha256")
        or prepared.get("fingerprint") != digest(identity)
        or run_binding(spec) != prepared.get("security_binding_sha256")
        or run_binding(spec) != spec["policy"].get("security_binding_sha256")
        or digest(spec["policy"]) != spec["policy_digest"]
    ):
        raise PreparationError("prepared execution authority changed")
    validate_environment(json.loads(content), root, identity)


def prepare_authority(store: Any, spec: dict) -> dict:
    if spec.get("preparation_version") != 1:
        return spec
    root = Path(spec["state_dir"]).parents[1] / "preparation"
    with _lock(root):
        frozen = store.prepared_spec(spec["run_id"])
        if frozen is not None:
            verify_prepared_spec(frozen)
            return frozen
        store.preparation_progress(
            spec["run_id"], "docker", "Checking the local Docker execution boundary"
        )
        container = _launch_policy(spec)
        engine = _engine(container, start=True)
        store.preparation_progress(spec["run_id"], "image", "Resolving the pinned runtime image")
        container = _resolve_image(container, root)
        identity = environment_identity(spec, container, engine)
        fingerprint = digest(identity)
        path = root / "environments" / fingerprint / "proof.json"
        reused = path.exists() or path.is_symlink()
        if reused:
            proof = json.loads(_private_bytes(path, root))
        else:
            store.preparation_progress(
                spec["run_id"],
                "boundary",
                "Measuring native parent and child boundaries and cleanup",
            )
            measured_spec = {**spec, "policy": {**spec["policy"], "container": container}}
            proof = measure_environment(measured_spec, root, fingerprint, identity)
            validate_environment(proof, root, identity)
            _write(path, proof)
        validate_environment(proof, root, identity)
        effective = bind_prepared_spec(spec, container, path, proof, reused=reused)
        verify_prepared_spec(effective)
        return store.freeze_preparation(spec, effective)
