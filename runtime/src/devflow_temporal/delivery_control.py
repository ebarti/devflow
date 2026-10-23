"""Persistent, locally owned Temporal, worker, and dashboard lifecycle."""

from __future__ import annotations

import argparse
import asyncio
import errno
import fcntl
import json
import os
import re
import shutil
import signal
import socket
import stat
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlsplit

import uvicorn
from temporalio.api.enums.v1 import TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest
from temporalio.client import Client
from temporalio.common import VersioningBehavior
from temporalio.worker import Worker, WorkerDeploymentConfig, WorkerDeploymentVersion

from .delivery_activities import DELIVERY_ACTIVITIES, DatabaseContentionInterceptor
from .delivery_api import create_app
from .delivery_client import DeliveryClient, ServiceUnavailable, read_only_client
from .delivery_client import client as api_client
from .delivery_codec import DELIVERY_DATA_CONVERTER
from .delivery_config import DeliveryConfig
from .delivery_execution_config import transport_digest
from .delivery_merge_workflow import MergeWorkflow
from .delivery_store import DeliveryStore, _private_directory
from .delivery_temporal_retention import ensure_namespace_retention
from .delivery_workflow import DeliveryWorkflow
from .payload import payload_digest
from .supervisor import _process_identity as _local_identity


def _config(path: str) -> DeliveryConfig:
    config = DeliveryConfig.load(Path(path).expanduser().resolve(strict=True))
    DeliveryStore(config)
    return config


def _import_closure(
    config: DeliveryConfig, run_id: str, workflow_id: str, history: Path,
) -> dict:
    """Seal an authentic exported history for a closure Temporal no longer retains."""
    from .delivery_closures import import_archived_closure

    store = DeliveryStore(config)

    async def run() -> dict:
        client = await Client.connect(
            config.temporal_address,
            namespace=config.raw.get("temporal_namespace", "default"),
            data_converter=DELIVERY_DATA_CONVERTER,
        )
        return await import_archived_closure(
            store, client, run_id=run_id, workflow_id=workflow_id,
            history_path=history.expanduser().resolve(strict=True),
        )

    return asyncio.run(run())


def _manifest(config: DeliveryConfig) -> Path:
    return config.state_root / "service-processes.json"


def _read_manifest(config: DeliveryConfig) -> dict | None:
    path = _manifest(config)
    if not path.exists():
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or not isinstance(value.get("processes"), dict):
        raise ValueError(f"invalid service process manifest: {path}")
    if set(value["processes"]) - {"temporal", "worker", "api"}:
        raise ValueError(f"unknown processes in service manifest: {path}")
    return value


def _recorded_deployment(manifest: dict) -> dict | None:
    worker = manifest.get("processes", {}).get("worker", {}).get("deployment")
    recorded = manifest.get("worker_deployment", worker)
    if "worker_deployment" in manifest and recorded is None:
        raise ValueError("recorded worker deployment is invalid")
    if worker is not None and recorded != worker:
        raise ValueError("worker deployment differs from its lifecycle selection")
    return recorded


def _validated_deployment(recorded: dict, *, same_payload: bool = True):
    if (not isinstance(recorded, dict)
            or not re.fullmatch(r"[a-f0-9]{64}", str(recorded.get("runtime_payload_sha256", "")))
            or (same_payload and recorded["runtime_payload_sha256"]
                != payload_digest(Path(__file__).resolve().parent))):
        raise ValueError(
            "recorded worker source payload differs or is unknown; "
            "retain the original artifact and drain it before changing")
    selected = _worker_deployment(recorded.get("name"), recorded.get("build_id"))
    if selected is None:
        raise ValueError("recorded worker deployment is invalid")
    return selected


def _write_manifest(
    config: DeliveryConfig, processes: dict, *, deployment: dict | None = None,
) -> None:
    path = _manifest(config)
    temporary = path.with_suffix(".tmp")
    if deployment is None:
        deployment = (processes.get("worker", {}).get("deployment")
                      or _recorded_deployment(_read_manifest(config) or {}))
    value = {"config_path": str(config.path), "processes": processes}
    if deployment is not None:
        value["worker_deployment"] = deployment
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def _deadline(config: DeliveryConfig) -> float:
    timeout = float(config.raw.get("service_start_timeout", 30))
    if not 0 < timeout <= 120:
        raise ValueError("service_start_timeout must be greater than 0 and at most 120 seconds")
    return time.monotonic() + timeout


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ValueError("local service startup timed out")
    return remaining


def _ensure_namespace_retention(config: DeliveryConfig, deadline: float) -> None:
    async def ensure() -> None:
        try:
            async with asyncio.timeout(_remaining(deadline)):
                client = await Client.connect(
                    config.temporal_address,
                    namespace=config.raw.get("temporal_namespace", "default"),
                    data_converter=DELIVERY_DATA_CONVERTER,
                )
                await ensure_namespace_retention(client)
        except Exception as exc:
            raise RuntimeError(f"Temporal retention startup check failed: {exc}") from exc

    asyncio.run(ensure())


@contextmanager
def _lifecycle_lock(config: DeliveryConfig, deadline: float):
    _private_directory(config.state_root)
    path = config.state_root / "service.lock"
    message = f"service lock must be an owned private file (0600): {path}"
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise ValueError(message) from exc
        raise
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
        ):
            raise ValueError(message)
        if stat.S_IMODE(metadata.st_mode) != 0o600:
            # Owner-owned mode drift (a restore, chmod -R) heals in place.
            os.fchmod(descriptor, 0o600)
            if stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600:
                raise ValueError(message)
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                time.sleep(min(0.05, _remaining(deadline)))
        yield
    finally:
        os.close(descriptor)


def _process_identity(pid: int) -> str | None:
    """A member's start identity, independent of the controller's locale and zone."""
    identity = _local_identity(pid, canonical=True)
    if identity is None:
        return None
    # A different launcher may own the exited child and not have reaped it yet.
    # Its start time still matches. Retain ownership if its group has live children.
    state = subprocess.run(
        ["ps", "-p", str(pid), "-o", "stat="],
        capture_output=True, text=True, check=False, timeout=5,
    )
    if state.returncode != 0 or not state.stdout.strip().startswith("Z"):
        return identity
    groups = subprocess.run(
        ["ps", "-eo", "pgid=,stat="], capture_output=True, text=True, check=False, timeout=5,
    )
    if groups.returncode != 0:
        return identity
    states = [parts[1] for line in groups.stdout.splitlines()
              if len(parts := line.split()) == 2 and parts[0] == str(pid)]
    return identity if not states or any(not state.startswith("Z") for state in states) else None


def _owned(process: dict) -> bool:
    if not isinstance(process, dict) or not isinstance(process.get("pid"), int):
        return False
    try:
        # Reap a directly launched child if it exited, including after SIGKILL.
        os.waitpid(process["pid"], os.WNOHANG)
    except ChildProcessError:
        pass
    recorded = process.get("identity")
    if not recorded:
        return False
    live = _process_identity(process["pid"])
    # Manifests written before identities were canonical hold the local form.
    return live is not None and (
        live == recorded or _local_identity(process["pid"]) == recorded
    )


def _free(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET) as sock:
        sock.settimeout(0.2)
        return sock.connect_ex((host, port)) != 0


def _wait_port(host: str, port: int, process: dict, *, deadline: float) -> None:
    while True:
        if not _owned(process):
            raise RuntimeError(f"service process for port {port} exited during startup")
        if not _free(host, port):
            return
        time.sleep(min(0.1, _remaining(deadline)))


def _ports(config: DeliveryConfig) -> tuple[str, int, int, int]:
    address = urlsplit(config.dashboard_url)
    if address.scheme != "http" or address.hostname not in {"127.0.0.1", "::1"}:
        raise ValueError("dashboard must bind to a loopback HTTP address")
    if not address.port or address.path or address.query or address.fragment:
        raise ValueError("dashboard URL must contain only a loopback host and port")
    temporal_host, _, temporal_port = config.temporal_address.rpartition(":")
    if temporal_host not in {"127.0.0.1", "::1"}:
        raise ValueError("Temporal must bind to a loopback address")
    ui_port = int(config.raw.get("temporal_ui_port", int(temporal_port) + 1000))
    return address.hostname, address.port, int(temporal_port), ui_port


def _launch(config: DeliveryConfig, name: str, argv: list[str]) -> dict:
    log = config.state_root / f"{name}.log"
    descriptor = os.open(log, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "ab") as stream:
        process = subprocess.Popen(
            argv,
            cwd=config.state_root,
            stdin=subprocess.DEVNULL,
            stdout=stream,
            stderr=stream,
            start_new_session=True,
        )
    try:
        for _ in range(20):
            identity = _process_identity(process.pid)
            if identity:
                return {"pid": process.pid, "identity": identity, "log": str(log)}
            if process.poll() is not None:
                raise RuntimeError(f"{name} exited before registering its process identity")
            time.sleep(0.05)
        raise RuntimeError(f"{name} process identity was unavailable")
    except BaseException:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        raise


def _terminate(name: str, process: dict) -> str:
    """Stop one owned member by its process group, escalating only after a grace period."""
    if not _owned(process):
        return "already_stopped_or_changed"
    try:
        if os.getpgid(process["pid"]) != process["pid"]:
            raise ValueError(
                f"owned {name} is not its process group leader; refusing to stop it"
            )
        os.killpg(process["pid"], signal.SIGTERM)
    except ProcessLookupError:
        return "already_stopped"
    for _ in range(30):
        if not _owned(process):
            break
        time.sleep(0.1)
    if _owned(process):
        try:
            os.killpg(process["pid"], signal.SIGKILL)
        except ProcessLookupError:
            pass
        for _ in range(20):
            if not _owned(process):
                break
            time.sleep(0.05)
    return "stopped" if not _owned(process) else "termination_pending"


def _stop(config: DeliveryConfig, manifest: dict) -> dict:
    """Stop the owned members and retain the recorded selection.

    Stopping never compares the recorded source payload with the checkout:
    stopping a drifted stack is the safe action, and each member is still
    proven owned by its identity before it is signalled. The next start
    validates the retained selection before launching anything.
    """
    recorded = _recorded_deployment(manifest)
    if recorded is None:
        recorded = _recorded_deployment(_read_manifest(config) or {})
    results = {}
    for name in ("api", "worker", "temporal"):
        process = manifest.get("processes", {}).get(name)
        if process is None:
            continue
        results[name] = _terminate(name, process)
    if all(not _owned(process) for process in manifest.get("processes", {}).values()):
        if recorded is None:
            _manifest(config).unlink(missing_ok=True)
        else:
            _write_manifest(config, {}, deployment=recorded)
        (config.state_root / "worker-ready.json").unlink(missing_ok=True)
    return results


def _stopped_marker(config: DeliveryConfig) -> Path:
    """Where an explicit `stop` records the owner's intent for supervised starts."""
    return config.state_root / "service-stopped.json"


def service_stop(config: DeliveryConfig) -> dict:
    with _lifecycle_lock(config, _deadline(config)):
        results = _stop(config, _read_manifest(config) or {"processes": {}})
        # A supervisor's periodic start must not undo this; the next explicit
        # start, restart or write-triggered start lifts it.
        descriptor = os.open(_stopped_marker(config), os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump({"command": "stop", "stopped_at": time.time()}, stream)
        return results


def _registered_worker(config: DeliveryConfig, processes: dict) -> dict | None:
    """The owned worker's readiness marker when it registered this stack's worker."""
    worker = processes.get("worker")
    if not isinstance(worker, dict):
        return None
    marker = config.state_root / "worker-ready.json"
    try:
        registered = json.loads(marker.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return None
    if not isinstance(registered, dict):
        return None
    # The transport the worker polls, its interpreter and its source root are
    # recorded for restart and upgrade decisions; workers that predate their
    # recording still register.
    identity = {key: value for key, value in registered.items()
                if key in {"pid", "identity", "config_path", "deployment"}}
    expected = {
        "pid": worker.get("pid"),
        "identity": worker.get("identity"),
        "config_path": str(config.path),
        **({"deployment": worker["deployment"]} if "deployment" in worker else {}),
    }
    return registered if identity == expected else None


def _api_probe(config: DeliveryConfig, api: dict) -> DeliveryConfig:
    """Reach the running API where it listens, not where an edited file says it should."""
    recorded = api.get("dashboard_url") if isinstance(api, dict) else None
    if not isinstance(recorded, str) or recorded == config.dashboard_url:
        return config
    return DeliveryConfig(config.path, {**config.raw, "dashboard_url": recorded})


def _ready(config: DeliveryConfig, manifest: dict, deadline: float) -> dict | None:
    """The running API's self-description when the owned stack is ready, else None."""
    processes = manifest.get("processes", {})
    if set(processes) != {"temporal", "worker", "api"} or not all(
        _owned(process) for process in processes.values()
    ):
        return None
    try:
        caller = DeliveryClient(_api_probe(config, processes["api"]))
        caller.login(timeout=min(2, _remaining(deadline)))
        info = caller._request("GET", "/api/service", timeout=min(2, _remaining(deadline)))
    except (ServiceUnavailable, FileNotFoundError):
        return None
    # Authentication errors propagate without restarting the owned stack.
    ready = (
        info.get("pid") == processes["api"]["pid"]
        and info.get("temporal") == "connected"
        and _registered_worker(config, processes) is not None
    )
    return info if ready else None


def _await_ready(config: DeliveryConfig, manifest: dict, deadline: float) -> dict | None:
    """Probe readiness while every member stays owned.

    A delayed API/DB read does not authorize interrupting live work: at the
    deadline this reports the uncertainty and preserves the stack.
    """
    while True:
        info = _ready(config, manifest, deadline)
        if info:
            return info
        processes = manifest.get("processes", {})
        if set(processes) != {"temporal", "worker", "api"} or not all(
            _owned(process) for process in processes.values()
        ):
            return None
        time.sleep(min(0.1, _remaining(deadline)))


def _api_argv(config: DeliveryConfig) -> list[str]:
    return [sys.executable, "-m", "devflow_temporal.delivery_control",
            "--config", str(config.path), "api"]


def _held_address(config: DeliveryConfig, api: dict | None) -> tuple | None:
    """The loopback address an owned API member holds; None when no API is running."""
    if not _owned(api):
        return None
    recorded = api.get("dashboard_url")
    if isinstance(recorded, str):
        address = urlsplit(recorded)
        return address.hostname, address.port
    host, port, _, _ = _ports(config)
    return host, port


def _validate_api_config(config: DeliveryConfig, held: tuple | None = None) -> None:
    """Fail the way the API process would, before any running member is touched.

    ``held`` is the address the running API itself occupies; stopping it frees
    that port. Any other configured port must be free now.
    """
    host, port, _, _ = _ports(config)
    if not _dashboard_bundle().is_file():
        raise ValueError("dashboard bundle is missing; build runtime/ui before service start")
    if held != (host, port) and not _free(host, port):
        raise ValueError(f"configured service port {port} is already occupied")
    DeliveryStore(config)


def _worker_polls(config: DeliveryConfig, processes: dict) -> bool:
    """Whether the registered worker recorded exactly this configuration's transport."""
    registered = _registered_worker(config, processes)
    return (registered is not None
            and registered.get("transport_digest") == transport_digest(config))


def _launch_api(config: DeliveryConfig, manifest: dict, deadline: float) -> dict:
    """Launch the API member beside an owned Temporal server and registered worker."""
    processes = {name: process for name, process in manifest["processes"].items()
                 if name != "api"}
    host, port, _, _ = _ports(config)
    grace = time.monotonic() + 2
    while not _free(host, port):
        # A listener this lifecycle just stopped releases its port shortly after
        # exiting; anything still bound after that is a foreign process.
        if time.monotonic() > grace:
            raise ValueError(f"configured service port {port} is already occupied")
        time.sleep(min(0.05, _remaining(deadline)))
    processes["api"] = {**_launch(config, "api", _api_argv(config)),
                        "dashboard_url": config.dashboard_url}
    _write_manifest(config, processes)
    while not _ready(config, {"processes": processes}, deadline):
        if not _owned(processes["api"]):
            raise RuntimeError("api exited during service startup")
        time.sleep(min(0.1, _remaining(deadline)))
    return _read_manifest(config)


def _restart_api(config: DeliveryConfig, manifest: dict, deadline: float) -> dict:
    """Reload only the dashboard/API member; worker roles in flight are never signalled.

    The edited configuration is validated before the running API (if any) is
    stopped, so a configuration the API cannot start on leaves it serving.
    """
    processes = dict(manifest["processes"])
    api = processes.pop("api", None)
    _validate_api_config(config, _held_address(config, api))
    if api is not None and _terminate("api", api) == "termination_pending":
        raise ValueError("owned api termination is pending; inspect service status")
    _write_manifest(config, processes)
    return _launch_api(config, {**manifest, "processes": processes}, deadline)


def _only_api_missing(config: DeliveryConfig, manifest: dict) -> bool:
    """An owned Temporal server and registered worker whose API is dead or absent."""
    processes = manifest.get("processes", {})
    return (
        {"temporal", "worker"} <= set(processes)
        and all(_owned(processes[name]) for name in ("temporal", "worker"))
        and not _owned(processes.get("api"))
        and _registered_worker(config, processes) is not None
    )


def ensure_service_running(
    config: DeliveryConfig, *, deployment: WorkerDeploymentConfig | None = None,
    unless_stopped: bool = False,
) -> dict:
    """Have the stack running; ``unless_stopped`` (supervised) honours an owner's stop."""
    deadline = _deadline(config)
    try:
        _ports(config)
        with _lifecycle_lock(config, deadline):
            marker = _stopped_marker(config)
            if unless_stopped and marker.exists():
                return {"dashboard_url": config.dashboard_url, "stopped": str(marker),
                        **(_read_manifest(config) or {"processes": {}})}
            marker.unlink(missing_ok=True)
            existing = _read_manifest(config)
            if existing:
                if existing.get("config_path", str(config.path)) != str(config.path) and any(
                    _owned(process) for process in existing["processes"].values()
                ):
                    raise ValueError("state root has a running service for another configuration")
                recorded = _recorded_deployment(existing)
                different = isinstance(recorded, dict) and deployment is not None and (
                    deployment.version.deployment_name != recorded.get("name")
                    or deployment.version.build_id != recorded.get("build_id"))
                replace = different and not existing["processes"]
                if different and not replace:
                    raise ValueError(
                        "running worker deployment differs; drain it before changing")
                # A ready stack keeps running whatever the checkout now contains: its
                # members already hold the source they were launched from. The
                # recorded payload is compared only when a member must be launched
                # from the current source.
                info = _await_ready(config, existing, deadline)
                if info:
                    # An empty or partial inventory reports readiness only through
                    # patched probes; members it lacks have nothing to compare.
                    members = existing.get("processes") or {}
                    if (deployment is not None and members.get("worker", {}).get(
                            "deployment") != _deployment_identity(deployment)):
                        raise ValueError(
                            "running worker deployment differs; drain it before changing")
                    _ensure_namespace_retention(config, deadline)
                    # A stale API keeps answering (its 409s name `restart`); ensuring
                    # never reloads a member, which could split the API's transport
                    # from the worker's.
                    listening = members.get("api", {}).get("dashboard_url")
                    if isinstance(listening, str) and listening != config.dashboard_url:
                        raise ValueError(
                            f"the running API listens at {listening}; run `devflow-delivery "
                            f"--config {config.path} restart` to move it to "
                            f"{config.dashboard_url}")
                    return {"dashboard_url": config.dashboard_url, **existing}
                if recorded is not None:
                    selected = _validated_deployment(recorded, same_payload=not replace)
                    if not replace:
                        deployment = selected
                    else:
                        # Explicit owner selection only after the prior lifecycle was stopped.
                        existing = {**existing,
                                    "worker_deployment": _deployment_identity(deployment)}
                        _write_manifest(config, {}, deployment=existing["worker_deployment"])
                if _only_api_missing(config, existing):
                    # A registered worker is never stopped because its API died or
                    # failed to relaunch; only the API member is brought back, and
                    # only onto the transport that worker polls.
                    if not _worker_polls(config, existing["processes"]):
                        raise ValueError(
                            "the registered worker polls a transport other than the one "
                            f"{config.path} names; run `devflow-delivery --config "
                            f"{config.path} restart` to move the whole service")
                    existing = _restart_api(config, existing, deadline)
                    return {"dashboard_url": config.dashboard_url, **existing}
                _stop(config, existing)
                if any(_owned(process) for process in existing["processes"].values()):
                    raise ValueError("owned service termination is pending; inspect service status")
            if deployment is not None:
                # Selection outlives PIDs, including failure before the first worker launch.
                _write_manifest(config, {}, deployment=_deployment_identity(deployment))
            return (_start(config, deadline, deployment=deployment) if deployment
                    else _start(config, deadline))
    except (OSError, RuntimeError, ValueError) as exc:
        raise ValueError(
            f"local service startup failed: {exc}; logs: "
            + ", ".join(
                str(config.state_root / f"{name}.log") for name in ("temporal", "worker", "api")
            )
        ) from None


def service_start(
    config: DeliveryConfig, *, deployment: WorkerDeploymentConfig | None = None,
) -> dict:
    return (ensure_service_running(config, deployment=deployment) if deployment
            else ensure_service_running(config))


def service_restart(config: DeliveryConfig) -> dict:
    """Reload the service after a configuration edit.

    When the stack is ready and the registered worker already polls the edited
    configuration's transport, only the API reloads, so roles in flight
    continue. A transport change, a worker that registered no transport, or an
    incomplete stack takes the ordinary stop, then start.
    """
    deadline = _deadline(config)
    try:
        with _lifecycle_lock(config, deadline):
            existing = _read_manifest(config)
            if existing and (
                _await_ready(config, existing, deadline)
                or _only_api_missing(config, existing)
            ) and _worker_polls(config, existing["processes"]):
                manifest = _restart_api(config, existing, deadline)
                return {"dashboard_url": config.dashboard_url, "restarted": ["api"], **manifest}
    except (OSError, RuntimeError, ValueError) as exc:
        raise ValueError(
            f"local service restart failed: {exc}; logs: {config.state_root / 'api.log'}"
        ) from None
    service_stop(config)
    return {"restarted": ["temporal", "worker", "api"], **ensure_service_running(config)}


def _dashboard_bundle() -> Path:
    return Path(__file__).resolve().parents[2] / "ui" / "dist" / "index.html"


def _start(
    config: DeliveryConfig, deadline: float, *, deployment: WorkerDeploymentConfig | None = None,
) -> dict:
    bundle = _dashboard_bundle()
    if not bundle.is_file():
        raise ValueError("dashboard bundle is missing; build runtime/ui before service start")
    host, dashboard_port, temporal_port, ui_port = _ports(config)
    for port in (dashboard_port, temporal_port, ui_port):
        if not _free(host, port):
            raise ValueError(f"configured service port {port} is already occupied")
    temporal = config.raw.get("temporal_bin") or shutil.which("temporal")
    if not temporal or not Path(temporal).is_file():
        raise ValueError("Temporal CLI executable is unavailable")
    store = DeliveryStore(config)
    try:
        # Seeding is explicit at start: runs admitted under the live configuration
        # are covered; runs whose bytes are gone are named with their remedy.
        snapshots = store.seed_execution_snapshots()
    except (OSError, ValueError) as exc:
        snapshots = {"error": str(exc)}
    (config.state_root / "worker-ready.json").unlink(missing_ok=True)
    processes = {}
    try:
        processes["temporal"] = _launch(
            config,
            "temporal",
            [
                str(temporal),
                "server",
                "start-dev",
                "--ip",
                host,
                "--port",
                str(temporal_port),
                "--ui-port",
                str(ui_port),
                "--db-filename",
                str(config.state_root / "temporal.sqlite3"),
                "--ui-disable-news-fetch",
            ],
        )
        _write_manifest(config, processes)
        _wait_port(host, temporal_port, processes["temporal"], deadline=deadline)
        _ensure_namespace_retention(config, deadline)
        processes["worker"] = _launch(
            config,
            "worker",
            [
                sys.executable,
                "-m",
                "devflow_temporal.delivery_control",
                "--config",
                str(config.path),
                "worker",
                *([] if deployment is None else [
                    "--deployment-name", deployment.version.deployment_name,
                    "--deployment-build-id", deployment.version.build_id,
                ]),
            ],
        )
        if deployment is not None:
            processes["worker"]["deployment"] = _deployment_identity(deployment)
        _write_manifest(config, processes)
        processes["api"] = {**_launch(config, "api", _api_argv(config)),
                            "dashboard_url": config.dashboard_url}
        _write_manifest(config, processes)
        while not _ready(config, {"processes": processes}, deadline):
            for name, process in processes.items():
                if not _owned(process):
                    raise RuntimeError(f"{name} exited during service startup")
            time.sleep(min(0.1, _remaining(deadline)))
    except BaseException:
        _stop(config, {"processes": processes})
        raise
    return {"dashboard_url": config.dashboard_url, "processes": processes,
            "configuration_snapshots": snapshots}


def _deployment_identity(deployment: WorkerDeploymentConfig) -> dict:
    return {"name": deployment.version.deployment_name, "build_id": deployment.version.build_id,
            "runtime_payload_sha256": payload_digest(Path(__file__).resolve().parent)}


def _worker_deployment(name: str | None, build_id: str | None) -> WorkerDeploymentConfig | None:
    if name is None and build_id is None:
        return None
    if (not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", name)
            or not isinstance(build_id, str)
            or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", build_id)):
        raise ValueError(
            "deployment name and build ID must be supplied together as bounded identifiers")
    return WorkerDeploymentConfig(
        WorkerDeploymentVersion(name, build_id), use_worker_versioning=True,
        default_versioning_behavior=VersioningBehavior.PINNED,
    )


async def worker(
    config: DeliveryConfig, *, deployment: WorkerDeploymentConfig | None = None,
) -> None:
    identity = f"devflow-{os.getpid()}-{time.time_ns()}"
    client = await Client.connect(
        config.temporal_address,
        namespace=config.raw.get("temporal_namespace", "default"),
        data_converter=DELIVERY_DATA_CONVERTER,
    )
    await ensure_namespace_retention(client)
    async with Worker(
        client,
        task_queue=config.queue,
        workflows=[DeliveryWorkflow, MergeWorkflow],
        activities=DELIVERY_ACTIVITIES,
        interceptors=[DatabaseContentionInterceptor()],
        workflow_failure_exception_types=[ValueError, KeyError, TypeError],
        identity=identity,
        **({"deployment_config": deployment} if deployment is not None else {}),
    ):
        # Registration of both pollers proves readiness without starting a workflow.
        deadline = _deadline(config)
        for queue_type in (
            TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
            TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY,
        ):
            while True:
                response = await client.workflow_service.describe_task_queue(
                    DescribeTaskQueueRequest(
                        namespace=client.namespace,
                        task_queue=TaskQueue(name=config.queue),
                        task_queue_type=queue_type,
                    ),
                    timeout=timedelta(seconds=min(2, _remaining(deadline))),
                )
                if any(poller.identity == identity for poller in response.pollers):
                    break
                await asyncio.sleep(min(0.1, _remaining(deadline)))
        marker = config.state_root / "worker-ready.json"
        readiness = {
            "pid": os.getpid(),
            "identity": _process_identity(os.getpid()),
            "config_path": str(config.path),
            # The transport this worker polls; `restart` reloads only the API
            # while an edited configuration still names the same transport.
            "transport_digest": transport_digest(config),
            # Where this worker runs from; an upgrade refuses to change that
            # checkout underneath it.
            "interpreter": sys.executable,
            "source_root": str(Path(__file__).resolve().parents[2]),
        }
        if deployment is not None:
            readiness["deployment"] = _deployment_identity(deployment)
        descriptor = os.open(marker, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(readiness, stream)
        try:
            await asyncio.Event().wait()
        finally:
            if marker.exists() and json.loads(marker.read_text()) == readiness:
                marker.unlink()


_COMMANDS = (
    "start",
    "status",
    "stop",
    "restart",
    "supervise",
    "token",
    "worker",
    "api",
    "submit",
    "runs",
    "run",
    "evidence",
    "decision",
    "merge",
    "merge-preflight",
    "cancel",
    "refresh-issue-context",
    "disable-limits",
    "reconcile-tracker",
    "recover-publication",
    "repair-admission-preflight",
    "gates-only-preflight",
    "admit-gates-only",
    "continue-repair",
    "continue-feature",
    "revise-feature-plan",
    "retry-prelaunch",
    "amend-scope",
    "import-closure",
    "reconcile-attempt",
)


def main() -> None:
    from .delivery_native_guard import reject_nested_controller

    reject_nested_controller()
    parser = argparse.ArgumentParser(prog="devflow-delivery")
    parser.add_argument("--config", required=True)
    parser.add_argument("command", choices=_COMMANDS)
    parser.add_argument("--request", type=Path, help="JSON request file for a mutation")
    parser.add_argument("--id", help="run ID")
    parser.add_argument("--evidence-id", help="indexed evidence ID")
    parser.add_argument("--deployment-name", help="explicit Temporal worker deployment")
    parser.add_argument("--deployment-build-id", help="immutable retained source artifact version")
    parser.add_argument("--limit", type=int, default=50, help="run page size (1-100)")
    parser.add_argument("--cursor", help="next_cursor from a previous run page")
    parser.add_argument("--archived", action="store_true", help="list archived runs")
    parser.add_argument("--workflow-id", help="workflow ID of an archived closure")
    parser.add_argument("--history", type=Path, help="exported Temporal history JSON file")
    parser.add_argument("--job-key", help="native attempt key for reconcile-attempt")
    parser.add_argument("--role", help="role of the attempt for reconcile-attempt")
    parser.add_argument("--iteration", type=int, help="iteration of the attempt")
    args = parser.parse_args()
    try:
        _run(args, parser)
    except (ValueError, OSError, RuntimeError) as exc:
        parser.exit(1, f"devflow-delivery: {exc}\n")


def _run(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if args.command not in _COMMANDS:
        parser.error(f"invalid choice: {args.command!r}")
    deployment = _worker_deployment(
        getattr(args, "deployment_name", None), getattr(args, "deployment_build_id", None))
    if deployment is not None and args.command not in {"start", "worker"}:
        parser.error("worker deployment options apply only to start or worker")
    read_commands = {"merge-preflight",
        "runs", "run", "evidence", "gates-only-preflight", "repair-admission-preflight",
    }
    if args.command in read_commands | {"status", "token", "disable-limits"}:
        config = DeliveryConfig.load(Path(args.config).expanduser().resolve(strict=True))
    else:
        config = _config(args.config)
    if args.command == "worker":
        asyncio.run(worker(config, deployment=deployment))
        return
    if args.command == "api":
        host, port, _, _ = _ports(config)
        _ensure_namespace_retention(config, _deadline(config))
        uvicorn.run(create_app(config.path), host=host, port=port, access_log=False)
        return
    if args.command == "token":
        print((config.state_root / "service-token").read_text(encoding="utf-8").strip())
        return
    if args.command == "import-closure":
        if not (args.id and args.workflow_id and args.history):
            parser.error("import-closure requires --id, --workflow-id and --history")
        print(json.dumps(_import_closure(config, args.id, args.workflow_id, args.history),
                         indent=2, sort_keys=True))
        return
    if args.command == "reconcile-attempt":
        from .delivery_orphans import reconcile_attempt

        if not args.job_key and not (args.id and args.role and args.iteration is not None):
            parser.error("reconcile-attempt needs --job-key or --id with --role and --iteration")
        try:
            outcome = reconcile_attempt(
                DeliveryStore(config), job_key=args.job_key, run_id=args.id,
                role=args.role, iteration=args.iteration,
            )
        except ValueError as exc:
            parser.exit(1, f"devflow-delivery: {exc}\n")
        print(json.dumps(outcome, sort_keys=True, indent=2))
        if outcome["state"] != "finished":
            parser.exit(1)
        return
    if args.command in {
        "submit",
        "runs",
        "run",
        "evidence",
        "decision",
        "merge",
        "merge-preflight",
        "cancel",
        "refresh-issue-context",
        "disable-limits",
        "reconcile-tracker",
        "recover-publication",
        "repair-admission-preflight",
        "gates-only-preflight",
        "admit-gates-only",
        "continue-repair",
        "continue-feature",
        "revise-feature-plan",
        "retry-prelaunch",
        "amend-scope",
    }:
        request = json.loads(args.request.read_text(encoding="utf-8")) if args.request else None
        if args.command in {
            "submit",
            "decision",
            "merge",
            "cancel",
            "refresh-issue-context",
            "disable-limits",
            "reconcile-tracker",
            "recover-publication",
            "repair-admission-preflight",
            "admit-gates-only",
            "continue-repair",
            "continue-feature",
            "revise-feature-plan",
            "retry-prelaunch",
            "amend-scope",
        } and not isinstance(request, dict):
            parser.error("--request must name a JSON object file for this command")
        if (
            args.command
            in {
                "run",
                "evidence",
                "decision",
                "merge",
                "merge-preflight",
                "cancel",
                "refresh-issue-context",
                "disable-limits",
                "reconcile-tracker",
                "recover-publication",
                "repair-admission-preflight",
                "gates-only-preflight",
                "admit-gates-only",
                "continue-repair",
                "continue-feature",
                "revise-feature-plan",
                "retry-prelaunch",
                "amend-scope",
            }
            and not args.id
        ):
            parser.error("--id is required for this command")
        if args.command == "evidence" and not args.evidence_id:
            parser.error("--evidence-id is required")
        try:
            if args.command == "submit":
                from .delivery_origin import bind_origin

                request = bind_origin(request, os.environ.get("CODEX_THREAD_ID"))
            # An active-run control must never start/reload a service as a side effect.
            factory = (read_only_client if args.command in read_commands | {"disable-limits"}
                       else api_client)
            caller = factory(config.path)
            result = {
                "submit": lambda: caller.submit(request),
                "runs": lambda: caller.runs(limit=getattr(args, "limit", 50),
                                            cursor=getattr(args, "cursor", None),
                                            archived=getattr(args, "archived", False)),
                "run": lambda: caller.status(args.id),
                "evidence": lambda: caller.evidence(args.id, args.evidence_id),
                "decision": lambda: caller.decision(args.id, request),
                "merge": lambda: caller.merge(args.id, request),
                "merge-preflight": lambda: caller.merge_preflight(args.id),
                "cancel": lambda: caller.cancel(args.id, request),
                "refresh-issue-context": lambda: caller.refresh_issue_context(args.id, request),
                "disable-limits": lambda: caller.disable_delivery_limits(args.id, request),
                "reconcile-tracker": lambda: caller.reconcile_tracker(args.id, request),
                "recover-publication": lambda: caller.recover_publication(args.id, request),
                "repair-admission-preflight": lambda: caller.repair_admission_preflight(
                    args.id, request),
                "gates-only-preflight": lambda: caller.gates_only_preflight(args.id),
                "admit-gates-only": lambda: caller.admit_gates_only(args.id, request),
                "continue-repair": lambda: caller.continue_repair(args.id, request),
                "continue-feature": lambda: caller.continue_feature(args.id, request),
                "revise-feature-plan": lambda: caller.revise_feature_plan(args.id, request),
                "retry-prelaunch": lambda: caller.retry_prelaunch(args.id, request),
                "amend-scope": lambda: caller.amend_scope(args.id, request),
            }[args.command]()
        except ValueError as exc:
            parser.exit(1, f"devflow-delivery: {exc}\n")
        print(json.dumps(result, sort_keys=True, indent=2))
        return
    if args.command == "start":
        print(json.dumps(service_start(config, deployment=deployment), sort_keys=True))
        return
    if args.command == "restart":
        print(json.dumps(service_restart(config), sort_keys=True))
        return
    if args.command == "supervise":
        # The launchd agent's periodic start: never undoes the owner's `stop`.
        print(json.dumps(ensure_service_running(config, unless_stopped=True), sort_keys=True))
        return
    manifest = _read_manifest(config)
    if args.command == "status":
        print(
            json.dumps(
                {
                    "dashboard_url": config.dashboard_url,
                    "processes": {
                        name: {**process, "running": _owned(process)}
                        for name, process in (manifest or {}).get("processes", {}).items()
                    },
                },
                sort_keys=True,
            )
        )
        return
    print(json.dumps(service_stop(config), sort_keys=True))


if __name__ == "__main__":
    main()
