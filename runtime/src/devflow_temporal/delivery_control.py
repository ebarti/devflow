"""Persistent, locally owned Temporal, worker, and dashboard lifecycle."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
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
from temporalio.worker import Worker

from .delivery_activities import DELIVERY_ACTIVITIES
from .delivery_api import create_app
from .delivery_client import DeliveryClient, ServiceUnavailable
from .delivery_client import client as api_client
from .delivery_codec import DELIVERY_DATA_CONVERTER
from .delivery_config import DeliveryConfig
from .delivery_store import DeliveryStore, _private_directory
from .delivery_workflow import DeliveryWorkflow
from .supervisor import _process_identity


def _config(path: str) -> DeliveryConfig:
    config = DeliveryConfig.load(Path(path).expanduser().resolve(strict=True))
    DeliveryStore(config)
    return config


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


def _write_manifest(config: DeliveryConfig, processes: dict) -> None:
    path = _manifest(config)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps({"config_path": str(config.path), "processes": processes}, indent=2) + "\n"
    )
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


@contextmanager
def _lifecycle_lock(config: DeliveryConfig, deadline: float):
    _private_directory(config.state_root)
    path = config.state_root / "service.lock"
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
        ):
            raise ValueError(f"service lock must be an owned private file (0600): {path}")
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                time.sleep(min(0.05, _remaining(deadline)))
        yield
    finally:
        os.close(descriptor)


def _owned(process: dict) -> bool:
    if not isinstance(process, dict) or not isinstance(process.get("pid"), int):
        return False
    try:
        # Reap a directly launched child if it exited, including after SIGKILL.
        os.waitpid(process["pid"], os.WNOHANG)
    except ChildProcessError:
        pass
    return (
        bool(process.get("identity")) and _process_identity(process["pid"]) == process["identity"]
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


def _stop(config: DeliveryConfig, manifest: dict) -> dict:
    results = {}
    for name in ("api", "worker", "temporal"):
        process = manifest.get("processes", {}).get(name)
        if process is None:
            continue
        if not _owned(process):
            results[name] = "already_stopped_or_changed"
            continue
        try:
            if os.getpgid(process["pid"]) != process["pid"]:
                raise ValueError(
                    f"owned {name} is not its process group leader; refusing to stop it"
                )
            os.killpg(process["pid"], signal.SIGTERM)
        except ProcessLookupError:
            results[name] = "already_stopped"
            continue
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
        results[name] = "stopped" if not _owned(process) else "termination_pending"
    if all(not _owned(process) for process in manifest.get("processes", {}).values()):
        _manifest(config).unlink(missing_ok=True)
        (config.state_root / "worker-ready.json").unlink(missing_ok=True)
    return results


def service_stop(config: DeliveryConfig) -> dict:
    with _lifecycle_lock(config, _deadline(config)):
        return _stop(config, _read_manifest(config) or {"processes": {}})


def _ready(config: DeliveryConfig, manifest: dict, deadline: float) -> bool:
    processes = manifest.get("processes", {})
    if set(processes) != {"temporal", "worker", "api"} or not all(
        _owned(process) for process in processes.values()
    ):
        return False
    try:
        caller = DeliveryClient(config)
        caller.login(timeout=min(2, _remaining(deadline)))
        info = caller._request("GET", "/api/service", timeout=min(2, _remaining(deadline)))
    except (ServiceUnavailable, FileNotFoundError):
        return False
    # Authentication errors propagate without restarting the owned stack.
    marker = config.state_root / "worker-ready.json"
    try:
        worker_ready = json.loads(marker.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return False
    return (
        info.get("pid") == processes["api"]["pid"]
        and info.get("temporal") == "connected"
        and worker_ready
        == {
            "pid": processes["worker"]["pid"],
            "identity": processes["worker"]["identity"],
            "config_path": str(config.path),
        }
    )


def ensure_service_running(config: DeliveryConfig) -> dict:
    deadline = _deadline(config)
    try:
        _ports(config)
        with _lifecycle_lock(config, deadline):
            existing = _read_manifest(config)
            if existing:
                if existing.get("config_path", str(config.path)) != str(config.path) and any(
                    _owned(process) for process in existing["processes"].values()
                ):
                    raise ValueError("state root has a running service for another configuration")
                while True:
                    if _ready(config, existing, deadline):
                        return {"dashboard_url": config.dashboard_url, **existing}
                    processes = existing["processes"]
                    if set(processes) != {"temporal", "worker", "api"} or not all(
                        _owned(process) for process in processes.values()
                    ):
                        break
                    # A delayed API/DB read does not authorize interrupting live work.
                    # At the deadline, report the uncertainty and preserve this stack.
                    time.sleep(min(0.1, _remaining(deadline)))
                _stop(config, existing)
                if any(_owned(process) for process in existing["processes"].values()):
                    raise ValueError("owned service termination is pending; inspect service status")
            return _start(config, deadline)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ValueError(
            f"local service startup failed: {exc}; logs: "
            + ", ".join(
                str(config.state_root / f"{name}.log") for name in ("temporal", "worker", "api")
            )
        ) from None


def service_start(config: DeliveryConfig) -> dict:
    return ensure_service_running(config)


def _dashboard_bundle() -> Path:
    return Path(__file__).resolve().parents[2] / "ui" / "dist" / "index.html"


def _start(config: DeliveryConfig, deadline: float) -> dict:
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
    DeliveryStore(config)
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
            ],
        )
        _write_manifest(config, processes)
        processes["api"] = _launch(
            config,
            "api",
            [
                sys.executable,
                "-m",
                "devflow_temporal.delivery_control",
                "--config",
                str(config.path),
                "api",
            ],
        )
        _write_manifest(config, processes)
        while not _ready(config, {"processes": processes}, deadline):
            for name, process in processes.items():
                if not _owned(process):
                    raise RuntimeError(f"{name} exited during service startup")
            time.sleep(min(0.1, _remaining(deadline)))
    except BaseException:
        _stop(config, {"processes": processes})
        raise
    return {"dashboard_url": config.dashboard_url, "processes": processes}


async def worker(config: DeliveryConfig) -> None:
    identity = f"devflow-{os.getpid()}-{time.time_ns()}"
    client = await Client.connect(
        config.temporal_address,
        namespace=config.raw.get("temporal_namespace", "default"),
        data_converter=DELIVERY_DATA_CONVERTER,
    )
    async with Worker(
        client,
        task_queue=config.queue,
        workflows=[DeliveryWorkflow],
        activities=DELIVERY_ACTIVITIES,
        identity=identity,
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
        }
        descriptor = os.open(marker, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(readiness, stream)
        try:
            await asyncio.Event().wait()
        finally:
            if marker.exists() and json.loads(marker.read_text()) == readiness:
                marker.unlink()


def main() -> None:
    from .delivery_native_guard import reject_nested_controller

    reject_nested_controller()
    parser = argparse.ArgumentParser(prog="devflow-delivery")
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "command",
        choices=(
            "start",
            "status",
            "stop",
            "token",
            "worker",
            "api",
            "submit",
            "runs",
            "run",
            "evidence",
            "decision",
            "cancel",
            "recover-publication",
            "continue-repair",
            "retry-prelaunch",
            "amend-scope",
            "recover-precheck-prelaunch",
        ),
    )
    parser.add_argument("--request", type=Path, help="JSON request file for a mutation")
    parser.add_argument("--id", help="run ID")
    parser.add_argument("--evidence-id", help="indexed evidence ID")
    args = parser.parse_args()
    try:
        _run(args, parser)
    except (ValueError, OSError, RuntimeError) as exc:
        parser.exit(1, f"devflow-delivery: {exc}\n")


def _run(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    config = _config(args.config)
    if args.command == "worker":
        asyncio.run(worker(config))
        return
    if args.command == "api":
        host, port, _, _ = _ports(config)
        uvicorn.run(create_app(config.path), host=host, port=port, access_log=False)
        return
    if args.command == "token":
        print((config.state_root / "service-token").read_text(encoding="utf-8").strip())
        return
    if args.command in {
        "submit",
        "runs",
        "run",
        "evidence",
        "decision",
        "cancel",
        "recover-publication",
        "continue-repair",
        "retry-prelaunch",
        "amend-scope",
        "recover-precheck-prelaunch",
    }:
        request = json.loads(args.request.read_text(encoding="utf-8")) if args.request else None
        if args.command in {
            "submit",
            "decision",
            "cancel",
            "recover-publication",
            "continue-repair",
            "retry-prelaunch",
            "amend-scope",
            "recover-precheck-prelaunch",
        } and not isinstance(request, dict):
            parser.error("--request must name a JSON object file for this command")
        if (
            args.command
            in {
                "run",
                "evidence",
                "decision",
                "cancel",
                "recover-publication",
                "continue-repair",
                "retry-prelaunch",
                "amend-scope",
                "recover-precheck-prelaunch",
            }
            and not args.id
        ):
            parser.error("--id is required for this command")
        if args.command == "evidence" and not args.evidence_id:
            parser.error("--evidence-id is required")
        try:
            caller = api_client(config.path)
            result = {
                "submit": lambda: caller.submit(request),
                "runs": caller.runs,
                "run": lambda: caller.status(args.id),
                "evidence": lambda: caller.evidence(args.id, args.evidence_id),
                "decision": lambda: caller.decision(args.id, request),
                "cancel": lambda: caller.cancel(args.id, request),
                "recover-publication": lambda: caller.recover_publication(args.id, request),
                "continue-repair": lambda: caller.continue_repair(args.id, request),
                "retry-prelaunch": lambda: caller.retry_prelaunch(args.id, request),
                "amend-scope": lambda: caller.amend_scope(args.id, request),
                "recover-precheck-prelaunch": lambda: caller.recover_precheck_prelaunch(
                    args.id, request
                ),
            }[args.command]()
        except ValueError as exc:
            parser.exit(1, f"devflow-delivery: {exc}\n")
        print(json.dumps(result, sort_keys=True, indent=2))
        return
    if args.command == "start":
        print(json.dumps(service_start(config), sort_keys=True))
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
