"""Persistent, locally owned Temporal, worker, and dashboard lifecycle."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

import uvicorn
from temporalio.client import Client
from temporalio.worker import Worker

from .delivery_activities import DELIVERY_ACTIVITIES
from .delivery_api import create_app
from .delivery_client import client as api_client
from .delivery_config import DeliveryConfig
from .delivery_store import DeliveryStore
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
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None


def _owned(process: dict) -> bool:
    return (
        bool(process.get("identity")) and _process_identity(process["pid"]) == process["identity"]
    )


def _free(host: str, port: int) -> bool:
    with socket.socket() as sock:
        return sock.connect_ex((host, port)) != 0


def _wait_port(host: str, port: int, process: dict, *, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _owned(process):
            raise RuntimeError(f"service process for port {port} exited during startup")
        if not _free(host, port):
            return
        time.sleep(0.2)
    raise RuntimeError(f"service did not bind configured port {port}")


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
    for _ in range(20):
        identity = _process_identity(process.pid)
        if identity:
            return {"pid": process.pid, "identity": identity, "log": str(log)}
        if process.poll() is not None:
            raise RuntimeError(f"{name} exited before registering its process identity")
        time.sleep(0.05)
    raise RuntimeError(f"{name} process identity was unavailable")


def _stop(config: DeliveryConfig, manifest: dict) -> dict:
    results = {}
    for name, process in reversed(list(manifest.get("processes", {}).items())):
        if not _owned(process):
            results[name] = "already_stopped_or_changed"
            continue
        try:
            os.killpg(process["pid"], signal.SIGTERM)
        except ProcessLookupError:
            results[name] = "already_stopped"
            continue
        for _ in range(30):
            if not _owned(process):
                break
            time.sleep(0.1)
        if _owned(process):
            os.killpg(process["pid"], signal.SIGKILL)
        results[name] = "stopped" if not _owned(process) else "termination_pending"
    if all(not _owned(process) for process in manifest.get("processes", {}).values()):
        _manifest(config).unlink(missing_ok=True)
    return results


def service_start(config: DeliveryConfig) -> dict:
    existing = _read_manifest(config)
    if existing:
        if any(_owned(process) for process in existing.get("processes", {}).values()):
            raise ValueError("an owned service is already running; inspect service status")
        _manifest(config).unlink()
    host, dashboard_port, temporal_port, ui_port = _ports(config)
    for port in (dashboard_port, temporal_port, ui_port):
        if not _free(host, port):
            raise ValueError(f"configured service port {port} is already occupied")
    temporal = config.raw.get("temporal_bin") or shutil.which("temporal")
    if not temporal or not Path(temporal).is_file():
        raise ValueError("Temporal CLI executable is unavailable")
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
        _wait_port(host, temporal_port, processes["temporal"])
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
        _wait_port(host, dashboard_port, processes["api"])
        if not _owned(processes["worker"]):
            raise RuntimeError("Temporal worker exited during service startup")
        path = _manifest(config)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps({"processes": processes}, indent=2) + "\n")
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    except BaseException:
        _stop(config, {"processes": processes})
        raise
    return {"dashboard_url": config.dashboard_url, "processes": processes}


async def worker(config: DeliveryConfig) -> None:
    client = await Client.connect(
        config.temporal_address,
        namespace=config.raw.get("temporal_namespace", "default"),
    )
    async with Worker(
        client,
        task_queue=config.queue,
        workflows=[DeliveryWorkflow],
        activities=DELIVERY_ACTIVITIES,
    ):
        await asyncio.Event().wait()


def main() -> None:
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
        ),
    )
    parser.add_argument("--request", type=Path, help="JSON request file for a mutation")
    parser.add_argument("--id", help="run ID")
    parser.add_argument("--evidence-id", help="indexed evidence ID")
    args = parser.parse_args()
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
    if args.command in {"submit", "runs", "run", "evidence", "decision", "cancel"}:
        caller = api_client(config.path)
        request = json.loads(args.request.read_text(encoding="utf-8")) if args.request else None
        if args.command in {"submit", "decision", "cancel"} and not isinstance(request, dict):
            parser.error("--request must name a JSON object file for this command")
        if args.command in {"run", "evidence", "decision", "cancel"} and not args.id:
            parser.error("--id is required for this command")
        if args.command == "evidence" and not args.evidence_id:
            parser.error("--evidence-id is required")
        result = {
            "submit": lambda: caller.submit(request),
            "runs": caller.runs,
            "run": lambda: caller.status(args.id),
            "evidence": lambda: caller.evidence(args.id, args.evidence_id),
            "decision": lambda: caller.decision(args.id, request),
            "cancel": lambda: caller.cancel(args.id, request),
        }[args.command]()
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
    print(json.dumps(_stop(config, manifest or {"processes": {}}), sort_keys=True))


if __name__ == "__main__":
    main()
