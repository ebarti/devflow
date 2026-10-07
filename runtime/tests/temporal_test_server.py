"""Single owned CLI launch; SDK start_local hardcodes a five-second startup limit."""

from __future__ import annotations

import asyncio
import math
import shutil
import socket
import subprocess
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment

from devflow_temporal.delivery_native_process import process_table, sample, stop_observed


@asynccontextmanager
async def local_temporal(
    *,
    dev_server_existing_path: str | None = None,
    dev_server_database_filename: str | None = None,
    startup_timeout: float = 15,
):
    """Wait for real gRPC readiness, preserving a finite startup budget and CLI errors."""
    if not math.isfinite(startup_timeout) or startup_timeout <= 0:
        raise ValueError("Temporal startup timeout must be finite and positive")
    binary = dev_server_existing_path or shutil.which("temporal")
    if not binary:
        raise RuntimeError("Temporal CLI is required; CI pins version 1.9.1")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        # Avoid Linux reallocating this ephemeral port to another bind(:0).
        listener.listen()
        with socket.create_connection(listener.getsockname()):
            connection, _ = listener.accept()
            connection.close()
    argv = [
        binary,
        "server",
        "start-dev",
        "--headless",
        "--ip",
        "127.0.0.1",
        "--port",
        str(port),
        "--namespace",
        "default",
        "--log-level",
        "warn",
        "--dynamic-config-value",
        "frontend.enableServerVersionCheck=false",
        "--dynamic-config-value",
        "frontend.enableUpdateWorkflowExecution=true",
        "--dynamic-config-value",
        "frontend.enableUpdateWorkflowExecutionAsyncAccepted=true",
    ]
    if dev_server_database_filename:
        argv.extend(["--db-filename", dev_server_database_filename])
    with tempfile.TemporaryDirectory(prefix="devflow-test-temporal-") as directory:
        log_path = Path(directory) / "server.log"
        with log_path.open("w+") as log:
            deadline = asyncio.get_running_loop().time() + startup_timeout
            process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
            owned = {}
            monitor = None
            try:
                identity = process_table().get(process.pid)
                if identity is None:
                    process.wait(timeout=1)
                    raise RuntimeError(
                        f"Temporal CLI exited {process.returncode}: {log_path.read_text()}"
                    )
                owned[process.pid] = identity

                async def observe():
                    while True:
                        sample(owned)
                        await asyncio.sleep(0.1)

                monitor = asyncio.create_task(observe())
                last_error = None
                while True:
                    if process.poll() is not None:
                        raise RuntimeError(
                            f"Temporal CLI exited {process.returncode}: {log_path.read_text()}"
                        )
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        raise RuntimeError(
                            f"Temporal CLI did not become ready within {startup_timeout}s; "
                            f"last connection error: {last_error}; CLI logs: {log_path.read_text()}"
                        )
                    try:
                        client = await asyncio.wait_for(
                            Client.connect(f"127.0.0.1:{port}"), timeout=min(1, remaining)
                        )
                        break
                    except (RuntimeError, TimeoutError) as error:
                        last_error = error
                        await asyncio.sleep(
                            min(0.1, max(0, deadline - asyncio.get_running_loop().time()))
                        )
                yield WorkflowEnvironment.from_client(client)
            finally:

                async def clean_up():
                    if monitor is not None:
                        monitor.cancel()
                        await asyncio.gather(monitor, return_exceptions=True)
                    stopped = await asyncio.to_thread(stop_observed, owned)
                    # An unreaped Popen child cannot have its PID reused, including if
                    # the first identity inspection failed before it could be recorded.
                    if process.poll() is None:
                        process.kill()
                    await asyncio.to_thread(process.wait, timeout=5)
                    if not stopped:
                        raise RuntimeError(
                            "Temporal test server cleanup could not confirm teardown"
                        )

                cleanup = asyncio.create_task(clean_up())
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    await cleanup
                    raise
