from __future__ import annotations

import asyncio
import json
import shutil
import socket
import sys
import time
from pathlib import Path

import pytest
import temporal_test_server
from temporal_test_server import _available_port, local_temporal
from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest

from devflow_temporal.delivery_native_process import process_table


def launcher(tmp_path: Path, *, delay: float = 6, child: bool = False) -> tuple[Path, Path]:
    """Delay a real CLI exec, recording owned identities independently of the helper."""
    marker = tmp_path / "owned.json"
    script = tmp_path / "delayed-temporal"
    binary = shutil.which("temporal")
    assert binary, "Temporal CLI is required for the real delayed-start regression"
    script.write_text(
        f"#!{sys.executable}\n"
        "import json,os,subprocess,sys,time\n"
        "from pathlib import Path\n"
        "def identity(pid):\n"
        " return subprocess.check_output(['ps','-p',str(pid),'-o','lstart='],text=True).strip()\n"
        "owned={str(os.getpid()):identity(os.getpid())}\n"
        + (
            "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'])\n"
            "owned[str(p.pid)]=identity(p.pid)\n"
            if child
            else ""
        )
        + f"Path({str(marker)!r}).write_text(json.dumps({{'owned':owned,"
        "'port':int(sys.argv[sys.argv.index('--port')+1])}))\n"
        "print('owned delayed startup',flush=True)\n"
        f"time.sleep({delay!r})\n"
        f"os.execv({binary!r},[{binary!r},*sys.argv[1:]])\n"
    )
    script.chmod(0o700)
    return script, marker


def assert_stopped(marker: Path):
    recorded = json.loads(marker.read_text())
    table = process_table()
    for pid, identity in recorded["owned"].items():
        assert table.get(int(pid), {}).get("identity") != identity, recorded
    with socket.socket() as connection:
        assert connection.connect_ex(("127.0.0.1", recorded["port"])) != 0


@pytest.mark.asyncio
async def test_six_second_real_server_startup_uses_observed_grpc_readiness(tmp_path):
    script, marker = launcher(tmp_path)
    started = time.monotonic()
    async with local_temporal(dev_server_existing_path=str(script)) as env:
        result = await env.client.workflow_service.describe_namespace(
            DescribeNamespaceRequest(namespace="default")
        )
        assert result.namespace_info.name == "default"
        assert time.monotonic() - started >= 6
    assert_stopped(marker)


@pytest.mark.asyncio
async def test_startup_deadline_preserves_logs_and_stops_owned_tree(tmp_path):
    script, marker = launcher(tmp_path, child=True)
    with pytest.raises(RuntimeError, match="within 0.5s.*owned delayed startup"):
        async with local_temporal(dev_server_existing_path=str(script), startup_timeout=0.5):
            pytest.fail("unready server must not enter the test")
    assert_stopped(marker)


@pytest.mark.asyncio
async def test_cancelled_startup_stops_owned_tree(tmp_path):
    script, marker = launcher(tmp_path, child=True)

    async def start():
        async with local_temporal(dev_server_existing_path=str(script)):
            pytest.fail("cancelled startup must not enter the test")

    task = asyncio.create_task(start())
    try:
        async with asyncio.timeout(3):
            while not marker.exists():
                await asyncio.sleep(0.01)
        # Allow one identity observation before cancellation, independently of readiness.
        await asyncio.sleep(0.15)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert_stopped(marker)
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_cli_exit_preserves_exit_status_and_diagnostic_without_relaunch(tmp_path):
    script = tmp_path / "failed-temporal"
    launches = tmp_path / "launches"
    script.write_text(
        f"#!{sys.executable}\nfrom pathlib import Path\n"
        f"with Path({str(launches)!r}).open('a') as out: out.write('launch\\n')\n"
        "print('authentic CLI startup failure',flush=True)\nraise SystemExit(23)\n"
    )
    script.chmod(0o700)
    with pytest.raises(RuntimeError, match="exited 23.*authentic CLI startup failure"):
        async with local_temporal(dev_server_existing_path=str(script)):
            pytest.fail("failed CLI must not enter the test")
    assert launches.read_text().splitlines() == ["launch"]


def test_reserved_port_can_be_rebound_immediately_by_reusing_server_socket():
    # Linux keeps the server side in TIME_WAIT; both bindings must enable reuse.
    port = _available_port()
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", port))
        listener.listen()


@pytest.mark.asyncio
async def test_foreign_grpc_readiness_is_rejected_without_stopping_foreign_server(
    tmp_path, monkeypatch
):
    outer, outer_marker = launcher(tmp_path, delay=0)
    async with local_temporal(dev_server_existing_path=str(outer)) as foreign:
        port = json.loads(outer_marker.read_text())["port"]
        inner_folder = tmp_path / "inner"
        inner_folder.mkdir()
        inner, inner_marker = launcher(inner_folder)
        monkeypatch.setattr(temporal_test_server, "_available_port", lambda: port)
        connect = temporal_test_server.Client.connect

        async def connect_after_owned_identity(*args, **kwargs):
            client = await connect(*args, **kwargs)
            async with asyncio.timeout(3):
                while not inner_marker.exists():
                    await asyncio.sleep(0.01)
            return client

        # Observe the real inner launch before its guard rejects the real foreign RPC.
        monkeypatch.setattr(temporal_test_server.Client, "connect", connect_after_owned_identity)
        with pytest.raises(RuntimeError, match="listener does not belong to the owned CLI"):
            async with local_temporal(dev_server_existing_path=str(inner)):
                pytest.fail("a foreign ready service must not authorize this fixture")
        table = process_table()
        recorded = json.loads(inner_marker.read_text())
        for pid, identity in recorded["owned"].items():
            assert table.get(int(pid), {}).get("identity") != identity
        result = await foreign.client.workflow_service.describe_namespace(
            DescribeNamespaceRequest(namespace="default")
        )
        assert result.namespace_info.name == "default"
    assert_stopped(outer_marker)
