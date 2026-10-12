"""A lost native gate monitor yields no verdict; one fresh invocation re-runs it (N-14)."""
from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import sys
from importlib.metadata import distribution
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_store import _git

from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_native_process import lost_monitor_without_provider, process_table
from devflow_temporal.delivery_preparation import prepare_authority
from devflow_temporal.delivery_resources import (
    RunResources,
    observe_finalized_resources,
    read_private,
)
from devflow_temporal.delivery_store import DeliveryStore

# The gate command keeps running long enough for its monitor to be lost while
# the child is authorized, binds the browser ports when given, and retains one
# artifact exactly like a real browser suite.
FIXTURE = (
    "import os, socket, time\nfrom pathlib import Path\n"
    "report=Path('dist/playwright-report')\nreport.mkdir(parents=True, exist_ok=True)\n"
    "(report/'result.json').write_text('retained fixture output')\n"
    "ports=[int(os.environ[k]) for k in ('QA_API_PORT','QA_WEB_PORT') if k in os.environ]\n"
    "sockets=[]\nfor port in ports:\n"
    " s=socket.socket();s.bind(('127.0.0.1',port));s.listen();sockets.append(s)\n"
    "time.sleep(4)\nprint('1 passed')\n"
)

pytestmark = pytest.mark.skipif(sys.platform != "darwin",
                                reason="native macOS execution authority")


@pytest.fixture
def native_gates(api_fixture, tmp_path):
    path, request = api_fixture
    raw = json.loads(path.read_text())
    repository = raw["repositories"]["fixture"]
    source = Path(repository["source_path"])
    (source / ".gitignore").write_text("dist/\nnode_modules/\n")
    (source / "fixture.py").write_text(FIXTURE)
    _git(source, "add", ".")
    _git(source, "commit", "-qm", "fixture gate")
    repository["expected_base_sha"] = _git(source, "rev-parse", "HEAD")
    sockets = [socket.socket(), socket.socket()]
    for item in sockets:
        item.bind(("127.0.0.1", 0))
    ports = [item.getsockname()[1] for item in sockets]
    for item in sockets:
        item.close()
    recipe = {"id": "regression", "kind": "check", "cwd": ".",
              "argv": [sys.executable, "fixture.py"], "timeout_seconds": 30}
    repository.update(
        prepublish_checks=[recipe], checks=[recipe], required_ci=["test"],
        project_url="https://github.com/users/example/projects/1", assignee="example",
        browser_qa={"id": "fixture", "argv": recipe["argv"], "cwd": ".",
                    "ports": dict(zip(["QA_API_PORT", "QA_WEB_PORT"], ports, strict=True)),
                    "env": {"JOBCTRL_E2E_ISOLATED": "1"}, "read_roots": [],
                    "artifact_paths": ["dist/playwright-report"],
                    "test_count_regex": r"(\d+) passed", "min_tests": 1,
                    "timeout_seconds": 30})
    auth = tmp_path / "fixture-auth.json"
    auth.write_text('{"OPENAI_API_KEY":"unusable-owned-fixture"}')
    auth.chmod(0o600)
    raw.update(provider="codex", execution_mode="trusted-local", codex_auth_path=str(auth),
               codex_bin=str(distribution("openai-codex-cli-bin").locate_file(
                   "codex_cli_bin/bin/codex")))
    raw["roles"] = {role: {"model": "gpt-6.1-sol", "effort": "high"}
                    for role in ["intake", "implement", "review", "verify"]}
    path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(request)
    owned = prepare_authority(store, store.submitted_spec(request["run_id"]))
    broker = DeliveryBroker(store, owned)
    candidate = broker.prepare()["candidate"]
    yield SimpleNamespace(store=store, spec=owned, broker=broker, candidate=candidate)
    assert RunResources(owned).finalize("blocked")["state"] == "confirmed"


async def _lose_authorized_monitor(journal_path: Path) -> dict:
    """Kill the monitor once its child is authorized, as a reboot or OOM kill would."""
    async with asyncio.timeout(60):
        while True:
            journal = None
            if journal_path.exists():
                try:
                    journal = read_private(journal_path)
                except (OSError, ValueError):
                    journal = None
            if (journal and journal.get("phase") == "authorized" and journal.get("owned")
                    and journal.get("monitor")):
                break
            await asyncio.sleep(0.01)
    monitor = journal["monitor"]
    assert process_table()[monitor["pid"]]["identity"] == monitor["identity"]
    os.kill(monitor["pid"], signal.SIGKILL)
    return journal


def _native_folders(folder: Path) -> list[str]:
    return sorted(item.name for item in folder.iterdir() if item.name.startswith("native"))


@pytest.mark.asyncio
@pytest.mark.parametrize("losses", [1, 2])
async def test_lost_check_monitor_reruns_the_gate_once(native_gates, losses):
    broker, candidate = native_gates.broker, native_gates.candidate
    folder = broker.evidence_dir / "checks" / "0" / "regression"
    loop = asyncio.get_running_loop()
    task = loop.run_in_executor(None, broker.run_checks, 0, candidate)
    await _lose_authorized_monitor(folder / "native" / "native-process.json")
    if losses == 2:
        await _lose_authorized_monitor(folder / "native-retry" / "native-process.json")
    result = await asyncio.wait_for(task, 120)
    interrupted = read_private(folder / "native" / "native-process.json")
    assert interrupted["result"]["interrupted_monitor"] is True
    assert interrupted["result"]["provider_outcome"] == "unknown"
    if losses == 1:
        assert result["state"] == "passed", result
        record = result["results"][0]
        assert record["passed"] and record["exit_code"] == 0 and Path(record["log"]).is_file()
        assert record["native_process"]["journal"] == str(
            folder / "native-retry" / "native-process.json")
        assert [item["journal"] for item in record["interrupted_native_processes"]] == [
            str(folder / "native" / "native-process.json")]
        return
    assert result["state"] == "unknown" and result["cleanup"] == "confirmed"
    assert result["process_cleanup"] == "observed-native-confirmed"
    assert "lost twice" in result["reason"]
    assert len(result["interrupted_native_processes"]) == 2
    # A Temporal retry reattaches both finalized journals and launches nothing new.
    again = await loop.run_in_executor(None, broker.run_checks, 0, candidate)
    assert again["state"] == "unknown" and again["cleanup"] == "confirmed"
    assert _native_folders(folder) == ["native", "native-retry"]


@pytest.mark.asyncio
@pytest.mark.parametrize("losses", [1, 2])
async def test_lost_browser_qa_monitor_reruns_the_gate_once(native_gates, losses):
    broker, candidate = native_gates.broker, native_gates.candidate
    folder = broker.evidence_dir / "browser-qa" / "0"
    loop = asyncio.get_running_loop()
    task = loop.run_in_executor(None, broker.run_browser_qa, 0, candidate)
    await _lose_authorized_monitor(folder / "native" / "native-process.json")
    if losses == 2:
        await _lose_authorized_monitor(folder / "native-retry" / "native-process.json")
    result = await asyncio.wait_for(task, 120)
    if losses == 1:
        assert result["state"] == "passed", result.get("diagnostic")
        assert result["cleanup"] == "confirmed" and result["test_count"] == 1
        assert result["native_process"]["journal"] == str(
            folder / "native-retry" / "native-process.json")
        assert len(result["interrupted_native_processes"]) == 1
        # The retained receipt replays against the journal that produced the verdict.
        assert broker.run_browser_qa(0, candidate) == result
        return
    assert result["state"] == "unknown" and result["cleanup"] == "confirmed"
    assert "lost twice" in result["reason"]
    assert len(result["interrupted_native_processes"]) == 2
    again = await loop.run_in_executor(None, broker.run_browser_qa, 0, candidate)
    assert again["state"] == "unknown" and again["cleanup"] == "confirmed"
    assert _native_folders(folder) == ["native", "native-retry"]


@pytest.mark.asyncio
async def test_single_gate_loss_keeps_custody_observable(native_gates):
    """One lost gate monitor must not block merge, release or automatic retry later."""
    broker, candidate = native_gates.broker, native_gates.candidate
    folder = broker.evidence_dir / "checks" / "0" / "regression"
    task = asyncio.get_running_loop().run_in_executor(None, broker.run_checks, 0, candidate)
    await _lose_authorized_monitor(folder / "native" / "native-process.json")
    result = await asyncio.wait_for(task, 120)
    assert result["state"] == "passed", result
    interrupted = read_private(folder / "native" / "native-process.json")
    assert interrupted["monitoring_complete"] is False
    assert interrupted["result"]["provider_outcome"] == "unknown"
    receipt = RunResources(native_gates.spec).finalize("blocked")
    assert receipt["outcome"] == "blocked"
    # The interrupted journal is a gate's, outside attempts/: its bound teardown
    # proof stands in for completed monitoring, while the same journal shape
    # under attempts/ (a provider that ran) would not be admitted.
    assert observe_finalized_resources(native_gates.spec)["finalization_sha256"]
    assert lost_monitor_without_provider(interrupted, role_attempt=False)
    assert not lost_monitor_without_provider(interrupted, role_attempt=True)
