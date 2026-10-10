"""Actual broker/browser/process paths; admission and preparation are fixtures."""
from __future__ import annotations

import asyncio
import fcntl
import json
import os
import socket
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_delivery_check_slots import wait_until
from test_delivery_resources import spec
from test_delivery_store import _git

from devflow_temporal import delivery_activities as activities
from devflow_temporal.candidate import candidate_for
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_native_process import listeners, process_table, reconcile_process
from devflow_temporal.delivery_resources import RunResources, read_private

pytestmark = pytest.mark.skipif(
    sys.platform != "darwin", reason="actual macOS native port ownership inspector required",
)


def assert_interrupted_browser_cleanup(
    result, native_cleanup_confirmed, original, terminal, table, ports, *, failure,
):
    if failure == "exception":
        assert result["state"] == result["cleanup"] == "unknown"
        assert native_cleanup_confirmed is False
        return
    assert failure == "interrupted"
    assert result["cleanup"] == "confirmed"
    assert native_cleanup_confirmed is True
    assert terminal["intent"] == original["intent"]
    assert terminal["ports"] == original["ports"] == original["intent"]["ports"]
    assert terminal["monitor"] == original["monitor"]
    assert terminal["phase"] == "finished" and terminal["monitoring_complete"] is True
    native = result["native_process"]
    assert native == terminal["result"]
    assert native["cleanup"] == "observed-native-confirmed"
    assert native["monitoring_complete"] is True and native["stdio_drained"] is True
    owned = terminal["owned"]
    assert owned and original["owned"].keys() <= owned.keys()
    for pid, actor in original["owned"].items():
        assert owned[pid]["identity"] == actor["identity"]
    monitor = original["monitor"]
    assert owned[str(monitor["pid"])]["identity"] == monitor["identity"]
    assert native["observed_owned_pids"] == sorted(map(int, owned))
    for pid, actor in owned.items():
        observed = table.get(int(pid), {})
        assert (observed.get("identity") != actor["identity"]
                or observed["stat"].startswith("Z"))
    assert set(ports) == set(original["ports"]) and not any(ports.values())


@pytest.fixture
def browser_checks(tmp_path, monkeypatch):
    guards = [socket.socket() for _ in range(2)]
    for guard in guards:
        guard.bind(("127.0.0.1", 0))
    ports = [guard.getsockname()[1] for guard in guards]
    for guard in guards:
        guard.close()
    brokers, hearts = {}, []
    config = SimpleNamespace(state_root=tmp_path, raw={"check_concurrency": 2})
    database = tmp_path / "fixture.sqlite3"
    with sqlite3.connect(database) as db:
        db.execute("CREATE TABLE delivery_runs(run_id TEXT, phase TEXT)")

    def connect():
        db = sqlite3.connect(database)
        db.row_factory = sqlite3.Row
        return db

    def request(name, seconds=0.5):
        value = spec(tmp_path, name)
        checkout = Path(value["checkout"])
        checkout.mkdir(parents=True)
        (checkout / "README.md").write_text("Owned browser fixture\n")
        _git(checkout, "init", "-q")
        _git(checkout, "add", ".")
        _git(checkout, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
             "commit", "-qm", "owned fixture")
        code = (
            "import socket,subprocess,sys,time; "
            "sockets=[socket.socket() for _ in range(2)]; "
            f"[s.bind(('127.0.0.1',p)) for s,p in zip(sockets,{ports!r})]; "
            "[s.listen() for s in sockets]; "
            "subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']); "
            f"time.sleep({seconds});print('1 passed')"
        )
        value.update(provider="codex")
        value["policy"].update(host_sandbox="trusted-local", browser_qa={
            "ports": {"QA_WEB_PORT": ports[0], "QA_API_PORT": ports[1]},
            "argv": [sys.executable, "-c", code], "timeout_seconds": 60,
            "artifact_paths": [], "test_count_regex": r"(\d+) passed", "min_tests": 1,
        })
        broker = DeliveryBroker.__new__(DeliveryBroker)
        broker.spec, broker.store = value, SimpleNamespace(_connect=connect)
        broker.evidence_dir, broker.effect_namespace = Path(value["state_dir"]), ""
        broker.native_cleanup_confirmed, broker.check_cancelled = True, lambda: False
        broker.candidate = lambda: candidate_for(checkout)
        broker.gate_checkout = lambda *args: checkout
        broker._effect = lambda *args: None
        broker._finish_effect = lambda *args: None
        brokers[name] = broker
        return {"spec": value, "iteration": 0, "candidate": broker.candidate()}

    def prepare(_spec, _checkout, folder, _scratch, _qa):
        profile = folder / "browser-qa.sb"
        profile.write_text("owned trusted-local fixture profile")
        return profile, {"PATH": os.environ["PATH"]}

    monkeypatch.setattr(activities, "_context", lambda value: (
        SimpleNamespace(config=config), brokers[value["run_id"]],
    ))
    monkeypatch.setattr("devflow_temporal.delivery_native_guard.validate_native_turn",
                        lambda *a: None)
    monkeypatch.setattr("devflow_temporal.delivery_preparation.verify_prepared_spec",
                        lambda *a: None)
    monkeypatch.setattr("devflow_temporal.delivery_browser_qa.prepare_browser_qa", prepare)
    monkeypatch.setattr(activities.activity, "in_activity", lambda: True)
    monkeypatch.setattr(activities.activity, "heartbeat", hearts.append)
    monkeypatch.setattr(activities, "_CHECK_HEARTBEAT_INTERVAL", 0.05)
    retained = len(activities._UNCLEAN_CHECK_SLOTS)
    yield SimpleNamespace(request=request, brokers=brokers, hearts=hearts,
                          ports=ports, config=config)
    for descriptor in activities._UNCLEAN_CHECK_SLOTS[retained:]:
        os.close(descriptor)
    del activities._UNCLEAN_CHECK_SLOTS[retained:]
    for broker in brokers.values():
        resource = RunResources(broker.spec)
        if resource.manifest.exists():
            for journal in read_private(resource.manifest)["processes"]:
                assert reconcile_process(Path(journal))["observed_owned_stopped"]
        scratch = Path("/private/tmp") / ("dfqa-" + digest(broker.spec["state_dir"])[:20])
        if scratch.exists():
            scratch.rmdir()  # This fixture's registered empty scratch, never another origin.
    assert all(not listeners(port) for port in ports)


async def test_actual_browser_runs_queue_on_same_frozen_ports(browser_checks):
    requests = [browser_checks.request(name) for name in ("one", "two")]
    first = asyncio.create_task(activities.delivery_browser_qa(requests[0]))
    await wait_until(lambda: all(listeners(port) for port in browser_checks.ports))
    second = asyncio.create_task(activities.delivery_browser_qa(requests[1]))
    results = await asyncio.gather(first, second)
    assert [result["state"] for result in results] == ["passed", "passed"]
    assert any(heart["run_id"] == "two" and heart["stage"] == "waiting-browser-ports"
               for heart in browser_checks.hearts)
    for request, result in zip(requests, results, strict=True):
        journal = read_private(Path(result["native_process"]["journal"]))
        assert journal["intent"]["argv"] == request["spec"]["policy"]["browser_qa"]["argv"]
        assert journal["intent"]["ports"] == list(browser_checks.ports)


async def test_actual_browser_run_cancelled_in_port_queue_never_launches(browser_checks):
    first_request = browser_checks.request("first", seconds=1.0)
    request = browser_checks.request("queued")
    first = asyncio.create_task(activities.delivery_browser_qa(first_request))
    await wait_until(lambda: all(listeners(port) for port in browser_checks.ports))
    queued = asyncio.create_task(activities.delivery_browser_qa(request))
    try:
        await wait_until(lambda: any(
            heart["run_id"] == "queued" and heart["stage"] == "waiting-browser-ports"
            for heart in browser_checks.hearts
        ))
        db = browser_checks.brokers["queued"].store._connect()
        with db:
            db.execute("INSERT INTO delivery_runs(run_id, phase) VALUES ('queued', 'cancelling')")
        db.close()
        result = await queued
    finally:
        await first
    assert result["cleanup"] == "confirmed"
    assert result["cancelled"] is True
    assert not (Path(request["spec"]["state_dir"]) / "browser-qa").exists()


async def test_actual_browser_cancelled_after_preparation_cleans_owned_scratch(
    browser_checks, monkeypatch,
):
    request = browser_checks.request("prepared")
    broker = browser_checks.brokers["prepared"]
    from devflow_temporal.delivery_browser_qa import prepare_browser_qa

    def cancel_after_preparation(*args):
        result = prepare_browser_qa(*args)
        db = broker.store._connect()
        with db:
            db.execute("INSERT INTO delivery_runs(run_id, phase) VALUES ('prepared', 'cancelling')")
        db.close()
        return result

    monkeypatch.setattr("devflow_temporal.delivery_browser_qa.prepare_browser_qa",
                        cancel_after_preparation)
    result = await activities.delivery_browser_qa(request)
    assert result["cleanup"] == "confirmed"
    assert result["cancelled"] is True
    assert not (Path(broker.spec["state_dir"]) / "browser-qa/0/native/native-process.json").exists()
    resources = RunResources(request["spec"])
    scratch = resources.browser_scratch()
    assert scratch.exists()
    cleanup = resources.finalize("cancelled")
    assert cleanup["state"] == "confirmed"
    assert not scratch.exists()


def assert_locked(path):
    descriptor = os.open(path, os.O_RDWR)
    try:
        with pytest.raises(BlockingIOError):
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        os.close(descriptor)


def assert_unlocked(path):
    descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        os.close(descriptor)


async def test_browser_port_wait_cancels_without_taking_generic_capacity(browser_checks):
    first_request = browser_checks.request("first", seconds=2)
    first = asyncio.create_task(activities.delivery_browser_qa(first_request))
    await wait_until(lambda: all(listeners(port) for port in browser_checks.ports))
    queued_request = browser_checks.request("queued")
    queued = asyncio.create_task(activities.delivery_browser_qa(queued_request))
    await wait_until(lambda: any(heart["run_id"] == "queued"
                                and heart["stage"] == "waiting-browser-ports"
                                for heart in browser_checks.hearts))
    following = browser_checks.request("generic")
    launched = []
    assert await activities._execute_check(
        following, lambda _: (launched.append(True), {"state": "passed"})[1],
    ) == {"state": "passed"}
    assert launched and not first.done()
    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued
    assert not (browser_checks.brokers["queued"].evidence_dir / "browser-qa").exists()
    assert (await first)["state"] == "passed", first.result()


@pytest.mark.parametrize("failure", ["interrupted", "exception"])
async def test_actual_interrupted_browser_retains_ports_and_generic_capacity(
    browser_checks, tmp_path, monkeypatch, failure,
):
    browser_checks.config.raw["check_concurrency"] = 1
    request = browser_checks.request("interrupted", seconds=30)
    broker = browser_checks.brokers["interrupted"]
    folder = broker.evidence_dir / "browser-qa/0/native"
    input_path = tmp_path / "owned-spec.json"
    input_path.write_text(json.dumps(request["spec"]))
    code = (
        "import json,os,pathlib,sys; "
        "from devflow_temporal.delivery_native_process import NativeProcess; "
        "spec=json.loads(pathlib.Path(sys.argv[1]).read_text());qa=spec['policy']['browser_qa']; "
        "NativeProcess(spec,pathlib.Path(sys.argv[2]),argv=qa['argv'],"
        "cwd=pathlib.Path(spec['checkout']),environment={'PATH':os.environ['PATH']},timeout=60,"
        "ports=tuple(qa['ports'].values())).run()"
    )
    worker = subprocess.Popen([sys.executable, "-c", code, str(input_path), str(folder)])
    journal = folder / "native-process.json"
    try:
        await wait_until(lambda: journal.exists() and len(read_private(journal)["owned"]) >= 2
                         and all(listeners(port) for port in browser_checks.ports))
        original_journal = read_private(journal)
        assert original_journal["intent"] == {
            "run_id": broker.spec["run_id"], "policy_digest": broker.spec["policy_digest"],
            "argv": broker.spec["policy"]["browser_qa"]["argv"], "cwd": broker.spec["checkout"],
            "environment_sha256": digest({"PATH": os.environ["PATH"]}), "timeout": 60,
            "ports": browser_checks.ports,
        }
        assert original_journal["monitor"]["pid"] != worker.pid
        worker.kill()
        worker.wait(timeout=5)
        if failure == "exception":
            original = reconcile_process

            def failed_readback(path):
                original(path)  # Real identity-bound teardown still executes.
                raise RuntimeError("owned fixture readback unavailable")

            monkeypatch.setattr("devflow_temporal.delivery_native_process.reconcile_process",
                                failed_readback)
        result = await activities.delivery_browser_qa(request)
        # Worker loss leaves the detached monitor supervising this same invocation.
        # Only its authenticated completion permits the reservations to be reused.
        assert_interrupted_browser_cleanup(
            result, broker.native_cleanup_confirmed, original_journal, read_private(journal),
            process_table(), {port: listeners(port) for port in browser_checks.ports},
            failure=failure,
        )
        root = tmp_path / "check-execution"
        assert_reservation = assert_locked if failure == "exception" else assert_unlocked
        assert_reservation(root / "slot-0.lock")
        for port in browser_checks.ports:
            assert_reservation(root / f"port-{port}.lock")
        following = browser_checks.request("following")
        launched = []
        queued = asyncio.create_task(activities._execute_check(
            following, lambda _: (launched.append(True), {"state": "passed"})[1],
        ))
        if failure == "exception":
            await asyncio.sleep(0.15)
            queued.cancel()
            with pytest.raises(asyncio.CancelledError):
                await queued
            assert not launched
        else:
            assert await asyncio.wait_for(queued, 5) == {"state": "passed"}
            assert launched
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.wait(timeout=5)
        assert reconcile_process(journal)["observed_owned_stopped"]


@pytest.mark.parametrize("failure", ["unknown", "exception"])
async def test_actual_cached_browser_reconciliation_retains_capacity(
    browser_checks, monkeypatch, failure,
):
    request = browser_checks.request("cached")
    broker = browser_checks.brokers["cached"]
    assert (await activities.delivery_browser_qa(request))["state"] == "passed"

    def uncertain(path):
        result = reconcile_process(path)
        if failure == "exception":
            raise RuntimeError("owned fixture readback unavailable")
        return {**result, "cleanup": "unknown"}

    monkeypatch.setattr("devflow_temporal.delivery_native_process.reconcile_process", uncertain)
    result = await activities.delivery_browser_qa(request)
    assert result["cleanup"] == "unknown"
    assert broker.native_cleanup_confirmed is False
    root = browser_checks.config.state_root / "check-execution"
    assert_locked(root / "slot-0.lock")
    for port in browser_checks.ports:
        assert_locked(root / f"port-{port}.lock")
