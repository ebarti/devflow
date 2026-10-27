"""Interrupted pre-ready custody remains unknown, even with no recorded child."""

from __future__ import annotations

import json
import multiprocessing
import os
import signal
import subprocess
import sys
import time
from importlib.metadata import distribution
from pathlib import Path

from test_delivery_api import api_fixture as api_fixture

from devflow_temporal import delivery_native_process as native
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_orphans import reconcile_attempt
from devflow_temporal.delivery_resources import RunResources, read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.supervisor import DeliverySupervisor


def _run(process):
    process.run()


def _not_live(pid, identity):
    entry = native.process_table().get(pid)
    return (
        entry is None or entry.get("identity") != identity or entry.get("stat", "Z").startswith("Z")
    )


def _wait(predicate, message):
    until = time.monotonic() + 20
    while time.monotonic() < until:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(message)


def test_real_pre_ready_child_survives_killed_owners_and_public_reconcile(
    api_fixture,
    tmp_path,
    monkeypatch,
):
    config_path, submission = api_fixture
    home = tmp_path / "owned-home"
    home.mkdir(mode=0o700)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: home))
    raw = json.loads(config_path.read_text())
    raw.update(
        provider="codex",
        execution_mode="trusted-local",
        codex_bin=str(distribution("openai-codex-cli-bin").locate_file("codex_cli_bin/bin/codex")),
        execution_registry=str(home / "registry.sqlite3"),
    )
    raw["roles"] = {role: {"model": "gpt-6.1-sol", "effort": "high"} for role in raw["roles"]}
    raw["repositories"]["fixture"]["readiness"] = {
        "version": 1,
        "generation": 0,
        "browser_engines": [],
        "temporal_checks": [],
    }
    check = {
        "id": "fixture-check",
        "argv": [str(Path(sys.executable).resolve()), "-c", "print('1 passed')"],
        "test_count_regex": r"(\d+) passed",
        "min_tests": 1,
    }
    raw["repositories"]["fixture"].update(
        prepublish_checks=[check],
        checks=[check],
        required_ci=["test"],
        project_url="https://github.com/users/example/projects/1",
        assignee="example",
    )
    config_path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(config_path))
    store.submit(submission)
    spec = store.spec(submission["run_id"])
    assert spec["readiness_version"] == 1
    broker = DeliveryBroker(store, spec)
    candidate = broker.prepare()["candidate"]
    request = {
        "spec": spec,
        "role": "implement",
        "iteration": 0,
        "candidate": candidate,
        "workspace": str(broker.checkout),
    }
    key = DeliverySupervisor._job_key(request)
    folder = Path(spec["state_dir"]) / "attempts" / key
    request.update(start_path=str(folder / "start.json"), result_path=str(folder / "result.json"))
    write_private(folder / "request.json", request)
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,"
            "state,result_path,started_at,cleanup) VALUES (?,?,?,?,?,'unknown',?,?,'unknown')",
            (
                key,
                spec["run_id"],
                "implement",
                0,
                candidate["id"],
                request["result_path"],
                "prior-owner",
            ),
        )
    commands = tmp_path / "controlled-bin"
    commands.mkdir(mode=0o700)
    paused = tmp_path / "paused-ps-pid"
    shim = commands / "ps"
    shim.write_text(f'#!/bin/sh\necho $$ > "{paused}"\nkill -STOP $$\nexec /bin/ps "$@"\n')
    shim.chmod(0o700)
    marker = tmp_path / "provider-effect"
    process = native.NativeProcess(
        spec,
        folder,
        argv=[
            sys.executable,
            "-c",
            "import pathlib,sys; pathlib.Path(sys.argv[1]).touch()",
            str(marker),
        ],
        cwd=broker.checkout,
        environment={"PATH": str(commands) + ":/usr/bin:/bin"},
        timeout=10,
    )
    real_popen = subprocess.Popen

    def isolated_monitor(args, *a, **kw):
        if isinstance(args, list) and "devflow_temporal.delivery_native_process" in args:
            kw["env"] = {**os.environ, "HOME": str(home)}
        return real_popen(args, *a, **kw)

    monkeypatch.setattr(subprocess, "Popen", isolated_monitor)
    worker = multiprocessing.get_context("fork").Process(target=_run, args=(process,))
    monitor = child_pid = child_identity = None
    try:
        worker.start()
        _wait(lambda: paused.exists() or not worker.is_alive(), "native child before ready")
        assert worker.is_alive()
        journal = read_private(folder / "native-process.json")
        assert journal["phase"] == "allocated" and journal["owned"] == {}
        assert not (folder / "ready.json").exists()
        monitor = journal["monitor"]
        table = native.process_table()
        shim_pid = int(paused.read_text())
        child_pid = table[shim_pid]["ppid"]
        child_identity = table[child_pid]["identity"]
        assert table[child_pid]["ppid"] == monitor["pid"]
        assert os.getpgid(child_pid) == child_pid
        worker.kill()
        worker.join(5)
        os.kill(monitor["pid"], signal.SIGKILL)
        _wait(lambda: _not_live(monitor["pid"], monitor["identity"]), "monitor exit")
        assert native.process_table()[child_pid]["identity"] == child_identity
        result = reconcile_attempt(store, job_key=key)
        assert result["changed"] is False and result["cleanup"] == "unknown"
        assert "cleanup remains unknown" in result["reason"]
        cli = subprocess.run(
            [
                str(Path(sys.executable).parent / "devflow-delivery"),
                "--config",
                str(config_path),
                "reconcile-attempt",
                "--job-key",
                key,
            ],
            text=True,
            capture_output=True,
            env={**os.environ, "HOME": str(home)},
            timeout=20,
        )
        assert cli.returncode == 1, cli.stderr
        assert json.loads(cli.stdout)["cleanup"] == "unknown"
        assert read_private(folder / "native-process.json")["phase"] == "allocated"
        with store._connect() as db:
            row = db.execute(
                "SELECT state,cleanup FROM delivery_attempts WHERE job_key=?", (key,)
            ).fetchone()
            assert tuple(row) == ("unknown", "unknown")
        replacement = native.NativeProcess(
            spec,
            folder,
            argv=process.argv,
            cwd=process.cwd,
            environment=process.environment,
            timeout=process.timeout,
        )
        assert replacement.run()["cleanup"] == "unknown"
        assert not marker.exists()
        os.killpg(child_pid, signal.SIGKILL)
        _wait(lambda: _not_live(child_pid, child_identity), "owned child exit")
        assert reconcile_attempt(store, job_key=key)["cleanup"] == "unknown"
        assert (
            native.reconcile_process(folder / "native-process.json", spec=spec)["cleanup"]
            == "unknown"
        )
        assert not native.reconciled_not_started(folder)
        finalization = RunResources(spec).finalize("blocked")
        assert finalization["resource_cleanup"] == "unknown"
        assert folder.exists()
    finally:
        if worker.is_alive():
            worker.kill()
            worker.join(5)
        if child_pid is not None:
            entry = native.process_table().get(child_pid)
            if entry and entry["identity"] == child_identity and not entry["stat"].startswith("Z"):
                if os.getpgid(child_pid) == child_pid:
                    os.killpg(child_pid, signal.SIGKILL)
        if monitor is not None:
            entry = native.process_table().get(monitor["pid"])
            if (
                entry
                and entry["identity"] == monitor["identity"]
                and not entry["stat"].startswith("Z")
            ):
                os.kill(monitor["pid"], signal.SIGKILL)
        if broker.checkout.exists():
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(broker.source),
                    "worktree",
                    "remove",
                    "--force",
                    str(broker.checkout),
                ],
                check=True,
                capture_output=True,
            )


def test_interrupted_authorized_journal_cannot_claim_provider_absence(tmp_path):
    from devflow_temporal.delivery_resources import write_private

    path = tmp_path / "attempt" / "native-process.json"
    intent = {"run_id": "fixture", "ports": []}
    owned = {"900001": {"identity": "old-monitor"}}
    proof = {
        "version": 1,
        "kind": "interrupted_monitor",
        "previous_phase": "authorized",
        "intent_sha256": digest(intent),
        "owned_sha256": digest(owned),
    }
    journal = {
        "phase": "finished",
        "intent": intent,
        "owned": owned,
        "monitoring_complete": False,
        "cleanup_reconciliation": proof,
        "result": {
            "interrupted_monitor": True,
            "provider_outcome": "not_started",
            "cleanup": "observed-native-confirmed",
            "stdio_drained": False,
        },
    }
    write_private(path, journal)
    assert not native.interrupted_cleanup_proven(journal)
    assert not native.lost_monitor_without_provider(journal, role_attempt=True)
    assert native.reconcile_process(path)["cleanup"] == "unknown"
    assert read_private(path) == journal
