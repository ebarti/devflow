"""Actual installed Python/SDK, public capture and owned persistent Temporal data."""

from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
from temporal_test_server import local_temporal
from test_delivery_runtime_location import ROOT, command
from test_delivery_runtime_location import installed_pair as installed_pair

SCENARIO = Path(__file__).with_name("retained_capture_scenario.py")
pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="actual native macOS installation")


async def owned_retained_case(root, old_template, new, environment, python):
    old = Path(environment["HOME"]) / ".codex/worktrees/capture/developer-workflow"
    old.parent.mkdir(parents=True, exist_ok=True)
    command(["git", "clone", "--quiet", "--no-hardlinks", "--dissociate", old_template, old])
    command(
        ["uv", "sync", "--frozen", "--offline", "--python", python],
        cwd=old / "runtime",
        env=environment,
    )
    case = root / "capture"
    original = root / "retained-temporal.sqlite3"
    async with local_temporal(dev_server_database_filename=str(original)) as temporal:
        address = temporal.client.service_client.config.target_host
        bound = {**environment, "DEVFLOW_TEST_LOCATION_TEMPORAL_ADDRESS": address}
        command(
            [old / "runtime/.venv/bin/python", "-I", SCENARIO, "seed", case], env=bound, timeout=180
        )
        cap = command(
            [new / "runtime/.venv/bin/python", "-I", SCENARIO, "receive-bound", case],
            env=bound,
            timeout=30,
        )
        assert json.loads(cap.splitlines()[-1])["sdk_refused"] is True
    state = case / "service-state"
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(original) + suffix)
        if path.exists():
            shutil.copyfile(path, state / ("temporal.sqlite3" + suffix))
            (state / ("temporal.sqlite3" + suffix)).chmod(0o600)
    (state / "temporal.log").write_text("Temporal CLI 1.9.1 (Server 1.32.0, UI 2.54.1)\n")
    (state / "temporal.log").chmod(0o600)
    # A REAL committed, uncheckpointed WAL survives an owned setup writer's exit.
    command(
        [
            python,
            "-I",
            "-c",
            "import sqlite3,os,sys;d=sqlite3.connect(sys.argv[1]);"
            "d.execute('PRAGMA journal_mode=WAL');"
            "d.execute('CREATE TABLE capture_probe(value TEXT)');"
            "d.commit();d.execute(\"INSERT INTO capture_probe VALUES ('committed WAL')\");"
            "d.commit();os._exit(0)",
            state / "temporal.sqlite3",
        ]
    )
    source_bytes = {p.name: p.read_bytes() for p in state.glob("temporal.sqlite3*")}
    assert source_bytes["temporal.sqlite3-wal"]
    shutil.rmtree(old)
    assert not old.exists()
    return case, state, address, bound, source_bytes


@pytest.mark.asyncio
async def test_actual_public_capture_crash_replay_six_migration_and_finite_continuation(
    installed_pair,
):
    root, old_template, new, environment, python = installed_pair
    case, state, address, bound, source_bytes = await owned_retained_case(
        root, old_template, new, environment, python
    )
    executable = new / "runtime/.venv/bin/python"
    volume = command([executable, "-I", SCENARIO, "volume", case], env=bound, timeout=90)
    assert json.loads(volume.splitlines()[-1])["actors"] == 909720

    def tracking():
        with closing(sqlite3.connect(case / "workflow.sqlite3")) as db:
            db.row_factory = sqlite3.Row
            rows = [dict(v) for v in db.execute("SELECT * FROM delivery_runs ORDER BY run_id")]
            count = db.execute(
                "SELECT COUNT(*) FROM delivery_closures WHERE workflow_id IN (?,?)",
                ("delivery-run-1", "delivery-run-2"),
            ).fetchone()[0]
            commands = db.execute(
                "SELECT COUNT(*) FROM delivery_commands WHERE command_id LIKE 'retained-history-%'"
            ).fetchone()[0]
            return rows, count, commands

    before, count, commands = tracking()
    assert count == commands == 0
    refused = command(
        [executable, "-I", SCENARIO, "capture", case, "inputs"], env=bound, timeout=240
    )
    assert len(json.loads(refused.splitlines()[-1])["refused"]) == 6
    assert tracking() == (before, 0, 0)
    sentinel = command(
        [executable, "-I", SCENARIO, "capture", case, "live-tail"], env=bound, timeout=120
    )
    assert json.loads(sentinel.splitlines()[-1])["far_end_live_actor_refused"] is True
    assert tracking() == (before, 0, 0)
    pre_go = command(
        [executable, "-I", SCENARIO, "capture", case, "pre-go"], env=bound, timeout=240
    )
    assert json.loads(pre_go.splitlines()[-1])["pre_go_refused"] is True
    assert tracking() == (before, 0, 0)
    assert {p.name: p.read_bytes() for p in state.glob("temporal.sqlite3*")} == source_bytes
    for mode, code in (("popen", 73), ("transaction", 74), ("ack", 75)):
        result = subprocess.run(
            [str(executable), "-I", str(SCENARIO), "capture", str(case), mode],
            env=bound,
            capture_output=True,
            text=True,
            timeout=240,
        )
        assert result.returncode == code, result.stdout[-2000:] + result.stderr[-6000:]
        rows, count, commands = tracking()
        assert rows == before
        assert (count, commands) == ((2, 1) if mode == "ack" else (0, 0))
        assert {p.name: p.read_bytes() for p in state.glob("temporal.sqlite3*")} == source_bytes
    output = command([executable, "-I", SCENARIO, "capture", case], env=bound, timeout=240)
    assert json.loads(output.splitlines()[-1])["rows_unchanged"] is True
    assert {p.name: p.read_bytes() for p in state.glob("temporal.sqlite3*")} == source_bytes
    installed = command([executable, "-I", SCENARIO, "install", case], env=bound, timeout=240)
    assert json.loads(installed.splitlines()[-1])["all_six"] is True
    # Only after the actual six-member installation, expose the retained fixture
    # server at its ORIGINAL transport for normal historical maintenance. No API,
    # delivery worker or provider is launched by the recovery operation.
    import temporal_test_server

    patch = pytest.MonkeyPatch()
    patch.setattr(temporal_test_server, "_available_port", lambda: int(address.rsplit(":", 1)[1]))
    try:
        async with local_temporal(dev_server_database_filename=str(state / "temporal.sqlite3")):
            continued = command(
                [executable, "-I", SCENARIO, "continue", case], env=bound, timeout=240
            )
    finally:
        patch.undo()
    assert json.loads(continued.splitlines()[-1]) == {
        "state": "continued",
        "same_run": True,
        "finite_maximum": 4,
        "original_unknown_unchanged": True,
        "full_resource_readback": True,
    }


@pytest.mark.asyncio
async def test_previous_ba0_public_capture_reproduces_mixed_scope_refusal(installed_pair):
    original_root, old_template, _new, environment, python = installed_pair
    root = original_root / "baseline-ba0"
    home = root / "home"
    new = home / ".local/share/devflow/developer-workflow"
    new.parent.mkdir(parents=True)
    bound = {**environment, "HOME": str(home)}
    command(["git", "clone", "--quiet", "--no-hardlinks", "--dissociate", ROOT, new])
    command(
        [
            "git",
            "-C",
            new,
            "checkout",
            "--quiet",
            "--detach",
            "ba0efc53941dd3df47c71adef1b7bc640ee5b9a4",
        ]
    )
    command(
        ["uv", "sync", "--frozen", "--offline", "--python", python], cwd=new / "runtime", env=bound
    )
    case, state, _address, bound, original = await owned_retained_case(
        root, old_template, new, bound, python
    )
    with closing(sqlite3.connect(case / "workflow.sqlite3")) as db:
        before = db.execute("SELECT * FROM delivery_runs ORDER BY run_id").fetchall()
    refused = subprocess.run(
        [str(new / "runtime/.venv/bin/python"), "-I", str(SCENARIO), "capture", str(case)],
        env=bound,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert refused.returncode == 1
    assert "capture cohort has different native runtime authority" in refused.stderr
    with closing(sqlite3.connect(case / "workflow.sqlite3")) as db:
        assert db.execute("SELECT * FROM delivery_runs ORDER BY run_id").fetchall() == before
        assert not db.execute(
            "SELECT 1 FROM delivery_commands WHERE command_id LIKE 'retained-history-%'"
        ).fetchone()
    assert {p.name: p.read_bytes() for p in state.glob("temporal.sqlite3*")} == original
    assert not (state / "closure-captures").exists()
    assert not (state / "runtime-installations/current.json").exists()
    assert (state / "service.lock").stat().st_nlink == 1


@pytest.mark.asyncio
async def test_previous_030_public_capture_reproduces_complete_authority_size_refusal(
    installed_pair,
):
    original_root, old_template, _new, environment, python = installed_pair
    root = original_root / "baseline-030"
    home = root / "home"
    new = home / ".local/share/devflow/developer-workflow"
    new.parent.mkdir(parents=True)
    bound = {**environment, "HOME": str(home)}
    command(["git", "clone", "--quiet", "--no-hardlinks", "--dissociate", ROOT, new])
    command(
        [
            "git",
            "-C",
            new,
            "checkout",
            "--quiet",
            "--detach",
            "030fda7b3c792eef0014aa4bc86c09a5f60ad091",
        ]
    )
    command(
        ["uv", "sync", "--frozen", "--offline", "--python", python], cwd=new / "runtime", env=bound
    )
    case, state, _address, bound, original = await owned_retained_case(
        root, old_template, new, bound, python
    )
    executable = new / "runtime/.venv/bin/python"
    command([executable, "-I", SCENARIO, "volume", case], env=bound, timeout=90)
    with closing(sqlite3.connect(case / "workflow.sqlite3")) as db:
        before = db.execute("SELECT * FROM delivery_runs ORDER BY run_id").fetchall()
    refused = subprocess.run(
        [str(executable), "-I", str(SCENARIO), "capture", str(case)],
        env=bound,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert refused.returncode == 1
    assert "installation evidence is not a bounded private owned file" in refused.stderr
    folder = state / "closure-captures/owned-two-history-capture"
    assert (folder / "intent.json").stat().st_size > 30571522
    assert not (folder / "master").exists()
    assert not (folder / "readers").exists()
    assert not (folder / "receipt.json").exists()
    with closing(sqlite3.connect(case / "workflow.sqlite3")) as db:
        assert db.execute("SELECT * FROM delivery_runs ORDER BY run_id").fetchall() == before
        assert not db.execute(
            "SELECT 1 FROM delivery_commands WHERE command_id LIKE 'retained-history-%'"
        ).fetchone()
    assert {p.name: p.read_bytes() for p in state.glob("temporal.sqlite3*")} == original
    assert (state / "service.lock").stat().st_nlink == 1
