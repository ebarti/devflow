"""Owned native public capture/migration fixture; no external provider or GitHub.

The workflow protocol is primed with already-recorded role results at the
terminal boundary. The REAL DeliveryWorkflow terminal/finalization/tracker
commands run on Temporal; activities model unavailable projection and tracker.
No history event is fabricated or inserted into the Temporal database.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import io
import json
import os
import sys
from contextlib import redirect_stdout
from datetime import UTC, datetime
from pathlib import Path

import pytest

import devflow_temporal

SOURCE = Path(devflow_temporal.__file__).resolve().parents[3]
sys.path.insert(0, str(SOURCE / "runtime/tests"))
import runtime_location_scenario as fixture  # noqa: E402

from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER  # noqa: E402
from devflow_temporal.delivery_config import DeliveryConfig  # noqa: E402
from devflow_temporal.delivery_resources import read_private, write_private  # noqa: E402
from devflow_temporal.delivery_store import DeliveryStore  # noqa: E402


async def original_histories(root):
    from temporalio import activity
    from temporalio.client import Client
    from temporalio.exceptions import ApplicationError
    from temporalio.worker import UnsandboxedWorkflowRunner, Worker

    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    case = fixture.restored(root)
    store = DeliveryStore(DeliveryConfig.load(Path(case.owner)))
    states, specs = {}, {}
    for run, protocol in (("run-1", 48), ("run-2", 39)):
        specs[run] = store.effective_spec(run)
        if run == "run-1":
            state = copy.deepcopy(case.state)
        else:
            state = {
                "run_id": run,
                "phase": "blocked",
                "execution_state": "blocked",
                "outcome": "blocked",
                "iteration": 3,
                "candidate": None,
                "pull_request": None,
                "roles": [],
                "checks": {},
                "tracker": None,
                "usage": {},
                "decision": None,
                "intake": None,
            }
        state.update(
            revision=protocol,
            iteration=3,
            cleanup="unknown",
            error="terminal tracker synchronization conflicted",
        )
        for key in ("terminal_projection_failure", "terminal_tracker_checkpoint"):
            state["checks"].pop(key, None)
        states[run] = state
        with store._connect() as db:
            db.execute("DELETE FROM delivery_closures WHERE workflow_id=?", ("delivery-" + run,))
            db.execute(
                "UPDATE delivery_runs SET phase=?,execution_state='running',outcome=NULL,"
                "protocol_revision=?,iteration=3,cleanup='unknown',error=NULL,updated_at=? "
                "WHERE run_id=?",
                (
                    "checks" if run == "run-1" else "repair",
                    protocol - 2,
                    datetime.now(UTC).isoformat(),
                    run,
                ),
            )

    @activity.defn(name="delivery_project")
    async def unavailable(_request):
        raise ApplicationError("Owned terminal projection unavailable", non_retryable=True)

    @activity.defn(name="delivery_finalize_resources")
    async def finalizer(_request):
        raise ApplicationError("Owned aggregate receipt unavailable", non_retryable=True)

    @activity.defn(name="delivery_terminal_tracker")
    async def tracker(_request):
        return {"state": "conflicted", "retryable": False}

    async def terminal(self, spec, recovery=None):
        assert recovery is None
        self.state = copy.deepcopy(states[spec["run_id"]])
        await self._project(spec, "blocked", self.state["error"])
        return self.state

    patch = pytest.MonkeyPatch()
    patch.setattr(DeliveryWorkflow, "_run_protocol", terminal)
    client = await Client.connect(
        store.config.temporal_address, data_converter=DELIVERY_DATA_CONVERTER
    )
    selected = []
    try:
        async with Worker(
            client,
            task_queue="owned-retained-capture",
            workflows=[DeliveryWorkflow],
            activities=[unavailable, finalizer, tracker],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            for run in states:
                handle = await client.start_workflow(
                    DeliveryWorkflow.run,
                    args=[specs[run], None],
                    id="delivery-" + run,
                    task_queue="owned-retained-capture",
                    memo={"request_digest": specs[run]["request_digest"]},
                )
                state = await asyncio.wait_for(handle.result(), 30)
                description = await handle.describe()
                assert state["revision"] == (48 if run == "run-1" else 39)
                assert (
                    state["checks"]["terminal_projection_failure"]["event_type"]
                    == "tracker_deadline"
                )
                assert state["checks"]["terminal_tracker_checkpoint"]["state"] == "conflicted"
                with store._connect() as db:
                    row = dict(
                        db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run,)).fetchone()
                    )
                selected.append(
                    {
                        key: row[key]
                        for key in ("run_id", "revision", "protocol_revision", "request_digest")
                    }
                )
                selected[-1].update(
                    workflow_id=description.id,
                    execution_run_id=description.run_id,
                    recovery_digest=None,
                )
                if run == "run-1":
                    case.state = state
                    case.command["expected_revision"] = 48
    finally:
        patch.undo()
    saved = read_private(root / "case.json")
    saved.update(state=case.state, command=case.command, captures=selected)
    write_private(root / "case.json", saved)


def seed(root):
    original = fixture.fixture_service

    def service(path):
        store, request = original(path)
        raw = copy.deepcopy(store.config.raw)
        raw["temporal_bin"] = "/opt/homebrew/bin/temporal"
        store.config.path.write_text(json.dumps(raw))
        return DeliveryStore(DeliveryConfig.load(store.config.path)), request

    fixture.fixture_service = service
    fixture.seed(root, "stale1169")
    asyncio.run(original_histories(root))


def migration(case, config):
    from importlib.metadata import distribution

    from devflow_temporal import delivery_runtime_location as location
    from devflow_temporal.payload import payload_digest

    after = copy.deepcopy(config.raw)
    after["codex_bin"] = str(
        distribution("openai-codex-cli-bin").locate_file("codex_cli_bin/bin/codex")
    )
    proposal = config.state_root / "operator-inputs/permanent-owner.json"
    write_private(proposal, after)
    value = {
        "command_id": "capture-six-permanent",
        "owner_before_sha256": hashlib.sha256(config.path.read_bytes()).hexdigest(),
        "owner_after_path": str(proposal),
        "owner_after_sha256": hashlib.sha256(proposal.read_bytes()).hexdigest(),
        "destination": {
            "root": str(SOURCE),
            "revision": fixture.git(SOURCE, "rev-parse", "HEAD"),
            "tree": fixture.git(SOURCE, "rev-parse", "HEAD^{tree}"),
            "payload_sha256": payload_digest(SOURCE / "runtime/src/devflow_temporal"),
        },
        "runs": ["run-" + str(i) for i in range(1, 7)],
        "host_home": str(Path.home()),
        "host_cli": case.cli,
        "host_cli_sha256": hashlib.sha256(Path(case.cli).read_bytes()).hexdigest(),
        "baseline_revision": "1ebfc71ea5522ee74dbf6cd820f01649e3197bca",
        "answer": location.ANSWER,
    }
    request = config.state_root / "operator-inputs/permanent-request.json"
    write_private(request, value)
    return request


def public(config, name, *, request=None, run=None, role=None, iteration=None):
    from devflow_temporal import delivery_control

    argv = ["devflow-delivery", "--config", str(config.path), name]
    for flag, value in (
        ("--request", request),
        ("--id", run),
        ("--role", role),
        ("--iteration", iteration),
    ):
        if value is not None:
            argv += [flag, str(value)]
    patch = pytest.MonkeyPatch()
    patch.setattr(sys, "argv", argv)
    output = io.StringIO()
    try:
        with redirect_stdout(output):
            delivery_control.main()
        return json.loads(output.getvalue())
    finally:
        patch.undo()


def capture(root, mode):
    from devflow_temporal import delivery_retained_closures as owner

    case = fixture.restored(root)
    config = DeliveryConfig.load(Path(case.owner))
    assert not Path(case.old_root).exists()
    with pytest.raises(ValueError, match="helper is missing"):
        DeliveryStore(config)
    request = migration(case, config)
    value = {
        "command_id": "owned-two-history-capture",
        "migration_path": str(request),
        "migration_sha256": hashlib.sha256(request.read_bytes()).hexdigest(),
        "captures": case.captures,
        "answer": owner.ANSWER,
        "reader": {
            "configured_path": "/opt/homebrew/bin/temporal",
            "resolved_path": "/opt/homebrew/Cellar/temporal/1.9.1/bin/temporal",
            "sha256": owner.READER_SHA,
            "version": "1.9.1",
            "server_version": "1.32.0",
            "historical_start_binary_sha256": None,
        },
    }
    path = config.state_root / "operator-inputs/capture.json"
    write_private(path, value)
    before = fixture.original_rows(config)
    if mode != "normal":
        # Faults are scoped to this owned public CLI process; production has no fault flag.
        if mode == "popen":
            original = owner.subprocess.Popen

            def crash(*args, **kwargs):
                result = original(*args, **kwargs)
                if "devflow_temporal.native_child" in args[0]:
                    os._exit(73)
                return result

            owner.subprocess.Popen = crash
        elif mode == "transaction":
            from devflow_temporal import delivery_closures

            original = delivery_closures.insert_seal

            def crash(*args, **kwargs):
                result = original(*args, **kwargs)
                os._exit(74)
                return result

            delivery_closures.insert_seal = crash
        elif mode == "ack":
            original = owner.capture

            def crash(*args, **kwargs):
                original(*args, **kwargs)
                os._exit(75)

            owner.capture = crash
        public(config, "capture-retained-closures", request=path)
        raise AssertionError("The intended owned crash did not happen")
    response = fixture.control(config, "capture-retained-closures", path)
    assert fixture.original_rows(config) == before
    assert response["kind"] == owner.SCHEMA and len(response["histories"]) == 2
    replay = fixture.control(config, "capture-retained-closures", path)
    assert replay["replayed"] is True
    assert fixture.original_rows(config) == before
    assert (config.state_root / "service.lock").stat().st_nlink == 1
    assert not (config.state_root / "runtime-installations/current.json").exists()
    write_private(
        root / "capture-result.json",
        {
            "response": response,
            "migration": str(request),
            "owner": str(config.path),
            "rows": before,
        },
    )
    print(json.dumps({"state": "captured", "rows_unchanged": True, "histories": 2}))


def install(root):
    case = fixture.restored(root)
    config = DeliveryConfig.load(Path(case.owner))
    captured = read_private(root / "capture-result.json")
    rows = fixture.original_rows(config)
    assert rows == captured["rows"]
    request = Path(captured["migration"])
    preflight = fixture.control(config, "runtime-location-preflight", request)
    assert len(preflight["authority"]["runs"]) == 6
    result = fixture.control(config, "adopt-runtime-location", request)
    assert result["state"] == "acknowledged"
    assert fixture.original_rows(config) == rows
    print(json.dumps({"state": "installed", "all_six": True, "rows_unchanged": True}))


def continue_original(root):
    from devflow_temporal.delivery_activities import delivery_repair_preflight
    from devflow_temporal.delivery_closed_projection import reconcile_completed_projections
    from devflow_temporal.delivery_closures import sealed_closure
    from devflow_temporal.delivery_native_preparation import verify_native_spec
    from devflow_temporal.delivery_resources import _gate_roots

    case = fixture.restored(root)
    case.store = DeliveryStore(DeliveryConfig.load(Path(case.owner)))
    patch = pytest.MonkeyPatch()
    fixture.scoped_external_boundaries(case, patch)

    async def maintenance():
        from temporalio.client import Client

        client = await Client.connect(
            case.store.config.temporal_address, data_converter=DELIVERY_DATA_CONVERTER
        )
        return await reconcile_completed_projections(case.store, client)

    observed = asyncio.run(maintenance())
    assert {v["run_id"] for v in observed if v["state"] == "reconciled"} == {"run-1", "run-2"}, (
        observed
    )
    assert case.store.detail("run-1")["protocol_revision"] == 48
    assert case.store.detail("run-2")["protocol_revision"] == 39
    cleanup = public(
        case.store.config, "reconcile-attempt", run="run-1", role="cleanup", iteration=3
    )
    assert cleanup["cleanup"] == "confirmed"
    case.command["expected_revision"] = 48
    request = case.store.config.state_root / "operator-inputs/finite-continuation.json"
    write_private(request, case.command)
    outcome = public(case.store.config, "repair-admission-preflight", run="run-1", request=request)
    assert outcome["authorized_through_iteration"] == 4
    response = public(case.store.config, "continue-repair", run="run-1", request=request)
    assert response["run_id"] == "run-1"
    current = case.store.effective_spec("run-1")
    with case.store._connect() as db:
        recovery = json.loads(
            db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0]
        )
        assert db.execute("SELECT COUNT(*) FROM delivery_runs").fetchone()[0] == 6
    assert (
        recovery["maximum_iteration"] == 4 and recovery["session_id"] == case.result["session_id"]
    )
    verify_native_spec(current)
    roots = {}
    _gate_roots(current, execution_specs=roots)
    assert current in roots.values() and case.spec in roots.values()
    assert asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery})) == {
        "state": "confirmed"
    }
    original = asyncio.run(sealed_closure(case.store, "delivery-run-1"))
    assert original["result"] == case.state and original["result"]["cleanup"] == "unknown"
    assert original["result"]["checks"]["local"]["state"] == "failed"
    patch.undo()
    print(
        json.dumps(
            {
                "state": "continued",
                "same_run": True,
                "finite_maximum": 4,
                "original_unknown_unchanged": True,
                "full_resource_readback": True,
            }
        )
    )


if __name__ == "__main__":
    phase, raw_root, *rest = sys.argv[1:]
    selected = {"seed": seed, "install": install, "continue": continue_original}
    if phase == "capture":
        capture(Path(raw_root), rest[0] if rest else "normal")
    else:
        selected[phase](Path(raw_root))
