"""Bounded public reads retain fresh custody without reconstructing it per gate."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import time
from importlib.metadata import distribution
from pathlib import Path

import httpx
import pytest
from test_delivery_stopped_resume import project, saved
from test_delivery_store import service as service
from test_delivery_unused_budget_resume import fixed_stop as fixed_stop

from devflow_temporal import delivery_resources as resources
from devflow_temporal import delivery_runtime_location as location
from devflow_temporal import delivery_runtime_service_guard as guard
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_resources import RunResources, write_private
from devflow_temporal.delivery_store import DeliveryStore


def acknowledge_reads(store, request, *, historical_specs=(), current_format=False):
    """Exact ACK reader contracts; real native migration has a separate integration."""
    for index in range(2, 7):
        store.submit(
            {
                **request,
                "command_id": f"owned-read-{index}",
                "run_id": f"run-{index}",
                "work_id": f"work-{index}",
                "issue_url": f"https://github.com/example/fixture/issues/{index + 2}",
                "branch": f"feat/read-{index}",
            }
        )
    config = store.config
    config.path.chmod(0o600)
    command = {"command_id": "owned-read-location", "runs": [f"run-{i}" for i in range(1, 7)]}
    folder = config.state_root / "runtime-installations" / command["command_id"]
    folder.mkdir(parents=True, mode=0o700)
    lock = config.state_root / "service.lock"
    lock.touch(mode=0o600)
    descriptor = os.open(lock, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        service_guard = guard.describe(config, command["command_id"], descriptor)
        os.link(lock, service_guard["link"])
    finally:
        os.close(descriptor)
    with store._connect() as db:
        rows = [dict(row) for row in db.execute("SELECT * FROM delivery_runs ORDER BY run_id")]
    runs = []
    for row in rows:
        original = store.spec(row["run_id"]) if current_format else json.loads(row["request_json"])
        runs.append(
            {
                "run_id": row["run_id"],
                "request_digest": row["request_digest"],
                "request_json_sha256": hashlib.sha256(row["request_json"].encode()).hexdigest(),
                "original_spec": original,
                "historical_specs": list(historical_specs) if row["run_id"] == "run-1" else [],
                "row": row,
            }
        )
    destination = {"identity": {"codex_bin": config.raw["codex_bin"]}}
    if current_format:
        from devflow_temporal import delivery_runtime_installation as installation

        measured = runs[0]["original_spec"]["policy"]["native_identity"]
        root = Path(measured["codex_bin"]).parents[7]
        destination = installation.shared_destination({"root": str(root), "identity": measured})
        for item in runs:
            item["installed_native_identity"] = item["original_spec"]["policy"]["native_identity"]
    receipt = {
        "schema": location.SCHEMA,
        "command": command,
        "transport": location._transport(config),
        "runs": runs,
        "owner_path": str(config.path),
        "helpers_dir": str(config.helpers_dir),
        "destination": destination,
        "service_guard": service_guard,
    }
    path = folder / "receipt.json"
    raw = canonical_json(receipt).encode()
    if current_format:
        assert 14684252 <= len(raw) < location.RECEIPT_LIMIT, len(raw)
    path.write_bytes(raw)
    path.chmod(0o600)
    reference = {
        "command_id": command["command_id"],
        "receipt": str(path),
        "receipt_sha256": hashlib.sha256(raw).hexdigest(),
    }
    with store._connect() as db:
        for item in runs:
            key = location.command_id(receipt, item["run_id"])
            response = {
                **reference,
                "run_id": item["run_id"],
                "command_id": key,
                "original_request_digest": item["request_digest"],
            }
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (
                    key,
                    item["run_id"],
                    digest({"installation": command, "run_id": item["run_id"]}),
                    canonical_json(response),
                ),
            )
            store._event(
                db,
                item["run_id"],
                item["row"]["revision"],
                location.KIND,
                "Owned complete installation read fixture",
                response,
            )
    write_private(folder.parent / "current.json", reference)
    assert location._acknowledged(config)["command"] == command
    return receipt, reference


def test_acknowledgement_hydrates_once_and_rechecks_each_call(service, monkeypatch):
    store, request = service
    store.submit(request)
    _receipt, reference = acknowledge_reads(store, request)
    original = location.load_authority
    calls = []

    def load(*args, **kwargs):
        calls.append(args[0])
        return original(*args, **kwargs)

    monkeypatch.setattr(location, "load_authority", load)
    assert location._acknowledged(store.config)
    assert calls == [Path(reference["receipt"])]
    assert location._acknowledged(store.config)
    assert len(calls) == 2
    Path(_receipt["service_guard"]["link"]).unlink()
    with pytest.raises(guard.GuardCustodyError):
        location._acknowledged(store.config)
    assert len(calls) == 3


@pytest.mark.parametrize(
    "drift",
    ["receipt", "pointer", "owner", "command", "event", "row", "mode", "symlink", "hardlink"],
)
def test_each_acknowledgement_keeps_fresh_mutable_and_file_custody(service, drift):
    store, request = service
    store.submit(request)
    _receipt, reference = acknowledge_reads(store, request)
    assert location._acknowledged(store.config)
    path = Path(reference["receipt"])
    if drift == "receipt":
        path.write_bytes(path.read_bytes() + b" ")
    elif drift == "pointer":
        write_private(
            path.parent.parent / "current.json", {**reference, "receipt_sha256": "0" * 64}
        )
    elif drift == "owner":
        raw = copy.deepcopy(store.config.raw)
        raw["codex_bin"] = "/owned/foreign-program"
        write_private(store.config.path, raw)
    elif drift == "mode":
        path.chmod(0o644)
    elif drift == "symlink":
        target = path.with_name("replacement.json")
        path.rename(target)
        path.symlink_to(target)
    elif drift == "hardlink":
        os.link(path, path.with_name("another-name.json"))
    else:
        with store._connect() as db:
            if drift == "command":
                db.execute(
                    "DELETE FROM delivery_commands WHERE command_id=?",
                    (location.command_id(_receipt, "run-6"),),
                )
            elif drift == "event":
                db.execute(
                    "DELETE FROM delivery_events WHERE run_id='run-6' AND type=?", (location.KIND,)
                )
            else:
                db.execute(
                    "UPDATE delivery_runs SET request_json=request_json||' ' WHERE run_id='run-6'"
                )
    if drift in {"command", "event"}:
        assert location._acknowledged(store.config) is None
    else:
        with pytest.raises((ValueError, OSError)):
            location._acknowledged(store.config)


@pytest.mark.parametrize("drift", [False, True])
def test_retained_gate_authority_is_observation_local_and_rechecked(tmp_path, monkeypatch, drift):
    from test_delivery_resources import spec

    current = spec(tmp_path)
    registry = RunResources(current)
    gates = []
    for iteration in range(8):
        for role in ("review", "verify"):
            gate = Path(current["state_dir"]) / "gates" / str(iteration) / role
            resources.private_directory(gate.parent)
            registry.register(gate, "gate")
            gate.mkdir()
            registry.created(gate)
            gates.append(gate)
    original = resources._gate_roots
    calls = []

    def observe(*args, **kwargs):
        calls.append(True)
        root, values = original(*args, **kwargs)
        if drift and len(calls) > 1:
            values.add(Path(current["state_dir"]) / "changed-execution")
        return root, values

    monkeypatch.setattr(resources, "_gate_roots", observe)
    result = resources.retained_worktrees(current)
    assert len(calls) == 2
    assert result["state"] == ("unavailable" if drift else "observed")
    assert result["count"] == (None if drift else len(gates))
    if not drift:
        assert resources.retained_worktrees(current)["count"] == len(gates)
        assert len(calls) == 4


@pytest.fixture
def heavy_read(service, monkeypatch):
    store, request = service
    raw = copy.deepcopy(store.config.raw)
    raw["codex_bin"] = str(
        distribution("openai-codex-cli-bin").locate_file("codex_cli_bin/bin/codex")
    )
    # An admitted snapshot retains the entire configured repository catalog.
    # These are valid explicit check declarations, not an ignored padding key.
    for index in range(96):
        repository = copy.deepcopy(raw["repositories"]["fixture"])
        repository["checks"] = [
            {
                "id": f"owned-regression-{check}",
                "argv": [
                    "/usr/bin/true",
                    *[f"--owned-case=module-{index}-{check}-scenario-{case}" for case in range(12)],
                ],
            }
            for check in range(5)
        ]
        raw["repositories"][f"owned-module-{index}"] = repository
    store.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(store.config.path))
    # Controlled native measurement, with actual installed interpreter/SDK/CLI
    # hashes. Provider execution remains fake; the separate native integration
    # exercises the permanent-path preparation owner and real migration.
    from devflow_temporal.delivery_native_guard import NATIVE_OVERRIDES
    from devflow_temporal.delivery_native_preparation import native_identity
    from devflow_temporal.runtime_dependencies import locked_dependency_identity

    admit = DeliveryConfig.admit
    measured = None

    def measured_admit(config, body, **kwargs):
        nonlocal measured
        original = admit(config, body, **kwargs)
        if measured is None:
            probe = copy.deepcopy(original)
            probe["policy"].update(
                runtime_dependencies=locked_dependency_identity(),
                config_overrides=NATIVE_OVERRIDES,
                host_sandbox="trusted-local",
            )
            measured = native_identity(probe)
        effective = copy.deepcopy(original)
        effective["policy"]["native_identity"] = measured
        effective["policy_digest"] = digest(effective["policy"])
        return effective

    monkeypatch.setattr(DeliveryConfig, "admit", measured_admit)
    store, broker, state, command = fixed_stop.__wrapped__((store, request), monkeypatch)
    # Actual recorded check diagnostics and immutable continuation lineage,
    # rather than an unused padding member or a globally large actor list.
    state["checks"]["prepublish"]["results"].extend(
        {
            "id": f"owned-worker-case-{index}",
            "passed": False,
            "cleanup": "confirmed",
            "exit_code": 1,
            "diagnostic": "\n".join(
                f"case{index}: source module{line} expected finite worker recovery; "
                "observed failure"
                for line in range(18)
            ),
        }
        for index in range(1250)
    )
    from devflow_temporal.delivery_check_evidence import retain_artifacts

    check = Path(broker.spec["state_dir"]) / "checks/1/owned-prepublish"
    artifacts = check / "pytest-artifacts"
    resources.private_directory(artifacts)
    for index in range(810):
        (artifacts / f"worker-case-{index}.json").write_text(
            json.dumps({"case": index, "state": "failed", "synthetic": True})
        )
    state["checks"]["prepublish"]["results"][0]["artifacts"] = retain_artifacts(
        check, broker.candidate()
    )
    for index in range(10):
        role = {
            "role": "review" if index % 2 else "verify",
            "iteration": 1,
            "status": "findings",
            "cleanup": "confirmed",
            "session_id": None,
            "candidate": state["candidate"],
            "findings": [f"Owned worker regression group{index} remains failed"],
        }
        state["roles"].append(role)
        with store._connect() as db:
            db.execute(
                "INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,"
                "state,result_json,cleanup) VALUES (?,'run-1',?,1,?,'finished',?,'confirmed')",
                (
                    f"owned-role-{index}",
                    role["role"],
                    state["candidate"]["id"],
                    canonical_json(role),
                ),
            )
    project(store, state)
    history = [store.spec("run-1")]
    store.continue_repair("run-1", command)
    previous = saved(store)
    current = store.effective_spec("run-1")
    for index in range(6):
        candidate = DeliveryBroker(store, current).candidate()
        later = copy.deepcopy(state)
        later.update(
            iteration=2,
            revision=15 + index * 5,
            candidate=candidate,
            error="implementer did not establish a pass",
        )
        project(store, later)
        with store._connect() as db:
            store.state.release_work(db, current["work_id"], "external:devflow:run-1")
        closed = {
            "workflow_id": store.active_workflow_id("run-1"),
            "execution_run_id": f"owned-closed-unstarted-{index}",
            "request_digest": current["request_digest"],
            "recovery_digest": digest(previous),
            "result": later,
        }
        monkeypatch.setattr(
            store, "_completed_temporal_result", lambda *_a, value=closed, **_kw: value
        )
        store.continue_repair(
            "run-1",
            {
                "continuation_kind": "stopped_delivery_resume",
                "command_id": f"owned-read-unstarted-{index}",
                "expected_revision": later["revision"],
                "expected_iteration": 2,
                "expected_candidate_id": candidate["id"],
                "expected_candidate_head": candidate["head"],
                "additional_iterations": 0,
            },
        )
        previous, current = saved(store), store.effective_spec("run-1")
        history.append(current)
    registry = RunResources(current)
    namespaces = resources._gate_roots(current)[1]
    gates = []
    for namespace in sorted(namespaces):
        for role in ("review", "verify"):
            iteration = 1
            gate = namespace / "gates" / str(iteration) / role
            resources.private_directory(gate.parent)
            registry.register(gate, "gate")
            gate.mkdir()
            registry.created(gate)
            gates.append(gate)
    # Historical registered roots still need authority checks after teardown.
    # Keep two visible roots despite much larger historical resource custody.
    with registry.locked() as manifest:
        checkouts = sum(
            entry["kind"] == "checkout" and Path(path).exists()
            for path, entry in manifest["roots"].items()
        )
        retained_gates = 2 - checkouts
        assert 1 <= retained_gates <= len(gates)
        for gate in gates[:-retained_gates]:
            entry = manifest["roots"][str(gate)]
            resources.remove_directory(gate, entry["identity"], spec=current)
            entry.update(
                state="removed", receipt={"path": str(gate), "kind": "gate", "state": "removed"}
            )
        write_private(registry.manifest, manifest)
    with store._connect() as db:
        for index in range(65):
            store._event(
                db, "run-1", 40, "owned_readback", "Owned retained observation", {"case": index}
            )
    app = create_app(store.config.path)
    request = {
        "command_id": "unused",
        "run_id": "unused",
        "work_id": "unused",
        "issue_url": "https://github.com/example/fixture/issues/999",
        "repository_key": "fixture",
        "goal": "Owned bounded read fixture",
        "accepted_plan": "Preserve the original work.",
        "base_ref": "HEAD",
        "branch": "feat/owned",
        "authorized_endpoint": "published_unmerged",
    }
    receipt, reference = acknowledge_reads(
        app.state.delivery.store, request, historical_specs=history, current_format=True
    )
    assert location._binding(store.effective_spec("run-1")) is not None
    print(
        json.dumps(
            {
                "calibration": {
                    "receipt_bytes": Path(reference["receipt"]).stat().st_size,
                    "config_bytes": len(canonical_json(store.config.raw)),
                    "historical_gates": len(gates),
                    "roles": len(state["roles"]),
                }
            }
        )
    )
    return app, store, receipt, reference


@pytest.mark.asyncio
async def test_heavy_per_run_public_get_keeps_normal_thirty_second_boundary(
    heavy_read, monkeypatch
):
    app, store, _receipt, reference = heavy_read
    from devflow_temporal import delivery_runtime_installation as installation

    detail = app.state.delivery.store.detail
    scoped = installation.scoped_identity
    detail_calls, scoped_calls = [], []

    def read_detail(*args, **kwargs):
        detail_calls.append(True)
        return detail(*args, **kwargs)

    def scoped_identity(*args, **kwargs):
        scoped_calls.append(True)
        return scoped(*args, **kwargs)

    async def healthy():
        return True  # No transport/server is needed to validate the API read owners.

    monkeypatch.setattr(app.state.delivery.store, "detail", read_detail)
    monkeypatch.setattr(installation, "scoped_identity", scoped_identity)
    monkeypatch.setattr(app.state.delivery, "healthy_client", healthy)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(
        transport=transport, base_url=store.config.dashboard_url, timeout=30
    ) as client:
        started = time.monotonic()
        response = await asyncio.wait_for(client.get("/api/runs/run-1"), timeout=30)
        elapsed = time.monotonic() - started
        print(
            json.dumps(
                {
                    "public_get_seconds": elapsed,
                    "bytes": len(response.content),
                    "evidence": len(response.json()["evidence"]),
                    "roles": len(response.json()["run"]["roles"]),
                    "events": len(response.json()["events"]),
                }
            )
        )
        assert len(detail_calls) == 1
        assert len(scoped_calls) >= 6
        listing = await asyncio.wait_for(client.get("/api/runs?limit=6"), timeout=30)
        service_info = await asyncio.wait_for(client.get("/api/service"), timeout=30)
        assert listing.status_code == service_info.status_code == 200
        assert len(listing.json()["runs"]) == 6
        assert len(detail_calls) == 1  # Compact list/service never reconstruct full detail.
        link = Path(_receipt["service_guard"]["link"])
        link.unlink()
        try:
            refused = await asyncio.wait_for(client.get("/api/runs/run-1"), timeout=30)
            assert refused.status_code == 404
            assert "guard custody changed" in refused.json()["detail"]
        finally:
            os.link(_receipt["service_guard"]["path"], link)
        again = await asyncio.wait_for(client.get("/api/runs/run-1"), timeout=30)
        assert again.status_code == 200
    assert response.status_code == 200, response.text[:1000]
    assert response.json()["run"]["retained_worktrees"]["state"] == "observed"
    assert response.json()["run"]["retained_worktrees"]["count"] == 2
    assert response.json()["run"]["iteration"] == 2
    assert len(response.json()["run"]["roles"]) == 11
    assert len(response.json()["events"]) >= 65
    assert len(response.json()["evidence"]) >= 810
    with pytest.raises(ValueError, match="another run"):
        app.state.delivery.store.evidence_index("run-1", details={"run_id": "run-2"})
    assert elapsed < 30
    print(
        json.dumps(
            {
                "receipt_bytes": Path(reference["receipt"]).stat().st_size,
                "response_bytes": len(response.content),
                "seconds": elapsed,
            }
        )
    )
