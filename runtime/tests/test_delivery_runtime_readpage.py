"""Default-page load cannot multiply installed-owner proof work per row.

The large reader fixture uses actual native/global observation constructors and
synthetic finished journal/check facts. Its ACK is controlled; the separate
installed-location test exercises the actual public installer and native path.
"""

from __future__ import annotations

import asyncio
import copy
import json
import time
from contextlib import ExitStack
from pathlib import Path

import httpx
import pytest
from test_delivery_blocked_scope import prepared_spec, scope_case
from test_delivery_runtime_readpath import acknowledge_reads
from test_delivery_store import _write_lock_free
from test_delivery_store import service as service

from devflow_temporal import delivery_runtime_installation as installation
from devflow_temporal import delivery_runtime_location as location
from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_resources import RunResources, write_private
from devflow_temporal.delivery_store import DeliveryStore


def _observation(store, receipt):
    """Real bounded constructors; all actors and closed test facts are synthetic."""
    count = 0
    for item in receipt["runs"]:
        spec = item["original_spec"]
        path = Path(spec["state_dir"]) / "owned-read-history/native-process.json"
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        owned = {}
        for _ in range(13500):
            owned[str(10000000 + count)] = {"identity": f"Owned start 2026-10-25 {count:06d}"}
            count += 1
        journal = {
            "phase": "finished", "monitoring_complete": True, "owned": owned,
            "ports": [], "result": {"cleanup": "observed-native-confirmed"},
        }
        path.write_text(canonical_json(journal))
        path.chmod(0o600)
        for name in ("native-process.lock", "native-monitor.lock"):
            path.with_name(name).touch(mode=0o600)
        RunResources(spec).process(path)
        with ExitStack() as leases:
            item["native"] = installation._native_observation(spec, leases)
        item["closed"] = {
            "workflow_id": "delivery-" + spec["run_id"],
            "execution_run_id": "owned-closed-" + spec["run_id"], "status": "COMPLETED",
            "result": {"run_id": spec["run_id"], "phase": "blocked", "cleanup": "confirmed",
                       "checks": {"local": {"state": "failed", "junit": {"cases": [
                           {"name": f"owned-test-{i}",
                            "outcome": {"passed": False, "exit_code": 1}}
                           for i in range(1000)]}}}},
        }
    remaining = 113498 - count
    for index in range(2521):
        path = store.config.state_root / "owned-global-history" / str(index) / "native-process.json"
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        number = remaining // (2521 - index)
        owned = {}
        for _ in range(number):
            owned[str(10000000 + count)] = {"identity": f"Owned start 2026-10-25 {count:06d}"}
            count += 1
        remaining -= number
        path.write_text(canonical_json({"phase": "finished", "owned": owned, "ports": []}))
        path.chmod(0o600)
    with pytest.MonkeyPatch.context() as external:
        external.setattr(installation, "_process_views", lambda: {})
        external.setattr(installation, "_original_programs_absent", lambda _receipt: None)
        with ExitStack() as leases:
            receipt["global_native"] = installation._global_native(store.config, receipt, leases)
    assert count == 113498
    counts = {"nodes": 0, "dicts": 0, "lists": 0}

    def visit(value):
        counts["nodes"] += 1
        if isinstance(value, dict):
            counts["dicts"] += 1
            for child in value.values():
                visit(child)
        elif isinstance(value, (tuple, list)):
            counts["lists"] += 1
            for child in value:
                visit(child)

    visit(receipt)
    # Calibrate canonical size AND object cardinality before public reads. The
    # real refusal's proof was14,684,252B/604,284nodes/164,257lists; strings alone
    # do not reproduce its scanner/allocation cost.
    assert 14684252 <= len(canonical_json(receipt).encode()) < location.RECEIPT_LIMIT
    assert counts["nodes"] >= 604284 and counts["lists"] >= 164257
    assert counts["dicts"] >= 12000
    return counts


@pytest.fixture
def node_heavy_page(service, monkeypatch):
    from runtime_location_scenario import observe_resource_holder_failures

    observe_resource_holder_failures(monkeypatch)
    owner, request = service
    raw = copy.deepcopy(owner.config.raw)
    for index in range(96):
        repository = copy.deepcopy(raw["repositories"]["fixture"])
        repository["checks"] = [
            {"id": f"check-{check}", "argv": ["/usr/bin/true", *[
                f"--owned-case=module-{index}-{check}-scenario-{case}" for case in range(12)
            ]]} for check in range(5)
        ]
        raw["repositories"][f"owned-module-{index}"] = repository
    owner.config.path.write_text(json.dumps(raw))
    case = scope_case((DeliveryStore(DeliveryConfig.load(owner.config.path)), request), monkeypatch)
    assert case.spec["provider"] == "codex"  # No fake-provider custody shortcut.
    case.state["checks"]["local"] = {
        "state": "failed", "cleanup": "confirmed", "results": [
            {"id": f"owned-check-{index}", "passed": False, "exit_code": 1,
             "cleanup": "confirmed", "diagnostic": "\n".join(
                 f"case{index}: source module{line} expected finite recovery; observed failure"
                 for line in range(18))} for index in range(2500)
        ],
    }
    case.store.project(
        "run-1", phase="blocked", execution_state="blocked", outcome="blocked",
        event_type="blocked", message=case.state["error"], checks=case.state["checks"],
        candidate=case.input, pull_request=case.publication, iteration=3, protocol_revision=40,
        cleanup="confirmed", tracker=case.state["tracker"], error=case.state["error"],
    )
    case.store.continue_repair("run-1", case.command)
    current = case.store.effective_spec("run-1")
    app = create_app(case.store.config.path)
    store = app.state.delivery.store
    original_admit = DeliveryConfig.admit

    def admit(config, body, **kwargs):
        # These controlled historical ACK records predate execution readiness.
        kwargs.setdefault("_readiness_base", {})
        return prepared_spec(original_admit(config, body, **kwargs))

    monkeypatch.setattr(DeliveryConfig, "admit", admit)
    calibration = []
    receipt, reference = acknowledge_reads(
        store, request, historical_specs=[case.spec, current], current_format=True,
        observation=lambda value: calibration.append(_observation(store, value)),
    )
    with store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-6'").fetchone())
        # Forty-four saved terminal board projections exercise the historical
        # checkout-absent branch, without adding them to the six-run ACK.
        for index in range(7, 51):
            item = dict(row)
            item.update(run_id=f"run-{index}", work_id=f"work-{index}",
                        issue_url=f"https://github.com/example/fixture/issues/{index+2}",
                        phase="blocked", execution_state="blocked", outcome="blocked",
                        cleanup="confirmed", checks_json="{}")
            spec = json.loads(item["request_json"])
            spec.update(run_id=item["run_id"], work_id=item["work_id"], issue_url=item["issue_url"])
            item["request_json"] = canonical_json(spec)
            db.execute(
                "INSERT INTO delivery_runs (" + ",".join(item) + ") VALUES (" +
                ",".join("?" for _ in item) + ")", tuple(item.values()),
            )
    return app, store, receipt, reference, calibration[0]


@pytest.mark.asyncio
async def test_default_page_node_heavy_native_authority_and_concurrent_public_reads(
    node_heavy_page, monkeypatch,
):
    app, store, _receipt, reference, calibration = node_heavy_page
    calls = []
    load = location.load_authority

    def observed_load(*args, **kwargs):
        calls.append(args[0])
        return load(*args, **kwargs)

    async def healthy():
        return True  # Public service projection; no transport/server is required.

    monkeypatch.setattr(location, "load_authority", observed_load)
    monkeypatch.setattr(app.state.delivery, "healthy_client", healthy)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url=store.config.dashboard_url,
                               timeout=30) as client:
        started = time.monotonic()
        response = await asyncio.wait_for(client.get("/api/runs"), timeout=30)
        elapsed = time.monotonic() - started
        assert response.status_code == 200 and len(response.json()["runs"]) == 50
        assert len(calls) == 2  # Beginning and end; independent of rows/history.
        assert sum("execution_readback" in row for row in response.json()["runs"]) == 44
        # Finite overlapping default-page polls plus native detail/service. The
        # existing API/client30s bound is unchanged; no request is discarded.
        started = time.monotonic()
        responses = await asyncio.wait_for(asyncio.gather(
            client.get("/api/runs"), client.get("/api/runs"), client.get("/api/runs"),
            client.get("/api/runs/run-1"), client.get("/api/service"),
        ), timeout=30)
        concurrent_elapsed = time.monotonic() - started
        assert all(value.status_code == 200 for value in responses)
        # No cache crosses requests: the next normal page rehydrates twice.
        before = len(calls)
        assert (await client.get("/api/runs")).status_code == 200
        assert len(calls) - before == 2
    print(json.dumps({
        "default_page_seconds": elapsed,
        "concurrent_page_detail_service_seconds": concurrent_elapsed,
        "receipt_bytes": Path(reference["receipt"]).stat().st_size,
        "calibration": calibration, "provider": "codex", "rows": 50,
        "begin_end_hydrations": 2,
    }))
    assert elapsed < 5  # A page completes before the existing poll interval.


@pytest.mark.parametrize(
    "drift", ["guard", "pointer", "receipt", "owner", "row", "command", "event", "pending"],
)
def test_compact_page_rechecks_complete_authority_after_row_observation(
    service, monkeypatch, drift,
):
    store, request = service
    store.submit(request)
    receipt, reference = acknowledge_reads(store, request)
    original = store._compact
    changed = False

    def row(value, **kwargs):
        nonlocal changed
        assert kwargs["db"].in_transaction is False
        _write_lock_free(store)  # External observation holds no SQLite write lock.
        result = original(value, **kwargs)
        if not changed:
            changed = True
            if drift == "guard":
                Path(receipt["service_guard"]["link"]).unlink()
            elif drift == "pointer":
                write_private(Path(reference["receipt"]).parent.parent / "current.json",
                              {**reference, "receipt_sha256": "0" * 64})
            elif drift == "pending":
                write_private(Path(reference["receipt"]).parent.parent / "pending.json",
                              {**reference, "command_id": "unacknowledged"})
            elif drift == "receipt":
                path = Path(reference["receipt"])
                path.write_bytes(path.read_bytes() + b" ")
            elif drift == "owner":
                write_private(store.config.path, {**store.config.raw, "codex_bin": "/foreign"})
            else:
                with store.state.connect(store.config.tracking_db) as db:
                    if drift == "row":
                        db.execute("UPDATE delivery_runs SET request_json=request_json||' ' "
                                   "WHERE run_id='run-6'")
                    elif drift == "command":
                        db.execute("DELETE FROM delivery_commands WHERE command_id=?",
                                   (location.command_id(receipt, "run-6"),))
                    else:
                        db.execute("DELETE FROM delivery_events WHERE run_id='run-6' AND type=?",
                                   (location.KIND,))
        return result

    monkeypatch.setattr(store, "_compact", row)
    with pytest.raises((ValueError, OSError)):
        store.list_runs_page()
    assert changed
