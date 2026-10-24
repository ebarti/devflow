"""Frozen execution6, independent live owner2, private amendment2 through public owners."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from pathlib import Path

import pytest
import test_delivery_blocked_scope as scoped
import test_delivery_browser_mode_recovery as browser
from test_delivery_store import service as service

from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_activities import _context, delivery_repair_preflight
from devflow_temporal.delivery_amendment_capacity import FIELD
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_execution_config import load_execution_config
from devflow_temporal.delivery_gate_retry import prepare_runtime
from devflow_temporal.delivery_resources import _gate_roots, read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.supervisor import get_supervisor


def frozen_six(service):
    store, request = service
    raw = copy.deepcopy(store.config.raw)
    raw["capacity"] = 6
    store.config.path.write_text(json.dumps(raw))
    return DeliveryStore(DeliveryConfig.load(store.config.path)), request


def cut_operational_capacity(case):
    assert case.spec["policy"]["capacity"] == 6
    owner = case.store.config.path
    live = copy.deepcopy(case.store.config.raw)
    live["capacity"] = 2
    owner.write_text(json.dumps(live))
    delta = case.command.get("browser_qa_amendment", case.command)
    private = Path(delta["amended_config_path"])
    raw = read_private(private)
    raw["capacity"] = 2
    write_private(private, raw)
    delta["amended_config_sha256"] = hashlib.sha256(private.read_bytes()).hexdigest()
    case.owner_path, case.private_path = owner, private
    return case


def paired(service, monkeypatch, kind):
    value = frozen_six(service)
    case = (
        scoped.scope_case(value, monkeypatch)
        if kind == "scope"
        else browser.browser_case(value, monkeypatch)
    )
    cut_operational_capacity(case)
    monkeypatch.setattr(resume, "prepare_runtime", prepare_runtime)
    return case


@pytest.fixture(params=["scope", "browser"])
def amendment(service, monkeypatch, request):
    return paired(service, monkeypatch, request.param)


def admit(case):
    response = case.store.continue_repair("run-1", case.command)
    current = case.store.effective_spec("run-1")
    with case.store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
    return response, current, recovery


def assert_owner_pair(case, current, recovery):
    assert case.spec["policy"]["capacity"] == current["policy"]["capacity"] == 6
    assert load_execution_config(case.spec).raw["capacity"] == 6
    assert load_execution_config(current).raw["capacity"] == 2
    receipt = recovery[FIELD]
    assert receipt["capacity"] == 2 and receipt["policy_capacity"] == 6
    assert receipt["owner_path"] == str(case.owner_path)
    assert receipt["owner_path"] != current["config_path"]
    assert receipt["live_config_sha256"] == hashlib.sha256(case.owner_path.read_bytes()).hexdigest()
    owner, _broker = _context(current)
    assert owner.config.path == case.private_path and owner.config.raw["capacity"] == 2
    assert get_supervisor(owner).capacity == 2
    bindings = {}
    _gate_roots(current, execution_specs=bindings)
    assert current in bindings.values() and case.spec in bindings.values()
    assert case.spec["policy"]["max_repairs"] == current["policy"]["max_repairs"] == 10


def test_frozen_six_live_two_private_two_preflight_preserves_originals(amendment):
    case = amendment
    original = case.folder / "request.json"
    before = original.read_bytes()
    result = case.store.repair_admission_preflight("run-1", case.command)
    assert result["authorized_through_iteration"] == 4
    assert original.read_bytes() == before
    assert case.spec["policy"]["capacity"] == 6
    assert DeliveryConfig.load(case.owner_path).raw["capacity"] == 2
    with case.store._connect() as db:
        assert not db.execute(
            "SELECT 1 FROM delivery_commands WHERE command_id=?", (case.command["command_id"],)
        ).fetchone()


def test_public_preparation_worker_resource_supervisor_preserve_six_two_pair(amendment):
    case = amendment
    with case.store._connect() as db:
        original = db.execute("SELECT request_json FROM delivery_runs").fetchone()[0]
    _response, current, recovery = admit(case)
    assert asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery})) == {
        "state": "confirmed"
    }
    assert_owner_pair(case, current, recovery)
    with case.store._connect() as db:
        assert db.execute("SELECT request_json FROM delivery_runs").fetchone()[0] == original
    # An in-memory Store pointing to the private amendment cannot appoint itself
    # owner or raise the shared supervisor, even while retaining the real spec.
    bad = DeliveryStore(
        DeliveryConfig(
            path=case.private_path, raw={**load_execution_config(current).raw, "capacity": 3}
        ),
        read_only=True,
    )
    bad.operational_execution = current
    with pytest.raises(ValueError, match="private supervisor configuration"):
        get_supervisor(bad)


@pytest.mark.parametrize("capacity", [1, 3, 6])
def test_private_capacity_must_equal_canonical_live_owner(amendment, capacity):
    case = amendment
    value = read_private(case.private_path)
    value["capacity"] = capacity
    write_private(case.private_path, value)
    delta = case.command.get("browser_qa_amendment", case.command)
    delta["amended_config_sha256"] = hashlib.sha256(case.private_path.read_bytes()).hexdigest()
    # The calling Store's value is deliberately misleading; durable submitted
    # owner2, not this private/caller value, controls the admission.
    bad = DeliveryStore(DeliveryConfig(path=case.private_path, raw=value), read_only=True)
    bad._completed_temporal_result = case.store._completed_temporal_result
    with pytest.raises(ValueError):
        bad.repair_admission_preflight("run-1", case.command)


@pytest.mark.parametrize("defect", ["missing", "symlink", "transport", "model"])
def test_owner_source_must_remain_original_available_and_compatible(amendment, defect):
    case = amendment
    if defect == "missing":
        case.owner_path.unlink()
    elif defect == "symlink":
        alternative = case.owner_path.with_name("other-owner.json")
        alternative.write_bytes(case.owner_path.read_bytes())
        case.owner_path.unlink()
        case.owner_path.symlink_to(alternative)
    else:
        value = DeliveryConfig.load(case.owner_path).raw
        if defect == "transport":
            value["queue"] = "foreign-queue"
        else:
            value["roles"]["review"]["model"] = "changed-model"
        case.owner_path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        case.store.repair_admission_preflight("run-1", case.command)


@pytest.mark.parametrize("defect", ["capacity", "bytes", "model", "missing"])
def test_worker_rechecks_independent_live_owner_after_admission(amendment, defect):
    case = amendment
    _response, current, recovery = admit(case)
    if defect == "missing":
        case.owner_path.unlink()
    elif defect == "bytes":
        case.owner_path.write_bytes(case.owner_path.read_bytes() + b"\n")
    else:
        value = DeliveryConfig.load(case.owner_path).raw
        if defect == "capacity":
            value["capacity"] = 3
        else:
            value["roles"]["review"]["model"] = "changed-model"
        case.owner_path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery}))
    assert current["policy"]["capacity"] == 6
    assert read_private(case.private_path)["capacity"] == 2


@pytest.mark.parametrize("defect", ["receipt", "policy"])
def test_matching_ack_cannot_drop_owner_or_rewrite_frozen_policy(amendment, defect):
    case = amendment
    response, current, recovery = admit(case)
    if defect == "receipt":
        del recovery[FIELD]
    else:
        current["policy"]["capacity"] = 2
        current["policy_digest"] = digest(current["policy"])
        recovery["execution_spec"] = current
    path = Path(current["state_dir"]) / resume.namespace(recovery) / "admission.json"
    write_private(path, recovery)
    response["admission_sha256"] = digest(recovery)
    with case.store._connect() as db:
        db.execute("UPDATE delivery_runs SET recovery_json=?", (canonical_json(recovery),))
        db.execute(
            "UPDATE delivery_commands SET response_json=? WHERE command_id=?",
            (canonical_json(response), case.command["command_id"]),
        )
    with pytest.raises(ValueError):
        asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery}))


def test_live_owner_race_during_actual_runtime_preparation_refuses(amendment, monkeypatch):
    case = amendment

    def changed(*args, **kwargs):
        result = prepare_runtime(*args, **kwargs)
        case.owner_path.write_bytes(case.owner_path.read_bytes() + b"\n")
        return result

    monkeypatch.setattr(resume, "prepare_runtime", changed)
    with pytest.raises(ValueError, match="operational owner changed"):
        case.store.continue_repair("run-1", case.command)
    with case.store._connect() as db:
        assert not db.execute(
            "SELECT 1 FROM delivery_commands WHERE command_id=?", (case.command["command_id"],)
        ).fetchone()
        assert case.store.state.claim_for(db, case.spec["work_id"]) is None


@pytest.mark.asyncio
async def test_scope_six_two_pair_runs_actual_preparation_worker_and_history(service, monkeypatch):
    original = scoped.scope_case
    saved = []

    def create(value, patch, **kwargs):
        case = original(frozen_six(value), patch, **kwargs)
        cut_operational_capacity(case)
        patch.setattr(resume, "prepare_runtime", prepare_runtime)
        saved.append(case)
        return case

    monkeypatch.setattr(scoped, "scope_case", create)
    await scoped.test_public_native_blocked_scope_runs_worker_same_session_and_all_normal_gates(
        service, monkeypatch
    )
    case = saved[0]
    with case.store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
    assert_owner_pair(case, case.store.effective_spec("run-1"), recovery)


@pytest.mark.asyncio
async def test_browser_six_two_pair_runs_actual_native_worker_and_history(service, monkeypatch):
    original = browser.browser_case
    saved = []

    def create(value, patch, **kwargs):
        case = original(frozen_six(value), patch, **kwargs)
        cut_operational_capacity(case)
        saved.append(case)
        return case

    monkeypatch.setattr(browser, "browser_case", create)
    await browser.test_public_browser_amendment_real_preparation_worker_native_gate_and_replay(
        service, monkeypatch
    )
    case = saved[0]
    with case.store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
    assert_owner_pair(case, case.store.effective_spec("run-1"), recovery)


def test_real_supervisor_reserves_two_slots_and_defers_third(amendment):
    case = amendment
    _response, current, _recovery = admit(case)
    store, broker = _context(current)
    supervisor = get_supervisor(store)
    keys = []
    for role in ("implement", "review", "verify"):
        request = {
            "spec": current,
            "role": role,
            "iteration": 4,
            "candidate": broker.candidate(),
            "workspace": current["checkout"],
        }
        key = supervisor._job_key(request)
        supervisor._claim(request)
        keys.append(key)
    # Exercise the owning transactional reservation without launching a provider.
    assert supervisor._admit(keys[0])
    assert supervisor._admit(keys[1])
    assert supervisor._admit(keys[2]) is None
    with store._connect() as db:
        states = dict(db.execute("SELECT job_key,state FROM delivery_attempts WHERE iteration=4"))
    assert [states[key] for key in keys] == ["starting", "starting", "queued"]
    assert current["policy"]["capacity"] == 6 and supervisor.capacity == 2
