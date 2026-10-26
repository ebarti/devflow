"""Combined scope/tools receipts retain history without granting current Git access."""

from __future__ import annotations

import asyncio
import copy
from pathlib import Path

import pytest
import test_delivery_temporal_prerequisites as composed
from test_delivery_scope_frozen_base import advance_main
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.delivery_activities import delivery_repair_preflight
from devflow_temporal.delivery_config import scope_amended_spec, scope_receipt_spec


@pytest.fixture
def combined_scope(service, monkeypatch):
    original = composed.scope_case

    def named(*args, **kwargs):
        return original(*args, **kwargs, named_base=True)

    monkeypatch.setattr(composed, "scope_case", named)
    case = composed.temporal_case(service, monkeypatch, amendment="scope")
    advance_main(case)
    return case


@pytest.mark.parametrize("drift", ["missing_ref", "replace", "graft"])
def test_combined_history_is_data_only_but_current_worker_keeps_git_authority(
    combined_scope, drift
):
    case = combined_scope
    _response, current, recovery = composed.admitted(case)
    original_state = copy.deepcopy(case.state)
    original_result = (case.folder / "result.json").read_bytes()
    source = case.broker.source
    head = _git(source, "rev-parse", "refs/remotes/origin/main")
    if drift == "missing_ref":
        _git(source, "update-ref", "-d", "refs/remotes/origin/main")
    elif drift == "replace":
        _git(source, "replace", case.spec["base_sha"], head)
    else:
        (source / ".git/info/grafts").write_text(head + " " + case.spec["base_sha"] + "\n")
    try:
        assert case.store.effective_spec("run-1") == current
        with case.store._connect() as db:
            assert resume.custody(db, recovery) == current
        with pytest.raises(ValueError):
            asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery}))
    finally:
        if drift == "missing_ref":
            _git(source, "update-ref", "refs/remotes/origin/main", head)
        elif drift == "replace":
            _git(source, "replace", "-d", case.spec["base_sha"])
        else:
            (source / ".git/info/grafts").unlink()
    assert asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery})) == {
        "state": "confirmed"
    }
    assert recovery["state"] == case.state == original_state
    assert (case.folder / "result.json").read_bytes() == original_result
    assert current["base_sha"] == case.spec["base_sha"]
    assert current["publication_base_ref"] == "main"
    assert recovery["maximum_iteration"] == 4


def test_receipt_scope_derivation_matches_current_proposal_without_a_new_admission(combined_scope):
    case = combined_scope
    args = (case.spec, Path(case.command["amended_config_path"]),
            case.command["amended_config_sha256"], case.command["added_paths"])
    expected = scope_amended_spec(*args)
    assert scope_receipt_spec(*args) == expected
    _git(case.broker.source, "update-ref", "-d", "refs/remotes/origin/main")
    assert scope_receipt_spec(*args) == expected
    with pytest.raises(ValueError):
        scope_amended_spec(*args)


@pytest.mark.parametrize("evidence", ["scope_config", "provenance", "native_journal"])
def test_data_only_combined_history_keeps_every_original_private_binding(combined_scope, evidence):
    case = combined_scope
    _response, _current, _recovery = composed.admitted(case)
    path = {
        "scope_config": Path(case.command["amended_config_path"]),
        "provenance": case.provenance_path,
        "native_journal": case.folder / "native-process.json",
    }[evidence]
    path.write_bytes(path.read_bytes() + b"\n")
    with pytest.raises(ValueError):
        case.store.effective_spec("run-1")


@pytest.mark.asyncio
async def test_scope_tools_fast_forward_runs_actual_native_sdk_worker_and_replay(
    service, monkeypatch
):
    original = composed.scope_case

    def named(*args, **kwargs):
        case = original(*args, **kwargs, named_base=True)
        advance_main(case)
        return case

    monkeypatch.setattr(composed, "scope_case", named)
    await composed.test_composed_public_admission_actual_temporal_worker_native_sdk_and_replay(
        service, monkeypatch, "scope"
    )
