"""A confirmed capacity wait does not invalidate a later native source pass."""
from __future__ import annotations

import json
from copy import deepcopy
from types import SimpleNamespace

import pytest
from test_delivery_feature_completed_build import _source

from devflow_temporal import delivery_feature_completed_build as completed
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_native_threads import NativeThreadObservation
from devflow_temporal.delivery_resources import read_private, write_private


def capacity_source(tmp_path, monkeypatch, *, error="serverOverloaded", start_error=None):
    # The existing portable fixture authenticates native files, launch, session,
    # result and journal; current source/publication metadata are explicit seams.
    f = _source(tmp_path, monkeypatch)
    path = f.folder / "native-thread-observation.json"
    observed = read_private(path)
    final = deepcopy(observed["turns"][0])
    prefix = {"thread_id": final["thread_id"], "turn_id": "capacity-turn",
              "turn": {"status": "failed", "error": {"codexErrorInfo": error}}}
    if start_error is not None:
        prefix = {"thread_id": final["thread_id"], "start_error": {
            "type": start_error, "code": -32000, "message": "Controlled provider rejection"}}
    observed["turns"] = [prefix, final]
    bind_observation(f, observed)
    return f, observed


def bind_observation(f, observed):
    path = f.folder / "native-thread-observation.json"
    write_private(path, observed)
    reference = NativeThreadObservation.reference(SimpleNamespace(
        data=observed, path=path, max_attempts=1))
    raw = read_private(f.folder / "result.json")
    raw["native_thread_observation"] = reference
    write_private(f.folder / "result.json", raw)
    result = json.loads(f.attempts[0]["result_json"])
    result["native_thread_observation"] = reference
    f.attempts[0]["result_json"] = canonical_json(result)
    journal = read_private(f.folder / "native-process.json")
    journal["provider_session"]["result_digest"] = digest(result)
    write_private(f.folder / "native-process.json", journal)


def test_completed_source_keeps_capacity_history_and_final_pass(tmp_path, monkeypatch):
    f, observed = capacity_source(tmp_path, monkeypatch)
    proof = completed._native_pass(f.spec, f.row, f.attempts, f.output)
    assert proof["observation_sha256"]
    assert read_private(f.folder / "native-thread-observation.json") == observed


@pytest.mark.parametrize("error", ["rateLimitExceeded", None])
def test_completed_source_does_not_promote_other_failed_turns(tmp_path, monkeypatch, error):
    f, _ = capacity_source(tmp_path, monkeypatch, error=error)
    with pytest.raises(ValueError, match="native thread observation"):
        completed._native_pass(f.spec, f.row, f.attempts, f.output)


@pytest.mark.parametrize("change", ["final_failed", "foreign_final"])
def test_capacity_history_still_requires_actual_same_session_pass(tmp_path, monkeypatch, change):
    f, observed = capacity_source(tmp_path, monkeypatch)
    if change == "final_failed":
        observed["turns"][-1] = deepcopy(observed["turns"][0])
    else:
        observed["turns"][0 if change == "foreign_prefix" else -1]["thread_id"] = "unrelated"
    bind_observation(f, observed)
    with pytest.raises(ValueError, match="native thread observation"):
        completed._native_pass(f.spec, f.row, f.attempts, f.output)


@pytest.mark.parametrize("kind", ["ServerBusyError", "TransportClosedError"])
def test_only_explicit_capacity_start_rejection_can_precede_pass(tmp_path, monkeypatch, kind):
    f, _ = capacity_source(tmp_path, monkeypatch, start_error=kind)
    if kind == "ServerBusyError":
        assert completed._native_pass(f.spec, f.row, f.attempts, f.output)
    else:
        with pytest.raises(ValueError, match="native thread observation"):
            completed._native_pass(f.spec, f.row, f.attempts, f.output)
