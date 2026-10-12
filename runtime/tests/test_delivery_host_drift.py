"""Routine host drift must not strand in-flight runs or misreport process cleanup."""

from __future__ import annotations

import json
import os
import stat
from types import SimpleNamespace

import pytest
from temporalio import activity
from test_delivery_api import api_fixture as api_fixture
from test_delivery_prelaunch_capacity import native_role as native_role

import devflow_temporal.delivery_broker as broker_module
import devflow_temporal.delivery_native_preparation as native
import devflow_temporal.delivery_preparation as preparation
import devflow_temporal.delivery_transport_adoption as adoption
from devflow_temporal import delivery_sandbox, supervisor
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_activities import delivery_role
from devflow_temporal.delivery_broker import CheckPreparationFailure, DeliveryBroker
from devflow_temporal.delivery_preparation import PreparationError

FROZEN = {"os_version": "26.0.1", "architecture": "arm64", "codex_bin_sha256": "binary",
          "runtime_payload_sha256": "payload", "config_overrides": ["sealed"]}


def _spec(tmp_path, identity):
    return {
        "state_dir": str(tmp_path / "state" / "runs" / "run-1"),
        "policy": {"native_identity": identity, "host_sandbox": "trusted-local"},
        "preparation": {"schema": native.SCHEMA, "fingerprint": digest(identity),
                        "environment": {"path": str(tmp_path / "elsewhere" / "proof.json")}},
    }


def test_os_update_is_recorded_but_not_pinned_for_trusted_local(tmp_path, monkeypatch):
    spec = _spec(tmp_path, dict(FROZEN))
    monkeypatch.setattr(native, "native_identity", lambda _spec: {**FROZEN, "os_version": "27.0"})
    # Identity verification passes; the next authenticated step is the proof location.
    with pytest.raises(PreparationError, match="left its canonical private cache"):
        native.verify_native_spec(spec)


def test_native_profile_keeps_the_os_pinned_because_its_proof_measures_denials(
    tmp_path, monkeypatch,
):
    spec = _spec(tmp_path, dict(FROZEN))
    spec["policy"]["host_sandbox"] = "native-profile"
    monkeypatch.setattr(native, "native_identity", lambda _spec: {**FROZEN, "os_version": "26.1"})
    with pytest.raises(PreparationError, match="has not been frozen or changed"):
        native.verify_native_spec(spec)


def test_renewal_and_closure_readiness_tolerate_a_trusted_os_update():
    from devflow_temporal.delivery_native_renewal import _payload_only, _unchanged

    trusted = {"policy": {"host_sandbox": "trusted-local"}}
    profile = {"policy": {"host_sandbox": "native-profile"}}
    updated = {**FROZEN, "os_version": "26.1"}
    _payload_only(FROZEN, updated, trusted)
    assert _unchanged(FROZEN, updated, trusted)
    with pytest.raises(ValueError, match="non-payload identity"):
        _payload_only(FROZEN, updated, profile)
    assert not _unchanged(FROZEN, updated, profile)
    with pytest.raises(ValueError, match="non-payload identity"):
        _payload_only(FROZEN, {**updated, "codex_bin_sha256": "changed"}, trusted)


def test_resource_closure_needs_no_renewal_generation_for_a_trusted_os_update(monkeypatch):
    from devflow_temporal import delivery_resource_closure as closure

    spec = {"policy": {"host_sandbox": "trusted-local", "native_identity": dict(FROZEN)}}
    monkeypatch.setattr(native, "native_identity", lambda _spec: {**FROZEN, "os_version": "26.1"})
    monkeypatch.setattr(native, "verify_native_spec", lambda _spec: None)
    monkeypatch.setattr(closure, "_controller", lambda *_args: {
        "installed_source_root": "/source", "source_revision": "revision",
        "config_sha256": "config"})
    readiness, _controller = closure._readiness(None, spec, {}, None, None)
    assert readiness["required"] is False
    spec["policy"]["host_sandbox"] = "native-profile"
    with pytest.raises(ValueError, match="non-payload identity"):
        closure._readiness(None, spec, {}, None, None)


@pytest.mark.parametrize("field", ["codex_bin_sha256", "architecture", "config_overrides"])
def test_measured_identity_drift_still_refuses(tmp_path, monkeypatch, field):
    spec = _spec(tmp_path, dict(FROZEN))
    monkeypatch.setattr(native, "native_identity",
                        lambda _spec: {**FROZEN, "os_version": "26.1", field: "changed"})
    with pytest.raises(PreparationError, match="has not been frozen or changed"):
        native.verify_native_spec(spec)


def test_trusted_payload_update_checks_only_the_runtime_package(tmp_path, monkeypatch):
    spec = _spec(tmp_path, dict(FROZEN))
    current = {**FROZEN, "runtime_payload_sha256": "updated"}
    monkeypatch.setattr(native, "native_identity", lambda _spec: current)
    source = native.PACKAGE.parents[2]
    calls = []

    def git(path, *args):
        calls.append(args)
        if args[:1] == ("rev-parse",):
            return str(source) if args[1] == "--show-toplevel" else "revision"
        return ""  # an untracked editor directory elsewhere would only show unscoped

    monkeypatch.setattr(broker_module, "_git", git)
    monkeypatch.setattr(adoption, "_historical_payload", lambda *_args: "updated")
    with pytest.raises(PreparationError, match="left its canonical private cache"):
        native.verify_native_spec(spec)
    status = next(args for args in calls if args[0] == "status")
    # The editable import root, not the whole checkout (editor directories ignored).
    assert status[-2:] == ("--", str(native.PACKAGE.parent.relative_to(source)))


def test_changed_native_authority_is_a_preparation_failure_with_confirmed_cleanup(
    tmp_path, monkeypatch,
):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    broker = DeliveryBroker.__new__(DeliveryBroker)
    broker.spec = {"provider": "codex", "run_id": "run-1",
                   "policy": {"execution_backend": "native-macos"}}

    for failure in (PreparationError("native execution authority has not been frozen or changed"),
                    FileNotFoundError("codex binary is missing")):
        def refuse(_spec, failure=failure):
            raise failure

        monkeypatch.setattr(preparation, "verify_prepared_spec", refuse)
        with pytest.raises(CheckPreparationFailure) as raised:
            broker._run_check_list(checkout, [{"id": "unit", "argv": ["true"]}],
                                   tmp_path / "evidence", {"id": "candidate"})
        assert raised.value.results[-1] == {
            "id": "native-execution-authority", "passed": False, "cleanup": "confirmed",
            "launched": False, "failure_kind": "preparation", "diagnostic": str(failure),
        }


def _auth(path, access, refreshed=None):
    value = {"tokens": {"access_token": access}}
    if refreshed:
        value["last_refresh"] = refreshed
    path.write_text(json.dumps(value))
    os.chmod(path, 0o600)


def test_role_credential_copy_keeps_the_newest_token(tmp_path):
    source = tmp_path / "auth.json"
    copy = tmp_path / "home" / "auth.json"
    copy.parent.mkdir(mode=0o700)
    _auth(source, "A", "2026-10-11T08:00:00Z")
    delivery_sandbox._refresh_credential(copy, source.read_bytes())
    assert copy.read_bytes() == source.read_bytes()
    assert stat.S_IMODE(copy.stat().st_mode) == 0o600
    # The owner's CLI refreshed: the newer source replaces the copy.
    _auth(source, "B", "2026-10-11T10:00:00Z")
    delivery_sandbox._refresh_credential(copy, source.read_bytes())
    assert json.loads(copy.read_text())["tokens"]["access_token"] == "B"
    assert stat.S_IMODE(copy.stat().st_mode) == 0o600
    # The role refreshed its own copy later: an older source never overwrites it.
    _auth(copy, "C", "2026-10-11T12:00:00Z")
    delivery_sandbox._refresh_credential(copy, source.read_bytes())
    assert json.loads(copy.read_text())["tokens"]["access_token"] == "C"
    # A source caught mid-rewrite never replaces a usable copy.
    current = copy.read_bytes()
    delivery_sandbox._refresh_credential(copy, b'{"tokens": {"acc')
    assert copy.read_bytes() == current
    # A switched auth mode or account wins even over a more recently refreshed copy.
    api_key = json.dumps({"OPENAI_API_KEY": "sk-test", "tokens": None,
                          "last_refresh": None}).encode()
    delivery_sandbox._refresh_credential(copy, api_key)
    assert copy.read_bytes() == api_key
    _auth(copy, "D", "2026-10-11T23:00:00Z")
    other = json.dumps({"tokens": {"access_token": "E", "account_id": "other"},
                        "last_refresh": "2026-10-11T09:00:00Z"}).encode()
    delivery_sandbox._refresh_credential(copy, other)
    assert copy.read_bytes() == other
    # A foreign or widened copy is still refused.
    os.chmod(copy, 0o644)
    with pytest.raises(ValueError, match="private owned regular file"):
        delivery_sandbox._refresh_credential(copy, source.read_bytes())


@pytest.mark.asyncio
async def test_refreshed_credential_does_not_block_the_next_implement_iteration(
    native_role, monkeypatch, tmp_path,
):
    case = native_role
    home = tmp_path / "home"
    (home / ".codex").mkdir(parents=True, mode=0o700)
    auth = home / ".codex" / "auth.json"
    _auth(auth, "A")
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setattr(supervisor, "prepare_native_role", delivery_sandbox.prepare_native_role)
    monkeypatch.setattr(activity, "in_activity", lambda: True)
    monkeypatch.setattr(activity, "heartbeat", lambda *args: None)
    monkeypatch.setattr(activity, "cancellation_details", lambda: SimpleNamespace(
        cancel_requested=False, timed_out=False, not_found=False, worker_shutdown=False))

    first = await delivery_role(case.request)
    assert first["finish_reason"] == "fixture", first
    copied = case.state / "role-homes" / "implement" / "codex" / "auth.json"
    assert copied.read_bytes() == auth.read_bytes()

    _auth(auth, "B", "2026-10-11T10:00:00Z")  # `codex login` or a token refresh
    second = await delivery_role({**case.request, "iteration": 1,
                                  "resume_session": first["session_id"]})
    assert second["finish_reason"] == "fixture", second
    assert copied.read_bytes() == auth.read_bytes()
