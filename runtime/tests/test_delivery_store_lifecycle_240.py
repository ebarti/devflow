"""Owner preflight uses the same sealed store admission as execution."""

import json

import pytest
from test_delivery_store import _scope_amendment_fixture, _sealed_prelaunch_retry_fixture

pytest_plugins = ["test_delivery_store"]


def _counts(store):
    with store._connect() as db:
        return tuple(
            db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("delivery_commands", "delivery_scope_amendments", "delivery_events")
        )


def test_scope_preflight_and_execution_share_admission(service, monkeypatch):
    store, request = service
    _original, state, command, _closed = _scope_amendment_fixture(store, request, monkeypatch)
    before = _counts(store)
    preview = store.amend_scope("run-1", command, preflight=True)["lifecycle_assessment"]
    assert preview["eligible"] and preview["candidate"]["id"] == command["expected_candidate_id"]
    assert _counts(store) == before
    response = store.amend_scope("run-1", command)
    assert response["authorized_through_iteration"] == state["iteration"] + 1


def test_prelaunch_preflight_keeps_original_candidate_and_budget(service, monkeypatch):
    store, request = service
    _spec, state, command, _folder = _sealed_prelaunch_retry_fixture(store, request, monkeypatch)
    before = _counts(store)
    preview = store.retry_prelaunch(request["run_id"], command, preflight=True)[
        "lifecycle_assessment"
    ]
    assert preview["eligible"] and preview["candidate"]["id"] == command["expected_candidate_id"]
    assert _counts(store) == before
    response = store.retry_prelaunch(request["run_id"], command)
    assert response["authorized_through_iteration"] == state["iteration"]
    with store._connect() as db:
        recovery = json.loads(
            db.execute(
                "SELECT recovery_json FROM delivery_runs WHERE run_id=?", (request["run_id"],)
            ).fetchone()[0]
        )
    assert recovery["candidate"]["id"] == command["expected_candidate_id"]
    assert recovery["maximum_iteration"] == state["iteration"]


@pytest.mark.parametrize(
    "field,value", [("expected_revision", 999), ("expected_candidate_id", "f" * 64)]
)
def test_scope_stale_request_never_mutates(service, monkeypatch, field, value):
    store, request = service
    _original, _state, command, _closed = _scope_amendment_fixture(store, request, monkeypatch)
    command[field] = value
    before = _counts(store)
    for preflight in (True, False):
        with pytest.raises(ValueError):
            store.amend_scope("run-1", command, preflight=preflight)
        assert _counts(store) == before


def test_required_ci_preflight_does_not_consume_observation(service, monkeypatch):
    store, request = service
    _spec, _state, command, _folder = _sealed_prelaunch_retry_fixture(
        store, request, monkeypatch, required_ci=True
    )
    closure = store._completed_temporal_result
    calls = []

    def read_closure(*args, **kwargs):
        calls.append(kwargs)
        return closure(*args, **kwargs)

    monkeypatch.setattr(store, "_completed_temporal_result", read_closure)

    def unexpected(*_args, **_kwargs):
        raise AssertionError("preflight must not retain or consume a CI observation")

    monkeypatch.setattr("devflow_temporal.delivery_repair.current_head_ci_evidence", unexpected)
    before = _counts(store)
    assessment = store.retry_prelaunch(request["run_id"], command, preflight=True)[
        "lifecycle_assessment"
    ]
    assert not assessment["eligible"]
    assert "ci_evidence_proof_unknown" in {item["code"] for item in assessment["blockers"]}
    assert calls and calls[0]["preserve"] is False
    assert _counts(store) == before


def test_closure_preflight_reads_live_result_without_sealing(service, monkeypatch):
    from datetime import UTC, datetime

    from temporalio.client import WorkflowExecutionStatus

    from devflow_temporal import delivery_store

    store, _request = service
    sealed = []
    monkeypatch.setattr(store, "effective_spec", lambda _run_id: {})
    monkeypatch.setattr(store, "owns_execution", lambda _spec: True)
    monkeypatch.setattr(store, "_preserve_closure", lambda *args: sealed.append(args))

    class Description:
        status = WorkflowExecutionStatus.COMPLETED
        close_time = datetime.now(UTC)
        id = "delivery-run-1"
        run_id = "execution-1"

        async def memo_value(self, key, default):
            return default

    class Handle:
        async def describe(self):
            return Description()

        async def result(self):
            return {"run_id": "run-1"}

    class Client:
        def get_workflow_handle(self, *_args, **_kwargs):
            return Handle()

    async def connect(*_args, **_kwargs):
        return Client()

    monkeypatch.setattr(delivery_store.Client, "connect", connect)
    assert store._completed_temporal_result("run-1", preserve=False)["result"] == {
        "run_id": "run-1"
    }
    assert sealed == []
    store.read_only = True
    store._completed_temporal_result("run-1")
    assert sealed == []
    store.read_only = False
    store._completed_temporal_result("run-1")
    assert len(sealed) == 1


@pytest.mark.parametrize("method", ["recover_publication", "retry_prelaunch", "amend_scope"])
def test_readonly_preflight_does_not_open_write_transaction(service, monkeypatch, method):
    store, _request = service
    store.read_only = True

    def unexpected():
        raise AssertionError("read-only preflight must not open an admission transaction")

    monkeypatch.setattr(store, "_connect", unexpected)
    assessment = getattr(store, method)("run-1", {}, preflight=True)["lifecycle_assessment"]
    assert not assessment["eligible"]
    assert "locked_proof_unavailable" in {item["code"] for item in assessment["blockers"]}
