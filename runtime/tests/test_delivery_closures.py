"""Closed histories outlive Temporal retention only through authentic sealed copies."""

from __future__ import annotations

import asyncio
import json
import os
import stat
from pathlib import Path

import pytest
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.client import WorkflowHistory
from temporalio.exceptions import ApplicationError
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_store import service as service

from devflow_temporal import delivery_closures as closures
from devflow_temporal import delivery_control
from devflow_temporal import delivery_feature_activities as feature_activities
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow

QUEUE = "closure-seal"


@activity.defn(name="delivery_project")
async def _project(request):
    return {"phase": request["phase"], "revision": request.get("protocol_revision", 1)}


@activity.defn(name="delivery_prepare")
async def _prepare(_request):
    raise ApplicationError("isolated preparation failed", non_retryable=True)


def _worker(client):
    return Worker(
        client, task_queue=QUEUE, workflows=[DeliveryWorkflow],
        activities=[_project, _prepare], workflow_runner=UnsandboxedWorkflowRunner(),
        workflow_failure_exception_types=[ValueError, KeyError, TypeError],
    )


async def _close(client, workflow_id, run_id, request_digest, recovery=None):
    """Run the real delivery workflow to an authentic COMPLETED closure."""
    memo = {"request_digest": request_digest}
    if recovery is not None:
        from devflow_temporal.contracts import digest

        memo["recovery_digest"] = digest(recovery)
    handle = await client.start_workflow(
        DeliveryWorkflow.run, args=[{"run_id": run_id, "provider": "fake", "policy": {}},
                                    recovery],
        id=workflow_id, task_queue=QUEUE, memo=memo,
    )
    result = await asyncio.wait_for(handle.result(), 30)
    description = await handle.describe()
    closed = {
        "workflow_id": description.id,
        "execution_run_id": description.run_id,
        "closed_at": description.close_time.isoformat(),
        "request_digest": await description.memo_value("request_digest", "unknown"),
        "recovery_digest": await description.memo_value("recovery_digest", None),
        "result": result,
    }
    return closed, await handle.fetch_history()


def _bound_store(store, address):
    """Rebind the fixture service to one Temporal server before admission."""
    path = Path(store.config.path)
    raw = json.loads(path.read_text())
    raw["temporal_address"] = address
    path.write_text(json.dumps(raw))
    return DeliveryStore(DeliveryConfig.load(path))


def _admit(store, request, run_id):
    item = {**request, "run_id": run_id, "work_id": f"work-{run_id}",
            "command_id": f"command-{run_id}",
            "issue_url": "https://github.com/example/fixture/issues/"
                         + "".join(filter(str.isdigit, run_id))}
    store.submit(item)
    store.mark_start(run_id, accepted=True)
    with store._connect() as db:
        return dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                               (run_id,)).fetchone())


def _write(path: Path, history: WorkflowHistory | dict) -> Path:
    text = history.to_json() if isinstance(history, WorkflowHistory) else json.dumps(history)
    path.write_text(text)
    return path


@pytest.mark.asyncio
async def test_closure_facts_rebuild_the_live_closure_and_refuse_partial_histories():
    async with local_temporal() as environment, _worker(environment.client):
        closed, history = await _close(environment.client, "facts-1", "run-facts",
                                       "request-digest")
    facts = await closures.closure_facts(history)
    assert {key: facts[key] for key in closed} == closed
    assert facts["status"] == "COMPLETED" and facts["result"]["outcome"] == "blocked"
    assert facts["inputs"][0]["run_id"] == "run-facts"

    events = json.loads(history.to_json())["events"]
    truncated = WorkflowHistory.from_json("facts-1", {"events": events[:-1]})
    with pytest.raises(ValueError, match="incomplete"):
        await closures.closure_facts(truncated)
    gap = WorkflowHistory.from_json("facts-1", {"events": events[:1] + events[2:]})
    with pytest.raises(ValueError, match="not contiguous at event 2"):
        await closures.closure_facts(gap)
    with pytest.raises(ValueError, match="starts workflow facts-1, not other"):
        await closures.closure_facts(WorkflowHistory.from_json("other", {"events": events}))


@pytest.mark.asyncio
async def test_expired_closure_is_restored_only_from_its_authentic_archive(service, tmp_path):
    original_store, request = service
    async with local_temporal() as retained:
        async with _worker(retained.client):
            # Server B stands for the owner's namespace after retention deleted history.
            async with local_temporal() as expired:
                store = _bound_store(original_store,
                                     expired.client.service_client.config.target_host)
                row = _admit(store, request, "run-1")
                closed, history = await _close(retained.client, "delivery-run-1", "run-1",
                                               row["request_digest"])
                # A later execution reusing the ID with another request's memo.
                _reused, reused_history = await _close(
                    retained.client, "delivery-run-1", "run-1", "another-request")
                foreign, foreign_history = await _close(
                    retained.client, "delivery-run-1-foreign", "run-1", row["request_digest"])

                with pytest.raises(closures.ClosureUnavailable, match="import-closure"):
                    store._completed_temporal_result("run-1")

                archive = _write(tmp_path / "archive.json", history)
                events = json.loads(history.to_json())["events"]
                refusals = [
                    (dict(workflow_id="delivery-run-2"), archive,
                     "starts workflow delivery-run-1, not delivery-run-2"),
                    (dict(workflow_id="delivery-run-1-foreign"),
                     _write(tmp_path / "foreign.json", foreign_history),
                     "bound to workflow delivery-run-1"),
                    ({}, _write(tmp_path / "partial.json", {"events": events[:-1]}),
                     "incomplete"),
                    ({}, _write(tmp_path / "reused.json", reused_history),
                     "different request digest"),
                ]
                for override, path, message in refusals:
                    with pytest.raises(ValueError, match=message):
                        await closures.import_archived_closure(
                            store, expired.client, run_id="run-1",
                            workflow_id=override.get("workflow_id", "delivery-run-1"),
                            history_path=path)
                assert closures.sealed_record(store, "delivery-run-1") is None

                # The CLI entry point, as the operator runs it.
                record = await asyncio.to_thread(
                    delivery_control._import_closure, store.config, "run-1",
                    "delivery-run-1", archive)
                assert record["source"] == "archive"
                assert record["status"] == "COMPLETED"
                assert record["execution_run_id"] == closed["execution_run_id"]
                # Exactly the closure the retained Temporal history returned.
                assert store._completed_temporal_result("run-1") == closed
                # Idempotent for the same history; a different one is refused.
                again = await closures.import_archived_closure(
                    store, expired.client, run_id="run-1", workflow_id="delivery-run-1",
                    history_path=archive)
                assert again["history_sha256"] == record["history_sha256"]
                altered = json.loads(history.to_json())
                altered["events"][0]["workflowExecutionStartedEventAttributes"][
                    "identity"] = "another-worker"
                with pytest.raises(closures.SealConflict, match="different history"):
                    await closures.import_archived_closure(
                        store, expired.client, run_id="run-1",
                        workflow_id="delivery-run-1",
                        history_path=_write(tmp_path / "altered.json", altered))

                sealed = Path(store.config.state_root) / "closures"
                assert stat.S_IMODE(sealed.stat().st_mode) == 0o700
                for item in sealed.iterdir():
                    assert stat.S_IMODE(item.stat().st_mode) == 0o600
                assert foreign["workflow_id"] == "delivery-run-1-foreign"

                # Never import over an execution Temporal still retains for the workflow.
                async with _worker(expired.client):
                    await _close(expired.client, "delivery-run-1", "run-1",
                                 row["request_digest"])
                    with pytest.raises(ValueError, match="Temporal still retains workflow"):
                        await closures.import_archived_closure(
                            store, expired.client, run_id="run-1",
                            workflow_id="delivery-run-1", history_path=archive)


@pytest.mark.asyncio
async def test_retained_closures_are_sealed_by_reads_and_maintenance(service):
    original_store, request = service
    async with local_temporal() as environment, _worker(environment.client):
        address = environment.client.service_client.config.target_host
        store = _bound_store(original_store, address)
        first = _admit(store, request, "run-1")
        closed, _history = await _close(environment.client, "delivery-run-1", "run-1",
                                        first["request_digest"])
        assert closures.sealed_record(store, "delivery-run-1") is None
        assert store._completed_temporal_result("run-1") == closed
        assert closures.sealed_record(store, "delivery-run-1")["source"] == "temporal"

        second = _admit(store, request, "run-2")
        await _close(environment.client, "delivery-run-2", "run-2", second["request_digest"])
        _admit(store, request, "run-3")  # dispatched, but its workflow never ran here
        observed = {item["run_id"]: item["state"] for item in
                    await closures.seal_closed_executions(store, environment.client)}
        assert observed == {"run-2": "sealed", "run-3": "expired"}
        assert closures.sealed_record(store, "delivery-run-2")["status"] == "COMPLETED"
        # Sealed and expired executions are not polled again.
        assert await closures.seal_closed_executions(store, environment.client) == []


@pytest.mark.asyncio
async def test_sealed_history_corruption_is_refused(service):
    original_store, request = service
    async with local_temporal() as environment, _worker(environment.client):
        store = _bound_store(original_store,
                             environment.client.service_client.config.target_host)
        row = _admit(store, request, "run-1")
        await _close(environment.client, "delivery-run-1", "run-1", row["request_digest"])
        store._completed_temporal_result("run-1")
    record = closures.sealed_record(store, "delivery-run-1")
    path = Path(store.config.state_root) / "closures" / f"{record['history_sha256']}.json.xz"
    import lzma

    tampered = json.loads(lzma.decompress(path.read_bytes()))
    tampered["events"][-1]["eventTime"] = "2030-01-01T00:00:00Z"
    os.chmod(path, 0o600)
    path.write_bytes(lzma.compress(json.dumps(tampered).encode()))
    with pytest.raises(ValueError, match="does not match its recorded digest"):
        await closures.sealed_closure(store, "delivery-run-1")


@pytest.mark.asyncio
async def test_unowned_runs_back_off_instead_of_starving_sealing(service):
    original_store, request = service
    async with local_temporal() as environment, _worker(environment.client):
        address = environment.client.service_client.config.target_host
        # Ten older runs admitted while the service used another Temporal address.
        previous = _bound_store(original_store, "127.0.0.1:1")
        for index in range(1, 11):
            _admit(previous, request, f"run-{index:02d}")
        store = _bound_store(previous, address)
        row = _admit(store, request, "run-11")
        await _close(environment.client, "delivery-run-11", "run-11", row["request_digest"])
        first = await closures.seal_closed_executions(store, environment.client)
        assert {item["state"] for item in first} == {"unowned"} and len(first) == 10
        second = await closures.seal_closed_executions(store, environment.client)
        assert second == [{"run_id": "run-11", "workflow_id": "delivery-run-11",
                           "state": "sealed", "status": "COMPLETED"}]


@activity.defn(name="delivery_prepare")
async def _held_prepare(_request):
    await asyncio.sleep(3600)


@pytest.mark.asyncio
async def test_worker_result_polls_again_while_the_child_has_not_closed(service, monkeypatch):
    original_store, request = service
    async with local_temporal() as environment:
        async with Worker(environment.client, task_queue=QUEUE, workflows=[DeliveryWorkflow],
                          activities=[_project, _held_prepare],
                          workflow_runner=UnsandboxedWorkflowRunner()):
            store = _bound_store(original_store,
                                 environment.client.service_client.config.target_host)
            row = _admit(store, request, "run-1")
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, args=[{"run_id": "run-1", "provider": "fake",
                                             "policy": {}}, None],
                id="delivery-run-1", task_queue=QUEUE,
                memo={"request_digest": row["request_digest"]})
            # The child's terminal projection is committed before Temporal closes it.
            with store._connect() as db:
                db.execute("UPDATE delivery_runs SET outcome='delivered' WHERE run_id='run-1'")
            monkeypatch.setattr(feature_activities, "_feature_context",
                                lambda _spec: (store, None, None, None))
            outcome = await feature_activities.delivery_feature_worker_result(
                {"spec": {}, "child_id": "run-1"})
            await handle.terminate("test finished")
    assert outcome == {"closed": False, "reason": "temporal_closure_pending"}
