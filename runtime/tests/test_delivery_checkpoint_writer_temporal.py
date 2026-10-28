"""A closed real SDK actor cannot enter after checkpoint ACK releases its EX lease."""

from __future__ import annotations

import asyncio
import contextvars
import json
import sys
import threading
from datetime import timedelta
from types import SimpleNamespace

import pytest
from temporal_test_server import local_temporal
from temporalio import activity, workflow
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_checkpoints import stopped
from test_delivery_store import service as service

from devflow_temporal import delivery_execution_authority as authority
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_activities import _context, _tracker_sync
from devflow_temporal.delivery_api import _dispatch_start
from devflow_temporal.delivery_checkpoints import create_checkpoint
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_github_contract import GitHubDelivery
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_writer_custody import (
    require_activity_entry,
    require_tracking_operation,
    require_writer,
    shared,
    to_thread,
)


@workflow.defn(name="DevflowDeliveryWorkflow")
class OwnedCheckpointWriter:
    @workflow.run
    async def run(self, spec, recovery=None):
        name = ("delivery_publication_validation_readback"
                if recovery and recovery.get("kind") == "published_head_validation"
                else "delivery_terminal_tracker"
                if recovery and recovery.get("kind") == "terminal_tracker_recovery"
                else "owned_checkpoint_writer")
        return await workflow.execute_activity(
            name, {"spec": spec},
            start_to_close_timeout=timedelta(seconds=30))


@pytest.mark.asyncio
async def test_real_closed_sdk_late_entry_missing_foreign_and_authorized_successor(
    service, monkeypatch, tmp_path,
):
    async with local_temporal() as env:
        original, request = service
        raw = dict(original.config.raw)
        raw["temporal_address"] = env.client.service_client.config.target_host
        original.config.path.write_text(json.dumps(raw))
        configured = DeliveryStore(DeliveryConfig.load(original.config.path))
        store, spec, _broker, row = stopped.__wrapped__((configured, request), monkeypatch)
        old_id = "delivery-" + spec["run_id"]
        successor_id = old_id + "-authorized-successor"
        release, finished = threading.Event(), threading.Event()
        events, threads = [], []
        escaped = tmp_path / "unexpected-readback-mutation"
        command = tmp_path / "owned-readback-gh"
        command.write_text("#!" + sys.executable + "\nfrom pathlib import Path\n"
                           "Path(" + repr(str(escaped)) + ").touch()\nprint('{}')\n")
        command.chmod(0o700)
        monkeypatch.setenv("DEVFLOW_GH", str(command))

        @activity.defn(name="delivery_terminal_tracker")
        async def terminal_probe(request):
            proof = await authority.authenticate_activity(store, request["spec"])
            token = authority._ACTIVITY_AUTHORITY.set(proof)
            try:
                def settle():
                    require_tracking_operation(spec, "blocked", True, terminal=True,
                                               reason="owned terminal intent")
                    for status, release, reason in (
                        ("done", True, "owned terminal intent"),
                        ("blocked", False, "owned terminal intent"),
                        ("blocked", True, "unrelated intent"),
                    ):
                        with pytest.raises(ValueError, match="exact sealed settlement grant"):
                            require_tracking_operation(spec, status, release, terminal=True,
                                                       reason=reason)
                    with pytest.raises(ValueError, match="read-only writer custody"):
                        require_writer(spec)
                    result = _tracker_sync(spec, "blocked", release=True, terminal=True,
                                           reason="owned terminal intent")
                    assert result["state"] == "recorded" and result["claim_released"]
                    events.append(("terminal_settled", proof))
                await to_thread(spec, settle)
                return {"run_id": spec["run_id"], "execution_state": "blocked",
                        "outcome": "blocked", "iteration": 1}
            finally:
                authority._ACTIVITY_AUTHORITY.reset(token)

        @activity.defn(name="delivery_publication_validation_readback")
        async def readonly_probe(request):
            proof = await authority.authenticate_activity(store, request["spec"])
            token = authority._ACTIVITY_AUTHORITY.set(proof)
            try:
                def observe():
                    require_activity_entry(spec)
                    with store._connect() as db:
                        authority.require_current(db, spec, proof, require_claim=False)
                    with pytest.raises(ValueError, match="read-only writer custody"):
                        require_writer(spec)
                    with pytest.raises(ValueError, match="read-only writer custody"):
                        with shared(spec):
                            raise AssertionError("read-only lease was promoted")
                    with pytest.raises(ValueError, match="read-only writer custody"):
                        GitHubDelivery().api("repos/owned/fixture/issues/1/comments",
                                             method="POST", body={"body": "forbidden"})
                    events.append(("readback_without_claim", proof))
                await to_thread(spec, observe)
                return {"run_id": spec["run_id"], "execution_state": "blocked",
                        "outcome": "blocked", "iteration": 1}
            finally:
                authority._ACTIVITY_AUTHORITY.reset(token)

        @activity.defn(name="owned_checkpoint_writer")
        async def probe(request):
            result = {"run_id": spec["run_id"], "execution_state": "blocked",
                      "outcome": "blocked", "iteration": 1}
            try:
                proof = await authority.authenticate_activity(store, request["spec"])
            except ValueError as exc:
                events.append(("foreign_refused", str(exc)))
                with pytest.raises(ValueError, match="authenticated|grant|authority"):
                    with shared(spec):
                        events.append(("foreign_ran", None))
                return result
            token = authority._ACTIVITY_AUTHORITY.set(proof)
            try:
                if activity.info().workflow_id == successor_id:
                    await to_thread(spec, lambda: events.append(("successor_ran", proof)))
                else:
                    context = contextvars.copy_context()

                    def delayed():
                        assert release.wait(20)
                        try:
                            _context(spec)
                            events.append(("old_context_ran", proof))
                        except ValueError as exc:
                            events.append(("old_context_refused", str(exc)))
                        try:
                            with shared(spec):
                                events.append(("old_ran", proof))
                        except ValueError as exc:
                            events.append(("old_refused", str(exc)))
                        finally:
                            finished.set()

                    thread = threading.Thread(target=lambda: context.run(delayed))
                    threads.append(thread)
                    thread.start()
                return result
            finally:
                authority._ACTIVITY_AUTHORITY.reset(token)

        service_context = SimpleNamespace(store=store, config=store.config)
        async with Worker(
            env.client, task_queue=store.config.queue,
            workflow_runner=UnsandboxedWorkflowRunner(),
            workflows=[OwnedCheckpointWriter], activities=[probe, readonly_probe, terminal_probe],
        ):
            try:
                await _dispatch_start(service_context, env.client, store.pending_starts()[0], spec)
                result = await env.client.get_workflow_handle(old_id).result()
                assert result["outcome"] == "blocked"
                receipt = await asyncio.to_thread(
                    create_checkpoint, store, spec["run_id"], "seal-closed-sdk", row["revision"])
                assert receipt["resumable"]
                with store._connect() as db:
                    cutoff = db.execute(
                        "SELECT * FROM delivery_checkpoint_writer_closures WHERE run_id=?",
                        (spec["run_id"],)).fetchone()
                    assert cutoff["workflow_id"] == old_id
                    bound = authority.read_reservation(store, spec["run_id"], old_id)
                    assert cutoff["execution_run_id"] == bound["execution_run_id"]
                release.set()
                assert await asyncio.to_thread(finished.wait, 5)
                assert any(kind == "old_refused" for kind, _ in events)
                assert any(kind == "old_context_refused" for kind, _ in events)
                assert not any(kind == "old_ran" for kind, _ in events)
                assert not any(kind == "old_context_ran" for kind, _ in events)
                with pytest.raises(ValueError, match="authenticated SDK actor"):
                    require_writer(spec)

                # A distinct real SDK identity copied from the old input is no grant.
                foreign = await env.client.execute_workflow(
                    OwnedCheckpointWriter.run, args=[spec, None],
                    id=old_id + "-foreign", task_queue=store.config.queue)
                assert foreign["outcome"] == "blocked"
                assert any(kind == "foreign_refused" for kind, _ in events)
                assert not any(kind == "foreign_ran" for kind, _ in events)

                # The fixture selects the control-plane successor explicitly;
                # public dispatch then reserves and authenticates its real event 1.
                # This proves writer admission, not a particular recovery endpoint.
                with store._connect() as db:
                    db.execute("UPDATE delivery_runs SET workflow_id=? WHERE run_id=?",
                               (successor_id, spec["run_id"]))
                    db.execute("UPDATE delivery_outbox SET state='pending' WHERE run_id=?",
                               (spec["run_id"],))
                await _dispatch_start(service_context, env.client, store.pending_starts()[0], spec)
                assert (await env.client.get_workflow_handle(successor_id).result())["outcome"]
                ran = [proof for kind, proof in events if kind == "successor_ran"]
                assert len(ran) == 1
                assert ran[0]["execution_run_id"] != cutoff["execution_run_id"]
                with store._connect() as db:
                    authority.require_current(db, spec, ran[0])
                    assert db.execute("SELECT COUNT(*) FROM delivery_checkpoint_writer_closures"
                                      ).fetchone()[0] == 1
                    store.state.release_work(db, spec["work_id"],
                                             "external:devflow:" + spec["run_id"])
                    db.execute("UPDATE delivery_runs SET workflow_id=?,recovery_json=? "
                               "WHERE run_id=?", (old_id + "-readback",
                               json.dumps({"kind": "published_head_validation"}), spec["run_id"]))
                    db.execute("UPDATE delivery_outbox SET state='pending' WHERE run_id=?",
                               (spec["run_id"],))
                await _dispatch_start(service_context, env.client, store.pending_starts()[0], spec)
                readback = await env.client.get_workflow_handle(old_id + "-readback").result()
                assert readback["outcome"]
                assert len([item for item in events if item[0] == "readback_without_claim"]) == 1
                assert not escaped.exists()
                history = await env.client.get_workflow_handle(old_id).fetch_history()
                from devflow_temporal.delivery_closures import history_sha256

                terminal = {
                    "kind": "terminal_tracker_recovery", "spec_sha256": digest(spec),
                    "command_id": "owned-terminal-grant", "seal": {}, "original_recovery": None,
                    "closed": {"workflow_id": old_id,
                               "execution_run_id": cutoff["execution_run_id"],
                               "history_sha256": history_sha256(history)},
                    "state": {"checks": {"terminal_tracker_checkpoint": {
                        "status": "blocked", "release": True, "reason": "owned terminal intent"}}},
                }
                with store._connect() as db:
                    db.execute("UPDATE delivery_runs SET workflow_id=?,recovery_json=? "
                               "WHERE run_id=?", (old_id + "-terminal", json.dumps(terminal),
                                                  spec["run_id"]))
                    db.execute("UPDATE delivery_outbox SET state='pending' WHERE run_id=?",
                               (spec["run_id"],))
                await _dispatch_start(service_context, env.client, store.pending_starts()[0], spec)
                terminal_handle = env.client.get_workflow_handle(old_id + "-terminal")
                terminal_result = await terminal_handle.result()
                assert terminal_result["outcome"] == "blocked"
                assert len([item for item in events if item[0] == "terminal_settled"]) == 1
                with store._connect() as db:
                    assert store.state.row(db, "works", spec["work_id"])["status"] == "blocked"
            finally:
                release.set()
                for thread in threads:
                    thread.join(timeout=5)
                    assert not thread.is_alive()
