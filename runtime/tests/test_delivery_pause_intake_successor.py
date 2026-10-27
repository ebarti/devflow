"""The queued intake fence applies only to the initially selected workflow."""
import json

import pytest
from temporalio import workflow
from test_delivery_gate_retry import unpublished as unpublished  # noqa: F401
from test_delivery_stopped_resume import stopped as stopped  # noqa: F401
from test_delivery_store import service as service  # noqa: F401

from devflow_temporal.delivery_api import _intake_initial_dispatch


@workflow.defn(name="DevflowDeliveryWorkflow")
class RecoveryProbe:
    @workflow.run
    async def run(self, spec, recovery=None):
        return "recovery-started"


@pytest.mark.parametrize("workflow,recovery,expected", [
    (None, None, True),
    ("delivery-run-1", None, True),
    ("delivery-run-1-resume-abc", '{"kind":"stopped_delivery_resume"}', False),
    ("delivery-run-1-publish-recovery-1", '{"state":{"phase":"blocked"}}', False),
])
def test_initial_and_admitted_successor_are_routed_separately(workflow, recovery, expected):
    spec = {"run_id": "run-1", "pause_intake_version": 1, "execution_authority_version": 1}
    assert _intake_initial_dispatch({"workflow_id": workflow, "recovery_json": recovery}, spec) is (
        expected)


@pytest.mark.parametrize("workflow,recovery", [
    (None, '{"kind":"stopped_delivery_resume"}'),
    ("delivery-run-1", '{}'),
    ("delivery-run-1-resume-abc", None),
    ("delivery-run-1-resume-abc", '{}'),
    ("delivery-run-1-resume-abc", 'null'),
    ("delivery-run-1-resume-abc", '[]'),
])
def test_intake_ambiguous_selection_is_refused(workflow, recovery):
    spec = {"run_id": "run-1", "pause_intake_version": 1, "execution_authority_version": 1}
    with pytest.raises((ValueError, json.JSONDecodeError)):
        _intake_initial_dispatch({"workflow_id": workflow, "recovery_json": recovery}, spec)


def test_legacy_unmarked_dispatch_keeps_original_routing():
    assert not _intake_initial_dispatch({}, {"run_id": "run-1"})


@pytest.mark.asyncio
async def test_stopped_intake_recovery_dispatches_selected_successor(request, monkeypatch):
    """Use actual stopped-recovery admission and SDK dispatch; predecessor closure is controlled."""
    import asyncio
    from pathlib import Path
    from types import SimpleNamespace

    from temporal_test_server import local_temporal
    from temporalio.worker import UnsandboxedWorkflowRunner, Worker

    from devflow_temporal.delivery_api import _dispatch_start
    from devflow_temporal.delivery_config import DeliveryConfig

    async with local_temporal() as env:
        original_load = DeliveryConfig.load
        original_admit = DeliveryConfig.admit

        def load(cls, path):
            # This modifies only the fixture-owned config before its first admission.
            path = Path(path)
            if path.name == "service.json":
                data = json.loads(path.read_text())
                data["temporal_address"] = env.client.service_client.config.target_host
                path.write_text(json.dumps(data))
            return original_load(path)

        def admit(config, *args, **kwargs):
            return {**original_admit(config, *args, **kwargs), "pause_intake_version": 1}

        monkeypatch.setattr(DeliveryConfig, "load", classmethod(load))
        monkeypatch.setattr(DeliveryConfig, "admit", admit)
        store, _, _, command = request.getfixturevalue("stopped")
        with store._connect() as db:
            before = db.execute("SELECT request_json FROM delivery_runs WHERE run_id='run-1'")
            before = before.fetchone()[0]
            # Controlled initial intake resume; public resume is covered separately.
            db.execute("UPDATE delivery_pause_control SET state='running' WHERE root_id='run-1'")
        admitted = store.continue_repair("run-1", command)
        selected = admitted["workflow_id"]
        item = store.pending_starts()[0]
        spec = store.effective_spec("run-1")
        assert spec["pause_intake_version"] == 1
        assert item["workflow_id"] == selected and item["recovery_json"]
        dispatcher = SimpleNamespace(store=store, config=store.config)
        async with Worker(env.client, task_queue=store.config.queue,
                          workflow_runner=UnsandboxedWorkflowRunner(),
                          workflows=[RecoveryProbe]):
            await _dispatch_start(dispatcher, env.client, item, spec)
            assert not store.pending_starts()
            handle = env.client.get_workflow_handle(selected)
            assert await asyncio.wait_for(handle.result(), 15) == "recovery-started"
            with store._connect() as db:
                current = db.execute("SELECT request_json,workflow_id FROM delivery_runs "
                                     "WHERE run_id='run-1'").fetchone()
                assert current["request_json"] == before and current["workflow_id"] == selected
                control = db.execute("SELECT workflow_id FROM delivery_pause_control "
                                     "WHERE root_id='run-1'").fetchone()
                assert control[0] == "delivery-run-1"
                authority = db.execute("SELECT execution_run_id FROM delivery_execution_authority "
                                       "WHERE workflow_id=?", (selected,)).fetchone()
                assert authority[0] == (await handle.describe()).run_id


@pytest.mark.parametrize("version", [None, True, 2])
def test_intake_successor_requires_exact_execution_authority_version(version):
    spec = {"run_id": "run-1", "pause_intake_version": 1,
            "execution_authority_version": version}
    item = {"workflow_id": "delivery-run-1-resume-x", "recovery_json": '{"kind":"resume"}'}
    with pytest.raises(ValueError, match="execution authority"):
        _intake_initial_dispatch(item, spec)


@pytest.mark.asyncio
async def test_ambiguous_intake_selection_does_not_reach_sdk():
    from types import SimpleNamespace

    from devflow_temporal.delivery_api import _dispatch_start

    class NoSDK:
        def get_workflow_handle(self, *_args, **_kwargs):
            pytest.fail("ambiguous intake selection reached the SDK")

    spec = {"run_id": "run-1", "pause_intake_version": 1, "execution_authority_version": 1}
    item = {"workflow_id": "delivery-run-1-resume-x", "recovery_json": "{}"}
    await _dispatch_start(SimpleNamespace(), NoSDK(), item, spec)
