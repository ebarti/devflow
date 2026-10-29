"""Explicit SDK activity origins for direct merge-unit calls."""
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

from devflow_temporal import delivery_merge_execution as execution


def sdk_info(store):
    selected = store._test_merge_execution
    return SimpleNamespace(workflow_id=selected["workflow_id"],
                           workflow_run_id=selected["execution_run_id"],
                           namespace=store.config.raw.get("temporal_namespace", "default"),
                           workflow_type="DevflowMergeWorkflow",
                           task_queue=store.config.queue)


@contextmanager
def selected_activity(store, spec):
    selected = store._test_merge_execution
    request = {"spec": spec, "merge_authorization": selected["grant_sha256"]}
    # Unit fixture models a completed, SDK-authenticated dispatch binding.
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        execution.bind_execution(db, spec, selected["grant_sha256"], {
            "workflow_id": selected["workflow_id"],
            "execution_run_id": selected["execution_run_id"],
        })
    with patch("temporalio.activity.info", return_value=sdk_info(store)):
        with execution.activity_scope(store, request):
            yield


def activity_call(function, store, spec, *args, **kwargs):
    with selected_activity(store, spec):
        return function(store, spec, *args, **kwargs)


async def activity_call_async(function, store, spec, *args, **kwargs):
    with selected_activity(store, spec):
        return await function(store, spec, *args, **kwargs)


async def role_call(function, broker, spec, *args, **kwargs):
    with selected_activity(broker.store, spec):
        return await function(broker, spec, *args, **kwargs)
