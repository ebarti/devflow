"""Authenticate the original Temporal update identity for operation recovery."""

from __future__ import annotations

import asyncio
import contextvars
import json
from datetime import timedelta

from temporalio.client import WorkflowUpdateFailedError

from .contracts import digest

_operation = contextvars.ContextVar("delivery_operation", default=None)
_NAMES = {
    "decision": "decision",
    "cancel": "cancel",
    "disable-limits": "disable_delivery_limits",
    "reconcile-tracker": "reconcile_tracker",
}


def enter(service, operation):
    return _operation.set((service, operation)) if operation["kind"] in _NAMES else None


def leave(token):
    if token is not None:
        _operation.reset(token)


def current_operation(service):
    current = _operation.get()
    return current[1] if current is not None and current[0] is service else None


def _transport(service):
    return digest(
        {
            "address": service.config.temporal_address,
            "namespace": service.config.raw.get("temporal_namespace", "default"),
        }
    )


def _reference(service, operation, workflow_id, workflow_run_id, update_name, command_id):
    if update_name != _NAMES[operation["kind"]] or command_id != operation["request"].get(
        "command_id"
    ):
        raise ValueError("Temporal update does not match the durable operation")
    reference = {
        "type": "temporal_update",
        "workflow_id": workflow_id,
        "workflow_run_id": workflow_run_id,
        "command_id": command_id,
        "name": update_name,
        "request_digest": operation["request_digest"],
        "transport_digest": _transport(service),
    }
    with service.store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            "SELECT state,effect_refs,request_digest,owner_generation FROM delivery_operations "
            "WHERE operation_id=?",
            (operation["operation_id"],),
        ).fetchone()
        if (
            row is None
            or row["state"] != "running"
            or row["request_digest"] != operation["request_digest"]
            or row["owner_generation"] != operation["owner_generation"]
        ):
            raise ValueError("operation no longer owns the Temporal update")
        refs = json.loads(row["effect_refs"]) if row["effect_refs"] else []
        old = [ref for ref in refs if ref.get("type") == "temporal_update"]
        if old and old != [reference]:
            raise ValueError("operation Temporal update identity changed")
        if not old:
            refs.append(reference)
            db.execute(
                "UPDATE delivery_operations SET effect_refs=? WHERE operation_id=?",
                (json.dumps(refs, sort_keys=True), operation["operation_id"]),
            )


class _Handle:
    def __init__(self, client, handle, workflow_id, service, operation):
        self._client, self._handle, self._workflow_id = client, handle, workflow_id
        self._service, self._operation = service, operation

    def __getattr__(self, name):
        return getattr(self._handle, name)

    async def execute_update(self, update, *args, **kwargs):
        # Record the exact target before Temporal can see the update. The
        # domain owner's existing begin/finish mutation journal is unchanged.
        description = await self._handle.describe()
        workflow_run_id = description.run_id
        if not isinstance(workflow_run_id, str) or not workflow_run_id:
            raise ValueError("Temporal execution run identity is unavailable")
        _reference(
            self._service,
            self._operation,
            self._workflow_id,
            workflow_run_id,
            update,
            kwargs.get("id"),
        )
        # The described execution may close or roll over before submission.
        # Pin the send itself to the exact recorded run, never to its successor.
        handle = self._client.get_workflow_handle(self._workflow_id, run_id=workflow_run_id)
        return await handle.execute_update(update, *args, **kwargs)


class _Client:
    def __init__(self, client, service, operation):
        self._client, self._service, self._operation = client, service, operation

    def __getattr__(self, name):
        return getattr(self._client, name)

    def get_workflow_handle(self, workflow_id, *args, **kwargs):
        return _Handle(
            self._client,
            self._client.get_workflow_handle(workflow_id, *args, **kwargs),
            workflow_id,
            self._service,
            self._operation,
        )


def wrap_client(service, client):
    current = _operation.get()
    if current is None or current[0] is not service:
        return client
    return _Client(client, service, current[1])


async def reconcile(service, operation):
    """Read the recorded update; absent/unavailable evidence remains unknown."""
    references = [
        item for item in operation["effect_refs"] if item.get("type") == "temporal_update"
    ]
    if len(references) != 1:
        return None, None
    reference = references[0]
    command_id = operation["request"].get("command_id")
    if (
        reference.get("command_id") != command_id
        or reference.get("name") != _NAMES[operation["kind"]]
        or reference.get("request_digest") != operation["request_digest"]
        or reference.get("transport_digest") != _transport(service)
        or not isinstance(reference.get("workflow_id"), str)
        or not isinstance(reference.get("workflow_run_id"), str)
    ):
        return None, None
    try:
        client = await service.client()
        handle = client.get_workflow_handle(
            reference["workflow_id"], run_id=reference["workflow_run_id"]
        )
        result = await handle.get_update_handle(command_id).result(rpc_timeout=timedelta(seconds=2))
    except WorkflowUpdateFailedError as exc:
        reason = str(exc.__cause__ or exc)
        await asyncio.to_thread(service.store.reject_mutation, command_id, reason)
        return None, reason
    except Exception:
        # Not found is not proof of no effect, and transport loss is unknown.
        return None, None
    if (
        not isinstance(result, dict)
        or not isinstance(result.get("phase"), str)
        or type(result.get("revision")) is not int
    ):
        return None, None
    run_id = operation["run_id"]
    response = {"run_id": run_id, "phase": result["phase"], "revision": result["revision"]}
    try:
        if operation["kind"] == "disable-limits":
            await asyncio.to_thread(
                service.store.record_delivery_limits_disabled, run_id, result, command_id
            )
            response["delivery_limits_disabled"] = True
        if operation["kind"] == "cancel":

            def project():
                with service.store._connect() as db:
                    row = db.execute(
                        "SELECT revision,protocol_revision,outcome FROM delivery_runs "
                        "WHERE run_id=?",
                        (run_id,),
                    ).fetchone()
                if row is None:
                    raise ValueError("run ID not found")
                if row["outcome"] is None and (row["protocol_revision"] or 0) <= result["revision"]:
                    service.store.project(
                        run_id,
                        phase="cancelling",
                        execution_state="cancelling",
                        event_type="cancel_requested",
                        message="Cancellation requested",
                        protocol_revision=result["revision"],
                        key=f"cancel:{command_id}",
                        expected_revision=row["revision"],
                    )

            await asyncio.to_thread(project)
        await asyncio.to_thread(service.store.finish_mutation, command_id, response)
    except (ValueError, OSError, RuntimeError):
        return None, None
    return response, None
