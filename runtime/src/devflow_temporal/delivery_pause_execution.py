"""Authenticate the original queued-intake Temporal delivery execution."""
from __future__ import annotations

import asyncio
import json
import sqlite3

from temporalio.client import Client, WorkflowExecutionStatus

from .delivery_codec import DELIVERY_DATA_CONVERTER


def original_request(store, run_id, *, bounded=False):
    with store._connect() as db:
        if bounded:
            db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 4 * 1024 * 1024)
        row = db.execute("""SELECT request_json,request_digest,recovery_json,workflow_id
            FROM delivery_runs WHERE run_id=?""", (run_id,)).fetchone()
        if row is None:
            raise ValueError("pause run not found")
        original = {"spec": json.loads(row["request_json"]), "digest": row["request_digest"],
                    "recovery": json.loads(row["recovery_json"]) if row["recovery_json"] else None,
                    "workflow_id": row["workflow_id"] or "delivery-" + run_id}
        if original["spec"].get("execution_authority_version") == 1:
            authority = db.execute("""SELECT * FROM delivery_execution_authority
                WHERE workflow_id=?""", (original["workflow_id"],)).fetchone()
            if authority is None or authority["run_id"] != run_id:
                raise ValueError("pause actor lacks its selected execution reservation")
            original["execution_authority"] = dict(authority)
        return original


async def authenticate_original_start(store, spec, identity, *, bounded=False):
    """Authenticate the exact selected running intake execution against SDK history."""
    original = await asyncio.to_thread(original_request, store, spec["run_id"], bounded=bounded)
    if identity["workflow_id"] != original["workflow_id"]:
        raise ValueError("pause workflow is not the selected delivery start")
    reservation = original.get("execution_authority")
    if reservation is None or json.loads(reservation["start_spec_json"]) != spec:
        raise ValueError("pause actor differs from its reserved start specification")
    from .delivery_execution_authority import authenticate_original_start as authenticate

    proof = await authenticate(store, reservation, identity["execution_run_id"])
    return {**original, "sdk_start_proof": proof}


async def authenticate_closed_original_start(store, spec, identity):
    """Authenticate an exact terminal original start without granting actor custody."""
    original = await asyncio.to_thread(original_request, store, spec["run_id"], bounded=True)
    if identity["workflow_id"] != original["workflow_id"]:
        raise ValueError("pause workflow is not the selected delivery start")
    reservation = original.get("execution_authority")
    if reservation is None or json.loads(reservation["start_spec_json"]) != spec:
        raise ValueError("closed pause start requires its exact execution reservation")
    from .delivery_execution_authority import authenticate_original_start as authenticate

    client = await Client.connect(store.config.temporal_address,
        namespace=store.config.raw.get("temporal_namespace", "default"),
        data_converter=DELIVERY_DATA_CONVERTER)
    proof = await authenticate(store, reservation, identity["execution_run_id"], client=client,
                               require_running=False)
    description = await client.get_workflow_handle(
        identity["workflow_id"], run_id=identity["execution_run_id"]).describe()
    closed = {WorkflowExecutionStatus.COMPLETED, WorkflowExecutionStatus.FAILED,
              WorkflowExecutionStatus.CANCELED, WorkflowExecutionStatus.TERMINATED,
              WorkflowExecutionStatus.TIMED_OUT}
    if (description.id != identity["workflow_id"]
            or description.run_id != identity["execution_run_id"]
            or description.workflow_type != "DevflowDeliveryWorkflow"
            or description.status not in closed):
        raise ValueError("pause execution has no authenticated terminal SDK status")
    return {**original, "sdk_start_proof": proof}


def require_original_in_transaction(db, run_id, identity, original):
    """Recheck selection after network observation, under the admission lock."""
    row = db.execute("""SELECT request_json,request_digest,recovery_json,workflow_id
        FROM delivery_runs WHERE run_id=?""", (run_id,)).fetchone()
    if row is None:
        raise ValueError("pause run disappeared")
    current = {"spec": json.loads(row["request_json"]), "digest": row["request_digest"],
               "recovery": json.loads(row["recovery_json"]) if row["recovery_json"] else None,
               "workflow_id": row["workflow_id"] or "delivery-" + run_id}
    reservation = original.get("execution_authority")
    if reservation is not None:
        saved = db.execute("""SELECT * FROM delivery_execution_authority WHERE workflow_id=?""",
                           (current["workflow_id"],)).fetchone()
        saved = dict(saved) if saved else None
        if (saved is None or any(saved[key] != value for key, value in reservation.items()
                                 if key != "execution_run_id")
                or saved["execution_run_id"] not in (None, identity.get("execution_run_id"))):
            raise ValueError("pause execution reservation changed after SDK observation")
        current["execution_authority"] = reservation
    observed_original = {key: value for key, value in original.items()
                         if key != "sdk_start_proof"}
    if current != observed_original or current["workflow_id"] != identity["workflow_id"]:
        raise ValueError("pause delivery owner changed after SDK observation")


