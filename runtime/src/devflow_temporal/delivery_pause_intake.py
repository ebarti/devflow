"""A closed, never-started ordinary owner has a durable logical checkpoint.

The service pump uses an independently authenticated exact-SDK no-start proof.
New intake admission remains closed until dispatcher cutover is authenticated.
Timed owners and feature or merge graphs are deliberately unsupported.
"""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from . import delivery_pause as pause
from .contracts import canonical_json, digest
from .delivery_pause_context import dispatch_enabled
from .delivery_pause_writers import checkpoint_writers
from .delivery_resources import read_private
from .delivery_workspace_custody import source_for


def _spec(store, root_id, epoch, prove_unstarted):
    if type(epoch) is not int or epoch < 1 or not callable(prove_unstarted):
        raise ValueError("intake checkpoint requires authenticated no-start proof")
    spec = store.effective_spec(root_id)
    from .delivery_checkpoints import _root
    from .delivery_limits import policy

    _root(spec)  # Authenticate the canonical run and checkout namespaces.
    if (not dispatch_enabled(spec)
            or spec.get("workspace_custody_version") != 1
            or spec.get("feature_delivery") or spec.get("feature_worker")
            or policy(spec.get("policy") or {})):
        raise ValueError("intake checkpoint owner or timer policy is unsupported")
    if not store.owns_execution(spec):
        raise ValueError("intake checkpoint lacks its admitted execution transport")
    return spec


def _source(spec):
    physical = source_for(spec)
    receipt = read_private(Path(spec["state_dir"]) / "workspace-owner.json")
    return {"path": str(physical), "receipt_sha256": digest(receipt)}


def _capture(db, store, spec, control, epoch, expected_revision, source, prove_unstarted):
    root_id = spec["run_id"]
    row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (root_id,)).fetchone()
    outbox = db.execute("SELECT * FROM delivery_outbox WHERE run_id=?", (root_id,)).fetchone()
    if (row is None or outbox is None or row["revision"] != expected_revision
            or row["execution_state"] != "queued" or row["outcome"] is not None
            or outbox["state"] not in {"pending", "unknown"}
            or control["epoch"] != epoch or control["execution_run_id"]
            or control["workflow_id"] != (row["workflow_id"] or "delivery-" + root_id)
            or os.path.lexists(spec["checkout"])):
        raise ValueError("intake checkpoint owner, outbox or checkout changed")
    for table in ("delivery_attempts", "delivery_effects", "delivery_preparations",
                  "delivery_merge_requests"):
        if db.execute(f"SELECT 1 FROM {table} WHERE run_id=? LIMIT 1", (root_id,)).fetchone():
            raise ValueError("intake checkpoint has already admitted work")
    for table in ("delivery_pause_actors", "delivery_pause_pending_merges",
                  "delivery_pause_feature_children", "delivery_pause_steps"):
        if db.execute(f"SELECT 1 FROM {table} WHERE root_id=? LIMIT 1", (root_id,)).fetchone():
            raise ValueError("intake checkpoint has an actor or admitted handoff")
    claim = store.state.claim_for(db, row["work_id"])
    if claim is None or claim["owner"] != "external:devflow:" + root_id:
        raise ValueError("intake checkpoint work claim changed")
    authority = db.execute("""SELECT * FROM delivery_execution_authority
        WHERE workflow_id=?""", (control["workflow_id"],)).fetchone()
    if authority is not None and (authority["run_id"] != root_id or authority["execution_run_id"]
            or authority["generation"] != control["owner_generation"]
            or authority["request_digest"] != row["request_digest"]
            or authority["request_json"] != row["request_json"]
            or authority["recovery_json"] != row["recovery_json"]
            or json.loads(authority["start_spec_json"]) != spec):
        raise ValueError("intake checkpoint execution reservation changed")
    proof = prove_unstarted(db, spec, control)
    selected = {"root_id": root_id, "epoch": epoch, "workflow_id": control["workflow_id"],
                "request_digest": row["request_digest"], "request_json": row["request_json"],
                "recovery_json": row["recovery_json"]}
    if (not isinstance(proof, dict) or proof.get("schema") != "devflow-sdk-no-start-v1"
            or proof.get("state") != "not_found" or proof.get("root_id") != root_id
            or proof.get("epoch") != epoch or proof.get("workflow_id") != control["workflow_id"]
            or proof.get("selection_sha256") != digest(selected)
            or proof.get("endpoint") != store.config.temporal_address
            or proof.get("namespace") != store.config.raw.get("temporal_namespace", "default")
            or not isinstance(proof.get("namespace_id"), str) or not proof["namespace_id"]):
        raise ValueError("intake SDK no-start outcome is not proven")
    return {"schema": "devflow-pause-intake-v1", "root_id": root_id, "epoch": epoch,
            "spec_sha256": digest(spec), "run": dict(row), "outbox": dict(outbox),
            "claim": dict(claim),
            "authority_sha256": digest(dict(authority)) if authority else None,
            "source": source, "no_start": proof}


@contextmanager
def _observation(store, spec, epoch, expected_revision, source, prove_unstarted, writers, *,
                 action):
    """Capture and hash the bounded intake state before acquiring its root fence."""
    root_id = spec["run_id"]
    with store._connect() as db:
        db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 4 * 1024 * 1024)
        version = db.execute("PRAGMA data_version").fetchone()[0]
        db.execute("BEGIN")
        writers.verify(db)
        control = pause._row(db, root_id)
        if action == "seal":
            if control["state"] not in {"pausing", "paused"}:
                raise ValueError("intake checkpoint is not awaiting settlement")
        elif control["state"] != "resuming" or control["epoch"] != epoch:
            raise ValueError("resume control state or epoch changed")
        pause.require_no_expiry(db, root_id, epoch)
        payload = _capture(db, store, spec, control, epoch, expected_revision,
                           source, prove_unstarted)
        encoded, sha = canonical_json(payload), digest(payload)
        if len(encoded.encode()) > 4 * 1024 * 1024:
            raise ValueError("intake checkpoint evidence exceeds its bound")
        receipt = {"checkpoint_id": "intake:" + sha, "manifest_sha256": sha,
                   "resumable": True, "local_custody": "confirmed", "remote_uncertain": [],
                   "kind": "unstarted_intake"}
        encoded_receipt = canonical_json(receipt)
        prior = db.execute("""SELECT * FROM delivery_pause_intake_checkpoints
            WHERE root_id=? AND epoch=?""", (root_id, epoch)).fetchone()
        if action == "seal":
            replay = control["checkpoint_json"] is not None
            if replay:
                if (prior is None or prior["payload_json"] != encoded
                        or prior["payload_sha256"] != sha
                        or control["checkpoint_json"] != encoded_receipt):
                    raise ValueError("intake checkpoint replay no longer matches its receipt")
            elif control["state"] != "pausing" or prior is not None:
                raise ValueError("intake checkpoint acknowledgement is inconsistent")
            stamp = None
        else:
            replay = False
            if (prior is None or prior["payload_json"] != encoded
                    or prior["payload_sha256"] != sha
                    or control["checkpoint_json"] != encoded_receipt):
                raise ValueError("intake checkpoint identity or authority changed")
            interval = db.execute("""SELECT * FROM delivery_pause_intervals WHERE root_id=?
                AND epoch=?""", (root_id, epoch)).fetchone()
            if (interval is None or interval["resumed_at"] is not None
                    or not isinstance(interval["paused_at"], str)
                    or len(interval["paused_at"]) > 128):
                raise ValueError("pause interval clock or settlement changed")
            stamp = datetime.fromisoformat(interval["paused_at"])
        db.commit()
        if db.execute("PRAGMA data_version").fetchone()[0] != version:
            raise ValueError("intake checkpoint changed during observation")
        yield db, version, receipt, encoded_receipt, encoded, sha, replay, stamp


def _fenced(db, version, writers):
    """Protect SQL observation against concurrent writes, including ABA."""
    db.execute("BEGIN IMMEDIATE")
    if db.execute("PRAGMA data_version").fetchone()[0] != version:
        raise ValueError("intake checkpoint changed before acknowledgement")
    # This still rechecks filesystem identities and inventories inside the
    # fence. It must not be treated as bounded until typed custody is available.
    writers.verify(db)


def seal_unstarted(store, root_id, epoch, expected_revision, *, prove_unstarted):
    """Seal no-work admission without starting a workflow or inventing a checkout."""
    if type(expected_revision) is not int or expected_revision < 0:
        raise ValueError("invalid intake checkpoint run revision")
    spec = _spec(store, root_id, epoch, prove_unstarted)
    _source(spec)
    with checkpoint_writers(store, spec) as writers:
        source = _source(spec)
        with _observation(store, spec, epoch, expected_revision, source, prove_unstarted,
                          writers, action="seal") as observed:
            db, version, receipt, encoded_receipt, encoded, sha, replay, _ = observed
            with pause.root_mutex(store, root_id):
                _fenced(db, version, writers)
                if replay:
                    return receipt
                db.execute("INSERT INTO delivery_pause_intake_checkpoints VALUES (?,?,?,?)",
                           (root_id, epoch, encoded, sha))
                db.execute("""UPDATE delivery_pause_control SET checkpoint_json=?,
                    revision=revision+1,updated_at=? WHERE root_id=?""",
                    (encoded_receipt, pause._now(), root_id))
                pause._notice(db, root_id)
                db.commit()
                return receipt


def activate_unstarted(store, root_id, epoch, expected_revision, *, prove_unstarted):
    """Open only a requested resume with the same retained owner and no start."""
    if type(expected_revision) is not int or expected_revision < 0:
        raise ValueError("invalid intake checkpoint run revision")
    spec = _spec(store, root_id, epoch, prove_unstarted)
    _source(spec)
    with checkpoint_writers(store, spec) as writers:
        source = _source(spec)
        with _observation(store, spec, epoch, expected_revision, source, prove_unstarted,
                          writers, action="activate") as observed:
            db, version, _, _, _, _, _, paused_at = observed
            with pause.root_mutex(store, root_id):
                _fenced(db, version, writers)
                stamp = pause._now()
                if datetime.fromisoformat(stamp) < paused_at:
                    raise ValueError("pause interval clock or settlement changed")
                db.execute("""UPDATE delivery_pause_intervals SET resumed_at=? WHERE root_id=?
                    AND epoch=?""", (stamp, root_id, epoch))
                db.execute("""UPDATE delivery_pause_actor_status SET state='resume_pending'
                    WHERE root_id=? AND state='running'""", (root_id,))
                db.execute("""UPDATE delivery_pause_control SET state='running',revision=revision+1,
                    updated_at=? WHERE root_id=?""", (stamp, root_id))
                pause._notice(db, root_id)
                result = pause._row(db, root_id)
                db.commit()
                return result


def _selection(store, spec, epoch):
    """Read the exact admitted identity used for one bounded SDK observation."""
    with store._connect() as db:
        row = db.execute("""SELECT workflow_id,request_digest,request_json,recovery_json
            FROM delivery_runs WHERE run_id=?""", (spec["run_id"],)).fetchone()
        control = pause._row(db, spec["run_id"])
        if (row is None or control["epoch"] != epoch or control["execution_run_id"]
                or control["workflow_id"] != (row["workflow_id"] or "delivery-" + spec["run_id"])):
            raise ValueError("intake SDK observation owner changed")
        return {"root_id": spec["run_id"], "epoch": epoch,
                "workflow_id": control["workflow_id"], "request_digest": row["request_digest"],
                "request_json": row["request_json"], "recovery_json": row["recovery_json"]}


async def _observe_no_start(store, spec, epoch, *, client=None):
    """Observe NOT_FOUND only for the configured Temporal endpoint and selected ID."""
    import asyncio

    from temporalio.api.enums.v1 import NamespaceState
    from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest
    from temporalio.client import Client
    from temporalio.service import RPCError, RPCStatusCode

    from .delivery_codec import DELIVERY_DATA_CONVERTER

    selected = await asyncio.to_thread(_selection, store, spec, epoch)
    namespace = store.config.raw.get("temporal_namespace", "default")
    if client is None:
        client = await Client.connect(store.config.temporal_address, namespace=namespace,
                                      data_converter=DELIVERY_DATA_CONVERTER)
    if (client.namespace != namespace
            or client.service_client.config.target_host != store.config.temporal_address):
        raise ValueError("intake SDK endpoint or namespace changed")
    observed_namespace = await asyncio.wait_for(client.workflow_service.describe_namespace(
        DescribeNamespaceRequest(namespace=namespace)), timeout=15)
    info = observed_namespace.namespace_info
    if (info.name != namespace or not info.id
            or info.state != NamespaceState.NAMESPACE_STATE_REGISTERED):
        raise ValueError("intake SDK namespace is not the registered configured namespace")
    handle = client.get_workflow_handle(selected["workflow_id"])
    try:
        await asyncio.wait_for(handle.describe(), timeout=15)
    except RPCError as exc:
        if exc.status != RPCStatusCode.NOT_FOUND:
            raise
    else:
        raise ValueError("intake selected workflow already exists; reconcile its start")
    return {"schema": "devflow-sdk-no-start-v1", "state": "not_found",
            "root_id": selected["root_id"], "epoch": selected["epoch"],
            "workflow_id": selected["workflow_id"], "selection_sha256": digest(selected),
            "endpoint": store.config.temporal_address, "namespace": namespace,
            "namespace_id": info.id, "selection": selected}


def _no_start_validator(observation):
    """Recheck the SDK observation's selected input inside the fenced transaction."""
    def validate(db, spec, control):
        row = db.execute("""SELECT workflow_id,request_digest,request_json,recovery_json
            FROM delivery_runs WHERE run_id=?""", (spec["run_id"],)).fetchone()
        if row is None:
            return False
        current = {"root_id": spec["run_id"], "epoch": control["epoch"],
                   "workflow_id": row["workflow_id"] or "delivery-" + spec["run_id"],
                   "request_digest": row["request_digest"], "request_json": row["request_json"],
                   "recovery_json": row["recovery_json"]}
        selected = observation["selection"]
        if current != selected or control["workflow_id"] != selected["workflow_id"]:
            return None
        return {key: value for key, value in observation.items() if key != "selection"}
    return validate


async def seal_with_sdk(store, root_id, epoch, expected_revision, *, client=None):
    """Seal only after a bounded exact no-start observation from the selected SDK."""
    spec = _spec(store, root_id, epoch, lambda *_: True)
    proof = await _observe_no_start(store, spec, epoch, client=client)
    from .delivery_pause_async import joined_thread

    return await joined_thread(seal_unstarted, store, root_id, epoch, expected_revision,
                               prove_unstarted=_no_start_validator(proof))


async def activate_with_sdk(store, root_id, epoch, expected_revision, *, client=None):
    """Reobserve no-start before honoring an already-requested explicit resume."""
    spec = _spec(store, root_id, epoch, lambda *_: True)
    proof = await _observe_no_start(store, spec, epoch, client=client)
    from .delivery_pause_async import joined_thread

    return await joined_thread(activate_unstarted, store, root_id, epoch, expected_revision,
                               prove_unstarted=_no_start_validator(proof))
