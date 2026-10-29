"""Versioned stopped installation authority, separate from original execution grants.

The retained controller calls these readers with its immutable SDK source. They
do not prepare roles, reconstruct absent state, publish history, or repair modes.
An authentic closed SDK execution and absence of a native launch grant are facts
distinct from a row labelled closed or a monitor labelled clean.
"""

from __future__ import annotations

import fcntl
import hashlib
import importlib.util
import json
import os
import re
from pathlib import Path

from temporalio.client import WorkflowHistory

from .contracts import canonical_json, digest
from .delivery_execution_config import observation_mode
from .delivery_lifecycle import RecoveryRefused, evaluate_transition
from .delivery_runtime_location import _transport, bootstrap, private_bytes

MAX_RUNS = 256
MAX_TABLE_ROWS = 8192


class TransitionRefused(RecoveryRefused):
    """A typed missing fact; it never grants an installation or execution effect."""


def refuse(code, fact, *, dependency=None):
    assessment = evaluate_transition(
        {
            "operation": "installation_successor",
            "owner": "runtime_installation_successor",
            "owner_proof": None,
            "comparisons": [],
            "predicates": [
                {
                    "code": code,
                    "satisfied": None,
                    "missing_or_unknown_fact": fact,
                    "next_action": "reconcile the original installation owner before retrying",
                }
            ],
        },
        {},
    )
    if dependency is not None:
        assessment["dependencies"] = [dependency]
    raise TransitionRefused(assessment)


def _rows(db, table, run_id):
    columns = [row[1] for row in db.execute('PRAGMA table_info("' + table + '")')]
    if "run_id" not in columns:
        return None
    rows = [
        dict(row)
        for row in db.execute(
            'SELECT * FROM "' + table + '" WHERE run_id=? LIMIT ?', (run_id, MAX_TABLE_ROWS + 1)
        )
    ]
    if len(rows) > MAX_TABLE_ROWS:
        refuse("installation_evidence_oversized", "bounded original run authority tables")
    return sorted(rows, key=canonical_json)


def _run_tables(db, run_id):
    result = {}
    for (table,) in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        if not re.fullmatch(r"[a-z_][a-z_0-9]*", table):
            refuse("installation_schema_unknown", "known original tracking schema")
        if table in {"delivery_events", "delivery_commands"}:
            continue  # Our own idempotent acknowledgement metadata is separate.
        values = _rows(db, table, run_id)
        if values is not None:
            result[table] = values
    return result


def _native_grants(history):
    """The server's immutable schedules, not a final role/result summary."""
    scheduled, started, grants = {}, set(), []
    for event in history.events:
        if event.HasField("activity_task_scheduled_event_attributes"):
            attributes = event.activity_task_scheduled_event_attributes
            scheduled[event.event_id] = attributes.activity_type.name
        if event.HasField("activity_task_started_event_attributes"):
            started.add(event.activity_task_started_event_attributes.scheduled_event_id)
        for name in ("completed", "failed", "timed_out", "canceled"):
            field = "activity_task_" + name + "_event_attributes"
            if event.HasField(field):
                attributes = getattr(event, field)
                if attributes.started_event_id:
                    started.add(attributes.scheduled_event_id)
        if event.HasField("start_child_workflow_execution_initiated_event_attributes"):
            attributes = event.start_child_workflow_execution_initiated_event_attributes
            grants.append({"event_id": event.event_id, "child": attributes.workflow_id})
    for event_id in sorted(started):
        if event_id not in scheduled:
            raise ValueError("closed SDK activity has no exact server schedule")
        activity = scheduled[event_id]
        # Only this inspected activity performs an in-process projection. A
        # fake role/prepare activity can still derive accepted-plan host checks;
        # its provider label or empty configured checks grants no closure fact.
        if activity != "delivery_project":
            grants.append({"event_id": event_id, "activity": activity})
    return grants


def require_closed_body_custody(history):
    grants = _native_grants(history)
    if grants:
        refuse(
            "host_body_custody_unknown",
            "independent closure of every started host-capable activity or child, "
            "regardless of provider",
            dependency="https://github.com/ebarti/devflow/issues/242",
        )
    return grants


def _resource_observation(spec, leases, *, required):
    from .delivery_filesystem_custody import _opened
    from .delivery_resources import RunResources, same_object

    path = Path(spec["state_dir"]) / "resources/manifest.json"
    if not path.exists() and not path.is_symlink():
        if required:
            refuse("legacy_resource_custody_unknown", "original registered resource ownership")
        return None  # Authentic closed SDK history granted no body to create it.
    registry = RunResources(spec, read_only=True)
    leases.enter_context(_opened(registry.root / "ownership.lock", lock=True))
    raw = private_bytes(path)
    value = json.loads(raw)
    if value.get("run_id") != spec["run_id"] or not same_object(
        spec, registry.state, value["state_identity"]
    ):
        refuse("legacy_resource_custody_unknown", "unchanged original resource identity")
    if value.get("processes"):
        refuse(
            "native_descendant_custody_unknown",
            "independent settlement of registered native descendants",
            dependency="https://github.com/ebarti/devflow/issues/242",
        )
    return {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest()}


def _source_custody(config, supplied, descriptor):
    """The foundation's actual EX, not a stopped row or caller declaration."""
    installed_owner = Path.home().resolve() / ".local/share/devflow"
    root = Path(supplied["root"])
    source = Path(__file__).resolve().parents[3]
    spec = importlib.util.spec_from_file_location(
        "successor_paths", source / "scripts/installation-paths.py"
    )
    paths = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(paths)
    if paths.production_owner(root) != installed_owner:
        refuse("source_custody_unknown", "the original source's canonical installation owner")
    actual, expected = os.fstat(descriptor), (installed_owner / ".production-install.lock").stat()
    if (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino):
        refuse("source_custody_unknown", "the actual inherited lifetime exclusion descriptor")
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    receipt_path = installed_owner / ".production-install.json"
    raw = paths.read_bytes(receipt_path)
    receipt = json.loads(raw)
    if (
        hashlib.sha256(raw).hexdigest() != supplied["receipt_sha256"]
        or receipt.get("phase") != "verified"
        or receipt.get("born_new_owner") is not True
        or receipt.get("managed_owner_version") != 1
        or not paths.same_node(receipt.get("target_identity"), paths.node(root))
    ):
        refuse("source_custody_unknown", "unchanged foundation-born source and receipt")
    paths.layout_helper().installed(root)
    inventory = paths.read_inventory(root, receipt)
    record = {
        "config_sha256": hashlib.sha256(paths.read_bytes(config.path)).hexdigest(),
        "state_root": str(config.state_root),
        "tracking_db": str(config.tracking_db),
    }
    if record not in inventory["configurations"].get(str(config.path), []):
        refuse(
            "source_custody_unknown", "the complete source owner's exact configuration enrollment"
        )
    return {
        "root": str(root),
        "root_identity": paths.node(root),
        "receipt_sha256": supplied["receipt_sha256"],
    }


def capture_owner(config, owner, *, configured_paths, leases, source_owner, descriptor):
    """Read complete original membership and retained SDK closure under caller custody."""
    from .delivery_closures import _private_root, _read_sealed
    from .delivery_runtime_installation import _closure, _directory
    from .delivery_store import DeliveryStore

    source_custody = _source_custody(config, source_owner, descriptor)
    for folder in (
        config.state_root,
        config.state_root / "closures",
        config.state_root / "configuration-snapshots",
    ):
        if _directory(folder)["mode"] != 0o700:
            refuse("legacy_custody_unknown", "already-private original evidence directories")
    with (
        bootstrap(
            config, {"helpers_dir": str(config.helpers_dir), "transport": _transport(config)}
        ),
        observation_mode(),
    ):
        store = DeliveryStore(config, read_only=True)
        with store._connect() as db:
            all_rows = [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM delivery_runs ORDER BY run_id LIMIT ?", (MAX_RUNS + 1,)
                )
            ]
            if len(all_rows) > MAX_RUNS:
                refuse("installation_evidence_oversized", "bounded explicit original cohort")
            selected = []
            for row in all_rows:
                original = json.loads(row["request_json"])
                if original["config_path"] not in configured_paths:
                    refuse(
                        "legacy_owner_namespace_unknown",
                        "explicit enrollment of every original owner sharing this database",
                    )
                if original["config_path"] == str(config.path):
                    selected.append(row)
            if sorted(owner["runs"]) != [row["run_id"] for row in selected]:
                refuse("installation_cohort_changed", "exact complete original owner membership")
        entries = []
        for row in selected:
            original = json.loads(row["request_json"])
            effective = store.effective_spec(row["run_id"])
            if effective.get("feature_delivery") or effective.get("feature_worker"):
                refuse(
                    "feature_custody_unknown",
                    "independent closed feature aggregate custody",
                    dependency="https://github.com/ebarti/devflow/issues/245",
                )
            if not store.owns_execution(effective):
                refuse("legacy_transport_unknown", "unchanged original SDK execution transport")
            closed, closure_record = _closure(store, row)
            history = WorkflowHistory.from_json(
                closed["workflow_id"],
                _read_sealed(
                    _private_root(store, read_only=True), closure_record["history_sha256"]
                ).decode("utf-8"),
            )
            native_grants = require_closed_body_custody(history)
            if original.get("provider") != "fake" and effective.get("preparation"):
                refuse(
                    "native_descendant_custody_unknown",
                    "independent settlement of original native preparation actors",
                    dependency="https://github.com/ebarti/devflow/issues/242",
                )
            with store._connect() as db:
                tables = _run_tables(db, row["run_id"])
                commands = [
                    dict(value)
                    for value in db.execute(
                        "SELECT * FROM delivery_commands WHERE run_id=? ORDER BY command_id",
                        (row["run_id"],),
                    )
                ]
                claim = store.state.claim_for(db, original["work_id"])
                from .delivery_policy_recovery import work_binding

                work = work_binding(store, effective, db)
            if any(values for table, values in tables.items() if "diagnostic" in table):
                refuse(
                    "native_descendant_custody_unknown",
                    "independent custody of the separately granted diagnostic actor",
                    dependency="https://github.com/ebarti/devflow/issues/242",
                )
            resource = _resource_observation(effective, leases, required=bool(native_grants))
            entries.append(
                {
                    "run_id": row["run_id"],
                    "request_digest": row["request_digest"],
                    "request_json_sha256": hashlib.sha256(row["request_json"].encode()).hexdigest(),
                    "original_spec": original,
                    "effective_spec": effective,
                    "original_policy_digest": original["policy_digest"],
                    "original_config_digest": original["config_digest"],
                    "tables": tables,
                    "tables_sha256": digest(tables),
                    "commands": commands,
                    "commands_sha256": digest(commands),
                    "closed": closed,
                    "closure_record": closure_record,
                    "resource_observation": resource,
                    "claim": dict(claim) if claim else None,
                    "work_binding": work,
                }
            )
        return {
            "owner_path": str(config.path),
            "transport": _transport(config),
            "source_custody": source_custody,
            "runs": entries,
            "cohort_sha256": digest(entries),
        }
