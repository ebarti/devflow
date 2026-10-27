"""New generated-child allocations authorized by an acknowledged source restore.

Restoration preserves original ownership evidence. A missing generated child can
begin its next generation only when that exact previous entry belonged to the
checkpoint's original checkout and the replacement checkout has a completed
restoration acknowledgement. Existing or unproven output is never adopted.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path

from .contracts import canonical_json, digest


def missing_generated_reallocation(spec, path, manifest, entry):
    """Return custody proof for this missing archived child, otherwise no grant.

    The caller holds the resource registry's exclusive allocation lock. This
    function only reads durable acknowledgements and checkpoint bytes; the caller
    retains the normal absent-resource generation transition and creation guard.
    """
    checkout, path = Path(spec.get("checkout", "")), Path(path)
    if (
        spec.get("workspace_custody_version") != 1
        or entry.get("kind") != "generated"
        or not entry.get("identity")
        or os.path.lexists(path)
        or path == checkout
        or not path.is_relative_to(checkout)
        or entry.get("receipt", {}).get("state") in {"removed", "already_absent"}
    ):
        return None
    from .delivery_checkpoints import RESTORED, _read_acknowledged, _root
    from .delivery_execution_config import load_execution_config
    from .delivery_resources import _identity, read_private

    current = _identity(checkout)
    config = load_execution_config(spec)
    with closing(sqlite3.connect(config.tracking_db.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute(
            "SELECT command_id,request_digest,response_json FROM delivery_commands WHERE run_id=? "
            "AND command_id LIKE 'checkpoint-restore:%' ORDER BY rowid DESC",
            (spec["run_id"],),
        )
        for row in rows:
            receipt = json.loads(row["response_json"])
            if receipt.get("restored_identity") != current:
                continue
            request = {
                "kind": RESTORED,
                "run_id": spec["run_id"],
                "checkpoint_id": receipt["checkpoint_id"],
                "command_id": row["command_id"].removeprefix("checkpoint-restore:"),
                "expected_revision": receipt["revision"],
            }
            mutation = db.execute(
                "SELECT run_id,kind,request_digest,state,response_json FROM delivery_mutations "
                "WHERE command_id=?", (row["command_id"],),
            ).fetchone()
            if (
                row["request_digest"] != digest(request)
                or mutation is None
                or mutation["run_id"] != spec["run_id"]
                or mutation["kind"] != RESTORED
                or mutation["request_digest"] != digest(request)
                or mutation["state"] != "complete"
                or json.loads(mutation["response_json"]) != receipt
                or db.execute(
                    "SELECT 1 FROM delivery_events WHERE run_id=? AND type=? AND payload_json=?",
                    (spec["run_id"], RESTORED, canonical_json(receipt)),
                ).fetchone() is None
            ):
                raise ValueError("checkpoint child allocation lost its restore acknowledgement")
            value, ack = _read_acknowledged(db, spec, receipt["checkpoint_id"])
            if (
                value["spec_sha256"] != digest(spec)
                or receipt.get("manifest_sha256") != ack["manifest_sha256"]
                or receipt.get("resource_manifest_sha256") != value["resource_manifest_sha256"]
                or receipt.get("original_identity") != value["checkout_identity"]
            ):
                raise ValueError("checkpoint child allocation changed its restore binding")
            reference = value["state_refs"].get("resources/manifest.json")
            if reference is None:
                raise ValueError("checkpoint child allocation has no original resource evidence")
            raw = read_private(
                _root(spec) / receipt["checkpoint_id"] / reference["artifact"], raw=True
            )
            if (
                hashlib.sha256(raw).hexdigest() != reference["sha256"]
                or reference["sha256"] != value["resource_manifest_sha256"]
            ):
                raise ValueError("checkpoint child allocation resource evidence changed")
            archived = json.loads(raw)
            old_root = archived.get("roots", {}).get(str(checkout))
            old_child = archived.get("roots", {}).get(str(path))
            root = manifest.get("roots", {}).get(str(checkout))
            if (
                archived.get("run_id") != spec["run_id"]
                or archived.get("state_identity") != manifest.get("state_identity")
                or not old_root
                or old_root.get("kind") != "checkout"
                or old_root.get("identity") != receipt["original_identity"]
                or root != old_root
                or old_child != entry
                or old_child.get("kind") != "generated"
                or "generated_output_preservation" in old_child
            ):
                raise ValueError(
                    "checkpoint child allocation changed its original resource generation"
                )
            if os.path.lexists(path) or _identity(checkout) != current:
                raise ValueError("checkpoint child allocation changed during observation")
            return {
                "schema": "devflow-checkpoint-child-allocation-v1",
                "restore_command_id": row["command_id"],
                "checkpoint_id": receipt["checkpoint_id"],
                "manifest_sha256": ack["manifest_sha256"],
                "resource_manifest_sha256": value["resource_manifest_sha256"],
                "checkout_identity": current,
                "previous_identity": old_child["identity"],
                "previous_generation": old_child.get("generation", 0),
                "previous_entry_sha256": digest(old_child),
            }
    return None
