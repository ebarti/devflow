"""Read-only observations of feature-owner settlement at the registry boundary."""

from __future__ import annotations

import os
import sqlite3
import stat
from pathlib import Path

from .contracts import digest
from .delivery_lifecycle import evaluate_transition


def handoff_facts(db, token, *, operation="continue_feature"):
    """Derive the same stopped-owner predicates while the registry is held by its owner."""
    token = token if isinstance(token, dict) else {}
    issue_id = token.get("issue_id")
    row = db.execute("SELECT * FROM execution_claims WHERE issue_id=?", (issue_id,)).fetchone()
    claim = dict(row) if row else None
    keys = ("issue_id", "run_id", "store_path", "generation")
    exact = bool(
        claim and all(claim.get(k) == token.get(k) and token.get(k) is not None for k in keys)
    )
    workers = [
        dict(item)
        for item in db.execute(
            "SELECT worker_key,generation,state FROM execution_workers "
            "WHERE issue_id=? AND state!='finished'",
            (issue_id,),
        )
    ]
    effects = [
        dict(item)
        for item in db.execute(
            "SELECT effect_key,generation,state FROM execution_effects "
            "WHERE issue_id=? AND (state NOT IN ('complete','absent') OR result_json IS NULL)",
            (issue_id,),
        )
    ]
    return {
        "operation": operation,
        "owner": "feature_registry",
        "owner_proof": exact,
        "proof_digest": digest({"claim": claim, "workers": workers, "effects": effects}),
        "normalized_lifecycle": "stopped" if exact and claim["state"] == "stopped" else "unknown",
        "comparisons": [],
        "predicates": [
            {
                "code": "feature_owner_changed",
                "satisfied": exact,
                "missing_or_unknown_fact": "exact feature owner and generation",
                "next_action": "inspect current feature ownership",
            },
            {
                "code": "feature_owner_not_stopped",
                "satisfied": bool(exact and claim["state"] == "stopped"),
                "missing_or_unknown_fact": "stopped feature owner",
                "next_action": "reconcile feature settlement",
            },
            {
                "code": "feature_workers_unsettled",
                "satisfied": not workers,
                "missing_or_unknown_fact": "finished feature worker custody",
                "next_action": "reconcile feature workers",
            },
            {
                "code": "feature_effects_unsettled",
                "satisfied": not effects,
                "missing_or_unknown_fact": "settled feature external effects",
                "next_action": "reconcile feature effects",
            },
        ],
    }


def observe_handoff(store, run_id, request, *, operation):
    """Observe immutable owner and registry facts without taking over or settling them.

    A writable registry or a rollback/WAL journal cannot be treated as an immutable
    snapshot. Report unknown rather than starting recovery or creating SQLite files.
    """
    owner = "feature_revision" if operation == "revise_feature_plan" else "feature_continuation"
    unknown = {
        "operation": operation,
        "owner": owner,
        "owner_proof": None,
        "comparisons": [],
        "predicates": [
            {
                "code": "feature_registry_proof_unknown",
                "satisfied": None,
                "missing_or_unknown_fact": "readable stable feature registry",
                "next_action": "inspect feature registry custody",
            }
        ],
    }
    try:
        with store._connect() as tracking:
            row = tracking.execute(
                "SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)
            ).fetchone()
        if row is None:
            return evaluate_transition(unknown, request)
        import json

        spec = json.loads(row["request_json"])
        feature = spec.get("feature_delivery")
        if not isinstance(feature, dict) or spec.get("feature_worker"):
            return evaluate_transition(unknown, request)
        token = feature["owner"]
        if token.get("run_id") != run_id or token.get("store_path") != str(
            store.config.tracking_db
        ):
            return evaluate_transition(unknown, request)
        path = Path(feature["registry"])
        if not path.is_absolute() or path.resolve() != path:
            return evaluate_transition(unknown, request)
        parent = path.parent.lstat()
        if (not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid()
                or stat.S_IMODE(parent.st_mode) != 0o700):
            return evaluate_transition(unknown, request)
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_nlink != 1
                or any(Path(str(path) + suffix).exists() for suffix in ("-wal", "-journal"))
            ):
                return evaluate_transition(unknown, request)
            db = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
            db.row_factory = sqlite3.Row
            try:
                facts = handoff_facts(db, token, operation=operation)
                # Budget uses the same authenticated allowance checkpoints as admission.
                if operation == "revise_feature_plan":
                    from .delivery_execution_registry import ExecutionRegistry

                    budget = ExecutionRegistry._budget(db, token["issue_id"])
                    facts["predicates"].append(
                        {
                            "code": "feature_product_budget_exhausted",
                            "satisfied": budget["used"] < budget["maximum"],
                            "missing_or_unknown_fact": "remaining product repair allowance",
                            "next_action": "inspect the feature repair allowance",
                            "new_authorization_required": False,
                        }
                    )
            finally:
                db.close()
            after = path.stat(follow_symlinks=False)
            if ((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
                    != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
                    or any(Path(str(path) + suffix).exists()
                           for suffix in ("-wal", "-journal"))):
                return evaluate_transition(unknown, request)
        finally:
            os.close(fd)
        facts.update(
            owner=owner,
            revision=row["revision"],
            comparisons=[("revision", "expected_revision", "revision_changed")],
        )
        facts["predicates"].append(
            {
                "code": "feature_checkpoint_unavailable",
                "satisfied": (
                    row["outcome"] in {"blocked", "cancelled"}
                    and (operation != "revise_feature_plan" or row["cleanup"] == "confirmed")
                ),
                "missing_or_unknown_fact": "stopped feature projection and cleanup",
                "next_action": "inspect the stopped feature checkpoint",
            }
        )
        facts["predicates"].append(
            {
                "code": "feature_successor_admission_proof_unknown",
                "satisfied": None,
                "missing_or_unknown_fact": "closed coordinator and successor admission proof",
                "next_action": "inspect feature successor admission",
            }
        )
        return evaluate_transition(facts, request)
    except (OSError, ValueError, TypeError, KeyError, sqlite3.Error):
        return evaluate_transition(unknown, request)
