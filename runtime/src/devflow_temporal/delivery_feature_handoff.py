"""Read-only observations of feature-owner settlement at the registry boundary."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
from contextlib import contextmanager
from pathlib import Path

from .contracts import digest
from .delivery_lifecycle import evaluate_transition


class _RegistryView:
    """The existing feature validators over one already-open immutable registry."""

    def __init__(self, db):
        self.db = db

    @contextmanager
    def connect(self):
        yield self.db

    def current(self, issue_id):
        row = self.db.execute(
            "SELECT * FROM execution_claims WHERE issue_id=?", (issue_id,)).fetchone()
        return dict(row) if row else None

    def checkpoints(self, issue_id):
        return {item["checkpoint_key"]: json.loads(item["content_json"])
                for item in self.db.execute(
                    "SELECT checkpoint_key,content_json FROM execution_checkpoints "
                    "WHERE issue_id=? ORDER BY rowid", (issue_id,))}

    def snapshot(self, run_id, store_path):
        row = self.db.execute(
            "SELECT input_json FROM execution_snapshots WHERE run_id=? AND store_path=?",
            (run_id, store_path),
        ).fetchone()
        return json.loads(row[0]) if row else None

    @staticmethod
    def require(db, token, *, active=True):
        from .delivery_execution_registry import ExecutionRegistry

        return ExecutionRegistry.require(db, token, active=active)

    @staticmethod
    def _settled(db, issue_id):
        from .delivery_execution_registry import ExecutionRegistry

        return ExecutionRegistry._settled(db, issue_id)


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



def readonly_feature_spec(store, run_id):
    """Authenticate the unamended feature overlay through an immutable registry view."""
    import json

    with store._connect() as tracking:
        row = tracking.execute(
            "SELECT request_json,recovery_json,accepted_plan_text "
            "FROM delivery_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if row is None or row["recovery_json"] is not None:
            raise ValueError("feature recovery overlay is not available to read-only observation")
        spec = store._prepared_original(tracking, json.loads(row["request_json"]))
    if row["accepted_plan_text"] is not None:
        spec["accepted_plan"] = row["accepted_plan_text"]
    feature = spec.get("feature_delivery")
    if not isinstance(feature, dict) or spec.get("feature_worker"):
        raise ValueError("feature coordinator authority is unavailable")
    token = feature.get("owner") or {}
    if token.get("run_id") != run_id or token.get("store_path") != str(store.config.tracking_db):
        raise ValueError("feature coordinator owner changed")
    path = Path(feature["registry"])
    if not path.is_absolute() or path.resolve() != path:
        raise ValueError("feature registry path is not canonical")
    parent = path.parent.lstat()
    if (not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid()
            or stat.S_IMODE(parent.st_mode) != 0o700):
        raise ValueError("feature registry directory is not private")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
                or any(Path(str(path) + suffix).exists() for suffix in ("-wal", "-journal"))):
            raise ValueError("feature registry cannot be observed immutably")
        db = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
        db.row_factory = sqlite3.Row
        try:
            from .delivery_feature_revisions import effective_spec

            effective = effective_spec(store, spec, shared=_RegistryView(db))
        finally:
            db.close()
        after = path.stat(follow_symlinks=False)
        if ((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
                or any(Path(str(path) + suffix).exists() for suffix in ("-wal", "-journal"))):
            raise ValueError("feature registry changed during observation")
        return effective
    finally:
        os.close(fd)


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
                if operation == "continue_feature":
                    plan_proven = None
                    try:
                        from .delivery_config import COMMAND_RE
                        from .delivery_feature_revisions import _validate_continued_plan

                        if (isinstance(request, dict)
                                and set(request) == {"command_id", "expected_revision"}
                                and isinstance(request["command_id"], str)
                                and COMMAND_RE.fullmatch(request["command_id"])
                                and type(request["expected_revision"]) is int
                                and facts["normalized_lifecycle"] == "stopped"):
                            original = readonly_feature_spec(store, run_id)
                            successor = "run-feature-" + digest(
                                {"run": run_id, "command": request})[:20]
                            candidate = {"run_id": successor, "feature_predecessor": token}
                            if original.get("accepted_plan"):
                                candidate["accepted_plan"] = original["accepted_plan"]
                            _validate_continued_plan(store, candidate, original, _RegistryView(db))
                            plan_proven = True
                    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error):
                        pass
                    facts["predicates"].append({
                        "code": "feature_plan_authority_unknown", "satisfied": plan_proven,
                        "missing_or_unknown_fact": "accepted plan and predecessor lineage",
                        "next_action": "inspect accepted feature plan custody",
                    })
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
        if facts["owner_proof"] and facts["normalized_lifecycle"] == "stopped":
            try:
                from .delivery_feature_closure import closed_coordinator

                closed = closed_coordinator(store, run_id, preserve=False)
            except Exception:
                closed = None
            if closed is not None:
                facts["proof_digest"] = digest({"registry": facts["proof_digest"],
                                               "coordinator_closure": closed})
            facts["predicates"].append({
                "code": "feature_coordinator_closure_unknown",
                "satisfied": closed is not None,
                "missing_or_unknown_fact": "authenticated closed coordinator execution",
                "next_action": "inspect coordinator closure",
            })
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
