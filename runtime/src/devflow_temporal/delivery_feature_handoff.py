"""Read-only observations of feature-owner settlement at the registry boundary."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
import subprocess
from contextlib import contextmanager
from copy import deepcopy
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

    @staticmethod
    def _pending_revision(db, issue_id):
        from .delivery_execution_registry import ExecutionRegistry

        return ExecutionRegistry._pending_revision(db, issue_id)

    @staticmethod
    def _preparation_successor(db, pending, predecessor, run_id):
        from .delivery_execution_registry import ExecutionRegistry

        return ExecutionRegistry._preparation_successor(db, pending, predecessor, run_id)

    def budget(self, issue_id):
        return self._budget(self.db, issue_id)

    @staticmethod
    def _budget(db, issue_id):
        from .delivery_execution_registry import ExecutionRegistry

        return ExecutionRegistry._budget(db, issue_id)

    @staticmethod
    def _repair_allowances(db, issue_id):
        from .delivery_execution_registry import ExecutionRegistry

        return ExecutionRegistry._repair_allowances(db, issue_id)

    @staticmethod
    def validate_checkpoint(db, token, key, value):
        from .delivery_execution_registry import ExecutionRegistry

        return ExecutionRegistry.validate_checkpoint(db, token, key, value)

    def plan_repair_allowance(self, db, token, baseline, current):
        from .delivery_execution_registry import ExecutionRegistry

        return ExecutionRegistry.plan_repair_allowance(self, db, token, baseline, current)

    def validate_claim(self, snapshot, run_id, store_path, *, maximum_repairs, predecessor):
        from .delivery_execution_registry import ExecutionRegistry

        return ExecutionRegistry.validate_claim(self, self.db, snapshot, run_id, store_path,
                                                maximum_repairs=maximum_repairs,
                                                predecessor=predecessor)


def feature_configuration_refusal(message):
    """Represent the known configuration-admission stage without parsing its message."""
    from .delivery_lifecycle import RecoveryRefused

    assessment = evaluate_transition({
        "operation": "continue_feature", "owner": "feature_continuation",
        "owner_proof": None, "comparisons": [],
        "predicates": [{"code": "feature_submission_config_unknown", "satisfied": False,
                        "missing_or_unknown_fact": "successor source and configuration admission",
                        "next_action": "inspect successor configuration and source"}],
    }, {})
    error = RecoveryRefused(assessment)
    error.args = (message,)
    return error


def feature_projection_facts(row, operation):
    """Shared observed revision and stopped-checkpoint predicates."""
    return {
        "revision": row["revision"] if row else None,
        "comparisons": [("revision", "expected_revision", "revision_changed")],
        "predicates": [{
            "code": "feature_checkpoint_unavailable",
            "satisfied": bool(row and row["outcome"] in {"blocked", "cancelled"}
                              and (operation != "revise_feature_plan"
                                   or row["cleanup"] == "confirmed")),
            "missing_or_unknown_fact": "stopped feature projection and cleanup",
            "next_action": "inspect the stopped feature checkpoint",
        }],
    }


@contextmanager
def readonly_registry(spec):
    """Open a private, stable immutable feature registry without creating any file."""
    path = Path(spec["feature_delivery"]["registry"])
    if not path.is_absolute() or path.resolve() != path:
        raise ValueError("feature registry path is not canonical")
    parent = path.parent.lstat()
    if (not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid()
            or stat.S_IMODE(parent.st_mode) != 0o700):
        raise ValueError("feature registry directory is not private")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1
                or any(Path(str(path) + suffix).exists() for suffix in ("-wal", "-journal"))):
            raise ValueError("feature registry cannot be observed immutably")
        db = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
        db.row_factory = sqlite3.Row
        try:
            yield _RegistryView(db)
        finally:
            db.close()
        after = path.stat(follow_symlinks=False)
        if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
                or any(Path(str(path) + suffix).exists() for suffix in ("-wal", "-journal"))):
            raise ValueError("feature registry changed during observation")
    finally:
        os.close(fd)


def validate_retained_configuration(store, retained):
    """Require the predecessor's frozen authority while allowing capacity changes."""
    from .delivery_execution_config import is_observation_mode

    try:
        _validate_retained_configuration(store, retained)
    except ValueError as exc:
        if is_observation_mode():
            raise
        raise feature_configuration_refusal(str(exc)) from exc


def _validate_retained_configuration(store, retained):
    from .delivery_execution_config import is_observation_mode, load_execution_config

    frozen = load_execution_config(retained)
    scope = retained.get("feature_scope_amendment")
    if scope:
        if is_observation_mode():
            raise ValueError("feature scope lineage is unavailable to read-only observation")
        from .delivery_feature_scope import authenticate_receipt

        amended = authenticate_receipt(store, scope)
        if amended.path != frozen.path or digest(amended.raw) != digest(frozen.raw):
            raise ValueError("feature continuation changed its retained scope authority")
        frozen = load_execution_config(store.effective_spec(scope["predecessor"]["run_id"]))
    current_authority = {key: value for key, value in store.config.raw.items() if key != "capacity"}
    frozen_authority = {key: value for key, value in frozen.raw.items() if key != "capacity"}
    if (store.config.path != frozen.path
            or digest(current_authority) != digest(frozen_authority)):
        raise ValueError("feature continuation changed its original service configuration")


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


def observe_handoff(store, run_id, request, *, operation, revision_admission=None):
    from .delivery_execution_config import observation_mode

    with observation_mode():
        return _observe_handoff(store, run_id, request, operation=operation,
                                revision_admission=revision_admission)


def _observe_handoff(store, run_id, request, *, operation, revision_admission=None):
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
                    config_proven = None
                    access_proven = None
                    claim_proven = None
                    registry_proven = None
                    allowance_proven = None
                    command_proven = None
                    snapshot_proven = None
                    revision_proven = None
                    submission_seal = None
                    try:
                        from .delivery_config import COMMAND_RE, DeliveryConfig
                        from .delivery_feature_revisions import _validate_continued_plan

                        if (isinstance(request, dict)
                                and set(request) == {"command_id", "expected_revision"}
                                and isinstance(request["command_id"], str)
                                and COMMAND_RE.fullmatch(request["command_id"])
                                and type(request["expected_revision"]) is int
                                and request["expected_revision"] == row["revision"]
                                and facts["normalized_lifecycle"] == "stopped"):
                            original = readonly_feature_spec(store, run_id)
                            successor = "run-feature-" + digest(
                                {"run": run_id, "command": request})[:20]
                            fields = {"work_id", "issue_url", "repository_key", "goal", "base_ref",
                                      "authorized_endpoint", "origin_thread_id",
                                      "publication_summary", "plan_approval"}
                            supplied = {key: value for key, value in spec.items() if key in fields}
                            supplied.update(command_id=request["command_id"], run_id=successor,
                                            branch="feat/df-coordinator-" + digest(successor)[:16],
                                            feature_predecessor=token)
                            if original.get("accepted_plan"):
                                supplied.update(accepted_plan=original["accepted_plan"],
                                                plan_approval="automatic")
                            _validate_continued_plan(store, deepcopy(supplied), original,
                                                     _RegistryView(db))
                            plan_proven = True
                            live = DeliveryConfig.load(store.config.path)
                            if digest(live.raw) != digest(store.config.raw):
                                raise ValueError("current service configuration changed")
                            from .delivery_execution_config import verify_existing_execution_config

                            verify_existing_execution_config(store.config)
                            snapshot_proven = True
                            validate_retained_configuration(store, original)
                            admitted = store.config.admit(supplied, _feature_base=original)
                            _validate_continued_plan(store, admitted, original, _RegistryView(db))
                            if revision_admission is not None:
                                from .delivery_feature_revisions import (
                                    PREFIX,
                                    _validate_revision_submission_binding,
                                )

                                key = PREFIX + "request:" + revision_admission["revision_id"]
                                saved_row = db.execute(
                                    "SELECT content_json,content_digest FROM execution_checkpoints "
                                    "WHERE issue_id=? AND checkpoint_key=?",
                                    (token["issue_id"], key)).fetchone()
                                saved = json.loads(saved_row["content_json"]) if saved_row else None
                                if (saved_row is None
                                        or saved_row["content_digest"] != digest(saved)):
                                    raise ValueError("revision reservation proof is unavailable")
                                _validate_revision_submission_binding(
                                    admitted, revision_admission, original, saved)
                                admitted["plan_approval"] = original.get(
                                    "plan_approval", "required")
                                admitted["feature_plan_revision_request"] = deepcopy(
                                    revision_admission)
                                admitted["feature_plan_revision"] = {
                                    key: revision_admission["old_identity"][key]
                                    for key in ("plan_revision", "plan_digest")}
                            config_proven = True
                            from .delivery_feature_execution import _require_no_legacy_custody
                            from .delivery_settings import require_repository_access

                            with store._connect() as tracking:
                                command_row = tracking.execute(
                                    "SELECT 1 FROM delivery_commands WHERE command_id=?",
                                    (request["command_id"],)).fetchone()
                                successor_row = tracking.execute(
                                    "SELECT 1 FROM delivery_runs WHERE run_id=?", (successor,)
                                ).fetchone()
                                if command_row or successor_row:
                                    raise ValueError("successor or command already exists")
                                command_proven = True
                                require_repository_access(store, tracking, admitted["github_repo"])
                                access_proven = True
                                work = store.state.row(tracking, "works", admitted["work_id"])
                                if (not work or not work["issue"]
                                        or store.state.issue_resource(work["issue"])
                                        != store.state.issue_resource(admitted["issue_url"])):
                                    raise ValueError("successor work binding is unavailable")
                                _require_no_legacy_custody(tracking, admitted)
                                store.state.validate_claim_work(
                                    tracking, admitted["work_id"], "external:devflow:" + successor)
                                admitted["request_digest"] = digest({
                                    key: value for key, value in supplied.items()
                                    if key != "command_id"})
                                if admitted.get("delivery_limits_version") == 1:
                                    from .delivery_store import _now

                                    admitted["delivery_admitted_at"] = (
                                        original.get("delivery_admitted_at") or _now())
                                from .delivery_control_lineage import inherit_delivery_control

                                inherit_delivery_control(store, tracking, admitted, row)
                                claim_proven = True
                            from .delivery_feature_execution import (
                                GitHubDelivery,
                                _prepare_snapshot_plan,
                                _require_migrated_store,
                                _validate_preparation_successor,
                            )
                            from .delivery_github_contract import bounded_github_observation

                            view = _RegistryView(db)
                            from .delivery_feature_revisions import revision_request

                            if (revision_admission is None
                                    and revision_request(original, shared=view) is not None):
                                raise ValueError(
                                    "feature revision preparation requires its own proof")
                            revision_proven = True
                            source = str(store.config.tracking_db)
                            _require_migrated_store(view, admitted, source)
                            snapshot = view.snapshot(successor, source)
                            if snapshot is None:
                                with bounded_github_observation(admitted["github_repo"]):
                                    snapshot = GitHubDelivery().snapshot(
                                        admitted["issue_url"], admitted["github_repo"])
                            candidate = deepcopy(admitted)
                            predecessor = candidate.pop("feature_predecessor", None)
                            _validate_preparation_successor(view, candidate, snapshot, predecessor)
                            _prepare_snapshot_plan(candidate, snapshot)
                            view.validate_claim(snapshot, successor, source,
                                                maximum_repairs=candidate["policy"]["max_repairs"],
                                                predecessor=predecessor)
                            registry_proven = True
                            if original.get("provider") == "fake":
                                allowance = {"mode": "not_required"}
                            else:
                                from .delivery_feature_execution import _observe_repair_allowance

                                allowance = _observe_repair_allowance(store, view, token, original)
                            allowance_proven = True
                            submission_seal = digest({"admitted": admitted, "snapshot": snapshot,
                                                      "allowance": allowance, "predecessor": token,
                                                      "revision": row["revision"]})
                    except (AttributeError, OSError, RuntimeError, ValueError, KeyError, TypeError,
                            sqlite3.Error, subprocess.SubprocessError):
                        pass
                    facts["predicates"].extend([
                        {"code": "feature_command_proof_unknown", "satisfied": command_proven,
                         "missing_or_unknown_fact": "unused successor and command identity",
                         "next_action": "inspect successor command custody"},
                        {"code": "feature_config_snapshot_unknown", "satisfied": snapshot_proven,
                         "missing_or_unknown_fact": "existing private configuration snapshot",
                         "next_action": "inspect the admitted configuration snapshot"},
                        {"code": "feature_revision_preparation_unknown",
                         "satisfied": revision_proven,
                         "missing_or_unknown_fact": "absence of a pending revision preparation",
                         "next_action": "inspect feature revision custody"},
                        {"code": "feature_plan_authority_unknown", "satisfied": plan_proven,
                         "missing_or_unknown_fact": "accepted plan and predecessor lineage",
                         "next_action": "inspect accepted feature plan custody"},
                        {"code": "feature_submission_config_unknown", "satisfied": config_proven,
                         "missing_or_unknown_fact": "successor source and configuration admission",
                         "next_action": "inspect successor configuration and source"},
                        {"code": "feature_repository_access_unknown", "satisfied": access_proven,
                         "missing_or_unknown_fact": "repository admission permission",
                         "next_action": "inspect repository access"},
                        {"code": "feature_work_claim_unknown", "satisfied": claim_proven,
                         "missing_or_unknown_fact": "successor work claim and control lineage",
                         "next_action": "inspect work claim and predecessor control"},
                        {"code": "feature_registry_claim_unknown", "satisfied": registry_proven,
                         "missing_or_unknown_fact": "GitHub snapshot and successor registry claim",
                         "next_action": "inspect GitHub snapshot and feature registry"},
                        {"code": "feature_allowance_unknown", "satisfied": allowance_proven,
                         "missing_or_unknown_fact": "original or compatible repair allowance",
                         "next_action": "inspect feature repair allowance"},
                    ])
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
        projection = feature_projection_facts(row, operation)
        facts.update(owner=owner, revision=projection["revision"],
                     comparisons=projection["comparisons"])
        facts["predicates"].extend(projection["predicates"])
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
        if operation == "continue_feature" and submission_seal is not None:
            tracking_proven = None
            try:
                from .delivery_config import DeliveryConfig
                from .delivery_execution_config import verify_existing_execution_config
                from .delivery_settings import require_repository_access

                live = DeliveryConfig.load(store.config.path)
                if digest(live.raw) != digest(store.config.raw):
                    raise ValueError("service configuration changed during observation")
                verify_existing_execution_config(store.config)
                with store._connect() as tracking:
                    current = tracking.execute(
                        "SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
                    if (current is None or dict(current) != dict(row)
                            or tracking.execute(
                                "SELECT 1 FROM delivery_commands WHERE command_id=?",
                                (request["command_id"],)).fetchone()
                            or tracking.execute(
                                "SELECT 1 FROM delivery_runs WHERE run_id=?",
                                (successor,)).fetchone()):
                        raise ValueError("feature successor changed during observation")
                    require_repository_access(store, tracking, admitted["github_repo"])
                    store.state.validate_claim_work(
                        tracking, admitted["work_id"], "external:devflow:" + successor)
                if readonly_feature_spec(store, run_id) != original:
                    raise ValueError("feature authority changed during observation")
                tracking_proven = True
            except (AttributeError, OSError, RuntimeError, ValueError, KeyError, TypeError,
                    sqlite3.Error):
                submission_seal = None
            facts["predicates"].append({
                "code": "feature_tracking_changed", "satisfied": tracking_proven,
                "missing_or_unknown_fact": "fresh predecessor, access and work claim",
                "next_action": "refresh the feature checkpoint",
            })
        if operation == "continue_feature" and submission_seal is not None:
            facts["proof_digest"] = digest({"owner": facts["proof_digest"],
                                            "successor_admission": submission_seal})
        else:
            facts["predicates"].append(
                {
                    "code": "feature_successor_admission_proof_unknown",
                    "satisfied": None,
                    "missing_or_unknown_fact": "complete successor admission proof",
                    "next_action": "inspect feature successor admission",
                }
            )
        return evaluate_transition(facts, request)
    except (OSError, ValueError, TypeError, KeyError, sqlite3.Error):
        return evaluate_transition(unknown, request)
