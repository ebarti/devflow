"""One acknowledged frozen-check diagnostic, without delivery advancement."""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import os
import re
import sqlite3
import stat
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_metadata_recovery import _immutable
from .delivery_resources import _ancestors, _identity, private_directory

KIND = "native_check_diagnostic"
CHECK_ID = "worker-regressions"
CHECK_SHA256 = "a95f797b4cf53e38d0b04b84ad1fc7bf38be8e581cda05718b5ceee0cb12a8d1"
PROVENANCE_SHA256 = "70e40d7284f61cfc4f3054ed9005028ec08394a6abb3ad30e10e2c59e991c0e1"
TEMPORAL = "temporal_test_prerequisites"
ACK = "diagnostic_only_no_delivery_acceptance"
FIELDS = {
    "command_id",
    "expected_revision",
    "expected_projection_revision",
    "expected_iteration",
    "expected_candidate_id",
    "expected_candidate_head",
    "expected_content_sha256",
    "expected_pr_number",
    "expected_pr_head",
    "check_id",
    "expected_check_sha256",
}
ERROR = "native diagnostic holds the stopped execution"
ACTIVE = "('queued','running','unknown')"


def evidence_value(path, *, raw=False):
    """Existing private FD/name/size custody, with no installation bootstrap authority."""
    from .delivery_runtime_location import private_bytes

    data = private_bytes(path)
    return data if raw else json.loads(data)


read_private = evidence_value


def initialize(db):
    """Runtime-owned metadata and a row-scoped fence; shared helper schema is unchanged."""
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_native_diagnostics (
        identity TEXT PRIMARY KEY, command_id TEXT UNIQUE NOT NULL,
        run_id TEXT NOT NULL REFERENCES delivery_runs(run_id), work_id TEXT NOT NULL,
        issue_resource TEXT NOT NULL, request_digest TEXT NOT NULL,
        snapshot_json TEXT NOT NULL, intent_sha256 TEXT NOT NULL, state TEXT NOT NULL,
        response_json TEXT NOT NULL, created_at TEXT NOT NULL)""")
    db.execute(
        "CREATE INDEX IF NOT EXISTS diagnostic_run_state "
        "ON delivery_native_diagnostics(run_id,state)"
    )
    # The fence is in SQLite so a writer with an older in-memory Store cannot
    # change this stopped execution after the diagnostic acknowledgement.
    for table in (
        "delivery_runs",
        "delivery_attempts",
        "delivery_effects",
        "delivery_mutations",
        "delivery_outbox",
        "delivery_repair_grants",
        "delivery_gate_admissions",
        "delivery_scope_amendments",
        "delivery_policy_recoveries",
        "delivery_preparations",
        "delivery_events",
        "delivery_repair_grant_extensions",
        "delivery_repair_grant_thirds",
        "delivery_repair_grant_successors",
    ):
        for operation, row in (("INSERT", "NEW"), ("UPDATE", "OLD"), ("DELETE", "OLD")):
            db.execute(f"""CREATE TRIGGER IF NOT EXISTS diagnostic_{table}_{operation.lower()}
                BEFORE {operation} ON {table} WHEN EXISTS (
                    SELECT 1 FROM delivery_native_diagnostics WHERE run_id={row}.run_id
                    AND state IN {ACTIVE})
                BEGIN SELECT RAISE(ABORT, '{ERROR}'); END""")
    for operation, row in (("INSERT", "NEW"), ("UPDATE", "OLD"), ("DELETE", "OLD")):
        db.execute(f"""CREATE TRIGGER IF NOT EXISTS diagnostic_claim_{operation.lower()}
            BEFORE {operation} ON claims WHEN EXISTS (
                SELECT 1 FROM delivery_native_diagnostics
                WHERE (work_id={row}.work_id OR issue_resource={row}.resource)
                AND state IN {ACTIVE})
            BEGIN SELECT RAISE(ABORT, '{ERROR}'); END""")
    db.execute(f"""CREATE TRIGGER IF NOT EXISTS diagnostic_work_authority
        BEFORE UPDATE OF issue,repository ON works
        WHEN (NEW.issue IS NOT OLD.issue OR NEW.repository IS NOT OLD.repository) AND EXISTS (
            SELECT 1 FROM delivery_native_diagnostics WHERE work_id=OLD.id
            AND state IN {ACTIVE})
        BEGIN SELECT RAISE(ABORT, '{ERROR}'); END""")


def _request(value, *, acknowledged=False):
    extra = {"preflight_key", "acknowledgement"} if acknowledged else set()
    supplied_fields = set(value) if isinstance(value, dict) else set()
    optional = {TEMPORAL} if TEMPORAL in supplied_fields else set()
    if (
        not isinstance(value, dict)
        or supplied_fields != FIELDS | extra | optional
        or not isinstance(value.get("command_id"), str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", value["command_id"])
        or any(
            type(value.get(key)) is not int or value[key] < 0
            for key in ("expected_revision", "expected_projection_revision", "expected_iteration")
        )
        or type(value.get("expected_pr_number")) is not int
        or value["expected_pr_number"] < 1
        or value.get("check_id") != CHECK_ID
        or value.get("expected_check_sha256") != CHECK_SHA256
        or (
            acknowledged
            and (
                value.get("acknowledgement") != ACK
                or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("preflight_key")))
            )
        )
    ):
        raise ValueError("native diagnostic requires its exact finite frozen-check request")
    for key, length in (
        ("expected_candidate_id", 64),
        ("expected_content_sha256", 64),
        ("expected_candidate_head", 40),
        ("expected_pr_head", 40),
    ):
        if not isinstance(value.get(key), str) or not re.fullmatch(
            "[0-9a-f]{" + str(length) + "}", value[key]
        ):
            raise ValueError("native diagnostic has an invalid original candidate binding")
    return {key: value[key] for key in FIELDS | optional}


def _readonly(store):
    from .delivery_store import DeliveryStore

    return store if store.read_only else DeliveryStore(store.config, read_only=True)


def _rows(db, run_id):
    result = {}
    for table, order in (
        ("delivery_runs", "run_id"),
        ("delivery_attempts", "job_key"),
        ("delivery_effects", "effect_key"),
        ("delivery_mutations", "command_id"),
        ("delivery_outbox", "run_id"),
        ("delivery_repair_grants", "run_id"),
        ("delivery_scope_amendments", "run_id"),
        ("delivery_policy_recoveries", "run_id"),
        ("delivery_preparations", "run_id"),
        ("delivery_events", "sequence"),
        ("delivery_repair_grant_extensions", "run_id"),
        ("delivery_repair_grant_thirds", "run_id"),
        ("delivery_repair_grant_successors", "run_id"),
        ("delivery_gate_admissions", "command_id"),
    ):
        result[table] = [
            dict(row)
            for row in db.execute(
                f"SELECT * FROM {table} WHERE run_id=? ORDER BY {order}", (run_id,)
            )
        ]
    return result


def _check(spec):
    matches = [
        [item for item in spec["policy"][stage] if item["id"] == CHECK_ID]
        for stage in ("prepublish_checks", "checks")
    ]
    if (
        any(len(items) != 1 for items in matches)
        or matches[0] != matches[1]
        or digest(matches[0][0]) != CHECK_SHA256
    ):
        raise ValueError("native diagnostic differs from the exact frozen worker recipe")
    return matches[0][0]


def snapshot(store, run_id, command, *, return_assessment=False):
    """Observe original authority only. This function creates no path or database object."""
    from . import delivery_temporal_prerequisites as temporal
    from .delivery_amendment_capacity import live_owner
    from .delivery_policy_recovery import work_binding
    from .delivery_session_custody import completed_blocked_finding
    from .delivery_stopped_resume import review_snapshot
    from .delivery_unpublished_recovery import teardown_checkpoint

    store = _readonly(store)
    seal = review_snapshot(store, run_id)
    spec, row, state, candidate = (
        seal[k] for k in ("predecessor_spec", "row", "state", "candidate")
    )
    latest = next((r for r in reversed(state.get("roles", [])) if r.get("role") == "implement"), {})
    publication = seal.get("publication") or {}
    from .delivery_lifecycle import require_transition

    lifecycle_assessment = require_transition({
        "operation": KIND, "owner": "delivery_native_diagnostic",
        "owner_proof": True, "normalized_lifecycle": "stopped",
        "proof_digest": digest(seal),
        "revision": row["protocol_revision"], "projection_revision": row["revision"],
        "iteration": row["iteration"], "candidate_id": candidate.get("id"),
        "candidate_head": candidate.get("head"), "content_sha256": candidate.get("content_sha256"),
        "publication_number": publication.get("number"),
        "publication_head": publication.get("head"),
        "publication_required": True,
        "comparisons": [
            ("revision", "expected_revision", "revision_changed"),
            ("projection_revision", "expected_projection_revision", "projection_revision_changed"),
            ("iteration", "expected_iteration", "iteration_changed"),
            ("candidate_id", "expected_candidate_id", "candidate_changed"),
            ("candidate_head", "expected_candidate_head", "candidate_head_changed"),
            ("content_sha256", "expected_content_sha256", "candidate_content_changed"),
            ("publication_number", "expected_pr_number", "publication_changed"),
            ("publication_head", "expected_pr_head", "publication_head_changed"),
        ],
        "predicates": [{
            "code": "diagnostic_scope_or_checkpoint_ineligible",
            "satisfied": (spec.get("provider") == "codex" and not spec.get("feature_worker")
                and not spec.get("feature_delivery")
                and spec["policy"].get("host_sandbox") == "trusted-local"
                and latest.get("status") == "blocked" and latest.get("finish_reason") == "done"
                and latest.get("iteration") == row["iteration"]),
            "missing_or_unknown_fact": (
                "original closed blocked implementation and diagnostic scope"
            ),
            "next_action": "inspect the original diagnostic owner and retained execution",
        }],
    }, command)
    check = _check(spec)
    from .delivery_preparation import verify_prepared_spec

    verify_prepared_spec(spec)
    from .delivery_feature_review_context import current_runtime_identity

    diagnostic_runtime = current_runtime_identity()
    assessment = completed_blocked_finding(seal)
    # This is the existing acknowledged Temporal contract, not the non-native
    # role probe or a newly supplied executable/environment declaration.
    with store._connect() as db:
        preparation = temporal.custody(db, spec, seal.get("original_recovery"))
        _owner, operational = live_owner(spec, db=db)
        rows = _rows(db, run_id)
        claim = store.state.claim_for(db, spec["work_id"])
        binding = work_binding(store, spec, db)
        if claim is not None or len(rows["delivery_runs"]) != 1:
            raise ValueError("native diagnostic lost its released original work claim")
        latest_run = next(
            (
                item["run_id"]
                for item in db.execute(
                    "SELECT run_id,issue_url FROM delivery_runs ORDER BY rowid DESC"
                )
                if store.state.issue_resource(item["issue_url"]) == binding["issue_resource"]
            ),
            None,
        )
        if (
            latest_run != run_id
            or any(
                item["state"] != "complete" or item["kind"] == "cancel"
                for item in rows["delivery_mutations"]
            )
            or any(item["state"] != "sent" for item in rows["delivery_outbox"])
        ):
            raise ValueError("native diagnostic has a superseded issue or unfinished command")
    def refuse_tool_custody(fact):
        # An authenticated closed seal is evidence, not a grant of tool authority.
        require_transition({
            "operation": KIND, "owner": "delivery_native_diagnostic",
            "owner_proof": True, "proof_digest": digest(seal),
            "normalized_lifecycle": "stopped", "comparisons": [],
            "predicates": [{
                "code": "diagnostic_tool_custody_unavailable",
                "satisfied": False,
                "missing_or_unknown_fact": fact,
                "next_action": "inspect the original acknowledged official tool custody",
            }],
        }, command)

    marker = spec.get(temporal.MARKER)
    declaration = command.get(TEMPORAL)
    if declaration is not None:
        if (
            not isinstance(declaration, dict)
            or declaration.get("provenance_sha256") != PROVENANCE_SHA256
        ):
            refuse_tool_custody("authorized official tool provenance")
        if preparation is None:
            derived, preparation = temporal.proposal(seal, command, execution=spec)
            marker = derived[temporal.MARKER]
            if {k: v for k, v in derived.items() if k != temporal.MARKER} != spec:
                raise ValueError("diagnostic tool bridge changed the original admitted execution")
        elif declaration != preparation["declaration"]:
            refuse_tool_custody("exact acknowledged tool declaration")
    if preparation is None or marker is None:
        refuse_tool_custody("acknowledged official tool custody and marker")
    from .delivery_resources import observe_finalized_resources

    teardown = teardown_checkpoint(seal.get("historical_quarantines", []))
    physical = observe_finalized_resources(spec, **({"quarantined": teardown} if teardown else {}))
    local = state.get("checks", {}).get("local") or {}
    gate = local.get("results", [])
    failed = [item for item in gate if item.get("id") == CHECK_ID and item.get("passed") is False]
    if len(failed) != 1:
        raise ValueError("native diagnostic requires the retained original failed worker gate")
    failed_proof = failed_gate(spec, candidate, check, failed[0], physical, local)
    closed = {k: v for k, v in seal["closed"].items() if k != "result"}
    result = {
        "run_id": run_id,
        "work_id": spec["work_id"],
        "closed": closed,
        "closed_result_sha256": digest(state),
        "rows_sha256": digest(rows),
        "spec_sha256": digest(spec),
        "candidate": candidate,
        "publication": publication,
        "assessment": assessment,
        "failed_gate_sha256": digest(failed[0]),
        "failed_gate_evidence": failed_proof,
        "check": check,
        "preparation": preparation,
        "temporal_marker": marker,
        "work_binding": binding,
        "operational_owner": operational,
        "controller_browser_binding": str(Path.home() / "Library/Caches/ms-playwright"),
        "diagnostic_runtime": diagnostic_runtime,
        "claim": None,
        "cleanup_digest": seal["cleanup_digest"],
        "physical_resources": physical,
        "iteration": row["iteration"],
    }
    result["identity"] = digest(
        {
            key: result[key]
            for key in (
                "run_id",
                "closed",
                "closed_result_sha256",
                "candidate",
                "check",
                "spec_sha256",
            )
        }
    )
    return (result, lifecycle_assessment) if return_assessment else result


def failed_gate(spec, candidate, check, failed, physical, local):
    """The old failure remains authentic evidence, never a replacement passing verdict."""
    from .delivery_check_evidence import _owned_bytes

    native = failed.get("native_process") or {}
    journal = Path(native.get("journal", ""))
    log = Path(failed.get("log", ""))
    root = Path(spec["state_dir"])
    if (
        failed.get("argv") != check["argv"]
        or local.get("candidate_id") != candidate["id"]
        or native.get("cleanup") != "observed-native-confirmed"
        or native.get("state") != "finished"
        or not journal.is_absolute()
        or not journal.is_relative_to(root)
        or not log.is_absolute()
        or log != journal.with_name("process.log")
        or str(journal) not in physical["journal_sha256"]
    ):
        raise ValueError("diagnostic failed gate lacks its exact registered native evidence")
    raw = read_private(journal, raw=True)
    value = json.loads(raw)
    log_raw = _owned_bytes(log)
    intent = value.get("intent", {})
    if (
        value.get("result") != native
        or hashlib.sha256(raw).hexdigest() != physical["journal_sha256"][str(journal)]
        or hashlib.sha256(log_raw).hexdigest() != failed.get("log_sha256")
        or intent.get("run_id") != spec["run_id"]
        or intent.get("policy_digest") != spec["policy_digest"]
        or intent.get("argv") != check["argv"]
        or intent.get("timeout") != check["timeout_seconds"]
        or intent.get("cwd") != failed.get("cwd")
        or intent.get("ports") != []
    ):
        raise ValueError("diagnostic retained failed gate was changed or misattributed")
    return {
        "journal": str(journal),
        "journal_sha256": hashlib.sha256(raw).hexdigest(),
        "log": str(log),
        "log_sha256": hashlib.sha256(log_raw).hexdigest(),
    }


def _root(spec, identity):
    if not isinstance(identity, str) or not re.fullmatch(r"[0-9a-f]{64}", identity):
        raise ValueError("native diagnostic identity is invalid")
    path = Path(spec["state_dir"]) / "native-diagnostics" / identity
    _ancestors(path, allow_missing=True)
    return path


@contextmanager
def generation_lease(root, *, create=False, expected=None):
    """Only ACK creates this private generation; all execution/replay opens are exact."""
    path = root / "execution.lock"
    flags = os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK | (os.O_CREAT if create else 0)
    fd = os.open(path, flags, 0o600)

    def authenticate():
        info, name = os.fstat(fd), path.lstat()
        value = {
            "root": _identity(root),
            "lock": {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid},
        }
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(root.lstat().st_mode) != 0o700
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
            or info.st_size != 0
            or (info.st_dev, info.st_ino) != (name.st_dev, name.st_ino)
            or (expected is not None and value != expected)
        ):
            raise ValueError("native diagnostic execution generation changed")
        return value

    try:
        observed = authenticate()
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if authenticate() != observed:
            raise ValueError("native diagnostic generation changed while acquiring its lease")
        yield observed
        if authenticate() != observed:
            raise ValueError("native diagnostic generation changed during its operation")
    finally:
        os.close(fd)


def require_generation(root, expected):
    path = root / "execution.lock"
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info, name = os.fstat(fd), path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(root.lstat().st_mode) != 0o700
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
            or info.st_size != 0
            or (info.st_dev, info.st_ino) != (name.st_dev, name.st_ino)
            or {
                "root": _identity(root),
                "lock": {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid},
            }
            != expected
        ):
            raise ValueError("native diagnostic lost its acknowledged resource generation")
    finally:
        os.close(fd)


def _saved(db, run_id, *, command_id=None, identity=None):
    sql = "SELECT * FROM delivery_native_diagnostics WHERE run_id=?"
    args = [run_id]
    if command_id is not None:
        sql += " AND command_id=?"
        args.append(command_id)
    if identity is not None:
        sql += " AND identity=?"
        args.append(identity)
    try:
        row = db.execute(sql, args).fetchone()
    except sqlite3.OperationalError as exc:
        if "no such table: delivery_native_diagnostics" not in str(exc):
            raise
        return None  # An old read-only store is not migrated by preflight.
    return dict(row) if row else None


def require_resource_idle(spec):
    """Fence ordinary file finalizers too, including a writer queued on its resource lease."""
    if "config_path" not in spec:
        return
    from .delivery_execution_config import load_execution_config
    from .delivery_preparation_resources import canonical_owner

    parent = canonical_owner(spec)
    config = load_execution_config(parent or spec)
    with closing(sqlite3.connect(config.tracking_db.as_uri() + "?mode=ro", uri=True)) as db:
        try:
            active = db.execute(
                f"SELECT 1 FROM delivery_native_diagnostics WHERE run_id=? AND state IN {ACTIVE}",
                (spec["run_id"],),
            ).fetchone()
        except sqlite3.OperationalError as exc:
            if "no such table: delivery_native_diagnostics" not in str(exc):
                raise
            return
    if active:
        raise ValueError(ERROR)


def active(store, run_id):
    with store._connect() as db:
        try:
            return (
                db.execute(
                    f"SELECT 1 FROM delivery_native_diagnostics WHERE run_id=? "
                    f"AND state IN {ACTIVE}",
                    (run_id,),
                ).fetchone()
                is not None
            )
        except sqlite3.OperationalError as exc:
            if "no such table: delivery_native_diagnostics" not in str(exc):
                raise
            return False


def preflight(store, run_id, supplied):
    command = _request(supplied)
    observed, lifecycle_assessment = snapshot(store, run_id, command, return_assessment=True)
    with store._connect() as db:
        prior = _saved(db, run_id, identity=observed["identity"])
    if prior is not None:
        raise ValueError("this exact closed diagnostic generation was already acknowledged")
    return {
        "kind": KIND,
        "run_id": run_id,
        "lifecycle_assessment": lifecycle_assessment,
        "identity": observed["identity"],
        "preflight_key": digest(observed),
        "check_sha256": digest(observed["check"]),
        "diagnostic_only": True,
        "provider_turns": 0,
        "delivery_acceptance": False,
    }


def acknowledge(store, run_id, supplied):
    command = _request(supplied, acknowledged=True)
    request_digest = digest(supplied)
    with store._connect() as db:
        prior = _saved(db, run_id, command_id=command["command_id"])
    if prior is not None:
        if prior["request_digest"] != request_digest:
            raise ValueError("native diagnostic command changed after acknowledgement")
        from .delivery_native_diagnostic_execution import reconcile_complete

        try:
            reconcile_complete(store, prior)
        except BlockingIOError:
            pass  # The same acknowledged body still owns its execution lease.
        result = info(store, run_id, command["command_id"])
        if result["state"] == "queued" and getattr(store, "_native_diagnostic_notify", None):
            store._native_diagnostic_notify()
        return result
    observed = snapshot(store, run_id, command)
    if digest(observed) != supplied["preflight_key"]:
        raise ValueError("native diagnostic authority changed after its public preflight")
    spec = store.effective_spec(run_id)
    root = _root(spec, observed["identity"])
    private_directory(root)
    with generation_lease(root, create=True) as generation:
        intent = {
            "kind": KIND,
            "generation": generation,
            "command": supplied,
            "request_digest": request_digest,
            "snapshot": observed,
            "spec_sha256": digest(spec),
        }
        _immutable(root / "intent.json", intent)
        response = {
            "kind": KIND,
            "run_id": run_id,
            "command_id": command["command_id"],
            "identity": observed["identity"],
            "state": "queued",
            "diagnostic_only": True,
            "provider_turns": 0,
            "delivery_acceptance": False,
        }
        # Fence an already validated ordinary finalizer as well as DB writers.
        # Its exclusive resource lease cannot overlap this acknowledgement, and
        # after commit its own post-lock diagnostic check refuses file effects.
        from .delivery_resources import RunResources

        with RunResources(spec, read_only=True).locked(read_only=True), store._connect() as db:
            if snapshot(store, run_id, command) != observed:
                raise ValueError("native diagnostic authority changed before acknowledgement")
            db.execute("BEGIN IMMEDIATE")
            if (
                digest(_rows(db, run_id)) != observed["rows_sha256"]
                or store.state.claim_for(db, observed["work_id"]) is not None
            ):
                raise ValueError(
                    "native diagnostic original execution changed before acknowledgement"
                )
            existing = db.execute(
                "SELECT 1 FROM delivery_commands WHERE command_id=?", (command["command_id"],)
            ).fetchone()
            if existing or _saved(db, run_id, identity=observed["identity"]):
                raise ValueError("native diagnostic identity or command was already acknowledged")
            db.execute(
                "INSERT INTO delivery_native_diagnostics VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    observed["identity"],
                    command["command_id"],
                    run_id,
                    observed["work_id"],
                    observed["work_binding"]["issue_resource"],
                    request_digest,
                    canonical_json(observed),
                    hashlib.sha256(read_private(root / "intent.json", raw=True)).hexdigest(),
                    "queued",
                    canonical_json(response),
                    datetime.now(UTC).isoformat(),
                ),
            )
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (command["command_id"], run_id, request_digest, canonical_json(response)),
            )
    notify = getattr(store, "_native_diagnostic_notify", None)
    if notify is not None:
        notify()  # Wake hint only; execution freshly authenticates the durable ACK.
    return response


def info(store, run_id, command_id):
    with store._connect() as db:
        row = _saved(db, run_id, command_id=command_id)
    if row is None:
        raise ValueError("native diagnostic command is absent")
    result = {**json.loads(row["response_json"]), "state": row["state"]}
    if row["state"] == "complete":
        spec = store.effective_spec(run_id)
        receipt = _root(spec, row["identity"]) / "receipt.json"
        raw = read_private(receipt, raw=True)
        if hashlib.sha256(raw).hexdigest() != result.get("receipt_sha256"):
            raise ValueError("native diagnostic immutable receipt changed")
        from .delivery_native_diagnostic_execution import verify_receipt

        receipt_value = json.loads(raw)
        verify_receipt(spec, row, receipt_value)
        result = {**result, "receipt": receipt_value}
    return result


def indexed_evidence(spec, row):
    """Index ACK-bound diagnostics; selecting content still verifies the individual SHA."""
    root = _root(spec, row["identity"])
    response = json.loads(row["response_json"])
    expected = {
        "intent.json": row["intent_sha256"],
        "readiness.json": response.get("readiness_sha256"),
        "receipt.json": response.get("receipt_sha256"),
    }
    entries = []
    prefix = "native-diagnostic-" + row["identity"] + "-"
    for name, sha in expected.items():
        path = root / name
        if sha and os.path.lexists(path):
            entries.append(
                {
                    "id": prefix + name,
                    "path": path,
                    "expected_sha256": sha,
                    "label": "Diagnostic only; no delivery acceptance: " + name,
                    "limit": 4 * 1024 * 1024,
                }
            )
    if row["state"] == "complete":
        raw = read_private(root / "receipt.json", raw=True)
        value = json.loads(raw)
        if (
            hashlib.sha256(raw).hexdigest() != expected["receipt.json"]
            or value.get("identity") != row["identity"]
            or value.get("run_id") != row["run_id"]
            or value.get("command_id") != row["command_id"]
            or value.get("diagnostic_only") is not True
            or value.get("delivery_acceptance") is not False
        ):
            raise ValueError("diagnostic evidence lost its exact receipt acknowledgement")
        artifacts, total = value.get("artifacts"), 0
        if not isinstance(artifacts, dict) or not 2 <= len(artifacts) <= 4200:
            raise ValueError("diagnostic evidence inventory is incomplete or unbounded")
        for index, (name, reference) in enumerate(sorted(artifacts.items())):
            relative = Path(name)
            if (
                relative.is_absolute()
                or relative.as_posix() != name
                or ".." in relative.parts
                or not isinstance(reference, dict)
                or type(reference.get("bytes")) is not int
                or not 0 <= reference["bytes"] <= 50 * 1024 * 1024
                or not re.fullmatch("[0-9a-f]{64}", str(reference.get("sha256")))
            ):
                raise ValueError("diagnostic evidence has an invalid immutable reference")
            total += reference["bytes"]
            if total > 96 * 1024 * 1024:
                raise ValueError("diagnostic evidence exceeds its complete aggregate bound")
            path = root / spec["run_id"] / relative
            if path.resolve() != path:
                raise ValueError("diagnostic evidence path was linked")
            entries.append(
                {
                    "id": prefix + "artifact-" + str(index),
                    "path": path,
                    "expected_sha256": reference["sha256"],
                    "label": "Diagnostic only: " + name,
                    "limit": reference["bytes"],
                }
            )
    else:
        for name in ("manifest.json", "finalization.json"):
            path = root / spec["run_id"] / "resources" / name
            if path.exists() and path.resolve() == path:
                entries.append(
                    {
                        "id": prefix + "observed-" + name,
                        "path": path,
                        "label": "Unfinished diagnostic resource observation: " + name,
                        "limit": 4 * 1024 * 1024,
                    }
                )
    return entries


async def dispatch_once(service):
    """Dispatch a durable queued identity once; a running identity is never relaunched."""
    import time

    from .delivery_activities import _execute_check
    from .delivery_preparation import execution_retired

    if not getattr(service, "_native_diagnostic_pending", True):
        return
    if time.monotonic() < getattr(service, "_native_diagnostic_retry_at", 0):
        return
    # This is only a wake hint, cleared before the read so a concurrent ACK
    # sets it again. Idle pumps do not hydrate the full installation receipt
    # merely to poll an empty diagnostic table; no proof is cached here.
    service._native_diagnostic_pending = False

    def queued():
        with service.store._connect() as db:
            return [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM delivery_native_diagnostics "
                    "WHERE state='queued' ORDER BY created_at"
                )
            ]

    if not hasattr(service, "native_diagnostic_tasks"):
        service.native_diagnostic_tasks = {}
    try:
        rows = await asyncio.to_thread(queued)
    except Exception:
        service._native_diagnostic_pending = True
        raise
    for row in rows:
        identity = row["identity"]
        task = service.native_diagnostic_tasks.get(identity)
        if task is not None:
            continue
        try:
            spec = await asyncio.to_thread(service.store.effective_spec, row["run_id"])
            if execution_retired(spec):
                continue
            if not await asyncio.to_thread(service.store.owns_execution, spec):
                # Ownership may become observable again after a transient loss.
                # Keep a bounded wake hint, without authorizing execution here.
                service._native_diagnostic_pending = True
                service._native_diagnostic_retry_at = time.monotonic() + 5
                continue
        except (ValueError, RuntimeError, OSError, KeyError):
            # Keep the wake hint for bounded retry after transient observation
            # failures, without relaunching running or unknown diagnostics.
            service._native_diagnostic_pending = True
            service._native_diagnostic_retry_at = time.monotonic() + 5
            continue

        async def once(spec=spec, row=row):
            try:
                # Queue wait and setup are finite too. Cancellation joins the
                # actual monitor through the existing shared-slot owner.
                async with asyncio.timeout(7200):
                    return await _execute_check(
                        {
                            "spec": spec,
                            "iteration": json.loads(row["snapshot_json"])["iteration"],
                            "candidate": json.loads(row["snapshot_json"])["candidate"],
                        },
                        lambda broker: execute(broker, row),
                        kind="native-diagnostic",
                    )
            except Exception as exc:
                await asyncio.to_thread(record_unknown, service.store, row, exc)

        service.native_diagnostic_tasks[identity] = asyncio.create_task(once())


def record_unknown(store, row, exc):
    response = {
        "kind": KIND,
        "state": "unknown",
        "run_id": row["run_id"],
        "command_id": row["command_id"],
        "identity": row["identity"],
        "diagnostic_only": True,
        "delivery_acceptance": False,
        "provider_turns": 0,
        "error": type(exc).__name__ + ": " + str(exc)[:500],
    }
    with store._connect() as db:
        saved = _saved(db, row["run_id"], identity=row["identity"])
        if saved is not None:
            previous = json.loads(saved["response_json"])
            for name in (
                "subject_sha256",
                "native_launches",
                "phase_result_sha256",
                "readiness_sha256",
            ):
                if name in previous:
                    response[name] = previous[name]
        changed = db.execute(
            "UPDATE delivery_native_diagnostics SET state='unknown',response_json=? "
            "WHERE identity=? AND state IN ('queued','running')",
            (canonical_json(response), row["identity"]),
        ).rowcount
        if changed:
            db.execute(
                "UPDATE delivery_commands SET response_json=? WHERE command_id=?",
                (canonical_json(response), row["command_id"]),
            )


def execute(broker, row):
    from .delivery_native_diagnostic_execution import execute as run_owned

    return run_owned(broker, row)
