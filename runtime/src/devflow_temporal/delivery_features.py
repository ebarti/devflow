"""Transactional current feature state, distinct from immutable delivery receipts."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .contracts import canonical_json

logger = logging.getLogger(__name__)

STATUSES = (
    "Backlog", "Queued", "Planning", "In Progress", "Validating", "Awaiting merge",
    "Done", "Blocked", "Cancelled",
)


def now() -> str:
    return datetime.now(UTC).isoformat()


def initialize(db: sqlite3.Connection) -> None:
    from .delivery_project_board import initialize as initialize_board

    initialize_board(db)
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_features (
        issue TEXT PRIMARY KEY, run_id TEXT NOT NULL, version INTEGER NOT NULL,
        payload_json TEXT NOT NULL, updated_at TEXT NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_feature_events (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT, issue TEXT NOT NULL,
        version INTEGER NOT NULL, payload_json TEXT NOT NULL, created_at TEXT NOT NULL,
        UNIQUE(issue,version))""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pr_observations (
        url TEXT PRIMARY KEY, observation_json TEXT, checked_at TEXT,
        next_check_at TEXT NOT NULL, error TEXT)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pr_refresh_requests (
        url TEXT PRIMARY KEY, request_id TEXT NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_issue_observations (
        url TEXT PRIMARY KEY, observation_json TEXT, checked_at TEXT,
        next_check_at TEXT NOT NULL, error TEXT)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_legacy_publication_bindings (
        issue TEXT PRIMARY KEY, projection_run_id TEXT NOT NULL,
        publication_run_id TEXT NOT NULL, publication_json TEXT NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pr_observation_inputs (
        url TEXT PRIMARY KEY, publication_key TEXT NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_feature_views (
        issue TEXT PRIMARY KEY, payload_json TEXT NOT NULL, updated_at TEXT NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_project_outbox (
        issue TEXT PRIMARY KEY, identity TEXT NOT NULL, payload_json TEXT NOT NULL,
        state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at TEXT NOT NULL, last_error TEXT, receipt_json TEXT,
        checked_at TEXT)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_sync_settings (
        key TEXT PRIMARY KEY, value INTEGER NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_feature_projections (
        run_id TEXT PRIMARY KEY, revision INTEGER NOT NULL, generation TEXT NOT NULL,
        error TEXT, projected_at TEXT NOT NULL)""")
    # The newest-run-per-issue lookup in transition() runs on every projection;
    # without this expression index it scans delivery_runs, which makes a pass
    # over history quadratic.
    db.execute("""CREATE INDEX IF NOT EXISTS delivery_runs_issue_newest
        ON delivery_runs(lower(rtrim(issue_url,'/')), created_at, run_id)""")


NEWEST_RUN_SQL = (
    "SELECT run_id FROM delivery_runs WHERE lower(rtrim(issue_url,'/'))=? "
    "AND json_extract(request_json,'$.feature_worker') IS NULL "
    "ORDER BY created_at DESC,run_id DESC LIMIT 1"
)


def issue_key(url: str) -> str:
    return url.rstrip("/").casefold()


def ownership_order(value: dict) -> tuple:
    """Explicit execution generations supersede legacy timestamp selection."""
    owner = value.get("execution_owner")
    return (bool(owner), owner["generation"] if owner else 0,
            value["admitted_at"], value["run_id"], value["source"])


def publication_urls(receipt: dict | None) -> list[str]:
    """Only explicit publication custody grants PR observation scope."""
    if not receipt:
        return []
    # A stack receipt must enumerate its members. Never infer a feature from a
    # repository-wide list of unrelated PRs or only the top PR of a stack.
    members = receipt.get("pull_requests") or [receipt]
    return sorted({member["url"] for member in members if member.get("url")})


def _status(row: dict, observations: list[dict], receipt: dict | None = None,
            issue_observation: dict | None = None) -> str:
    known = [item.get("observation") for item in observations]
    receipt = receipt or json.loads(row["pr_json"] or "{}")
    spec = json.loads(row["request_json"])
    settled = (row.get("execution_state") in {"terminal", "blocked", "cancelled"}
               or row.get("outcome") in {"blocked", "cancelled", "published_unmerged"})
    completed_issue = bool(settled and issue_observation
                           and issue_observation.get("state") == "CLOSED"
                           and issue_observation.get("state_reason") == "COMPLETED")
    complete = (not spec.get("feature_delivery") or receipt.get("scope_complete") is True
                or completed_issue)
    if complete and known and all(item and item["state"] == "MERGED" for item in known):
        return "Done"
    labels = {item.casefold() for item in (issue_observation or {}).get("labels", [])}
    if (issue_observation or {}).get("state_reason") == "NOT_PLANNED" or labels & {
        "status: cancelled", "wontfix", "won't fix",
    }:
        return "Cancelled"
    if labels & {"status: deferred", "do not merge yet"}:
        return "Backlog"
    if any(item and item["state"] == "CLOSED" for item in known):
        return "Backlog"
    if completed_issue:
        return "Backlog"  # Issue closure alone does not prove delivery.
    if spec.get("feature_delivery") and any(item and item["state"] == "MERGED" for item in known):
        return "In Progress"
    phase = row["phase"]
    if phase == "waiting_feature_readback":
        return "In Progress"
    if row["outcome"] == "cancelled" or phase == "cancelled":
        return "Backlog"
    if row["outcome"] == "blocked" or phase == "blocked":
        heads = {item["url"]: item.get("head") for item in
                 (receipt.get("pull_requests") or [receipt]) if item.get("url")}
        if any(item and item["state"] == "OPEN" and item.get("head") != heads.get(item["url"])
               for item in known):
            return "Validating"
        return "Blocked"
    if phase in {"delivered", "awaiting_merge", "merged"}:
        heads = {item["url"]: item.get("head") for item in
                 (receipt.get("pull_requests") or [receipt]) if item.get("url")}
        if any(item and item.get("head") != heads.get(item["url"]) for item in known):
            return "Validating"
        return "Awaiting merge"
    if phase in {"accepted", "queued", "preparing", "temporal_pending"}:
        return "Queued"
    if phase in {"waiting_question", "waiting_plan", "waiting_plan_revision",
                 "waiting_decision", "needs_decision",
                 "awaiting_answers", "awaiting_plan"}:
        return "Blocked"
    if any(part in phase for part in ("intake", "plan", "decision", "question")):
        return "Planning"
    if phase in {"merging", "merge"}:
        return "In Progress"
    if "review" in phase:
        return "Validating"
    if any(part in phase for part in ("verify", "verification", "qa", "ci", "check", "gate")):
        return "Validating"
    return "In Progress"


def transition(db: sqlite3.Connection, config: Any, run_id: str) -> None:
    """Called in the SAME transaction as a delivery event or PR observation."""
    row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
    if row is None:
        return
    row = dict(row)
    spec = json.loads(row["request_json"])
    if spec.get("feature_worker"):
        from .delivery_feature_execution import registry

        actual = registry(spec).current(spec["feature_delivery"]["owner"]["issue_id"])
        if actual and actual["store_path"] == str(config.tracking_db):
            transition(db, config, actual["run_id"])
        return
    key = issue_key(row["issue_url"])
    owner = spec.get("feature_delivery", {}).get("owner")
    if owner:
        from .delivery_feature_execution import registry

        actual = registry(spec).current(owner["issue_id"])
        if actual is None or registry(spec).token(actual) != owner:
            return
    newest = db.execute(NEWEST_RUN_SQL, (key,)).fetchone()
    if not owner and newest[0] != run_id:
        return
    work = db.execute("SELECT details FROM works WHERE id=?", (row["work_id"],)).fetchone()
    tracking = json.loads(work[0] or "{}").get("github", {}) if work else {}
    repository = config.raw["repositories"].get(row["repository_key"], {})
    binding = spec.get("project_binding") or {
        "project": tracking.get("project") or repository.get("project_url"),
        "assignee": (tracking.get("sync") or {}).get("assignee") or repository.get("assignee"),
    }
    from .delivery_legacy_publication import publication

    receipt, publication_binding = publication(db, row)
    issue_row = db.execute("SELECT * FROM delivery_issue_observations WHERE url=?",
                           (key,)).fetchone()
    issue_observation = ({"observation": json.loads(issue_row["observation_json"] or "null"),
                          "checked_at": issue_row["checked_at"], "error": issue_row["error"]}
                         if issue_row else None)
    observations = []
    for url in publication_urls(receipt):
        observation = db.execute(
            "SELECT * FROM delivery_pr_observations WHERE url=?", (url,),
        ).fetchone()
        observations.append({
            "url": url,
            "observation": json.loads(observation["observation_json"] or "null")
            if observation else None,
            "checked_at": observation["checked_at"] if observation else None,
            "error": observation["error"] if observation else None,
        })
    merge_status = None
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='delivery_merge_requests'").fetchone():
        merge = db.execute("SELECT phase FROM delivery_merge_requests WHERE run_id=?",
                           (run_id,)).fetchone()
        if merge:
            merge_status = {"merged": "Done", "blocked": "Blocked",
                            "decision_required": "Blocked", "unknown": "Blocked",
                            "waiting_ci": "Validating"}
            merge_status = merge_status.get(merge[0], "In Progress")
    current_status = _status(
        row, observations, receipt, (issue_observation or {}).get("observation"))
    payload = {
        "issue": key, "run_id": run_id, "work_id": row["work_id"],
        "admitted_at": row["created_at"], "run_revision": row["revision"],
        "status": (current_status if current_status in {"Done", "Backlog", "Cancelled"}
                   else merge_status or current_status),
        "binding": binding, "issue_observation": issue_observation,
        "legacy_tracking": spec.get("project_sync_version") != 1
        and row["execution_state"] not in {"terminal", "blocked", "cancelled"},
        "pull_requests": observations,
        **({"legacy_publication_binding": publication_binding} if publication_binding else {}),
        **({"execution_owner": owner, "repair_budget": registry(spec).budget(owner["issue_id"]),
            "github_record": registry(spec).checkpoints(owner["issue_id"]).get("github-record")}
           if owner else {}),
    }
    if owner:
        from .delivery_feature_pass import checkpoints as current_checkpoints

        checkpoints = current_checkpoints(spec)
        from .delivery_feature_revisions import adopted_plan, local_revision_readback

        shared = registry(spec)
        raw_plan = row.get("accepted_plan_text") or spec.get("accepted_plan")
        plan = adopted_plan(spec, shared=shared) or (json.loads(raw_plan) if raw_plan else {})
        payload["feature_plan"] = local_revision_readback(
            spec, expected_revision=row["revision"], phase=row["phase"], cleanup=row["cleanup"])
        members = (receipt or {}).get("pull_requests", [])
        observation_by_url = {item["url"]: item for item in observations}
        member_by_chunk = {member["chunk_id"]: member for member in members
                           if member.get("chunk_id")}
        workstreams = []
        for stream in plan.get("workstreams", []):
            binding = checkpoints.get("workstream-issues", {}).get(stream["id"])
            if not binding:
                continue
            workers = []
            finished = 0
            for chunk in stream["chunks"]:
                proof = checkpoints.get("verified:" + chunk["id"])
                finished += bool(proof)
                assignment = (checkpoints.get("assignment:" + chunk["id"] + ":chunk")
                              or checkpoints.get("assignment:" + chunk["id"] + ":build"))
                if assignment:
                    worker = db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                                        (assignment["run_id"],)).fetchone()
                    if worker:
                        workers.append(dict(worker))
            chunk_ids = [chunk["id"] for chunk in stream["chunks"]]
            child_members = [member_by_chunk[chunk_id] for chunk_id in chunk_ids
                             if chunk_id in member_by_chunk]
            child_observations = [observation_by_url[member["url"]] for member in child_members
                                  if member["url"] in observation_by_url]
            observed = [item.get("observation") for item in child_observations]
            child_issue = issue_key(binding["url"])
            child_row = db.execute("SELECT * FROM delivery_issue_observations WHERE url=?",
                                   (child_issue,)).fetchone()
            child_observation = ({
                "observation": json.loads(child_row["observation_json"] or "null"),
                "checked_at": child_row["checked_at"], "error": child_row["error"],
            } if child_row else None)
            child_fact = (child_observation or {}).get("observation") or {}
            child_completed = (row["execution_state"] in {"terminal", "blocked", "cancelled"}
                               and child_fact.get("state") == "CLOSED"
                               and child_fact.get("state_reason") == "COMPLETED")
            fully_merged = ((len(child_members) == len(chunk_ids) or child_completed)
                            and observed
                            and all(item and item["state"] == "MERGED" for item in observed))
            status = "Queued"
            if fully_merged:
                status = "Done"
            elif child_completed:
                status = "Backlog"
            elif any(item and item["state"] == "MERGED" for item in observed):
                status = "In Progress"
            elif any(item and item["state"] == "CLOSED" for item in observed):
                status = "Backlog"
            elif finished == len(stream["chunks"]):
                status = "Awaiting merge"
            elif any(worker["outcome"] == "blocked" for worker in workers):
                status = "Blocked"
            elif payload["status"] in {"Cancelled", "Blocked", "Backlog", "Validating"}:
                status = payload["status"]
            elif workers:
                status = "In Progress"
            workstreams.append({"id": stream["id"], "issue_id": binding["id"],
                                "issue": issue_key(binding["url"]),
                                "issue_number": binding["number"], "status": status,
                                "required_chunks": chunk_ids,
                                "pull_requests": child_observations,
                                "issue_observation": child_observation})
        payload["workstreams"] = workstreams
    encoded = canonical_json(payload)
    prior = db.execute("SELECT * FROM delivery_features WHERE issue=?", (key,)).fetchone()
    if prior and prior["payload_json"] == encoded:
        return
    version = prior["version"] + 1 if prior else 1
    stamp = now()
    db.execute("""INSERT INTO delivery_features VALUES (?,?,?,?,?)
        ON CONFLICT(issue) DO UPDATE SET run_id=excluded.run_id,version=excluded.version,
        payload_json=excluded.payload_json,updated_at=excluded.updated_at""",
        (key, run_id, version, encoded, stamp))
    db.execute("INSERT INTO delivery_feature_events(issue,version,payload_json,created_at) "
               "VALUES (?,?,?,?)", (key, version, encoded, stamp))


_generation: str | None = None
_retried: set[tuple[str, str]] = set()
PASS_LIMIT = 500


def projection_generation() -> str:
    """Identity of the runtime code that shapes a projection.

    The whole package is digested: a projection reads registry, scope and
    GitHub contract modules well beyond this file, and any code change then
    re-projects history once per database. Nothing from a configuration enters,
    so processes running the same code agree whichever admitted configuration
    (live file or sealed per-run snapshot) builds their store.
    """
    global _generation
    if _generation is None:
        source = hashlib.sha256()
        for module in sorted(Path(__file__).parent.glob("*.py")):
            source.update(module.name.encode())
            source.update(module.read_bytes())
        _generation = source.hexdigest()
    return _generation


def _stale_rows(db: sqlite3.Connection, generation: str, tracking_db: Any,
                limit: int) -> list[str]:
    """Runs whose projection predates their revision or this code, oldest first."""
    rows = db.execute(
        "SELECT r.run_id, r.revision, p.revision AS projected, p.generation, p.error "
        "FROM delivery_runs r LEFT JOIN delivery_feature_projections p ON p.run_id=r.run_id "
        "WHERE p.run_id IS NULL OR p.revision != r.revision OR p.generation != ? "
        "OR p.error IS NOT NULL ORDER BY r.created_at, r.run_id",
        (generation,),
    ).fetchall()
    selected: list[str] = []
    for row in rows:
        stale = row["projected"] != row["revision"] or row["generation"] != generation
        if not stale and (str(tracking_db), row["run_id"]) in _retried:
            continue  # a skipped row is retried once per service process, and when it changes
        selected.append(row["run_id"])
        if len(selected) == limit:
            break
    return selected


def _project_row(store: Any, run_id: str, generation: str) -> tuple[str, str | None]:
    """One short write transaction per row; the marker is re-read under the lock."""
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            "SELECT r.revision, p.revision AS projected, p.generation, p.error "
            "FROM delivery_runs r LEFT JOIN delivery_feature_projections p ON p.run_id=r.run_id "
            "WHERE r.run_id=?", (run_id,),
        ).fetchone()
        if row is None or (row["projected"] == row["revision"]
                           and row["generation"] == generation and row["error"] is None):
            return "fresh", None  # gone, or refreshed by a writer meanwhile
        db.execute("SAVEPOINT feature_projection")
        error = None
        try:
            transition(db, store.config, run_id)
        except Exception as exc:
            db.execute("ROLLBACK TO feature_projection")
            error = f"{type(exc).__name__}: {str(exc)[:300]}"
            logger.warning("Feature projection for run %s skipped: %s", run_id, error)
        db.execute("RELEASE feature_projection")
        record_projection(db, store.config, run_id, revision=row["revision"], error=error)
        return ("skipped" if error else "projected"), error


def maintain_projections(store: Any, *, limit: int = PASS_LIMIT) -> dict[str, Any]:
    """Service maintenance: re-project runs whose revision or projection code changed.

    Store construction never projects history. This pass does, one short write
    transaction per row, so other writers interleave freely and a row that
    cannot be projected is recorded (see projection_failures) instead of
    stopping anything. Only the service's loop runs it: the CLI, activities
    and other installs never replay history, and a row they leave stale is
    repaired by the service's next pass. Write paths keep fresh rows fresh.
    """
    generation = projection_generation()
    with store._connect() as db:
        pending = _stale_rows(db, generation, store.config.tracking_db, limit)
    projected, skipped = 0, {}
    for run_id in pending:
        _retried.add((str(store.config.tracking_db), run_id))
        state, error = _project_row(store, run_id, generation)
        if state == "projected":
            projected += 1
        elif state == "skipped":
            skipped[run_id] = error
    return {"pending": len(pending), "projected": projected, "skipped": skipped}


def record_projection(db: sqlite3.Connection, config: Any, run_id: str, *,
                      revision: int | None = None, error: str | None = None) -> None:
    """Remember which revision of a run the current projection code has seen."""
    if revision is None:
        row = db.execute("SELECT revision FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            return
        revision = row[0]
    db.execute(
        "INSERT INTO delivery_feature_projections VALUES (?,?,?,?,?) "
        "ON CONFLICT(run_id) DO UPDATE SET revision=excluded.revision,"
        "generation=excluded.generation,error=excluded.error,projected_at=excluded.projected_at",
        (run_id, revision, projection_generation(), error, now()),
    )


def projection_failures(db: sqlite3.Connection, run_id: str | None = None,
                        limit: int = 20) -> list[dict[str, Any]]:
    """Rows whose last projection was skipped, newest first, for operators."""
    rows = db.execute(
        "SELECT run_id,revision,error,projected_at FROM delivery_feature_projections "
        "WHERE error IS NOT NULL AND (? IS NULL OR run_id=?) "
        "ORDER BY projected_at DESC, run_id LIMIT ?",
        (run_id, run_id, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def current(db: sqlite3.Connection, issue: str) -> dict | None:
    key = issue_key(issue)
    local = db.execute("SELECT * FROM delivery_features WHERE issue=?", (key,)).fetchone()
    saved = db.execute("SELECT payload_json FROM delivery_feature_views WHERE issue=?",
                       (key,)).fetchone()
    source = str(Path(db.execute("PRAGMA database_list").fetchone()[2]).resolve())
    local_value = ({**json.loads(local["payload_json"]), "version": local["version"],
                    "source": source}
                   if local else None)
    view = json.loads(saved[0]) if saved else None
    if not local_value:
        return mirror_freshness(view)
    local_identity = ownership_order(local_value)
    selected_identity = ownership_order(view) if view else None
    if view and (selected_identity > local_identity or (
        selected_identity == local_identity and view["version"] >= local["version"]
    )):
        return mirror_freshness(view)
    return {**local_value, "mirror": {"state": "pending", "last_error": None}}


def mirror_freshness(feature: dict | None) -> dict | None:
    if feature and feature.get("mirror", {}).get("state") == "consistent":
        due = feature["mirror"].get("next_attempt_at")
        if due and datetime.now(UTC) > datetime.fromisoformat(due) + timedelta(minutes=5):
            return {**feature, "mirror": {**feature["mirror"], "state": "stale",
                    "last_error": "Project synchronizer has not refreshed its readback"}}
    return feature


def record_tracking(store: Any, spec: dict, status: str, release: bool) -> dict:
    """Local ownership transition only. The independent consumer owns Project writes."""
    from .delivery_policy_recovery import work_binding

    owner = f"external:devflow:{spec['run_id']}"
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        work_binding(store, spec, db)
        claim = store.state.claim_for(db, spec["work_id"])
        if claim and claim["owner"] != owner:
            raise ValueError("tracking transition belongs to another owner")
        if not claim and not release:
            raise ValueError("tracking transition requires the admitted claim")
        legacy = {"in-progress": ("active", "implementation"),
                  "in-review": ("waiting" if release else "active", "review"),
                  "blocked": ("blocked", None), "done": ("done", "delivered")}[status]
        db.execute("UPDATE works SET status=?,stage=?,updated_at=? WHERE id=?",
                   (*legacy, store.state.now(), spec["work_id"]))
        if release and claim:
            store.state.release_work(db, spec["work_id"], owner)
        # The actual feature phase commits with delivery_project; this receipt
        # confirms only local tracking/claim durability, never remote consistency.
        return {"state": "recorded", "pending": False, "scope": "local",
                "project_sync": "asynchronous", "status": status,
                "claim_released": release, "readback_at": now()}
