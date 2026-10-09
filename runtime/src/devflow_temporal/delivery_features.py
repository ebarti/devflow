"""Transactional current feature state, distinct from immutable delivery receipts."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .contracts import canonical_json

STATUSES = (
    "Queued", "Planning", "In progress", "In review", "Validating", "Awaiting merge",
    "Merging", "Merged", "Blocked", "Cancelled", "PR closed", "Needs validation",
)


def now() -> str:
    return datetime.now(UTC).isoformat()


def initialize(db: sqlite3.Connection) -> None:
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
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_feature_views (
        issue TEXT PRIMARY KEY, payload_json TEXT NOT NULL, updated_at TEXT NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_project_outbox (
        issue TEXT PRIMARY KEY, identity TEXT NOT NULL, payload_json TEXT NOT NULL,
        state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at TEXT NOT NULL, last_error TEXT, receipt_json TEXT,
        checked_at TEXT)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_sync_settings (
        key TEXT PRIMARY KEY, value INTEGER NOT NULL)""")


def issue_key(url: str) -> str:
    return url.rstrip("/").casefold()


def publication_urls(receipt: dict | None) -> list[str]:
    """Only explicit publication custody grants PR observation scope."""
    if not receipt:
        return []
    # A stack receipt must enumerate its members. Never infer a feature from a
    # repository-wide list of unrelated PRs or only the top PR of a stack.
    members = receipt.get("pull_requests") or [receipt]
    return sorted({member["url"] for member in members if member.get("url")})


def _status(row: dict, observations: list[dict]) -> str:
    known = [item.get("observation") for item in observations]
    if known and all(item and item["state"] == "MERGED" for item in known):
        return "Merged"
    if any(item and item["state"] == "CLOSED" for item in known):
        return "PR closed"
    phase = row["phase"]
    if row["outcome"] == "cancelled" or phase == "cancelled":
        return "Cancelled"
    if row["outcome"] == "blocked" or phase == "blocked":
        return "Blocked"
    if phase == "delivered":
        receipt = json.loads(row["pr_json"] or "{}")
        heads = {item["url"]: item.get("head") for item in
                 (receipt.get("pull_requests") or [receipt]) if item.get("url")}
        if any(item and item.get("head") != heads.get(item["url"]) for item in known):
            return "Needs validation"
        return "Awaiting merge"
    if phase in {"accepted", "queued", "preparing", "temporal_pending"}:
        return "Queued"
    if any(part in phase for part in ("intake", "plan", "decision", "question")):
        return "Planning"
    if phase in {"merging", "merge"}:
        return "Merging"
    if "review" in phase:
        return "In review"
    if any(part in phase for part in ("verify", "verification", "qa", "ci", "check", "gate")):
        return "Validating"
    return "In progress"


def transition(db: sqlite3.Connection, config: Any, run_id: str) -> None:
    """Called in the SAME transaction as a delivery event or PR observation."""
    row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
    if row is None:
        return
    row = dict(row)
    key = issue_key(row["issue_url"])
    newest = db.execute(
        "SELECT run_id FROM delivery_runs WHERE lower(rtrim(issue_url,'/'))=? "
        "ORDER BY created_at DESC,run_id DESC LIMIT 1", (key,),
    ).fetchone()
    if newest[0] != run_id:
        return
    spec = json.loads(row["request_json"])
    work = db.execute("SELECT details FROM works WHERE id=?", (row["work_id"],)).fetchone()
    tracking = json.loads(work[0] or "{}").get("github", {}) if work else {}
    repository = config.raw["repositories"].get(row["repository_key"], {})
    binding = spec.get("project_binding") or {
        "project": tracking.get("project") or repository.get("project_url"),
        "assignee": (tracking.get("sync") or {}).get("assignee") or repository.get("assignee"),
    }
    observations = []
    for url in publication_urls(json.loads(row["pr_json"] or "null")):
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
    payload = {
        "issue": key, "run_id": run_id, "work_id": row["work_id"],
        "admitted_at": row["created_at"], "run_revision": row["revision"],
        "status": _status(row, observations), "binding": binding,
        "legacy_tracking": spec.get("project_sync_version") != 1
        and row["execution_state"] not in {"terminal", "blocked", "cancelled"},
        "pull_requests": observations,
    }
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
    local_identity = (local_value["admitted_at"], local_value["run_id"], source)
    selected_identity = (view["admitted_at"], view["run_id"], view["source"]) if view else None
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
