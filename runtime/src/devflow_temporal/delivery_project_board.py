"""Cached configured Project work, without admitting it for execution."""

from __future__ import annotations

import json
from datetime import timedelta
from urllib.parse import urlsplit

from .contracts import canonical_json


def initialize(db):
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_project_boards (
        project TEXT PRIMARY KEY, payload_json TEXT, checked_at TEXT,
        next_check_at TEXT NOT NULL, error TEXT)""")


def configured(config):
    projects = {}
    for repository in config.raw["repositories"].values():
        project, repo = repository.get("project_url"), repository.get("github_repo")
        if project and repo:
            projects.setdefault(project, set()).add(repo.casefold())
    return projects


def desired_status(item):
    """A PR attempt is distinct from the disposition of its underlying work."""
    status = item.get("status") or "Backlog"
    labels = {label.casefold() for label in item.get("labels", [])}
    if item.get("state") == "MERGED":
        return "Done"
    if labels & {"status: cancelled", "wontfix", "won't fix"} or (
        item.get("state") == "CLOSED" and item.get("state_reason") == "NOT_PLANNED"
    ):
        return "Cancelled"
    if labels & {"status: deferred", "do not merge yet"}:
        return "Backlog"
    if item.get("type") == "PullRequest" and item.get("state") == "CLOSED":
        return "Backlog"
    if (item.get("type") == "PullRequest" and item.get("state") == "OPEN"
            and status in {"Done", "Merged"}):
        return "Awaiting merge"
    if status == "PR closed":
        return "Backlog"
    # A proposal's old triage label is not an automated validation stage.
    if status == "Needs validation":
        return "Backlog"
    return {"Merged": "Done", "In review": "Awaiting merge",
            "Partially merged": "In Progress", "Merging": "In Progress",
            "In progress": "In Progress", "Delivered": "Awaiting merge"}.get(status, status)


def refresh(synchronizer):
    """One bounded remote snapshot per configured Project and refresh interval."""
    if not hasattr(synchronizer.github, "project_board"):
        return  # Lightweight protocol fakes need not provide the optional board reader.
    projects = {}
    for store in synchronizer.stores:
        for project, repositories in configured(store.config).items():
            entry = projects.setdefault(project, {"repositories": set(), "stores": []})
            entry["repositories"].update(repositories)
            entry["stores"].append(store)
    selected = synchronizer.selected()
    for project, entry in projects.items():
        stamp = synchronizer.clock()
        due = False
        for store in entry["stores"]:
            with store._connect() as db:
                prior = db.execute("SELECT next_check_at FROM delivery_project_boards "
                                   "WHERE project=?", (project,)).fetchone()
            due |= prior is None or prior[0] <= stamp.isoformat()
        if not due:
            continue
        snapshot, error = None, None
        try:
            snapshot = synchronizer.github.project_board(project)
            changes = {}
            for item in snapshot["items"]:
                if item["repository"].casefold() not in entry["repositories"]:
                    continue
                # Runtime custody and validation take precedence over generic triage.
                if item["url"].rstrip("/").casefold() in selected:
                    continue
                status = desired_status(item)
                if status != item.get("status"):
                    synchronizer.github.project_item_status(snapshot, item, status)
                    changes[item["id"]] = status
            if changes:
                snapshot = synchronizer.github.project_board(project)
                observed = {item["id"]: item.get("status") for item in snapshot["items"]}
                if any(observed.get(key) != value for key, value in changes.items()):
                    raise ValueError("Project status changes did not read back")
            snapshot["items"] = [item for item in snapshot["items"]
                                 if item["repository"].casefold() in entry["repositories"]]
        except Exception as exc:
            snapshot, error = None, str(exc)[:1000]
        for store in entry["stores"]:
            with store._connect() as db:
                db.execute("""INSERT INTO delivery_project_boards VALUES (?,?,?,?,?)
                    ON CONFLICT(project) DO UPDATE SET
                    payload_json=COALESCE(excluded.payload_json,delivery_project_boards.payload_json),
                    checked_at=COALESCE(excluded.checked_at,delivery_project_boards.checked_at),
                    next_check_at=excluded.next_check_at,error=excluded.error""",
                    (project, canonical_json(snapshot) if snapshot else None,
                     stamp.isoformat() if snapshot else None,
                     (stamp + timedelta(seconds=15 if error else
                                        synchronizer.project_interval)).isoformat(), error))


def read(store):
    projects, items = [], {}
    for project, repositories in configured(store.config).items():
        with store._connect() as db:
            row = db.execute("SELECT * FROM delivery_project_boards WHERE project=?",
                             (project,)).fetchone()
        projects.append({"url": project, "checked_at": row["checked_at"] if row else None,
                         "error": row["error"] if row else None,
                         "state": "unavailable" if not row or not row["payload_json"]
                         else "stale" if row["error"] else "observed"})
        for item in json.loads(row["payload_json"] or "{}").get("items", []) if row else []:
            url = urlsplit(item["url"])
            if (item["repository"].casefold() not in repositories or url.scheme != "https"
                    or url.username or url.password or url.netloc != urlsplit(project).netloc):
                continue
            items[item["url"].casefold()] = {**item, "project": project,
                                            "checked_at": row["checked_at"]}
    return {"items": list(items.values()), "projects": projects}
