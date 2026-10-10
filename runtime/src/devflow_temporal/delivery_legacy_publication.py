"""Explicit tracking references for stopped executions predating feature custody."""

from __future__ import annotations

import json

from .contracts import digest


def stopped(row) -> bool:
    return row["outcome"] is not None and row["execution_state"] in {
        "terminal", "blocked", "cancelled",
    }


def publication(db, row) -> tuple[dict | None, dict | None]:
    """Own receipts take precedence; a bridge never grants execution authority."""
    receipt = json.loads(row["pr_json"] or "null")
    if receipt or not stopped(row) or json.loads(row["request_json"]).get("feature_delivery"):
        return receipt, None
    binding = db.execute(
        "SELECT * FROM delivery_legacy_publication_bindings WHERE projection_run_id=?",
        (row["run_id"],),
    ).fetchone()
    if not binding:
        return None, None
    publisher = db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                           (binding["publication_run_id"],)).fetchone()
    issue = row["issue_url"].rstrip("/").casefold()
    if (not publisher or not stopped(publisher)
            or publisher["issue_url"].rstrip("/").casefold() != issue
            or binding["issue"] != issue or publisher["pr_json"] != binding["publication_json"]
            or json.loads(publisher["request_json"]).get("feature_delivery")):
        return None, None
    return json.loads(binding["publication_json"]), {
        "run_id": publisher["run_id"], "projection_run_id": row["run_id"],
        "publication_digest": digest(json.loads(binding["publication_json"])),
    }


def bind(synchronizer, run_id: str) -> dict:
    """Bind one known publisher to its stopped, unpublished legacy projection."""
    from .delivery_features import issue_key, publication_urls, transition
    from .delivery_project_events import notify

    matches = []
    for store in synchronizer.stores:
        with store._connect() as db:
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            if row:
                matches.append((store, dict(row)))
    if len(matches) != 1:
        raise ValueError("legacy publication run must identify exactly one configured store")
    store, publisher = matches[0]
    issue = issue_key(publisher["issue_url"])
    urls = publication_urls(json.loads(publisher["pr_json"] or "null"))
    if (not stopped(publisher) or not urls
            or json.loads(publisher["request_json"]).get("feature_delivery")
            or any(url.rsplit("/pull/", 1)[0] != publisher["issue_url"].rsplit("/issues/", 1)[0]
                   for url in urls)):
        raise ValueError("binding requires a stopped legacy publisher in the admitted repository")
    selected = synchronizer.selected().get(issue)
    if (not selected or selected[0].config.tracking_db != store.config.tracking_db
            or selected[1].get("execution_owner")):
        raise ValueError("legacy publication must belong to the selected legacy projection store")
    target = selected[1]["run_id"]
    for owner in synchronizer.stores:
        with owner._connect() as db:
            rows = db.execute("SELECT * FROM delivery_runs WHERE lower(rtrim(issue_url,'/'))=?",
                              (issue,)).fetchall()
            if any(not stopped(row) or json.loads(row["request_json"]).get("feature_delivery")
                   for row in rows):
                raise ValueError("legacy binding requires stopped legacy executions for the issue")
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        current = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (target,)).fetchone()
        source = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
        newest = db.execute("SELECT run_id FROM delivery_runs WHERE lower(rtrim(issue_url,'/'))=? "
                            "ORDER BY created_at DESC,run_id DESC LIMIT 1", (issue,)).fetchone()
        if (not current or not source or not stopped(current) or not stopped(source)
                or newest[0] != target or current["pr_json"] is not None
                or source["pr_json"] != publisher["pr_json"]):
            raise ValueError("legacy projection changed or already owns a publication")
        db.execute("""INSERT INTO delivery_legacy_publication_bindings VALUES (?,?,?,?)
            ON CONFLICT(issue) DO UPDATE SET projection_run_id=excluded.projection_run_id,
            publication_run_id=excluded.publication_run_id,publication_json=excluded.publication_json
            WHERE projection_run_id!=excluded.projection_run_id
               OR publication_run_id!=excluded.publication_run_id
               OR publication_json!=excluded.publication_json""",
            (issue, target, run_id, publisher["pr_json"]))
        transition(db, store.config, target)
    notify(store.config.tracking_db)
    return {"state": "recorded", "project_sync": "asynchronous", "issue": issue,
            "projection_run_id": target, "publication_run_id": run_id,
            "pull_requests": urls, "publication_digest": digest(json.loads(publisher["pr_json"]))}
