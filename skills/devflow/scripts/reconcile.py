#!/usr/bin/env python3.12
"""Bounded, deterministic reconciliation of explicitly managed Devflow issues."""

import argparse
from contextlib import closing
from datetime import datetime, timedelta, timezone
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
from urllib.parse import urlsplit

sys.dont_write_bytecode = True
import github
import state


def intent(db, work_id):
    found = db.execute("SELECT * FROM reconcile_intents WHERE work_id=?", (work_id,)).fetchone()
    return dict(found) if found else None


def queue(db, work_id, kind, payload, owner=None, next_action=None):
    """Call within a short write transaction, before any requested remote mutation."""
    work = state.row(db, "works", work_id)
    if not work or not work["issue"]:
        raise ValueError("reconciliation needs a linked issue")
    tracking = github.details(work).get("github", {})
    if not tracking.get("project"):
        raise ValueError("reconciliation needs an explicit managed Project")
    claim = state.claim_for(db, work_id)
    if owner and (not claim or claim["owner"] != owner):
        raise ValueError("work ownership mismatch")
    token = claim["claimed_at"] if claim else None
    prior = intent(db, work_id)
    encoded = state.encode(payload)
    if prior and (prior["kind"], prior["owner"], prior["claim_token"], prior["payload"]) == (
            kind, owner, token, encoded) and prior["state"] in {"pending", "acknowledged", "needs_decision"}:
        return prior
    stamp = state.now()
    revision = (prior["revision"] + 1) if prior else 1
    db.execute("""INSERT INTO reconcile_intents
        (work_id,revision,kind,owner,claim_token,payload,state,attempts,last_error,
         next_attempt_at,next_action,created_at,updated_at,acknowledged_at)
        VALUES (?,?,?,?,?,?,'pending',0,NULL,?,?,?,?,NULL)
        ON CONFLICT(work_id) DO UPDATE SET revision=excluded.revision,kind=excluded.kind,
        owner=excluded.owner,claim_token=excluded.claim_token,payload=excluded.payload,
        state='pending',attempts=0,last_error=NULL,next_attempt_at=excluded.next_attempt_at,
        next_action=excluded.next_action,updated_at=excluded.updated_at,acknowledged_at=NULL""",
        (work_id, revision, kind, owner, token, encoded, stamp, next_action, stamp, stamp))
    return intent(db, work_id)


def current(db, saved):
    latest = intent(db, saved["work_id"])
    if not latest or latest["revision"] != saved["revision"] or latest["state"] != "pending":
        return False
    claim = state.claim_for(db, saved["work_id"])
    if saved["owner"]:
        return bool(claim and claim["owner"] == saved["owner"] and claim["claimed_at"] == saved["claim_token"])
    return claim is None


def fence(db, saved):
    if not current(db, saved):
        raise ValueError("reconciliation intent superseded by owner or revision")


def finish(db, saved, status, error=None, next_action=None, retry_at=None):
    with db:
        db.execute("BEGIN IMMEDIATE")
        if not current(db, saved):
            return False
        stamp = state.now()
        db.execute("""UPDATE reconcile_intents SET state=?,attempts=attempts+1,last_error=?,
            next_attempt_at=?,next_action=?,updated_at=?,acknowledged_at=?
            WHERE work_id=? AND revision=?""",
            (status, error, retry_at, next_action, stamp, stamp if status == "acknowledged" else None,
             saved["work_id"], saved["revision"]))
        return True


def retry(db, saved, exc):
    attempts = saved["attempts"] + 1
    seconds = min(3600, 15 * (2 ** min(attempts - 1, 8)))
    at = (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()
    finish(db, saved, "pending", str(exc), "retry GitHub synchronization", at)
    return {"work_id": saved["work_id"], "state": "pending", "error": str(exc), "next_attempt_at": at}


def descendants_terminal(db, root):
    """A closed root is insufficient while any bound descendant remains open/unknown."""
    rows = db.execute("""WITH RECURSIVE tree(id,closed_at) AS (
        SELECT id,closed_at FROM runtime_sessions WHERE id=?
        UNION ALL SELECT child.id,child.closed_at FROM runtime_sessions child JOIN tree ON child.parent_id=tree.id
    ) SELECT id,closed_at FROM tree""", (root,)).fetchall()
    return bool(rows and rows[0]["closed_at"] and all(row["closed_at"] for row in rows))


def apply_sync(db, saved):
    payload = json.loads(saved["payload"])
    work = state.row(db, "works", saved["work_id"])
    if not work or work["issue"] != payload["issue"]:
        raise ValueError("linked issue changed; choose a new intent")
    if saved["kind"] == "owner_stop" and not descendants_terminal(db, saved["owner"]):
        finish(db, saved, "pending", "owner or descendant has no terminal observation",
               "wait for authoritative terminal owner and descendants",
               (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat())
        return {"work_id": saved["work_id"], "state": "pending", "reason": "owner_or_descendant_active_or_unknown"}
    selected = github.project(payload["project"], payload["project_status"])
    observed = github.view(payload["issue"])
    if observed["state"] == "CLOSED" and payload["status"] != "done":
        finish(db, saved, "needs_decision", "issue closed during nonterminal transition",
               "inspect the closed issue and record a new semantic event")
        return {"work_id": saved["work_id"], "state": "needs_decision", "reason": "issue_closed"}
    if observed["state"] == "OPEN" and payload["status"] == "done":
        finish(db, saved, "needs_decision", "done requires an already closed issue",
               "close the issue within authorized acceptance scope or record a new status")
        return {"work_id": saved["work_id"], "state": "needs_decision", "reason": "issue_open"}
    login = payload["assignee"]
    if login == "@me":
        login = github.gh("api", "--hostname", selected["host"], "user", "--jq", ".login")
    tracking = github.details(work)["github"]
    sync = tracking.get("sync") or {}
    item_id = sync.get("item_id") if sync.get("project_id") == selected["id"] else None
    item = github.project_item(selected["host"], item_id) if item_id else None
    if not item:
        item = github.legacy_project_item(selected["url"], observed["id"])
        item_id = item["id"] if item else None
    if not item_id:
        fence(db, saved)
        added = github.graphql(selected["host"], """mutation($project:ID!,$issue:ID!){
            addProjectV2ItemById(input:{projectId:$project,contentId:$issue}){item{id}}}""",
            project=selected["id"], issue=observed["id"])
        item_id = added["addProjectV2ItemById"]["item"]["id"]
        item = github.project_item(selected["host"], item_id)
    if login.casefold() not in {a["login"].casefold() for a in observed["assignees"]}:
        fence(db, saved)
        github.gh("issue", "edit", payload["issue"], "--add-assignee", login)
    field = (item or {}).get("fieldValueByName") or {}
    if field.get("optionId") != selected["option"] or field.get("name") != selected["status"]:
        fence(db, saved)
        github.graphql(selected["host"], """mutation($project:ID!,$item:ID!,$field:ID!,$option:String!){
            updateProjectV2ItemFieldValue(input:{projectId:$project,itemId:$item,fieldId:$field,
            value:{singleSelectOptionId:$option}}){projectV2Item{id}}}""",
            project=selected["id"], item=item_id, field=selected["field"], option=selected["option"])
    readback = github.project_item(selected["host"], item_id)
    verified = github.view(payload["issue"])
    if (not readback or (readback.get("project") or {}).get("id") != selected["id"]
            or (readback.get("fieldValueByName") or {}).get("optionId") != selected["option"]
            or (readback.get("fieldValueByName") or {}).get("name") != selected["status"]
            or login.casefold() not in {a["login"].casefold() for a in verified["assignees"]}
            or verified["state"] != observed["state"]):
        raise RuntimeError("GitHub readback disagrees with requested issue, assignee or Project Status")
    with db:
        db.execute("BEGIN IMMEDIATE")
        fence(db, saved)
        work = state.row(db, "works", saved["work_id"])
        details = github.details(work)
        tracking = details.setdefault("github", {})
        tracking["sync"] = dict(status=payload["status"], issue_state=verified["state"], assignee=login,
                                project=selected["url"], project_id=selected["id"], item_id=item_id,
                                field_id=selected["field"], option_id=selected["option"],
                                project_status=selected["status"], readback_at=state.now())
        if payload.get("await_url"):
            tracking["await"] = dict(url=payload["await_url"], follow_up=payload["follow_up"])
        elif payload["status"] != "blocked":
            tracking.pop("await", None)
        state.update(db, "work", dict(id=saved["work_id"], details=details), None)
        local_status, stage, _ = github.STATUSES[payload["status"]]
        if payload.get("release") and local_status == "active":
            local_status = "waiting"
        values = dict(id=saved["work_id"], status=local_status,
                      blocker=payload.get("reason") if local_status == "blocked" else None)
        if stage:
            values["stage"] = stage
        state.update(db, "work", values, None)
        if saved["owner"]:
            state.require_owner(db, saved["work_id"], saved["owner"])
        state.record(db, "result", dict(id="reconcile:" + saved["work_id"] + ":" + str(saved["revision"]),
                     work_id=saved["work_id"], kind="github", status="synchronized",
                     evidence_ref=payload["issue"], summary=selected["url"] + ": " + selected["status"] + "; assigned to " + login))
        db.execute("""UPDATE reconcile_intents SET state='acknowledged',attempts=attempts+1,
            last_error=NULL,next_attempt_at=NULL,next_action=NULL,updated_at=?,acknowledged_at=?
            WHERE work_id=? AND revision=?""", (state.now(), state.now(), saved["work_id"], saved["revision"]))
        if payload.get("release") or payload["status"] in {"paused", "done"} or saved["kind"] == "owner_stop":
            if saved["owner"]:
                state.release_work(db, saved["work_id"], saved["owner"])
    return {"work_id": saved["work_id"], "state": "acknowledged", "issue": payload["issue"],
            "status": payload["status"], "project_status": selected["status"], "assignee": login}


def apply_one(db, saved):
    if not current(db, saved):
        return {"work_id": saved["work_id"], "state": "superseded"}
    try:
        return apply_sync(db, saved)
    except ValueError as exc:
        if "superseded" in str(exc):
            return {"work_id": saved["work_id"], "state": "superseded"}
        finish(db, saved, "needs_decision", str(exc), "resolve mapping or semantic decision")
        return {"work_id": saved["work_id"], "state": "needs_decision", "error": str(exc)}
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        return retry(db, saved, exc)


def external_outcome(db, work, tracking):
    awaited = tracking.get("await")
    if not isinstance(awaited, dict) or not awaited.get("url"):
        return None
    url = github.action_url(awaited["url"], urlsplit(work["issue"]).netloc)
    parts = urlsplit(url).path.strip("/").split("/")
    run = github.gh("run", "view", parts[-1], "--repo", f"{urlsplit(url).netloc}/{parts[0]}/{parts[1]}",
                    "--json", "status,conclusion,url", as_json=True)
    if run["status"] != "completed":
        return None
    outcome = {"url": url, "status": run["status"], "conclusion": run.get("conclusion"),
               "follow_up": awaited.get("follow_up")}
    with db:
        db.execute("BEGIN IMMEDIATE")
        fresh = state.row(db, "works", work["id"])
        details = github.details(fresh)
        current_await = details.get("github", {}).get("await")
        if not isinstance(current_await, dict) or current_await.get("url") != url:
            return None
        if details["github"].get("external_outcome") == outcome:
            return None
        details["github"].pop("await", None)
        details["github"]["external_outcome"] = outcome
        state.update(db, "work", dict(id=work["id"], details=details, status="waiting", blocker=None), None)
        next_action = ("review completed external run and record semantic acceptance or repair" if
                       run.get("conclusion") == "success" else "inspect failed external run and record repair decision")
        queue(db, work["id"], "external_outcome", {"issue": work["issue"], "outcome": outcome},
              next_action=next_action)
        db.execute("UPDATE reconcile_intents SET state='needs_decision' WHERE work_id=?", (work["id"],))
    return {"work_id": work["id"], "state": "needs_decision", "external_outcome": outcome,
            "next_action": next_action}


def discover(db, limit):
    """Read only opted-in managed work; never infer acceptance from CI or a PR."""
    results = []
    rows = db.execute("SELECT * FROM works WHERE issue IS NOT NULL ORDER BY id LIMIT ?", (limit,)).fetchall()
    for item in rows:
        work = dict(item)
        tracking = github.details(work).get("github", {})
        sync = tracking.get("sync")
        if not tracking.get("project") or not isinstance(sync, dict) or not sync.get("project_status"):
            continue
        if intent(db, work["id"]) and intent(db, work["id"])["state"] in {"pending", "needs_decision"}:
            continue
        try:
            outcome = external_outcome(db, work, tracking)
            if outcome:
                results.append(outcome)
                continue
            observed = github.view(work["issue"])
            if observed["state"] == "CLOSED" and work["status"] != "done":
                status = "done"
            elif observed["state"] == "OPEN" and work["status"] == "done":
                with db:
                    db.execute("BEGIN IMMEDIATE")
                    queue(db, work["id"], "unknown", {"issue": work["issue"]},
                          next_action="inspect reopened issue and record new semantic status")
                    db.execute("UPDATE reconcile_intents SET state='needs_decision' WHERE work_id=?", (work["id"],))
                results.append({"work_id": work["id"], "state": "needs_decision", "reason": "done_issue_open"})
                continue
            else:
                status = sync["status"]
            if status not in github.STATUSES:
                continue
            chosen = tracking.get("statuses", {}).get(status) or github.STATUSES[status][2]
            selected = github.project(tracking["project"], chosen)
            item_id = sync.get("item_id")
            project_item = github.project_item(selected["host"], item_id) if item_id else None
            field = (project_item or {}).get("fieldValueByName") or {}
            mismatch = (status != sync["status"] or observed["state"] != sync["issue_state"]
                        or sync["assignee"].casefold() not in {a["login"].casefold() for a in observed["assignees"]}
                        or (project_item or {}).get("project", {}).get("id") != selected["id"]
                        or field.get("optionId") != selected["option"] or field.get("name") != selected["status"])
            if mismatch:
                payload = dict(issue=work["issue"], status=status, project=tracking["project"],
                               project_status=chosen, assignee=sync["assignee"], reason=None,
                               release=status == "done", await_url=None, follow_up=None)
                with db:
                    db.execute("BEGIN IMMEDIATE")
                    queue(db, work["id"], "sync", payload, owner=(state.claim_for(db, work["id"]) or {}).get("owner"))
                results.append({"work_id": work["id"], "state": "pending", "reason": "remote_drift"})
        except (OSError, RuntimeError, ValueError) as exc:
            results.append({"work_id": work["id"], "state": "unknown", "error": str(exc)})
    return results


def once(db, limit=20, dry_run=False):
    if dry_run:
        return [{"work_id": r["id"], "issue": r["issue"], "status": r["status"],
                 "project": github.details(dict(r)).get("github", {}).get("project"),
                 "intent": (intent(db, r["id"]) or {}).get("state")}
                for r in db.execute("SELECT * FROM works WHERE issue IS NOT NULL ORDER BY id LIMIT ?", (limit,))
                if github.details(dict(r)).get("github", {}).get("project")]
    results = discover(db, limit)
    due = db.execute("""SELECT * FROM reconcile_intents WHERE state='pending'
        AND (next_attempt_at IS NULL OR next_attempt_at<=?) ORDER BY updated_at,work_id LIMIT ?""",
        (state.now(), limit)).fetchall()
    for item in due:
        results.append(apply_one(db, dict(item)))
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(Path(os.environ.get("XDG_STATE_HOME",
                        str(Path.home() / ".local/state"))) / "devflow/workflow.sqlite3"))
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("once", "daemon"):
        command = commands.add_parser(name)
        command.add_argument("--limit", type=int, default=20)
        command.add_argument("--dry-run", action="store_true")
        if name == "daemon":
            command.add_argument("--interval", type=int, default=60)
    args = parser.parse_args()
    if args.limit < 1 or args.limit > 100 or (args.command == "daemon" and args.interval < 15):
        parser.error("limit must be 1..100 and daemon interval at least 15 seconds")
    path = Path(args.db).expanduser().resolve()
    if not path.exists():
        print(state.encode({"state": "no_database", "db": str(path)}))
        return 0
    lock = path.with_suffix(path.suffix + ".reconcile.lock")
    fd = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(fd, "w") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(state.encode({"state": "already_running", "db": str(path)}))
            return 0
        while True:
            with closing(state.connect(path)) as db:
                result = once(db, args.limit, args.dry_run)
            print(state.encode({"state": "completed", "actions": result}), flush=True)
            if args.command == "once":
                return 0
            time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
