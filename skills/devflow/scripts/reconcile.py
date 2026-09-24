#!/usr/bin/env python3.12
"""Bounded, deterministic reconciliation of explicitly managed Devflow issues."""

import argparse
from contextlib import closing
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from urllib.parse import urlsplit

sys.dont_write_bytecode = True
import github
import state

SYNC_KEYS = ("status", "issue_state", "assignee", "project", "project_id",
             "item_id", "field_id", "option_id", "project_status", "readback_at")


def managed(tracking):
    sync = tracking.get("sync")
    return (bool(tracking.get("project")) and isinstance(sync, dict)
            and all(sync.get(key) for key in SYNC_KEYS) and sync["status"] in github.STATUSES)


def intent(db, work_id):
    found = db.execute("SELECT * FROM reconcile_intents WHERE work_id=?", (work_id,)).fetchone()
    return dict(found) if found else None


def owner_token(db, claim):
    if not claim:
        return None
    session = db.execute("SELECT generation FROM runtime_sessions WHERE id=?", (claim["owner"],)).fetchone()
    return state.encode([claim["claimed_at"], session["generation"] if session else None])


def queue(db, work_id, kind, payload, owner=None, next_action=None, force=False):
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
    token = owner_token(db, claim)
    prior = intent(db, work_id)
    encoded = state.encode(payload)
    # A human repeating the same explicit request after fixing a missing
    # mapping must create a fresh revision; needs_decision is not executable.
    reusable = {"pending"}
    if kind != "probe":
        reusable.add("acknowledged")
    if not force and prior and (prior["kind"], prior["owner"], prior["claim_token"], prior["payload"]) == (
            kind, owner, token, encoded) and prior["state"] in reusable:
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
        return bool(claim and claim["owner"] == saved["owner"] and owner_token(db, claim) == saved["claim_token"])
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
    if saved["kind"] in {"owner_stop", "closed_convergence", "external_transition"} and saved["owner"] and not descendants_terminal(db, saved["owner"]):
        finish(db, saved, "pending", "owner or descendant has no terminal observation",
               "wait for authoritative terminal owner and descendants",
               (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat())
        return {"work_id": saved["work_id"], "state": "pending", "reason": "owner_or_descendant_active_or_unknown"}
    observed = github.view(payload["issue"])
    if observed["state"] == "CLOSED" and payload["status"] != "done":
        if saved["kind"] in {"owner_stop", "external_transition"}:
            tracking = github.details(work)["github"]
            replacement = dict(payload, status="done",
                               project_status=tracking.get("statuses", {}).get("done") or github.STATUSES["done"][2],
                               reason=None, release=True)
            with db:
                db.execute("BEGIN IMMEDIATE")
                fence(db, saved)
                newer = queue(db, saved["work_id"], "closed_convergence", replacement,
                              owner=saved["owner"], force=True)
            return {"work_id": saved["work_id"], "state": "pending",
                    "reason": "closed_issue_superseded_stop", "revision": newer["revision"]}
        finish(db, saved, "needs_decision", "issue closed during nonterminal transition",
               "inspect the closed issue and record a new semantic event")
        return {"work_id": saved["work_id"], "state": "needs_decision", "reason": "issue_closed"}
    if observed["state"] == "OPEN" and payload["status"] == "done":
        finish(db, saved, "needs_decision", "done requires an already closed issue",
               "close the issue within authorized acceptance scope or record a new status")
        return {"work_id": saved["work_id"], "state": "needs_decision", "reason": "issue_open"}
    # Resolve only the final desired option. A terminal owner stop may have
    # queued Blocked before an external actor closed the issue; Blocked can be
    # absent even though configured Done is valid for that closed issue.
    selected = github.project(payload["project"], payload["project_status"])
    if urlsplit(payload["issue"]).netloc.casefold() != selected["host"].casefold():
        raise ValueError("issue and Project must belong to the same GitHub host")
    login = payload["assignee"]
    if login == "@me":
        login = github.gh("api", "--hostname", selected["host"], "user", "--jq", ".login")
    tracking = github.details(work)["github"]
    sync = tracking.get("sync") or {}
    item_id = sync.get("item_id") if sync.get("project_id") == selected["id"] else None
    item = github.project_item(selected["host"], item_id) if item_id else None
    if item and (item.get("project") or {}).get("id") != selected["id"]:
        item = None
        item_id = None
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
        if saved["kind"] == "sync" and isinstance(tracking.get("external_outcome"), dict):
            tracking["external_outcome"]["resolved_at"] = state.now()
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
        decision = saved["kind"] == "external_transition"
        stamp = state.now()
        db.execute("""UPDATE reconcile_intents SET state=?,attempts=attempts+1,
            last_error=NULL,next_attempt_at=NULL,next_action=?,updated_at=?,acknowledged_at=?
            WHERE work_id=? AND revision=?""",
            ("needs_decision" if decision else "acknowledged",
             payload.get("next_action") if decision else None, stamp, stamp,
             saved["work_id"], saved["revision"]))
        if payload.get("release") or payload["status"] in {"paused", "done"} or saved["kind"] == "owner_stop":
            if saved["owner"]:
                state.release_work(db, saved["work_id"], saved["owner"])
    return {"work_id": saved["work_id"], "state": "needs_decision" if decision else "acknowledged", "issue": payload["issue"],
            "status": payload["status"], "project_status": selected["status"], "assignee": login}


def apply_one(db, saved):
    lock_name = hashlib.sha256(saved["work_id"].encode()).hexdigest()[:24]
    filename = db.execute("PRAGMA database_list").fetchone()[2]
    lock = Path(filename).with_name(".reconcile-" + lock_name + ".lock")
    fd = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(fd, "w") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        return _apply_one(db, saved)


def _apply_one(db, saved):
    if not current(db, saved):
        return {"work_id": saved["work_id"], "state": "superseded"}
    try:
        if saved["kind"] == "probe":
            payload = json.loads(saved["payload"])
            github.view(payload["issue"])
            github.project(payload["project"], payload["project_status"])
            if payload.get("await_url"):
                url = urlsplit(payload["await_url"])
                parts = url.path.strip("/").split("/")
                github.gh("run", "view", parts[-1], "--repo", f"{url.netloc}/{parts[0]}/{parts[1]}",
                          "--json", "status,conclusion,url", as_json=True)
            finish(db, saved, "acknowledged")
            return {"work_id": saved["work_id"], "state": "acknowledged", "kind": "probe"}
        return apply_sync(db, saved)
    except ValueError as exc:
        if "superseded" in str(exc):
            return {"work_id": saved["work_id"], "state": "superseded"}
        finish(db, saved, "needs_decision", str(exc), "resolve mapping or semantic decision")
        return {"work_id": saved["work_id"], "state": "needs_decision", "error": str(exc)}
    except (OSError, RuntimeError, sqlite3.Error, subprocess.TimeoutExpired) as exc:
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
    succeeded = run.get("conclusion") == "success"
    next_action = ("review completed external run and record semantic acceptance or repair" if succeeded
                   else "inspect failed external run and record repair decision")
    outcome["next_action"] = next_action
    issue_state = github.view(work["issue"])["state"]
    if issue_state not in {"OPEN", "CLOSED"}:
        raise ValueError("unknown issue state for external outcome")
    target = "done" if issue_state == "CLOSED" else "in-review" if succeeded else "blocked"
    chosen = tracking.get("statuses", {}).get(target) or github.STATUSES[target][2]
    reason = None if succeeded else "External Actions run completed with " + str(run.get("conclusion") or "unknown conclusion")
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
        # Replace the stale waiting reason immediately. Project movement is a
        # separate queued, verified operation and cannot imply acceptance.
        state.update(db, "work", dict(id=work["id"], details=details, status="blocked",
                                      blocker="External run completed; tracker transition pending"), None)
        queue(db, work["id"], "external_transition", dict(
            issue=work["issue"], status=target, project=tracking["project"],
            project_status=chosen, assignee=tracking["sync"]["assignee"],
            reason=reason, release=True, await_url=None, follow_up=None,
            next_action=next_action), owner=(state.claim_for(db, work["id"]) or {}).get("owner"),
            next_action=next_action)
    return {"work_id": work["id"], "state": "pending", "external_outcome": outcome,
            "next_action": next_action}


def discover(db, limit):
    """Read only opted-in managed work; never infer acceptance from CI or a PR."""
    results = []
    rows = []
    for item in db.execute("SELECT * FROM works WHERE issue IS NOT NULL ORDER BY id"):
        work = dict(item)
        tracking = github.details(work).get("github", {})
        if managed(tracking):
            rows.append(work)
    cursor = db.execute("SELECT last_work_id FROM reconcile_cursor WHERE id=1").fetchone()[0]
    start = next((index for index, work in enumerate(rows) if work["id"] > (cursor or "")), 0)
    selected_rows = (rows[start:] + rows[:start])[:limit]
    if len(rows) > limit and selected_rows:
        with db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE reconcile_cursor SET last_work_id=? WHERE id=1", (selected_rows[-1]["id"],))
    for work in selected_rows:
        tracking = github.details(work).get("github", {})
        sync = tracking.get("sync")
        pending = intent(db, work["id"])
        if pending and pending["state"] == "pending" and pending["next_attempt_at"] and pending["next_attempt_at"] > state.now() and pending["last_error"]:
            continue
        # An external run can finish while an earlier sync is pending or
        # awaiting a decision. Observe that terminal fact before gating drift.
        if tracking.get("await"):
            try:
                outcome = external_outcome(db, work, tracking)
                if outcome:
                    results.append(outcome)
                    continue
            except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
                with db:
                    db.execute("BEGIN IMMEDIATE")
                    if pending and pending["state"] == "pending":
                        seconds = min(3600, 15 * (2 ** min(pending["attempts"], 8)))
                        at = (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()
                        db.execute("""UPDATE reconcile_intents SET attempts=attempts+1,last_error=?,
                            next_attempt_at=?,next_action='retry external run read',updated_at=?
                            WHERE work_id=? AND revision=?""",
                            (str(exc), at, state.now(), work["id"], pending["revision"]))
                    elif not pending or pending["state"] == "acknowledged":
                        chosen = tracking.get("statuses", {}).get(sync.get("status")) or sync["project_status"]
                        queue(db, work["id"], "probe", dict(issue=work["issue"],
                              project=tracking["project"], project_status=chosen,
                              await_url=tracking["await"]["url"]),
                              owner=(state.claim_for(db, work["id"]) or {}).get("owner"),
                              next_action="retry external run read")
                results.append({"work_id": work["id"], "state": "unknown", "error": str(exc)})
                continue
        if pending and pending["state"] == "pending" and not current(db, pending):
            with db:
                db.execute("BEGIN IMMEDIATE")
                db.execute("""UPDATE reconcile_intents SET state='needs_decision',
                    last_error='owner or lifecycle generation changed',
                    next_action='record current owner decision before synchronizing',updated_at=?
                    WHERE work_id=? AND revision=? AND state='pending'""",
                    (state.now(), work["id"], pending["revision"]))
            results.append({"work_id": work["id"], "state": "needs_decision", "reason": "stale_owner_generation"})
            continue
        if pending and pending["state"] in {"pending", "needs_decision"}:
            continue
        try:
            observed = github.view(work["issue"])
            if observed["state"] == "CLOSED" and work["status"] != "done":
                claim = state.claim_for(db, work["id"])
                if claim and not descendants_terminal(db, claim["owner"]):
                    with db:
                        db.execute("BEGIN IMMEDIATE")
                        queue(db, work["id"], "unknown", {"issue": work["issue"]},
                              owner=claim["owner"], next_action="live owner must confirm closed issue")
                        db.execute("UPDATE reconcile_intents SET state='needs_decision' WHERE work_id=?", (work["id"],))
                    results.append({"work_id": work["id"], "state": "needs_decision", "reason": "live_claim_on_closed_issue"})
                    continue
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
                    queue(db, work["id"], "closed_convergence" if status == "done" else "sync",
                          payload, owner=(state.claim_for(db, work["id"]) or {}).get("owner"), force=True)
                results.append({"work_id": work["id"], "state": "pending", "reason": "remote_drift"})
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
            chosen = tracking.get("statuses", {}).get(sync.get("status")) or github.STATUSES.get(sync.get("status"), (None, None, None))[2]
            if chosen:
                with db:
                    db.execute("BEGIN IMMEDIATE")
                    queue(db, work["id"], "probe", dict(issue=work["issue"],
                          project=tracking["project"], project_status=chosen),
                          owner=(state.claim_for(db, work["id"]) or {}).get("owner"),
                          next_action="retry remote read or resolve mapping")
                    if isinstance(exc, ValueError):
                        db.execute("UPDATE reconcile_intents SET state='needs_decision',last_error=? WHERE work_id=?",
                                   (str(exc), work["id"]))
            results.append({"work_id": work["id"], "state": "unknown", "error": str(exc)})
    return results


def preview_pending(db, queued):
    payload = json.loads(queued["payload"])
    summary = {"kind": queued["kind"], "revision": queued["revision"],
               "state": queued["state"], "next_attempt_at": queued["next_attempt_at"],
               "next_action": queued["next_action"]}
    if queued["state"] != "pending" or not current(db, queued):
        return summary, [], []
    if queued["kind"] in {"probe", "unknown"}:
        return summary, [], []
    if queued["kind"] in {"owner_stop", "closed_convergence", "external_transition"} and queued["owner"]:
        if not descendants_terminal(db, queued["owner"]):
            summary["next_action"] = "wait for terminal root and descendants"
            return summary, [], []
    observed = github.view(payload["issue"])
    if observed["state"] == "CLOSED" and payload["status"] != "done":
        if queued["kind"] in {"owner_stop", "external_transition"}:
            tracking = github.details(state.row(db, "works", queued["work_id"]))["github"]
            payload = dict(payload, status="done",
                           project_status=tracking.get("statuses", {}).get("done") or github.STATUSES["done"][2],
                           release=True)
            summary["next_action"] = "supersede terminal stop with closed-issue convergence"
        else:
            summary["next_action"] = "resolve closed issue before nonterminal transition"
            return summary, [], []
    if observed["state"] == "OPEN" and payload["status"] == "done":
        summary["next_action"] = "resolve open issue before Done transition"
        return summary, [], []
    selected = github.project(payload["project"], payload["project_status"])
    login = payload["assignee"]
    if login == "@me":
        login = github.gh("api", "--hostname", selected["host"], "user", "--jq", ".login")
    work = state.row(db, "works", queued["work_id"])
    sync = github.details(work).get("github", {}).get("sync") or {}
    item_id = sync.get("item_id") if sync.get("project_id") == selected["id"] else None
    item = github.project_item(selected["host"], item_id) if item_id else None
    if not item or (item.get("project") or {}).get("id") != selected["id"]:
        item = github.legacy_project_item(selected["url"], observed["id"])
    writes = []
    if not item:
        writes.append("add Project item")
    if login.casefold() not in {a["login"].casefold() for a in observed["assignees"]}:
        writes.append("assignee")
    field = (item or {}).get("fieldValueByName") or {}
    if field.get("optionId") != selected["option"] or field.get("name") != selected["status"]:
        writes.append("Project Status")
    local = ["update local work after readback"]
    if queued["owner"] and (payload.get("release") or payload["status"] in {"paused", "done"}
                            or queued["kind"] == "owner_stop"):
        local.append("release terminal owner claim after readback")
    summary["desired_status"] = payload["status"]
    summary["desired_project_status"] = selected["status"]
    return summary, writes, local


def once(db, limit=20, dry_run=False):
    if dry_run:
        rows = []
        total = 0
        has_queue = bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reconcile_intents'").fetchone())
        has_claims = bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='claims'").fetchone())
        for raw in db.execute("SELECT * FROM works WHERE issue IS NOT NULL ORDER BY id"):
            work = dict(raw)
            tracking = github.details(work).get("github", {})
            if not tracking.get("project"):
                continue
            total += 1
            if len(rows) >= limit:
                continue
            sync = tracking.get("sync")
            mappings = tracking.get("statuses", {})
            eligible = managed(tracking)
            queued = intent(db, work["id"]) if has_queue else None
            first_sync = bool(not eligible and queued and queued["kind"] == "sync"
                              and queued["state"] in {"pending", "needs_decision"})
            first_sync_decision = bool(first_sync and queued["state"] == "needs_decision")
            preview = {"work_id": work["id"], "issue": work["issue"], "local_status": work["status"],
                       "project": tracking["project"],
                       "eligibility": "managed" if eligible else
                                      "first_sync_needs_decision" if first_sync_decision else
                                      "pending_first_sync" if first_sync else "legacy_needs_explicit_mapping",
                       "missing_mappings": [name for name in ("blocked", "paused", "done") if name not in mappings],
                       "local_claim_owner": (state.claim_for(db, work["id"]) or {}).get("owner") if has_claims else None,
                       "intent": (queued or {}).get("state"),
                       "possible_remote_writes": []}
            if eligible:
                try:
                    audit = github.audit(db, work["id"])
                    preview["audit_state"] = audit["state"]
                    reasons = audit["reconciliation_required"]
                    preview["possible_remote_writes"] = [
                        name for name, cause in (("assignee", "assignee_mismatch"),
                                                 ("Project Status", "project_status_mismatch")) if cause in reasons]
                    if "unfinished_issue_closed" in reasons:
                        preview["possible_remote_writes"].append("Project Done if configured; local completion")
                    if audit.get("awaited_run", {}).get("status") == "completed":
                        preview["next_action"] = "record external outcome; review semantic acceptance"
                except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
                    preview["audit_state"] = "unknown"
                    preview["error"] = str(exc)
            else:
                preview["audit_state"] = "unknown"
                preview["next_action"] = (queued["next_action"] or "resolve mapping or semantic decision"
                                          if first_sync_decision else
                                          "drain explicit first synchronization after readback" if first_sync else
                                          "supply and verify explicit mapping before opt-in")
                if first_sync_decision:
                    preview["last_error"] = queued["last_error"]
                try:
                    observed = github.view(work["issue"])
                    item = github.legacy_project_item(tracking["project"], observed["id"])
                    preview["remote_observed"] = {
                        "issue_state": observed["state"],
                        "assignees": [person["login"] for person in observed["assignees"]],
                        "project_status": ((item or {}).get("fieldValueByName") or {}).get("name"),
                        "project_item_found": bool(item)}
                except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
                    preview["remote_observed"] = {"state": "unknown", "error": str(exc)}
            if queued:
                try:
                    summary, writes, local = preview_pending(db, queued)
                    preview["pending_intent"] = summary
                    if queued["state"] in {"pending", "needs_decision"}:
                        preview["possible_remote_writes"] = writes
                    preview["possible_local_actions"] = local
                except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
                    preview["pending_intent"] = {"kind": queued["kind"], "state": queued["state"],
                                                 "error": str(exc), "next_action": "retry remote readback"}
            rows.append(preview)
        return {"records": rows, "coverage": {"total_project_records": total,
                "returned": len(rows), "truncated": total > len(rows),
                "next_action": "rerun with a larger --limit (maximum 100)" if total > len(rows) else None}}
    results = discover(db, limit)
    due = db.execute("""SELECT * FROM reconcile_intents WHERE state='pending'
        AND (next_attempt_at IS NULL OR next_attempt_at<=?) ORDER BY updated_at,work_id LIMIT ?""",
        (state.now(), limit)).fetchall()
    for item in due:
        results.append(apply_one(db, dict(item)))
    return results


def installation_ready(args):
    if not args.activation_token:
        return True
    try:
        marker = json.loads(Path(args.activation_marker).read_text())
        manifest = json.loads(Path(args.manifest).read_text())
        source = Path(__file__).resolve().parents[3]
        if marker.get("token") != args.activation_token or Path(manifest["source"]).resolve() != source:
            return False
        head = subprocess.check_output([manifest["git"], "-C", str(source), "rev-parse", "HEAD"],
                                       text=True, timeout=10).strip()
        return (manifest.get("head") == head and
                manifest.get("files", {}).get("skills/devflow/scripts/reconcile.py") ==
                hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False


def acquire_lock(path, create_parent=False):
    if create_parent:
        path.parent.mkdir(parents=True, exist_ok=True)
    lock = path.with_suffix(path.suffix + ".reconcile.lock")
    fd = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
    handle = os.fdopen(fd, "w")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    return handle


def perform_once(path, limit):
    with closing(state.connect(path)) as db:
        return {"state": "completed", "actions": once(db, limit)}


def run_once_locked(path, limit):
    handle = acquire_lock(path)
    if handle is None:
        return {"state": "already_running", "db": str(path)}
    with handle:
        return perform_once(path, limit)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(Path(os.environ.get("XDG_STATE_HOME",
                        str(Path.home() / ".local/state"))) / "devflow/workflow.sqlite3"))
    parser.add_argument("--manifest")
    parser.add_argument("--activation-marker")
    parser.add_argument("--activation-token")
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
    if any((args.manifest, args.activation_marker, args.activation_token)) and not all(
            (args.manifest, args.activation_marker, args.activation_token)):
        parser.error("manifest, activation marker and token are required together")
    path = Path(args.db).expanduser().resolve()
    if args.dry_run:
        if not path.exists():
            print(state.encode({"state": "no_database", "db": str(path)}))
            return 0
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
            db.row_factory = sqlite3.Row
            result = once(db, args.limit, True)
        print(state.encode({"state": "preview", **result}), flush=True)
        return 0
    if args.command == "once":
        result = run_once_locked(path, args.limit) if path.exists() else {"state": "no_database", "db": str(path)}
        print(state.encode(result), flush=True)
        return 0
    last_wait = None
    handle = None
    try:
        while True:
            if not installation_ready(args):
                reason = "waiting_for_activation"
            else:
                if handle is None:
                    # Hold the same flock for the entire daemon lifetime,
                    # including sleep, so two processes cannot alternate passes.
                    handle = acquire_lock(path, create_parent=True)
                    if handle is None:
                        print(state.encode({"state": "already_running", "db": str(path)}), flush=True)
                        return 0
                reason = "no_database" if not path.exists() else None
            if reason:
                if reason != last_wait:
                    print(state.encode({"state": reason, "db": str(path)}), flush=True)
                last_wait = reason
            else:
                last_wait = None
                print(state.encode(perform_once(path, args.limit)), flush=True)
            time.sleep(args.interval)
    finally:
        if handle is not None:
            handle.close()


if __name__ == "__main__":
    raise SystemExit(main())
