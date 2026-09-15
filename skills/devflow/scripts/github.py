#!/usr/bin/env python3
"""Assign and label a GitHub issue for its local owning task through gh."""

import argparse
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import uuid
from urllib.parse import urlsplit

sys.dont_write_bytecode = True
import state


STATUSES = {
    "in-progress": ("active", "implementation", "0969da"),
    "in-review": ("active", "review", "8250df"),
    "blocked": ("blocked", None, "d1242f"),
    "paused": ("paused", None, "bf8700"),
    "done": ("done", "delivered", "1a7f37"),
}


def gh(*args, as_json=False):
    result = subprocess.run(["gh", *args], text=True, capture_output=True, timeout=60)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "gh failed")
    return json.loads(result.stdout) if as_json else result.stdout.strip()


def view(issue):
    return gh("issue", "view", issue, "--json", "url,title,state,assignees,labels", as_json=True)


def status_labels(issue):
    return {item["name"] for item in issue["labels"] if item["name"].casefold().startswith("status:")}


def ensure_label(host, repository, label, color):
    def names():
        pages = gh("api", "--hostname", host, "--paginate", "--slurp",
                   f"repos/{repository}/labels?per_page=100", as_json=True)
        return {item["name"].casefold() for page in pages for item in page}
    if label.casefold() in names():
        return
    try:
        gh("label", "create", label, "--repo", host + "/" + repository,
           "--color", color, "--description", "Current work status")
    except RuntimeError:
        # A concurrent task may have created this label for another issue.
        if label.casefold() not in names():
            raise


def synchronize(db, args):
    with db:
        db.execute("BEGIN IMMEDIATE")
        work = state.row(db, "works", args.work_id)
        if args.action != "start":
            state.require_owner(db, args.work_id, args.owner)
    issue_url = args.issue if args.action == "start" else work["issue"]
    state.issue_resource(issue_url)
    observed = view(issue_url)
    issue_url = observed["url"]
    parsed = urlsplit(issue_url)
    repository = "/".join(parsed.path.strip("/").split("/")[:2])
    if args.status == "done" and observed["state"] != "CLOSED":
        raise ValueError("done requires an already closed issue; an open PR is in-review")
    if args.status != "done" and observed["state"] != "OPEN":
        raise ValueError("reopen the issue before resuming work")
    if args.release and args.status == "in-progress":
        raise ValueError("choose paused, blocked, in-review or done when releasing work")

    with db:
        db.execute("BEGIN IMMEDIATE")
        work = state.row(db, "works", args.work_id)
        if args.action == "start":
            if work is None:
                state.record(db, "work", dict(id=args.work_id, title=observed["title"],
                             repository=parsed.netloc + "/" + repository, issue=issue_url,
                             status="starting"))
            elif work["issue"] is None:
                state.update(db, "work", dict(id=args.work_id, issue=issue_url), None)
            elif state.issue_resource(work["issue"]) != state.issue_resource(issue_url):
                raise ValueError("work is already linked to another issue")
            state.claim_work(db, args.work_id, args.owner, args.source_ref)
        else:
            state.require_owner(db, args.work_id, args.owner)

    # No database transaction spans network requests or the agent's actual work.
    login = args.assignee
    if login == "@me":
        login = gh("api", "--hostname", parsed.netloc, "user", "--jq", ".login")
    label = "status: " + args.status.replace("-", " ")
    assigned = {item["login"].casefold() for item in observed["assignees"]}
    labels = status_labels(observed)
    if {item.casefold() for item in labels} != {label} or login.casefold() not in assigned:
        ensure_label(parsed.netloc, repository, label, STATUSES[args.status][2])
        command = ["issue", "edit", issue_url, "--add-assignee", login, "--add-label", label]
        for previous in sorted(item for item in labels if item.casefold() != label):
            command += ["--remove-label", previous]
        gh(*command)
    verified = view(issue_url)
    if ({item.casefold() for item in status_labels(verified)} != {label}
            or login.casefold() not in {item["login"].casefold() for item in verified["assignees"]}
            or verified["state"] != observed["state"]):
        raise RuntimeError("GitHub readback disagrees with requested assignment or status; reconcile before retrying")

    with db:
        db.execute("BEGIN IMMEDIATE")
        state.require_owner(db, args.work_id, args.owner)
        status, stage, _ = STATUSES[args.status]
        if args.release and status == "active":
            status = "waiting"
        values = dict(id=args.work_id, status=status, blocker=args.reason if status == "blocked" else None)
        if stage:
            values["stage"] = stage
        state.update(db, "work", values, None)
        state.claim_work(db, args.work_id, args.owner)
        state.record(db, "result", dict(id=uuid.uuid4().hex, work_id=args.work_id,
                     kind="github", status="synchronized", evidence_ref=issue_url,
                     summary=label + "; assigned to " + login))
        if args.release or args.status in {"paused", "done"}:
            state.release_work(db, args.work_id, args.owner)
        return dict(issue=issue_url, assignee=login, status=args.status,
                    claim=state.claim_for(db, args.work_id))


def parser():
    root = argparse.ArgumentParser(description=__doc__)
    default = Path(os.environ.get("XDG_STATE_HOME", str(Path.home() / ".local/state"))) / "devflow/workflow.sqlite3"
    root.add_argument("--db", default=str(default))
    commands = root.add_subparsers(dest="action", required=True)
    for action in ("start", "set"):
        command = commands.add_parser(action)
        command.add_argument("--work-id", required=True)
        command.add_argument("--owner", required=True, help="Actual host task or coordinator ID")
        command.add_argument("--assignee", default="@me")
        command.add_argument("--reason")
        command.add_argument("--release", action="store_true")
        command.add_argument("--status", choices=STATUSES, default="in-progress" if action == "start" else None,
                             required=action != "start")
        if action == "start":
            command.add_argument("issue", help="Full GitHub issue URL")
            command.add_argument("--source-ref", help="Private locator for the owning host task")
    return root


def main():
    args = parser().parse_args()
    try:
        if args.status == "blocked" and not args.reason:
            raise ValueError("blocked requires --reason")
        if args.assignee.startswith("@") and args.assignee != "@me":
            raise ValueError("assignee must be a GitHub login or @me")
        with closing(state.connect(args.db)) as db:
            result = synchronize(db, args)
        print(state.encode(result))
        return 0
    except (ValueError, OSError, RuntimeError, sqlite3.Error, subprocess.TimeoutExpired) as exc:
        try:
            with closing(state.connect(args.db)) as db, db:
                db.execute("BEGIN IMMEDIATE")
                owned = state.claim_for(db, args.work_id)
                if owned and owned["owner"] == args.owner:
                    work = state.row(db, "works", args.work_id)
                    state.update(db, "work", dict(id=args.work_id, status="blocked", blocker=str(exc)), None)
                    state.record(db, "result", dict(id=uuid.uuid4().hex, work_id=args.work_id,
                                 kind="github", status="failed", evidence_ref=work["issue"], summary=str(exc)))
        except (ValueError, OSError, sqlite3.Error):
            pass  # The original failure remains visible even if recording is unavailable.
        print(state.encode({"error": str(exc), "work_id": args.work_id}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
