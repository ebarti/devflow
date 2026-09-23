#!/usr/bin/env python3.12
"""Create, synchronize, and audit an owned issue and its existing GitHub Project."""

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
    "in-progress": ("active", "implementation", "In progress"),
    "in-review": ("active", "review", "In review"),
    "blocked": ("blocked", None, "Blocked"),
    "paused": ("paused", None, "Paused"),
    "done": ("done", "delivered", "Done"),
}


def gh(*args, as_json=False):
    result = subprocess.run(["gh", *args], text=True, capture_output=True, timeout=60)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "gh failed")
    return json.loads(result.stdout) if as_json else result.stdout.strip()


def view(issue):
    return gh("issue", "view", issue, "--json", "id,url,title,state,assignees", as_json=True)


def graphql(host, query, **variables):
    command = ["api", "graphql", "--hostname", host, "-f", "query=" + query]
    for key, value in variables.items():
        command += ["-F" if isinstance(value, int) else "-f", f"{key}={value}"]
    response = gh(*command, as_json=True)
    if response.get("errors"):
        raise RuntimeError(state.encode(response["errors"]))
    return response["data"]


def details(work):
    value = json.loads(work["details"]) if work and work["details"] else {}
    if not isinstance(value, dict) or not isinstance(value.get("github", {}), dict):
        raise ValueError("work details and details.github must be objects")
    return value


def action_url(url, host):
    parsed = urlsplit(url or "")
    parts = parsed.path.strip("/").split("/")
    if (parsed.scheme != "https" or parsed.netloc.casefold() != host.casefold()
            or parsed.username or parsed.password or parsed.query or parsed.fragment
            or len(parts) != 5 or parts[2:4] != ["actions", "runs"]
            or not parts[0] or not parts[1] or not parts[4].isdigit()):
        raise ValueError("--await-url must be a full Actions run URL on the issue host")
    return url


def project_item(host, item_id):
    return graphql(host, """query($item:ID!){node(id:$item){
        ... on ProjectV2Item{project{id} fieldValueByName(name:"Status"){
        ... on ProjectV2ItemFieldSingleSelectValue{optionId}}}}}""", item=item_id)["node"]


def legacy_project_item(project_url, issue_id):
    """Observe a bounded legacy Project membership without inventing its expected Status."""
    parsed = urlsplit(project_url)
    parts = parsed.path.strip("/").split("/")
    if (parsed.scheme != "https" or len(parts) != 4 or parts[0] not in {"users", "orgs"}
            or parts[2] != "projects" or not parts[3].isdigit()):
        raise ValueError("legacy Project URL is invalid")
    owner_type = "user" if parts[0] == "users" else "organization"
    selected = graphql(parsed.netloc, """query($owner:String!,$number:Int!){OWNER(login:$owner){
        projectV2(number:$number){id}}}""".replace("OWNER", owner_type),
        owner=parts[1], number=int(parts[3]))
    project_id = ((selected.get(owner_type) or {}).get("projectV2") or {}).get("id")
    if not project_id:
        return None
    observed = graphql(parsed.netloc, """query($issue:ID!){node(id:$issue){
        ... on Issue{projectItems(first:100){nodes{id project{id}
        fieldValueByName(name:"Status"){
        ... on ProjectV2ItemFieldSingleSelectValue{optionId name}}}}}}}""", issue=issue_id)
    items = (((observed.get("node") or {}).get("projectItems") or {}).get("nodes") or [])
    return next((item for item in items if (item.get("project") or {}).get("id") == project_id), None)


def project(url, status_name):
    parsed = urlsplit(url or "")
    parts = parsed.path.strip("/").split("/")
    if (parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password
            or len(parts) != 4 or parts[0] not in {"users", "orgs"}
            or not parts[1] or parts[2] != "projects" or not parts[3].isdigit()
            or int(parts[3]) < 1):
        raise ValueError("supply --project with an existing GitHub Project URL")
    owner_type = "user" if parts[0] == "users" else "organization"
    query = """query($owner:String!,$number:Int!){OWNER(login:$owner){
        projectV2(number:$number){id url closed field(name:"Status"){
            ... on ProjectV2SingleSelectField{id options{id name}}}}}}""".replace("OWNER", owner_type)
    data = graphql(parsed.netloc, query, owner=parts[1], number=int(parts[3]))
    found = (data.get(owner_type) or {}).get("projectV2")
    if not found or found["closed"]:
        raise ValueError("selected Project is missing or closed")
    field = found.get("field") or {}
    option = next((item for item in field.get("options", [])
                   if item["name"].casefold() == status_name.casefold()), None)
    if not option:
        choices = [item["name"] for item in field.get("options", [])]
        raise ValueError("choose --project-status from this Project's Status options: " + state.encode(choices))
    return dict(host=parsed.netloc, id=found["id"], url=found["url"],
                field=field["id"], option=option["id"], status=option["name"])


def bind_issue(db, args, issue_url):
    work = state.row(db, "works", args.work_id)
    if work["issue"] is None:
        if state.claim_for(db, args.work_id):
            state.release_work(db, args.work_id, args.owner)
        repository = state.issue_resource(issue_url).removeprefix("issue:").rsplit("/issues/", 1)[0]
        state.update(db, "work", dict(id=args.work_id, issue=issue_url, repository=repository), None)
    elif state.issue_resource(work["issue"]) != state.issue_resource(issue_url):
        raise ValueError("work is already linked to another issue")
    state.claim_work(db, args.work_id, args.owner, args.source_ref)


def create_issue(db, args, project_url):
    repository = args.repo if args.repo.count("/") == 2 else "github.com/" + args.repo
    repository = state.issue_resource("https://" + repository + "/issues/1").removeprefix("issue:").rsplit("/issues/", 1)[0]
    if repository.split("/")[0] != urlsplit(project_url).netloc.casefold():
        raise ValueError("issue and Project must belong to the same GitHub host")
    with db:
        db.execute("BEGIN IMMEDIATE")
        work = state.row(db, "works", args.work_id)
        if work is None:
            state.record(db, "work", dict(id=args.work_id, title=args.title,
                         repository=repository, status="starting"))
            work = state.row(db, "works", args.work_id)
        if work["repository"] != repository:
            raise ValueError("work is already linked to another repository")
        state.claim_work(db, args.work_id, args.owner, args.source_ref)
        if work["issue"]:
            return work["issue"]
        saved = details(work)
        tracking = saved.setdefault("github", {})
        if tracking.get("create_pending"):
            raise ValueError("issue creation is unresolved; find its outcome and use start ISSUE_URL; do not create again")
        tracking.update(project=project_url, create_pending=True)
        state.update(db, "work", dict(id=args.work_id, details=saved), None)
    # Persist the attempt before the network call so interruption cannot silently duplicate it.
    issue_url = gh("issue", "create", "--repo", repository, "--title", args.title,
                   "--body-file", args.body_file, "--assignee", args.assignee)
    state.issue_resource(issue_url)
    with db:
        db.execute("BEGIN IMMEDIATE")
        state.require_owner(db, args.work_id, args.owner)
        bind_issue(db, args, issue_url)
    return issue_url


def synchronize(db, args):
    with db:
        db.execute("BEGIN IMMEDIATE")
        work = state.row(db, "works", args.work_id)
        if args.action == "set" or state.claim_for(db, args.work_id):
            state.require_owner(db, args.work_id, args.owner)
        saved = details(work).get("github", {})
    status_name = args.project_status or saved.get("statuses", {}).get(args.status) or STATUSES[args.status][2]
    selected = project(args.project or saved.get("project"), status_name)
    if args.await_url:
        action_url(args.await_url, selected["host"])
    if args.action == "create":
        issue_url = create_issue(db, args, selected["url"])
    else:
        issue_url = args.issue if args.action == "start" else work["issue"]
    state.issue_resource(issue_url)
    observed = view(issue_url)
    issue_url = observed["url"]
    parsed = urlsplit(issue_url)
    if parsed.netloc.casefold() != selected["host"].casefold():
        raise ValueError("issue and Project must belong to the same GitHub host")
    if args.status == "done" and observed["state"] != "CLOSED":
        raise ValueError("done requires an already closed issue; an open PR is in-review")
    if args.status != "done" and observed["state"] != "OPEN":
        raise ValueError("reopen the issue before resuming work")

    with db:
        db.execute("BEGIN IMMEDIATE")
        work = state.row(db, "works", args.work_id)
        if work is None:
            state.record(db, "work", dict(id=args.work_id, title=observed["title"], status="starting"))
        bind_issue(db, args, issue_url)
        saved = details(state.row(db, "works", args.work_id))
        tracking = saved.setdefault("github", {})
        tracking.update(project=selected["url"], create_pending=False)
        tracking.setdefault("statuses", {})[args.status] = selected["status"]
        state.update(db, "work", dict(id=args.work_id, details=saved), None)

    # No database transaction spans network requests or the agent's actual work.
    login = args.assignee
    if login == "@me":
        login = gh("api", "--hostname", parsed.netloc, "user", "--jq", ".login")
    assigned = {item["login"].casefold() for item in observed["assignees"]}
    if login.casefold() not in assigned:
        gh("issue", "edit", issue_url, "--add-assignee", login)
    added = graphql(selected["host"], """mutation($project:ID!,$issue:ID!){
        addProjectV2ItemById(input:{projectId:$project,contentId:$issue}){item{id}}}""",
        project=selected["id"], issue=observed["id"])
    item_id = added["addProjectV2ItemById"]["item"]["id"]
    graphql(selected["host"], """mutation($project:ID!,$item:ID!,$field:ID!,$option:String!){
        updateProjectV2ItemFieldValue(input:{projectId:$project,itemId:$item,fieldId:$field,
        value:{singleSelectOptionId:$option}}){projectV2Item{id}}}""",
        project=selected["id"], item=item_id, field=selected["field"], option=selected["option"])
    readback = project_item(selected["host"], item_id)
    verified = view(issue_url)
    if (not readback or readback["project"]["id"] != selected["id"]
            or (readback.get("fieldValueByName") or {}).get("optionId") != selected["option"]
            or login.casefold() not in {item["login"].casefold() for item in verified["assignees"]}
            or verified["state"] != observed["state"]):
        raise RuntimeError("GitHub readback disagrees with assignment or Project Status; reconcile before retrying")

    with db:
        db.execute("BEGIN IMMEDIATE")
        state.require_owner(db, args.work_id, args.owner)
        saved = details(state.row(db, "works", args.work_id))
        tracking = saved.setdefault("github", {})
        tracking["sync"] = dict(status=args.status, issue_state=verified["state"], assignee=login,
                                project=selected["url"], project_id=selected["id"],
                                item_id=item_id, field_id=selected["field"], option_id=selected["option"],
                                project_status=selected["status"], readback_at=state.now())
        if args.await_url:
            tracking["await"] = dict(url=action_url(args.await_url, parsed.netloc),
                                     follow_up=args.follow_up)
        elif args.status != "blocked":
            tracking.pop("await", None)
        state.update(db, "work", dict(id=args.work_id, details=saved), None)
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
                     summary=selected["url"] + ": " + selected["status"] + "; assigned to " + login))
        if args.release or args.status in {"paused", "done"}:
            state.release_work(db, args.work_id, args.owner)
        return dict(issue=issue_url, assignee=login, status=args.status, project=selected["url"],
                    project_status=selected["status"],
                    claim=state.claim_for(db, args.work_id))


def audit(db, work_id):
    """Read local and remote observations without claiming, syncing, or changing a work."""
    work = state.row(db, "works", work_id)
    if not work:
        raise ValueError("unknown work: " + work_id)
    tracking = details(work).get("github", {})
    claim = state.claim_for(db, work_id)
    result = dict(work_id=work_id, issue=work["issue"], local_status=work["status"],
                  claim=claim, reconciliation_required=[], unknown=[])
    if not work["issue"]:
        result["unknown"].append("issue_not_linked")
    else:
        observed = view(work["issue"])
        result["issue_state"] = observed["state"]
        result["assignees"] = [item["login"] for item in observed["assignees"]]
        sync = tracking.get("sync")
        if not isinstance(sync, dict) or not all(sync.get(k) for k in (
                "issue_state", "assignee", "project", "project_id", "item_id", "option_id", "readback_at")):
            result["unknown"].append("legacy_sync_expectation")
            if tracking.get("project"):
                result["project_observed"] = legacy_project_item(tracking["project"], observed["id"])
                if not result["project_observed"]:
                    result["unknown"].append("legacy_project_item_not_found_in_first_100")
        else:
            result["expected"] = sync
            if observed["state"] != sync["issue_state"]:
                result["reconciliation_required"].append("issue_state_mismatch")
            if sync["assignee"].casefold() not in {a.casefold() for a in result["assignees"]}:
                result["reconciliation_required"].append("assignee_mismatch")
            selected = project_item(urlsplit(sync["project"]).netloc, sync["item_id"])
            result["project_observed"] = selected
            if (not selected or (selected.get("project") or {}).get("id") != sync["project_id"]
                    or (selected.get("fieldValueByName") or {}).get("optionId") != sync["option_id"]):
                result["reconciliation_required"].append("project_status_mismatch")
            desired = ("in-review" if work["stage"] == "review" else "in-progress") if work["status"] == "active" else work["status"]
            if desired in STATUSES and sync["status"] != desired:
                result["reconciliation_required"].append("local_status_mismatch")
        if work["status"] == "done" and observed["state"] != "CLOSED":
            result["reconciliation_required"].append("done_issue_open")
        if work["status"] in {"active", "blocked", "paused", "waiting"} and observed["state"] != "OPEN":
            result["reconciliation_required"].append("unfinished_issue_closed")
    if claim:
        owner = db.execute("SELECT role,closed_at FROM runtime_sessions WHERE id=?", (claim["owner"],)).fetchone()
        if owner and owner["role"] == "coordinator" and owner["closed_at"]:
            result["reconciliation_required"].append("stopped_owner_retains_claim")
        elif not owner:
            result["unknown"].append("owner_runtime_unknown")
        result["owner_runtime"] = dict(owner) if owner else None
    awaited = tracking.get("await")
    if not awaited and isinstance(details(work).get("release_run"), str):
        awaited = dict(url=details(work)["release_run"], follow_up=None, legacy=True)
    if awaited and not work["issue"]:
        result["unknown"].append("awaited_run_issue_unknown")
    elif awaited:
        url = action_url(awaited["url"], urlsplit(work["issue"]).netloc)
        parsed = urlsplit(url)
        owner, repository, _, _, run_id = parsed.path.strip("/").split("/")
        run = gh("run", "view", run_id, "--repo", f"{parsed.netloc}/{owner}/{repository}",
                 "--json", "status,conclusion,url", as_json=True)
        result["awaited_run"] = dict(url=url, status=run["status"], conclusion=run.get("conclusion"),
                                      follow_up=awaited.get("follow_up"))
        if run["status"] == "completed":
            if run.get("conclusion") != "success" or work["status"] in {"blocked", "waiting", "active"}:
                result["reconciliation_required"].append("awaited_run_completed")
        else:
            if run["status"] not in {"queued", "in_progress", "requested", "waiting", "pending"}:
                result["unknown"].append("awaited_run_status_unknown")
            if not awaited.get("follow_up"):
                result["unknown"].append("awaited_run_follow_up_unknown")
    result["state"] = ("reconciliation_required" if result["reconciliation_required"] else
                       "unknown" if result["unknown"] else "consistent")
    return result


def parser():
    root = argparse.ArgumentParser(description=__doc__)
    default = Path(os.environ.get("XDG_STATE_HOME", str(Path.home() / ".local/state"))) / "devflow/workflow.sqlite3"
    root.add_argument("--db", default=str(default))
    commands = root.add_subparsers(dest="action", required=True)
    for action in ("create", "start", "set"):
        command = commands.add_parser(action)
        command.add_argument("--work-id", required=True)
        command.add_argument("--owner", required=True, help="Actual host task or coordinator ID")
        command.add_argument("--assignee", default="@me")
        command.add_argument("--project", help="Existing Project URL; retained in the work record")
        command.add_argument("--project-status", help="Existing Status option, when it differs from the work status")
        command.add_argument("--source-ref", help="Private locator for the owning host task")
        command.add_argument("--reason")
        command.add_argument("--release", action="store_true")
        command.add_argument("--await-url", help="Actions run URL for a blocked external handoff")
        command.add_argument("--follow-up", help="Concrete owner and trigger for checking the run")
        command.add_argument("--status", choices=STATUSES, default=None if action == "set" else "in-progress",
                             required=action == "set")
        if action == "create":
            command.add_argument("--repo", required=True)
            command.add_argument("--title", required=True)
            command.add_argument("--body-file", required=True)
        if action == "start":
            command.add_argument("issue", help="Full GitHub issue URL")
    audit_command = commands.add_parser("audit", help="Read-only audit of one linked work and live GitHub state")
    audit_command.add_argument("--work-id", required=True)
    return root


def main():
    args = parser().parse_args()
    try:
        if args.action == "audit":
            with closing(sqlite3.connect(Path(args.db).expanduser().resolve().as_uri() + "?mode=ro", uri=True)) as db:
                db.row_factory = sqlite3.Row
                result = audit(db, args.work_id)
            print(state.encode(result))
            return 0 if result["state"] == "consistent" else 1
        if args.status == "blocked" and not args.reason:
            raise ValueError("blocked requires --reason")
        if bool(args.await_url) != bool(args.follow_up) or (args.await_url and args.status != "blocked"):
            raise ValueError("--await-url and --follow-up are required together with --status blocked")
        if args.await_url and args.action == "create":
            raise ValueError("create cannot hand off an Actions run; use set after the issue exists")
        if args.action == "create" and args.status == "done":
            raise ValueError("create starts an open issue; use start to reconcile a closed issue")
        if args.release and args.status == "in-progress":
            raise ValueError("choose paused, blocked, in-review or done when releasing work")
        if args.assignee.startswith("@") and args.assignee != "@me":
            raise ValueError("assignee must be a GitHub login or @me")
        with closing(state.connect(args.db)) as db:
            result = synchronize(db, args)
        print(state.encode(result))
        return 0
    except (ValueError, OSError, RuntimeError, sqlite3.Error, subprocess.TimeoutExpired) as exc:
        if args.action == "audit":
            print(state.encode({"error": str(exc), "work_id": args.work_id}), file=sys.stderr)
            return 1
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
