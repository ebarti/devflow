"""Independent durable Project projector and five-minute PR observer.

Run with --config pointing to {"version":1,"owners":["/absolute/service.json",...]}.
This process has no workflow client, agent dispatch, merge, or product-repair authority.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import logging
import os
import subprocess
import tempfile
import time
from collections.abc import Callable
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

from .contracts import canonical_json, digest
from .delivery_config import DeliveryConfig
from .delivery_features import issue_key, publication_urls, transition
from .delivery_store import DeliveryStore, _private_directory

logger = logging.getLogger(__name__)


class Superseded(RuntimeError):
    """A newer feature event arrived while a projection was in flight."""


class GitHub:
    def command(self, *args: str, payload: dict | None = None) -> dict:
        result = subprocess.run(
            [os.environ.get("DEVFLOW_GH", "gh"), *args],
            input=canonical_json(payload) if payload else None,
            text=True, capture_output=True, timeout=60, check=False,
        )
        if result.returncode:
            raise RuntimeError((result.stderr or result.stdout).strip()[:1000])
        value = json.loads(result.stdout)
        if isinstance(value, dict) and value.get("errors"):
            raise RuntimeError(canonical_json(value["errors"])[:1000])
        return value

    def graphql(self, host: str, query: str, **variables):
        return self.command("api", "graphql", "--hostname", host, "--input", "-",
                            payload={"query": query, "variables": variables})["data"]

    def pull_request(self, url: str) -> dict:
        value = self.command("pr", "view", url, "--json",
                             "url,state,headRefOid,baseRefOid,mergedAt")
        if issue_key(value["url"]) != issue_key(url) or value["state"] not in {
            "OPEN", "CLOSED", "MERGED",
        }:
            raise ValueError("PR readback differs from the bound publication")
        return {"url": url, "state": value["state"], "head": value["headRefOid"],
                "base": value["baseRefOid"], "merged_at": value["mergedAt"]}

    def mirror(self, feature: dict, fence: Callable[[], None]) -> dict:
        project_url = feature["binding"]["project"]
        assignee = feature["binding"]["assignee"]
        if not project_url or not assignee:
            raise ValueError("feature Project and assignee are not configured")
        parsed = urlsplit(project_url)
        parts = parsed.path.strip("/").split("/")
        if (parsed.scheme != "https" or parsed.username or parsed.password
                or parsed.query or parsed.fragment or len(parts) != 4
                or parts[0] not in {"users", "orgs"} or parts[2] != "projects"
                or not parts[3].isdigit()
                or parsed.netloc.casefold() != urlsplit(feature["issue"]).netloc.casefold()):
            raise ValueError("Project binding must identify an existing Project on the issue host")
        if assignee == "@me":
            assignee = self.command("api", "user", "--hostname", parsed.netloc)["login"]
        owner_type = "user" if parts[0] == "users" else "organization"
        selected = self.graphql(parsed.netloc, """query($owner:String!,$number:Int!){
            OWNER(login:$owner){projectV2(number:$number){id closed field(name:"Status"){
            ... on ProjectV2SingleSelectField{id options{id name color description}}}}}}"""
            .replace("OWNER", owner_type), owner=parts[1], number=int(parts[3]))
        project = (selected.get(owner_type) or {}).get("projectV2")
        if not project or project["closed"] or not project.get("field"):
            raise ValueError("Project is missing, closed, or lacks a single-select Status field")
        field = project["field"]
        option = next((item for item in field["options"]
                       if item["name"] == feature["status"]), None)
        if option is None:
            # GitHub replaces this entire list. IDs preserve existing item values.
            options = [{key: item[key] for key in ("id", "name", "color", "description")}
                       for item in field["options"]]
            options.append({"name": feature["status"], "color": "GRAY", "description": ""})
            fence()
            result = self.graphql(parsed.netloc, """mutation($input:UpdateProjectV2FieldInput!){
                updateProjectV2Field(input:$input){projectV2Field{
                ... on ProjectV2SingleSelectField{id options{id name}}}}}""",
                input={"fieldId": field["id"], "singleSelectOptions": options})
            option = next(item for item in result["updateProjectV2Field"]["projectV2Field"][
                "options"] if item["name"] == feature["status"])
        issue = self.command("issue", "view", feature["issue"], "--json", "id,url,state,assignees")
        if issue_key(issue["url"]) != feature["issue"]:
            raise ValueError("issue readback changed the feature binding")
        if assignee.casefold() not in {item["login"].casefold() for item in issue["assignees"]}:
            fence()
            self.graphql(parsed.netloc, """mutation($id:ID!,$assignees:[ID!]!){
                addAssigneesToAssignable(input:{assignableId:$id,assigneeIds:$assignees}){
                assignable{... on Issue{id}}}}""", id=issue["id"], assignees=[self.graphql(
                parsed.netloc, "query($login:String!){user(login:$login){id}}",
                login=assignee)["user"]["id"]])
        item_id = None
        cursor = None
        while True:
            page = self.graphql(parsed.netloc, """query($issue:ID!,$cursor:String){node(id:$issue){
                ... on Issue{projectItems(first:100,after:$cursor){nodes{id project{id}}
                pageInfo{hasNextPage endCursor}}}}}""", issue=issue["id"], cursor=cursor)[
                    "node"]["projectItems"]
            item_id = next((item["id"] for item in page["nodes"]
                            if item["project"]["id"] == project["id"]), None)
            if item_id or not page["pageInfo"]["hasNextPage"]:
                break
            next_cursor = page["pageInfo"]["endCursor"]
            if not next_cursor or next_cursor == cursor:
                raise ValueError("Project membership pagination did not advance")
            cursor = next_cursor
        if item_id is None:
            fence()
            item_id = self.graphql(parsed.netloc, """mutation($project:ID!,$content:ID!){
                addProjectV2ItemById(input:{projectId:$project,contentId:$content}){item{id}}}""",
                project=project["id"], content=issue["id"])["addProjectV2ItemById"]["item"]["id"]

        def readback():
            return self.graphql(parsed.netloc, """query($item:ID!){node(id:$item){
                ... on ProjectV2Item{project{id} fieldValueByName(name:"Status"){
                ... on ProjectV2ItemFieldSingleSelectValue{optionId name}}}}}""",
                item=item_id)["node"]

        def matches(observed):
            return (observed and observed["project"]["id"] == project["id"]
                    and (observed.get("fieldValueByName") or {}).get("optionId") == option["id"]
                    and observed["fieldValueByName"]["name"] == feature["status"])

        if not matches(readback()):
            fence()
            self.graphql(parsed.netloc, """mutation(
                $project:ID!,$item:ID!,$field:ID!,$option:String!){
                updateProjectV2ItemFieldValue(input:{projectId:$project,itemId:$item,fieldId:$field,
                value:{singleSelectOptionId:$option}}){projectV2Item{id}}}""",
                project=project["id"], item=item_id, field=field["id"], option=option["id"])
        observed = readback()
        assigned = self.command("issue", "view", feature["issue"], "--json", "assignees")
        if not matches(observed) or assignee.casefold() not in {
            item["login"].casefold() for item in assigned["assignees"]
        }:
            raise ValueError("Project or assignee readback differs from the canonical feature")
        fence()
        return {"project": project_url, "project_id": project["id"], "item_id": item_id,
                "field_id": field["id"], "option_id": option["id"],
                "status": feature["status"], "assignee": assignee}


class ProjectSynchronizer:
    def __init__(self, stores: list[DeliveryStore], github=None, *, interval: int = 300,
                 clock=None):
        if not stores or len({str(store.config.tracking_db) for store in stores}) != len(stores):
            raise ValueError("synchronizer needs distinct, explicitly configured runtime owners")
        if not 300 <= interval <= 3600:
            raise ValueError("PR observation interval must be between 300 and 3600 seconds")
        self.stores = stores
        self.github = github or GitHub()
        self.interval = interval
        self.clock = clock or (lambda: datetime.now(UTC))
        self.lock_root = Path(tempfile.gettempdir()) / f"devflow-project-sync-{os.getuid()}"

    @contextmanager
    def ownership(self):
        with ExitStack() as stack:
            _private_directory(self.lock_root)
            projects = {feature["binding"]["project"]
                        for _, feature in self.selected().values()
                        if feature["binding"].get("project")}
            projects.update(repository["project_url"] for store in self.stores
                            for repository in store.config.raw["repositories"].values()
                            if repository.get("project_url"))
            for project in sorted(projects):
                path = self.lock_root / digest(issue_key(project))
                lock = stack.enter_context(open(path, "a", encoding="utf-8"))
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            for store in sorted(self.stores, key=lambda value: str(value.config.tracking_db)):
                lock_path = str(store.config.tracking_db) + ".project-sync.lock"
                lock = stack.enter_context(open(lock_path,
                                                "a", encoding="utf-8"))
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield

    def selected(self) -> dict:
        selected = {}
        for store in self.stores:
            with store._connect() as db:
                for row in db.execute("SELECT * FROM delivery_features"):
                    value = {**json.loads(row["payload_json"]), "version": row["version"],
                             "source": str(store.config.tracking_db.resolve())}
                    prior = selected.get(value["issue"])
                    if not prior or (value["admitted_at"], value["run_id"], value["source"]) > (
                        prior[1]["admitted_at"], prior[1]["run_id"], prior[1]["source"]
                    ):
                        selected[value["issue"]] = (store, value)
        return selected

    def observe_prs(self) -> None:
        references: dict[str, list[tuple[DeliveryStore, str]]] = {}
        saved = {}
        for store in self.stores:
            with store._connect() as db:
                for row in db.execute("SELECT run_id,pr_json,issue_url FROM delivery_runs "
                                      "WHERE pr_json IS NOT NULL"):
                    for url in publication_urls(json.loads(row["pr_json"])):
                        # A publication cannot grant access outside its admitted repository.
                        if urlsplit(url).netloc != urlsplit(row["issue_url"]).netloc or (
                            url.rsplit("/pull/", 1)[0] != row["issue_url"].rsplit("/issues/", 1)[0]
                        ):
                            raise ValueError("publication PR is outside the admitted repository")
                        references.setdefault(url, []).append((store, row["run_id"]))
                for row in db.execute("SELECT * FROM delivery_pr_observations"):
                    if row["url"] not in saved or row["next_check_at"] > saved[row["url"]][
                        "next_check_at"]:
                        saved[row["url"]] = dict(row)
        for url, owners in references.items():
            stamp = self.clock()
            prior = saved.get(url)
            due = not prior or prior["next_check_at"] <= stamp.isoformat()
            observed, error = None, None
            if due:
                try:
                    observed = self.github.pull_request(url)
                except Exception as exc:
                    error = str(exc)[:1000]
                checked_at = stamp.isoformat() if observed else None
                next_check = (stamp + timedelta(seconds=self.interval)).isoformat()
            else:
                observed = json.loads(prior["observation_json"] or "null")
                error, checked_at = prior["error"], prior["checked_at"]
                next_check = prior["next_check_at"]
            for store, run_id in owners:
                with store._connect() as db:
                    db.execute("BEGIN IMMEDIATE")
                    local = db.execute("SELECT * FROM delivery_pr_observations WHERE url=?",
                                       (url,)).fetchone()
                    if not due and local and dict(local) == prior:
                        continue
                    db.execute("""INSERT INTO delivery_pr_observations VALUES (?,?,?,?,?)
                        ON CONFLICT(url) DO UPDATE SET observation_json=
                        COALESCE(excluded.observation_json,delivery_pr_observations.observation_json),
                        checked_at=COALESCE(excluded.checked_at,delivery_pr_observations.checked_at),
                        next_check_at=excluded.next_check_at,error=excluded.error""",
                        (url, canonical_json(observed) if observed else None,
                         checked_at, next_check, error))
                    transition(db, store.config, run_id)

    @staticmethod
    def identity(feature: dict) -> str:
        return digest({key: feature[key] for key in
                       ("issue", "run_id", "source", "status", "binding", "legacy_tracking")})

    @contextmanager
    def handoff(self, issue: str):
        """Serialize selected ownership with every participating legacy writer."""
        def paths():
            locks = set()
            for store in self.stores:
                with store._connect() as db:
                    for row in db.execute("SELECT id FROM works WHERE lower(rtrim(issue,'/'))=?",
                                          (issue,)):
                        name = hashlib.sha256(row[0].encode()).hexdigest()[:24]
                        locks.add(store.config.tracking_db.with_name(f".reconcile-{name}.lock"))
            return locks

        with ExitStack() as stack:
            selected = paths()
            for path in sorted(selected):
                fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
                lock = stack.enter_context(os.fdopen(fd, "w"))
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if selected != paths():
                raise Superseded("feature works changed during ownership handoff")
            yield

    def publish_view(self, feature: dict, outbox: dict, stamp: datetime) -> dict:
        view = {**feature, "mirror": {
            key: outbox[key]
            for key in ("state", "last_error", "checked_at", "next_attempt_at")
        }, "project_receipt": json.loads(outbox["receipt_json"] or "null")}
        for destination in self.stores:
            with destination._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute("""INSERT INTO delivery_feature_views VALUES (?,?,?)
                    ON CONFLICT(issue) DO UPDATE SET payload_json=excluded.payload_json,
                    updated_at=excluded.updated_at
                    WHERE delivery_feature_views.payload_json!=excluded.payload_json""",
                    (feature["issue"], canonical_json(view), stamp.isoformat()))
        return view

    def tick(self) -> dict:
        self.observe_prs()
        results = {}
        for issue, (store, feature) in self.selected().items():
            try:
                with self.handoff(issue):
                    view = self.project(store, feature)
                    if view:
                        results[issue] = view
            except (BlockingIOError, Superseded):
                # The existing owner finishes first; the next local tick retries.
                continue
        return results

    def project(self, store: DeliveryStore, feature: dict) -> dict | None:
        issue = feature["issue"]
        identity = self.identity(feature)
        stamp = self.clock()

        def fence():
            latest = self.selected().get(issue)
            if not latest or self.identity(latest[1]) != identity:
                raise Superseded("feature owner or status changed")

        fence()
        with store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute("SELECT * FROM delivery_project_outbox WHERE issue=?",
                               (issue,)).fetchone()
            if not prior or prior["identity"] != identity:
                db.execute("""INSERT INTO delivery_project_outbox
                    (issue,identity,payload_json,state,next_attempt_at)
                    VALUES (?,?,?,'pending',?)
                    ON CONFLICT(issue) DO UPDATE SET identity=excluded.identity,
                    payload_json=excluded.payload_json,state='pending',attempts=0,
                    next_attempt_at=excluded.next_attempt_at,last_error=NULL,receipt_json=NULL,
                    checked_at=NULL""",
                    (issue, identity, canonical_json(feature), stamp.isoformat()))
            current = dict(db.execute("SELECT * FROM delivery_project_outbox WHERE issue=?",
                                      (issue,)).fetchone())
        # Establish selected ownership everywhere before making any remote
        # effect. Legacy reconcile locks remain held through final readback.
        self.publish_view(feature, current, stamp)
        if current["next_attempt_at"] <= stamp.isoformat():
            try:
                fence()
                if feature.get("legacy_tracking"):
                    raise ValueError("historical workflow retains its frozen tracking contract "
                                     "until it stops")
                receipt = self.github.mirror(feature, fence)
                fence()
                state, error = "consistent", None
                delay = self.interval
            except Superseded:
                return None
            except Exception as exc:
                receipt, state, error = None, "pending", str(exc)[:1000]
                delay = min(3600, 15 * 2 ** min(current["attempts"], 8))
            with store._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute("""UPDATE delivery_project_outbox SET state=?,attempts=?,
                    next_attempt_at=?,last_error=?,receipt_json=?,checked_at=?
                    WHERE issue=? AND identity=?""", (
                    state, 0 if state == "consistent" else current["attempts"] + 1,
                    (stamp + timedelta(seconds=delay)).isoformat(), error,
                    canonical_json(receipt) if receipt else None, stamp.isoformat(),
                    issue, identity))
                current = dict(db.execute("SELECT * FROM delivery_project_outbox WHERE issue=?",
                                          (issue,)).fetchone())
        fence()
        return self.publish_view(feature, current, stamp)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--activation-file", type=Path)
    parser.add_argument("--activation-token")
    parser.add_argument("--expected-revision")
    parser.add_argument("--config-sha256")
    args = parser.parse_args()
    if args.activation_file:
        if not all((args.activation_token, args.expected_revision, args.config_sha256)):
            raise ValueError("managed synchronization requires complete activation identity")
        while True:
            try:
                marker = json.loads(args.activation_file.read_text())
            except FileNotFoundError:
                marker = {}
            if marker.get("token") == args.activation_token:
                break
            time.sleep(1)
        source = Path(__file__).resolve().parents[3]
        revision = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"],
                                           text=True).strip()
        config_hash = hashlib.sha256(args.config.read_bytes()).hexdigest()
        if revision != args.expected_revision or config_hash != args.config_sha256:
            raise ValueError("synchronizer source or configuration changed; reinstall the service")
    config = json.loads(args.config.read_text())
    if config.get("version") != 1 or not isinstance(config.get("owners"), list):
        raise ValueError("invalid Project synchronizer configuration")
    stores = [DeliveryStore(DeliveryConfig.load(Path(path))) for path in config["owners"]]
    synchronizer = ProjectSynchronizer(stores, interval=config.get("pr_interval_seconds", 300))
    logging.basicConfig(level=logging.INFO)
    with synchronizer.ownership():
        while True:
            try:
                result = synchronizer.tick()
                if args.once:
                    print(canonical_json(result))
                    return
            except Exception:
                logger.exception("Project synchronization tick failed; durable state retained")
                if args.once:
                    raise
            time.sleep(2)


if __name__ == "__main__":
    main()
