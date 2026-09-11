"""GitHub REST/GraphQL adapter through authenticated ``gh api`` argv calls.

All bodies travel over stdin. Runners follow subprocess.run's keyword contract.
Reads retry transient transport failures at most three times; writes are attempted
once and must reconcile through independent reads before they can be confirmed.
The application must persist an outbox intent before calling a mutating method.
Only allowlisted validation diagnostics are copied into adapter errors; never raw
response bodies, tokens or subprocess stderr.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import subprocess
import time
from datetime import date
from typing import Callable
from urllib.parse import quote

from devflow.errors import WorkflowError
from devflow.validation import digest

_COMMENT_FIELDS = """id databaseId:fullDatabaseId body path line originalLine subjectType
    commit { oid } originalCommit { oid }"""
_THREAD_FIELDS = """id isResolved path line originalLine diffSide"""
_PAGE_INFO = "pageInfo { hasNextPage endCursor }"


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]*", value):
        raise WorkflowError("invalid_identity", "A stable workflow identifier is required")
    return value


def _sha(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", value):
        raise WorkflowError("invalid_sha", "An exact Git object ID is required")
    return value


def _rejection_details(output: str, status: int) -> dict:
    """Expose useful schema diagnostics without echoing submitted/private values."""
    details = {"http_status": status}
    try:
        data = json.loads(output)
    except (TypeError, ValueError):
        return details
    if not isinstance(data, dict):
        return details
    fields = {"position", "positioning", "subject_type", "line", "side", "path",
              "commit_id", "body", "in_reply_to", "pull_request_review_id"}
    codes = {"missing", "missing_field", "invalid", "unprocessable", "already_exists",
             "custom", "not_permitted"}
    errors = data.get("errors", [])
    if not isinstance(errors, list):
        errors = []
    normalized = []
    for error in errors:
        if isinstance(error, dict):
            item = {key: error[key] for key, allowed in (("field", fields), ("code", codes))
                    if isinstance(error.get(key), str) and error[key] in allowed}
            if item:
                normalized.append(item)
    # GitHub's schema-union rejection sometimes uses a single message string.
    messages = [data.get("message", "")] + [
        error.get("message", "") if isinstance(error, dict) else error for error in errors
    ]
    combined = "\n".join(message for message in messages if isinstance(message, str))
    for field in sorted(fields):
        for phrase, code in (("wasn't supplied", "missing_field"),
                             ("is not a permitted key", "not_permitted")):
            if re.search(r"(?:[\"']?" + re.escape(field) + r"[\"']?) " + phrase,
                         combined):
                normalized.append({"field": field, "code": code})
    if normalized:
        details["validation_errors"] = normalized[:30]
    missing_position = any(e.get("field") in {"position", "positioning"}
                           and e.get("code") == "missing_field" for e in normalized)
    incompatible_line = any(e.get("field") in {"line", "subject_type"}
                            and e.get("code") == "not_permitted" for e in normalized)
    if status == 422 and missing_position and incompatible_line:
        details["position_compatibility_rejection"] = True
    return details


class GitHubRepository:
    def __init__(
        self, owner: str, name: str, runner: Callable | None = None, *, sleep: Callable = time.sleep
    ):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", owner) or not re.fullmatch(
            r"[A-Za-z0-9_.-]+", name
        ):
            raise WorkflowError("invalid_repository", "GitHub owner/name is invalid")
        self.owner, self.name = owner, name
        self.root = f"repos/{owner}/{name}"
        self.runner, self.sleep = runner or subprocess.run, sleep
        self._mutation_may_have_applied = False

    def _api(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        payload: dict | None = None,
        read_only: bool | None = None,
    ) -> object:
        readonly = method == "GET" if read_only is None else read_only
        previous_mutation = self._mutation_may_have_applied
        if not readonly:
            self._mutation_may_have_applied = True
        try:
            return self._api_request(
                endpoint, method=method, payload=payload, read_only=read_only
            )
        except WorkflowError as exc:
            # A definite rejection can establish nonexecution only for this call.
            # Earlier writes in a multi-step action still require reconciliation.
            if not readonly and exc.details.get("http_status") in {
                400, 401, 403, 404, 405, 409, 410, 422
            }:
                self._mutation_may_have_applied = previous_mutation
            if not self._mutation_may_have_applied:
                exc.details["no_mutation"] = True
            raise

    def _api_request(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        payload: dict | None = None,
        read_only: bool | None = None,
    ) -> object:
        readonly = method == "GET" if read_only is None else read_only
        argv = [
            "gh",
            "api",
            "--include",
            "--method",
            method,
            endpoint,
            "-H",
            "Accept: application/vnd.github+json",
            "-H",
            "X-GitHub-Api-Version: 2026-03-10",
        ]
        if payload is not None:
            argv += ["--input", "-"]
        for attempt in range(3 if readonly else 1):
            try:
                result = self.runner(
                    argv,
                    input=json.dumps(payload) if payload is not None else None,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=60,
                )
                output, headers, status = result.stdout, {}, None
                if output.startswith("HTTP/"):
                    header, _, output = output.replace("\r\n", "\n").partition("\n\n")
                    status = int(header.splitlines()[0].split()[1])
                    headers = {
                        k.lower().strip(): v.strip()
                        for k, v in (
                            line.split(":", 1) for line in header.splitlines()[1:] if ":" in line
                        )
                    }
                if result.returncode == 0 and (status is None or status < 400):
                    try:
                        data = json.loads(output) if output.strip() else None
                    except ValueError as exc:
                        code = "invalid_github_response" if readonly else "ambiguous_github_action"
                        raise WorkflowError(code, "GitHub returned an unreadable response") from exc
                    if isinstance(data, dict) and data.get("errors"):
                        if any(
                            error.get("type") in {"FORBIDDEN", "INSUFFICIENT_SCOPES"}
                            or "required scopes" in error.get("message", "").lower()
                            or "requires one of the following scopes"
                            in error.get("message", "").lower()
                            for error in data["errors"]
                        ):
                            raise WorkflowError(
                                "github_forbidden",
                                "GitHub token lacks required permission or scope",
                            )
                        raise WorkflowError(
                            "github_graphql_error" if readonly else "ambiguous_github_action",
                            "GitHub GraphQL request did not complete",
                        )
                    return data
                if status is None:
                    match = re.search(r"HTTP\s+(\d{3})", result.stderr or "")
                    status = int(match.group(1)) if match else None
                if status == 403:
                    raise WorkflowError(
                        "github_forbidden", "GitHub permission or policy denied the operation",
                        {"http_status": status},
                    )
                if status in {400, 401, 404, 405, 409, 410, 422}:
                    raise WorkflowError(
                        "github_rejected", "GitHub rejected the operation",
                        _rejection_details(output, status)
                    )
                retry_after = headers.get("retry-after", "")
                delay = float(retry_after) if retry_after.isdigit() else 2**attempt
                if not readonly:
                    raise WorkflowError(
                        "ambiguous_github_action",
                        "Mutation outcome is uncertain; readback required",
                    )
                if status not in {None, 429, 500, 502, 503, 504}:
                    raise WorkflowError("github_failed", "GitHub operation failed")
                if delay > 60:
                    raise WorkflowError(
                        "github_rate_limited",
                        "GitHub requested a later retry",
                        {"retry_after_seconds": delay},
                    )
            except (OSError, subprocess.TimeoutExpired) as exc:
                if not readonly:
                    raise WorkflowError(
                        "ambiguous_github_action", "Mutation transport interrupted; reconcile"
                    ) from exc
                delay = 2**attempt
            if attempt < 2:
                self.sleep(delay)
        raise WorkflowError(
            "github_transport", "GitHub read unavailable after three transport attempts"
        )

    def _pages(self, endpoint: str, key: str | None = None) -> list:
        result = []
        page = 1
        while True:
            sep = "&" if "?" in endpoint else "?"
            data = self._api(f"{endpoint}{sep}per_page=100&page={page}")
            items = data.get(key) if key and isinstance(data, dict) else data
            if not isinstance(items, list):
                raise WorkflowError(
                    "invalid_github_response", "Expected a paginated GitHub collection"
                )
            result.extend(items)
            if len(items) < 100:
                return result
            page += 1

    def _graphql(self, query: str, variables: dict, *, mutation: bool = False) -> dict:
        result = self._api(
            "graphql",
            method="POST",
            payload={"query": query, "variables": variables},
            read_only=not mutation,
        )
        if not isinstance(result, dict) or not isinstance(result.get("data"), dict):
            raise WorkflowError("invalid_github_response", "Expected GraphQL data")
        return result["data"]

    @staticmethod
    def _next_page(connection: dict, seen: set) -> str | None:
        info = connection["pageInfo"]
        if not info["hasNextPage"]:
            return None
        cursor = info["endCursor"]
        if not cursor or cursor in seen:
            raise WorkflowError("invalid_pagination", "GitHub pagination did not advance")
        seen.add(cursor)
        return cursor

    def issue(self, number: int) -> dict:
        return self._api(f"{self.root}/issues/{int(number)}")

    def issues(self, *, state: str = "all") -> list[dict]:
        if state not in {"open", "closed", "all"}:
            raise WorkflowError("invalid_state", "Invalid issue state")
        return [
            issue
            for issue in self._pages(f"{self.root}/issues?state={state}")
            if "pull_request" not in issue
        ]

    def backlog_creation_context(self) -> dict:
        repository, principal = self._api(self.root), self._api("user")
        if (
            not isinstance(repository, dict) or not isinstance(principal, dict)
            or repository.get("full_name", "").lower() != f"{self.owner}/{self.name}".lower()
            or type(repository.get("id")) is not int or not repository.get("node_id")
            or type(principal.get("id")) is not int or not principal.get("node_id")
        ):
            raise WorkflowError("backlog_identity", "Authenticated creation identity is unavailable")
        return {"repository_id": repository["id"], "repository_node_id": repository["node_id"],
                "creator_id": principal["id"], "creator_node_id": principal["node_id"]}

    @staticmethod
    def backlog_body(capture_id, body):
        return f"{body.rstrip()}\n\n<!-- devflow-backlog:{_identifier(capture_id)} -->"

    def _backlog_observation(self, issue, repository):
        creator = issue.get("user") or {}
        expected_root = f"https://api.github.com/{self.root}"
        if (
            "pull_request" in issue
            or issue.get("repository_url", "").lower() != expected_root.lower()
            or type(issue.get("number")) is not int or type(issue.get("id")) is not int
            or not issue.get("node_id") or type(creator.get("id")) is not int
            or not creator.get("node_id") or not isinstance(issue.get("title"), str)
            or "body" not in issue or (issue["body"] is not None and not isinstance(issue["body"], str))
            or not issue.get("updated_at")
            or type(repository.get("id")) is not int or not repository.get("node_id")
            or repository.get("full_name", "").lower() != f"{self.owner}/{self.name}".lower()
        ):
            raise WorkflowError("backlog_identity", "Readback is not an issue in the bound repository")
        return {
            "id": issue["id"], "node_id": issue["node_id"], "number": issue["number"],
            "url": issue["html_url"], "repository": f"github:{self.owner}/{self.name}",
            "repository_id": repository["id"], "repository_node_id": repository["node_id"],
            "creator_id": creator["id"], "creator_node_id": creator["node_id"],
            "consumed_digest": digest({"title": issue["title"], "body": issue["body"]}),
            "revision": issue["updated_at"], "origin": "unknown",
        }

    def reconcile_backlog_issue(
        self, capture_id: str, *, issue_number: int | None = None, expected: dict | None = None
    ) -> dict | None:
        """Markers correlate pending writes; they never establish origin or authority."""
        marker = f"<!-- devflow-backlog:{_identifier(capture_id)} -->"
        matches = (
            [self.issue(issue_number)] if issue_number is not None else
            [issue for issue in self.issues() if marker in (issue.get("body") or "")]
        )
        if len(matches) > 1:
            raise WorkflowError("duplicate_backlog", "More than one issue has this capture marker")
        if not matches:
            return None
        if issue_number is None and expected is None:
            raise WorkflowError("unverified_backlog", "Marker has no pending authenticated creation")
        issue = matches[0]
        observed = self._backlog_observation(issue, self._api(self.root))
        if issue_number is not None and observed["number"] != issue_number:
            raise WorkflowError("backlog_identity", "Readback differs from the exact issue")
        if expected is not None:
            if marker not in (issue["body"] or "") or any(
                observed[key] != expected[key] for key in (
                    "repository_id", "repository_node_id", "creator_id", "creator_node_id", "consumed_digest"
                )
            ):
                raise WorkflowError("backlog_creation_mismatch", "Pending creation creator or content differs")
            if expected.get("issue") and any(
                observed[key] != expected["issue"][key] for key in ("id", "node_id", "number")
            ):
                raise WorkflowError("backlog_creation_mismatch", "POST identity differs from exact readback")
        return {**observed, "capture_mode": "created_readback" if expected else "reused_unknown"}

    def create_backlog_issue(self, capture_id: str, *, title: str, body: str) -> dict:
        """Return POST identity for durable journaling before independent readback."""
        created = self._api(
            f"{self.root}/issues", method="POST",
            payload={"title": title, "body": self.backlog_body(capture_id, body)},
        )
        if not isinstance(created, dict) or any(
            not created.get(key) for key in ("id", "node_id", "number")
        ):
            raise WorkflowError("ambiguous_backlog", "POST identity unavailable; reconcile pending creation")
        return {key: created[key] for key in ("id", "node_id", "number")}

    def dependencies(self, number: int) -> list[dict]:
        return self._pages(f"{self.root}/issues/{int(number)}/dependencies/blocked_by")

    def pull_request(self, number: int) -> dict:
        return self._api(f"{self.root}/pulls/{int(number)}")

    def checks(self, sha: str) -> dict:
        sha = _sha(sha)
        return {
            "check_runs": self._pages(f"{self.root}/commits/{sha}/check-runs", "check_runs"),
            "statuses": self._pages(f"{self.root}/commits/{sha}/statuses"),
        }

    def reviews(self, number: int) -> list[dict]:
        return self._pages(f"{self.root}/pulls/{int(number)}/reviews")

    def review_threads(self, number: int) -> list[dict]:
        query = (
            """query($owner:String!,$name:String!,$number:Int!,$after:String) {
          repository(owner:$owner,name:$name) { pullRequest(number:$number) {
            reviewThreads(first:100,after:$after) { nodes { """
            + _THREAD_FIELDS
            + """
              comments(first:100) { nodes { """
            + _COMMENT_FIELDS
            + " } "
            + _PAGE_INFO
            + """ }
            } """
            + _PAGE_INFO
            + " } } } }"
        )
        threads, after, seen = [], None, set()
        while True:
            data = self._graphql(
                query,
                {"owner": self.owner, "name": self.name, "number": int(number), "after": after},
            )
            connection = data["repository"]["pullRequest"]["reviewThreads"]
            for thread in connection["nodes"]:
                comment_connection = thread["comments"]
                comments = list(comment_connection["nodes"])
                comment_seen = set()
                cursor = self._next_page(comment_connection, comment_seen)
                while cursor is not None:
                    nested = (
                        """query($id:ID!,$after:String) {
                      node(id:$id) { ... on PullRequestReviewThread {
                        comments(first:100,after:$after) { nodes { """
                        + _COMMENT_FIELDS
                        + " } "
                        + _PAGE_INFO
                        + " } } } }"
                    )
                    comment_connection = self._graphql(
                        nested, {"id": thread["id"], "after": cursor}
                    )["node"]["comments"]
                    comments.extend(comment_connection["nodes"])
                    cursor = self._next_page(comment_connection, comment_seen)
                threads.append(
                    thread
                    | {
                        "comments": [
                            comment | {"diffSide": thread.get("diffSide")} for comment in comments
                        ]
                    }
                )
            after = self._next_page(connection, seen)
            if after is None:
                return threads

    @staticmethod
    def finding_marker(finding_id: str) -> str:
        return f"<!-- devflow-finding:{_identifier(finding_id)} -->"

    def _find_publication(
        self,
        number: int,
        marker: str,
        body: str,
        path: str,
        line: int | None,
        side: str,
        expected_head: str,
    ) -> dict | None:
        matches = [
            (thread, comment)
            for thread in self.review_threads(number)
            for comment in thread["comments"]
            if marker in comment["body"]
        ]
        if len(matches) > 1:
            raise WorkflowError("duplicate_publication", "Finding marker occurs more than once")
        if not matches:
            return None
        thread, comment = matches[0]
        original = (comment.get("originalCommit") or comment.get("commit") or {}).get("oid")
        observed_line = comment.get("originalLine") or comment.get("line")
        if (
            comment["body"] != body
            or comment.get("path") != path
            or original != expected_head
            or observed_line != line
            or (line is not None and comment.get("diffSide") != side)
        ):
            raise WorkflowError(
                "publication_conflict", "Existing finding differs from its admitted body or anchor"
            )
        return {
            "status": "published",
            "thread_id": thread["id"],
            "comment_id": comment["databaseId"],
            "node_id": comment["id"],
            "head_sha": expected_head,
        }

    def reconcile_finding(
        self,
        number: int,
        *,
        finding_id: str,
        body: str,
        path: str,
        expected_head: str,
        line: int | None = None,
        side: str = "RIGHT",
    ) -> dict | None:
        """Read-only lost-write recovery; absence is not authorization to retry."""
        marker = self.finding_marker(finding_id)
        rendered = f"{body.rstrip()}\n\n{marker}"
        return self._find_publication(
            number, marker, rendered, path, line, side, _sha(expected_head)
        )

    @staticmethod
    def _anchor_position(file: dict, line: int, side: str) -> int | None:
        """Map exact side/line to GitHub's file-relative unified-diff position.

        Positions count every line after the first hunk header, including later
        hunk headers and no-newline markers. Removed lines belong only to LEFT.
        """
        old = new = None
        position = 0
        for entry in file.get("patch", "").splitlines():
            hunk = re.match(r"@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@", entry)
            if old is not None:
                position += 1
            if hunk:
                old, new = map(int, hunk.groups())
            elif old is not None:
                prefix = entry[:1]
                if prefix not in {"+", "-", " "}:
                    continue
                if side == "LEFT" and prefix != "+" and old == line:
                    return position
                if side == "RIGHT" and prefix != "-" and new == line:
                    return position
                old += prefix != "+"
                new += prefix != "-"
        return None

    @staticmethod
    def _valid_anchor(file: dict, line: int, side: str) -> bool:
        return GitHubRepository._anchor_position(file, line, side) is not None

    def publish_finding(
        self,
        number: int,
        *,
        finding_id: str,
        body: str,
        path: str,
        expected_head: str,
        line: int | None = None,
        side: str = "RIGHT",
    ) -> dict:
        expected_head = _sha(expected_head)
        marker = self.finding_marker(finding_id)
        if "<!-- devflow-" in body:
            raise WorkflowError(
                "invalid_public_body", "Body must not contain workflow correlation markers"
            )
        rendered = f"{body.rstrip()}\n\n{marker}"
        if side not in {"LEFT", "RIGHT"} or (
            line is not None and (type(line) is not int or line < 1)
        ):
            raise WorkflowError("blocked_anchor", "Invalid review anchor")
        existing = self._find_publication(number, marker, rendered, path, line, side, expected_head)
        if existing:
            return existing
        pr = self.pull_request(number)
        if pr["head"]["sha"] != expected_head or pr["state"] != "open":
            raise WorkflowError("stale_head", "PR head changed before finding publication")
        files = self._pages(f"{self.root}/pulls/{int(number)}/files")
        matching = [file for file in files if file["filename"] == path]
        if len(matching) != 1 or (
            line is not None and not self._valid_anchor(matching[0], line, side)
        ):
            raise WorkflowError("blocked_anchor", "Finding has no valid file or diff line anchor")
        payload = {
            "body": rendered,
            "commit_id": expected_head,
            "path": path,
            "subject_type": "file" if line is None else "line",
        }
        if line is not None:
            payload |= {"line": line, "side": side}
        # Recheck after retrieving a potentially long paginated diff.
        if self.pull_request(number)["head"]["sha"] != expected_head:
            raise WorkflowError("stale_head", "PR head changed while validating the anchor")
        try:
            self._api(f"{self.root}/pulls/{int(number)}/comments", method="POST", payload=payload)
        except WorkflowError as exc:
            if (line is not None and exc.details.get("no_mutation") is True
                    and exc.details.get("position_compatibility_rejection") is True):
                # Only a definite schema rejection permits a different wire form.
                # Re-read head and diff; never guess a position or duplicate an
                # uncertain first write. Context lines have no unique legacy side.
                current = self.pull_request(number)
                current_files = self._pages(f"{self.root}/pulls/{int(number)}/files")
                matches = [file for file in current_files if file["filename"] == path]
                if (current["head"]["sha"] != expected_head or current["state"] != "open"
                        or len(matches) != 1 or matches[0] != matching[0]
                        or self.pull_request(number)["head"]["sha"] != expected_head):
                    raise WorkflowError("stale_head", "PR diff changed before compatibility retry")
                position = self._anchor_position(matches[0], line, side)
                # Legacy position cannot express LEFT on unchanged context.
                entry = matches[0]["patch"].splitlines()[position]
                if side == "LEFT" and not entry.startswith("-"):
                    raise WorkflowError("blocked_anchor", "Legacy anchor cannot preserve LEFT context")
                fallback = {key: value for key, value in payload.items()
                            if key not in {"line", "side", "subject_type"}}
                fallback["position"] = position
                try:
                    self._api(f"{self.root}/pulls/{int(number)}/comments",
                              method="POST", payload=fallback)
                except WorkflowError as fallback_error:
                    if fallback_error.code != "ambiguous_github_action":
                        raise
            elif exc.code != "ambiguous_github_action":
                raise
        observed = self._find_publication(number, marker, rendered, path, line, side, expected_head)
        if observed is None:
            raise WorkflowError(
                "ambiguous_github_action",
                "Finding write is not confirmed; do not retry without reconciliation",
            )
        return observed

    def _thread(self, number: int, thread_id: str) -> dict:
        matches = [thread for thread in self.review_threads(number) if thread["id"] == thread_id]
        if len(matches) != 1:
            raise WorkflowError(
                "thread_missing", "Expected review thread was not independently observed"
            )
        return matches[0]

    def _contains(self, ancestor: str, descendant: str) -> bool:
        data = self._api(f"{self.root}/compare/{_sha(ancestor)}...{_sha(descendant)}")
        return (
            data["status"] in {"ahead", "identical"}
            and data["merge_base_commit"]["sha"] == ancestor
        )

    def reconcile_thread_closure(
        self,
        number: int,
        *,
        thread_id: str,
        finding_id: str,
        proof: str,
        expected_head: str,
        fix_commit: str,
    ) -> dict | None:
        """Read-only closure proof; a resolved flag without its exact fix reply is insufficient."""
        expected_head, fix_commit = _sha(expected_head), _sha(fix_commit)
        if self.pull_request(number)["head"]["sha"] != expected_head or not self._contains(
            fix_commit, expected_head
        ):
            raise WorkflowError(
                "unverified_fix_head", "Remote PR head does not contain the evaluated fix"
            )
        thread = self._thread(number, thread_id)
        marker = self.finding_marker(finding_id)
        if sum(marker in comment["body"] for comment in thread["comments"]) != 1:
            raise WorkflowError(
                "finding_thread_mismatch", "Thread does not uniquely contain this finding"
            )
        if "<!-- devflow-" in proof:
            raise WorkflowError("invalid_public_body", "Proof must not contain correlation markers")
        reply_marker = f"<!-- devflow-fix:{_identifier(finding_id)}:{fix_commit} -->"
        body = f"{proof.rstrip()}\n\nFix commit: {fix_commit}\n\n{reply_marker}"
        replies = [comment for comment in thread["comments"] if reply_marker in comment["body"]]
        if len(replies) > 1 or (replies and replies[0]["body"] != body):
            raise WorkflowError(
                "reply_conflict", "Observed proof reply differs from admitted proof"
            )
        if not thread["isResolved"] or not replies:
            return None
        if self.pull_request(number)["head"]["sha"] != expected_head:
            raise WorkflowError("stale_head", "PR head changed during closure readback")
        return {
            "status": "resolved",
            "thread_id": thread_id,
            "reply_id": replies[0]["databaseId"],
            "head_sha": expected_head,
            "fix_commit": fix_commit,
        }

    def close_thread(
        self,
        number: int,
        *,
        thread_id: str,
        finding_id: str,
        proof: str,
        expected_head: str,
        fix_commit: str,
    ) -> dict:
        """After domain fix/gate validation, verify containment, reply, resolve, read back."""
        expected_head, fix_commit = _sha(expected_head), _sha(fix_commit)
        pr = self.pull_request(number)
        if pr["head"]["sha"] != expected_head or not self._contains(fix_commit, expected_head):
            raise WorkflowError(
                "unverified_fix_head", "Remote PR head does not contain the evaluated fix"
            )
        marker = self.finding_marker(finding_id)
        thread = self._thread(number, thread_id)
        roots = [comment for comment in thread["comments"] if marker in comment["body"]]
        if len(roots) != 1:
            raise WorkflowError(
                "finding_thread_mismatch", "Thread does not uniquely contain this finding"
            )
        reply_marker = f"<!-- devflow-fix:{_identifier(finding_id)}:{fix_commit} -->"
        if "<!-- devflow-" in proof:
            raise WorkflowError("invalid_public_body", "Proof must not contain correlation markers")
        body = f"{proof.rstrip()}\n\nFix commit: {fix_commit}\n\n{reply_marker}"
        replies = [comment for comment in thread["comments"] if reply_marker in comment["body"]]
        if len(replies) > 1 or (replies and replies[0]["body"] != body):
            raise WorkflowError(
                "reply_conflict", "Existing proof reply differs from the admitted proof"
            )
        if not replies:
            try:
                self._api(
                    f"{self.root}/pulls/{int(number)}/comments/{roots[0]['databaseId']}/replies",
                    method="POST",
                    payload={"body": body},
                )
            except WorkflowError as exc:
                if exc.code != "ambiguous_github_action":
                    raise
            thread = self._thread(number, thread_id)
            replies = [comment for comment in thread["comments"] if comment["body"] == body]
            if len(replies) != 1:
                raise WorkflowError(
                    "ambiguous_github_action", "Proof reply is not independently confirmed"
                )
        if self.pull_request(number)["head"]["sha"] != expected_head:
            raise WorkflowError("stale_head", "PR head changed before thread resolution")
        if not thread["isResolved"]:
            mutation = """mutation($id:ID!) { resolveReviewThread(input:{threadId:$id}) {
                thread { id isResolved } } }"""
            try:
                self._graphql(mutation, {"id": thread_id}, mutation=True)
            except WorkflowError as exc:
                if exc.code != "ambiguous_github_action":
                    raise
        result = self.reconcile_thread_closure(
            number,
            thread_id=thread_id,
            finding_id=finding_id,
            proof=proof,
            expected_head=expected_head,
            fix_commit=fix_commit,
        )
        if result is None:
            raise WorkflowError(
                "ambiguous_github_action",
                "Thread resolution and proof were not independently confirmed",
            )
        return result

    def reconcile_status(self, sha: str, *, binding_hash: str, state: str) -> dict | None:
        """Read-only proof-status reconciliation against the latest context value."""
        sha = _sha(sha)
        if not re.fullmatch(r"[a-f0-9]{64}", binding_hash) or state not in {
            "pending",
            "success",
            "error",
            "failure",
        }:
            raise WorkflowError("invalid_proof_status", "Invalid proof binding or status")
        values = self._pages(f"{self.root}/commits/{sha}/statuses")
        current = next((value for value in values if value["context"] == "devflow/verified"), None)
        if (
            current
            and current["state"] == state
            and current["description"] == "devflow:" + binding_hash
        ):
            return current
        return None

    def publish_status(self, sha: str, *, binding_hash: str, state: str) -> dict:
        sha = _sha(sha)
        if not re.fullmatch(r"[a-f0-9]{64}", binding_hash) or state not in {
            "pending",
            "success",
            "error",
            "failure",
        }:
            raise WorkflowError("invalid_proof_status", "Invalid proof binding or status")
        description = "devflow:" + binding_hash

        value = self.reconcile_status(sha, binding_hash=binding_hash, state=state)
        if not value:
            try:
                self._api(
                    f"{self.root}/statuses/{sha}",
                    method="POST",
                    payload={
                        "state": state,
                        "context": "devflow/verified",
                        "description": description,
                    },
                )
            except WorkflowError as exc:
                if exc.code != "ambiguous_github_action":
                    raise
            value = self.reconcile_status(sha, binding_hash=binding_hash, state=state)
        if not value:
            raise WorkflowError("ambiguous_github_action", "Proof status publication not confirmed")
        return value

    def _protection(self, target_ref: str) -> dict:
        """Conservative supported profile: live classic enforcement, including admins.

        Ruleset-only repositories remain blocked until their actor/bypass and
        backend conformance adapter exists. A local profile flag grants nothing.
        """
        ref = quote(target_ref, safe="")
        rules = self._pages(f"{self.root}/rules/branches/{ref}")
        if any(rule.get("type") == "merge_queue" for rule in rules):
            raise WorkflowError(
                "merge_queue_unsupported", "Merge-group proof adapter is not available"
            )
        try:
            protection = self._api(f"{self.root}/branches/{ref}/protection")
        except WorkflowError as exc:
            if exc.code == "github_rejected" and exc.details.get("http_status") == 404:
                raise WorkflowError(
                    "merge_policy_unavailable", "Live classic branch enforcement is required"
                ) from exc
            raise
        required = protection.get("required_status_checks") or {}
        contexts = set(required.get("contexts", [])) | {
            c["context"] for c in required.get("checks", [])
        }
        if (
            required.get("strict") is not True
            or "devflow/verified" not in contexts
            or protection.get("enforce_admins", {}).get("enabled") is not True
        ):
            raise WorkflowError(
                "merge_policy_unavailable",
                "Strict required proof checks must be enforced for every actor",
            )
        return {"classic": protection, "active_rules": rules}

    def protection_snapshot(self, target_ref: str) -> dict:
        """Read and validate live enforcement before persisting a merge intent."""
        snapshot = self._protection(target_ref)
        return {"snapshot": snapshot, "hash": _digest(snapshot)}

    def _verify_checks(self, sha: str, protection: dict, binding_hash: str) -> None:
        observation = self.checks(sha)
        required = protection["classic"]["required_status_checks"]
        contexts = set(required.get("contexts", [])) | {
            c["context"] for c in required.get("checks", [])
        }
        statuses = {}
        for status in observation["statuses"]:  # GitHub supplies latest first.
            statuses.setdefault(status["context"], status)
        runs = {}
        for run in observation["check_runs"]:
            key = (run["name"], (run.get("app") or {}).get("id"))
            if key not in runs or run["id"] > runs[key]["id"]:
                runs[key] = run
        proof = statuses.get("devflow/verified")
        if (
            not proof
            or proof["state"] != "success"
            or proof["description"] != "devflow:" + binding_hash
        ):
            raise WorkflowError(
                "required_checks_pending",
                "Current source head lacks matching workflow proof status",
            )
        for context in contexts:
            app_ids = {
                check.get("app_id")
                for check in required.get("checks", [])
                if check["context"] == context
            }
            pinned = {app for app in app_ids if app not in (None, -1)}
            matching_runs = [
                run
                for (name, app), run in runs.items()
                if name == context and (not pinned or app in pinned)
            ]
            status = statuses.get(context) if not pinned else None
            if status is not None and status["state"] != "success":
                raise WorkflowError(
                    "required_checks_pending", "A required commit status has not succeeded"
                )
            if any(
                run["status"] != "completed"
                or run["conclusion"] not in {"success", "neutral", "skipped"}
                for run in matching_runs
            ):
                raise WorkflowError(
                    "required_checks_pending", "A required check run has not succeeded"
                )
            if not matching_runs and not (status and status["state"] == "success"):
                raise WorkflowError(
                    "required_checks_pending", "A required check is missing or from the wrong app"
                )

    def _merge_readback(
        self,
        number: int,
        *,
        expected_head: str,
        target_ref: str,
        target_sha: str,
        expected_tree: str,
        protection_hash: str,
        method: str,
    ) -> dict | None:
        pr = self.pull_request(number)
        if not pr.get("merged"):
            return None
        commit_sha = pr.get("merge_commit_sha")
        if not commit_sha:
            raise WorkflowError(
                "ambiguous_github_action", "Merged PR has no resulting commit identity"
            )
        commit = self._api(f"{self.root}/git/commits/{_sha(commit_sha)}")
        target = self._api(f"{self.root}/git/ref/heads/{quote(target_ref, safe='')}")["object"][
            "sha"
        ]
        reachable = self._contains(commit_sha, target)
        tree = commit["tree"]["sha"]
        verified = (
            tree == expected_tree
            and reachable
            and pr["head"]["sha"] == expected_head
            and pr["base"]["ref"] == target_ref
        )
        return {
            "status": "verified" if verified else "exposed_unverified",
            "pr_number": number,
            "head_sha": expected_head,
            "target_ref": target_ref,
            "target_sha": target_sha,
            "expected_integrated_tree": expected_tree,
            "protection_snapshot_hash": protection_hash,
            "merge_method": method,
            "commit_sha": commit_sha,
            "tree_sha": tree,
            "target_ancestry_verified": reachable,
            "observed_target_sha": target,
        }

    def reconcile_delivery(
        self,
        number: int,
        *,
        expected_head: str,
        target_ref: str,
        target_sha: str,
        expected_tree: str,
        protection_snapshot_hash: str,
        method: str = "merge",
    ) -> dict | None:
        """Read actual result using the original persisted policy snapshot identity."""
        if not re.fullmatch(r"[a-f0-9]{64}", protection_snapshot_hash):
            raise WorkflowError(
                "invalid_merge_binding", "Original protection snapshot hash is required"
            )
        return self._merge_readback(
            number,
            expected_head=_sha(expected_head),
            target_ref=target_ref,
            target_sha=_sha(target_sha),
            expected_tree=_sha(expected_tree),
            protection_hash=protection_snapshot_hash,
            method=method,
        )

    def deliver(
        self,
        number: int,
        *,
        expected_head: str,
        target_ref: str,
        target_sha: str,
        expected_tree: str,
        binding_hash: str,
        protection_snapshot_hash: str,
        method: str = "merge",
    ) -> dict:
        """Protected ordinary direct merge with expected SHA and actual result proof.

        Caller computes binding_hash from candidate/scope/policy/gates, persists
        intent and authorizes merge. This adapter only supplies remote observations.
        No queue, stack fallback, branch deletion, reset, release or policy mutation.
        """
        expected_head, target_sha, expected_tree = map(
            _sha, (expected_head, target_sha, expected_tree)
        )
        if method not in {"merge", "squash"}:
            raise WorkflowError(
                "merge_method_unsupported", "Only merge and squash have verified result mappings"
            )
        if (
            not target_ref
            or target_ref.startswith("refs/")
            or not re.fullmatch(r"[a-f0-9]{64}", binding_hash)
        ):
            raise WorkflowError(
                "invalid_merge_binding", "Target branch name and proof binding are required"
            )
        existing = self.reconcile_delivery(
            number,
            expected_head=expected_head,
            target_ref=target_ref,
            target_sha=target_sha,
            expected_tree=expected_tree,
            protection_snapshot_hash=protection_snapshot_hash,
            method=method,
        )
        if existing is not None:
            return existing
        protection = self._protection(target_ref)
        protection_hash = _digest(protection)
        if protection_hash != protection_snapshot_hash:
            raise WorkflowError(
                "merge_policy_changed", "Live enforcement differs from the persisted merge intent"
            )
        readback_args = dict(
            expected_head=expected_head,
            target_ref=target_ref,
            target_sha=target_sha,
            expected_tree=expected_tree,
            protection_hash=protection_hash,
            method=method,
        )
        pr = self.pull_request(number)
        if (
            pr["state"] != "open"
            or pr["head"]["sha"] != expected_head
            or pr["base"]["ref"] != target_ref
        ):
            raise WorkflowError("stale_head", "Source PR no longer matches the admitted merge")
        # Multi-layer selection must use a separately conformant atomic backend.
        if pr["base"]["repo"]["full_name"].lower() != f"{self.owner}/{self.name}".lower():
            raise WorkflowError(
                "merge_target_mismatch", "PR does not target the enrolled repository"
            )
        ref_endpoint = f"{self.root}/git/ref/heads/{quote(target_ref, safe='')}"
        if self._api(ref_endpoint)["object"]["sha"] != target_sha:
            raise WorkflowError(
                "stale_target", "Target advanced; integration proof must be recomputed"
            )
        if not self._contains(target_sha, expected_head):
            raise WorkflowError(
                "stale_integration", "Evaluated source must include the exact target ancestry"
            )
        source = self._api(f"{self.root}/git/commits/{expected_head}")
        if source["tree"]["sha"] != expected_tree:
            raise WorkflowError(
                "unverified_integration", "Expected integrated tree differs from evaluated source"
            )
        self._verify_checks(expected_head, protection, binding_hash)
        if _digest(self._protection(target_ref)) != protection_hash:
            raise WorkflowError(
                "merge_policy_changed", "Branch enforcement changed during admission"
            )
        if self.pull_request(number)["head"]["sha"] != expected_head:
            raise WorkflowError("stale_head", "Source changed immediately before merge")
        if self._api(ref_endpoint)["object"]["sha"] != target_sha:
            raise WorkflowError("stale_target", "Target changed immediately before merge")
        try:
            response = self._api(
                f"{self.root}/pulls/{int(number)}/merge",
                method="PUT",
                payload={"sha": expected_head, "merge_method": method},
            )
            if not response or not response.get("merged"):
                raise WorkflowError("merge_rejected", "GitHub did not confirm a completed merge")
        except WorkflowError as exc:
            if exc.code != "ambiguous_github_action":
                raise
        result = self._merge_readback(number, **readback_args)
        if result is None:
            raise WorkflowError(
                "ambiguous_github_action",
                "Merge outcome unconfirmed; reconcile before another mutation",
            )
        return result

    def deliver_stack(self, *args, **kwargs) -> dict:
        raise WorkflowError(
            "stack_conformance_unavailable",
            "Atomic stack delivery requires enrolled backend per-head conformance evidence",
        )

    @staticmethod
    def _public_text(text: str, *, title: bool = False) -> str:
        if not isinstance(text, str) or (title and not text.strip()) or "<!-- devflow-" in text:
            raise WorkflowError(
                "invalid_public_body", "Public content has an invalid title or reserved marker"
            )
        return text.rstrip()

    @staticmethod
    def _branch(ref: str) -> str:
        if not isinstance(ref, str) or not ref or ref.startswith(("-", "refs/")):
            raise WorkflowError("invalid_ref", "Explicit branch or tag name is required")
        return ref

    def _remote_branch(self, ref: str) -> str:
        return _sha(
            self._api(f"{self.root}/git/ref/heads/{quote(self._branch(ref), safe='')}")["object"][
                "sha"
            ]
        )

    def _pr_publication(
        self,
        *,
        head_ref: str,
        base_ref: str,
        expected_head: str,
        title: str,
        body: str,
        action_id: str,
    ) -> tuple[dict | None, list[dict]]:
        marker = f"<!-- devflow-pr:{_identifier(action_id)} -->"
        rendered = f"{self._public_text(body)}\n\n{marker}"
        title = self._public_text(title, title=True)
        endpoint = (
            f"{self.root}/pulls?state=all&head={quote(self.owner + ':' + self._branch(head_ref), safe='')}"
            f"&base={quote(self._branch(base_ref), safe='')}"
        )
        prs = self._pages(endpoint)
        matches = [pr for pr in prs if marker in (pr.get("body") or "")]
        if len(matches) > 1:
            raise WorkflowError("duplicate_publication", "PR action marker occurs more than once")
        if not matches:
            return None, prs
        pr = self.pull_request(matches[0]["number"])
        repository = f"{self.owner}/{self.name}".lower()
        if (
            pr.get("body") != rendered
            or pr["title"] != title
            or pr["head"]["ref"] != head_ref
            or pr["base"]["ref"] != base_ref
            or (pr["head"].get("repo") or {}).get("full_name", "").lower() != repository
            or (pr["base"].get("repo") or {}).get("full_name", "").lower() != repository
            or pr.get("draft") is not False
        ):
            raise WorkflowError(
                "pr_publication_conflict",
                "Published PR differs from its admitted identity or content",
            )
        if pr["head"]["sha"] != expected_head:
            raise WorkflowError(
                "stale_head",
                "Published PR head changed; candidate must be reconciled",
                {"pr_number": pr["number"], "head_sha": pr["head"]["sha"]},
            )
        if pr["state"] != "open" and not pr.get("merged"):
            raise WorkflowError(
                "pr_publication_closed",
                "Published PR was closed without merging",
                {"pr_number": pr["number"]},
            )
        return {
            "status": "published",
            "pr_number": pr["number"],
            "node_id": pr["node_id"],
            "url": pr["html_url"],
            "head_ref": head_ref,
            "head_sha": expected_head,
            "base_ref": base_ref,
            "action_marker": marker,
            "draft": False,
        }, prs

    def reconcile_pr(
        self,
        *,
        head_ref: str,
        base_ref: str,
        expected_head: str,
        title: str,
        body: str,
        action_id: str,
    ) -> dict | None:
        """Read-only PR creation reconciliation by marker, exact refs and repository."""
        observed, _ = self._pr_publication(
            head_ref=head_ref,
            base_ref=base_ref,
            expected_head=_sha(expected_head),
            title=title,
            body=body,
            action_id=action_id,
        )
        return observed

    def publish_pr(
        self,
        *,
        head_ref: str,
        base_ref: str,
        expected_head: str,
        title: str,
        body: str,
        action_id: str,
    ) -> dict:
        """Publish one regular same-repository PR; caller has already pushed the exact head."""
        expected_head = _sha(expected_head)
        if self._branch(head_ref) == self._branch(base_ref):
            raise WorkflowError("invalid_pr_binding", "PR source and target branches must differ")
        args = dict(
            head_ref=head_ref,
            base_ref=base_ref,
            expected_head=expected_head,
            title=title,
            body=body,
            action_id=action_id,
        )
        existing, prs = self._pr_publication(**args)
        if existing is not None:
            return existing
        if any(pr["state"] == "open" for pr in prs):
            raise WorkflowError(
                "pr_publication_conflict", "An existing open PR is not bound to this action"
            )
        if self._remote_branch(head_ref) != expected_head:
            raise WorkflowError(
                "stale_head", "Remote source branch differs from the admitted candidate"
            )
        self._remote_branch(base_ref)  # Confirm target exists before creation.
        marker = f"<!-- devflow-pr:{_identifier(action_id)} -->"
        payload = {
            "head": head_ref,
            "base": base_ref,
            "title": self._public_text(title, title=True),
            "body": f"{self._public_text(body)}\n\n{marker}",
            "draft": False,
        }
        try:
            self._api(f"{self.root}/pulls", method="POST", payload=payload)
        except WorkflowError as exc:
            if exc.code != "ambiguous_github_action":
                raise
        observed = self.reconcile_pr(**args)
        if observed is None:
            raise WorkflowError(
                "ambiguous_github_action", "PR publication is not independently confirmed"
            )
        return observed

    def tag_commit(self, tag: str) -> str:
        """Resolve an existing remote lightweight/annotated tag without creating or moving it."""
        obj = self._api(f"{self.root}/git/ref/tags/{quote(self._branch(tag), safe='')}")["object"]
        seen = set()
        while obj.get("type") == "tag":
            sha = _sha(obj["sha"])
            if sha in seen or len(seen) >= 100:
                raise WorkflowError("invalid_tag", "Remote tag dereference did not terminate")
            seen.add(sha)
            obj = self._api(f"{self.root}/git/tags/{sha}")["object"]
        if obj.get("type") != "commit":
            raise WorkflowError("invalid_tag", "Release tag must resolve to a commit")
        return _sha(obj["sha"])

    def reconcile_release(
        self, *, tag: str, expected_sha: str, title: str, notes: str, action_id: str
    ) -> dict | None:
        """Read-only release reconciliation against its tag and stable public marker."""
        expected_sha = _sha(expected_sha)
        marker = f"<!-- devflow-release:{_identifier(action_id)} -->"
        rendered = f"{self._public_text(notes)}\n\n{marker}"
        title = self._public_text(title, title=True)
        try:
            release = self._api(f"{self.root}/releases/tags/{quote(self._branch(tag), safe='')}")
        except WorkflowError as exc:
            if exc.code == "github_rejected" and exc.details.get("http_status") == 404:
                return None
            raise
        if (
            release.get("body") != rendered
            or release["name"] != title
            or release["tag_name"] != tag
            or release.get("draft") is not False
            or release.get("prerelease") is not False
        ):
            raise WorkflowError(
                "release_publication_conflict",
                "Existing release is not the admitted action/content",
            )
        actual = self.tag_commit(tag)
        return {
            "status": "published" if actual == expected_sha else "exposed_unverified",
            "release_id": release["id"],
            "node_id": release["node_id"],
            "url": release["html_url"],
            "tag": tag,
            "commit_sha": actual,
            "expected_sha": expected_sha,
            "action_marker": marker,
            "draft": False,
        }

    def publish_release(
        self, *, tag: str, expected_sha: str, title: str, notes: str, action_id: str
    ) -> dict:
        """Publish a regular release for an existing exact tag, without tag/asset commands.

        GitHub has no atomic tag-exists precondition on release creation. The API can
        recreate a tag deleted concurrently; target_commitish binds that fallback to
        the admitted SHA. Tag identity is read before and after, and a changed target
        is exposed_unverified. Repositories requiring atomic tag-existence enforcement
        must use an external no-delete tag rule; this method never changes such rules.
        """
        expected_sha = _sha(expected_sha)
        args = dict(
            tag=tag, expected_sha=expected_sha, title=title, notes=notes, action_id=action_id
        )
        existing = self.reconcile_release(**args)
        if existing is not None:
            return existing
        if self.tag_commit(tag) != expected_sha:
            raise WorkflowError(
                "release_tag_mismatch", "Existing remote tag does not match the admitted candidate"
            )
        marker = f"<!-- devflow-release:{_identifier(action_id)} -->"
        payload = {
            "tag_name": tag,
            "target_commitish": expected_sha,
            "name": self._public_text(title, title=True),
            "body": f"{self._public_text(notes)}\n\n{marker}",
            "draft": False,
            "prerelease": False,
            "generate_release_notes": False,
        }
        try:
            self._api(f"{self.root}/releases", method="POST", payload=payload)
        except WorkflowError as exc:
            if exc.code != "ambiguous_github_action":
                raise
        observed = self.reconcile_release(**args)
        if observed is None:
            raise WorkflowError(
                "ambiguous_github_action", "Release publication is not independently confirmed"
            )
        return observed

    def project_fields(self, project_node_id: str) -> list[dict]:
        if not isinstance(project_node_id, str) or not project_node_id.strip():
            raise WorkflowError("project_binding_missing", "Enrolled Project node ID is required")
        query = (
            """query($id:ID!,$after:String) { node(id:$id) { ... on ProjectV2 {
          fields(first:100,after:$after) { nodes {
            ... on ProjectV2Field { id name dataType }
            ... on ProjectV2SingleSelectField { id name dataType options { id name } }
            ... on ProjectV2IterationField { id name dataType }
            ... on ProjectV2MultiSelectField { id name dataType }
          } """
            + _PAGE_INFO
            + " } } } }"
        )
        fields, after, seen = [], None, set()
        while True:
            node = self._graphql(query, {"id": project_node_id, "after": after}).get("node")
            if not node or "fields" not in node:
                raise WorkflowError(
                    "project_unavailable", "Project fields are inaccessible; binding is required"
                )
            connection = node["fields"]
            fields.extend(connection["nodes"])
            after = self._next_page(connection, seen)
            if after is None:
                return fields

    def _project_graphql(self, query: str, variables: dict, *, mutation: bool = False) -> dict:
        try:
            return self._graphql(query, variables, mutation=mutation)
        except WorkflowError as exc:
            if exc.code == "github_forbidden":
                raise WorkflowError(
                    "project_permission_missing", "Project read/write scope is not granted",
                    exc.details,
                ) from exc
            raise

    def project_items(self, project_id: str) -> list[dict]:
        """Read every item and field-value page, retaining explicit content and field IDs."""
        if not isinstance(project_id, str) or not project_id.strip():
            raise WorkflowError("project_binding_missing", "Enrolled Project node ID is required")
        field_id = """field { ... on ProjectV2Field { id }
            ... on ProjectV2SingleSelectField { id } ... on ProjectV2IterationField { id }
            ... on ProjectV2MultiSelectField { id } }"""
        values = (
            """__typename
            ... on ProjectV2ItemFieldTextValue { text """
            + field_id
            + """ }
            ... on ProjectV2ItemFieldNumberValue { number """
            + field_id
            + """ }
            ... on ProjectV2ItemFieldDateValue { date """
            + field_id
            + """ }
            ... on ProjectV2ItemFieldSingleSelectValue { optionId """
            + field_id
            + """ }
            ... on ProjectV2ItemFieldIterationValue { iterationId """
            + field_id
            + """ }"""
        )
        query = (
            """query($id:ID!,$after:String) { node(id:$id) { ... on ProjectV2 {
          id items(first:100,after:$after) { nodes { id isArchived
            content { __typename ... on Issue { id } ... on PullRequest { id }
                      ... on DraftIssue { id } }
            fieldValues(first:100) { nodes { """
            + values
            + " } "
            + _PAGE_INFO
            + """ }
          } """
            + _PAGE_INFO
            + " } } } }"
        )
        result, after, seen = [], None, set()
        while True:
            node = self._project_graphql(query, {"id": project_id, "after": after}).get("node")
            if not node or node.get("id") != project_id or "items" not in node:
                raise WorkflowError(
                    "project_unavailable", "Enrolled Project is missing or inaccessible"
                )
            connection = node["items"]
            for item in connection["nodes"]:
                field_connection = item["fieldValues"]
                fields, field_seen = list(field_connection["nodes"]), set()
                cursor = self._next_page(field_connection, field_seen)
                while cursor is not None:
                    nested = (
                        """query($id:ID!,$after:String) { node(id:$id) {
                      ... on ProjectV2Item { fieldValues(first:100,after:$after) {
                        nodes { """
                        + values
                        + " } "
                        + _PAGE_INFO
                        + " } } } }"
                    )
                    field_node = self._project_graphql(
                        nested, {"id": item["id"], "after": cursor}
                    ).get("node")
                    if not field_node or "fieldValues" not in field_node:
                        raise WorkflowError(
                            "project_item_unavailable", "Project item vanished during pagination"
                        )
                    field_connection = field_node["fieldValues"]
                    fields.extend(field_connection["nodes"])
                    cursor = self._next_page(field_connection, field_seen)
                result.append(item | {"fieldValues": fields})
            after = self._next_page(connection, seen)
            if after is None:
                return result

    def _project_item(self, project_id: str, content_id: str) -> dict | None:
        matches = [
            item
            for item in self.project_items(project_id)
            if (item.get("content") or {}).get("id") == content_id
        ]
        if len(matches) > 1:
            raise WorkflowError(
                "project_item_conflict", "Project contains duplicate items for the content identity"
            )
        if matches and matches[0].get("isArchived"):
            raise WorkflowError(
                "project_item_archived",
                "Existing Project item is archived; explicit restoration is required",
            )
        return matches[0] if matches else None

    @staticmethod
    def _project_values(item: dict) -> dict:
        observed = {}
        keys = {
            "text": "text",
            "number": "number",
            "date": "date",
            "optionId": "singleSelectOptionId",
            "iterationId": "iterationId",
        }
        for entry in item["fieldValues"]:
            field_id = (entry.get("field") or {}).get("id")
            if not field_id:
                continue  # Unmanaged field kinds remain untouched on GitHub.
            if field_id in observed:
                raise WorkflowError(
                    "project_field_conflict", "Project item has duplicate values for a field"
                )
            observed[field_id] = {
                target: entry[source]
                for source, target in keys.items()
                if source in entry and entry[source] is not None
            }
        return observed

    def _validate_project_updates(
        self, project_id: str, field_updates: dict, expected_owned_fields: dict
    ) -> None:
        if not isinstance(field_updates, dict) or not isinstance(expected_owned_fields, dict):
            raise WorkflowError(
                "project_binding_missing", "Explicit owned field IDs and types are required"
            )
        if not set(field_updates) <= set(expected_owned_fields):
            raise WorkflowError(
                "project_field_not_owned",
                "Projection attempted to update a field outside its manifest",
            )
        try:
            fields = {field["id"]: field for field in self.project_fields(project_id)}
        except WorkflowError as exc:
            if exc.code == "github_forbidden":
                raise WorkflowError(
                    "project_permission_missing", "Project read/write scope is not granted",
                    exc.details,
                ) from exc
            raise
        supported = {
            "TEXT": "text",
            "NUMBER": "number",
            "DATE": "date",
            "SINGLE_SELECT": "singleSelectOptionId",
        }
        for field_id, value in field_updates.items():
            kind = expected_owned_fields[field_id]
            if kind not in supported:
                raise WorkflowError(
                    "project_field_type_unsupported",
                    "Enrolled field type is not supported for writes",
                )
            field = fields.get(field_id)
            if not field or field.get("dataType") != kind:
                raise WorkflowError(
                    "project_binding_changed",
                    "Enrolled field ID/type does not match the live Project",
                )
            key = supported[kind]
            if not isinstance(value, dict) or set(value) != {key}:
                raise WorkflowError(
                    "invalid_project_value", "Project value must use exactly its enrolled typed key"
                )
            typed = value[key]
            if kind == "NUMBER":
                if type(typed) not in {int, float} or not math.isfinite(typed):
                    raise WorkflowError("invalid_project_value", "Project number must be finite")
            elif not isinstance(typed, str):
                raise WorkflowError(
                    "invalid_project_value", "Project field requires a string value"
                )
            elif kind == "DATE":
                try:
                    if date.fromisoformat(typed).isoformat() != typed:
                        raise ValueError("noncanonical date")
                except ValueError as exc:
                    raise WorkflowError(
                        "invalid_project_value", "Project date must use YYYY-MM-DD"
                    ) from exc
            elif kind == "SINGLE_SELECT" and typed not in {
                option["id"] for option in field.get("options", [])
            }:
                raise WorkflowError(
                    "project_binding_changed",
                    "Enrolled single-select option is absent from the field",
                )

    def reconcile_project_item(
        self, *, project_id: str, content_id: str, field_updates: dict, action_id: str
    ) -> dict | None:
        """Read-only reconciliation; partial projection is not reported synchronized."""
        item = self._project_item(project_id, content_id)
        if item is None:
            return None
        observed = self._project_values(item)
        if any(observed.get(field_id) != value for field_id, value in field_updates.items()):
            return None
        return {
            "status": "synchronized",
            "action_id": action_id,
            "project_id": project_id,
            "content_id": content_id,
            "item_id": item["id"],
            "field_values": {field_id: observed[field_id] for field_id in field_updates},
            "observations": [
                "Project content identity and all requested owned field values independently read back"
            ],
        }

    def sync_project_item(
        self,
        *,
        project_id: str,
        content_id: str,
        field_updates: dict,
        expected_owned_fields: dict,
        action_id: str,
    ) -> dict:
        """Add/reconcile content and update only enrolled field IDs, without touching prose.

        Values use GraphQL typed shapes, e.g. {field_id: {"singleSelectOptionId": option_id}};
        expected_owned_fields maps manifest-owned IDs to TEXT/NUMBER/DATE/SINGLE_SELECT.
        Persist an outbox intent before invocation. Existing item identity and per-field
        readback make restart safe after partial progress; ambiguous writes are not retried.
        """
        _identifier(action_id)
        if not isinstance(content_id, str) or not content_id.strip():
            raise WorkflowError(
                "project_binding_missing", "Bound issue/PR content node ID is required"
            )
        query = """query($id:ID!) { node(id:$id) { __typename
          ... on Issue { id repository { nameWithOwner } }
          ... on PullRequest { id repository { nameWithOwner } } } }"""
        content = self._project_graphql(query, {"id": content_id}).get("node")
        if not content or content.get("__typename") not in {"Issue", "PullRequest"}:
            raise WorkflowError(
                "project_content_missing", "Bound content node is not an accessible issue or PR"
            )
        if (
            content.get("id") != content_id
            or content["repository"]["nameWithOwner"].lower() != f"{self.owner}/{self.name}".lower()
        ):
            raise WorkflowError(
                "project_content_mismatch", "Content does not belong to the enrolled repository"
            )
        self._validate_project_updates(project_id, field_updates, expected_owned_fields)
        item = self._project_item(project_id, content_id)
        if item is None:
            mutation = """mutation($project:ID!,$content:ID!,$action:String!) {
              addProjectV2ItemById(input:{projectId:$project,contentId:$content,clientMutationId:$action}) {
                item { id } clientMutationId } }"""
            try:
                self._project_graphql(
                    mutation,
                    {"project": project_id, "content": content_id, "action": action_id},
                    mutation=True,
                )
            except WorkflowError as exc:
                if exc.code != "ambiguous_github_action":
                    raise
            item = self._project_item(project_id, content_id)
            if item is None:
                raise WorkflowError(
                    "ambiguous_github_action",
                    "Project item addition is not independently confirmed",
                )
        for field_id, value in field_updates.items():
            # Refresh item state before each write to preserve/reuse partial progress.
            item = self._project_item(project_id, content_id)
            if item is None:
                raise WorkflowError(
                    "project_item_unavailable", "Project item disappeared before field update"
                )
            if self._project_values(item).get(field_id) == value:
                continue
            mutation = """mutation($project:ID!,$item:ID!,$field:ID!,$value:ProjectV2FieldValue!,$action:String!) {
              updateProjectV2ItemFieldValue(input:{projectId:$project,itemId:$item,fieldId:$field,
                value:$value,clientMutationId:$action}) { projectV2Item { id } clientMutationId } }"""
            try:
                self._project_graphql(
                    mutation,
                    {
                        "project": project_id,
                        "item": item["id"],
                        "field": field_id,
                        "value": value,
                        "action": action_id + ":" + _digest(field_id),
                    },
                    mutation=True,
                )
            except WorkflowError as exc:
                if exc.code != "ambiguous_github_action":
                    raise
            observed = self._project_item(project_id, content_id)
            if observed is None or self._project_values(observed).get(field_id) != value:
                raise WorkflowError(
                    "ambiguous_github_action", "Project field write is not independently confirmed"
                )
        result = self.reconcile_project_item(
            project_id=project_id,
            content_id=content_id,
            field_updates=field_updates,
            action_id=action_id,
        )
        if result is None:
            raise WorkflowError(
                "project_projection_changed", "Project projection changed before final readback"
            )
        return result

    def sync_issue_summary(self, number: int, *, expected_body: str, summary: str) -> dict:
        """Replace only one tool-owned summary region, preserving the accepted issue.

        GitHub issue PATCH has no documented compare-and-swap body precondition;
        automatic whole-body updates cannot safely preserve concurrent human edits.
        Return a concrete proposal for native owner review instead of overwriting.
        """
        issue = self.issue(number)
        if (issue.get("body") or "") != expected_body:
            raise WorkflowError(
                "issue_scope_conflict", "Issue changed since the accepted source revision"
            )
        start, end = "<!-- devflow-summary:start -->", "<!-- devflow-summary:end -->"
        if start in summary or end in summary:
            raise WorkflowError("invalid_summary", "Summary contains reserved delimiters")
        if expected_body.count(start) != expected_body.count(end) or expected_body.count(start) > 1:
            raise WorkflowError(
                "summary_conflict", "Issue contains malformed workflow summary boundaries"
            )
        region = f"{start}\n{summary}\n{end}"
        if start in expected_body:
            first, last = expected_body.index(start), expected_body.index(end)
            if last < first:
                raise WorkflowError("summary_conflict", "Issue summary boundaries are reversed")
            proposed = expected_body[:first] + region + expected_body[last + len(end) :]
        else:
            proposed = expected_body.rstrip() + "\n\n" + region
        return {
            "status": "prepared",
            "issue_node_id": issue["node_id"],
            "number": number,
            "expected_body_hash": _digest(expected_body),
            "proposed_body": proposed,
            "blocker": "issue_body_compare_and_swap_unavailable",
        }
