"""GitHub-owned delivery plans and explicit stack identities.

A dedicated issue comment holds the accepted plan and publication binding. A
single issue label points to that comment by ID. Updating the delivery record
never overwrites the human-owned issue body. Bound records are fetched directly;
comment listing is limited to recovery of an uncertain initial create operation.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from copy import deepcopy
from urllib.parse import quote, urlsplit

from .contracts import canonical_json, digest
from .delivery_execution_registry import OwnershipConflict, UnresolvedEffect

MARKER = "<!-- devflow-delivery:v1 -->"
LABEL_PREFIX = "devflow-plan-"
IDENTIFIER = re.compile(r"[a-z][a-z0-9_-]{0,63}")
SHA = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")


class GitHubContractError(RuntimeError):
    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


def issue_identity(url: str, repository: str) -> tuple[str, int]:
    if not isinstance(url, str) or not isinstance(repository, str):
        raise ValueError("delivery issue requires a GitHub URL and repository")
    parsed = urlsplit(url)
    parts = parsed.path.strip("/").split("/")
    if (
        parsed.scheme != "https"
        or parsed.netloc.casefold() != "github.com"
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or len(parts) != 4
        or parts[2] != "issues"
        or not parts[3].isdecimal()
        or int(parts[3]) < 1
        or "/".join(parts[:2]).casefold() != repository.casefold()
    ):
        raise ValueError("delivery issue must belong to its configured GitHub repository")
    return "/".join(parts[:2]), int(parts[3])


def _text(value, label, maximum=8192):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum or "\x00" in value:
        raise ValueError(label + " must be nonempty bounded text")
    return value


def _strings(value, label, maximum=64):
    if not isinstance(value, list) or not 1 <= len(value) <= maximum:
        raise ValueError(label + " must be a bounded nonempty list")
    return [_text(item, label) for item in value]


def validate_plan(value: dict, *, allowed_paths: list[str] | None = None) -> dict:
    """Validate business decomposition without accepting new execution authority."""
    if not isinstance(value, dict) or set(value) != {"scope", "acceptance", "workstreams"}:
        raise ValueError("delivery plan requires scope, acceptance and workstreams")
    _text(value["scope"], "feature scope")
    _strings(value["acceptance"], "feature acceptance")
    streams = value["workstreams"]
    if not isinstance(streams, list) or not 1 <= len(streams) <= 16:
        raise ValueError("delivery plan requires between one and sixteen workstreams")
    stream_ids, chunks = set(), {}
    issue_numbers = set()
    required_stream = {"id", "title", "issue_number", "acceptance", "chunks"}
    required_chunk = {
        "id",
        "title",
        "scope",
        "steps",
        "verification",
        "acceptance",
        "allowed_paths",
        "depends_on",
    }
    for stream in streams:
        if not isinstance(stream, dict) or set(stream) != required_stream:
            raise ValueError("workstream fields do not match the delivery contract")
        ident = stream["id"]
        if not isinstance(ident, str) or not IDENTIFIER.fullmatch(ident) or ident in stream_ids:
            raise ValueError("workstream ID is invalid or duplicated")
        stream_ids.add(ident)
        _text(stream["title"], "workstream title", 200)
        _strings(stream["acceptance"], "workstream acceptance")
        number = stream["issue_number"]
        if number is not None and (type(number) is not int or number < 1):
            raise ValueError("workstream issue number must be positive or null")
        if number is not None:
            if number in issue_numbers:
                raise ValueError("a sub-issue can own only one workstream")
            issue_numbers.add(number)
        if not isinstance(stream["chunks"], list) or not 1 <= len(stream["chunks"]) <= 32:
            raise ValueError("workstream requires a bounded nonempty chunk list")
        previous = None
        for chunk in stream["chunks"]:
            if not isinstance(chunk, dict) or set(chunk) != required_chunk:
                raise ValueError("chunk fields do not match the delivery contract")
            key = chunk["id"]
            if not isinstance(key, str) or not IDENTIFIER.fullmatch(key) or key in chunks:
                raise ValueError("chunk ID is invalid or duplicated")
            for field in ("title", "scope"):
                _text(chunk[field], "chunk " + field, 200 if field == "title" else 8192)
            for field in ("steps", "verification", "acceptance", "allowed_paths"):
                _strings(chunk[field], "chunk " + field)
            paths = chunk["allowed_paths"]
            if len(set(paths)) != len(paths):
                raise ValueError("chunk paths must be unique")
            for path in paths:
                if (
                    path.startswith("/")
                    or "\\" in path
                    or ".." in path.split("/")
                    or any(part in {".git", ".codex", ".agents"} for part in path.split("/"))
                    or path in {"", "."}
                ):
                    raise ValueError("chunk path is outside an owned source scope")
            if allowed_paths is not None and set(paths) - set(allowed_paths):
                raise ValueError("delivery plan exceeds the configured source scope")
            dependencies = chunk["depends_on"]
            if (
                not isinstance(dependencies, list)
                or len(dependencies) > 32
                or any(not isinstance(item, str) for item in dependencies)
                or len(set(dependencies)) != len(dependencies)
                or key in dependencies
            ):
                raise ValueError("chunk dependencies are invalid")
            if previous and previous not in dependencies:
                raise ValueError("chunks in a workstream must declare their sequential dependency")
            chunks[key] = chunk
            previous = key
    if len(chunks) > 32:
        raise ValueError("feature exceeds the thirty-two chunk bound")
    for chunk in chunks.values():
        if set(chunk["depends_on"]) - chunks.keys():
            raise ValueError("chunk depends on an unknown feature chunk")
    ready = set()
    while len(ready) < len(chunks):
        next_items = {
            key
            for key, chunk in chunks.items()
            if key not in ready and set(chunk["depends_on"]) <= ready
        }
        if not next_items:
            raise ValueError("chunk dependency graph contains a cycle")
        ready |= next_items
    return deepcopy(value)


def ordered_chunks(plan: dict) -> list[dict]:
    """A stable topological order; independent work can build concurrently."""
    validate_plan(plan)
    pending = [
        {**chunk, "workstream_id": stream["id"], "issue_number": stream["issue_number"]}
        for stream in plan["workstreams"]
        for chunk in stream["chunks"]
    ]
    done, result = set(), []
    while pending:
        selected = next(chunk for chunk in pending if set(chunk["depends_on"]) <= done)
        result.append(selected)
        done.add(selected["id"])
        pending.remove(selected)
    return result


def validate_manifest(value: dict, issue: dict) -> dict:
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "version",
            "issue_id",
            "repository_id",
            "revision",
            "creation_key",
            "plan",
            "workstream_issues",
            "publication",
        }
        or type(value["version"]) is not int
        or value["version"] != 1
        or value["issue_id"] != issue["id"]
        or value["repository_id"] != issue["repository_id"]
        or type(value["revision"]) is not int
        or value["revision"] < 1
    ):
        raise ValueError("GitHub delivery record has a different identity or schema")
    _text(value["creation_key"], "creation key", 128)
    plan = validate_plan(value["plan"])
    streams = {stream["id"] for stream in plan["workstreams"]}
    bindings = value["workstream_issues"]
    if not isinstance(bindings, dict) or bindings.keys() - streams:
        raise ValueError("workstream issue binding is invalid")
    owners = {stream["id"]: stream["issue_number"] for stream in plan["workstreams"]}
    bound_ids, bound_numbers = set(), set()
    for stream_id, binding in bindings.items():
        if (
            not isinstance(binding, dict)
            or set(binding) != {"id", "number", "url"}
            or not isinstance(binding["id"], str)
            or not binding["id"]
            or type(binding["number"]) is not int
            or binding["number"] < 1
        ):
            raise ValueError("workstream binding requires an exact GitHub issue")
        _, number = issue_identity(binding["url"], issue["repository"])
        if number != binding["number"]:
            raise ValueError("workstream issue number differs from its URL")
        if owners[stream_id] is not None and number != owners[stream_id]:
            raise OwnershipConflict("workstream binding differs from the accepted issue")
        if (binding["id"] == issue["id"] or binding["id"] in bound_ids
                or number == issue["number"] or number in bound_numbers):
            raise OwnershipConflict("workstreams require distinct child issue identities")
        bound_ids.add(binding["id"])
        bound_numbers.add(number)
    publication = value["publication"]
    if not isinstance(publication, dict) or set(publication) != {"stack_id", "members"}:
        raise ValueError("publication requires an explicit stack and member list")
    stack_id = publication["stack_id"]
    if stack_id is not None and (type(stack_id) is not int or stack_id < 1):
        raise ValueError("remote stack identity is invalid")
    members = publication["members"]
    ordered = [chunk["id"] for chunk in ordered_chunks(plan)]
    if not isinstance(members, list) or len(members) > len(ordered):
        raise ValueError("publication exceeds the accepted feature plan")
    numbers, branches = set(), set()
    for index, member in enumerate(members):
        if (
            not isinstance(member, dict)
            or set(member) != {"chunk_id", "number", "url", "branch", "head", "base_branch"}
            or member["chunk_id"] != ordered[index]
            or type(member["number"]) is not int
            or member["number"] < 1
            or not isinstance(member["branch"], str)
            or not isinstance(member["url"], str)
            or not isinstance(member["head"], str)
            or member["number"] in numbers
            or member["branch"] in branches
            or member["url"].casefold()
            != (f"https://github.com/{issue['repository']}/pull/{member['number']}".casefold())
            or not SHA.fullmatch(member["head"])
        ):
            raise ValueError("publication is not the unique ordered feature PR prefix")
        for branch in (member["branch"], member["base_branch"]):
            if (
                not isinstance(branch, str)
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", branch)
                or ".." in branch
                or branch.endswith("/")
                or branch.endswith(".lock")
            ):
                raise ValueError("publication branch is invalid")
        if index and member["base_branch"] != members[index - 1]["branch"]:
            raise ValueError("publication PRs do not form the recorded stack")
        numbers.add(member["number"])
        branches.add(member["branch"])
    if bool(stack_id) != (len(members) > 1):
        raise ValueError("multiple publications require their explicit GitHub stack identity")
    return deepcopy(value)


def encode_manifest(manifest: dict) -> str:
    plan = manifest["plan"]
    lines = ["## Delivery plan", "", plan["scope"], "", "Acceptance criteria:"]
    lines.extend("- " + item for item in plan["acceptance"])
    lines.extend(["", "Delivery chunks:"])
    for chunk in ordered_chunks(plan):
        lines.append(f"- **{chunk['title']}** ({chunk['id']})")
    members = manifest["publication"]["members"]
    if members:
        lines.extend(["", "Published changes:"])
        lines.extend(f"- {member['url']}" for member in members)
    lines.extend(["", MARKER, "```json", canonical_json(manifest), "```", ""])
    body = "\n".join(lines)
    if len(body.encode()) > 60000:
        raise ValueError("delivery plan exceeds the GitHub comment limit")
    return body


def decode_manifest(body: str, issue: dict) -> dict:
    if not isinstance(body, str) or body.count(MARKER) != 1 or len(body.encode()) > 60000:
        raise ValueError("GitHub delivery comment lacks its unique record")
    encoded = body.split(MARKER, 1)[1].strip()
    if not encoded.startswith("```json\n") or not encoded.endswith("\n```"):
        raise ValueError("GitHub delivery record is malformed")
    return validate_manifest(json.loads(encoded[8:-4]), issue)


class GitHubDelivery:
    def api(self, endpoint: str, *, method: str = "GET", body=None):
        argv = [os.environ.get("DEVFLOW_GH", "gh"), "api", "--method", method, endpoint]
        if body is not None:
            argv += ["--input", "-"]
        result = subprocess.run(
            argv,
            input=canonical_json(body) if body is not None else None,
            text=True,
            capture_output=True,
            timeout=60,
            check=False,
        )
        if result.returncode:
            match = re.search(r"HTTP (\d{3})", result.stderr)
            raise GitHubContractError(
                (result.stderr or result.stdout)[:1000], int(match[1]) if match else None
            )
        return json.loads(result.stdout) if result.stdout.strip() else None

    def optional(self, endpoint):
        try:
            return self.api(endpoint)
        except GitHubContractError as exc:
            if exc.status == 404:
                return None
            raise

    def pages(self, endpoint):
        for page in range(1, 101):
            separator = "&" if "?" in endpoint else "?"
            items = self.api(endpoint + f"{separator}per_page=100&page={page}")
            if not isinstance(items, list):
                raise GitHubContractError("GitHub returned an invalid collection")
            yield from items
            if len(items) < 100:
                return
        raise GitHubContractError("GitHub collection exceeds bounded pagination")

    def issue(self, repository: str, number: int, repository_id: str) -> dict:
        raw = self.api(f"repos/{repository}/issues/{number}")
        _, actual_number = issue_identity(raw["html_url"], repository)
        if raw.get("pull_request") or actual_number != number or not raw.get("node_id"):
            raise ValueError("GitHub returned a different issue identity")
        return {
            "id": raw["node_id"],
            "database_id": raw["id"],
            "number": number,
            "url": raw["html_url"],
            "repository_id": repository_id,
            "repository": repository,
            "title": raw["title"],
            "body": raw.get("body") or "",
            "state": raw["state"],
            "updated_at": raw["updated_at"],
            "labels": [label["name"] for label in raw.get("labels", [])],
        }

    def snapshot(self, url: str, repository: str) -> dict:
        _, number = issue_identity(url, repository)
        repo = self.api(f"repos/{repository}")
        if repo["full_name"].casefold() != repository.casefold() or not repo.get("node_id"):
            raise ValueError("GitHub repository identity differs from configured origin")
        issue = self.issue(repository, number, repo["node_id"])
        parent = self.optional(f"repos/{repository}/issues/{number}/parent")
        if parent is not None:
            raise ValueError(
                "feature admission requires the parent issue; select its workstream there"
            )
        children = []
        for child in self.pages(f"repos/{repository}/issues/{number}/sub_issues"):
            _, child_number = issue_identity(child["html_url"], repository)
            children.append(self.issue(repository, child_number, repo["node_id"]))
        return {
            "issue": issue,
            "workstreams": children,
            "default_branch": repo["default_branch"],
            "delivery": self.bound_record(issue),
        }

    def bound_record(self, issue: dict) -> dict | None:
        labels = [label for label in issue["labels"] if label.startswith(LABEL_PREFIX)]
        if not labels:
            return None
        if len(labels) != 1 or not labels[0][len(LABEL_PREFIX) :].isdecimal():
            raise OwnershipConflict("feature has an ambiguous GitHub delivery binding")
        comment_id = int(labels[0][len(LABEL_PREFIX) :])
        record = self.api(f"repos/{issue['repository']}/issues/comments/{comment_id}")
        expected_issue = (
            f"https://api.github.com/repos/{issue['repository']}/issues/{issue['number']}"
        )
        if (
            record["id"] != comment_id
            or record["issue_url"].casefold() != expected_issue.casefold()
            or not record.get("node_id")
        ):
            raise OwnershipConflict("delivery comment belongs to a different issue")
        return {
            "comment_id": comment_id,
            "comment_node_id": record["node_id"],
            "manifest": decode_manifest(record["body"], issue),
        }

    def initialize(self, issue: dict, plan: dict, registry, token: dict) -> dict:
        """Publish the plan without modifying the human-owned issue description."""
        validate_plan(plan)
        key = "github-plan:" + digest({"issue": issue["id"], "plan": plan})
        manifest = {
            "version": 1,
            "issue_id": issue["id"],
            "repository_id": issue["repository_id"],
            "revision": 1,
            "creation_key": key,
            "plan": plan,
            "workstream_issues": {},
            "publication": {"stack_id": None, "members": []},
        }
        body = encode_manifest(manifest)
        with registry.mutation(token):
            current = self.issue(issue["repository"], issue["number"], issue["repository_id"])
            existing = self.bound_record(current)
            if existing:
                if existing["manifest"]["plan"] != plan:
                    raise OwnershipConflict(
                        "feature already has a different accepted delivery plan"
                    )
                self.reconcile_record(issue, existing, registry, token)
                binding_key = "github-plan-bind:" + LABEL_PREFIX + str(existing["comment_id"])
                pending = registry.effect(issue["id"], binding_key)
                if pending and pending["state"] == "pending":
                    registry.finish_effect(token, binding_key, {
                        "issue": issue["id"], "name": LABEL_PREFIX + str(existing["comment_id"]),
                    })
                return existing
            receipt = registry.intent(token, key, "github_plan", {"body": body})
            if receipt["state"] == "complete":
                comment = receipt["result"]
            elif receipt["fresh"]:
                comment = self.api(
                    f"repos/{issue['repository']}/issues/{issue['number']}/comments",
                    method="POST",
                    body={"body": body},
                )
            else:
                # Only the original uncertain creation is reconciled. This is not
                # discovery of a substitute stack or another feature's publication.
                matches = [
                    item
                    for item in self.pages(
                        f"repos/{issue['repository']}/issues/{issue['number']}/comments"
                    )
                    if item.get("body") == body
                ]
                if len(matches) != 1:
                    raise UnresolvedEffect("original delivery comment creation is unresolved")
                comment = matches[0]
            registry.finish_effect(token, key, {"id": comment["id"], "node_id": comment["node_id"]})
            name = LABEL_PREFIX + str(comment["id"])
            self._bind_label(issue, name, registry, token)
            refreshed = self.issue(issue["repository"], issue["number"], issue["repository_id"])
            saved = self.bound_record(refreshed)
            if saved is None or saved["manifest"] != manifest:
                raise OwnershipConflict("GitHub delivery plan readback differs from publication")
            return saved

    def _bind_label(self, issue, name, registry, token):
        endpoint = f"repos/{issue['repository']}/labels/{quote(name, safe='')}"
        key = "github-plan-label:" + name
        receipt = registry.intent(token, key, "github_plan_label", {"name": name})
        label = self.optional(endpoint)
        if label is None:
            if not receipt["fresh"]:
                raise UnresolvedEffect("original delivery label creation is unresolved")
            self.api(
                f"repos/{issue['repository']}/labels",
                method="POST",
                body={
                    "name": name,
                    "color": "ededed",
                    "description": "Recorded delivery plan reference",
                },
            )
            label = self.api(endpoint)
        registry.finish_effect(token, key, {"name": label["name"]})
        key = "github-plan-bind:" + name
        receipt = registry.intent(
            token, key, "github_plan_bind", {"issue": issue["id"], "name": name}
        )
        current = self.issue(issue["repository"], issue["number"], issue["repository_id"])
        other = [
            item for item in current["labels"] if item.startswith(LABEL_PREFIX) and item != name
        ]
        if other:
            raise OwnershipConflict("feature acquired a different delivery plan reference")
        if name not in current["labels"]:
            if not receipt["fresh"]:
                raise UnresolvedEffect("original delivery binding operation is unresolved")
            self.api(
                f"repos/{issue['repository']}/issues/{issue['number']}/labels",
                method="POST",
                body={"labels": [name]},
            )
            current = self.issue(issue["repository"], issue["number"], issue["repository_id"])
        if name not in current["labels"]:
            raise OwnershipConflict("delivery label was not attached to the feature")
        registry.finish_effect(token, key, {"issue": issue["id"], "name": name})

    def update(self, issue: dict, record: dict, manifest: dict, registry, token: dict) -> dict:
        validate_manifest(manifest, issue)
        old = record["manifest"]
        if (
            manifest["revision"] != old["revision"] + 1
            or manifest["plan"] != old["plan"]
            or manifest["creation_key"] != old["creation_key"]
        ):
            raise OwnershipConflict("publication update changed its accepted business plan")
        endpoint = f"repos/{issue['repository']}/issues/comments/{record['comment_id']}"
        body = encode_manifest(manifest)
        key = f"github-record:{record['comment_id']}:{manifest['revision']}"
        with registry.mutation(token):
            receipt = registry.intent(
                token,
                key,
                "github_record",
                {
                    "before": digest(old),
                    "after": digest(manifest),
                    "comment_id": record["comment_id"],
                },
            )
            current = self.api(endpoint)
            saved = decode_manifest(current["body"], issue)
            if saved != manifest:
                if saved != old:
                    raise OwnershipConflict("GitHub delivery record changed before integration")
                if not receipt["fresh"]:
                    raise UnresolvedEffect("original delivery record update is unresolved")
                self.api(endpoint, method="PATCH", body={"body": body})
                saved = decode_manifest(self.api(endpoint)["body"], issue)
            if saved != manifest:
                raise OwnershipConflict("GitHub delivery record readback differs from integration")
            registry.finish_effect(token, key, {"digest": digest(saved)})
            return {**record, "manifest": saved}

    def reconcile_record(self, issue, record, registry, token):
        """A recorded PATCH may have succeeded before its local receipt was saved."""
        with registry.connect() as db:
            pending = [dict(row) for row in db.execute(
                "SELECT effect_key,request_json FROM execution_effects WHERE issue_id=? "
                "AND kind='github_record' AND state='pending'", (issue["id"],),
            )]
        observed = digest(record["manifest"])
        for receipt in pending:
            request = json.loads(receipt["request_json"])
            if request["comment_id"] != record["comment_id"] or request["after"] != observed:
                raise UnresolvedEffect("recorded GitHub update does not match current readback")
            registry.finish_effect(token, receipt["effect_key"], {"digest": observed})

    def workstreams(self, issue, record, registry, token):
        """Create or adopt exactly the plan's sub-issues, then bind their IDs."""
        for stream in record["manifest"]["plan"]["workstreams"]:
            existing = record["manifest"]["workstream_issues"].get(stream["id"])
            key = "workstream:" + stream["id"]
            with registry.mutation(token):
                if existing:
                    if stream["issue_number"] and stream["issue_number"] != existing["number"]:
                        raise OwnershipConflict("recorded workstream differs from accepted issue")
                    child = self.issue(
                        issue["repository"], existing["number"], issue["repository_id"]
                    )
                    if child["id"] != existing["id"]:
                        raise OwnershipConflict("recorded sub-issue identity changed")
                elif stream["issue_number"]:
                    child = self.issue(
                        issue["repository"], stream["issue_number"], issue["repository_id"]
                    )
                else:
                    body = (
                        stream["title"]
                        + "\n\nAcceptance:\n"
                        + "\n".join("- " + item for item in stream["acceptance"])
                        + f"\n\n<!-- devflow-workstream:{issue['id']}:{stream['id']} -->\n"
                    )
                    receipt = registry.intent(
                        token, key, "github_workstream", {"title": stream["title"], "body": body}
                    )
                    if receipt["state"] == "complete":
                        number = receipt["result"]["number"]
                    elif receipt["fresh"]:
                        raw = self.api(
                            f"repos/{issue['repository']}/issues",
                            method="POST",
                            body={"title": stream["title"], "body": body},
                        )
                        number = raw["number"]
                    else:
                        # Exact creation-operation recovery, never generic PR discovery.
                        matches = [
                            item
                            for item in self.pages(f"repos/{issue['repository']}/issues?state=all")
                            if item.get("body") == body and not item.get("pull_request")
                        ]
                        if len(matches) != 1:
                            raise UnresolvedEffect("original workstream creation is unresolved")
                        number = matches[0]["number"]
                    child = self.issue(issue["repository"], number, issue["repository_id"])
                    registry.finish_effect(token, key, {"number": number, "id": child["id"]})
                if child["id"] == issue["id"]:
                    raise OwnershipConflict("feature cannot be its own workstream")
                parent_path = f"repos/{issue['repository']}/issues/{child['number']}/parent"
                parent = self.optional(parent_path)
                link = registry.intent(
                    token,
                    key + ":parent",
                    "github_subissue",
                    {"parent": issue["id"], "child": child["id"], "child_number": child["number"]},
                )
                if parent is None:
                    if not link["fresh"]:
                        raise UnresolvedEffect("original sub-issue linkage is unresolved")
                    self.api(
                        f"repos/{issue['repository']}/issues/{issue['number']}/sub_issues",
                        method="POST",
                        body={"sub_issue_id": child["database_id"]},
                    )
                    parent = self.api(parent_path)
                if parent["node_id"] != issue["id"]:
                    raise OwnershipConflict("workstream belongs to another feature")
                registry.finish_effect(
                    token, key + ":parent", {"child": child["id"], "parent": issue["id"]}
                )
            binding = {field: child[field] for field in ("id", "number", "url")}
            if not existing:
                manifest = deepcopy(record["manifest"])
                manifest["revision"] += 1
                manifest["workstream_issues"][stream["id"]] = binding
                record = self.update(issue, record, manifest, registry, token)
        return record
