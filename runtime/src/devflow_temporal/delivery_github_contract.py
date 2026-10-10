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
from .delivery_plan_model import (
    _text,
    compact_plan,
    index_chunks,
    ordered_chunks,
    plan_version,
    validate_plan,
    validate_plan_index,
)

MARKER = "<!-- devflow-delivery:v1 -->"
MARKER_V2 = "<!-- devflow-delivery:v2 -->"
LABEL_PREFIX = "devflow-plan-"
IDENTIFIER = re.compile(r"[a-z][a-z0-9_-]{0,63}")
SHA = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
# Bound v2 GitHub identities so intake can be admitted before remote allocation.
GITHUB_ID_MAX = 10**32 - 1
GITHUB_NODE_MAX = 128


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


def wire_manifest(manifest: dict) -> dict:
    """The expanded in-memory plan must never be serialized on a v2 parent."""
    value = deepcopy(manifest)
    if value.get("version") == 2:
        plan = value["plan"]
        if (not isinstance(plan, dict) or not isinstance(plan.get("workstreams"), list)
                or any(not isinstance(stream, dict) or not isinstance(stream.get("chunks"), list)
                       for stream in plan["workstreams"])):
            raise ValueError("v2 parent index requires structured workstreams")
        chunks = [chunk for stream in plan["workstreams"] for chunk in stream["chunks"]]
        if any(not isinstance(chunk, dict) for chunk in chunks):
            raise ValueError("v2 parent index requires structured chunks")
        if chunks and "scope" in chunks[0]:
            if digest(validate_plan(value["plan"])) != value["plan_digest"]:
                raise OwnershipConflict("assembled plan differs from the recorded plan digest")
            value["plan"] = compact_plan(value["plan"])
        else:
            validate_plan_index(value["plan"])
    return value


def _validate_bindings(value, issue, plan, *, complete=False):
    owners = {stream["id"]: stream["issue_number"] for stream in plan["workstreams"]}
    bindings = value["workstream_issues"]
    if (not isinstance(bindings, dict) or bindings.keys() - owners.keys()
            or (complete and bindings.keys() != owners.keys())):
        raise ValueError("workstream issue binding is invalid")
    bound_ids, bound_numbers = set(), set()
    for stream_id, binding in bindings.items():
        if (not isinstance(binding, dict) or set(binding) != {"id", "number", "url"}
                or not isinstance(binding["id"], str) or not binding["id"]
                or type(binding["number"]) is not int or binding["number"] < 1):
            raise ValueError("workstream binding requires an exact GitHub issue")
        _, number = issue_identity(binding["url"], issue["repository"])
        if complete and (number > GITHUB_ID_MAX or len(binding["id"]) > GITHUB_NODE_MAX
                         or binding["url"].casefold() != (
                             f"https://github.com/{issue['repository']}/issues/{number}".casefold())):
            raise ValueError("v2 workstream binding exceeds its bounded GitHub identity")
        if number != binding["number"]:
            raise ValueError("workstream issue number differs from its URL")
        if owners[stream_id] is not None and number != owners[stream_id]:
            raise OwnershipConflict("workstream binding differs from the accepted issue")
        if complete and owners[stream_id] != number:
            raise OwnershipConflict("v2 workstream issue must be resolved in the published plan")
        if (binding["id"] == issue["id"] or binding["id"] in bound_ids
                or number == issue["number"] or number in bound_numbers):
            raise OwnershipConflict("workstreams require distinct child issue identities")
        bound_ids.add(binding["id"])
        bound_numbers.add(number)
    return bindings


def _validate_publication(value, issue, chunks, bindings):
    publication = value["publication"]
    if not isinstance(publication, dict) or set(publication) != {"stack_id", "members"}:
        raise ValueError("publication requires an explicit stack and member list")
    stack_id = publication["stack_id"]
    if stack_id is not None and (type(stack_id) is not int or stack_id < 1):
        raise ValueError("remote stack identity is invalid")
    members = publication["members"]
    if not isinstance(members, list) or len(members) > len(chunks):
        raise ValueError("publication exceeds the accepted feature plan")
    fields = {"chunk_id", "number", "url", "branch", "head", "base_branch"}
    if value["version"] == 2:
        fields |= {"workstream_id", "issue_id", "issue_number", "issue_url"}
    numbers, branches = set(), set()
    for index, member in enumerate(members):
        if (not isinstance(member, dict) or set(member) != fields
                or member["chunk_id"] != chunks[index]["id"]
                or type(member["number"]) is not int or member["number"] < 1
                or not isinstance(member["branch"], str) or not isinstance(member["url"], str)
                or not isinstance(member["head"], str) or member["number"] in numbers
                or member["branch"] in branches
                or member["url"].casefold()
                != f"https://github.com/{issue['repository']}/pull/{member['number']}".casefold()
                or not SHA.fullmatch(member["head"])):
            raise ValueError("publication is not the unique ordered feature PR prefix")
        if value["version"] == 2:
            stream_id = chunks[index]["workstream_id"]
            child = bindings[stream_id]
            if (member["workstream_id"] != stream_id or member["issue_id"] != child["id"]
                    or member["issue_number"] != child["number"]
                    or member["issue_url"] != child["url"]):
                raise OwnershipConflict("publication child ownership differs from its chunk")
        for branch in (member["branch"], member["base_branch"]):
            if (not isinstance(branch, str)
                    or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", branch)
                    or ".." in branch or branch.endswith("/") or branch.endswith(".lock")):
                raise ValueError("publication branch is invalid")
        if index and member["base_branch"] != members[index - 1]["branch"]:
            raise ValueError("publication PRs do not form the recorded stack")
        numbers.add(member["number"])
        branches.add(member["branch"])
    if bool(stack_id) != (len(members) > 1):
        raise ValueError("multiple publications require their explicit GitHub stack identity")


def validate_manifest(value: dict, issue: dict) -> dict:
    base_fields = {"version", "issue_id", "repository_id", "revision", "creation_key", "plan",
                   "workstream_issues", "publication"}
    if (not isinstance(value, dict) or type(value.get("version")) is not int
            or value["version"] not in {1, 2}
            or set(value) != (base_fields if value["version"] == 1 else
                              base_fields | {"plan_revision", "plan_digest", "workstream_plans"})
            or value["issue_id"] != issue["id"] or value["repository_id"] != issue["repository_id"]
            or type(value["revision"]) is not int or value["revision"] < 1):
        raise ValueError("GitHub delivery record has a different identity or schema")
    _text(value["creation_key"], "creation key", 128)
    if value["version"] == 1:
        plan = validate_plan(value["plan"])
        if plan_version(plan) != 1:
            raise ValueError("v1 record must retain its legacy plan")
        chunks = ordered_chunks(plan)
    else:
        if (type(value["plan_revision"]) is not int or value["plan_revision"] < 1
                or not isinstance(value["plan_digest"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", value["plan_digest"])):
            raise ValueError("v2 record requires an exact plan revision and digest")
        plan = wire_manifest(value)["plan"]
        chunks = index_chunks(plan)
    bindings = _validate_bindings(value, issue, plan, complete=value["version"] == 2)
    if value["version"] == 2:
        refs = value["workstream_plans"]
        if not isinstance(refs, dict) or refs.keys() != bindings.keys():
            raise ValueError("v2 parent requires every exact child-plan comment")
        comment_ids = set()
        for stream_id, ref in refs.items():
            if (not isinstance(ref, dict)
                    or set(ref) != {"comment_id", "comment_node_id", "digest", "url"}
                    or type(ref["comment_id"]) is not int
                    or not 1 <= ref["comment_id"] <= GITHUB_ID_MAX
                    or ref["comment_id"] in comment_ids
                    or not isinstance(ref["comment_node_id"], str) or not ref["comment_node_id"]
                    or len(ref["comment_node_id"]) > GITHUB_NODE_MAX
                    or not isinstance(ref["digest"], str)
                    or not re.fullmatch(r"[0-9a-f]{64}", ref["digest"])
                    or ref["url"] != bindings[stream_id]["url"]
                    + f"#issuecomment-{ref['comment_id']}"):
                raise ValueError("child-plan reference requires immutable "
                                 "comment identity and digest")
            comment_ids.add(ref["comment_id"])
    _validate_publication(value, issue, chunks, bindings)
    return deepcopy(value)


def encode_manifest(manifest: dict) -> str:
    value = wire_manifest(manifest)
    plan = value["plan"]
    lines = ["## Delivery plan", "", plan["scope"], "", "Acceptance criteria:"]
    lines.extend("- " + item for item in plan["acceptance"])
    if value["version"] == 2:
        lines.extend(["", f"Plan revision: {value['plan_revision']}", "", "Workstream plans:"])
        for stream in plan["workstreams"]:
            ref = value["workstream_plans"][stream["id"]]
            lines.append(f"- [{stream['title']}]({ref['url']}) (#{stream['issue_number']})")
        chunks = index_chunks(plan)
    else:
        chunks = ordered_chunks(plan)
    lines.extend(["", "Delivery chunks:"])
    for chunk in chunks:
        dependencies = ", ".join(chunk["depends_on"]) or "none"
        lines.append(f"- **{chunk['title']}** ({chunk['id']}); prerequisites: {dependencies}")
    members = value["publication"]["members"]
    if members:
        lines.extend(["", "Published changes:"])
        lines.extend(f"- {member['url']}" for member in members)
    marker = MARKER if value["version"] == 1 else MARKER_V2
    lines.extend(["", marker, "```json", canonical_json(value), "```", ""])
    body = "\n".join(lines)
    if len(body.encode()) > 60000:
        raise ValueError("delivery plan exceeds the GitHub comment limit")
    return body


def decode_manifest(body: str, issue: dict) -> dict:
    if not isinstance(body, str) or len(body.encode()) > 60000:
        raise ValueError("GitHub delivery comment lacks its unique record")
    markers = [marker for marker in (MARKER, MARKER_V2) if marker in body]
    if len(markers) != 1 or body.count(markers[0]) != 1:
        raise ValueError("GitHub delivery comment lacks its unique record")
    marker = markers[0]
    encoded = body.split(marker, 1)[1].strip()
    if not encoded.startswith("```json\n") or not encoded.endswith("\n```"):
        raise ValueError("GitHub delivery record is malformed")
    value = json.loads(encoded[8:-4])
    if not isinstance(value, dict) or value.get("version") != (1 if marker == MARKER else 2):
        raise ValueError("GitHub delivery marker and schema disagree")
    return validate_manifest(value, issue)


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
        saved = {
            "comment_id": comment_id,
            "comment_node_id": record["node_id"],
            "manifest": decode_manifest(record["body"], issue),
        }
        if saved["manifest"]["version"] == 2:
            from .delivery_github_plans import hydrate_record

            return hydrate_record(self, issue, saved)
        return saved

    def initialize(self, issue: dict, plan: dict, registry, token: dict) -> dict:
        """Publish the plan without modifying the human-owned issue description."""
        validate_plan(plan)
        if plan_version(plan) == 2:
            from .delivery_github_plans import initialize_v2

            return initialize_v2(self, issue, plan, registry, token)
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
            or manifest["version"] != old["version"]
            or any(manifest.get(key) != old.get(key)
                   for key in ("plan_revision", "plan_digest", "workstream_plans"))
            or (old["version"] == 2
                and manifest["workstream_issues"] != old["workstream_issues"])
        ):
            raise OwnershipConflict("publication update changed its accepted business plan")
        old_wire, new_wire = wire_manifest(old), wire_manifest(manifest)
        endpoint = f"repos/{issue['repository']}/issues/comments/{record['comment_id']}"
        body = encode_manifest(manifest)
        key = f"github-record:{record['comment_id']}:{manifest['revision']}"
        with registry.mutation(token):
            receipt = registry.intent(
                token,
                key,
                "github_record",
                {
                    "before": digest(old_wire),
                    "after": digest(new_wire),
                    "comment_id": record["comment_id"],
                },
            )
            current = self.api(endpoint)
            saved = decode_manifest(current["body"], issue)
            from .delivery_github_plans import authenticate_comment, verify_parent_binding

            if old["version"] == 2:
                verify_parent_binding(self, issue, record)
            authenticate_comment(current, issue, record)
            if saved != new_wire:
                if saved != old_wire:
                    raise OwnershipConflict("GitHub delivery record changed before integration")
                if not receipt["fresh"]:
                    raise UnresolvedEffect("original delivery record update is unresolved")
                self.api(endpoint, method="PATCH", body={"body": body})
                current = self.api(endpoint)
                authenticate_comment(current, issue, record)
                saved = decode_manifest(current["body"], issue)
            if saved != new_wire:
                raise OwnershipConflict("GitHub delivery record readback differs from integration")
            registry.finish_effect(token, key, {"digest": digest(saved)})
            if saved["version"] == 2:
                return self.load_record(issue, record)
            return {**record, "manifest": saved}

    def reconcile_record(self, issue, record, registry, token):
        """A recorded PATCH may have succeeded before its local receipt was saved."""
        with registry.connect() as db:
            pending = [dict(row) for row in db.execute(
                "SELECT effect_key,request_json FROM execution_effects WHERE issue_id=? "
                "AND kind='github_record' AND state='pending'", (issue["id"],),
            )]
        observed = digest(wire_manifest(record["manifest"]))
        for receipt in pending:
            request = json.loads(receipt["request_json"])
            if request["comment_id"] != record["comment_id"] or request["after"] != observed:
                raise UnresolvedEffect("recorded GitHub update does not match current readback")
            registry.finish_effect(token, receipt["effect_key"], {"digest": observed})

    def _resolve_workstream(self, issue, stream, existing, registry, token):
        """Resolve one exact child under the caller's registry mutation lock."""
        key = "workstream:" + stream["id"]
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
        return child

    def workstreams(self, issue, record, registry, token):
        """Create or adopt exactly the plan's sub-issues, then bind their IDs."""
        if record["manifest"]["version"] == 2:
            return self.load_record(issue, record)
        for stream in record["manifest"]["plan"]["workstreams"]:
            existing = record["manifest"]["workstream_issues"].get(stream["id"])
            with registry.mutation(token):
                child = self._resolve_workstream(issue, stream, existing, registry, token)
            binding = {field: child[field] for field in ("id", "number", "url")}
            if not existing:
                manifest = deepcopy(record["manifest"])
                manifest["revision"] += 1
                manifest["workstream_issues"][stream["id"]] = binding
                record = self.update(issue, record, manifest, registry, token)
        return record

    def load_record(self, issue, reference):
        from .delivery_github_plans import load_record

        return load_record(self, issue, reference)

    def stage_plan_revision(self, issue, record, plan, registry, token, *, operation_id):
        from .delivery_github_plans import stage_plan_revision

        return stage_plan_revision(self, issue, record, plan, registry, token,
                                   operation_id=operation_id)

    def publish_plan_revision(self, issue, record, plan, registry, token, *, operation_id):
        from .delivery_github_plans import publish_plan_revision

        return publish_plan_revision(self, issue, record, plan, registry, token,
                                     operation_id=operation_id)

    def settle_plan_effect(self, issue, entry, registry, token):
        from .delivery_github_plans import settle_plan_effect

        return settle_plan_effect(self, issue, entry, registry, token)
