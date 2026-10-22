"""Read-only GitHub evidence acquisition, independent of delivery decomposition."""

from __future__ import annotations

import base64
import json
import re
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

from .contracts import canonical_json, digest
from .delivery_github_contract import GitHubContractError, GitHubDelivery, issue_identity
from .delivery_resources import read_private, write_private
from .delivery_store import _now

MAX_LINKS = 16
MAX_TEXT = 24000
MAX_TOTAL = 240000
_URL = re.compile(r'https://github\.com/[^\s<>"\)\]]+')


def initialize(db):
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_issue_context (
        command_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES delivery_runs(run_id),
        binding TEXT NOT NULL, sha256 TEXT NOT NULL, context_json TEXT NOT NULL)""")


def _binding(spec):
    return digest({key: spec.get(key) for key in
                   ("run_id", "work_id", "issue_url", "github_repo", "request_digest")})


def _restore(row, spec):
    context = json.loads(row["context_json"])
    if row["binding"] != _binding(spec) or digest(context) != row["sha256"]:
        raise ValueError("issue context changed its run, issue or recorded content")
    path = Path(spec["state_dir"]) / "issue-context" / (row["sha256"] + ".json")
    if read_private(path) != context:
        raise ValueError("issue context artifact differs from its recorded snapshot")
    return context


def latest(store, spec):
    with store._connect() as db:
        row = db.execute("SELECT * FROM delivery_issue_context WHERE run_id=? "
                         "ORDER BY rowid DESC LIMIT 1", (spec["run_id"],)).fetchone()
    return _restore(row, spec) if row else None


def _reference(url):
    parsed = urlsplit(url.rstrip('.,;'))
    if (parsed.scheme != "https" or parsed.netloc.casefold() != "github.com"
            or parsed.query):
        raise ValueError("only direct GitHub evidence URLs are acquired")
    parts = unquote(parsed.path).strip('/').split('/')
    if (len(parts) < 4 or any(p in {"", ".", ".."} for p in parts)
            or any(not re.fullmatch(r'[A-Za-z0-9_.-]+', p) for p in parts[:2])):
        raise ValueError("invalid GitHub evidence identity")
    repo = '/'.join(parts[:2])
    if parts[2] in {"issues", "pull"} and len(parts) == 4 and parts[3].isdecimal():
        return repo, parts[2], int(parts[3]), parsed.fragment
    if parts[2] == "blob" and len(parts) >= 5 and re.fullmatch(r'[0-9a-fA-F]{40}', parts[3]):
        return repo, "blob", (parts[3], '/'.join(parts[4:])), parsed.fragment
    raise ValueError("evidence must identify an issue, PR or immutable GitHub file")


def _clip(text, limit=MAX_TEXT):
    return {"text": text[:limit], "truncated": len(text) > limit}


def collect(spec, github=None):
    github = github or GitHubDelivery()
    repo, number = issue_identity(spec["issue_url"], spec["github_repo"])
    metadata = github.api(f"repos/{repo}")
    if metadata["full_name"].casefold() != repo.casefold():
        raise ValueError("issue context repository differs from configured origin")
    issue = github.issue(repo, number, metadata["node_id"])
    if len(issue["body"]) > MAX_TOTAL:
        raise ValueError("primary issue body exceeds the acquisition bound")
    context = {"version": 1, "run_id": spec["run_id"], "issue": issue,
               "fetched_at": _now(), "evidence": [], "unavailable": [],
               "limits": {"links": MAX_LINKS, "text_per_reference": MAX_TEXT,
                          "total_reference_text": MAX_TOTAL}}
    links = list(_URL.findall(issue["body"]))
    remaining = MAX_TOTAL
    # Cross-referenced prior work is frequently absent from the issue body.
    for suffix in ("comments", "timeline"):
        endpoint = f"repos/{repo}/issues/{number}/{suffix}?per_page=100"
        try:
            records = github.api(endpoint)
            if not isinstance(records, list):
                raise ValueError("GitHub evidence collection is not a list")
            if len(records) == 100:
                context["unavailable"].append({"url": endpoint, "reason": "pagination limit"})
            for record in records:
                if suffix == "comments":
                    if remaining <= 0:
                        context["unavailable"].append({"url": endpoint, "reason": "text limit"})
                        break
                    body = record.get("body", "")
                    clipped = _clip(body, min(4000, remaining))
                    context["evidence"].append({"kind": "comment", "url": record["html_url"],
                        "updated_at": record.get("updated_at"), **clipped})
                    remaining -= len(clipped["text"])
                    links.extend(_URL.findall(body))
                else:
                    url = record.get("source", {}).get("issue", {}).get("html_url")
                    if url:
                        links.append(url)
        except (GitHubContractError, ValueError) as exc:
            context["unavailable"].append({"url": endpoint, "reason": str(exc)[:300]})
    links.extend(f"https://github.com/{repo}/issues/{n}"
                 for n in re.findall(r'(?<![\w/])#([1-9][0-9]*)\b', issue["body"]))
    seen = {issue["url"]}
    acquired = 0
    public = {repo.casefold(): True}
    for url in links:
        if url in seen:
            continue
        seen.add(url)
        if acquired >= MAX_LINKS or remaining <= 0:
            context["unavailable"].append({"url": url, "reason": "reference limit"})
            continue
        acquired += 1
        try:
            linked_repo, kind, target, fragment = _reference(url)
            if linked_repo.casefold() not in public:
                public[linked_repo.casefold()] = not github.api(f"repos/{linked_repo}")["private"]
            if not public[linked_repo.casefold()]:
                raise ValueError("private cross-repository evidence is not acquired")
            if kind == "blob":
                ref, path = target
                raw = github.api(f"repos/{linked_repo}/contents/{quote(path, safe='/')}?ref={ref}")
                if raw.get("encoding") != "base64" or raw.get("size", MAX_TOTAL + 1) > MAX_TOTAL:
                    raise ValueError("linked file is unavailable or exceeds the acquisition bound")
                text = base64.b64decode(raw["content"]).decode("utf-8")
                lines = re.fullmatch(r'L([1-9][0-9]*)(?:-L?([1-9][0-9]*))?', fragment)
                if lines:
                    text = '\n'.join(text.splitlines()[int(lines[1])-1:int(lines[2] or lines[1])])
                value = {"kind": "file", "url": url, "ref": ref, "path": path,
                         "blob_sha": raw["sha"], **_clip(text, min(MAX_TEXT, remaining))}
            else:
                raw = github.api(f"repos/{linked_repo}/issues/{target}")
                is_pr = kind == "pull" or "pull_request" in raw
                if is_pr:
                    raw = github.api(f"repos/{linked_repo}/pulls/{target}")
                value = {"kind": "pull_request" if is_pr else "issue", "url": url,
                         "title": raw["title"], "state": raw["state"],
                         "updated_at": raw.get("updated_at"), "merged_at": raw.get("merged_at"),
                         **_clip(raw.get("body") or "", min(MAX_TEXT, remaining))}
                if is_pr:
                    value["head"] = raw["head"]["sha"]
                    files = github.api(f"repos/{linked_repo}/pulls/{target}/files?per_page=100")
                    value["files"] = []
                    patch_budget = remaining - len(value["text"])
                    for file in files[:20]:
                        if patch_budget <= 0:
                            break
                        patch = _clip(file.get("patch") or "", min(4000, patch_budget))
                        if not file.get("patch"):
                            patch["unavailable"] = "GitHub did not return a textual patch"
                        value["files"].append({"path": file["filename"], **patch})
                        patch_budget -= len(patch["text"])
                        remaining -= len(patch["text"])
                    value["files_truncated"] = len(files) >= 100 or len(value["files"]) < len(files)
            remaining -= len(value["text"])
            context["evidence"].append(value)
        except (GitHubContractError, ValueError, UnicodeError, KeyError) as exc:
            context["unavailable"].append({"url": url, "reason": str(exc)[:300]})
    return context


def refresh(store, run_id, payload):
    if (not isinstance(payload, dict) or set(payload) != {"command_id"}
            or not isinstance(payload["command_id"], str)
            or not 0 < len(payload["command_id"].strip()) <= 200):
        raise ValueError("issue context refresh requires a command ID")
    spec = store.effective_spec(run_id)
    if not store.owns_execution(spec):
        raise ValueError("issue context belongs to another delivery owner")
    with store._connect() as db:
        prior = db.execute("SELECT * FROM delivery_issue_context WHERE command_id=?",
                           (payload["command_id"],)).fetchone()
    if prior:
        if prior["run_id"] != run_id:
            raise ValueError("issue context command belongs to another run")
        return _restore(prior, spec)
    context = collect(spec)
    sha = digest(context)
    write_private(Path(spec["state_dir"]) / "issue-context" / (sha + ".json"), context)
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        prior = db.execute("SELECT * FROM delivery_issue_context WHERE command_id=?",
                           (payload["command_id"],)).fetchone()
        if prior:
            if prior["run_id"] != run_id:
                raise ValueError("issue context command belongs to another run")
            return _restore(prior, spec)
        if _binding(store.effective_spec(run_id)) != _binding(spec):
            raise ValueError("issue context authority changed during retrieval")
        db.execute("INSERT INTO delivery_issue_context VALUES (?,?,?,?,?)",
                   (payload["command_id"], run_id, _binding(spec), sha, canonical_json(context)))
        revision = db.execute("SELECT revision FROM delivery_runs WHERE run_id=?",
                              (run_id,)).fetchone()[0]
        store._event(db, run_id, revision, "issue_context_fetched",
                     "Fetched issue body, acceptance and linked evidence", {
                         "sha256": sha, "fetched_at": context["fetched_at"],
                         "evidence_count": len(context["evidence"]),
                         "unavailable": context["unavailable"]})
    return context


def prepare_intake(store, request):
    if request["spec"].get("provider") != "codex":
        return request
    from .supervisor import DeliverySupervisor

    key = DeliverySupervisor._job_key({**request, "role": "intake"})
    path = Path(request["spec"]["state_dir"]) / "attempts" / key / "request.json"
    if path.exists():
        prior = read_private(path)
        return {**request, **({"issue_context": prior["issue_context"]}
                            if "issue_context" in prior else {})}
    context = refresh(store, request["spec"]["run_id"], {"command_id": "intake-context:" + key})
    return {**request, "issue_context": context}


def attach_latest(store, request):
    if request.get("issue_context") is not None:
        return request
    context = latest(store, request["spec"])
    return {**request, "issue_context": context} if context else request
