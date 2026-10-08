"""Merge a fresh explicitly authorized run after its original independent gates."""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
import time
from pathlib import Path
from urllib.parse import quote

from .contracts import canonical_json, digest
from .delivery_resources import observe_finalized_resources, read_private, write_private
from .delivery_store import _now


def require_merge_gates(spec, candidate, pr, checks, attempts, publication):
    if (spec.get("authorized_endpoint") != "merged"
            or type(spec.get("merge_version")) is not int or spec["merge_version"] != 1
            or spec.get("provider") != "codex"
            or spec.get("resource_cleanup_version") != 1
            or spec.get("terminal_tracker_version") != 1):
        raise ValueError("run has no frozen merged-endpoint authority")
    if (not isinstance(candidate, dict) or not isinstance(pr, dict)
            or not candidate.get("id") or candidate.get("head") != pr.get("head")
            or candidate.get("base_sha") != spec["base_sha"]
            or candidate.get("policy_digest") != spec["policy_digest"]):
        raise ValueError("merge candidate differs from frozen publication")
    origin_id = publication.get("input_candidate_id")
    if not origin_id:
        raise ValueError("merge has no authenticated original publication candidate")
    for stage in ("prepublish", "local", "review", "qa"):
        result = checks.get(stage, {})
        if (result.get("state") != "passed"
                or result.get("candidate_id") != (origin_id if stage == "prepublish"
                                                 else candidate["id"])):
            raise ValueError(stage + " has not passed for this candidate")
    if spec["policy"].get("browser_qa"):
        result = checks.get("browser_qa", {})
        if (result.get("state") != "passed" or result.get("cleanup") != "confirmed"
                or result.get("candidate_id") != candidate["id"]):
            raise ValueError("browser QA has not passed for this candidate")
    if (checks.get("ci", {}).get("state") != "passed"
            or checks["ci"].get("head") != candidate["head"]
            or not spec["policy"].get("required_ci")):
        raise ValueError("required CI has not passed for this head")
    cleanup = checks.get("resource_cleanup", {})
    if (cleanup.get("state") != "confirmed"
            or cleanup.get("process_cleanup") != "observed-native-confirmed"
            or cleanup.get("resource_cleanup") != "confirmed"):
        raise ValueError("merge requires confirmed process and resource cleanup")
    if any(a.get("state") != "finished" or a.get("cleanup") != "confirmed"
           for a in attempts):
        raise ValueError("merge cannot overlap an unfinished native role")
    selected = {}
    for role in ("implement", "review", "verify"):
        matches = [a for a in attempts if a.get("role") == role]
        if role != "implement":
            matches = [a for a in matches if a.get("candidate_id") == candidate["id"]]
        if not matches:
            raise ValueError("candidate has no completed " + role + " role")
        last = matches[-1]
        result = json.loads(last["result_json"])
        if result.get("status") != "pass" or not last.get("session_id"):
            raise ValueError(role + " did not pass with a native session")
        output = last.get("controller_output_candidate", {})
        if role == "implement" and (
            output.get("id") != origin_id
            or output.get("content_sha256") != candidate.get("content_sha256")
        ):
            raise ValueError("publication changed the implementer's accepted source content")
        selected[role] = last["session_id"]
    if len(set(selected.values())) != 3:
        raise ValueError("merge requires independent implementation, review and QA sessions")


def native_implementation_output(spec, attempt):
    """Read the supervisor's separate output binding, never activity-only enrichment."""
    from .delivery_gates_admission import _native_result_bytes
    from .delivery_resources import _ancestors
    from .supervisor import DeliverySupervisor

    raw = json.loads(_native_result_bytes(spec, attempt))
    saved = json.loads(attempt["result_json"])
    enriched = {"cleanup", "process_cleanup", "resource_cleanup", "native_process",
                "role_artifacts"}
    if canonical_json(raw) != canonical_json({k: v for k, v in saved.items() if k not in enriched}):
        raise ValueError("merge implementation assessment changed from its original receipt")
    root = Path(spec["state_dir"]) / "attempts" / attempt["job_key"]
    _ancestors(root / "request.json")
    request = read_private(root / "request.json")
    journal = read_private(root / "native-process.json")
    metadata, intent = journal.get("provider_session", {}), journal.get("intent", {})
    output = metadata.get("output_candidate")
    if (request.get("spec") != spec or request.get("role") != "implement"
            or request.get("iteration") != attempt["iteration"]
            or request.get("candidate", {}).get("id") != attempt["candidate_id"]
            or request.get("workspace") != spec["checkout"]
            or DeliverySupervisor._job_key(request) != attempt["job_key"]
            or journal.get("phase") != "finished" or not journal.get("owned")
            or intent.get("run_id") != spec["run_id"]
            or intent.get("policy_digest") != spec["policy_digest"]
            or intent.get("cwd") != spec["checkout"]
            or metadata.get("role") != "implement"
            or metadata.get("iteration") != attempt["iteration"]
            or metadata.get("session_id") != attempt["session_id"]
            or saved.get("session_id") != attempt["session_id"]
            or metadata.get("result_digest") != digest(saved)
            or journal.get("result") != saved.get("native_process")
            or saved.get("process_cleanup") != "observed-native-confirmed"
            or saved.get("native_process", {}).get("monitoring_complete") is not True
            or not isinstance(output, dict) or set(output) != {"id", "head", "content_sha256"}):
        raise ValueError("merge implementation lost its controller-bound native output")
    return output


class MergeBroker:
    def __init__(self, store, spec, cancel_event=None):
        self.store, self.spec = store, spec
        self.cancel_event = cancel_event
        self.deadline = time.monotonic() + 9 * 60
        self.root = Path(spec["state_dir"]) / "merge"
        self.sequence = 0

    def api(self, endpoint, *, method="GET", body=None, raw=False):
        if (self.cancel_event is not None and self.cancel_event.is_set()
                or time.monotonic() >= self.deadline):
            raise RuntimeError("merge activity no longer owns a live execution boundary")
        self.sequence += 1
        argv = ["gh", "api", "--method", method, endpoint]
        if body is not None:
            argv.extend(["--input", "-"])
        result = subprocess.run(argv, input=json.dumps(body).encode() if body is not None else None,
                                capture_output=True, timeout=60, check=False)
        stdout, stderr = result.stdout.decode(), result.stderr.decode()
        write_private(self.root / (f"{_now().replace(':', '-')}-{self.sequence}.json"), {
            "at": _now(), "argv": argv, "body": body, "exit_code": result.returncode,
            "stdout": stdout, "stderr": stderr,
            "stdout_sha256": hashlib.sha256(result.stdout).hexdigest(),
            "stderr_sha256": hashlib.sha256(result.stderr).hexdigest(),
        })
        if result.returncode:
            raise RuntimeError("GitHub merge observation/effect failed: " + stderr[:500])
        return stdout if raw else json.loads(stdout)

    def intent(self, key, kind, request):
        with self.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute("SELECT * FROM delivery_effects WHERE effect_key=?",
                                  (key,)).fetchone()
            if previous:
                if previous["kind"] != kind or previous["request_json"] != canonical_json(request):
                    raise ValueError("merge effect identity belongs to another request")
                return False
            db.execute("INSERT INTO delivery_effects "
                       "(effect_key,run_id,kind,request_json,state,updated_at) "
                       "VALUES (?,?,?,?,'pending',?)",
                       (key, self.spec["run_id"], kind, canonical_json(request), _now()))
        return True

    def finish(self, key, result):
        with self.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE delivery_effects SET state='complete',observed_json=?,updated_at=? "
                       "WHERE effect_key=?", (canonical_json(result), _now(), key))

    def publication(self, pr):
        found = self.api(f"repos/{self.spec['github_repo']}/pulls/{pr['number']}")
        if (found["number"] != pr["number"] or found["html_url"] != pr["url"]
                or found["head"]["sha"] != pr["head"]
                or found["head"]["ref"] != self.spec["branch"] or found["draft"]
                or found["head"]["repo"]["full_name"] != self.spec["github_repo"]
                or found["base"]["repo"]["full_name"] != self.spec["github_repo"]
                or found["base"]["ref"] != self.spec["publication_base_ref"]):
            raise ValueError("merge PR changed its owned repository, branch, head or base")
        return found

    def merged(self, pr, found, head_tree):
        if not found["merged"] or found["state"] != "closed" or not found["merged_at"]:
            raise ValueError("owned PR is not merged")
        commit = self.api(f"repos/{self.spec['github_repo']}/git/commits/"
                          + found["merge_commit_sha"])
        parents = [p["sha"] for p in commit["parents"]]
        if commit["tree"]["sha"] != head_tree or len(parents) != 1:
            raise ValueError("actual squash merge differs from the verified tree or base")
        actual_base = parents[0]
        if actual_base != self.spec["base_sha"]:
            for before, after in ((self.spec["base_sha"], actual_base), (actual_base, pr["head"])):
                compare = self.api(f"repos/{self.spec['github_repo']}/compare/{before}...{after}")
                if (compare.get("status") not in {"ahead", "identical"}
                        or compare.get("base_commit", {}).get("sha") != before
                        or compare.get("merge_base_commit", {}).get("sha") != before):
                    raise ValueError("actual squash merge differs from the verified tree or base")
        return {"state": "confirmed", "number": pr["number"], "url": pr["url"],
                "head": pr["head"], "base": self.spec["base_sha"], "tree": head_tree,
                "actual_merge_base": actual_base,
                "merged_commit": commit["sha"], "merged_at": found["merged_at"]}

    def live_ci(self, pr, found, head_tree):
        repo = self.spec["github_repo"]
        if found["state"] != "open" or found["merged"]:
            raise ValueError("merge preflight requires the owned open PR")
        if found["base"]["sha"] != self.spec["base_sha"]:
            raise ValueError("upstream moved; a new verified integration candidate is required")
        hosted = self.api(f"repos/{repo}/git/commits/" + found["merge_commit_sha"])
        if (hosted["tree"]["sha"] != head_tree
                or [p["sha"] for p in hosted["parents"]] != [self.spec["base_sha"], pr["head"]]):
            raise ValueError("hosted checkout differs from the verified candidate/base")
        checks = self.api(f"repos/{repo}/commits/{pr['head']}/check-runs?per_page=100")
        if checks["total_count"] > 100:
            raise ValueError("CI check inventory exceeds the complete observed page")
        observed = []
        for name in self.spec["policy"]["required_ci"]:
            matches = [c for c in checks["check_runs"] if c["name"] == name]
            if len(matches) != 1:
                raise ValueError("required CI is missing or ambiguous: " + name)
            check = matches[0]
            match = re.fullmatch(r"https://github.com/" + re.escape(repo)
                                 + r"/actions/runs/(\d+)/job/(\d+)", check["details_url"])
            if (check["head_sha"] != pr["head"] or check["status"] != "completed"
                    or check["conclusion"] != "success" or not match):
                raise ValueError("required CI is not a successful head-bound hosted job")
            run = self.api(f"repos/{repo}/actions/runs/{match[1]}")
            job = self.api(f"repos/{repo}/actions/jobs/{match[2]}")
            if (run["event"] != "pull_request" or run["run_attempt"] != 1
                    or run["head_sha"] != pr["head"] or job["run_attempt"] != 1
                    or job["head_sha"] != pr["head"] or job["run_id"] != run["id"]
                    or job["name"] != name or job["status"] != "completed"
                    or job["conclusion"] != "success"):
                raise ValueError("required CI job has a different head, trigger or attempt")
            log_path = self.root / f"job-{job['id']}-log.json"
            if not log_path.exists():
                log = self.api(f"repos/{repo}/actions/jobs/{job['id']}/logs", raw=True)
                write_private(log_path, {"job_id": job["id"], "stdout": log,
                                         "sha256": hashlib.sha256(log.encode()).hexdigest()})
            retained = read_private(log_path)
            log = retained["stdout"]
            if (retained["job_id"] != job["id"]
                    or retained["sha256"] != hashlib.sha256(log.encode()).hexdigest()
                    or hosted["sha"] not in log
                    or "Merge " + pr["head"] + " into " + self.spec["base_sha"] not in log):
                raise ValueError("required CI checkout log differs from the frozen whole tree")
            observed.append({"name": name, "check_id": check["id"], "run_id": run["id"],
                             "job_id": job["id"], "attempt": 1,
                             "log_sha256": retained["sha256"], "log": str(log_path)})
        return {"head": pr["head"], "base": self.spec["base_sha"],
                "hosted_checkout": hosted["sha"], "hosted_tree": head_tree, "jobs": observed}

    def require_base_enforcement(self):
        """GitHub must reject base movement atomically at the merge operation."""
        repo = self.spec["github_repo"]
        branch = quote(self.spec["publication_base_ref"], safe="")
        rules = []
        # Observe every applicable active rule, including later pages.
        for page in range(1, 101):
            items = self.api(f"repos/{repo}/rules/branches/{branch}?per_page=100&page={page}")
            if not isinstance(items, list):
                raise ValueError("GitHub branch enforcement inventory is invalid")
            rules.extend(items)
            if len(items) < 100:
                break
        else:
            raise ValueError("GitHub branch enforcement inventory is incomplete")
        required = set(self.spec["policy"]["required_ci"])
        for rule in rules:
            params = rule.get("parameters", {})
            contexts = {c["context"] for c in params.get("required_status_checks", [])}
            if (rule.get("type") != "required_status_checks"
                    or rule.get("ruleset_source_type") != "Repository"
                    or rule.get("ruleset_source") != repo
                    or params.get("strict_required_status_checks_policy") is not True
                    or not required or not required <= contexts):
                continue
            found = self.api(f"repos/{repo}/rulesets/{rule['ruleset_id']}")
            if (found.get("id") == rule["ruleset_id"] and found.get("target") == "branch"
                    and found.get("enforcement") == "active"
                    and found.get("source_type") == "Repository" and found.get("source") == repo
                    and found.get("bypass_actors") == []
                    and any(r.get("type") == "required_status_checks"
                            and r.get("parameters") == params for r in found.get("rules", []))):
                return {"ruleset_id": found["id"], "required_ci": sorted(required),
                        "strict": True, "bypass_actors": []}
        raise ValueError(
            "merge requires active strict GitHub checks without bypass on its base branch")

    def close_issue(self, merged):
        repo = self.spec["github_repo"]
        prefix = f"https://github.com/{repo}/issues/"
        if not self.spec["issue_url"].startswith(prefix):
            raise ValueError("merge cannot close an unrelated issue")
        number = self.spec["issue_url"].removeprefix(prefix)
        if not number.isdecimal():
            raise ValueError("merge issue identity is invalid")
        endpoint = f"repos/{repo}/issues/{number}"
        found = self.api(endpoint)
        if found.get("pull_request") or found["html_url"] != self.spec["issue_url"]:
            raise ValueError("merge closure target is not its admitted issue")
        key = "merge-issue:" + self.spec["run_id"]
        request = {"issue": self.spec["issue_url"], "merged_commit": merged["merged_commit"]}
        fresh = self.intent(key, "merge_issue", request)
        if found["state"] != "closed":
            if not fresh:
                raise ValueError("original issue closure is unresolved; no mutation replay")
            self.api(endpoint, method="PATCH",
                     body={"state": "closed", "state_reason": "completed"})
            found = self.api(endpoint)
        if found["state"] != "closed" or found.get("state_reason") != "completed":
            raise ValueError("merged issue closure has not been independently confirmed")
        result = {**request, "state": "confirmed", "issue_state": "CLOSED"}
        self.finish(key, result)
        return result


def merge_verified(store, spec, request, cancel_event=None):
    from .delivery_policy_recovery import work_binding

    candidate, pr, checks = (request[key] for key in ("candidate", "pull_request", "checks"))
    with store._connect() as db:
        work_binding(store, spec, db)
        row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (spec["run_id"],)).fetchone()
        attempts = [dict(a) for a in db.execute(
            "SELECT * FROM delivery_attempts WHERE run_id=? ORDER BY rowid", (spec["run_id"],))]
        publication = db.execute(
            "SELECT * FROM delivery_effects WHERE effect_key=? "
            "AND kind='publish' AND state='complete'",
            (f"publish:{spec['run_id']}:{row['iteration']}",),
        ).fetchone()
        saved = json.loads(row["checks_json"] or "{}")
        if (json.loads(row["candidate_json"] or "null") != candidate
                or json.loads(row["pr_json"] or "null") != pr
                or any(saved.get(k) != checks.get(k)
                       for k in ("prepublish", "local", "review", "qa", "ci", "browser_qa"))):
            raise ValueError("merge request differs from the durable candidate and gates")
        if not publication or json.loads(publication["observed_json"] or "null") != pr:
            raise ValueError("merge has no completed original publication effect")
    implementations = [a for a in attempts if a["role"] == "implement"]
    if implementations:
        implementations[-1]["controller_output_candidate"] = native_implementation_output(
            spec, implementations[-1])
    require_merge_gates(spec, candidate, pr, checks, attempts,
                       json.loads(publication["request_json"]))
    resources = observe_finalized_resources(spec)
    broker = MergeBroker(store, spec, cancel_event)
    head = broker.api(f"repos/{spec['github_repo']}/git/commits/{pr['head']}")
    head_tree = head["tree"]["sha"]
    found = broker.publication(pr)
    key = "merge:" + spec["run_id"]
    intent = {"candidate": candidate, "pr": pr, "base": spec["base_sha"], "tree": head_tree,
              "policy_digest": spec["policy_digest"], "checks_sha256": digest({
                  k: saved.get(k) for k in (
                      "prepublish", "local", "review", "qa", "ci", "browser_qa")})}
    if found["merged"]:
        with store._connect() as db:
            prior = db.execute("SELECT * FROM delivery_effects WHERE effect_key=?",
                               (key,)).fetchone()
        if not prior or prior["request_json"] != canonical_json(intent):
            raise ValueError("merged PR has no matching original owned merge intent")
    else:
        ci = broker.live_ci(pr, found, head_tree)
        write_private(broker.root / "ci.json", ci)
        enforcement = broker.require_base_enforcement()
        write_private(broker.root / "base-enforcement.json", enforcement)
        if not broker.intent(key, "merge", intent):
            raise ValueError("original merge is unresolved; no mutation replay")
        broker.api(f"repos/{spec['github_repo']}/pulls/{pr['number']}/merge", method="PUT",
                   body={"sha": pr["head"], "merge_method": "squash"})
        found = broker.publication(pr)
    merged = broker.merged(pr, found, head_tree)
    closure = broker.close_issue(merged)
    result = {**merged, "issue": closure, "resources": resources, "candidate_id": candidate["id"]}
    write_private(broker.root / "receipt.json", result)
    broker.finish(key, result)
    return result


def merged_readback(store, spec, candidate, pr):
    broker = MergeBroker(store, spec)
    receipt = read_private(broker.root / "receipt.json")
    if receipt["candidate_id"] != candidate["id"] or receipt["head"] != candidate["head"]:
        raise ValueError("merged terminal candidate changed")
    found = broker.publication(pr)
    current = broker.merged(pr, found, receipt["tree"])
    if any(current[k] != receipt[k] for k in current):
        raise ValueError("merged terminal readback changed its original receipt")
    issue = broker.api(f"repos/{spec['github_repo']}/issues/" + spec["issue_url"].rsplit('/', 1)[1])
    if issue["html_url"] != spec["issue_url"] or issue["state"] != "closed":
        raise ValueError("merged issue is no longer closed")
    return current
