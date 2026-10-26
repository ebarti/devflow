"""Independent exact original ordinary executions, never PR/ruleset eligibility."""

from __future__ import annotations

import hashlib
import re

from .contracts import digest
from .delivery_ci_inventory_contract import approved_profile, document, relative, workflow_plan
from .delivery_ci_inventory_io import MAX_LOG, MAX_SOURCE, sha256
from .delivery_ci_inventory_logs import executed_log

APP = {"id": 15368, "slug": "github-actions"}
METHOD = "acknowledged_original_dispatch_inventory/v1"


def source_key(pin):
    return pin["repository"] + "@" + pin["commit"] + ":" + pin["path"]


def _same_repo(value, subject):
    return (
        isinstance(value, dict)
        and value.get("id") == subject["repository_id"]
        and value.get("full_name") == subject["repository"]
    )


def _success(value):
    return value.get("status") == "completed" and value.get("conclusion") == "success"


def _actor(value, subject):
    return (
        isinstance(value, dict)
        and {key: value.get(key) for key in ("id", "login")} == subject["actor"]
    )


def _run_identity(run):
    # Repository/social counters are neither source nor execution authority.
    # All identity, source, actor and terminal fields remain freshly compared.
    result = {
        key: run.get(key)
        for key in (
            "id",
            "workflow_id",
            "run_attempt",
            "event",
            "path",
            "head_sha",
            "head_branch",
            "status",
            "conclusion",
            "referenced_workflows",
            "check_suite_id",
        )
    }
    for key in ("repository", "head_repository"):
        result[key] = {k: (run.get(key) or {}).get(k) for k in ("id", "full_name")}
    for key in ("actor", "triggering_actor"):
        result[key] = {k: (run.get(key) or {}).get(k) for k in ("id", "login")}
    return result


def _source_inputs(reader, profile):
    sources, commits, trees, bindings = {}, {}, {}, {}
    pins = profile["sources"]
    if not isinstance(pins, list) or not 1 <= len(pins) <= 128:
        raise ValueError("ordinary CI source graph is incomplete or excessive")
    for pin in pins:
        if (
            not isinstance(pin, dict)
            or set(pin) != {"repository", "commit", "path", "sha256", "blob_sha1", "bytes", "role"}
            or pin["repository"] not in reader.repositories
            or not re.fullmatch(r"[0-9a-f]{40}", str(pin["commit"]))
            or not re.fullmatch(r"[0-9a-f]{40}", str(pin["blob_sha1"]))
            or not re.fullmatch(r"[0-9a-f]{64}", str(pin["sha256"]))
            or type(pin["bytes"]) is not int
            or not 0 < pin["bytes"] <= MAX_SOURCE
        ):
            raise ValueError("ordinary CI source pin is malformed")
        path = relative(pin["path"])
        key = source_key(pin)
        if key in sources:
            raise ValueError("ordinary CI source graph has duplicate identities")
        commit_key = (pin["repository"], pin["commit"])
        if commit_key not in commits:
            value = reader.get(f"repos/{pin['repository']}/git/commits/{pin['commit']}")
            if value.get("sha") != pin["commit"] or not re.fullmatch(
                r"[0-9a-f]{40}", str((value.get("tree") or {}).get("sha"))
            ):
                raise ValueError("ordinary CI immutable source commit changed")
            commits[commit_key] = value["tree"]["sha"]
        tree = commits[commit_key]
        entry = None
        parts = path.split("/")
        for depth, component in enumerate(parts):
            tree_key = (pin["repository"], tree)
            if tree_key not in trees:
                value = reader.get(f"repos/{pin['repository']}/git/trees/{tree}")
                if (
                    value.get("sha") != tree
                    or value.get("truncated") is not False
                    or not isinstance(value.get("tree"), list)
                    or len(value["tree"]) > 4096
                    or any(not isinstance(item, dict) for item in value["tree"])
                ):
                    raise ValueError("ordinary CI source tree inventory is incomplete")
                trees[tree_key] = value["tree"]
            matched = [item for item in trees[tree_key] if item.get("path") == component]
            if len(matched) != 1:
                raise ValueError("ordinary CI source path is absent or ambiguous")
            entry = matched[0]
            if depth + 1 < len(parts):
                if entry.get("type") != "tree" or entry.get("mode") != "040000":
                    raise ValueError("ordinary CI source ancestor is not a pinned tree")
                tree = entry["sha"]
        if (
            entry.get("type") != "blob"
            or entry.get("mode") not in {"100644", "100755"}
            or entry.get("sha") != pin["blob_sha1"]
        ):
            raise ValueError("ordinary CI source blob is linked, foreign or changed")
        raw = reader.get(
            f"repos/{pin['repository']}/contents/{path}?ref={pin['commit']}",
            raw=True,
            limit=MAX_SOURCE,
        )
        blob = hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
        if len(raw) != pin["bytes"] or sha256(raw) != pin["sha256"] or blob != pin["blob_sha1"]:
            raise ValueError("ordinary CI source bytes changed from the reviewed graph")
        sources[key] = raw
        bindings[key] = {**pin, "mode": entry["mode"], "root_tree": commits[commit_key]}
    return sources, bindings


def _plans(profile, sources):
    subject = profile["subject"]
    actions = {}
    for action in profile["actions"]:
        uses = action["uses"]
        if (
            not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}", uses)
            or uses in actions
            or action["metadata"] not in sources
        ):
            raise ValueError("ordinary CI pinned action identity is unsupported")
        repository, commit = uses.split("@")
        metadata_key = repository + "@" + commit + ":action.yml"
        if action["metadata"] != metadata_key:
            raise ValueError("ordinary CI action metadata left its original immutable source")
        metadata = document(sources[metadata_key])
        run = metadata.get("runs") or {}
        if run.get("using") not in {"node20", "node24"} or not isinstance(run.get("main"), str):
            raise ValueError("ordinary CI action execution format is unsupported")
        entrypoints = {
            repository + "@" + commit + ":" + relative(run[key])
            for key in ("main", "post")
            if key in run
        }
        if (
            entrypoints != set(action["entrypoints"])
            or not entrypoints.issubset(sources)
            or not set(action["sources"]).issubset(sources)
            or not entrypoints.issubset(action["sources"])
        ):
            raise ValueError("ordinary CI action main/post source is incomplete")
        actions[uses] = {"has_post": "post" in run, "post_if": run.get("post-if", "always()")}
    plans, used = {}, set()
    for workflow in profile["workflows"]:
        pin = workflow["source"]
        if (
            pin not in sources
            or pin != subject["repository"] + "@" + subject["head"] + ":" + workflow["path"]
        ):
            raise ValueError("ordinary CI workflow is not the exact published source")
        plan = workflow_plan(
            sources[pin],
            repository=subject["repository"],
            branch=subject["branch"],
            inputs=workflow["inputs"],
        )
        if digest(plan) != workflow["plan_sha256"]:
            raise ValueError("ordinary CI workflow selector/matrix/input contract changed")
        for job in plan:
            post = []
            for step in job["steps"]:
                if step["enabled"] and "uses" in step["source"]:
                    uses = step["source"]["uses"]
                    if uses not in actions:
                        raise ValueError("ordinary CI executed an unbound action")
                    used.add(uses)
                    if actions[uses]["has_post"]:
                        if actions[uses]["post_if"] not in {"always()", "success()"}:
                            raise ValueError("ordinary CI action post condition is unsupported")
                        post.insert(0, "Post " + step["source"]["name"])
            job["post_steps"] = post
        plans[workflow["id"]] = plan
    if used != set(actions):
        raise ValueError("ordinary CI action graph contains unselected authority")
    return plans


def _remote_identity(reader, profile, required, expected_tree):
    subject = profile["subject"]
    repo, head = subject["repository"], subject["head"]
    account = reader.account()
    repository = reader.get("repos/" + repo)
    pr = reader.get(f"repos/{repo}/pulls/{subject['pr']}")
    commit = reader.get(f"repos/{repo}/git/commits/{head}")
    if (
        not _actor(account, subject)
        or not _same_repo(repository, subject)
        or pr.get("number") != subject["pr"]
        or pr.get("state") != "open"
        or pr.get("draft") is not False
        or pr.get("merged") is not False
        or not _same_repo((pr.get("head") or {}).get("repo"), subject)
        or not _same_repo((pr.get("base") or {}).get("repo"), subject)
        or pr["head"].get("sha") != head
        or pr["head"].get("ref") != subject["branch"]
        or commit.get("sha") != head
        or (commit.get("tree") or {}).get("sha") != expected_tree
    ):
        raise ValueError("ordinary CI current account/repository/PR/head/tree authority changed")
    canonical = reader.rollup(repo, subject["pr"], head)
    if (
        canonical["headRefName"] != subject["branch"]
        or not canonical["contexts"]
        or any(
            x.get("name") in required
            for x in canonical["contexts"]
            if x["__typename"] == "CheckRun"
        )
    ):
        raise ValueError("ordinary CI original canonical-rollup omission predicate changed")
    checks = reader.pages(f"repos/{repo}/commits/{head}/check-runs", "check_runs")
    names = [x for x in checks if x.get("name") in required]
    if (
        len(names) != len(required)
        or {x.get("name") for x in names} != set(required)
        or len({x["name"] for x in names}) != len(names)
    ):
        raise ValueError("ordinary CI required check inventory is missing or ambiguous")
    runs = reader.pages(f"repos/{repo}/actions/runs", "workflow_runs", head=head)
    workflows = {w["id"] for w in profile["workflows"]}
    relevant = [
        r
        for r in runs
        if r.get("workflow_id") in workflows and r.get("event") == "workflow_dispatch"
    ]
    if (
        len(relevant) != len(profile["workflows"])
        or {r["id"] for r in relevant} != {w["run_id"] for w in profile["workflows"]}
        or any(r.get("head_sha") != head or r.get("run_attempt") != 1 for r in relevant)
    ):
        raise ValueError("ordinary CI selected run inventory has another or later attempt")
    return canonical, {x["name"]: x for x in names}


def collect(reader, profile, *, required, expected_tree):
    """Observe independently. The caller still owns current local authority and ACK."""
    profile = approved_profile(profile)
    subject = profile["subject"]
    if (
        profile["required_ci"] != list(required)
        or len(required) != 11
        or len(set(required)) != 11
        or len(profile["workflows"]) != 5
        or len({w["id"] for w in profile["workflows"]}) != 5
        or len({w["run_id"] for w in profile["workflows"]}) != 5
        or set(profile["jobs"]) != set(required)
    ):
        raise ValueError("ordinary CI inventory differs from its full original frozen contract")
    before, checks = _remote_identity(reader, profile, required, expected_tree)
    sources, source_bindings = _source_inputs(reader, profile)
    plans = _plans(profile, sources)
    repo, head = subject["repository"], subject["head"]
    observed, normalized, run_facts, selected_authority = [], {}, [], []
    for workflow in profile["workflows"]:
        run = reader.get(f"repos/{repo}/actions/runs/{workflow['run_id']}")
        current = reader.get(f"repos/{repo}/actions/workflows/{workflow['id']}")
        if (
            run.get("id") != workflow["run_id"]
            or run.get("workflow_id") != workflow["id"]
            or run.get("run_attempt") != 1
            or run.get("event") != "workflow_dispatch"
            or run.get("path") != workflow["path"]
            or run.get("head_sha") != head
            or run.get("head_branch") != subject["branch"]
            or not _success(run)
            or not _same_repo(run.get("repository"), subject)
            or not _same_repo(run.get("head_repository"), subject)
            or not _actor(run.get("actor"), subject)
            or not _actor(run.get("triggering_actor"), subject)
            or run.get("referenced_workflows") != []
            or current.get("id") != workflow["id"]
            or current.get("path") != workflow["path"]
            or current.get("state") != "active"
        ):
            raise ValueError("ordinary CI original workflow/run/actor/attempt binding changed")
        critical = _run_identity(run)
        suite = reader.get(f"repos/{repo}/check-suites/{run['check_suite_id']}")
        if (
            suite.get("id") != run["check_suite_id"]
            or suite.get("head_sha") != head
            or suite.get("head_branch") != subject["branch"]
            or {k: (suite.get("app") or {}).get(k) for k in APP} != APP
            or not _success(suite)
        ):
            raise ValueError("ordinary CI original check suite binding changed")
        jobs = reader.pages(f"repos/{repo}/actions/runs/{run['id']}/attempts/1/jobs", "jobs")
        plan = plans[workflow["id"]]
        by_name = {j["name"]: j for j in plan}
        if len({j.get("name") for j in jobs}) != len(jobs) or any(
            j.get("name") not in by_name for j in jobs
        ):
            raise ValueError("ordinary CI job inventory has unknown or duplicate names")
        for job in jobs:
            source = by_name[job["name"]]
            if not source["enabled"]:
                excluded = [
                    x
                    for x in profile["excluded_jobs"]
                    if x["workflow_id"] == workflow["id"] and x["name"] == job["name"]
                ]
                if (
                    len(excluded) != 1
                    or job.get("id") != excluded[0]["job_id"]
                    or job.get("run_id") != run["id"]
                    or job.get("head_sha") != head
                    or job.get("steps") != []
                    or ("run_attempt" in job and job["run_attempt"] != 1)
                    or ("head_branch" in job and job["head_branch"] != subject["branch"])
                    or job.get("conclusion") != "skipped"
                    or job.get("status") != "completed"
                ):
                    raise ValueError("ordinary CI excluded publication job unexpectedly ran")
                observed.append(
                    {
                        "name": job["name"],
                        "job_id": job["id"],
                        "source_condition": False,
                        "observed_conclusion": "skipped",
                    }
                )
                continue
            name = job["name"]
            if name not in required:
                raise ValueError("ordinary CI source includes an unselected active job")
            pin, check = profile["jobs"][name], checks[name]
            details = f"https://github.com/{repo}/actions/runs/{run['id']}/job/{job['id']}"
            if (
                job.get("id") != pin["job_id"]
                or check.get("id") != pin["check_id"]
                or run["id"] != pin["run_id"]
                or workflow["id"] != pin["workflow_id"]
                or run["check_suite_id"] != pin["suite_id"]
                or (check.get("check_suite") or {}).get("id") != run["check_suite_id"]
                or {k: (check.get("app") or {}).get(k) for k in APP} != APP
                or check.get("head_sha") != head
                or not _success(check)
                or check.get("details_url") != details
                or check.get("html_url") != details
                or check.get("url")
                != f"https://api.github.com/repos/{repo}/check-runs/{check['id']}"
                or job.get("check_run_url") != check["url"]
                or job.get("run_id") != run["id"]
                or job.get("run_attempt") != 1
                or job.get("head_sha") != head
                or job.get("head_branch") != subject["branch"]
                or job.get("html_url") != details
                or not _success(job)
                or source["runs_on"] not in (job.get("labels") or [])
                or job.get("runner_group_name") != "GitHub Actions"
                or pin["post_steps"] != source["post_steps"]
            ):
                raise ValueError("ordinary CI exact check/app/run/job/checkout contract changed")
            raw = reader.get(f"repos/{repo}/actions/jobs/{job['id']}/logs", raw=True, limit=MAX_LOG)
            log = executed_log(raw, job, source, pin, head=head)
            observed.append(
                {
                    "name": name,
                    "check_id": check["id"],
                    "job_id": job["id"],
                    "run_id": run["id"],
                    "workflow_id": workflow["id"],
                    "suite_id": run["check_suite_id"],
                    "attempt": 1,
                    "app": APP,
                    "source_condition": True,
                    **log,
                }
            )
            normalized[name] = {
                "__typename": "CheckRun",
                "name": name,
                "conclusion": "SUCCESS",
                "status": "COMPLETED",
                "detailsUrl": details,
                "execution_check_id": check["id"],
                "source": METHOD,
            }
        if any(j["enabled"] and not any(x["name"] == j["name"] for x in jobs) for j in plan):
            raise ValueError("ordinary CI workflow did not execute every required matrix job")
        if any(
            x["run_id"] == run["id"] and not any(j.get("id") == x["job_id"] for j in jobs)
            for x in profile["excluded_jobs"]
        ):
            raise ValueError("ordinary CI excluded publication job inventory changed")
        artifacts = reader.pages(f"repos/{repo}/actions/runs/{run['id']}/artifacts", "artifacts")
        selected_authority.append(
            (
                workflow,
                critical,
                {key: current.get(key) for key in ("id", "path", "state")},
                jobs,
                suite,
                artifacts,
            )
        )
        expected_artifacts = [x for x in profile["artifacts"] if x["run_id"] == run["id"]]
        if {x["id"] for x in artifacts} != {x["id"] for x in expected_artifacts}:
            raise ValueError("ordinary CI uploaded report/artifact inventory changed")
        for pin in expected_artifacts:
            item = next(x for x in artifacts if x["id"] == pin["id"])
            link = item.get("workflow_run") or {}
            if (
                item.get("name") != pin["name"]
                or item.get("size_in_bytes") != pin["bytes"]
                or item.get("digest") != "sha256:" + pin["sha256"]
                or item.get("expired") is not False
                or link.get("id") != run["id"]
                or link.get("repository_id") != subject["repository_id"]
                or link.get("head_repository_id") != subject["repository_id"]
                or link.get("head_sha") != head
                or link.get("head_branch") != subject["branch"]
            ):
                raise ValueError("ordinary CI source-required artifact has foreign provenance")
            raw = reader.get(
                f"repos/{repo}/actions/artifacts/{pin['id']}/zip", raw=True, limit=MAX_LOG
            )
            if len(raw) != pin["bytes"] or sha256(raw) != pin["sha256"]:
                raise ValueError("ordinary CI source-required artifact bytes changed")
        run_facts.append(
            {
                "workflow_id": workflow["id"],
                "run_id": run["id"],
                "attempt": 1,
                "event": "workflow_dispatch",
                "path": workflow["path"],
                "source": workflow["source"],
                "inputs": workflow["inputs"],
                "actor": subject["actor"],
                "job_ids": sorted(j["id"] for j in jobs),
                "artifacts": expected_artifacts,
            }
        )
    for workflow, original, active, jobs, suite, artifacts in selected_authority:
        run = reader.get(f"repos/{repo}/actions/runs/{workflow['run_id']}")
        current = reader.get(f"repos/{repo}/actions/workflows/{workflow['id']}")
        if _run_identity(run) != original or {key: current.get(key) for key in active} != active:
            raise ValueError(
                "ordinary CI original workflow/run authority changed during observation"
            )
        if (
            reader.pages(f"repos/{repo}/actions/runs/{run['id']}/attempts/1/jobs", "jobs") != jobs
            or reader.get(f"repos/{repo}/check-suites/{run['check_suite_id']}") != suite
            or reader.pages(f"repos/{repo}/actions/runs/{run['id']}/artifacts", "artifacts")
            != artifacts
        ):
            raise ValueError("ordinary CI completed job/suite/artifact authority changed")
    after, after_checks = _remote_identity(reader, profile, required, expected_tree)
    fields = (
        "id",
        "name",
        "head_sha",
        "status",
        "conclusion",
        "details_url",
        "check_suite",
        "app",
        "url",
        "html_url",
    )
    if (
        before != after
        or {n: {k: x.get(k) for k in fields} for n, x in checks.items()}
        != {n: {k: x.get(k) for k in fields} for n, x in after_checks.items()}
        or set(normalized) != set(required)
    ):
        raise ValueError("ordinary CI authority changed during complete observation")
    return {
        "method": METHOD,
        "state": "passed",
        "profile_sha256": digest(profile),
        "subject": subject,
        "required_ci": list(required),
        "checks": normalized,
        "canonical_rollup": before,
        "canonical_missing_names": list(required),
        "PR_ruleset_eligibility": False,
        "merge_context_verified": False,
        "workflows": run_facts,
        "jobs": observed,
        "source_bindings": source_bindings,
        "checkout_tree": expected_tree,
        "complete_source_and_report_observation": True,
    }
