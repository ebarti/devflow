"""Neutral authored remote-boundary data, never replayed provider/GitHub output.

The in-process pin substitution exists only in pytest. No production request,
configuration, environment variable or HTTP header can supply another profile.
"""

from __future__ import annotations

import copy
import hashlib
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_ci_inventory_contract import SCHEMA, workflow_plan
from devflow_temporal.delivery_ci_inventory_logs import execution_facts, lines, report

NAMES = [
    "Owned release",
    "Owned scripts",
    "Owned docs",
    "Owned API",
    "Owned web",
    "Owned storybook",
    "Owned e2e",
    "Owned extension",
    "Owned Python 3.11",
    "Owned Python 3.12",
    "Owned Python 3.13",
]


def fixture(
    *,
    head="a" * 40,
    tree="e" * 40,
    branch="owned-ci",
    run_id="run-1",
    policy_digest="c" * 64,
    candidate_id="d" * 64,
    content_sha256="f" * 64,
    pr_number=10,
):
    repo = "example/fixture"
    subject = {
        "repository": repo,
        "repository_id": 42,
        "actor": {"id": 37, "login": "fixture"},
        "pr": pr_number,
        "head": head,
        "tree": tree,
        "branch": branch,
        "run_id": run_id,
        "policy_digest": policy_digest,
        "intake_policy_digest": policy_digest,
        "candidate_id": candidate_id,
        "content_sha256": content_sha256,
    }
    profile = {
        "schema": SCHEMA,
        "subject": subject,
        "required_ci": NAMES[:],
        "sources": [],
        "actions": [],
        "workflows": [],
        "jobs": {},
        "artifacts": [],
        "excluded_jobs": [],
        "PR_ruleset_eligibility": False,
        "merge_context_verified": False,
    }
    data, raw_sources, tree_nodes = {}, {}, {}

    def pin(repository, commit, path, raw):
        blob = hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
        key = repository + "@" + commit + ":" + path
        profile["sources"].append(
            {
                "repository": repository,
                "commit": commit,
                "path": path,
                "sha256": hashlib.sha256(raw).hexdigest(),
                "bytes": len(raw),
                "blob_sha1": blob,
                "role": "neutral owned source",
            }
        )
        raw_sources[key] = raw
        root = tree if repository == repo else "9" * 40
        data[f"repos/{repository}/git/commits/{commit}"] = {"sha": commit, "tree": {"sha": root}}
        current = root
        parts = path.split("/")
        for n, part in enumerate(parts):
            entries = tree_nodes.setdefault((repository, current), {})
            if n == len(parts) - 1:
                entries[part] = {"path": part, "mode": "100644", "type": "blob", "sha": blob}
            else:
                folder = hashlib.sha1((repository + "/".join(parts[: n + 1])).encode()).hexdigest()
                entries[part] = {"path": part, "mode": "040000", "type": "tree", "sha": folder}
                current = folder
        data[f"repos/{repository}/contents/{path}?ref={commit}"] = raw
        return key

    action_repo, revision = "owned/checkout", "b" * 40
    uses = action_repo + "@" + revision
    metadata = pin(
        action_repo,
        revision,
        "action.yml",
        b"name: Owned checkout\nruns:\n  using: node20\n"
        b"  main: dist/index.js\n  post: dist/post.js\n",
    )
    main = pin(action_repo, revision, "dist/index.js", b"// Owned neutral entrypoint.\n")
    post = pin(action_repo, revision, "dist/post.js", b"// Owned neutral post.\n")
    profile["actions"] = [
        {
            "uses": uses,
            "metadata": metadata,
            "entrypoints": [main, post],
            "sources": [metadata, main, post],
        }
    ]
    all_checks, all_runs = [], []
    account = copy.deepcopy(subject["actor"])
    repository = {"id": 42, "full_name": repo}
    data["user"] = account
    data["repos/" + repo] = repository
    data[f"repos/{repo}/pulls/{pr_number}"] = {
        "number": pr_number,
        "state": "open",
        "draft": False,
        "merged": False,
        "head": {"sha": head, "ref": branch, "repo": repository},
        "base": {"ref": "main", "sha": "6" * 40, "repo": repository},
    }
    index = 0
    for ordinal, group in enumerate(([NAMES[0]], [NAMES[1]], [NAMES[2]], NAMES[3:8], NAMES[8:])):
        workflow_id, run_number, suite = 1500 + ordinal, 2000 + ordinal, 3000 + ordinal
        path = f".github/workflows/owned-{ordinal}.yml"
        jobs_source = {}
        for name in group:
            jobs_source["job" + str(index)] = {
                "name": name,
                "runs-on": "ubuntu-latest",
                "steps": [
                    {"name": "Checkout", "uses": uses, "with": {"persist-credentials": False}},
                    {
                        "name": "Test",
                        "run": "python -m pytest tests/test_owned.py -q",
                        "env": {"OWNED_CI": "1"},
                    },
                ],
            }
            index += 1
        raw = canonical_json(
            {"name": "Owned workflow", "on": {"workflow_dispatch": None}, "jobs": jobs_source}
        ).encode()
        key = pin(repo, head, path, raw)
        plan = workflow_plan(raw, repository=repo, branch=branch, inputs={})
        profile["workflows"].append(
            {
                "id": workflow_id,
                "run_id": run_number,
                "path": path,
                "source": key,
                "inputs": {},
                "plan_sha256": digest(plan),
            }
        )
        run = {
            "id": run_number,
            "workflow_id": workflow_id,
            "run_attempt": 1,
            "event": "workflow_dispatch",
            "path": path,
            "head_sha": head,
            "head_branch": branch,
            "status": "completed",
            "conclusion": "success",
            "repository": repository,
            "head_repository": repository,
            "actor": account,
            "triggering_actor": account,
            "referenced_workflows": [],
            "check_suite_id": suite,
        }
        all_runs.append(run)
        data[f"repos/{repo}/actions/runs/{run_number}"] = run
        data[f"repos/{repo}/actions/workflows/{workflow_id}"] = {
            "id": workflow_id,
            "path": path,
            "state": "active",
        }
        data[f"repos/{repo}/check-suites/{suite}"] = {
            "id": suite,
            "head_sha": head,
            "head_branch": branch,
            "app": {"id": 15368, "slug": "github-actions"},
            "status": "completed",
            "conclusion": "success",
        }
        actual_jobs = []
        for planned in plan:
            number = 7000 + len(all_checks)
            details = f"https://github.com/{repo}/actions/runs/{run_number}/job/{number}"
            check_url = f"https://api.github.com/repos/{repo}/check-runs/{number}"
            step_meta = [
                {"name": name, "number": n + 1, "status": "completed", "conclusion": "success"}
                for n, name in enumerate(
                    ("Set up job", "Checkout", "Test", "Post Checkout", "Complete job")
                )
            ]
            job = {
                "id": number,
                "name": planned["name"],
                "run_id": run_number,
                "run_attempt": 1,
                "head_sha": head,
                "head_branch": branch,
                "status": "completed",
                "conclusion": "success",
                "steps": step_meta,
                "html_url": details,
                "check_run_url": check_url,
                "labels": ["ubuntu-latest"],
                "runner_group_name": "GitHub Actions",
            }
            log = (
                f"Download action repository '{uses}' (SHA:{revision})\n"
                f"##[group]Run {uses}\nwith:\n  persist-credentials: false\n##[endgroup]\n"
                f"[command]/usr/bin/git log -1 --format=%H\n{head}\n"
                "##[group]Run python -m pytest tests/test_owned.py -q\n"
                "python -m pytest tests/test_owned.py -q\nenv:\n  OWNED_CI: 1\n##[endgroup]\n"
                "\x1b[32m2 passed, 1 skipped in 0.01s\x1b[0m\n"
            ).encode()
            data[f"repos/{repo}/actions/jobs/{number}/logs"] = log
            profile["jobs"][planned["name"]] = {
                "job_id": number,
                "check_id": number,
                "run_id": run_number,
                "workflow_id": workflow_id,
                "suite_id": suite,
                "steps_sha256": digest(step_meta),
                "canonical_job_sha256": digest(job),
                "post_steps": ["Post Checkout"],
                "log_sha256": hashlib.sha256(log).hexdigest(),
                "log_bytes": len(log),
                "execution": execution_facts(lines(log), planned),
                "reports": [
                    {"step_index": 1, "format": "pytest", "observed": report("pytest", lines(log))}
                ],
            }
            actual_jobs.append(job)
            all_checks.append(
                {
                    "id": number,
                    "name": planned["name"],
                    "head_sha": head,
                    "status": "completed",
                    "conclusion": "success",
                    "details_url": details,
                    "html_url": details,
                    "url": check_url,
                    "check_suite": {"id": suite},
                    "app": {"id": 15368, "slug": "github-actions"},
                }
            )
        data[f"repos/{repo}/actions/runs/{run_number}/attempts/1/jobs"] = {"jobs": actual_jobs}
        data[f"repos/{repo}/actions/runs/{run_number}/artifacts"] = {"artifacts": []}
    data[f"repos/{repo}/commits/{head}/check-runs"] = {"check_runs": all_checks}
    data[f"repos/{repo}/actions/runs"] = {"workflow_runs": all_runs}
    for (repository_name, sha), entries in tree_nodes.items():
        data[f"repos/{repository_name}/git/trees/{sha}"] = {
            "sha": sha,
            "truncated": False,
            "tree": list(entries.values()),
        }
    canonical = {
        "__typename": "CheckRun",
        "id": "canonical-owned",
        "name": "Owned CodeQL",
        "status": "COMPLETED",
        "conclusion": "SUCCESS",
        "detailsUrl": "https://example.test",
    }
    case = SimpleNamespace(
        profile=profile,
        data=data,
        calls=[],
        sources=raw_sources,
        canonical=canonical,
        callback=None,
    )

    def transport(command, _limit, _remaining):
        # Neutral remote boundary only: the production HTTP/framing/page owner
        # and full source/log/checkout/report validators remain unmodified.
        case.calls.append(command[:])
        if case.callback:
            case.callback(command, case)
        if "graphql" in command:
            value = {
                "data": {
                    "repository": {
                        "pullRequest": {
                            "number": pr_number,
                            "state": "OPEN",
                            "isDraft": False,
                            "headRefOid": head,
                            "headRefName": branch,
                            "commits": {
                                "nodes": [
                                    {
                                        "commit": {
                                            "oid": head,
                                            "statusCheckRollup": {
                                                "contexts": {
                                                    "totalCount": 1,
                                                    "nodes": [copy.deepcopy(case.canonical)],
                                                    "pageInfo": {
                                                        "hasNextPage": False,
                                                        "endCursor": None,
                                                    },
                                                }
                                            },
                                        }
                                    }
                                ]
                            },
                        }
                    }
                }
            }
        else:
            endpoint = next((s for s in command if s == "user" or s.startswith("repos/")), None)
            parsed = urlsplit(endpoint)
            query = parse_qs(parsed.query)
            key = endpoint if "ref" in query else parsed.path
            assert key in case.data, key
            value = copy.deepcopy(case.data[key])
            if isinstance(value, dict) and set(value) & {
                "check_runs",
                "workflow_runs",
                "jobs",
                "artifacts",
            }:
                values = next(
                    v
                    for k, v in value.items()
                    if k in {"check_runs", "workflow_runs", "jobs", "artifacts"}
                )
                assert query.get("page", ["1"]) == ["1"]
                value["total_count"] = len(values)
        raw = value if isinstance(value, bytes) else canonical_json(value).encode()
        return b"HTTP/2.0 200 OK\r\nContent-Length: " + str(len(raw)).encode() + b"\r\n\r\n" + raw

    case.transport = transport
    return case


def approve(monkeypatch, case):
    from devflow_temporal import delivery_ci_inventory as owner
    from devflow_temporal import delivery_ci_inventory_contract as contract
    from devflow_temporal import delivery_ci_teardown as teardown

    sha = digest(case.profile)
    monkeypatch.setattr(contract, "APPROVED_PROFILE_SHA256", sha)
    monkeypatch.setattr(owner, "APPROVED_PROFILE_SHA256", sha)
    monkeypatch.setattr(teardown, "APPROVED_PROFILE_SHA256", sha)
    return sha
