"""Deterministic GitHub server fixture behind the actual argv transport."""

import copy
import json
import subprocess

import pytest

from devflow.adapters.github import GitHubRepository
from devflow.errors import WorkflowError

HEAD, TARGET, TREE, MERGED, LATER = (digit * 40 for digit in "12345")
BINDING = "a" * 64


class Server:
    def __init__(self):
        self.calls, self.writes, self.threads = [], [], []
        self.lost, self.race = None, None
        self.resolve_effect = True
        self.ref = TARGET
        self.pr = {
            "state": "open",
            "merged": False,
            "merge_commit_sha": None,
            "head": {"sha": HEAD},
            "base": {"ref": "main", "repo": {"full_name": "fixture/repo"}},
        }
        self.protection = {
            "enforce_admins": {"enabled": True},
            "required_status_checks": {
                "strict": True,
                "contexts": ["devflow/verified", "ci"],
                "checks": [],
            },
        }
        self.statuses = [
            {
                "id": 1,
                "context": "devflow/verified",
                "state": "success",
                "description": "devflow:" + BINDING,
            },
            {"id": 2, "context": "ci", "state": "success", "description": "CI"},
        ]
        self.rules, self.runs = [], []
        self.actual_tree = TREE
        self.merge_count = 0

    def runner(self, argv, **kwargs):
        assert argv[:2] == ["gh", "api"] and "shell" not in kwargs
        assert kwargs["check"] is False and kwargs["capture_output"] is True
        method, endpoint = argv[argv.index("--method") + 1 : argv.index("--method") + 3]
        payload = json.loads(kwargs["input"]) if kwargs.get("input") else None
        self.calls.append((method, endpoint, payload))
        data = self.route(method, endpoint, payload)
        return subprocess.CompletedProcess(argv, 0, json.dumps(copy.deepcopy(data)), "")

    def route(self, method, endpoint, payload):
        if endpoint == "graphql":
            query = payload["query"]
            variables = payload["variables"]
            if "resolveReviewThread" in query:
                self.writes.append(("resolve", payload))
                if self.resolve_effect:
                    self.threads[0]["isResolved"] = True
                self.maybe_lost("resolve")
                return {"data": {"resolveReviewThread": {"thread": self.threads[0]}}}
            if "reviewThreads" in query:
                threads = [
                    thread
                    | {
                        "comments": {
                            "nodes": thread["comments"],
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        }
                    }
                    for thread in self.threads
                ]
                return {
                    "data": {
                        "repository": {
                            "pullRequest": {
                                "reviewThreads": {
                                    "nodes": threads,
                                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                                }
                            }
                        }
                    }
                }
            raise AssertionError((query, variables))
        suffix = endpoint.removeprefix("repos/fixture/repo/").split("?")[0]
        if method == "GET":
            if suffix == "pulls/1":
                return self.pr
            if suffix == "pulls/1/files":
                return [{"filename": "file.py", "patch": "@@ -1,2 +1,2 @@\n context\n-old\n+new"}]
            if suffix == "rules/branches/main":
                return self.rules
            if suffix == "branches/main/protection":
                return self.protection
            if suffix == "git/ref/heads/main":
                return {"object": {"sha": self.ref}}
            if suffix.startswith("compare/"):
                ancestor, descendant = suffix.removeprefix("compare/").split("...")
                return {
                    "status": "identical" if ancestor == descendant else "ahead",
                    "merge_base_commit": {"sha": ancestor},
                }
            if suffix.startswith("git/commits/"):
                sha = suffix.split("/")[-1]
                return {"sha": sha, "tree": {"sha": TREE if sha == HEAD else self.actual_tree}}
            if suffix.endswith("/check-runs"):
                return {"check_runs": self.runs}
            if suffix.endswith("/statuses"):
                return self.statuses
        if method == "POST" and suffix == "pulls/1/comments":
            self.writes.append(("finding", payload))
            self.threads.append(
                {
                    "id": "thread-1",
                    "isResolved": False,
                    "path": payload["path"],
                    "line": payload.get("line"),
                    "originalLine": payload.get("line"),
                    "diffSide": payload.get("side"),
                    "comments": [
                        {
                            "id": "node-1",
                            "databaseId": 100,
                            "body": payload["body"],
                            "path": payload["path"],
                            "line": payload.get("line"),
                            "originalLine": payload.get("line"),
                            "diffSide": payload.get("side"),
                            "commit": {"oid": HEAD},
                            "originalCommit": {"oid": HEAD},
                        }
                    ],
                }
            )
            self.maybe_lost("finding")
            return {"id": 100}
        if method == "POST" and suffix == "pulls/1/comments/100/replies":
            self.writes.append(("reply", payload))
            self.threads[0]["comments"].append(
                {"id": "node-2", "databaseId": 101, "body": payload["body"]}
            )
            self.maybe_lost("reply")
            return {"id": 101}
        if method == "PUT" and suffix == "pulls/1/merge":
            self.writes.append(("merge", payload))
            self.merge_count += 1
            assert payload["sha"] == HEAD
            if self.race == "source":
                self.pr["head"]["sha"] = LATER
                raise WorkflowError(
                    "github_rejected", "Expected SHA rejected", {"http_status": 409}
                )
            if self.race == "target":
                self.ref = LATER
                raise WorkflowError(
                    "github_rejected", "Strict freshness rejected", {"http_status": 405}
                )
            self.pr |= {"merged": True, "state": "closed", "merge_commit_sha": MERGED}
            self.ref = LATER  # Another unrelated commit after the operation's merge.
            self.maybe_lost("merge")
            return {"merged": True, "sha": MERGED}
        raise AssertionError((method, endpoint, payload))

    def maybe_lost(self, operation):
        if self.lost == operation:
            self.lost = None
            raise subprocess.TimeoutExpired(["gh", "api"], 60)


@pytest.fixture
def setup():
    server = Server()
    return server, GitHubRepository("fixture", "repo", server.runner, sleep=lambda _: None)


def publish(repo, line=2):
    return repo.publish_finding(
        1,
        finding_id="finding-1",
        body="Synthetic invariant defect",
        path="file.py",
        expected_head=HEAD,
        line=line,
    )


def deliver(repo):
    return repo.deliver(
        1,
        expected_head=HEAD,
        target_ref="main",
        target_sha=TARGET,
        expected_tree=TREE,
        binding_hash=BINDING,
        protection_snapshot_hash=repo.protection_snapshot("main")["hash"],
    )


@pytest.mark.parametrize("line", [None, 1, 2])
def test_finding_valid_file_and_line_anchors_readback_idempotency(setup, line):
    server, repo = setup
    first = publish(repo, line)
    assert first["thread_id"] == "thread-1"
    assert publish(repo, line) == first
    assert len(server.writes) == 1
    payload = server.writes[0][1]
    assert payload["subject_type"] == ("file" if line is None else "line")
    if line is None:
        assert "line" not in payload and "side" not in payload


def test_invalid_anchor_blocks_before_mutation(setup):
    server, repo = setup
    with pytest.raises(WorkflowError) as error:
        publish(repo, 999)
    assert error.value.code == "blocked_anchor" and not server.writes


def test_finding_lost_response_reconciles_no_duplicate(setup):
    server, repo = setup
    server.lost = "finding"
    assert publish(repo)["status"] == "published"
    assert len(server.writes) == 1
    assert publish(repo)["status"] == "published"
    assert len(server.writes) == 1


@pytest.mark.parametrize("lost", ["reply", "resolve", None])
def test_close_requires_proof_reply_resolve_and_readback(setup, lost):
    server, repo = setup
    publish(repo)
    server.lost = lost
    args = dict(
        thread_id="thread-1",
        finding_id="finding-1",
        proof="Synthetic independent regression passed",
        expected_head=HEAD,
        fix_commit=TARGET,
    )
    result = repo.close_thread(1, **args)
    assert result["status"] == "resolved"
    assert [name for name, _ in server.writes] == ["finding", "reply", "resolve"]
    assert repo.close_thread(1, **args) == result
    assert len(server.writes) == 3


def test_resolve_response_is_not_proof_of_resolved_state(setup):
    server, repo = setup
    publish(repo)
    server.resolve_effect = False
    with pytest.raises(WorkflowError) as error:
        repo.close_thread(
            1,
            thread_id="thread-1",
            finding_id="finding-1",
            proof="Fixture proof",
            expected_head=HEAD,
            fix_commit=TARGET,
        )
    assert error.value.code == "ambiguous_github_action"
    assert not server.threads[0]["isResolved"]


def test_merge_actual_commit_tree_not_later_target_tree_and_lost_response(setup):
    server, repo = setup
    server.lost = "merge"
    result = deliver(repo)
    assert result["status"] == "verified"
    assert result["commit_sha"] == MERGED and result["tree_sha"] == TREE
    assert result["observed_target_sha"] == LATER and result["target_ancestry_verified"] is True
    assert deliver(repo)["commit_sha"] == MERGED
    assert server.merge_count == 1


@pytest.mark.parametrize("race", ["source", "target"])
def test_concurrent_remote_writer_rejected_by_expected_sha_or_strict_enforcement(setup, race):
    server, repo = setup
    server.race = race
    with pytest.raises(WorkflowError) as error:
        deliver(repo)
    assert error.value.code == "github_rejected"
    assert not server.pr["merged"] and server.merge_count == 1


def test_actual_tree_mismatch_records_exposed_unverified_without_reset(setup):
    server, repo = setup
    server.actual_tree = "f" * 40
    result = deliver(repo)
    assert result["status"] == "exposed_unverified"
    assert server.pr["merged"] and len(server.writes) == 1


@pytest.mark.parametrize("weaken", ["strict", "admins", "proof", "binding", "ci", "queue"])
def test_live_protection_and_actual_checks_required_no_profile_boolean(setup, weaken):
    server, repo = setup
    if weaken == "strict":
        server.protection["required_status_checks"]["strict"] = False
    if weaken == "admins":
        server.protection["enforce_admins"]["enabled"] = False
    if weaken == "proof":
        server.protection["required_status_checks"]["contexts"] = ["ci"]
    if weaken == "binding":
        server.statuses[0]["description"] = "devflow:" + "b" * 64
    if weaken == "ci":
        server.statuses[1]["state"] = "pending"
    if weaken == "queue":
        server.rules = [{"type": "merge_queue"}]
    with pytest.raises(WorkflowError):
        deliver(repo)
    assert not server.writes


def test_preflight_target_advance_blocks_without_mutation(setup):
    server, repo = setup
    server.ref = LATER
    with pytest.raises(WorkflowError) as error:
        deliver(repo)
    assert error.value.code == "stale_target" and not server.writes


def test_transport_backoff_three_attempts_403_no_retry_and_no_secret_output():
    calls, delays = [], []

    def runner(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(
            argv, 1, "HTTP/2.0 429 Too Many Requests\r\nRetry-After: 2\r\n\r\n{}", "secret-token"
        )

    repo = GitHubRepository("fixture", "repo", runner, sleep=delays.append)
    with pytest.raises(WorkflowError) as error:
        repo.issue(1)
    assert len(calls) == 3 and delays == [2, 2] and "secret-token" not in str(error.value)
    calls.clear()

    def forbidden(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 1, "", "HTTP 403 secret-token")

    repo.runner = forbidden
    with pytest.raises(WorkflowError) as error:
        repo.issue(1)
    assert error.value.code == "github_forbidden" and len(calls) == 1
    assert "secret-token" not in str(error.value)


def test_all_outer_thread_and_inner_comment_pages_are_read():
    seen = []

    def runner(argv, **kwargs):
        request = json.loads(kwargs["input"])
        variables = request["variables"]
        seen.append(variables)
        if "reviewThreads" in request["query"]:
            second = variables["after"] == "thread-page-2"
            comment = {
                "nodes": [{"id": "comment-last" if second else "comment-1"}],
                "pageInfo": {"hasNextPage": not second, "endCursor": "comment-page-2"},
            }
            connection = {
                "nodes": [{"id": "thread-last" if second else "thread-1", "comments": comment}],
                "pageInfo": {"hasNextPage": not second, "endCursor": "thread-page-2"},
            }
            data = {"repository": {"pullRequest": {"reviewThreads": connection}}}
        else:
            assert variables == {"id": "thread-1", "after": "comment-page-2"}
            data = {
                "node": {
                    "comments": {
                        "nodes": [{"id": "hidden-blocker"}],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            }
        return subprocess.CompletedProcess(argv, 0, json.dumps({"data": data}), "")

    threads = GitHubRepository("fixture", "repo", runner).review_threads(1)
    assert len(threads) == 2
    assert threads[0]["comments"][-1]["id"] == "hidden-blocker"
    assert len(seen) == 3


def test_issue_dependency_and_check_pages_include_later_blockers():
    calls = []

    def runner(argv, **kwargs):
        endpoint = argv[argv.index("--method") + 2]
        calls.append(endpoint)
        first = "&page=1" in endpoint
        items = (
            [{"node_id": f"item-{i}", "state": "closed"} for i in range(100)]
            if first
            else [{"node_id": "last", "state": "open"}]
        )
        data = {"check_runs": items} if "check-runs" in endpoint else items
        return subprocess.CompletedProcess(argv, 0, json.dumps(data), "")

    repo = GitHubRepository("fixture", "repo", runner)
    assert len(repo.dependencies(1)) == 101
    assert len(repo.issues()) == 101
    checks = repo.checks(HEAD)
    assert len(checks["check_runs"]) == len(checks["statuses"]) == 101
    assert len(calls) == 8


def test_unconfirmed_write_never_retried():
    calls = []

    def runner(argv, **kwargs):
        calls.append(argv)
        raise subprocess.TimeoutExpired(argv, 60)

    repo = GitHubRepository("fixture", "repo", runner)
    with pytest.raises(WorkflowError) as error:
        repo._api(
            "repos/fixture/repo/statuses/" + HEAD, method="POST", payload={"state": "pending"}
        )
    assert error.value.code == "ambiguous_github_action" and len(calls) == 1


def test_stack_backend_cannot_be_enabled_by_boolean(setup):
    _, repo = setup
    with pytest.raises(WorkflowError) as error:
        repo.deliver_stack(enrolled=True, verified=True)
    assert error.value.code == "stack_conformance_unavailable"


def test_reconcile_delivery_retains_original_policy_if_protection_later_changes(setup):
    server, repo = setup
    original_hash = repo.protection_snapshot("main")["hash"]
    assert deliver(repo)["status"] == "verified"
    server.protection["enforce_admins"]["enabled"] = False
    result = repo.reconcile_delivery(
        1,
        expected_head=HEAD,
        target_ref="main",
        target_sha=TARGET,
        expected_tree=TREE,
        protection_snapshot_hash=original_hash,
    )
    assert result["protection_snapshot_hash"] == original_hash
    assert server.merge_count == 1


def test_absent_reconciliation_observations_never_mutate(setup):
    server, repo = setup
    assert (
        repo.reconcile_finding(
            1, finding_id="finding-1", body="Unobserved", path="file.py", expected_head=HEAD
        )
        is None
    )
    assert (
        repo.reconcile_delivery(
            1,
            expected_head=HEAD,
            target_ref="main",
            target_sha=TARGET,
            expected_tree=TREE,
            protection_snapshot_hash="a" * 64,
        )
        is None
    )
    assert not server.writes


def test_changed_admitted_protection_snapshot_blocks_merge(setup):
    server, repo = setup
    with pytest.raises(WorkflowError) as error:
        repo.deliver(
            1,
            expected_head=HEAD,
            target_ref="main",
            target_sha=TARGET,
            expected_tree=TREE,
            binding_hash=BINDING,
            protection_snapshot_hash="f" * 64,
        )
    assert error.value.code == "merge_policy_changed"
    assert not server.writes


def test_same_name_wrong_app_or_pending_check_cannot_satisfy_required_check(setup):
    server, repo = setup
    server.protection["required_status_checks"]["checks"] = [{"context": "ci", "app_id": 12}]
    server.runs = [
        {"id": 10, "name": "ci", "status": "completed", "conclusion": "success", "app": {"id": 13}}
    ]
    with pytest.raises(WorkflowError) as error:
        deliver(repo)
    assert error.value.code == "required_checks_pending"
    server.runs = [
        {"id": 10, "name": "ci", "status": "in_progress", "conclusion": None, "app": {"id": 12}}
    ]
    with pytest.raises(WorkflowError) as error:
        deliver(repo)
    assert error.value.code == "required_checks_pending"
    assert not server.writes


def test_read_only_closure_reconcile_needs_reply_and_resolution(setup):
    server, repo = setup
    publish(repo)
    args = dict(
        thread_id="thread-1",
        finding_id="finding-1",
        proof="Synthetic proof",
        expected_head=HEAD,
        fix_commit=TARGET,
    )
    assert repo.reconcile_thread_closure(1, **args) is None
    server.threads[0]["isResolved"] = True
    assert repo.reconcile_thread_closure(1, **args) is None
    assert len(server.writes) == 1
    expected = repo.close_thread(1, **args)
    count = len(server.writes)
    assert repo.reconcile_thread_closure(1, **args) == expected
    assert len(server.writes) == count
    server.pr["head"]["sha"] = LATER
    with pytest.raises(WorkflowError) as error:
        repo.reconcile_thread_closure(1, **args)
    assert error.value.code == "unverified_fix_head"


def test_status_reconcile_uses_latest_exact_binding_and_never_writes(setup):
    server, repo = setup
    assert repo.reconcile_status(HEAD, binding_hash=BINDING, state="success")["id"] == 1
    assert repo.reconcile_status(HEAD, binding_hash="b" * 64, state="success") is None
    server.statuses.insert(
        0,
        {
            "id": 3,
            "context": "devflow/verified",
            "state": "pending",
            "description": "devflow:" + BINDING,
        },
    )
    assert repo.reconcile_status(HEAD, binding_hash=BINDING, state="success") is None
    assert repo.reconcile_status(HEAD, binding_hash=BINDING, state="pending")["id"] == 3
    assert not server.writes
