import json
import signal
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from devflow.adapters.github import GitHubRepository
from devflow.adapters.sqlite_store import SQLiteStore
from devflow.backlog import capture
from devflow.errors import WorkflowError

REPOSITORY = "github:synthetic/example"
REQUEST = {"work_id": "synthetic-outcome", "title": "Synthetic outcome", "body": "One observable result."}


class Server:
    def __init__(self, store):
        self.store, self.items, self.posts = store, [], 0
        self.post_status, self.read_status, self.lose_response = 201, 200, False

    def issue(self, number=1, body="Existing synthetic issue"):
        return {
            "id": number + 100, "number": number, "node_id": f"synthetic-{number}", "body": body,
            "title": REQUEST["title"], "user": {"id": 10, "node_id": "synthetic-creator"},
            "updated_at": "2026-09-10T00:00:00Z",
            "repository_url": "https://api.github.com/repos/synthetic/example",
            "html_url": f"https://github.com/synthetic/example/issues/{number}",
        }

    def run(self, argv, **kwargs):
        method, endpoint = argv[argv.index("--method") + 1:argv.index("--method") + 3]
        if method == "POST":
            # The actual independent SQLite read must see dispatch committed before gh runs.
            assert capture(self.store, REPOSITORY, {"work_id": REQUEST["work_id"]}, action="list")["captures"][0]["status"] == "dispatched"
            self.posts += 1
            status = self.post_status
            if status == 201:
                self.items.append(self.issue(body=json.loads(kwargs["input"])["body"]))
            if self.lose_response:
                raise subprocess.TimeoutExpired(argv, 60)
            value = self.items[-1] if self.items else {}
        else:
            status = self.read_status
            if endpoint == "user":
                value = {"id": 10, "node_id": "synthetic-creator"}
            elif endpoint == "repos/synthetic/example":
                value = {"id": 20, "node_id": "synthetic-repository", "full_name": "synthetic/example"}
            elif "?" in endpoint:
                value = self.items
            else:
                number = int(endpoint.rsplit("/", 1)[1])
                value = next(item for item in self.items if item["number"] == number)
        return SimpleNamespace(
            returncode=0 if status < 400 else 1,
            stdout=f"HTTP/2.0 {status} Synthetic\n\n" + json.dumps(value), stderr="",
        )

    def factory(self, owner, name):
        assert (owner, name) == ("synthetic", "example")
        return GitHubRepository(owner, name, runner=self.run, sleep=lambda _: None)


@pytest.fixture
def setup(tmp_path):
    store = SQLiteStore(tmp_path / "state")
    server = Server(store)

    def run(request=None, action="capture"):
        return capture(store, REPOSITORY, request or {"work_id": REQUEST["work_id"]}, action=action, github_factory=server.factory)

    return store, server, run


def test_capture_replays_and_resumes_from_saved_request_without_duplicate(setup):
    store, server, run = setup
    first = run(REQUEST)
    assert first["status"] == "confirmed"
    assert first["issue"]["node_id"] == "synthetic-1"
    assert run() == first
    assert server.posts == 1
    assert run(action="list")["captures"] == [first]
    with store.connect() as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM operations").fetchone()[0] == 4
    with pytest.raises(WorkflowError, match="different capture request"):
        run({**REQUEST, "body": "Different acceptance"})


def test_concurrent_capture_has_one_dispatch(setup):
    _, server, run = setup
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _: run(REQUEST), range(4)))
    assert all(result == results[0] for result in results)
    assert server.posts == 1


def test_existing_issue_reused_without_changing_its_body(setup):
    _, server, run = setup
    server.items = [server.issue(7)]
    result = run({"work_id": REQUEST["work_id"], "issue_number": 7})
    assert result["issue"]["number"] == 7
    assert server.items[0]["body"] == "Existing synthetic issue"
    assert server.posts == 0


def test_definite_rejection_requires_explicit_audited_retry(setup):
    store, server, run = setup
    server.post_status = 403
    with pytest.raises(WorkflowError, match="denied"):
        run(REQUEST)
    assert run(action="show")["status"] == "failed"
    with pytest.raises(WorkflowError, match="backlog retry"):
        run()
    server.post_status = 201
    assert run(action="retry")["status"] == "confirmed"
    assert server.posts == 2  # One definitely rejected request, one actual creation.
    with store.connect() as db:
        states = [json.loads(row[0])["status"] for row in db.execute("SELECT result FROM operations ORDER BY operation_id")]
    assert states == ["prepared", "dispatched", "failed", "prepared", "dispatched", "dispatched", "confirmed"]


def test_uncertain_creation_remains_read_only_after_later_denied_read(setup):
    _, server, run = setup
    server.lose_response = True
    with pytest.raises(WorkflowError, match="interrupted"):
        run(REQUEST)
    assert run(action="show")["status"] == "ambiguous"
    server.read_status = 403
    with pytest.raises(WorkflowError, match="denied"):
        run()
    assert run(action="show")["status"] == "ambiguous"
    with pytest.raises(WorkflowError, match="proven unsent"):
        run(action="retry")
    server.read_status = 200
    assert run()["status"] == "confirmed"
    assert server.posts == 1


def test_absent_issue_after_dispatch_is_not_permission_to_repeat(setup):
    _, server, run = setup
    server.lose_response = True
    server.post_status = 500
    with pytest.raises(WorkflowError):
        run(REQUEST)
    with pytest.raises(WorkflowError, match="uncertain"):
        run()
    with pytest.raises(WorkflowError, match="proven unsent"):
        run(action="retry")
    assert server.posts == 1


def test_preflight_failure_keeps_the_request_and_can_retry(setup):
    _, server, run = setup
    server.read_status = 403
    with pytest.raises(WorkflowError):
        run(REQUEST)
    assert run(action="show")["payload"]["body"] == REQUEST["body"]
    assert run(action="show")["no_mutation"] is True
    assert server.posts == 0
    server.read_status = 200
    assert run(action="retry")["status"] == "confirmed"


@pytest.mark.parametrize("change", [
    {"pull_request": {}}, {"repository_url": "https://api.github.com/repos/other/repository"},
    {"node_id": None},
])
def test_issue_readback_cannot_bind_another_resource(setup, change):
    _, server, run = setup
    server.items = [server.issue(7) | change]
    with pytest.raises(WorkflowError, match="bound repository"):
        run({"work_id": REQUEST["work_id"], "issue_number": 7})
    assert server.posts == 0


def test_sigkill_after_remote_write_resumes_one_issue(tmp_path):
    state_dir, remote_file = tmp_path / "state", tmp_path / "synthetic-remote.json"
    script = '''
import json, os, signal, sys
from pathlib import Path
from types import SimpleNamespace
from devflow.adapters.github import GitHubRepository
from devflow.adapters.sqlite_store import SQLiteStore
from devflow.backlog import capture
store = SQLiteStore(Path(sys.argv[1]))
remote = Path(sys.argv[2])
def run(argv, **kwargs):
    method = argv[argv.index("--method") + 1]
    if method == "POST":
        item = {"id": 101, "number": 1, "node_id": "synthetic-1", "body": json.loads(kwargs["input"])["body"],
                "title": "Synthetic outcome", "user": {"id": 10, "node_id": "synthetic-creator"},
                "updated_at": "2026-09-10T00:00:00Z",
                "repository_url": "https://api.github.com/repos/synthetic/example",
                "html_url": "https://github.com/synthetic/example/issues/1"}
        with remote.open("w") as stream:
            json.dump(item, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.kill(os.getpid(), signal.SIGKILL)
    endpoint = argv[argv.index("--method") + 2]
    value = {"id": 10, "node_id": "synthetic-creator"} if endpoint == "user" else (
        {"id": 20, "node_id": "synthetic-repository", "full_name": "synthetic/example"}
        if endpoint == "repos/synthetic/example" else [])
    return SimpleNamespace(returncode=0, stdout=json.dumps(value), stderr="")
capture(store, "github:synthetic/example", json.loads(sys.argv[3]),
        github_factory=lambda o,n: GitHubRepository(o,n,runner=run))
'''
    process = subprocess.run([sys.executable, "-c", script, str(state_dir), str(remote_file), json.dumps(REQUEST)], capture_output=True, text=True, timeout=30)
    assert process.returncode == -signal.SIGKILL, process.stderr
    store = SQLiteStore(state_dir)
    server = Server(store)
    server.items = [json.loads(remote_file.read_text())]
    pending = capture(store, REPOSITORY, {}, action="list")["captures"][0]
    assert pending["status"] == "dispatched"
    # The original request file and owner conversation are unnecessary.
    result = capture(store, REPOSITORY, {"work_id": pending["work_id"]}, github_factory=server.factory)
    assert result["status"] == "confirmed"
    assert server.posts == 0
    with store.connect() as db:
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_copied_marker_before_creation_is_not_our_capture(setup):
    from devflow.backlog import _key

    _, server, run = setup
    marker = _key(REPOSITORY, REQUEST["work_id"]).split(":")[1]
    server.items = [server.issue(body=f"Outsider text\n<!-- devflow-backlog:{marker} -->")]
    server.items[0]["user"] = {"id": 999, "node_id": "synthetic-outsider"}
    with pytest.raises(WorkflowError, match="pending authenticated creation"):
        run(REQUEST)
    assert server.posts == 0
    assert run(action="show")["status"] == "failed"


@pytest.mark.parametrize("change", [
    {"user": {"id": 999, "node_id": "synthetic-outsider"}},
    {"title": "Changed after sending"},
    {"body": "Different consumed content"},
])
def test_lost_create_response_requires_expected_creator_and_content(setup, change):
    _, server, run = setup
    server.lose_response = True
    with pytest.raises(WorkflowError, match="interrupted"):
        run(REQUEST)
    # Preserve marker while changing content so only exact binding can reject it.
    if "body" in change:
        change = {"body": server.items[0]["body"] + change["body"]}
    server.items[0].update(change)
    with pytest.raises(WorkflowError, match="creator or content differs"):
        run()
    assert run(action="show")["status"] == "ambiguous"
    assert server.posts == 1


def test_lost_response_marker_collision_remains_ambiguous_without_duplicate_write(setup):
    _, server, run = setup
    server.lose_response = True
    with pytest.raises(WorkflowError):
        run(REQUEST)
    server.items.append(server.issue(2, body=server.items[0]["body"]))
    with pytest.raises(WorkflowError, match="More than one issue"):
        run()
    assert run(action="show")["status"] == "ambiguous"
    assert server.posts == 1


def test_successful_post_identity_must_match_independent_exact_readback(setup):
    _, server, run = setup
    original = server.run

    def corrupt_readback(argv, **kwargs):
        result = original(argv, **kwargs)
        if argv[argv.index("--method") + 2].endswith("/issues/1"):
            issue = json.loads(result.stdout.split("\n\n", 1)[1])
            issue["id"] = 876
            result.stdout = "HTTP/2.0 200 Synthetic\n\n" + json.dumps(issue)
        return result

    server.run = corrupt_readback
    with pytest.raises(WorkflowError, match="POST identity differs"):
        run(REQUEST)
    assert server.posts == 1
    assert run(action="show")["status"] == "ambiguous"


def test_owner_created_projection_preserves_lineage_but_grants_no_trust(setup):
    _, server, run = setup
    lineage = [{"source_id": "synthetic-external-report", "origin": "external",
                "content_digest": "e" * 64, "revision": "external-pr-head"}]
    result = run({**REQUEST, "body": "Résumé derived from external report", "source_lineage": lineage})
    assert result["payload"]["source_lineage"] == lineage
    assert result["issue"]["origin"] == "unknown"
    assert result["issue"]["capture_mode"] == "created_readback"
    assert result["issue"]["repository_id"] == 20
    assert result["issue"]["creator_id"] == 10
    assert len(result["issue"]["consumed_digest"]) == 64
    server.items[0]["body"] = "Substantive later change not consumed by replay"
    assert run() == result
    assert server.posts == 1


def test_existing_outsider_issue_is_unknown_even_with_ready_labels(setup):
    _, server, run = setup
    server.items = [server.issue(7)]
    server.items[0].update(user={"id": 999, "node_id": "synthetic-outsider"},
                           labels=[{"name": "ready"}, {"name": "human-approved"}])
    result = run({"work_id": REQUEST["work_id"], "issue_number": 7})
    assert result["issue"]["origin"] == "unknown"
    assert result["issue"]["capture_mode"] == "reused_unknown"
    assert result["issue"]["creator_id"] == 999
    assert server.posts == 0


def test_legacy_confirmed_capture_replays_without_inventing_provenance(setup):
    from devflow.backlog import _append, _key
    from devflow.validation import digest

    store, _, run = setup
    payload = {"title": REQUEST["title"], "body": REQUEST["body"], "issue_number": None}
    legacy = _append(store, _key(REPOSITORY, REQUEST["work_id"]), {
        "kind": "backlog_capture", "schema_version": 1, "repository": REPOSITORY,
        "work_id": REQUEST["work_id"], "payload": payload, "payload_hash": digest(payload),
        "status": "confirmed", "capture_id": "legacy", "issue": {"number": 7},
    })
    assert run(REQUEST) == legacy
    assert "origin" not in legacy["issue"]


def test_existing_issue_with_null_body_remains_unknown_read_only_capture(setup):
    from devflow.validation import digest

    _, server, run = setup
    server.items = [server.issue(7, body=None)]
    result = run({"work_id": REQUEST["work_id"], "issue_number": 7})
    assert result["issue"]["capture_mode"] == "reused_unknown"
    assert result["issue"]["origin"] == "unknown"
    assert result["issue"]["consumed_digest"] == digest({"title": REQUEST["title"], "body": None})
    assert server.items[0]["body"] is None
    assert server.posts == 0


@pytest.mark.parametrize("body", [0, False, [], {}])
def test_existing_issue_rejects_nonstring_nonnull_body(setup, body):
    _, server, run = setup
    server.items = [server.issue(7, body=body)]
    with pytest.raises(WorkflowError) as caught:
        run({"work_id": REQUEST["work_id"], "issue_number": 7})
    assert caught.value.code == "backlog_identity"
    assert server.posts == 0


def test_created_issue_cleared_body_mismatches_exact_expected_content(setup):
    _, server, run = setup
    original = server.run

    def cleared_body(argv, **kwargs):
        if argv[argv.index("--method") + 2].endswith("/issues/1"):
            server.items[0]["body"] = None
        return original(argv, **kwargs)

    server.run = cleared_body
    with pytest.raises(WorkflowError) as caught:
        run(REQUEST)
    assert caught.value.code == "backlog_creation_mismatch"
    assert run(action="show")["status"] == "ambiguous"
    with pytest.raises(WorkflowError) as recovered:
        run()
    assert recovered.value.code == "backlog_creation_mismatch"
    assert server.posts == 1
