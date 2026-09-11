"""Use real transport parsing, independently specified legacy diff coordinates."""
import json
import subprocess

import pytest
from adapters.test_github import HEAD, Server

from devflow.adapters.github import GitHubRepository
from devflow.errors import WorkflowError

REJECTION = {"message": "Invalid request.\n\n'positioning' wasn't supplied. "
             "'position' wasn't supplied. 'in_reply_to' wasn't supplied. "
             "'subject_type' is not a permitted key. 'line' is not a permitted key."}
PATCH = "@@ -1,2 +1,2 @@\n context\n-old\n+new\n@@ -8,2 +8,2 @@\n same\n-gone\n+added"


class CompatibilityServer(Server):
    def __init__(self, *, path="renamed.py", patch=PATCH, error=REJECTION):
        super().__init__()
        self.path, self.patch, self.error = path, patch, error
        self.posts = []
        self.failure = "reject"
        self.diff_race = False

    def runner(self, argv, **kwargs):
        payload = json.loads(kwargs["input"]) if kwargs.get("input") else None
        if argv[argv.index("--method") + 1] == "POST" and any("/pulls/1/comments" in arg for arg in argv):
            self.posts.append(payload)
            if "position" not in payload:
                if self.failure == "timeout":
                    raise subprocess.TimeoutExpired(argv, 60)
                if self.failure == "server":
                    return subprocess.CompletedProcess(argv, 1, 'HTTP/2.0 503 Unavailable\n\n{}', '')
                return subprocess.CompletedProcess(argv, 1,
                    "HTTP/2.0 422 Unprocessable Entity\n\n" + json.dumps(self.error), "private-stderr")
            # This expected coordinate map is independent of adapter parsing.
            anchors = {1: (1, "RIGHT"), 2: (2, "LEFT"), 3: (2, "RIGHT"),
                       5: (8, "RIGHT"), 6: (9, "LEFT"), 7: (9, "RIGHT")}
            line, side = anchors[payload["position"]]
            super().route("POST", "repos/fixture/repo/pulls/1/comments",
                          {**payload, "line": line, "side": side})
            if self.failure == "fallback_lost":
                raise subprocess.TimeoutExpired(argv, 60)
            return subprocess.CompletedProcess(argv, 0, '{}', '')
        return super().runner(argv, **kwargs)

    def route(self, method, endpoint, payload):
        if method == "GET" and "/pulls/1/files" in endpoint:
            return [{"filename": self.path, "previous_filename": "old.py", "status": "renamed",
                     "patch": self.patch + ("\n+race" if self.diff_race and self.posts else "")}]
        return super().route(method, endpoint, payload)


def publish(server, *, line=9, side="RIGHT", path="renamed.py"):
    return GitHubRepository("fixture", "repo", server.runner, sleep=lambda _: None).publish_finding(
        1, finding_id="synthetic-anchor", body="Synthetic defect", expected_head=HEAD,
        path=path, line=line, side=side)


@pytest.mark.parametrize("line,side,position", [(2, "LEFT", 2), (2, "RIGHT", 3),
                                              (9, "LEFT", 6), (9, "RIGHT", 7)])
def test_compatibility_fallback_preserves_renamed_diff_line_and_side(line, side, position):
    server = CompatibilityServer()
    assert publish(server, line=line, side=side)["status"] == "published"
    assert len(server.posts) == 2
    assert server.posts[1]["position"] == position
    assert not {"subject_type", "side", "line"} & server.posts[1].keys()
    assert server.posts[1]["path"] == "renamed.py"
    assert len(server.threads) == 1


def test_deleted_file_left_anchor_is_supported():
    server = CompatibilityServer(path="deleted.py", patch="@@ -1,2 +0,0 @@\n-first\n-second")
    assert publish(server, path="deleted.py", line=2, side="LEFT")["status"] == "published"
    assert server.posts[1]["position"] == 2


@pytest.mark.parametrize("failure", ["timeout", "server"])
def test_uncertain_modern_write_never_selects_compatibility_fallback(failure):
    server = CompatibilityServer()
    server.failure = failure
    with pytest.raises(WorkflowError) as error:
        publish(server)
    assert error.value.code == "ambiguous_github_action"
    assert len(server.posts) == 1


def test_lost_fallback_response_reconciles_one_publication():
    server = CompatibilityServer()
    server.failure = "fallback_lost"
    assert publish(server)["status"] == "published"
    assert publish(server)["status"] == "published"
    assert len(server.posts) == 2 and len(server.threads) == 1


def test_unrelated_validation_rejection_is_sanitized_and_never_retried():
    server = CompatibilityServer(error={"message": "private-token private-body",
        "errors": [{"resource": "private-resource", "field": "line", "code": "invalid",
                    "message": "private-comment", "value": "private-value"}]})
    with pytest.raises(WorkflowError) as error:
        publish(server)
    assert error.value.details == {"http_status": 422,
        "validation_errors": [{"field": "line", "code": "invalid"}], "no_mutation": True}
    assert "private" not in str(error.value.details)
    assert len(server.posts) == 1


def test_changed_diff_blocks_fallback():
    server = CompatibilityServer()
    server.diff_race = True
    with pytest.raises(WorkflowError) as error:
        publish(server)
    assert error.value.code == "stale_head"
    assert len(server.posts) == 1


def test_old_rename_path_and_wrong_deletion_side_are_rejected_before_write():
    server = CompatibilityServer()
    with pytest.raises(WorkflowError, match="no valid"):
        publish(server, path="old.py")
    server = CompatibilityServer(path="deleted.py", patch="@@ -1,2 +0,0 @@\n-first\n-second")
    with pytest.raises(WorkflowError, match="no valid"):
        publish(server, path="deleted.py", line=2, side="RIGHT")
    assert not server.posts


def test_legacy_cannot_represent_left_context_without_changing_anchor():
    server = CompatibilityServer()
    with pytest.raises(WorkflowError, match="LEFT context"):
        publish(server, line=1, side="LEFT")
    assert len(server.posts) == 1


def test_no_newline_marker_counts_towards_following_hunk_position():
    patch = "@@ -1 +1 @@\n-old\n+new\n\\ No newline at end of file\n@@ -8 +8 @@\n-gone\n+added"
    file = {"patch": patch}
    assert GitHubRepository._anchor_position(file, 8, "LEFT") == 5
    assert GitHubRepository._anchor_position(file, 8, "RIGHT") == 6
