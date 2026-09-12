"""Raw captured source crosses the actual CLI; no synthetic_source normalization."""

import json
import os
import shutil
import sqlite3
import subprocess
import sys
from contextlib import closing
from copy import deepcopy
from pathlib import Path

import pytest

from devflow.adapters.github import GitHubRepository
from devflow.application.commands import WorkflowService
from devflow.validation import digest

ROOT = Path(__file__).parents[1]
REPOSITORY = "github:fixture/repo"


def synthetic_capture():
    """Exercise the real observation mapping using only explicitly synthetic readback."""
    issue = {"id": 301, "node_id": "I_synthetic301", "number": 7,
             "html_url": "https://github.com/fixture/repo/issues/7",
             "repository_url": "https://api.github.com/repos/fixture/repo",
             "title": "Synthetic résumé input", "body": "Exact synthetic issue body.\n",
             "updated_at": "2026-09-11T10:00:00Z", "user": {"id": 41, "node_id": "U_synthetic41"}}
    observed = GitHubRepository("fixture", "repo")._backlog_observation(
        issue, {"id": 23, "node_id": "R_synthetic23", "full_name": "fixture/repo"})
    inherited = {"kind": "pull_request", "repository_id": "other-repository",
                 "source_id": "inherited-pr", "creator_id": "external-creator",
                 "revision": "a" * 40, "content_digest": digest("synthetic inherited PR"),
                 "origin": "external"}
    other = {"kind": "comment", "repository_id": "23", "source_id": "comment-9",
             "creator_id": "42", "revision": "2026-09-11T10:02:00Z",
             "content_digest": digest("synthetic consumed comment"), "origin": "unknown"}
    return {"status": "confirmed", "issue": observed, "payload": {"source_lineage": [inherited]}}, [other]


class IntakeConsole:
    def __init__(self, path):
        self.path = path
        self.repository = path / "repository"
        shutil.copytree(ROOT / "fixtures/repositories/prose", self.repository)
        profile = self.repository / ".devflow/repository.toml"
        profile.write_text(profile.read_text().replace("synthetic:prose-fixture", REPOSITORY))
        self.state_dir = path / "private"
        self.env = {**os.environ, "PYTHONPATH": str(ROOT / "src"), "PYTHONDONTWRITEBYTECODE": "1"}
        self.trace = []

    def call(self, command, request, error=None):
        argv = [sys.executable, "-m", "devflow.cli", *command.split(), "--request-file", "-",
                "--repository", str(self.repository), "--state-dir", str(self.state_dir), "--json"]
        result = subprocess.run(argv, input=json.dumps(request), capture_output=True, text=True,
                                check=False, timeout=30, cwd=self.repository, env=self.env)
        self.trace.append({"argv": argv, "request": deepcopy(request), "exit_code": result.returncode,
                           "stdout": result.stdout, "stderr": result.stderr})
        (self.path / "cli-evidence.json").write_text(json.dumps(self.trace, indent=2) + "\n")
        response = json.loads(result.stdout)
        assert result.returncode == (2 if error else 0), response
        if error:
            assert response["error"]["code"] == error, response
            return response["error"]
        assert response["ok"], response
        return response["result"]

    def mapped_source(self):
        capture, other = synthetic_capture()
        before = deepcopy(capture), deepcopy(other)
        recipe = ROOT / "skills/devflow-defining-work/references/source-lineage.md"
        code = recipe.read_text().split("```python\n")[1].split("```")[0]
        script = ("import json,sys\ncapture,other_consumed_observations=json.load(sys.stdin)\n"
                  + code + "\nprint(json.dumps(source))\n")
        run = subprocess.run([sys.executable, "-c", script], input=json.dumps([capture, other]),
                             capture_output=True, text=True, check=False, timeout=30, env=self.env)
        (self.path / "documented-mapping.json").write_text(json.dumps({
            "capture": capture, "other_consumed_observations": other, "executed_code": code,
            "exit_code": run.returncode, "stdout": run.stdout, "stderr": run.stderr}, indent=2) + "\n")
        assert run.returncode == 0, run.stderr
        assert (capture, other) == before
        return json.loads(run.stdout), capture, other

    def stored_rows(self):
        with closing(sqlite3.connect((self.state_dir / "state.sqlite3").as_uri() + "?mode=ro", uri=True)) as db:
            return {table: db.execute(f"SELECT * FROM {table} ORDER BY 1, 2").fetchall()
                    for table in ("works", "records", "operations", "claims")}

    def contract(self, source):
        record = json.loads((ROOT / "docs/design/work-contract.example.json").read_text())
        record["source"] = deepcopy(source)
        return record

    @staticmethod
    def ready_request(record):
        return {"operation_id": "synthetic-ready", "work_id": record["work_id"],
                "expected_revision": 0, "record": record, "user_request": {
                    "reference": "synthetic:actual-test-request", "summary": "Verify the synthetic intake only",
                    "allowed_operations": ["edit", "check", "publish_pr"]}}


@pytest.mark.parametrize("incomplete,expected", [
    ("lineage", ["source.lineage"]),
    ("aggregate", ["source.consumed_digest"]),
    ("issue_digest", ["source.consumed_digest"]),
    ("both", ["source.lineage", "source.consumed_digest"]),
    ("unassembled_capture", ["source.lineage", "source.consumed_digest"]),
])
def test_raw_source_preparation_reports_admission_prerequisites_without_state(tmp_path, incomplete, expected):
    console = IntakeConsole(tmp_path)
    source, capture, _ = console.mapped_source()
    if incomplete in {"lineage", "both", "unassembled_capture"}:
        del source["lineage"]
    if incomplete == "lineage":
        source["consumed_digest"] = digest([])
    elif incomplete in {"aggregate", "both"}:
        del source["consumed_digest"]
    else:
        source["consumed_digest"] = capture["issue"]["consumed_digest"]
    record = console.contract(source)
    original = deepcopy(record)
    # All these raw sources remain valid legacy schema shapes.
    assert console.call("validate record", record)["valid"]
    prepared = console.call("work prepare", {"record": record})
    assert prepared == {"ready": False, "authority_required": True, "missing": expected, "record": original}
    assert record == original and not console.state_dir.exists()
    service = WorkflowService(console.state_dir, repository=REPOSITORY)
    baseline = console.stored_rows()
    assert baseline == {table: [] for table in ("works", "records", "operations", "claims")}
    assert service.execute("work.prepare", {"record": record}) == prepared
    assert console.stored_rows() == baseline
    console.call("work ready", console.ready_request(record), error="admission_source")
    assert console.stored_rows() == baseline


def test_documented_unknown_origin_capture_admits_exact_source_without_normalization(tmp_path):
    console = IntakeConsole(tmp_path)
    source, capture, other = console.mapped_source()
    assert source["lineage"][:-1] == capture["payload"]["source_lineage"] + other
    assert source["lineage"][-1] == {
        "kind": "issue", "repository_id": "23", "source_id": "301", "creator_id": "41",
        "revision": capture["issue"]["revision"], "content_digest": capture["issue"]["consumed_digest"],
        "origin": "unknown"}
    assert source["stable_id"] == capture["issue"]["node_id"]
    assert source["consumed_digest"] == digest(source["lineage"]) != capture["issue"]["consumed_digest"]
    record = console.contract(source)
    prepared = console.call("work prepare", {"record": record})
    assert prepared == {"ready": False, "authority_required": True, "missing": [], "record": record}
    assert not console.state_dir.exists()
    # Complete source alone cannot supply conversational authority.
    request = console.ready_request(record)
    console.call("work ready", {k: v for k, v in request.items() if k != "user_request"},
                 error="user_request_required")
    assert console.stored_rows() == {table: [] for table in ("works", "records", "operations", "claims")}
    admitted = console.call("work ready", request)
    saved = console.call("work show", {"work_id": record["work_id"]})
    assert saved["lifecycle"] == "ready" and saved["attempt"] is None
    assert saved["contract"]["source"] == source
    admission = saved["records"]["intake_admission:" + admitted["admission_id"]]
    assert admission["source"] == source and admission["source_digest"] == digest(source)
    assert admission["user_request"] == request["user_request"]
    baseline = console.stored_rows()
    console.call("work prepare", {"record": record})
    assert console.call("work show", {"work_id": record["work_id"]}) == saved
    service = WorkflowService(console.state_dir, repository=REPOSITORY)
    assert service.execute("work.prepare", {"record": record}) == prepared
    assert console.stored_rows() == baseline
