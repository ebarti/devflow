"""Public merge boundary with owned Git refs, SQLite journals and an executable CLI.

The CLI below simulates the documented single-PR head condition against a local
bare repository. These tests do not qualify a hosted GitHub merge or its CI.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from test_delivery_feature_merge import Remote, previously_submitted, setup_feature
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_merge as merger
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import registry
from devflow_temporal.delivery_feature_workflow import publication


def git_publication(service, monkeypatch, *, single=False):
    store, spec, record, _, command, _ = setup_feature(service, monkeypatch, single=single)
    source = Path(spec["source_path"])
    origin = Path(_git(source, "remote", "get-url", "origin"))
    _git(source, "push", "origin", "HEAD:refs/heads/main")
    members = record["manifest"]["publication"]["members"]
    for member in members:
        (source / "README.md").write_text(member["chunk_id"] + "\n")
        _git(source, "add", "README.md")
        _git(source, "commit", "-qm", member["chunk_id"])
        member["head"] = _git(source, "rev-parse", "HEAD")
        _git(source, "push", "origin", "HEAD:refs/heads/" + member["branch"])
    requested = publication(record, complete=True)
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET pr_json=? WHERE run_id=?",
                   (json.dumps(requested), spec["run_id"]))
    trees = [_git(origin, "rev-parse", member["head"] + "^{tree}") for member in members]
    monkeypatch.setattr(merger, "evidence", lambda *_a, **_kw: trees)

    class GitRemote(Remote):
        drift_after_read = None

        def api(self, path, *, method="GET", body=None):
            if "/git/commits/" in path:
                head = path.rsplit("/", 1)[-1]
                return {"tree": {"sha": _git(origin, "rev-parse", head + "^{tree}")}}
            if "/pulls/" not in path:
                return super().api(path, method=method, body=body)
            number = int(path.rsplit("/", 1)[-1])
            member = next(item for item in members if item["number"] == number)
            head = _git(origin, "rev-parse", "refs/heads/" + member["branch"])
            self.merged = {item["number"] for item in members
                           if _git(origin, "rev-parse", "refs/heads/main") == item["head"]}
            raw = super().api(path, method=method, body=body)
            raw["head"]["sha"] = head
            if raw["merged"]:
                raw["merge_commit_sha"] = head
            # The response describes the qualified snapshot; a concurrent push
            # lands after the final read and before command submission.
            if number == members[-1]["number"] and self.drift_after_read is not None:
                changed = next(item for item in members
                               if item["number"] == self.drift_after_read)
                _git(source, "commit", "--allow-empty", "-qm", "Concurrent owned fixture push")
                _git(source, "push", "--force", "origin",
                     "HEAD:refs/heads/" + changed["branch"])
                self.drift_after_read = None
            return raw

    return store, spec, record, requested, command, GitRemote(record), origin


@pytest.mark.parametrize("reconciling", [False, True], ids=["retained", "current"])
@pytest.mark.parametrize("changed", [20, 21, 22], ids=["bottom", "middle", "selected"])
def test_native_stack_head_push_after_read_cannot_submit_any_member(
    service, monkeypatch, reconciling, changed,
):
    store, spec, record, requested, command, gh, origin = git_publication(service, monkeypatch)
    if reconciling:
        spec = {**spec, "merge_reconciliation_version": 1}
    before = _git(origin, "rev-parse", "refs/heads/main")
    gh.drift_after_read = changed
    monkeypatch.setattr(merger, "evidence",
                        lambda *_a, **_kw: pytest.fail("cannot qualify unsafe submission"))
    from devflow_temporal import delivery_merge_reconciliation

    monkeypatch.setattr(delivery_merge_reconciliation, "feature",
                        lambda *_a: pytest.fail("cannot update target before capability check"))
    with pytest.raises(merger.StackMergeConditionUnavailable) as refused:
        merger.merge(store, spec, requested, command, gh=gh,
                     execute=lambda *_a, **_kw: pytest.fail("unbound stack submitted"))
    assert refused.value.blocker == {
        "code": "stack_member_head_condition_unavailable", "stack_id": 42,
        "pull_requests": [20, 21, 22],
        "required_capability": "atomic_stack_membership_and_member_heads",
        "mutation_started": False,
    }
    member = next(item for item in record["manifest"]["publication"]["members"]
                  if item["number"] == changed)
    assert _git(origin, "rev-parse", "refs/heads/" + member["branch"]) != member["head"]
    assert _git(origin, "rev-parse", "refs/heads/main") == before
    assert not gh.closed and not any(method != "GET" for method, _, _ in gh.calls)
    with registry(spec).connect() as db:
        assert db.execute("SELECT 1 FROM execution_effects").fetchone() is None


def test_nested_readonly_settlement_never_creates_issue_closure(service, monkeypatch):
    from devflow_temporal.delivery_execution_registry import UnresolvedEffect
    from devflow_temporal.delivery_feature_readback import settle_one
    from devflow_temporal.delivery_writer_custody import require_writer
    from devflow_temporal.delivery_writer_custody import shared as custody

    service[0].config.raw["workspace_custody_version"] = 1
    store, spec, record, requested, command, gh = setup_feature(service, monkeypatch)
    key = previously_submitted(store, spec, record, requested, command)
    gh.merged = {20, 21, 22}
    reg = registry(spec)
    with reg.connect() as db:
        entry = dict(db.execute("SELECT * FROM execution_effects WHERE effect_key=?", (key,))
                     .fetchone())
    with custody(spec):
        require_writer(spec)
        with pytest.raises(UnresolvedEffect, match="no original intent"):
            settle_one(store, spec, entry, gh)
        require_writer(spec)  # Scope downgrade ends with the readback.
    with reg.connect() as db:
        assert db.execute("SELECT count(*) FROM execution_effects "
                          "WHERE kind='close_merged_issue'").fetchone()[0] == 0
    assert not gh.closed and all(method == "GET" for method, _, _ in gh.calls)


def conditional_cli(tmp_path, monkeypatch, origin, member):
    executable = tmp_path / "cli" / "gh"
    executable.parent.mkdir()
    journal = tmp_path / "commands.jsonl"
    executable.write_text("#!" + sys.executable + "\n" + "\n".join([
        "import json, subprocess, sys",
        "from pathlib import Path",
        f"origin = {str(origin)!r}",
        f"branch = {'refs/heads/' + member['branch']!r}",
        f"journal = Path({str(journal)!r})",
        "args = sys.argv[1:]",
        "with journal.open('a') as file: file.write(json.dumps(args) + '\\n')",
        "assert args[:3] == ['pr', 'merge', '20']",
        "assert '--squash' in args and '--repo' in args",
        "expected = args[args.index('--match-head-commit') + 1]",
        "command = ['git', '-C', origin, 'rev-parse', branch]",
        "live = subprocess.check_output(command, text=True).strip()",
        "if live != expected:",
        "    print('head changed before conditional merge', file=sys.stderr)",
        "    sys.exit(1)",
        "command = ['git', '-C', origin, 'update-ref', 'refs/heads/main', expected]",
        "subprocess.run(command, check=True)",
        "print('owned fixture merge accepted')",
    ]) + "\n")
    executable.chmod(0o755)
    import os

    monkeypatch.setenv("PATH", str(executable.parent) + os.pathsep + os.environ["PATH"])
    return journal


@pytest.mark.parametrize("mode", ["unchanged", "head_push", "lost_response"])
def test_single_pr_condition_reaches_executable_and_response_loss_is_reconciled(
    service, monkeypatch, tmp_path, mode,
):
    store, spec, record, requested, command, gh, origin = git_publication(
        service, monkeypatch, single=True)
    member = record["manifest"]["publication"]["members"][0]
    journal = conditional_cli(tmp_path, monkeypatch, origin, member)
    before = _git(origin, "rev-parse", "refs/heads/main")
    if mode == "head_push":
        gh.drift_after_read = member["number"]

    def execute(argv, **kwargs):
        with registry(spec).connect() as db:
            pending = json.loads(db.execute(
                "SELECT result_json FROM execution_effects WHERE kind='feature_merge'"
            ).fetchone()[0])
        assert pending == {"submissions": [{"attempt": 1, "state": "started"}]}
        result = subprocess.run(argv, **kwargs)
        if mode == "lost_response":
            raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
        return result

    if mode == "lost_response":
        with pytest.raises(subprocess.TimeoutExpired):
            merger.merge(store, spec, requested, command, gh=gh, execute=execute)
        assert not gh.closed
    elif mode == "head_push":
        # The executable rejected the conditional request after the owned push.
        # The remote outcome stays unknown until reconciled; it is never retried.
        with pytest.raises(OwnershipConflict, match="moved from"):
            merger.merge(store, spec, requested, command, gh=gh, execute=execute)
        assert _git(origin, "rev-parse", "refs/heads/main") == before and not gh.closed
        with registry(spec).connect() as db:
            outcome = json.loads(db.execute(
                "SELECT result_json FROM execution_effects WHERE kind='feature_merge'"
            ).fetchone()[0])
        assert outcome["submissions"][0]["state"] == "unknown"
    else:
        result = merger.merge(store, spec, requested, command, gh=gh, execute=execute)
        assert result["state"] == "confirmed"
    if mode != "head_push":
        assert _git(origin, "rev-parse", "refs/heads/main") == member["head"]
        result = merger.merge(store, spec, requested, command, gh=gh,
                              execute=lambda *_a, **_kw: pytest.fail("duplicate merge"))
        assert result["state"] == "confirmed" and gh.closed == {3, 10}
    calls = [json.loads(line) for line in journal.read_text().splitlines()]
    assert calls == [["pr", "merge", "20", "--repo", "example/fixture", "--squash",
                      "--match-head-commit", member["head"]]]
