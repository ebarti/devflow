"""Real conflicted Git trees; native assessment and GitHub transports are controlled."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from test_delivery_merge_followup import delivered as delivered
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_merge_followup as followup
from devflow_temporal import delivery_merge_resolution as resolution
from devflow_temporal import supervisor
from devflow_temporal.candidate import candidate_for
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_merge import MergeBroker
from devflow_temporal.delivery_merge_reconciliation import reconcile
from devflow_temporal.delivery_resources import read_private, write_private


@pytest.fixture
def conflicted(delivered, monkeypatch, tmp_path):
    d = delivered
    receipt = followup.admit(d.store, "run-1", d.command)
    repo = Path(d.spec["source_path"])
    (repo / "conflict.txt").write_text("original\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "chore: original")
    base = _git(repo, "rev-parse", "HEAD")
    (repo / "conflict.txt").write_text("feature requirement\n")
    _git(repo, "commit", "-qam", "feat: accepted feature")
    previous = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "--detach", base)
    (repo / "conflict.txt").write_text("main requirement\n")
    _git(repo, "commit", "-qam", "feat: independent main")
    target = _git(repo, "rev-parse", "HEAD")
    remote = tmp_path / "remote.git"
    _git(tmp_path, "clone", "--bare", str(repo), str(remote))
    _git(remote, "update-ref", "refs/heads/" + d.spec["branch"], previous)
    _git(remote, "update-ref", "refs/heads/main", target)
    calls, assessments = [], {}
    state = {"decision": False, "escape": False, "lost_push": False, "pushes": 0}
    native_git = resolution._git

    def transport(workspace, *args):
        if args[:3] == ("remote", "set-url", "origin"):
            args = (*args[:3], str(remote))
        result = native_git(workspace, *args)
        if args[0] == "push":
            state["pushes"] += 1
            if state["lost_push"]:
                raise RuntimeError("push accepted; response lost")
        return result

    monkeypatch.setattr(resolution, "_git", transport)
    pr = {"number": 1, "url": d.remote["html_url"], "head": previous}
    proof = {"pull_request": pr}
    monkeypatch.setattr(followup, "authorize", lambda *_: proof)

    def api(_broker, path, **_kwargs):
        if "/git/commits/" in path:
            head = path.rsplit("/", 1)[-1]
            return {"sha": head, "tree": {"sha": _git(remote, "rev-parse", head + "^{tree}")},
                    "parents": [{"sha": p} for p in _git(
                        remote, "show", "-s", "--format=%P", head).split()]}
        if "/git/ref/heads/" in path:
            return {"object": {"sha": target}}
        if "/compare/" in path:
            before, after = path.rsplit("/", 1)[-1].split("...")
            return {"status": "ahead", "merge_base_commit": {
                "sha": _git(remote, "merge-base", before, after)}}
        raise AssertionError(path)

    monkeypatch.setattr(MergeBroker, "api", api)

    class Native:
        async def run(self, request):
            resolution.authenticate(request, d.store)
            key = supervisor.DeliverySupervisor._job_key(request)
            if key in assessments:
                return assessments[key]
            calls.append(request)
            workspace = Path(request["workspace"])
            result = {"status": "pass", "summary": "Both parent requirements preserved",
                      "findings": [], "merge_decision": None, "cleanup": "confirmed",
                      "process_cleanup": "observed-native-confirmed"}
            if state["decision"]:
                result.update(status="blocked", merge_decision={
                    "question": "Which incompatible product requirement should apply?",
                    "feature_requirement": "Feature requires A",
                    "main_requirement": "Main prohibits A",
                    "why_incompatible": "The same operation cannot require and prohibit A",
                    "files": ["conflict.txt"],
                })
            elif request["role"] == "implement":
                (workspace / "conflict.txt").write_text("feature requirement\nmain requirement\n")
                if state["escape"]:
                    (workspace / "unrelated.txt").write_text("out of scope")
            else:
                assert (workspace / "conflict.txt").read_text() == (
                    "feature requirement\nmain requirement\n")
            assessments[key] = result
            return result

    monkeypatch.setattr(supervisor, "get_supervisor", lambda _: Native())
    conflict = {"state": "resolve_conflicts", "original_head": previous,
                "previous_head": previous, "target": target}
    return SimpleNamespace(d=d, pr=pr, receipt=receipt, target=target, previous=previous,
                           calls=calls, state=state, conflict=conflict, remote=remote, base=base)


@pytest.mark.asyncio
async def test_conflict_is_resolved_reviewed_and_published_as_exact_two_parent_tree(conflicted):
    c = conflicted
    result = await resolution.resolve(c.d.store, c.d.spec, c.receipt["grant_sha256"], c.conflict)
    assert result["state"] == "pending"
    head = _git(c.remote, "rev-parse", c.d.spec["branch"])
    assert head == result["head"]
    assert _git(c.remote, "show", "-s", "--format=%P", head).split() == [c.previous, c.target]
    assert _git(c.remote, "show", head + ":conflict.txt") == (
        "feature requirement\nmain requirement")
    assert [request["role"] for request in c.calls] == ["implement", "review"]
    # Reattachment keeps both native attempts and the original push, rather than replaying them.
    again = await resolution.resolve(c.d.store, c.d.spec, c.receipt["grant_sha256"], c.conflict)
    assert again == result and len(c.calls) == 2 and c.state["pushes"] == 1
    for request in c.calls:
        assert read_private(Path(request["merge_context"]["evidence_path"]))["conflict.txt"] == {
            "base": "original\n", "feature": "feature requirement\n", "main": "main requirement\n"}


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["decision", "escape"])
async def test_only_proven_product_divergence_is_a_user_decision_and_nothing_is_pushed(
    conflicted, mode,
):
    c = conflicted
    c.state[mode] = True
    result = await resolution.resolve(c.d.store, c.d.spec, c.receipt["grant_sha256"], c.conflict)
    assert result["state"] == ("decision_required" if mode == "decision" else "blocked")
    assert bool(result.get("decision")) == (mode == "decision")
    assert c.state["pushes"] == 0
    assert _git(c.remote, "rev-parse", c.d.spec["branch"]) == c.previous


@pytest.mark.asyncio
async def test_lost_push_acknowledgement_is_reconciled_without_a_second_push(conflicted):
    c = conflicted
    c.state["lost_push"] = True
    with pytest.raises(RuntimeError, match="response lost"):
        await resolution.resolve(c.d.store, c.d.spec, c.receipt["grant_sha256"], c.conflict)
    head = _git(c.remote, "rev-parse", c.d.spec["branch"])
    raw = {**c.d.remote, "head": {**c.d.remote["head"], "sha": head},
           "base": {**c.d.remote["base"], "sha": c.target}}
    broker = MergeBroker(c.d.store, {**c.d.spec, "base_sha": c.base})
    result = reconcile(broker, c.pr, raw, resolve_conflicts=True)
    assert result["state"] == "ready" and result["pr"]["head"] == head
    assert c.state["pushes"] == 1


def test_arbitrary_merge_context_cannot_launch_a_native_role(delivered):
    d = delivered
    followup.admit(d.store, "run-1", d.command)
    request = {"spec": d.spec, "role": "implement", "iteration": 0,
               "candidate": candidate_for(Path(d.spec["source_path"])),
               "workspace": d.spec["source_path"], "merge_context": {"effect_key": "forged"}}
    with pytest.raises(ValueError, match="no admitted resolution"):
        resolution.authenticate(request, d.store)


def test_merge_process_cleanup_preserves_historical_resource_registry(tmp_path):
    import os
    import sys

    from devflow_temporal.delivery_native_process import NativeProcess

    state = tmp_path / "runs" / "run-merge"
    state.mkdir(parents=True, mode=0o700)
    spec = {"run_id": "run-merge", "state_dir": str(state), "policy_digest": "frozen",
            "policy": {"execution_backend": "native-macos"}}
    process = NativeProcess(spec, state / "attempts" / "merge-role",
                            argv=[sys.executable, "-c", "print('isolated merge process')"],
                            cwd=tmp_path, environment=dict(os.environ), timeout=30,
                            register_resources=False)
    result = process.run()
    assert result["cleanup"] == "observed-native-confirmed"
    assert result["monitoring_complete"] and result["exit_code"] == 0
    assert read_private(process.journal)["result"] == result
    assert not (state / "resources").exists()


def test_merge_role_home_isolated_from_original_implementation_and_other_merges(delivered):
    from devflow_temporal.delivery_sandbox import _native_role_home

    request = {"spec": delivered.spec, "role": "implement", "iteration": 0}
    original = _native_role_home(request)
    first = {**request, "merge_context": {"effect_key": "first-merge"}}
    second = {**request, "merge_context": {"effect_key": "second-merge"}}
    assert len({original, _native_role_home(first), _native_role_home(second)}) == 3
    assert _native_role_home(first) == _native_role_home(first)


@pytest.mark.parametrize("launched", [False, True])
def test_proven_prelaunch_continues_same_grant_without_replaying_the_failed_attempt(
    delivered, launched,
):
    d = delivered
    original = followup.admit(d.store, "run-1", d.command)
    key = "merge-resolution:run-1:failed:implement"
    request = {"spec": d.spec, "role": "implement", "iteration": 0,
               "workspace": d.spec["source_path"],
               "candidate": candidate_for(Path(d.spec["source_path"])),
               "merge_context": {"effect_key": key}}
    job = supervisor.DeliverySupervisor._job_key(request)
    folder = Path(d.spec["state_dir"]) / "attempts" / job
    write_private(folder / "request.json", request)
    write_private(folder / "native-environment.json", {"version": 3})
    if launched:
        write_private(folder / "launch.json", {"proof": "a process may have started"})
    failed = {"status": "blocked", "summary": "configuration collision", "findings": [],
              "cleanup": "confirmed", "finish_reason": "prelaunch", "session_id": None}
    bound = {"job_key": job, "merge_authorization": original["grant_sha256"],
             "merge_context": request["merge_context"], "workspace": request["workspace"],
             "candidate": request["candidate"], "spec_sha256": digest(d.spec)}
    with d.store._connect() as db:
        db.execute("INSERT INTO delivery_effects VALUES (?,?,?,?,'pending',NULL,'now')",
                   (key, "run-1", "merge_resolution", canonical_json(bound)))
        db.execute("INSERT INTO delivery_attempts "
                   "(job_key,run_id,role,iteration,candidate_id,state,cleanup,"
                   "result_json,result_path) "
                   "VALUES (?,'run-1','implement',0,?,'finished','confirmed',?,?)",
                   (job, request["candidate"]["id"], canonical_json(failed),
                    str(folder / "result.json")))
    followup.record(d.store, d.spec, "unknown", {
        "state": "unknown",
        "reason": "RuntimeError: merge resolution native cleanup remains unconfirmed"})
    command = {**d.command, "command_id": "continue-proven-prelaunch"}
    if launched:
        with pytest.raises(ValueError, match="provider absence"):
            followup.admit(d.store, "run-1", command)
        return
    continued = followup.admit(d.store, "run-1", command)
    assert continued["grant_sha256"] == original["grant_sha256"]
    assert followup.admit(d.store, "run-1", command) == continued
    with d.store._connect() as db:
        assert followup.current(db, "run-1")["phase"] == "queued"
        assert db.execute("SELECT state FROM delivery_effects WHERE effect_key=?", (key,)
                          ).fetchone()[0] == "complete"
        assert db.execute("SELECT result_json FROM delivery_attempts WHERE job_key=?", (job,)
                          ).fetchone()[0] == canonical_json(failed)
    assert followup.authorize(d.store, d.spec, original["grant_sha256"])


@pytest.mark.asyncio
async def test_activity_keeps_observing_a_push_with_a_lost_acknowledgement(conflicted, monkeypatch):
    from devflow_temporal.delivery_activities import delivery_resolve_merge_conflicts

    c = conflicted
    c.state["lost_push"] = True
    monkeypatch.setattr(
        "devflow_temporal.delivery_activities._context", lambda _: (c.d.store, None))
    result = await delivery_resolve_merge_conflicts({
        "spec": c.d.spec, "merge_authorization": c.receipt["grant_sha256"], "conflict": c.conflict,
    })
    assert result == {"state": "pending", "reason": "resolved_target_update_readback"}
    with c.d.store._connect() as db:
        saved = followup.current(db, c.d.spec["run_id"])
        assert saved["phase"] == "unknown"
        assert c.d.store.state.claim_for(db, c.d.spec["work_id"])
    assert c.state["pushes"] == 1


@pytest.mark.asyncio
async def test_temporal_merge_runs_resolution_before_rechecking_the_combined_candidate(delivered):
    import asyncio

    from temporal_test_server import local_temporal
    from temporalio import activity
    from temporalio.worker import Worker

    from devflow_temporal.delivery_merge_workflow import MergeWorkflow

    stages = []
    conflict = {"state": "resolve_conflicts", "original_head": "original",
                "previous_head": "original", "target": "current-main"}

    @activity.defn(name="delivery_merge_followup")
    async def step(request):
        stages.append("observe")
        return conflict if len(stages) == 1 else {"state": "confirmed"}

    @activity.defn(name="delivery_resolve_merge_conflicts")
    async def resolve(request):
        stages.append("resolve")
        assert request["conflict"] == conflict and request["merge_authorization"] == "grant"
        return {"state": "pending", "reason": "resolved_target_update_submitted"}

    @activity.defn(name="delivery_merge_followup_failed")
    async def failed(request):
        pytest.fail(request["reason"])

    async with local_temporal() as environment, Worker(
        environment.client, task_queue="merge-resolution-test", workflows=[MergeWorkflow],
        activities=[step, resolve, failed],
    ):
        handle = await environment.client.start_workflow(
            MergeWorkflow.run, args=[delivered.spec, "grant"], id="merge-resolution-workflow",
            task_queue="merge-resolution-test",
        )
        assert await asyncio.wait_for(handle.result(), 40) == {"state": "confirmed"}
    assert stages == ["observe", "resolve", "observe"]
