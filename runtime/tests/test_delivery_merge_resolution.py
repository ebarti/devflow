"""Real conflicted Git trees; native assessment and GitHub transports are controlled."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from merge_activity_test_helper import selected_activity
from test_delivery_merge_followup import delivered as delivered
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_merge_followup as followup
from devflow_temporal import delivery_merge_resolution as resolution
from devflow_temporal import supervisor
from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_merge import MergeBroker


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
        if path.endswith("/pulls/1"):
            return dict(d.remote)
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
            from devflow_temporal.delivery_sandbox import _review_diff_path

            resolution.authenticate(request, d.store)
            _review_diff_path(request)
            key = supervisor.DeliverySupervisor._job_key(request)
            if key in assessments:
                return assessments[key]
            calls.append(request)
            workspace = Path(request["workspace"])
            if request["role"] == "review" and state.get("review_prelaunch"):
                raise RuntimeError("review failed before provider launch")
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
            if request.get('merge_native_preparation'):
                # This entire assessment is an authored external seam. Retain
                # its typed completed row so replay cannot invent another role.
                folder = Path(d.spec['state_dir']) / 'attempts' / key
                with d.store._connect() as db:
                    db.execute('INSERT INTO delivery_attempts '
                               '(job_key,run_id,role,iteration,candidate_id,state,cleanup,'
                               'result_json,result_path,finished_at) VALUES '
                               "(?,'run-1',?,0,?,'finished','confirmed',?,?,'now')",
                               (key, request['role'], request['candidate']['id'],
                                canonical_json(result), str(folder / 'result.json')))
            return result

    monkeypatch.setattr(supervisor, "get_supervisor", lambda _: Native())
    # This fixture controls the entire native assessment boundary. The separate
    # native-preparation module exercises the real preparation/supervisor path.
    monkeypatch.setattr('devflow_temporal.delivery_merge_native_preparation.prepare',
                        lambda _store, original, *_args: (original, None))
    conflict = {"state": "resolve_conflicts", "original_head": previous,
                "previous_head": previous, "target": target}
    return SimpleNamespace(d=d, pr=pr, receipt=receipt, target=target, previous=previous,
                           calls=calls, state=state, conflict=conflict, remote=remote, base=base)


@pytest.mark.asyncio
async def test_activity_keeps_observing_a_push_with_a_lost_acknowledgement(conflicted, monkeypatch):
    from devflow_temporal.delivery_activities import delivery_resolve_merge_conflicts

    c = conflicted
    c.state["lost_push"] = True
    monkeypatch.setattr(
        "devflow_temporal.delivery_activities._context", lambda _: (c.d.store, None))
    with selected_activity(c.d.store, c.d.spec):
        result = await delivery_resolve_merge_conflicts({
            "spec": c.d.spec, "merge_authorization": c.receipt["grant_sha256"],
            "conflict": c.conflict,
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
