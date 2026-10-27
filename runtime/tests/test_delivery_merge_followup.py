"""Public ordinary merge admission keeps native evidence and publication immutable."""

from __future__ import annotations

import copy
import json
from threading import Event
from types import SimpleNamespace

import httpx
import pytest
from merge_activity_test_helper import activity_call
from test_delivery_merge import gates
from test_delivery_store import service as service

from devflow_temporal import delivery_merge_followup as followup
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_merge import MergeBroker
from devflow_temporal.delivery_resources import write_private
from devflow_temporal.supervisor import DeliverySupervisor


@pytest.fixture
def delivered(service, monkeypatch, *, native_spec=None):
    store, request = service
    spec = store.config.admit(request)
    spec.update(
        provider="codex",
        resource_cleanup_version=1,
        terminal_tracker_version=1,
        publication_base_ref="main",
    )
    spec["policy"]["required_ci"] = ["test"]
    spec["policy"]["execution_backend"] = "native-macos"
    if native_spec is not None:
        native_spec(spec)
    spec["policy_digest"] = digest(spec["policy"])
    monkeypatch.setattr(DeliveryConfig, "admit", lambda *_: copy.deepcopy(spec))
    store.submit(request)
    spec = store.effective_spec("run-1")
    _, candidate, pr, checks, attempts, publication = gates()
    candidate.update(base_sha=spec["base_sha"], policy_digest=spec["policy_digest"])
    pr.update(url="https://github.com/example/fixture/pull/1", candidate=candidate)
    # A real supervisor-bound implementation result, not a caller-provided pass.
    native_request = {
        "spec": spec,
        "role": "implement",
        "iteration": 0,
        "candidate": {"id": "input"},
        "workspace": spec["checkout"],
    }
    job = DeliverySupervisor._job_key(native_request)
    from pathlib import Path

    root = Path(spec["state_dir"]) / "attempts" / job
    assessment = {"status": "pass", "session_id": "implement", "findings": []}
    process = {
        "state": "finished",
        "monitoring_complete": True,
        "cleanup": "observed-native-confirmed",
        "journal": str(root / "native-process.json"),
    }
    saved = {
        **assessment,
        "cleanup": "confirmed",
        "process_cleanup": "observed-native-confirmed",
        "resource_cleanup": "pending_workflow_finalization",
        "native_process": process,
    }
    journal = {
        "phase": "finished",
        "owned": {"123": {"identity": "owned"}},
        "intent": {
            "run_id": spec["run_id"],
            "policy_digest": spec["policy_digest"],
            "cwd": spec["checkout"],
        },
        "result": process,
        "provider_session": {
            "role": "implement",
            "iteration": 0,
            "session_id": "implement",
            "output_candidate": {
                "id": "source",
                "head": "source-head",
                "content_sha256": "contents",
            },
            "result_digest": digest(saved),
        },
    }
    for name, value in (
        ("request.json", native_request),
        ("result.json", assessment),
        ("native-process.json", journal),
    ):
        write_private(root / name, value)
    attempts[0].update(
        job_key=job,
        candidate_id="input",
        result_json=canonical_json(saved),
        result_path=str(root / "result.json"),
    )
    with store._connect() as db:
        for index, attempt in enumerate(attempts):
            db.execute(
                "INSERT INTO delivery_attempts "
                "(job_key,run_id,role,iteration,candidate_id,state,cleanup,session_id,"
                "result_json,result_path) VALUES (?,?,?,0,?,'finished','confirmed',?,?,?)",
                (
                    attempt.get("job_key", str(index)),
                    spec["run_id"],
                    attempt["role"],
                    attempt["candidate_id"],
                    attempt["session_id"],
                    attempt["result_json"],
                    attempt.get("result_path"),
                ),
            )
        db.execute(
            "UPDATE delivery_runs SET phase='delivered',execution_state='terminal',"
            "outcome='delivered',cleanup='confirmed',protocol_revision=7,"
            "candidate_json=?,pr_json=?,checks_json=? WHERE run_id='run-1'",
            tuple(canonical_json(value) for value in (candidate, pr, checks)),
        )
        db.execute(
            "INSERT INTO delivery_effects VALUES "
            "('publish:run-1:0','run-1','publish',?,'complete',?,?)",
            (canonical_json(publication), canonical_json(pr), "2026-10-19T10:00:00Z"),
        )
        db.execute("UPDATE delivery_outbox SET state='sent'")
        store.state.release_work(db, spec["work_id"], "external:devflow:run-1")
        row = db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone()
        snapshot = dict(row)
    monkeypatch.setattr(followup, "observe_finalized_resources", lambda _: {"state": "confirmed"})
    closed = {
        "workflow_id": "delivery-run-1",
        "execution_run_id": "original-execution",
        "closed_at": "2026-10-19T10:01:00Z",
        "request_digest": row["request_digest"],
        "recovery_digest": None,
        "result": {
            "run_id": "run-1",
            "revision": 7,
            "outcome": "delivered",
            "cleanup": "confirmed",
            "candidate": candidate,
            "pull_request": pr,
        },
    }
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_, **__: copy.deepcopy(closed))
    remote = {
        "number": 1,
        "html_url": pr["url"],
        "draft": False,
        "head": {
            "sha": pr["head"],
            "ref": spec["branch"],
            "repo": {"full_name": spec["github_repo"]},
        },
        "base": {
            "sha": spec["base_sha"],
            "ref": spec["publication_base_ref"],
            "repo": {"full_name": spec["github_repo"]},
        },
        "merged": False,
        "state": "open",
    }
    monkeypatch.setattr(MergeBroker, "api", lambda *_args, **_kwargs: copy.deepcopy(remote))
    command = {
        "command_id": "merge-1",
        "expected_revision": 7,
        "candidate_id": candidate["id"],
        "head": pr["head"],
        "pr_number": 1,
        "answer": "merge",
    }
    from devflow_temporal import delivery_merge_execution

    def controlled_closure(_store, current_spec, saved):
        selected = store._test_merge_execution
        if saved["workflow_id"] != selected["workflow_id"]:
            raise ValueError("predecessor workflow differs from its controlled SDK execution")
        return {"workflow_id": selected["workflow_id"],
                "execution_run_id": selected["execution_run_id"],
                "namespace": store.config.raw.get("temporal_namespace", "default"),
                "workflow_type": "DevflowMergeWorkflow", "status": "COMPLETED",
                "closed_at": "2026-10-19T10:01:00+00:00",
                "grant_sha256": selected["grant_sha256"],
                "original_spec_sha256": digest(current_spec),
                "result_sha256": digest(json.loads(saved["result_json"] or "null")),
                "history_sha256": "0" * 64, "event_count": 2}

    monkeypatch.setattr(delivery_merge_execution, "predecessor_closure", controlled_closure)
    original_admit = followup.admit

    def admit_with_origin(*args, **kwargs):
        result = original_admit(*args, **kwargs)
        store._test_merge_execution = {
            "workflow_id": result["workflow_id"],
            "execution_run_id": result["workflow_id"] + "-test-run",
            "grant_sha256": result["grant_sha256"],
        }
        return result

    monkeypatch.setattr(followup, "admit", admit_with_origin)
    return SimpleNamespace(
        store=store,
        spec=spec,
        command=command,
        before=snapshot,
        remote=remote,
        closed=closed,
        root=root,
    )


def test_explicit_merge_admission_is_durable_exact_and_does_not_rewrite_delivery(delivered):
    d = delivered
    assert followup.preflight(d.store, "run-1")["head"] == "h"
    first = followup.admit(d.store, "run-1", d.command)
    assert followup.admit(d.store, "run-1", d.command) == first
    assert (
        followup.authorize(d.store, d.spec, first["grant_sha256"])["candidate"]["id"] == "published"
    )
    with d.store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
        assert row == d.before
        assert db.execute("SELECT COUNT(*) FROM delivery_merge_requests").fetchone()[0] == 1
        assert d.store.state.claim_for(db, d.spec["work_id"])["owner"] == "external:devflow:run-1"
    assert d.store.effective_spec("run-1")["authorized_endpoint"] == "published_unmerged"


@pytest.mark.asyncio
async def test_public_reads_remain_available_during_resultless_merge_progress(delivered):
    d = delivered
    admitted = followup.admit(d.store, "run-1", d.command)
    app = create_app(d.store.config.path)
    app.state.delivery.store = d.store
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=d.store.config.dashboard_url,
    ) as client:
        for phase in ("queued", "merging", "resolving_conflicts", "waiting_ci",
                      "waiting_tracker", "waiting_cleanup"):
            if phase != "queued":
                activity_call(followup.record, d.store, d.spec, phase)
            listed = await client.get("/api/runs")
            detail = await client.get("/api/runs/run-1")
            assert listed.status_code == detail.status_code == 200
            for run in (listed.json()["runs"][0], detail.json()["run"]):
                assert run["merge"]["phase"] == phase
                assert run["merge"]["result"] is None
                assert run["merge"]["available"] is False
                assert run["merge"]["resume"] is False
    with d.store._connect() as db:
        saved = followup.current(db, "run-1")
        assert saved["workflow_id"] == admitted["workflow_id"]
        assert saved["command_id"] == d.command["command_id"]
        assert dict(db.execute("SELECT * FROM delivery_runs").fetchone()) == d.before


def test_conflict_continuation_retains_original_grant_and_is_idempotent(delivered):
    d = delivered
    original = followup.admit(d.store, "run-1", d.command)
    activity_call(followup.record, d.store, d.spec, "decision_required", {
        "state": "decision_required", "reason": "conflicting_upstream_changes"})
    with d.store._connect() as db:
        before = followup.current(db, "run-1")
        projected = followup.action(db, d.before, d.spec)
    assert projected["available"] and projected["resume"]
    command = {**d.command, "command_id": "continue-conflicts"}
    continued = followup.admit(d.store, "run-1", command)
    assert continued["grant_sha256"] == original["grant_sha256"]
    assert continued["workflow_id"] != original["workflow_id"]
    assert followup.admit(d.store, "run-1", command) == continued
    assert followup.authorize(d.store, d.spec, original["grant_sha256"])
    with d.store._connect() as db:
        after = followup.current(db, "run-1")
        assert after["grant_json"] == before["grant_json"]
        assert after["command_id"] == d.command["command_id"]
        assert after["phase"] == "queued" and after["dispatched"] == 0
        assert dict(db.execute("SELECT * FROM delivery_runs").fetchone()) == d.before


def test_cleanup_wait_preserves_claim_then_merges_same_candidate(delivered, monkeypatch):
    d = delivered
    remote = git_remote(d, monkeypatch)
    admitted = followup.admit(d.store, "run-1", d.command)
    bound = admitted["grant_sha256"]
    with d.store._connect() as db:
        db.execute("UPDATE delivery_runs SET cleanup='unknown'")
    assert activity_call(followup.step, d.store, d.spec, bound, Event())["state"] == "pending"
    with d.store._connect() as db:
        assert followup.current(db, "run-1")["phase"] == "waiting_cleanup"
        assert d.store.state.claim_for(db, d.spec["work_id"])
        db.execute("UPDATE delivery_runs SET cleanup='confirmed'")

    def uncertain(_):
        raise ValueError("fresh process observation is incomplete")

    monkeypatch.setattr(followup, "observe_finalized_resources", uncertain)
    assert activity_call(followup.step, d.store, d.spec, bound, Event())["state"] == "pending"
    assert remote.merge_calls == remote.updates == 0
    monkeypatch.setattr(followup, "observe_finalized_resources", lambda _: {"state": "confirmed"})
    assert activity_call(followup.step, d.store, d.spec, bound, Event())["state"] == "pending"
    remote.ci_ready = True
    assert activity_call(followup.step, d.store, d.spec, bound, Event())["state"] == "confirmed"
    assert remote.updates == remote.merge_calls == 1


def test_second_command_cannot_replace_accepted_authority(delivered):
    d = delivered
    first = followup.admit(d.store, "run-1", d.command)
    with pytest.raises(ValueError):
        followup.admit(d.store, "run-1", {**d.command, "command_id": "different"})
    with pytest.raises(ValueError, match="different inputs"):
        followup.admit(d.store, "run-1", {**d.command, "answer": "merge", "head": "other"})
    assert followup.admit(d.store, "run-1", d.command) == first


@pytest.mark.asyncio
async def test_public_endpoint_needs_csrf_and_records_only_explicit_merge(delivered):
    d = delivered
    app = create_app(d.store.config.path)
    app.state.delivery.store = d.store
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=d.store.config.dashboard_url
    ) as client:
        url = "/api/runs/run-1/merge"
        assert (await client.post(url, json=d.command)).status_code == 403
        session = (await client.get("/api/session")).json()
        headers = {"X-Devflow-CSRF": session["csrf_token"], "Origin": d.store.config.dashboard_url}
        response = await client.post(url, json=d.command, headers=headers)
        assert response.status_code == 200, response.text
        assert (await client.post(url, json=d.command, headers=headers)).json() == response.json()


def git_remote(d, monkeypatch, *, conflict=False):
    """Real Git trees with a controlled GitHub boundary; never a real PR."""
    from pathlib import Path

    from test_delivery_merge_reconciliation import Remote
    from test_delivery_store import _git

    repo = Path(d.spec["source_path"])
    base = d.spec["base_sha"]
    (repo / ("conflict.txt" if conflict else "feature.txt")).write_text("Reviewed source\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "feat: reviewed fixture")
    feature = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "--detach", base)
    (repo / ("conflict.txt" if conflict else "main.txt")).write_text("New main work\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "chore: advance fixture main")
    target = _git(repo, "rev-parse", "HEAD")
    remote = Remote(repo, base)
    remote.head, remote.target = feature, target
    remote.merged, remote.merge_calls, remote.issue_state, remote.ci_ready = False, 0, "open", False
    remote.squash = None
    remote.lose_merge_ack = False

    def raw():
        if conflict:
            import subprocess

            try:
                _git(repo, 'merge-base', '--is-ancestor', remote.target, remote.head)
            except subprocess.CalledProcessError as exc:
                if exc.returncode != 1:
                    raise
                value = copy.deepcopy(d.remote)
                value['head']['sha'], value['base']['sha'] = remote.head, remote.target
                value.update(mergeable=False, merge_commit_sha=None)
            else:
                value = remote.raw()
        else:
            value = remote.raw()
        value.update(
            html_url=d.remote["html_url"],
            merged=remote.merged,
            state="closed" if remote.merged else "open",
            merged_at="2026-10-19T10:10:00Z" if remote.merged else None,
        )
        value["head"].update(ref=d.spec["branch"], repo={"full_name": d.spec["github_repo"]})
        value["base"].update(repo={"full_name": d.spec["github_repo"]})
        if remote.merged:
            value["merge_commit_sha"] = remote.squash
        return value

    def api(_broker, endpoint, *, method="GET", body=None, raw=False):
        if endpoint.endswith("/pulls/1"):
            return remote.current_raw()
        if endpoint.endswith("/pulls/1/merge"):
            assert method == "PUT" and body == {"sha": remote.head, "merge_method": "squash"}
            remote.squash = _git(
                repo,
                "commit-tree",
                remote.head + "^{tree}",
                "-p",
                remote.target,
                "-m",
                "Squash fixture",
            )
            remote.merged = True
            remote.merge_calls += 1
            if remote.lose_merge_ack:
                remote.lose_merge_ack = False
                raise TimeoutError("accepted merge response lost")
            return {"merged": True, "sha": remote.squash}
        if endpoint.endswith("/issues/3"):
            if method == "PATCH":
                remote.issue_state = "closed"
            return {
                "html_url": d.spec["issue_url"],
                "state": remote.issue_state,
                "state_reason": "completed" if remote.issue_state == "closed" else None,
            }
        return remote.api(endpoint, method=method, body=body)

    def live_ci(broker, pr, found, tree):
        from devflow_temporal.delivery_merge_reconciliation import MergeChecksPending

        assert broker.spec["base_sha"] == remote.target
        assert pr["head"] == remote.head and tree == remote.commit(remote.head)["tree"]["sha"]
        if not remote.ci_ready:
            raise MergeChecksPending("waiting for current-target CI: test")
        return {"head": remote.head, "base": remote.target, "jobs": [{"name": "test"}]}

    remote.current_raw = raw
    monkeypatch.setattr(MergeBroker, "api", api)
    monkeypatch.setattr(MergeBroker, "live_ci", live_ci)
    monkeypatch.setattr(MergeBroker, "require_base_enforcement", lambda _: {"strict": True})
    monkeypatch.setattr(
        "devflow_temporal.delivery_activities._tracker_sync",
        lambda *_args, **_kw: {"state": "recorded", "pending": False},
    )
    with d.store._connect() as db:
        candidate = json.loads(d.before["candidate_json"])
        candidate["head"] = feature
        pr = {**json.loads(d.before["pr_json"]), "candidate": candidate, "head": feature}
        checks = json.loads(d.before["checks_json"])
        checks["ci"]["head"] = feature
        db.execute(
            "UPDATE delivery_runs SET candidate_json=?,pr_json=?,checks_json=?",
            tuple(canonical_json(v) for v in (candidate, pr, checks)),
        )
        db.execute("UPDATE delivery_effects SET observed_json=?", (canonical_json(pr),))
    d.command["head"] = feature
    d.closed["result"].update(candidate=candidate, pull_request=pr)
    return remote


@pytest.mark.asyncio
async def test_real_temporal_dispatch_starts_merge_only_workflow_and_recovers_lost_start_ack(
    service,
    monkeypatch,
):
    import asyncio

    from temporal_test_server import local_temporal
    from temporalio.worker import Worker

    from devflow_temporal.delivery_activities import (
        delivery_merge_followup,
        delivery_merge_followup_failed,
    )
    from devflow_temporal.delivery_merge_workflow import MergeWorkflow
    from devflow_temporal.delivery_store import DeliveryStore

    async with local_temporal() as environment:
        old, submitted = service
        raw = copy.deepcopy(old.config.raw)
        raw["temporal_address"] = environment.client.service_client.config.target_host
        old.config.path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(old.config.path))
        d = delivered.__wrapped__((store, submitted), monkeypatch)
        remote = git_remote(d, monkeypatch)
        # This workflow test starts from an already current target; the separate
        # real-Git test above exercises both asynchronous reconciliation and CI waits.
        remote.target = remote.original
        remote.ci_ready = True
        admitted = followup.admit(d.store, "run-1", d.command)
        monkeypatch.setattr(
            "devflow_temporal.delivery_activities._context", lambda _spec: (d.store, None)
        )
        service = SimpleNamespace(store=d.store, config=d.store.config)
        starts = []

        async def start(*args, **kwargs):
            starts.append(kwargs["id"])
            return await environment.client.start_workflow(*args, **kwargs)

        transport = SimpleNamespace(
            get_workflow_handle=environment.client.get_workflow_handle, start_workflow=start
        )
        await followup.dispatch(service, transport)
        with d.store._connect() as db:
            db.execute("UPDATE delivery_merge_requests SET dispatched=0")
        await followup.dispatch(service, transport)
        assert starts == [admitted["workflow_id"]]
        async with Worker(
            environment.client,
            task_queue=d.store.config.queue,
            workflows=[MergeWorkflow],
            activities=[delivery_merge_followup, delivery_merge_followup_failed],
        ):
            handle = environment.client.get_workflow_handle(admitted["workflow_id"])
            result = await asyncio.wait_for(handle.result(), 30)
            assert result["state"] == "confirmed", result
    assert remote.merge_calls == 1
