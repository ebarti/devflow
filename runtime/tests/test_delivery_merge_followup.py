"""Public ordinary merge admission keeps native evidence and publication immutable."""

from __future__ import annotations

import copy
import json
from threading import Event
from types import SimpleNamespace

import httpx
import pytest
from test_delivery_merge import gates
from test_delivery_store import service as service

from devflow_temporal import delivery_merge_followup as followup
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_merge import MergeBroker
from devflow_temporal.delivery_resources import write_private
from devflow_temporal.supervisor import DeliverySupervisor


@pytest.mark.parametrize("rotations", [1, 2])
@pytest.mark.parametrize("drift", [None, "head", "content", "unsealed"])
def test_sealed_runtime_policy_renewal_keeps_original_publication_custody(
    delivered, rotations, drift,
):
    from pathlib import Path

    from devflow_temporal.delivery_gate_retry import gate_namespace

    d = delivered
    spec = copy.deepcopy(d.spec)
    with d.store._connect() as db:
        pr = json.loads(db.execute("SELECT pr_json FROM delivery_runs").fetchone()[0])
    original_publication = copy.deepcopy(pr)
    recovery = None
    for generation in range(1, rotations + 1):
        previous_spec, previous_pr = copy.deepcopy(spec), copy.deepcopy(pr)
        spec["policy"]["native_preparation"] = {"runtime_payload_sha256": str(generation) * 64}
        spec["policy_digest"] = digest(spec["policy"])
        spec.update(gate_retry_generation=generation, gate_retry_stage="published")
        candidate = {**pr["candidate"], "policy_digest": spec["policy_digest"]}
        pr = {**pr, "candidate": candidate}
        recovery = {"kind": "published_gate_retry", "original_spec": d.spec,
                    "execution_spec": copy.deepcopy(spec), "original_recovery": recovery,
                    "command": {"additional_iterations": 0}, "candidate": candidate,
                    "publication": copy.deepcopy(pr),
                    "seal": {"original_spec": previous_spec, "publication": previous_pr}}
        root = Path(spec["state_dir"]) / gate_namespace(generation, "published")
        write_private(root / "admission.json", recovery)
    with d.store._connect() as db:
        db.execute("INSERT INTO delivery_gate_admissions VALUES ('run-1','renew-policy',?)",
                   (canonical_json(recovery),))
        db.execute("UPDATE delivery_runs SET recovery_json=?,candidate_json=?,pr_json=?",
                   tuple(canonical_json(v) for v in (recovery, candidate, pr)))
    if drift == "head":
        pr["head"] = "f" * 40
    elif drift == "content":
        pr["candidate"] = {**pr["candidate"], "content_sha256": "changed"}
    elif drift == "unsealed":
        recovery["seal"]["original_spec"]["policy_digest"] = "f" * 64
    with d.store._connect() as db:
        db.execute("UPDATE delivery_runs SET pr_json=?,recovery_json=?",
                   (canonical_json(pr), canonical_json(recovery)))
    # A second generic gate assessment is not an admitted historical path.
    if drift or rotations > 1:
        with pytest.raises(ValueError):
            followup.preflight(d.store, "run-1")
    else:
        assert followup.preflight(d.store, "run-1")["phase"] == "available"
        with d.store._connect() as db:
            saved = json.loads(db.execute("SELECT observed_json FROM delivery_effects "
                                         "WHERE kind='publish'").fetchone()[0])
        assert saved == original_publication


@pytest.fixture
def delivered(service, monkeypatch):
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
    return SimpleNamespace(
        store=store,
        spec=spec,
        command=command,
        before=snapshot,
        remote=remote,
        closed=closed,
        root=root,
    )


@pytest.mark.parametrize("hold", [False, True])
def test_updated_pr_or_explicit_owner_hold_cannot_advertise_or_admit_merge(delivered, hold):
    d = delivered
    with d.store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
        pr = json.loads(row["pr_json"])
        if hold:
            db.execute("INSERT INTO delivery_issue_observations VALUES (?,?,?,?,NULL)",
                       (d.spec["issue_url"].casefold(), canonical_json(
                           {"state": "OPEN", "labels": ["do not merge yet"]}), "now", "later"))
        else:
            db.execute("INSERT INTO delivery_pr_observations VALUES (?,?,?,?,NULL)",
                       (pr["url"], canonical_json({"state": "OPEN", "head": "new-commit"}),
                        "now", "later"))
        assert followup.action(db, row, d.spec)["available"] is False
    with pytest.raises(ValueError, match="current validated PR commit"):
        followup.preflight(d.store, "run-1")
    with d.store._connect() as db:
        assert db.execute("SELECT count(*) FROM delivery_merge_requests").fetchone()[0] == 0


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


def test_conflict_continuation_retains_original_grant_and_is_idempotent(delivered):
    d = delivered
    original = followup.admit(d.store, "run-1", d.command)
    followup.record(d.store, d.spec, "decision_required", {
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


def test_real_product_decision_cannot_be_restarted_as_a_routine_conflict(delivered):
    d = delivered
    followup.admit(d.store, "run-1", d.command)
    followup.record(d.store, d.spec, "decision_required", {
        "state": "decision_required", "reason": "divergent_product_direction"})
    with pytest.raises(ValueError, match="already owns"):
        followup.admit(d.store, "run-1", {**d.command, "command_id": "retry"})


def test_merge_turns_do_not_replace_original_delivery_gates(delivered):
    d = delivered
    first = followup.admit(d.store, "run-1", d.command)
    with d.store._connect() as db:
        db.execute("INSERT INTO delivery_effects VALUES "
                   "('merge-role','run-1','merge_resolution',?,'complete','{}','now')",
                   (canonical_json({"job_key": "merge-attempt"}),))
        db.execute("INSERT INTO delivery_attempts "
                   "(job_key,run_id,role,iteration,candidate_id,state,cleanup,result_json) "
                   "VALUES ('merge-attempt','run-1','implement',0,'combined','finished',"
                   "'confirmed','{}')")
    assert followup.authorize(d.store, d.spec, first["grant_sha256"])


def test_fresh_custody_observation_does_not_replace_native_source_authority(delivered, monkeypatch):
    d = delivered
    first = followup.admit(d.store, "run-1", d.command)
    checks = json.loads(d.before["checks_json"])
    checks["resource_cleanup"]["observed_at"] = "later"
    with d.store._connect() as db:
        db.execute("UPDATE delivery_runs SET checks_json=?", (canonical_json(checks),))
    monkeypatch.setattr(followup, "observe_finalized_resources",
                        lambda _: {"manifest_sha256": "freshly-confirmed"})
    assert followup.authorize(d.store, d.spec, first["grant_sha256"])
    checks["qa"]["candidate_id"] = "foreign"
    with d.store._connect() as db:
        db.execute("UPDATE delivery_runs SET checks_json=?", (canonical_json(checks),))
    with pytest.raises(ValueError, match="authority, candidate or ownership changed"):
        followup.authorize(d.store, d.spec, first["grant_sha256"])


def test_cleanup_wait_preserves_claim_then_merges_same_candidate(delivered, monkeypatch):
    d = delivered
    remote = git_remote(d, monkeypatch)
    admitted = followup.admit(d.store, "run-1", d.command)
    bound = admitted["grant_sha256"]
    with d.store._connect() as db:
        db.execute("UPDATE delivery_runs SET cleanup='unknown'")
    assert followup.step(d.store, d.spec, bound, Event())["state"] == "pending"
    with d.store._connect() as db:
        assert followup.current(db, "run-1")["phase"] == "waiting_cleanup"
        assert d.store.state.claim_for(db, d.spec["work_id"])
        db.execute("UPDATE delivery_runs SET cleanup='confirmed'")

    def uncertain(_):
        raise ValueError("fresh process observation is incomplete")

    monkeypatch.setattr(followup, "observe_finalized_resources", uncertain)
    assert followup.step(d.store, d.spec, bound, Event())["state"] == "pending"
    assert remote.merge_calls == remote.updates == 0
    monkeypatch.setattr(followup, "observe_finalized_resources", lambda _: {"state": "confirmed"})
    assert followup.step(d.store, d.spec, bound, Event())["state"] == "pending"
    remote.ci_ready = True
    assert followup.step(d.store, d.spec, bound, Event())["state"] == "confirmed"
    assert remote.updates == remote.merge_calls == 1


def test_closed_delivery_maintenance_leaves_the_later_merge_to_its_owner(delivered, monkeypatch):
    from devflow_temporal import delivery_orphans

    d = delivered
    followup.admit(d.store, "run-1", d.command)

    def forbidden(*_):
        raise AssertionError("Historical delivery maintenance entered the later merge")

    monkeypatch.setattr(delivery_orphans, "_unchanged", forbidden)
    delivery_orphans._reconcile(d.store, d.before, d.spec, {})
    with d.store._connect() as db:
        assert dict(db.execute("SELECT * FROM delivery_runs").fetchone()) == d.before


def test_legacy_continuation_requalifies_custody_and_owned_head_without_replaying_work(
    delivered, monkeypatch,
):
    d = delivered
    remote = git_remote(d, monkeypatch)
    admitted = followup.admit(d.store, "run-1", d.command)
    assert followup.step(d.store, d.spec, admitted["grant_sha256"], Event())["state"] == "pending"
    assert followup.step(d.store, d.spec, admitted["grant_sha256"], Event())["state"] == "pending"
    # Model a v1 grant that predated a separate immutable native evidence digest.
    with d.store._connect() as db:
        saved = followup.current(db, "run-1")
        grant = json.loads(saved["grant_json"])
        grant.pop("native_proof_sha256")
        legacy_grant = canonical_json(grant)
        bound = digest(grant)
        response = {k: v for k, v in admitted.items() if k != "native_proof_sha256"}
        response["grant_sha256"] = bound
        db.execute("UPDATE delivery_merge_requests SET grant_json=?", (legacy_grant,))
        db.execute("UPDATE delivery_commands SET response_json=? WHERE command_id=?",
                   (canonical_json(response), d.command["command_id"]))
        checks = json.loads(db.execute("SELECT checks_json FROM delivery_runs").fetchone()[0])
        checks["resource_cleanup"]["observed_at"] = "later"
        db.execute("UPDATE delivery_runs SET checks_json=?", (canonical_json(checks),))
    monkeypatch.setattr(followup, "observe_finalized_resources",
                        lambda _: {"manifest_sha256": "later-observation"})
    with pytest.raises(ValueError, match="native evidence changed"):
        followup.authorize(d.store, d.spec, bound)
    followup.record(d.store, d.spec, "blocked", {
        "state": "blocked",
        "reason": "ValueError: predecessor process/root custody is unconfirmed"})
    remote.ci_ready = True
    command = {**d.command, "command_id": "continue-custody"}
    continued = followup.admit(d.store, "run-1", command)
    assert continued["grant_sha256"] == bound
    assert followup.authorize(d.store, d.spec, bound)
    with d.store._connect() as db:
        assert followup.current(db, "run-1")["grant_json"] == legacy_grant
    assert remote.updates == 1 and remote.merge_calls == 0
    assert followup.step(d.store, d.spec, bound, Event())["state"] == "confirmed"
    assert remote.updates == remote.merge_calls == 1


@pytest.mark.parametrize(
    "drift",
    [
        "revision",
        "head",
        "candidate",
        "permission",
        "closed",
        "native",
        "claim",
        "pending_effect",
        "unfinished",
        "gate",
    ],
)
def test_stale_unowned_or_unverified_work_cannot_acquire_merge_authority(delivered, drift):
    d = delivered
    if drift in {"revision", "head", "candidate", "permission"}:
        key = {
            "revision": "expected_revision",
            "head": "head",
            "candidate": "candidate_id",
            "permission": "answer",
        }[drift]
        d.command[key] = 0 if drift == "revision" else "other"
    elif drift == "closed":
        d.closed["result"]["outcome"] = "blocked"
    elif drift == "native":
        write_private(d.root / "result.json", {"status": "pass", "session_id": "foreign"})
    else:
        with d.store._connect() as db:
            if drift == "claim":
                d.store.state.claim_work(db, d.spec["work_id"], "external:other")
            elif drift == "pending_effect":
                db.execute("UPDATE delivery_effects SET state='pending'")
            elif drift == "unfinished":
                db.execute("UPDATE delivery_attempts SET state='running' WHERE role='verify'")
            else:
                checks = json.loads(d.before["checks_json"])
                checks["qa"]["candidate_id"] = "stale"
                db.execute("UPDATE delivery_runs SET checks_json=?", (canonical_json(checks),))
    with pytest.raises(ValueError):
        followup.admit(d.store, "run-1", d.command)
    with d.store._connect() as db:
        assert followup.current(db, "run-1") is None


def test_second_command_cannot_replace_accepted_authority(delivered):
    d = delivered
    first = followup.admit(d.store, "run-1", d.command)
    with pytest.raises(ValueError):
        followup.admit(d.store, "run-1", {**d.command, "command_id": "different"})
    with pytest.raises(ValueError, match="different inputs"):
        followup.admit(d.store, "run-1", {**d.command, "answer": "merge", "head": "other"})
    assert followup.admit(d.store, "run-1", d.command) == first


def test_active_merge_cannot_be_archived_and_dispatch_error_does_not_replace_completion(delivered):
    from devflow_temporal.delivery_dashboard import mutate

    d = delivered
    followup.admit(d.store, "run-1", d.command)
    with pytest.raises(ValueError, match="merge is still active"):
        mutate(
            d.store,
            "run-1",
            "archive",
            {"command_id": "archive", "expected_revision": 1, "archived": True},
        )
    followup.record(d.store, d.spec, "merged", {"state": "confirmed"})
    followup.record(
        d.store, d.spec, "queued", {"state": "pending"}, "late transport error", only_if="queued"
    )
    with d.store._connect() as db:
        assert followup.current(db, "run-1")["phase"] == "merged"


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


@pytest.mark.parametrize("state", ["pending", "decision_required", "confirmed"])
def test_merge_only_step_projects_result_and_settles_its_claim(delivered, monkeypatch, state):
    import devflow_temporal.delivery_merge as merger

    d = delivered
    admitted = followup.admit(d.store, "run-1", d.command)
    calls = []

    def merge(store, spec, request, _cancelled):
        followup.authorize(store, spec, request["merge_authorization"])
        calls.append("merge")
        return {"state": state, "reason": "test observation"}

    monkeypatch.setattr(merger, "merge_verified", merge)
    monkeypatch.setattr(
        "devflow_temporal.delivery_activities._tracker_sync",
        lambda *_args, **_kw: {"state": "recorded", "pending": False},
    )
    result = followup.step(d.store, d.spec, admitted["grant_sha256"], Event())
    assert result["state"] == state and calls == ["merge"]
    with d.store._connect() as db:
        saved = followup.current(db, "run-1")
        assert saved["phase"] == {"pending": "waiting_ci", "confirmed": "merged"}.get(state, state)
        assert bool(d.store.state.claim_for(db, d.spec["work_id"])) == (state == "pending")
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 3


def test_title_identifies_actual_change_without_altering_original_goal(delivered):
    spec = {
        **delivered.spec,
        "publication_summary": "feat: preview exact cover-letter PDFs",
        "github_repo": "ebarti/JobCtrl",
        "issue_url": "https://github.com/ebarti/JobCtrl/issues/1042",
    }
    before = copy.deepcopy(spec)
    assert delivered.store._display_title(spec) == "JobCtrl #1042 · Preview exact cover-letter PDFs"
    assert spec == before


def git_remote(d, monkeypatch):
    """Real Git trees with a controlled GitHub boundary; never a real PR."""
    from pathlib import Path

    from test_delivery_merge_reconciliation import Remote
    from test_delivery_store import _git

    repo = Path(d.spec["source_path"])
    base = d.spec["base_sha"]
    (repo / "feature.txt").write_text("Reviewed source\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "feat: reviewed fixture")
    feature = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "--detach", base)
    (repo / "main.txt").write_text("New main work\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "chore: advance fixture main")
    target = _git(repo, "rev-parse", "HEAD")
    remote = Remote(repo, base)
    remote.head, remote.target = feature, target
    remote.merged, remote.merge_calls, remote.issue_state, remote.ci_ready = False, 0, "open", False
    remote.squash = None
    remote.lose_merge_ack = False

    def raw():
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


@pytest.mark.parametrize("lost", ["none", "update", "merge"])
def test_merge_entry_reconciles_real_main_and_waits_for_new_ci_without_another_worker(
    delivered,
    monkeypatch,
    lost,
):
    from test_delivery_store import _git

    d = delivered
    remote = git_remote(d, monkeypatch)
    remote.lost = lost == "update"
    remote.lose_merge_ack = lost == "merge"
    admitted = followup.admit(d.store, "run-1", d.command)
    bound = admitted["grant_sha256"]
    assert followup.step(d.store, d.spec, bound, Event())["state"] == "pending"
    assert remote.updates == 1 and remote.merge_calls == 0
    assert followup.step(d.store, d.spec, bound, Event())["state"] == "pending"
    assert remote.updates == 1 and remote.merge_calls == 0
    remote.ci_ready = True
    result = followup.step(d.store, d.spec, bound, Event())
    if lost == "merge":
        assert result["state"] == "pending"
        result = followup.step(d.store, d.spec, bound, Event())
    assert result["state"] == "confirmed", result
    assert result["base"] == remote.target and result["head"] == remote.head
    assert _git(remote.repo, "show", remote.squash + ":feature.txt") == "Reviewed source"
    assert _git(remote.repo, "show", remote.squash + ":main.txt") == "New main work"
    assert followup.step(d.store, d.spec, bound, Event()) == result
    assert remote.merge_calls == 1 and remote.issue_state == "closed"
    with d.store._connect() as db:
        assert d.store.state.claim_for(db, d.spec["work_id"]) is None
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 3
        assert followup.current(db, "run-1")["phase"] == "merged"


@pytest.mark.asyncio
async def test_real_temporal_dispatch_starts_merge_only_workflow_and_recovers_lost_start_ack(
    delivered,
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

    d = delivered
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
    async with local_temporal() as environment:
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
