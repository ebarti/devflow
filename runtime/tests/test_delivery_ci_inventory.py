"""Public metadata ACK with real current native/resource/SQLite authority.

Original provider assessment, historical closure and GitHub are controlled;
the source/config/native/resource/claim owners and public API are real.
"""

from __future__ import annotations

import copy
import json
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from ci_inventory_fixture import NAMES, approve, fixture
from test_delivery_blocked_scope import rewrite_assessment, rewrite_observation, scope_case
from test_delivery_browser_mode_recovery import restore_native_observers
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_ci_inventory as owner
from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_ci_inventory_io import GitHubRead
from devflow_temporal.delivery_resources import RunResources, read_private
from devflow_temporal.delivery_store import DeliveryStore


@pytest.fixture
def ci_case(service, monkeypatch):
    case = scope_case(service, monkeypatch, required_ci=NAMES)
    raw = {**case.raw, "status": "pass", "summary": "Owned completed change.", "findings": []}
    rewrite_assessment(case, raw)
    observation = read_private(case.folder / "native-thread-observation.json")
    assessment = {key: raw[key] for key in ("status", "summary", "findings")}
    observation["turns"][-1]["items"][-1]["text"] = json.dumps(assessment)
    observation["turns"][-1]["turn"]["items"][-1]["text"] = json.dumps(assessment)
    rewrite_observation(case, observation)
    _git(case.broker.checkout, "add", case.api)
    _git(case.broker.checkout, "commit", "-qm", "Publish owned CI subject")
    _git(case.broker.checkout, "push", "origin", "HEAD:refs/heads/" + case.spec["branch"])
    candidate = case.broker.candidate()
    case.publication.update(head=candidate["head"], candidate=candidate)
    monkeypatch.setattr(
        DeliveryBroker,
        "_existing_pr",
        lambda *_a, **_k: {
            "number": 12,
            "headRefOid": candidate["head"],
            "state": "OPEN",
            "isDraft": False,
            "url": case.publication["url"],
        },
    )
    restore_native_observers(monkeypatch)
    local = case.broker.run_checks(3, candidate)
    prepublish = case.broker.run_prechecks(3, candidate)
    assert local["state"] == prepublish["state"] == "passed"
    final = RunResources(case.spec).finalize("blocked")
    case.registry = RunResources(case.spec)
    assert final["state"] == "confirmed"
    roles = case.state["roles"]
    roles[0]["status"] = "pass"
    roles[0]["candidate"] = candidate
    roles[0]["findings"] = []
    roles.extend(
        {
            "role": role,
            "iteration": 3,
            "status": "pass",
            "findings": [],
            "session_id": "owned-original-" + role,
            "candidate": candidate,
            "cleanup": "confirmed",
            "finish_reason": "done",
        }
        for role in ("review", "verify")
    )
    error = "required CI did not confirm this PR head"
    checks = {
        "prepublish": prepublish,
        "local": local,
        "review": {"state": "passed", "candidate_id": candidate["id"]},
        "qa": {"state": "passed", "candidate_id": candidate["id"]},
        "ci": {"state": "pending", "reason": "CI deadline reached", "head": candidate["head"]},
        "resource_cleanup": final,
    }
    case.state.update(
        candidate=candidate, pull_request=case.publication, roles=roles, checks=checks, error=error
    )
    case.store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        outcome="blocked",
        event_type="blocked",
        message=error,
        error=error,
        checks=checks,
        candidate=candidate,
        pull_request=case.publication,
        iteration=3,
        protocol_revision=40,
        cleanup="confirmed",
    )
    closed = {
        "workflow_id": "delivery-run-1",
        "execution_run_id": "owned-closed-CI",
        "closed_at": "2026-10-26T00:00:00+00:00",
        "request_digest": case.spec["request_digest"],
        "recovery_digest": None,
        "result": case.state,
    }
    monkeypatch.setattr(DeliveryStore, "_completed_temporal_result", lambda *_a, **_k: closed)
    monkeypatch.setattr(case.store, "_completed_temporal_result", lambda *_a, **_k: closed)
    remote = fixture(
        head=candidate["head"],
        tree=_git(case.broker.checkout, "rev-parse", "HEAD^{tree}"),
        branch=case.spec["branch"],
        policy_digest=case.spec["policy_digest"],
        candidate_id=candidate["id"],
        content_sha256=candidate["content_sha256"],
        pr_number=12,
    )
    remote.profile["subject"]["intake_policy_digest"] = case.store.intake_execution_spec("run-1")[
        "policy_digest"
    ]
    profile_sha = approve(monkeypatch, remote)
    path = case.store.config.state_root / "owned-ci-profile.json"
    path.write_text(canonical_json(remote.profile))
    path.chmod(0o600)
    original_reader = GitHubRead
    monkeypatch.setattr(
        owner, "GitHubRead", lambda **kwargs: original_reader(**kwargs, runner=remote.transport)
    )
    with case.store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs").fetchone())
    command = {
        "command_id": "one-ordinary-ci-inventory",
        "expected_revision": 40,
        "expected_projection_revision": row["revision"],
        "expected_iteration": 3,
        "expected_candidate_id": candidate["id"],
        "expected_candidate_head": candidate["head"],
        "expected_content_sha256": candidate["content_sha256"],
        "expected_pr_number": 12,
        "profile_path": str(path),
        "expected_profile_sha256": profile_sha,
    }
    gate = {
        "continuation_kind": "published_ci_retry",
        "command_id": "one-normal-CI-retry",
        "expected_revision": 40,
        "expected_iteration": 3,
        "expected_candidate_id": candidate["id"],
        "expected_pr_number": 12,
        "expected_pr_head": candidate["head"],
        "additional_iterations": 0,
    }
    return SimpleNamespace(
        case=case,
        store=case.store,
        remote=remote,
        command=command,
        gate=gate,
        closed=closed,
        candidate=candidate,
        profile_path=path,
    )


@asynccontextmanager
async def public(case):
    app = create_app(case.store.config.path)
    host = case.store.config.dashboard_url
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 10001)), base_url=host
    )
    async with client:
        token = (await client.get("/api/session")).json()["csrf_token"]
        yield client, {"Origin": host, "X-Devflow-CSRF": token}


@pytest.mark.asyncio
async def test_public_ack_is_metadata_only_and_cannot_mint_normal_or_merge_success(ci_case):
    c = ci_case
    before = copy.deepcopy(c.closed)
    files = {
        p: p.read_bytes()
        for p in (
            c.case.registry.manifest,
            c.case.registry.state / "resources/finalization.json",
            c.case.folder / "result.json",
        )
    }
    with c.store._connect() as db:
        rows = [dict(r) for r in db.execute("SELECT * FROM delivery_runs")]
        events = [dict(r) for r in db.execute("SELECT * FROM delivery_events")]
    async with public(c) as (client, headers):
        path = "/api/runs/run-1/"
        pre = await client.post(path + "ci-inventory-preflight", json=c.command)
        assert pre.status_code == 200, pre.text
        assert pre.json()["delivery_acceptance"] is False
        assert not (await client.get(path + "ci-inventory")).status_code == 200
        ack = {
            **c.command,
            "preflight_key": pre.json()["preflight_key"],
            "acknowledgement": owner.ACK,
        }
        assert (await client.post(path + "ack-ci-inventory", json=ack)).status_code == 403
        response = await client.post(path + "ack-ci-inventory", json=ack, headers=headers)
        assert response.status_code == 200, response.text
        receipt = response.json()
        assert receipt["PR_ruleset_eligibility"] is False
        assert receipt["merge_context_verified"] is False
        assert receipt["additional_iterations"] == 0
        replay = await client.post(path + "ack-ci-inventory", json=ack, headers=headers)
        assert replay.status_code == 200 and replay.json()["replayed"] is True, replay.text
        foreign = await client.post(
            path + "ack-ci-inventory", json={**ack, "command_id": "another"}, headers=headers
        )
        assert foreign.status_code == 409, foreign.text
        assert (await client.get(path + "ci-inventory")).json() == receipt
    with c.store._connect() as db:
        assert [dict(r) for r in db.execute("SELECT * FROM delivery_runs")] == rows
        assert [dict(r) for r in db.execute("SELECT * FROM delivery_events")] == events
        assert db.execute("SELECT COUNT(*) FROM delivery_ci_inventories").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM delivery_ci_inventory_audit").fetchone()[0] == 1
        assert not db.execute("SELECT 1 FROM delivery_repair_grants").fetchone()
        assert c.store.state.claim_for(db, c.case.spec["work_id"]) is None
    assert c.closed == before
    assert all(p.read_bytes() == value for p, value in files.items())
    assert all(x["limit"] < 4 * 1024 * 1024 for x in owner.indexed_evidence(c.store, "run-1"))


@pytest.mark.parametrize(
    "drift", ["profile", "source", "claim", "role", "unknown", "candidate", "request"]
)
def test_preflight_preserves_all_existing_closed_and_current_native_guards(ci_case, drift):
    c = ci_case
    if drift == "profile":
        c.profile_path.write_text("{}")
    elif drift == "source":
        (c.case.broker.checkout / c.case.api).write_text("Changed current source\n")
    elif drift == "claim":
        with c.store._connect() as db:
            c.store.state.claim_work(db, c.case.spec["work_id"], "foreign", "https://example.test")
    elif drift == "role":
        c.closed["result"]["roles"][-1]["status"] = "blocked"
    elif drift == "unknown":
        c.closed["result"]["cleanup"] = "unknown"
    elif drift == "candidate":
        c.command["expected_candidate_id"] = "8" * 64
    else:
        c.command["format"] = "pytest"
    with pytest.raises((ValueError, OSError)):
        owner.preflight(c.store, "run-1", c.command)
    with c.store._connect() as db:
        assert not db.execute("SELECT 1 FROM delivery_ci_inventories").fetchone()


def test_mutable_authority_is_rechecked_after_remote_observation(ci_case, monkeypatch):
    c = ci_case
    observe = owner._observe

    def changed(*a, **k):
        value = observe(*a, **k)
        (c.case.broker.checkout / c.case.api).write_text("Changed after remote reads\n")
        return value

    monkeypatch.setattr(owner, "_observe", changed)
    with pytest.raises(ValueError):
        owner.preflight(c.store, "run-1", c.command)


def test_missing_or_tampered_raw_archived_proof_refuses_replay(ci_case):
    c = ci_case
    pre = owner.preflight(c.store, "run-1", c.command)
    ack = {**c.command, "preflight_key": pre["preflight_key"], "acknowledgement": owner.ACK}
    owner.acknowledge(c.store, "run-1", ack)
    _, receipt, _ = owner.archive(c.store, "run-1")
    original = Path(
        next(
            x["body"]["path"]
            for x in receipt["observation"]["raw_observations"]
            if "/logs" in x["request"].get("endpoint", "")
        )
    )
    raw = original.read_bytes()
    original.write_bytes(raw + b"changed")
    with pytest.raises(ValueError):
        owner.acknowledge(c.store, "run-1", ack)


def test_published_ci_retry_before_ack_retains_the_original_rollup_reader(ci_case):
    c = ci_case
    assert owner.result_for(c.case.broker, c.case.publication) is None
    assert c.closed["result"]["checks"]["ci"]["state"] == "pending"
    assert not c.remote.calls
