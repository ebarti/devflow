"""Public metadata ACK with real current native/resource/SQLite authority.

Original provider assessment, historical closure and GitHub are controlled;
the source/config/native/resource/claim owners and public API are real.
"""

from __future__ import annotations

import asyncio
import copy
import json
import shutil
import sys
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
from devflow_temporal import delivery_control
from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_ci_inventory_io import GitHubRead
from devflow_temporal.delivery_client import DeliveryClient
from devflow_temporal.delivery_resources import RunResources, read_private
from devflow_temporal.delivery_store import DeliveryStore


@pytest.fixture
def ci_case(service, monkeypatch):
    service[0].config.raw["repositories"]["fixture"]["readiness"] = {
        "version": 1, "generation": 0, "browser_engines": [], "temporal_checks": []
    }
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
async def test_cli_readback_routes_to_public_archive_without_starting_or_accepting(
    ci_case, monkeypatch, capsys,
):
    """The CLI/client are real; its HTTP transport uses the owned ASGI app."""
    c = ci_case
    async with public(c) as (client, headers):
        preflight = await client.post("/api/runs/run-1/ci-inventory-preflight", json=c.command)
        assert preflight.status_code == 200, preflight.text
        response = await client.post("/api/runs/run-1/ack-ci-inventory", headers=headers, json={
            **c.command, "preflight_key": preflight.json()["preflight_key"],
            "acknowledgement": owner.ACK})
        assert response.status_code == 200, response.text
        acknowledgement = response.json()
    routes = []

    async def read(method, path):
        async with public(c) as (client, _headers):
            response = await client.request(method, path)
            assert response.status_code == 200, response.text
            return response.json()

    def transport(method, path):
        routes.append((method, path))
        return asyncio.run(read(method, path))

    caller = object.__new__(DeliveryClient)
    caller._request = transport

    def readonly(path):
        assert path == c.store.config.path
        return caller

    def forbidden(*_args, **_kwargs):
        pytest.fail("CI inventory readback must not start or reload a service")

    monkeypatch.setattr(delivery_control, "read_only_client", readonly)
    monkeypatch.setattr(delivery_control, "api_client", forbidden)
    monkeypatch.setattr(delivery_control, "service_start", forbidden)
    monkeypatch.setattr(sys, "argv", ["devflow-delivery", "--config", str(c.store.config.path),
                                     "ci-inventory", "--id", "run-1"])
    with c.store._connect() as db:
        before = [dict(row) for row in db.execute("SELECT * FROM delivery_runs")]
    await asyncio.to_thread(delivery_control.main)
    result = json.loads(capsys.readouterr().out)
    assert result == acknowledgement
    assert result["state"] == "acknowledged" and result["delivery_acceptance"] is False
    assert result["ci_assessment"]["delivery_policy"]["state"] == "not_evaluated"
    assert result["ci_assessment"]["pr_eligibility"]["state"] == "not_established"
    assert result["ci_assessment"]["merge_readiness"]["state"] == "unverified"
    assert routes == [("GET", "/api/runs/run-1/ci-inventory")]
    with c.store._connect() as db:
        assert [dict(row) for row in db.execute("SELECT * FROM delivery_runs")] == before
        assert not db.execute("SELECT 1 FROM delivery_ci_inventory_results").fetchone()


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
        assert pre.json()["ci_assessment"]["execution"]["state"] == "observed"
        assert pre.json()["ci_assessment"]["execution"]["read_complete"] is True
        assert pre.json()["ci_assessment"]["execution"]["source"] == owner.METHOD
        assert pre.json()["ci_assessment"]["delivery_policy"]["state"] == "not_evaluated"
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
        assert receipt["ci_assessment"]["pr_eligibility"]["state"] == "not_established"
        assert receipt["ci_assessment"]["merge_readiness"]["state"] == "unverified"
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


@pytest.mark.asyncio
async def test_public_wrong_type_command_id_refuses_before_observation_or_any_write(ci_case):
    c = ci_case
    with c.store._connect() as db:
        before = {
            table: [dict(row) for row in db.execute("SELECT * FROM " + table)]
            for table in ("delivery_runs", "delivery_events", "delivery_commands")
        }
    async with public(c) as (client, headers):
        for command_id in (123, True, 1.5, None, [], {}):
            body = {**c.command, "command_id": command_id}
            preflight = await client.post("/api/runs/run-1/ci-inventory-preflight", json=body)
            assert preflight.status_code == 409, preflight.text
            acknowledgement = await client.post(
                "/api/runs/run-1/ack-ci-inventory",
                json={**body, "preflight_key": "a" * 64, "acknowledgement": owner.ACK},
                headers=headers,
            )
            assert acknowledgement.status_code == 409, acknowledgement.text
            for response in (preflight, acknowledgement):
                assert "exact reviewed purpose and selection" in response.json()["detail"]
    assert not c.remote.calls
    with c.store._connect() as db:
        for table, rows in before.items():
            assert [dict(row) for row in db.execute("SELECT * FROM " + table)] == rows
        assert not db.execute("SELECT 1 FROM delivery_ci_inventories").fetchone()
        assert not db.execute("SELECT 1 FROM delivery_ci_inventory_audit").fetchone()


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


@pytest.mark.parametrize("change", ["foreign", "same_owner", "gate_event", "gate_command"])
def test_worker_binds_the_exact_normal_CI_claim_and_admission_generation(ci_case, change):
    c = ci_case
    pre = owner.preflight(c.store, "run-1", c.command)
    owner.acknowledge(
        c.store,
        "run-1",
        {**c.command, "preflight_key": pre["preflight_key"], "acknowledgement": owner.ACK},
    )
    c.store.continue_repair("run-1", c.gate)
    current = c.store.effective_spec("run-1")
    with c.store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
    owner.validate(c.store, current, recovery)
    with c.store._connect() as db:
        if change in {"foreign", "same_owner"}:
            c.store.state.release_work(db, current["work_id"], "external:devflow:run-1")
            c.store.state.claim_work(
                db,
                current["work_id"],
                "foreign" if change == "foreign" else "external:devflow:run-1",
                c.store.config.dashboard_url,
            )
        elif change == "gate_event":
            db.execute(
                "UPDATE delivery_events SET payload_json='{}' WHERE type='gates_retry_queued'"
            )
        else:
            db.execute(
                "UPDATE delivery_commands SET response_json='{}' WHERE command_id=?",
                (c.gate["command_id"],),
            )
    with pytest.raises(ValueError):
        owner.validate(c.store, current, recovery)
    with c.store._connect() as db:
        assert not db.execute("SELECT 1 FROM delivery_ci_inventory_results").fetchone()


def test_file_only_success_cannot_substitute_for_normal_CI_observation(ci_case):
    c = ci_case
    pre = owner.preflight(c.store, "run-1", c.command)
    response = owner.acknowledge(
        c.store,
        "run-1",
        {**c.command, "preflight_key": pre["preflight_key"], "acknowledgement": owner.ACK},
    )
    forged = {
        "state": "passed",
        "head": c.candidate["head"],
        "checks": {},
        "execution_inventory": {
            "method": "acknowledged_original_dispatch_inventory/v1",
            "receipt": response["receipt"],
        },
    }
    with pytest.raises(ValueError, match="file-only"):
        owner.validate_result(c.store, "run-1", forged)


@pytest.mark.parametrize("change", ["source", "selected_raw", "claim", "profile"])
def test_ack_replay_reobserves_current_authority_and_selected_bytes(ci_case, change):
    c = ci_case
    pre = owner.preflight(c.store, "run-1", c.command)
    ack = {**c.command, "preflight_key": pre["preflight_key"], "acknowledgement": owner.ACK}
    response = owner.acknowledge(c.store, "run-1", ack)
    if change == "source":
        (c.case.broker.checkout / c.case.api).write_text("Changed after ACK\n")
    elif change == "selected_raw":
        endpoint = next(k for k in c.remote.data if k.endswith("/logs"))
        c.remote.data[endpoint] += b"changed"
    elif change == "claim":
        with c.store._connect() as db:
            c.store.state.claim_work(
                db, c.case.spec["work_id"], "foreign", c.store.config.dashboard_url
            )
    else:
        c.profile_path.write_text("{}")
    with pytest.raises((ValueError, OSError)):
        owner.acknowledge(c.store, "run-1", ack)
    # A historical data-only acknowledgement read grants no new execution and
    # does not depend on the ended/mutated current checkout or original input.
    assert owner.info(c.store, "run-1") == response


@pytest.mark.parametrize("when", ["before", "after"])
def test_readonly_preflight_keeps_current_installation_guard_separate_from_history(
    ci_case, monkeypatch, when
):
    from devflow_temporal import delivery_runtime_location as location

    c = ci_case
    readonly = DeliveryStore(c.store.config, read_only=True)
    actual = location.require_not_pending
    lost = []

    def require(config):
        if lost:
            raise ValueError("owned current service guard custody lost")
        return actual(config)

    monkeypatch.setattr(location, "require_not_pending", require)
    if when == "before":
        lost.append(True)
    else:
        observe = owner._observe

        def after(*args, **kw):
            result = observe(*args, **kw)
            lost.append(True)
            return result

        monkeypatch.setattr(owner, "_observe", after)
    with pytest.raises(ValueError, match="current service guard"):
        owner.preflight(readonly, "run-1", c.command)


@pytest.mark.asyncio
async def test_lifecycle_public_preflight_and_ack_agree_on_changed_checkpoint(ci_case):
    c = ci_case
    with c.store._connect() as db:
        before = {table: [dict(row) for row in db.execute("SELECT * FROM " + table)]
                  for table in ("delivery_runs", "delivery_events", "delivery_commands")}
    async with public(c) as (client, headers):
        path = "/api/runs/run-1/"
        good = await client.post(path + "ci-inventory-preflight", json=c.command)
        assert good.status_code == 200, good.text
        assert good.json()["lifecycle_assessment"]["eligible"] is True
        for field, value, code in (
            ("expected_revision", c.command["expected_revision"] + 1, "revision_changed"),
            ("expected_projection_revision", c.command["expected_projection_revision"] + 1,
             "projection_revision_changed"),
            ("expected_candidate_id", "0" * 64, "candidate_changed"),
            ("expected_candidate_head", "0" * 40, "candidate_head_changed"),
            ("expected_content_sha256", "0" * 64, "candidate_content_changed"),
            ("expected_pr_number", c.command["expected_pr_number"] + 1, "publication_changed"),
        ):
            body = {**c.command, field: value}
            pre = await client.post(path + "ci-inventory-preflight", json=body)
            ack = await client.post(path + "ack-ci-inventory", headers=headers, json={
                **body, "preflight_key": good.json()["preflight_key"],
                "acknowledgement": owner.ACK})
            assert pre.status_code == ack.status_code == 409
            for response in (pre, ack):
                detail = response.json()["detail"]
                assert not detail["eligible"]
                assert code in {blocker["code"] for blocker in detail["blockers"]}
    with c.store._connect() as db:
        for table, rows in before.items():
            assert [dict(row) for row in db.execute("SELECT * FROM " + table)] == rows
        assert not db.execute("SELECT 1 FROM delivery_ci_inventories").fetchone()


@pytest.mark.asyncio
async def test_public_preflight_absent_resource_root_is_read_only(ci_case, monkeypatch):
    """Missing cleanup evidence must not be replaced by a new resource directory."""
    from devflow_temporal import delivery_ci_teardown, delivery_gate_retry

    c = ci_case
    root = c.case.registry.root
    shutil.rmtree(root)
    assert not root.exists()
    observations = []
    profile_args = []
    cleanup = delivery_gate_retry._stopped_cleanup

    def no_historical_teardown(store, spec, previous, candidate, publication, profile=None):
        profile_args.append(profile)
        return None

    def inspect_cleanup(spec, *, read_only=False):
        observations.append(read_only)
        return cleanup(spec, read_only=read_only)

    monkeypatch.setattr(delivery_ci_teardown, "snapshot_cleanup", no_historical_teardown)
    monkeypatch.setattr(delivery_gate_retry, "_stopped_cleanup", inspect_cleanup)
    async with public(c) as (client, _headers):
        response = await client.post("/api/runs/run-1/ci-inventory-preflight", json=c.command)
    assert response.status_code == 409, response.text
    assert len(profile_args) == 1 and profile_args[0] is not None
    assert observations == [True]
    assert not root.exists()


@pytest.mark.asyncio
async def test_public_preflight_ci_teardown_shares_tracking_snapshot(ci_case, monkeypatch):
    """Teardown and nested resource reads must use one read-only tracking snapshot."""
    from contextlib import contextmanager

    from devflow_temporal import delivery_store
    from devflow_temporal.delivery_execution_config import observation_scope

    c = ci_case
    tracking = c.store.config.tracking_db
    paths = [Path(str(tracking) + suffix) for suffix in ("", "-wal", "-shm", "-journal")]
    original = delivery_store.tracking_read_connection
    identities = []
    depth = 0
    max_depth = 0

    @contextmanager
    def observed(path):
        nonlocal depth, max_depth
        with original(path) as db:
            scope = observation_scope()
            assert scope is not None
            identity = scope.resources[("tracking", str(path))]
            identities.append(id(identity))
            depth += 1
            max_depth = max(max_depth, depth)
            try:
                yield db
            finally:
                depth -= 1

    monkeypatch.setattr(delivery_store, "tracking_read_connection", observed)
    async with public(c) as (client, _headers):
        before = {path: path.read_bytes() if path.exists() else None for path in paths}
        response = await client.post("/api/runs/run-1/ci-inventory-preflight", json=c.command)
        assert response.status_code == 200, response.text
        assert len(identities) >= 2 and len(set(identities)) == 1
        assert max_depth >= 2
        assert {path: path.read_bytes() if path.exists() else None for path in paths} == before
