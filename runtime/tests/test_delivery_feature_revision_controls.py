"""Behavioral public controls over the real journal/registry and sealed remote bindings."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from mcp.shared.memory import create_connected_server_and_client_session
from test_delivery_feature_merge import setup_feature
from test_delivery_store import service as legacy_service

from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_client import DeliveryClient
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import registry
from devflow_temporal.delivery_feature_revisions import revise_feature_plan
from devflow_temporal.delivery_github_contract import GitHubDelivery
from devflow_temporal.delivery_mcp import build_server

service = legacy_service


def stopped_feature(service, monkeypatch):
    store, spec, record, _requested, _merge_command, gh = setup_feature(service, monkeypatch)
    shared = registry(spec)
    token = spec["feature_delivery"]["owner"]
    shared.repair(token, "historical-1", "first product repair")
    shared.repair(token, "historical-2", "second product repair")
    shared.repair(token, "historical-3", "third product repair")
    shared.repair(token, "historical-4", "fourth product repair")
    store.mark_start(spec["run_id"], accepted=True)
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET phase='blocked',outcome='blocked',"
            "execution_state='blocked',cleanup='confirmed' WHERE run_id=?",
            (spec["run_id"],),
        )
        store.state.release_work(db, spec["work_id"], "external:devflow:" + spec["run_id"])
        original = dict(
            db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (spec["run_id"],)).fetchone()
        )
    shared.stop(token, "closed-test-feature", {"cleanup": "confirmed"})
    monkeypatch.setattr(
        GitHubDelivery, "api", lambda _self, *args, **kwargs: gh.api(*args, **kwargs)
    )
    monkeypatch.setattr(
        type(store),
        "_completed_temporal_result",
        lambda *_args, **_kwargs: {
            "workflow_id": "delivery-" + spec["run_id"],
            "result": {"cleanup": "confirmed"},
        },
    )
    evidence = Path(spec["state_dir"]) / "planning-defect.json"
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text(
        json.dumps({"chunk": "model", "diagnostic": "Future selector is not in this chunk."})
    )
    command = {
        "command_id": "plan-control-1",
        "expected_revision": original["revision"],
        "reason": "The API chunk inherited a later chunk browser selector.",
        "evidence": [
            {"path": str(evidence), "sha256": hashlib.sha256(evidence.read_bytes()).hexdigest()}
        ],
    }
    return store, spec, record, gh, original, command


def assert_revision_receipt(store, spec, original, result):
    assert result["predecessor_run_id"] == spec["run_id"]
    assert result["revision_phase"] == "requested"
    assert result["expected_plan_revision"] == 1
    assert result["repair_budget"]["used"] == 4
    assert result["affected_chunks"] == []
    successor = store.effective_spec(result["run_id"])
    assert successor["issue_url"] == spec["issue_url"]
    assert successor["authorized_endpoint"] == "published_unmerged"
    assert successor["accepted_plan"] == spec["accepted_plan"]
    assert successor["policy"] == spec["policy"]
    assert successor["feature_delivery"]["owner"]["generation"] == 2
    assert successor["feature_plan_revision_request"]["old_identity"]["comment_id"] == 11
    with store._connect() as db:
        old = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (spec["run_id"],)).fetchone()
        assert old["request_json"] == original["request_json"]
        assert old["pr_json"] == original["pr_json"]
        assert old["iteration"] == original["iteration"]
        runs = [row[0] for row in db.execute("SELECT run_id FROM delivery_runs")]
    assert set(runs) == {spec["run_id"], result["run_id"]}
    assert registry(successor).checkpoints("I_feature")["github-record"]["comment_id"] == 11
    return successor


@pytest.mark.asyncio
async def test_authenticated_http_revision_returns_one_recorded_successor_and_current_readback(
    service,
    monkeypatch,
):
    store, spec, _record, gh, original, command = stopped_feature(service, monkeypatch)
    app = create_app(store.config.path)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    url = store.config.dashboard_url
    route = "/api/runs/" + spec["run_id"] + "/revise-feature-plan"
    async with httpx.AsyncClient(transport=transport, base_url=url) as browser:
        blocked = await browser.post(route, json=command, headers={"Origin": url})
        assert blocked.status_code == 403
        with store._connect() as db:
            assert (
                db.execute(
                    "SELECT run_id FROM delivery_runs WHERE run_id!=?", (spec["run_id"],)
                ).fetchone()
                is None
            )
        readback = (await browser.get("/api/runs/" + spec["run_id"])).json()["run"]["feature_plan"]
        assert readback["can_revise"] is True
        assert readback["expected_revision"] == command["expected_revision"]
        assert readback["plan_identity"]["comment_id"] == 11
        assert readback["authority"]["allowed_files"] == ["README.md"]
        assert readback["expected_paths"]["model"] == ["README.md"]
        session = await browser.get("/api/session")
        headers = {"Origin": url, "X-Devflow-CSRF": session.json()["csrf_token"]}
        first = await browser.post(route, json=command, headers=headers)
        assert first.status_code == 200, first.text
        result = first.json()
        assert_revision_receipt(store, spec, original, result)
        replay = await browser.post(route, json=command, headers=headers)
        assert replay.status_code == 200 and replay.json() == result
        conflict = await browser.post(
            route, json={**command, "reason": "A different proposal"}, headers=headers
        )
        assert conflict.status_code == 409 and "different inputs" in conflict.json()["detail"]
        successor = (await browser.get("/api/runs/" + result["run_id"])).json()["run"]
        assert successor["feature_plan"]["phase"] == "requested"
        assert successor["feature_plan"]["reason"] == command["reason"]
        assert successor["feature_plan"]["repair_budget"]["used"] == 4
        assert successor["feature_plan"]["can_revise"] is False
    assert not any(method != "GET" for method, _path, _body in gh.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["stale", "unchecked_plan", "unchecked_policy", "long_reason", "changed_evidence"]
)
async def test_http_invalid_revision_cannot_create_successor_or_change_remote_bindings(
    service,
    monkeypatch,
    change,
):
    store, spec, _record, gh, _original, command = stopped_feature(service, monkeypatch)
    if change == "stale":
        command["expected_revision"] += 1
    elif change == "unchecked_plan":
        command["plan"] = {"scope": "caller replacement"}
    elif change == "unchecked_policy":
        command["policy"] = {"allowed_paths": ["*"]}
    elif change == "long_reason":
        command["reason"] = "x" * 4001
    else:
        Path(command["evidence"][0]["path"]).write_text("changed after diagnostic")
    app = create_app(store.config.path)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(
        transport=transport, base_url=store.config.dashboard_url
    ) as browser:
        session = await browser.get("/api/session")
        headers = {
            "Origin": store.config.dashboard_url,
            "X-Devflow-CSRF": session.json()["csrf_token"],
        }
        refused = await browser.post(
            "/api/runs/" + spec["run_id"] + "/revise-feature-plan", json=command, headers=headers
        )
        assert refused.status_code == 409
    with store._connect() as db:
        assert (
            db.execute(
                "SELECT run_id FROM delivery_runs WHERE run_id!=?", (spec["run_id"],)
            ).fetchone()
            is None
        )
    assert registry(spec).current("I_feature")["state"] == "stopped"
    assert registry(spec).budget("I_feature")["used"] == 4
    assert not any(method != "GET" for method, _path, _body in gh.calls)


@pytest.mark.asyncio
async def test_mcp_revision_replays_the_real_admission_receipt_and_rejects_overrides(
    service, monkeypatch
):
    store, spec, _record, _gh, original, command = stopped_feature(service, monkeypatch)
    adapter = SimpleNamespace(
        revise_feature_plan=lambda run_id, request: revise_feature_plan(store, run_id, request)
    )
    monkeypatch.setattr("devflow_temporal.delivery_mcp.client", lambda _path: adapter)
    async with create_connected_server_and_client_session(
        build_server(store.config.path)
    ) as session:
        args = {"run_id": spec["run_id"], "request_json": json.dumps(command)}
        first = await session.call_tool("revise_feature_plan", args)
        assert not first.isError
        result = json.loads(first.content[0].text)
        assert_revision_receipt(store, spec, original, result)
        replay = await session.call_tool("revise_feature_plan", args)
        assert not replay.isError and json.loads(replay.content[0].text) == result
        refused = await session.call_tool(
            "revise_feature_plan",
            {**args, "request_json": json.dumps({**command, "policy": {"allowed_paths": ["*"]}})},
        )
        assert refused.isError and "bounded reason" in refused.content[0].text


def test_cli_and_client_revision_reach_real_admission_without_replacing_historical_input(
    service,
    monkeypatch,
    capsys,
):
    from devflow_temporal import delivery_control as cli

    store, spec, _record, _gh, original, command = stopped_feature(service, monkeypatch)
    caller = DeliveryClient(store.config)

    def request(method, path, payload, **_kwargs):
        assert method == "POST" and path == "/api/runs/" + spec["run_id"] + "/revise-feature-plan"
        return revise_feature_plan(store, spec["run_id"], payload)

    monkeypatch.setattr(caller, "_request", request)
    monkeypatch.setattr(cli, "api_client", lambda _path: caller)
    request_path = store.config.path.with_name("revision-request.json")
    request_path.write_text(json.dumps(command))
    args = argparse.Namespace(
        config=str(store.config.path),
        command="revise-feature-plan",
        request=request_path,
        id=spec["run_id"],
    )
    cli._run(args, argparse.ArgumentParser())
    first = json.loads(capsys.readouterr().out)
    assert_revision_receipt(store, spec, original, first)
    cli._run(args, argparse.ArgumentParser())
    assert json.loads(capsys.readouterr().out) == first
    with pytest.raises(OwnershipConflict, match="different inputs"):
        caller.revise_feature_plan(spec["run_id"], {**command, "reason": "Changed reason"})


@pytest.mark.asyncio
async def test_current_v2_readback_uses_exact_adopted_child_comment_links_and_frozen_authority(
    service,
    monkeypatch,
):
    from copy import deepcopy

    from test_delivery_github_plan_records import PlanRemote
    from test_delivery_plan_model import v2_plan

    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_feature_revisions import record_initial_plan_adoption
    from devflow_temporal.delivery_features import transition
    from devflow_temporal.delivery_github_plans import child_plan_links
    from devflow_temporal.delivery_store import DeliveryStore

    legacy, request = service
    raw = deepcopy(legacy.config.raw)
    raw.update(
        feature_delivery_version=1,
        execution_registry=str(legacy.config.path.parent / "ownership" / "registry.sqlite3"),
    )
    raw["repositories"]["fixture"]["github_repo"] = "owner/repo"
    raw["repositories"]["fixture"]["allowed_paths"] = ["README.md", "future-model.py"]
    legacy.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(legacy.config.path))
    gh = PlanRemote()
    issue = gh.issues[1]
    value = v2_plan()
    for stream in value["workstreams"]:
        for chunk in stream["chunks"]:
            chunk["expected_paths"] = ["README.md"]
    value["workstreams"][0]["chunks"][0]["expected_paths"] = ["future-model.py"]
    snapshot = {"issue": issue, "workstreams": [], "delivery": None, "default_branch": "main"}
    monkeypatch.setattr(GitHubDelivery, "snapshot", lambda *_args: deepcopy(snapshot))
    request = {**request, "issue_url": issue["url"], "accepted_plan": json.dumps(value)}
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    frozen = deepcopy(spec)
    shared = registry(spec)
    owner = spec["feature_delivery"]["owner"]
    record = gh.initialize(issue, value, shared, owner)
    adopted = record_initial_plan_adoption(spec, record, shared)
    assert adopted["child_plan_links"] == child_plan_links(record)
    for index in range(5):
        shared.repair(owner, "historical-cycle-" + str(index), "Product repair " + str(index))
    store.mark_start(spec["run_id"], accepted=True)
    shared.stop(owner, "known-stopped", {"cleanup": "confirmed"})
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET phase='blocked',outcome='blocked',cleanup='confirmed' "
            "WHERE run_id=?",
            (spec["run_id"],),
        )
        transition(db, store.config, spec["run_id"])
        assert (
            json.loads(
                db.execute(
                    "SELECT request_json FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
                ).fetchone()[0]
            )
            == frozen
        )
    gh.calls.clear()
    app = create_app(store.config.path)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=store.config.dashboard_url
    ) as browser:
        response = await browser.get("/api/runs/" + spec["run_id"])
        assert response.status_code == 200, response.text
        run = response.json()["run"]
    current = run["feature_plan"]
    assert current["plan_identity"]["plan_revision"] == 1
    assert current["plan_identity"]["plan_digest"] == digest(value)
    assert current["plan_identity"]["comment_id"] == record["comment_id"]
    assert current["plan_identity"]["workstream_issues"] == record["manifest"]["workstream_issues"]
    assert current["child_plan_links"] == child_plan_links(record)
    for stream_id, reference in current["child_plan_links"].items():
        assert reference["url"] == (
            current["plan_identity"]["workstream_issues"][stream_id]["url"]
            + "#issuecomment-"
            + str(reference["comment_id"])
        )
        assert reference["digest"] == digest(gh.comments[reference["comment_id"]]["body"])
    assert current["phase"] == "current" and current["can_revise"] is True
    assert current["expected_revision"] == run["projection_revision"]
    assert current["affected_chunks"] == []
    assert (
        current["repair_budget"]["used"] == 5
        and current["repair_budget"]["learning_required"] is True
    )
    assert current["authority"]["allowed_files"] == ["README.md", "future-model.py"]
    assert current["expected_paths"]["model"] == ["future-model.py"]
    assert (
        store.feature(spec["run_id"])["feature_plan"]["child_plan_links"]
        == current["child_plan_links"]
    )
    assert (
        gh.calls == []
    )  # Local status reads use the adopted publisher receipt, without remote polling.


@pytest.mark.asyncio
async def test_fresh_native_intake_is_readable_before_an_accepted_plan_exists(service, monkeypatch):
    from test_delivery_feature_execution import feature_service

    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_store import DeliveryStore

    store, request, _snapshot = feature_service(service, monkeypatch)
    raw = json.loads(store.config.path.read_text())
    raw["roles"]["intake"] = {"model": "fixture", "effort": "low"}
    store.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(store.config.path))
    request.pop("accepted_plan")
    receipt = store.submit(request)
    assert receipt["phase"] == "accepted"
    app = create_app(store.config.path)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=store.config.dashboard_url
    ) as browser:
        response = await browser.get("/api/runs/" + request["run_id"])
        assert response.status_code == 200, response.text
        current = response.json()["run"]["feature_plan"]
    assert current["phase"] == "unplanned"
    assert current["plan_identity"]["plan_digest"] is None
    assert current["plan_identity"]["comment_id"] is None
    assert current["child_plan_links"] == {} and current["expected_paths"] == {}
    assert current["can_revise"] is False and current["reason_ineligible"]
    assert store.feature(request["run_id"])["feature_plan"]["phase"] == "unplanned"
