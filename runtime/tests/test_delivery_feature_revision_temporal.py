"""Real Temporal coordination of evidenced correction and preserved worker custody."""
from __future__ import annotations

import asyncio
import shutil
from copy import deepcopy

import pytest
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from test_delivery_github_contract import plan
from test_delivery_store import service as service

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_workflow import DeliveryWorkflow


class RevisionActivities:
    def __init__(self, scenario):
        self.scenario = scenario
        self.calls = []
        self.old = plan()
        self.corrected = deepcopy(self.old)
        self.corrected.update(version=2, final_gates=[])
        for stream in self.corrected["workstreams"]:
            for chunk in stream["chunks"]:
                chunk["expected_paths"] = chunk.pop("allowed_paths")
                chunk["gates"] = []
        endpoint = self.corrected["workstreams"][0]["chunks"][1]
        endpoint["verification"] = ["Run current compatibility, then final endpoint checks"]
        self.record = {"manifest": {
            "revision": 1, "plan_revision": 1, "plan": deepcopy(self.old),
            "publication": {"stack_id": 42, "members": []},
        }}
        self.completed = set()
        self.used = 10 if scenario == "exhausted" else 4
        self.adopted = False
        self.diagnostic = {
            "version": 1, "kind": "planning_defect", "category": "gate_prerequisite",
            "chunk_id": "endpoint", "plan_revision": 1, "plan_sha256": digest(self.old),
            "candidate_id": "c" * 64,
            "evidence": [{"path": "/retained/selector-preflight.json", "sha256": "d" * 64}],
            "detail": "Inherited recipe requires a future chunk selector unavailable here",
        }
        if scenario == "stopped":
            self.completed = {"model", "client"}
            self.record["manifest"]["publication"]["members"] = [
                {"chunk_id": key, "number": number} for number, key in enumerate(
                    ("model", "client"), 1)
            ]

    def budget(self):
        return {"used": self.used, "maximum": 10, "learning_required": self.used >= 5}

    async def execute(self, name, payload):
        self.calls.append((name, deepcopy(payload)))
        if name == "delivery_prepare":
            return {"candidate": {"id": "a" * 64, "head": "parent-head"}}
        if name == "delivery_feature_revision_request":
            return {"revision_id": "revision-original"} if self.scenario == "stopped" else None
        if name == "delivery_feature_open":
            return {"record": deepcopy(self.record), "budget": self.budget(),
                    "checkpoints": {"verified:" + key: {} for key in self.completed}}
        if name == "delivery_feature_reserve":
            key = payload["kind"] + ":" + payload["chunk_id"]
            return {"spec": {"run_id": key}, "workflow_id": key, "completed": True}
        if name == "delivery_feature_finish_worker":
            kind, chunk = payload["child_id"].split(":")
            if (kind == "chunk" and chunk == "endpoint" and not self.adopted
                    and self.scenario != "stopped"):
                return {"outcome": "blocked", "record": deepcopy(self.record),
                        "budget": self.budget(),
                        **({"planning_defect": self.diagnostic}
                           if self.scenario != "ordinary" else {})}
            if kind == "chunk":
                self.completed.add(chunk)
                members = self.record["manifest"]["publication"]["members"]
                if not any(item["chunk_id"] == chunk for item in members):
                    members.append({"chunk_id": chunk, "number": len(members) + 1})
                self.record["manifest"]["revision"] += 1
            return {"outcome": "delivered", "record": deepcopy(self.record),
                    "budget": self.budget()}
        if name == "delivery_feature_revision_begin":
            if self.used == 10:
                raise ApplicationError("cumulative ten-cycle allowance exhausted",
                                       type="OwnershipConflict", non_retryable=True)
            self.used += 1
            return {"revision_id": "revision-original", "phase": "investigating",
                    "old_plan": deepcopy(self.old),
                    "old_identity": {"plan_revision": 1, "plan_digest": digest(self.old)},
                    "diagnostic": self.diagnostic, "repair_key": "plan-revision:original",
                    "budget": self.budget()}
        if name == "delivery_feature_revision_propose":
            return {"status": "plan", "plan": self.corrected, "diagnostic": self.diagnostic,
                    "summary": "Retain early compatibility and relocate the unavailable selector",
                    "session_id": "intake-session", "cleanup": "confirmed",
                    "proposal_receipt": {"affected_chunks": ["endpoint"]}}
        if name == "delivery_feature_revision_review":
            return {"status": "findings" if self.scenario == "review_rejects" else "pass",
                    "summary": "Final verification would be weakened" if self.scenario ==
                    "review_rejects" else "Outcome, authority and final verification retained",
                    "findings": ["missing final coverage"] if self.scenario == "review_rejects"
                    else [], "reviewed_plan_sha256": digest(self.corrected),
                    "session_id": "independent-review", "proposal_session_id": "intake-session",
                    "candidate_id": "a" * 64, "cleanup": "confirmed"}
        if name == "delivery_feature_revision_adopt":
            self.adopted = True
            self.record["manifest"].update(plan=self.corrected, plan_revision=2)
            spec = {**payload["spec"], "feature_plan_revision": {
                "plan_revision": 2, "plan_digest": digest(self.corrected)},
            }
            return {"state": "adopted", "spec": spec, "budget": self.budget(),
                    "plan_identity": spec["feature_plan_revision"], "affected_chunks": ["endpoint"],
                    "checkpoints": {"verified:" + key: {} for key in self.completed}}
        if name == "delivery_feature_revision_reject":
            return {"phase": "rejected", "budget": self.budget()}
        if name == "delivery_feature_merge":
            return {"state": "confirmed", "pull_requests": [
                {"number": item["number"]} for item in
                payload["publication"]["pull_requests"]]}
        return {"state": "confirmed"}

    def handlers(self):
        def stub(name):
            @activity.defn(name=name)
            async def handler(payload):
                return await self.execute(name, payload)
            return handler
        return [stub(name) for name in (
            "delivery_prepare", "delivery_project", "delivery_feature_open",
            "delivery_feature_reserve", "delivery_feature_finish_worker", "delivery_feature_merge",
            "delivery_feature_settle_workers", "delivery_feature_settle_effects",
            "delivery_feature_stop", "delivery_feature_revision_request",
            "delivery_feature_revision_begin", "delivery_feature_revision_propose",
            "delivery_feature_revision_review", "delivery_feature_revision_adopt",
            "delivery_feature_revision_reject",
        )]


@pytest.mark.parametrize("scenario", [
    "automatic", "ordinary", "review_rejects", "exhausted", "stopped",
])
async def test_safe_revision_transition_runs_on_temporal_and_replays(tmp_path, scenario):
    fixtures = RevisionActivities(scenario)
    spec = {"run_id": "revision-temporal-" + scenario, "provider": "fake",
            "authorized_endpoint": "merged", "plan_approval": "automatic",
            "feature_delivery": {"version": 1, "owner": {"issue_id": "I_original"}},
            "policy": {"max_repairs": 10}}
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "owned-temporal.sqlite3"),
    ) as environment:
        async with Worker(environment.client, task_queue=spec["run_id"],
                          workflows=[DeliveryWorkflow], activities=fixtures.handlers()):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue=spec["run_id"])
            result = await asyncio.wait_for(handle.result(), 40)
            history = await handle.fetch_history()
    names = [name for name, _ in fixtures.calls]
    if scenario in {"automatic", "stopped"}:
        assert result["phase"] == "merged"
        assert result["checks"]["plan_revision"]["identity"]["plan_revision"] == 2
        assert result["checks"]["repair_budget"]["used"] == 5
        assert names.index("delivery_feature_settle_workers") < names.index(
            "delivery_feature_revision_begin")
        assert names.index("delivery_feature_settle_effects") < names.index(
            "delivery_feature_revision_begin")
        assert names.index("delivery_feature_revision_review") < names.index(
            "delivery_feature_revision_adopt")
        assert names.count("delivery_feature_revision_begin") == 1
        assert names.count("delivery_feature_revision_adopt") == 1
        verified = [payload["child_id"] for name, payload in fixtures.calls
                    if name == "delivery_feature_finish_worker"
                    and payload["child_id"].startswith("chunk:")]
        assert verified.count("chunk:model") == (0 if scenario == "stopped" else 1)
        assert verified.count("chunk:client") == (0 if scenario == "stopped" else 1)
        assert verified[-1] == "chunk:endpoint"
    else:
        assert result["outcome"] == "blocked"
        assert "delivery_feature_revision_adopt" not in names
        if scenario == "ordinary":
            assert "delivery_feature_revision_begin" not in names
            assert "delivery_feature_revision_propose" not in names
            assert fixtures.used == 4
        elif scenario == "exhausted":
            assert "delivery_feature_revision_propose" not in names
            assert fixtures.used == 10
        else:
            assert names.count("delivery_feature_revision_reject") == 1
            assert fixtures.used == 5
    # No per-poll full-spec worker observations were added to this transition.
    assert "delivery_feature_worker_result" not in names
    assert len(history.events) < 450
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)


async def test_ordinary_build_findings_repair_in_the_original_session(tmp_path):
    calls = []
    def stub(name):
        @activity.defn(name=name)
        async def handler(payload):
            calls.append((name, deepcopy(payload)))
            if name == "delivery_prepare":
                return {"candidate": {"id": "original", "head": "base"}}
            if name == "delivery_role":
                iteration = payload["iteration"]
                return {"status": "findings" if iteration == 0 else "pass",
                        "findings": ["broken assertion"] if iteration == 0 else [],
                        "summary": "ordinary product defect", "session_id": "original-worker",
                        "cleanup": "confirmed", "candidate": {
                            "id": "candidate-" + str(iteration), "head": "base"}}
            return {"state": "confirmed"}
        return handler
    spec = {"run_id": "ordinary-build", "provider": "fake",
            "feature_worker": {"kind": "build", "chunk_id": "api1"},
            "policy": {"max_repairs": 10}}
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "build-temporal.sqlite3"),
    ) as environment:
        async with Worker(environment.client, task_queue=spec["run_id"],
                          workflows=[DeliveryWorkflow], activities=[stub(name) for name in (
                              "delivery_prepare", "delivery_project", "delivery_role",
                              "delivery_feature_seal_build",
                          )]):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue=spec["run_id"])
            result = await asyncio.wait_for(handle.result(), 40)
            history = await handle.fetch_history()
    roles = [payload for name, payload in calls if name == "delivery_role"]
    assert result["outcome"] == "delivered" and result["iteration"] == 1
    assert roles[1]["resume_session"] == "original-worker"
    assert roles[1]["findings"] == ["broken assertion"]
    assert not any("revision" in name for name, _ in calls)
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)


async def test_fresh_v2_intake_normalizes_once_and_derives_chunk_gates_on_temporal(
    tmp_path, service, monkeypatch,
):
    import json
    import subprocess
    import sys
    from pathlib import Path

    from devflow_temporal.delivery_activities import (
        delivery_accept_plan,
        delivery_intake,
        delivery_prepare,
        delivery_project,
    )
    from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_feature_activities import (
        delivery_feature_open,
        delivery_feature_revision_request,
    )
    from devflow_temporal.delivery_feature_execution import plan_for, registry, worker_spec
    from devflow_temporal.delivery_github_contract import GitHubDelivery, ordered_chunks
    from devflow_temporal.delivery_store import DeliveryStore

    original_store, request = service
    raw = deepcopy(original_store.config.raw)
    source = Path(raw["repositories"]["fixture"]["source_path"])
    (source / "test_current.py").write_text("def test_current():\n    assert True\n")
    subprocess.run(["git", "-C", str(source), "add", "test_current.py"], check=True)
    subprocess.run(["git", "-C", str(source), "commit", "-qm", "Current compatibility fixture"],
                   check=True)
    base_sha = subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    raw["repositories"]["fixture"].update(
        expected_base_sha=base_sha, allowed_paths=["README.md", "test_future.py"],
        checks=[{"id": "compatibility", "argv": [sys.executable, "-m", "pytest",
                                                  "test_current.py", "test_future.py"]}],
    )
    def selection(names):
        return {"stage": "checks", "recipe_id": "compatibility", "selectors": names}
    def chunk(key, deps, paths, tests):
        return {"id": key, "title": key, "scope": "Complete " + key,
                "steps": ["Implement the complete layer"], "verification": ["Run selected tests"],
                "acceptance": ["Compatible behavior"], "expected_paths": paths,
                "depends_on": deps, "gates": [selection(tests)]}
    value = {"version": 2, "scope": "Combined behavior", "acceptance": ["Complete behavior"],
             "workstreams": [
                 {"id": "api", "title": "API", "issue_number": None, "acceptance": ["API ready"],
                  "chunks": [chunk("api1", [], ["README.md"], ["test_current.py"])]},
                 {"id": "web", "title": "Web", "issue_number": None, "acceptance": ["Web ready"],
                  "chunks": [chunk("web1", ["api1"], ["test_future.py"],
                                   ["test_current.py", "test_future.py"])]}],
             "final_gates": [selection(["test_current.py", "test_future.py"])]}
    raw.update(feature_delivery_version=1,
               execution_registry=str(tmp_path / "ownership" / "registry.sqlite3"),
               fake_intake=[{"status": "plan", "summary": "Complete chunk-specific plan",
                             "plan": value}])
    raw["roles"]["intake"] = {"model": "configured-intake", "effort": "medium"}
    original_store.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(original_store.config.path))
    issue = {"id": "I_new_v2", "repository_id": "R_fixture", "repository": "example/fixture",
             "number": 3, "url": request["issue_url"], "labels": [], "body": "Complete behavior"}
    snapshot = {"issue": issue, "workstreams": [], "delivery": None, "default_branch": "main"}
    monkeypatch.setattr(GitHubDelivery, "snapshot", lambda *_a: deepcopy(snapshot))
    record = {}
    def initialize(_self, parent, candidate_plan, *_args):
        record.update(comment_id=91, comment_node_id="IC_91", manifest={
            "version": 2, "revision": 1, "plan_revision": 1, "issue_id": parent["id"],
            "repository_id": parent["repository_id"], "plan": deepcopy(candidate_plan),
            "workstream_issues": {}, "workstream_plans": {},
            "publication": {"stack_id": 88, "members": []}})
        return deepcopy(record)
    def workstreams(_self, _parent, input_record, *_args):
        assert input_record == record
        for number, stream in enumerate(record["manifest"]["plan"]["workstreams"], 41):
            stream["issue_number"] = number
            record["manifest"]["workstream_issues"][stream["id"]] = {
                "id": "I_" + stream["id"], "number": number,
                "url": "https://github.com/example/fixture/issues/" + str(number)}
        return deepcopy(record)
    monkeypatch.setattr(GitHubDelivery, "initialize", initialize)
    monkeypatch.setattr(GitHubDelivery, "workstreams", workstreams)
    monkeypatch.setattr("devflow_temporal.delivery_feature_activities.live_members", lambda *_a: [])
    request.pop("accepted_plan")
    request.update(plan_approval="automatic", authorized_endpoint="published_unmerged")
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    assert spec["feature_plan_version"] == 2 and spec["intake_required"]
    workers = []
    calls = []
    def stub(name):
        @activity.defn(name=name)
        async def handler(payload):
            calls.append((name, deepcopy(payload)))
            if name == "delivery_feature_reserve":
                parent = payload["spec"]
                current = plan_for(parent)
                selected = next(c for c in ordered_chunks(current)
                                if c["id"] == payload["chunk_id"])
                child_issue = record["manifest"]["workstream_issues"][selected["workstream_id"]]
                worker = worker_spec(parent, selected, child_issue, kind=payload["kind"],
                                     base_sha=parent["base_sha"], base_branch="main")
                workers.append(worker)
                return {"spec": worker, "completed": True, "workflow_id": worker["run_id"]}
            if name == "delivery_feature_finish_worker":
                child = next(w for w in workers if w["run_id"] == payload["child_id"])
                if child["feature_worker"]["kind"] == "chunk":
                    key = child["feature_worker"]["chunk_id"]
                    members = record["manifest"]["publication"]["members"]
                    number = len(members) + 1
                    members.append({"chunk_id": key, "number": number,
                                    "url": "https://github.com/example/fixture/pull/" + str(number),
                                    "head": child["base_sha"], "branch": child["branch"],
                                    "base": "main"})
                return {"outcome": "delivered", "record": deepcopy(record),
                        "budget": registry(payload["spec"]).budget(issue["id"])}
            if name == "delivery_feature_merge":
                return {"state": "confirmed",
                        "pull_requests": payload["publication"]["pull_requests"]}
            return {"state": "confirmed"}
        return handler
    open_attempts = 0
    @activity.defn(name="delivery_feature_open")
    async def lost_initial_completion(payload):
        nonlocal open_attempts
        open_attempts += 1
        result = await delivery_feature_open(payload)
        if open_attempts == 1:
            raise ConnectionError("initial adoption persisted; completion was lost")
        return result
    handlers = [delivery_prepare, delivery_project, delivery_intake, delivery_accept_plan,
                lost_initial_completion, delivery_feature_revision_request]
    handlers += [stub(name) for name in (
                    "delivery_feature_reserve", "delivery_feature_finish_worker",
                    "delivery_feature_merge",
                    "delivery_feature_settle_workers", "delivery_feature_settle_effects",
                    "delivery_feature_stop")]
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "new-v2-temporal.sqlite3"),
        data_converter=DELIVERY_DATA_CONVERTER,
    ) as environment:
        async with Worker(environment.client, task_queue="new-v2", workflows=[DeliveryWorkflow],
                          activities=handlers):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue="new-v2")
            for _ in range(600):
                state = await handle.query(DeliveryWorkflow.status)
                if (state.get("decision") or {}).get("kind") == "merge":
                    break
                if state.get("outcome") == "blocked":
                    raise AssertionError(state)
                await asyncio.sleep(0.05)
            decision = state["decision"]
            await handle.execute_update(DeliveryWorkflow.decision, {
                "command_id": "authorize-fixture-merge", "expected_revision": state["revision"],
                "decision_id": decision["id"], "decision_revision": decision["revision"],
                "candidate_revision": decision["candidate_revision"], "answer": "merge",
            })
            result = await asyncio.wait_for(handle.result(), 40)
            history = await handle.fetch_history()
    assert result["phase"] == "merged", result
    assert result["checks"]["repair_budget"]["used"] == 0
    assert open_attempts == 2
    current = store.effective_spec(spec["run_id"])
    resolved = json.loads(current["accepted_plan"])
    assert [s["issue_number"] for s in resolved["workstreams"]] == [41, 42]
    assert current["feature_plan_revision"] == {"plan_revision": 1, "plan_digest": digest(resolved)}
    assert store.submitted_spec(spec["run_id"])["accepted_plan"] == ""
    checkpoints = registry(current).checkpoints(issue["id"])
    assert checkpoints["accepted-plan"]["digest"] == digest(resolved)
    assert checkpoints["plan-revision:adopted:1"]["repair_key"] is None
    api = next(w for w in workers if w["feature_worker"]["chunk_id"] == "api1")
    final = next(w for w in workers if w["feature_worker"]["chunk_id"] == "web1"
                 and w["feature_worker"]["kind"] == "chunk")
    assert api["policy"]["checks"][0]["required_selectors"] == ["test_current.py"]
    assert "test_future.py" not in api["policy"]["checks"][0]["argv"]
    assert final["policy"]["checks"][0]["required_selectors"] == [
        "test_current.py", "test_future.py"]
    assert api["policy"]["allowed_paths"] == spec["policy"]["allowed_paths"]
    assert api["expected_paths"] == ["README.md"]
    assert all(w["feature_plan_revision"] == current["feature_plan_revision"] for w in workers)
    assert len(history.events) < 260
    await Replayer(workflows=[DeliveryWorkflow],
                   data_converter=DELIVERY_DATA_CONVERTER).replay_workflow(history)


async def test_gate_only_revision_resume_at_iteration_ten_keeps_candidate_and_no_implementer(
    tmp_path,
):
    candidate = {"id": "preserved-candidate", "head": "preserved-head"}
    spec = {"run_id": "revision-gates-only", "provider": "fake", "retry_budget_version": 1,
            "feature_worker": {"kind": "chunk", "chunk_id": "api1"},
            "feature_plan_revision": {"plan_revision": 2, "plan_digest": "a" * 64},
            "policy": {"max_repairs": 10, "browser_qa": {"id": "corrected-browser"}}}
    original = {"run_id": spec["run_id"], "phase": "blocked", "execution_state": "blocked",
                "outcome": "blocked", "revision": 20, "iteration": 10, "cleanup": "confirmed",
                "candidate": candidate, "candidate_revision": 11, "pull_request": None,
                "decision": None, "error": "Incorrect prerequisite placement", "findings": [],
                "checks": {"retained_checkpoint": {"custody_ref": "/private/original-custody"}},
                "usage": {}, "tracker": {}, "roles": [{"role": "implement", "iteration": 10,
                    "status": "pass", "session_id": "original-session", "candidate": candidate}]}
    recovery = {"kind": "stopped_delivery_resume", "execution_spec": spec,
                "execution_candidate": candidate, "state": original,
                "session_id": "original-session",
                "maximum_iteration": 10, "command": {"additional_iterations": 0},
                "resume_stage": "checks", "resume_iteration": 10,
                "feature_plan_revision": {"revision_id": "sealed-revision", "gate_evidence": {}}}
    calls = []
    def stub(name):
        @activity.defn(name=name)
        async def handler(payload):
            calls.append((name, deepcopy(payload)))
            if name == "delivery_role":
                assert payload["role"] in {"review", "verify"}
                return {"status": "pass", "summary": "Exact preserved candidate passes",
                        "findings": [], "candidate": candidate, "cleanup": "confirmed",
                        "session_id": "independent-" + payload["role"]}
            if name == "delivery_publish":
                return {"state": "published", "candidate": candidate, "head": candidate["head"],
                        "number": 7, "url": "https://github.com/example/fixture/pull/7"}
            if name == "delivery_browser_qa":
                return {"state": "passed", "cleanup": "confirmed", "receipt": "/owned/browser.json",
                        "receipt_sha256": "b" * 64, "log": "/owned/browser.log",
                        "log_sha256": "c" * 64}
            if name in {"delivery_tracker_start", "delivery_tracker"}:
                return {"state": "consistent"}
            passed = name in {"delivery_checks", "delivery_precheck", "delivery_ci"}
            return {"state": "passed" if passed else "confirmed", "cleanup": "confirmed"}
        return handler
    handlers = [stub(name) for name in (
        "delivery_repair_preflight", "delivery_tracker_start", "delivery_project",
        "delivery_precheck", "delivery_publish", "delivery_checks", "delivery_browser_qa",
        "delivery_role", "delivery_ci", "delivery_tracker")]
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "gate-resume-temporal.sqlite3"),
    ) as environment:
        async with Worker(environment.client, task_queue=spec["run_id"],
                          workflows=[DeliveryWorkflow], activities=handlers):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, args=[spec, recovery], id=spec["run_id"],
                task_queue=spec["run_id"])
            result = await asyncio.wait_for(handle.result(), 30)
            history = await handle.fetch_history()
    assert result["outcome"] == "delivered", result
    assert result["candidate"] == candidate and result["iteration"] == 10
    assessments = [p for name, p in calls if name == "delivery_role"]
    assert [p["role"] for p in assessments] == ["review", "verify"]
    assert all(p["iteration"] == 10 for p in assessments)
    assert any(name == "delivery_browser_qa" for name, _ in calls)
    assert result["roles"][0]["session_id"] == "original-session"
    assert not any(name.startswith("delivery_feature_revision_") for name, _ in calls)
    assert len(history.events) < 180
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
