"""Completed source reuse and ordinary stopped-worker repair remain distinct."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
import test_delivery_feature_revision_subject as subject_tests
from test_delivery_feature_review_context import context_repository
from test_delivery_feature_revision_subject import (
    findings_reconsideration as findings_reconsideration,
)
from test_delivery_feature_revision_subject import (
    intake_fixture as intake_fixture,
)
from test_delivery_feature_revision_subject import (
    native_configuration as native_configuration,
)
from test_delivery_feature_revision_subject import (
    service as service,
)

from devflow_temporal import delivery_feature_completed_build as completed
from devflow_temporal import delivery_feature_revisions as revisions
from devflow_temporal import delivery_policy_recovery as policy
from devflow_temporal import delivery_stopped_resume as stopped
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_feature_execution import worker_key
from devflow_temporal.delivery_native_threads import NativeThreadObservation
from devflow_temporal.delivery_resources import write_private
from devflow_temporal.delivery_sandbox import _native_role_home
from devflow_temporal.supervisor import DeliverySupervisor


def _source(tmp_path, monkeypatch, *, spec=None, store=None, shared=None, runtime=None):
    """Exact portable native receipt shape; current metadata is an explicit seam."""
    if runtime is None:
        _, _, _, runtime = context_repository(tmp_path, monkeypatch)
    configuration = {"captured": "isolated source authority"}
    spec = spec or {
        "run_id": "completed-build", "work_id": "build-work", "provider": "codex",
        "config_path": str(tmp_path / "configuration.json"), "config_digest": digest(configuration),
        "checkout": str(tmp_path / "build-checkout"),
        "state_dir": str(tmp_path / "completed-build"),
        "base_sha": "b" * 40, "policy": {"execution_backend": "native-macos",
            "allowed_paths": ["README.md"], "max_repairs": 10},
        "feature_worker": {"kind": "build", "chunk_id": "model"},
        "feature_delivery": {"owner": {"issue_id": "parent", "generation": 1}},
    }
    spec["policy_digest"] = digest(spec["policy"])
    output = {"id": digest("output"), "head": spec["base_sha"], "base_sha": spec["base_sha"],
              "content_sha256": digest("completed content"), "environment_digest": digest("env"),
              "policy_digest": spec["policy_digest"]}
    input_candidate = {**output, "id": digest("input"), "content_sha256": digest("input bytes")}
    request = {"spec": spec, "role": "implement", "iteration": 0, "candidate": input_candidate,
               "resume_session": None}
    job = DeliverySupervisor._job_key(request)
    folder = Path(spec["state_dir"]) / "attempts" / job
    request.update(native_authorized=True, role_evidence_key=job, workspace=spec["checkout"],
                   result_path=str(folder / "result.json"), start_path=str(folder / "start.json"))
    session, identity = "completed-original-session", "finished-native-pid"
    observation = {"state": "confirmed", "role": "implement", "iteration": 0,
        "run_id": spec["run_id"], "parent_thread_id": session, "pid": 321,
        "start_identity": identity, "resumed_from": None, "new_child_thread_ids": [],
        "collaboration_items": [], "turns": [{"turn": {"status": "completed", "error": None},
                                               "thread_id": session, "turn_id": "original-turn"}],
        "schema": "devflow-native-kit-threads-v1", "raw_turn_items": 1,
        "thread_inventory_before": [], "thread_inventory_after": [session],
        "observation_source": "SDK typed TurnResult and isolated CODEX_HOME thread_list"}
    observation_path = folder / "native-thread-observation.json"
    write_private(observation_path, observation)
    reference = NativeThreadObservation.reference(SimpleNamespace(
        data=observation, path=observation_path, max_attempts=1))
    raw = {"status": "pass", "finish_reason": "done", "session_id": session,
           "findings": [], "native_thread_observation": reference}
    # This is the actual legacy pass shape: candidate is in the journal's
    # controller-measured output binding; cleanup is controller enrichment.
    native = {"state": "finished", "exit_code": 0, "stdio_drained": True,
              "monitoring_complete": True, "cancelled": False, "timed_out": False,
              "cleanup": "observed-native-confirmed",
              "journal": str(folder / "native-process.json"),
              "runtime_identity": {"dirty": False, "local_digest": None,
                **{key: runtime[key] for key in ("revision", "runtime_payload_sha256")}}}
    result = {**raw, "cleanup": "confirmed", "native_process": native}
    argv = ["/fixture/python", "-I", "-m", "devflow_temporal.role_runner",
            str(folder / "request.json")]
    launch = {"argv": argv,
              "environment": {"CODEX_HOME": str(_native_role_home(request) / "codex")}}
    journal = {"phase": "finished", "monitoring_complete": True, "result": native,
        "runtime_identity": native["runtime_identity"],
        "owned": {"321": {"identity": identity}, "654": {"identity": "finished-descendant"}},
        "intent": {"argv": argv, "run_id": spec["run_id"], "policy_digest": spec["policy_digest"],
                   "cwd": spec["checkout"], "environment_sha256": digest(launch["environment"])},
        "provider_session": {"session_id": session, "resumed_from": None, "role": "implement",
            "iteration": 0, "result_digest": digest(result),
            "output_candidate": {key: output[key] for key in ("id", "head", "content_sha256")}}}
    for name, value in (("request.json", request), ("result.json", raw),
                        ("native-process.json", journal), ("launch.json", launch),
                        ("start.json", {"pid": 321, "started_at": "2026-10-10T12:01:01+00:00"})):
        write_private(folder / name, value)
    patch = Path(spec["state_dir"]) / "feature-implementation.patch"
    patch.write_bytes(b"sealed implementation patch\n")
    patch.chmod(0o600)
    checkpoint = {"path": str(patch), "sha256": hashlib.sha256(patch.read_bytes()).hexdigest(),
                  "base": spec["base_sha"], "candidate": output, "worker_run_id": spec["run_id"],
                  "paths": list(spec["policy"]["allowed_paths"])}
    finalization = {"state": "confirmed", "outcome": "delivered", "resource_cleanup": "confirmed",
                    "process_cleanup": "observed-native-confirmed", "roots": []}
    finalization_path = Path(spec["state_dir"]) / "resources/finalization.json"
    write_private(finalization_path, finalization)
    checks = {"build_checkpoint": checkpoint, "resource_cleanup": {**finalization,
        "receipt": str(finalization_path),
        "receipt_sha256": hashlib.sha256(finalization_path.read_bytes()).hexdigest()}}
    row = {"run_id": spec["run_id"], "phase": "delivered", "outcome": "delivered",
           "execution_state": "terminal", "cleanup": "confirmed", "iteration": 0,
           "recovery_json": None, "pr_json": None, "candidate_json": canonical_json(output),
           "checks_json": canonical_json(checks), "created_at": "2026-10-10T12:00:00+00:00",
           "updated_at": "2026-10-10T12:03:00+00:00", "request_json": canonical_json(spec)}
    attempt = {"job_key": job, "run_id": spec["run_id"], "role": "implement", "iteration": 0,
        "candidate_id": input_candidate["id"], "state": "finished", "cleanup": "confirmed",
        "pid": 654, "process_identity": "finished-descendant", "session_id": session,
        "started_at": "2026-10-10T12:01:00+00:00", "finished_at": "2026-10-10T12:02:00+00:00",
        "result_path": request["result_path"], "result_json": canonical_json(result)}
    assignment = {"run_id": spec["run_id"], "kind": "build", "chunk_id": "model",
                  **({"store_path": str(store.config.tracking_db)} if store is not None else {})}
    owner = spec["feature_delivery"]["owner"]
    values = {"build:model": checkpoint, "assignment:model:build": assignment,
              "worker-input:" + worker_key(spec["run_id"], owner): spec,
              "worker-input:source-consumer:generation:1": {
                  "feature_worker": {"kind": "chunk", "seed": checkpoint}}}
    if shared is None:
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        db.execute("CREATE TABLE execution_checkpoints "
                   "(issue_id,checkpoint_key,content_digest,content_json)")
        db.execute("CREATE TABLE execution_workers(issue_id,worker_key,state,receipt_json)")

        @contextmanager
        def connect():
            yield db

        shared = SimpleNamespace(connect=connect)
    with shared.connect() as db:
        for key, value in values.items():
            # Fixture seeding only, never a production controller mutation.
            if hasattr(shared, "_checkpoint"):
                shared._checkpoint(db, owner, key, value)
            else:
                db.execute("INSERT INTO execution_checkpoints VALUES (?,?,?,?)",
                           (owner["issue_id"], key, digest(value), canonical_json(value)))
        receipt = {"run_id": spec["run_id"], "outcome": "delivered", "cleanup": "confirmed"}
        columns = "issue_id,worker_key,state,receipt_json"
        parameters = [owner["issue_id"], worker_key(spec["run_id"], owner), "finished",
                      canonical_json(receipt)]
        if hasattr(shared, "_checkpoint"):
            columns += ",workstream_id,generation"
            parameters += [spec["feature_worker"]["workstream_id"], owner["generation"]]
        db.execute("INSERT INTO execution_workers (" + columns + ") VALUES ("
                   + ",".join("?" for _ in parameters) + ")", parameters)
        db.commit()
        if store is None:
            db.execute("PRAGMA query_only=ON")
    store = store or SimpleNamespace(owns_execution=lambda _: True)
    old_rows = policy._rows
    monkeypatch.setattr(policy, "_rows", lambda s, r: (deepcopy(row), [deepcopy(attempt)], [], None)
                        if r == spec["run_id"] else old_rows(s, r))
    old_binding = policy.work_binding
    monkeypatch.setattr(policy, "work_binding", lambda s, p, d: {"source": "same-original-issue"}
                        if p["run_id"] == spec["run_id"] else old_binding(s, p, d))
    old_cleanup = stopped.observed_native_cleanup
    monkeypatch.setattr(stopped, "observed_native_cleanup", lambda p, **kw: digest("observed")
                        if p["run_id"] == spec["run_id"] else old_cleanup(p, **kw))
    if not hasattr(store, "_connect"):
        store._connect = shared.connect
        monkeypatch.setattr("devflow_temporal.delivery_config.DeliveryConfig.load",
                            lambda _: SimpleNamespace(raw=configuration))

    class Broker:
        def __init__(self, _store, observed):
            assert observed == spec

        def validate_candidate_scope(self):
            return set(checkpoint["paths"])

        def candidate(self):
            return deepcopy(output)

        def _record(self):
            return {"canonical": "same-parent-and-child"}

        def observe_unpublished(self):
            pass

    # Read-only current Git, canonical publication and resource availability are
    # metadata seams here. Native files/checkpoint/patch and result binding run.
    monkeypatch.setattr(completed, "ObservationBroker", Broker)
    return SimpleNamespace(store=store, spec=spec, row=row, attempts=[attempt],
        assignment=assignment,
        values=values, shared=shared, checkpoint=checkpoint, request=request, output=output,
        folder=folder, checks=checks)


def test_completed_source_proves_legacy_native_pass_without_temporal_or_execution(
    tmp_path, monkeypatch,
):
    f = _source(tmp_path, monkeypatch)
    before = digest((f.spec, f.row, f.attempts, f.values))
    proof = completed.completed_build_subject(f.store, f.spec, f.spec, f.row, f.attempts,
                                             f.assignment, f.values, f.shared)
    assert proof == completed.completed_build_subject(f.store, f.spec, f.spec, f.row, f.attempts,
                                                      f.assignment, f.values, f.shared)
    assert proof["candidate"] == f.output and proof["current_review_subject"] is None
    assert proof["progress_custody"]["kind"] == "completed_unchanged_build_source"
    assert "closed" not in proof and "session_id" not in proof
    assert before == digest((f.spec, f.row, f.attempts, f.values))
    assert "candidate" not in json.loads((f.folder / "result.json").read_text())


def test_completed_source_refuses_missing_changed_native_patch_cleanup_and_consumers(
    tmp_path, monkeypatch,
):
    f = _source(tmp_path, monkeypatch)

    def observe():
        return completed.completed_build_subject(f.store, f.spec, f.spec, f.row, f.attempts,
                                                 f.assignment, f.values, f.shared)

    for name, mutate in (
        ("request.json", lambda v: v.update(native_authorized=False)),
        ("native-process.json", lambda v: v["provider_session"].update(output_candidate={})),
        ("native-process.json", lambda v: v.update(monitoring_complete=False)),
        ("native-thread-observation.json", lambda v: v.update(state="unknown")),
    ):
        path = f.folder / name
        original = path.read_bytes()
        value = json.loads(original)
        mutate(value)
        write_private(path, value)
        with pytest.raises(ValueError):
            observe()
        path.write_bytes(original)
    patch = Path(f.checkpoint["path"])
    raw_patch = patch.read_bytes()
    patch.write_bytes(b"changed patch")
    with pytest.raises(ValueError, match="sealed patch"):
        observe()
    patch.write_bytes(raw_patch)
    f.checks["resource_cleanup"]["receipt_sha256"] = "0" * 64
    f.row["checks_json"] = canonical_json(f.checks)
    with pytest.raises(ValueError, match="finalization"):
        observe()
    f.checks["resource_cleanup"]["receipt_sha256"] = hashlib.sha256(
        (Path(f.spec["state_dir"]) / "resources/finalization.json").read_bytes()).hexdigest()
    f.row["checks_json"] = canonical_json(f.checks)
    f.values["worker-input:source-consumer:generation:1"] = {
        "feature_worker": {"seed": {**f.checkpoint, "sha256": "0" * 64}}}
    with pytest.raises(ValueError, match="consumed source"):
        observe()


@pytest.mark.skipif(sys.platform != "darwin", reason="native preparation fixture")
def test_public_mixed_completed_source_and_unpassed_chunk_keep_seven_and_eight(
    findings_reconsideration, tmp_path, monkeypatch,
):
    from test_delivery_feature_revisions import _boundary_snapshot

    from devflow_temporal.delivery_execution_registry import OwnershipConflict

    fixture, _, runtime, child, _, _, _ = findings_reconsideration
    store, parent, shared, plan, _, _, prior, _, _, _ = fixture
    spec = deepcopy(child)
    spec.update(run_id="zz-completed-build", work_id="zz-build-work",
                state_dir=str(tmp_path / "zz-completed-build"),
                checkout=str(tmp_path / "zz-build-checkout"), branch="fixture-build-source")
    spec["feature_worker"].update(kind="build", seed=None)
    f = _source(tmp_path, monkeypatch, spec=spec, store=store, shared=shared, runtime=runtime)
    with store._connect() as db:
        original = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                                   (child["run_id"],)).fetchone())
        row = {**original, **f.row, "workflow_id": "expired-build-workflow"}
        columns = ",".join(row)
        db.execute("INSERT INTO delivery_runs (" + columns + ") VALUES ("
                   + ",".join("?" for _ in row) + ")", tuple(row.values()))
        columns = ",".join(f.attempts[0])
        db.execute("INSERT INTO delivery_attempts (" + columns + ") VALUES ("
                   + ",".join("?" for _ in f.attempts[0]) + ")", tuple(f.attempts[0].values()))
    f.row.update(row)
    changed = deepcopy(plan)
    changed_chunk = next(c for stream in changed["workstreams"] for c in stream["chunks"]
                         if c["id"] == spec["feature_worker"]["chunk_id"])
    changed_chunk["steps"].append("Change the implementation consumed by dependent work")
    _, _, implementation = revisions._admit_plan(
        parent, prior["old_plan"], changed, prior["old_identity"])
    assert changed_chunk["id"] in implementation
    assert all(c["id"] in implementation for stream in changed["workstreams"]
               for c in stream["chunks"] if changed_chunk["id"] in c["depends_on"])
    values = shared.checkpoints(prior["owner"]["issue_id"])
    observe = stopped.review_snapshot
    strict_build = []

    def require_execution_closure(s, run):
        if run == spec["run_id"]:
            strict_build.append(run)
            raise ValueError("changed build requires exact Temporal execution closure")
        return observe(s, run)

    # A source change and its dependency closure take the ordinary execution
    # proof path. There is no NotFound fallback for a completed affected build.
    with monkeypatch.context() as selected:
        selected.setattr(stopped, "review_snapshot", require_execution_closure)
        selected.setattr(completed, "completed_build_subject", lambda *_: pytest.fail(
            "implementation-changing proposal cannot reuse completed build source"))
        with pytest.raises(ValueError, match="requires exact Temporal execution closure"):
            revisions._worker_subject(store, parent, prior, {"plan": changed}, values)
    assert strict_build == [spec["run_id"]]
    original_record, original_adopt = revisions.record_proposal, revisions.adopt_revision
    refused = []

    def record_exact(s, p, revision, value, **kwargs):
        before = _boundary_snapshot(s, shared, prior["owner"]["issue_id"])
        with pytest.raises(ValueError, match="differs from selected review subject"):
            original_record(s, p, revision, changed, **kwargs)
        assert _boundary_snapshot(s, shared, prior["owner"]["issue_id"]) == before
        refused.append("proposal")
        return original_record(s, p, revision, value, **kwargs)

    def adopt_exact(s, p, revision, value, review, **kwargs):
        before = _boundary_snapshot(s, shared, prior["owner"]["issue_id"])
        with pytest.raises(OwnershipConflict, match="sealed correction proposal"):
            original_adopt(s, p, revision, changed, review, **kwargs)
        assert _boundary_snapshot(s, shared, prior["owner"]["issue_id"]) == before
        refused.append("adoption")
        return original_adopt(s, p, revision, value, review, **kwargs)

    monkeypatch.setattr(revisions, "record_proposal", record_exact)
    monkeypatch.setattr(revisions, "adopt_revision", adopt_exact)
    # The existing consolidated public sequence checks exact lost-response replay,
    # fresh intake/review/adoption, original finding/session dispatch and no-alias
    # planning7/writer8. The completed build adds its real source proof path.
    subject_tests.test_public_findings_review_then_original_worker_handoff_charges_seven_and_eight_once(
                findings_reconsideration, monkeypatch)
    assert refused == ["proposal", "adoption"]
    current_parent = store.effective_spec(shared.current(prior["owner"]["issue_id"])["run_id"])
    before = shared.budget(prior["owner"]["issue_id"])
    result = revisions.resume_revision_worker(store, current_parent, spec)
    assert result["completed"] is True and result["spec"] == spec
    assert shared.budget(prior["owner"]["issue_id"]) == before == {
        "used": 8, "maximum": 10, "learning_required": True}
