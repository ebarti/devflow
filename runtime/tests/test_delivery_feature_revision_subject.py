"""Native progress, read-only observation and unpassed-subject boundaries."""
from __future__ import annotations

import asyncio
import inspect
import json
import os
import sqlite3
import sys
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_delivery_feature_review_context import context_repository
from test_delivery_feature_revisions import feature as feature
from test_delivery_gate_retry import unpublished as unpublished
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_stopped_resume import stopped as stopped
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_revision_subject as subject
from devflow_temporal import delivery_policy_recovery as policy_recovery
from devflow_temporal import delivery_stopped_resume as stopped_resume
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_publication import current_record as original_current_record
from devflow_temporal.delivery_feature_publication import live_members as original_live_members
from devflow_temporal.delivery_resources import _gate_evidence_root, write_private
from devflow_temporal.delivery_sandbox import _native_role_home
from devflow_temporal.supervisor import DeliverySupervisor


def candidate(spec, name):
    return {"id": digest(name), "head": "a" * 40, "content_sha256": digest("content:" + name),
            "base_sha": spec["base_sha"], "environment_digest": "c" * 64,
            "policy_digest": spec["policy_digest"]}


def _progress_fixture(tmp_path, monkeypatch, *, spec=None, runtime=None, owner_store=None,
                      candidates=None, last_iteration=3, session="original-implementation-session"):
    if runtime is None:
        _, _, _, runtime = context_repository(tmp_path, monkeypatch)
    spec = spec or {"run_id": "retained-worker", "state_dir": str(tmp_path / "retained-worker"),
            "checkout": str(tmp_path / "checkout"), "base_sha": "b" * 40,
            "policy_digest": "d" * 64, "provider": "codex",
            "policy": {"execution_backend": "native-macos", "max_repairs": 10},
            "feature_worker": {"kind": "chunk"}}
    Path(spec["state_dir"]).mkdir(mode=0o700, exist_ok=True)
    Path(spec["checkout"]).mkdir(exist_ok=True)
    original_session = session
    admission, terminal, current = candidates or tuple(candidate(spec, name) for name in (
        "admit", "pass", "failed"))
    prefix = [{"role": "implement", "iteration": 1, "candidate": admission,
               "status": "pass", "session_id": original_session, "cleanup": "confirmed"}]
    prior = {"run_id": spec["run_id"], "iteration": 1, "candidate": admission, "roles": prefix}
    command = {"continuation_kind": stopped_resume.KIND, "command_id": "sealed-original-resume",
               "expected_revision": 9, "expected_iteration": 1,
               "expected_candidate_id": admission["id"],
               "expected_candidate_head": admission["head"],
               "additional_iterations": 9}
    recovery = {"kind": stopped_resume.KIND, "execution_spec": spec, "state": prior,
                "execution_candidate": admission, "session_id": original_session,
                "closed": {"closed_at": "2026-10-10T12:00:00+00:00"}, "command": command,
                "command_digest": digest({"run_id": spec["run_id"], **command}),
                "maximum_iteration": 10, "predecessor_result_digest": digest(prior)}
    admission_path = Path(spec["state_dir"]) / stopped_resume.namespace(recovery) / "admission.json"
    response = {"workflow_id": "delivery-retained-worker-resume",
                "authorized_through_iteration": 10,
                "admission_sha256": digest(recovery)}
    if owner_store is not None:
        # The original admission precedes its native role requests, so the
        # controller-owned continuation evidence namespace already exists.
        write_private(admission_path, recovery)
        with owner_store._connect() as owner_db:
            owner_db.execute("INSERT INTO delivery_commands VALUES (?,?,?,?)", (
                command["command_id"], spec["run_id"], recovery["command_digest"],
                json.dumps(response)))
            owner_db.execute("UPDATE delivery_runs SET recovery_json=? WHERE run_id=?",
                             (json.dumps(recovery), spec["run_id"]))
    attempts, roles, files = [], deepcopy(prefix), {}
    input_candidate = admission
    turns = [(role, iteration, terminal) for iteration in range(2, last_iteration)
             for role in ("implement", "review")]
    turns.append(("implement", last_iteration, current))
    for role, iteration, output in turns:
        request = {"spec": spec, "role": role, "iteration": iteration,
                   "candidate": input_candidate,
                   "resume_session": original_session if role == "implement" else None}
        job = DeliverySupervisor._job_key(request)
        folder = Path(spec["state_dir"]) / "attempts" / job
        workspace = (spec["checkout"] if role == "implement" else
                     str(_gate_evidence_root(spec) / "gates" / str(iteration) / role))
        request.update(native_authorized=True, role_evidence_key=job, workspace=workspace,
                       result_path=str(folder / "result.json"),
                       start_path=str(folder / "start.json"))
        session = (original_session if role == "implement" else
                   "independent-review-" + str(iteration))
        raw = {"status": "findings" if iteration == last_iteration else "pass",
               "finish_reason": "done",
               "session_id": session, "findings": ["Original unresolved product finding"]
               if iteration == last_iteration else [], "cleanup": "confirmed"}
        identity = {"revision": runtime["revision"], "runtime_payload_sha256":
                    runtime["runtime_payload_sha256"], "dirty": False, "local_digest": None}
        native = {"state": "finished", "monitoring_complete": True, "stdio_drained": True,
                  "exit_code": 0, "cancelled": False, "timed_out": False,
                  "cleanup": "observed-native-confirmed", "runtime_identity": identity,
                  "journal": str(folder / "native-process.json")}
        result = {**raw, "native_process": native}
        # Native provider output omits the monitor's controller enrichment.
        raw.pop("cleanup")
        argv = ["/fixture/python", "-I", "-m", "devflow_temporal.role_runner",
                str(folder / "request.json")]
        launch = {"argv": argv, "environment": {
            "CODEX_HOME": str(_native_role_home(request) / "codex")}}
        journal = {"phase": "finished", "monitoring_complete": True, "result": native,
                   "runtime_identity": identity, "owned": {"321": {"identity": "finished-pid"}},
                   "intent": {"argv": argv, "run_id": spec["run_id"], "policy_digest":
                              spec["policy_digest"], "cwd": workspace,
                              "environment_sha256": digest(launch["environment"])},
                   "provider_session": {"role": role, "iteration": iteration,
                       "session_id": session, "resumed_from": request["resume_session"],
                       "result_digest": digest(result), "output_candidate": {
                           key: output[key] for key in ("id", "head", "content_sha256")}}}
        for name, value in (("request.json", request), ("result.json", raw),
                            ("native-process.json", journal), ("launch.json", launch)):
            path = folder / name
            write_private(path, value)
            files[(role, iteration, name)] = path
        attempts.append({"job_key": job, "run_id": spec["run_id"], "role": role,
                         "iteration": iteration, "candidate_id": input_candidate["id"],
                         "session_id": session, "state": "finished", "cleanup": "confirmed",
                         "pid": 321, "process_identity": "finished-pid",
                         "started_at": "2026-10-10T12:01:00+00:00",
                         "finished_at": "2026-10-10T12:02:00+00:00",
                         "result_path": request["result_path"], "result_json": json.dumps(result)})
        roles.append({**result, "role": role, "iteration": iteration, "candidate": output,
                      "input_candidate_id": input_candidate["id"]})
        input_candidate = output
    state = {"run_id": spec["run_id"], "iteration": last_iteration, "revision": 17,
             "candidate": terminal, "roles": roles}
    closed = {"result": state, "workflow_id": "delivery-retained-worker-resume",
              "execution_run_id": "exact-closed-execution", "closed_at":
              "2026-10-10T12:03:00+00:00",
              "request_digest": spec.get("request_digest", digest(spec)),
              "recovery_digest": digest(recovery)}
    row = {"protocol_revision": 17, "candidate_json": json.dumps(terminal)}
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE delivery_commands(command_id,run_id,request_digest,response_json)")
    if owner_store is None:
        db.execute("INSERT INTO delivery_commands VALUES (?,?,?,?)", (command["command_id"],
                   spec["run_id"], recovery["command_digest"], json.dumps(response)))
    db.commit()
    db.execute("PRAGMA query_only=ON")
    if owner_store is None:
        write_private(admission_path, recovery)

    @contextmanager
    def connect():
        yield db

    store = owner_store or SimpleNamespace(_connect=connect)
    seal = {"predecessor_spec": spec, "row": row, "attempts": attempts, "state": state,
            "closed": closed, "candidate": current, "session_id": original_session,
            "original_recovery": recovery, "effects": [], "cleanup_digest": "observed",
            "work_binding": {"issue": "same-original-issue"},
            "session_custody": {"session_id": original_session, "session_state_digest": "same",
                "generation": spec.get("implementation_role_home_generation",
                                       spec.get("role_home_generation", ""))}}
    # Metadata observation is an explicit isolated seam. The production observer
    # and its mutation refusal are tested separately; all native progress runs here.
    monkeypatch.setattr(stopped_resume, "review_snapshot", lambda *_: seal)
    yield store, spec, row, attempts, recovery, seal, files
    db.close()


@pytest.fixture
def progress(tmp_path, monkeypatch):
    yield from _progress_fixture(tmp_path, monkeypatch)


def test_current_findings_prove_progress_and_preserve_all_candidates(progress):
    store, spec, row, attempts, recovery, seal, _ = progress
    before = digest((row, attempts, recovery, seal))
    proof = subject.retained_worker_subject(store, spec, row, attempts, recovery)
    assert proof == subject.retained_worker_subject(store, spec, row, attempts, recovery)
    assert proof["candidate"] == seal["candidate"] != proof["terminal_candidate"]
    assert proof["progress_custody"]["admission_candidate"] == recovery["execution_candidate"]
    assert proof["current_review_subject"]["status"] == "findings"
    assert proof["current_review_subject"]["unresolved_product_findings"] == [
        "Original unresolved product finding"]
    assert digest((row, attempts, recovery, seal)) == before
    assert len(json.dumps(proof).encode()) < 16 * 1024


def test_progress_refuses_changed_native_launcher_monitor_and_time(progress):
    store, spec, row, attempts, recovery, seal, files = progress
    for name, mutate in (
        ("request.json", lambda v: v.update(workspace="/foreign/checkout")),
        ("request.json", lambda v: v.update(native_authorized=False)),
        ("request.json", lambda v: v["spec"].update(policy_digest="e" * 64)),
        ("native-process.json", lambda v: v.update(monitoring_complete=False)),
        ("native-process.json", lambda v: v["provider_session"].update(resumed_from=None)),
        ("launch.json", lambda v: v["environment"].update(CODEX_HOME="/foreign/home")),
    ):
        path = files[("implement", 3, name)]
        raw = path.read_bytes()
        value = json.loads(raw)
        mutate(value)
        write_private(path, value)
        with pytest.raises(ValueError):
            subject.retained_worker_subject(store, spec, row, attempts, recovery)
        path.write_bytes(raw)
    for timestamp in ("2026-10-10T11:59:00+00:00", "2026-10-10T12:01:00"):
        old = attempts[-1]["started_at"]
        attempts[-1]["started_at"] = timestamp
        with pytest.raises(ValueError):
            subject.retained_worker_subject(store, spec, row, attempts, recovery)
        attempts[-1]["started_at"] = old


def test_progress_refuses_omission_reordered_prefix_policy_and_unpassed_weakening(progress):
    store, spec, row, attempts, recovery, seal, _ = progress
    original = deepcopy(seal)
    for mutate in (
        lambda: seal["state"]["roles"].pop(1),
        lambda: seal["state"]["roles"][0].update(status="findings"),
        lambda: seal["candidate"].update(policy_digest="f" * 64),
        lambda: seal["state"]["roles"][-1].update(status="blocked"),
        lambda: seal["state"]["roles"][-1].update(finish_reason="prelaunch"),
        lambda: seal["state"]["roles"][-1].update(findings=["x" * (16 * 1024 + 1)]),
    ):
        mutate()
        with pytest.raises(ValueError):
            subject.retained_worker_subject(store, spec, row, attempts, recovery)
        seal.clear()
        seal.update(deepcopy(original))


def test_progress_authenticates_compact_admission_and_distinct_full_history(progress):
    from devflow_temporal.delivery_feature_revision_recovery import compact_recovery

    store, spec, row, attempts, recovery, seal, _ = progress
    # Storage representation only: the public test separately authenticates real
    # adoption/derivation. This older review was omitted from the workflow seed,
    # while its exact full predecessor history remains in the private custody file.
    recovery["state"]["roles"].append({"role": "review", "iteration": 1, "status": "pass"})
    recovery["predecessor_result_digest"] = digest(recovery["state"])
    recovery["feature_plan_revision"] = None
    root = Path(spec["state_dir"]) / stopped_resume.namespace(recovery)
    raw = compact_recovery(recovery, root)
    assert raw["state"]["roles"] != recovery["state"]["roles"]
    write_private(root / "admission.json", raw)
    with store._connect() as db:
        db.execute("PRAGMA query_only=OFF")
        response = json.loads(db.execute(
            "SELECT response_json FROM delivery_commands").fetchone()[0])
        response["admission_sha256"] = digest(raw)
        db.execute("UPDATE delivery_commands SET response_json=?", (json.dumps(response),))
        db.commit()
        db.execute("PRAGMA query_only=ON")
    seal["original_recovery"] = raw
    seal["closed"]["recovery_digest"] = digest(raw)
    proof = subject.retained_worker_subject(store, spec, row, attempts, recovery)
    proof = proof["progress_custody"]
    assert proof["admission_sha256"] == digest(raw) != digest(recovery)
    assert proof["admission_custody_sha256"] == digest(recovery)
    assert proof["prefix_sha256"] == digest(raw["state"]["roles"])
    before = digest((raw, recovery, row, attempts))
    for mutate in (
        lambda: raw["custody_ref"].update(sha256="f" * 64),
        lambda: raw["state"]["roles"][0].update(status="findings"),
        lambda: seal["closed"].update(recovery_digest=digest(recovery)),
        lambda: recovery["state"]["roles"][-1].update(status="findings"),
    ):
        original_raw, original_full = deepcopy(raw), deepcopy(recovery)
        closed = deepcopy(seal["closed"])
        mutate()
        with pytest.raises(ValueError):
            subject.retained_worker_subject(store, spec, row, attempts, recovery)
        raw.clear()
        raw.update(original_raw)
        recovery.clear()
        recovery.update(original_full)
        seal["closed"] = closed
    assert digest((raw, recovery, row, attempts)) == before


def test_readonly_snapshot_reuses_full_selector_and_refuses_index_scope(stopped, monkeypatch):
    store, broker, _, _ = stopped
    before = stopped_resume.snapshot(store, broker.spec["run_id"])
    monkeypatch.setattr(stopped_resume, "private_directory", lambda *_: pytest.fail("mkdir"))
    assert stopped_resume.review_snapshot(store, broker.spec["run_id"]) == before
    (broker.checkout / "foreign.md").write_text("Unowned staged source\n")
    from test_delivery_store import _git
    _git(broker.checkout, "add", "foreign.md")
    with pytest.raises(ValueError, match="outside allowed paths"):
        stopped_resume.review_snapshot(store, broker.spec["run_id"])


def test_native_cleanup_observer_never_creates_missing_roots(tmp_path, monkeypatch):
    state = tmp_path / "native-stopped"
    state.mkdir(mode=0o700)
    spec = {"run_id": state.name, "state_dir": str(state)}
    with monkeypatch.context() as observation:
        observation.setattr("devflow_temporal.delivery_resources.private_directory",
                            lambda *_: pytest.fail("readonly observer attempted mkdir"))
        with pytest.raises(FileNotFoundError):
            policy_recovery._stopped_cleanup(spec, read_only=True)
    assert not (state / "resources").exists()
    root = state / "resources"
    root.mkdir(mode=0o700)
    write_private(root / "manifest.json", {"processes": []})
    write_private(root / "finalization.json", {"state": "confirmed", "process_cleanup":
                  "observed-native-confirmed", "resource_cleanup": "confirmed", "roots": []})
    monkeypatch.setattr("devflow_temporal.delivery_resources.private_directory",
                        lambda *_: pytest.fail("readonly observer attempted mkdir"))
    monkeypatch.setattr(policy_recovery, "process_table", lambda: {})
    before = {path: path.read_bytes() for path in root.iterdir()}
    assert policy_recovery._stopped_cleanup(spec, read_only=True)["journal_sha256"] == {}
    assert {path: path.read_bytes() for path in root.iterdir()} == before
    assert not (root / "ownership.lock").exists()


def test_registry_observer_reads_actual_rollback_database_and_refuses_wal_or_unsafe_files(
    tmp_path, monkeypatch,
):
    from devflow_temporal.delivery_execution_registry import ExecutionRegistry

    shared = ExecutionRegistry(tmp_path / "ownership" / "registry.sqlite3")
    issue = {"id": "original-issue", "repository_id": "repository", "url": "https://issue"}
    token = shared.claim({"issue": issue}, "original", str(tmp_path / "store"))
    shared.checkpoint(token, "accepted-plan", {"digest": "original-plan"})
    before = {p: p.read_bytes() for p in shared.path.parent.iterdir() if p.is_file()}
    real_open = os.open

    def readonly_open(path, flags, *args, **kwargs):
        assert not flags & (os.O_CREAT | os.O_RDWR | os.O_WRONLY | os.O_TRUNC)
        return real_open(path, flags, *args, **kwargs)

    view = subject._ReadOnlyRegistry(shared.path)
    with monkeypatch.context() as readonly:
        readonly.setattr(os, "open", readonly_open)
        readonly.setattr(ExecutionRegistry, "__init__", lambda *_: pytest.fail("schema creation"))
        assert view.current(issue["id"])["run_id"] == "original"
        assert view.checkpoints(issue["id"]) == {"accepted-plan": {"digest": "original-plan"}}
        with view.connect() as db, pytest.raises(sqlite3.OperationalError):
            db.execute("DELETE FROM execution_claims")
    assert {p: p.read_bytes() for p in shared.path.parent.iterdir() if p.is_file()} == before
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(str(shared.path) + suffix)
        sidecar.write_bytes(b"Unsettled original observation")
        with pytest.raises(ValueError, match="settled rollback"):
            view.current(issue["id"])
        sidecar.unlink()
    shared.path.chmod(0o644)
    with pytest.raises(ValueError, match="private owned file"):
        view.current(issue["id"])
    shared.path.chmod(0o600)
    # Closing the last real WAL writer removes sidecars but retains the WAL header.
    with sqlite3.connect(shared.path) as db:
        assert db.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    db.close()
    assert not Path(str(shared.path) + "-wal").exists()
    assert not Path(str(shared.path) + "-shm").exists()
    with monkeypatch.context() as unopened:
        unopened.setattr(sqlite3, "connect", lambda *_a, **_k: pytest.fail("opened WAL"))
        with pytest.raises(ValueError, match="settled rollback"):
            view.current(issue["id"])
    assert not Path(str(shared.path) + "-wal").exists()
    assert not Path(str(shared.path) + "-shm").exists()
    with pytest.raises(FileNotFoundError):
        subject._ReadOnlyRegistry(shared.path.parent / "missing.sqlite3").current(issue["id"])


def test_observer_uses_canonical_parent_identity_without_reconcile_or_registry_constructor(
    tmp_path, monkeypatch,
):
    from test_delivery_github_plan_records import legacy_publication, setup

    from devflow_temporal.delivery_execution_registry import ExecutionRegistry
    from devflow_temporal.delivery_github_contract import GitHubDelivery

    remote, issue, shared, token = setup(tmp_path)
    record = legacy_publication(remote, issue, shared, token)
    shared.checkpoint(token, "accepted-plan", {"digest": digest(record["manifest"]["plan"])})
    shared.checkpoint(token, "github-record", {key: record[key] for key in (
        "comment_id", "comment_node_id")})
    shared.checkpoint(token, "workstream-issues", record["manifest"]["workstream_issues"])
    child = record["manifest"]["workstream_issues"]["api"]
    spec = {"run_id": "state", "provider": "fake", "source_path": str(tmp_path / "source"),
            "checkout": str(tmp_path / "checkout"), "state_dir": str(tmp_path / "state"),
            "github_repo": issue["repository"], "issue_url": child["url"],
            "accepted_plan": "The worker chunk, not the full parent plan.",
            "branch": "feat/original", "publication_base_ref": "main",
            "feature_worker": {"kind": "chunk", "chunk_id": "model", "workstream_id": "api"},
            "feature_delivery": {"registry": str(shared.path), "owner": token,
                                 "snapshot": {"issue": issue, "delivery": None}}}
    store = SimpleNamespace(config=SimpleNamespace(tracking_db=Path(token["store_path"])))
    member = record["manifest"]["publication"]["members"][0]
    pr = {"number": member["number"], "url": member["url"], "state": "OPEN", "isDraft": False,
          "baseRefName": "main", "headRefName": member["branch"], "headRefOid": member["head"],
          "title": "feat: preserve retained custody"}
    original_api = remote.api

    def api(path, *, method="GET", body=None):
        assert method == "GET"
        if path.endswith("/pulls/1076"):
            return {"number": pr["number"], "html_url": pr["url"], "merged": False,
                    "state": "open", "draft": pr["isDraft"],
                    "head": {"ref": pr["headRefName"], "sha": pr["headRefOid"],
                             "repo": {"full_name": issue["repository"]}},
                    "base": {"ref": pr["baseRefName"],
                             "repo": {"full_name": issue["repository"]}}}
        return original_api(path, method=method, body=body)

    remote.api = api
    monkeypatch.setattr(subject, "_run", lambda argv, **_k: json.dumps(pr))
    monkeypatch.setattr("devflow_temporal.delivery_github_contract.GitHubDelivery", lambda: remote)
    monkeypatch.setattr(GitHubDelivery, "reconcile_record", lambda *_: pytest.fail("reconcile"))
    before = shared.path.read_bytes()
    with monkeypatch.context() as readonly:
        readonly.setattr(ExecutionRegistry, "__init__", lambda *_: pytest.fail("registry ctor"))
        assert subject.ObservationBroker(store, spec)._record() == record
        assert subject.ObservationBroker(store, spec)._existing_pr() == pr
        for field, value in (("baseRefName", "foreign"), ("title", "unconventional"),
                             ("headRefName", "foreign"), ("headRefOid", "f" * 40)):
            original = pr[field]
            pr[field] = value
            with pytest.raises((ValueError, RuntimeError)):
                subject.ObservationBroker(store, spec)._existing_pr()
            pr[field] = original
    assert shared.path.read_bytes() == before
    original = remote.comments[record["comment_id"]]["body"]
    remote.comments[record["comment_id"]]["body"] = original.replace("model.py", "foreign.py")
    with pytest.raises(ValueError):
        subject.ObservationBroker(store, spec)._record()
    remote.comments[record["comment_id"]]["body"] = original
    with shared.connect() as db:
        db.execute("UPDATE execution_claims SET store_path='foreign'")
    with pytest.raises(OwnershipConflict, match="canonical feature ownership"):
        subject.ObservationBroker(store, spec)._record()


def test_equal_candidate_build_observer_preserves_completion_and_initial_stopped_worker(
    stopped, monkeypatch,
):
    store, broker, state, _ = stopped
    worker = {**broker.spec, "feature_worker": {"kind": "build", "chunk_id": "model"}}
    monkeypatch.setattr(store, "effective_spec", lambda _: worker)
    # This fixture's immutable Temporal result is supplied by its closure seam;
    # the production scope/session/branch/origin/current-candidate selector runs.
    initial = stopped_resume.review_snapshot(store, worker["run_id"])
    proof = subject.retained_worker_subject(store, worker, initial["row"],
                                            initial["attempts"], None)
    assert proof["candidate"] == state["candidate"]
    assert proof["progress_custody"] is None and proof["current_review_subject"] is None
    state.update(phase="delivered", outcome="delivered", execution_state="terminal")
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET phase='delivered',outcome='delivered',"
                   "execution_state='terminal' WHERE run_id=?", (worker["run_id"],))
        commands_before = db.execute("SELECT count(*) FROM delivery_commands").fetchone()[0]
    delivered = stopped_resume.review_snapshot(store, worker["run_id"])
    proof = subject.retained_worker_subject(store, worker, delivered["row"],
                                            delivered["attempts"], None)
    assert proof["candidate"] == state["candidate"] and proof["progress_custody"] is None
    with pytest.raises(ValueError, match="closed finalized"):
        stopped_resume.snapshot(store, worker["run_id"])
    with store._connect() as db:
        assert db.execute("SELECT count(*) FROM delivery_commands").fetchone()[0] == commands_before


def test_publication_observer_requires_existing_target_objects_without_fetch(service, monkeypatch):
    from devflow_temporal import delivery_broker
    from devflow_temporal.delivery_broker import DeliveryBroker

    store, request = service
    store.submit(request)
    broker = DeliveryBroker(store, store.spec(request["run_id"]))
    broker.prepare()
    found = {"number": 9, "url": "https://github.com/example/fixture/pull/9", "state": "OPEN",
             "isDraft": False, "baseRefName": "main", "headRefName": broker.spec["branch"],
             "headRefOid": broker.spec["base_sha"]}
    with store._connect() as db:
        db.execute("INSERT INTO delivery_effects(effect_key,run_id,kind,request_json,state,"
                   "observed_json,updated_at) VALUES (?,?,'publish','{}','complete',?,'now')",
                   ("original-published", request["run_id"], json.dumps({**found,
                    "head": found["headRefOid"], "base": broker.spec["base_sha"]})))
    tip = broker.spec["base_sha"]
    retargeted = False
    fetched = []
    real_run = delivery_broker._run

    def observe(argv, **kwargs):
        if argv[:3] == ["gh", "api", "graphql"]:
            return json.dumps({"data": {"repository": {"pullRequest": {**found,
                "baseRef": {"name": "main", "target": {"oid": tip}}, "timelineItems": {
                    "totalCount": 1, "pageInfo": {"hasNextPage": False}, "nodes": [{
                        "__typename": "BaseRefChangedEvent" if retargeted else
                        "PullRequestCommit"}]}}}}})
        if "fetch" in argv:
            fetched.append(argv)
            raise RuntimeError("fixture refuses the ordinary object's fetch effect")
        return real_run(argv, **kwargs)

    monkeypatch.setattr(delivery_broker, "_run", observe)
    readonly = subject.ObservationBroker(store, broker.spec)
    objects = broker.source / ".git" / "objects"
    before = {p.relative_to(objects): p.read_bytes() for p in objects.rglob("*") if p.is_file()}
    assert readonly._legacy_published_base(found) == "main"
    tip = "f" * 40  # An absent object is not fetched by the observer.
    with pytest.raises(ValueError, match="existing live object"):
        readonly._legacy_published_base(found)
    assert not fetched
    assert {p.relative_to(objects): p.read_bytes() for p in objects.rglob("*")
            if p.is_file()} == before
    # Retain the original default behavior without executing its fetch in this test.
    with pytest.raises(ValueError, match="ancestry unavailable"):
        broker._legacy_published_base(found)
    assert len(fetched) == 1 and "--no-write-fetch-head" in fetched[0]
    retargeted = True
    with pytest.raises(ValueError, match="history changed"):
        readonly._legacy_published_base(found)
    assert len(fetched) == 1


def test_approved_revision_snapshot_reads_exact_canonical_record_and_keeps_old_worker_fenced(
    feature, monkeypatch,
):
    from test_delivery_feature_revisions import passing, proposed
    from test_delivery_store import _git

    from devflow_temporal import delivery_feature_revision_recovery as recovery_module
    from devflow_temporal import delivery_feature_revisions as revisions
    from devflow_temporal.delivery_broker import DeliveryBroker
    from devflow_temporal.delivery_execution_registry import ExecutionRegistry
    from devflow_temporal.delivery_feature_execution import register_worker, worker_key, worker_spec
    from devflow_temporal.delivery_github_contract import (
        LABEL_PREFIX,
        GitHubDelivery,
        encode_manifest,
    )
    from devflow_temporal.delivery_github_plans import _child_plan_body
    from devflow_temporal.delivery_plan_model import ordered_chunks

    store, parent, shared, record, diagnostic = feature
    issue = parent["feature_delivery"]["snapshot"]["issue"]
    owner = parent["feature_delivery"]["owner"]
    child_issue = record["manifest"]["workstream_issues"]["api"]
    child = worker_spec(parent, ordered_chunks(record["manifest"]["plan"])[0], child_issue,
                        kind="chunk", base_sha=parent["base_sha"], base_branch="main")
    register_worker(store, parent, child)
    broker = DeliveryBroker(store, child)
    broker.prepare()
    (broker.checkout / "README.md").write_text("Published original retained source\n")
    _git(broker.checkout, "add", "README.md")
    _git(broker.checkout, "commit", "-qm", "Retained source")
    _git(broker.checkout, "push", "origin", "HEAD:refs/heads/" + child["branch"])
    current = broker.candidate()
    member = {"chunk_id": "model", "number": 1076,
              "url": "https://github.com/example/fixture/pull/1076", "branch": child["branch"],
              "head": current["head"], "base_branch": "main"}
    record["manifest"]["publication"]["members"] = [member]
    record["manifest"].update(issue_id=issue["id"], repository_id=issue["repository_id"],
                              creation_key="fixture-original-plan")
    implementation = {"role": "implement", "iteration": 1, "status": "pass",
                      "candidate": current, "session_id": "original", "cleanup": "confirmed"}
    pr = {"number": 1076, "url": member["url"], "head": current["head"],
          "base": child["base_sha"], "state": "OPEN"}
    state = {"run_id": child["run_id"], "revision": 9, "iteration": 1, "phase": "blocked",
             "outcome": "blocked", "execution_state": "blocked", "cleanup": "confirmed",
             "candidate": current, "checks": {}, "roles": [implementation], "pull_request": pr,
             "error": "repair limit exhausted"}
    store.project(child["run_id"], phase="blocked", execution_state="blocked", event_type="blocked",
                  message=state["error"], candidate=current, checks={}, pull_request=pr,
                  iteration=1, protocol_revision=9, outcome="blocked", cleanup="confirmed",
                  error=state["error"])
    with store._connect() as db:
        store.state.release_work(db, child["work_id"], "external:devflow:" + child["run_id"])
    shared.finish_worker(owner, worker_key(child["run_id"], owner), {"cleanup": "confirmed"})
    closed = {"workflow_id": "delivery-" + child["run_id"], "execution_run_id": "closed-original",
              "result": state, "request_digest": child["request_digest"], "recovery_digest": None}
    # Only the closed Temporal transport and remote GitHub reads are simulated.
    # The complete snapshot, adopted custody, broker/Git and private DB reads run.
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_a, **_k: closed)
    context = revisions.begin_revision(store, parent, diagnostic)
    value = proposed(record)
    revisions.record_proposal(store, parent, context["revision_id"], value)

    class CanonicalRemote(GitHubDelivery):
        def __init__(self):
            self.comments = {}

        def publish_plan_revision(self, _issue, old, plan, *_a, **_k):
            result = deepcopy(old)
            manifest = result["manifest"]
            manifest.update(version=2, revision=2, plan_revision=2,
                            plan_digest=digest(plan), plan=plan, workstream_plans={})
            manifest["publication"]["members"][0].update(
                workstream_id="api", issue_id=child_issue["id"],
                issue_number=child_issue["number"], issue_url=child_issue["url"])
            for number, stream in enumerate(plan["workstreams"], 100):
                binding = manifest["workstream_issues"][stream["id"]]
                _, body = _child_plan_body(issue, binding, stream, plan, 2, context["revision_id"])
                self.comments[number] = {"id": number, "node_id": "IC_" + str(number), "body": body,
                    "issue_url": f"https://api.github.com/repos/{issue['repository']}/issues/{binding['number']}"}
                manifest["workstream_plans"][stream["id"]] = {"comment_id": number,
                    "comment_node_id": "IC_" + str(number), "digest": digest(body),
                    "url": binding["url"] + "#issuecomment-" + str(number)}
            self.comments[old["comment_id"]] = {
                "id": old["comment_id"], "node_id": old["comment_node_id"],
                "body": encode_manifest(manifest),
                "issue_url": f"https://api.github.com/repos/{issue['repository']}/issues/{issue['number']}"}
            return result

        def issue(self, _repo, number, _repository_id):
            if number == issue["number"]:
                return {**issue, "labels": [LABEL_PREFIX + str(record["comment_id"])]}
            binding = next(binding for binding in record["manifest"]["workstream_issues"].values()
                           if binding["number"] == number)
            return {**issue, **binding, "labels": []}

        def api(self, endpoint, *, method="GET", body=None):
            assert method == "GET"
            if "/issues/comments/" in endpoint:
                return deepcopy(self.comments[int(endpoint.rsplit("/", 1)[1])])
            if endpoint.endswith("/parent"):
                return {"node_id": issue["id"]}
            assert endpoint.endswith("/pulls/1076")
            return {"number": 1076, "html_url": pr["url"], "merged": False, "state": "open",
                    "draft": False, "head": {"sha": current["head"], "ref": child["branch"],
                    "repo": {"full_name": child["github_repo"]}}, "base": {"ref": "main",
                    "repo": {"full_name": child["github_repo"]}}}

    remote = CanonicalRemote()
    adopted = revisions.adopt_revision(store, parent, context["revision_id"], value,
                                       passing(context, value), gh=remote)
    receipt = shared.checkpoints(owner["issue_id"])[revisions.PREFIX + "adopted:2"]
    derived = recovery_module._derive(adopted["spec"], receipt, child)
    admission = {"revision_id": context["revision_id"],
                 "plan_identity": derived["feature_plan_revision"],
                 "parent_run_id": parent["run_id"], "predecessor_spec_digest": digest(child),
                 "derived_spec_digest": digest(derived)}
    monkeypatch.setattr("devflow_temporal.delivery_feature_publication.current_record",
                        original_current_record)
    monkeypatch.setattr("devflow_temporal.delivery_feature_publication.live_members",
                        original_live_members)
    monkeypatch.setattr("devflow_temporal.delivery_github_contract.GitHubDelivery", lambda: remote)
    monkeypatch.setattr(subject, "_run", lambda *_a, **_k: json.dumps({
        "number": 1076, "url": pr["url"], "state": "OPEN", "isDraft": False,
        "baseRefName": "main", "headRefName": child["branch"], "headRefOid": current["head"],
        "title": "feat: retained source"}))
    before = shared.path.read_bytes()
    original_constructor = ExecutionRegistry.__init__

    def no_observer_constructor(self, *args, **kwargs):
        assert not any(frame.function == "_record" for frame in inspect.stack())
        return original_constructor(self, *args, **kwargs)

    with monkeypatch.context() as readonly:
        # Original adopted-custody validation still uses its existing registry.
        # The publication observer itself must never initialize/reconcile it.
        readonly.setattr(ExecutionRegistry, "__init__", no_observer_constructor)
        readonly.setattr(GitHubDelivery, "reconcile_record", lambda *_: pytest.fail("reconcile"))
        observed = stopped_resume.snapshot(store, child["run_id"], _revision=admission)
        assert observed["candidate"] == current and observed["state"] == state
    assert shared.path.read_bytes() == before
    with pytest.raises(OwnershipConflict, match="superseded plan revision"):
        stopped_resume.snapshot(store, child["run_id"])
    with pytest.raises(OwnershipConflict):
        stopped_resume.snapshot(store, child["run_id"],
                                _revision={**admission, "revision_id": "foreign"})
    original_comment = remote.comments.pop(record["comment_id"])
    with pytest.raises(KeyError):
        stopped_resume.snapshot(store, child["run_id"], _revision=admission)
    remote.comments[record["comment_id"]] = original_comment
    with store._connect() as db:
        db.execute("INSERT INTO delivery_effects "
                   "(effect_key,run_id,kind,state,request_json,updated_at) "
                   "VALUES ('unsettled',?,'publish','pending','{}','2026-10-11')",
                   (child["run_id"],))
    with pytest.raises(ValueError, match="closed finalized"):
        stopped_resume.snapshot(store, child["run_id"], _revision=admission)
    with store._connect() as db:
        db.execute("DELETE FROM delivery_effects WHERE effect_key='unsettled'")
    raw_config = Path(child["config_path"]).read_bytes()
    snapshot = store.config.state_root / "configuration-snapshots" / (
        child["config_digest"] + ".json")
    frozen_config = snapshot.read_bytes()
    changed_config = json.dumps({**store.config.raw, "max_repairs": 9})
    Path(child["config_path"]).write_text(changed_config)
    observed = stopped_resume.snapshot(store, child["run_id"], _revision=admission)
    assert observed["candidate"] == current and observed["state"] == state
    assert snapshot.read_bytes() == frozen_config
    snapshot.write_text(changed_config)
    with pytest.raises(ValueError, match="configuration.*changed"):
        stopped_resume.snapshot(store, child["run_id"], _revision=admission)
    snapshot.write_bytes(frozen_config)
    Path(child["config_path"]).write_bytes(raw_config)
    with shared.connect() as db:
        db.execute("UPDATE execution_claims SET store_path='foreign'")
    with pytest.raises(OwnershipConflict, match="canonical feature ownership"):
        stopped_resume.snapshot(store, child["run_id"], _revision=admission)


@pytest.fixture
def findings_reconsideration(native_configuration, tmp_path, monkeypatch, request):
    from test_delivery_feature_revisions import _genuine_fixture

    from devflow_temporal.contracts import canonical_json
    from devflow_temporal.delivery_broker import DeliveryBroker

    fixture, command, _, _, _, runtime = _genuine_fixture(
        native_configuration, monkeypatch, tmp_path, retain_kind="chunk",
        prior_cycles=getattr(request, "param", 5))
    store, parent, shared, _, _, _, context, _, _, _ = fixture
    assignment = shared.checkpoints(context["owner"]["issue_id"])["assignment:model:chunk"]
    child = store.effective_spec(assignment["run_id"])
    broker = DeliveryBroker(store, child)
    admission = broker.candidate()
    write_private(Path(child["state_dir"]) / "resources" / "finalization.json", {
        "state": "confirmed", "process_cleanup": "observed-native-confirmed",
        "resource_cleanup": "confirmed", "roots": []})
    (broker.checkout / "README.md").write_text("Previously passed product checkpoint\n")
    terminal = broker.candidate()
    (broker.checkout / "README.md").write_text("Current unpassed product findings\n")
    current = broker.candidate()
    generation = _progress_fixture(
        tmp_path, monkeypatch, spec=child, runtime=runtime, owner_store=store,
        candidates=(admission, terminal, current), last_iteration=4,
        session="retained-original-session")
    _, _, _, native_attempts, recovery, seal, files = next(generation)
    state = seal["state"]
    state.update(phase="blocked", outcome="blocked", cleanup="confirmed", execution_state="blocked",
                 error="implementer did not establish a pass", checks={}, pull_request=None,
                 findings=["Original unresolved product finding"], usage={}, candidate_revision=2)
    store.project(child["run_id"], phase="blocked", execution_state="blocked", event_type="blocked",
                  message=state["error"], candidate=terminal, checks={}, iteration=4,
                  protocol_revision=17, outcome="blocked", cleanup="confirmed",
                  error=state["error"])
    with store._connect() as db:
        for attempt in native_attempts:
            columns = ",".join(attempt)
            db.execute("INSERT INTO delivery_attempts (" + columns + ") VALUES ("
                       + ",".join("?" for _ in attempt) + ")", tuple(attempt.values()))
        db.execute("UPDATE delivery_runs SET recovery_json=?,workflow_id=? WHERE run_id=?",
                   (canonical_json(recovery), seal["closed"]["workflow_id"], child["run_id"]))
        store.state.release_work(db, child["work_id"], "external:devflow:" + child["run_id"])
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                              (child["run_id"],)).fetchone())
        attempts = [dict(a) for a in db.execute("SELECT * FROM delivery_attempts WHERE run_id=? "
                                              "ORDER BY job_key", (child["run_id"],))]
        effects = [dict(e) for e in db.execute("SELECT * FROM delivery_effects WHERE run_id=? "
                                             "ORDER BY effect_key", (child["run_id"],))]
        binding = stopped_resume.work_binding(store, child, db)
    seal.update(row=row, attempts=attempts, effects=effects, work_binding=binding)

    def observe(_store, run_id):
        assert run_id == child["run_id"]
        selected, session = stopped_resume._candidate(broker, state, attempts)
        assert selected == current and session == "retained-original-session"
        return deepcopy({**seal, "candidate": selected, "session_id": session})

    # The current closure/session/resource/publication observations are isolated
    # metadata, not live eligibility. The real selector and complete dynamic native
    # progress/private admission proofs run against this fixture's original inputs.
    monkeypatch.setattr(stopped_resume, "review_snapshot", observe)
    yield fixture, command, runtime, child, seal, observe, files
    generation.close()


@pytest.mark.skipif(sys.platform != "darwin", reason="native configuration preparation required")
@pytest.mark.parametrize("findings_reconsideration", [5, 8], indirect=True,
                         ids=["aggregate-findings-bound", "planning-and-writer-budget"])
def test_public_unpassed_context_refuses_before_owner_or_debit(findings_reconsideration):
    from test_delivery_feature_revisions import _boundary_snapshot

    from devflow_temporal import delivery_feature_revisions as revisions

    fixture, command, _, child, seal, _, files = findings_reconsideration
    store, parent, shared, _, _, _, prior, _, _, _ = fixture
    issue_id = prior["owner"]["issue_id"]
    budget = shared.budget(issue_id)["used"]
    if budget == 6:
        # Model a genuine bounded current native result with 32 product findings.
        # The one original independent plan finding makes the aggregate exceed
        # the contract; all raw/enriched/journal fields are still exactly bound.
        latest = seal["state"]["roles"][-1]
        latest["findings"] = ["Unresolved product finding " + str(n) for n in range(32)]
        result = {key: value for key, value in latest.items() if key not in subject.ROLE_FIELDS}
        raw_path = files[("implement", 4, "result.json")]
        raw = json.loads(raw_path.read_bytes())
        raw["findings"] = latest["findings"]
        write_private(raw_path, raw)
        journal_path = files[("implement", 4, "native-process.json")]
        journal = json.loads(journal_path.read_bytes())
        journal["provider_session"]["result_digest"] = digest(result)
        write_private(journal_path, journal)
        with store._connect() as db:
            db.execute("UPDATE delivery_attempts SET result_json=? "
                       "WHERE run_id=? AND role='implement' AND iteration=4",
                       (json.dumps(result), child["run_id"]))
            seal["attempts"][:] = [dict(a) for a in db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=? ORDER BY job_key",
                (child["run_id"],))]
        reason = "exceed bounded fresh review context"
    else:
        assert budget == 9
        reason = "lacks the ordinary planning and repair budget"
    old = _boundary_snapshot(store, shared, issue_id)
    with pytest.raises(ValueError, match=reason):
        revisions.revise_feature_plan(store, parent["run_id"], command)
    assert _boundary_snapshot(store, shared, issue_id) == old
    assert shared.budget(issue_id)["used"] == budget


def _exercise_public_findings_review_and_original_worker_handoff(
    findings_reconsideration, monkeypatch,
):
    from test_delivery_feature_revisions import (
        _boundary_snapshot,
        _complete_native_role_fixture,
        _revision_diagnostic_for,
    )

    from devflow_temporal import delivery_activities as activities
    from devflow_temporal import delivery_feature_revision_recovery as recovery_module
    from devflow_temporal import delivery_feature_revisions as revisions
    from devflow_temporal.contracts import canonical_json
    from devflow_temporal.delivery_broker import DeliveryBroker
    from devflow_temporal.delivery_feature_execution import continue_feature, require_execution
    from devflow_temporal.delivery_preparation import prepare_authority
    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    fixture, command, runtime, child, seal, observe, files = findings_reconsideration
    store, original, shared, plan, diagnostic, _, prior, _, rejection, parent_row = fixture
    old = _boundary_snapshot(store, shared, prior["owner"]["issue_id"])
    latest_request = files[("implement", 4, "request.json")]
    raw = latest_request.read_bytes()
    latest_request.unlink()
    with pytest.raises(OwnershipConflict, match="missing or unsupported"):
        revisions.revise_feature_plan(store, original["run_id"], command)
    latest_request.write_bytes(raw)
    latest_request.chmod(0o600)
    assert _boundary_snapshot(store, shared, prior["owner"]["issue_id"]) == old
    before_receipts = shared.checkpoints(prior["owner"]["issue_id"])

    def lost_response(*args, **kwargs):
        continue_feature(*args, **kwargs)
        raise ConnectionError("successor owner/outbox durable; outer response lost")

    monkeypatch.setattr("devflow_temporal.delivery_feature_execution.continue_feature",
                        lost_response)
    with pytest.raises(ConnectionError, match="outer response lost"):
        revisions.revise_feature_plan(store, original["run_id"], command)
    monkeypatch.setattr("devflow_temporal.delivery_feature_execution.continue_feature",
                        continue_feature)
    response = revisions.revise_feature_plan(store, original["run_id"], command)
    assert revisions.revise_feature_plan(store, original["run_id"], command) == response
    assert response["repair_budget"]["used"] == 6
    summary = response["reconsideration"]["worker_summaries"][0]["current_review_subject"]
    assert summary["status"] == "findings" and summary["candidate"] == seal["candidate"]
    assert summary["candidate"] != seal["state"]["candidate"]
    assert summary["candidate"] != seal["original_recovery"]["execution_candidate"]
    assert summary["unresolved_product_findings"] == ["Original unresolved product finding"]
    with pytest.raises(OwnershipConflict):
        revisions.revise_feature_plan(store, original["run_id"],
                                     {**command, "command_id": "same-context-new-command"})
    successor = prepare_authority(store, store.effective_spec(response["run_id"]))
    broker = DeliveryBroker(store, successor)
    broker.prepare()
    context = revisions.begin_revision(store, successor)
    assert context["budget"]["used"] == 7
    assert revisions.begin_revision(store, successor) == context
    assert context["unresolved_prior_findings"] != summary["unresolved_product_findings"]
    diagnostic = _revision_diagnostic_for(successor, diagnostic, broker.candidate())
    completed = {"dirty": False, "local_digest": None, **{key: runtime[key] for key in (
        "revision", "runtime_payload_sha256")}}
    intake = _complete_native_role_fixture(
        store, successor, context, plan, diagnostic, "intake", completed)
    proposal = revisions.record_proposal(store, successor, context["revision_id"], plan,
                                         diagnostic=diagnostic)
    review = _complete_native_role_fixture(
        store, successor, context, plan, diagnostic, "review", completed, status="pass")

    class Publisher:
        def publish_plan_revision(self, _issue, record, value, *_a, **_k):
            result = deepcopy(record)
            result["manifest"].update(version=2, revision=2, plan_revision=2, plan=value)
            return result

    adopted = revisions.adopt_revision(store, successor, context["revision_id"], plan, {
        **review, "candidate_id": context["candidate_id"],
        "proposal_session_id": intake["session_id"]}, gh=Publisher())
    assert adopted["budget"]["used"] == 7 and proposal["proposal_digest"] == digest(plan)
    assert recovery_module.gate_evidence(seal) is None
    adopted_receipt = shared.checkpoints(prior["owner"]["issue_id"])[
        revisions.PREFIX + "adopted:2"]
    assert adopted_receipt["diagnostic_worker_run_id"] is None
    # Native current metadata observations are the same explicit fixture seam;
    # ordinary adoption/custody/derivation, finite admission, command replay and
    # execution fences run without an adoption or policy-authority override.
    monkeypatch.setattr(stopped_resume, "snapshot", lambda s, run, **_kw: observe(s, run))
    # The dirty implementation checkout cannot be an installed native runtime.
    # This seam preserves the exact authenticated derivation and models only
    # executable preparation; actual native progress is qualified independently.
    monkeypatch.setattr(stopped_resume, "prepare_runtime", lambda spec, *_: deepcopy(spec))
    first = revisions.resume_revision_worker(store, adopted["spec"], child)
    assert first == revisions.resume_revision_worker(store, adopted["spec"], child)
    resumed = first["spec"]
    require_execution(store, resumed)
    with pytest.raises(OwnershipConflict):
        require_execution(store, child)
    with store._connect() as db:
        envelope = json.loads(db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id=?",
                                         (child["run_id"],)).fetchone()[0])
        assert dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                               (original["run_id"],)).fetchone()) == parent_row
    assert envelope["execution_candidate"]["id"] == seal["candidate"]["id"]
    assert envelope["state"]["candidate"] == seal["state"]["candidate"]
    assert envelope["session_id"] == "retained-original-session"
    assert envelope["command"]["expected_iteration"] == 4
    assert envelope["command"]["additional_iterations"] == 3
    assert "resume_stage" not in envelope and len(canonical_json(envelope).encode()) < 256 * 1024
    assert shared.budget(prior["owner"]["issue_id"])["used"] == 7
    values = shared.checkpoints(prior["owner"]["issue_id"])
    assert all(values[key] == value for key, value in before_receipts.items())
    assert values[revisions.PREFIX + "rejected:" + prior["revision_id"]] == rejection
    assert revisions.PREFIX + "repair-alias:" + child["run_id"] + ":5" not in values

    class FinishedFixtureRole:
        async def run(self, request):
            assert request["iteration"] == 5
            assert request["resume_session"] == envelope["session_id"]
            assert all(text in request["findings"]
                       for text in summary["unresolved_product_findings"])
            return {"status": "findings", "findings": request["findings"],
                    "session_id": request["resume_session"], "cleanup": "confirmed"}

    worker_broker = DeliveryBroker(store, resumed)
    monkeypatch.setattr(activities, "_context", lambda _spec: (store, worker_broker))
    monkeypatch.setattr(activities, "_role_context", lambda req, *_a: (
        req, worker_broker.checkout, None, None))
    monkeypatch.setattr(activities, "get_supervisor", lambda _: FinishedFixtureRole())
    controller = DeliveryWorkflow()

    async def preflight(*_args):
        return True

    async def activity(name, *_args, **_kwargs):
        assert name == "delivery_tracker_start"
        return {"state": "consistent"}

    async def project(*_args):
        pass

    async def run_iterations(spec, **kwargs):
        assert kwargs["start_iteration"] == 5 and kwargs["authorized_max_iteration"] == 7
        assert not kwargs["resume_prechecks"]
        assert controller.state["candidate"] == envelope["execution_candidate"]
        return await activities._run_role({
            "spec": spec, "role": "implement", "iteration": kwargs["start_iteration"],
            "candidate": controller.state["candidate"],
            "resume_session": kwargs["prior_implementer_session"],
            "findings": kwargs["repair_findings"]})

    controller._confirm_repair_preflight = preflight
    controller._activity = activity
    controller._project = project
    controller._run_iterations = run_iterations
    # Tracker/preflight and the finished supervisor are isolated seams. Actual
    # stopped-workflow candidate/session/iteration dispatch and the normal
    # activity debit/result/scope handling run; no provider starts.
    retained = digest(envelope)
    asyncio.run(controller._resume_stopped(resumed, envelope))
    asyncio.run(controller._resume_stopped(resumed, envelope))
    assert digest(envelope) == retained
    assert shared.budget(prior["owner"]["issue_id"])["used"] == 8
