"""Owned Git/state fixtures exercise the public preservation and restore contract."""

from __future__ import annotations

import json
import multiprocessing
import os
import shutil
import sqlite3
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_store import service as service
from test_native_interrupted_gate import native_gates as native_gates

from devflow_temporal.candidate import candidate_for
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_checkpoints import (
    create_checkpoint,
    read_checkpoint,
    restore_checkpoint,
)
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_filesystem_custody import matches
from devflow_temporal.delivery_resources import RunResources, read_private, write_private
from devflow_temporal.delivery_resources import _identity as checkpoints_identity
from devflow_temporal.delivery_store import DeliveryStore


def git(path, *args):
    return subprocess.check_output(["git", "-C", str(path), *args], text=True).strip()


def durable_fence(store, spec, epoch=1):
    from devflow_temporal.contracts import digest

    binding = {
        "schema": "owned-fixture-pause-v1",
        "root_id": spec["run_id"],
        "epoch": epoch,
        "workflow_id": "delivery-" + spec["run_id"],
        "execution_run_id": "owned-execution",
        "owner_generation": 1,
    }
    command = "owned-fixture-fence-" + str(epoch)
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_mutations "
            "(command_id,run_id,kind,request_digest,state,response_json) "
            "VALUES (?,?,'fixture-pause',?,'paused',?)",
            (command, spec["run_id"], digest(binding), json.dumps(binding)),
        )

    def fence(db, row):
        control = db.execute(
            "SELECT * FROM delivery_mutations WHERE command_id=?", (command,)
        ).fetchone()
        if (
            control["state"] != "paused"
            or control["request_digest"] != digest(binding)
            or row["run_id"] != spec["run_id"]
        ):
            raise ValueError("owned fixture pause fence changed")
        return binding

    return fence


@pytest.fixture
def stopped(service):
    store, request = service
    configuration = json.loads(store.config.path.read_text())
    configuration["workspace_custody_version"] = 1
    store.config.path.write_text(json.dumps(configuration))
    store = DeliveryStore(DeliveryConfig.load(store.config.path))
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    broker = DeliveryBroker(store, spec)
    checkout = Path(spec["checkout"])
    checkout.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    resources = RunResources(spec)
    resources.register(checkout, "checkout")
    broker.prepare()
    resources.created(checkout)
    (checkout / "README.md").write_text("staged\n")
    git(checkout, "add", "README.md")
    (checkout / "README.md").write_text("staged plus dirty\n")
    (checkout / "owned-untracked.txt").write_text("recover me\n")
    # Ignored private material must never enter a checkpoint.
    (checkout / ".gitignore").write_text("auth.json\n")
    (checkout / "auth.json").write_text("owned fixture credential, excluded\n")
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET execution_state='blocked',phase='blocked',"
            "outcome='blocked',usage_json=?,iteration=1 WHERE run_id=?",
            (json.dumps({"tokens": 419, "wall_seconds": 12}), request["run_id"]),
        )
        row = dict(
            db.execute(
                "SELECT * FROM delivery_runs WHERE run_id=?", (request["run_id"],)
            ).fetchone()
        )
    return store, spec, broker, row


def test_dirty_index_untracked_restore_after_source_and_checkout_deleted(stopped):
    store, spec, broker, row = stopped
    before = candidate_for(broker.checkout)
    staged = git(broker.checkout, "diff", "--cached")
    receipt = create_checkpoint(store, spec["run_id"], "save-1", row["revision"])
    assert receipt["candidate"] == before
    assert receipt["resumable"] is True
    assert create_checkpoint(store, spec["run_id"], "save-1", row["revision"]) == receipt
    checkpoint = Path(spec["state_dir"]) / "checkpoints" / receipt["checkpoint_id"]
    assert not list(checkpoint.rglob("auth.json"))
    manifest = read_private(checkpoint / "manifest.json")
    assert manifest["frozen"]["run"]["usage_json"] == row["usage_json"]
    original_resource = read_private(Path(spec["state_dir"]) / "resources/manifest.json")
    original_identity = original_resource["roots"][str(broker.checkout)]["identity"]
    shutil.rmtree(Path(spec["source_path"]))
    shutil.rmtree(broker.checkout)
    restored = restore_checkpoint(
        store, spec["run_id"], receipt["checkpoint_id"], "restore-1", row["revision"]
    )
    assert restored["execution_started"] is False
    assert restored["restored_identity"] != original_identity
    assert candidate_for(broker.checkout) == before
    assert git(broker.checkout, "diff", "--cached") == staged
    assert (broker.checkout / "owned-untracked.txt").read_text() == "recover me\n"
    assert not (broker.checkout / "auth.json").exists()
    assert matches(spec, broker.checkout, original_identity)
    assert read_private(Path(spec["state_dir"]) / "resources/manifest.json") == original_resource
    assert (
        restore_checkpoint(
            store, spec["run_id"], receipt["checkpoint_id"], "restore-1", row["revision"]
        )
        == restored
    )
    with store._connect() as db:
        current = dict(
            db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (spec["run_id"],)).fetchone()
        )
    assert current == row  # Preserves authorization, budgets, original workflow and run state.
    assert read_checkpoint(store, spec["run_id"], receipt["checkpoint_id"])["resumable"]


@pytest.mark.parametrize(
    "failure",
    ["artifact_tamper", "missing_artifact", "manifest_tamper", "unknown_effect", "active"],
)
def test_refuses_unknown_or_corrupt_preservation(stopped, failure):
    store, spec, broker, row = stopped
    if failure in {"unknown_effect", "active"}:
        with store._connect() as db:
            if failure == "unknown_effect":
                db.execute(
                    "INSERT INTO delivery_effects VALUES (?,?,?,?,'pending',NULL,'now')",
                    ("unresolved-publish", spec["run_id"], "publish", "{}"),
                )
            else:
                db.execute(
                    "UPDATE delivery_runs SET execution_state='running',outcome=NULL "
                    "WHERE run_id=?",
                    (spec["run_id"],),
                )
        with pytest.raises(ValueError, match="reconciliation|stopped run"):
            create_checkpoint(store, spec["run_id"], "save-1", row["revision"])
        assert not list((Path(spec["state_dir"]) / "checkpoints").glob("*/manifest.json"))
        return
    receipt = create_checkpoint(store, spec["run_id"], "save-1", row["revision"])
    checkpoint = Path(spec["state_dir"]) / "checkpoints" / receipt["checkpoint_id"]
    if failure == "manifest_tamper":
        target = checkpoint / "manifest.json"
        value = json.loads(target.read_text())
        value["resumable"] = False
        target.write_text(json.dumps(value))
    else:
        target = checkpoint / "source/owned-untracked.txt"
        if failure == "missing_artifact":
            target.unlink()
        else:
            target.write_text("changed")
    shutil.rmtree(broker.checkout)
    with pytest.raises(ValueError, match="corrupt|acknowledgement|missing"):
        restore_checkpoint(
            store, spec["run_id"], receipt["checkpoint_id"], "restore-1", row["revision"]
        )
    assert not broker.checkout.exists()


def test_partial_publication_is_inert_and_authority_budget_changes_refuse(stopped):
    store, spec, broker, row = stopped
    receipt = create_checkpoint(store, spec["run_id"], "save-1", row["revision"])
    with store._connect() as db:
        db.execute("DELETE FROM delivery_commands WHERE command_id='checkpoint-create:save-1'")
    with pytest.raises(ValueError, match="acknowledgement"):
        read_checkpoint(store, spec["run_id"], receipt["checkpoint_id"])
    with pytest.raises(ValueError, match="unacknowledged partial"):
        create_checkpoint(store, spec["run_id"], "save-1", row["revision"])
    second = create_checkpoint(store, spec["run_id"], "save-2", row["revision"])
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET usage_json=? WHERE run_id=?",
            ('{"tokens":420}', spec["run_id"]),
        )
    shutil.rmtree(broker.checkout)
    with pytest.raises(ValueError, match="budget or effects changed"):
        restore_checkpoint(
            store, spec["run_id"], second["checkpoint_id"], "restore-2", row["revision"]
        )
    assert not broker.checkout.exists()


def _crash_restore_allocation(store, spec, receipt, revision, boundary):
    from devflow_temporal import delivery_checkpoints as checkpoints

    if boundary in {"before_mkdir", "after_mkdir"}:
        original = os.mkdir
        def mkdir(path, *args, **kwargs):
            if Path(path).name.startswith(".checkpoint-restore-"):
                if boundary == "before_mkdir":
                    os._exit(77)
                original(path, *args, **kwargs)
                os._exit(77)
            return original(path, *args, **kwargs)
        os.mkdir = mkdir
    else:
        original = checkpoints._git
        def git_in_progress(path, *args, **kwargs):
            result = original(path, *args, **kwargs)
            if args[:2] == ("worktree", "add"):
                stage = Path(args[-2])
                lock = Path(original(stage, "rev-parse", "--git-path", "index.lock")
                            .decode().strip())
                lock.write_bytes(b"owned interrupted Git fixture")
                os._exit(77)
            return result
        checkpoints._git = git_in_progress
    restore_checkpoint(store, spec["run_id"], receipt["checkpoint_id"],
                       "public-restore-crash", revision)


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["before_mkdir", "after_mkdir", "git_active"])
async def test_public_restore_process_loss_reserves_and_preserves_unknown(stopped, boundary):
    store, spec, broker, row = stopped
    receipt = create_checkpoint(store, spec["run_id"], "save-public-crash", row["revision"])
    shutil.rmtree(broker.checkout)
    process = multiprocessing.get_context("fork").Process(
        target=_crash_restore_allocation, args=(store, spec, receipt, row["revision"], boundary))
    process.start()
    process.join(30)
    assert process.exitcode == 77
    with store._connect() as db:
        saved = db.execute("SELECT response_json FROM delivery_mutations WHERE command_id=?",
                           ("checkpoint-restore:public-restore-crash",)).fetchone()
    intent = json.loads(saved[0])
    stage = Path(intent["staging"])
    assert intent["phase"] == ("git_active" if boundary == "git_active" else "reserved")
    app = create_app(store.config.path)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(
            transport=transport, base_url=store.config.dashboard_url) as client:
        csrf = (await client.get("/api/session")).json()["csrf_token"]
        response = await client.post(
            f"/api/runs/{spec['run_id']}/checkpoints/{receipt['checkpoint_id']}/restore",
            json={"command_id": "public-restore-crash", "expected_revision": row["revision"]},
            headers={"origin": store.config.dashboard_url, "x-devflow-csrf": csrf},
        )
    if boundary == "before_mkdir":
        assert response.status_code == 200, response.text
        assert candidate_for(broker.checkout) == receipt["candidate"]
    else:
        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "checkpoint_restore_custody_unknown"
        assert stage.exists() and not broker.checkout.exists()
        if boundary == "git_active":
            lock = Path(git(stage, "rev-parse", "--git-path", "index.lock"))
            assert lock.read_bytes() == b"owned interrupted Git fixture"
        assert list(broker.checkout.parent.glob(".checkpoint-restore-*")) == [stage]


def test_checkpoint_ack_hook_commits_with_its_receipt_and_reconciles_rollback(stopped):
    store, spec, broker, row = stopped
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_mutations "
            "(command_id,run_id,kind,request_digest,state) "
            "VALUES ('owned-ack',?,'fixture','same','requested')",
            (spec["run_id"],),
        )

    def failed(db, current, receipt, manifest):
        assert current["run_id"] == spec["run_id"] == manifest["run_id"] == receipt["run_id"]
        db.execute("UPDATE delivery_mutations SET state='completed' WHERE command_id='owned-ack'")
        raise RuntimeError("owned rollback boundary")

    with pytest.raises(RuntimeError, match="owned rollback"):
        create_checkpoint(store, spec["run_id"], "save-ack", row["revision"], on_ack=failed)
    with store._connect() as db:
        assert (
            db.execute(
                "SELECT state FROM delivery_mutations WHERE command_id='owned-ack'"
            ).fetchone()[0]
            == "requested"
        )
        assert not db.execute(
            "SELECT 1 FROM delivery_commands WHERE command_id='checkpoint-create:save-ack'"
        ).fetchone()

    def accepted(db, _row, _receipt, _manifest):
        db.execute("UPDATE delivery_mutations SET state='completed' WHERE command_id='owned-ack'")

    receipt = create_checkpoint(store, spec["run_id"], "save-ack", row["revision"], on_ack=accepted)
    assert receipt["reconciled_partial"] is True
    with store._connect() as db:
        assert (
            db.execute(
                "SELECT state FROM delivery_mutations WHERE command_id='owned-ack'"
            ).fetchone()[0]
            == "completed"
        )


def test_process_death_after_checkpoint_publish_reconciles_same_command(stopped):
    store, spec, broker, row = stopped
    script = """
import os, sys
from pathlib import Path
from devflow_temporal import delivery_checkpoints as checkpoints
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
store=DeliveryStore(DeliveryConfig.load(Path(sys.argv[1])))
rename=checkpoints.os.rename
def interrupted(source, target):
    rename(source, target)
    if Path(target).parent.name == 'checkpoints' and not Path(target).name.startswith('.'):
        os._exit(77)
checkpoints.os.rename=interrupted
checkpoints.create_checkpoint(store, sys.argv[2], 'create-crash', int(sys.argv[3]))
"""
    process = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            script,
            str(store.config.path),
            spec["run_id"],
            str(row["revision"]),
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert process.returncode == 77, process.stderr
    with store._connect() as db:
        assert not db.execute(
            "SELECT 1 FROM delivery_commands WHERE command_id='checkpoint-create:create-crash'"
        ).fetchone()
    receipt = create_checkpoint(store, spec["run_id"], "create-crash", row["revision"])
    assert receipt["reconciled_partial"] is True
    assert read_checkpoint(store, spec["run_id"], receipt["checkpoint_id"])["resumable"]


@pytest.mark.parametrize("boundary", ["allocated", "staged", "prepared", "moved"])
def test_process_death_during_restore_git_retains_unknown_allocation(stopped, boundary):
    store, spec, broker, row = stopped
    receipt = create_checkpoint(store, spec["run_id"], "save-crash", row["revision"])
    shutil.rmtree(Path(spec["source_path"]))
    shutil.rmtree(broker.checkout)
    script = """
import os, sys
from pathlib import Path
from devflow_temporal import delivery_checkpoints as checkpoints
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
store = DeliveryStore(DeliveryConfig.load(Path(sys.argv[1])))
original = checkpoints._git
def interrupted(path, *args, **options):
    if sys.argv[5] == 'prepared' and args[:1] == ('symbolic-ref',):
        os._exit(77)
    value = original(path, *args, **options)
    if sys.argv[5] == 'allocated' and args[:2] == ('worktree', 'add'):
        os._exit(77)
    if sys.argv[5] == 'staged' and args[:2] == ('apply', '--index'):
        os._exit(77)
    if sys.argv[5] == 'moved' and args[:2] == ('worktree', 'move'):
        os._exit(77)
    return value
checkpoints._git = interrupted
checkpoints.restore_checkpoint(store, sys.argv[2], sys.argv[3], 'restore-crash', int(sys.argv[4]))
"""
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            script,
            str(store.config.path),
            spec["run_id"],
            receipt["checkpoint_id"],
            str(row["revision"]),
            boundary,
        ],
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 77, result.stderr
    with store._connect() as db:
        intent = dict(
            db.execute(
                "SELECT * FROM delivery_mutations WHERE command_id=?",
                ("checkpoint-restore:restore-crash",),
            ).fetchone()
        )
        assert (
            db.execute(
                "SELECT 1 FROM delivery_commands WHERE command_id=?",
                ("checkpoint-restore:restore-crash",),
            ).fetchone()
            is None
        )
    assert intent["state"] == "pending"  # A requested move is not a verified restoration.
    allocation = json.loads(intent["response_json"])
    identity = allocation.get("prepared_identity", allocation["allocated_identity"])
    with pytest.raises(ValueError, match="unfinished restoration"):
        restore_checkpoint(
            store,
            spec["run_id"],
            receipt["checkpoint_id"],
            "second-command-must-not-allocate",
            row["revision"],
        )
    from devflow_temporal.delivery_checkpoints import CheckpointRestoreUnknown

    assert allocation["phase"] == "git_active"
    with pytest.raises(CheckpointRestoreUnknown, match="Git/helper outcome is unknown"):
        restore_checkpoint(
            store, spec["run_id"], receipt["checkpoint_id"], "restore-crash", row["revision"]
        )
    retained = broker.checkout if broker.checkout.exists() else Path(allocation["staging"])
    assert checkpoints_identity(retained) == identity
    with store._connect() as db:
        assert (
            db.execute(
                "SELECT state FROM delivery_mutations WHERE command_id=?",
                ("checkpoint-restore:restore-crash",),
            ).fetchone()[0]
            == "pending"
        )


def test_selected_session_bytes_survive_source_deletion(stopped):
    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_continuation import session_state_digest

    store, spec, broker, row = stopped
    state = Path(spec["state_dir"])
    session_id = "owned-session-fixture"
    home = state / "role-homes/implement"
    codex = home / "codex"
    (codex / "sessions").mkdir(parents=True, mode=0o700)
    transcript = codex / "sessions" / (session_id + ".jsonl")
    transcript.write_text('{"fixture":"preserved transcript"}\n')
    transcript.chmod(0o600)
    (codex / "auth.json").write_text("fixture credentials must stay excluded")
    (codex / "config.toml").write_text("fixture profile must stay excluded")
    with sqlite3.connect(codex / "state_1.sqlite") as db:
        db.execute("CREATE TABLE sessions(id TEXT PRIMARY KEY)")
        db.execute("INSERT INTO sessions VALUES (?)", (session_id,))
    db.close()
    (codex / "state_1.sqlite").chmod(0o600)
    session_bytes = (codex / "state_1.sqlite").read_bytes()
    original_session_digest = session_state_digest(home, session_id)
    job_key = digest({"owned": "finished-fixture-session"})
    request = {
        "spec": spec,
        "role": "implement",
        "iteration": 1,
        "candidate": candidate_for(broker.checkout),
    }
    write_private(state / "attempts" / job_key / "request.json", request)
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_attempts "
            "(job_key,run_id,role,iteration,candidate_id,state,session_id,cleanup) "
            "VALUES (?,?,'implement',1,?,'finished',?,'confirmed')",
            (job_key, spec["run_id"], request["candidate"]["id"], session_id),
        )
    receipt = create_checkpoint(store, spec["run_id"], "save-native", row["revision"])
    checkpoint = state / "checkpoints" / receipt["checkpoint_id"]
    manifest = read_private(checkpoint / "manifest.json")
    assert "role-homes/implement/codex/state_1.sqlite" in manifest["state_refs"]
    assert not list(checkpoint.rglob("auth.json"))
    assert not list(checkpoint.rglob("config.toml"))
    shutil.rmtree(home)
    shutil.rmtree(Path(spec["source_path"]))
    shutil.rmtree(broker.checkout)
    restored = restore_checkpoint(
        store, spec["run_id"], receipt["checkpoint_id"], "restore-native", row["revision"]
    )
    assert restored["candidate"] == receipt["candidate"]
    assert transcript.read_text() == '{"fixture":"preserved transcript"}\n'
    assert (codex / "state_1.sqlite").read_bytes() == session_bytes
    assert session_state_digest(home, session_id) == original_session_digest
    assert not (codex / "auth.json").exists()
    assert not (codex / "config.toml").exists()


@pytest.mark.asyncio
async def test_public_checkpoint_routes_require_csrf_and_return_verified_readback(stopped):
    store, spec, broker, row = stopped
    app = create_app(store.config.path)
    url = store.config.dashboard_url.rstrip("/")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=url) as client:
        path = "/api/runs/" + spec["run_id"] + "/checkpoints"
        command = {"command_id": "http-save", "expected_revision": row["revision"]}
        assert (await client.post(path, json=command)).status_code == 403
        session = (await client.get("/api/session")).json()
        headers = {"origin": url, "x-devflow-csrf": session["csrf_token"]}
        response = await client.post(path, json=command, headers=headers)
        assert response.status_code == 200, response.text
        receipt = response.json()
        assert (await client.get(path + "/" + receipt["checkpoint_id"])).json()["resumable"]
        shutil.rmtree(Path(spec["source_path"]))
        shutil.rmtree(broker.checkout)
        response = await client.post(
            path + "/" + receipt["checkpoint_id"] + "/restore",
            json={"command_id": "http-restore", "expected_revision": row["revision"]},
            headers=headers,
        )
        assert response.status_code == 200, response.text
        assert response.json()["execution_started"] is False


@pytest.mark.asyncio
async def test_public_ci_preflight_refuses_missing_owner_lock_without_creating_it(
    stopped, monkeypatch
):
    from devflow_temporal import delivery_ci_inventory as ci
    from devflow_temporal import delivery_gate_retry as gate

    store, spec, _broker, _row = stopped
    lock = Path(spec["state_dir"]) / "workspace-owner.lock"
    lock.unlink()
    real_snapshot, observed = gate.snapshot, []

    def snapshot(*args, **kwargs):
        observed.append(kwargs.get("_observation"))
        return real_snapshot(*args, **kwargs)

    monkeypatch.setattr(gate, "snapshot", snapshot)
    monkeypatch.setattr(ci, "_profile", lambda _path: ({}, b""))
    monkeypatch.setattr(DeliveryStore, "_completed_temporal_result", lambda *_a, **_k: {
        "result": {"checks": {}, "roles": []}
    })
    monkeypatch.setattr(gate, "reconciled_cleanup", lambda *_a, **_k: None)
    monkeypatch.setattr(gate, "closed_checks", lambda *_a, **_k: ({}, {}))
    command = {
        "command_id": "ci-preflight-owned", "expected_revision": 1,
        "expected_projection_revision": 1, "expected_iteration": 1,
        "expected_candidate_id": "a" * 64, "expected_candidate_head": "b" * 40,
        "expected_content_sha256": "c" * 64, "expected_pr_number": 1,
        "profile_path": "/private/tmp/controlled-profile",
        "expected_profile_sha256": ci.APPROVED_PROFILE_SHA256,
    }
    app = create_app(store.config.path)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(
            transport=transport, base_url=store.config.dashboard_url) as client:
        response = await client.post(
            "/api/runs/" + spec["run_id"] + "/ci-inventory-preflight", json=command
        )
    assert response.status_code == 409, response.text
    assert observed == [True]
    assert "observation lock is unavailable" in response.text
    assert not lock.exists()


def test_checkpoint_rechecks_revision_after_publication(stopped, monkeypatch):
    from devflow_temporal import delivery_checkpoints as checkpoints

    store, spec, _, row = stopped
    original = checkpoints._publish_checkpoint

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        with sqlite3.connect(store.config.tracking_db) as db:
            db.execute(
                "UPDATE delivery_runs SET revision=revision+1 WHERE run_id=?",
                (spec["run_id"],),
            )
        return result

    monkeypatch.setattr(checkpoints, "_publish_checkpoint", changed)
    with pytest.raises(ValueError, match="before acknowledgement"):
        create_checkpoint(store, spec["run_id"], "late-change", row["revision"])
    with sqlite3.connect(store.config.tracking_db) as db:
        assert (
            db.execute(
                "SELECT COUNT(*) FROM delivery_commands WHERE command_id=?",
                ("checkpoint-create:late-change",),
            ).fetchone()[0]
            == 0
        )


def test_checkpoint_rejects_feature_registry_aba_after_publication(stopped, monkeypatch, tmp_path):
    from devflow_temporal import delivery_checkpoints as checkpoints
    from devflow_temporal.delivery_execution_registry import ExecutionRegistry
    from devflow_temporal.delivery_registry_epoch import initialize

    store, old_spec, _, row = stopped
    registry = ExecutionRegistry(tmp_path / "feature-registry" / "db.sqlite3")
    issue = {"id": "issue-1", "url": "https://github.com/o/r/issues/1", "repository_id": "r"}
    owner = registry.claim({"issue": issue}, old_spec["run_id"], str(store.config.tracking_db))
    with registry.connect() as db:
        initialize(db)
        db.commit()
    spec = {**old_spec, "feature_delivery": {"registry": str(registry.path), "owner": owner,
                                            "snapshot": {"issue": issue}}}
    monkeypatch.setattr(store, "effective_spec", lambda _run_id: spec)
    original = checkpoints._publish_checkpoint

    def mutate(*args, **kwargs):
        result = original(*args, **kwargs)
        with sqlite3.connect(registry.path) as db:
            db.execute(
                "UPDATE execution_claims SET state='draining' WHERE issue_id=?", (issue["id"],)
            )
            db.execute(
                "UPDATE execution_claims SET state='active' WHERE issue_id=?", (issue["id"],)
            )
        return result

    monkeypatch.setattr(checkpoints, "_publish_checkpoint", mutate)
    with pytest.raises(ValueError, match="registry authority changed"):
        create_checkpoint(store, spec["run_id"], "feature-aba-late", row["revision"])
    with sqlite3.connect(store.config.tracking_db) as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_commands WHERE command_id=?",
                          ("checkpoint-create:feature-aba-late",)).fetchone()[0] == 0


def test_checkpoint_rechecks_graph_within_final_transaction(stopped, monkeypatch):
    from devflow_temporal import delivery_checkpoints as checkpoints

    store, spec, _, row = stopped
    original = checkpoints._publish_checkpoint
    graph = {"current": True, "validations": 0}

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        graph["current"] = False
        return result

    def validate(db, registry_db):
        assert db.in_transaction and registry_db is None
        graph["validations"] += 1
        if not graph["current"]:
            raise ValueError("checkpoint graph changed")

    monkeypatch.setattr(checkpoints, "_publish_checkpoint", changed)
    with pytest.raises(ValueError, match="checkpoint graph changed"):
        create_checkpoint(
            store, spec["run_id"], "graph-change", row["revision"], graph_validation=validate
        )
    assert graph["validations"] == 2
    with sqlite3.connect(store.config.tracking_db) as db:
        assert db.execute(
            "SELECT COUNT(*) FROM delivery_commands WHERE command_id=?",
            ("checkpoint-create:graph-change",),
        ).fetchone()[0] == 0


def test_checkpoint_verifies_pinned_attempt_observation_at_ack(stopped, monkeypatch):
    from devflow_temporal import delivery_checkpoints as checkpoints

    store, spec, _, row = stopped
    fence = durable_fence(store, spec)
    original = checkpoints._publish_checkpoint
    status = {"valid": True, "checks": 0, "closed": False}

    @contextmanager
    def admission():
        yield

    class Proof:
        settlements = {"schema": "devflow-paused-attempt-settlement-v1", "attempts": []}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            status["closed"] = True

        def verify(self, db, registry_db):
            assert db.in_transaction and registry_db is None
            status["checks"] += 1
            if not status["valid"]:
                raise ValueError("pinned settlement changed")
            return self.settlements

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        status["valid"] = False
        return result

    monkeypatch.setattr(checkpoints, "_publish_checkpoint", changed)
    with pytest.raises(ValueError, match="pinned settlement changed"):
        create_checkpoint(
            store, spec["run_id"], "proof-change", row["revision"], fence=fence,
            admission_context=admission,
            attempt_observation=lambda _store, _spec, _attempts: Proof(),
        )
    assert status["checks"] >= 2 and status["closed"]
    with sqlite3.connect(store.config.tracking_db) as db:
        assert db.execute(
            "SELECT COUNT(*) FROM delivery_commands WHERE command_id=?",
            ("checkpoint-create:proof-change",),
        ).fetchone()[0] == 0


def test_checkpoint_refuses_pause_change_after_publication(stopped, monkeypatch):
    from devflow_temporal import delivery_checkpoints as checkpoints

    store, spec, _, row = stopped
    fence = durable_fence(store, spec)
    original = checkpoints._publish_checkpoint

    @contextmanager
    def admission():
        yield

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        with sqlite3.connect(store.config.tracking_db) as db:
            db.execute(
                "UPDATE delivery_mutations SET state='running' WHERE command_id=?",
                ("owned-fixture-fence-1",),
            )
        return result

    monkeypatch.setattr(checkpoints, "_publish_checkpoint", changed)
    with pytest.raises(ValueError, match="pause fence changed"):
        create_checkpoint(
            store, spec["run_id"], "pause-change", row["revision"], fence=fence,
            admission_context=admission,
        )
    with sqlite3.connect(store.config.tracking_db) as db:
        assert db.execute(
            "SELECT COUNT(*) FROM delivery_commands WHERE command_id=?",
            ("checkpoint-create:pause-change",),
        ).fetchone()[0] == 0
