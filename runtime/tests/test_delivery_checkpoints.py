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
from devflow_temporal.delivery_resources import RunResources, _identity as checkpoints_identity
from devflow_temporal.delivery_resources import read_private, write_private
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


def test_live_unknown_workspace_actor_prevents_resumability(stopped):
    store, spec, broker, row = stopped
    reader = subprocess.Popen(
        ["/bin/sh", "-c", "exec 3< README.md; exec sleep 30"], cwd=broker.checkout
    )
    try:
        with pytest.raises(ValueError, match="open actor|unknown writer"):
            create_checkpoint(store, spec["run_id"], "save-1", row["revision"])
    finally:
        reader.terminate()
        reader.wait(timeout=10)


def test_foreign_linked_source_is_never_copied(stopped, tmp_path):
    store, spec, broker, row = stopped
    foreign = tmp_path / "foreign-owned-fixture.txt"
    foreign.write_text("foreign bytes outside the candidate")
    os.link(foreign, broker.checkout / "foreign-linked.txt")
    with pytest.raises(ValueError, match="owned unlinked regular file"):
        create_checkpoint(store, spec["run_id"], "save-foreign", row["revision"])


def test_unclassified_ignored_work_is_not_silently_lost(stopped):
    store, spec, broker, row = stopped
    with (broker.checkout / ".gitignore").open("a") as stream:
        stream.write("important-draft.txt\n")
    (broker.checkout / "important-draft.txt").write_text("unpublished owned source")
    with pytest.raises(ValueError, match="unclassified ignored source.*important-draft"):
        create_checkpoint(store, spec["run_id"], "save-ignored", row["revision"])


def test_fresh_pause_epoch_cannot_restore_prior_epoch_source(stopped):
    store, spec, broker, row = stopped
    first = durable_fence(store, spec, 1)
    receipt = create_checkpoint(store, spec["run_id"], "save-epoch", row["revision"], fence=first)
    second = durable_fence(store, spec, 2)
    with pytest.raises(ValueError, match="pause epoch or execution fence changed"):
        create_checkpoint(store, spec["run_id"], "save-epoch", row["revision"], fence=second)
    shutil.rmtree(broker.checkout)
    with pytest.raises(ValueError, match="pause epoch or execution fence changed"):
        restore_checkpoint(
            store,
            spec["run_id"],
            receipt["checkpoint_id"],
            "restore-epoch",
            row["revision"],
            fence=second,
        )
    assert not broker.checkout.exists()


def test_completed_restore_replay_still_requires_current_pause_epoch(stopped):
    store, spec, broker, row = stopped
    first = durable_fence(store, spec, 1)
    receipt = create_checkpoint(
        store, spec["run_id"], "save-epoch-replay", row["revision"], fence=first
    )
    shutil.rmtree(broker.checkout)
    restore_checkpoint(
        store,
        spec["run_id"],
        receipt["checkpoint_id"],
        "restore-epoch-replay",
        row["revision"],
        fence=first,
    )
    second = durable_fence(store, spec, 2)
    with pytest.raises(ValueError, match="pause epoch or execution fence changed"):
        restore_checkpoint(
            store,
            spec["run_id"],
            receipt["checkpoint_id"],
            "restore-epoch-replay",
            row["revision"],
            fence=second,
        )


def test_trusted_controller_snapshot_is_bound_to_creation_and_restore(stopped):
    from devflow_temporal.contracts import digest

    store, spec, broker, row = stopped
    fence = durable_fence(store, spec)
    context = {
        "schema": "owned-sdk-snapshot-v1",
        "execution_run_id": "owned-execution",
        "role_cursor": 1,
        "check_cursor": 0,
        "budget": {"used": 1, "maximum": 2},
        "clock": {"pause_epoch": 1, "active_seconds": 12},
    }
    receipt = create_checkpoint(
        store,
        spec["run_id"],
        "save-controller",
        row["revision"],
        fence=fence,
        checkpoint_context=context,
    )
    assert receipt["checkpoint_context_sha256"] == digest(context)
    manifest = read_private(
        Path(spec["state_dir"]) / "checkpoints" / receipt["checkpoint_id"] / "manifest.json"
    )
    assert manifest["checkpoint_context"] == context
    changed = {**context, "role_cursor": 2}
    with pytest.raises(ValueError, match="original custody"):
        create_checkpoint(
            store,
            spec["run_id"],
            "save-controller",
            row["revision"],
            fence=fence,
            checkpoint_context=changed,
        )
    shutil.rmtree(broker.checkout)
    with pytest.raises(ValueError, match="controller snapshot changed"):
        restore_checkpoint(
            store,
            spec["run_id"],
            receipt["checkpoint_id"],
            "restore-controller",
            row["revision"],
            fence=fence,
            checkpoint_context=changed,
        )
    restore_checkpoint(
        store,
        spec["run_id"],
        receipt["checkpoint_id"],
        "restore-controller",
        row["revision"],
        fence=fence,
        checkpoint_context=context,
    )


def test_native_and_writer_custody_precede_root_fence_and_survive_ack(stopped, monkeypatch):
    from devflow_temporal import delivery_checkpoints as checkpoints

    store, spec, broker, row = stopped
    order = []
    original = checkpoints._closed

    def closed(*args):
        assert "root" not in order
        result = original(*args)
        order.append("native")
        return result

    class Writers:
        def __enter__(self):
            assert order[-1] == "native"
            order.append("writer")
            return self

        def verify(self, db):
            assert order[-1] == "root" and db.in_transaction

        def __exit__(self, *_args):
            assert order.pop() == "writer"

    @contextmanager
    def root():
        assert order[-1] == "writer"
        order.append("root")
        try:
            yield
        finally:
            assert order.pop() == "root"

    monkeypatch.setattr(checkpoints, "_closed", closed)
    receipt = create_checkpoint(
        store,
        spec["run_id"],
        "save-writer-order",
        row["revision"],
        writer_context=Writers(),
        admission_context=root,
    )
    shutil.rmtree(broker.checkout)
    restored = restore_checkpoint(
        store,
        spec["run_id"],
        receipt["checkpoint_id"],
        "restore-writer-order",
        row["revision"],
        writer_context=Writers(),
        admission_context=root,
    )
    assert restored["execution_started"] is False


def test_publication_executor_inventory_cannot_be_assumed_closed(stopped):
    store, spec, _broker, row = stopped
    root = Path(spec["state_dir"]) / "publication-executors"
    root.mkdir(mode=0o700)
    (root / ("a" * 64 + ".lock")).touch(mode=0o600)
    with pytest.raises(ValueError, match="publication writer inventory.*unbound"):
        create_checkpoint(store, spec["run_id"], "save-unknown-writer", row["revision"])


def test_requested_retained_merge_is_not_mistaken_for_stopped_delivery(stopped):
    store, spec, _broker, row = stopped
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_merge_requests "
            "(run_id,command_id,grant_json,workflow_id,phase,created_at,updated_at) "
            "VALUES (?,'owned-merge','{}','owned-merge-workflow','queued','now','now')",
            (spec["run_id"],),
        )
    with pytest.raises(ValueError, match="active merge needs authenticated controller"):
        create_checkpoint(store, spec["run_id"], "save-active-merge", row["revision"])


def test_queued_attempt_requires_versioned_owner_proof_under_same_fence(stopped):
    from hashlib import sha256

    from devflow_temporal.contracts import digest

    store, spec, broker, row = stopped
    state = Path(spec["state_dir"])
    key = digest({"owned": "never-started-attempt"})
    folder = state / "attempts" / key
    folder.mkdir(parents=True, mode=0o700)
    for name in ("native-monitor.lock", "native-process.lock"):
        (folder / name).touch(mode=0o600)
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_attempts "
            "(job_key,run_id,role,iteration,candidate_id,state,cleanup,result_path) "
            "VALUES (?,?,'implement',1,?,'queued','none',?)",
            (
                key,
                spec["run_id"],
                candidate_for(broker.checkout)["id"],
                str(folder / "result.json"),
            ),
        )
    proof_path = state / "no-go/owned-settlement.json"
    write_private(
        proof_path,
        {"schema": "owned-fixture-never-started-v1", "job_key": key, "provider_started": False},
    )
    fence = durable_fence(store, spec)

    def validator(db, execution, attempts):
        attempt = next(item for item in attempts if item["job_key"] == key)
        assert execution["run_id"] == spec["run_id"] and db.in_transaction
        assert read_private(proof_path)["provider_started"] is False
        return {
            "schema": "devflow-paused-attempt-settlement-v1",
            "attempts": [
                {
                    "job_key": key,
                    "row_sha256": digest(attempt),
                    "receipt_path": str(proof_path),
                    "receipt_sha256": sha256(proof_path.read_bytes()).hexdigest(),
                }
            ],
        }

    with pytest.raises(ValueError, match="attempts remain unfinished"):
        create_checkpoint(store, spec["run_id"], "save-queued", row["revision"], fence=fence)
    receipt = create_checkpoint(
        store,
        spec["run_id"],
        "save-queued",
        row["revision"],
        fence=fence,
        attempt_validation=validator,
    )
    shutil.rmtree(broker.checkout)
    with pytest.raises(ValueError, match="fresh owner validation"):
        restore_checkpoint(
            store,
            spec["run_id"],
            receipt["checkpoint_id"],
            "restore-queued",
            row["revision"],
            fence=fence,
        )
    restore_checkpoint(
        store,
        spec["run_id"],
        receipt["checkpoint_id"],
        "restore-queued",
        row["revision"],
        fence=fence,
        attempt_validation=validator,
    )
    with store._connect() as db:
        attempt = db.execute("SELECT * FROM delivery_attempts WHERE job_key=?", (key,)).fetchone()
    assert attempt["state"] == "queued" and attempt["result_json"] is None
    assert not (folder / "result.json").exists()


def test_completed_restore_retry_refuses_replaced_or_changed_candidate(stopped):
    store, spec, broker, row = stopped
    receipt = create_checkpoint(store, spec["run_id"], "save-replay", row["revision"])
    shutil.rmtree(broker.checkout)
    restore_checkpoint(
        store, spec["run_id"], receipt["checkpoint_id"], "restore-replay", row["revision"]
    )
    (broker.checkout / "README.md").write_text("changed after restoration")
    with pytest.raises(ValueError, match="workspace changed"):
        restore_checkpoint(
            store, spec["run_id"], receipt["checkpoint_id"], "restore-replay", row["revision"]
        )


def test_authoritative_browser_and_direct_artifacts_are_preserved(stopped):
    from hashlib import sha256

    store, spec, broker, row = stopped
    state = Path(spec["state_dir"])
    artifacts = [
        (state / "browser-qa/1/artifacts/junit.xml", b"<testsuite/>"),
        (state / "checks/1/retained/result.pdf", b"%PDF-owned-fixture"),
        (state / "direct-namespace/report.bin", b"owned referenced bytes"),
    ]
    references = []
    for path, raw in artifacts:
        path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        path.write_bytes(raw)
        path.chmod(0o600)
        references.append({"path": str(path), "sha256": sha256(raw).hexdigest(), "size": len(raw)})
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET checks_json=? WHERE run_id=?",
            (json.dumps({"state": "failed", "artifacts": references}), spec["run_id"]),
        )
    receipt = create_checkpoint(store, spec["run_id"], "save-artifacts", row["revision"])
    for path, _ in artifacts:
        path.unlink()
    shutil.rmtree(broker.checkout)
    restore_checkpoint(
        store, spec["run_id"], receipt["checkpoint_id"], "restore-artifacts", row["revision"]
    )
    for path, raw in artifacts:
        assert path.read_bytes() == raw


def _crash_restored_file(store, spec, receipt, revision, filename, boundary):
    from devflow_temporal import delivery_checkpoints as checkpoints

    original = checkpoints._restore_file

    def interrupted(db, key, intent, target, raw, mode, **kwargs):
        if target.name != filename:
            return original(db, key, intent, target, raw, mode, **kwargs)
        if boundary == "open":
            previous = os.open
            def open_file(path, *args, **kw):
                descriptor = previous(path, *args, **kw)
                if Path(path).name.startswith(".checkpoint-file-"):
                    os._exit(77)
                return descriptor
            os.open = open_file
        elif boundary == "write":
            previous = os.ftruncate
            def truncate(fd, length):
                previous(fd, length)
                os.write(fd, raw[:max(1, len(raw) // 2)])
                os._exit(77)
            os.ftruncate = truncate
        elif boundary == "mode":
            previous = os.fchmod
            def chmod(fd, value):
                previous(fd, value)
                os._exit(77)
            os.fchmod = chmod
        elif boundary == "mtime":
            previous = os.utime
            def utime(path, *args, **kw):
                previous(path, *args, **kw)
                os._exit(77)
            os.utime = utime
        elif boundary == "publish":
            previous = os.link
            def link(source, target, **kw):
                previous(source, target, **kw)
                os._exit(77)
            os.link = link
        return original(db, key, intent, target, raw, mode, **kwargs)

    checkpoints._restore_file = interrupted
    restore_checkpoint(store, spec["run_id"], receipt["checkpoint_id"],
                       "restore-file-crash", revision)


@pytest.mark.parametrize("kind,boundary", [
    ("source", "write"), ("source", "mode"), ("source", "publish"),
    ("state", "write"), ("state", "mode"), ("state", "mtime"), ("state", "publish"),
    ("state", "open"),
])
def test_restore_file_process_loss_replays_only_authenticated_stage(stopped, kind, boundary):
    from hashlib import sha256

    from devflow_temporal.delivery_checkpoints import CheckpointRestoreUnknown

    store, spec, broker, row = stopped
    artifact = Path(spec["state_dir"]) / "direct-namespace/report.bin"
    artifact.parent.mkdir(parents=True, mode=0o700)
    raw = b"owned evidence for interrupted restore"
    artifact.write_bytes(raw)
    artifact.chmod(0o600)
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET checks_json=? WHERE run_id=?", (
            json.dumps({"artifacts": [{"path": str(artifact), "sha256": sha256(raw).hexdigest()}]}),
            spec["run_id"],
        ))
    receipt = create_checkpoint(store, spec["run_id"], "save-file-crash", row["revision"])
    artifact.unlink()
    shutil.rmtree(broker.checkout)
    filename = "owned-untracked.txt" if kind == "source" else "report.bin"
    process = multiprocessing.get_context("fork").Process(
        target=_crash_restored_file, args=(store, spec, receipt, row["revision"], filename, boundary))
    process.start()
    process.join(30)
    assert process.exitcode == 77
    if boundary == "open":
        with pytest.raises(CheckpointRestoreUnknown, match="staging identity is unknown"):
            restore_checkpoint(store, spec["run_id"], receipt["checkpoint_id"],
                               "restore-file-crash", row["revision"])
        assert len(list(artifact.parent.glob(".checkpoint-file-*"))) == 1
        return
    restored = restore_checkpoint(store, spec["run_id"], receipt["checkpoint_id"],
                                  "restore-file-crash", row["revision"])
    assert restored["execution_started"] is False
    assert artifact.read_bytes() == raw
    assert (broker.checkout / "owned-untracked.txt").read_text() == "recover me\n"
    assert not list(Path(spec["state_dir"]).rglob(".checkpoint-file-*"))
    assert not list(broker.checkout.rglob(".checkpoint-file-*"))


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
    async with httpx.AsyncClient(transport=transport, base_url=store.config.dashboard_url) as client:
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


@pytest.mark.parametrize("replace", ["staging", "published"])
def test_restore_refuses_replaced_file_generation(stopped, replace):
    from hashlib import sha256

    from devflow_temporal.delivery_checkpoints import CheckpointRestoreUnknown

    store, spec, broker, row = stopped
    artifact = Path(spec["state_dir"]) / "direct-namespace/report.bin"
    artifact.parent.mkdir(parents=True, mode=0o700)
    raw = b"original owned evidence"
    artifact.write_bytes(raw)
    artifact.chmod(0o600)
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET checks_json=? WHERE run_id=?", (
            json.dumps({"artifacts": [{"path": str(artifact), "sha256": sha256(raw).hexdigest()}]}),
            spec["run_id"],
        ))
    receipt = create_checkpoint(store, spec["run_id"], "save-foreign-file", row["revision"])
    original_mtime = artifact.stat().st_mtime_ns
    artifact.unlink()
    shutil.rmtree(broker.checkout)
    process = multiprocessing.get_context("fork").Process(
        target=_crash_restored_file,
        args=(store, spec, receipt, row["revision"], "report.bin",
              "write" if replace == "staging" else "publish"))
    process.start()
    process.join(30)
    assert process.exitcode == 77
    temporary = next(artifact.parent.glob(".checkpoint-file-*"))
    replacement = temporary if replace == "staging" else artifact
    if replace == "staging":
        replacement.rename(artifact.parent / "retained-original-fixture")
    else:
        replacement.unlink()
    replacement.write_bytes(raw)
    replacement.chmod(0o600)
    os.utime(replacement, ns=(original_mtime, original_mtime))
    new_identity = replacement.stat().st_ino
    with pytest.raises(CheckpointRestoreUnknown, match="identity"):
        restore_checkpoint(store, spec["run_id"], receipt["checkpoint_id"],
                           "restore-file-crash", row["revision"])
    assert replacement.stat().st_ino == new_identity and replacement.read_bytes() == raw


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


def test_fenced_unknown_effect_is_preserved_without_claiming_resumability(stopped):
    store, spec, broker, row = stopped
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_effects VALUES (?,?,?,?,'pending',NULL,'now')",
            (
                "unknown-publish",
                spec["run_id"],
                "publish",
                json.dumps(
                    {"iteration": 1, "input_candidate_id": candidate_for(broker.checkout)["id"]}
                ),
            ),
        )
        db.execute(
            "INSERT INTO delivery_mutations "
            "(command_id,run_id,kind,request_digest,state,response_json) "
            "VALUES ('owned-pause-fence',?,'fixture-pause','frozen','paused',?)",
            (spec["run_id"], json.dumps({"pause_epoch": 1})),
        )

    def fence(db, current):
        paused = db.execute(
            "SELECT * FROM delivery_mutations WHERE command_id='owned-pause-fence'"
        ).fetchone()
        if paused["state"] != "paused" or paused["run_id"] != current["run_id"]:
            raise ValueError("owned fixture admission fence is no longer paused")
        return {
            "pause_epoch": 1,
            "workflow_id": "delivery-" + current["run_id"],
            "temporal_run_id": "owned-fixture-workflow-generation",
            "controller_sha256": paused["request_digest"],
        }

    with pytest.raises(ValueError, match="authenticated pause fence"):
        create_checkpoint(
            store, spec["run_id"], "save-pause", row["revision"], allow_unresolved_effects=True
        )
    receipt = create_checkpoint(
        store,
        spec["run_id"],
        "save-pause",
        row["revision"],
        fence=fence,
        allow_unresolved_effects=True,
    )
    assert receipt["resumable"] is False
    assert receipt["resume_blockers"] == [
        {
            "effect_key": "unknown-publish",
            "kind": "publish",
            "state": "pending",
            "scope": "delivery",
            "reason": "external_outcome_unknown",
        }
    ]
    assert read_checkpoint(store, spec["run_id"], receipt["checkpoint_id"])["pending_effects"]
    shutil.rmtree(broker.checkout)
    with pytest.raises(ValueError, match="require reconciliation"):
        restore_checkpoint(
            store,
            spec["run_id"],
            receipt["checkpoint_id"],
            "restore-pause",
            row["revision"],
            fence=fence,
        )
    for state, unproven in (("complete", "null"), ("complete", "{}"), ("absent", None)):
        with store._connect() as db:
            db.execute(
                "UPDATE delivery_effects SET state=?,observed_json=?,updated_at='later' "
                "WHERE effect_key='unknown-publish'",
                (state, unproven),
            )
        with pytest.raises(ValueError, match="budget or effects changed|require reconciliation"):
            restore_checkpoint(
                store,
                spec["run_id"],
                receipt["checkpoint_id"],
                "restore-pause",
                row["revision"],
                fence=fence,
            )
    publication = {
        "number": 7,
        "url": "https://github.com/example/fixture/pull/7",
        "state": "OPEN",
        "head": receipt["candidate"]["head"],
        "base": spec["base_sha"],
        "candidate": {**receipt["candidate"], "policy_digest": spec["policy_digest"]},
    }
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_effects SET state='complete',observed_json=?,updated_at='later' "
            "WHERE effect_key='unknown-publish'",
            (json.dumps(publication),),
        )
    restored = restore_checkpoint(
        store,
        spec["run_id"],
        receipt["checkpoint_id"],
        "restore-pause",
        row["revision"],
        fence=fence,
    )
    assert restored["execution_started"] is False
    with store._connect() as db:
        effect = dict(
            db.execute(
                "SELECT * FROM delivery_effects WHERE effect_key='unknown-publish'"
            ).fetchone()
        )
    assert effect["state"] == "complete"
    assert json.loads(effect["observed_json"]) == publication


@pytest.mark.asyncio
@pytest.mark.parametrize("state,observed", [("complete", None), ("complete", "null"),
                                            ("absent", None)])
async def test_unproven_effect_never_grants_checkpoint_resume(stopped, state, observed):
    store, spec, broker, row = stopped
    with store._connect() as db:
        db.execute("INSERT INTO delivery_effects VALUES (?,?,?,?,?,?,?)", (
            "unproven-effect", spec["run_id"], "fixture", "{}", state, observed, "now"))
    app = create_app(store.config.path)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url=store.config.dashboard_url) as client:
        csrf = (await client.get("/api/session")).json()["csrf_token"]
        response = await client.post(
            f"/api/runs/{spec['run_id']}/checkpoints",
            json={"command_id": "effect-public", "expected_revision": row["revision"]},
            headers={"origin": store.config.dashboard_url, "x-devflow-csrf": csrf},
        )
    assert response.status_code == 409
    assert "require reconciliation" in response.text
    fence = durable_fence(store, spec)
    receipt = create_checkpoint(store, spec["run_id"], "effect-fenced", row["revision"],
                                fence=fence, allow_unresolved_effects=True)
    assert receipt["resumable"] is False
    assert receipt["resume_blockers"][0]["effect_key"] == "unproven-effect"
    shutil.rmtree(broker.checkout)
    with pytest.raises(ValueError, match="require reconciliation"):
        restore_checkpoint(store, spec["run_id"], receipt["checkpoint_id"],
                           "effect-restore", row["revision"], fence=fence)
    assert not broker.checkout.exists()


def test_historical_effect_ack_readback_does_not_claim_resume_without_receipt(stopped):
    import hashlib

    from devflow_temporal.contracts import canonical_json

    store, spec, broker, row = stopped
    with store._connect() as db:
        db.execute("INSERT INTO delivery_effects VALUES (?,?,?,?,?,?,?)", (
            "legacy-unproven", spec["run_id"], "fixture", "{}", "complete", "{}", "now"))
    receipt = create_checkpoint(store, spec["run_id"], "legacy-effect", row["revision"])
    path = Path(spec["state_dir"]) / "checkpoints" / receipt["checkpoint_id"] / "manifest.json"
    manifest = read_private(path)
    effect = next(item for item in manifest["frozen"]["delivery_effects"]
                  if item["effect_key"] == "legacy-unproven")
    effect["observed_json"] = None
    raw = canonical_json(manifest).encode()
    path.write_bytes(raw)
    receipt["manifest_sha256"] = hashlib.sha256(raw).hexdigest()
    with store._connect() as db:
        db.execute("UPDATE delivery_commands SET response_json=? WHERE command_id=?",
                   (canonical_json(receipt), "checkpoint-create:legacy-effect"))
        db.execute("UPDATE delivery_events SET payload_json=? WHERE run_id=? AND type=?",
                   (canonical_json(receipt), spec["run_id"], "delivery_checkpoint_created"))
    read = read_checkpoint(store, spec["run_id"], receipt["checkpoint_id"])
    assert read["acknowledged_resumable"] is True and read["resumable"] is False
    assert any(item["effect_key"] == "legacy-unproven" for item in read["resume_blockers"])
    shutil.rmtree(broker.checkout)
    with pytest.raises(ValueError, match="budget or effects changed"):
        restore_checkpoint(store, spec["run_id"], receipt["checkpoint_id"],
                           "legacy-effect-restore", row["revision"])
    assert not broker.checkout.exists()


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


@pytest.mark.skipif(sys.platform != "darwin", reason="actual native macOS execution authority")
def test_passed_native_gate_checkpoint_requires_complete_descendant_closure(
    native_gates, monkeypatch
):
    store, spec, broker = native_gates.store, native_gates.spec, native_gates.broker
    result = broker.run_checks(1, native_gates.candidate)
    assert result["state"] == "passed", result
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET execution_state='blocked',phase='blocked',"
            "outcome='blocked',checks_json=? WHERE run_id=?",
            (json.dumps(result), spec["run_id"]),
        )
        row = dict(
            db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (spec["run_id"],)).fetchone()
        )
    with monkeypatch.context() as missing:

        def unavailable(*_args, **_kwargs):
            raise RuntimeError("owned fixture controller response is unavailable")

        missing.setattr(store, "_completed_temporal_result", unavailable)
        with pytest.raises(RuntimeError, match="controller response is unavailable"):
            create_checkpoint(store, spec["run_id"], "must-not-trust-projection", row["revision"])
    fence = durable_fence(store, spec)
    from devflow_temporal.delivery_checkpoints import NativeCheckpointCustodyUnknown

    with pytest.raises(NativeCheckpointCustodyUnknown):
        create_checkpoint(store, spec["run_id"], "save-passed-native", row["revision"],
                          fence=fence)
    with store._connect() as db:
        assert db.execute("SELECT 1 FROM delivery_commands WHERE command_id=?",
                          ("checkpoint-create:save-passed-native",)).fetchone() is None


def test_admission_context_follows_resources_and_observation_holds_no_write_lock(
    stopped, monkeypatch
):
    import fcntl

    from devflow_temporal import delivery_checkpoints as checkpoints

    store, spec, broker, row = stopped
    entered = False

    @contextmanager
    def admission():
        nonlocal entered
        descriptor = os.open(Path(spec["state_dir"]) / "resources/ownership.lock", os.O_RDONLY)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(descriptor)
        entered = True
        try:
            yield
        finally:
            entered = False

    original_git = checkpoints._git

    def observed(path, *args, **options):
        assert not entered
        with sqlite3.connect(store.config.tracking_db, timeout=0) as db:
            db.execute("BEGIN IMMEDIATE")
            db.rollback()
        return original_git(path, *args, **options)

    monkeypatch.setattr(checkpoints, "_git", observed)
    receipt = create_checkpoint(
        store, spec["run_id"], "save-lock-order", row["revision"], admission_context=admission
    )
    assert not entered
    shutil.rmtree(broker.checkout)
    restored = restore_checkpoint(
        store,
        spec["run_id"],
        receipt["checkpoint_id"],
        "restore-lock-order",
        row["revision"],
        admission_context=admission,
    )
    assert restored["execution_started"] is False
    assert not entered


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
    from devflow_temporal import delivery_ci_inventory as ci, delivery_gate_retry as gate

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
    async with httpx.AsyncClient(transport=transport, base_url=store.config.dashboard_url) as client:
        response = await client.post(
            "/api/runs/" + spec["run_id"] + "/ci-inventory-preflight", json=command
        )
    assert response.status_code == 409, response.text
    assert observed == [True]
    assert "observation lock is unavailable" in response.text
    assert not lock.exists()


@pytest.mark.asyncio
async def test_public_checkpoint_refuses_registered_native_custody(stopped):
    store, spec, broker, row = stopped
    receipt = create_checkpoint(store, spec["run_id"], "before-native", row["revision"])
    registry = RunResources(spec, read_only=True)
    resources = read_private(registry.manifest)
    resources["processes"].append(str(Path(spec["state_dir"]) / "native-process.json"))
    write_private(registry.manifest, resources)
    app = create_app(store.config.path)
    url = store.config.dashboard_url.rstrip("/")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=url) as client:
        path = "/api/runs/" + spec["run_id"] + "/checkpoints"
        session = (await client.get("/api/session")).json()
        headers = {"origin": url, "x-devflow-csrf": session["csrf_token"]}
        read = (await client.get(path + "/" + receipt["checkpoint_id"])).json()
        assert read["acknowledged_resumable"] is True
        assert read["resumable"] is False
        assert read["resume_blockers"][-1]["reason"] == "native_checkpoint_custody_unknown"
        response = await client.post(path, json={
            "command_id": "native-refused", "expected_revision": row["revision"]
        }, headers=headers)
        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "native_checkpoint_custody_unknown"
        shutil.rmtree(broker.checkout)
        response = await client.post(path + "/" + receipt["checkpoint_id"] + "/restore", json={
            "command_id": "native-restore", "expected_revision": row["revision"]
        }, headers=headers)
        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "native_checkpoint_custody_unknown"
    assert not broker.checkout.exists()
    assert not list(broker.checkout.parent.glob(".checkpoint-restore-*"))
    with store._connect() as db:
        assert db.execute("SELECT 1 FROM delivery_commands WHERE command_id=?",
                          ("checkpoint-create:native-refused",)).fetchone() is None
        assert db.execute("SELECT 1 FROM delivery_mutations WHERE command_id=?",
                          ("checkpoint-restore:native-restore",)).fetchone() is None
        assert db.execute("SELECT count(*) FROM delivery_events WHERE run_id=? AND type=?",
                          (spec["run_id"], "delivery_checkpoint_created")).fetchone()[0] == 1


def test_readback_preserves_ack_when_current_resources_are_unavailable(stopped):
    store, spec, _broker, row = stopped
    receipt = create_checkpoint(store, spec["run_id"], "read-unavailable", row["revision"])
    (Path(spec["state_dir"]) / "resources" / "manifest.json").unlink()
    read = read_checkpoint(store, spec["run_id"], receipt["checkpoint_id"])
    assert read["acknowledged_resumable"] is True
    assert read["resumable"] is False
    assert read["resume_blockers"][-1]["reason"] == "native_checkpoint_custody_unknown"


def test_historical_native_ack_is_readable_but_not_replayable_or_restorable(stopped):
    import hashlib

    from devflow_temporal.contracts import canonical_json
    from devflow_temporal.delivery_checkpoints import NativeCheckpointCustodyUnknown

    store, spec, broker, row = stopped
    receipt = create_checkpoint(store, spec["run_id"], "historical-native", row["revision"])
    path = Path(spec["state_dir"]) / "checkpoints" / receipt["checkpoint_id"] / "manifest.json"
    manifest = read_private(path)
    # Construct an owned historical ACK without running a native process.
    manifest["closed_journals"] = [{"journal": "owned-native", "sha256": "0" * 64}]
    raw = canonical_json(manifest).encode()
    path.write_bytes(raw)
    receipt["manifest_sha256"] = hashlib.sha256(raw).hexdigest()
    with store._connect() as db:
        db.execute("UPDATE delivery_commands SET response_json=? WHERE command_id=?",
                   (canonical_json(receipt), "checkpoint-create:historical-native"))
        db.execute("UPDATE delivery_events SET payload_json=? WHERE run_id=? AND type=?",
                   (canonical_json(receipt), spec["run_id"], "delivery_checkpoint_created"))
    read = read_checkpoint(store, spec["run_id"], receipt["checkpoint_id"])
    assert read["acknowledged_resumable"] is True and read["resumable"] is False
    assert read["manifest_sha256"] == receipt["manifest_sha256"]
    with pytest.raises(NativeCheckpointCustodyUnknown):
        create_checkpoint(store, spec["run_id"], "historical-native", row["revision"])
    shutil.rmtree(broker.checkout)
    with pytest.raises(NativeCheckpointCustodyUnknown):
        restore_checkpoint(store, spec["run_id"], receipt["checkpoint_id"],
                           "historical-restore", row["revision"])
    assert not broker.checkout.exists()
    with store._connect() as db:
        assert db.execute("SELECT 1 FROM delivery_mutations WHERE command_id=?",
                          ("checkpoint-restore:historical-restore",)).fetchone() is None


def test_checkpoint_rejects_revision_changed_during_unlocked_copy(stopped, monkeypatch):
    import concurrent.futures
    import threading

    from devflow_temporal import delivery_checkpoints as checkpoints

    store, spec, _, row = stopped
    mutex = threading.Lock()
    entered = threading.Event()
    proceed = threading.Event()
    original = checkpoints._candidate_snapshot

    @contextmanager
    def admission():
        with mutex:
            yield

    def blocked(*args, **kwargs):
        entered.set()
        assert proceed.wait(timeout=10)
        return original(*args, **kwargs)

    monkeypatch.setattr(checkpoints, "_candidate_snapshot", blocked)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            create_checkpoint,
            store,
            spec["run_id"],
            "concurrent-change",
            row["revision"],
            admission_context=admission,
        )
        try:
            assert entered.wait(timeout=10)
            assert mutex.acquire(timeout=2), "candidate copy held the root mutex"
            try:
                with sqlite3.connect(store.config.tracking_db) as db:
                    db.execute(
                        "UPDATE delivery_runs SET revision=revision+1 WHERE run_id=?",
                        (spec["run_id"],),
                    )
            finally:
                mutex.release()
        finally:
            proceed.set()
        with pytest.raises(ValueError, match="changed during preservation"):
            future.result(timeout=10)
    with sqlite3.connect(store.config.tracking_db) as db:
        assert (
            db.execute(
                "SELECT COUNT(*) FROM delivery_commands WHERE command_id=?",
                ("checkpoint-create:concurrent-change",),
            ).fetchone()[0]
            == 0
        )


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


@pytest.mark.asyncio
async def test_cancelled_checkpoint_waiter_keeps_worker_resource_custody(stopped, monkeypatch):
    import asyncio
    import fcntl
    import threading

    from devflow_temporal import delivery_checkpoints as checkpoints

    store, spec, _, row = stopped
    entered, proceed, exited = threading.Event(), threading.Event(), threading.Event()
    original = checkpoints._candidate_snapshot

    def blocked(*args, **kwargs):
        entered.set()
        assert proceed.wait(timeout=10)
        return original(*args, **kwargs)

    def run():
        try:
            return create_checkpoint(store, spec["run_id"], "cancel-waiter", row["revision"])
        finally:
            exited.set()

    monkeypatch.setattr(checkpoints, "_candidate_snapshot", blocked)
    task = asyncio.create_task(asyncio.to_thread(run))
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        descriptor = os.open(Path(spec["state_dir"]) / "resources/ownership.lock", os.O_RDONLY)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(descriptor)
        assert not exited.is_set()
    finally:
        proceed.set()
        assert await asyncio.to_thread(exited.wait, 10)


def test_checkpoint_rechecks_work_claim_after_publication(stopped, monkeypatch):
    from devflow_temporal import delivery_checkpoints as checkpoints

    store, spec, _, row = stopped
    original = checkpoints._publish_checkpoint

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        with sqlite3.connect(store.config.tracking_db) as db:
            updated = db.execute(
                "UPDATE claims SET owner='different-owner' WHERE work_id=?", (row["work_id"],)
            )
            assert updated.rowcount == 1
        return result

    monkeypatch.setattr(checkpoints, "_publish_checkpoint", changed)
    with pytest.raises(ValueError, match="before acknowledgement"):
        create_checkpoint(store, spec["run_id"], "claim-change", row["revision"])
    with sqlite3.connect(store.config.tracking_db) as db:
        assert (
            db.execute(
                "SELECT COUNT(*) FROM delivery_commands WHERE command_id=?",
                ("checkpoint-create:claim-change",),
            ).fetchone()[0]
            == 0
        )


def test_checkpoint_rejects_feature_registry_aba_during_copy(stopped, monkeypatch, tmp_path):
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
    original = checkpoints._candidate_snapshot

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

    monkeypatch.setattr(checkpoints, "_candidate_snapshot", mutate)
    with pytest.raises(ValueError, match="registry authority changed"):
        create_checkpoint(store, spec["run_id"], "feature-aba", row["revision"])
    with sqlite3.connect(store.config.tracking_db) as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_commands WHERE command_id=?",
                          ("checkpoint-create:feature-aba",)).fetchone()[0] == 0


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


def test_checkpoint_accepts_unchanged_feature_registry(service, monkeypatch):
    from test_delivery_feature_execution import feature_service

    configured, request, _ = feature_service(service, monkeypatch)
    # Persist the real admitted feature authority, including its registry owner,
    # so the final projection and manifest observe the same frozen request.
    store, spec, broker, row = stopped.__wrapped__((configured, request))
    receipt = create_checkpoint(store, spec["run_id"], "feature-stable", row["revision"])
    shutil.rmtree(broker.checkout)
    restored = restore_checkpoint(
        store, spec["run_id"], receipt["checkpoint_id"], "feature-restore", row["revision"]
    )
    assert restored["execution_started"] is False


def test_feature_effect_without_receipt_blocks_checkpoint_resume(service, monkeypatch):
    from test_delivery_feature_execution import feature_service
    from devflow_temporal.delivery_feature_execution import registry

    configured, request, _ = feature_service(service, monkeypatch)
    store, spec, broker, row = stopped.__wrapped__((configured, request))
    shared = registry(spec)
    token = spec["feature_delivery"]["owner"]
    shared.intent(token, "unproven-feature-effect", "fixture", {})
    with shared.connect() as db:
        db.execute("UPDATE execution_effects SET state='complete',result_json=NULL "
                   "WHERE effect_key='unproven-feature-effect'")
    with pytest.raises(ValueError, match="shared external effects require reconciliation"):
        create_checkpoint(store, spec["run_id"], "feature-unproven", row["revision"])
    fence = durable_fence(store, spec)
    receipt = create_checkpoint(store, spec["run_id"], "feature-unproven-fenced", row["revision"],
                                fence=fence, allow_unresolved_effects=True)
    assert receipt["resumable"] is False
    assert any(item["scope"] == "feature" and item["effect_key"] == "unproven-feature-effect"
               for item in receipt["resume_blockers"])
    # The registry's no-effect writer records a concrete result for absent.
    with shared.connect() as db:
        db.execute("UPDATE execution_effects SET state='absent',result_json=? "
                   "WHERE effect_key='unproven-feature-effect'",
                   (json.dumps({"rejected_before_effect": True}),))
    assert create_checkpoint(store, spec["run_id"], "feature-proven-absent", row["revision"])[
        "resumable"] is True
    with shared.connect() as db:
        db.execute("UPDATE execution_effects SET state='complete',result_json=NULL "
                   "WHERE effect_key='unproven-feature-effect'")
    shutil.rmtree(broker.checkout)
    with pytest.raises(ValueError, match="shared external effects require reconciliation"):
        restore_checkpoint(store, spec["run_id"], receipt["checkpoint_id"],
                           "feature-unproven-restore", row["revision"], fence=fence)


@pytest.mark.parametrize("current", ["new_pending_effect", "unavailable"])
def test_feature_checkpoint_readback_rechecks_current_registry(service, monkeypatch, current):
    from test_delivery_feature_execution import feature_service
    from devflow_temporal import delivery_checkpoints as checkpoints
    from devflow_temporal.delivery_feature_execution import registry

    configured, request, _ = feature_service(service, monkeypatch)
    store, spec, broker, row = stopped.__wrapped__((configured, request))
    receipt = create_checkpoint(store, spec["run_id"], "feature-readback", row["revision"])
    assert read_checkpoint(store, spec["run_id"], receipt["checkpoint_id"])["resumable"]
    if current == "new_pending_effect":
        shared = registry(spec)
        shared.intent(spec["feature_delivery"]["owner"], "new-feature-effect", "fixture", {})
    else:
        def unavailable(*_args):
            raise ValueError("unavailable")
        monkeypatch.setattr(checkpoints, "_feature_observation", unavailable)
    read = read_checkpoint(store, spec["run_id"], receipt["checkpoint_id"])
    assert read["acknowledged_resumable"] is True and read["resumable"] is False
    assert any(item["scope"] == "feature" and item["reason"] == (
        "registry_authority_changed" if current == "new_pending_effect"
        else "registry_observation_unknown") for item in read["resume_blockers"])
    if current == "new_pending_effect":
        assert any(item.get("effect_key") == "new-feature-effect" for item in read["resume_blockers"])
        shutil.rmtree(broker.checkout)
        with pytest.raises(ValueError, match="shared external effects require reconciliation"):
            restore_checkpoint(store, spec["run_id"], receipt["checkpoint_id"],
                               "feature-readback-restore", row["revision"])
        assert not broker.checkout.exists()


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


def test_checkpoint_refuses_legacy_attempt_validator_under_root_lock(stopped):
    store, spec, _, row = stopped

    @contextmanager
    def admission():
        yield

    with pytest.raises(ValueError, match="requires pinned attempt observation"):
        create_checkpoint(
            store, spec["run_id"], "legacy-validator", row["revision"],
            fence=durable_fence(store, spec), admission_context=admission,
            attempt_validation=lambda *_args: None,
        )




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


@pytest.mark.parametrize("authority", ["limit", "assignment"])
def test_checkpoint_refuses_changed_feature_authority_after_publication(
    stopped, monkeypatch, tmp_path, authority
):
    from devflow_temporal import delivery_checkpoints as checkpoints
    from devflow_temporal.delivery_execution_registry import ExecutionRegistry

    store, old_spec, _, row = stopped
    registry = ExecutionRegistry(tmp_path / "feature-authority" / "db.sqlite3")
    issue = {"id": "issue-1", "url": "https://github.com/o/r/issues/1", "repository_id": "r"}
    owner = registry.claim({"issue": issue}, old_spec["run_id"], str(store.config.tracking_db))
    with registry.connect() as db:
        db.execute(
            "INSERT INTO execution_workers VALUES (?,?,?,?,?,?)",
            (issue["id"], "worker", "workstream", 1, "finished", None),
        )
    spec = {**old_spec, "feature_delivery": {"registry": str(registry.path), "owner": owner,
                                            "snapshot": {"issue": issue}}}
    monkeypatch.setattr(store, "effective_spec", lambda _run_id: spec)
    original = checkpoints._publish_checkpoint

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        with sqlite3.connect(registry.path) as db:
            if authority == "limit":
                db.execute(
                    "UPDATE execution_repair_limits SET maximum=maximum+1 WHERE issue_id=?",
                    (issue["id"],),
                )
            else:
                db.execute(
                    "UPDATE execution_workers SET workstream_id='different' WHERE issue_id=?",
                    (issue["id"],),
                )
        return result

    monkeypatch.setattr(checkpoints, "_publish_checkpoint", changed)
    with pytest.raises(ValueError, match="registry authority changed"):
        create_checkpoint(store, spec["run_id"], "authority-change", row["revision"])
    with sqlite3.connect(store.config.tracking_db) as db:
        assert db.execute(
            "SELECT COUNT(*) FROM delivery_commands WHERE command_id=?",
            ("checkpoint-create:authority-change",),
        ).fetchone()[0] == 0


def test_checkpoint_detaches_context_and_ack_callback_objects(stopped, monkeypatch):
    from devflow_temporal import delivery_checkpoints as checkpoints
    from devflow_temporal.contracts import digest

    store, spec, broker, row = stopped
    fence = durable_fence(store, spec)
    context = {"schema": "large-owned-context", "payload": "x" * 900_000, "version": 1}
    expected = dict(context)
    expected_digest = digest(expected)
    entered = False
    original_digest = checkpoints.digest
    original_publish = checkpoints._publish_checkpoint

    @contextmanager
    def admission():
        nonlocal entered
        entered = True
        try:
            yield
        finally:
            entered = False

    def observed_digest(value):
        if isinstance(value, dict) and value.get("schema") == "large-owned-context":
            assert not entered, "context hashed under root admission fence"
        return original_digest(value)

    def changed(*args, **kwargs):
        result = original_publish(*args, **kwargs)
        context["version"] = 2
        return result

    def ack(_db, _row, receipt, manifest):
        receipt["checkpoint_context_sha256"] = "changed"
        manifest["checkpoint_context"]["version"] = 3

    monkeypatch.setattr(checkpoints, "digest", observed_digest)
    monkeypatch.setattr(checkpoints, "_publish_checkpoint", changed)
    receipt = create_checkpoint(
        store, spec["run_id"], "detached-context", row["revision"], fence=fence,
        admission_context=admission, checkpoint_context=context, on_ack=ack,
    )
    assert context["version"] == 2
    assert receipt["checkpoint_context_sha256"] == expected_digest
    manifest = read_private(
        Path(spec["state_dir"]) / "checkpoints" / receipt["checkpoint_id"] / "manifest.json"
    )
    assert manifest["checkpoint_context"] == expected
    shutil.rmtree(broker.checkout)
    restore_context = dict(expected)
    original_populate = checkpoints._populate_restore

    def mutate_restore(*args, **kwargs):
        restore_context["version"] = 4
        return original_populate(*args, **kwargs)

    monkeypatch.setattr(checkpoints, "_populate_restore", mutate_restore)
    restored = restore_checkpoint(
        store, spec["run_id"], receipt["checkpoint_id"], "detached-restore", row["revision"],
        fence=fence, admission_context=admission, checkpoint_context=restore_context,
    )
    assert restored["execution_started"] is False
    assert restore_context["version"] == 4


@pytest.mark.parametrize("unauthorized", [False, True])
def test_graph_acknowledgement_validates_exact_transition(stopped, unauthorized):
    store, spec, _, row = stopped
    with store._connect() as db:
        db.execute(
            "CREATE TABLE checkpoint_graph_control (revision INTEGER, receipt TEXT, owner TEXT)"
        )
        db.execute("INSERT INTO checkpoint_graph_control VALUES (7,NULL,'owner')")

    entered = False

    @contextmanager
    def admission():
        nonlocal entered
        entered = True
        try:
            yield
        finally:
            entered = False

    def prepare(receipt, manifest):
        assert not entered
        result = receipt["checkpoint_id"]
        receipt["checkpoint_id"] = "prepare-mutation"
        manifest["checkpoint_id"] = "prepare-mutation"
        return result

    def graph_before(db, _registry):
        actual = tuple(db.execute("SELECT * FROM checkpoint_graph_control").fetchone())
        if actual != (7, None, "owner"):
            raise ValueError("initial graph changed")

    def ack(db, _row, receipt, _manifest, prepared):
        assert entered
        db.execute("UPDATE checkpoint_graph_control SET revision=8,receipt=?,owner=?", (
            prepared, "changed-owner" if unauthorized else "owner",
        ))
        # Callback mutation must not alter the receipt passed to final validation.
        receipt["checkpoint_id"] = "callback-mutation"

    def graph_after(db, _registry, receipt, prepared):
        assert prepared == receipt["checkpoint_id"]
        actual = tuple(db.execute("SELECT * FROM checkpoint_graph_control").fetchone())
        if actual != (8, receipt["checkpoint_id"], "owner"):
            raise ValueError("unauthorized graph transition")

    if unauthorized:
        with pytest.raises(ValueError, match="unauthorized graph transition"):
            create_checkpoint(
                store, spec["run_id"], "graph-transition", row["revision"], on_ack=ack,
                prepare_ack=prepare, admission_context=admission,
                graph_validation=graph_before, graph_transition_validation=graph_after,
            )
    else:
        receipt = create_checkpoint(
            store, spec["run_id"], "graph-transition", row["revision"], on_ack=ack,
            prepare_ack=prepare, admission_context=admission,
            graph_validation=graph_before, graph_transition_validation=graph_after,
        )
        assert receipt["checkpoint_id"] != "callback-mutation"
    with store._connect() as db:
        actual = tuple(db.execute("SELECT * FROM checkpoint_graph_control").fetchone())
        commands = db.execute(
            "SELECT COUNT(*) FROM delivery_commands WHERE command_id=?",
            ("checkpoint-create:graph-transition",),
        ).fetchone()[0]
    if unauthorized:
        assert actual == (7, None, "owner") and commands == 0
    else:
        assert actual == (8, receipt["checkpoint_id"], "owner") and commands == 1
