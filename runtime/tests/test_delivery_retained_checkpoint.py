from __future__ import annotations

import os
from contextlib import ExitStack
from pathlib import Path

import pytest
from test_delivery_checkpoints import stopped as stopped
from test_delivery_store import service as service

from devflow_temporal.candidate import candidate_for
from devflow_temporal.delivery_checkpoints import (
    _locked,
    _read_acknowledged,
    _root,
    create_checkpoint,
)
from devflow_temporal.delivery_resources import RunResources
from devflow_temporal.delivery_retained_checkpoint import (
    REVALIDATED,
    RetainedCheckpointCustody,
    _query,
    verify_retained_source,
    verify_retained_state,
)
from devflow_temporal.delivery_writer_custody import CheckpointWriters


@pytest.fixture
def retained(stopped):
    store, spec, broker, row = stopped
    receipt = create_checkpoint(store, spec["run_id"], "retained-unit", row["revision"])
    with ExitStack() as stack:
        stack.enter_context(_locked(spec))
        resources = stack.enter_context(RunResources(spec, read_only=True).locked(
            read_only=True, exclusive=True))
        stack.enter_context(CheckpointWriters(store, spec))
        with store._connect() as db:
            value, _ = _read_acknowledged(db, spec, receipt["checkpoint_id"])
        yield spec, broker, resources, value, _root(spec) / receipt["checkpoint_id"]


def test_readonly_retained_comparison_keeps_checkout_and_index_unchanged(retained):
    spec, broker, resources, value, root = retained
    index_path = _query(
        broker.checkout, "rev-parse", "--path-format=absolute", "--git-path", "index")
    index = Path(index_path.decode().strip())
    before = (broker.checkout.stat().st_ino, index.read_bytes(), index.stat().st_mtime_ns)
    proof = verify_retained_source(spec, value, root, resources)
    verify_retained_state(spec, value, root)
    assert proof["head"] == value["source"]["candidate"]["head"]
    assert before == (broker.checkout.stat().st_ino, index.read_bytes(), index.stat().st_mtime_ns)


@pytest.mark.parametrize("change", ["index", "branch", "extra", "executable", "missing"])
def test_retained_comparison_refuses_changes_without_repairing_source(retained, change):
    spec, broker, resources, value, root = retained
    target = broker.checkout / "README.md"
    if change == "index":
        content = target.read_bytes()
        target.write_text("different staged source\n")
        _query(broker.checkout, "add", "README.md")
        target.write_bytes(content)
        assert candidate_for(broker.checkout) == value["source"]["candidate"]
    elif change == "branch":
        _query(broker.checkout, "branch", "owned-other-branch", "HEAD")
        _query(broker.checkout, "symbolic-ref", "HEAD", "refs/heads/owned-other-branch")
        assert candidate_for(broker.checkout) == value["source"]["candidate"]
    elif change == "extra":
        (broker.checkout / "owned-new-untracked").write_text("new work\n")
    elif change == "executable":
        target.chmod(target.stat().st_mode ^ 0o100)
    else:
        target.unlink()
    before = {path.name: path.read_bytes() for path in broker.checkout.iterdir()
              if path.is_file() and path.name != ".git"}
    with pytest.raises(ValueError):
        verify_retained_source(spec, value, root, resources)
    assert before == {path.name: path.read_bytes() for path in broker.checkout.iterdir()
                      if path.is_file() and path.name != ".git"}
    assert os.path.isdir(broker.checkout)


def test_source_only_ack_is_idempotent_and_never_grants_transition_authority(stopped):
    store, spec, broker, row = stopped
    checkpoint = create_checkpoint(store, spec["run_id"], "retained-create", row["revision"])
    original = broker.checkout.stat().st_ino
    with RetainedCheckpointCustody(store, spec["run_id"], checkpoint["checkpoint_id"],
                                  row["revision"]) as owner:
        receipt = owner.acknowledge("retained-bind")
        assert receipt["copied"] is False and receipt["execution_started"] is False
        with pytest.raises(ValueError, match="fresh custody scope"):
            owner.acknowledge("retained-bind")
    with pytest.raises(ValueError, match="live custody owner"):
        owner.acknowledge("expired")
    with RetainedCheckpointCustody(store, spec["run_id"], checkpoint["checkpoint_id"],
                                  row["revision"]) as retry:
        assert retry.acknowledge("retained-bind") == receipt
        with pytest.raises(TypeError):
            retry.acknowledge("retained-bind", sql_transition=lambda *_: pytest.fail("replayed"))
    assert broker.checkout.stat().st_ino == original
    with store._connect() as db:
        assert db.execute("SELECT count(*) FROM delivery_events WHERE run_id=? AND type=?",
                          (spec["run_id"], REVALIDATED)).fetchone()[0] == 1
    assert not list(broker.checkout.parent.glob(".checkpoint-restore-*"))


def test_ack_sql_policy_cannot_modify_preserved_budget(stopped):
    import sqlite3

    from devflow_temporal.delivery_retained_tracking import RetainedSQLPolicy

    store, spec, _broker, row = stopped
    policy = RetainedSQLPolicy()
    with store._connect(write_authorizer=policy) as db:
        db.execute("BEGIN IMMEDIATE")
        with policy.allow("record"):
            with pytest.raises(sqlite3.DatabaseError, match="authorized"):
                db.execute("UPDATE delivery_runs SET usage_json='{}' WHERE run_id=?",
                           (spec["run_id"],))
        db.rollback()
    with store._connect() as db:
        assert db.execute("SELECT usage_json FROM delivery_runs WHERE run_id=?",
                          (spec["run_id"],)).fetchone()[0] == row["usage_json"]


def test_reused_sql_is_reauthorized_after_policy_phase_change(
    stopped, monkeypatch,
):
    import sqlite3

    from devflow_temporal.delivery_retained_tracking import RetainedSQLPolicy

    store, spec, _broker, _row = stopped
    original_connect = store.state.connect

    def legacy_connect(path):
        return original_connect(path)

    monkeypatch.setattr(store.state, "connect", legacy_connect)
    policy = RetainedSQLPolicy()
    with store._connect(write_authorizer=policy) as db:
        db.execute("BEGIN IMMEDIATE")
        sql = "INSERT INTO delivery_commands VALUES (?,?,?,?)"
        first = ("retained-policy-first", spec["run_id"], "request", "{}")
        second = ("retained-policy-second", spec["run_id"], "request", "{}")
        with policy.allow("record"):
            db.execute(sql, first)
        with policy.allow("read"):
            with pytest.raises(sqlite3.DatabaseError, match="authorized"):
                db.execute(sql, second)
        db.rollback()


def test_retained_replay_rechecks_index_even_with_unchanged_candidate(stopped):
    store, spec, broker, row = stopped
    checkpoint = create_checkpoint(store, spec["run_id"], "retained-replay", row["revision"])
    with RetainedCheckpointCustody(store, spec["run_id"], checkpoint["checkpoint_id"],
                                  row["revision"]) as owner:
        owner.acknowledge("same-retained-command")
    target = broker.checkout / "README.md"
    content = target.read_bytes()
    target.write_text("different index\n")
    _query(broker.checkout, "add", "README.md")
    target.write_bytes(content)
    assert candidate_for(broker.checkout) == checkpoint["candidate"]
    with pytest.raises(ValueError):
        with RetainedCheckpointCustody(store, spec["run_id"], checkpoint["checkpoint_id"],
                                      row["revision"]) as owner:
            owner.acknowledge("same-retained-command")


@pytest.mark.parametrize("change", ["extra_evidence", "evidence_mtime", "native_registration"])
def test_retained_scope_refuses_changed_evidence_or_unknown_native_without_source_changes(
    stopped, change,
):
    from devflow_temporal.delivery_resources import read_private, write_private

    store, spec, broker, row = stopped
    evidence = Path(spec["state_dir"]) / "checks" / "owned-retained.json"
    evidence.parent.mkdir(mode=0o700, exist_ok=True)
    write_private(evidence, {"owned": "evidence"})
    checkpoint = create_checkpoint(store, spec["run_id"], "retained-evidence", row["revision"])
    original = broker.checkout.stat().st_ino
    if change == "extra_evidence":
        write_private(evidence.with_name("owned-extra.json"), {"extra": True})
    elif change == "evidence_mtime":
        info = evidence.stat()
        os.utime(evidence, ns=(info.st_atime_ns, info.st_mtime_ns + 1))
    else:
        manifest = Path(spec["state_dir"]) / "resources" / "manifest.json"
        data = read_private(manifest)
        data["processes"].append(str(Path(spec["state_dir"]) / "owned-opaque-native.json"))
        write_private(manifest, data)
    with pytest.raises(ValueError):
        with RetainedCheckpointCustody(store, spec["run_id"], checkpoint["checkpoint_id"],
                                      row["revision"]) as owner:
            owner.acknowledge("changed-retained-evidence")
    assert broker.checkout.stat().st_ino == original
    with store._connect() as db:
        assert not db.execute("SELECT 1 FROM delivery_events WHERE run_id=? AND type=?",
                              (spec["run_id"], REVALIDATED)).fetchone()


def test_retained_custody_refuses_another_thread_and_expired_context(stopped):
    from concurrent.futures import ThreadPoolExecutor

    store, spec, _broker, row = stopped
    checkpoint = create_checkpoint(store, spec["run_id"], "retained-thread", row["revision"])
    with RetainedCheckpointCustody(store, spec["run_id"], checkpoint["checkpoint_id"],
                                  row["revision"]) as owner:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(owner.acknowledge, "wrong-thread")
            with pytest.raises(ValueError, match="exact live custody"):
                future.result()
    with pytest.raises(ValueError, match="exact live custody"):
        owner.acknowledge("expired-context")


def test_retained_custody_does_not_promote_unobserved_effect_archive(stopped):
    from test_delivery_checkpoints import durable_fence

    store, spec, _broker, row = stopped
    with store._connect() as db:
        db.execute("UPDATE delivery_effects SET observed_json=NULL WHERE run_id=?",
                   (spec["run_id"],))
    fence = durable_fence(store, spec)
    checkpoint = create_checkpoint(store, spec["run_id"], "retained-unobserved",
                                   row["revision"], fence=fence, allow_unresolved_effects=True)
    assert checkpoint["resumable"] is False
    with pytest.raises(ValueError, match="reconciled effects and a fresh seal"):
        with RetainedCheckpointCustody(store, spec["run_id"], checkpoint["checkpoint_id"],
                                      row["revision"]):
            pytest.fail("local equality promoted an unobserved outcome")


def test_fence_bound_checkpoint_refuses_retained_verification(stopped):
    from test_delivery_checkpoints import durable_fence

    store, spec, _broker, row = stopped
    first = durable_fence(store, spec, epoch=1)
    checkpoint = create_checkpoint(store, spec["run_id"], "retained-epoch", row["revision"],
                                   fence=first)
    with pytest.raises(ValueError, match="pause/control verification is unsupported"):
        with RetainedCheckpointCustody(store, spec["run_id"], checkpoint["checkpoint_id"],
                                      row["revision"]):
            pytest.fail("copied pause data became live authority")
    with pytest.raises(TypeError):
        RetainedCheckpointCustody(store, spec["run_id"], checkpoint["checkpoint_id"],
                                  row["revision"], fence=first)


@pytest.mark.parametrize("revision", [True, False, -1, 1.0, "1"])
def test_retained_revision_is_exact_nonnegative_integer(stopped, revision):
    store, spec, _broker, _row = stopped
    with pytest.raises(ValueError, match="nonnegative integer"):
        RetainedCheckpointCustody(store, spec["run_id"], "0" * 64, revision)


def test_fork_cannot_use_the_parent_custody_scope(stopped):
    store, spec, _broker, row = stopped
    checkpoint = create_checkpoint(store, spec["run_id"], "retained-process", row["revision"])
    with RetainedCheckpointCustody(store, spec["run_id"], checkpoint["checkpoint_id"],
                                  row["revision"]) as owner:
        pid = os.fork()
        if pid == 0:
            try:
                owner.acknowledge("forked-scope")
            except ValueError:
                os._exit(0)
            os._exit(1)
        waited, status = os.waitpid(pid, 0)
        assert waited == pid and os.waitstatus_to_exitcode(status) == 0
    with store._connect() as db:
        assert not db.execute("SELECT 1 FROM delivery_commands WHERE command_id=?",
                              ("checkpoint-retained:forked-scope",)).fetchone()


@pytest.mark.parametrize("flag", ["assume-unchanged", "skip-worktree", "intent-to-add"])
def test_retained_index_flags_cannot_change_behind_same_candidate(stopped, flag):
    store, spec, broker, row = stopped
    checkpoint = create_checkpoint(store, spec["run_id"], "retained-index-flags", row["revision"])
    if flag == "intent-to-add":
        _query(broker.checkout, "add", "-N", "owned-untracked.txt")
    else:
        _query(broker.checkout, "update-index", "--" + flag, "README.md")
    assert candidate_for(broker.checkout) == checkpoint["candidate"]
    with pytest.raises(ValueError, match="semantic"):
        with RetainedCheckpointCustody(store, spec["run_id"], checkpoint["checkpoint_id"],
                                      row["revision"]) as owner:
            owner.acknowledge("changed-index-flags")


@pytest.mark.parametrize("changed", [False, True])
def test_one_final_physical_scan_catches_source_changes_during_scope(stopped, monkeypatch, changed):
    from devflow_temporal import delivery_checkpoints as checkpoints

    store, spec, broker, row = stopped
    checkpoint = create_checkpoint(store, spec["run_id"], "single-scan", row["revision"])
    scans = []
    original = checkpoints._read_acknowledged

    def observed(*args, **kwargs):
        scans.append(kwargs.get("verify_artifacts", True))
        return original(*args, **kwargs)

    monkeypatch.setattr(checkpoints, "_read_acknowledged", observed)
    with RetainedCheckpointCustody(store, spec["run_id"], checkpoint["checkpoint_id"],
                                  row["revision"]) as owner:
        assert scans == [False]  # Entry owns leases; it grants no verified receipt.
        if changed:
            (broker.checkout / "README.md").write_text("changed while custody was held\n")
            with pytest.raises(ValueError):
                owner.acknowledge("final-scan")
        else:
            assert owner.acknowledge("final-scan")["retained_verified"]
    assert scans == [False, True]


def test_tracking_aba_refuses_without_fresh_payload_read_under_ack(stopped, monkeypatch):
    from devflow_temporal import delivery_checkpoints as checkpoints

    store, spec, _broker, row = stopped
    checkpoint = create_checkpoint(store, spec["run_id"], "retained-tracking-aba", row["revision"])
    with RetainedCheckpointCustody(store, spec["run_id"], checkpoint["checkpoint_id"],
                                  row["revision"]) as owner:
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET usage_json='{}' WHERE run_id=?", (spec["run_id"],))
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET usage_json=? WHERE run_id=?",
                       (row["usage_json"], spec["run_id"]))
        monkeypatch.setattr(checkpoints, "_frozen",
                            lambda *_: pytest.fail("ACK re-materialized tracking rows"))
        with pytest.raises(ValueError, match="tracking changed"):
            owner.acknowledge("retained-aba")


@pytest.mark.asyncio
async def test_public_retained_route_revalidates_without_copy_or_activation(stopped):
    import httpx

    from devflow_temporal.delivery_api import create_app

    store, spec, broker, row = stopped
    checkpoint = create_checkpoint(store, spec["run_id"], "retained-http", row["revision"])
    inode = broker.checkout.stat().st_ino
    app = create_app(store.config.path)
    url = store.config.dashboard_url.rstrip("/")
    path = ("/api/runs/" + spec["run_id"] + "/checkpoints/" + checkpoint["checkpoint_id"]
            + "/verify-retained")
    command = {"command_id": "retained-http-command", "expected_revision": row["revision"]}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=url) as client:
        assert (await client.post(path, json=command)).status_code == 403
        session = (await client.get("/api/session")).json()
        headers = {"origin": url, "x-devflow-csrf": session["csrf_token"]}
        first = await client.post(path, json=command, headers=headers)
        assert first.status_code == 200, first.text
        assert first.json()["retained_verified"] and not first.json()["execution_started"]
        assert first.json()["copied"] is False
        assert (await client.post(path, json=command, headers=headers)).json() == first.json()
        _query(broker.checkout, "update-index", "--assume-unchanged", "README.md")
        refused = await client.post(path, json=command, headers=headers)
        assert refused.status_code == 409 and "semantic" in refused.text
    assert broker.checkout.stat().st_ino == inode


@pytest.mark.asyncio
async def test_public_legacy_executor_without_requested_effect_keeps_existing_flow(stopped):
    import httpx

    from devflow_temporal.delivery_api import create_app
    from devflow_temporal.delivery_publication_execution import execution

    store, spec, broker, row = stopped
    # Exercise the actual legacy executor lifecycle and its original lock/journal.
    # No mutation is requested and no external result is fabricated.
    assert "pause_control_version" not in spec
    with execution(broker, 1, broker.candidate()) as writer:
        assert writer is not None
        key = writer.key
    with store._connect() as db:
        assert db.execute("SELECT 1 FROM delivery_publication_executors WHERE effect_key=?",
                          (key,)).fetchone()
        assert not db.execute("SELECT 1 FROM delivery_effects WHERE effect_key=?",
                              (key,)).fetchone()
    inode = broker.checkout.stat().st_ino
    app = create_app(store.config.path)
    url = store.config.dashboard_url.rstrip("/")
    path = "/api/runs/" + spec["run_id"] + "/checkpoints"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=url) as client:
        session = (await client.get("/api/session")).json()
        headers = {"origin": url, "x-devflow-csrf": session["csrf_token"]}
        created = await client.post(path, headers=headers,
            json={"command_id": "legacy-source-save", "expected_revision": row["revision"]})
        assert created.status_code == 200, created.text
        receipt = await client.post(path + "/" + created.json()["checkpoint_id"]
            + "/verify-retained", headers=headers,
            json={"command_id": "legacy-source-retained", "expected_revision": row["revision"]})
        assert receipt.status_code == 200, receipt.text
        assert receipt.json()["retained_verified"] and not receipt.json()["execution_started"]
        assert not receipt.json()["copied"]
    assert broker.checkout.stat().st_ino == inode


@pytest.mark.parametrize("flag", ["assume-unchanged", "skip-worktree", "intent-to-add"])
def test_seal_refuses_unsupported_index_flags_without_rewriting_them(stopped, flag):
    store, spec, broker, row = stopped
    if flag == "intent-to-add":
        _query(broker.checkout, "add", "-N", "owned-untracked.txt")
    else:
        _query(broker.checkout, "update-index", "--" + flag, "README.md")
    before = _query(broker.checkout, "ls-files", "--debug", "-z")
    with pytest.raises(ValueError, match="semantic flags"):
        create_checkpoint(store, spec["run_id"], "unsupported-index", row["revision"])
    assert _query(broker.checkout, "ls-files", "--debug", "-z") == before
    with store._connect() as db:
        assert not db.execute("SELECT 1 FROM delivery_commands WHERE command_id=?",
                              ("checkpoint-create:unsupported-index",)).fetchone()
