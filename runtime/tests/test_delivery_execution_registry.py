from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest

from devflow_temporal.delivery_execution_registry import (
    ExecutionRegistry,
    OwnershipConflict,
    UnresolvedEffect,
)


@pytest.fixture
def registry(tmp_path):
    return ExecutionRegistry(tmp_path / "control" / "execution.sqlite3")


@pytest.fixture
def snapshot():
    return {
        "issue": {
            "id": "I_feature",
            "url": "https://github.com/o/r/issues/1",
            "repository_id": "R_repo",
        },
        "body": "A complete feature",
        "plan": None,
    }


def owner(registry, snapshot, run="run-one", **kwargs):
    return registry.claim(snapshot, run, "/runtime-one/workflow.sqlite3", **kwargs)


def test_two_stores_racing_for_same_feature_have_one_owner(registry, snapshot):
    barrier = Barrier(2)

    def attempt(index):
        other = ExecutionRegistry(registry.path)
        barrier.wait()
        try:
            return other.claim(snapshot, f"run-{index}", f"/runtime-{index}/workflow.sqlite3")
        except OwnershipConflict:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, [1, 2]))
    assert sum(result is not None for result in results) == 1
    assert registry.current("I_feature")["state"] == "active"


def test_replayed_admission_retains_snapshot_and_generation(registry, snapshot):
    token = owner(registry, snapshot)
    assert owner(registry, snapshot) == token
    with pytest.raises(OwnershipConflict, match="different GitHub inputs"):
        owner(registry, {**snapshot, "body": "Changed business scope"})
    with registry.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM execution_snapshots").fetchone()[0] == 1


def test_stopped_owner_cannot_be_replaced_by_an_unrelated_fresh_run(registry, snapshot):
    token = owner(registry, snapshot)
    registry.stop(token, "stop-1", {"stack_id": 42, "unfinished_workspace": "/owned/work"})
    with pytest.raises(OwnershipConflict, match="continue its owner"):
        owner(registry, snapshot, "run-two")
    successor = owner(registry, snapshot, "run-two", predecessor=token)
    assert successor["generation"] == 2
    with pytest.raises(OwnershipConflict, match="ownership changed"):
        with registry.mutation(token):
            pytest.fail("stale owner reached the mutation")
    with registry.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM execution_snapshots").fetchone()[0] == 2
        assert db.execute("SELECT COUNT(*) FROM execution_checkpoints").fetchone()[0] == 1


def test_ownership_does_not_expire_by_elapsed_time(registry, snapshot):
    owner(registry, snapshot)
    with registry.connect() as db:
        db.execute("UPDATE execution_claims SET updated_at='2000-01-01T00:00:00+00:00'")
    with pytest.raises(OwnershipConflict):
        owner(registry, snapshot, "run-two")


def test_parallel_workstreams_are_allowed_but_same_workstream_is_sequential(registry, snapshot):
    token = owner(registry, snapshot)
    registry.reserve_worker(token, "editor-1", "I_editor")
    registry.reserve_worker(token, "export-1", "I_export")
    with pytest.raises(OwnershipConflict, match="active writer"):
        registry.reserve_worker(token, "editor-2", "I_editor")
    with pytest.raises(OwnershipConflict, match="unfinished"):
        registry.stop(token, "stop-1", {})
    with pytest.raises(OwnershipConflict, match="cleanup"):
        registry.finish_worker(token, "editor-1", {"cleanup": "unknown"})
    registry.finish_worker(token, "editor-1", {"cleanup": "confirmed", "candidate": "c1"})
    registry.reserve_worker(token, "editor-2", "I_editor")


def test_external_effect_is_journalled_before_execution_and_never_blindly_replayed(
    registry,
    snapshot,
):
    token = owner(registry, snapshot)
    request = {"branch": "feat/existing", "head": "a" * 40}
    assert registry.intent(token, "publish-c1", "publish", request)["fresh"]
    assert registry.intent(token, "publish-c1", "publish", request) == {
        "fresh": False,
        "state": "pending",
        "result": None,
    }
    with pytest.raises(UnresolvedEffect):
        registry.stop(token, "stop-1", {})
    with pytest.raises(OwnershipConflict, match="identity changed"):
        registry.intent(token, "publish-c1", "publish", {**request, "head": "b" * 40})
    receipt = {"number": 7, "head": request["head"]}
    registry.finish_effect(token, "publish-c1", receipt)
    assert registry.intent(token, "publish-c1", "publish", request)["result"] == receipt
    registry.stop(token, "stop-1", {"pull_requests": [receipt]})


def test_in_flight_mutation_excludes_stop_even_before_effect_intent(registry, snapshot):
    token = owner(registry, snapshot)
    with registry.mutation(token):
        with pytest.raises(OwnershipConflict, match="operation in progress"):
            registry.stop(token, "stop-1", {})
    registry.stop(token, "stop-1", {})


def test_completed_effect_and_checkpoint_receipts_are_immutable(registry, snapshot):
    token = owner(registry, snapshot)
    registry.intent(token, "p", "publish", {"head": "a"})
    registry.finish_effect(token, "p", {"head": "a"})
    with pytest.raises(OwnershipConflict, match="receipt changed"):
        registry.finish_effect(token, "p", {"head": "b"})
    registry.checkpoint(token, "c", {"head": "a"})
    registry.checkpoint(token, "c", {"head": "a"})
    with pytest.raises(OwnershipConflict, match="checkpoint identity"):
        registry.checkpoint(token, "c", {"head": "b"})


def test_ten_cumulative_repairs_survive_handoff_and_learning_is_passive(registry, snapshot):
    token = owner(registry, snapshot)
    for n in range(5):
        result = registry.repair(token, f"repair-{n}", "candidate needs repair")
    assert result == {"used": 5, "maximum": 10, "learning_required": True}
    assert registry.repair(token, "repair-4", "candidate needs repair")["used"] == 5
    registry.stop(token, "stop-1", {})
    next_token = owner(registry, snapshot, "run-two", predecessor=token, maximum_repairs=20)
    for n in range(5, 10):
        result = registry.repair(next_token, f"repair-{n}", "candidate needs repair")
    assert result["maximum"] == 10
    assert result["used"] == 10
    with pytest.raises(OwnershipConflict, match="repair limit exhausted"):
        registry.repair(next_token, "repair-10", "candidate needs repair")


def test_confirmed_runtime_defect_marks_learning_without_consuming_product_allowance(
    registry,
    snapshot,
):
    token = owner(registry, snapshot)
    result = registry.repair(
        token, "runtime-defect", "confirmed runtime defect", devflow_defect=True
    )
    assert result == {"used": 0, "maximum": 10, "learning_required": True}


def test_registry_has_execution_records_and_no_business_feature_entity(registry):
    with registry.connect() as db:
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert tables
    assert all(name.startswith("execution_") for name in tables)
    assert "features" not in tables


def test_registry_rejects_symlink_and_public_directory(tmp_path):
    directory = tmp_path / "public"
    directory.mkdir(mode=0o755)
    with pytest.raises(ValueError, match="directory"):
        ExecutionRegistry(directory / "db")
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    target = private / "target"
    target.touch(mode=0o600)
    (private / "link").symlink_to(target)
    with pytest.raises(OSError):
        ExecutionRegistry(private / "link")


def test_registry_rejects_relative_paths():
    with pytest.raises(ValueError, match="absolute"):
        ExecutionRegistry(Path("relative.sqlite3"))
