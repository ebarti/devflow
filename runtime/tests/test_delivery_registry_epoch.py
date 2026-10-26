from __future__ import annotations

import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from devflow_temporal.delivery_execution_registry import ExecutionRegistry
from devflow_temporal.delivery_registry_epoch import EpochUnavailable, observe

URL = "https://github.com/o/r/issues/1"
STORE = "/runtime/workflow.sqlite3"


@pytest.fixture
def setup(tmp_path):
    registry = ExecutionRegistry(tmp_path / "registry" / "db.sqlite3")
    token = registry.claim(
        {"issue": {"id": "i", "url": URL, "repository_id": "r"}}, "run", STORE
    )
    return registry, token


def observation(registry, issue="i", url=URL, store=STORE):
    return observe(registry.path, issue, url, store)


def test_old_writer_aba_and_unrelated_issue_isolation(setup):
    registry, token = setup
    with observation(registry) as observed:

        def old_writer():
            with sqlite3.connect(registry.path) as db:
                db.execute(
                    "UPDATE execution_claims SET state='draining' WHERE issue_id='i'"
                )
                db.execute(
                    "UPDATE execution_claims SET state='active' WHERE issue_id='i'"
                )

        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(old_writer).result()
        assert registry.current("i")["state"] == "active"
        with pytest.raises(EpochUnavailable, match="changed"):
            observed.validate()
    with observation(registry) as observed:
        with sqlite3.connect(registry.path) as db:
            db.execute(
                "INSERT INTO execution_workers VALUES ('other','w','s',1,'reserved',NULL)"
            )
        observed.validate()
    assert registry.current("i")["generation"] == token["generation"]


def test_rollback_and_budget_replay(setup):
    registry, token = setup
    before = registry.budget("i")
    with observation(registry) as observed:
        with sqlite3.connect(registry.path) as db:
            db.execute(
                "UPDATE execution_claims SET state='draining' WHERE issue_id='i'"
            )
            db.rollback()
        observed.validate()
    assert registry.budget("i") == before
    assert (
        registry.claim(
            {"issue": {"id": "i", "url": URL, "repository_id": "r"}}, "run", STORE
        )
        == token
    )
    assert registry.budget("i") == before


def test_same_owner_drain_and_worker_and_checkpoint(setup):
    registry, token = setup
    with observation(registry) as observed:
        registry.drain(token)
        with pytest.raises(EpochUnavailable):
            observed.validate()
    with observation(registry) as observed:
        registry.revision_checkpoint(token, "plan-revision:test", {"a": 1})
        with pytest.raises(EpochUnavailable):
            observed.validate()


def test_delete_reinsert_replace_tracks_displaced_snapshot_issue(setup):
    registry, _ = setup
    with observation(registry) as observed:
        with sqlite3.connect(registry.path) as db:
            db.execute("DELETE FROM execution_claims WHERE issue_id='i'")
            db.execute(
                "INSERT INTO execution_claims VALUES ('i',?,'r','run',?,1,'active','now')",
                (URL, STORE),
            )
        with pytest.raises(EpochUnavailable):
            observed.validate()
    with observation(registry) as observed:
        with sqlite3.connect(registry.path) as db:
            db.execute(
                "INSERT OR REPLACE INTO execution_snapshots "
                "VALUES (?, 'run','other',1,'d','{}','now')",
                (STORE,),
            )
        with pytest.raises(EpochUnavailable):
            observed.validate()
    with observation(registry) as observed:
        assert (
            dict(observed.epochs and [(x[0], x[2]) for x in observed.epochs])["issue"]
            > 0
        )


def test_file_replacement_and_copied_bytes_refused(setup, tmp_path):
    registry, _ = setup
    with observation(registry) as observed:
        other = tmp_path / "replacement"
        other.write_bytes(registry.path.read_bytes())
        other.chmod(0o600)
        os.replace(other, registry.path)
        with pytest.raises(EpochUnavailable, match="identity"):
            observed.validate()


def test_missing_or_modified_schema_refused_without_healing(setup):
    registry, _ = setup
    with sqlite3.connect(registry.path) as db:
        db.execute("DROP TRIGGER authority_epoch_v1_execution_workers_update")
    with pytest.raises(EpochUnavailable, match="schema"):
        observation(registry)
    with pytest.raises(EpochUnavailable, match="schema"):
        ExecutionRegistry(registry.path)
    with sqlite3.connect(registry.path) as db:
        assert not db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='authority_epoch_v1_execution_workers_update'"
        ).fetchone()


def test_epoch_overflow_rolls_back_authority_mutation(setup):
    registry, _ = setup
    with sqlite3.connect(registry.path) as db:
        db.execute(
            "UPDATE execution_authority_epochs SET epoch=? WHERE kind='issue' AND key='i'",
            (9223372036854775807,),
        )
    with pytest.raises(sqlite3.IntegrityError, match="overflow"):
        with sqlite3.connect(registry.path) as db:
            db.execute(
                "UPDATE execution_claims SET state='draining' WHERE issue_id='i'"
            )
    assert registry.current("i")["state"] == "active"


def test_legacy_custody_and_store_mutations_are_scoped(setup):
    registry, _ = setup
    with observation(registry) as observed:
        with sqlite3.connect(registry.path) as db:
            db.execute("INSERT INTO execution_legacy_custody VALUES (?, '[]')", (URL,))
        with pytest.raises(EpochUnavailable):
            observed.validate()
    with observation(registry) as observed:
        with sqlite3.connect(registry.path) as db:
            db.execute(
                "INSERT INTO execution_migrated_stores VALUES (?, 'now')", (STORE,)
            )
        with pytest.raises(EpochUnavailable):
            observed.validate()


def test_admission_file_pinning_detects_changes_and_replacement(tmp_path):
    from devflow_temporal.delivery_registry_epoch import PinnedAdmissionFile

    path = tmp_path / "admission.json"
    path.write_bytes(b'{"original":true}')
    with PinnedAdmissionFile(path) as pinned:
        assert pinned.content == b'{"original":true}'
        path.write_bytes(b'{"changed":true}')
        with pytest.raises(EpochUnavailable, match="changed"):
            pinned.validate()
    path.write_bytes(b"original")
    with PinnedAdmissionFile(path) as pinned:
        other = tmp_path / "copy"
        other.write_bytes(b"original")
        os.replace(other, path)
        with pytest.raises(EpochUnavailable, match="changed"):
            pinned.validate()


def test_unknown_schema_is_refused(setup):
    registry, _ = setup
    with sqlite3.connect(registry.path) as db:
        db.execute("ALTER TABLE execution_workers ADD COLUMN future_authority TEXT")
    with pytest.raises(EpochUnavailable, match="authority table"):
        observation(registry)


def test_worker_effect_and_repair_changes_invalidate(setup):
    registry, token = setup
    with observation(registry) as observed:
        registry.reserve_worker(token, "worker", "stream")
        with pytest.raises(EpochUnavailable):
            observed.validate()
    with observation(registry) as observed:
        registry.intent(token, "effect", "publish", {"x": 1})
        with pytest.raises(EpochUnavailable):
            observed.validate()
    with observation(registry) as observed:
        registry.repair(token, "repair", "reason")
        with pytest.raises(EpochUnavailable):
            observed.validate()
    assert registry.budget("i")["used"] == 1
    registry.repair(token, "repair", "reason")
    assert registry.budget("i")["used"] == 1


def test_observation_refuses_active_writer_without_waiting(setup):
    registry, _ = setup
    with observation(registry) as observed:
        db = sqlite3.connect(registry.path)
        try:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE execution_claims SET state='draining' WHERE issue_id='i'"
            )
            with pytest.raises(EpochUnavailable):
                observed.validate()
        finally:
            db.rollback()
            db.close()
        observed.validate()


def test_missing_registry_is_not_created(tmp_path):
    path = tmp_path / 'missing' / 'registry'
    with pytest.raises(EpochUnavailable):
        observe(path, 'i', URL, STORE)
    assert not path.parent.exists()


def test_unrecognized_unique_index_is_refused(setup):
    registry, _ = setup
    with sqlite3.connect(registry.path) as db:
        db.execute('CREATE UNIQUE INDEX unexpected ON execution_snapshots(input_digest)')
    with pytest.raises(EpochUnavailable, match='index'):
        observation(registry)


@pytest.mark.parametrize(
    'table', ['execution_claims', 'execution_authority_epochs', 'execution_epoch_meta']
)
def test_unknown_triggers_are_refused_at_observation_and_bootstrap(setup, table):
    registry, _ = setup
    with sqlite3.connect(registry.path) as db:
        db.execute(f'''CREATE TRIGGER custom_reset AFTER UPDATE ON {table} BEGIN
            UPDATE execution_authority_epochs SET epoch=1 WHERE kind='issue' AND key='i';
        END''')
    with pytest.raises(EpochUnavailable, match='triggers'):
        observation(registry)
    with pytest.raises(EpochUnavailable, match='triggers'):
        ExecutionRegistry(registry.path)


def test_first_bootstrap_rolls_back_if_authority_table_has_unknown_trigger(setup):
    registry, _ = setup
    with sqlite3.connect(registry.path) as db:
        names = [row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' "
            "AND name LIKE 'authority_epoch_%'")]
        for name in names:
            db.execute(f'DROP TRIGGER "{name}"')
        db.execute('DROP TABLE execution_authority_epochs')
        db.execute('DROP TABLE execution_epoch_meta')
        db.execute('CREATE TRIGGER custom_reset AFTER UPDATE ON execution_claims '
                   'BEGIN SELECT 1; END')
    with pytest.raises(EpochUnavailable, match='triggers'):
        ExecutionRegistry(registry.path)
    with sqlite3.connect(registry.path) as db:
        assert not db.execute("SELECT 1 FROM sqlite_master "
                              "WHERE name='execution_epoch_meta'").fetchone()
