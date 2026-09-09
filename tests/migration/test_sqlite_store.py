import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from devflow.adapters.sqlite_store import SQLiteStore
from devflow.errors import WorkflowError


def test_private_store_backup_restore_preserves_history_and_artifacts(tmp_path):
    store = SQLiteStore(tmp_path / "state")
    artifact = store.put_artifact(b"Synthetic retained evidence")
    with store.transaction() as db:
        db.execute("INSERT INTO works VALUES (?,?,?)", ("synthetic", 1, '{"revision":1}'))
        db.execute(
            "INSERT INTO records VALUES (?,?,?)",
            ("synthetic", "synthetic-record", '{"immutable":true}'),
        )
    backup = store.backup()
    with store.transaction() as db:
        db.execute("UPDATE works SET revision=2,state=?", ('{"revision":2}',))
    store.restore(backup)
    assert store.read("synthetic")["revision"] == 1
    store.require_artifact(artifact)
    with store.connect() as db:
        assert db.execute("SELECT payload FROM records").fetchone()[0] == '{"immutable":true}'
    assert len(list((store.root / "backups").glob("*.sqlite3"))) == 2
    for path in (store.root, store.root / "artifacts", store.root / "backups"):
        assert path.stat().st_mode & 0o777 == 0o700
    for path in (store.path, backup, store.root / "artifacts" / artifact):
        assert path.stat().st_mode & 0o777 == 0o600


def test_transaction_interruption_rolls_back_every_write(tmp_path):
    store = SQLiteStore(tmp_path)
    with pytest.raises(KeyboardInterrupt):
        with store.transaction() as db:
            db.execute("INSERT INTO works VALUES ('synthetic',1,'{}')")
            db.execute("INSERT INTO operations VALUES ('synthetic','hash','{}')")
            raise KeyboardInterrupt
    with store.connect() as db:
        assert db.execute("SELECT count(*) FROM works").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM operations").fetchone()[0] == 0


def test_unknown_new_store_version_stops_admissions_without_modifying_data(tmp_path):
    store = SQLiteStore(tmp_path)
    with store.transaction() as db:
        db.execute("PRAGMA user_version=999")
        db.execute("INSERT INTO works VALUES ('synthetic',1,'{}')")
    with pytest.raises(WorkflowError, match="newer"):
        SQLiteStore(tmp_path)
    with sqlite3.connect(store.path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 999
        assert db.execute("SELECT count(*) FROM works").fetchone()[0] == 1


def test_unversioned_foreign_database_not_adopted(tmp_path):
    path = tmp_path / "state.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE foreign_data (value TEXT)")
        db.execute("INSERT INTO foreign_data VALUES ('synthetic sentinel')")
    with pytest.raises(WorkflowError, match="Unversioned"):
        SQLiteStore(tmp_path)
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 0
        assert db.execute("SELECT * FROM foreign_data").fetchone()[0] == "synthetic sentinel"
        assert db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == [
            ("foreign_data",)
        ]


def test_parallel_artifact_ingestion_has_one_intact_object(tmp_path):
    store = SQLiteStore(tmp_path)
    with ThreadPoolExecutor(max_workers=4) as executor:
        hashes = list(executor.map(store.put_artifact, [b"Synthetic identical evidence"] * 8))
    assert len(set(hashes)) == 1
    store.require_artifact(hashes[0])
    assert len(list((store.root / "artifacts").iterdir())) == 1


def test_symlink_state_and_artifact_refused(tmp_path):
    target = tmp_path / "foreign"
    target.mkdir()
    link = tmp_path / "symlink"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(WorkflowError, match="symlinks"):
        SQLiteStore(link)
    store = SQLiteStore(tmp_path / "state")
    key = store.put_artifact(b"Synthetic")
    path = store.root / "artifacts" / key
    path.unlink()
    path.symlink_to(target)
    with pytest.raises(WorkflowError, match="unavailable"):
        store.require_artifact(key)


def test_backup_snapshot_contains_wal_commits(tmp_path):
    store = SQLiteStore(tmp_path)
    with store.connect() as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("INSERT INTO works VALUES ('synthetic',1,'{}')")
        backup = store.backup()
        with sqlite3.connect(backup) as snapshot:
            assert snapshot.execute("SELECT count(*) FROM works").fetchone()[0] == 1


def test_hardlinked_database_refused_without_changing_foreign_contents(tmp_path):
    foreign = tmp_path / "foreign.sqlite3"
    with sqlite3.connect(foreign) as db:
        db.execute("CREATE TABLE sentinel (value TEXT)")
    state = tmp_path / "state"
    state.mkdir()
    os.link(foreign, state / "state.sqlite3")
    with pytest.raises(WorkflowError, match="solely linked"):
        SQLiteStore(state)
    with sqlite3.connect(foreign) as db:
        assert db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == [
            ("sentinel",)
        ]


def test_restore_refuses_to_replace_active_work(tmp_path):
    store = SQLiteStore(tmp_path)
    backup = store.backup()
    with store.transaction() as db:
        db.execute("INSERT INTO claims VALUES ('synthetic','synthetic/repo','attempt-1','host-1')")
    with pytest.raises(WorkflowError, match="active claim"):
        store.restore(backup)
    with store.connect() as db:
        assert db.execute("SELECT count(*) FROM claims").fetchone()[0] == 1
