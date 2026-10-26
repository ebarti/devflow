"""Observation never creates or changes the original tracking database and sidecars."""

import asyncio
import hashlib
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from devflow_temporal import delivery_store
from devflow_temporal.delivery_execution_config import observation_mode


def _store(path):
    store = delivery_store.DeliveryStore.__new__(delivery_store.DeliveryStore)
    store.config = SimpleNamespace(tracking_db=path)
    store.read_only = False
    return store


def _inventory(root):
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.iterdir()
        if path.is_file()
    }


def _live_wal(path):
    writer = sqlite3.connect(path)
    assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    writer.execute("CREATE TABLE data (value TEXT)")
    writer.commit()
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    writer.execute("INSERT INTO data VALUES ('only-in-wal')")
    writer.commit()
    return writer


def test_missing_tracking_parent_and_db_are_never_created(tmp_path):
    path = tmp_path / "missing" / "tracking.sqlite3"
    with observation_mode(), pytest.raises(sqlite3.OperationalError):
        with _store(path)._connect():
            pass
    assert not path.parent.exists()


def test_non_wal_observation_never_changes_db_or_creates_sidecars(tmp_path):
    path = tmp_path / "tracking.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE data (value TEXT)")
        db.execute("INSERT INTO data VALUES ('original')")
    before = _inventory(tmp_path)
    with observation_mode(), _store(path)._connect() as db:
        assert db.execute("SELECT value FROM data").fetchone()[0] == "original"
        with pytest.raises(sqlite3.OperationalError):
            db.execute("INSERT INTO data VALUES ('forbidden')")
    assert _inventory(tmp_path) == before


def test_wal_only_commit_is_visible_without_touching_source_or_sidecars(tmp_path, monkeypatch):
    path = tmp_path / "tracking.sqlite3"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(delivery_store.tempfile, "tempdir", str(scratch))
    writer = _live_wal(path)
    try:
        before = _inventory(tmp_path)
        with observation_mode(), _store(path)._connect() as db:
            assert db.execute("SELECT value FROM data").fetchone()[0] == "only-in-wal"
        assert _inventory(tmp_path) == before
        assert list(scratch.iterdir()) == []
    finally:
        writer.close()


def test_wal_without_existing_shm_refuses_without_creating_it(tmp_path):
    source = tmp_path / "source.sqlite3"
    writer = _live_wal(source)
    target_dir = tmp_path / "copy"
    target_dir.mkdir()
    target = target_dir / "tracking.sqlite3"
    target.write_bytes(source.read_bytes())
    Path(str(target) + "-wal").write_bytes(Path(str(source) + "-wal").read_bytes())
    before = _inventory(target_dir)
    try:
        with observation_mode(), pytest.raises(sqlite3.OperationalError, match="sidecar"):
            with _store(target)._connect():
                pass
        assert _inventory(target_dir) == before
        assert not Path(str(target) + "-shm").exists()
    finally:
        writer.close()


def test_source_change_during_copy_refuses_snapshot(tmp_path, monkeypatch):
    path = tmp_path / "tracking.sqlite3"
    writer = _live_wal(path)
    original = delivery_store._copy_tracking_stream
    calls = []

    def copying(source, destination, deadline, size):
        original(source, destination, deadline, size)
        calls.append(True)
        if len(calls) == 1:
            writer.execute("INSERT INTO data VALUES ('concurrent')")
            writer.commit()

    monkeypatch.setattr(delivery_store, "_copy_tracking_stream", copying)
    try:
        with observation_mode(), pytest.raises(sqlite3.OperationalError, match="changed"):
            with _store(path)._connect():
                pass
        assert calls == [True, True]
    finally:
        writer.close()


def test_oversized_tracking_store_refuses_without_creating_sidecars(tmp_path):
    path = tmp_path / "tracking.sqlite3"
    with path.open("wb") as stream:
        stream.truncate(delivery_store._OBSERVATION_MAX_BYTES + 1)
    before = _inventory(tmp_path)
    with observation_mode(), pytest.raises(sqlite3.OperationalError, match="oversized"):
        with _store(path)._connect():
            pass
    assert _inventory(tmp_path) == before


def test_tracking_snapshot_deadline_refuses_and_cleans_private_copy(tmp_path, monkeypatch):
    path = tmp_path / "tracking.sqlite3"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(delivery_store.tempfile, "tempdir", str(scratch))
    monkeypatch.setattr(delivery_store, "_OBSERVATION_COPY_SECONDS", 0)
    writer = _live_wal(path)
    try:
        before = _inventory(tmp_path)
        with observation_mode(), pytest.raises(sqlite3.OperationalError, match="timed out"):
            with _store(path)._connect():
                pass
        assert _inventory(tmp_path) == before
        assert list(scratch.iterdir()) == []
    finally:
        writer.close()


def test_tracking_database_symlink_is_refused_without_touching_target(tmp_path):
    actual = tmp_path / "actual.sqlite3"
    with sqlite3.connect(actual) as db:
        db.execute("CREATE TABLE data (value TEXT)")
    link = tmp_path / "tracking.sqlite3"
    link.symlink_to(actual)
    before = _inventory(tmp_path)
    with observation_mode(), pytest.raises(sqlite3.OperationalError):
        with _store(link)._connect():
            pass
    assert _inventory(tmp_path) == before
    assert link.is_symlink()
    assert not Path(str(link) + "-wal").exists()


def test_tracking_inode_replacement_during_copy_refuses_snapshot(tmp_path, monkeypatch):
    path = tmp_path / "tracking.sqlite3"
    replacement = tmp_path / "replacement.sqlite3"
    for item in (path, replacement):
        with sqlite3.connect(item) as db:
            db.execute("CREATE TABLE data (value TEXT)")
    original = delivery_store._copy_tracking_stream

    def swapping(source, destination, deadline, size):
        original(source, destination, deadline, size)
        replacement.replace(path)

    monkeypatch.setattr(delivery_store, "_copy_tracking_stream", swapping)
    with observation_mode(), pytest.raises(sqlite3.OperationalError, match="changed"):
        with _store(path)._connect():
            pass
    assert not Path(str(path) + "-wal").exists()
    assert not Path(str(path) + "-journal").exists()


def test_nonprivate_tracking_mode_refuses_without_chmod(tmp_path):
    path = tmp_path / "tracking.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE data (value TEXT)")
    path.chmod(0o666)
    with observation_mode(), pytest.raises(sqlite3.OperationalError, match="ownership"):
        with _store(path)._connect():
            pass
    assert path.stat().st_mode & 0o777 == 0o666


def test_ordinary_read_only_connection_does_not_make_observation_copy(tmp_path, monkeypatch):
    path = tmp_path / "tracking.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE data (value TEXT)")
        db.execute("INSERT INTO data VALUES ('visible')")
    store = _store(path)
    store.read_only = True

    def unexpected(_path):
        raise AssertionError("ordinary status reads must remain bounded")

    monkeypatch.setattr(delivery_store, "_observation_tracking_connection", unexpected)
    with store._connect() as db:
        assert db.execute("SELECT value FROM data").fetchone()[0] == "visible"


def test_tracking_wal_symlink_is_refused_without_touching_source(tmp_path):
    source = tmp_path / "source.sqlite3"
    writer = _live_wal(source)
    target_dir = tmp_path / "copy"
    target_dir.mkdir()
    target = target_dir / "tracking.sqlite3"
    target.write_bytes(source.read_bytes())
    Path(str(target) + "-wal").symlink_to(Path(str(source) + "-wal"))
    Path(str(target) + "-shm").write_bytes(Path(str(source) + "-shm").read_bytes())
    before = _inventory(target_dir)
    try:
        with observation_mode(), pytest.raises(sqlite3.OperationalError):
            with _store(target)._connect():
                pass
        assert _inventory(target_dir) == before
        assert Path(str(target) + "-wal").is_symlink()
    finally:
        writer.close()


def test_one_snapshot_is_reused_for_all_tracking_connections(tmp_path, monkeypatch):
    path = tmp_path / "tracking.sqlite3"
    writer = _live_wal(path)
    original = delivery_store._copy_tracking_stream
    copies = []

    def copying(source, destination, deadline, size):
        copies.append(source.name)
        original(source, destination, deadline, size)

    monkeypatch.setattr(delivery_store, "_copy_tracking_stream", copying)
    try:
        with observation_mode():
            for _ in range(2):
                with _store(path)._connect() as db:
                    assert db.execute("SELECT value FROM data").fetchone()[0] == "only-in-wal"
            assert len(copies) == 2  # one DB and one WAL, not a new copy per connection
    finally:
        writer.close()


def test_tracking_change_between_connections_refuses_entire_observation(tmp_path):
    path = tmp_path / "tracking.sqlite3"
    writer = _live_wal(path)
    try:
        with pytest.raises(sqlite3.OperationalError, match="changed"):
            with observation_mode():
                with _store(path)._connect() as db:
                    assert db.execute("SELECT count(*) FROM data").fetchone()[0] == 1
                writer.execute("INSERT INTO data VALUES ('new-commit')")
                writer.commit()
                with _store(path)._connect():
                    pytest.fail("changed source must not be mixed into another observation")
    finally:
        writer.close()


def test_tracking_change_after_last_read_is_detected_at_scope_exit(tmp_path):
    path = tmp_path / "tracking.sqlite3"
    writer = _live_wal(path)
    try:
        with pytest.raises(sqlite3.OperationalError, match="changed"):
            with observation_mode():
                with _store(path)._connect() as db:
                    assert db.execute("SELECT count(*) FROM data").fetchone()[0] == 1
                writer.execute("INSERT INTO data VALUES ('new-commit')")
                writer.commit()
    finally:
        writer.close()


@pytest.mark.asyncio
async def test_cancelled_caller_cannot_create_a_late_tracking_snapshot(tmp_path, monkeypatch):
    path = tmp_path / "tracking.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE data (value TEXT)")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(delivery_store.tempfile, "tempdir", str(scratch))
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    errors = []

    def late_worker():
        started.set()
        try:
            if not release.wait(5):
                raise AssertionError("test did not release worker")
            with _store(path)._connect():
                raise AssertionError("closed scope admitted a late tracking snapshot")
        except Exception as exc:
            errors.append(exc)
        finally:
            finished.set()

    with observation_mode():
        task = asyncio.create_task(asyncio.to_thread(late_worker))
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    release.set()
    assert await asyncio.to_thread(finished.wait, 5)
    assert len(errors) == 1
    assert isinstance(errors[0], sqlite3.OperationalError)
    assert "scope is closed" in str(errors[0])
    assert list(scratch.iterdir()) == []


def test_public_assessment_reports_unknown_when_source_changes_after_last_read(
    tmp_path, monkeypatch,
):
    from devflow_temporal import delivery_lifecycle

    path = tmp_path / "tracking.sqlite3"
    writer = _live_wal(path)
    store = _store(path)

    def assessment(*_args, **_kwargs):
        with store._connect() as db:
            assert db.execute("SELECT count(*) FROM data").fetchone()[0] == 1
        writer.execute("INSERT INTO data VALUES ('concurrent')")
        writer.commit()
        return {"eligible": True}

    monkeypatch.setattr(delivery_lifecycle, "_assess_transition", assessment)
    try:
        result = delivery_lifecycle.assess_transition(store, "run-1", "continue_feature", {})
        assert result["eligible"] is False
        assert "tracking_store_unavailable" in {item["code"] for item in result["blockers"]}
    finally:
        writer.close()


@pytest.mark.asyncio
async def test_cancelled_caller_closes_an_already_open_tracking_snapshot(tmp_path, monkeypatch):
    path = tmp_path / "tracking.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE data (value TEXT)")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(delivery_store.tempfile, "tempdir", str(scratch))
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    errors = []

    def worker():
        try:
            with _store(path)._connect() as db:
                assert db.execute("SELECT count(*) FROM data").fetchone()[0] == 0
                started.set()
                if not release.wait(5):
                    raise AssertionError("test did not release worker")
        except Exception as exc:
            errors.append(exc)
        finally:
            finished.set()

    with observation_mode():
        task = asyncio.create_task(asyncio.to_thread(worker))
        assert await asyncio.to_thread(started.wait, 5)
        assert list(scratch.iterdir())
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert list(scratch.iterdir()) == []
    release.set()
    assert await asyncio.to_thread(finished.wait, 5)
    assert len(errors) == 1
    assert isinstance(errors[0], sqlite3.OperationalError)
    assert list(scratch.iterdir()) == []
