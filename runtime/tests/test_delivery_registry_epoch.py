from __future__ import annotations

import sqlite3

import pytest

from devflow_temporal.delivery_execution_registry import ExecutionRegistry
from devflow_temporal.delivery_registry_epoch import observe

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


def test_registry_fence_blocks_old_writer_through_tracking_commit(setup, tmp_path):
    import subprocess
    import sys

    registry, _ = setup
    tracking = tmp_path / "tracking.db"
    with sqlite3.connect(tracking) as db:
        db.execute("CREATE TABLE acknowledgements (value TEXT)")
    with observation(registry) as observed:
        with observed.writer_fence() as registry_db:
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                registry_db.execute("UPDATE execution_claims SET state='stopped'")
            script = (
                "import sqlite3,sys; db=sqlite3.connect(sys.argv[1],timeout=0); "
                'db.execute("BEGIN IMMEDIATE")'
            )
            child = subprocess.run(
                [sys.executable, "-c", script, str(registry.path)],
                capture_output=True,
                text=True,
                timeout=5,
            )
            assert child.returncode != 0 and "database is locked" in child.stderr
            with sqlite3.connect(tracking, timeout=0) as db:
                db.execute("BEGIN IMMEDIATE")
                observed.validate_transaction(registry_db)
                db.execute("INSERT INTO acknowledgements VALUES ('accepted')")
                db.commit()
        with sqlite3.connect(tracking) as db:
            assert (
                db.execute("SELECT value FROM acknowledgements").fetchone()[0]
                == "accepted"
            )
