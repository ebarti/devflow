"""Bounded retained-work tracking observation on one live SQLite connection."""

from __future__ import annotations

import json
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from . import delivery_checkpoints as checkpoints
from .delivery_registry_epoch import PinnedRegistry

_TABLES = (
    "delivery_runs", "delivery_attempts", "delivery_effects", "delivery_preparations",
    "delivery_merge_requests", "delivery_native_renewal_anchors", "delivery_execution_authority",
    "delivery_merge_executions", "delivery_publication_executors", "claims",
    "delivery_pause_retained_ready",
)


class RetainedTrackingObservation:
    """Any other connection's commit invalidates this observation, including ABA."""

    def __init__(self, store, run_id):
        self.store, self.run_id = store, run_id
        self.pin = PinnedRegistry(Path(store.config.tracking_db))
        self.db = None
        try:
            self.db = sqlite3.connect(self.pin.path.as_uri() + "?mode=ro", uri=True, timeout=0)
            self.db.row_factory = sqlite3.Row
            self.db.execute("PRAGMA query_only=ON")
            steps = 0

            def progress():
                nonlocal steps
                steps += 1000
                return int(steps > 1000000)

            self.db.set_progress_handler(progress, 1000)
            self.version = self.db.execute("PRAGMA data_version").fetchone()[0]
            self.db.execute("BEGIN")
            self.schema = self.db.execute("PRAGMA schema_version").fetchone()[0]
            work = self.db.execute("SELECT CASE WHEN length(work_id)<=512 THEN work_id END "
                                   "FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            if work is None or not isinstance(work[0], str):
                raise ValueError("retained tracking work identity is unavailable")
            usage = [0, 0]
            for table in _TABLES:
                if not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                                       (table,)).fetchone():
                    continue
                if table == "claims":
                    where, args = "work_id=?", (work[0],)
                elif table == "delivery_pause_retained_ready":
                    where, args = "root_id=?", (run_id,)
                elif table == "delivery_publication_executors":
                    prefix = "publish:" + run_id + ":"
                    where, args = "effect_key>=? AND effect_key<?", (prefix, prefix[:-1] + ";")
                else:
                    where, args = "run_id=?", (run_id,)
                columns = [row[1] for row in self.db.execute(f"PRAGMA table_info({table})")]
                if (not columns or len(columns) > 64
                        or any(not re.fullmatch(r"[a-zA-Z_][a-zA-Z_0-9]*", col)
                               for col in columns)):
                    raise ValueError("retained tracking table schema is unsupported")
                lengths = [f"coalesce(length(CAST({col} AS BLOB)),0)" for col in columns]
                for largest, total in self.db.execute(
                    f"SELECT max({','.join(lengths)}),{'+'.join(lengths)} FROM {table} "
                    f"WHERE {where} LIMIT ?", (*args, 513 - usage[0]),
                ):
                    usage[0] += 1
                    usage[1] += total
                    if usage[0] > 512 or largest > 256 * 1024 or usage[1] > 4 * 1024 * 1024:
                        raise ValueError("retained tracking snapshot exceeds its bounded inventory")
            self.row = checkpoints._row(self.db, run_id)
            self.frozen = checkpoints._frozen(self.db, self.row)
            claim = store.state.claim_for(self.db, self.row["work_id"])
            self.claim = dict(claim) if claim else None
            self.db.commit()
            if self.db.execute("PRAGMA data_version").fetchone()[0] != self.version:
                raise ValueError("retained tracking changed during bounded observation")
            self.pin.validate_identity()
        except BaseException:
            self.close()
            raise

    def saved_command(self, key, run_id, request_sha256):
        self._unchanged()
        row = self.db.execute("SELECT run_id,request_digest,CASE WHEN "
                              "length(CAST(response_json AS BLOB))<=16384 THEN response_json END "
                              "FROM delivery_commands WHERE command_id=?", (key,)).fetchone()
        self._unchanged()
        if row is None:
            return None
        if row[0] != run_id or row[1] != request_sha256 or not isinstance(row[2], str):
            raise ValueError("retained command belongs to different or oversized inputs")
        return json.loads(row[2])

    def _unchanged(self):
        self.pin.validate_identity()
        if self.db is None or self.db.execute("PRAGMA data_version").fetchone()[0] != self.version:
            raise ValueError("retained tracking changed since its original observation")

    def verify_token(self, db):
        self._unchanged()
        self.pin.validate_transaction(db)
        if db.execute("PRAGMA schema_version").fetchone()[0] != self.schema:
            raise ValueError("retained tracking schema changed")

    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None
        self.pin.close()


class RetainedSQLPolicy:
    """Restrict this ACK's writes instead of rereading large post-hook state."""

    def __init__(self):
        self.phase = "idle"

    @contextmanager
    def allow(self, phase):
        previous, self.phase = self.phase, phase
        try:
            yield
        finally:
            self.phase = previous

    def __call__(self, action, table, column, database, trigger):
        writes = {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}
        if action in writes:
            permitted = (
                self.phase == "record" and action == sqlite3.SQLITE_INSERT
                and table in {"delivery_commands", "delivery_events", "delivery_phase_events"}
            ) or (self.phase == "ready" and action == sqlite3.SQLITE_INSERT
                  and table == "delivery_pause_retained_ready")
            return sqlite3.SQLITE_OK if permitted else sqlite3.SQLITE_DENY
        forbidden = {
            sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH, sqlite3.SQLITE_ALTER_TABLE,
            sqlite3.SQLITE_CREATE_TABLE, sqlite3.SQLITE_CREATE_INDEX, sqlite3.SQLITE_CREATE_TRIGGER,
            sqlite3.SQLITE_DROP_TABLE, sqlite3.SQLITE_DROP_INDEX, sqlite3.SQLITE_DROP_TRIGGER,
        }
        if action in forbidden or (action in {sqlite3.SQLITE_TRANSACTION, sqlite3.SQLITE_SAVEPOINT}
                                   and self.phase != "idle"):
            return sqlite3.SQLITE_DENY
        if action == sqlite3.SQLITE_PRAGMA:
            scalar = table in {"schema_version", "data_version", "database_list"} and column is None
            metadata = self.phase == "idle" and table in {
                "table_info", "index_list", "index_info", "index_xinfo"}
            if not scalar and not metadata:
                return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK
