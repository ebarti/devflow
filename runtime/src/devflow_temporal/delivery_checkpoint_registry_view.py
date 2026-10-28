"""Read-only projection queries over the checkpoint's already-held registry.

No constructor bootstrap, transaction, commit, close, filesystem lookup or repair
allowance renewal is available through this view. The caller owns its canonical
registry writer fence and retains it through the tracking acknowledgement.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing, contextmanager
from copy import deepcopy
from pathlib import Path

from .delivery_execution_registry import ExecutionRegistry

MAX_PROJECTION_ROWS = 512
MAX_PROJECTION_BYTES = 2 * 1024 * 1024
MAX_PROJECTION_CELL_BYTES = 256 * 1024


class PreparedRegistryProjection:
    """Bound and decode the captured issue history before admission locks.

    The original observation's supported-writer epochs must still validate in
    the exact ACK connection. The limits bound later SQL readers too, including
    repair allowance and pending-revision readers that use the query facade.
    """

    def __init__(self, spec, observation):
        self.observation = observation
        self.path = Path(spec["feature_delivery"]["registry"])
        self.issue_id = spec["feature_delivery"]["owner"]["issue_id"]
        if observation.path != self.path or observation.issue_id != self.issue_id:
            raise ValueError("checkpoint projection changed its observed registry owner")
        cells, rows = 0, 0
        for table_rows in observation.facts.values():
            rows += len(table_rows)
            if rows > MAX_PROJECTION_ROWS:
                raise ValueError("checkpoint feature projection exceeds its bounded history")
            for row in table_rows:
                for value in row.values():
                    if isinstance(value, str):
                        size = len(value.encode("utf-8"))
                        cells += size
                        if size > MAX_PROJECTION_CELL_BYTES or cells > MAX_PROJECTION_BYTES:
                            raise ValueError(
                                "checkpoint feature projection exceeds its bounded payload")
        self.checkpoints = {
            row["checkpoint_key"]: json.loads(row["content_json"])
            for row in observation.facts.get("execution_checkpoints", [])
        }
        # Reuse the owning registry validators against only the bounded captured
        # rows. Their JSON parsing, hashes and revision scans run here, never
        # under the root/registry/tracking acknowledgement fences.
        with closing(sqlite3.connect(":memory:")) as snapshot:
            snapshot.row_factory = sqlite3.Row
            tables = {
                "execution_checkpoints": (
                    "issue_id checkpoint_key generation content_digest content_json created_at"),
                "execution_repair_limits": "issue_id maximum learning_required",
                "execution_repairs": "issue_id repair_key generation reason created_at",
            }
            for table, columns in tables.items():
                names = columns.split()
                snapshot.execute(f"CREATE TABLE {table} ({','.join(names)})")
                snapshot.executemany(
                    f"INSERT INTO {table} VALUES ({','.join('?' for _ in names)})",
                    [tuple(row[name] for name in names)
                     for row in observation.facts.get(table, [])])
            self.budget = ExecutionRegistry._budget(snapshot, self.issue_id)
            self.pending_revision = ExecutionRegistry._pending_revision(snapshot, self.issue_id)

    def verify(self, db, spec):
        if (Path(spec["feature_delivery"]["registry"]) != self.path
                or spec["feature_delivery"]["owner"]["issue_id"] != self.issue_id):
            raise ValueError("checkpoint projection changed its observed registry owner")
        self.observation.validate_transaction(db)


class _Queries:
    def __init__(self, owner):
        self.owner = owner

    def execute(self, statement, parameters=()):
        self.owner._live()
        if not statement.lstrip().upper().startswith("SELECT "):
            raise ValueError("checkpoint registry projection permits queries only")
        return self.owner._db.execute(statement, parameters)


class CheckpointRegistryView:
    def __init__(self, db, spec, prepared):
        if type(prepared) is not PreparedRegistryProjection:
            raise ValueError("checkpoint projection requires bounded preflight history")
        self._db = db
        self._prepared = prepared
        self.path = Path(spec["feature_delivery"]["registry"])
        self._queries = _Queries(self)
        self._live()
        paths = [Path(row[2]) for row in db.execute("PRAGMA database_list") if row[1] == "main"]
        if paths != [self.path]:
            raise ValueError("checkpoint registry projection belongs to another canonical registry")
        prepared.verify(db, spec)

    def _live(self):
        try:
            active = self._db.in_transaction
        except sqlite3.Error as exc:
            raise ValueError("checkpoint registry projection fence has closed") from exc
        if not active:
            raise ValueError("checkpoint registry projection needs its held transaction")

    def for_spec(self, spec):
        self._live()
        if (Path(spec["feature_delivery"]["registry"]) != self.path
                or spec["feature_delivery"]["owner"]["issue_id"] != self._prepared.issue_id):
            raise ValueError("checkpoint parent projection changed its canonical registry")
        return self

    @contextmanager
    def connect(self):
        self._live()
        yield self._queries

    def current(self, issue_id):
        self._issue(issue_id)
        row = self._queries.execute("SELECT * FROM execution_claims WHERE issue_id=?",
                                    (issue_id,)).fetchone()
        return dict(row) if row else None

    def checkpoints(self, issue_id):
        self._issue(issue_id)
        return deepcopy(self._prepared.checkpoints)

    def budget(self, issue_id):
        self._issue(issue_id)
        return deepcopy(self._prepared.budget)

    def _issue(self, issue_id):
        self._live()
        if issue_id != self._prepared.issue_id:
            raise ValueError("checkpoint projection changed its bounded issue")

    def _settled(self, db, issue_id):
        self._issue(issue_id)
        return ExecutionRegistry._settled(db, issue_id)

    def _pending_revision(self, db, issue_id):
        self._issue(issue_id)
        if db is not self._queries:
            raise ValueError("checkpoint projection changed its held query view")
        return deepcopy(self._prepared.pending_revision)

    token = staticmethod(ExecutionRegistry.token)
