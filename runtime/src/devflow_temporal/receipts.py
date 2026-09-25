"""Single-host durable activity receipts for non-idempotent model work."""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4


@dataclass(frozen=True)
class Claim:
    state: Literal["new", "finished", "ambiguous"]
    generation: str | None = None
    result: dict[str, Any] | None = None


class ReceiptStore:
    def __init__(self, state_dir: Path) -> None:
        state_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        os.chmod(state_dir, 0o700)
        self.path = state_dir / "receipts.sqlite3"
        with self._connect() as db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS receipts (
                    run_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    iteration INTEGER NOT NULL,
                    candidate_id TEXT NOT NULL,
                    generation TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('running', 'finished')),
                    result_json TEXT,
                    PRIMARY KEY (run_id, role, iteration, candidate_id)
                )"""
            )
        os.chmod(self.path, 0o600)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=30)
        try:
            db.execute("PRAGMA journal_mode=WAL")
            with db:
                yield db
        finally:
            db.close()

    def claim(self, run_id: str, role: str, iteration: int, candidate_id: str) -> Claim:
        key = (run_id, role, iteration, candidate_id)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """SELECT generation, state, result_json FROM receipts
                   WHERE run_id=? AND role=? AND iteration=? AND candidate_id=?""",
                key,
            ).fetchone()
            if row is not None:
                if row[1] == "finished":
                    return Claim("finished", result=json.loads(row[2]))
                return Claim("ambiguous")
            generation = uuid4().hex
            db.execute(
                """INSERT INTO receipts
                   (run_id, role, iteration, candidate_id, generation, state)
                   VALUES (?, ?, ?, ?, ?, 'running')""",
                (*key, generation),
            )
            return Claim("new", generation=generation)

    def finish(
        self,
        run_id: str,
        role: str,
        iteration: int,
        candidate_id: str,
        generation: str,
        result: dict[str, Any],
    ) -> None:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            updated = db.execute(
                """UPDATE receipts SET state='finished', result_json=?
                   WHERE run_id=? AND role=? AND iteration=? AND candidate_id=?
                     AND generation=? AND state='running'""",
                (
                    json.dumps(result, sort_keys=True),
                    run_id,
                    role,
                    iteration,
                    candidate_id,
                    generation,
                ),
            ).rowcount
            if updated != 1:
                raise RuntimeError("activity receipt ownership changed before completion")
