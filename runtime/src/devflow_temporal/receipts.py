"""Single-host durable activity receipts for non-idempotent model work."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from .contracts import digest, public_inputs


class RunBindingError(ValueError):
    """A run ID already belongs to different inputs in this state store."""


@dataclass(frozen=True)
class Claim:
    state: Literal["new", "finished", "ambiguous"]
    generation: str | None = None
    result: dict[str, Any] | None = None


class ReceiptStore:
    def __init__(self, state_dir: Path) -> None:
        try:
            state_dir.mkdir(parents=True, mode=0o700)
        except FileExistsError:
            pass
        else:
            # A restrictive umask may have removed owner permissions.
            os.chmod(state_dir, 0o700)
        directory = state_dir.lstat()
        if (
            not stat.S_ISDIR(directory.st_mode)
            or stat.S_IMODE(directory.st_mode) != 0o700
            or directory.st_uid != os.getuid()
        ):
            raise ValueError("state directory must be an owned private directory (0700)")
        self.state_dir = state_dir
        self.path = state_dir / "receipts.sqlite3"
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            receipt_file = self.path.lstat()
            if (
                not stat.S_ISREG(receipt_file.st_mode)
                or stat.S_IMODE(receipt_file.st_mode) != 0o600
                or receipt_file.st_uid != os.getuid()
            ):
                raise ValueError("receipt database must be an owned private file (0600)") from None
        else:
            try:
                os.fchmod(descriptor, 0o600)
            finally:
                os.close(descriptor)
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
            db.execute(
                """CREATE TABLE IF NOT EXISTS run_bindings (
                    run_id TEXT PRIMARY KEY,
                    input_digest TEXT NOT NULL,
                    repo TEXT NOT NULL,
                    state_dir TEXT NOT NULL,
                    initial_candidate_id TEXT NOT NULL
                )"""
            )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=30)
        try:
            db.execute("PRAGMA journal_mode=WAL")
            with db:
                yield db
        finally:
            db.close()

    def claim(self, spec: dict[str, Any], role: str, iteration: int, candidate_id: str) -> Claim:
        run_id = spec["run_id"]
        if spec.get("input_digest") != digest(public_inputs(spec)):
            raise RunBindingError("run input digest does not match its inputs")
        if spec.get("state_dir") != str(self.state_dir):
            raise RunBindingError("run state directory does not match receipt store")
        binding = (
            spec["input_digest"],
            spec["repo"],
            spec["state_dir"],
            spec["initial_candidate"]["id"],
        )
        key = (run_id, role, iteration, candidate_id)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            bound = db.execute(
                """SELECT input_digest, repo, state_dir, initial_candidate_id
                   FROM run_bindings WHERE run_id=?""",
                (run_id,),
            ).fetchone()
            if bound is None:
                legacy_receipt = db.execute(
                    "SELECT 1 FROM receipts WHERE run_id=? LIMIT 1", (run_id,)
                ).fetchone()
                if legacy_receipt is not None:
                    raise RunBindingError(
                        "run has unbound legacy receipts; use a new state directory"
                    )
                db.execute(
                    """INSERT INTO run_bindings
                       (run_id, input_digest, repo, state_dir, initial_candidate_id)
                       VALUES (?, ?, ?, ?, ?)""",
                    (run_id, *binding),
                )
            elif bound != binding:
                raise RunBindingError("run ID already bound to different inputs")
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
