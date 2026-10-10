"""Shared execution ownership for GitHub issues, not a local feature database.

Business definitions live on GitHub. This registry contains execution claims,
immutable input snapshots, effect journals, worker leases and repair accounting.
All runtime instances for a user must use the same configured registry.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
import stat
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .contracts import canonical_json, digest


class OwnershipConflict(ValueError):
    """An execution cannot acquire or mutate another execution's delivery."""


class UnresolvedEffect(OwnershipConflict):
    """An external operation needs readback before execution can continue."""


def now() -> str:
    return datetime.now(UTC).isoformat()


def private_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or path.resolve() != path
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise ValueError("execution registry directory must be private, owned and canonical")


class ExecutionRegistry:
    def __init__(self, path: Path):
        if not path.is_absolute():
            raise ValueError("execution registry path must be absolute")
        self.path = path
        private_directory(path.parent)
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_nlink != 1
            ):
                raise ValueError("execution registry must be a private owned regular file")
        finally:
            os.close(fd)
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS execution_claims (
                    issue_id TEXT PRIMARY KEY, issue_url TEXT NOT NULL,
                    repository_id TEXT NOT NULL, run_id TEXT NOT NULL,
                    store_path TEXT NOT NULL, generation INTEGER NOT NULL,
                    state TEXT NOT NULL, updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS execution_snapshots (
                    store_path TEXT NOT NULL, run_id TEXT NOT NULL,
                    issue_id TEXT NOT NULL, generation INTEGER NOT NULL,
                    input_digest TEXT NOT NULL, input_json TEXT NOT NULL,
                    created_at TEXT NOT NULL, PRIMARY KEY(store_path,run_id));
                CREATE TABLE IF NOT EXISTS execution_workers (
                    issue_id TEXT NOT NULL, worker_key TEXT NOT NULL,
                    workstream_id TEXT NOT NULL, generation INTEGER NOT NULL,
                    state TEXT NOT NULL, receipt_json TEXT,
                    PRIMARY KEY(issue_id,worker_key));
                CREATE UNIQUE INDEX IF NOT EXISTS execution_workstream_writer
                    ON execution_workers(issue_id,workstream_id)
                    WHERE state IN ('reserved','running','unknown');
                CREATE TABLE IF NOT EXISTS execution_effects (
                    issue_id TEXT NOT NULL, effect_key TEXT NOT NULL,
                    generation INTEGER NOT NULL, kind TEXT NOT NULL,
                    request_digest TEXT NOT NULL, request_json TEXT NOT NULL,
                    state TEXT NOT NULL, result_json TEXT, updated_at TEXT NOT NULL,
                    PRIMARY KEY(issue_id,effect_key));
                CREATE TABLE IF NOT EXISTS execution_repair_limits (
                    issue_id TEXT PRIMARY KEY, maximum INTEGER NOT NULL,
                    learning_required INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS execution_repairs (
                    issue_id TEXT NOT NULL, repair_key TEXT NOT NULL,
                    generation INTEGER NOT NULL, reason TEXT NOT NULL,
                    created_at TEXT NOT NULL, PRIMARY KEY(issue_id,repair_key));
                CREATE TABLE IF NOT EXISTS execution_checkpoints (
                    issue_id TEXT NOT NULL, checkpoint_key TEXT NOT NULL,
                    generation INTEGER NOT NULL, content_digest TEXT NOT NULL,
                    content_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    PRIMARY KEY(issue_id,checkpoint_key));
                CREATE TABLE IF NOT EXISTS execution_legacy_custody (
                    issue_url TEXT PRIMARY KEY, receipts_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS execution_migrated_stores (
                    store_path TEXT PRIMARY KEY, imported_at TEXT NOT NULL);
            """)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            with db:
                yield db
        finally:
            db.close()

    @contextmanager
    def serialized(self, issue_id: str):
        """Serialize effects and takeover, including effects already in flight."""
        filename = hashlib.sha256(issue_id.encode()).hexdigest() + ".lock"
        fd = os.open(self.path.parent / filename, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_nlink != 1
            ):
                raise ValueError("execution lock is not private and owned")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise OwnershipConflict("feature has an operation in progress") from exc
            yield
        finally:
            os.close(fd)

    @staticmethod
    def token(row) -> dict:
        return {key: row[key] for key in ("issue_id", "run_id", "store_path", "generation")}

    @staticmethod
    def require(db, token: dict, *, active: bool = True):
        row = db.execute(
            "SELECT * FROM execution_claims WHERE issue_id=?", (token["issue_id"],)
        ).fetchone()
        if (
            row is None
            or ExecutionRegistry.token(row) != token
            or active
            and row["state"] != "active"
        ):
            raise OwnershipConflict("execution ownership changed or is no longer active")
        return row

    def claim(
        self,
        snapshot: dict,
        run_id: str,
        store_path: str,
        *,
        maximum_repairs: int = 10,
        predecessor: dict | None = None,
    ) -> dict:
        """Acquire a fresh issue or a confirmed stopped execution; never expire claims."""
        if type(maximum_repairs) is not int or not 1 <= maximum_repairs <= 100:
            raise ValueError("product repair limit must be between 1 and 100")
        issue = snapshot["issue"]
        if not all(
            isinstance(issue.get(k), str) and issue[k] for k in ("id", "url", "repository_id")
        ):
            raise ValueError("execution snapshot lacks a GitHub issue identity")
        if not Path(store_path).is_absolute() or not run_id:
            raise ValueError("execution needs its exact source store and run")
        with self.serialized(issue["id"]), self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            legacy = db.execute(
                "SELECT receipts_json FROM execution_legacy_custody WHERE issue_url=?",
                (issue["url"].rstrip("/").casefold(),),
            ).fetchone()
            if legacy:
                raise OwnershipConflict(
                    "feature has legacy publication custody requiring explicit adoption: "
                    + legacy[0]
                )
            saved = db.execute(
                "SELECT * FROM execution_snapshots WHERE store_path=? AND run_id=?",
                (store_path, run_id),
            ).fetchone()
            if saved:
                token = {
                    "issue_id": saved["issue_id"],
                    "run_id": run_id,
                    "store_path": store_path,
                    "generation": saved["generation"],
                }
                self.require(db, token)
                if saved["input_digest"] != digest(snapshot):
                    raise OwnershipConflict("run is already bound to different GitHub inputs")
                return token
            old = db.execute(
                "SELECT * FROM execution_claims WHERE issue_id=?", (issue["id"],)
            ).fetchone()
            generation = 1
            if old:
                if (
                    old["repository_id"] != issue["repository_id"]
                    or old["issue_url"] != issue["url"]
                ):
                    raise OwnershipConflict("GitHub feature identity changed")
                if predecessor != self.token(old) or old["state"] != "stopped":
                    raise OwnershipConflict("feature already has an execution; continue its owner")
                self._settled(db, issue["id"])
                pending = self._pending_revision(db, issue["id"])
                if pending and pending.get("successor_id") != run_id:
                    raise OwnershipConflict("feature has a recorded plan revision in custody")
                generation = old["generation"] + 1
            elif predecessor is not None:
                raise OwnershipConflict("continuation predecessor is missing")
            db.execute(
                """INSERT INTO execution_claims VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(issue_id) DO UPDATE SET run_id=excluded.run_id,
                store_path=excluded.store_path,generation=excluded.generation,
                state=excluded.state,updated_at=excluded.updated_at""",
                (
                    issue["id"],
                    issue["url"],
                    issue["repository_id"],
                    run_id,
                    store_path,
                    generation,
                    "active",
                    now(),
                ),
            )
            db.execute(
                "INSERT INTO execution_snapshots VALUES (?,?,?,?,?,?,?)",
                (
                    store_path,
                    run_id,
                    issue["id"],
                    generation,
                    digest(snapshot),
                    canonical_json(snapshot),
                    now(),
                ),
            )
            db.execute(
                "INSERT OR IGNORE INTO execution_repair_limits VALUES (?,?,0)",
                (issue["id"], maximum_repairs),
            )
            return {
                "issue_id": issue["id"],
                "run_id": run_id,
                "store_path": store_path,
                "generation": generation,
            }

    @staticmethod
    def _pending_revision(db, issue_id):
        values = {
            row["checkpoint_key"]: json.loads(row["content_json"])
            for row in db.execute(
                "SELECT checkpoint_key,content_json FROM execution_checkpoints "
                "WHERE issue_id=? AND checkpoint_key LIKE 'plan-revision:%'",
                (issue_id,),
            )
        }
        requests = [
            value
            for key, value in values.items()
            if key.startswith("plan-revision:request:")
            and not any(
                value["revision_id"] == terminal.get("revision_id")
                for name, terminal in values.items()
                if name.startswith(("plan-revision:adopted:", "plan-revision:rejected:"))
            )
        ]
        if len(requests) > 1:
            raise OwnershipConflict("feature has conflicting revision custody")
        return requests[0] if requests else None

    def revision_checkpoint(self, token: dict, key: str, value: dict, *, stopped=False) -> None:
        """Append a revision admission without rewriting historic input snapshots."""
        if not key.startswith("plan-revision:"):
            raise ValueError("revision checkpoint must use its versioned namespace")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self.require(db, token, active=False)
            if row["state"] not in ({"stopped"} if stopped else {"active", "draining"}):
                raise OwnershipConflict("revision owner is not at its required checkpoint")
            if stopped:
                self._settled(db, token["issue_id"])
                pending = self._pending_revision(db, token["issue_id"])
                if pending and pending.get("revision_id") != value.get("revision_id"):
                    raise OwnershipConflict("feature has another revision command in custody")
            self._checkpoint(db, token, key, value)

    def adopt_plan_revision(self, token: dict, receipt: dict, spec: dict) -> None:
        """Commit a read-back plan and effective input overlay in one append-only transaction."""
        identity = receipt["identity"]
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.require_settlement(db, token)
            self._settled(db, token["issue_id"])
            rows = list(
                db.execute(
                    "SELECT content_json FROM execution_checkpoints WHERE issue_id=? "
                    "AND checkpoint_key LIKE 'plan-revision:adopted:%'",
                    (token["issue_id"],),
                )
            )
            previous = [json.loads(row[0]) for row in rows]
            current = (
                max(previous, key=lambda item: item["identity"]["plan_revision"])
                if previous
                else None
            )
            if current and current["identity"] == identity:
                if current != receipt:
                    raise OwnershipConflict("plan revision adoption receipt changed")
                return
            expected = receipt["old_identity"]
            if current and current["identity"] != expected:
                raise OwnershipConflict("plan revision was superseded before local adoption")
            if identity["plan_revision"] != expected["plan_revision"] + 1:
                raise OwnershipConflict("plan revision did not advance exactly once")
            self._checkpoint(
                db,
                token,
                "plan-revision:spec:" + spec["run_id"] + ":" + str(identity["plan_revision"]),
                {
                    "spec": spec,
                    "spec_digest": digest(spec),
                    "identity": identity,
                    "revision_id": receipt["revision_id"],
                },
            )
            self._checkpoint(
                db, token, "plan-revision:adopted:" + str(identity["plan_revision"]), receipt
            )
            db.execute(
                "UPDATE execution_claims SET state='active',updated_at=? WHERE issue_id=?",
                (now(), token["issue_id"]),
            )

    def initialize_plan_revision(self, token: dict, receipt: dict, spec: dict) -> None:
        """Seal only initial v2 child-number normalization; it grants no repair allowance."""
        if receipt["identity"]["plan_revision"] != 1 or receipt.get("affected_chunks"):
            raise ValueError("initial normalization cannot invalidate executed work")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.require(db, token)
            if db.execute(
                "SELECT 1 FROM execution_checkpoints WHERE issue_id=? "
                "AND checkpoint_key LIKE 'plan-revision:adopted:%' "
                "AND checkpoint_key!='plan-revision:adopted:1'",
                (token["issue_id"],),
            ).fetchone():
                raise OwnershipConflict("initial plan is already superseded")
            self._checkpoint(
                db,
                token,
                "plan-revision:spec:" + spec["run_id"] + ":1",
                {
                    "spec": spec,
                    "spec_digest": digest(spec),
                    "identity": receipt["identity"],
                    "revision_id": receipt["revision_id"],
                },
            )
            self._checkpoint(db, token, "plan-revision:adopted:1", receipt)

    @staticmethod
    def _settled(db, issue_id):
        if db.execute(
            "SELECT 1 FROM execution_workers WHERE issue_id=? AND state!='finished'", (issue_id,)
        ).fetchone():
            raise OwnershipConflict("feature still has unfinished or unknown workers")
        if db.execute(
            "SELECT 1 FROM execution_effects WHERE issue_id=? AND state='pending'", (issue_id,)
        ).fetchone():
            raise UnresolvedEffect("feature has an external operation awaiting readback")

    def stop(self, token: dict, checkpoint_key: str, checkpoint: dict) -> None:
        with self.serialized(token["issue_id"]), self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.require(db, token, active=False)
            self._settled(db, token["issue_id"])
            self._checkpoint(db, token, checkpoint_key, checkpoint)
            db.execute(
                "UPDATE execution_claims SET state='stopped',updated_at=? WHERE issue_id=?",
                (now(), token["issue_id"]),
            )

    @staticmethod
    def _checkpoint(db, token, key, value):
        old = db.execute(
            "SELECT content_digest FROM execution_checkpoints "
            "WHERE issue_id=? AND checkpoint_key=?",
            (token["issue_id"], key),
        ).fetchone()
        if old and old[0] != digest(value):
            raise OwnershipConflict("checkpoint identity already records different work")
        db.execute(
            "INSERT OR IGNORE INTO execution_checkpoints VALUES (?,?,?,?,?,?)",
            (
                token["issue_id"],
                key,
                token["generation"],
                digest(value),
                canonical_json(value),
                now(),
            ),
        )

    def checkpoint(self, token: dict, key: str, value: dict) -> None:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            current = self.require(db, token, active=False)
            if current["state"] not in {"active", "draining"}:
                raise OwnershipConflict("stopped execution cannot append a checkpoint")
            self._checkpoint(db, token, key, value)

    def reserve_worker(self, token: dict, worker_key: str, workstream_id: str) -> dict:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.require_settlement(db, token)
            old = db.execute(
                "SELECT * FROM execution_workers WHERE issue_id=? AND worker_key=?",
                (token["issue_id"], worker_key),
            ).fetchone()
            if old:
                if (
                    old["generation"] != token["generation"]
                    or old["workstream_id"] != workstream_id
                ):
                    raise OwnershipConflict("worker assignment belongs to another execution")
                return dict(old)
            self.require(db, token)
            try:
                db.execute(
                    "INSERT INTO execution_workers VALUES (?,?,?,?,'reserved',NULL)",
                    (token["issue_id"], worker_key, workstream_id, token["generation"]),
                )
            except sqlite3.IntegrityError as exc:
                raise OwnershipConflict("workstream already has an active writer") from exc
            return {
                "worker_key": worker_key,
                "workstream_id": workstream_id,
                "generation": token["generation"],
                "state": "reserved",
            }

    def reserve_revision_worker(self, token, worker_key, workstream_id, admission):
        """Reactivate only a closed original assignment under an adopted correction."""
        custody_key = "plan-revision:worker-custody:" + digest(
            {
                "worker_key": worker_key,
                "revision_id": admission["revision_id"],
            }
        )
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.require(db, token)
            receipt_row = db.execute(
                "SELECT content_json FROM execution_checkpoints "
                "WHERE issue_id=? AND checkpoint_key LIKE 'plan-revision:adopted:%' "
                "ORDER BY json_extract(content_json,'$.identity.plan_revision') DESC LIMIT 1",
                (token["issue_id"],),
            ).fetchone()
            receipt = json.loads(receipt_row[0]) if receipt_row else None
            if (
                not receipt
                or receipt["revision_id"] != admission["revision_id"]
                or {key: receipt["identity"][key] for key in ("plan_revision", "plan_digest")}
                != admission["plan_identity"]
                or admission["parent_run_id"] != token["run_id"]
            ):
                raise OwnershipConflict("worker reactivation has no exact adopted correction")
            old = db.execute(
                "SELECT * FROM execution_workers WHERE issue_id=? AND worker_key=?",
                (token["issue_id"], worker_key),
            ).fetchone()
            prior = db.execute(
                "SELECT content_json FROM execution_checkpoints "
                "WHERE issue_id=? AND checkpoint_key=?",
                (token["issue_id"], custody_key),
            ).fetchone()
            if prior:
                if json.loads(prior[0])["admission"] != admission or not old:
                    raise OwnershipConflict("worker revision reactivation custody changed")
                if (
                    old["generation"] != token["generation"]
                    or old["workstream_id"] != workstream_id
                ):
                    raise OwnershipConflict("worker revision assignment changed ownership")
                if old["state"] != "finished":
                    return dict(old)
            if old:
                closed = json.loads(old["receipt_json"] or "null")
                if (
                    old["generation"] != token["generation"]
                    or old["workstream_id"] != workstream_id
                    or old["state"] != "finished"
                    or not closed
                    or closed.get("cleanup") != "confirmed"
                ):
                    raise OwnershipConflict(
                        "worker revision requires its confirmed closed assignment"
                    )
            else:
                # A continuation generation creates a new assignment; the old
                # generation's row and its confirmed completion remain unchanged.
                predecessor = db.execute(
                    "SELECT * FROM execution_workers WHERE issue_id=? "
                    "AND substr(worker_key,1,instr(worker_key,':generation:')-1)=? "
                    "AND state='finished' ORDER BY generation DESC LIMIT 1",
                    (token["issue_id"], worker_key.split(":generation:")[0]),
                ).fetchone()
                closed = json.loads(predecessor["receipt_json"] or "null") if predecessor else None
                if not closed or closed.get("cleanup") != "confirmed":
                    raise OwnershipConflict("worker revision has no retained closed assignment")
            if not prior:
                self._checkpoint(
                    db,
                    token,
                    custody_key,
                    {
                        "admission": admission,
                        "predecessor_assignment": dict(old or predecessor),
                    },
                )
            try:
                if old:
                    db.execute(
                        "UPDATE execution_workers SET state='reserved',receipt_json=NULL "
                        "WHERE issue_id=? AND worker_key=?",
                        (token["issue_id"], worker_key),
                    )
                else:
                    db.execute(
                        "INSERT INTO execution_workers VALUES (?,?,?,?,'reserved',NULL)",
                        (token["issue_id"], worker_key, workstream_id, token["generation"]),
                    )
            except sqlite3.IntegrityError as exc:
                raise OwnershipConflict("workstream already has an active writer") from exc
            return {
                "worker_key": worker_key,
                "workstream_id": workstream_id,
                "generation": token["generation"],
                "state": "reserved",
            }

    def finish_worker(self, token: dict, worker_key: str, receipt: dict) -> None:
        if receipt.get("cleanup") != "confirmed":
            raise OwnershipConflict("worker cleanup is not confirmed")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.require(db, token, active=False)
            row = db.execute(
                "SELECT * FROM execution_workers WHERE issue_id=? AND worker_key=?",
                (token["issue_id"], worker_key),
            ).fetchone()
            if not row or row["generation"] != token["generation"]:
                raise OwnershipConflict("worker has no matching execution assignment")
            if row["state"] == "finished" and row["receipt_json"] != canonical_json(receipt):
                raise OwnershipConflict("worker completion receipt changed")
            db.execute(
                "UPDATE execution_workers SET state='finished',receipt_json=? "
                "WHERE issue_id=? AND worker_key=?",
                (canonical_json(receipt), token["issue_id"], worker_key),
            )

    @contextmanager
    def mutation(self, token: dict):
        with self.serialized(token["issue_id"]):
            with self.connect() as db:
                self.require_settlement(db, token)
            yield

    def require_settlement(self, db, token):
        row = self.require(db, token, active=False)
        if row["state"] not in {"active", "draining"}:
            raise OwnershipConflict("stopped execution cannot settle remote operations")
        return row

    def intent(self, token: dict, key: str, kind: str, request: dict) -> dict:
        """Record before an effect. A pending receipt never authorizes blind replay."""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.require_settlement(db, token)
            old = db.execute(
                "SELECT * FROM execution_effects WHERE issue_id=? AND effect_key=?",
                (token["issue_id"], key),
            ).fetchone()
            if old:
                if old["kind"] != kind or old["request_digest"] != digest(request):
                    raise OwnershipConflict("external operation identity changed")
                return {
                    "fresh": False,
                    "state": old["state"],
                    "result": json.loads(old["result_json"] or "null"),
                }
            db.execute(
                "INSERT INTO execution_effects VALUES (?,?,?,?,?,?,'pending',NULL,?)",
                (
                    token["issue_id"],
                    key,
                    token["generation"],
                    kind,
                    digest(request),
                    canonical_json(request),
                    now(),
                ),
            )
            return {"fresh": True, "state": "pending", "result": None}

    def finish_effect(
        self, token: dict, key: str, result: dict, *, no_effect: bool = False
    ) -> None:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.require_settlement(db, token)
            row = db.execute(
                "SELECT * FROM execution_effects WHERE issue_id=? AND effect_key=?",
                (token["issue_id"], key),
            ).fetchone()
            if not row or (row["state"] == "pending" and row["generation"] != token["generation"]):
                raise OwnershipConflict("external operation has no matching intent")
            state = "absent" if no_effect else "complete"
            if row["state"] != "pending" and (
                row["state"] != state or row["result_json"] != canonical_json(result)
            ):
                raise OwnershipConflict("external operation receipt changed")
            db.execute(
                "UPDATE execution_effects SET state=?,result_json=?,updated_at=? "
                "WHERE issue_id=? AND effect_key=?",
                (state, canonical_json(result), now(), token["issue_id"], key),
            )

    def effect(self, issue_id: str, key: str) -> dict | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT * FROM execution_effects WHERE issue_id=? AND effect_key=?", (issue_id, key)
            ).fetchone()
            if not row:
                return None
            return {
                "state": row["state"],
                "kind": row["kind"],
                "request": json.loads(row["request_json"]),
                "result": json.loads(row["result_json"] or "null"),
            }

    def repair(self, token: dict, key: str, reason: str, *, devflow_defect: bool = False) -> dict:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if key.startswith("plan-revision:"):
                self.require_settlement(db, token)
            else:
                self.require(db, token)
            limit = db.execute(
                "SELECT * FROM execution_repair_limits WHERE issue_id=?", (token["issue_id"],)
            ).fetchone()
            old = db.execute(
                "SELECT * FROM execution_repairs WHERE issue_id=? AND repair_key=?",
                (token["issue_id"], key),
            ).fetchone()
            count = db.execute(
                "SELECT COUNT(*) FROM execution_repairs WHERE issue_id=?", (token["issue_id"],)
            ).fetchone()[0]
            alias = db.execute(
                "SELECT content_json FROM execution_checkpoints "
                "WHERE issue_id=? AND checkpoint_key=?",
                (token["issue_id"], "plan-revision:repair-alias:" + key),
            ).fetchone()
            if alias:
                bound = json.loads(alias[0])
                debit = db.execute(
                    "SELECT 1 FROM execution_repairs WHERE issue_id=? AND repair_key=?",
                    (token["issue_id"], bound["repair_key"]),
                ).fetchone()
                if (
                    not debit
                    or bound.get("reason") != reason
                    or bound.get("generation") != token["generation"]
                ):
                    raise OwnershipConflict("revision repair alias lost its exact cumulative debit")
                return {
                    "used": count,
                    "maximum": limit["maximum"],
                    "learning_required": bool(limit["learning_required"]),
                }
            if old and old["reason"] != reason:
                raise OwnershipConflict("repair identity changed")
            if not old and not devflow_defect:
                if count >= limit["maximum"]:
                    raise OwnershipConflict("feature product repair limit exhausted")
                db.execute(
                    "INSERT INTO execution_repairs VALUES (?,?,?,?,?)",
                    (token["issue_id"], key, token["generation"], reason, now()),
                )
                count += 1
            learning = bool(limit["learning_required"] or count >= 5 or devflow_defect)
            db.execute(
                "UPDATE execution_repair_limits SET learning_required=? WHERE issue_id=?",
                (int(learning), token["issue_id"]),
            )
            return {"used": count, "maximum": limit["maximum"], "learning_required": learning}

    def current(self, issue_id: str) -> dict[str, Any] | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT * FROM execution_claims WHERE issue_id=?", (issue_id,)
            ).fetchone()
            return dict(row) if row else None

    def snapshot(self, run_id: str, store_path: str) -> dict | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT input_json FROM execution_snapshots WHERE run_id=? AND store_path=?",
                (run_id, store_path),
            ).fetchone()
            return json.loads(row[0]) if row else None

    def checkpoints(self, issue_id: str) -> dict:
        with self.connect() as db:
            return {
                row["checkpoint_key"]: json.loads(row["content_json"])
                for row in db.execute(
                    "SELECT checkpoint_key,content_json FROM execution_checkpoints "
                    "WHERE issue_id=?",
                    (issue_id,),
                )
            }

    def budget(self, issue_id: str) -> dict:
        with self.connect() as db:
            row = db.execute(
                "SELECT * FROM execution_repair_limits WHERE issue_id=?", (issue_id,)
            ).fetchone()
            if not row:
                raise OwnershipConflict("feature has no admitted repair allowance")
            used = db.execute(
                "SELECT COUNT(*) FROM execution_repairs WHERE issue_id=?", (issue_id,)
            ).fetchone()[0]
            return {
                "used": used,
                "maximum": row["maximum"],
                "learning_required": bool(row["learning_required"]),
            }

    def drain(self, token: dict) -> None:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self.require(db, token, active=False)
            if row["state"] not in {"active", "draining"}:
                raise OwnershipConflict("execution is already stopped")
            db.execute(
                "UPDATE execution_claims SET state='draining',updated_at=? WHERE issue_id=?",
                (now(), token["issue_id"]),
            )
