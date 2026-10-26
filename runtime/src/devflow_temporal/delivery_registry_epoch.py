"""Transactional registry observation epochs.

These epochs validate an observation, not permission to start work. A caller must
separately fence the final owner/assignment/debit transition in a short writer
transaction or reservation. Never perform RPC while holding that transaction.
Runtime cutover must stop older instances before installing this schema. Triggers
cover ordinary old SQLite writers after cutover, not arbitrary file edits, schema
rewrites, restoration of a database backup, or rollback of the whole database.
"""

from __future__ import annotations

import os
import sqlite3
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path


class EpochUnavailable(ValueError):
    """The registry observation cannot be authenticated or is stale."""


_ISSUE_TABLES = (
    "execution_claims",
    "execution_snapshots",
    "execution_workers",
    "execution_effects",
    "execution_repair_limits",
    "execution_repairs",
    "execution_checkpoints",
)
_KEYS = {
    **{name: ("issue", "issue_id") for name in _ISSUE_TABLES},
    "execution_legacy_custody": ("url", "issue_url"),
    "execution_migrated_stores": ("store", "store_path"),
}
_COLUMNS = {
    "execution_claims": ("issue_id issue_url repository_id run_id store_path "
        "generation state updated_at"),
    "execution_snapshots": ("store_path run_id issue_id generation input_digest "
        "input_json created_at"),
    "execution_workers": "issue_id worker_key workstream_id generation state receipt_json",
    "execution_effects": ("issue_id effect_key generation kind request_digest request_json "
        "state result_json updated_at"),
    "execution_repair_limits": "issue_id maximum learning_required",
    "execution_repairs": "issue_id repair_key generation reason created_at",
    "execution_checkpoints": ("issue_id checkpoint_key generation content_digest "
        "content_json created_at"),
    "execution_legacy_custody": "issue_url receipts_json",
    "execution_migrated_stores": "store_path imported_at",
}
_PRIMARY_KEYS = {
    "execution_claims": ("issue_id",),
    "execution_snapshots": ("store_path", "run_id"),
    "execution_workers": ("issue_id", "worker_key"),
    "execution_effects": ("issue_id", "effect_key"),
    "execution_repair_limits": ("issue_id",),
    "execution_repairs": ("issue_id", "repair_key"),
    "execution_checkpoints": ("issue_id", "checkpoint_key"),
    "execution_legacy_custody": ("issue_url",),
    "execution_migrated_stores": ("store_path",),
}
_INTEGER_COLUMNS = {"generation", "maximum", "learning_required"}
_NULLABLE_COLUMNS = {"receipt_json", "result_json"}
_VERSION = 1
_MAX = 9223372036854775807
_META_SQL = "CREATE TABLE execution_epoch_meta (version INTEGER NOT NULL, identity TEXT NOT NULL)"
_EPOCH_SQL = (
    "CREATE TABLE execution_authority_epochs (kind TEXT NOT NULL, key TEXT NOT NULL, "
    "epoch INTEGER NOT NULL CHECK(typeof(epoch)='integer' AND epoch > 0), "
    "PRIMARY KEY(kind,key))"
)


def _bump(kind, expression):
    # A BEFORE INSERT on snapshots also fences the displaced issue in REPLACE,
    # whose implicit DELETE trigger is disabled unless recursive_triggers is on.
    return (
        f"INSERT INTO execution_authority_epochs(kind,key,epoch) SELECT '{kind}',{expression},1 "
        f"WHERE {expression} IS NOT NULL ON CONFLICT(kind,key) DO UPDATE SET epoch="
        f"CASE WHEN epoch < {_MAX} THEN epoch+1 ELSE RAISE(ABORT,'authority epoch overflow') END;"
    )


def _triggers():
    result = {}
    for table, (kind, column) in _KEYS.items():
        for operation in ("INSERT", "UPDATE", "DELETE"):
            name = f"authority_epoch_v{_VERSION}_{table}_{operation.lower()}"
            refs = (
                ("NEW",)
                if operation == "INSERT"
                else (("OLD", "NEW") if operation == "UPDATE" else ("OLD",))
            )
            body = " ".join(_bump(kind, f"{ref}.{column}") for ref in refs)
            result[name] = (
                f"CREATE TRIGGER {name} AFTER {operation} ON {table} BEGIN {body} END"
            )
    name = f"authority_epoch_v{_VERSION}_snapshot_replace"
    result[name] = (
        f"CREATE TRIGGER {name} BEFORE INSERT ON execution_snapshots BEGIN "
        + _bump(
            "issue",
            "(SELECT issue_id FROM execution_snapshots WHERE "
            "store_path=NEW.store_path AND run_id=NEW.run_id)",
        )
        + " END"
    )
    return result


def _schema(db):
    return {
        row[0]: row[1]
        for row in db.execute(
            "SELECT name,sql FROM sqlite_master WHERE type IN ('table','trigger') "
            "AND (name LIKE 'execution_%' OR name LIKE 'authority_epoch_%')"
        )
    }


def verify_schema(db):
    schema = _schema(db)
    allowed = set(_KEYS) | {"execution_epoch_meta", "execution_authority_epochs"}
    tables = {
        row[0]
        for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'execution_%'"
        )
    }
    if tables != allowed:
        raise EpochUnavailable("unsupported execution registry tables")
    expected = _triggers()
    if (
        schema.get("execution_epoch_meta") != _META_SQL
        or schema.get("execution_authority_epochs") != _EPOCH_SQL
        or {
            key: val
            for key, val in schema.items()
            if key.startswith("authority_epoch_")
        }
        != expected
    ):
        raise EpochUnavailable("unsupported or incomplete authority epoch schema")
    for table, (_, column) in _KEYS.items():
        info = list(db.execute(f"PRAGMA table_info({table})"))
        columns = [row[1] for row in info]
        primary = _PRIMARY_KEYS[table]
        if columns != _COLUMNS[table].split() or column not in columns:
            raise EpochUnavailable("unsupported registry authority table")
        for row in info:
            name = row[1]
            expected_type = "INTEGER" if name in _INTEGER_COLUMNS else "TEXT"
            key_position = primary.index(name) + 1 if name in primary else 0
            nullable = name in _NULLABLE_COLUMNS or (
                len(primary) == 1 and name in primary
            )
            if (
                row[2] != expected_type
                or row[3] != int(not nullable)
                or row[5] != key_position
            ):
                raise EpochUnavailable("unsupported registry authority table")
        indexes = list(db.execute(f"PRAGMA index_list({table})"))
        for index in indexes:
            if index[3] == "pk":
                continue
            if (table != "execution_workers" or index[1] != "execution_workstream_writer"
                    or index[2] != 1 or index[3] != "c" or index[4] != 1):
                raise EpochUnavailable("unsupported registry authority index")
            sql = db.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name=?", (index[1],)
            ).fetchone()
            expected = ("CREATE UNIQUE INDEX execution_workstream_writer ON "
                        "execution_workers(issue_id,workstream_id) "
                        "WHERE state IN ('reserved','running','unknown')")
            if not sql or " ".join(sql[0].split()) != expected:
                raise EpochUnavailable("unsupported registry authority index")
        if table == "execution_workers" and len(indexes) != 2:
            raise EpochUnavailable("unsupported registry authority index")
        if table != "execution_workers" and len(indexes) != 1:
            raise EpochUnavailable("unsupported registry authority index")
    rows = db.execute("SELECT version,identity FROM execution_epoch_meta").fetchall()
    if len(rows) != 1 or rows[0][0] != _VERSION:
        raise EpochUnavailable("unsupported authority epoch version")
    try:
        if str(uuid.UUID(rows[0][1])) != rows[0][1]:
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise EpochUnavailable("invalid registry lifetime identity") from None
    return rows[0][1]


def initialize(db):
    """Install once in an authorized writable bootstrap, never during observation."""
    db.execute("BEGIN IMMEDIATE")
    schema = _schema(db)
    present = any(
        name in schema
        for name in ("execution_epoch_meta", "execution_authority_epochs")
    )
    present |= any(name.startswith("authority_epoch_") for name in schema)
    if present:
        verify_schema(db)
        return
    tables = {
        row[0]
        for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'execution_%'"
        )
    }
    if tables != set(_KEYS):
        raise EpochUnavailable("unsupported execution registry tables")
    db.execute(_META_SQL)
    db.execute(_EPOCH_SQL)
    db.execute(
        "INSERT INTO execution_epoch_meta VALUES (?,?)", (_VERSION, str(uuid.uuid4()))
    )
    for statement in _triggers().values():
        db.execute(statement)
    verify_schema(db)


def _identity(info):
    return info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_nlink


def _private(info):
    return (
        stat.S_ISREG(info.st_mode)
        and info.st_uid == os.getuid()
        and stat.S_IMODE(info.st_mode) == 0o600
        and info.st_nlink == 1
    )


@dataclass
class AuthorityObservation:
    """Keep this object open through resource observation and revalidation."""

    path: Path
    fd: int
    identity: tuple
    issue_id: str
    issue_url: str
    store_path: str
    lifetime: str
    epochs: tuple
    facts: dict

    def close(self):
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def validate(self):
        """Nonblocking recheck of observation validity; not a launch authorization."""
        if self.fd < 0:
            raise EpochUnavailable("registry observation is closed")
        lifetime, epochs, _ = _read(self, include_facts=False)
        if lifetime != self.lifetime or epochs != self.epochs:
            raise EpochUnavailable("registry authority changed during observation")


def _read(observation, *, include_facts):
    path = observation.path
    try:
        pinned = os.fstat(observation.fd)
        if (
            _identity(pinned) != observation.identity
            or not _private(pinned)
            or _identity(path.lstat()) != observation.identity
            or path.resolve(strict=True) != path
        ):
            raise EpochUnavailable("registry file identity changed")
        header = os.pread(observation.fd, 100, 0)
        if (
            len(header) != 100
            or header[:16] != b"SQLite format 3\x00"
            or header[18:20] != b"\x01\x01"
            or any(
                os.path.lexists(str(path) + suffix)
                for suffix in ("-wal", "-shm", "-journal")
            )
        ):
            raise EpochUnavailable("registry must be a settled rollback database")
        db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0)
        try:
            if _identity(path.lstat()) != observation.identity:
                raise EpochUnavailable("registry file was replaced")
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            lifetime = verify_schema(db)
            keys = (
                ("issue", observation.issue_id),
                ("url", observation.issue_url),
                ("store", observation.store_path),
            )
            epochs = tuple(
                (
                    kind,
                    key,
                    (
                        row[0]
                        if (
                            row := db.execute(
                                "SELECT epoch FROM execution_authority_epochs "
                                "WHERE kind=? AND key=?",
                                (kind, key),
                            ).fetchone()
                        )
                        else 0
                    ),
                )
                for kind, key in keys
            )
            facts = {}
            if include_facts:
                for table in _ISSUE_TABLES:
                    facts[table] = [
                        dict(row)
                        for row in db.execute(
                            f"SELECT * FROM {table} WHERE issue_id=? ORDER BY rowid",
                            (observation.issue_id,),
                        )
                    ]
                for table in ("execution_legacy_custody", "execution_migrated_stores"):
                    _, col = _KEYS[table]
                    key = (
                        observation.issue_url
                        if col == "issue_url"
                        else observation.store_path
                    )
                    facts[table] = [
                        dict(row)
                        for row in db.execute(
                            f"SELECT * FROM {table} WHERE {col}=?", (key,)
                        )
                    ]
            if _identity(path.lstat()) != observation.identity:
                raise EpochUnavailable("registry file was replaced")
            return lifetime, epochs, facts
        finally:
            db.close()
    except (OSError, sqlite3.Error) as exc:
        raise EpochUnavailable("registry observation unavailable") from exc


def observe(
    path: Path, issue_id: str, issue_url: str, store_path: str
) -> AuthorityObservation:
    """Read facts and epochs atomically, without creating or migrating anything.

    issue_url must be the canonical normalized legacy custody key; store_path must
    be the exact canonical store key. Keep the returned descriptor pinned until
    validation, and independently pin/revalidate admission files used by callers.
    """
    path = Path(path)
    if (
        not path.is_absolute()
        or not issue_id
        or not issue_url
        or not store_path
        or issue_url != issue_url.rstrip("/").casefold()
        or not Path(store_path).is_absolute()
    ):
        raise EpochUnavailable(
            "observation needs canonical registry and authority keys"
        )
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        observation = AuthorityObservation(
            path,
            fd,
            _identity(os.fstat(fd)),
            issue_id,
            issue_url,
            store_path,
            "",
            (),
            {},
        )
        observation.lifetime, observation.epochs, observation.facts = _read(
            observation, include_facts=True
        )
        return observation
    except OSError as exc:
        if "fd" in locals():
            os.close(fd)
        raise EpochUnavailable("registry observation unavailable") from exc
    except BaseException:
        if "fd" in locals():
            os.close(fd)
        raise


class PinnedAdmissionFile:
    """Pin exact admission bytes through observation; callers close after recheck.

    This is an observation check. Mutations can still occur after validation,
    so final launch authorization needs its own atomic fence.
    """

    def __init__(self, path: Path, *, maximum_bytes: int = 16 * 1024 * 1024):
        import hashlib

        self.path = Path(path)
        self.fd = -1
        self.maximum_bytes = maximum_bytes
        if not self.path.is_absolute() or maximum_bytes <= 0:
            raise EpochUnavailable("invalid admission file path or size limit")
        try:
            self.fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            info = os.fstat(self.fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_nlink != 1
                or info.st_size > maximum_bytes
                or self.path.resolve(strict=True) != self.path
                or _identity(self.path.lstat()) != _identity(info)
            ):
                raise EpochUnavailable("admission file is not canonical and owned")
            self.identity = _identity(info)
            self.content = self._bytes()
            self.sha256 = hashlib.sha256(self.content).hexdigest()
            self.validate()
        except BaseException:
            self.close()
            raise

    def _bytes(self):
        chunks = []
        offset = 0
        while offset <= self.maximum_bytes:
            chunk = os.pread(
                self.fd, min(65536, self.maximum_bytes + 1 - offset), offset
            )
            if not chunk:
                return b"".join(chunks)
            chunks.append(chunk)
            offset += len(chunk)
        raise EpochUnavailable("admission file exceeds size limit")

    def validate(self):
        try:
            if (
                self.fd < 0
                or _identity(os.fstat(self.fd)) != self.identity
                or _identity(self.path.lstat()) != self.identity
                or self.path.resolve(strict=True) != self.path
                or self._bytes() != self.content
                or _identity(self.path.lstat()) != self.identity
            ):
                raise EpochUnavailable("admission file changed during observation")
        except OSError as exc:
            raise EpochUnavailable("admission file unavailable") from exc

    def close(self):
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
