"""Private, transactional single-host state with immutable record history."""

import fcntl
import hashlib
import json
import os
import sqlite3
import stat
import uuid
from contextlib import closing, contextmanager
from decimal import Decimal
from pathlib import Path

from devflow.durability import durable_directory, flush_descriptor, flush_directory
from devflow.errors import WorkflowError
from devflow.validation import canonical_json

SCHEMA_VERSION = 1


class SQLiteStore:
    def __init__(self, state_dir: Path):
        self.root = Path(state_dir).absolute()
        for path in (self.root, self.root / "artifacts", self.root / "backups"):
            if path.is_symlink():
                raise WorkflowError("unsafe_state", "State directories must not be symlinks")
            durable_directory(path)
            if path.stat().st_uid != os.getuid():
                raise WorkflowError("unsafe_state", "State directory is not owned by current user")
            os.chmod(path, 0o700)
            flush_directory(path)
        self.path = self.root / "state.sqlite3"
        if self.path.is_symlink():
            raise WorkflowError("unsafe_state", "Database must not be a symlink")
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        metadata = os.fstat(fd)
        os.close(fd)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
        ):
            raise WorkflowError(
                "unsafe_state", "Database must be a solely linked user-owned regular file"
            )
        os.chmod(self.path, 0o600)
        self.migrate()
        flush_directory(self.root)

    def connect(self):
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        try:
            self._configure(connection)
            connection.row_factory = sqlite3.Row
            return connection
        except BaseException:
            connection.close()
            raise

    @staticmethod
    def _configure(connection):
        connection.execute("PRAGMA synchronous=EXTRA")
        connection.execute("PRAGMA fullfsync=ON")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")

    def migrate(self):
        with closing(self.connect()) as db:
            db.execute("BEGIN EXCLUSIVE")
            try:
                version = db.execute("PRAGMA user_version").fetchone()[0]
                if version > SCHEMA_VERSION:
                    raise WorkflowError(
                        "unsupported_store", "Store is newer than this package; admissions stopped"
                    )
                if version == 0:
                    existing = db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                    if existing:
                        raise WorkflowError(
                            "unsupported_store",
                            "Unversioned nonempty database is not a managed devflow store",
                        )
                    db.execute(
                        "CREATE TABLE IF NOT EXISTS works (work_id TEXT PRIMARY KEY, revision INTEGER NOT NULL, state TEXT NOT NULL)"
                    )
                    db.execute(
                        "CREATE TABLE IF NOT EXISTS operations (operation_id TEXT PRIMARY KEY, payload_hash TEXT NOT NULL, result TEXT NOT NULL)"
                    )
                    db.execute(
                        "CREATE TABLE IF NOT EXISTS records (work_id TEXT NOT NULL, record_key TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(work_id, record_key))"
                    )
                    db.execute(
                        "CREATE TABLE IF NOT EXISTS claims (work_id TEXT PRIMARY KEY, repository TEXT NOT NULL UNIQUE, attempt_id TEXT NOT NULL UNIQUE, host_id TEXT NOT NULL)"
                    )
                    db.execute("PRAGMA user_version=1")
                db.commit()
            except BaseException:
                db.rollback()
                raise

    @contextmanager
    def _exclusive(self):
        descriptor = os.open(
            self.root / ".store.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
        )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    @contextmanager
    def transaction(self):
        with self._exclusive():
            with self._transaction_unlocked() as db:
                yield db

    @contextmanager
    def _transaction_unlocked(self):
        db = self.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def operation(self, operation_id):
        """Return a previously committed operation without changing its revision."""
        with closing(self.connect()) as db:
            row = db.execute(
                "SELECT payload_hash,result FROM operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
        if row is None:
            return None
        return {"payload_hash": row[0], "result": json.loads(row[1], parse_float=Decimal)}

    def read(self, work_id):
        with closing(self.connect()) as db:
            row = db.execute("SELECT state FROM works WHERE work_id=?", (work_id,)).fetchone()
        if row is None:
            raise WorkflowError("unknown_work", f"Unknown work: {work_id}")
        return json.loads(row[0], parse_float=Decimal)

    def put_artifact(self, content: bytes):
        if not isinstance(content, bytes):
            raise WorkflowError("invalid_artifact", "Artifact content must be bytes")
        key = hashlib.sha256(content).hexdigest()
        path = self.root / "artifacts" / key
        temporary = path.parent / f".pending-{uuid.uuid4().hex}"
        try:
            fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(content)
                stream.flush()
                flush_descriptor(stream.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                self.require_artifact(key)
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                flush_descriptor(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)
        return key

    def require_artifact(self, key):
        if len(key) != 64 or any(c not in "0123456789abcdef" for c in key):
            raise WorkflowError("invalid_artifact", "Artifact key must be SHA-256")
        path = self.root / "artifacts" / key
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, "rb") as stream:
                metadata = os.fstat(stream.fileno())
                if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o077:
                    raise WorkflowError(
                        "unsafe_artifact", "Artifact must be a private regular file"
                    )
                actual = hashlib.file_digest(stream, "sha256").hexdigest()
        except OSError as exc:
            raise WorkflowError(
                "missing_artifact", f"Evidence artifact unavailable: {key}"
            ) from exc
        if actual != key:
            raise WorkflowError("corrupt_artifact", "Evidence hash does not match artifact")

    def backup(self):
        path = self.root / "backups" / f"state-{uuid.uuid4().hex}.sqlite3"
        temporary = path.with_suffix(".pending")
        try:
            fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            os.close(fd)
            with closing(self.connect()) as source, closing(sqlite3.connect(temporary)) as target:
                self._configure(target)
                source.backup(target)
                if target.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise WorkflowError("backup_failed", "Backup integrity check failed")
            fd = os.open(temporary, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                flush_descriptor(fd)
            finally:
                os.close(fd)
            os.replace(temporary, path)
            flush_directory(path.parent)
        finally:
            temporary.unlink(missing_ok=True)
        return path

    def restore(self, backup_path: Path):
        """Restore SQLite state only, retaining content-addressed artifacts and backup history."""
        with self._exclusive():
            self._restore_unlocked(backup_path)

    def _restore_unlocked(self, backup_path: Path):
        backup_path = Path(backup_path).absolute()
        if backup_path.parent != self.root / "backups" or backup_path.is_symlink():
            raise WorkflowError("unsafe_backup", "Restore requires a managed backup")
        with closing(sqlite3.connect(backup_path.as_uri() + "?mode=ro", uri=True)) as source:
            if source.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION:
                raise WorkflowError("unsupported_store", "Backup version incompatible")
            if source.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise WorkflowError("backup_failed", "Backup integrity check failed")
            with closing(self.connect()) as current:
                if current.execute("SELECT 1 FROM claims LIMIT 1").fetchone():
                    raise WorkflowError(
                        "active_restore",
                        "Cannot replace a store while an attempt owns an active claim",
                    )
                captures = {}
                for row in current.execute(
                    "SELECT result FROM operations WHERE operation_id LIKE 'backlog:%' "
                    "ORDER BY operation_id"
                ):
                    state = json.loads(row[0])
                    if state.get("kind") == "backlog_capture":
                        captures[(state["repository"], state["work_id"])] = state
                if any(state["status"] in {"dispatched", "ambiguous"}
                       for state in captures.values()):
                    raise WorkflowError(
                        "active_restore", "Reconcile uncertain backlog writes before restoring state"
                    )
            self.backup()
            with closing(self.connect()) as target:
                source.backup(target)

    @staticmethod
    def persist_records(db, work_id, records):
        for key, record in records.items():
            payload = canonical_json(record)
            old = db.execute(
                "SELECT payload FROM records WHERE work_id=? AND record_key=?", (work_id, key)
            ).fetchone()
            if old and old[0] != payload:
                raise WorkflowError("immutable_record", f"Cannot change immutable record {key}")
            db.execute("INSERT OR IGNORE INTO records VALUES (?, ?, ?)", (work_id, key, payload))
