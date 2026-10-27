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
from contextlib import ExitStack, contextmanager
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
    "execution_claims": (
        "issue_id issue_url repository_id run_id store_path generation state updated_at"
    ),
    "execution_snapshots": (
        "store_path run_id issue_id generation input_digest input_json created_at"
    ),
    "execution_workers": "issue_id worker_key workstream_id generation state receipt_json",
    "execution_effects": (
        "issue_id effect_key generation kind request_digest request_json "
        "state result_json updated_at"
    ),
    "execution_repair_limits": "issue_id maximum learning_required",
    "execution_repairs": "issue_id repair_key generation reason created_at",
    "execution_checkpoints": (
        "issue_id checkpoint_key generation content_digest content_json created_at"
    ),
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


@dataclass(frozen=True)
class FactLimits:
    rows: int
    total_bytes: int
    cell_bytes: int

    def __post_init__(self):
        if any(type(value) is not int or value <= 0
               for value in (self.rows, self.total_bytes, self.cell_bytes)):
            raise EpochUnavailable("registry observation limits must be positive integers")


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
    protected_tables = set(_KEYS) | {
        "execution_epoch_meta",
        "execution_authority_epochs",
    }
    actual_triggers = {
        row[0]
        for row in db.execute(
            "SELECT name,tbl_name FROM sqlite_master WHERE type='trigger'"
        )
        if row[1] in protected_tables
    }
    if actual_triggers != set(expected):
        raise EpochUnavailable(
            "unsupported registry authority schema: unexpected triggers"
        )
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
            if (
                table != "execution_workers"
                or index[1] != "execution_workstream_writer"
                or index[2] != 1
                or index[3] != "c"
                or index[4] != 1
            ):
                raise EpochUnavailable("unsupported registry authority index")
            sql = db.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
                (index[1],),
            ).fetchone()
            expected = (
                "CREATE UNIQUE INDEX execution_workstream_writer ON "
                "execution_workers(issue_id,workstream_id) "
                "WHERE state IN ('reserved','running','unknown')"
            )
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


class PinnedRegistry:
    """Pin an existing canonical registry for a short read-only writer fence.

    For ordinary non-feature operations this provides physical identity and
    a writer lock, not mutation epochs for host-resource tables. Exact host
    reservation evidence must be rechecked through the yielded connection.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.fd = -1
        if not self.path.is_absolute():
            raise EpochUnavailable("registry path must be absolute")
        try:
            self.fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            self.identity = _identity(os.fstat(self.fd))
            self.validate_identity()
        except OSError as exc:
            self.close()
            raise EpochUnavailable("registry identity unavailable") from exc
        except BaseException:
            self.close()
            raise

    def validate_identity(self):
        try:
            info = os.fstat(self.fd)
            if (
                self.fd < 0
                or not _private(info)
                or _identity(info) != self.identity
                or _identity(self.path.lstat()) != self.identity
                or self.path.resolve(strict=True) != self.path
            ):
                raise EpochUnavailable("registry file identity changed")
        except OSError as exc:
            raise EpochUnavailable("registry file identity unavailable") from exc

    def validate_transaction(self, db):
        """Recheck physical pin and main DB name under the caller's transaction."""
        if not db.in_transaction:
            raise EpochUnavailable(
                "registry fence requires an existing writer transaction"
            )
        self.validate_identity()
        try:
            main = next(
                (
                    row[2]
                    for row in db.execute("PRAGMA database_list")
                    if row[1] == "main"
                ),
                None,
            )
        except sqlite3.Error as exc:
            raise EpochUnavailable("registry transaction unavailable") from exc
        if main != str(self.path):
            raise EpochUnavailable("registry transaction database changed")
        self.validate_identity()

    @contextmanager
    def writer_fence(self):
        """Hold the canonical SQLite writer lock until the caller finishes ACK."""
        self.validate_identity()
        try:
            db = sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True, timeout=0)
            db.row_factory = sqlite3.Row
            try:
                self.validate_identity()
                db.execute("PRAGMA busy_timeout=0")
                db.execute("BEGIN IMMEDIATE")
                db.execute("PRAGMA query_only=ON")
                self.validate_transaction(db)
            except BaseException:
                db.close()
                raise
        except sqlite3.Error as exc:
            raise EpochUnavailable("registry writer fence unavailable") from exc
        try:
            yield db
            self.validate_transaction(db)
            db.commit()
        finally:
            db.close()

    def close(self):
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


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
    schema_version: int = 0

    def close(self):
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def validate_identity(self):
        """Recheck the retained file descriptor and canonical path, not its epoch.

        This relies on the documented supported-writer/cutover contract. It is
        not an OS-level proof that SQLite opened the same inode during an
        adversarial same-UID path swap and is not protection from database restore.
        """
        try:
            pinned = os.fstat(self.fd)
            if (
                self.fd < 0
                or not _private(pinned)
                or _identity(pinned) != self.identity
                or _identity(self.path.lstat()) != self.identity
                or self.path.resolve(strict=True) != self.path
            ):
                raise EpochUnavailable("registry file identity changed")
        except OSError as exc:
            raise EpochUnavailable("registry file identity unavailable") from exc

    def validate_transaction(self, db, *, assignment=None):
        return self.validate_transaction_token(db, assignment=assignment)

    def validate_transaction_token(self, db, *, assignment=None):
        """Compare the original observation under the caller's registry writer lock.

        `assignment`, if supplied, is an exact captured execution_workers row.
        This only checks authority; the caller separately owns tracking ACK and
        must call it immediately before committing that ACK while this DB is held.
        """
        if not db.in_transaction:
            raise EpochUnavailable(
                "registry fence requires an existing writer transaction"
            )
        try:
            self.validate_identity()
            main = next(
                (
                    row[2]
                    for row in db.execute("PRAGMA database_list")
                    if row[1] == "main"
                ),
                None,
            )
            if main != str(self.path):
                raise EpochUnavailable("registry transaction database changed")
            # The complete schema was authenticated in the initial read
            # snapshot. Supported DDL changes SQLite's schema_version; do not
            # rescan sqlite_master/index/trigger definitions under the writer.
            schema = db.execute("PRAGMA schema_version").fetchone()
            if (self.schema_version <= 0 or schema is None
                    or schema[0] != self.schema_version):
                raise EpochUnavailable("registry authenticated schema changed")
            metadata = db.execute(
                "SELECT version,CASE WHEN length(identity)=36 THEN identity END "
                "FROM execution_epoch_meta LIMIT 2").fetchall()
            if len(metadata) != 1 or metadata[0][0] != _VERSION:
                raise EpochUnavailable("registry lifetime metadata changed")
            lifetime = metadata[0][1]
            current = []
            for kind, key, _ in self.epochs:
                row = db.execute(
                    "SELECT epoch FROM execution_authority_epochs WHERE kind=? AND key=?",
                    (kind, key),
                ).fetchone()
                current.append((kind, key, row[0] if row else 0))
            if lifetime != self.lifetime or tuple(current) != self.epochs:
                raise EpochUnavailable("registry authority changed during observation")
            columns = _COLUMNS["execution_claims"].split()
            selected = ",".join(
                f"CASE WHEN length(CAST({name} AS BLOB))<=8192 THEN {name} END AS {name}"
                for name in columns)
            claims = [dict(row) for row in db.execute(
                f"SELECT {selected} FROM execution_claims WHERE issue_id=? LIMIT 2",
                (self.issue_id,))]
            if claims != self.facts.get("execution_claims"):
                raise EpochUnavailable("registry execution owner changed")
            if assignment is not None:
                if (
                    not isinstance(assignment, dict)
                    or assignment.get("issue_id") != self.issue_id
                ):
                    raise EpochUnavailable("registry assignment identity is invalid")
                row = db.execute(
                    "SELECT * FROM execution_workers WHERE issue_id=? AND worker_key=?",
                    (self.issue_id, assignment.get("worker_key")),
                ).fetchone()
                if row is None or dict(row) != assignment:
                    raise EpochUnavailable("registry worker assignment changed")
            self.validate_identity()
        except sqlite3.Error as exc:
            raise EpochUnavailable("registry transaction unavailable") from exc

    @contextmanager
    def writer_fence(self, *, assignment=None):
        """Hold a nonblocking canonical registry writer lock across tracking ACK.

        Order: existing resource/native custody, pause-root mutex, this fence,
        then the short tracking writer transaction. No RPC, copying, Git, ledger
        reconstruction or unbounded I/O may run inside. Keep this observation's
        FD open throughout, and recheck validate_transaction immediately before
        the tracking commit. The yielded registry connection is query-only.
        """
        self.validate_identity()
        try:
            db = sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True, timeout=0)
            db.row_factory = sqlite3.Row
            try:
                self.validate_identity()
                db.execute("PRAGMA busy_timeout=0")
                db.execute("BEGIN IMMEDIATE")
                db.execute("PRAGMA query_only=ON")
                self.validate_transaction(db, assignment=assignment)
            except BaseException:
                db.close()
                raise
        except sqlite3.Error as exc:
            raise EpochUnavailable("registry writer fence unavailable") from exc
        try:
            yield db
            self.validate_transaction(db, assignment=assignment)
            db.commit()
        finally:
            db.close()

    def validate(self):
        """Nonblocking recheck of observation validity; not a launch authorization."""
        if self.fd < 0:
            raise EpochUnavailable("registry observation is closed")
        lifetime, epochs, _ = _read(self, include_facts=False)
        if lifetime != self.lifetime or epochs != self.epochs:
            raise EpochUnavailable("registry authority changed during observation")


def _bounded_fact_rows(db, table, column, key, limits, usage):
    """Reject oversized facts using scalar metadata before fetching their bytes."""
    if limits is not None:
        names = _COLUMNS[table].split()
        sizes = [f"coalesce(length(CAST({name} AS BLOB)),0)" for name in names]
        metadata = db.execute(
            f"SELECT max({','.join(sizes)}), {'+'.join(sizes)} FROM {table} "
            f"WHERE {column}=? LIMIT ?", (key, limits.rows - usage[0] + 1))
        for largest, total in metadata:
            usage[0] += 1
            usage[1] += total
            if usage[0] > limits.rows:
                raise EpochUnavailable("registry observation exceeds its bounded fact rows")
            if largest > limits.cell_bytes or usage[1] > limits.total_bytes:
                raise EpochUnavailable("registry observation exceeds its bounded fact payload")
    return [dict(row) for row in db.execute(
        f"SELECT * FROM {table} WHERE {column}=? ORDER BY rowid", (key,))]


def _read(observation, *, include_facts, fact_limits=None):
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
            schema_version = db.execute("PRAGMA schema_version").fetchone()[0]
            if include_facts:
                # Never refresh this authenticated token during later checks.
                observation.schema_version = schema_version
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
                usage = [0, 0]
                for table in _ISSUE_TABLES:
                    facts[table] = _bounded_fact_rows(
                        db, table, "issue_id", observation.issue_id, fact_limits, usage)
                for table in ("execution_legacy_custody", "execution_migrated_stores"):
                    _, col = _KEYS[table]
                    key = (
                        observation.issue_url
                        if col == "issue_url"
                        else observation.store_path
                    )
                    facts[table] = _bounded_fact_rows(db, table, col, key, fact_limits, usage)
                claims = facts.get("execution_claims", [])
                if len(claims) > 1 or any(
                    isinstance(value, str) and len(value.encode("utf-8")) > 8192
                    for row in claims for value in row.values()
                ):
                    raise EpochUnavailable("registry claim exceeds its bounded transaction token")
            if _identity(path.lstat()) != observation.identity:
                raise EpochUnavailable("registry file was replaced")
            return lifetime, epochs, facts
        finally:
            db.close()
    except (OSError, sqlite3.Error) as exc:
        raise EpochUnavailable("registry observation unavailable") from exc


def observe(
    path: Path, issue_id: str, issue_url: str, store_path: str, *,
    fact_limits: FactLimits | None = None,
) -> AuthorityObservation:
    """Read facts and epochs atomically, without creating or migrating anything.

    issue_url must be the canonical normalized legacy custody key; store_path must
    be the exact canonical store key. Keep the returned descriptor pinned until
    validation, and independently pin/revalidate admission files used by callers.
    """
    path = Path(path)
    if fact_limits is not None and type(fact_limits) is not FactLimits:
        raise EpochUnavailable("registry observation requires exact fact limits")
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
            observation, include_facts=True, fact_limits=fact_limits
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


class OrderedFences:
    """Already-held canonical registry writer connections for one ACK."""

    def __init__(self, connections, observations):
        self.connections = connections
        self.observations = observations

    def connection(self, path):
        return self.connections[Path(path)]

    def validate(self):
        """Call immediately before tracking COMMIT while all fences remain held."""
        for observation in self.observations:
            observation.validate_transaction(self.connection(observation.path))


@contextmanager
def ordered_writer_fences(observations):
    """Acquire distinct canonical registries in path order; share equal paths.

    The caller retains every observation pin across prior I/O, acquires the
    pause-root mutex first and the tracking transaction after these fences, and
    performs no RPC or unbounded I/O while writer locks are held. Generic pins
    do not imply a mutation epoch; each observation defines its own checks.
    """
    observations = tuple(observations)
    if not observations:
        raise EpochUnavailable("registry fence needs an observation")
    groups = {}
    for observation in observations:
        observation.validate_identity()
        peers = groups.setdefault(observation.path, [])
        if peers and peers[0].identity != observation.identity:
            raise EpochUnavailable("registry observations disagree on file identity")
        peers.append(observation)
    with ExitStack() as stack:
        connections = {}
        for path in sorted(groups, key=str):
            db = stack.enter_context(groups[path][0].writer_fence())
            connections[path] = db
            for observation in groups[path]:
                observation.validate_transaction(db)
        fences = OrderedFences(connections, observations)
        yield fences
        fences.validate()


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
