"""Register operational roots before a new durable installation can use them.

This inventory is not execution authority. It makes the installation's stopped
update boundary complete for owners created with this protocol. Historical
owners never acquire that completeness merely by loading this module.
"""

from __future__ import annotations

import fcntl
import hashlib
import importlib.util
import json
import os
import stat
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

PROTOCOL = 1
_PROCESS_LEASES = {}
_STORE_INTENTS = set()


def _source_private(path):
    """Read the external bootstrap chain without importing mutable source."""
    path = Path(path)
    if path.resolve(strict=True) != path:
        raise ValueError("qualified source bootstrap evidence contains an alias")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
            or info.st_size > 4 * 1024 * 1024
        ):
            raise ValueError("qualified source bootstrap evidence lost its private custody")
        raw = os.pread(descriptor, info.st_size + 1, 0)
        named, after = path.lstat(), os.fstat(descriptor)
        if (
            len(raw) != info.st_size
            or (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
            != (named.st_dev, named.st_ino, named.st_size, named.st_mtime_ns, named.st_ctime_ns)
            or (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
        ):
            raise ValueError("qualified source bootstrap evidence changed")
        return raw
    finally:
        os.close(descriptor)


def source_start(config=None):
    root = Path(__file__).resolve().parents[3]
    owner = Path.home().resolve() / ".local/share/devflow"
    if root.parent.parent != owner / ".production-generations":
        raise ValueError("fresh namespace requires an active qualified source generation")
    if not os.environ.get("DEVFLOW_SOURCE_START_FD", "").isdigit():
        raise ValueError(
            "fresh namespace requires a managed start through its installed entry before imports"
        )
    birth_raw = _source_private(
        owner / ".production-generation-births" / (root.parent.name + ".json")
    )
    birth = json.loads(birth_raw)
    active = json.loads(_source_private(owner / ".production-active.json"))
    if (
        active.get("root") != str(root)
        or active.get("generation_id") != root.parent.name
        or active.get("birth_sha256") != hashlib.sha256(birth_raw).hexdigest()
    ):
        raise ValueError("qualified source process is no longer its active generation")
    folder = owner / ".production-bootstraps" / (birth["revision"] + "-" + root.parent.name)
    metadata_raw = _source_private(folder / "manifest.json")
    metadata = json.loads(metadata_raw)
    if birth.get("startup") != {
        "directory": str(folder),
        "manifest_sha256": hashlib.sha256(metadata_raw).hexdigest(),
    }:
        raise ValueError("qualified source bootstrap differs from its birth")
    file = folder / "production-source.py"
    raw = _source_private(file)
    if hashlib.sha256(raw).hexdigest() != metadata["source_helper_sha256"]:
        raise ValueError("qualified source process verifier changed")
    spec = importlib.util.spec_from_file_location("qualified_source_verifier", file)
    module = importlib.util.module_from_spec(spec)
    exec(compile(raw, str(file), "exec", dont_inherit=True), module.__dict__)
    return module.verify(root)


def _namespace_paths(database):
    database = Path(database)
    parent = Path.home().resolve() / ".local/state/devflow/namespaces"
    if database.name != "tracking.sqlite3" or database.parent.parent != parent:
        return None
    if len(database.parent.name) != 32 or any(
        char not in "0123456789abcdef" for char in database.parent.name
    ):
        raise ValueError("installation namespace identity is invalid")
    return database.parent


def _namespace_bootstrap(paths, root, database, config=None):
    """Authenticate the sole schema bootstrap child before it can own a writer."""
    supplied = os.environ.get("DEVFLOW_NAMESPACE_ALLOCATION_FD", "")
    folder = _namespace_paths(database)
    if not supplied or folder is None:
        return None
    if not supplied.isdigit():
        raise ValueError("namespace allocation has no actual owner descriptor")
    descriptor = int(supplied)
    owner = paths.production_owner(root)
    actual, expected = os.fstat(descriptor), (owner / ".production-install.lock").stat()
    if (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino):
        raise ValueError("namespace allocation did not inherit the production owner")
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    intent = paths.read_json(folder / "allocation.json")
    installed = paths.read_json(owner / ".production-install.json")
    if (
        intent.get("schema") != "devflow.fresh-namespace/v1"
        or intent.get("phase") != "bootstrapping"
        or intent.get("root") != str(root)
        or intent.get("database") != str(database)
        or intent.get("owner_generation") != installed["owner_generation"]
        or not paths.same_node(intent["namespace_identity"], paths.node(folder))
        or not paths.same_node(intent["state_identity"], paths.node(folder / "state"))
        or hashlib.sha256(paths.read_bytes(folder / "config.json")).hexdigest()
        != intent["config_sha256"]
        or (
            config is not None
            and (config.path != folder / "config.json" or config.tracking_db != database)
        )
    ):
        raise ValueError("namespace bootstrap scope differs from its private creation intent")
    return descriptor


def observe_configuration(config, *, writing=False):
    from . import delivery_execution_config

    # The lower installation layer also supports the pre-observation SDK;
    # composed #240 readers supply this existing context before loading configs.
    check_observation = getattr(delivery_execution_config, "is_observation_mode", None)
    observing = bool(check_observation and check_observation())
    if observing and writing:
        raise ValueError("observational configuration cannot create a store write intent")
    root = Path(__file__).resolve().parents[3]
    if _namespace_paths(config.tracking_db) is not None and not os.environ.get(
        "DEVFLOW_NAMESPACE_ALLOCATION_FD"
    ):
        source_start(config)
    owner = Path.home().resolve() / ".local/share/devflow"
    if (
        root != owner / "developer-workflow"
        and root.parent.parent != owner / ".production-generations"
    ):
        return
    receipt = owner / ".production-install.json"
    if not receipt.exists():
        return  # Legacy owner: readable, but no complete-update inventory claim.
    spec = importlib.util.spec_from_file_location(
        "installation_paths", root / "scripts/installation-paths.py"
    )
    paths = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(paths)
    paths.layout_helper().installed(root)
    registered = paths.read_json(receipt)
    if registered.get("managed_owner_version") != PROTOCOL:
        return
    if os.environ.get("DEVFLOW_INSTALLATION_PROBE_FD"):
        raise ValueError("controller readiness probes cannot load operational configuration")
    bootstrap = _namespace_bootstrap(paths, root, config.tracking_db, config)
    held = _PROCESS_LEASES.get(str(root))
    lock = (
        bootstrap if bootstrap is not None else held[1] if held and held[0] == os.getpid() else None
    )
    acquired = lock is None
    if acquired:
        inherited = os.environ.pop("DEVFLOW_INSTALLATION_LOCK_FD", "")
        if inherited.isdigit():
            candidate = int(inherited)
            try:
                actual = os.fstat(candidate)
                expected = (owner / ".production-install.lock").stat()
                if (actual.st_dev, actual.st_ino) == (expected.st_dev, expected.st_ino):
                    lock = candidate
            except OSError:
                pass
        if lock is None:
            lock = os.open(owner / ".production-install.lock", os.O_RDWR | os.O_NOFOLLOW)
    inventory_lock = None
    try:
        if bootstrap is None:
            fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
        registered = paths.read_json(receipt)
        identity = paths.node(root)
        if registered.get("phase") != "verified" or not paths.same_node(
            registered.get("target_identity"), identity
        ):
            raise ValueError("production installation is unfinished or its owner changed")
        update = owner / ".production-update.json"
        if update.exists() and paths.read_json(update).get("phase") not in {
            "verified",
            "rolled_back",
        }:
            raise ValueError("production update is unfinished; run its retained recovery command")
        if observing:
            value = paths.read_inventory(root, registered)
            raw = paths.read_bytes(config.path)
            observed = paths.configuration(config.path)
            record = {
                "config_sha256": hashlib.sha256(raw).hexdigest(),
                "state_root": observed["state_root"],
                "tracking_db": observed["tracking_db"],
            }
            if json.dumps(json.loads(raw), sort_keys=True) != json.dumps(
                config.raw, sort_keys=True
            ) or record not in value["configurations"].get(str(config.path), []):
                raise ValueError("production configuration custody is unknown during observation")
            _PROCESS_LEASES[str(root)] = (os.getpid(), lock)
            acquired = False
            return True
        inventory_lock = os.open(
            owner / ".production-inventory.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600
        )
        deadline = time.monotonic() + 10
        while True:
            try:
                fcntl.flock(inventory_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise ValueError("production configuration inventory remains busy") from None
                time.sleep(0.01)
        inventory = paths.inventory_path(root, registered)
        value = paths.read_inventory(root, registered)
        changed = False
        if writing:
            # A frozen authorized config can retain its original logical path;
            # after require_not_pending, only its physical store roots matter.
            state = str(paths.persistence_path(config.state_root, "authorized persistence root"))
            database = str(
                paths.persistence_path(config.tracking_db, "authorized tracking database")
            )
            if database not in value["store_births"]:
                value["store_births"][database] = {"intent_version": 1, "state_roots": []}
                changed = True
            record = value["store_births"][database]
            if state not in record["state_roots"]:
                record["state_roots"].append(state)
                changed = True
        else:
            raw = paths.read_bytes(config.path)
            if json.dumps(json.loads(raw), sort_keys=True) != json.dumps(
                config.raw, sort_keys=True
            ):
                raise ValueError("operational configuration changed during owner registration")
            observed = paths.configuration(config.path)
            records = value["configurations"].setdefault(str(config.path), [])
            record = {
                "config_sha256": hashlib.sha256(raw).hexdigest(),
                "state_root": observed["state_root"],
                "tracking_db": observed["tracking_db"],
            }
            if record not in records:
                records.append(record)
                changed = True
            database, state = observed["tracking_db"], observed["state_root"]
            if database not in value["store_observations"]:
                value["store_observations"][database] = {
                    "first_observed_absent": not Path(database).exists(),
                    "state_roots": [],
                }
                changed = True
            record = value["store_observations"][database]
            if state not in record["state_roots"]:
                record["state_roots"].append(state)
                changed = True
        if changed:
            paths.write_json(inventory, value)
        _PROCESS_LEASES[str(root)] = (os.getpid(), lock)
        acquired = False  # Hold SH until this CLI/API/worker process exits.
        return True
    finally:
        if inventory_lock is not None:
            os.close(inventory_lock)
        if acquired:
            os.close(lock)


def before_store_write(config):
    """Record possible schema/data creation before any authorized write connect."""
    key = (os.getpid(), str(Path(__file__).resolve().parents[3]), str(config.tracking_db))
    if key not in _STORE_INTENTS and observe_configuration(config, writing=True):
        _STORE_INTENTS.add(key)


def subprocess_options(config):
    """Carry actual production custody through exec before any child imports."""
    if not observe_configuration(config):
        return {}
    root = Path(__file__).resolve().parents[3]
    held = _PROCESS_LEASES.get(str(root))
    if held is None or held[0] != os.getpid():
        raise ValueError("production subprocess has no current installation custodian")
    descriptor = held[1]
    actual = os.fstat(descriptor)
    owner = Path.home().resolve() / ".local/share/devflow"
    expected = (owner / ".production-install.lock").stat()
    if (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino):
        raise ValueError("production subprocess installation witness changed")
    return {
        "pass_fds": (descriptor,),
        "env": {**os.environ, "DEVFLOW_INSTALLATION_LOCK_FD": str(descriptor)},
    }


def subprocess_command(config, command):
    """Start SDK children before their imports with their own source witness."""
    root = Path(__file__).resolve().parents[3]
    owner = Path.home().resolve() / ".local/share/devflow"
    if (
        root.parent.parent != owner / ".production-generations"
        or len(command) < 3
        or Path(command[0]).resolve() != Path(sys.executable).resolve()
        or command[1] not in {"-m", "-c"}
    ):
        return command
    if (
        _namespace_paths(config.tracking_db) is None
        and not os.environ.get("DEVFLOW_SOURCE_START_FD", "").isdigit()
    ):
        return command  # Preserve ordinary unqualified service startup.
    birth = json.loads(
        _source_private(owner / ".production-generation-births" / (root.parent.name + ".json"))
    )
    folder = Path(birth["startup"]["directory"])
    data = json.loads(_source_private(folder / "manifest.json"))
    return [
        data["base_python"],
        "-I",
        "-S",
        "-B",
        str(folder / "production-entry.py"),
        str(root),
        "--python",
        *command[1:],
    ]


def source_successor(config):
    """Explicit helper relocation for an acknowledged nonnative source cohort.

    This global generation receipt is separate from native v1 installation
    authority. It cannot relocate a native run or erase its existing gate.
    """
    from .contracts import digest

    root = Path(__file__).resolve().parents[3]
    if _namespace_paths(config.tracking_db) is not None and not os.environ.get(
        "DEVFLOW_NAMESPACE_ALLOCATION_FD"
    ):
        source_start(config)
    owner = Path.home().resolve() / ".local/share/devflow"
    if root.parent.parent != owner / ".production-generations":
        return None
    spec = importlib.util.spec_from_file_location(
        "generation_paths", root / "scripts/installation-paths.py"
    )
    paths = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(paths)
    layout = paths.layout_helper().installed(root)
    installed = paths.read_json(owner / ".production-install.json")
    reference = installed.get("source_successor")
    if not isinstance(reference, dict) or set(reference) != {"path", "sha256"}:
        raise ValueError("active source generation has no exact successor acknowledgement")
    file = Path(reference["path"])
    if (
        file.name != "receipt.json"
        or file.parent.parent != owner / ".production-transitions"
        or file.parent.name != root.parent.name
        or layout["active"].get("transition") != reference
    ):
        raise ValueError("source successor acknowledgement left its exact active namespace")
    raw = paths.read_bytes(file)
    value = json.loads(raw)
    if (
        hashlib.sha256(raw).hexdigest() != reference["sha256"]
        or value.get("schema") != "devflow.production-source-successor/v2"
        or value.get("after_root") != str(root)
    ):
        raise ValueError("active source successor acknowledgement changed")
    record = next((item for item in value["owners"] if item["path"] == str(config.path)), None)
    if record is None:
        return None  # A newly registered owner has no historical helper relocation.
    allowed = {digest(record["before"]), digest(record["after"])}
    for row in value["tracking"].get(record["tracking_db"], {}).get("runs", []):
        original = json.loads(row["request_json"])
        if original["config_path"] == str(config.path):
            allowed.add(original["config_digest"])
    if digest(config.raw) not in allowed:
        return None
    if (
        config.raw.get("state_root") != record["state_root"]
        or config.raw.get("tracking_db") != record["tracking_db"]
    ):
        raise ValueError("source successor changed original persistence authority")
    return {"root": root, "owner": record, "helpers": value["helpers"]}


def namespace_database_identity(database):
    descriptor = os.open(database, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info, named = os.fstat(descriptor), Path(database).lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
            or (info.st_dev, info.st_ino) != (named.st_dev, named.st_ino)
        ):
            raise ValueError("namespace tracking object is not a private owned file")
        # The containing directory pins the volume (including APFS UUID). A
        # device-number change across reboot does not change this object birth.
        return {
            "inode": info.st_ino,
            "uid": info.st_uid,
            "mode": stat.S_IMODE(info.st_mode),
            "birthtime": getattr(info, "st_birthtime", None),
        }
    finally:
        os.close(descriptor)


def require_namespace_database(database):
    """Called by the namespace's pinned state helper before ANY connection."""
    folder = _namespace_paths(database)
    if folder is None:
        return None
    root = Path(__file__).resolve().parents[3]
    owner = Path.home().resolve() / ".local/share/devflow"
    if (
        root.parent.parent != owner / ".production-generations"
        or Path(sys.prefix) != root / "runtime/.venv"
    ):
        raise ValueError(
            "namespace requires its active generation SDK and interpreter before tracking access"
        )
    qualified = None if os.environ.get("DEVFLOW_NAMESPACE_ALLOCATION_FD") else source_start()
    spec = importlib.util.spec_from_file_location(
        "namespace_paths", root / "scripts/installation-paths.py"
    )
    paths = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(paths)
    layout = paths.layout_helper().installed(root)
    if _namespace_bootstrap(paths, root, Path(database)) is not None:
        return {"bootstrapping": True}
    value = paths.read_json(folder / "witness.json")
    raw = paths.read_bytes(folder / "config.json")
    installed = paths.read_json(owner / ".production-install.json")
    if (
        value.get("schema") != "devflow.fresh-namespace/v1"
        or value.get("phase") != "verified"
        or value.get("namespace_id") != folder.name
        or value.get("root") != str(root)
        or value.get("owner_generation") != installed["owner_generation"]
        or value.get("birth_sha256") != layout["birth_sha256"]
        or value.get("database") != str(database)
        or value.get("config_sha256") != hashlib.sha256(raw).hexdigest()
        or not paths.same_node(value["namespace_identity"], paths.node(folder))
        or not paths.same_node(value["state_identity"], paths.node(folder / "state"))
        or namespace_database_identity(database) != value["database_identity"]
    ):
        raise ValueError("fresh namespace custody changed or has no completed allocation witness")
    from .contracts import digest
    from .delivery_config import DeliveryConfig

    config = DeliveryConfig._from_raw(folder / "config.json", json.loads(raw))
    marker = config.raw.get("installation_namespace")
    if marker != {
        "version": 1,
        "namespace_id": folder.name,
        "witness": str(folder / "witness.json"),
    }:
        raise ValueError("namespace configuration is not bound to its exact allocation witness")
    observe_configuration(config)
    if (
        paths.read_bytes(folder / "config.json") != raw
        or paths.read_json(folder / "witness.json") != value
        or paths.layout_helper().installed(root)["birth_sha256"] != value["birth_sha256"]
    ):
        raise ValueError("namespace custody changed before its lifetime lease was acquired")
    return {
        "namespace_id": folder.name,
        "witness": str(folder / "witness.json"),
        "witness_sha256": hashlib.sha256(paths.read_bytes(folder / "witness.json")).hexdigest(),
        "owner_generation": installed["owner_generation"],
        "config_sha256": value["config_sha256"],
        "config_digest": digest(config.raw),
        "database_identity": value["database_identity"],
        **qualified,
    }


@dataclass(frozen=True)
class FreshNamespaceScope:
    config: object
    witness: dict
    process: int

    def verify(self):
        from .contracts import digest

        if os.getpid() != self.process:
            raise ValueError("fresh namespace scope belongs to another process")
        _namespace_caller(self.config)
        actual = require_namespace_database(self.config.tracking_db)
        if actual != self.witness or digest(self.config.raw) != actual["config_digest"]:
            raise ValueError("fresh namespace scope changed during its admitted operation")
        return dict(actual)


def _namespace_caller(config):
    folder = _namespace_paths(config.tracking_db)
    if (
        folder is None
        or config.path != folder / "config.json"
        or config.state_root != folder / "state"
        or config.path.resolve(strict=True) != config.path
        or config.tracking_db.resolve(strict=True) != config.tracking_db
        or config.state_root.resolve(strict=True) != config.state_root
    ):
        raise ValueError(
            "fresh namespace caller must use the exact allocated config, state and tracking objects"
        )


@contextmanager
def fresh_namespace_scope(config):
    """Hold authenticated source custody through admission, pump and resume."""
    _namespace_caller(config)
    witness = require_namespace_database(config.tracking_db)
    if not witness or witness.get("bootstrapping"):
        raise ValueError("fresh namespace admission requires a completed private allocation")
    from .contracts import digest
    from .delivery_config import DeliveryConfig

    live = DeliveryConfig.load(config.path)
    if digest(live.raw) != digest(config.raw):
        raise ValueError("fresh namespace admission configuration changed")
    scope = FreshNamespaceScope(config, witness, os.getpid())
    scope.verify()
    yield scope
    scope.verify()
    # The process-level SH remains held. Closing a borrowed scope must never
    # unlock the shared open-file description held by a parent or sibling.
