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
import time
from pathlib import Path

PROTOCOL = 1
_PROCESS_LEASES = {}
_STORE_INTENTS = set()


def observe_configuration(config, *, writing=False):
    from . import delivery_execution_config

    # The lower installation layer also supports the pre-observation SDK;
    # composed #240 readers supply this existing context before loading configs.
    check_observation = getattr(delivery_execution_config, "is_observation_mode", None)
    observing = bool(check_observation and check_observation())
    if observing and writing:
        raise ValueError("observational configuration cannot create a store write intent")
    root = Path(__file__).resolve().parents[3]
    if root != Path.home().resolve() / ".local/share/devflow/developer-workflow":
        return
    receipt = root.parent / ".production-install.json"
    if not receipt.exists():
        return  # Legacy owner: readable, but no complete-update inventory claim.
    spec = importlib.util.spec_from_file_location(
        "installation_paths", root / "scripts/installation-paths.py"
    )
    paths = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(paths)
    registered = paths.read_json(receipt)
    if registered.get("managed_owner_version") != PROTOCOL:
        return
    if os.environ.get("DEVFLOW_INSTALLATION_PROBE_FD"):
        raise ValueError("controller readiness probes cannot load operational configuration")
    held = _PROCESS_LEASES.get(str(root))
    lock = held[1] if held and held[0] == os.getpid() else None
    acquired = lock is None
    if acquired:
        inherited = os.environ.pop("DEVFLOW_INSTALLATION_LOCK_FD", "")
        if inherited.isdigit():
            candidate = int(inherited)
            try:
                actual = os.fstat(candidate)
                expected = (root.parent / ".production-install.lock").stat()
                if (actual.st_dev, actual.st_ino) == (expected.st_dev, expected.st_ino):
                    lock = candidate
            except OSError:
                pass
        if lock is None:
            lock = os.open(root.parent / ".production-install.lock", os.O_RDWR | os.O_NOFOLLOW)
    inventory_lock = None
    try:
        fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
        registered = paths.read_json(receipt)
        identity = paths.node(root)
        if (registered.get("phase") != "verified"
                or not paths.same_node(registered.get("target_identity"), identity)):
            raise ValueError("production installation is unfinished or its owner changed")
        update = root.parent / ".production-update.json"
        if update.exists() and paths.read_json(update).get("phase") not in {
            "verified", "rolled_back"
        }:
            raise ValueError("production update is unfinished; run its retained recovery command")
        if observing:
            value = paths.read_inventory(root, registered)
            raw = paths.read_bytes(config.path)
            observed = paths.configuration(config.path)
            record = {"config_sha256": hashlib.sha256(raw).hexdigest(),
                      "state_root": observed["state_root"], "tracking_db": observed["tracking_db"]}
            if (json.dumps(json.loads(raw), sort_keys=True)
                    != json.dumps(config.raw, sort_keys=True)
                    or record not in value["configurations"].get(str(config.path), [])):
                raise ValueError("production configuration custody is unknown during observation")
            _PROCESS_LEASES[str(root)] = (os.getpid(), lock)
            acquired = False
            return True
        inventory_lock = os.open(root.parent / ".production-inventory.lock",
                                 os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
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
            if (json.dumps(json.loads(raw), sort_keys=True)
                    != json.dumps(config.raw, sort_keys=True)):
                raise ValueError("operational configuration changed during owner registration")
            observed = paths.configuration(config.path)
            records = value["configurations"].setdefault(str(config.path), [])
            record = {"config_sha256": hashlib.sha256(raw).hexdigest(),
                      "state_root": observed["state_root"], "tracking_db": observed["tracking_db"]}
            if record not in records:
                records.append(record)
                changed = True
            database, state = observed["tracking_db"], observed["state_root"]
            if database not in value["store_observations"]:
                value["store_observations"][database] = {
                    "first_observed_absent": not Path(database).exists(), "state_roots": []}
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
    expected = (root.parent / ".production-install.lock").stat()
    if (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino):
        raise ValueError("production subprocess installation witness changed")
    return {"pass_fds": (descriptor,), "env": {
        **os.environ, "DEVFLOW_INSTALLATION_LOCK_FD": str(descriptor)}}
