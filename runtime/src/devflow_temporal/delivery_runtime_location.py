"""Explicit installed-location authority; frozen measurements stay historical.

An installation receipt never changes native_identity or the launcher's bundled
SDK equality. Only the public installer may acknowledge it. Readers join that
immutable receipt to every cohort command in the original tracking database.
"""

from __future__ import annotations

import contextvars
import hashlib
import json
import os
import sqlite3
import stat
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path

from .contracts import canonical_json, digest

KIND = "installed_runtime_location_acknowledged"
PENDING = "installed_runtime_location_pending"
REVERSED = "installed_runtime_location_rolled_back"
SCHEMA = "devflow-installed-runtime-location-v1"
RECEIPT_LIMIT = 16 * 1024 * 1024
ANSWER = "Migrate this stopped installation; preserve every original run and execution allowance"
_BOOTSTRAP = contextvars.ContextVar("devflow_runtime_location_bootstrap", default=None)


def source_root():
    return Path(__file__).resolve(strict=True).parents[3]


def private_bytes(path, *, maximum=4 * 1024 * 1024):
    """No healing, aliases, partial reads or blocking special files in authority."""
    path = Path(path)
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("installation evidence path is linked or not canonical")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != 1
            or before.st_size > maximum
        ):
            raise ValueError("installation evidence is not a bounded private owned file")
        with os.fdopen(os.dup(descriptor), "rb") as stream:
            raw = stream.read(maximum + 1)
        after = os.fstat(descriptor)
        keys = (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_uid",
            "st_nlink",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        named = path.lstat()
        if (
            len(raw) != before.st_size
            or any(getattr(before, key) != getattr(after, key) for key in keys)
            or any(getattr(after, key) != getattr(named, key) for key in keys)
        ):
            raise ValueError("installation evidence changed during observation")
        return raw
    finally:
        os.close(descriptor)


def receipt_bytes(path):
    return private_bytes(path, maximum=RECEIPT_LIMIT)


def command_id(receipt, run_id):
    return "runtime-location-" + digest([receipt["command"]["command_id"], run_id])[:48]


def intent_command_id(receipt, run_id):
    return "runtime-location-intent-" + digest([receipt["command"]["command_id"], run_id])[:48]


def pending_acknowledged(config, receipt, reference, *, db=None):
    """An original preimage on disk alone is not an attributable interrupted apply."""
    if db is None:
        connection = sqlite3.connect(config.tracking_db.as_uri() + "?mode=ro", uri=True)
        try:
            connection.execute("PRAGMA query_only=ON")
            return pending_acknowledged(config, receipt, reference, db=connection)
        finally:
            connection.close()
    for item in receipt["runs"]:
        key = intent_command_id(receipt, item["run_id"])
        response = {**reference, "run_id": item["run_id"], "command_id": key}
        row = db.execute(
            "SELECT request_digest,response_json FROM delivery_commands "
            "WHERE command_id=? AND run_id=?",
            (key, item["run_id"]),
        ).fetchone()
        event = db.execute(
            "SELECT 1 FROM delivery_events WHERE run_id=? AND type=? AND payload_json=?",
            (item["run_id"], PENDING, canonical_json(response)),
        ).fetchone()
        if not row or not event:
            return False
        if (
            row[0] != digest({"installation_intent": receipt["command"], "run_id": item["run_id"]})
            or json.loads(row[1]) != response
        ):
            raise ValueError("installation pending cohort changed")
    return True


def reversed_reference(config, reference, *, db=None):
    """A reversed intent is final even when its forward ACK never committed."""
    if db is None:
        with sqlite3.connect(config.tracking_db.as_uri() + "?mode=ro", uri=True) as source:
            source.execute("PRAGMA query_only=ON")
            return reversed_reference(config, reference, db=source)
    return bool(
        db.execute(
            "SELECT 1 FROM delivery_events WHERE type=? AND payload_json=? LIMIT 1",
            (REVERSED, canonical_json(reference)),
        ).fetchone()
    )


def _acknowledged(config, *, db=None, historical=False, reference=None):
    """A complete file and a partial cohort are both inert."""
    path = config.state_root / "runtime-installations/current.json"
    if reference is None and not os.path.lexists(path):
        return None
    pointer = reference if reference is not None else json.loads(private_bytes(path))
    receipt_path = Path(pointer["receipt"])
    expected = config.state_root / "runtime-installations" / pointer["command_id"] / "receipt.json"
    if receipt_path != expected:
        raise ValueError("installation receipt left its exact owner namespace")
    raw = receipt_bytes(receipt_path)
    if hashlib.sha256(raw).hexdigest() != pointer["receipt_sha256"]:
        raise ValueError("installation receipt changed")
    receipt = json.loads(raw)
    if (
        receipt.get("schema") != SCHEMA
        or receipt.get("transport") != _transport(config)
        or receipt["command"]["command_id"] != pointer["command_id"]
    ):
        raise ValueError("installation receipt belongs to another operational owner")
    if db is None:
        connection = sqlite3.connect(config.tracking_db.as_uri() + "?mode=ro", uri=True)
        try:
            connection.execute("PRAGMA query_only=ON")
            return _acknowledged(config, db=connection, historical=historical, reference=pointer)
        finally:
            connection.close()
    for item in receipt["runs"]:
        key = command_id(receipt, item["run_id"])
        saved = db.execute(
            "SELECT request_digest,response_json FROM delivery_commands "
            "WHERE command_id=? AND run_id=?",
            (key, item["run_id"]),
        ).fetchone()
        response = {
            **pointer,
            "run_id": item["run_id"],
            "command_id": key,
            "original_request_digest": item["request_digest"],
        }
        request_digest = digest({"installation": receipt["command"], "run_id": item["run_id"]})
        event = db.execute(
            "SELECT payload_json FROM delivery_events WHERE run_id=? AND type=? "
            "AND payload_json=? LIMIT 1",
            (item["run_id"], KIND, canonical_json(response)),
        ).fetchone()
        if not saved or not event:
            return None
        if saved[0] != request_digest or json.loads(saved[1]) != response:
            raise ValueError("installation cohort acknowledgement changed")
        original = db.execute(
            "SELECT request_digest,request_json FROM delivery_runs WHERE run_id=?",
            (item["run_id"],),
        ).fetchone()
        if (
            not original
            or original[0] != item["request_digest"]
            or hashlib.sha256(original[1].encode()).hexdigest() != item["request_json_sha256"]
            or json.loads(original[1])["config_path"] != receipt["owner_path"]
        ):
            raise ValueError("installation original owner or submitted authority changed")
    if reversed_reference(config, pointer, db=db) and not historical:
        return None
    if not historical:
        from .delivery_config import DeliveryConfig

        owner_path = Path(receipt["owner_path"])
        live = DeliveryConfig._from_raw(owner_path, json.loads(private_bytes(owner_path)))
        if (
            live.raw["codex_bin"] != receipt["destination"]["identity"]["codex_bin"]
            or str(live.helpers_dir) != receipt["helpers_dir"]
            or _transport(live) != receipt["transport"]
        ):
            raise ValueError("acknowledged live installation location/transport changed")
    return receipt


def _transport(config):
    from .delivery_execution_config import transport_binding

    return [str(value) for value in transport_binding(config)]


def _owner_config(spec):
    """Find the original owner without appointing a private amendment as owner."""
    from .delivery_amendment_capacity import submitted_owner

    _original, owner = submitted_owner(spec)
    return owner


def _binding(spec, *, historical=False):
    if not os.path.lexists(
        Path(spec["state_dir"]).parents[1] / "runtime-installations/current.json"
    ):
        return None
    owner = _owner_config(spec)
    receipt = _acknowledged(owner, historical=historical)
    if receipt is None:
        return None
    item = next((entry for entry in receipt["runs"] if entry["run_id"] == spec["run_id"]), None)
    if (
        receipt["owner_path"] != str(owner.path)
        or not item
        or item["request_digest"] != spec.get("request_digest")
    ):
        raise ValueError("installation authority does not include this original run")
    for key in (
        "run_id",
        "work_id",
        "state_dir",
        "checkout",
        "source_path",
        "github_repo",
        "origin_url",
        "repository_key",
        "provider",
        "authorized_endpoint",
    ):
        if spec.get(key) != item["original_spec"].get(key):
            raise ValueError("installation authority changed original source or work custody")
    return receipt, item


def historical_proof(spec):
    """Authenticate only an explicitly retained old spec, never launch it."""
    value = _binding(spec, historical=True)
    if value is None:
        return False
    _receipt, item = value
    if digest(spec) not in item["historical_spec_digests"]:
        return False
    from .delivery_native_renewal import _old_proof

    _old_proof(spec)
    return True


def same_program_location(expected, actual):
    """Prove this one measured successor path; never exclude paths wholesale."""
    if expected["policy"].get("codex_bin") == actual["policy"].get("codex_bin"):
        return True
    # This is provenance comparison, also used while reading an older closed
    # admission. Active launch permission is checked separately by the actual
    # preparation and launcher; rollback must not erase historical provenance.
    value = _binding(expected, historical=True)
    if value is None:
        return False
    receipt, item = value
    previous = next(
        (
            value
            for value in item["historical_specs"]
            if value["policy"]["codex_bin"] == expected["policy"]["codex_bin"]
            and value["policy"]["native_identity"] == expected["policy"].get("native_identity")
        ),
        None,
    )
    if previous is None:
        raise ValueError("installation transition has no recorded original native predecessor")
    before, after = previous["policy"], receipt["destination"]["identity"]
    if (
        expected["policy"].get("codex_bin") != before["codex_bin"]
        or actual["policy"].get("codex_bin") != after["codex_bin"]
        or actual["policy"].get("codex_bin_sha256") != before["codex_bin_sha256"]
    ):
        raise ValueError("installation successor is not the acknowledged bundled executable")
    from .delivery_native_renewal import _old_proof
    from .delivery_runtime_installation import validate_measured_location

    validate_measured_location(
        expected, actual["policy"]["native_identity"], Path(receipt["destination"]["root"])
    )
    _old_proof(actual)
    return True


def require_preparation_location(spec, binary):
    if str(binary) == spec["policy"]["codex_bin"]:
        return
    value = _binding(spec)
    if value is None:
        raise ValueError("new installed program location requires public acknowledgement")
    receipt, _item = value
    if str(binary) != receipt["destination"]["identity"]["codex_bin"]:
        raise ValueError("preparation selected a different installed bundled executable")
    # The grant owns the installed objects/paths. A later clean committed
    # payload still needs the existing fresh native preparation owner.
    validate_destination(receipt["destination"], spec, strict_source=False)


def require_execution_location(spec):
    """Fresh admitted runs own their new path; old cohort launches need active adoption."""
    if spec.get("provider") != "codex":
        return
    if not spec.get("preparation"):
        # The existing native measurement owner removes the predecessor's
        # preparation/identity, measures the actual imported bundle, and runs
        # its fresh probe in a separate (unadmitted) preparation directory.
        # prepare_runtime already checks the original location grant before
        # entering that owner. A stored prepared proof never takes this path.
        return
    from .delivery_execution_config import load_execution_config

    require_not_pending(load_execution_config(spec))
    if not os.path.lexists(
        Path(spec["state_dir"]).parents[1] / "runtime-installations/current.json"
    ):
        return
    owner = _owner_config(spec)
    historical = _acknowledged(owner, historical=True)
    if historical is None:
        raise ValueError("installed location cohort is not completely acknowledged")
    if _acknowledged(owner) is None:
        raise ValueError("installed location permission was rolled back")
    item = next((value for value in historical["runs"] if value["run_id"] == spec["run_id"]), None)
    if item is None:
        return  # A new registration has its own strict current native proof.
    active = _binding(spec)
    if active is None:
        raise ValueError("installed location permission was rolled back")
    receipt, _item = active
    if spec["policy"]["codex_bin"] != receipt["destination"]["identity"]["codex_bin"]:
        raise ValueError("historical installed program location must be prepared before launch")
    validate_destination(receipt["destination"], item["original_spec"], strict_source=False)


def verify_historical_prepared_spec(spec):
    if historical_proof(spec):
        return
    from .delivery_native_preparation import verify_native_spec

    verify_native_spec(spec)


def bind_provenance(effective, original, *, preserve_measurement=True):
    """A proposal rebinds a recorded proof; only prepare_runtime can measure a successor."""
    from .delivery_native_preparation import bind_native_spec

    historical = historical_proof(original)
    verify_historical_prepared_spec(original)
    return bind_native_spec(
        effective,
        original["policy"]["native_identity"],
        Path(original["preparation"]["environment"]["path"]),
        reused=True,
        measured=(
            original["preparation"].get("measured_policy_digest") if preserve_measurement else None
        ),
        historical_parent=original if historical else None,
    )


def policy_for_comparison(expected, actual):
    value = deepcopy(actual["policy"])
    if value.get("codex_bin") != expected["policy"].get("codex_bin"):
        if not same_program_location(expected, actual):
            raise ValueError("installed program location has no durable acknowledgement")
        value["codex_bin"] = expected["policy"]["codex_bin"]
    return value


def compatible_owner(spec, frozen, live):
    value = _binding(spec)
    if value is None:
        return False
    receipt, _item = value
    if (
        str(frozen.path) != receipt["owner_path"]
        or live.path != frozen.path
        or live.raw.get("codex_bin") != receipt["destination"]["identity"]["codex_bin"]
    ):
        return False
    before, after = deepcopy(frozen.raw), deepcopy(live.raw)
    before.pop("capacity", None)
    after.pop("capacity", None)
    after["codex_bin"] = before["codex_bin"]
    return before == after


def helper_path(config, *, read_only=False):
    boot = _BOOTSTRAP.get()
    if (
        boot
        and _transport(config) == boot["transport"]
        and (str(config.helpers_dir) == boot["helpers_dir"])
    ):
        return source_root() / "skills/devflow/scripts/state.py"
    receipt = _acknowledged(config)
    if receipt is None and read_only:
        receipt = _acknowledged(config, historical=True)
    if receipt is None:
        if (
            os.path.lexists(config.state_root / "runtime-installations/current.json")
            and _acknowledged(config, historical=True) is None
        ):
            raise ValueError("installed helper cohort is not completely acknowledged")
        return config.helpers_dir / "state.py"
    if str(config.helpers_dir) != receipt["helpers_dir"]:
        raise ValueError("installation helper registration differs from original authority")
    root = Path(receipt["destination"]["root"]) / "skills/devflow/scripts"
    from .delivery_runtime_registrations import _read

    for name in ("state.py", "schema.sql"):
        if (
            hashlib.sha256(_read(root / name)).hexdigest()
            != receipt["destination"]["helpers"][name]
        ):
            raise ValueError("acknowledged installed shared helper or schema changed")
    return root / "state.py"


def require_not_pending(config):
    """A new-code writer cannot pass a staged, unacknowledged location intent."""
    if _BOOTSTRAP.get():
        return
    pending = config.state_root / "runtime-installations/pending.json"
    if not os.path.lexists(pending):
        folder = pending.parent
        if (
            not folder.exists()
            or os.path.lexists(folder / "current.json")
            or not config.tracking_db.exists()
        ):
            return
        with sqlite3.connect(config.tracking_db.as_uri() + "?mode=ro", uri=True) as db:
            db.execute("PRAGMA query_only=ON")
            values = db.execute(
                "SELECT payload_json FROM delivery_events WHERE type=? ORDER BY sequence DESC",
                (PENDING,),
            )
            if any(Path(json.loads(row[0])["receipt"]).is_relative_to(folder) for row in values):
                raise ValueError(
                    "runtime installation is incomplete; replay its public apply or rollback"
                )
        return
    reference = json.loads(private_bytes(pending))
    if _acknowledged(config, reference=reference) is None:
        raise ValueError("runtime installation is incomplete; replay its public apply or rollback")


def require_ready(config):
    """Fence execution-critical new code; this does not fence old tracker writers."""
    if _BOOTSTRAP.get():
        return
    require_not_pending(config)
    pending = config.state_root / "runtime-installations/pending.json"
    has_pending = os.path.lexists(pending)
    if not has_pending and not os.path.lexists(
        config.state_root / "runtime-installations/current.json"
    ):
        return
    value = json.loads(private_bytes(pending)) if has_pending else None
    receipt = _acknowledged(config)
    if receipt is None and not has_pending:
        if _acknowledged(config, historical=True) is None:
            raise ValueError("installed runtime cohort is not completely acknowledged")
        return  # Rollback restored old registrations, not a healthy absent runtime.
    if receipt is None or (value and receipt["command"]["command_id"] != value["command_id"]):
        raise ValueError("runtime installation is incomplete; replay its public apply or rollback")
    raw = private_bytes(Path(receipt["owner_path"]))
    if has_pending and hashlib.sha256(raw).hexdigest() != receipt["owner_after_sha256"]:
        raise ValueError("acknowledged runtime owner configuration is not installed")
    if str(config.path) == receipt["owner_path"] and digest(config.raw) != digest(json.loads(raw)):
        from .delivery_config import DeliveryConfig

        live = DeliveryConfig._from_raw(config.path, json.loads(raw))
        previous = next(
            (
                spec
                for item in receipt["runs"]
                for spec in item["historical_specs"]
                if spec["config_path"] == str(config.path)
                and spec["config_digest"] == digest(config.raw)
            ),
            None,
        )
        if previous is None or not compatible_owner(previous, config, live):
            raise ValueError("operational installation configuration changed during bootstrap")
    validate_destination(
        receipt["destination"], receipt["runs"][0]["original_spec"], strict_source=False
    )


def validate_destination(expected, original, *, strict_source=True):
    """Implemented by the installation owner, also called freshly by consumers."""
    from .delivery_runtime_installation import destination, same_destination

    if not same_destination(expected, destination(original), strict_source=strict_source):
        raise ValueError("acknowledged installed runtime destination changed")


@contextmanager
def bootstrap(config, authenticated):
    """Only the public installation owner supplies already authenticated inputs."""
    token = _BOOTSTRAP.set(
        {"transport": _transport(config), "helpers_dir": authenticated["helpers_dir"]}
    )
    try:
        yield
    finally:
        _BOOTSTRAP.reset(token)
