"""Explicit current-object custody after mount device renumbering.

Legacy device/inode records cannot prove historical volume continuity. This grant
adopts only inventoried current objects; original evidence and execution authority
remain unchanged. A staged file is inert until its command commits in SQLite.
"""

from __future__ import annotations

import asyncio
import ctypes
import errno
import fcntl
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from contextvars import copy_context
from pathlib import Path

from .contracts import canonical_json, digest

KIND = "filesystem_custody_adopted"
ANSWER = "adopt_current_filesystem"
SOURCE_FIELDS = (
    "head",
    "workspace_declaration_sha256",
    "configured_environment_sha256",
    "accepted_plan_sha256",
)


def _raw(info):
    return {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid}


def _volume(descriptor):
    """Read a filesystem UUID from the opened object, never a pathname guess."""
    if sys.platform != "darwin":
        raise ValueError("filesystem custody adoption requires native macOS volume inspection")

    class Attributes(ctypes.Structure):
        _fields_ = [
            ("count", ctypes.c_uint16),
            ("reserved", ctypes.c_uint16),
            ("common", ctypes.c_uint32),
            ("volume", ctypes.c_uint32),
            ("directory", ctypes.c_uint32),
            ("file", ctypes.c_uint32),
            ("fork", ctypes.c_uint32),
        ]

    attributes = Attributes(count=5, volume=0x80040000)  # ATTR_VOL_INFO | ATTR_VOL_UUID
    buffer = ctypes.create_string_buffer(20)
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.fgetattrlist(descriptor, ctypes.byref(attributes), buffer, len(buffer), 0) != 0:
        raise OSError(ctypes.get_errno(), "filesystem UUID inspection unavailable")
    if int.from_bytes(buffer.raw[:4], sys.byteorder) != 20 or not any(buffer.raw[4:20]):
        raise ValueError("filesystem UUID inspection is incomplete")
    return str(uuid.UUID(bytes=buffer.raw[4:20]))


def _boot():
    if sys.platform != "darwin":
        raise ValueError("filesystem custody adoption requires native macOS boot inspection")
    libc = ctypes.CDLL(None, use_errno=True)
    buffer = ctypes.create_string_buffer(128)
    length = ctypes.c_size_t(len(buffer))
    if libc.sysctlbyname(b"kern.bootsessionuuid", buffer, ctypes.byref(length), None, 0) != 0:
        raise OSError(ctypes.get_errno(), "boot UUID inspection unavailable")
    return str(uuid.UUID(buffer.value.decode("ascii")))


def _metadata(descriptor):
    info = os.fstat(descriptor)
    return {
        "identity": _raw(info),
        "mode": stat.S_IMODE(info.st_mode),
        "type": stat.S_IFMT(info.st_mode),
        "birthtime": getattr(info, "st_birthtime", None),
        "volume_uuid": _volume(descriptor),
    }


@contextmanager
def _opened(path, *, lock=False):
    from .delivery_resources import _ancestors

    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("filesystem custody path is not canonical")
    _ancestors(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:

        def check():
            info, current = os.fstat(descriptor), path.lstat()
            if (
                _raw(info) != _raw(current)
                or info.st_uid != os.getuid()
                or not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode))
                or (
                    stat.S_ISREG(info.st_mode)
                    and (stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1)
                )
            ):
                raise ValueError("filesystem custody object changed or is not privately owned")
            _ancestors(path)
            return info

        check()
        if lock:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            check()
        yield descriptor
        check()
    finally:
        os.close(descriptor)


def _acknowledged(spec):
    """An unacknowledged/staged receipt cannot change an identity comparison."""
    from .delivery_execution_config import load_execution_config, transport_digest
    from .delivery_resources import _ancestors, read_private

    if "config_path" not in spec:
        return None
    config = load_execution_config(spec)
    from .delivery_store import tracking_read_connection

    with tracking_read_connection(config.tracking_db) as db:
        row = db.execute(
            "SELECT payload_json FROM delivery_events WHERE run_id=? AND type=? "
            "ORDER BY sequence DESC LIMIT 1",
            (spec["run_id"], KIND),
        ).fetchone()
        if row is None:
            return None
        acknowledgement = json.loads(row[0])
        command = db.execute(
            "SELECT request_digest,response_json FROM delivery_commands "
            "WHERE command_id=? AND run_id=?",
            (acknowledgement["command_id"], spec["run_id"]),
        ).fetchone()
        original = db.execute(
            "SELECT request_digest FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
    path = (
        Path(spec["state_dir"]) / "filesystem-custody" / (acknowledgement["command_id"] + ".json")
    )
    _ancestors(path)
    raw = read_private(path, raw=True)
    receipt = json.loads(raw)
    if (
        command is None
        or original is None
        or json.loads(command[1]) != acknowledgement
        or hashlib.sha256(raw).hexdigest() != acknowledgement.get("receipt_sha256")
        or command[0] != digest({"run_id": spec["run_id"], **receipt["command"]})
        or receipt.get("kind") != KIND
        or receipt.get("run_id") != spec["run_id"]
        or receipt["evidence"]["state_dir"] != spec["state_dir"]
        or receipt["evidence"]["row"]["request_digest"] != original[0]
        or receipt["evidence"]["service_binding"] != transport_digest(config)
    ):
        raise ValueError("filesystem custody receipt lost its acknowledged authority")
    return receipt


def identity_for(spec, path, current, expected):
    """Normalize one adopted object to its legacy identity, never a device map."""
    if spec is None or not os.path.lexists(Path(spec["state_dir"]) / "filesystem-custody"):
        return current
    receipt = _acknowledged(spec)
    if receipt is None:
        return current
    entry = receipt["evidence"]["objects"].get(str(path))
    if entry is None or expected != entry["original"]:
        return current
    from .delivery_resources import read_private

    registration = entry["registration"]
    source = (
        registration["journal"]
        if registration["kind"] == "lifecycle-lock"
        else str(Path(spec["state_dir"]) / "resources/manifest.json")
    )
    sha = receipt["evidence"]["originals"][source]
    archive = Path(spec["state_dir"]) / "filesystem-custody/originals" / (sha + ".json")
    if hashlib.sha256(read_private(archive, raw=True)).hexdigest() != sha:
        raise ValueError("filesystem custody original evidence archive changed")
    manifest = read_private(Path(spec["state_dir"]) / "resources/manifest.json")
    if registration["kind"] == "state":
        valid = manifest.get("state_identity") == entry["original"]
    elif registration["kind"] == "lifecycle-lock":
        journal = read_private(Path(registration["journal"]))
        valid = (
            digest(journal.get("intent")) == registration["intent_sha256"]
            and journal.get("lifecycle_locks", {}).get(path.name) == entry["original"]
        )
    else:
        root = manifest.get("roots", {}).get(str(path), {})
        valid = (
            root.get("kind") == registration["kind"]
            and root.get("generation", 0) == registration["generation"]
            and root.get("identity") == entry["original"]
            and root.get("state") not in {"removed", "already_absent"}
            and root.get("receipt", {}).get("state") not in {"removed", "already_absent"}
            and {key: root.get(key) for key in SOURCE_FIELDS} == registration["source"]
        )
    if not valid:
        return current  # A new resource generation never inherits a preceding adoption.
    if current != entry["current"]["identity"]:
        raise ValueError("adopted filesystem object identity changed")
    with _opened(path) as descriptor:
        if _metadata(descriptor) != entry["current"] or _boot() != receipt["evidence"]["boot_uuid"]:
            raise ValueError("adopted filesystem object or boot identity changed")
    return entry["original"]


def matches(spec, path, recorded, *, current=None, directory=True):
    """Compare an expected legacy identity; new allocations always retain raw stat."""
    info = path.lstat()
    if (
        info.st_uid != os.getuid()
        or not (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode))
        or (not directory and (stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1))
    ):
        raise ValueError("filesystem custody object type or ownership changed")
    physical = _raw(info)
    if current is not None and current != physical:
        return False  # An opened descriptor and its current name must still agree.
    current = physical
    return identity_for(spec, path, current, recorded) == recorded


def _workflow(store, row):
    from temporalio.client import Client

    from .delivery_codec import DELIVERY_DATA_CONVERTER

    async def read():
        client = await Client.connect(
            store.config.temporal_address,
            namespace=store.config.raw.get("temporal_namespace", "default"),
            data_converter=DELIVERY_DATA_CONVERTER,
        )
        workflow_id = row["workflow_id"] or "delivery-" + row["run_id"]
        description = await client.get_workflow_handle(workflow_id).describe()
        recovery = json.loads(row["recovery_json"]) if row["recovery_json"] else None
        if (
            description.id != workflow_id
            or await description.memo_value("request_digest", None) != row["request_digest"]
            or await description.memo_value("recovery_digest", None)
            != (digest(recovery) if recovery else None)
        ):
            raise ValueError("filesystem custody lost its original Temporal execution")
        return {
            "workflow_id": description.id,
            "execution_run_id": description.run_id,
            "status": description.status.name,
        }

    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(
            copy_context().run, lambda: asyncio.run(asyncio.wait_for(read(), 20))).result()


def _rows(store, run_id):
    with store._connect() as db:
        row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise ValueError("filesystem custody run is absent")
        attempts = [
            dict(r)
            for r in db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=? ORDER BY job_key", (run_id,)
            )
        ]
        if any(
            db.execute(
                f"SELECT 1 FROM {table} WHERE run_id=? AND state IN "
                "('pending','unknown','dispatching') LIMIT 1",
                (run_id,),
            ).fetchone()
            for table in ("delivery_effects", "delivery_mutations")
        ):
            raise ValueError("filesystem custody cannot settle an uncertain external effect")
    return dict(row), attempts


def _collect(store, run_id, leases):
    from .candidate import candidate_for
    from .delivery_execution_config import load_execution_config, transport_digest
    from .delivery_native_process import listeners, process_table
    from .delivery_resources import read_private
    from .supervisor import DeliverySupervisor

    row, attempts = _rows(store, run_id)
    spec = store.effective_spec(run_id)
    config = load_execution_config(spec)
    if (
        spec.get("provider") != "codex"
        or not store.owns_execution(spec)
        or spec["policy"].get("execution_backend") != "native-macos"
        or spec.get("feature_delivery")
        or spec.get("feature_worker")
        or any(a["state"] not in {"finished", "unknown"} for a in attempts)
    ):
        raise ValueError("filesystem adoption requires an owned quarantined native run")
    state = Path(spec["state_dir"])
    manifest_path = state / "resources/manifest.json"
    leases.enter_context(_opened(manifest_path.with_name("ownership.lock"), lock=True))
    manifest = read_private(manifest_path)
    if manifest.get("run_id") != run_id:
        raise ValueError("filesystem custody resource manifest belongs to another run")
    workflow = _workflow(store, row)
    with store._connect() as db:
        claim = store.state.claim_for(db, spec["work_id"])
        claim = dict(claim) if claim is not None else None
    if claim is not None and claim["owner"] != "external:devflow:" + run_id:
        raise ValueError("filesystem custody work claim belongs to another owner")
    if workflow["status"] == "RUNNING" and claim is None:
        raise ValueError("an open workflow lost its retained work claim")
    if workflow["status"] == "RUNNING" and not any(a["state"] == "unknown" for a in attempts):
        raise ValueError("an open workflow must have a durably quarantined unknown attempt")
    evidence = {
        "state_dir": str(state),
        "row": row,
        "attempts": attempts,
        "workflow": workflow,
        "claim": claim,
        "service_binding": transport_digest(config),
        "boot_uuid": _boot(),
        "objects": {},
        "originals": {},
        "candidate": candidate_for(Path(spec["checkout"])),
    }
    originals, descriptors = {}, []

    def original(path):
        raw = read_private(path, raw=True)
        sha = hashlib.sha256(raw).hexdigest()
        evidence["originals"][str(path)] = sha
        originals[sha] = raw
        return json.loads(raw)

    def adopt(path, old, registration, *, lock=False):
        if not isinstance(old, dict) or set(old) != {"device", "inode", "uid"}:
            raise ValueError("filesystem custody lacks an exact original identity")
        descriptor = leases.enter_context(_opened(path, lock=lock))
        descriptors.append((path, descriptor))
        current = _metadata(descriptor)
        raw = current["identity"]
        if current["type"] != (stat.S_IFREG if lock else stat.S_IFDIR):
            raise ValueError("filesystem custody object type changed")
        if raw["inode"] != old["inode"] or raw["uid"] != old["uid"]:
            raise ValueError("filesystem custody permits only device-number drift")
        if raw != old:
            evidence["objects"][str(path)] = {
                "original": old,
                "current": current,
                "registration": registration,
            }

    original(manifest_path)
    if manifest_path.with_name("finalization.json").exists():
        original(manifest_path.with_name("finalization.json"))
    adopt(state, manifest["state_identity"], {"kind": "state"})
    for raw, entry in sorted(manifest["roots"].items()):
        path = Path(raw)
        if not path.exists() and not path.is_symlink():
            continue  # Absence is observed, never adopted as a recreated resource.
        if entry.get("state") in {"removed", "already_absent"} or entry.get("receipt", {}).get(
            "state"
        ) in {"removed", "already_absent"}:
            raise ValueError("filesystem custody cannot adopt a recreated ended-custody root")
        if entry.get("identity") is None:
            raise ValueError("filesystem custody cannot adopt an unproven generated root")
        adopt(
            path,
            entry["identity"],
            {
                "kind": entry["kind"],
                "generation": entry.get("generation", 0),
                "source": {key: entry.get(key) for key in SOURCE_FIELDS},
            },
        )
    actors, ports = [], set()
    for raw in manifest["processes"]:
        path = Path(raw)
        if not path.is_relative_to(state) or path.name != "native-process.json":
            raise ValueError("filesystem custody journal left its owned run")
        journal = original(path)
        if journal.get("intent", {}).get("run_id") != run_id:
            raise ValueError("filesystem custody journal belongs to another run")
        if not journal.get("monitor"):
            raise ValueError("filesystem custody has incomplete native actor evidence")
        actors.extend((int(pid), entry) for pid, entry in journal.get("owned", {}).items())
        monitor = journal["monitor"]
        actors.append((monitor["pid"], monitor))
        ports.update(journal.get("ports", []))
        for name in ("native-monitor.lock", "native-process.lock"):
            adopt(
                path.with_name(name),
                journal.get("lifecycle_locks", {}).get(name),
                {
                    "kind": "lifecycle-lock",
                    "journal": raw,
                    "intent_sha256": digest(journal["intent"]),
                },
                lock=True,
            )
        for name in ("request.json", "launch.json", "result.json"):
            if path.with_name(name).exists():
                original(path.with_name(name))
    for attempt in attempts:
        owner = DeliverySupervisor._owner_lock_path(spec, attempt["job_key"])
        if owner.exists():
            leases.enter_context(_opened(owner, lock=True))
    if any(type(pid) is not int or pid <= 1 or not actor.get("identity") for pid, actor in actors):
        raise ValueError("filesystem custody has incomplete native identities")
    table = process_table()
    if any(
        table.get(pid, {}).get("identity") == actor.get("identity")
        and not table[pid]["stat"].startswith("Z")
        for pid, actor in actors
    ):
        raise ValueError("filesystem custody still has a live original native actor")
    if any(listeners(port) for port in ports):
        raise ValueError("filesystem custody still has an occupied native port")
    if not evidence["objects"]:
        raise ValueError("filesystem custody has no device-only drift to adopt")
    if (
        len(
            {
                (e["original"]["device"], e["current"]["identity"]["device"])
                for e in evidence["objects"].values()
            }
        )
        > 1
    ):
        raise ValueError("filesystem custody has ambiguous volume/device drift")
    # These bytes and descriptors must remain exact until the publication transaction.
    return spec, evidence, originals, descriptors


def _lifecycle_facts(evidence, request):
    evidence_sha256 = digest(evidence)
    return {
        "operation": "adopt_filesystem_custody", "owner": "filesystem_custody",
        "owner_proof": True, "normalized_lifecycle": "stopped", "proof_digest": evidence_sha256,
        "revision": evidence["row"]["revision"], "evidence_sha256": evidence_sha256,
        "comparisons": [("revision", "expected_revision", "revision_changed"),
                        ("evidence_sha256", "expected_evidence_sha256", "custody_changed")],
        "predicates": [{"code": "filesystem_adoption_authorization_required",
                        "satisfied": request.get("answer") == ANSWER,
                        "missing_or_unknown_fact": "explicit current-object adoption decision",
                        "next_action": "provide the explicit adoption decision",
                        "new_authorization_required": True}],
    }


def preflight(store, run_id):
    from .delivery_interrupted_recovery import installation_recovery_lease

    with installation_recovery_lease(store.config, exclusive=True), ExitStack() as leases:
        _, evidence, _, _ = _collect(store, run_id, leases)
        from .delivery_lifecycle import evaluate_transition

        request = {"expected_revision": evidence["row"]["revision"],
                   "expected_evidence_sha256": digest(evidence)}
        return {
            "run_id": run_id,
            "lifecycle_assessment": evaluate_transition(
                _lifecycle_facts(evidence, request), request),
            "expected_revision": evidence["row"]["revision"],
            "expected_evidence_sha256": digest(evidence),
            "objects": evidence["objects"],
            "workflow": evidence["workflow"],
            "historical_volume_continuity": "unproven",
            "answer": ANSWER,
        }


def _publish_exclusive(stage, path):
    """Atomically rename without replacement and without a two-hard-link window."""
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "darwin":
        rename = libc.renamex_np
        rename.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
        result = rename(os.fsencode(stage), os.fsencode(path), 0x00000004)  # RENAME_EXCL
    elif sys.platform == "linux" and hasattr(libc, "renameat2"):
        rename = libc.renameat2
        rename.argtypes = (
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        )
        result = rename(-100, os.fsencode(stage), -100, os.fsencode(path), 1)  # NOREPLACE
    else:
        raise ValueError("atomic exclusive custody publication is unavailable")
    if result != 0:
        error = ctypes.get_errno()
        if error == errno.EEXIST:
            raise FileExistsError(error, os.strerror(error), str(path))
        raise OSError(error, os.strerror(error), str(path))


def _immutable(path, raw):
    from .delivery_resources import private_directory, read_private

    private_directory(path.parent)
    descriptor, name = tempfile.mkstemp(prefix=".custody-", dir=path.parent)
    stage = Path(name)
    staged_identity = _raw(os.fstat(descriptor))
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        if _raw(stage.lstat()) != staged_identity:
            raise ValueError("filesystem custody staging object changed before publication")
        try:
            _publish_exclusive(stage, path)
        except FileExistsError:
            if read_private(path, raw=True) != raw:
                raise ValueError("filesystem custody immutable receipt conflicts") from None
        if read_private(path, raw=True) != raw:
            raise ValueError("filesystem custody published object changed")
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.lexists(stage):
            if _raw(stage.lstat()) != staged_identity:
                raise ValueError("filesystem custody staging object changed before removal")
            stage.unlink()


def adopt(store, run_id, supplied):
    from .delivery_interrupted_recovery import installation_recovery_lease
    from .delivery_resources import read_private

    if (
        not isinstance(supplied, dict)
        or set(supplied)
        != {"command_id", "expected_revision", "expected_evidence_sha256", "answer"}
        or not isinstance(supplied["command_id"], str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", supplied["command_id"])
        or type(supplied["expected_revision"]) is not int
        or supplied["expected_revision"] < 1
        or supplied["answer"] != ANSWER
        or not isinstance(supplied["expected_evidence_sha256"], str)
        or not re.fullmatch(r"[0-9a-f]{64}", supplied["expected_evidence_sha256"])
    ):
        raise ValueError("invalid explicit filesystem custody adoption request")
    command_digest = digest({"run_id": run_id, **supplied})
    with store._connect() as db:
        prior = db.execute(
            "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
            (supplied["command_id"],),
        ).fetchone()
    if prior:
        if prior[0] != command_digest:
            raise ValueError("command ID already belongs to different inputs")
        return json.loads(prior[1])
    with installation_recovery_lease(store.config, exclusive=True), ExitStack() as leases:
        spec, evidence, originals, descriptors = _collect(store, run_id, leases)
        from .delivery_lifecycle import require_transition

        require_transition(_lifecycle_facts(evidence, supplied), supplied)
        folder = Path(spec["state_dir"]) / "filesystem-custody"
        receipt = {
            "kind": KIND,
            "run_id": run_id,
            "command": supplied,
            "evidence": evidence,
            "historical_volume_continuity": "unproven",
            "authority": "explicit_operator_current_object_adoption",
            "provider_verdict": "unchanged",
            "execution_authority": "unchanged",
        }
        raw = (canonical_json(receipt) + "\n").encode()
        path = folder / (supplied["command_id"] + ".json")
        with store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            current = dict(
                db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            )
            attempts = [
                dict(r)
                for r in db.execute(
                    "SELECT * FROM delivery_attempts WHERE run_id=? ORDER BY job_key", (run_id,)
                )
            ]
            claim = store.state.claim_for(db, spec["work_id"])
            claim = dict(claim) if claim is not None else None
            if (
                current != evidence["row"]
                or attempts != evidence["attempts"]
                or claim != evidence["claim"]
            ):
                raise ValueError("filesystem custody run or attempts changed during adoption")
            for original, sha in evidence["originals"].items():
                if hashlib.sha256(read_private(Path(original), raw=True)).hexdigest() != sha:
                    raise ValueError("filesystem custody original evidence changed during adoption")
            for object_path, descriptor in descriptors:
                captured = evidence["objects"].get(str(object_path))
                if _raw(os.fstat(descriptor)) != _raw(object_path.lstat()) or (
                    captured and _metadata(descriptor) != captured["current"]
                ):
                    raise ValueError("filesystem custody object replaced during adoption")
            if any(
                db.execute(
                    f"SELECT 1 FROM {table} WHERE run_id=? AND state IN "
                    "('pending','unknown','dispatching') LIMIT 1",
                    (run_id,),
                ).fetchone()
                for table in ("delivery_effects", "delivery_mutations")
            ):
                raise ValueError("filesystem custody has a concurrent uncertain effect or command")
            from .candidate import candidate_for
            from .delivery_native_process import listeners, process_table

            if (
                candidate_for(Path(spec["checkout"])) != evidence["candidate"]
                or _boot() != evidence["boot_uuid"]
            ):
                raise ValueError("filesystem custody source or boot changed during adoption")
            table = process_table()
            for original in evidence["originals"]:
                if Path(original).name != "native-process.json":
                    continue
                journal = read_private(Path(original))
                actors = [(int(pid), actor) for pid, actor in journal.get("owned", {}).items()]
                monitor = journal.get("monitor")
                if monitor:
                    actors.append((monitor["pid"], monitor))
                if any(
                    table.get(pid, {}).get("identity") == actor["identity"]
                    and not table[pid]["stat"].startswith("Z")
                    for pid, actor in actors
                ) or any(listeners(port) for port in journal.get("ports", [])):
                    raise ValueError("filesystem custody native actors or ports changed")
            for sha, original in originals.items():
                _immutable(folder / "originals" / (sha + ".json"), original)
            _immutable(path, raw)
            response = {
                "kind": KIND,
                "run_id": run_id,
                "command_id": supplied["command_id"],
                "receipt": str(path),
                "receipt_sha256": hashlib.sha256(raw).hexdigest(),
                "historical_volume_continuity": "unproven",
            }
            store._event(
                db,
                run_id,
                current["revision"],
                KIND,
                "Operator adopted exact current filesystem objects; original evidence retained",
                response,
            )
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (supplied["command_id"], run_id, command_digest, canonical_json(response)),
            )
        return response
