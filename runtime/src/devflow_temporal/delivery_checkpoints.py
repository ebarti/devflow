"""Acknowledged, immutable source checkpoints for closed delivery writers.

A directory publication is not a checkpoint acknowledgement. SQLite records the
exact manifest digest only after source, evidence and session bytes are durable.
Restoration changes filesystem custody through a new receipt; admission, budget,
workflow history and external-effect outcomes are never rewritten.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import secrets
import shutil
import sqlite3
import stat
import subprocess
import tempfile
from collections.abc import Callable
from contextlib import AbstractContextManager, ExitStack, closing, contextmanager, nullcontext
from pathlib import Path
from typing import Any, Protocol

from .candidate import SKIP_NAMES, _files, candidate_for
from .contracts import canonical_json, digest
from .delivery_resources import RunResources, _ancestors, _identity, private_directory, read_private

SCHEMA = "devflow-delivery-checkpoint-v1"
CREATED = "delivery_checkpoint_created"
RESTORED = "delivery_checkpoint_restored"
_ID = re.compile(r"[a-f0-9]{64}\Z")


class NativeCheckpointCustodyUnknown(ValueError):
    """Native descendant closure is not established for checkpoint admission."""

    code = "native_checkpoint_custody_unknown"

    def __init__(self):
        super().__init__("checkpoint native descendant closure is unknown")


class CheckpointRestoreUnknown(ValueError):
    """Restore retains an allocation whose original writer is not proven closed."""

    code = "checkpoint_restore_custody_unknown"


def _require_supported_native_custody(journals):
    # A journal enumerates observed identities, not an authenticated complete
    # descendant set. No current backend supplies that closure capability.
    if journals:
        raise NativeCheckpointCustodyUnknown()


def _index_baseline_supported(value):
    baseline = value.get("source", {}).get("index_semantics")
    return (isinstance(baseline, dict)
            and set(baseline) == {"schema", "stage_sha256", "normal_paths_sha256"}
            and baseline["schema"] == "devflow-index-semantics-v1"
            and all(isinstance(baseline[key], str) and _ID.fullmatch(baseline[key])
                    for key in ("stage_sha256", "normal_paths_sha256")))


def _require_index_baseline(value):
    if not _index_baseline_supported(value):
        raise ValueError("checkpoint index semantics are unverified; a fresh seal is required")


def _legacy_index_format(value):
    return "index_semantics" not in value.get("source", {})


def _verify_index_baseline(checkout, value):
    from .delivery_checkpoint_index import observe_index

    observed = observe_index(checkout, _git)
    if _legacy_index_format(value):
        # v1 recorded a tree and binary patches, not index flags. Observe a
        # normal reconstructed index without pretending it was the old index.
        return {"schema": "devflow-legacy-index-reconstruction-v1",
                "original_index_flags": "not_recorded", "restored_index_semantics": observed}
    _require_index_baseline(value)
    if observed != value["source"]["index_semantics"]:
        raise ValueError("checkpoint Git index semantics changed")
    return None


class AttemptSettlementObservation(Protocol):
    """Trusted server-owned evidence retained through checkpoint transactions."""

    settlements: dict[str, Any]
    host_observation: Any | None

    def verify(self, tracking_db: sqlite3.Connection, host_db: sqlite3.Connection | None) -> dict:
        """Revalidate captured authority using already-held short transactions."""
        ...


AttemptObservationFactory = Callable[
    [Any, dict, list[dict]], AbstractContextManager[AttemptSettlementObservation]
]
GraphValidation = Callable[[sqlite3.Connection, sqlite3.Connection | None], None]
GraphTransitionValidation = Callable[
    [sqlite3.Connection, sqlite3.Connection | None, dict, str | bytes | None], None
]
_SECRET_NAMES = {
    "auth.json",
    "credentials",
    "credentials.json",
    ".env",
    ".netrc",
    ".git-credentials",
    "id_rsa",
    "id_ed25519",
}
_SECRET_DIRS = {".aws", ".ssh", ".gnupg", ".codex", ".agents"}


def _git(path, *args, input=None):
    from .delivery_writer_custody import inherited_descriptors

    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(
        GIT_NO_REPLACE_OBJECTS="1",
        GIT_GRAFT_FILE=os.devnull,
        GIT_TERMINAL_PROMPT="0",
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull,
    )
    result = subprocess.run(
        [
            "git",
            "--no-replace-objects",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-C",
            str(path),
            *args,
        ],
        input=input,
        env=environment,
        capture_output=True,
        timeout=120,
        pass_fds=inherited_descriptors(),
    )
    if result.returncode:
        raise ValueError(
            "checkpoint Git operation failed: "
            + result.stderr.decode("utf-8", "replace").strip()[:300]
        )
    return result.stdout


def _hash(raw):
    return hashlib.sha256(raw).hexdigest()


def _path(root, relative):
    value = Path(relative)
    if (
        not isinstance(relative, str)
        or value.is_absolute()
        or not value.parts
        or relative != value.as_posix()
        or any(p in {".", ".."} for p in value.parts)
    ):
        raise ValueError("checkpoint contains an unsafe relative path")
    result = root / value
    _ancestors(result, allow_missing=True)
    return result


def _source_path_allowed(relative):
    path = Path(relative)
    return (
        not any(p in _SECRET_DIRS for p in path.parts)
        and path.name not in _SECRET_NAMES
        and (
            not path.name.startswith(".env.")
            or path.name in {".env.example", ".env.sample", ".env.template"}
        )
    )


def _write(path, raw):
    private_directory(path.parent)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def _sync_tree(root):
    for base, _directories, files in os.walk(root, topdown=False, followlinks=False):
        for name in files:
            path = Path(base) / name
            with path.open("rb") as stream:
                os.fsync(stream.fileno())
        descriptor = os.open(base, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _sync_parents(path, root):
    parent = path.parent
    while parent.is_relative_to(root):
        descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        if parent == root:
            break
        parent = parent.parent


def _root(spec):
    state = Path(spec["state_dir"])
    if state.name != spec["run_id"] or state.resolve() != state or state.parent.name != "runs":
        raise ValueError("checkpoint state left its canonical delivery namespace")
    checkout = Path(spec["checkout"])
    if checkout != state.parent.parent / "checkouts" / spec["run_id"]:
        raise ValueError("checkpoint checkout left its admitted namespace")
    return state / "checkpoints"


@contextmanager
def _locked(spec):
    root = _root(spec)
    private_directory(root)
    lock = root / "checkpoint.lock"
    descriptor = os.open(lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
        ):
            raise ValueError("checkpoint lock is not a private owned file")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        if (info.st_dev, info.st_ino) != (lock.lstat().st_dev, lock.lstat().st_ino):
            raise ValueError("checkpoint lock changed")
        yield root
    finally:
        os.close(descriptor)


def _row(db, run_id):
    row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
    if row is None:
        raise ValueError("run ID not found")
    return dict(row)


def _frozen(db, row):
    """Only this run's authority, budget, effects and evidence references."""
    run_id = row["run_id"]
    frozen = {
        "run": row,
        **{
            table: [
                dict(item)
                for item in db.execute(
                    f"SELECT * FROM {table} WHERE run_id=? ORDER BY {order}", (run_id,)
                )
            ]
            for table, order in (
                ("delivery_attempts", "job_key"),
                ("delivery_effects", "effect_key"),
                ("delivery_preparations", "run_id"),
                ("delivery_merge_requests", "run_id"),
            )
        },
    }
    # These are tracking authority, not recreatable filesystem projections.
    for table, order in (
        ("delivery_native_renewal_anchors", "namespace"),
        ("delivery_execution_authority", "workflow_id"),
        ("delivery_merge_executions", "workflow_id"),
    ):
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                      (table,)).fetchone():
            frozen[table] = [dict(item) for item in db.execute(
                f"SELECT * FROM {table} WHERE run_id=? ORDER BY {order}", (run_id,))]
    prefix = "publish:" + run_id + ":"
    frozen["delivery_publication_executors"] = []
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                  "AND name='delivery_publication_executors'").fetchone():
        frozen["delivery_publication_executors"] = [dict(item) for item in db.execute(
            "SELECT * FROM delivery_publication_executors WHERE substr(effect_key,1,?)=? "
            "ORDER BY effect_key", (len(prefix), prefix))]
    return frozen


def _stopped(db, row, fence):
    if fence is not None:
        # Internal only: the pause owner supplies a validator of its durable
        # admission fence, under its admission lock. HTTP cannot supply one.
        binding = fence(db, row)
        if not isinstance(binding, dict) or not binding:
            raise ValueError("checkpoint pause fence has no authenticated binding")
        return binding
    if row["execution_state"] not in {"blocked", "cancelled", "terminal"} or not row["outcome"]:
        raise ValueError("checkpoint requires a stopped run or an authenticated pause fence")
    return None


def _controller_binding(store, spec, expected_revision, fence):
    """A stopped projection cannot establish that its execution owner stopped."""
    if fence is not None:
        return None  # The authenticated same-workflow pause fence owns this proof.
    with store._connect() as db:
        merge = db.execute("SELECT * FROM delivery_merge_requests WHERE run_id=?",
                           (spec["run_id"],)).fetchone()
        merge = dict(merge) if merge is not None else None
        if spec["provider"] == "fake":
            selected = db.execute(
                "SELECT 1 FROM delivery_execution_authority WHERE run_id=?",
                (spec["run_id"],)).fetchone()
            if selected is None and merge is None:
                return {"kind": "fixture_provider"}
        # Fake roles may still use a real SDK controller. A requested start or
        # lost start ACK cannot be reduced to an in-process fixture controller.
    with store._connect() as db:
        row = _row(db, spec["run_id"])
        if row["revision"] != expected_revision:
            raise ValueError("checkpoint controller revision changed")
        _stopped(db, row, None)
    closed = store._completed_temporal_result(spec["run_id"], workflow_id=row["workflow_id"])
    recovery = json.loads(row["recovery_json"]) if row["recovery_json"] else None
    result = closed.get("result")
    if (
        closed.get("workflow_id") != (row["workflow_id"] or "delivery-" + spec["run_id"])
        or not closed.get("execution_run_id")
        or not closed.get("closed_at")
        or closed.get("request_digest") != row["request_digest"]
        or closed.get("recovery_digest") != (digest(recovery) if recovery else None)
        or not isinstance(result, dict)
        or result.get("run_id") != spec["run_id"]
        or result.get("outcome") != row["outcome"]
        or result.get("execution_state") != row["execution_state"]
        or result.get("iteration") != row["iteration"]
    ):
        raise ValueError("checkpoint controller closure lost its exact execution binding")
    binding = {"kind": "closed_workflow", "closure": closed, "result_sha256": digest(result)}
    if merge is not None:
        from .delivery_merge_execution import predecessor_closure

        binding["merge_closure"] = predecessor_closure(store, spec, merge)
        binding["merge_request"] = merge
    return binding


def _writer_custody(store, spec, provided, fence, context):
    if provided is None:
        factory = getattr(store, "_checkpoint_writer_context", None)
        if factory is not None:
            provided = factory(spec)
    from .delivery_merge_followup import ACTIVE

    with store._connect() as db:
        merge = db.execute(
            "SELECT phase FROM delivery_merge_requests WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
    executors = Path(spec["state_dir"]) / "publication-executors"
    if provided is None and (
        merge is not None or (executors.exists() and any(executors.iterdir()))
    ):
        raise ValueError("checkpoint needs authenticated publication or merge writer custody")
    if merge is not None and merge["phase"] in ACTIVE:
        if fence is None or provided is None or context is None:
            raise ValueError(
                "checkpoint active merge needs authenticated controller and writer custody"
            )
    return provided


def _same_controller(current, original):
    if current is None or original is None:
        return current is original
    if current["kind"] != original["kind"]:
        return False
    if current["kind"] == "fixture_provider":
        return True
    return (current.get("merge_closure") == original.get("merge_closure")
            and current.get("merge_request") == original.get("merge_request")
            and current["result_sha256"] == original["result_sha256"] and all(
        current["closure"].get(key) == original["closure"].get(key)
        for key in ("workflow_id", "execution_run_id", "request_digest", "recovery_digest")
    ))


def _verify_frozen_controllers(frozen, controller, *, fence, checkpoint_context):
    """Reconcile controller admission against the actual post-custody DB snapshot."""
    from .delivery_merge_followup import ACTIVE

    rows = frozen["delivery_merge_requests"]
    if not rows:
        if controller and controller.get("merge_closure") is not None:
            raise ValueError("checkpoint retained merge controller disappeared")
        return
    if len(rows) != 1:
        raise ValueError("checkpoint has ambiguous retained merge controllers")
    merge = rows[0]
    if merge["phase"] in ACTIVE:
        if fence is None or checkpoint_context is None:
            raise ValueError("checkpoint active merge needs its authenticated pause controller")
        return
    closure = controller.get("merge_closure") if controller else None
    selected = [row for row in frozen.get("delivery_merge_executions", [])
                if row["workflow_id"] == merge["workflow_id"]]
    if (not closure or len(selected) != 1 or controller.get("merge_request") != merge
            or closure.get("workflow_id") != merge["workflow_id"]
            or closure.get("execution_run_id") != selected[0]["execution_run_id"]
            or closure.get("original_spec_sha256") != selected[0]["spec_sha256"]
            or closure.get("grant_sha256") != selected[0]["grant_sha256"]):
        raise ValueError("checkpoint retained merge lacks its exact closed SDK controller")


def _restore_fence(db, row, value, fence):
    if _stopped(db, row, fence) != value.get("control"):
        raise ValueError("checkpoint pause epoch or execution fence changed")


def _settled_effect(item, *, feature=False):
    states = {"complete", "absent"} if feature else {"complete"}
    raw = item.get("result_json" if feature else "observed_json")
    if item.get("state") not in states or not isinstance(raw, str):
        return False
    try:
        return isinstance(json.loads(raw), dict)
    except (TypeError, ValueError):
        return False


def _verify_effects(frozen, *, allow_unresolved=False, settlements=None):
    unknown = [
        {"effect_key": item["effect_key"], "kind": item["kind"], "state": item["state"]}
        for item in frozen["delivery_effects"]
        if not _settled_effect(item)
    ]
    unknown.extend(
        {"effect_key": item["command_id"], "kind": "retained_merge", "state": item["phase"]}
        for item in frozen.get("delivery_merge_requests", [])
        if item["phase"] == "unknown"
    )
    if unknown and not allow_unresolved:
        raise ValueError(
            "checkpoint external effects require reconciliation: "
            + ", ".join(item["effect_key"] for item in unknown)
        )
    proofs = {item["job_key"]: item for item in (settlements or {}).get("attempts", [])}
    if (
        settlements is not None
        and settlements.get("schema") != "devflow-paused-attempt-settlement-v1"
    ):
        raise ValueError("checkpoint attempt settlement has an unsupported schema")
    unfinished = [
        item["job_key"]
        for item in frozen["delivery_attempts"]
        if (
            (item["state"] != "finished" or item["cleanup"] != "confirmed")
            and (
                item["job_key"] not in proofs
                or proofs[item["job_key"]].get("row_sha256") != digest(item)
            )
        )
    ]
    if unfinished:
        raise ValueError("checkpoint native attempts remain unfinished: " + ", ".join(unfinished))
    return [{**item, "scope": "delivery", "reason": "external_outcome_unknown"} for item in unknown]


def _validate_settlements(spec, rows, settlements):
    if settlements is None:
        return
    if (
        not isinstance(settlements, dict)
        or settlements.get("schema") != "devflow-paused-attempt-settlement-v1"
        or not isinstance(settlements.get("attempts"), list)
    ):
        raise ValueError("checkpoint attempt settlement has an unsupported schema")
    by_key, seen = {row["job_key"]: row for row in rows}, set()
    for proof in settlements["attempts"]:
        key = proof.get("job_key")
        row = by_key.get(key)
        if (
            key in seen
            or row is None
            or row["state"] != "queued"
            or any(
                row[field] is not None
                for field in ("pid", "process_identity", "session_id", "result_json")
            )
            or proof.get("row_sha256") != digest(row)
            or not isinstance(proof.get("receipt_sha256"), str)
            or not _ID.fullmatch(proof["receipt_sha256"])
        ):
            raise ValueError("checkpoint no-GO settlement lost its exact queued attempt")
        if row.get("result_path") and os.path.lexists(row["result_path"]):
            raise ValueError("checkpoint no-GO settlement has an unexpected provider result")
        path = Path(proof["receipt_path"])
        state = Path(spec["state_dir"])
        if not path.is_absolute() or not path.is_relative_to(state):
            raise ValueError("checkpoint no-GO receipt escaped its original run")
        _path(state, path.relative_to(state).as_posix())
        if _hash(read_private(path, raw=True)) != proof["receipt_sha256"]:
            raise ValueError("checkpoint no-GO receipt digest changed")
        seen.add(key)


def _execution_ledger(spec, *, allow_unresolved=False, allow_unfinished_workers=False, facts=None):
    feature = spec.get("feature_delivery")
    if not feature:
        return None
    path, owner = Path(feature["registry"]), feature["owner"]
    if facts is not None:
        result = facts
    else:
        _ancestors(path)
        info = path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
        ):
            raise ValueError("checkpoint shared execution ledger lost private ownership")
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN")
            result = {
                table: [
                    dict(row)
                    for row in db.execute(
                        f"SELECT * FROM {table} WHERE issue_id=? ORDER BY rowid",
                        (owner["issue_id"],)
                    )
                ]
                for table in (
                    "execution_claims",
                    "execution_snapshots",
                    "execution_workers",
                    "execution_effects",
                    "execution_repair_limits",
                    "execution_repairs",
                    "execution_checkpoints",
                )
            }
    claims = result["execution_claims"]
    if len(claims) != 1 or any(
        claims[0].get(key) != owner.get(key)
        for key in ("issue_id", "run_id", "store_path", "generation")
    ):
        raise ValueError("checkpoint shared execution owner changed")
    if not allow_unresolved and any(
        not _settled_effect(row, feature=True) for row in result["execution_effects"]
    ):
        raise ValueError("checkpoint shared external effects require reconciliation")
    if not allow_unfinished_workers and any(
        row["state"] != "finished" for row in result["execution_workers"]
    ):
        raise ValueError("checkpoint feature workers remain unfinished")
    return result


def _same_frozen(current, original, *, effect_table="delivery_effects", publication=None):
    """Permit exact pending-effect readback; no budgets or authority roll back."""
    if {k: v for k, v in current.items() if k != effect_table} != {
        k: v for k, v in original.items() if k != effect_table
    }:
        return False
    old_effects, new_effects = original[effect_table], current[effect_table]
    if len(old_effects) != len(new_effects):
        return False
    for old, new in zip(old_effects, new_effects, strict=True):
        mutable = {"state", "updated_at", "observed_json", "result_json"}
        if {k: v for k, v in old.items() if k not in mutable} != {
            k: v for k, v in new.items() if k not in mutable
        }:
            return False
        if old["state"] in {"complete", "absent"}:
            if {k: v for k, v in old.items() if k != "updated_at"} != {
                k: v for k, v in new.items() if k != "updated_at"
            }:
                return False
        else:
            # Only the existing publication owner's exact readback receipt is a
            # supported transition here. A raw state flip, empty JSON or guessed
            # absence cannot turn an unknown external outcome into evidence.
            if (
                effect_table != "delivery_effects"
                or old["kind"] != "publish"
                or new["state"] != "complete"
                or publication is None
            ):
                return False
            try:
                observed = json.loads(new["observed_json"])
                request = json.loads(old["request_json"])
                authority, candidate = publication
                number = observed["number"]
                if (
                    type(number) is not int
                    or number < 1
                    or observed["url"]
                    != f"https://github.com/{authority['github_repo']}/pull/{number}"
                    or observed["state"] not in {"OPEN", "CLOSED", "MERGED"}
                    or observed["head"] != candidate["head"]
                    or observed["base"] != authority["base_sha"]
                    or type(request["iteration"]) is not int
                    or not _ID.fullmatch(request["input_candidate_id"])
                    or any(
                        observed["candidate"][key] != candidate[key]
                        for key in ("id", "head", "content_sha256")
                    )
                    or observed["candidate"].get("policy_digest") != authority["policy_digest"]
                ):
                    return False
            except (ValueError, TypeError, KeyError):
                return False
    return True


def _same_ledger(current, original):
    if current is None or original is None:
        return current is original
    return _same_frozen(current, original, effect_table="execution_effects")


def _same_snapshot(current, value):
    return _same_frozen(
        current, value["frozen"], publication=(value["authority"], value["source"]["candidate"])
    )


def _closed(registry, manifest, attempts, stack):
    _require_supported_native_custody(manifest["processes"])
    state = registry.state
    unstarted = [
        state / "attempts" / item["job_key"]
        for item in attempts
        if str(state / "attempts" / item["job_key"] / "native-process.json")
        not in manifest["processes"]
    ]
    return stack.enter_context(
        registry._closed_journals(manifest, manifest["processes"], unstarted=unstarted)
    )


def _openers(checkout):
    if not Path("/usr/sbin/lsof").is_file():
        raise ValueError("checkpoint cannot prove workspace writers closed: lsof is unavailable")
    observed = subprocess.run(
        ["/usr/sbin/lsof", "-nP", "-F", "p", "+D", str(checkout)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if observed.returncode not in (0, 1) or any(
        line.startswith("p") and line[1:].isdecimal() for line in observed.stdout.splitlines()
    ):
        raise ValueError("checkpoint workspace still has an open actor or unknown writer")


def _artifact(stage, relative, raw, inventory):
    _write(_path(stage, relative), raw)
    inventory[relative] = {"sha256": _hash(raw), "size": len(raw)}


def _candidate_snapshot(spec, stage, inventory, resources):
    from .delivery_checkpoint_index import observe_index

    checkout = Path(spec["checkout"])
    from .delivery_filesystem_custody import matches

    if any(
        entry.get("generated_output_preservation")
        for raw, entry in resources["roots"].items()
        if Path(raw).is_relative_to(checkout)
    ):
        raise ValueError("checkpoint generated-source preservation needs custody reconciliation")
    generated = [
        Path(raw)
        for raw, entry in resources["roots"].items()
        if entry["kind"] == "generated"
        and Path(raw).is_relative_to(checkout)
        and Path(raw).exists()
        and matches(spec, Path(raw), entry["identity"])
    ]
    ignored = _git(checkout, "ls-files", "--others", "--ignored", "--exclude-standard", "-z")
    for raw in ignored.split(b"\0"):
        if not raw:
            continue
        relative = Path(os.fsdecode(raw))
        if (
            not _source_path_allowed(relative.as_posix())
            or any(part in SKIP_NAMES for part in relative.parts)
            or any((checkout / relative).is_relative_to(root) for root in generated)
        ):
            continue
        raise ValueError(
            "checkpoint has unclassified ignored source requiring preservation: "
            + relative.as_posix()
        )
    candidate = candidate_for(checkout)
    files = {}
    for path in _files(checkout):
        relative = path.relative_to(checkout).as_posix()
        if not _source_path_allowed(relative):
            raise ValueError("checkpoint refuses credential/profile source: " + relative)
        if path.is_symlink():
            files[relative] = {"kind": "symlink", "target": os.readlink(path)}
        else:
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
                raise ValueError(
                    "checkpoint source is not an owned unlinked regular file: " + relative
                )
            raw = path.read_bytes()
            artifact = "source/" + relative
            _artifact(stage, artifact, raw, inventory)
            files[relative] = {
                "kind": "file",
                "artifact": artifact,
                "executable": bool(info.st_mode & stat.S_IXUSR),
            }
    for name, args in (
        ("staged.patch", ("diff", "--cached", "--binary", "--full-index", "HEAD")),
        ("unstaged.patch", ("diff", "--binary", "--full-index")),
    ):
        _artifact(stage, name, _git(checkout, *args), inventory)
    bundle = stage / "candidate.bundle"
    _git(checkout, "bundle", "create", str(bundle), "HEAD")
    bundle.chmod(0o600)
    raw = bundle.read_bytes()
    inventory["candidate.bundle"] = {"sha256": _hash(raw), "size": len(raw)}
    if candidate_for(checkout) != candidate:
        raise ValueError("checkpoint candidate changed during preservation")
    return {
        "candidate": candidate,
        "files": files,
        "branch": _git(checkout, "symbolic-ref", "--short", "HEAD").decode().strip(),
        "index_tree": _git(checkout, "write-tree").decode().strip(),
        "index_semantics": observe_index(checkout, _git),
    }


def _state_artifacts(spec, frozen, stage, inventory, journals, settlements=None):
    """Inventory selected evidence; stage=None verifies without copying bytes."""
    from .delivery_continuation import selected_manifest, session_state_paths
    from .delivery_sandbox import _native_role_home

    state, selected = Path(spec["state_dir"]), set()
    absent = {}
    proof_rows = {item["job_key"]: item for item in (settlements or {}).get("attempts", [])}
    for attempt in frozen["delivery_attempts"]:
        proof = proof_rows.get(attempt["job_key"])
        result = attempt.get("result_path")
        if proof is not None and proof.get("row_sha256") == digest(attempt) and result:
            path = Path(result)
            if not path.is_absolute() or not path.is_relative_to(state) or os.path.lexists(path):
                raise ValueError("checkpoint no-GO result lacks its authenticated absence")
            absent[path.relative_to(state).as_posix()] = {
                "kind": "absent",
                "reason": "authenticated_no_go",
                "job_key": attempt["job_key"],
                "row_sha256": proof["row_sha256"],
                "receipt_sha256": proof["receipt_sha256"],
            }
    parsed = set()
    path_keys = {
        "path",
        "log",
        "journal",
        "receipt",
        "receipt_path",
        "result_path",
        "start_path",
        "request_path",
        "history_result_path",
        "launch",
    }

    def references(value):
        if isinstance(value, str):
            if value.startswith(("{", "[")):
                try:
                    references(json.loads(value))
                except json.JSONDecodeError:
                    pass
            return
        if isinstance(value, list):
            for item in value:
                references(item)
        if not isinstance(value, dict):
            return
        for key, raw_path in value.items():
            references(raw_path)
            if key not in path_keys or not isinstance(raw_path, str):
                continue
            path = Path(raw_path)
            if not path.is_absolute() or not path.is_relative_to(state):
                continue
            relative = path.relative_to(state).as_posix()
            expected = value.get(
                "sha256"
                if key == "path"
                else "receipt_sha256"
                if key == "receipt_path"
                else key + "_sha256"
            )
            if (
                relative in absent
                and key == "result_path"
                and expected is None
                and not os.path.lexists(path)
            ):
                continue
            if path.is_dir() and not path.is_symlink():
                continue  # Resource directory custody is not an artifact inventory.
            if not os.path.lexists(path) and expected is None and key == "path":
                continue  # A finalized absent resource root has no artifact bytes.
            if not _source_path_allowed(relative):
                raise ValueError(
                    "checkpoint required reference selects credentials/profile: " + relative
                )
            _path(state, relative)
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
                raise ValueError(
                    "checkpoint required evidence has no owned file custody: " + relative
                )
            content = path.read_bytes()
            if expected is not None and (
                not isinstance(expected, str) or _hash(content) != expected
            ):
                raise ValueError("checkpoint required evidence digest changed: " + relative)
            selected.add(relative)
            if path.suffix == ".json" and relative not in parsed:
                parsed.add(relative)
                try:
                    references(json.loads(content))
                except json.JSONDecodeError:
                    pass

    references(frozen)
    references(settlements)
    references([{"path": item["journal"], "sha256": item["sha256"]} for item in journals])
    for name in ("manifest.json", "finalization.json"):
        path = state / "resources" / name
        if path.exists():
            references({"path": str(path)})
    for attempt in frozen["delivery_attempts"]:
        folder = state / "attempts" / attempt["job_key"]
        for name in ("request.json", "result.json", "native-process.json", "start.json"):
            if os.path.lexists(folder / name):
                selected.add((folder / name).relative_to(state).as_posix())
        if attempt["session_id"] and (folder / "request.json").exists():
            request = read_private(folder / "request.json")
            home = _native_role_home(request)
            if spec["provider"] == "fake" and not home.exists():
                continue
            if not home.is_relative_to(state):
                raise ValueError("checkpoint session home escaped its original run")
            _openers(home)
            paths = session_state_paths(home, attempt["session_id"])
            selected_manifest(home, paths)  # Private owned regular files, no linked ancestors.
            selected.update((home / item).relative_to(state).as_posix() for item in paths)
        elif attempt["session_id"] and spec["provider"] != "fake":
            raise ValueError("checkpoint required session request is missing")
    from .delivery_resources import _gate_roots

    _, namespaces = _gate_roots(spec)
    evidence_names = (
        "evidence",
        "gate-evidence",
        "browser-qa",
        "check-evidence",
        "checks",
        "prechecks",
        "baseline",
        "baseline-checks",
        "dependency-preparation",
        "implementation-preparation",
        "native-preparation",
        "recovery",
    )
    evidence_suffixes = {
        ".json",
        ".jsonl",
        ".log",
        ".txt",
        ".png",
        ".jpg",
        ".jpeg",
        ".webp",
        ".html",
        ".patch",
        ".xml",
        ".pdf",
    }
    for root in {namespace / name for namespace in namespaces for name in evidence_names}:
        if root.exists():
            for path in root.rglob("*"):
                if path.is_dir() and not path.is_symlink():
                    continue
                relative = path.relative_to(root)
                if (
                    path.suffix not in evidence_suffixes
                    or not _source_path_allowed(relative.as_posix())
                    or any(
                        part in {"node_modules", ".venv", "profiles", "browser-profile"}
                        for part in relative.parts
                    )
                ):
                    continue
                selected.add(path.relative_to(state).as_posix())
    refs = {}
    for relative in sorted(selected):
        path = _path(state, relative)
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
            raise ValueError("checkpoint evidence is not an owned regular file: " + relative)
        raw = path.read_bytes()
        artifact = "state/" + relative
        if stage is None:
            inventory[artifact] = {"sha256": _hash(raw), "size": len(raw)}
        else:
            _artifact(stage, artifact, raw, inventory)
        if path.read_bytes() != raw or path.stat().st_mtime_ns != info.st_mtime_ns:
            raise ValueError("checkpoint evidence changed during preservation: " + relative)
        refs[relative] = {
            "artifact": artifact,
            "sha256": _hash(raw),
            "mode": stat.S_IMODE(info.st_mode),
            "mtime_ns": info.st_mtime_ns,
        }
    return {**refs, **absent}


def _command(db, key, run_id, request):
    saved = db.execute("SELECT * FROM delivery_commands WHERE command_id=?", (key,)).fetchone()
    if saved is None:
        return None
    if saved["run_id"] != run_id or saved["request_digest"] != digest(request):
        raise ValueError("checkpoint command ID belongs to different inputs")
    return json.loads(saved["response_json"])


def _publish_checkpoint(db, spec, stage, destination, manifest, raw):
    if not os.path.lexists(destination):
        os.rename(stage, destination)
        _sync_tree(destination.parent)
        return False
    # Complete bytes without an ACK are an unknown outcome. Reconcile only by
    # reproducing their exact manifest from the still-fenced original source.
    if db.execute(
        "SELECT 1 FROM delivery_events WHERE run_id=? AND type=? "
        "AND json_extract(payload_json,'$.checkpoint_id')=?",
        (spec["run_id"], CREATED, manifest["checkpoint_id"]),
    ).fetchone():
        raise ValueError("checkpoint unacknowledged partial publication has an inconsistent event")
    if (
        _identity(destination)["uid"] != os.getuid()
        or read_private(destination / "manifest.json", raw=True) != raw
    ):
        raise ValueError(
            "checkpoint unacknowledged partial publication differs; reconcile before retry"
        )
    expected = {"manifest.json", *manifest["artifacts"]}
    actual = set()
    for path in destination.rglob("*"):
        if path.is_symlink():
            raise ValueError("checkpoint partial publication has an unowned linked artifact")
        if not path.is_dir():
            actual.add(path.relative_to(destination).as_posix())
    if actual != expected:
        raise ValueError(
            "checkpoint unacknowledged partial publication has missing or foreign artifacts"
        )
    for relative, item in manifest["artifacts"].items():
        content = read_private(_path(destination, relative), raw=True)
        if len(content) != item["size"] or _hash(content) != item["sha256"]:
            raise ValueError(
                "checkpoint unacknowledged partial publication is corrupt: " + relative
            )
    _sync_tree(destination)
    _sync_parents(destination, _root(spec))
    return True


@contextmanager
def _checkpoint_admission(admission_context, registry_observation, host_observation):
    observations = [item for item in (registry_observation, host_observation) if item is not None]
    if observations:
        from .delivery_registry_epoch import ordered_writer_fences

        registry_fence = ordered_writer_fences(observations)
    else:
        registry_fence = nullcontext(None)
    with admission_context() if admission_context is not None else nullcontext():
        with registry_fence as fences:
            registry_db = (
                fences.connection(registry_observation.path) if registry_observation else None
            )
            host_db = fences.connection(host_observation.path) if host_observation else None
            yield registry_db, host_db, fences


def _feature_observation(spec, stack):
    feature = spec.get("feature_delivery")
    if not feature:
        return None
    from .delivery_checkpoint_registry_view import (
        MAX_PROJECTION_BYTES,
        MAX_PROJECTION_CELL_BYTES,
        MAX_PROJECTION_ROWS,
    )
    from .delivery_registry_epoch import FactLimits, observe

    owner = feature["owner"]
    issue_url = feature["snapshot"]["issue"]["url"].rstrip("/").casefold()
    observation = stack.enter_context(observe(
        Path(feature["registry"]), owner["issue_id"], issue_url, owner["store_path"],
        fact_limits=FactLimits(MAX_PROJECTION_ROWS, MAX_PROJECTION_BYTES,
                               MAX_PROJECTION_CELL_BYTES),
    ))
    return observation


def _prepare_projection(spec, registry_observation):
    from .delivery_checkpoint_registry_view import PreparedRegistryProjection

    return (PreparedRegistryProjection(spec, registry_observation)
            if registry_observation is not None else None)


def _registry_current(registry_observation):
    registry_observation.validate()
    return True


def _validate_registry(fences):
    if fences is not None:
        fences.validate()


def create_checkpoint(
    store,
    run_id,
    command_id,
    expected_revision,
    reason="manual",
    *,
    fence=None,
    admission_context=None,
    on_ack=None,
    prepare_ack=None,
    writer_context=None,
    attempt_validation=None,
    attempt_observation: AttemptObservationFactory | None = None,
    checkpoint_context=None,
    graph_validation: GraphValidation | None = None,
    graph_transition_validation: GraphTransitionValidation | None = None,
    allow_unresolved_effects=False,
):
    """Seal stopped work using any trusted pause owner's durable fence.

    A graph owner may validate its captured pre-state and, after the prepared
    acknowledgement, must validate the exact authorized post-state in the same
    transaction. Preparation receives detached copies outside admission locks.
    """
    if (
        not isinstance(command_id, str)
        or not command_id
        or len(command_id) > 200
        or type(expected_revision) is not int
        or not isinstance(reason, str)
        or not reason.strip()
        or len(reason) > 500
    ):
        raise ValueError("checkpoint command, revision or reason is invalid")
    if allow_unresolved_effects and fence is None:
        raise ValueError("preserving unknown effects requires an authenticated pause fence")
    if checkpoint_context is not None and (
        fence is None or not isinstance(checkpoint_context, dict)
    ):
        raise ValueError("checkpoint controller context requires a trusted pause owner")
    if graph_transition_validation is not None and (
        graph_validation is None or on_ack is None or prepare_ack is None
    ):
        raise ValueError("checkpoint graph transition needs prepared exact acknowledgement")
    if prepare_ack is not None and on_ack is None:
        raise ValueError("checkpoint acknowledgement preparation needs an acknowledgement")
    context = json.loads(canonical_json(checkpoint_context))
    context_sha256 = digest(context) if context is not None else None
    if admission_context is not None and attempt_validation is not None:
        raise ValueError("checkpoint admission requires pinned attempt observation")
    spec = store.effective_spec(run_id)
    if spec.get("workspace_custody_version") != 1:
        raise ValueError("checkpoint requires independently owned workspace custody")
    if not store.owns_execution(spec):
        raise ValueError("checkpoint service does not own the admitted execution transport")
    writer_context = _writer_custody(store, spec, writer_context, fence, context)
    checkpoint_id = _hash(command_id.encode())
    request = {
        "kind": CREATED,
        "run_id": run_id,
        "command_id": command_id,
        "expected_revision": expected_revision,
        "reason": reason,
        "allow_unresolved_effects": allow_unresolved_effects,
    }
    key = "checkpoint-create:" + command_id
    controller = _controller_binding(store, spec, expected_revision, fence)
    with _locked(spec) as root, ExitStack() as stack:
        registry = RunResources(spec, read_only=True)
        resources = stack.enter_context(registry.locked(read_only=True, exclusive=True))
        with store._connect() as db:
            saved = _command(db, key, run_id, request)
            prior = _read_acknowledged(db, spec, checkpoint_id)[0] if saved is not None else None
            if prior is not None:
                _require_supported_native_custody(prior.get("closed_journals"))
                if not _legacy_index_format(prior):
                    _require_index_baseline(prior)
            db.execute("BEGIN")
            if _command(db, key, run_id, request) != saved:
                raise ValueError("checkpoint command changed during observation")
            row = _row(db, run_id)
            if row["revision"] != expected_revision:
                raise ValueError("checkpoint run revision changed")
            if fence is None:
                _stopped(db, row, None)
            frozen = _frozen(db, row)
            claim = store.state.claim_for(db, row["work_id"])
            owner = dict(claim) if claim else None
            db.commit()
            journals = _closed(registry, resources, frozen["delivery_attempts"], stack)
            writer = stack.enter_context(writer_context) if writer_context is not None else None
            replay_candidate = candidate_for(Path(spec["checkout"])) if prior is not None else None
            if prior is not None:
                _verify_index_baseline(Path(spec["checkout"]), prior)
            attempt_evidence = (
                stack.enter_context(attempt_observation(store, spec, frozen["delivery_attempts"]))
                if attempt_observation is not None else None
            )
            host_observation = getattr(attempt_evidence, "host_observation", None)
            if (attempt_evidence is not None and spec.get("provider") == "codex"
                    and attempt_evidence.settlements.get("attempts")
                    and host_observation is None):
                raise ValueError(
                    "checkpoint codex settlement requires the host-resource epoch fence"
                )
            registry_observation = _feature_observation(spec, stack)
            registry_projection = _prepare_projection(spec, registry_observation)
            execution_ledger = _execution_ledger(
                spec,
                allow_unresolved=allow_unresolved_effects,
                allow_unfinished_workers=fence is not None and writer is not None,
                facts=registry_observation.facts if registry_observation is not None else None,
            )
            with _checkpoint_admission(
                admission_context, registry_observation, host_observation
            ) as (registry_db, host_db, registry_fences):
                db.execute("BEGIN")
                if writer is not None:
                    writer.verify(db)
                current = _row(db, run_id)
                if _frozen(db, current) != frozen:
                    raise ValueError("checkpoint writer custody changed its original run snapshot")
                control = _stopped(db, current, fence)
                _verify_frozen_controllers(
                    _frozen(db, current), controller, fence=fence, checkpoint_context=context
                )
                if (
                    (attempt_validation is not None or attempt_evidence is not None)
                    and fence is None
                ):
                    raise ValueError("checkpoint queued-attempt settlement requires a pause fence")
                settlements = (
                    attempt_evidence.verify(db, host_db)
                    if attempt_evidence is not None
                    else attempt_validation(db, spec, frozen["delivery_attempts"])
                    if attempt_validation is not None
                    else None
                )
                if attempt_evidence is None:
                    _validate_settlements(spec, frozen["delivery_attempts"], settlements)
                if prior is not None:
                    _restore_fence(db, current, prior, fence)
                    if (
                        not _same_controller(controller, prior.get("controller"))
                        or prior.get("checkpoint_context_sha256") != context_sha256
                        or digest(spec) != prior["spec_sha256"]
                        or not _same_snapshot(frozen, prior)
                        or owner != prior["owner"]
                        or replay_candidate != prior["source"]["candidate"]
                    ):
                        raise ValueError("checkpoint creation replay lost its original custody")
                blockers = _verify_effects(
                    frozen, allow_unresolved=allow_unresolved_effects, settlements=settlements
                )
                if execution_ledger:
                    blockers.extend(
                        {
                            "scope": "feature",
                            "reason": "external_outcome_unknown",
                            "effect_key": item["effect_key"],
                            "kind": item["kind"],
                            "state": item["state"],
                        }
                        for item in execution_ledger["execution_effects"]
                        if not _settled_effect(item, feature=True)
                    )
                if attempt_evidence is not None:
                    attempt_evidence.verify(db, host_db)
                if graph_validation is not None:
                    graph_validation(db, registry_db)
                _validate_registry(registry_fences)
                db.commit()
            from .delivery_preparation import verify_prepared_spec

            verify_prepared_spec(spec)
            if saved is not None:
                return saved
            checkout = Path(spec["checkout"])
            entry = resources["roots"].get(str(checkout))
            from .delivery_filesystem_custody import matches

            if (
                not entry
                or entry["kind"] != "checkout"
                or not matches(spec, checkout, entry["identity"])
            ):
                raise ValueError("checkpoint candidate has no proven resource ownership")
            _openers(checkout)
            stage = Path(tempfile.mkdtemp(prefix=".checkpoint-", dir=root))
            try:
                inventory = {}
                source = _candidate_snapshot(spec, stage, inventory, resources)
                state_refs = _state_artifacts(spec, frozen, stage, inventory, journals, settlements)
                manifest = {
                    "schema": SCHEMA,
                    "run_id": run_id,
                    "checkpoint_id": checkpoint_id,
                    "command": request,
                    "spec_sha256": digest(spec),
                    "authority": spec,
                    "owner": owner,
                    "control": control,
                    "controller": controller,
                    "checkpoint_context": context,
                    "checkpoint_context_sha256": context_sha256,
                    "frozen": frozen,
                    "source": source,
                    "execution_ledger": execution_ledger,
                    "checkout_identity": entry["identity"],
                    "resource_manifest_sha256": _hash(read_private(registry.manifest, raw=True)),
                    "closed_journals": journals,
                    "attempt_settlements": settlements,
                    "state_refs": state_refs,
                    "artifacts": inventory,
                    "resumable": not blockers,
                    "resume_blockers": blockers,
                }
                raw = canonical_json(manifest).encode()
                _write(stage / "manifest.json", raw)
                _sync_tree(stage)
                if (
                    candidate_for(checkout) != source["candidate"]
                    or _frozen(db, _row(db, run_id)) != frozen
                    or (
                        registry_observation is not None
                        and not _registry_current(registry_observation)
                    )
                ):
                    raise ValueError("checkpoint state changed during preservation")
                destination = root / checkpoint_id
                reconciled = _publish_checkpoint(db, spec, stage, destination, manifest, raw)
                response = {
                    "run_id": run_id,
                    "checkpoint_id": checkpoint_id,
                    "manifest_sha256": _hash(raw),
                    "resumable": not blockers,
                    "resume_blockers": blockers,
                    "candidate": source["candidate"],
                    "revision": expected_revision,
                    "reconciled_partial": reconciled,
                    "checkpoint_context_sha256": context_sha256,
                }
                # The callback receives independent copies; it cannot mutate the
                # captured context or receipt used for publication and validation.
                ack_receipt = json.loads(canonical_json(response))
                transition_receipt = json.loads(canonical_json(response))
                ack_manifest = json.loads(raw)
                prepared_ack = None
                if prepare_ack is not None:
                    prepared_ack = prepare_ack(
                        json.loads(canonical_json(response)), json.loads(raw)
                    )
                    if not isinstance(prepared_ack, (str, bytes)):
                        raise ValueError("checkpoint prepared acknowledgement must be immutable")
                # Byte publication and external observation precede the short
                # acknowledgement transaction; both admission fences stay held.
                with _checkpoint_admission(
                    admission_context, registry_observation, host_observation
                ) as (registry_db, host_db, registry_fences):
                    db.execute("BEGIN IMMEDIATE")
                    current = _row(db, run_id)
                    if writer is not None:
                        writer.verify(db)
                    current_claim = store.state.claim_for(db, current["work_id"])
                    fresh_control = _stopped(db, current, fence)
                    _verify_frozen_controllers(
                        _frozen(db, current), controller, fence=fence, checkpoint_context=context
                    )
                    if (
                        _frozen(db, current) != frozen
                        or fresh_control != control
                        or (dict(current_claim) if current_claim else None) != owner
                    ):
                        raise ValueError(
                            "checkpoint admission state changed before acknowledgement"
                        )
                    db.execute(
                        "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                        (key, run_id, digest(request), canonical_json(response)),
                    )
                    store._event(
                        db,
                        run_id,
                        expected_revision,
                        CREATED,
                        "Durable delivery checkpoint verified",
                        response,
                        registry_db=registry_db,
                        registry_projection=registry_projection,
                    )
                    if graph_validation is not None:
                        graph_validation(db, registry_db)
                    if on_ack is not None:
                        if prepare_ack is None:
                            on_ack(db, current, ack_receipt, ack_manifest)
                        else:
                            on_ack(db, current, ack_receipt, ack_manifest, prepared_ack)
                    from .delivery_writer_custody import record_closed

                    record_closed(db, spec, controller)
                    if attempt_evidence is not None:
                        attempt_evidence.verify(db, host_db)
                    if graph_transition_validation is not None:
                        graph_transition_validation(
                            db, registry_db, transition_receipt, prepared_ack
                        )
                    elif graph_validation is not None:
                        graph_validation(db, registry_db)
                    _validate_registry(registry_fences)
                    db.commit()
                    return response
            finally:
                if stage.exists():
                    shutil.rmtree(stage)


def _read_acknowledged(db, spec, checkpoint_id, *, verify_artifacts=True):
    if not isinstance(checkpoint_id, str) or not _ID.fullmatch(checkpoint_id):
        raise ValueError("checkpoint ID is invalid")
    root = _root(spec) / checkpoint_id
    try:
        raw = read_private(root / "manifest.json", raw=True)
    except FileNotFoundError as exc:
        raise ValueError("checkpoint manifest is missing or was never acknowledged") from exc
    value = json.loads(raw)
    command = value.get("command", {})
    saved = _command(
        db, "checkpoint-create:" + command.get("command_id", ""), spec["run_id"], command
    )
    if (
        not saved
        or saved.get("checkpoint_id") != checkpoint_id
        or saved.get("manifest_sha256") != _hash(raw)
        or value.get("schema") != SCHEMA
        or value.get("run_id") != spec["run_id"]
        or value.get("checkpoint_id") != checkpoint_id
    ):
        raise ValueError("checkpoint manifest has no matching durable acknowledgement")
    if (
        db.execute(
            "SELECT 1 FROM delivery_events WHERE run_id=? AND type=? AND payload_json=?",
            (spec["run_id"], CREATED, canonical_json(saved)),
        ).fetchone()
        is None
    ):
        raise ValueError("checkpoint command lost its atomic event acknowledgement")
    if value.get("spec_sha256") != digest(value.get("authority")):
        raise ValueError("checkpoint execution authority digest changed")
    from .delivery_writer_custody import verify_closed

    verify_closed(db, value["authority"], value.get("controller"))
    for relative, item in (value["artifacts"].items() if verify_artifacts else ()):
        try:
            content = read_private(_path(root, relative), raw=True)
        except FileNotFoundError as exc:
            raise ValueError("checkpoint artifact is missing: " + relative) from exc
        if len(content) != item["size"] or _hash(content) != item["sha256"]:
            raise ValueError("checkpoint artifact is corrupt: " + relative)
    return value, saved


def read_checkpoint(store, run_id, checkpoint_id):
    spec = store.effective_spec(run_id)
    with store._connect() as db:
        value, receipt = _read_acknowledged(db, spec, checkpoint_id)
        # Keep the tracking snapshot coherent without holding a SQL read
        # transaction while the potentially large artifact inventory is hashed.
        db.execute("BEGIN")
        row = _row(db, run_id)
        current = _frozen(db, row)
        claim = store.state.claim_for(db, row["work_id"])
        owner = dict(claim) if claim else None
        db.commit()
    # Readback retains the original ACK, but does not advertise a native
    # checkpoint as resumable under today's admission contract. Current custody
    # can have changed since the ACK; inability to inspect it is also unknown.
    blockers = list(value.get("resume_blockers", []))
    if digest(spec) != value["spec_sha256"]:
        blockers.append({"scope": "authority", "reason": "checkpoint_authority_changed"})
    if not store.owns_execution(spec):
        blockers.append({"scope": "authority", "reason": "checkpoint_transport_ownership_unknown"})
    if not _same_snapshot(current, value):
        blockers.append({"scope": "run", "reason": "checkpoint_run_state_changed"})
    if owner != value["owner"]:
        blockers.append({"scope": "owner", "reason": "checkpoint_execution_owner_changed"})
    try:
        from .delivery_workspace_custody import source_for

        source = source_for(spec)
    except (OSError, ValueError, subprocess.SubprocessError):
        blockers.append({"scope": "source", "reason": "checkpoint_workspace_owner_unknown"})
    else:
        try:
            head = _git(source, "rev-parse", "refs/heads/" + value["source"]["branch"])
        except (OSError, ValueError, subprocess.SubprocessError):
            blockers.append({"scope": "source", "reason": "checkpoint_candidate_branch_unknown"})
        else:
            if head.decode().strip() != value["source"]["candidate"]["head"]:
                blockers.append({"scope": "source",
                                 "reason": "checkpoint_candidate_branch_changed"})
    effect_blockers = []
    feature_unknown = None
    current_feature = None
    if spec.get("feature_delivery"):
        try:
            with ExitStack() as stack:
                observation = _feature_observation(spec, stack)
                current_feature = _execution_ledger(
                    spec, allow_unresolved=True, allow_unfinished_workers=True,
                    facts=observation.facts,
                )
                observation.validate()
                if not _same_ledger(current_feature, value.get("execution_ledger")):
                    feature_unknown = "registry_authority_changed"
        except (OSError, ValueError, KeyError, sqlite3.Error):
            feature_unknown = "registry_observation_unknown"
    for scope, effects in (
        ("delivery", value["frozen"]["delivery_effects"]),
        ("delivery", current["delivery_effects"]),
        ("feature", (value.get("execution_ledger") or {}).get("execution_effects", [])),
        ("feature", (current_feature or {}).get("execution_effects", [])),
    ):
        for item in effects:
            if not _settled_effect(item, feature=scope == "feature"):
                blocker = {"scope": scope, "reason": "external_outcome_unknown",
                           "effect_key": item["effect_key"], "kind": item["kind"],
                           "state": item["state"]}
                if blocker not in effect_blockers:
                    effect_blockers.append(blocker)
                if blocker not in blockers:
                    blockers.append(blocker)
    retained_source_unknown = False
    try:
        registry = RunResources(spec, read_only=True)
        with registry.locked(read_only=True) as resources:
            custody_unknown = bool(value.get("closed_journals") or resources["processes"])
            entry = resources["roots"].get(spec["checkout"])
            resource_changed = (
                not entry
                or entry.get("kind") != "checkout"
                or entry.get("identity") != value["checkout_identity"]
                or _hash(read_private(registry.manifest, raw=True))
                != value["resource_manifest_sha256"]
            )
            try:
                # Follow creation/restoration's resource-before-writer lock order.
                # The exclusive witness is observational and grants no later work.
                with store._checkpoint_writer_context(spec):
                    writer_unknown = False
                    checkout = Path(spec["checkout"])
                    if os.path.lexists(checkout) and _index_baseline_supported(value):
                        try:
                            from .delivery_retained_checkpoint import verify_retained_source

                            _openers(checkout)
                            verify_retained_source(
                                spec, value, _root(spec) / checkpoint_id, resources)
                        except (OSError, ValueError, KeyError, sqlite3.Error,
                                subprocess.SubprocessError):
                            retained_source_unknown = True
            except (OSError, ValueError, sqlite3.Error, subprocess.SubprocessError):
                writer_unknown = True
    except (OSError, ValueError, KeyError):
        custody_unknown = True
        resource_changed = True
        writer_unknown = True
    if custody_unknown:
        blockers.append({"scope": "native", "reason": NativeCheckpointCustodyUnknown.code})
    if resource_changed:
        blockers.append({"scope": "resource", "reason": "checkpoint_resource_custody_unknown"})
    if writer_unknown:
        blockers.append({"scope": "writer", "reason": "checkpoint_writer_custody_unknown"})
    if retained_source_unknown:
        blockers.append({"scope": "source", "reason": "checkpoint_retained_source_unverified"})
    if feature_unknown:
        blockers.append({"scope": "feature", "reason": feature_unknown})
    index_unknown = not _index_baseline_supported(value)
    if index_unknown:
        blockers.append({"scope": "source", "reason": "index_semantics_unverified"})
    return {
        **receipt,
        "acknowledged_resumable": receipt["resumable"],
        "resumable": receipt["resumable"] and not blockers,
        "resume_blockers": blockers,
        "authority_sha256": value["spec_sha256"],
        "owner": value["owner"],
        "control": value["control"],
        "pending_effects": list(value.get("resume_blockers", [])) + [
            item for item in effect_blockers if item not in value.get("resume_blockers", [])
        ],
        "artifact_count": len(value["artifacts"]),
        "retention": "pinned-until-explicit-terminal-prune",
        **({"legacy_index_recovery": "restore-recorded-v1-source-then-fresh-seal"}
           if _legacy_index_format(value) else {}),
    }


def _record_restore_intent(db, key, intent):
    db.commit()
    db.execute("BEGIN IMMEDIATE")
    changed = db.execute(
        "UPDATE delivery_mutations SET response_json=? WHERE command_id=? AND state='pending'",
        (canonical_json(intent), key),
    ).rowcount
    if changed != 1:
        db.rollback()
        raise ValueError("checkpoint restore intent is unavailable")
    db.commit()


def _restore_git(db, key, intent, path, *args, **kwargs):
    """Record helper uncertainty before any Git operation that can mutate custody."""
    previous = intent["phase"]
    intent.update(phase="git_active", git_operation=list(args), previous_phase=previous)
    _record_restore_intent(db, key, intent)
    try:
        result = _git(path, *args, **kwargs)
    except Exception as exc:
        raise CheckpointRestoreUnknown(
            "checkpoint restore Git/helper outcome is unknown; retain its pending allocation "
            "until the original operation and helpers are reconciled"
        ) from exc
    intent["phase"] = previous
    intent.pop("git_operation")
    intent.pop("previous_phase")
    _record_restore_intent(db, key, intent)
    return result


def _restore_file(db, key, intent, target, raw, mode, *, mtime_ns=None, adjust_mode=False):
    """Publish a completed file without replacing existing filesystem custody."""
    private_directory(target.parent)
    identity_key = str(target)
    name = ".checkpoint-file-" + _hash((key + "\0" + identity_key).encode())
    temporary = target.parent / name
    files = intent.setdefault("file_publications", {})
    binding = {"temporary": str(temporary), "sha256": _hash(raw), "size": len(raw),
               "mode": mode, "mtime_ns": mtime_ns}
    record = files.get(identity_key)
    if record is not None and any(record.get(k) != v for k, v in binding.items()):
        raise ValueError("checkpoint restore file publication binding changed")

    def stat_identity(info):
        return {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid}

    if os.path.lexists(target):
        info = target.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or target.read_bytes() != raw
                or (not adjust_mode and stat.S_IMODE(info.st_mode) != mode)
                or (mtime_ns is not None and info.st_mtime_ns != mtime_ns)):
            raise ValueError(
                "checkpoint original evidence or restored file changed: " + identity_key)
        if record is not None and record.get("identity") != stat_identity(info):
            raise CheckpointRestoreUnknown("checkpoint restored file identity changed")
        if os.path.lexists(temporary):
            temp_info = temporary.lstat()
            if (record is None or record.get("identity") != stat_identity(temp_info)
                    or info.st_nlink != 2 or stat_identity(info) != stat_identity(temp_info)):
                raise CheckpointRestoreUnknown("checkpoint file publication identity is unknown")
            temporary.unlink()
            _sync_parents(temporary, target.parent)
        elif info.st_nlink != 1:
            raise ValueError("checkpoint restored file has foreign hard links: " + identity_key)
        if adjust_mode and stat.S_IMODE(info.st_mode) != mode:
            target.chmod(mode)
        return
    if record is None:
        record = {**binding, "identity": None}
        files[identity_key] = record
        _record_restore_intent(db, key, intent)
    if record["identity"] is None:
        if os.path.lexists(temporary):
            raise CheckpointRestoreUnknown("checkpoint file staging identity is unknown")
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        record["identity"] = stat_identity(os.fstat(descriptor))
        try:
            _record_restore_intent(db, key, intent)
        except BaseException:
            os.close(descriptor)
            raise
    else:
        try:
            descriptor = os.open(temporary, os.O_RDWR | os.O_NOFOLLOW)
        except FileNotFoundError as exc:
            raise CheckpointRestoreUnknown("checkpoint file staging is missing") from exc
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1
                or stat_identity(info) != record["identity"]
                or stat_identity(temporary.lstat()) != record["identity"]):
            raise CheckpointRestoreUnknown("checkpoint file staging identity changed")
        os.ftruncate(descriptor, 0)
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(raw)
            stream.flush()
        os.fchmod(descriptor, mode)
        if mtime_ns is not None:
            os.utime(descriptor, ns=(mtime_ns, mtime_ns))
        os.fsync(descriptor)
        if (stat_identity(os.fstat(descriptor)) != record["identity"]
                or stat_identity(temporary.lstat()) != record["identity"]):
            raise CheckpointRestoreUnknown("checkpoint file staging identity changed")
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError as exc:
            raise ValueError("checkpoint restore target appeared during publication") from exc
        if stat_identity(target.lstat()) != record["identity"]:
            raise CheckpointRestoreUnknown("checkpoint file publication identity changed")
        _sync_parents(target, target.parent)
        temporary.unlink()
        _sync_parents(target, target.parent)
    finally:
        os.close(descriptor)


def _finish_restore(
    store, db, spec, value, checkpoint, request, key, intent, *, fence, writer=None,
    admission_context=None, checkpoint_context=None, registry_observation=None,
    graph_validation=None, attempt_evidence=None, host_observation=None, registry_projection=None,
):
    """Complete an acknowledged allocation intent, including a lost move response."""
    from .delivery_workspace_custody import source_for

    checkout = Path(spec["checkout"])
    staging = Path(intent["staging"])
    if (
        staging.parent != checkout.parent
        or not staging.name.startswith(".checkpoint-restore-")
        or staging.resolve() != staging
    ):
        raise ValueError("checkpoint restore intent escaped its allocation namespace")
    current = checkout if os.path.lexists(checkout) else staging
    if (
        not current.exists()
        or current.is_symlink()
        or _identity(current) != intent["prepared_identity"]
        or candidate_for(current) != value["source"]["candidate"]
        or _git(current, "write-tree").decode().strip() != value["source"]["index_tree"]
    ):
        raise ValueError("checkpoint restore allocation has an unknown or changed outcome")
    _openers(current)
    index_reconstruction = _verify_index_baseline(current, value)
    if index_reconstruction is not None:
        root = _root(spec) / value["checkpoint_id"]
        for name, args in (
            ("staged.patch", ("diff", "--cached", "--binary", "--full-index", "HEAD")),
            ("unstaged.patch", ("diff", "--binary", "--full-index")),
        ):
            if _git(current, *args) != read_private(root / name, raw=True):
                raise CheckpointRestoreUnknown(
                    "legacy source bytes and recorded patches need index reconciliation; "
                    "the prepared workspace is retained without inventing old flags")
    observed_ledger = _execution_ledger(
        spec, allow_unfinished_workers=fence is not None and writer is not None,
        facts=registry_observation.facts if registry_observation is not None else None,
    )
    with _checkpoint_admission(
        admission_context, registry_observation, host_observation
    ) as (registry_db, host_db, registry_fences):
        if not db.in_transaction:
            db.execute("BEGIN")
        row = _row(db, spec["run_id"])
        _restore_fence(db, row, value, fence)
        _verify_frozen_controllers(
            _frozen(db, row), value.get("controller"),
            fence=fence, checkpoint_context=checkpoint_context,
        )
        if not _same_snapshot(_frozen(db, row), value):
            raise ValueError("checkpoint run or budget changed during restoration")
        claim = store.state.claim_for(db, row["work_id"])
        if (dict(claim) if claim else None) != value["owner"] or not _same_ledger(
            observed_ledger, value.get("execution_ledger"),
        ):
            raise ValueError("checkpoint execution owner changed during restoration")
        if attempt_evidence is not None:
            attempt_evidence.verify(db, host_db)
        if graph_validation is not None:
            graph_validation(db, registry_db)
        _validate_registry(registry_fences)
        db.commit()
    source = source_for(spec)
    if (
        Path(
            _git(current, "rev-parse", "--path-format=absolute", "--git-common-dir")
            .decode()
            .strip()
        )
        != source
    ):
        raise ValueError("checkpoint restored workspace lost its independent Git owner")
    branch = value["source"]["branch"]
    if (
        _git(source, "rev-parse", "refs/heads/" + branch).decode().strip()
        != value["source"]["candidate"]["head"]
    ):
        raise ValueError("checkpoint local branch moved; reconcile before restoring")
    if current == staging:
        _restore_git(db, key, intent, source, "worktree", "prune")
        _restore_git(db, key, intent, staging, "symbolic-ref", "HEAD", "refs/heads/" + branch)
        _restore_git(db, key, intent, source, "worktree", "move", str(staging), str(checkout))
    for relative, item in value["state_refs"].items():
        target = _path(Path(spec["state_dir"]), relative)
        if item.get("kind") == "absent":
            if os.path.lexists(target):
                raise ValueError("checkpoint no-GO result is no longer absent: " + relative)
            continue
        raw = read_private(_root(spec) / value["checkpoint_id"] / item["artifact"], raw=True)
        _restore_file(db, key, intent, target, raw, item["mode"], mtime_ns=item["mtime_ns"])
        _sync_parents(target, Path(spec["state_dir"]))
    _sync_tree(checkout)
    observed_ledger = _execution_ledger(
        spec, allow_unfinished_workers=fence is not None and writer is not None,
        facts=registry_observation.facts if registry_observation is not None else None,
    )
    if not _same_ledger(observed_ledger, value.get("execution_ledger")):
        raise ValueError("checkpoint cumulative repair budget changed before acknowledgement")
    restored_identity = _identity(checkout)
    with _checkpoint_admission(
        admission_context, registry_observation, host_observation
    ) as (registry_db, host_db, registry_fences):
        db.execute("BEGIN IMMEDIATE")
        current_row = _row(db, spec["run_id"])
        if writer is not None:
            writer.verify(db)
        _restore_fence(db, current_row, value, fence)
        _verify_frozen_controllers(
            _frozen(db, current_row), value.get("controller"),
            fence=fence, checkpoint_context=checkpoint_context,
        )
        current_claim = store.state.claim_for(db, current_row["work_id"])
        if (
            not _same_snapshot(_frozen(db, current_row), value)
            or (dict(current_claim) if current_claim else None) != value["owner"]
        ):
            raise ValueError(
                "checkpoint admission state changed before restoration acknowledgement"
            )
        response = {
            "run_id": spec["run_id"],
            "checkpoint_id": value["checkpoint_id"],
            "manifest_sha256": checkpoint["manifest_sha256"],
            "original_identity": value["checkout_identity"],
            "restored_identity": restored_identity,
            "resource_manifest_sha256": value["resource_manifest_sha256"],
            "candidate": value["source"]["candidate"],
            "revision": request["expected_revision"],
            "restored": True,
            "execution_started": False,
            **({"index_reconstruction": index_reconstruction}
               if index_reconstruction is not None else {}),
        }
        db.execute(
            "INSERT INTO delivery_commands VALUES (?,?,?,?)",
            (key, spec["run_id"], digest(request), canonical_json(response)),
        )
        db.execute(
            "UPDATE delivery_mutations SET state='complete',response_json=? WHERE command_id=?",
            (canonical_json(response), key),
        )
        store._event(
            db,
            spec["run_id"],
            request["expected_revision"],
            RESTORED,
            "Delivery checkpoint restored into a new owned workspace",
            response,
            registry_db=registry_db,
            registry_projection=registry_projection,
        )
        if attempt_evidence is not None:
            attempt_evidence.verify(db, host_db)
        if graph_validation is not None:
            graph_validation(db, registry_db)
        _validate_registry(registry_fences)
        db.commit()
        return response


def _populate_restore(
    store, db, spec, value, checkpoint, request, key, intent, *, fence, writer=None,
    admission_context=None, checkpoint_context=None, registry_observation=None,
    graph_validation=None, attempt_evidence=None, host_observation=None, registry_projection=None,
):
    """Reconcile an allocated inode before repeating any local Git side effect."""
    from .delivery_workspace_custody import source_for

    checkout, staging = Path(spec["checkout"]), Path(intent["staging"])
    if (
        staging.parent != checkout.parent
        or not staging.name.startswith(".checkpoint-restore-")
        or staging.resolve() != staging
        or os.path.lexists(checkout)
        or _identity(staging) != intent["allocated_identity"]
    ):
        raise ValueError("checkpoint allocation outcome is unknown or changed")
    db.commit()
    source = source_for(spec)
    root = _root(spec) / value["checkpoint_id"]
    if not os.path.lexists(staging / ".git"):
        if any(staging.iterdir()):
            raise ValueError("checkpoint allocation contains unowned partial source")
        _restore_git(
            db, key, intent, source,
            "fetch",
            "--no-tags",
            "--no-write-fetch-head",
            str(root / "candidate.bundle"),
            value["source"]["candidate"]["head"],
        )
        _restore_git(
            db, key, intent, source,
            "worktree",
            "add",
            "--detach",
            str(staging),
            value["source"]["candidate"]["head"],
        )
    if (
        _git(staging, "rev-parse", "HEAD").decode().strip() != value["source"]["candidate"]["head"]
        or Path(
            _git(staging, "rev-parse", "--path-format=absolute", "--git-common-dir")
            .decode()
            .strip()
        )
        != source
    ):
        raise ValueError("checkpoint allocated worktree lost its exact Git custody")
    _openers(staging)
    staged = read_private(root / "staged.patch", raw=True)
    current_index = _git(staging, "write-tree").decode().strip()
    if current_index != value["source"]["index_tree"]:
        if current_index != _git(staging, "rev-parse", "HEAD^{tree}").decode().strip():
            raise ValueError("checkpoint allocated index has unknown changes")
        if staged:
            _restore_git(
                db, key, intent, staging, "apply", "--index", "--binary", "-", input=staged)
    unstaged = read_private(root / "unstaged.patch", raw=True)
    current_diff = _git(staging, "diff", "--binary", "--full-index")
    if current_diff != unstaged:
        if current_diff:
            raise ValueError("checkpoint allocated worktree has unknown changes")
        if unstaged:
            _restore_git(db, key, intent, staging, "apply", "--binary", "-", input=unstaged)
    for relative, item in value["source"]["files"].items():
        target = _path(staging, relative)
        if item["kind"] == "symlink":
            if os.path.lexists(target):
                if not target.is_symlink() or os.readlink(target) != item["target"]:
                    raise ValueError("checkpoint allocated link changed: " + relative)
            else:
                private_directory(target.parent)
                target.symlink_to(item["target"])
        else:
            raw = read_private(root / item["artifact"], raw=True)
            _restore_file(db, key, intent, target, raw,
                          0o700 if item["executable"] else 0o600, adjust_mode=True)
    if (
        candidate_for(staging) != value["source"]["candidate"]
        or _git(staging, "write-tree").decode().strip() != value["source"]["index_tree"]
    ):
        raise ValueError("checkpoint restored candidate or index differs")
    _sync_tree(staging)
    intent = {**intent, "phase": "prepared", "prepared_identity": _identity(staging)}
    with _checkpoint_admission(
        admission_context, registry_observation, host_observation
    ) as (registry_db, host_db, registry_fences):
        db.execute("BEGIN IMMEDIATE")
        current = _row(db, spec["run_id"])
        if writer is not None:
            writer.verify(db)
        _restore_fence(db, current, value, fence)
        _verify_frozen_controllers(
            _frozen(db, current), value.get("controller"),
            fence=fence, checkpoint_context=checkpoint_context,
        )
        claim = store.state.claim_for(db, current["work_id"])
        if (
            not _same_snapshot(_frozen(db, current), value)
            or (dict(claim) if claim else None) != value["owner"]
        ):
            raise ValueError("checkpoint admission changed during source reconstruction")
        db.execute(
            "UPDATE delivery_mutations SET response_json=? WHERE command_id=? AND state='pending'",
            (canonical_json(intent), key),
        )
        if attempt_evidence is not None:
            attempt_evidence.verify(db, host_db)
        if graph_validation is not None:
            graph_validation(db, registry_db)
        _validate_registry(registry_fences)
        db.commit()
    return _finish_restore(
        store, db, spec, value, checkpoint, request, key, intent, fence=fence, writer=writer,
        admission_context=admission_context, checkpoint_context=checkpoint_context,
        registry_observation=registry_observation,
        registry_projection=registry_projection,
        graph_validation=graph_validation, attempt_evidence=attempt_evidence,
        host_observation=host_observation,
    )


def _allocate_restore(store, db, spec, value, checkpoint, request, key, intent, **kwargs):
    """Allocate only after its unique pathname is durable in the command journal."""
    checkout, staging = Path(spec["checkout"]), Path(intent["staging"])
    if (staging.parent != checkout.parent or not staging.name.startswith(".checkpoint-restore-")
            or staging.resolve() != staging or os.path.lexists(checkout)):
        raise ValueError("checkpoint reserved allocation left its authorized namespace")
    if os.path.lexists(staging):
        raise CheckpointRestoreUnknown(
            "checkpoint restore allocation exists before its identity was recorded; "
            "retain it for reconciliation"
        )
    os.mkdir(staging, 0o700)
    _sync_parents(staging, checkout.parent)
    intent.update(phase="allocated", allocated_identity=_identity(staging))
    _record_restore_intent(db, key, intent)
    return _populate_restore(store, db, spec, value, checkpoint, request, key, intent, **kwargs)


def restore_checkpoint(
    store,
    run_id,
    checkpoint_id,
    command_id,
    expected_revision,
    *,
    fence=None,
    admission_context=None,
    writer_context=None,
    attempt_validation=None,
    attempt_observation: AttemptObservationFactory | None = None,
    checkpoint_context=None,
    graph_validation: GraphValidation | None = None,
):
    """Rehydrate missing candidate bytes without scheduling or replaying an effect."""
    if (
        not isinstance(command_id, str)
        or not command_id
        or len(command_id) > 200
        or type(expected_revision) is not int
    ):
        raise ValueError("checkpoint restore command or revision is invalid")
    if admission_context is not None and attempt_validation is not None:
        raise ValueError("checkpoint admission requires pinned attempt observation")
    if checkpoint_context is not None and (
        fence is None or not isinstance(checkpoint_context, dict)
    ):
        raise ValueError("checkpoint controller context requires a trusted pause owner")
    context = json.loads(canonical_json(checkpoint_context))
    context_sha256 = digest(context) if context is not None else None
    spec = store.effective_spec(run_id)
    if not store.owns_execution(spec):
        raise ValueError("checkpoint service does not own the admitted execution transport")
    writer_context = _writer_custody(store, spec, writer_context, fence, context)
    request = {
        "kind": RESTORED,
        "run_id": run_id,
        "checkpoint_id": checkpoint_id,
        "command_id": command_id,
        "expected_revision": expected_revision,
    }
    key = "checkpoint-restore:" + command_id
    controller = _controller_binding(store, spec, expected_revision, fence)
    with _locked(spec), ExitStack() as stack:
        registry = RunResources(spec, read_only=True)
        resources = stack.enter_context(registry.locked(read_only=True, exclusive=True))
        with store._connect() as db:
            value, checkpoint = _read_acknowledged(db, spec, checkpoint_id)
            _require_supported_native_custody(value.get("closed_journals"))
            if not _legacy_index_format(value):
                _require_index_baseline(value)
            if value.get("checkpoint_context_sha256") != context_sha256:
                raise ValueError("checkpoint authenticated controller snapshot changed")
            db.execute("BEGIN")
            if not _same_controller(controller, value.get("controller")):
                raise ValueError("checkpoint controller execution owner changed")
            old = _command(db, key, run_id, request)
            checkout = Path(spec["checkout"])
            row = _row(db, run_id)
            if row["revision"] != expected_revision:
                raise ValueError("checkpoint restore run revision changed")
            if digest(spec) != value["spec_sha256"]:
                raise ValueError(
                    "checkpoint execution authority changed; reconcile before restoring"
                )
            frozen = _frozen(db, row)
            _verify_effects(frozen, settlements=value.get("attempt_settlements"))
            # Restoration cannot roll a budget or candidate back to an older run.
            if not _same_snapshot(frozen, value):
                raise ValueError(
                    "checkpoint durable run, budget or effects changed; reconcile before restoring"
                )
            claim = store.state.claim_for(db, row["work_id"])
            if (dict(claim) if claim else None) != value["owner"]:
                raise ValueError("checkpoint execution owner changed")
            db.commit()
            _closed(registry, resources, frozen["delivery_attempts"], stack)
            writer = stack.enter_context(writer_context) if writer_context is not None else None
            attempt_evidence = (
                stack.enter_context(attempt_observation(store, spec, frozen["delivery_attempts"]))
                if attempt_observation is not None else None
            )
            host_observation = getattr(attempt_evidence, "host_observation", None)
            if (attempt_evidence is not None and spec.get("provider") == "codex"
                    and attempt_evidence.settlements.get("attempts")
                    and host_observation is None):
                raise ValueError(
                    "checkpoint codex settlement requires the host-resource epoch fence"
                )
            registry_observation = _feature_observation(spec, stack)
            registry_projection = _prepare_projection(spec, registry_observation)
            execution_ledger = _execution_ledger(
                spec, allow_unfinished_workers=fence is not None and writer is not None,
                facts=registry_observation.facts if registry_observation is not None else None,
            )
            if not _same_ledger(execution_ledger, value.get("execution_ledger")):
                raise ValueError("checkpoint cumulative repair budget changed")
            with _checkpoint_admission(
                admission_context, registry_observation, host_observation
            ) as (registry_db, host_db, registry_fences):
                db.execute("BEGIN")
                if writer is not None:
                    writer.verify(db)
                current = _row(db, run_id)
                _restore_fence(db, current, value, fence)
                _verify_frozen_controllers(
                    _frozen(db, current), value.get("controller"),
                    fence=fence, checkpoint_context=context,
                )
                if not _same_snapshot(_frozen(db, current), value):
                    raise ValueError("checkpoint admission changed during writer custody")
                if value.get("attempt_settlements") is not None:
                    current_settlements = (
                        attempt_evidence.verify(db, host_db)
                        if attempt_evidence is not None
                        else attempt_validation(db, spec, frozen["delivery_attempts"])
                        if attempt_validation is not None else None
                    )
                    if fence is None or current_settlements != value["attempt_settlements"]:
                        raise ValueError(
                            "checkpoint queued-attempt settlement needs fresh owner validation"
                        )
                    if attempt_evidence is None:
                        _validate_settlements(
                            spec, frozen["delivery_attempts"], value["attempt_settlements"]
                        )
                if attempt_evidence is not None:
                    attempt_evidence.verify(db, host_db)
                if graph_validation is not None:
                    graph_validation(db, registry_db)
                _validate_registry(registry_fences)
                db.commit()
            entry = resources["roots"].get(str(checkout))
            if (
                not entry
                or entry["identity"] != value["checkout_identity"]
                or _hash(read_private(registry.manifest, raw=True))
                != value["resource_manifest_sha256"]
            ):
                raise ValueError("checkpoint original resource custody changed")
            from .delivery_preparation import verify_prepared_spec

            verify_prepared_spec(spec)
            if old is not None:
                if (
                    not checkout.exists()
                    or _identity(checkout) != old["restored_identity"]
                    or candidate_for(checkout) != checkpoint["candidate"]
                ):
                    raise ValueError("restored checkpoint workspace changed")
                if ("index_reconstruction" in old
                        and not isinstance(old["index_reconstruction"], dict)):
                    raise ValueError("legacy restore receipt has an unknown index reconstruction")
                from .delivery_retained_checkpoint import verify_restored_source

                verify_restored_source(spec, value, _root(spec) / checkpoint_id, resources,
                                       old.get("index_reconstruction"))
                return old
            pending = db.execute(
                "SELECT * FROM delivery_mutations WHERE command_id=?", (key,)
            ).fetchone()
            if pending is not None:
                if (
                    pending["kind"] != RESTORED
                    or pending["run_id"] != run_id
                    or pending["request_digest"] != digest(request)
                    or pending["state"] != "pending"
                ):
                    raise ValueError("checkpoint restoration intent belongs to different inputs")
                intent = json.loads(pending["response_json"])
                if (
                    intent.get("manifest_sha256") != checkpoint["manifest_sha256"]
                    or intent.get("checkpoint_id") != checkpoint_id
                ):
                    raise ValueError("checkpoint restoration intent lost its original manifest")
                if intent.get("phase") == "git_active":
                    raise CheckpointRestoreUnknown(
                        "checkpoint restore Git/helper outcome is unknown; retain its pending "
                        "allocation until the original operation and helpers are reconciled"
                    )
                if intent.get("phase") == "reserved":
                    return _allocate_restore(
                        store, db, spec, value, checkpoint, request, key, intent,
                        fence=fence, writer=writer, admission_context=admission_context,
                        checkpoint_context=context, registry_observation=registry_observation,
                        registry_projection=registry_projection, graph_validation=graph_validation,
                        attempt_evidence=attempt_evidence, host_observation=host_observation,
                    )
                if intent.get("phase") == "allocated":
                    return _populate_restore(
                        store,
                        db,
                        spec,
                        value,
                        checkpoint,
                        request,
                        key,
                        intent,
                        fence=fence,
                        writer=writer,
                        admission_context=admission_context,
                        checkpoint_context=context,
                        registry_observation=registry_observation,
                        registry_projection=registry_projection,
                        graph_validation=graph_validation,
                        attempt_evidence=attempt_evidence,
                        host_observation=host_observation,
                    )
                if intent.get("phase") != "prepared":
                    raise CheckpointRestoreUnknown("checkpoint restore phase is unknown")
                return _finish_restore(
                    store,
                    db,
                    spec,
                    value,
                    checkpoint,
                    request,
                    key,
                    intent,
                    fence=fence,
                    writer=writer,
                    admission_context=admission_context,
                    checkpoint_context=context,
                    registry_observation=registry_observation,
                    registry_projection=registry_projection,
                    graph_validation=graph_validation,
                    attempt_evidence=attempt_evidence,
                    host_observation=host_observation,
                )
            if db.execute(
                "SELECT 1 FROM delivery_mutations WHERE run_id=? AND kind=? "
                "AND state='pending' AND command_id!=?",
                (run_id, RESTORED, key),
            ).fetchone():
                raise ValueError(
                    "checkpoint has an unfinished restoration; reconcile its original command"
                )
            if os.path.lexists(checkout):
                raise ValueError(
                    "checkpoint restoration requires the original checkout to be absent"
                )
            private_directory(checkout.parent)
            staging = checkout.parent / (".checkpoint-restore-" + secrets.token_hex(16))
            if os.path.lexists(staging):
                raise ValueError("checkpoint restore allocation name already exists")
            intent = {
                "staging": str(staging),
                "phase": "reserved",
                "checkpoint_id": checkpoint_id,
                "manifest_sha256": checkpoint["manifest_sha256"],
            }
            db.commit()
            with _checkpoint_admission(
                admission_context, registry_observation, host_observation
            ) as (registry_db, host_db, registry_fences):
                db.execute("BEGIN IMMEDIATE")
                current_row = _row(db, run_id)
                if writer is not None:
                    writer.verify(db)
                _restore_fence(db, current_row, value, fence)
                _verify_frozen_controllers(
                    _frozen(db, current_row), value.get("controller"),
                    fence=fence, checkpoint_context=context,
                )
                current_claim = store.state.claim_for(db, current_row["work_id"])
                if (
                    not _same_snapshot(_frozen(db, current_row), value)
                    or (dict(current_claim) if current_claim else None) != value["owner"]
                ):
                    raise ValueError("checkpoint admission state changed before restore intent")
                # Reserve the pathname before creating any allocation.
                db.execute(
                    "INSERT INTO delivery_mutations "
                    "(command_id,run_id,kind,request_digest,state,response_json) "
                    "VALUES (?,?,?,?,'pending',?)",
                    (key, run_id, RESTORED, digest(request), canonical_json(intent)),
                )
                if attempt_evidence is not None:
                    attempt_evidence.verify(db, host_db)
                if graph_validation is not None:
                    graph_validation(db, registry_db)
                _validate_registry(registry_fences)
                db.commit()
            return _allocate_restore(
                store,
                db,
                spec,
                value,
                checkpoint,
                request,
                key,
                intent,
                fence=fence,
                writer=writer,
                admission_context=admission_context,
                checkpoint_context=context,
                registry_observation=registry_observation,
                registry_projection=registry_projection,
                graph_validation=graph_validation,
                attempt_evidence=attempt_evidence,
                host_observation=host_observation,
            )


def restored_identity(spec, path, current, expected):
    """Authenticate only an acknowledged canonical candidate replacement."""
    if (spec is None or Path(path) != Path(spec.get("checkout", ""))
            or spec.get("workspace_custody_version") != 1):
        return current
    from .delivery_execution_config import load_execution_config

    config = load_execution_config(spec)
    with closing(sqlite3.connect(config.tracking_db.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        for row in db.execute(
            "SELECT command_id,request_digest,response_json FROM delivery_commands WHERE run_id=? "
            "AND command_id LIKE 'checkpoint-restore:%' ORDER BY rowid DESC",
            (spec["run_id"],),
        ):
            receipt = json.loads(row["response_json"])
            if (
                receipt.get("original_identity") != expected
                or receipt.get("restored_identity") != current
            ):
                continue
            command = {
                "kind": RESTORED,
                "run_id": spec["run_id"],
                "checkpoint_id": receipt["checkpoint_id"],
                "command_id": row["command_id"].removeprefix("checkpoint-restore:"),
                "expected_revision": receipt["revision"],
            }
            mutation = db.execute(
                "SELECT state,response_json FROM delivery_mutations WHERE command_id=?",
                (row["command_id"],),
            ).fetchone()
            if (
                row["request_digest"] != digest(command)
                or mutation is None
                or mutation["state"] != "complete"
                or json.loads(mutation["response_json"]) != receipt
                or db.execute(
                    "SELECT 1 FROM delivery_events WHERE run_id=? AND type=? AND payload_json=?",
                    (spec["run_id"], RESTORED, canonical_json(receipt)),
                ).fetchone()
                is None
            ):
                raise ValueError("checkpoint restoration lost its atomic command acknowledgement")
            value, ack = _read_acknowledged(db, spec, receipt["checkpoint_id"])
            if (
                receipt.get("manifest_sha256") != ack["manifest_sha256"]
                or value["checkout_identity"] != expected
                or receipt.get("resource_manifest_sha256") != value["resource_manifest_sha256"]
            ):
                raise ValueError("checkpoint restoration custody binding changed")
            return expected
    return current
