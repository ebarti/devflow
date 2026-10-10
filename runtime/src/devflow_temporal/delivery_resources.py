"""Per-run ownership and resumable finalization of explicitly allocated roots."""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import sqlite3
import stat
import subprocess
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any

from .contracts import canonical_json, digest


def private_directory(path: Path) -> None:
    if not path.parent.exists():
        private_directory(path.parent)
    _ancestors(path)
    path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError("resource parent is not an owned directory")


def read_private(path: Path) -> dict:
    # Recheck the replacement once when publication unlinks the first inode.
    # A second authenticated descriptor remains a valid snapshot if unlinked too;
    # it need not be the latest pathname value under continuous publication.
    for observation in range(2):
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise ValueError("resource evidence is not a private owned file") from exc
            raise
        try:
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_mode & 0o777 != 0o600
                or info.st_nlink not in (0, 1)
            ):
                raise ValueError("resource evidence is not a private owned file")
            if info.st_nlink == 0 and observation == 0:
                continue
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                return json.load(stream)
        finally:
            os.close(descriptor)
    raise ValueError("resource evidence is not a private owned file")


def write_private(path: Path, value: dict) -> None:
    private_directory(path.parent)
    if path.exists() or path.is_symlink():
        read_private(path)
    temporary = path.with_name(path.name + f".{os.getpid()}.new")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(value, stream, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def _identity(path: Path) -> dict[str, int]:
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError("registered resource root was replaced")
    return {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid}


def _ancestors(path: Path, *, allow_missing: bool = False) -> None:
    for parent in reversed(path.parents):
        if parent.is_symlink() or (parent.exists() and not parent.is_dir()):
            raise ValueError("resource ancestor was replaced")
        if not allow_missing and not parent.exists():
            raise ValueError("resource ancestor is absent")


def _gate_roots(spec: dict) -> tuple[Path, set[Path]]:
    """Resolve existing continuation custody read-only, never from a caller path."""
    state = Path(spec['state_dir'])
    if (not state.is_absolute() or state.name != spec['run_id'] or '..' in state.parts
            or state.resolve() != state or 'evidence_root' in spec):
        raise ValueError('gate namespace left its canonical original run')
    _ancestors(state, allow_missing=True)
    namespace = None
    roots = {state}
    if 'config_path' in spec:
        from .delivery_config import DeliveryConfig

        config = DeliveryConfig.load(Path(spec['config_path']))
        if (digest(config.raw) != spec['config_digest']
                or config.path != Path(spec['config_path'])
                or state != config.state_root / 'runs' / spec['run_id']):
            raise ValueError('gate namespace frozen configuration changed')
        with closing(sqlite3.connect(config.tracking_db.as_uri() + '?mode=ro', uri=True)) as db:
            row = db.execute(
                'SELECT request_digest,recovery_json FROM delivery_runs WHERE run_id=?',
                (spec['run_id'],),
            ).fetchone()
            if row is None or row[0] != spec['request_digest']:
                raise ValueError('gate namespace has no durable original run')
            recovery = json.loads(row[1]) if row[1] else None
            first = True
            if recovery and recovery.get('kind') == 'pending_publication_retry':
                from .delivery_pending_publication import custody

                custody(db, recovery)
                if canonical_json(recovery['execution_spec']) != canonical_json(spec):
                    raise ValueError('pending publication execution authority changed')
                namespace = 'publication-retry'
                roots.add(state / namespace / 'evidence')
                first = False
                recovery = recovery.get('original_recovery')
            while recovery and recovery.get('kind') in {
                'terminal_tracker_recovery', 'repair_continuation',
                'investigation_assessment_adjudication', 'stopped_resource_closure',
                'stopped_delivery_resume',
            }:
                if recovery.get('kind') == 'stopped_delivery_resume':
                    from .delivery_stopped_resume import custody
                    from .delivery_stopped_resume import namespace as resume_namespace

                    custody(db, recovery)
                    if first and canonical_json(recovery['execution_spec']) != canonical_json(spec):
                        raise ValueError('stopped resume execution authority changed')
                    admitted = resume_namespace(recovery)
                    roots.add(state / admitted / 'evidence')
                    if first:
                        namespace = admitted
                    first = False
                if recovery.get('kind') == 'repair_continuation' and recovery.get(
                        'finalized_checkpoint'):
                    from .delivery_gate_retry import effective_repair

                    effective_repair(None, recovery, db=db)
                    if first and canonical_json(recovery['execution_spec']) != canonical_json(spec):
                        raise ValueError('finalized repair execution authority changed')
                    if first:
                        namespace = 'repair-continuation'
                    roots.add(state / 'repair-continuation' / 'evidence')
                    first = False
                if recovery.get('kind') == 'stopped_resource_closure':
                    from .delivery_resource_closure import custody

                    custody(db, recovery)
                    if first and canonical_json(recovery['execution_spec']) != canonical_json(spec):
                        raise ValueError('resource closure current execution authority changed')
                    if first:
                        namespace = 'resource-closure'
                    roots.add(state / 'resource-closure' / 'evidence')
                    first = False
                if recovery.get('kind') == 'investigation_assessment_adjudication':
                    from .delivery_investigation_adjudication import custody

                    custody(db, recovery)
                recovery = recovery.get('original_recovery')
            while recovery and recovery.get('kind') in {
                'published_metadata_recovery', 'investigation_gates_only',
                'accepted_technical_successor', 'published_gate_retry', 'prepublication_gate_retry',
                'pending_publication_retry',
                'published_check_prelaunch_retry', 'published_ci_retry',
                'published_controller_retry',
                'repair_continuation',
            }:
                if first and canonical_json(recovery.get('execution_spec')) != canonical_json(spec):
                    raise ValueError('gate namespace execution authority changed')
                if recovery['kind'] == 'repair_continuation':
                    from .delivery_gate_retry import effective_repair

                    if not recovery.get('finalized_checkpoint'):
                        raise ValueError('historical repair checkpoint is not finalized')
                    effective_repair(None, recovery, db=db)
                    admitted_namespace = 'repair-continuation'
                elif recovery['kind'] == 'pending_publication_retry':
                    admitted_namespace = 'publication-retry'
                    if canonical_json(read_private(state / admitted_namespace / 'admission.json')) \
                            != canonical_json(recovery):
                        raise ValueError('historical publication admission changed')
                elif recovery['kind'] == 'accepted_technical_successor':
                    from .delivery_technical_continuation import namespace_custody

                    admitted_namespace = 'technical-successor'
                    namespace_custody(db, recovery['execution_spec'], recovery)
                elif recovery['kind'] == 'published_metadata_recovery':
                    admitted_namespace = 'metadata-reconciliation'
                    _ancestors(state / admitted_namespace / 'intent.json')
                    intent = read_private(state / admitted_namespace / 'intent.json')
                    admitted = db.execute(
                        'SELECT grant_json,state FROM delivery_metadata_recoveries WHERE run_id=?',
                        (spec['run_id'],),
                    ).fetchone()
                    if (not admitted or admitted[1] != 'queued'
                            or canonical_json(json.loads(admitted[0])) != canonical_json(intent)
                            or digest(intent) != recovery.get('grant_digest')
                            or any(canonical_json(recovery.get(key)) != canonical_json(value)
                                   for key, value in intent.items())):
                        raise ValueError('gate namespace metadata admission changed')
                else:
                    from .delivery_gate_retry import gate_namespace

                    admitted_namespace = gate_namespace(
                        recovery['execution_spec'].get('gate_retry_generation', 1),
                        recovery['execution_spec'].get('gate_retry_stage'))
                    _ancestors(state / admitted_namespace / 'admission.json')
                    admission = read_private(state / admitted_namespace / 'admission.json')
                    admitted = db.execute(
                        'SELECT recovery_json FROM delivery_gate_admissions WHERE run_id=?',
                        (spec['run_id'],),
                    ).fetchone()
                    if (not admitted or canonical_json(admission) != canonical_json(recovery)
                            or (first and canonical_json(json.loads(admitted[0]))
                                != canonical_json(recovery))):
                        raise ValueError('gate namespace gates-only admission changed')
                roots.add(state / admitted_namespace / 'evidence')
                if first:
                    namespace = admitted_namespace
                first = False
                recovery = recovery.get('original_recovery')
    root = state / namespace / 'evidence' if namespace else state
    _ancestors(root, allow_missing=True)
    for path in (state, *((state / namespace, root) if namespace else ())):
        if path.exists():
            info = path.lstat()
            if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                    or (path != state and stat.S_IMODE(info.st_mode) != 0o700)):
                raise ValueError('gate namespace is not private and owned')
    for historical in roots:
        _ancestors(historical, allow_missing=True)
    return root, roots


def _gate_evidence_root(spec: dict) -> Path:
    return _gate_roots(spec)[0]


def _gate_path(spec: dict, role: str, iteration: int) -> Path:
    if role not in {'review', 'verify', 'baseline'} or type(iteration) is not int or iteration < 0:
        raise ValueError('gate namespace role or iteration is invalid')
    if role == 'baseline' and (spec.get('baseline_checks_version') not in (1, 2) or iteration != 0):
        raise ValueError('baseline checkout requires a new baseline-check admission')
    path = _gate_evidence_root(spec) / 'gates' / str(iteration) / role
    _ancestors(path, allow_missing=True)
    if path.is_symlink():
        raise ValueError('gate checkout is a symlink alias')
    return path


def _remove_contents(fd: int) -> None:
    """Never follow an entry, including a symlink swapped during traversal."""
    for name in os.listdir(fd):
        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
        if stat.S_ISDIR(info.st_mode):
            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            try:
                if os.fstat(child).st_ino != info.st_ino:
                    raise ValueError("temporary directory changed during finalization")
                _remove_contents(child)
            finally:
                os.close(child)
            os.rmdir(name, dir_fd=fd)
        else:
            os.unlink(name, dir_fd=fd)


def remove_directory(path: Path, identity: dict) -> None:
    _ancestors(path)
    parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        fd = os.open(path.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        try:
            info = os.fstat(fd)
            if {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid} != identity:
                raise ValueError("temporary root identity changed before deletion")
            _remove_contents(fd)
            if _identity(path) != identity:
                raise ValueError("temporary root changed during deletion")
        finally:
            os.close(fd)
        os.rmdir(path.name, dir_fd=parent)
    finally:
        os.close(parent)
    if os.path.lexists(path):
        raise ValueError("temporary root is still present after removal")


def projected_cleanup(spec: dict, checks: dict, recorded: str, *, terminal: bool) -> str:
    """Read legacy terminal success only from the exact owning finalization proof."""
    if recorded == "unknown" or not terminal:
        return recorded
    receipt = checks.get("resource_cleanup", {})
    if not isinstance(receipt, dict):
        return recorded
    try:
        path = Path(spec["state_dir"]) / "resources/finalization.json"
        if receipt.get("receipt") != str(path):
            return recorded
        observed = read_private(path)
        if hashlib.sha256(path.read_bytes()).hexdigest() != receipt.get("receipt_sha256"):
            return "unknown"
        expected = {key: value for key, value in receipt.items()
                    if key not in {"receipt", "receipt_sha256"}}
        if canonical_json(observed) != canonical_json(expected):
            return "unknown"
        if (observed.get("state") == "confirmed"
                and observed.get("process_cleanup") == "observed-native-confirmed"
                and observed.get("resource_cleanup") == "confirmed"):
            return "confirmed"
        return "unknown"
    except (OSError, ValueError, KeyError):
        return "unknown"


class RunResources:
    def __init__(self, spec: dict, *, read_only: bool = False) -> None:
        self.spec = spec
        self.state = Path(spec["state_dir"])
        self.root = self.state / "resources"
        self.manifest = self.root / "manifest.json"
        if not self.state.is_absolute() or self.state.name != spec["run_id"]:
            raise ValueError("resource registry is outside its run")
        if read_only:
            _identity(self.root)
        else:
            private_directory(self.root)
        _ancestors(self.state)

    @contextmanager
    def locked(self):
        descriptor = os.open(
            self.root / "ownership.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
        )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            value = (
                read_private(self.manifest)
                if self.manifest.exists()
                else {
                    "schema": "devflow-run-resources-v1",
                    "run_id": self.spec["run_id"],
                    "state_identity": _identity(self.state),
                    "roots": {},
                    "processes": [],
                }
            )
            if value.get("run_id") != self.spec["run_id"] or value["state_identity"] != _identity(
                self.state
            ):
                raise ValueError("run resource ownership changed")
            yield value
        finally:
            os.close(descriptor)

    def _allowed(self, path: Path, kind: str, *, finalizing: bool = False) -> None:
        short = Path("/private/tmp") / ("dfqa-" + digest(str(self.state))[:20])
        valid = path == self.state / "transient" or path == short
        if kind == "execution-scratch":
            valid = path == self._execution_scratch_root()
        if kind == "checkout":
            valid = path == Path(self.spec["checkout"])
        elif kind == "gate":
            valid = False
            for root in _gate_roots(self.spec)[1]:
                if not path.is_relative_to(root / 'gates'):
                    continue
                relative = path.relative_to(root / 'gates')
                valid = (len(relative.parts) == 2 and relative.parts[0].isdecimal()
                         and (relative.parts[1] in {"review", "verify"}
                              or (relative.parts == ("0", "baseline")
                                  and self.spec.get("baseline_checks_version") in (1, 2))))
        elif kind == "generated":
            allowed_names = {
                "node_modules",
                *(self.spec["policy"].get("browser_qa") or {}).get("artifact_paths", []),
            }
            ownership = read_private(self.manifest)["roots"] if self.manifest.exists() else {}
            valid = False
            # Broker admission bounds each gate against durable operator grants
            # before registering it. Finalization uses that recorded ownership,
            # including a removed parent, rather than the original repair budget.
            for raw_root, entry in ownership.items():
                root = Path(raw_root)
                if entry["kind"] not in {"checkout", "gate"}:
                    continue
                names = set(allowed_names)
                if (path.name == '.venv' and path.is_relative_to(root)
                        and self.spec['policy'].get('host_sandbox') == 'trusted-local'):
                    from .delivery_configured_resources import (
                        python_environment_authority,
                        require_locked_project,
                    )

                    relative = path.relative_to(root).as_posix()
                    configured = python_environment_authority(self.spec, relative)
                    retained = ownership.get(str(path), {})
                    if ('configured_environment_sha256' in retained
                            and configured != retained['configured_environment_sha256']):
                        raise ValueError('generated environment configured authority changed')
                    if finalizing:
                        if 'configured_environment_sha256' in retained:
                            matches = configured == retained['configured_environment_sha256']
                        else:
                            matches = (retained.get('accepted_plan_sha256')
                                       == digest(self.spec['accepted_plan']))
                        if retained.get('kind') != 'generated' or not matches:
                            raise ValueError('generated environment recorded plan changed')
                        # Partial implementation can change test discovery. Cleanup
                        # authenticates recorded custody, parent and resource identity.
                        names.add(relative)
                    elif configured and root.exists():
                        require_locked_project(root, relative)
                        names.add(relative)
                    elif root.exists():
                        from .delivery_plan_checks import planned_projects

                        names.update((project / '.venv').relative_to(root).as_posix()
                                     for project in planned_projects(
                                         self.spec, root, preparation=True))
                if not any(path == root / name for name in names):
                    continue
                self._allowed(root, entry["kind"], finalizing=finalizing)
                if os.path.lexists(root) and _identity(root) != entry["identity"]:
                    raise ValueError("generated resource parent was replaced")
                valid = True
        if not valid or not path.is_absolute() or ".." in path.parts:
            raise ValueError("resource root is outside its registered run boundary")
        _ancestors(path, allow_missing=finalizing)

    def register(self, path: Path, kind: str) -> None:
        self._allowed(path, kind)
        with self.locked() as manifest:
            old = manifest["roots"].get(str(path))
            if old is not None:
                if old["kind"] != kind:
                    raise ValueError("resource purpose changed")
                if (
                    old.get("identity")
                    and os.path.lexists(path)
                    and _identity(path) != old["identity"]
                ):
                    raise ValueError("registered resource root was replaced")
                if (
                    kind != "checkout"
                    and not os.path.lexists(path)
                    and old.get("receipt", {}).get("state") in {"removed", "already_absent"}
                ):
                    old.update(
                        identity=None, state="allocated", generation=old.get("generation", 0) + 1
                    )
                    old.pop("receipt", None)
                    write_private(self.manifest, manifest)
                return
            if os.path.lexists(path):
                raise ValueError("cannot adopt an existing unregistered resource")
            manifest["roots"][str(path)] = {"kind": kind, "state": "allocated", "identity": None}
            if kind == 'generated' and path.name == '.venv':
                from .delivery_configured_resources import python_environment_authority

                configured = next((python_environment_authority(
                    self.spec, path.relative_to(Path(root)).as_posix())
                    for root, entry in manifest['roots'].items()
                    if entry['kind'] in {'checkout', 'gate'} and path.is_relative_to(Path(root))),
                    None)
                binding = ('configured_environment_sha256' if configured
                           else 'accepted_plan_sha256')
                manifest['roots'][str(path)][binding] = configured or digest(
                    self.spec['accepted_plan'])
            write_private(self.manifest, manifest)

    def created(self, path: Path) -> None:
        with self.locked() as manifest:
            entry = manifest["roots"][str(path)]
            identity = _identity(path)
            if entry.get("identity") not in (None, identity):
                raise ValueError("resource identity changed after allocation")
            entry.update(identity=identity, state="created")
            entry.pop("receipt", None)
            write_private(self.manifest, manifest)

    def scratch(self, kind: str, key: str) -> Path:
        if not key or Path(key).is_absolute() or ".." in Path(key).parts:
            raise ValueError("scratch key escaped its run")
        path = self.state / "transient"
        self.register(path, "transient")
        with self.locked() as manifest:
            entry = manifest["roots"][str(path)]
            if entry["identity"] is None:
                path.mkdir(mode=0o700)
                entry.update(identity=_identity(path), state="created")
                write_private(self.manifest, manifest)
            elif _identity(path) != entry["identity"]:
                raise ValueError("transient root changed")
        result = path / kind / key
        _ancestors(path)
        private_directory(result)
        return result

    def _execution_scratch_root(self) -> Path:
        return Path("/private/tmp") / ("dftmp-" + digest(str(self.state))[:20])

    def execution_scratch(self, kind: str, key: str) -> Path:
        """Keep child IPC paths short, with the same exclusive run custody."""
        for value in (kind, key):
            if not value or Path(value).is_absolute() or ".." in Path(value).parts:
                raise ValueError("execution scratch key escaped its run")
        path = self._execution_scratch_root()
        self.register(path, "execution-scratch")
        with self.locked() as manifest:
            entry = manifest["roots"][str(path)]
            if entry["identity"] is None:
                path.mkdir(mode=0o700)
                entry.update(identity=_identity(path), state="created")
                write_private(self.manifest, manifest)
            elif _identity(path) != entry["identity"]:
                raise ValueError("execution scratch root changed")
            result = path / digest([kind, key])[:16]
            private_directory(result)
        return result

    def browser_scratch(self) -> Path:
        path = Path("/private/tmp") / ("dfqa-" + digest(str(self.state))[:20])
        self.register(path, "browser-scratch")
        if not path.exists():
            path.mkdir(mode=0o700)
            self.created(path)
        return path

    def process(self, journal: Path) -> None:
        if not journal.is_relative_to(self.state) or journal.is_symlink():
            raise ValueError("process journal left its run")
        with self.locked() as manifest:
            if str(journal) not in manifest["processes"]:
                manifest["processes"].append(str(journal))
                write_private(self.manifest, manifest)

    def finalize(self, outcome: str, *, uncertain: bool = False) -> dict[str, Any]:
        from .delivery_native_process import reconcile_process

        with self.locked() as manifest:
            process_receipts = [reconcile_process(Path(path)) for path in manifest["processes"]]
            uncertain = uncertain or any(
                item["cleanup"] != "observed-native-confirmed" for item in process_receipts
            )
            manifest["finalization"] = {"outcome": outcome, "state": "running"}
            write_private(self.manifest, manifest)
            receipts = []
            for raw_path, entry in sorted(
                manifest["roots"].items(), key=lambda item: -len(Path(item[0]).parts)
            ):
                path = Path(raw_path)
                receipt = {"path": raw_path, "kind": entry["kind"]}
                try:
                    self._allowed(path, entry["kind"], finalizing=True)
                    if not os.path.lexists(path):
                        receipt.update(state="already_absent")
                        if entry.get("clean_base_removal"):
                            receipt["clean_base_removal"] = entry["clean_base_removal"]
                    elif uncertain:
                        receipt.update(
                            state="retained",
                            reason="native process monitoring or teardown is unknown",
                        )
                    elif entry["identity"] is None or _identity(path) != entry["identity"]:
                        raise ValueError("resource creation or replacement has no proven ownership")
                    elif entry["kind"] in {"checkout", "gate"}:
                        reason = self._retain_source(path, entry["kind"], outcome)
                        if reason:
                            receipt.update(state="retained", reason=reason)
                        else:
                            if entry["kind"] == "checkout" and outcome == "cancelled":
                                from .candidate import candidate_for

                                candidate = candidate_for(path)
                                if candidate["head"] != self.spec["base_sha"]:
                                    raise ValueError("cancelled removable source is not the base")
                                # Persist before removal, so a lost completion can
                                # authenticate the absent clean source on retry.
                                proof = {"candidate": candidate, "base_sha": self.spec["base_sha"],
                                         "outcome": outcome}
                                entry["clean_base_removal"] = proof
                                receipt["clean_base_removal"] = proof
                            entry["state"] = "removing"
                            write_private(self.manifest, manifest)
                            subprocess.run(
                                [
                                    "git",
                                    "-C",
                                    self.spec["source_path"],
                                    "worktree",
                                    "remove",
                                    str(path),
                                ],
                                check=True,
                                capture_output=True,
                                timeout=30,
                            )
                            if os.path.lexists(path):
                                raise ValueError("Git worktree remains after removal")
                            receipt.update(state="removed")
                    else:
                        entry["state"] = "removing"
                        write_private(self.manifest, manifest)
                        remove_directory(path, entry["identity"])
                        receipt.update(state="removed")
                except (OSError, ValueError, subprocess.SubprocessError) as exc:
                    receipt.update(
                        state="failed_unknown",
                        reason=f"{type(exc).__name__}: {str(exc)[:300]}",
                        retryable=isinstance(exc, (OSError, subprocess.TimeoutExpired)),
                    )
                entry["receipt"] = receipt
                write_private(self.manifest, manifest)
                receipts.append(receipt)
            failed = uncertain or any(item["state"] == "failed_unknown" for item in receipts)
            result = {
                "state": "unknown" if failed else "confirmed",
                "outcome": outcome,
                "process_cleanup": "unknown" if uncertain else "observed-native-confirmed",
                "resource_cleanup": "unknown" if failed else "confirmed",
                "roots": receipts,
                "processes": process_receipts,
                "sessions_and_durable_evidence": "retained",
                "retryable": any(item.get("retryable") for item in receipts),
            }
            manifest["finalization"] = result
            write_private(self.manifest, manifest)
            receipt_path = self.root / "finalization.json"
            write_private(receipt_path, result)
            return {
                **result,
                "receipt": str(receipt_path),
                "receipt_sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
            }

    def _retain_source(self, path: Path, kind: str, outcome: str) -> str | None:
        if kind == "gate" and not path.is_relative_to(_gate_evidence_root(self.spec) / "gates"):
            return "historical gate checkout and candidate evidence are preserved"

        def git(*args):
            return subprocess.check_output(
                ["git", "--no-optional-locks", "-C", str(path), *args], text=True, timeout=30
            ).strip()

        if git("rev-parse", "--show-toplevel") != str(path):
            raise ValueError("registered worktree root changed")
        if git("status", "--porcelain", "--untracked-files=all"):
            return "dirty or untracked candidate source is preserved"
        if git("ls-files", "--others", "--ignored", "--exclude-standard"):
            return "ignored worktree data requires explicit preservation"
        if Path("/usr/sbin/lsof").is_file():
            observed = subprocess.run(
                ["/usr/sbin/lsof", "-nP", "-F", "p", "+D", str(path)],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if observed.returncode not in (0, 1):
                raise ValueError("candidate process/open-file inspection is unavailable")
            # macOS lsof can return 1 while reporting valid matches. Inspect
            # its machine-readable identities, not just its exit status.
            if any(
                line.startswith("p") and line[1:].isdecimal()
                for line in observed.stdout.splitlines()
            ):
                return "a live process still uses the candidate worktree"
        if kind == "gate":
            return None
        if outcome == "blocked":
            return "blocked-run candidate and recovery source are preserved"
        head = git("rev-parse", "HEAD")
        if head == self.spec["base_sha"]:
            return None
        remote = subprocess.check_output(
            [
                "git",
                "-C",
                self.spec["source_path"],
                "ls-remote",
                "origin",
                "refs/heads/" + self.spec["branch"],
            ],
            text=True,
            timeout=30,
        )
        if not remote.strip() or remote.split()[0] != head:
            return "unpushed or unconfirmed candidate source is preserved"
        if outcome != "delivered":
            return "published source remains available for recovery"
        return None


def observe_finalized_resources(spec, *, unknown_allowed=False):
    """Fresh read-only actor, port, lease and inode observation, never teardown."""
    return _observe_resources(spec, unknown_allowed=unknown_allowed)


def observe_completed_resources(spec, attempts):
    """Authenticate later blocked-run completion without changing historical finalization."""
    return _observe_resources(spec, freshly_completed=True, attempts=attempts)


def _observe_resources(spec, *, unknown_allowed=False, freshly_completed=False, attempts=()):
    state = Path(spec["state_dir"])
    _ancestors(state / "resources/manifest.json")
    manifest_path, final_path = (
        state / "resources" / name for name in ("manifest.json", "finalization.json")
    )
    manifest, finalization = read_private(manifest_path), read_private(final_path)
    if (
        manifest.get("run_id") != spec["run_id"]
        or manifest.get("state_identity") != _identity(state)
        or (freshly_completed and finalization.get('outcome') != 'blocked')
        or finalization.get("state")
        not in ({"confirmed", "unknown"} if unknown_allowed or freshly_completed else {"confirmed"})
        or finalization.get("process_cleanup")
        not in (
            {"observed-native-confirmed", "unknown"}
            if unknown_allowed or freshly_completed
            else {"observed-native-confirmed"}
        )
        or (not freshly_completed and any(
            item.get("cleanup") != "observed-native-confirmed"
            for item in finalization.get("processes", [])
        ))
    ):
        raise ValueError("predecessor process/root custody is unconfirmed")
    from .delivery_native_process import listeners, process_table

    table = process_table()
    journals, roots = {}, {}
    registry = RunResources(spec, read_only=True)
    receipts = {item["journal"]: item for item in finalization.get("processes", [])}
    if set(receipts) != set(manifest["processes"]):
        raise ValueError("predecessor finalized actor inventory changed")
    attempt_journals = {
        str(state / 'attempts' / item['job_key'] / 'native-process.json'): item
        for item in attempts
    }
    if freshly_completed and not set(attempt_journals).issubset(manifest['processes']):
        raise ValueError('original completed attempt is not registered')
    for raw in manifest["processes"]:
        path = Path(raw)
        if not path.is_relative_to(state) or path.resolve(strict=True) != path:
            raise ValueError("predecessor journal escaped its original root")
        _ancestors(path)
        value = read_private(path)
        receipt = receipts[raw]
        if not freshly_completed and (
            receipt.get("monitoring_complete") is not True
            or receipt.get("owned_ports_clear") is not True
            or sorted(receipt.get("observed_pids", [])) != sorted(map(int, value.get("owned", {})))
        ):
            raise ValueError(
                "predecessor actor identities lost their finalized inventory"
            )
        if freshly_completed:
            launch = read_private(path.with_name('launch.json'))
            intent = value['intent']
            if (intent['run_id'] != spec['run_id']
                    or intent['policy_digest'] != spec['policy_digest']
                    or intent['argv'] != launch['argv']
                    or intent['environment_sha256'] != digest(launch['environment'])
                    or intent['ports'] != value.get('ports')
                    or not value.get('owned')):
                raise ValueError('original native launch binding changed')
            attempt = attempt_journals.get(raw)
            if attempt is not None:
                request = read_private(path.with_name('request.json'))
                result = json.loads(attempt['result_json'])
                metadata = value.get('provider_session', {})
                identity = {'run_id': spec['run_id'], 'role': request['role'],
                            'iteration': request['iteration'],
                            'candidate_id': request['candidate']['id'],
                            'policy_digest': spec['policy_digest']}
                if request.get('attempt_generation'):
                    identity['attempt_generation'] = request['attempt_generation']
                monitor = value.get('monitor', {})
                if (attempt['state'] != 'finished' or attempt['cleanup'] != 'confirmed'
                        or request['spec'] != spec or digest(identity) != attempt['job_key']
                        or request['role'] != attempt['role']
                        or request['iteration'] != attempt['iteration']
                        or request['candidate']['id'] != attempt['candidate_id']
                        or request.get('result_path') != str(path.with_name('result.json'))
                        or request.get('start_path') != str(path.with_name('start.json'))
                        or intent['cwd'] != request['workspace']
                        or type(intent['timeout']) is not int or intent['timeout'] <= 0
                        or intent['timeout'] > spec['policy']['roles'][request['role']].get(
                            'timeout_seconds', 7200)
                        or intent['ports'] != [] or not monitor
                        or metadata.get('result_digest') != digest(result)
                        or metadata.get('session_id') != attempt['session_id']
                        or metadata.get('role') != attempt['role']
                        or metadata.get('iteration') != attempt['iteration']
                        or str(attempt['pid']) not in value['owned']
                        or value['owned'][str(attempt['pid'])]['identity']
                        != attempt['process_identity']
                        or value.get('result', {}).get('cleanup') != 'observed-native-confirmed'
                        or (table.get(monitor['pid'], {}).get('identity') == monitor['identity']
                            and not table[monitor['pid']]['stat'].startswith('Z'))):
                    raise ValueError('original completed provider binding is unconfirmed')
            monitor = value.get('monitor')
            if (monitor and table.get(monitor['pid'], {}).get('identity') == monitor['identity']
                    and not table[monitor['pid']]['stat'].startswith('Z')):
                raise ValueError('registered native monitor remains active')
        lock_path = path.with_name("native-process.lock")
        descriptor = os.open(lock_path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("predecessor still owns a native launch lease") from exc
        finally:
            os.close(descriptor)
        if (
            value.get("phase") != "finished"
            or value.get("monitoring_complete") is not True
            or any(
                table.get(int(pid), {}).get("identity") == actor["identity"]
                and not table[int(pid)]["stat"].startswith("Z")
                for pid, actor in value.get("owned", {}).items()
            )
            or any(listeners(port) for port in value.get("ports", []))
        ):
            raise ValueError("predecessor still has a live actor or port")
        journals[raw] = hashlib.sha256(path.read_bytes()).hexdigest()
    for raw, entry in manifest["roots"].items():
        path = Path(raw)
        registry._allowed(path, entry["kind"], finalizing=True)
        _ancestors(path, allow_missing=True)
        if os.path.lexists(path):
            if entry["identity"] != _identity(path):
                raise ValueError("predecessor root identity changed")
            if entry.get("receipt", {}).get("state") in {"removed", "already_absent"}:
                raise ValueError("predecessor finalized root was recreated")
            observed = subprocess.run(
                ["/usr/sbin/lsof", "-nP", "-F", "p", "+D", str(path)],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if observed.returncode not in (0, 1) or any(
                line.startswith("p") and line[1:].isdecimal()
                for line in observed.stdout.splitlines()
            ):
                raise ValueError("predecessor has a live root user or unreadable lease")
            roots[raw] = entry["identity"]
        else:
            roots[raw] = None
    return {
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "finalization_sha256": hashlib.sha256(final_path.read_bytes()).hexdigest(),
        "journal_sha256": journals,
        "roots": roots,
    }
