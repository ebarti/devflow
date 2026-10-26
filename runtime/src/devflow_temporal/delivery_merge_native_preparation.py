"""Native execution and absent-provider custody owned by an existing merge grant."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
from contextlib import ExitStack, contextmanager
from copy import deepcopy
from importlib.metadata import distribution
from pathlib import Path

from .candidate import candidate_for
from .contracts import digest
from .delivery_resources import _ancestors, read_private

SCHEMA = "devflow.merge-native-execution/v1"
FIELD = "merge_native_preparation"


def _path(original, request):
    context = request["merge_context"]
    if not isinstance(context.get("resolution_id"), str) or not re.fullmatch(
        r"[0-9a-f]{64}", context["resolution_id"]
    ):
        raise ValueError("merge native preparation has no exact resolution identity")
    root = Path(original["state_dir"]) / "merge-resolutions" / context["resolution_id"]
    if (
        Path(request["workspace"]) != root / "checkout"
        or context["effect_key"].split(":")[0] != "merge-resolution"
    ):
        raise ValueError("merge native preparation left its exact conflict workspace")
    _ancestors(root / "preparation.json")
    prepared = read_private(root / "preparation.json")
    if (
        context != {**prepared, "effect_key": context["effect_key"]}
        or prepared.get("evidence_path") != str(root / "parent-versions.json")
        or prepared.get("baseline_path") != str(root / "baseline.json")
    ):
        raise ValueError("merge native context differs from its immutable conflict preparation")
    return root / "native-executions" / digest(context["effect_key"]) / "execution.json"


def _artifacts(original, request):
    _path(original, request)
    return {
        key: hashlib.sha256(read_private(Path(request["merge_context"][key]), raw=True)).hexdigest()
        for key in ("evidence_path", "baseline_path")
    }


def _binding(original, request, authorization, workflow):
    from .delivery_resources import _identity
    from .delivery_merge_execution import FIELD as EXECUTION

    _ancestors(Path(request["workspace"]) / "unused")
    return {
        "schema": SCHEMA,
        "grant_sha256": authorization,
        "workflow_id": workflow,
        EXECUTION: request.get(EXECUTION),
        "original_spec_sha256": digest(original),
        "role": request["role"],
        "iteration": request["iteration"],
        "candidate": request["candidate"],
        "workspace": request["workspace"],
        "merge_context": request["merge_context"],
        "workspace_identity": _identity(Path(request["workspace"])),
        "conflict_artifacts_sha256": _artifacts(original, request),
    }


def _same_authority(original, execution):
    before, after = deepcopy(original), deepcopy(execution)
    for value in (before, after):
        value.pop("preparation", None)
        value.pop("policy_digest", None)
        for name in (
            "codex_bin",
            "native_identity",
            "environment_proof_sha256",
            "security_binding_sha256",
        ):
            value["policy"].pop(name, None)
    if before != after or digest(execution["policy"]) != execution["policy_digest"]:
        raise ValueError("merge native execution changed original non-preparation authority")


def prepare(store, original, request, authorization):
    """Only _role derives a fresh execution; standalone files grant nothing."""
    from .delivery_gate_retry import _immutable, prepare_runtime
    from .delivery_merge_followup import authorize, current
    from .delivery_preparation import verify_prepared_spec
    from .delivery_runtime_location import require_preparation_location

    if original.get("provider") == "fake":
        return original, None
    binary = Path(distribution("openai-codex-cli-bin").locate_file("codex_cli_bin/bin/codex"))
    if str(binary.resolve()) == original["policy"]["codex_bin"]:
        verify_prepared_spec(original)
        return original, None
    # This is the existing acknowledged location change, not a generic failed
    # proof renewal or an arbitrary equal-hash executable supplied by a caller.
    require_preparation_location(original, binary.resolve())
    authorize(store, original, authorization)
    with store._connect() as db:
        saved = current(db, original["run_id"])
    binding = _binding(original, request, authorization, saved["workflow_id"])
    if candidate_for(Path(request["workspace"])) != request["candidate"]:
        raise ValueError("merge workspace changed before native preparation")
    path = _path(original, request)
    execution = prepare_runtime(original, path.parent, digest(binding), digest(binding))
    if os.path.lexists(path):
        record = read_private(path)
        if record.get("binding") != binding or record.get("execution_spec") != execution:
            raise ValueError("merge native generation already belongs to different authority")
    else:
        _same_authority(original, execution)
        verify_prepared_spec(execution)
        authorize(store, original, authorization)
        if (
            _binding(original, request, authorization, saved["workflow_id"]) != binding
            or candidate_for(Path(request["workspace"])) != request["candidate"]
        ):
            raise ValueError("merge authority or workspace changed during native preparation")
        _immutable(path, {"binding": binding, "execution_spec": execution})
    _same_authority(original, execution)
    verify_prepared_spec(execution)
    authorize(store, original, authorization)
    with store._connect() as db:
        latest = current(db, original["run_id"])
    if (
        latest != saved
        or _binding(original, request, authorization, saved["workflow_id"]) != binding
        or candidate_for(Path(request["workspace"])) != request["candidate"]
    ):
        raise ValueError("merge preparation lost its current workflow or source generation")
    reference = {
        "path": str(path),
        "sha256": hashlib.sha256(read_private(path, raw=True)).hexdigest(),
    }
    return execution, reference


def recorded(original, request, authorization, reference):
    """Immutable metadata only; never claim that this historical value may launch."""
    path = _path(original, request)
    if not isinstance(reference, dict) or set(reference) != {"path", "sha256"}:
        raise ValueError("merge native reference is malformed")
    raw = read_private(path, raw=True)
    if reference["path"] != str(path) or hashlib.sha256(raw).hexdigest() != reference["sha256"]:
        raise ValueError("merge native preparation receipt changed")
    record = json.loads(raw)
    binding, execution = record["binding"], record["execution_spec"]
    prepared = read_private(path.parent / "runtime-preparation/intent.json")
    if (
        binding.get("schema") != SCHEMA
        or binding.get("grant_sha256") != authorization
        or binding.get("original_spec_sha256") != digest(original)
        or any(
            binding.get(key) != request.get(key)
            for key in ("role", "iteration", "candidate", "workspace", "merge_context")
        )
        or prepared.get("state") != "prepared"
        or prepared.get("command_digest") != digest(binding)
        or prepared.get("seal_digest") != digest(binding)
        or prepared.get("effective_spec") != execution
        or binding.get("conflict_artifacts_sha256") != _artifacts(original, request)
    ):
        raise ValueError("merge native receipt lost its original grant or request")
    _same_authority(original, execution)
    return execution, binding


def authenticate(store, original, request, bound, effect):
    """Current launch/reattachment authority; original grant remains separate."""
    from .delivery_merge_followup import authorize, current
    from .delivery_preparation import verify_prepared_spec
    from .delivery_resources import _identity
    from .delivery_merge_execution import FIELD as EXECUTION

    if request.get(EXECUTION) != bound.get(EXECUTION):
        raise ValueError("merge native execution changed its activity generation")
    if effect["state"] != "complete" and EXECUTION in bound:
        with store._connect() as db:
            saved = current(db, original["run_id"])
        if not saved or bound[EXECUTION]["workflow_id"] != saved["workflow_id"]:
            raise ValueError("merge native launch belongs to an obsolete execution")

    reference = bound.get(FIELD)
    if request.get(FIELD) != reference:
        raise ValueError("merge role changed its native preparation reference")
    if reference is None:
        if request["spec"] != original:
            raise ValueError("merge role has no derived native execution authority")
        return
    execution, binding = recorded(original, request, bound["merge_authorization"], reference)
    if execution != request["spec"] or bound.get("execution_spec_sha256") != digest(execution):
        raise ValueError("merge role changed its exact prepared execution")
    with store._connect() as db:
        saved = current(db, original["run_id"])
        attempt = db.execute(
            "SELECT state FROM delivery_attempts WHERE job_key=? AND run_id=?",
            (bound["job_key"], original["run_id"]),
        ).fetchone()
    if (
        not saved
        or (effect["state"] != "complete" and saved["workflow_id"] != binding["workflow_id"])
        or _identity(Path(request["workspace"])) != binding["workspace_identity"]
    ):
        raise ValueError("merge native execution lost its current workspace or workflow")
    if effect["state"] == "complete" and (not attempt or attempt["state"] != "finished"):
        raise ValueError("completed merge effect has no finished original native attempt")
    if effect["state"] == "complete":
        observed = json.loads(effect["observed_json"] or "null")
        if not isinstance(observed, dict) or candidate_for(
            Path(request["workspace"])
        ) != observed.get("candidate"):
            raise ValueError("completed merge role lost its observed output candidate")
    if not attempt or attempt["state"] in {"queued", "starting"}:
        if candidate_for(Path(request["workspace"])) != request["candidate"]:
            raise ValueError("merge workspace changed before provider launch")
    authorize(store, original, bound["merge_authorization"])
    if not attempt or attempt["state"] != "finished":
        verify_prepared_spec(execution)


def _directory_stamp(info):
    return (info.st_dev, info.st_ino, info.st_uid, info.st_mode, info.st_mtime_ns, info.st_ctime_ns)


class _PrelaunchProofs(list):
    """Observation leases survive until the public admission transaction commits."""

    def __init__(self, saved):
        super().__init__()
        self._checks = []
        self._saved = saved

    def unsettled(self, db, run_id):
        from .delivery_merge_followup import current

        self.recheck()
        if current(db, run_id) != self._saved:
            raise ValueError("merge prelaunch owner changed during observation")
        effects = [
            dict(e)
            for e in db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=? AND state!='complete'", (run_id,)
            )
        ]
        expected = {r["effect_key"]: r for r in self}
        for effect in effects:
            proof = expected.get(effect["effect_key"])
            attempt = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=? AND job_key=?",
                (run_id, proof["job_key"] if proof else ""),
            ).fetchone()
            if (
                not proof
                or effect["request_json"] != proof["request_json"]
                or not attempt
                or digest(dict(attempt)) != proof["attempt_sha256"]
            ):
                raise ValueError("merge prelaunch SQL custody changed during observation")
        if {e["effect_key"] for e in effects} != set(expected):
            raise ValueError("merge prelaunch effect inventory changed during observation")
        return []

    def recheck(self):
        for check in self._checks:
            check()


@contextmanager
def prelaunch_receipts(store, spec, saved):
    """Observe only until explicit admission settles the original setup intent.

    A missing request is supported only for a typed, never-capacity-admitted
    supervisor result and an exactly empty owned folder. It is never recreated.
    """
    from .delivery_merge_resolution import scratch_path
    from .supervisor import DeliverySupervisor
    from .delivery_merge_execution import FIELD as EXECUTION

    with store._connect() as db:
        effects = [
            dict(e)
            for e in db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=? AND state!='complete'",
                (spec["run_id"],),
            )
        ]
    if not effects:
        raise ValueError("merge continuation has no confirmed prelaunch failure")
    proofs = _PrelaunchProofs(saved)
    with ExitStack() as leases:
        for effect in effects:
            bound = json.loads(effect["request_json"])
            if (
                effect["kind"] != "merge_resolution"
                or bound.get("merge_authorization") != digest(json.loads(saved["grant_json"]))
                or bound.get("spec_sha256") != digest(spec)
                or bound.get("merge_context", {}).get("effect_key") != effect["effect_key"]
            ):
                raise ValueError("merge continuation retains an unresolved external effect")
            key = bound["job_key"]
            owner = DeliverySupervisor._owner_lock_path(spec, key)
            try:
                _ancestors(owner)
                lock = os.open(owner, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            except OSError as exc:
                raise ValueError("merge prelaunch original owner lease is unavailable") from exc
            leases.callback(os.close, lock)
            info = os.fstat(lock)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_nlink != 1
                or (info.st_dev, info.st_ino) != (owner.lstat().st_dev, owner.lstat().st_ino)
            ):
                raise ValueError("merge prelaunch original owner lock changed")
            owner_stamp = _directory_stamp(info)
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ValueError("merge prelaunch owner is still active") from exc
            with store._connect() as db:
                attempt = db.execute(
                    "SELECT * FROM delivery_attempts WHERE job_key=? AND run_id=?",
                    (key, spec["run_id"]),
                ).fetchone()
            attempt = dict(attempt) if attempt else None
            result = json.loads(attempt["result_json"] or "null") if attempt else None
            folder = Path(spec["state_dir"]) / "attempts" / key
            _ancestors(folder)
            descriptor = os.open(folder, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            leases.callback(os.close, descriptor)
            info = os.fstat(descriptor)
            if (
                not stat.S_ISDIR(info.st_mode)
                or stat.S_IMODE(info.st_mode) != 0o700
                or info.st_uid != os.getuid()
                or folder.resolve() != folder
                or _directory_stamp(info) != _directory_stamp(folder.lstat())
            ):
                raise ValueError("merge prelaunch provider absence is unproven")
            names = {p.name for p in folder.iterdir()}
            missing_request = "request.json" not in names
            request_raw = (
                None if missing_request else read_private(folder / "request.json", raw=True)
            )
            expected = {
                "spec": spec,
                "role": attempt["role"] if attempt else None,
                "iteration": attempt["iteration"] if attempt else None,
                "candidate": bound["candidate"],
                "workspace": bound["workspace"],
                "merge_context": bound["merge_context"],
            }
            if bound.get("review_diff"):
                expected["review_diff"] = bound["review_diff"]
            if EXECUTION in bound:
                if bound[EXECUTION]["workflow_id"] != saved["workflow_id"]:
                    raise ValueError("merge prelaunch belongs to an obsolete execution")
                expected[EXECUTION] = bound[EXECUTION]
            native_binding = None
            if FIELD in bound:
                execution, native_binding = recorded(
                    spec, expected, bound["merge_authorization"], bound[FIELD]
                )
                if (
                    bound.get("execution_spec_sha256") != digest(execution)
                    or native_binding["workflow_id"] != saved["workflow_id"]
                ):
                    raise ValueError("merge prelaunch lost its exact derived execution")
                expected["spec"] = execution
                expected[FIELD] = bound[FIELD]
                if (
                    _binding(spec, expected, bound["merge_authorization"], saved["workflow_id"])
                    != native_binding
                ):
                    raise ValueError("merge prelaunch derived workspace authority changed")
            request = expected if missing_request else json.loads(request_raw)
            if (
                not attempt
                or attempt["state"] != "finished"
                or attempt["cleanup"] != "confirmed"
                or any(attempt[k] is not None for k in ("pid", "process_identity", "session_id"))
                or not isinstance(result, dict)
                or result.get("status") != "blocked"
                or result.get("cleanup") != "confirmed"
                or result.get("finish_reason") != "prelaunch"
                or result.get("session_id") is not None
                or attempt["role"] not in {"implement", "review"}
                or attempt["iteration"] != 0
                or attempt["result_path"] != str(folder / "result.json")
                or (
                    missing_request
                    and (names or attempt["started_at"] is not None or not attempt["finished_at"])
                )
                or (not missing_request and names - {"request.json", "native-environment.json"})
                or request.get("spec") != expected["spec"]
                or request.get(FIELD) != expected.get(FIELD)
                or request.get(EXECUTION) != expected.get(EXECUTION)
                or request.get("review_diff") != expected.get("review_diff")
                or request.get("role") != attempt["role"]
                or request.get("iteration") != attempt["iteration"]
                or request.get("candidate") != bound["candidate"]
                or attempt["candidate_id"] != bound["candidate"]["id"]
                or request.get("workspace") != bound["workspace"]
                or request.get("merge_context") != bound["merge_context"]
                or request.get("resume_session") is not None
                or DeliverySupervisor._job_key(request) != key
                or candidate_for(Path(bound["workspace"])) != bound["candidate"]
            ):
                raise ValueError("merge prelaunch provider absence is unproven")
            scratch = scratch_path(request)
            if os.path.lexists(scratch):
                raise ValueError("merge prelaunch retains an unbound scratch generation")

            stamp = _directory_stamp(info)

            def unchanged(
                folder=folder,
                descriptor=descriptor,
                stamp=stamp,
                names=names,
                request_raw=request_raw,
                bound=bound,
                expected=expected,
                native_binding=native_binding,
                scratch=scratch,
                owner=owner,
                owner_descriptor=lock,
                owner_stamp=owner_stamp,
            ):
                if (
                    _directory_stamp(os.fstat(owner_descriptor)) != owner_stamp
                    or _directory_stamp(owner.lstat()) != owner_stamp
                    or os.fstat(owner_descriptor).st_nlink != 1
                    or _directory_stamp(os.fstat(descriptor)) != stamp
                    or _directory_stamp(folder.lstat()) != stamp
                    or {p.name for p in folder.iterdir()} != names
                    or (
                        request_raw is not None
                        and read_private(folder / "request.json", raw=True) != request_raw
                    )
                    or os.path.lexists(scratch)
                    or candidate_for(Path(bound["workspace"])) != bound["candidate"]
                    or (
                        native_binding is not None
                        and _binding(
                            spec, expected, bound["merge_authorization"], saved["workflow_id"]
                        )
                        != native_binding
                    )
                ):
                    raise ValueError("merge prelaunch custody changed during admission")

            proofs._checks.append(unchanged)
            proofs.append(
                {
                    "effect_key": effect["effect_key"],
                    "request_json": effect["request_json"],
                    "job_key": key,
                    "attempt_sha256": digest(attempt),
                    "assessment": result,
                    "request_source": "immutable_merge_effect" if missing_request else "request",
                    "cleanup": "confirmed_prelaunch_absent",
                }
            )
        proofs.recheck()
        yield proofs
