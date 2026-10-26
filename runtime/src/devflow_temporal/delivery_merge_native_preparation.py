"""Native execution and absent-provider custody owned by an existing merge grant."""

from __future__ import annotations

import json
import os
import shutil
import stat
from contextlib import ExitStack, contextmanager
from pathlib import Path

from .candidate import candidate_for
from .contracts import digest
from .delivery_resources import _ancestors, read_private


def _directory_stamp(info):
    return (info.st_dev, info.st_ino, info.st_uid, info.st_mode, info.st_mtime_ns, info.st_ctime_ns)


class _PrelaunchProofs(list):
    """Observation leases survive until the public admission transaction commits."""

    def __init__(self):
        super().__init__()
        self._checks = []
        self._scratch = []

    def recheck(self):
        for check in self._checks:
            check()

    def settle_scratch(self):
        self.recheck()
        for path in self._scratch:
            if os.path.lexists(path):
                info = path.lstat()
                if (
                    not stat.S_ISDIR(info.st_mode)
                    or stat.S_IMODE(info.st_mode) != 0o700
                    or info.st_uid != os.getuid()
                    or path.resolve() != path
                ):
                    raise ValueError("merge prelaunch scratch has no proven private ownership")
                shutil.rmtree(path)
        self.recheck()


@contextmanager
def prelaunch_receipts(store, spec, saved):
    """Observe only until explicit admission settles the original setup intent.

    A missing request is supported only for a typed, never-capacity-admitted
    supervisor result and an exactly empty owned folder. It is never recreated.
    """
    from .delivery_merge_resolution import scratch_path
    from .supervisor import DeliverySupervisor

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
    proofs = _PrelaunchProofs()
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
            lock = DeliverySupervisor._owner_lock(spec, key)
            if lock is None:
                raise ValueError("merge prelaunch owner is still active")
            leases.callback(os.close, lock)
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
            request = (
                {
                    "spec": spec,
                    "role": attempt["role"],
                    "iteration": attempt["iteration"],
                    "candidate": bound["candidate"],
                    "workspace": bound["workspace"],
                    "merge_context": bound["merge_context"],
                }
                if missing_request and attempt
                else json.loads(request_raw)
                if request_raw is not None
                else {}
            )
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
                or request.get("spec") != spec
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
            if missing_request and os.path.lexists(scratch):
                raise ValueError("empty prelaunch retains an unbound scratch generation")
            if not missing_request:
                proofs._scratch.append(scratch)

            stamp = _directory_stamp(info)

            def unchanged(
                folder=folder,
                descriptor=descriptor,
                stamp=stamp,
                names=names,
                request_raw=request_raw,
                bound=bound,
                missing_request=missing_request,
                scratch=scratch,
            ):
                if (
                    _directory_stamp(os.fstat(descriptor)) != stamp
                    or _directory_stamp(folder.lstat()) != stamp
                    or {p.name for p in folder.iterdir()} != names
                    or (
                        request_raw is not None
                        and read_private(folder / "request.json", raw=True) != request_raw
                    )
                    or (missing_request and os.path.lexists(scratch))
                    or candidate_for(Path(bound["workspace"])) != bound["candidate"]
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
