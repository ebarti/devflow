"""Single-host role admission and crash-conservative child supervision."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

from .contracts import canonical_json
from .delivery_container import Bind, ContainerUnknown, OwnedContainer
from .delivery_sandbox import _native_env, prepare_native_role, prepare_sandbox
from .delivery_store import DeliveryStore, _now


def _process_identity(pid: int) -> str | None:
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart="], capture_output=True, text=True, check=False
    )
    value = result.stdout.strip()
    return value if result.returncode == 0 and value else None


def _private_json(path: Path, value: dict[str, Any]) -> None:
    content = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    if path.exists() or path.is_symlink():
        if path.is_symlink() or path.read_bytes() != content:
            raise ContainerUnknown("role request changed across a durable attempt")
        return
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(content)


def _contained_role_spec(spec: dict[str, Any], role_policy: dict[str, Any]) -> dict[str, Any]:
    """Keep the frozen request context available after container path translation."""

    return {
        "run_id": spec["run_id"],
        "work_id": spec["work_id"],
        "issue_url": spec["issue_url"],
        "goal": spec["goal"],
        "accepted_plan": spec["accepted_plan"],
        "intake_required": spec.get("intake_required", False),
        "provider": "codex",
        "state_dir": "/attempt",
        "policy": role_policy,
    }


class DeliverySupervisor:
    def __init__(self, store: DeliveryStore, *, capacity: int) -> None:
        self.store = store
        self.capacity = capacity
        self.job_locks: dict[str, asyncio.Lock] = {}

    async def _acquire_capacity(self, job_key: str, *, cancelled=None) -> None:
        """Atomically claim one shared DB slot across config overlays and workers."""

        while True:
            if cancelled is not None and cancelled():
                raise ValueError("native role cancelled before capacity admission")
            with self.store._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute(
                    "SELECT state FROM delivery_attempts WHERE job_key=?", (job_key,)
                ).fetchone()
                if row is None or row["state"] != "queued":
                    raise ContainerUnknown("role attempt changed while waiting for capacity")
                occupied = db.execute(
                    """SELECT COUNT(*) FROM delivery_attempts
                       WHERE state IN ('starting','running','unknown')"""
                ).fetchone()[0]
                if occupied < self.capacity:
                    updated = db.execute(
                        """UPDATE delivery_attempts SET state='starting',started_at=?
                           WHERE job_key=? AND state='queued'""",
                        (_now(), job_key),
                    ).rowcount
                    if updated != 1:
                        raise ContainerUnknown("role capacity claim changed before launch")
                    return
            # Ambiguous attempts retain their slots until reconciled.
            await asyncio.sleep(1)

    def _claim(self, request: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
        spec = request["spec"]
        if spec["policy"].get("execution_backend") == "native-macos":
            from .delivery_native_guard import validate_native_turn

            validate_native_turn(spec, request["role"], request["iteration"], self.store)
        generation = request.get("attempt_generation", 0)
        if type(generation) is not int or generation not in (0, 1) or (
            generation and request["role"] != "implement"
        ):
            raise ValueError("unsupported role attempt recovery generation")
        identity = {
            "run_id": spec["run_id"],
            "role": request["role"],
            "iteration": request["iteration"],
            "candidate_id": request["candidate"]["id"],
            "policy_digest": spec["policy_digest"],
        }
        if generation:
            identity["attempt_generation"] = generation
        job_key = hashlib.sha256(canonical_json(identity).encode()).hexdigest()
        folder = Path(spec["state_dir"]) / "attempts" / job_key
        folder.mkdir(parents=True, mode=0o700, exist_ok=True)
        result_path = folder / "result.json"
        with self.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM delivery_attempts WHERE job_key=?", (job_key,)
            ).fetchone()
            if row:
                if row["candidate_id"] != identity["candidate_id"]:
                    raise ValueError("attempt key collision")
                if row["state"] == "finished":
                    return job_key, json.loads(row["result_json"])
                if spec.get("provider") != "codex" and result_path.is_file():
                    result = json.loads(result_path.read_text(encoding="utf-8"))
                    db.execute(
                        """UPDATE delivery_attempts SET state='finished',result_json=?,
                           session_id=?,finished_at=?,cleanup='confirmed' WHERE job_key=?""",
                        (
                            canonical_json(result),
                            result.get("session_id"),
                            _now(),
                            job_key,
                        ),
                    )
                    return job_key, result
                if row["state"] in {"starting", "running", "unknown"}:
                    if spec.get("provider") == "codex":
                        return job_key, None
                    return job_key, {
                        "status": "recovery_unknown",
                        "summary": "prior role process has no durable final result",
                        "findings": [
                            "role process outcome is ambiguous; "
                            "no duplicate invocation was launched"
                        ],
                        "session_id": row["session_id"],
                        "cleanup": "unknown",
                        "usage": None,
                        "finish_reason": "recovery_unknown",
                    }
                if row["state"] == "queued":
                    return job_key, None
                raise RuntimeError("unrecognized attempt state")
            db.execute(
                """INSERT INTO delivery_attempts
                   (job_key,run_id,role,iteration,candidate_id,state,result_path)
                   VALUES (?,?,?,?,?,'queued',?)""",
                (
                    job_key,
                    spec["run_id"],
                    request["role"],
                    request["iteration"],
                    request["candidate"]["id"],
                    str(result_path),
                ),
            )
            return job_key, None

    async def run(self, request: dict[str, Any]) -> dict[str, Any]:
        job_key, existing = self._claim(request)
        if existing is not None:
            return existing
        if request["spec"].get("provider") == "codex":
            lock = self.job_locks.setdefault(job_key, asyncio.Lock())
            async with lock:
                if request["spec"]["policy"].get("execution_backend") == "native-macos":
                    return await self._run_native(request, job_key)
                return await self._run_contained(request, job_key)
        spec = request["spec"]
        folder = Path(spec["state_dir"]) / "attempts" / job_key
        result_path = folder / "result.json"
        start_path = folder / "start.json"
        request_path = folder / "request.json"
        request = {**request, "result_path": str(result_path), "start_path": str(start_path)}
        await self._acquire_capacity(job_key)
        try:
            _private_json(request_path, request)
            if spec.get("provider") == "codex":
                _, role_env = prepare_native_role(request, folder)
                child_argv = [
                    sys.executable,
                    "-I",
                    "-m",
                    "devflow_temporal.role_runner",
                    str(request_path),
                ]
            elif Path("/usr/bin/sandbox-exec").is_file():
                profile, role_env = prepare_sandbox(request, folder)
                child_argv = [
                    "/usr/bin/sandbox-exec",
                    "-f",
                    str(profile),
                    sys.executable,
                    "-I",
                    "-m",
                    "devflow_temporal.role_runner",
                    str(request_path),
                ]
            elif spec.get("provider") == "fake":
                # The fake role runs only fixed fixture code and never a
                # candidate-controlled command. Keep its protocol tests
                # executable on Linux CI, where Seatbelt is unavailable.
                role_env = _native_env(folder, folder / "unused-codex", folder)
                child_argv = [
                    sys.executable,
                    "-I",
                    "-m",
                    "devflow_temporal.role_runner",
                    str(request_path),
                ]
            else:
                raise ValueError("real role has no supported OS sandbox")
            log_descriptor = os.open(
                folder / "process.log", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600
            )
        except Exception as exc:
            return self._mark_prelaunch_blocked(job_key, type(exc).__name__)
        log = os.fdopen(log_descriptor, "wb")
        try:
            child = await asyncio.create_subprocess_exec(
                *child_argv,
                cwd=request["workspace"],
                stdin=asyncio.subprocess.PIPE,
                stdout=log,
                stderr=log,
                env=role_env,
                start_new_session=True,
            )
            for _ in range(100):
                if start_path.is_file() or child.returncode is not None:
                    break
                await asyncio.sleep(0.05)
            if not start_path.is_file():
                await child.wait()
                return self._mark_unknown(
                    job_key, "role child did not report a startup identity"
                )
            identity = _process_identity(child.pid)
            with self.store._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    """UPDATE delivery_attempts SET state='running',pid=?,process_identity=?
                       WHERE job_key=? AND state='starting'""",
                    (child.pid, identity, job_key),
                )
            assert child.stdin is not None
            child.stdin.write(b"GO\n")
            await child.stdin.drain()
            child.stdin.close()
            await child.wait()
            if not result_path.is_file():
                return self._mark_unknown(job_key, "role child exited without a final receipt")
            result = json.loads(result_path.read_text(encoding="utf-8"))
            with self.store._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    """UPDATE delivery_attempts SET state='finished',result_json=?,
                       session_id=?,finished_at=?,cleanup='confirmed' WHERE job_key=?""",
                    (canonical_json(result), result.get("session_id"), _now(), job_key),
                )
            return result
        except asyncio.CancelledError:
            if "child" in locals() and child.returncode is None:
                try:
                    os.killpg(child.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(child.wait(), timeout=5)
                except TimeoutError:
                    os.killpg(child.pid, signal.SIGKILL)
                    await child.wait()
            self._mark_unknown(job_key, "role cancelled during provider work")
            raise
        except Exception as exc:
            if "child" not in locals():
                return self._mark_prelaunch_blocked(job_key, type(exc).__name__)
            if child.returncode is None:
                try:
                    os.killpg(child.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(child.wait(), timeout=5)
                except TimeoutError:
                    os.killpg(child.pid, signal.SIGKILL)
                    await child.wait()
            self._mark_unknown(job_key, "role child failed without a final receipt")
            raise
        finally:
            log.close()

    async def _run_native(self, request: dict[str, Any], job_key: str) -> dict[str, Any]:
        from .delivery_native_process import NativeProcess
        from .delivery_preparation import verify_prepared_spec
        from .delivery_resources import read_private, write_private

        spec = request["spec"]
        folder = Path(spec["state_dir"]) / "attempts" / job_key
        result_path = folder / "result.json"
        request_path = folder / "request.json"
        native_request = {
            **request,
            "native_authorized": True,
            "result_path": str(result_path),
            "start_path": str(folder / "start.json"),
        }
        stopped = threading.Event()

        def cancelled() -> bool:
            if stopped.is_set():
                return True
            with self.store._connect() as db:
                row = db.execute(
                    "SELECT phase FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
                ).fetchone()
            return row is not None and row["phase"] == "cancelling"

        try:
            verify_prepared_spec(spec)
            with self.store._connect() as db:
                row = db.execute(
                    "SELECT state FROM delivery_attempts WHERE job_key=?", (job_key,)
                ).fetchone()
            if row["state"] == "queued":
                await self._acquire_capacity(job_key, cancelled=cancelled)
            elif not (folder / "native-process.json").is_file():
                return self._mark_unknown(job_key, "native prelaunch identity gap")
            _private_json(request_path, native_request)
            _, environment = prepare_native_role(native_request, folder)
            process = NativeProcess(
                spec,
                folder,
                argv=[
                    sys.executable,
                    "-I",
                    "-m",
                    "devflow_temporal.role_runner",
                    str(request_path),
                ],
                cwd=Path(request["workspace"]),
                environment=environment,
                timeout=int(spec["policy"]["roles"][request["role"]].get("timeout_seconds", 7200)),
                cancelled=cancelled,
            )
            pending = asyncio.create_task(asyncio.to_thread(process.run))
            try:
                outcome = await asyncio.shield(pending)
            except asyncio.CancelledError:
                stopped.set()
                outcome = await asyncio.shield(pending)
                self._mark_unknown(
                    job_key,
                    "native activity cancelled during provider work",
                    cleanup="confirmed" if outcome["cleanup"] != "unknown" else "unknown",
                )
                raise
            if outcome["cleanup"] == "unknown":
                return self._mark_unknown(job_key, "native monitoring/provider outcome ambiguous")
            if result_path.is_file():
                result = read_private(result_path)
            else:
                reason = (
                    "timeout"
                    if outcome["timed_out"]
                    else "cancelled"
                    if outcome["cancelled"]
                    else "missing_receipt"
                )
                result = {
                    "status": "blocked",
                    "summary": "native provider has no final receipt",
                    "findings": ["provider outcome unknown; this attempt will not be repeated"],
                    "session_id": None,
                    "usage": None,
                    "finish_reason": reason,
                }
            result.update(
                cleanup="confirmed",
                process_cleanup=outcome["cleanup"],
                resource_cleanup="pending_workflow_finalization",
                native_process=outcome,
            )
            journal = read_private(process.journal)
            journal["provider_session"] = {
                "session_id": result.get("session_id"),
                "resumed_from": request.get("resume_session"),
                "role": request["role"],
                "iteration": request["iteration"],
            }
            write_private(process.journal, journal)
            with self.store._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    """UPDATE delivery_attempts SET state='finished',result_json=?,
                       session_id=?,finished_at=?,cleanup='confirmed',pid=?,process_identity=?
                       WHERE job_key=?""",
                    (
                        canonical_json(result),
                        result.get("session_id"),
                        _now(),
                        next(iter(journal["owned"]), None),
                        next(iter(journal["owned"].values()), {}).get("identity"),
                        job_key,
                    ),
                )
            return result
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not (folder / "native-process.json").exists():
                return self._mark_prelaunch_blocked(job_key, str(exc)[:300])
            return self._mark_unknown(job_key, f"native launch failed: {type(exc).__name__}")

    async def _run_contained(self, request: dict[str, Any], job_key: str) -> dict[str, Any]:
        """Run one real role in a durable private PID namespace."""

        spec = request["spec"]
        folder = Path(spec["state_dir"]) / "attempts" / job_key
        result_path = folder / "result.json"
        policy = spec["policy"]["container"]
        with self.store._connect() as db:
            row = db.execute(
                "SELECT state,result_json FROM delivery_attempts WHERE job_key=?", (job_key,)
            ).fetchone()
        if row["state"] == "finished":
            return json.loads(row["result_json"])
        newly_occupied = row["state"] == "queued"
        if newly_occupied:
            await self._acquire_capacity(job_key)
        cleanup_confirmed = False
        try:
            _, role_env = prepare_native_role(request, folder, containerized=True)
            role_home = Path(spec["state_dir"]) / "role-homes" / request["role"]
            if request["role"] != "implement":
                role_home /= str(request["iteration"])
            binds = [
                Bind(Path(request["workspace"]), "/work", request["role"] in {"intake", "review"}),
                Bind(role_home, "/rolehome"),
                Bind(folder, "/attempt"),
            ]
            review_diff = request.get("review_diff")
            if review_diff:
                binds.append(Bind(Path(review_diff["path"]), "/evidence/diff.patch", True))
            qa_evidence = request.get("qa_evidence")
            if qa_evidence:
                binds.extend(
                    (
                        Bind(Path(qa_evidence["path"]), "/qa/receipt.json", True),
                        Bind(Path(qa_evidence["log"]), "/qa/browser-qa.log", True),
                    )
                )
            recovery = Path(spec["state_dir"]) / "recovery"
            if request["role"] == "implement" and recovery.is_dir():
                binds.append(Bind(recovery, "/recovery", True))
            role_policy = {
                "roles": spec["policy"]["roles"],
                "allowed_paths": spec["policy"]["allowed_paths"],
                "recovery": spec["policy"].get("recovery"),
                "browser_qa": spec["policy"].get("browser_qa"),
                "host_sandbox": "native-profile",
                "codex_bin": policy["codex_bin"],
                "codex_bin_sha256": policy["codex_bin_sha256"],
                "config_overrides": spec["policy"]["config_overrides"],
            }
            translated = {
                "spec": _contained_role_spec(spec, role_policy),
                "role": request["role"],
                "iteration": request["iteration"],
                "candidate": request["candidate"],
                "workspace": "/work",
                "result_path": "/attempt/result.json",
                "start_path": "/attempt/start.json",
                "container_authorized": True,
                "findings": request.get("findings"),
                "resume_session": request.get("resume_session"),
                "continuation": request.get("continuation"),
                "intake": request.get("intake"),
                "recovery_path": "/recovery" if recovery.is_dir() else None,
                "review_diff": {**review_diff, "path": "/evidence/diff.patch"}
                if review_diff
                else None,
                "qa_evidence": {
                    **qa_evidence,
                    "path": "/qa/receipt.json",
                    "log": "/qa/browser-qa.log",
                }
                if qa_evidence
                else None,
            }
            _private_json(folder / "request.json", translated)
            container = OwnedContainer(
                spec,
                kind="role",
                identity={
                    "role": request["role"],
                    "iteration": request["iteration"],
                    "candidate_id": request["candidate"]["id"],
                },
                evidence_dir=folder / "container",
                binds=tuple(binds),
                command=(
                    "/opt/devflow-venv/bin/python",
                    "-m",
                    "devflow_temporal.role_runner",
                    "/attempt/request.json",
                ),
                cwd="/work",
                environment=role_env,
                # Trusted provider traffic needs egress; the native profile
                # denies shell/tool networking inside this private namespace.
                network="bridge",
                timeout_seconds=int(
                    spec["policy"]["roles"][request["role"]].get("timeout_seconds", 7200)
                ),
            )
            outcome = await asyncio.to_thread(container.run)
            cleanup_confirmed = outcome.cleanup == "confirmed"
            with self.store._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    """UPDATE delivery_attempts SET state='running',process_identity=?,cleanup=?
                       WHERE job_key=? AND state IN ('starting','running','unknown')""",
                    (outcome.container_id, outcome.cleanup, job_key),
                )
            if not result_path.is_file():
                return self._mark_unknown(
                    job_key,
                    "contained role exited without a final receipt",
                    cleanup=outcome.cleanup,
                )
            result = json.loads(result_path.read_text(encoding="utf-8"))
            if outcome.exit_code != 0 and result.get("status") == "pass":
                raise ContainerUnknown("role reported pass despite a failed container exit")
            result["cleanup"] = outcome.cleanup
            result["container_id"] = outcome.container_id
            result["container_log_sha256"] = outcome.log_sha256
            with self.store._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    """UPDATE delivery_attempts SET state='finished',result_json=?,
                       session_id=?,finished_at=?,cleanup=? WHERE job_key=?""",
                    (
                        canonical_json(result),
                        result.get("session_id"),
                        _now(),
                        outcome.cleanup,
                        job_key,
                    ),
                )
            return result
        except asyncio.CancelledError:
            self._mark_unknown(job_key, "contained role was interrupted")
            raise
        except Exception as exc:
            if isinstance(exc, ContainerUnknown):
                return self._mark_unknown(
                    job_key,
                    str(exc),
                    cleanup="confirmed" if cleanup_confirmed else "unknown",
                )
            if (folder / "container" / "container-start-authorized.json").exists():
                return self._mark_unknown(
                    job_key,
                    type(exc).__name__,
                    cleanup="confirmed" if cleanup_confirmed else "unknown",
                )
            cleanup_confirmed = True
            return self._mark_prelaunch_blocked(job_key, type(exc).__name__)


    def _mark_prelaunch_blocked(self, job_key: str, reason: str) -> dict[str, Any]:
        result = {
            "status": "blocked",
            "summary": "role launch failed before any provider process started",
            "findings": [reason],
            "session_id": None,
            "cleanup": "confirmed",
            "usage": None,
            "finish_reason": "prelaunch",
        }
        with self.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                """UPDATE delivery_attempts SET state='finished',result_json=?,
                   finished_at=?,cleanup='confirmed' WHERE job_key=? AND state='starting'""",
                (canonical_json(result), _now(), job_key),
            )
        return result

    def _mark_unknown(
        self, job_key: str, reason: str, *, cleanup: str = "unknown"
    ) -> dict[str, Any]:
        result = {
            "status": "recovery_unknown",
            "summary": reason,
            "findings": [reason],
            "session_id": None,
            "cleanup": cleanup,
            "usage": None,
            "finish_reason": "recovery_unknown",
        }
        with self.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                """UPDATE delivery_attempts SET state=?,result_json=?,cleanup=?,finished_at=?
                   WHERE job_key=?""",
                (
                    "finished" if cleanup == "confirmed" else "unknown",
                    canonical_json(result) if cleanup == "confirmed" else None,
                    cleanup,
                    _now(),
                    job_key,
                ),
            )
        return result


_SUPERVISORS: dict[tuple[str, str], DeliverySupervisor] = {}


def get_supervisor(store: DeliveryStore) -> DeliverySupervisor:
    key = (str(store.config.tracking_db.resolve()), str(store.config.state_root.resolve()))
    capacity = int(store.config.raw.get("capacity", 2))
    if key not in _SUPERVISORS:
        _SUPERVISORS[key] = DeliverySupervisor(store, capacity=capacity)
    elif _SUPERVISORS[key].capacity != capacity:
        raise ValueError("shared delivery capacity changed across service configurations")
    return _SUPERVISORS[key]
