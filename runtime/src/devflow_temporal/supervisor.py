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

from .candidate import candidate_for
from .contracts import canonical_json, digest
from .delivery_native_process import NativeProcessUnknown
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
            raise NativeProcessUnknown("role request changed across a durable attempt")
        return
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(content)




class DeliverySupervisor:
    def __init__(self, store: DeliveryStore, *, capacity: int) -> None:
        self.store = store
        self.capacity = capacity
        self.job_locks: dict[str, asyncio.Lock] = {}

    async def _acquire_capacity(self, job_key: str, *, cancelled=None) -> None:
        """Atomically claim one shared DB slot across config overlays and workers."""

        def enter():
            if cancelled is not None and cancelled():
                raise ValueError("native role cancelled before capacity admission")
            with self.store._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute(
                    "SELECT state FROM delivery_attempts WHERE job_key=?", (job_key,)
                ).fetchone()
                if row is None or row["state"] != "queued":
                    raise NativeProcessUnknown("role attempt changed while waiting for capacity")
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
                        raise NativeProcessUnknown("role capacity claim changed before launch")
                    return True
            return False

        while not await asyncio.to_thread(enter):
            # Ambiguous attempts retain their slots until reconciled.
            await asyncio.sleep(1)

    @staticmethod
    def _job_key(request: dict[str, Any]) -> str:
        spec = request["spec"]
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
        if spec.get("gate_retry_generation") in (1, 2) and request["role"] in {"review", "verify"}:
            identity["gate_retry_generation"] = spec["gate_retry_generation"]
            identity["gate_retry_stage"] = spec.get("gate_retry_stage")
        if generation:
            identity["attempt_generation"] = generation
        return hashlib.sha256(canonical_json(identity).encode()).hexdigest()

    def retained_request(self, request: dict[str, Any]) -> dict[str, Any] | None:
        """Reuse a launched role, never a queued input or unrelated checkout edit."""
        from .delivery_resources import read_private

        key = self._job_key(request)
        with self.store._connect() as db:
            row = db.execute(
                'SELECT state,result_json FROM delivery_attempts WHERE job_key=?', (key,),
            ).fetchone()
        if row is None or row['state'] == 'queued':
            return None
        folder = Path(request['spec']['state_dir']) / 'attempts' / key
        if not (folder / 'request.json').exists():
            return None
        saved = read_private(folder / 'request.json')
        # These fields are added by the controller after activity admission.
        enriched = {'native_authorized', 'result_path', 'start_path', 'role_evidence_key',
                    'artifact_directory', 'artifact_write_root', 'receipt_handoff', 'steering'}
        for name in saved.keys() | request.keys():
            if name in enriched and name not in request:
                continue
            old, new = saved.get(name), request.get(name)
            if name == 'evidence_context':
                generated = {'implementation_preparation', 'previous_iterations'}
                old = {k: v for k, v in (old or {}).items() if k not in generated}
                new = {k: v for k, v in (new or {}).items() if k not in generated}
            if old != new:
                raise NativeProcessUnknown('role request changed across a durable attempt')
        if row['state'] == 'finished':
            result = json.loads(row['result_json'])
            journal_path = folder / 'native-process.json'
            if journal_path.exists():
                journal = read_private(journal_path)
                intent = journal['intent']
                if (not journal.get('owned')
                        or intent['run_id'] != request['spec']['run_id']
                        or intent['policy_digest'] != request['spec']['policy_digest']
                        or intent['cwd'] != request['workspace']):
                    raise NativeProcessUnknown('completed role process ownership changed')
                metadata = journal.get('provider_session', {})
            else:
                start_path = folder / 'start.json'
                metadata = read_private(start_path) if start_path.exists() else {}
            expected = metadata.get('output_candidate') or request['candidate']
            if metadata.get('output_candidate') and (
                    not isinstance(expected, dict)
                    or set(expected) != {'id', 'head', 'content_sha256'}):
                raise NativeProcessUnknown('completed role output binding is malformed')
            if metadata.get('output_candidate') and metadata.get('result_digest') != digest(result):
                raise NativeProcessUnknown('completed role result changed after its output binding')
            if candidate_for(Path(request['workspace']))['id'] != expected['id']:
                raise ValueError('completed role candidate changed after its result')
        else:
            journal_path = folder / 'native-process.json'
            if not journal_path.exists():
                return None
            journal = read_private(journal_path)
            intent = journal['intent']
            if (not journal.get('owned')
                    or intent['run_id'] != request['spec']['run_id']
                    or intent['policy_digest'] != request['spec']['policy_digest']
                    or intent['cwd'] != request['workspace']):
                return None
        return saved

    def _claim(self, request: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
        spec = request["spec"]
        if spec["policy"].get("execution_backend") == "native-macos":
            from .delivery_native_guard import validate_native_turn

            validate_native_turn(spec, request["role"], request["iteration"], self.store)
        job_key = self._job_key(request)
        folder = Path(spec["state_dir"]) / "attempts" / job_key
        folder.mkdir(parents=True, mode=0o700, exist_ok=True)
        result_path = folder / "result.json"
        with self.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM delivery_attempts WHERE job_key=?", (job_key,)
            ).fetchone()
            if row:
                if row["candidate_id"] != request['candidate']['id']:
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
        from .delivery_preparation import require_native_execution

        require_native_execution(request["spec"])
        await asyncio.to_thread(self.retained_request, request)
        job_key, existing = await asyncio.to_thread(self._claim, request)
        if existing is not None:
            return existing
        if request["spec"].get("provider") == "codex":
            lock = self.job_locks.setdefault(job_key, asyncio.Lock())
            async with lock:
                return await self._run_native(request, job_key)
        spec = request["spec"]
        folder = Path(spec["state_dir"]) / "attempts" / job_key
        result_path = folder / "result.json"
        start_path = folder / "start.json"
        request_path = folder / "request.json"
        request = {**request, "result_path": str(result_path), "start_path": str(start_path)}
        await self._acquire_capacity(job_key)
        try:
            from .delivery_dashboard import launch_steering

            request = launch_steering(self.store, request, job_key)
            from .delivery_role_evidence import allocate

            request = allocate(request, job_key)
            _private_json(request_path, request)
            if Path("/usr/bin/sandbox-exec").is_file():
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
            from .delivery_resources import read_private, write_private

            output_candidate = await asyncio.to_thread(self._completed_candidate, request, result)
            metadata = read_private(start_path)
            metadata.update(output_candidate=output_candidate, result_digest=digest(result))
            write_private(start_path, metadata)
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
            await asyncio.to_thread(verify_prepared_spec, spec)
            with self.store._connect() as db:
                row = db.execute(
                    "SELECT state FROM delivery_attempts WHERE job_key=?", (job_key,)
                ).fetchone()
            if row["state"] == "queued":
                await self._acquire_capacity(job_key, cancelled=cancelled)
            elif not (folder / "native-process.json").is_file():
                return self._mark_unknown(job_key, "native prelaunch identity gap")
            from .delivery_dashboard import launch_steering

            native_request = launch_steering(self.store, native_request, job_key)
            from .delivery_role_evidence import allocate, seal

            if not request_path.exists():
                native_request = allocate(native_request, job_key)
            else:
                # A supervised reattachment keeps the original durable launch contract.
                native_request = read_private(request_path)
            _private_json(request_path, native_request)
            _, environment = await asyncio.to_thread(prepare_native_role, native_request, folder)
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
            if native_request.get("role_evidence_key"):
                try:
                    result.update(await asyncio.to_thread(seal, native_request))
                except (ValueError, OSError) as exc:
                    result["status"] = "blocked"
                    result.setdefault("findings", []).append(str(exc))
            result.update(
                cleanup="confirmed",
                process_cleanup=outcome["cleanup"],
                resource_cleanup="pending_workflow_finalization",
                native_process=outcome,
            )
            output_candidate = await asyncio.to_thread(self._completed_candidate, request, result)
            journal = read_private(process.journal)
            journal["provider_session"] = {
                "session_id": result.get("session_id"),
                "resumed_from": request.get("resume_session"),
                "role": request["role"],
                "iteration": request["iteration"],
                "output_candidate": output_candidate,
                "result_digest": digest(result),
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

    @staticmethod
    def _completed_candidate(request: dict[str, Any], result: dict[str, Any]) -> dict | None:
        """Source rejection does not undo independently confirmed process teardown."""
        try:
            return candidate_for(Path(request['workspace']))
        except (ValueError, OSError) as exc:
            result['status'] = 'blocked'
            result.setdefault('findings', []).append(str(exc))
            return None



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
