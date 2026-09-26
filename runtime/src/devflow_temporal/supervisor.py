"""Single-host role admission and crash-conservative child supervision."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import Any

from .contracts import canonical_json
from .delivery_sandbox import prepare_native_role, prepare_sandbox
from .delivery_store import DeliveryStore, _now


def _process_identity(pid: int) -> str | None:
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart="], capture_output=True, text=True, check=False
    )
    value = result.stdout.strip()
    return value if result.returncode == 0 and value else None


def _private_json(path: Path, value: dict[str, Any]) -> None:
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


class DeliverySupervisor:
    def __init__(self, store: DeliveryStore, *, capacity: int) -> None:
        self.store = store
        self.capacity = capacity
        with store._connect() as db:
            occupied = db.execute(
                """SELECT COUNT(*) FROM delivery_attempts
                   WHERE state IN ('starting','running','unknown')"""
            ).fetchone()[0]
        self.semaphore = asyncio.Semaphore(max(0, capacity - occupied))

    def _claim(self, request: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
        spec = request["spec"]
        identity = {
            "run_id": spec["run_id"],
            "role": request["role"],
            "iteration": request["iteration"],
            "candidate_id": request["candidate"]["id"],
            "policy_digest": spec["policy_digest"],
        }
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
                if result_path.is_file():
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
                    return job_key, {
                        "status": "recovery_unknown",
                        "summary": "prior role process has no durable final result",
                        "findings": [
                            "role process outcome is ambiguous; "
                            "no duplicate invocation was launched"
                        ],
                        "session_id": row["session_id"],
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
        spec = request["spec"]
        folder = Path(spec["state_dir"]) / "attempts" / job_key
        result_path = folder / "result.json"
        start_path = folder / "start.json"
        request_path = folder / "request.json"
        request = {**request, "result_path": str(result_path), "start_path": str(start_path)}
        async with self.semaphore:
            while True:
                with self.store._connect() as db:
                    db.execute("BEGIN IMMEDIATE")
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
                            raise RuntimeError("role attempt changed while waiting for capacity")
                        break
                # An ambiguous attempt retains its slot. An operator must
                # resolve it before queued work can acquire authority.
                await asyncio.sleep(5)
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
                else:
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

    def _mark_prelaunch_blocked(self, job_key: str, reason: str) -> dict[str, Any]:
        result = {
            "status": "blocked",
            "summary": "role launch failed before any provider process started",
            "findings": [reason],
            "session_id": None,
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

    def _mark_unknown(self, job_key: str, reason: str) -> dict[str, Any]:
        with self.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                """UPDATE delivery_attempts SET state='unknown',cleanup='unknown',finished_at=?
                   WHERE job_key=?""",
                (_now(), job_key),
            )
        return {
            "status": "recovery_unknown",
            "summary": reason,
            "findings": [reason],
            "session_id": None,
            "usage": None,
            "finish_reason": "recovery_unknown",
        }


_SUPERVISORS: dict[str, DeliverySupervisor] = {}


def get_supervisor(store: DeliveryStore) -> DeliverySupervisor:
    key = str(store.config.path)
    if key not in _SUPERVISORS:
        _SUPERVISORS[key] = DeliverySupervisor(
            store, capacity=int(store.config.raw.get("capacity", 2))
        )
    return _SUPERVISORS[key]
