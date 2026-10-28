"""Single-host role admission and crash-conservative child supervision."""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import logging
import os
import signal
import sqlite3
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
from .delivery_writer_custody import activity_custody, inherited_descriptors
from .delivery_writer_custody import to_thread as writer_thread

_LOG = logging.getLogger(__name__)
# Admission polls run about once a second; a persistent deferral is recorded
# on the run after this many consecutive ones and again every so often.
_DEFERRAL_NOTICE_AFTER = 3
_DEFERRAL_NOTICE_EVERY = 60


def activity_attempt_replaced() -> bool:
    """Whether Temporal superseded this attempt without an operator cancel request.

    A heartbeat timeout, a lost attempt or a worker shutdown replaces only the
    waiter; the durable native work it joined keeps running for the retry.
    """
    from temporalio import activity

    if not activity.in_activity():
        return False
    try:
        details = activity.cancellation_details()
    except RuntimeError:
        # Only a real activity context knows why it was cancelled.
        return False
    return bool(details and not details.cancel_requested and (
        details.timed_out or details.not_found or details.worker_shutdown
    ))


def activity_retry_pending() -> bool:
    """Whether Temporal can still start another attempt that would reattach.

    A last attempt or an exhausted schedule-to-close budget ends the activity
    for good, so nothing it leaves running would ever be joined again.
    """
    from datetime import UTC, datetime, timedelta

    from temporalio import activity

    if not activity.in_activity():
        return False
    try:
        info = activity.info()
    except RuntimeError:
        return False
    if info.schedule_to_close_timeout is not None and (
            datetime.now(UTC) - info.scheduled_time + timedelta(seconds=1)
            >= info.schedule_to_close_timeout):
        return False
    policy = info.retry_policy
    return not (policy is not None and policy.maximum_attempts
                and info.attempt >= policy.maximum_attempts)


class ProcessObservationUnknown(RuntimeError):
    """A recorded process cannot currently be proved present or absent."""


def _process_identity(
    pid: int, *, canonical: bool = False, require_observation: bool = False,
) -> str | None:
    """The process start time as ``ps`` prints it.

    ``canonical`` fixes the locale and time zone, so an identity recorded for
    a long-lived member still matches after either changes on the host.
    Lifecycle callers require a third outcome: failed inspection is not proof
    that a recorded process exited. A signal-zero probe can prove absence but
    never establishes its identity or grants permission to signal it.
    """
    env = {**os.environ, "LC_ALL": "C", "TZ": "UTC0"} if canonical else None
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "lstart="],
            capture_output=True, text=True, check=False, env=env,
            **({"timeout": 5} if require_observation else {}),
        )
    except (OSError, subprocess.TimeoutExpired):
        if not require_observation:
            raise
    else:
        value = result.stdout.strip()
        if result.returncode == 0 and value and (
            not require_observation or not result.stderr.strip()
        ):
            return value
        if not require_observation:
            return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return None
    except OSError as exc:
        raise ProcessObservationUnknown(
            f"process {pid} identity observation is unavailable; ownership remains unknown"
        ) from exc
    raise ProcessObservationUnknown(
        f"process {pid} identity observation is unavailable; ownership remains unknown"
    )


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
        # Consecutive admission polls per queued role that a failed sweep or a
        # busy store deferred; persistent waits are surfaced on the run.
        self.deferrals: dict[str, int] = {}

    def _reconcile_capacity(self) -> int:
        """Release dead native owners before admission; return failures that only retain slots."""
        from .delivery_orphans import _complete_attempt

        try:
            with self.store._connect() as db:
                attempts = [dict(row) for row in db.execute(
                    "SELECT * FROM delivery_attempts"
                    " WHERE state IN ('starting','running','unknown')"
                )]
        except Exception as exc:
            _LOG.warning("Capacity sweep could not read native attempts: %s", exc)
            return 1
        failures = 0
        for attempt in attempts:
            try:
                spec = self.store.effective_spec(attempt["run_id"])
                if (spec.get("provider") == "codex"
                        and spec["policy"].get("execution_backend") == "native-macos"
                        and self.store.owns_execution(spec)):
                    _complete_attempt(self.store, spec, attempt)
            except (OSError, ValueError, KeyError, RuntimeError):
                # Missing evidence, a live owner or a changed reservation retains
                # its slot; one unresolved row must not hide another dead owner.
                continue
            except Exception as exc:
                # An unavailable process table, port inspection or store retains
                # the slot too: the sweep is never charged to a waiting role.
                failures += 1
                _LOG.warning("Capacity sweep could not resolve native attempt %s: %s",
                             attempt["job_key"], exc)
                continue
        return failures

    def _enter_capacity(self, job_key: str, *, cancelled=None, stop_requested=None) -> str | None:
        try:
            if cancelled is not None and cancelled():
                raise ValueError("native role cancelled before capacity admission")
            failures = self._reconcile_capacity()
            admitted = self._admit(job_key, cancelled=cancelled, stop_requested=stop_requested)
        except sqlite3.OperationalError as exc:
            code = getattr(exc, "sqlite_errorcode", 0) & 0xFF
            if code not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                raise
            # Another writer holds the store (every state connection opens with
            # a write transaction): the role is simply not admitted yet and
            # polls again. A busy store is never a launch failure.
            _LOG.warning("Native capacity admission deferred for %s: %s", job_key, exc)
            self._defer_admission(job_key, f"store busy: {exc}")
            return None
        if failures and not admitted:
            self._defer_admission(
                job_key, f"capacity sweep could not resolve {failures} native attempt(s)",
            )
        else:
            self.deferrals.pop(job_key, None)
        return admitted

    def _defer_admission(self, job_key: str, reason: str) -> None:
        """Count consecutive deferred polls; a persistent wait is recorded on its run."""
        count = self.deferrals[job_key] = self.deferrals.get(job_key, 0) + 1
        if count != _DEFERRAL_NOTICE_AFTER and count % _DEFERRAL_NOTICE_EVERY:
            return
        try:
            with self.store._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute(
                    "SELECT run_id, role, iteration FROM delivery_attempts WHERE job_key=?",
                    (job_key,),
                ).fetchone()
                if row is None:
                    return
                run = db.execute("SELECT revision FROM delivery_runs WHERE run_id=?",
                                 (row["run_id"],)).fetchone()
                self.store._event(
                    db, row["run_id"], run["revision"], "native_capacity_deferred",
                    f"Native {row['role']} role still waits for capacity: {reason}",
                    {"job_key": job_key, "role": row["role"], "iteration": row["iteration"],
                     "consecutive_deferrals": count, "reason": reason},
                )
        except Exception as exc:
            # A store that cannot even record the wait leaves the log as the signal.
            _LOG.warning("Capacity deferral of %s could not be recorded: %s", job_key, exc)

    def _admit(self, job_key: str, *, cancelled=None, stop_requested=None) -> str | None:
        """Claim one shared DB slot inside a single admission transaction."""
        with self.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if stop_requested is not None and stop_requested():
                raise ValueError("native role cancelled before capacity admission")
            row = db.execute(
                "SELECT state,run_id FROM delivery_attempts WHERE job_key=?", (job_key,)
            ).fetchone()
            if row is None or row["state"] != "queued":
                raise NativeProcessUnknown("role attempt changed while waiting for capacity")
            if cancelled is not None:
                run = db.execute(
                    "SELECT phase FROM delivery_runs WHERE run_id=?", (row["run_id"],),
                ).fetchone()
                if run is not None and run["phase"] == "cancelling":
                    raise ValueError("native role cancelled before capacity admission")
            occupied = db.execute(
                """SELECT COUNT(*) FROM delivery_attempts
                   WHERE state IN ('starting','running','unknown')"""
            ).fetchone()[0]
            if occupied < self.capacity:
                if stop_requested is not None and stop_requested():
                    raise ValueError("native role cancelled before capacity admission")
                started = _now()
                updated = db.execute(
                    """UPDATE delivery_attempts SET state='starting',started_at=?
                       WHERE job_key=? AND state='queued'""",
                    (started, job_key),
                ).rowcount
                if updated != 1:
                    raise NativeProcessUnknown("role capacity claim changed before launch")
                return started
        return None

    async def _acquire_capacity(self, job_key: str, spec: dict, *, cancelled=None) -> None:
        """Atomically claim one shared DB slot across config overlays and workers."""
        while not await writer_thread(spec, self._enter_capacity, job_key, cancelled=cancelled):
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
        if spec.get("gate_admission") and request["role"] in {"review", "verify"}:
            identity["gate_admission"] = spec["gate_admission"]
        if spec.get("implement_admission") and request["role"] == "implement":
            identity["implement_admission"] = spec["implement_admission"]
        if generation:
            identity["attempt_generation"] = generation
        from .delivery_native_guard import revision_role_identity

        revision = revision_role_identity(request)
        if revision is not None:
            identity["plan_revision"] = revision
        if request.get("merge_context"):
            identity["merge_resolution"] = digest(request["merge_context"])
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
                    'artifact_directory', 'artifact_write_root', 'receipt_handoff', 'steering',
                    'review_diff', 'issue_context'}
        if request['spec'].get('readiness_version') == 1:
            enriched.add('readiness_worker_assignment')
        for name in saved.keys() | request.keys():
            if name in enriched and name not in request:
                continue
            old, new = saved.get(name), request.get(name)
            if name == 'evidence_context':
                generated = {'implementation_preparation', 'previous_iterations'}
                if request['spec'].get('readiness_version') == 1:
                    generated.add('readiness')
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
        from .delivery_native_guard import validate_revision_role

        validate_revision_role(request, self.store)
        from .delivery_merge_resolution import authenticate as authenticate_merge_role

        authenticate_merge_role(request, self.store)
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
        async with activity_custody(request["spec"]):
            return await self._run(request)

    async def _run(self, request: dict[str, Any]) -> dict[str, Any]:
        from .delivery_preparation import require_native_execution

        require_native_execution(request["spec"])
        await writer_thread(request["spec"], self.retained_request, request)
        job_key, existing = await writer_thread(request["spec"], self._claim, request)
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
        await self._acquire_capacity(job_key, spec)
        try:
            from .delivery_dashboard import launch_steering

            request = launch_steering(self.store, request, job_key)
            from .delivery_role_evidence import allocate

            request = allocate(request, job_key, store=self.store)
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
                pass_fds=inherited_descriptors(),
            )
            for _ in range(100):
                if start_path.is_file() or child.returncode is not None:
                    break
                await asyncio.sleep(0.05)
            if not start_path.is_file():
                # A late child still waits for GO; deny it with EOF before joining.
                assert child.stdin is not None
                child.stdin.close()
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
            await writer_thread(spec, self._admit_role_output, request, result)
            from .delivery_resources import read_private, write_private

            output_candidate = await writer_thread(spec, self._completed_candidate, request, result)
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
        from .delivery_resources import read_private

        spec = request["spec"]
        folder = Path(spec["state_dir"]) / "attempts" / job_key
        result_path = folder / "result.json"
        request_path = folder / "request.json"
        stopped = threading.Event()
        replaced = threading.Event()

        def cancelled() -> bool:
            if stopped.is_set():
                return True
            with self.store._connect() as db:
                row = db.execute(
                    "SELECT phase FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
                ).fetchone()
            return row is not None and row["phase"] == "cancelling"

        entered_monitor = threading.Event()

        def monitor():
            # One executor owner joins admission and preparation before deciding
            # whether cancellation preceded launch. Cancelling an asyncio waiter
            # cannot abandon a DB claim or let preparation launch a later provider.
            expected = None
            fresh = False
            process = None
            owner_lock = None
            try:
                # The owner lock proves this worker is alive for the whole
                # prelaunch window. Its release by process death, together with
                # a free monitor lock and no journal, is the only launch-absence
                # proof a later reconciler may use; a held lock means wait.
                owner_lock = self._owner_lock(spec, job_key)
                if owner_lock is None:
                    with self.store._connect() as db:
                        current = db.execute(
                            "SELECT state FROM delivery_attempts WHERE job_key=?", (job_key,),
                        ).fetchone()
                    if current is not None and current["state"] == "queued":
                        # Another live owner is admitting this attempt; join its outcome.
                        owner_lock = self._owner_lock(spec, job_key, blocking=True)
                with self.store._connect() as db:
                    expected = dict(db.execute(
                        "SELECT * FROM delivery_attempts WHERE job_key=?", (job_key,),
                    ).fetchone())
                if expected["state"] == "finished":
                    # A retry may have claimed before waiting on the job lock.
                    self.retained_request(request)
                    return {"cleanup": expected["cleanup"]}, json.loads(expected["result_json"])
                fresh = expected["state"] == "queued" and not any(
                    os.path.lexists(folder / name) for name in (
                        "request.json", "native-process.json", "native-monitor.lock",
                        "native-process.lock", "monitor.log", "launch.json", "ready.json",
                        "start.json", "result.json", "cancel",
                    )
                )
                if (expected["run_id"] != spec["run_id"]
                        or expected["role"] != request["role"]
                        or expected["iteration"] != request["iteration"]
                        or expected["candidate_id"] != request["candidate"]["id"]
                        or expected["result_path"] != str(result_path)):
                    raise NativeProcessUnknown("native role reservation ownership changed")
                if expected["state"] == "queued" and (not fresh or any(
                    expected[name] is not None for name in (
                        "pid", "process_identity", "session_id", "result_json",
                        "started_at", "finished_at",
                    )
                ) or expected["cleanup"] != "none"):
                    fresh = False
                    raise NativeProcessUnknown("queued native role retains prior launch evidence")
                verify_prepared_spec(spec)
                if expected["state"] == "queued":
                    while True:
                        started = self._enter_capacity(
                            job_key, cancelled=cancelled, stop_requested=stopped.is_set,
                        )
                        if started:
                            expected.update(state="starting", started_at=started)
                            break
                        stopped.wait(1)
                elif not (folder / "native-process.json").is_file():
                    # A claimed slot without a journal belongs to a dead owner
                    # when nobody holds the owner or monitor locks; a provider
                    # child cannot exist before its monitor journals.
                    result = None
                    if owner_lock is not None:
                        result = self._reconcile_launch_window(
                            spec, job_key, expected, locks={"attempt-owner": owner_lock},
                        )
                    if result is None:
                        result = self._mark_unknown(
                            job_key, "native prelaunch identity gap", expected=expected,
                        )
                    return {"cleanup": result["cleanup"]}, result
                if cancelled():
                    raise ValueError("native role cancelled before monitor entry")
                from .delivery_native_guard import validate_revision_role

                validate_revision_role(request, self.store)
                from .delivery_merge_resolution import authenticate as authenticate_merge_role

                authenticate_merge_role(request, self.store)
                from .delivery_dashboard import launch_steering
                from .delivery_role_evidence import allocate

                native_request = launch_steering(self.store, {
                    **request, "native_authorized": True, "result_path": str(result_path),
                    "start_path": str(folder / "start.json"),
                }, job_key)
                if not request_path.exists():
                    from .delivery_issue_context import attach_latest

                    native_request = attach_latest(self.store, native_request)
                    native_request = allocate(native_request, job_key, store=self.store)
                else:
                    # Reattachment keeps the original durable launch contract.
                    native_request = read_private(request_path)
                _private_json(request_path, native_request)
                _, environment = prepare_native_role(native_request, folder)
                if cancelled():
                    raise ValueError("native role cancelled before monitor entry")
                process = NativeProcess(
                    spec, folder,
                    argv=[sys.executable, "-I", "-m", "devflow_temporal.role_runner",
                          str(request_path)],
                    cwd=Path(request["workspace"]), environment=environment,
                    timeout=int(spec["policy"]["roles"][request["role"]].get(
                        "timeout_seconds", 7200)),
                    cancelled=cancelled,
                    register_resources=not bool(request.get("merge_context")),
                )
                if cancelled():
                    raise ValueError("native role cancelled before monitor entry")
                entered_monitor.set()
                outcome = process.run()
                return outcome, self._complete_native(
                    request, native_request, process, job_key, outcome,
                )
            except Exception as exc:
                result = self._native_failure(
                    request, job_key, expected, str(exc)[:300],
                    launch_absent=fresh and (
                        not entered_monitor.is_set() or getattr(process, "launch_absent", False)
                    ),
                    monitor_entered=entered_monitor.is_set(), preserve_queued=replaced.is_set(),
                )
                return {"cleanup": result["cleanup"]}, result
            finally:
                if owner_lock is not None:
                    os.close(owner_lock)

        # Submit once, independently of the cancellable asyncio waiter. Runner
        # shutdown can cancel that task without cancelling or hiding its thread.
        execution = asyncio.create_task(writer_thread(spec, monitor))

        async def wait_for_monitor():
            return await asyncio.shield(execution)

        pending = asyncio.create_task(wait_for_monitor())
        try:
            _, result = await asyncio.shield(pending)
            return result
        except asyncio.CancelledError:
            replacement = activity_attempt_replaced()
            if entered_monitor.is_set() and replacement:
                # An already entered durable monitor survives replacement of its
                # Temporal waiter and still observes cancellation of its run.
                execution.add_done_callback(
                    lambda future: future.exception() if not future.cancelled() else None
                )
                raise
            if replacement:
                replaced.set()
            stopped.set()
            while True:
                try:
                    outcome, result = await asyncio.shield(execution)
                    break
                except asyncio.CancelledError:
                    # Repeated cancellation still cannot strand executor work.
                    if execution.done():
                        outcome, result = execution.result()
                        break
                    continue
            if entered_monitor.is_set() and result.get("finish_reason") != "prelaunch":
                self._mark_unknown(
                    job_key, "native activity cancelled during provider work",
                    cleanup="confirmed" if outcome["cleanup"] != "unknown" else "unknown",
                )
            raise

    def _native_failure(self, request, job_key, expected, reason, *, launch_absent=False,
                        monitor_entered=False, preserve_queued=False):
        """Finalize a joined prelaunch owner, authenticating any entered monitor."""
        import stat

        folder = Path(request["spec"]["state_dir"]) / "attempts" / job_key
        unknown = {
            "status": "recovery_unknown", "summary": reason, "findings": [reason],
            "session_id": None, "cleanup": "unknown", "usage": None,
            "finish_reason": "recovery_unknown",
        }
        descriptors = []
        try:
            with self.store._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute(
                    "SELECT * FROM delivery_attempts WHERE job_key=?", (job_key,),
                ).fetchone()
                observer_unknown = (
                    launch_absent and expected is not None and expected["state"] == "starting"
                    and row is not None and row["state"] == "unknown"
                    and row["cleanup"] == "unknown"
                    and {k: v for k, v in dict(row).items()
                         if k not in {"state", "cleanup", "finished_at"}}
                    == {k: v for k, v in expected.items()
                        if k not in {"state", "cleanup", "finished_at"}}
                )
                if row is None or (dict(row) != expected and not observer_unknown) or (
                    row["run_id"] != request["spec"]["run_id"]
                    or row["role"] != request["role"] or row["iteration"] != request["iteration"]
                    or row["candidate_id"] != request["candidate"]["id"]
                    or row["result_path"] != str(folder / "result.json")
                ):
                    return unknown
                if row["state"] == "finished":
                    return unknown
                # An observer can mark an in-flight reservation unknown. Only
                # its original fresh owner, after terminal prelaunch proof, can
                # close that unchanged reservation; prior unknowns cannot pass.
                confirmed = launch_absent and (
                    row["state"] in {"queued", "starting"} or observer_unknown
                ) and (
                    row["pid"] is None and row["process_identity"] is None
                    and row["session_id"] is None and row["result_json"] is None
                    and ((row["cleanup"] == "none" and row["finished_at"] is None)
                         or observer_unknown)
                )
                if confirmed:
                    try:
                        for name in ("native-monitor.lock", "native-process.lock"):
                            path = folder / name
                            if not os.path.lexists(path):
                                if name == "native-monitor.lock" and monitor_entered:
                                    raise ValueError("entered native monitor lost its lock")
                                continue
                            descriptor = os.open(
                                path, os.O_RDWR | os.O_NOFOLLOW,
                            )
                            descriptors.append(descriptor)
                            info = os.fstat(descriptor)
                            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
                                raise ValueError("native lock ownership changed")
                            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            named = path.lstat()
                            if (named.st_dev, named.st_ino) != (info.st_dev, info.st_ino):
                                raise ValueError("native lock pathname changed")
                        confirmed = not any(os.path.lexists(folder / name) for name in (
                            "native-process.json", "launch.json", "ready.json",
                            "start.json", "result.json", "cancel",
                        ))
                    except (OSError, ValueError):
                        confirmed = False
                if confirmed and preserve_queued and row["state"] == "queued":
                    run = db.execute(
                        "SELECT phase FROM delivery_runs WHERE run_id=?", (row["run_id"],),
                    ).fetchone()
                    if run is not None and run["phase"] != "cancelling":
                        # Supersession claimed no slot and produced no launch.
                        # Its retry remains under Temporal's original deadline.
                        return unknown
                result = {
                    **unknown, "status": "blocked", "cleanup": "confirmed",
                    "summary": "role launch failed before any provider process started",
                    "finish_reason": "prelaunch",
                } if confirmed else unknown
                db.execute(
                    """UPDATE delivery_attempts SET state=?,result_json=?,cleanup=?,finished_at=?
                       WHERE job_key=?""",
                    ("finished" if confirmed else "unknown",
                     canonical_json(result) if confirmed else None,
                     result["cleanup"], _now(), job_key),
                )
                return result
        except (OSError, ValueError):
            return unknown
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)

    @staticmethod
    def _owner_lock_path(spec: dict[str, Any], job_key: str) -> Path:
        # Kept outside the attempt folder: retry-prelaunch requires that folder
        # to stay empty after a failure that never launched.
        return Path(spec["state_dir"]) / "attempt-owners" / job_key

    @classmethod
    def _owner_lock(cls, spec: dict[str, Any], job_key: str, *,
                    blocking: bool = False) -> int | None:
        """Hold the attempt's owner lock; None when another live owner holds it."""
        import stat

        from .delivery_resources import private_directory

        path = cls._owner_lock_path(spec, job_key)
        private_directory(path.parent)
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(descriptor)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
                raise ValueError("native owner lock is not a private owned file")
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
            except BlockingIOError:
                os.close(descriptor)
                return None
        except BaseException:
            os.close(descriptor)
            raise
        return descriptor

    def _reconcile_launch_window(self, spec, job_key, expected, *, locks=None, on_observed=None):
        """Finalize a claimed slot whose dead owner never reached a journaled launch.

        A provider child only starts after its monitor journals under the
        monitor lock, and a live owner holds the owner lock throughout its
        prelaunch window. Holding both locks with no journal or launch
        artifacts proves no provider ever received GO. Returns None whenever
        that proof is unavailable so callers retain the slot.
        """
        import stat

        if (expected is None or expected["state"] not in {"starting", "unknown"}
                or expected["cleanup"] == "confirmed"
                or any(expected[name] is not None for name in (
                    "pid", "process_identity", "session_id", "result_json"))):
            return None
        folder = Path(spec["state_dir"]) / "attempts" / job_key
        if not Path(spec["state_dir"]).is_dir():
            return None
        descriptors = []
        try:
            # Nothing is created inside the attempt folder: its emptiness is the
            # retry-prelaunch predicate. A missing monitor lock means run() never
            # started, and the held owner lock excludes a concurrent launch.
            for name, path in (("attempt-owner", self._owner_lock_path(spec, job_key)),
                               ("native-monitor.lock", folder / "native-monitor.lock")):
                descriptor = (locks or {}).get(name)
                if descriptor is None:
                    if name == "attempt-owner":
                        descriptor = self._owner_lock(spec, job_key)
                        if descriptor is None:
                            return None
                    elif os.path.lexists(path):
                        descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
                    else:
                        continue
                    descriptors.append(descriptor)
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                info = os.fstat(descriptor)
                named = path.lstat()
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                        or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
                        or (named.st_dev, named.st_ino) != (info.st_dev, info.st_ino)):
                    raise ValueError("native lock ownership changed")
            if any(os.path.lexists(folder / name) for name in (
                    "native-process.json", "launch.json", "ready.json", "start.json",
                    "result.json", "cancel")):
                return None
            receipt = {
                "journal": str(folder / "native-process.json"),
                "cleanup": "observed-native-confirmed", "launch_absent": True,
                "observed_pids": [], "previous_state": expected["state"], "reason": None,
            }
            if on_observed is not None:
                on_observed(receipt)
            reason = "native owner lost before its monitor journaled a launch"
            result = {
                "status": "blocked",
                "summary": "role launch failed before any provider process started",
                "findings": [reason], "session_id": None, "cleanup": "confirmed",
                "usage": None, "finish_reason": "prelaunch", "native_process": receipt,
            }
            with self.store._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute(
                    "SELECT * FROM delivery_attempts WHERE job_key=?", (job_key,),
                ).fetchone()
                if row is None or dict(row) != expected:
                    return None
                run = db.execute("SELECT revision FROM delivery_runs WHERE run_id=?",
                                 (row["run_id"],)).fetchone()
                self.store._event(db, row["run_id"], run["revision"],
                    "native_attempt_cleanup_reconciled",
                    "Lost native owner finalized before any provider process started",
                    {"original_attempts": [expected], "native_observations": [receipt]})
                db.execute(
                    """UPDATE delivery_attempts SET state='finished',result_json=?,
                       cleanup='confirmed',finished_at=? WHERE job_key=?""",
                    (canonical_json(result), _now(), job_key),
                )
            return result
        except (OSError, ValueError):
            return None
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)

    def _complete_native(self, request, native_request, process, job_key, outcome,
                         *, expected=None):
        from .delivery_resources import read_private, write_private
        from .delivery_role_evidence import seal

        # Superseded and replacement waiters can both observe the same process.
        # Use its existing lock to publish one stable result and session binding.
        descriptor = os.open(process.folder / "native-process.lock", os.O_RDWR | os.O_NOFOLLOW)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            with self.store._connect() as db:
                row = db.execute(
                    "SELECT * FROM delivery_attempts WHERE job_key=?", (job_key,),
                ).fetchone()
            if row is None:
                raise NativeProcessUnknown("native completion reservation disappeared")
            if row["state"] == "finished":
                return json.loads(row["result_json"])
            if expected is not None and dict(row) != expected:
                raise NativeProcessUnknown("native completion reservation changed")
            original_attempt = dict(row)
            result_path = process.folder / "result.json"
            if outcome["cleanup"] == "unknown":
                return self._mark_unknown(job_key, "native monitoring/provider outcome ambiguous")
            interrupted = outcome.get("interrupted_monitor") is True
            launch_absent = interrupted and outcome.get("provider_outcome") == "not_started"
            if launch_absent:
                # The monitor died before authorizing its child: no provider ran,
                # so the attempt is a retryable prelaunch failure, not a verdict.
                result = {"status": "blocked",
                          "summary": "role launch failed before any provider process started",
                          "findings": ["native monitor lost before any provider process started"],
                          "session_id": None, "usage": None, "finish_reason": "prelaunch"}
            elif interrupted:
                result = {"status": "recovery_unknown",
                          "summary": "native monitor lost; owned teardown independently confirmed",
                          "findings": ["provider outcome unknown; attempt will not be repeated"],
                          "session_id": None, "usage": None, "finish_reason": "interrupted_monitor"}
            elif result_path.is_file():
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
            if not interrupted:
                self._admit_role_output(native_request, result)
            if not interrupted and native_request.get("role_evidence_key"):
                try:
                    result.update(seal(native_request))
                except (ValueError, OSError) as exc:
                    result["status"] = "blocked"
                    result.setdefault("findings", []).append(str(exc))
            result.update(
                cleanup="confirmed",
                process_cleanup=outcome["cleanup"],
                resource_cleanup="pending_workflow_finalization",
                native_process=outcome,
            )
            output_candidate = self._completed_candidate(request, result)
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
                current = db.execute("SELECT * FROM delivery_attempts WHERE job_key=?",
                                     (job_key,)).fetchone()
                if current is None or dict(current) != original_attempt:
                    raise NativeProcessUnknown("native completion reservation changed at commit")
                if interrupted:
                    run = db.execute("SELECT revision FROM delivery_runs WHERE run_id=?",
                                     (original_attempt["run_id"],)).fetchone()
                    self.store._event(db, original_attempt["run_id"], run["revision"],
                        "native_attempt_cleanup_reconciled",
                        "Lost native monitor cleanup confirmed without a provider verdict",
                        {"original_attempts": [original_attempt], "native_observations": [outcome]})
                db.execute(
                    """UPDATE delivery_attempts SET state='finished',result_json=?,
                       session_id=?,finished_at=?,cleanup='confirmed',pid=?,process_identity=?
                       WHERE job_key=?""",
                    (
                        canonical_json(result),
                        result.get("session_id"),
                        _now(),
                        # A prelaunch receipt records no provider process.
                        None if launch_absent else next(iter(journal["owned"]), None),
                        None if launch_absent else
                        next(iter(journal["owned"].values()), {}).get("identity"),
                        job_key,
                    ),
                )
            return result
        finally:
            os.close(descriptor)

    @staticmethod
    def _completed_candidate(request: dict[str, Any], result: dict[str, Any]) -> dict | None:
        """Source rejection does not undo independently confirmed process teardown."""
        try:
            return candidate_for(Path(request['workspace']))
        except (ValueError, OSError) as exc:
            result['status'] = 'blocked'
            result.setdefault('findings', []).append(str(exc))
            return None

    def _admit_role_output(self, request: dict[str, Any], result: dict[str, Any]) -> None:
        if request.get("merge_context"):
            from .delivery_merge_resolution import validate_output

            try:
                validate_output(request, result)
            except (ValueError, RuntimeError, OSError) as exc:
                result["status"] = "blocked"
                result["merge_decision"] = None
                result.setdefault("findings", []).append(str(exc))
            return
        if request['role'] != 'implement':
            return
        from .delivery_broker import DeliveryBroker

        try:
            DeliveryBroker(self.store, request['spec']).admit_implementation(request['candidate'])
        except (ValueError, RuntimeError, OSError) as exc:
            result['status'] = 'blocked'
            result.setdefault('findings', []).append(str(exc))



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
        self, job_key: str, reason: str, *, cleanup: str = "unknown", expected=None,
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
            if expected is None:
                row = db.execute(
                    "SELECT state,result_json FROM delivery_attempts WHERE job_key=?", (job_key,),
                ).fetchone()
                if row is not None and row["state"] == "finished" and row["result_json"]:
                    return json.loads(row["result_json"])
            if expected is not None:
                row = db.execute(
                    "SELECT * FROM delivery_attempts WHERE job_key=?", (job_key,),
                ).fetchone()
                if row is None or dict(row) != expected:
                    if (row is not None and row["state"] == "finished" and row["result_json"]
                            and all(row[name] == value for name, value in expected.items()
                                    if name not in {"state", "result_json", "cleanup",
                                                    "finished_at"})):
                        return json.loads(row["result_json"])
                    return result
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
    from .delivery_execution_config import operational_config

    execution = getattr(store, 'operational_execution', None)
    if execution and not (execution.get('feature_worker') or execution.get('feature_delivery')):
        from .delivery_amendment_capacity import supervisor_config

        configuration = supervisor_config(store, execution)
    else:
        configuration = operational_config(store.config)
    capacity = int(configuration.raw.get("capacity", 2))
    if key not in _SUPERVISORS:
        _SUPERVISORS[key] = DeliverySupervisor(store, capacity=capacity)
    else:
        # A lower limit waits for occupied slots to finish; operational capacity
        # changes never rewrite any admitted execution policy.
        _SUPERVISORS[key].capacity = capacity
    return _SUPERVISORS[key]
