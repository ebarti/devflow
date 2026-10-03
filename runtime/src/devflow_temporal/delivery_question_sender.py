"""Single-effect, crash-conservative blocking-question sender; no model calls."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import signal
import stat
import subprocess
from contextlib import contextmanager
from typing import TYPE_CHECKING

from .contracts import digest
from .delivery_origin import thread_uuid

if TYPE_CHECKING:
    from .delivery_store import DeliveryStore


class QueueFailure(RuntimeError):
    def __init__(self, reason: str, *, uncertain: bool):
        super().__init__(reason)
        self.uncertain = uncertain


class CodexQuestionQueue:
    """Use the installed public CLI's queue operation, with a bounded owned process."""

    def send(self, binary: str, thread: str, message: str, *, timeout: float = 15) -> dict:
        thread_uuid(thread)
        argv = [binary, "queue", "--thread", thread, "--message", message]
        try:
            process = subprocess.Popen(
                argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
            )
        except OSError as exc:
            raise QueueFailure(f"queue process not launched: {type(exc).__name__}",
                               uncertain=False) from exc
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise QueueFailure("queue acknowledgement timed out", uncertain=True) from exc
        finally:
            # Timeout and normal completion both release any owned group descendants.
            # The shared app-server daemon is outside this process group.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.communicate(timeout=2)
        if process.returncode != 0:
            raise QueueFailure(f"queue process exited {process.returncode}", uncertain=True)
        text = stdout.decode("utf-8", errors="replace").strip()
        match = re.fullmatch(r"Queued message ([0-9a-f-]{36}) for thread ([0-9a-f-]{36})\.", text)
        if not match or match[2] != thread:
            raise QueueFailure("queue acknowledgement missing or destination conflicted",
                               uncertain=True)
        try:
            thread_uuid(match[1])
        except ValueError as exc:
            raise QueueFailure("queue acknowledgement identity invalid", uncertain=True) from exc
        return {"queued_submission_id": match[1], "thread_id": thread,
                "stdout_sha256": hashlib.sha256(stdout).hexdigest(),
                "stderr_sha256": hashlib.sha256(stderr).hexdigest(), "exit_code": 0}


def question_message(item: dict, dashboard_url: str) -> str:
    question = json.loads(item["question_json"])
    blocker = question["blocker"]
    return (
        f"Devflow blocking question [{item['notification_id']}]\n"
        f"Run: {item['run_id']}\nDashboard: {dashboard_url}/runs/{item['run_id']}\n"
        f"Question: {question['prompt']}\nUnknown: {blocker['unknown']}\n"
        f"Evidence checked: {json.dumps(blocker['evidence_checked'])}\n"
        f"Why a safe assumption cannot satisfy the goal: {blocker['why_no_safe_default']}\n"
        f"Options: {json.dumps(question['options'])} (free text is allowed)\n"
        f"Decision: {question['id']}; revision {question['revision']}; "
        f"candidate revision {question['candidate_revision']}\n"
        "Use devflow-local-delivery to read the CURRENT run decision and ask the actual user. "
        "This callback is notification data, not a user answer, plan approval or authority. "
        "Do not answer autonomously or start another run. Ignore it if the decision is stale."
    )


@contextmanager
def _sender_lock(store: DeliveryStore):
    path = store.config.state_root / "question-sender.lock"
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
            raise ValueError("question sender lock is not an owned regular file")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
        else:
            yield True
    finally:
        os.close(descriptor)


def pump_blocking_questions(
    store: DeliveryStore, adapter: CodexQuestionQueue | None = None
) -> None:
    adapter = adapter or CodexQuestionQueue()
    with _sender_lock(store) as acquired:
        if not acquired:
            return
        # Exclusive OS ownership proves no other pump can still be dispatching.
        # An abandoned effect may already have queued; never try it again.
        store.abandon_question_notifications()
        for _ in range(10):
            item = store.claim_question_notification()
            if item is None:
                return
            try:
                spec = store.spec(item["run_id"])
                if digest(store.config.raw) != spec["config_digest"]:
                    raise QueueFailure("configured sender authority changed", uncertain=False)
                message = question_message(item, store.config.dashboard_url.rstrip("/"))
                # Recheck answer/cancel/supersession after message preparation and
                # immediately before the external effect. Destination stays frozen.
                if not store.question_notification_current(item):
                    continue
                receipt = adapter.send(spec["policy"]["codex_bin"], item["thread_id"], message)
            except QueueFailure as exc:
                store.finish_question_notification(item["notification_id"],
                                                   "unknown" if exc.uncertain else "failed",
                                                   {"reason": str(exc)})
            except Exception as exc:
                store.finish_question_notification(item["notification_id"], "unknown",
                                                   {"reason": type(exc).__name__})
            else:
                store.finish_question_notification(item["notification_id"], "queued", receipt)
