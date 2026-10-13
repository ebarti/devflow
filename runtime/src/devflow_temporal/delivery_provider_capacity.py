"""Private, identity-bound clock credit for a confirmed provider capacity wait."""
from __future__ import annotations

import math
import time

from .delivery_resources import read_private, write_private

WAIT_FILE = "provider-capacity-wait.json"
MAX_BACKOFF = 60


def capacity_error(value):
    """Typed provider unavailability, separate from account rate/usage limits."""
    if isinstance(value, str):
        return value in {"serverOverloaded", "flexUnavailable"}
    if isinstance(value, dict):
        return any(isinstance(value.get(kind), dict)
                   and value[kind].get("httpStatusCode") == 503
                   for kind in ("httpConnectionFailed", "responseStreamConnectionFailed",
                                "responseStreamDisconnected", "responseTooManyFailedAttempts"))
    return False


class CapacityWait:
    def __init__(self, folder, run_id, pid, identity):
        self.path = folder / WAIT_FILE
        self.value = {
            "schema": "devflow-provider-capacity-wait-v1", "run_id": run_id,
            "pid": pid, "start_identity": identity, "credited_seconds": 0.0,
            "wait_started": None, "wait_seconds": 0,
        }

    def begin(self, rejected_seconds, delay):
        self.value["credited_seconds"] += rejected_seconds
        self.value.update(wait_started=time.monotonic(), wait_seconds=delay)
        write_private(self.path, self.value)

    def finish(self):
        elapsed = min(time.monotonic() - self.value["wait_started"], self.value["wait_seconds"])
        self.value["credited_seconds"] += elapsed
        self.value.update(wait_started=None, wait_seconds=0)
        write_private(self.path, self.value)
        return elapsed


def clock_credit(folder, run_id, pid, identity, started, now=None):
    """Only the supervised role PID can exclude its confirmed capacity wait.

    A dead or stalled writer grants at most its last scheduled backoff; it cannot
    suppress the execution deadline indefinitely with an abandoned wait marker.
    """
    try:
        value = read_private(folder / WAIT_FILE)
    except FileNotFoundError:
        return 0.0
    if now is None:
        now = time.monotonic()
    credit, wait, delay = (value.get(k) for k in (
        "credited_seconds", "wait_started", "wait_seconds"))
    if (value.get("schema") != "devflow-provider-capacity-wait-v1"
            or value.get("run_id") != run_id or value.get("pid") != pid
            or value.get("start_identity") != identity
            or type(credit) not in (int, float) or not math.isfinite(credit)
            or not 0 <= credit <= now - started
            or type(delay) is not int or not 0 <= delay <= MAX_BACKOFF
            or (wait is None and delay != 0)
            or (wait is not None and (type(wait) not in (int, float)
                or not math.isfinite(wait) or not started <= wait <= now or delay == 0))):
        raise ValueError("provider capacity wait changed its native owner or clock")
    if wait is not None:
        credit += min(now - wait, delay)
    if credit > now - started:
        raise ValueError("provider capacity wait exceeds observed elapsed time")
    return credit
