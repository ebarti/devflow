"""Gated native effects and honest teardown of observed identity-bound children."""

from __future__ import annotations

import fcntl
import hashlib
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path

from .contracts import digest
from .delivery_resources import RunResources, private_directory, read_private, write_private


def process_table() -> dict[int, dict]:
    result = subprocess.run(
        ["ps", "-A", "-o", "pid=,ppid=,pgid=,stat=,lstart="],
        capture_output=True,
        text=True,
        check=True,
        timeout=5,
    )
    table = {}
    for line in result.stdout.splitlines():
        values = line.strip().split(maxsplit=4)
        if len(values) == 5 and all(value.isdecimal() for value in values[:3]):
            table[int(values[0])] = {
                "ppid": int(values[1]),
                "pgid": int(values[2]),
                "stat": values[3],
                "identity": values[4],
            }
    return table


def sample(owned: dict[int, dict]) -> dict[int, dict]:
    table = process_table()
    live = {
        pid
        for pid, entry in owned.items()
        if table.get(pid, {}).get("identity") == entry["identity"]
    }
    while True:
        children = {
            pid for pid, entry in table.items() if entry["ppid"] in live and pid not in live
        }
        if not children:
            break
        for pid in children:
            owned[pid] = table[pid]
        live.update(children)
    return owned


def stop_observed(owned: dict[int, dict]) -> bool:
    for sig in (signal.SIGTERM, signal.SIGKILL):
        sample(owned)
        table = process_table()
        for pid, identity in owned.items():
            entry = table.get(pid)
            if (
                entry is not None
                and entry["identity"] == identity["identity"]
                and not entry["stat"].startswith("Z")
            ):
                try:
                    os.kill(pid, sig)
                except ProcessLookupError:
                    pass
        for _ in range(25):
            table = process_table()
            if not any(
                table.get(pid, {}).get("identity") == entry["identity"]
                and not table[pid]["stat"].startswith("Z")
                for pid, entry in owned.items()
            ):
                return True
            time.sleep(0.05)
    return False


def listeners(port: int) -> set[int]:
    result = subprocess.run(
        ["/usr/sbin/lsof", "-nP", "-t", f"-iTCP:{port}", "-sTCP:LISTEN"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    if result.returncode not in (0, 1):
        raise RuntimeError("owned port inspection unavailable")
    return {int(line) for line in result.stdout.splitlines() if line.isdecimal()}


def reconcile_process(path: Path) -> dict:
    if not path.exists():
        return {
            "journal": str(path),
            "cleanup": "unknown",
            "reason": "registered native launch has no journal",
        }
    journal = read_private(path)
    owned = {int(pid): entry for pid, entry in journal.get("owned", {}).items()}
    stopped = stop_observed(owned)
    ports_clear = all(not listeners(port) for port in journal.get("ports", []))
    clean = (
        journal.get("phase") == "finished"
        and journal.get("monitoring_complete") is True
        and stopped
        and ports_clear
    )
    return {
        "journal": str(path),
        "cleanup": "observed-native-confirmed" if clean else "unknown",
        "observed_pids": sorted(owned),
        "observed_owned_stopped": stopped,
        "owned_ports_clear": ports_clear,
        "monitoring_complete": journal.get("monitoring_complete", False),
        "reason": None
        if clean
        else "interrupted or ambiguous monitoring cannot certify native teardown",
    }


class NativeProcess:
    def __init__(
        self,
        spec: dict,
        folder: Path,
        *,
        argv: list[str],
        cwd: Path,
        environment: dict[str, str],
        timeout: int,
        ports: tuple[int, ...] = (),
        cancelled: Callable[[], bool] = lambda: False,
    ) -> None:
        if spec["policy"].get("execution_backend") != "native-macos":
            raise ValueError("native process did not receive native authority")
        from .delivery_native_guard import reject_nested_controller

        reject_nested_controller()
        self.spec, self.folder = spec, folder
        self.argv, self.cwd, self.environment = argv, cwd, environment
        self.timeout, self.ports, self.cancelled = timeout, ports, cancelled
        private_directory(folder)
        self.journal = folder / "native-process.json"
        if not self.journal.exists() and any(listeners(port) for port in ports):
            raise ValueError("native fixture port belongs to another process")
        RunResources(spec).process(self.journal)

    def run(self) -> dict:
        lock = os.open(
            self.folder / "native-process.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
        )
        try:
            fcntl.flock(lock, fcntl.LOCK_EX)
            intent = {
                "run_id": self.spec["run_id"],
                "policy_digest": self.spec["policy_digest"],
                "argv": self.argv,
                "cwd": str(self.cwd),
                "environment_sha256": digest(self.environment),
                "timeout": self.timeout,
                "ports": self.ports,
            }
            if self.journal.exists():
                old = read_private(self.journal)
                if old["intent"] != {**intent, "ports": list(self.ports)}:
                    raise ValueError("native attempt authority changed")
                receipt = reconcile_process(self.journal)
                if receipt["cleanup"] == "observed-native-confirmed":
                    return old["result"]
                return {
                    "state": "unknown",
                    "cleanup": "unknown",
                    "exit_code": None,
                    "reason": receipt["reason"],
                    "journal": str(self.journal),
                }
            for port in self.ports:
                if listeners(port):
                    raise ValueError("native fixture port belongs to another process")
            journal = {
                "intent": intent,
                "phase": "allocated",
                "owned": {},
                "ports": self.ports,
                "monitoring_complete": False,
            }
            write_private(self.journal, journal)
            write_private(
                self.folder / "launch.json", {"argv": self.argv, "environment": self.environment}
            )
            log_path = self.folder / "process.log"
            descriptor = os.open(
                log_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600
            )
            owned: dict[int, dict] = {}
            timed_out = cancelled = conflict = False
            process = None
            monitoring_complete = False
            observed_ports = {}
            try:
                with os.fdopen(descriptor, "wb") as log:
                    process = subprocess.Popen(
                        [
                            sys.executable,
                            "-I",
                            "-m",
                            "devflow_temporal.native_child",
                            str(self.folder),
                        ],
                        cwd=self.cwd,
                        env=self.environment,
                        stdin=subprocess.PIPE,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        start_new_session=True,
                    )
                    ready_path = self.folder / "ready.json"
                    deadline = time.monotonic() + 10
                    while not ready_path.exists():
                        if process.poll() is not None or time.monotonic() >= deadline:
                            raise RuntimeError("native child never reached its durable start gate")
                        time.sleep(0.02)
                    ready = read_private(ready_path)
                    entry = process_table().get(process.pid)
                    if not entry or ready != {"pid": process.pid, "identity": entry["identity"]}:
                        raise RuntimeError("native process identity changed before authorization")
                    owned[process.pid] = entry
                    journal.update(
                        phase="authorized", owned={str(pid): value for pid, value in owned.items()}
                    )
                    write_private(self.journal, journal)
                    assert process.stdin is not None
                    process.stdin.write(b"GO\n")
                    process.stdin.flush()
                    process.stdin.close()
                    deadline = time.monotonic() + self.timeout
                    while process.poll() is None:
                        sample(owned)
                        journal["owned"] = {str(pid): value for pid, value in owned.items()}
                        write_private(self.journal, journal)
                        for port in self.ports:
                            for pid in listeners(port):
                                if (
                                    pid not in owned
                                    or process_table().get(pid, {}).get("identity")
                                    != owned[pid]["identity"]
                                ):
                                    conflict = True
                                else:
                                    observed_ports[str(port)] = {"pid": pid, **owned[pid]}
                        timed_out, cancelled = time.monotonic() >= deadline, self.cancelled()
                        if timed_out or cancelled or conflict:
                            break
                        time.sleep(0.03)
                    sample(owned)
                    monitoring_complete = True
            finally:
                stopped = stop_observed(owned)
                if process is not None:
                    if not owned and process.poll() is None:
                        # A gated child can only exit without GO; its identity
                        # must be observed before any signal is sent.
                        entry = process_table().get(process.pid)
                        if entry:
                            owned[process.pid] = entry
                            stopped = stop_observed(owned)
                    process.wait(timeout=5)
                ports_clear = all(not listeners(port) for port in self.ports)
                cleanup = (
                    "observed-native-confirmed"
                    if monitoring_complete and stopped and ports_clear
                    else "unknown"
                )
                result = {
                    "state": "finished" if cleanup != "unknown" else "unknown",
                    "exit_code": process.returncode if process else None,
                    "cleanup": cleanup,
                    "timed_out": timed_out,
                    "cancelled": cancelled,
                    "listener_conflict": conflict,
                    "observed_listeners": observed_ports,
                    "observed_owned_pids": sorted(owned),
                    "monitoring_complete": monitoring_complete,
                    "log": str(log_path),
                    "log_sha256": hashlib.sha256(log_path.read_bytes()).hexdigest(),
                    "journal": str(self.journal),
                    "native_teardown_scope": "observed identity-bound descendants and owned ports",
                }
                journal.update(
                    phase="finished" if cleanup != "unknown" else "unknown",
                    owned={str(pid): value for pid, value in owned.items()},
                    monitoring_complete=monitoring_complete,
                    result=result,
                )
                write_private(self.journal, journal)
            return result
        finally:
            os.close(lock)
