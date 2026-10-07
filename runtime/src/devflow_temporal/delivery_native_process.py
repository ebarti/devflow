"""Gated native effects and honest teardown of observed identity-bound children."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import signal
import stat
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path

from .contracts import digest
from .delivery_resources import RunResources, private_directory, read_private, write_private


class NativeProcessUnknown(RuntimeError):
    """Native launch identity or durable lifecycle evidence cannot establish authority."""


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
    all_owned = dict(owned)
    if journal.get("monitor"):
        monitor = journal["monitor"]
        all_owned[monitor["pid"]] = monitor
        owned.pop(monitor["pid"], None)
    # An interrupted monitor must be allowed to collect its child's exit and
    # flush output. A finished monitor has no remaining command work to lose.
    stopped = stop_observed(all_owned if journal.get("phase") == "finished" else owned)
    table = process_table()
    stopped = stopped and not any(
        table.get(pid, {}).get("identity") == entry["identity"]
        and not table[pid]["stat"].startswith("Z")
        for pid, entry in all_owned.items()
    )
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
        "observed_pids": sorted(all_owned),
        "observed_owned_stopped": stopped,
        "owned_ports_clear": ports_clear,
        "monitoring_complete": journal.get("monitoring_complete", False),
        "reason": None
        if clean
        else "interrupted or ambiguous monitoring cannot certify native teardown",
    }


def _cancel_requested(folder: Path) -> bool:
    try:
        info = (folder / "cancel").lstat()
    except FileNotFoundError:
        return False
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1 or info.st_size != 0):
        raise ValueError("native cancellation signal is not a private owned file")
    return True


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
        # The monitor owns stdio and teardown independently of the activity
        # worker. Its inherited lock survives worker loss, so a replacement
        # waits for the same invocation instead of launching it again.
        lock = os.open(
            self.folder / "native-monitor.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
        )
        try:
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    self._request_cancel()
                    time.sleep(0.03)
            if self.journal.exists():
                return self._run()
            descriptor = os.open(
                self.folder / "monitor.log", os.O_CREAT | os.O_APPEND | os.O_WRONLY | os.O_NOFOLLOW,
                0o600,
            )
            try:
                info = os.fstat(descriptor)
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                        or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
                    raise ValueError("native monitor log is not a private owned file")
                monitor = subprocess.Popen(
                    [sys.executable, "-I", "-m", __name__],
                    stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=descriptor,
                    pass_fds=(lock,), start_new_session=True,
                )
            finally:
                os.close(descriptor)
            assert monitor.stdin is not None
            try:
                monitor.stdin.write(json.dumps({
                    "spec": self.spec, "folder": str(self.folder), "argv": self.argv,
                    "cwd": str(self.cwd), "environment": self.environment,
                    "timeout": self.timeout, "ports": self.ports,
                }).encode())
            finally:
                monitor.stdin.close()
            while monitor.poll() is None:
                self._request_cancel()
                time.sleep(0.03)
            monitor.wait()
            if not self.journal.exists():
                raise RuntimeError("native monitor failed before recording its invocation")
            return self._run()
        finally:
            # Do not LOCK_UN: the detached monitor shares this open file
            # description and must retain the lock if the worker disappears.
            os.close(lock)

    def _intent(self) -> dict:
        return {
            "run_id": self.spec["run_id"], "policy_digest": self.spec["policy_digest"],
            "argv": self.argv, "cwd": str(self.cwd),
            "environment_sha256": digest(self.environment),
            "timeout": self.timeout, "ports": list(self.ports),
        }

    def _request_cancel(self) -> None:
        if self.journal.exists():
            if read_private(self.journal)["intent"] != self._intent():
                raise ValueError("native attempt authority changed")
            if self.cancelled():
                try:
                    descriptor = os.open(
                        self.folder / "cancel",
                        os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600,
                    )
                except FileExistsError:
                    _cancel_requested(self.folder)
                else:
                    try:
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
                    directory = os.open(self.folder, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)

    def _run(self, *, monitor: bool = False) -> dict:
        lock = os.open(
            self.folder / "native-process.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
        )
        try:
            fcntl.flock(lock, fcntl.LOCK_EX)
            intent = self._intent()
            if self.journal.exists():
                old = read_private(self.journal)
                if old["intent"] != intent:
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
            from .delivery_dashboard import runtime_identity
            from .payload import payload_digest

            journal = {
                "intent": intent,
                "runtime_identity": {
                    **runtime_identity(),
                    "source_root": str(Path(__file__).resolve().parents[3]),
                    "runtime_payload_sha256": payload_digest(Path(__file__).parent),
                },
                "phase": "allocated",
                "owned": {},
                "ports": self.ports,
                "monitoring_complete": False,
            }
            if monitor:
                journal["monitor"] = {"pid": os.getpid(), **process_table()[os.getpid()]}
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
            pipe_eof = False

            def drain_output() -> None:
                # Candidate stdio points to a pipe, never to a protected controller
                # file. Node/other runtimes may safely inspect inherited descriptors.
                nonlocal pipe_eof
                if process is None or process.stdout is None:
                    return
                for _ in range(16):
                    try:
                        data = os.read(process.stdout.fileno(), 65536)
                    except BlockingIOError:
                        return
                    if not data:
                        pipe_eof = True
                        return
                    while data:
                        data = data[os.write(descriptor, data):]

            try:
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
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                assert process.stdout is not None
                os.set_blocking(process.stdout.fileno(), False)
                ready_path = self.folder / "ready.json"
                deadline = time.monotonic() + 10
                while not ready_path.exists():
                    drain_output()
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
                    drain_output()
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
                    drain_deadline = time.monotonic() + 1
                    while not pipe_eof and time.monotonic() < drain_deadline:
                        drain_output()
                        if not pipe_eof:
                            time.sleep(0.01)
                    if process.stdout is not None:
                        process.stdout.close()
                os.fsync(descriptor)
                os.close(descriptor)
                ports_clear = all(not listeners(port) for port in self.ports)
                cleanup = (
                    "observed-native-confirmed"
                    if monitoring_complete and stopped and ports_clear and pipe_eof
                    else "unknown"
                )
                finalized_owned = dict(owned)
                if "monitor" in journal:
                    finalized_owned[journal["monitor"]["pid"]] = journal["monitor"]
                result = {
                    "state": "finished" if cleanup != "unknown" else "unknown",
                    "runtime_identity": journal["runtime_identity"],
                    "exit_code": process.returncode if process else None,
                    "cleanup": cleanup,
                    "timed_out": timed_out,
                    "cancelled": cancelled,
                    "listener_conflict": conflict,
                    "observed_listeners": observed_ports,
                    "observed_owned_pids": sorted(finalized_owned),
                    "monitoring_complete": monitoring_complete,
                    "stdio_drained": pipe_eof,
                    "log": str(log_path),
                    "log_sha256": hashlib.sha256(log_path.read_bytes()).hexdigest(),
                    "journal": str(self.journal),
                    "native_teardown_scope": "observed identity-bound descendants and owned ports",
                }
                journal.update(
                    phase="finished" if cleanup != "unknown" else "unknown",
                    owned={str(pid): value for pid, value in finalized_owned.items()},
                    monitoring_complete=monitoring_complete,
                    result=result,
                )
                write_private(self.journal, journal)
            return result
        finally:
            os.close(lock)


if __name__ == "__main__":
    request = json.load(sys.stdin)
    folder = Path(request.pop("folder"))
    specification = request.pop("spec")
    request["cwd"] = Path(request["cwd"])
    request["ports"] = tuple(request["ports"])
    NativeProcess(
        specification, folder, **request,
        cancelled=lambda: _cancel_requested(folder),
    )._run(monitor=True)
