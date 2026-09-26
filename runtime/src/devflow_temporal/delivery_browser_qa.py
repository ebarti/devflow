"""Broker-owned, exact-port browser/API fixture gate and immutable evidence."""

from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import socket
import stat
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from .candidate import candidate_for
from .contracts import canonical_json, digest
from .delivery_sandbox import prepare_browser_qa
from .delivery_store import _now

# The child cannot run candidate code until the broker has durably recorded its
# process identity. EOF (including broker death before GO) exits without exec.
_START_GATE = (
    "import os,sys; "
    "line=sys.stdin.buffer.readline(); "
    "sys.exit(75) if line != b'GO\\n' else os.execvpe(sys.argv[1],sys.argv[1:],os.environ)"
)


def _hash(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _write_new(path: Path, content: bytes) -> None:
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(content)


def _identity(pid: int) -> str | None:
    observed = subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart="], capture_output=True, text=True, check=False
    )
    return observed.stdout.strip() if observed.returncode == 0 else None


def _listeners(port: int) -> set[int]:
    observed = subprocess.run(
        ["/usr/sbin/lsof", "-nP", "-t", f"-iTCP:{port}", "-sTCP:LISTEN"],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    if observed.returncode not in {0, 1}:
        raise RuntimeError("browser QA listener inspection failed")
    return {int(line) for line in observed.stdout.splitlines() if line.isdecimal()}


def _ports_free(ports: tuple[int, int]) -> None:
    for port in ports:
        if _listeners(port):
            raise RuntimeError("browser QA port is already owned by another process")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as guard:
            try:
                guard.bind(("127.0.0.1", port))
            except OSError as exc:
                raise RuntimeError("browser QA port is unavailable") from exc


def _process_table() -> dict[int, dict[str, Any]]:
    observed = subprocess.run(
        ["ps", "-A", "-o", "pid=,ppid=,pgid=,lstart="],
        capture_output=True,
        text=True,
        timeout=5,
        check=True,
    )
    table = {}
    for line in observed.stdout.splitlines():
        parts = line.strip().split(maxsplit=3)
        if len(parts) == 4 and all(item.isdecimal() for item in parts[:3]):
            table[int(parts[0])] = {
                "ppid": int(parts[1]),
                "pgid": int(parts[2]),
                "identity": parts[3],
            }
    return table


def _sample_owned(root_pid: int, prior: dict[int, dict[str, Any]]) -> dict[int, dict[str, Any]]:
    table = _process_table()
    owned = {
        pid: entry
        for pid, entry in prior.items()
        if pid in table and table[pid]["identity"] == entry["identity"]
    }
    if root_pid in table and root_pid not in prior:
        owned[root_pid] = table[root_pid]
    changed = True
    while changed:
        changed = False
        for pid, entry in table.items():
            if pid not in owned and entry["ppid"] in owned:
                owned[pid] = entry
                changed = True
    return owned


def _save_owned(path: Path, owned: dict[int, dict[str, Any]]) -> None:
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(canonical_json({str(pid): entry for pid, entry in owned.items()}) + "\n")
    os.replace(temporary, path)


def _stop_owned(owned: dict[int, dict[str, Any]]) -> bool:
    known = dict(owned)
    trustworthy = True
    for sig in (signal.SIGTERM, signal.SIGKILL):
        table = _process_table()
        for pid, entry in known.items():
            current = table.get(pid)
            if current is None:
                continue
            if current["identity"] != entry["identity"]:
                trustworthy = False
                continue
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                pass
            except PermissionError:
                trustworthy = False
        for _ in range(15):
            time.sleep(0.1)
            table = _process_table()
            if not any(
                table.get(pid, {}).get("identity") == entry["identity"]
                for pid, entry in known.items()
            ):
                return trustworthy
    return False


def _artifacts(checkout: Path, configured: list[str]) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for relative in configured:
        root = checkout / relative
        if not root.exists():
            continue
        if root.is_symlink() or not root.is_dir() or checkout not in root.resolve().parents:
            raise ValueError("browser QA artifact path escaped the checkout")
        for path in sorted(root.rglob("*")):
            if path.is_symlink():
                raise ValueError("browser QA artifact follows a symlink")
            if not path.is_file():
                continue
            if len(found) >= 128 or path.stat().st_size > 50 * 1024 * 1024:
                raise ValueError("browser QA artifact set exceeds the evidence bound")
            found.append({"path": str(path), "sha256": _hash(path), "size": path.stat().st_size})
    return found


def _begin(broker: Any, key: str, request: dict[str, Any]) -> dict[str, Any] | None:
    with broker.store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            "SELECT kind,request_json,state,observed_json FROM delivery_effects WHERE effect_key=?",
            (key,),
        ).fetchone()
        if row:
            if row[0] != "browser_qa" or row[1] != canonical_json(request):
                raise ValueError("browser QA effect identity changed")
            if row[2] == "complete" and row[3]:
                return json.loads(row[3])
            return {
                "state": "unknown",
                "cleanup": "unknown",
                "candidate_id": request["candidate_id"],
            }
        db.execute(
            """INSERT INTO delivery_effects
               (effect_key,run_id,kind,request_json,state,updated_at)
               VALUES (?,?,? ,?,'pending',?)""",
            (key, broker.spec["run_id"], "browser_qa", canonical_json(request), _now()),
        )
    return None


def _reconcile_pending(
    evidence_dir: Path, result: dict[str, Any], ports: tuple[int, int]
) -> dict[str, Any]:
    start = evidence_dir / "start.json"
    if start.is_file():
        saved = json.loads(start.read_text(encoding="utf-8"))
        pid, identity = saved.get("pid"), saved.get("identity")
        if type(pid) is int and isinstance(identity, str) and identity:
            recorded = {pid: {"identity": identity}}
            process_file = evidence_dir / "owned-processes.json"
            if process_file.is_file():
                recorded.update(
                    {int(key): value for key, value in json.loads(process_file.read_text()).items()}
                )
            result["cleanup"] = "confirmed" if _stop_owned(recorded) else "unknown"
    if any(_listeners(port) for port in ports):
        result["cleanup"] = "unknown"
    return result


def run_browser_qa(broker: Any, iteration: int, candidate: dict[str, Any]) -> dict[str, Any]:
    qa = broker.spec["policy"].get("browser_qa")
    if not qa:
        raise ValueError("browser QA is not configured")
    if broker.candidate() != candidate:
        raise ValueError("browser QA candidate changed before the gate")
    checkout = broker.gate_checkout("verify", iteration, candidate).resolve(strict=True)
    cwd = (checkout / qa.get("cwd", ".")).resolve(strict=True)
    if checkout not in (cwd, *cwd.parents):
        raise ValueError("browser QA cwd escaped the gate checkout")
    ports = tuple(qa["ports"].values())
    key = f"browser_qa:{broker.spec['run_id']}:{iteration}"
    request = {
        "iteration": iteration,
        "candidate_id": candidate["id"],
        "policy_digest": broker.spec["policy_digest"],
        "qa_config_sha256": digest(qa),
        "ports": ports,
        "argv": qa["argv"],
    }
    evidence_dir = broker.state_dir / "browser-qa" / str(iteration)
    existing = _begin(broker, key, request)
    if existing:
        if existing["state"] == "unknown":
            return _reconcile_pending(evidence_dir, existing, ports)
        if broker.candidate() != candidate:
            raise ValueError("browser QA candidate changed after its receipt")
        receipt = Path(existing["receipt"])
        if _hash(receipt) != existing["receipt_sha256"]:
            raise ValueError("browser QA receipt changed after completion")
        return existing
    evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = evidence_dir.lstat()
    if stat.S_IMODE(metadata.st_mode) != 0o700 or metadata.st_uid != os.getuid():
        raise ValueError("browser QA evidence directory is not private")
    scratch = Path(tempfile.mkdtemp(prefix="dfqa-", dir="/private/tmp"))
    profile, env = prepare_browser_qa(broker.spec, checkout, evidence_dir, scratch, qa)
    _ports_free(ports)
    log = evidence_dir / "browser-qa.log"
    started = evidence_dir / "start.json"
    process_file = evidence_dir / "owned-processes.json"
    descriptor = os.open(log, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    process: subprocess.Popen[bytes] | None = None
    observed: dict[int, dict[str, Any]] = {}
    conflict = False
    timed_out = False
    cleanup = "unknown"
    owned: dict[int, dict[str, Any]] = {}
    try:
        with os.fdopen(descriptor, "wb") as output:
            process = subprocess.Popen(
                [
                    "/usr/bin/sandbox-exec",
                    "-f",
                    str(profile),
                    "/usr/bin/python3",
                    "-I",
                    "-c",
                    _START_GATE,
                    *qa["argv"],
                ],
                cwd=cwd,
                env=env,
                stdin=subprocess.PIPE,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            identity = _identity(process.pid)
            if not identity:
                raise RuntimeError("browser QA child has no process identity")
            owned[process.pid] = {"identity": identity, "ppid": os.getpid(), "pgid": process.pid}
            _write_new(
                started,
                (
                    canonical_json({"pid": process.pid, "identity": identity, "pgid": process.pid})
                    + "\n"
                ).encode(),
            )
            assert process.stdin is not None
            process.stdin.write(b"GO\n")
            process.stdin.flush()
            process.stdin.close()
            deadline = time.monotonic() + qa["timeout_seconds"]
            while process.poll() is None:
                owned = _sample_owned(process.pid, owned)
                _save_owned(process_file, owned)
                for port in ports:
                    for pid in _listeners(port):
                        if pid not in owned or _identity(pid) != owned[pid]["identity"]:
                            conflict = True
                            break
                        observed[port] = {"pid": pid, **owned[pid]}
                if conflict or time.monotonic() >= deadline:
                    timed_out = not conflict
                    break
                time.sleep(0.25)
            cleanup = "confirmed" if _stop_owned(owned) else "unknown"
            process.wait(timeout=5)
    except BaseException:
        if process is not None:
            _stop_owned(owned)
            process.wait(timeout=5)
        raise
    if any(_listeners(port) for port in ports):
        cleanup = "unknown"
    output = log.read_text(encoding="utf-8", errors="replace")
    numbers = re.findall(qa["test_count_regex"], output)
    count = sum(int(number) for number in numbers) if numbers else 0
    rejected = bool(qa.get("reject_regex") and re.search(qa["reject_regex"], output))
    rejected = rejected or bool(
        re.search(r"(?m)^\s*\d+\s+(?:failed|skipped|flaky|did not run)\b", output)
    )
    artifacts = _artifacts(checkout, qa.get("artifact_paths", []))
    source_unchanged = candidate_for(checkout)["id"] == candidate["id"]
    result = {
        "state": "passed"
        if process is not None
        and process.returncode == 0
        and not conflict
        and not timed_out
        and set(observed) == set(ports)
        and cleanup == "confirmed"
        and count >= qa["min_tests"]
        and not rejected
        and source_unchanged
        else "failed",
        "candidate_id": candidate["id"],
        "iteration": iteration,
        "qa_config_sha256": digest(qa),
        "argv": qa["argv"],
        "cwd": str(cwd),
        "ports": qa["ports"],
        "profile_sha256": _hash(profile),
        "start": json.loads(started.read_text(encoding="utf-8")),
        "listeners": {str(port): observed.get(port) for port in ports},
        "exit_code": process.returncode if process else None,
        "timed_out": timed_out,
        "listener_conflict": conflict,
        "test_count": count,
        "rejected_output": rejected,
        "log": str(log),
        "log_sha256": _hash(log),
        "artifacts": artifacts,
        "source_unchanged": source_unchanged,
        "cleanup": cleanup,
        "scratch": str(scratch),
    }
    receipt = evidence_dir / "receipt.json"
    _write_new(receipt, (canonical_json(result) + "\n").encode())
    result["receipt"] = str(receipt)
    result["receipt_sha256"] = _hash(receipt)
    broker._finish_effect(key, result)
    return result
