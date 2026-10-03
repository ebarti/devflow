"""Native owned browser/API checks with exact port and resource receipts."""
from __future__ import annotations

import hashlib
import os
import re
import socket
from pathlib import Path
from typing import Any

from .candidate import candidate_for
from .contracts import digest
from .delivery_output import observed_test_count, visible_output
from .delivery_sandbox import prepare_browser_qa, trusted_local


def _hash(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _write_new(path: Path, content: bytes) -> None:
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(content)


def _ports_free(ports: tuple[int, int]) -> None:
    for port in ports:
        from .delivery_native_process import listeners

        if listeners(port):
            raise RuntimeError("browser QA port is already owned by another process")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as guard:
            try:
                guard.bind(("127.0.0.1", port))
            except OSError as exc:
                raise RuntimeError("browser QA port is unavailable") from exc


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


def run_browser_qa(broker: Any, iteration: int, candidate: dict[str, Any]) -> dict[str, Any]:
    from .delivery_native_guard import validate_native_turn
    from .delivery_native_process import NativeProcess, reconcile_process
    from .delivery_preparation import verify_prepared_spec
    from .delivery_resources import RunResources, private_directory, read_private, write_private

    spec = broker.spec
    validate_native_turn(spec, "verify", iteration, broker.store)
    verify_prepared_spec(spec)
    qa = spec["policy"].get("browser_qa")
    if not qa or broker.candidate() != candidate:
        raise ValueError("native browser QA needs its admitted exact candidate and configuration")
    key = f"browser_qa:{spec['run_id']}:{iteration}"
    request = {
        "iteration": iteration,
        "candidate_id": candidate["id"],
        "policy_digest": spec["policy_digest"],
        "qa_config_sha256": digest(qa),
        "ports": qa["ports"],
        "argv": qa["argv"],
    }
    done = broker._effect(key, "browser_qa", request)
    if done:
        if _hash(Path(done["receipt"])) != done["receipt_sha256"]:
            raise ValueError("native browser receipt changed")
        return done
    checkout = broker.gate_checkout("verify", iteration, candidate).resolve(strict=True)
    cwd = (checkout / qa.get("cwd", ".")).resolve(strict=True)
    if checkout not in (cwd, *cwd.parents):
        raise ValueError("browser QA cwd escaped the gate checkout")
    folder = broker.state_dir / "browser-qa" / str(iteration)
    private_directory(folder)
    receipt = folder / "receipt.json"
    if receipt.exists():
        saved = read_private(receipt)
        if (
            any(saved.get(name) != value for name, value in request.items())
            or saved.get("state") not in {"passed", "failed"}
            or saved.get("cleanup") != "confirmed"
            or _hash(Path(saved["log"])) != saved["log_sha256"]
            or _hash(folder / "browser-qa.sb") != saved["profile_sha256"]
            or any(_hash(Path(item["path"])) != item["sha256"] for item in saved["artifacts"])
        ):
            raise ValueError("pending native browser receipt authority changed")
        process_receipt = reconcile_process(folder / "native" / "native-process.json")
        if process_receipt["cleanup"] == "unknown":
            return {
                "state": "unknown",
                "cleanup": "unknown",
                "candidate_id": candidate["id"],
                "native_process": process_receipt,
            }
        result = {**saved, "receipt": str(receipt), "receipt_sha256": _hash(receipt)}
        broker._finish_effect(key, result)
        return result
    ports = tuple(qa["ports"].values())
    if not (folder / "native" / "native-process.json").exists():
        _ports_free(ports)
    scratch = RunResources(spec).browser_scratch()
    profile, environment = prepare_browser_qa(spec, checkout, folder, scratch, qa)
    generated = broker._register_generated(checkout, qa.get("artifact_paths", []))
    process = NativeProcess(
        spec,
        folder / "native",
        argv=(list(qa["argv"]) if trusted_local(spec)
              else ["/usr/bin/sandbox-exec", "-f", str(profile), *qa["argv"]]),
        cwd=cwd,
        environment=environment,
        timeout=qa["timeout_seconds"],
        ports=ports,
        cancelled=broker._native_cancelled,
    ).run()
    broker._record_generated(generated)
    if process["cleanup"] == "unknown":
        return {
            "state": "unknown",
            "cleanup": "unknown",
            "candidate_id": candidate["id"],
            "native_process": process,
        }
    log = folder / "browser-qa.log"
    content = Path(process["log"]).read_bytes()
    if log.exists():
        if log.read_bytes() != content:
            raise ValueError("native browser log changed across replay")
    else:
        _write_new(log, content)
    output = visible_output(content.decode("utf-8", errors="replace"))
    count = observed_test_count(output, qa["test_count_regex"])
    rejected = bool(qa.get("reject_regex") and re.search(qa["reject_regex"], output))
    rejected = rejected or bool(
        re.search(r"(?m)^\s*\d+\s+(?:failed|skipped|flaky|did not run)\b", output)
    )
    artifacts = []
    for index, item in enumerate(_artifacts(checkout, qa.get("artifact_paths", []))):
        original = Path(item["path"])
        destination = folder / "artifacts" / str(index) / original.name
        private_directory(destination.parent)
        if not destination.exists():
            _write_new(destination, original.read_bytes())
        if _hash(destination) != item["sha256"]:
            raise ValueError("native browser artifact changed while preserving evidence")
        artifacts.append({**item, "source": str(original), "path": str(destination)})
    unchanged = candidate_for(checkout)["id"] == candidate["id"]
    passed = (
        process["exit_code"] == 0
        and not process["timed_out"]
        and not process["cancelled"]
        and not process["listener_conflict"]
        and set(process["observed_listeners"]) == {str(port) for port in ports}
        and count >= qa["min_tests"]
        and not rejected
        and unchanged
    )
    result = {
        **request,
        "state": "passed" if passed else "failed",
        "cwd": str(cwd),
        "profile_sha256": _hash(profile),
        "execution_mode": spec["policy"].get("host_sandbox", "native-profile"),
        "exit_code": process["exit_code"],
        "test_count": count,
        "rejected_output": rejected,
        "log": str(log),
        "log_sha256": _hash(log),
        "artifacts": artifacts,
        "source_unchanged": unchanged,
        "cleanup": "confirmed",
        "process_cleanup": process["cleanup"],
        "native_process": process,
        "scratch": str(scratch),
        "diagnostic": output[-2000:] if not passed else None,
    }
    write_private(receipt, result)
    result.update(receipt=str(receipt), receipt_sha256=_hash(receipt))
    broker._finish_effect(key, result)
    return result
