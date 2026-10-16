"""Native owned browser/API checks with exact port and resource receipts."""
from __future__ import annotations

import hashlib
import json
import os
import socket
from pathlib import Path
from typing import Any

from .candidate import candidate_for
from .contracts import digest
from .delivery_feature_gates import (
    GateAdmissionError,
    resolve_required_selectors,
    retain_admission_failure,
    selector_evidence,
)
from .delivery_output import observed_test_count, rejection_causes, visible_output
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



def _playwright_evidence(qa: dict, checkout: Path) -> tuple[list[str], dict[str, str]]:
    """Generate real success evidence for selected recipes, including frozen v2 inputs."""
    argv = list(qa["argv"])
    if not qa.get("required_selectors"):
        return argv, {}
    start = next((i + 2 for i in range(len(argv) - 1)
                  if argv[i:i + 2] == ["playwright", "test"]), None)
    if start is None:
        return argv, {}
    if not qa.get("artifact_paths"):
        raise ValueError("selected Playwright QA requires an owned artifact directory")
    # Registration pins this existing configured root. Reject earlier output
    # symlinks before the reporters or runner can write through them.
    _artifacts(checkout, qa["artifact_paths"])
    root = checkout / qa["artifact_paths"][0]
    clean, index = argv[:start], start
    while index < len(argv):
        if argv[index] in {"--reporter", "--trace", "--output"}:
            index += 2
        elif argv[index].split("=", 1)[0] in {"--reporter", "--trace", "--output"}:
            index += 1
        else:
            clean.append(argv[index])
            index += 1
    return ([*clean, "--reporter=line,json,junit", "--trace=on",
             "--output=" + str(root / "test-results")],
            {"PLAYWRIGHT_JUNIT_OUTPUT_FILE": str(root / "junit.xml")})


def run_browser_qa(broker: Any, iteration: int, candidate: dict[str, Any]) -> dict[str, Any]:
    from .delivery_native_guard import validate_native_turn
    from .delivery_native_process import NativeProcess
    from .delivery_preparation import verify_prepared_spec
    from .delivery_resources import RunResources, private_directory, read_private, write_private

    spec = broker.spec
    validate_native_turn(spec, "verify", iteration, broker.store)
    verify_prepared_spec(spec)
    qa = spec["policy"].get("browser_qa")
    if not qa or broker.candidate() != candidate:
        raise ValueError("native browser QA needs its admitted exact candidate and configuration")
    key = f"browser_qa:{spec['run_id']}:{iteration}" + broker.effect_namespace
    request = {
        "iteration": iteration,
        "candidate_id": candidate["id"],
        "policy_digest": spec["policy_digest"],
        "qa_config_sha256": digest(qa),
        "ports": qa["ports"],
        "argv": qa["argv"],
    }
    selector_sources = []
    if qa.get("required_selectors"):
        checkout = broker.gate_checkout("verify", iteration, candidate).resolve(strict=True)
        try:
            selector_sources = resolve_required_selectors(qa, checkout, spec=spec)
        except GateAdmissionError as exc:
            return retain_admission_failure(spec, qa, broker.evidence_dir / "browser-qa" /
                                            str(iteration), candidate, exc)
        request["selector_sources"] = selector_sources
    done = broker._effect(key, "browser_qa", request)
    if done:
        if _hash(Path(done["receipt"])) != done["receipt_sha256"]:
            raise ValueError("native browser receipt changed")
        return done
    checkout = broker.gate_checkout("verify", iteration, candidate).resolve(strict=True)
    cwd = (checkout / qa.get("cwd", ".")).resolve(strict=True)
    if checkout not in (cwd, *cwd.parents):
        raise ValueError("browser QA cwd escaped the gate checkout")
    folder = broker.evidence_dir / "browser-qa" / str(iteration)
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
        process_receipt = broker._reconcile_native_check(Path(
            (saved.get("native_process") or {}).get("journal")
            or folder / "native" / "native-process.json"))
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
    argv, reporter_environment = _playwright_evidence(qa, checkout)
    environment.update(reporter_environment)
    options = {
        "argv": (argv if trusted_local(spec)
                 else ["/usr/bin/sandbox-exec", "-f", str(profile), *argv]),
        "cwd": cwd, "environment": environment, "timeout": qa["timeout_seconds"],
        "ports": ports, "cancelled": broker._native_cancelled,
    }
    process = broker._run_native_check(NativeProcess(spec, folder / "native", **options))
    interrupted = []
    if process.get("interrupted_monitor") is True:
        # A lost monitor yields no verdict. One fresh invocation re-runs the
        # gate on the confirmed-clear ports; the interrupted journal stays as evidence.
        interrupted.append(process)
        if not (folder / "native-retry" / "native-process.json").exists():
            _ports_free(ports)
        process = broker._run_native_check(NativeProcess(spec, folder / "native-retry", **options))
    broker._record_generated(generated)
    if process["cleanup"] == "unknown":
        return {
            "state": "unknown",
            "cleanup": "unknown",
            "candidate_id": candidate["id"],
            "native_process": process,
        }
    if process.get("interrupted_monitor") is True:
        interrupted.append(process)
        return {
            "state": "unknown",
            "cleanup": "confirmed",
            "process_cleanup": process["cleanup"],
            "candidate_id": candidate["id"],
            "reason": "native browser QA monitor was lost twice; the gate produced no verdict",
            "native_process": process,
            "interrupted_native_processes": interrupted,
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
    causes = rejection_causes(output, [
        *([qa["reject_regex"]] if qa.get("reject_regex") else []),
        r"(?m)^\s*\d+\s+(?:failed|skipped|flaky|did not run)\b",
    ], test_results=True)
    rejected = bool(causes)
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
    selector_receipts = None
    selector_failure = None
    if selector_sources:
        try:
            selector_receipts = selector_evidence(qa, checkout, selector_sources, playwright=output)
        except (ValueError, OSError) as exc:
            selector_failure = str(exc)
    unchanged = candidate_for(checkout)["id"] == candidate["id"]
    passed = (
        process["exit_code"] == 0
        and not process["timed_out"]
        and not process["cancelled"]
        and not process["listener_conflict"]
        and set(process["observed_listeners"]) == {str(port) for port in ports}
        and count >= qa["min_tests"]
        and not rejected
        and selector_failure is None
        and unchanged
    )
    result = {
        **request,
        "state": "passed" if passed else "failed",
        "cwd": str(cwd),
        "effective_argv": argv,
        "reporter_environment": reporter_environment,
        "profile_sha256": _hash(profile),
        "execution_mode": spec["policy"].get("host_sandbox", "native-profile"),
        "exit_code": process["exit_code"],
        "test_count": count,
        **({"selectors": selector_receipts} if selector_receipts is not None else {}),
        **({"evidence_failure": selector_failure} if selector_failure else {}),
        "rejected_output": rejected,
        "rejection_causes": causes,
        "log": str(log),
        "log_sha256": _hash(log),
        "artifacts": artifacts,
        "source_unchanged": unchanged,
        "cleanup": "confirmed",
        "process_cleanup": process["cleanup"],
        "native_process": process,
        **({"interrupted_native_processes": interrupted} if interrupted else {}),
        "scratch": str(scratch),
        "diagnostic": output[-2000:] if not passed else None,
    }
    write_private(receipt, result)
    result.update(receipt=str(receipt), receipt_sha256=_hash(receipt))
    broker._finish_effect(key, result)
    return result


def browser_artifact_possible(store, spec, row):
    """Read-only routing hint; the admission separately authenticates the cause."""
    if (spec.get("provider") != "codex" or spec.get("gate_retry_generation") is not None
            or spec.get("feature_worker", {}).get("kind") != "chunk"
            or row.get("error") != "implementer did not establish a pass"):
        return False
    from .delivery_feature_revision_subject import _private_receipt

    with store._connect() as db:
        attempts = list(db.execute("SELECT job_key FROM delivery_attempts WHERE run_id=? "
            "AND role='implement' AND iteration=? AND state='finished'",
            (spec["run_id"], row["iteration"])))
    if (len(attempts) != 1 or not isinstance(attempts[0][0], str)
            or len(attempts[0][0]) != 64
            or any(char not in "0123456789abcdef" for char in attempts[0][0])):
        return False
    request = _private_receipt(Path(spec["state_dir"]) / "attempts" / attempts[0][0]
                               / "request.json", canonical=True)
    checks = (request.get("evidence_context") or {}).get("checks") or {}
    findings = request.get("findings") or []
    return (request.get("spec") == spec and bool(findings)
            and all(isinstance(finding, str) and "junit" in finding.lower()
                    and "trace" in finding.lower() for finding in findings)
            and checks.get("local", {}).get("state") == "passed"
            and checks.get("browser_qa", {}).get("state") == "passed"
            and checks.get("qa", {}).get("state") == "failed")


def observe_browser_artifacts(
    store, spec, row, state, attempts, closed, transport, broker, *, resource_spec=None,
):
    """Retain a native rejection; authorize only fresh gates for absent browser artifacts."""
    from .delivery_check_evidence import _owned_bytes
    from .delivery_feature_gates import selector_evidence
    from .delivery_feature_revision_recovery import gate_evidence
    from .delivery_feature_revision_subject import (
        _private_receipt,
        _runtime,
        bounded_findings,
        full_candidate,
    )
    from .delivery_gate_attribution_recovery import _original_progress, eligible_stop

    if (transport is not None or not eligible_stop(spec, state)
            or spec.get("provider") != "codex" or spec.get("gate_selections_version") != 2):
        raise ValueError("browser artifact retry requires its original selected native worker")
    session, progress, proof = _original_progress(
        store, spec, row, state, attempts, closed, resource_spec=resource_spec)
    candidate = full_candidate(broker.candidate())
    latest = next(r for r in reversed(state["roles"]) if r.get("role") == "implement")
    passed = gate_evidence({"state": state, "candidate": candidate, "session_id": session,
                            "attempts": attempts})
    matches = [a for a in attempts if a["role"] == "implement"
               and a["iteration"] == latest["iteration"] and a["session_id"] == session]
    if (not passed or len(matches) != 1 or latest.get("iteration") != state["iteration"]
            or latest.get("finish_reason") != "done" or latest.get("cleanup") != "confirmed"
            or latest.get("session_id") != session
            or full_candidate(latest.get("candidate")) != candidate):
        raise ValueError("browser artifact retry cannot approve changed or unproved source")
    request = _private_receipt(Path(spec["state_dir"]) / "attempts" / matches[0]["job_key"]
                               / "request.json", canonical=True)
    context = request.get("evidence_context") or {}
    checks = context.get("checks") or {}
    prior = context.get("iteration")
    verifier = [r for r in state["roles"] if r.get("role") == "verify"
                and r.get("iteration") == prior and r.get("status") != "pass"]
    findings = bounded_findings(request.get("findings"))
    latest_findings = bounded_findings(latest.get("findings"))
    qa = spec["policy"].get("browser_qa") or {}
    browser = checks.get("browser_qa") or {}
    if (digest(request) != proof["request_sha256"] or len(verifier) != 1
            or verifier[0].get("findings") != findings
            or full_candidate(verifier[0].get("candidate")) != candidate
            or not all(all(token in finding.lower() for token in ("junit", "trace"))
                       for finding in [*findings, *latest_findings])
            or not all("controller" in finding.lower() for finding in latest_findings)
            or not all(token in spec.get("accepted_plan", "").lower()
                       for token in ("junit", "trace"))
            or full_candidate(context.get("candidate")) != candidate
            or request.get("candidate", {}).get("id") != candidate["id"]
            or not qa.get("required_selectors") or not qa.get("artifact_paths")
            or "--reporter=line,json" not in qa["argv"]
            or not any(qa["argv"][i:i + 2] == ["playwright", "test"]
                       for i in range(len(qa["argv"]) - 1))
            or checks.get("prepublish", {}).get("state") != "passed"
            or checks.get("prepublish", {}).get("source_unchanged") is not True
            or checks.get("local", {}).get("state") != "passed"
            or checks.get("local", {}).get("candidate_id") != candidate["id"]
            or checks.get("local", {}).get("source_unchanged") is not True
            or checks.get("review", {}).get("state") != "passed"
            or checks.get("qa", {}).get("state") != "failed"
            or checks.get("qa", {}).get("candidate_id") != candidate["id"]
            or browser.get("state") != "passed" or browser.get("cleanup") != "confirmed"
            or browser.get("source_unchanged") is not True
            or browser.get("candidate_id") != candidate["id"]):
        raise ValueError("browser artifact retry lost its exact native missing-artifact finding")
    root = Path(spec["state_dir"])
    folder = root / "browser-qa" / str(prior)
    receipt = folder / "receipt.json"
    raw = _owned_bytes(receipt)
    _private_receipt(receipt, canonical=True)
    if (browser.get("receipt") != str(receipt)
            or hashlib.sha256(raw).hexdigest() != browser.get("receipt_sha256")
            or json.loads(raw) != {k: v for k, v in browser.items()
                                  if k not in {"receipt", "receipt_sha256"}}
            or browser.get("argv") != qa["argv"]
            or browser.get("qa_config_sha256") != digest(qa)
            or browser.get("effective_argv", qa["argv"]) != qa["argv"]
            or browser.get("log") != str(folder / "browser-qa.log")
            or browser.get("cwd") != str(root / "gates" / str(prior) / "verify"
                                         / qa.get("cwd", "."))):
        raise ValueError("browser artifact retry lost its retained frozen browser receipt")
    if ((folder / "browser-qa.log").stat().st_size > 8 * 1024 * 1024
            or (folder / "browser-qa.log").stat().st_mode & 0o777 != 0o600):
        raise ValueError("browser artifact retry log exceeds its retained evidence bound")
    log = _owned_bytes(folder / "browser-qa.log")
    native = browser.get("native_process") or {}
    journal = _private_receipt(folder / "native/native-process.json")
    launch = _private_receipt(folder / "native/launch.json")
    if (hashlib.sha256(log).hexdigest() != browser.get("log_sha256")
            or native.get("journal") != str(folder / "native/native-process.json")
            or journal.get("phase") != "finished"
            or journal.get("monitoring_complete") is not True
            or journal.get("runtime_identity") != native.get("runtime_identity")
            or journal.get("result") != native or native.get("exit_code") != 0
            or native.get("cleanup") != "observed-native-confirmed"
            or native.get("monitoring_complete") is not True
            or native.get("stdio_drained") is not True
            or any(native.get(k) is not False
                   for k in ("cancelled", "timed_out", "listener_conflict"))
            or journal.get("intent", {}).get("run_id") != spec["run_id"]
            or journal.get("intent", {}).get("policy_digest") != spec["policy_digest"]
            or journal.get("intent", {}).get("cwd") != browser.get("cwd")
            or set(journal.get("intent", {}).get("ports", [])) != set(qa["ports"].values())
            or set(native.get("observed_listeners", {})) != {str(p) for p in qa["ports"].values()}
            or journal.get("intent", {}).get("argv") != qa["argv"]
            or launch.get("argv") != qa["argv"]
            or journal["intent"].get("environment_sha256") != digest(launch.get("environment"))):
        raise ValueError("browser artifact retry lost its completed frozen native invocation")
    sources = resolve_required_selectors(qa, broker.checkout, spec=spec)
    selected = selector_evidence(qa, broker.checkout, sources,
                                 playwright=visible_output(log.decode("utf-8", errors="replace")))
    fields = ("selector", "sha256", "passed", "failed", "skipped")
    if [{k: item[k] for k in fields} for item in selected] != [
            {k: item[k] for k in fields} for item in browser.get("selectors", [])]:
        raise ValueError("browser artifact retry lost its exact selected source evidence")
    artifacts = browser.get("artifacts") or []
    if (not 1 <= len(artifacts) <= 128
            or any(Path(item["path"]).suffix == ".xml"
                   or Path(item["path"]).name == "trace.zip" for item in artifacts)):
        raise ValueError("browser artifact retry does not prove absent JUnit and traces")
    for item in artifacts:
        path = Path(item["path"])
        if (not path.is_relative_to(folder / "artifacts")
                or not 0 <= item["size"] <= 50 * 1024 * 1024
                or path.stat().st_mode & 0o777 != 0o600
                or len(content := _owned_bytes(path)) != item["size"]
                or hashlib.sha256(content).hexdigest() != item["sha256"]):
            raise ValueError("browser artifact retry retained artifact custody changed")
    return {"cause": "missing_selected_browser_artifacts", "progress": progress,
            "implementation_evidence": passed, "latest_assessment": proof,
            "unpassed_status": latest["status"],
            "unresolved_findings": [*findings, *latest_findings],
            "browser_receipt_sha256": browser["receipt_sha256"],
            "check_runtime": _runtime(native.get("runtime_identity")),
            "selected_source_evidence": selected}
