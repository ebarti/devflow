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


# Full, every-page PDF evidence has many small files. Bound the aggregate bytes
# as well as the entry count, instead of treating 128 files as a failed runtime.
MAX_BROWSER_ARTIFACTS = 2048
MAX_BROWSER_ARTIFACT_BYTES = 50 * 1024 * 1024


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
    total_bytes = 0
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
            size = path.stat().st_size
            if (len(found) >= MAX_BROWSER_ARTIFACTS
                    or size > MAX_BROWSER_ARTIFACT_BYTES - total_bytes):
                raise ValueError("browser QA artifact set exceeds the evidence bound")
            total_bytes += size
            found.append({"path": str(path), "sha256": _hash(path), "size": size})
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


def run_browser_qa(broker: Any, iteration: int, candidate: dict[str, Any], *,
                   reconcile_only: bool = False) -> dict[str, Any]:
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
    folder = broker.evidence_dir / "browser-qa" / str(iteration)
    if reconcile_only:
        original = read_private(folder / "native" / "native-process.json")
        if (original.get("phase") != "finished"
                or original.get("monitoring_complete") is not True
                or not isinstance(original.get("result"), dict)
                or original["result"].get("interrupted_monitor") is True):
            raise ValueError("browser reconciliation requires a completed native verdict")
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
    native = NativeProcess(spec, folder / "native", **options)
    if reconcile_only:
        if read_private(native.journal)["intent"] != native._intent():
            raise ValueError("completed browser native authority changed")
        observed = broker._reconcile_native_check(native.journal)
        if observed["cleanup"] != "observed-native-confirmed":
            raise ValueError("completed browser cleanup remains unknown")
        process = read_private(native.journal)["result"]
    else:
        process = broker._run_native_check(native)
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
    evidence_failures = []
    try:
        for index, item in enumerate(_artifacts(checkout, qa.get("artifact_paths", []))):
            original = Path(item["path"])
            destination = folder / "artifacts" / str(index) / original.name
            private_directory(destination.parent)
            if not destination.exists():
                _write_new(destination, original.read_bytes())
            if _hash(destination) != item["sha256"]:
                raise ValueError("native browser artifact changed while preserving evidence")
            artifacts.append({**item, "source": str(original), "path": str(destination)})
    except (ValueError, OSError) as exc:
        # Supervision already established cleanup above. A rejected evidence
        # set fails this gate; it cannot erase the completed process result.
        evidence_failures.append(f"browser artifact collection failed: {exc}")
    selector_receipts = None
    selector_failure = None
    if selector_sources:
        try:
            selector_receipts = selector_evidence(qa, checkout, selector_sources, playwright=output)
        except (ValueError, OSError) as exc:
            selector_failure = str(exc)
    if selector_failure:
        evidence_failures.append(selector_failure)
    unchanged = candidate_for(checkout)["id"] == candidate["id"]
    passed = (
        process["exit_code"] == 0
        and not process["timed_out"]
        and not process["cancelled"]
        and not process["listener_conflict"]
        and set(process["observed_listeners"]) == {str(port) for port in ports}
        and count >= qa["min_tests"]
        and not rejected
        and not evidence_failures
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
        **({"evidence_failure": "; ".join(evidence_failures)} if evidence_failures else {}),
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
        "diagnostic": "\n".join([*evidence_failures, output[-2000:]]) if not passed else None,
    }
    write_private(receipt, result)
    result.update(receipt=str(receipt), receipt_sha256=_hash(receipt))
    broker._finish_effect(key, result)
    return result


def reconcile_browser_attempt(store, run_id: str, iteration: int) -> dict[str, Any]:
    """Finish evidence for one closed browser invocation, without executing it again."""
    from .delivery_automatic_retry import _close_cleanup
    from .delivery_broker import DeliveryBroker
    from .delivery_interrupted_recovery import installation_recovery_lease
    from .delivery_policy_recovery import reconciled_cleanup

    with installation_recovery_lease(store.config):
        with store._connect() as db:
            saved = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
        if saved is None:
            raise ValueError("browser reconciliation run is absent")
        row = dict(saved)
        spec = store.effective_spec(run_id)
        checks = json.loads(row["checks_json"] or "{}")
        historical = checks.get("browser_qa") or {}
        if (spec.get("provider") != "codex" or not store.owns_execution(spec)
                or spec.get("feature_worker") or spec.get("feature_delivery")
                or row["phase"] != row["outcome"] or row["phase"] != "blocked"
                or row["execution_state"] != "blocked"
                or type(iteration) is not int or row["iteration"] != iteration
                or row["error"] != "browser QA child cleanup is unknown"
                or historical.get("state") != historical.get("cleanup")
                or historical.get("state") != "unknown"
                or historical.get("reason") != "ValueError" or not row["pr_json"]):
            raise ValueError("browser reconciliation requires its closed postprocessing failure")
        closed = store._completed_temporal_result(run_id, workflow_id=row["workflow_id"])
        state = closed["result"]
        recovery = json.loads(row["recovery_json"]) if row["recovery_json"] else None
        if (closed["workflow_id"] != row["workflow_id"]
                or not closed["execution_run_id"] or not closed["closed_at"]
                or closed["request_digest"] != row["request_digest"]
                or closed["recovery_digest"] != (digest(recovery) if recovery else None)
                or state.get("run_id") != run_id or state.get("revision") != row["protocol_revision"]
                or any(state.get(key) != row[key] for key in
                       ("phase", "outcome", "execution_state", "iteration", "error"))
                or {k: v for k, v in state.get("checks", {}).items() if k != "resource_cleanup"}
                != {k: v for k, v in checks.items() if k != "resource_cleanup"}
                or state.get("candidate") != json.loads(row["candidate_json"])
                or state.get("pull_request") != json.loads(row["pr_json"])
                or state.get("tracker") != json.loads(row["tracker_json"] or "null")):
            raise ValueError("browser reconciliation lost its exact closed execution")
        broker = DeliveryBroker(store, spec)
        if broker.evidence_dir != broker.state_dir:
            namespace = str(broker.evidence_dir.relative_to(broker.state_dir).parent)
            broker.effect_namespace = ":" + namespace
        key = f"browser_qa:{run_id}:{iteration}" + broker.effect_namespace
        with store._connect() as db:
            effect = db.execute("SELECT * FROM delivery_effects WHERE effect_key=?", (key,)).fetchone()
            busy = (
                db.execute("SELECT 1 FROM delivery_effects WHERE run_id=? AND effect_key!=? "
                           "AND state!='complete'", (run_id, key)).fetchone()
                or db.execute("SELECT 1 FROM delivery_attempts WHERE run_id=? AND state!='finished'",
                              (run_id,)).fetchone()
                or db.execute("SELECT 1 FROM delivery_mutations WHERE run_id=? AND "
                              "(state!='complete' OR kind='cancel')", (run_id,)).fetchone()
            )
        candidate = state["candidate"]
        if (busy or effect is None or effect["run_id"] != run_id or effect["kind"] != "browser_qa"
                or effect["state"] not in {"pending", "complete"}
                or broker.candidate() != candidate):
            raise ValueError("browser reconciliation has changed ownership or candidate")
        if row["cleanup"] == "confirmed":
            if (effect["state"] != "complete"
                    or reconciled_cleanup(store, row, state, closed=closed) is None):
                raise ValueError("browser cleanup has no authenticated later observation")
            result = json.loads(effect["observed_json"])
            if _hash(Path(result["receipt"])) != result["receipt_sha256"]:
                raise ValueError("reconciled browser receipt changed")
            return {"state": "finished", "cleanup": "confirmed", "changed": False,
                    "run_id": run_id, "iteration": iteration, "gate_state": result["state"]}
        if row["cleanup"] != "unknown":
            raise ValueError("browser reconciliation has an unexpected cleanup state")
        gate = broker._gate_path("verify", iteration)
        if (not gate.is_dir() or gate.resolve(strict=True) != gate
                or candidate_for(gate)["id"] != candidate["id"]):
            raise ValueError("browser reconciliation lost the original gate checkout")
        result = broker.run_browser_qa(iteration, candidate, reconcile_only=True)
        updated = _close_cleanup(store, row, spec, closed, lambda: False, published=True)
        return {"state": "finished", "cleanup": updated["cleanup"], "changed": True,
                "run_id": run_id, "iteration": iteration, "gate_state": result["state"],
                "test_count": result["test_count"], "receipt": result["receipt"],
                "receipt_sha256": result["receipt_sha256"]}


def browser_artifact_possible(store, spec, row):
    """Read-only routing hint; the admission separately authenticates the cause."""
    if spec.get("gate_retry_generation") is not None:
        previous = json.loads(row.get("recovery_json") or "null") or {}
        checks = json.loads(row.get("checks_json") or "{}")
        return (spec.get("provider") == "codex"
                and spec.get("feature_worker", {}).get("kind") == "chunk"
                and spec.get("gate_retry_generation") == 1
                and spec.get("gate_retry_stage") == "published"
                and previous.get("execution_spec") == spec
                and previous.get("seal", {}).get("browser_artifact_observation", {}).get("cause")
                == "missing_selected_browser_artifacts"
                and row.get("error") == "repair limit exhausted"
                and checks.get("review", {}).get("state") == "failed"
                and "browser_qa" not in checks)
    if (spec.get("provider") != "codex"
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

    if spec.get("gate_retry_generation") is not None:
        return _observe_browser_ordering(store, spec, row, state, attempts, closed, transport,
                                         broker, resource_spec=resource_spec)
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
    gate = root / "gates" / str(prior) / "verify"
    if (gate.resolve(strict=True) != gate
            or any(value != candidate[key] for key, value in candidate_for(gate).items())):
        raise ValueError("browser artifact retry lost its exact historical gate candidate")
    current = resolve_required_selectors(qa, broker.checkout, spec=spec)
    sources = resolve_required_selectors(qa, gate, spec=spec)
    if [(item["selector"], item["sha256"]) for item in current] != [
            (item["selector"], item["sha256"]) for item in sources]:
        raise ValueError("browser artifact retry historical and current selected source differ")
    selected = selector_evidence(qa, gate, sources,
                                 playwright=visible_output(log.decode("utf-8", errors="replace")))
    fields = ("selector", "path", "sha256", "passed", "failed", "skipped")
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


def browser_ordering_authority(spec, recovery):
    """The one successor of an authenticated browser-artifact ordering failure."""
    from .delivery_continuation import continuation_authority

    previous = recovery.get("original_recovery") or {}
    before = previous.get("execution_spec") or {}
    proof = recovery.get("seal", {}).get("browser_artifact_observation") or {}
    if (recovery.get("kind") != "published_gate_retry"
            or proof.get("cause") != "browser_artifact_assessment_order"
            or spec.get("gate_retry_stage") != "published"
            or spec.get("gate_retry_generation") != 2
            or spec.get("gate_retry_preflight_generation")
            or spec.get("gate_retry_prepared_handoff")
            or before.get("gate_retry_stage") != "published"
            or before.get("gate_retry_generation") != 1
            or previous.get("original_recovery") is not None
            or previous.get("seal", {}).get("browser_artifact_observation", {}).get("cause")
            != "missing_selected_browser_artifacts"
            or recovery.get("command", {}).get("additional_iterations") != 0
            or recovery["seal"].get("previous_digest") != digest(previous)
            or recovery["seal"].get("prior_gate_digest") != digest(previous)
            or recovery["seal"].get("original_spec") != before
            or recovery["state"] != recovery["seal"]["closed"]["result"]
            or continuation_authority(spec["policy"]) != continuation_authority(before["policy"])
            or any(spec.get(key) != before.get(key) for key in (
                "run_id", "work_id", "state_dir", "checkout", "source_path", "branch",
                "base_sha", "accepted_plan", "feature_worker", "feature_delivery",
                "feature_plan_revision", "config_digest", "plan_approval", "authorized_endpoint",
                "origin_url", "base_ref", "publication_base_ref", "expected_paths",
                "implementation_role_home_generation", "provider"))):
        raise ValueError("browser ordering retry changed its singleton assessment authority")
    return previous, before, proof


def _observe_browser_ordering(
    store, spec, row, state, attempts, closed, previous, broker, *, resource_spec=None,
):
    """Authenticate a review that ran before its required browser producer."""
    import asyncio
    from concurrent.futures import ThreadPoolExecutor
    from datetime import UTC

    from . import delivery_gate_retry as gates
    from .delivery_closures import sealed_closure
    from .delivery_codec import DELIVERY_DATA_CONVERTER
    from .delivery_feature_closure import completed_worker
    from .delivery_feature_revision_subject import _native_role, bounded_findings
    from .delivery_resources import _gate_roots

    prior = previous or {}
    old = prior.get("seal", {}).get("browser_artifact_observation") or {}
    original = prior.get("state") or {}
    candidate = broker.candidate()
    checks = state.get("checks") or {}
    roles = state.get("roles") or []
    if (spec.get("provider") != "codex" or spec.get("gate_selections_version") != 2
            or spec.get("gate_retry_generation") != 1
            or spec.get("gate_retry_stage") != "published"
            or spec.get("gate_retry_preflight_generation")
            or spec.get("gate_retry_prepared_handoff")
            or prior.get("kind") != gates.KIND or prior.get("execution_spec") != spec
            or prior.get("original_recovery") is not None
            or old.get("cause") != "missing_selected_browser_artifacts"
            or prior.get("command", {}).get("additional_iterations") != 0
            or state.get("candidate") != candidate
            or state.get("pull_request") != prior.get("publication")
            or state.get("iteration") != original.get("iteration")
            or roles[:-1] != original.get("roles") or not roles
            or state.get("error") != "repair limit exhausted"
            or checks.get("failure") != {"classification": "terminal", "stage": "review",
                                         "reason": "repair limit exhausted"}
            or any(key in checks for key in ("browser_qa", "qa", "ci"))
            or any(checks.get(stage, {}).get("state") != "passed"
                   or checks[stage].get("candidate_id") != candidate["id"]
                   or checks[stage].get("source_unchanged") is not True
                   for stage in ("prepublish", "local"))):
        raise ValueError("browser ordering retry lost its exact closed gate-only assessment")
    review = roles[-1]
    findings = bounded_findings(review.get("findings"))
    if (review.get("role") != "review" or review.get("iteration") != state["iteration"]
            or review.get("status") not in {"findings", "blocked"}
            or review.get("finish_reason") != "done" or review.get("cleanup") != "confirmed"
            or review.get("candidate") != candidate or review.get("planning_defect")
            or not all("junit" in text.lower() and "trace" in text.lower() for text in findings)
            or checks.get("review", {}).get("state") != "failed"
            or checks["review"].get("candidate_id") != candidate["id"]):
        raise ValueError("browser ordering retry lost its native missing-artifact rejection")
    old_attempts = prior["seal"]["attempts"]
    current = {a["job_key"]: a for a in attempts}
    added = [a for a in attempts if a["job_key"] not in {a["job_key"] for a in old_attempts}]
    with store._connect() as db:
        effects = [dict(e) for e in db.execute(
            "SELECT * FROM delivery_effects WHERE run_id=? ORDER BY effect_key", (spec["run_id"],))]
    if (len(current) != len(attempts) or len(added) != 1
            or any(current.get(a["job_key"]) != a for a in old_attempts)
            or effects != prior["seal"]["effects"]):
        raise ValueError("browser ordering retry changed its complete original native ledger")
    latest = resource_spec or spec
    gates.readback(store, spec, prior, resource_spec=latest)
    bindings = {}
    _gate_roots(latest, execution_specs=bindings)
    root = Path(spec["state_dir"]) / gates.gate_namespace(1, "published") / "evidence"
    if bindings.get(root) != spec:
        raise ValueError("browser ordering retry lost its exact first assessment namespace")

    async def read():
        sealed = await sealed_closure(store, closed["workflow_id"])
        if sealed is None:
            raise ValueError("browser ordering retry requires its sealed completed history")
        authenticated = await completed_worker(sealed["history"], store, spec,
            {**row, "recovery_json": json.dumps(prior)}, closed)
        calls, done, times = {}, {}, {}
        names = {"delivery_precheck": "prepublish", "delivery_checks": "local",
                 "delivery_role": "review"}
        for event in sealed["history"].events:
            times[event.event_id] = event.event_time.ToDatetime(tzinfo=UTC)
            if event.HasField("activity_task_scheduled_event_attributes"):
                activity = event.activity_task_scheduled_event_attributes
                if activity.activity_type.name in {"delivery_browser_qa", "delivery_publish"}:
                    raise ValueError(
                        "browser ordering retry cannot replace produced browser or code")
                if activity.activity_type.name in names:
                    values = await DELIVERY_DATA_CONVERTER.decode(activity.input.payloads)
                    common = {"spec": spec, "candidate": candidate,
                              "iteration": state["iteration"]}
                    if len(values) != 1 or any(values[0].get(k) != v for k, v in common.items()):
                        raise ValueError("browser ordering retry scheduled another candidate")
                    request = values[0]
                    if activity.activity_type.name == "delivery_role":
                        brief = {"historical_assessment_remains_rejected": True,
                                 "unresolved_findings": old["unresolved_findings"],
                                 "controller_browser_artifact_proof": old}
                        if (request.get("role") != "review"
                                or request.get("resume_session") is not None
                                or request.get("qa_evidence") is not None
                                or request.get("check_evidence") != checks["local"]
                                or request.get("findings") != ["Operator acceptance criteria "
                                    "(requirements to assess, not evidence of success): "
                                    + json.dumps(brief, sort_keys=True)]
                                or request.get("evidence_context") != {"role_artifacts": [
                                    r["role_artifacts"] for r in original["roles"]
                                    if r.get("role_artifacts") and r.get("role") == "implement"
                                    and r.get("iteration") == state["iteration"]]}):
                            raise ValueError(
                                "browser ordering review lost its rejected predecessor")
                    elif request != common:
                        raise ValueError("browser ordering retry changed its check request")
                    calls[event.event_id] = (activity.activity_type.name, request)
            elif event.HasField("activity_task_completed_event_attributes"):
                activity = event.activity_task_completed_event_attributes
                if activity.scheduled_event_id in calls:
                    done[activity.scheduled_event_id] = (event.event_id,
                        await DELIVERY_DATA_CONVERTER.decode(activity.result.payloads))
        ids = list(calls)
        if ([calls[i][0] for i in ids] != list(names) or set(ids) != set(done)
                or any(done[i][1] != [review if calls[i][0] == "delivery_role"
                                     else checks[names[calls[i][0]]]] for i in ids)
                or any(not start < done[start][0] < stop
                       for start, stop in zip(ids, ids[1:], strict=False))):
            raise ValueError("browser ordering retry lost its exact completed gate sequence")
        event = ids[-1]
        session = next(r["session_id"] for r in reversed(original["roles"])
                       if r.get("role") == "implement")
        proof = _native_role(spec, review, added[0], session, times[event],
                             times[done[event][0]], candidate, resource_spec=latest,
                             scheduled_input=calls[event][1])
        return {"history_sha256": authenticated["history_sha256"], "review": proof}

    with ThreadPoolExecutor(max_workers=1) as pool:
        progress = pool.submit(lambda: asyncio.run(read())).result(35)
    return {"cause": "browser_artifact_assessment_order", "progress": progress,
            "original_observation_sha256": digest(old), "unpassed_status": review["status"],
            "unresolved_findings": [*old["unresolved_findings"], *findings],
            "check_runtime": progress["review"]["runtime_identity"]}
