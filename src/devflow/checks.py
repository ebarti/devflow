"""Run a trusted check recipe and record process and assertion outcomes separately."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from devflow.errors import WorkflowError
from devflow.profiles import RepositoryProfile, digest


def now() -> str:
    return datetime.now(UTC).isoformat()


def _execute(argv, *, cwd, capture_output, text, timeout, check):
    """Give each check an owned process group so a timeout also stops its children."""
    with subprocess.Popen(
        argv,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    ) as process:
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.communicate()
            raise
        return subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)


def _junit(path: Path, directory: Path) -> tuple[int, int, int]:
    if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(directory):
        raise WorkflowError(
            "check_report_missing", "The check did not produce its owned JUnit report"
        )
    if path.stat().st_size > 16 * 1024 * 1024:
        raise WorkflowError("check_report_invalid", "JUnit report exceeds 16 MiB")
    try:
        tree = ET.fromstring(path.read_bytes())
    except ET.ParseError as exc:
        raise WorkflowError("check_report_invalid", "The JUnit report is not valid XML") from exc
    if tree.tag not in {"testsuite", "testsuites"}:
        raise WorkflowError("check_report_invalid", "Expected a JUnit test suite")
    cases = list(tree.iter("testcase"))
    skipped = sum(case.find("skipped") is not None for case in cases)
    failed = sum(
        case.find("failure") is not None or case.find("error") is not None for case in cases
    )
    # Collection/setup failures may occur outside testcase nodes.
    if any(node.tag in {"error", "failure"} for node in tree.iter()):
        failed = max(1, failed)
    return len(cases) - skipped, skipped, failed


def run_check(
    profile: RepositoryProfile,
    recipe_id: str,
    candidate: dict,
    *,
    acceptance_ids: list[str],
    state_dir: Path,
    put_artifact,
    environment_profile: str = "local",
    runner=None,
) -> dict:
    recipe = profile.recipe(recipe_id)
    runner = runner or _execute
    cwd = (profile.root / recipe.get("cwd", ".")).resolve(strict=True)
    if not cwd.is_relative_to(profile.root):
        raise WorkflowError("check_path_escape", "Check cwd resolves outside the repository")
    state_dir = Path(state_dir)
    if state_dir.is_symlink():
        raise WorkflowError("state_path_invalid", "Private check root must not be a symlink")
    check_root = state_dir / "checks"
    if check_root.is_symlink():
        raise WorkflowError("state_path_invalid", "Private checks directory must not be a symlink")
    check_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(check_root, 0o700)
    started, process, execution = now(), "not_started", "BLOCKED"
    observations, executed, skipped = [], 0, 0
    output = ""
    with tempfile.TemporaryDirectory(prefix="check-", dir=check_root) as temporary:
        directory = Path(temporary).resolve()
        report = directory / "junit.xml"
        argv = [a.replace("{report_path}", str(report)) for a in recipe["argv"]]
        try:
            result = runner(
                argv,
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=recipe.get("timeout_seconds", 300),
                check=False,
            )
            output = result.stdout + result.stderr
            process = "success" if result.returncode == 0 else "failure"
            if recipe["kind"] == "static":
                execution = "PASS" if result.returncode == 0 else "FAIL"
                observations = [
                    f"Static invariant: {recipe['description']}; exit {result.returncode}"
                ]
            else:
                executed, skipped, failed = _junit(report, directory)
                enough = executed >= recipe.get("min_executed", 1)
                allowed_skips = skipped <= recipe.get("max_skipped", 0)
                execution = (
                    "PASS"
                    if result.returncode == 0 and not failed and enough and allowed_skips
                    else "FAIL"
                )
                observations = [f"JUnit: {executed} executed, {skipped} skipped, {failed} failed"]
                if not enough or not allowed_skips:
                    observations.append("Required execution or skip threshold was not met")
        except (FileNotFoundError, PermissionError) as exc:
            observations = [f"Check could not start: {type(exc).__name__}"]
        except subprocess.TimeoutExpired:
            process = "interrupted"
            observations = ["Check exceeded its configured timeout"]
        except WorkflowError as exc:
            execution = "FAIL" if process == "failure" else "BLOCKED"
            observations = [str(exc)]
        artifact = {
            "recipe": recipe_id,
            "kind": recipe["kind"],
            "argv": argv,
            "process_status": process,
            "execution_status": execution,
            "output": output,
            "observations": observations,
        }
        if (
            report.is_file()
            and not report.is_symlink()
            and report.stat().st_size <= 16 * 1024 * 1024
        ):
            artifact["report_sha256"] = hashlib.sha256(report.read_bytes()).hexdigest()
            artifact["junit"] = report.read_text(errors="replace")
        artifact_hash = put_artifact(json.dumps(artifact, sort_keys=True).encode())
    return {
        "schema_version": 1,
        "record_type": "check_evidence",
        "evidence_id": f"E-{uuid4().hex}",
        "candidate_id": candidate["candidate_id"],
        "input_signature": digest(
            {
                "tree": candidate["tree_sha"],
                "recipe": recipe,
                "profile": profile.fingerprint,
                "dependency": candidate["dependency_hash"],
                "environment": candidate["environment_hash"],
                "acceptance_ids": acceptance_ids,
            }
        ),
        "recipe_id": recipe_id,
        "recipe_version": digest(recipe),
        "acceptance_ids": acceptance_ids,
        "argv": argv,
        "cwd": str(cwd),
        "environment_profile": environment_profile,
        "started_at": started,
        "ended_at": now(),
        "process_status": process,
        "execution_status": execution,
        "executed_assertions": executed,
        "skipped_assertions": skipped,
        "scenario_ids": list(recipe.get("scenarios", [])) if execution == "PASS" else [],
        "observations": observations,
        "artifact_hash": artifact_hash,
    }
