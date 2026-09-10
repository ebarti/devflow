"""Command-line boundary for recorded workflow actions and host adapters."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from decimal import Decimal
from pathlib import Path

from devflow import __version__
from devflow.errors import WorkflowError


def _request(path: str | None) -> dict:
    if not path:
        return {}
    try:
        text = sys.stdin.read() if path == "-" else Path(path).read_text()
        value = json.loads(text, parse_float=Decimal)
    except (OSError, UnicodeError, ValueError) as exc:
        raise WorkflowError("invalid_request", "Cannot read a valid JSON request") from exc
    if not isinstance(value, dict):
        raise WorkflowError("invalid_request", "Request must be a JSON object")
    return value


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        prog="devflow", description="Versioned development workflow and execution evidence"
    )
    result.add_argument("--version", action="version", version=__version__)
    result.add_argument(
        "command",
        help="doctor, backlog, work, next, candidate, check, gate, finding, "
        "fix, action, assignment, host, artifact, profile, usage, outcome, "
        "report, install, validate, or deliver",
    )
    result.add_argument("action", nargs="?", help="Subcommand, such as ready, record, or run")
    result.add_argument("--request-file", help="Structured JSON input file, or - for stdin")
    result.add_argument("--work-id", help="Work identity for reads and reconciliation")
    result.add_argument("--repository", type=Path, default=Path.cwd())
    result.add_argument(
        "--release-root",
        type=Path,
        default=Path("~/.local/share/devflow").expanduser(),
        help="Managed installation root for immutable workflow releases",
    )
    result.add_argument(
        "--state-dir",
        type=Path,
        default=Path(os.environ.get("DEVFLOW_STATE_DIR", "~/.local/state/devflow")).expanduser(),
    )
    result.add_argument("--json", action="store_true", help="Emit a machine-readable JSON envelope")
    result.add_argument("--file", type=Path, help="Artifact or JSONL input file")
    result.add_argument(
        "--approved-path",
        action="append",
        type=Path,
        default=[],
        help="Explicitly managed installer target; may be repeated",
    )
    result.add_argument(
        "--approved-root", type=Path, help="Explicitly managed installer release root"
    )
    result.add_argument(
        "--approved-plan-id", help="Independently selected installation plan digest"
    )
    return result


def _service(args):
    from devflow.application.commands import WorkflowService
    from devflow.profiles import load_profile

    repository = None
    if (args.repository / ".devflow").exists():
        repository = load_profile(args.repository).repository["repository"]["id"]
    return WorkflowService(args.state_dir, repository=repository)


def _state(service, request, args):
    work_id = request.get("work_id") or args.work_id
    if not work_id:
        raise WorkflowError("invalid_request", "A work_id is required")
    return service.snapshot(work_id)


def _doctor(args):
    from devflow.profiles import load_profile
    from devflow.runtime import selected_runtime

    tools = {name: shutil.which(name) is not None for name in ("git", "gh", "uv", "npx")}
    result = {
        "package_version": __version__,
        "python": sys.version.split()[0],
        "tools": tools,
        "state_exists": (args.state_dir / "state.sqlite3").exists(),
        "native_host": "owner-provided native task tools required",
        "automatic_merge": "requires live protection and adapter conformance",
        "execution_admission": "trusted_intake_unavailable",
        "human_validation": "human_validation_unavailable",
        "execution_enabled": False,
    }
    try:
        profile = load_profile(args.repository)
        result.update(
            {
                "profile": "valid",
                "repository": profile.repository["repository"],
                "profile_hash": profile.fingerprint,
                "workflow_lock": profile.lock,
            }
        )
        runtime = selected_runtime(
            profile, state_dir=args.state_dir, work_id=args.work_id, release_root=args.release_root
        )
        result["selected_runtime"] = runtime or "current pinned release"
        result["status"] = "BLOCKED"
    except WorkflowError as exc:
        result.update({"status": "BLOCKED", "profile": exc.as_dict()})
    return result


def _host(action: str, request: dict):
    from devflow.adapters.codex_host import NativeHostBridge

    bridge = NativeHostBridge()
    assignment = request["assignment"]
    if action == "prepare":
        return bridge.prepare_assignment(assignment, request["brief"])
    if action == "record":
        return bridge.record_launch(assignment, request["response"])
    if action == "reconcile":
        return bridge.reconcile_launch(assignment, request["inventory"])
    if action == "wait":
        return bridge.wait_target(assignment, cursor=request.get("cursor"))
    if action == "result":
        return bridge.validate_result(
            assignment, request["result"], observed_task_id=request["observed_task_id"]
        )
    raise WorkflowError("unknown_command", f"Unknown host command: {action}")


def _installation(action: str, request: dict, args):
    from devflow.installation import apply_install, plan_install, rollback_install

    if action == "plan":
        return plan_install(
            request["source"],
            request["revision"],
            request["install_root"],
            links=request.get("links", {}),
            owned_paths=request["owned_paths"],
            consumers=request.get("consumers", []),
            files=request.get("files"),
        )
    if action not in {"apply", "rollback"}:
        raise WorkflowError("unknown_command", f"Unknown install command: {action}")
    if not args.approved_root or not args.approved_path or not args.approved_plan_id:
        raise WorkflowError(
            "installation_scope_required",
            "Supply --approved-root, --approved-plan-id, and each --approved-path "
            "independently of the manifest",
        )
    function = apply_install if action == "apply" else rollback_install
    return function(
        request,
        approved_paths=args.approved_path,
        approved_root=args.approved_root,
        approved_plan_id=args.approved_plan_id,
    )


def _usage(action: str, request: dict, args):
    from devflow.adapters.responses import import_responses
    from devflow.adapters.usage import CcusageAdapter

    if action == "collect":
        return CcusageAdapter().collect(
            data_root=Path(request["data_root"]), report=request.get("report", "session")
        )
    if action == "import":
        if args.file:
            try:
                events = [
                    json.loads(line, parse_float=Decimal)
                    for line in args.file.read_text().splitlines()
                    if line.strip()
                ]
            except (OSError, UnicodeError, ValueError) as exc:
                raise WorkflowError(
                    "invalid_usage", "Cannot parse supplied response JSONL"
                ) from exc
        else:
            events = request["events"]
        return {
            "records": import_responses(
                events,
                ccusage_version=request["ccusage_version"],
                assignments=request.get("assignments"),
                segments=request.get("segments"),
                prices=request.get("prices"),
                existing=request.get("existing", []),
            )
        }
    raise WorkflowError("unknown_command", f"Unknown usage command: {action}")


def _profile(action: str, request: dict, args):
    from devflow.profiles import load_profile

    profile = load_profile(args.repository)
    if action == "inspect":
        return {
            "repository": profile.repository,
            "lock": profile.lock,
            "recipes": profile.recipes,
            "fingerprint": profile.fingerprint,
            "sources": profile.sources,
        }
    if action == "route":
        from devflow.installation import resolve_workflow

        return {
            "revision": resolve_workflow(
                active_version=request.get("active_version"),
                repository_lock=profile.lock,
                legacy_version=request.get("legacy_version"),
            )
        }
    raise WorkflowError("unknown_command", f"Unknown profile command: {action}")


def dispatch(args) -> dict:
    request = _request(args.request_file)
    if args.work_id:
        if request.get("work_id", args.work_id) != args.work_id:
            raise WorkflowError("work_mismatch", "Flag and request work identities differ")
        request["work_id"] = args.work_id
    command = args.command + ("." + args.action if args.action else "")
    if args.command not in {
        "doctor", "backlog", "work", "next", "candidate", "check", "gate", "finding",
        "fix", "action", "assignment", "host", "artifact", "profile", "usage", "outcome",
        "report", "install", "validate", "deliver", "workspace", "snapshot", "segment", "evidence",
    }:
        raise WorkflowError("unknown_command", "Unknown workflow command")
    if command == "doctor":
        return _doctor(args)
    from devflow.admission import BOOKKEEPING, READ_ONLY

    service = None
    if command not in BOOKKEEPING | READ_ONLY | {"backlog.capture", "backlog.retry", "action.dispatch"}:
        service = _service(args)
        service.preflight(command, request)
    # Security exception to attempt-pin retention: this entry point never execs
    # an older runtime. Safe recovery uses the current reader; new execution must
    # pass current admission even when the historical attempt has an older pin.
    if args.command not in {"install", "profile", "usage", "report", "validate", "artifact", "host"}:
        from devflow.profiles import load_profile

        if command not in {"work.prepare", "work.show", "next"} or (args.repository / ".devflow").exists():
            load_profile(args.repository)
    if args.command == "host":
        if args.action == "prepare":
            state = _state(service, request, args)
            assignment = request["assignment"]
            action = state["actions"].get(assignment.get("action_id"))
            if (
                state["assignments"].get(assignment.get("assignment_id")) != assignment
                or not action or action["operation"] != "launch_role"
                or action["status"] != "prepared"
                or assignment.get("attempt_id") != state["attempt"]["attempt_id"]
                or assignment.get("candidate_id") != state["candidate_id"]
            ):
                raise WorkflowError("assignment_mismatch", "Native launch needs its current recorded assignment")
        return _host(args.action, request)
    if args.command == "install":
        return _installation(args.action, request, args)
    if args.command == "profile":
        return _profile(args.action, request, args)
    if command in {"usage.collect", "usage.import"}:
        return _usage(args.action, request, args)
    if command in {"validate", "validate.record"}:
        from devflow.validation import validate_record

        validate_record(request)
        return {"valid": True, "record_type": request["record_type"]}
    if command == "work.prepare":
        from devflow.validation import validate_record

        record = request.get("record", {})
        try:
            validate_record(record, "work_contract")
        except WorkflowError as exc:
            return {"ready": False, "missing": [str(exc)], "authority_required": True}
        return {"ready": False, "missing": [], "authority_required": True, "record": record}
    if command == "report.usage":
        from devflow.reporting import usage_report

        return usage_report(
            request["records"],
            cutoff=request["cutoff"],
            population=request.get("population", "portfolio"),
            observation_window=request.get("observation_window"),
        )
    if command == "report.metrics":
        from devflow.metrics import quality_report

        return quality_report(request["works"], cutoff=request["cutoff"])
    service = service or _service(args)
    if args.command == "backlog":
        from devflow.adapters.git import GitRepository
        from devflow.backlog import capture
        from devflow.profiles import load_profile

        repository = load_profile(args.repository).repository["repository"]["id"]
        if GitRepository(args.repository).identity() != repository:
            raise WorkflowError("repository_mismatch", "Checkout differs from the backlog repository")
        return capture(service.store, repository, request, action=args.action)
    if command == "work.start":
        from devflow.provenance import validate_start_snapshot

        validate_start_snapshot(
            args.repository, request.get("workflow_snapshot", {}), service.store.require_artifact
        )
    if command == "work.show":
        return _state(service, request, args)
    if command == "work.list":
        from devflow.profiles import load_profile

        repository = load_profile(args.repository).repository["repository"]["id"]
        return {"works": service.list_works(repository)}
    if command == "next":
        return service.next(request["work_id"])
    if command == "artifact.put":
        if not args.file:
            raise WorkflowError("invalid_request", "artifact put requires --file")
        return {"artifact_hash": service.put_artifact(args.file.read_bytes())}
    if command == "check.run":
        from devflow.check_execution import run_registered_check
        from devflow.profiles import load_profile

        return run_registered_check(
            service, request, profile=load_profile(args.repository), repository=args.repository
        )
    if command == "candidate.capture":
        from devflow.adapters.git import GitRepository

        state = _state(service, request, args)
        record = GitRepository(args.repository).snapshot(
            candidate_id=request["candidate_id"],
            attempt_id=state["attempt"]["attempt_id"],
            scope_hash=state["scope_hash"],
            base_ref=request["base_ref"],
            dependency_hash=request["dependency_hash"],
            environment_hash=request["environment_hash"],
            ownership_token=request["ownership_token"],
        )
        return service.execute("candidate.record", {**request, "record": record})
    if command == "workspace.register":
        from devflow.adapters.git import GitRepository

        state = _state(service, request, args)
        action = state["actions"].get(request["action_id"])
        if (
            not action
            or action["operation"] != "prepare_workspace"
            or action["status"] != "prepared"
        ):
            raise WorkflowError(
                "workspace_intent_required", "A pending workspace action is required"
            )
        git = GitRepository(args.repository)
        if git.identity() != state["authority"]["repository"]:
            raise WorkflowError(
                "repository_mismatch", "Workspace does not match admitted repository"
            )
        observation = git.register_checkout(
            action_id=action["action_id"],
            ownership_token=request["ownership_token"],
            expected_head=request["expected_head"],
            base_ref=request["base_ref"],
        )
        return {"action": action, "observation": observation}
    if command == "action.dispatch":
        from devflow.execution import dispatch_action

        return dispatch_action(service, request, repository=args.repository)
    if command == "snapshot.capture":
        from devflow.provenance import capture_snapshot

        return capture_snapshot(args.repository, request, service.put_artifact)
    if args.command == "report" and not args.action:
        state = _state(service, request, args)
        return {
            "work_id": state["work_id"],
            "revision": state["revision"],
            "lifecycle": state["lifecycle"],
            "phase": state["phase"],
            "candidate_id": state["candidate_id"],
            "next": service.next(state["work_id"]),
            "findings": list(state["findings"].values()),
        }
    return service.execute(command, request)


def main(argv: list[str] | None = None) -> int:
    original_argv = list(sys.argv[1:] if argv is None else argv)
    args = parser().parse_args(original_argv)
    args.original_argv = original_argv
    try:
        result = dispatch(args)
        response = {"ok": True, "result": result}
        status = 2 if result.get("status") == "BLOCKED" else 0
    except WorkflowError as exc:
        response, status = {"ok": False, "error": exc.as_dict()}, 2
    except (KeyError, TypeError, ValueError, OSError) as exc:
        # Do not leak native subprocess output or entire sensitive input records.
        response, status = {"ok": False, "error": {"code": "invalid_input", "message": str(exc)}}, 2
    from devflow.adapters.usage import decimal_json_dumps

    output = decimal_json_dumps(response)
    print(output)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
