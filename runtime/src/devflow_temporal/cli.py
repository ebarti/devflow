"""Local CLI for one experimental Temporal workflow."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from temporalio.client import Client, WorkflowUpdateFailedError
from temporalio.common import RetryPolicy, WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker

from .activities import run_role
from .bridge import preflight
from .candidate import candidate_for, validate_paths
from .contracts import digest, public_inputs, validate_spec
from .workflow import IssueWorkflow

DEFAULT_ADDRESS = "127.0.0.1:17333"
DEFAULT_QUEUE = "devflow-temporal-mvp"


def _print(value: Any) -> None:
    print(json.dumps(value, sort_keys=True, indent=2))


async def _client(args: argparse.Namespace) -> Client:
    return await Client.connect(args.address, namespace=args.namespace)


def _handle(client: Client, run_id: str):
    return client.get_workflow_handle(run_id)


async def _existing_status(client: Client, run_id: str) -> dict[str, Any] | None:
    try:
        return await _handle(client, run_id).query("status")
    except RPCError as exc:
        if exc.status == RPCStatusCode.NOT_FOUND:
            return None
        raise


async def _existing_digest(client: Client, run_id: str) -> str | None:
    try:
        description = await _handle(client, run_id).describe()
    except RPCError as exc:
        if exc.status == RPCStatusCode.NOT_FOUND:
            return None
        raise
    return await description.memo_value("input_digest", "unknown")


async def _start(args: argparse.Namespace) -> None:
    if not args.disposable:
        raise ValueError("pass --disposable to confirm this local Git copy may be edited")
    repo = Path(args.repo).expanduser().resolve(strict=True)
    state_dir = Path(args.state_dir).expanduser().resolve()
    spec: dict[str, Any] = {
        "version": 1,
        "run_id": args.id,
        "goal": args.goal,
        "repo": str(repo),
        "state_dir": str(state_dir),
        "provider": args.provider,
        "model": args.model,
        "effort": args.effort,
        "require_decision": args.decision,
        "fake_finding": args.fake_finding,
        "fake_change": args.fake_change,
    }
    spec["input_digest"] = digest(public_inputs(spec))
    client = await _client(args)
    existing_digest = await _existing_digest(client, args.id)
    if existing_digest is not None:
        if existing_digest != spec["input_digest"]:
            raise ValueError("run ID already exists with different inputs")
        _print({"run_id": args.id, "input_digest": existing_digest, "existing": True})
        return
    validate_paths(repo, state_dir, require_clean=True)
    spec["initial_candidate"] = candidate_for(repo)
    validate_spec(spec)
    if spec["provider"] == "codex":
        spec["preflight"] = await preflight(spec)
    try:
        await client.start_workflow(
            IssueWorkflow.run,
            spec,
            id=args.id,
            task_queue=args.queue,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
            retry_policy=RetryPolicy(maximum_attempts=1),
            memo={"input_digest": spec["input_digest"]},
        )
    except WorkflowAlreadyStartedError:
        existing_digest = await _existing_digest(client, args.id)
        if existing_digest != spec["input_digest"]:
            raise ValueError("run ID was taken by another input") from None
        _print({"run_id": args.id, "input_digest": existing_digest, "existing": True})
        return
    _print(
        {
            "run_id": args.id,
            "input_digest": spec["input_digest"],
            "phase": "started",
            "preflight": spec.get("preflight"),
        }
    )


async def _status(args: argparse.Namespace) -> None:
    client = await _client(args)
    status = await _existing_status(client, args.id)
    if status is None:
        raise ValueError("run ID not found")
    _print(status)


async def _decision(args: argparse.Namespace) -> None:
    client = await _client(args)
    result = await _handle(client, args.id).execute_update(
        "decision",
        {"decision_id": args.decision_id, "revision": args.revision, "answer": args.answer},
    )
    _print(result)


async def _cancel(args: argparse.Namespace) -> None:
    client = await _client(args)
    _print(await _handle(client, args.id).execute_update("cancel", args.reason))


async def _worker(args: argparse.Namespace) -> None:
    client = await _client(args)
    async with Worker(
        client, task_queue=args.queue, workflows=[IssueWorkflow], activities=[run_role]
    ):
        print(f"worker listening on {args.address}, queue {args.queue}", flush=True)
        await asyncio.Event().wait()


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="devflow-temporal")
    sub = root.add_subparsers(dest="command", required=True)

    def connection(command: argparse.ArgumentParser) -> None:
        command.add_argument("--address", default=DEFAULT_ADDRESS)
        command.add_argument("--namespace", default="default")
        command.add_argument("--queue", default=DEFAULT_QUEUE)

    worker = sub.add_parser("worker", help="run the Temporal worker")
    connection(worker)
    worker.set_defaults(func=_worker)

    start = sub.add_parser("start", help="submit a local task without waiting for a model")
    connection(start)
    start.add_argument("--id", required=True)
    start.add_argument("--goal", required=True)
    start.add_argument("--repo", required=True)
    start.add_argument("--state-dir", required=True)
    start.add_argument("--provider", choices=("fake", "codex"), required=True)
    start.add_argument("--model")
    start.add_argument("--effort")
    start.add_argument("--decision", action="store_true")
    start.add_argument("--fake-finding", choices=("review", "verify"))
    start.add_argument("--fake-change", choices=("review", "verify"))
    start.add_argument("--disposable", action="store_true")
    start.set_defaults(func=_start)

    status = sub.add_parser("status", help="query persisted workflow status")
    connection(status)
    status.add_argument("--id", required=True)
    status.set_defaults(func=_status)

    decision = sub.add_parser("decision", help="answer one pending decision")
    connection(decision)
    decision.add_argument("--id", required=True)
    decision.add_argument("--decision-id", required=True)
    decision.add_argument("--revision", required=True, type=int)
    decision.add_argument("--answer", choices=("proceed", "decline"), required=True)
    decision.set_defaults(func=_decision)

    cancel = sub.add_parser("cancel", help="request cancellation at the next role boundary")
    connection(cancel)
    cancel.add_argument("--id", required=True)
    cancel.add_argument("--reason", required=True)
    cancel.set_defaults(func=_cancel)
    return root


def main() -> None:
    args = parser().parse_args()
    try:
        asyncio.run(args.func(args))
    except WorkflowUpdateFailedError as exc:
        cause = exc.__cause__ or exc
        parser().exit(2, f"error: {cause}\n")
    except (ValueError, RPCError) as exc:
        parser().exit(2, f"error: {exc}\n")
    except KeyboardInterrupt:
        return
