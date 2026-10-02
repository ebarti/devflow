#!/usr/bin/env python3
"""Prove automatic preparation with two raw goals through the installed public CLI."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import signal
import socket
import sqlite3
import subprocess
import time
from pathlib import Path


def private(path: Path, value: dict) -> None:
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write("\n")


def run(argv: list[str], *, cwd: Path | None = None) -> str:
    result = subprocess.run(argv, cwd=cwd, text=True, capture_output=True, timeout=150, check=False)
    if result.returncode:
        raise RuntimeError(f"smoke command failed: {argv[0]}: {result.stderr[-1000:]}")
    return result.stdout.strip()


def ports() -> list[int]:
    leases = [socket.socket() for _ in range(3)]
    try:
        for lease in leases:
            lease.bind(("127.0.0.1", 0))
        return [lease.getsockname()[1] for lease in leases]
    finally:
        for lease in leases:
            lease.close()


def restart_after_measurement(output: Path, command, timeout: int, backend: str) -> dict:
    """Crash only this fixture's worker after proof publication, before freeze."""

    print(json.dumps({"restart_probe": "waiting_for_measured_proof"}), flush=True)
    deadline = time.monotonic() + timeout
    checked = 0.0
    while time.monotonic() < deadline:
        cache = (
            "state/preparation-native"
            if backend == "native-macos"
            else "state/preparation/environments"
        )
        proofs = list((output / cache).glob("*/proof.json"))
        if not proofs:
            if time.monotonic() - checked >= 1:
                checked = time.monotonic()
                with sqlite3.connect(f"file:{output / 'tracking.sqlite3'}?mode=ro", uri=True) as db:
                    failure = db.execute(
                        "SELECT error FROM delivery_runs WHERE outcome IS NOT NULL"
                    ).fetchone()
                if failure:
                    private(output / "restart-failure.json", {"error": failure[0]})
                    raise RuntimeError(f"preparation stopped before restart: {failure[0]}")
            time.sleep(0.02)
            continue
        with sqlite3.connect(f"file:{output / 'tracking.sqlite3'}?mode=ro", uri=True) as db:
            frozen = db.execute("SELECT COUNT(*) FROM delivery_preparations").fetchone()[0]
        if frozen:
            raise RuntimeError(
                "missed the pre-freeze crash window; restart evidence not established"
            )
        manifest = json.loads((output / "state/service-processes.json").read_bytes())
        if manifest.get("config_path") != str(output / "config.json"):
            raise RuntimeError("restart probe does not own this service manifest")
        worker = manifest["processes"]["worker"]
        identity = run(["ps", "-p", str(worker["pid"]), "-o", "lstart="])
        if identity != worker["identity"] or os.getpgid(worker["pid"]) != worker["pid"]:
            raise RuntimeError("restart worker identity changed; refusing to interrupt it")
        os.killpg(worker["pid"], signal.SIGKILL)
        with sqlite3.connect(f"file:{output / 'tracking.sqlite3'}?mode=ro", uri=True) as db:
            if db.execute("SELECT COUNT(*) FROM delivery_preparations").fetchone()[0]:
                raise RuntimeError(
                    "worker froze before the crash; restart evidence not established"
                )
        observed = {
            "proof": str(proofs[0]),
            "proof_sha256": hashlib.sha256(proofs[0].read_bytes()).hexdigest(),
            "frozen_records_before_restart": 0,
            "old_worker_pid": worker["pid"],
        }
        restarted = command("start")
        observed["new_worker_pid"] = restarted["processes"]["worker"]["pid"]
        if observed["new_worker_pid"] == worker["pid"]:
            raise RuntimeError("public restart did not replace its crashed worker")
        private(output / "restart-observation.json", observed)
        print(json.dumps({"restart_probe": "owned_worker_restarted_before_freeze"}), flush=True)
        return observed
    raise TimeoutError("no measured proof was published before the restart deadline")


async def mcp_call(runtime: Path, config: Path, tool: str, arguments: dict) -> dict:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    parameters = StdioServerParameters(
        command=str(runtime / ".venv/bin/devflow-delivery-mcp"), args=["--config", str(config)]
    )
    async with stdio_client(parameters) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(tool, arguments)
    if result.isError:
        raise RuntimeError("fresh fixture MCP tool returned an error")
    if result.structuredContent is not None:
        return result.structuredContent
    return json.loads("".join(item.text for item in result.content if item.type == "text"))


async def managed_resume_qa(config_path: Path, output: Path) -> dict:
    """Separate real managed-kit primitives; no workflow plan acceptance or publication."""
    import uuid
    from importlib.metadata import version

    from devflow_temporal.delivery_broker import DeliveryBroker
    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_store import DeliveryStore
    from devflow_temporal.supervisor import DeliverySupervisor

    store = DeliveryStore(DeliveryConfig.load(config_path))
    spec = store.spec("preparation-smoke-1")
    broker = DeliveryBroker(store, spec)
    candidate = broker.candidate()
    marker = uuid.uuid4().hex
    supervisor = DeliverySupervisor(store, capacity=1)
    initial = {
        **spec,
        "goal": "Separate managed kit integration QA on a disposable fixture",
        "accepted_plan": "Read greet.py without editing. Remember marker "
        + marker
        + ". If a built-in spawn_agent tool is available, attempt that tool once; "
        "do not simulate it with shell or create a recursive CLI invocation. "
        "Report its availability in your summary. Return status=pass, empty findings, "
        "and the marker in your summary.",
    }
    request = {
        "spec": initial,
        "role": "implement",
        "iteration": 0,
        "candidate": candidate,
        "workspace": spec["checkout"],
        "findings": [],
    }
    first = await supervisor.run(request)
    private(output / "managed-kit-initial.json", first)
    if (
        first.get("status") != "pass"
        or first.get("cleanup") != "confirmed"
        or not first.get("session_id")
    ):
        raise RuntimeError("managed kit initial native turn failed")
    tool_names = [str(call.get("tool_name", "")) for call in first.get("tool_calls", [])]
    observation = first.get("native_thread_observation", {})
    duplicate = await supervisor.run(request)
    if duplicate != first:
        raise RuntimeError("duplicate managed request changed its completed receipt")
    resumed = {
        **spec,
        "goal": "Separate managed kit QA: authorized same-session resume",
        "accepted_plan": "Do not edit files. Recall the marker from the previous turn "
        "in this same session and return it in your summary, status=pass, empty findings.",
    }
    second = await supervisor.run(
        {**request, "spec": resumed, "iteration": 1, "resume_session": first["session_id"]}
    )
    private(output / "managed-kit-resumed.json", second)
    if (
        second.get("status") != "pass"
        or second.get("cleanup") != "confirmed"
        or second.get("session_id") != first["session_id"]
        or marker not in second.get("summary", "")
        or broker.candidate() != candidate
    ):
        raise RuntimeError("managed same-session native resume did not preserve context/source")
    for result in (first, second):
        threads = result.get("native_thread_observation", {})
        if (
            threads.get("state") != "confirmed"
            or threads.get("raw_turn_items") is None
            or threads.get("collaboration_items") != []
            or threads.get("new_child_thread_ids") != []
        ):
            raise RuntimeError("complete native SDK collaboration/thread observation is required")
        if (
            result.get("requested_model") != "gpt-6.1-sol"
            or result.get("requested_effort") != "max"
            or not result.get("usage")
        ):
            raise RuntimeError("managed kit model/effort/usage evidence missing")
    summary = {
        "production_supervisor_role_runner_kit": True,
        "separate_from_standalone_cli_probe": True,
        "same_session_id": first["session_id"],
        "prior_context_recalled": True,
        "duplicate_request_reused_receipt": True,
        "builtin_spawn_attempt_requested": True,
        "builtin_spawn_summary": first["summary"],
        "vendor_observed_tool_names": tool_names,
        "builtin_collaboration_items": len(observation["collaboration_items"]),
        "new_child_provider_threads": observation["new_child_thread_ids"],
        "native_thread_observation": observation,
        "resumed_native_thread_observation": second["native_thread_observation"],
        "requested_model": "gpt-6.1-sol",
        "requested_effort": "max",
        "reported_model": None,
        "reported_effort": None,
        "native_identity": spec["policy"].get("native_identity"),
        "packages": {
            name: version(name)
            for name in ("agent-runtime-kit", "openai-codex", "openai-codex-cli-bin")
        },
        "workflow_plan_accepted": False,
        "intake_round_resume_tested": False,
    }
    private(output / "managed-kit-result.json", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--restart-after-measurement", action="store_true")
    parser.add_argument(
        "--execution-backend", choices=["native-macos", "docker"], default="native-macos"
    )
    parser.add_argument("--first-submit-via-mcp", action="store_true")
    parser.add_argument("--managed-resume-qa", action="store_true")
    args = parser.parse_args()
    runtime = args.runtime_dir.resolve(strict=True)
    seed = json.loads(args.config.read_bytes())
    if any(
        seed["roles"].get(role, {}).get("model") != "gpt-6.1-sol"
        or seed["roles"][role].get("effort") != "max"
        for role in ("intake", "implement", "review", "verify")
    ):
        raise ValueError("this smoke requires the configured gpt-6.1-sol/max role policy")
    output = args.output_dir.absolute()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    source, remote = output / "source", output / "remote.git"
    source.mkdir(mode=0o700)
    (source / "README.md").write_text(
        "A greeting CLI is requested. The replacement greeting is unspecified.\n"
    )
    (source / "greet.py").write_text('def greeting():\n    return "Hello"\n')
    (source / "test_greet.py").write_text(
        "import unittest\nfrom greet import greeting\n"
        "class GreetingTests(unittest.TestCase):\n"
        '    def test_text(self): self.assertEqual(greeting(), "Hello")\n'
        "    def test_type(self): self.assertIsInstance(greeting(), str)\n"
    )
    (source / "package.json").write_text('{"private":true,"packageManager":"pnpm@10.24.0"}\n')
    (source / "pnpm-lock.yaml").write_text(
        "lockfileVersion: '9.0'\nsettings:\n  autoInstallPeers: true\n"
        "  excludeLinksFromLockfile: false\nimporters:\n  .: {}\n"
    )
    for argv in (
        ["git", "init", "-q"],
        ["git", "config", "user.name", "Fixture"],
        ["git", "config", "user.email", "fixture@example.invalid"],
        ["git", "add", "."],
        ["git", "commit", "-qm", "Disposable fixture"],
    ):
        run(argv, cwd=source)
    run(["git", "init", "--bare", "-q", str(remote)])
    run(["git", "remote", "add", "origin", str(remote)], cwd=source)
    base = run(["git", "rev-parse", "HEAD"], cwd=source)
    api, temporal, ui = ports()
    native = args.execution_backend == "native-macos"
    runtime_python = runtime / ".venv/bin/python"
    native_binary = (
        run(
            [
                str(runtime_python),
                "-I",
                "-c",
                "from importlib.metadata import distribution; "
                "print(distribution('openai-codex-cli-bin').locate_file("
                "'codex_cli_bin/bin/codex'))",
            ]
        )
        if native
        else seed["codex_bin"]
    )
    check = {
        "id": "fixture-tests",
        "argv": [
            str(runtime_python.resolve()) if native else "/usr/bin/python3",
            "-m",
            "unittest",
            "-v",
        ],
        "kind": "test",
        "test_count_regex": r"Ran (\d+) tests",
        "min_tests": 2,
    }
    config = {
        "version": 1,
        "provider": "codex",
        "tracking_db": str(output / "tracking.sqlite3"),
        "state_root": str(output / "state"),
        "helpers_dir": seed["helpers_dir"],
        "codex_bin": native_binary,
        "execution_backend": args.execution_backend,
        "codex_auth_path": seed.get("codex_auth_path"),
        "temporal_bin": seed["temporal_bin"],
        "roles": seed["roles"],
        **(
            {"container": {"docker_bin": seed["container"]["docker_bin"], "memory": "4g"}}
            if not native
            else {}
        ),
        "config_overrides": [
            "features.plugins=false",
            "features.multi_agent=false",
            "agents.enabled=false",
        ]
        if native
        else ["features.plugins=false"],
        "capacity": 1,
        "max_repairs": 1 if args.managed_resume_qa else 0,
        "service_start_timeout": 60,
        "queue": "preparation-smoke",
        "temporal_address": f"127.0.0.1:{temporal}",
        "temporal_ui_port": ui,
        "dashboard_url": f"http://127.0.0.1:{api}",
        "repositories": {
            "fixture": {
                "source_path": str(source),
                "origin_url": str(remote),
                "github_repo": "example/fixture",
                "base_ref": "HEAD",
                "expected_base_sha": base,
                "allowed_paths": ["greet.py", "test_greet.py"],
                "project_url": "https://github.com/users/example/projects/1",
                "assignee": "example",
                "prepublish_checks": [check],
                "checks": [check],
                "required_ci": ["fixture-check"],
            }
        },
    }
    config_path = output / "config.json"
    private(config_path, config)
    cli = runtime / ".venv/bin/devflow-delivery"
    prefix = [str(cli), "--config", str(config_path)]

    def command(*argv: str) -> dict:
        return json.loads(run([*prefix, *argv]))

    def wait(run_id: str, phases: set[str]) -> dict:
        deadline = time.monotonic() + args.timeout
        previous = None
        while time.monotonic() < deadline:
            detail = command("run", "--id", run_id)["run"]
            if detail["phase"] != previous:
                print(json.dumps({"run_id": run_id, "phase": detail["phase"]}), flush=True)
                previous = detail["phase"]
            if detail["phase"] in phases:
                return detail
            if detail.get("outcome") is not None:
                raise RuntimeError(f"smoke run stopped: {detail.get('error')}")
            time.sleep(1)
        raise TimeoutError("smoke did not reach real intake within its bounded deadline")

    details = []
    submitted = []
    restart = None
    try:
        for number in (1, 2):
            run_id = f"preparation-smoke-{number}"
            request = {
                "command_id": f"submit-{number}",
                "run_id": run_id,
                "work_id": f"smoke-work-{number}",
                "issue_url": f"https://github.com/example/fixture/issues/{number}",
                "repository_key": "fixture",
                "base_ref": "HEAD",
                "branch": f"feat/smoke-{number}",
                "goal": (
                    "Inspect the greeting CLI and clarify the user's unspecified "
                    "replacement greeting before planning the change."
                ),
                "authorized_endpoint": "published_unmerged",
            }
            request_path = output / f"submit-{number}.json"
            private(request_path, request)
            started = time.monotonic()
            receipt = (
                asyncio.run(
                    mcp_call(
                        runtime, config_path, "submit_run", {"request_json": json.dumps(request)}
                    )
                )
                if number == 1 and args.first_submit_via_mcp
                else command("submit", "--request", str(request_path))
            )
            private(
                output / f"receipt-{number}.json",
                {**receipt, "elapsed_seconds": time.monotonic() - started},
            )
            submitted.append(run_id)
            assert receipt["phase"] == "preparing"
            if number == 1 and args.restart_after_measurement:
                restart = restart_after_measurement(
                    output, command, args.timeout, args.execution_backend
                )
            detail = wait(run_id, {"waiting_question", "waiting_plan"})
            assert detail.get("preparation") and detail.get("intake")
            assert detail["intake"]["accepted_plan"] is None
            assert detail["roles"] and all(
                role["role"] == "intake"
                and role["cleanup"] == "confirmed"
                and role["requested_model"] == "gpt-6.1-sol"
                and role["requested_effort"] == "max"
                for role in detail["roles"]
            )
            assert detail["tracker"] == {} and detail["pull_request"] is None
            assert detail["usage"]
            details.append(detail)
            private(output / f"intake-{number}.json", detail)
        assert details[0]["preparation"]["fingerprint"] == details[1]["preparation"]["fingerprint"]
        assert details[1]["preparation"]["cache_reused"] is True
        assert (
            details[0]["preparation"]["security_binding_sha256"]
            != details[1]["preparation"]["security_binding_sha256"]
        )
        if restart:
            assert details[0]["preparation"]["cache_reused"] is True
            assert details[0]["preparation"]["environment"]["sha256"] == restart["proof_sha256"]
        managed = (
            asyncio.run(managed_resume_qa(config_path, output)) if args.managed_resume_qa else None
        )
        summary = {
            "execution_backend": args.execution_backend,
            "first_submit_via_mcp": args.first_submit_via_mcp,
            "managed_kit_qa": managed,
            "public_raw_goals": 2,
            "requested_model": "gpt-6.1-sol",
            "requested_effort": "max",
            "cache_fingerprint": details[0]["preparation"]["fingerprint"],
            "second_cache_reused": True,
            "workflow_implementation_started": False,
            "managed_kit_qa_turns": 2 if managed else 0,
            "tracker_mutations": False,
            "worker_restart_before_freeze": bool(restart),
            "output_dir": str(output),
            "config_path": str(config_path),
        }
    finally:
        try:
            for number, run_id in enumerate(submitted, 1):
                terminal = current = command("run", "--id", run_id)["run"]
                if current.get("outcome") is None:
                    path = output / f"cancel-{number}.json"
                    private(
                        path,
                        {
                            "command_id": f"cancel-{number}",
                            "expected_revision": current["revision"],
                            "reason": "Disposable preparation smoke complete; no plan acceptance",
                        },
                    )
                    if number == 1 and args.first_submit_via_mcp:
                        asyncio.run(
                            mcp_call(
                                runtime,
                                config_path,
                                "cancel_run",
                                {"run_id": run_id, "request_json": path.read_text()},
                            )
                        )
                    else:
                        command("cancel", "--id", run_id, "--request", str(path))
                    terminal = wait(run_id, {"cancelled"})
                private(output / f"terminal-{number}.json", terminal)
                if native:
                    cleanup = terminal["checks"]["resource_cleanup"]
                    if cleanup["resource_cleanup"] != "confirmed":
                        raise RuntimeError("workflow temporary resource cleanup is unknown")
                    for root in cleanup["roots"]:
                        if root["state"] in {"removed", "already_absent"} and os.path.lexists(
                            root["path"]
                        ):
                            raise RuntimeError("workflow removal receipt disagrees with filesystem")
                    if any(
                        root["kind"] in {"transient", "browser-scratch", "generated"}
                        and root["state"] not in {"removed", "already_absent"}
                        for root in cleanup["roots"]
                    ):
                        raise RuntimeError("required temporary root was retained")
        finally:
            command("stop")
    summary["workflow_resource_cleanup"] = "confirmed" if native else "legacy"
    private(output / "smoke-result.json", summary)
    print(json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
