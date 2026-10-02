#!/usr/bin/env python3
"""Prove automatic preparation with two raw goals through the installed public CLI."""

from __future__ import annotations

import argparse
import json
import os
import socket
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=1800)
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
    check = {
        "id": "fixture-tests",
        "argv": ["/usr/bin/python3", "-m", "unittest", "-v"],
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
        "codex_bin": seed["codex_bin"],
        "codex_auth_path": seed.get("codex_auth_path"),
        "temporal_bin": seed["temporal_bin"],
        "roles": seed["roles"],
        "container": {"docker_bin": seed["container"]["docker_bin"], "memory": "4g"},
        "config_overrides": ["features.plugins=false"],
        "capacity": 1,
        "max_repairs": 0,
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
            receipt = command("submit", "--request", str(request_path))
            private(
                output / f"receipt-{number}.json",
                {**receipt, "elapsed_seconds": time.monotonic() - started},
            )
            submitted.append(run_id)
            assert receipt["phase"] == "preparing"
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
        summary = {
            "public_raw_goals": 2,
            "requested_model": "gpt-6.1-sol",
            "requested_effort": "max",
            "cache_fingerprint": details[0]["preparation"]["fingerprint"],
            "second_cache_reused": True,
            "implementation_started": False,
            "tracker_mutations": False,
            "output_dir": str(output),
            "config_path": str(config_path),
        }
        private(output / "smoke-result.json", summary)
        print(json.dumps(summary, sort_keys=True), flush=True)
    finally:
        for number, run_id in enumerate(submitted, 1):
            current = command("run", "--id", run_id)["run"]
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
                command("cancel", "--id", run_id, "--request", str(path))
                terminal = wait(run_id, {"cancelled"})
                private(output / f"cancelled-{number}.json", terminal)
        command("stop")


if __name__ == "__main__":
    main()
