from __future__ import annotations

import asyncio
import json
import subprocess
from argparse import Namespace
from pathlib import Path
from uuid import uuid4

import pytest
from temporalio import activity
from temporalio.client import WorkflowUpdateFailedError
from temporalio.common import RetryPolicy, WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from devflow_temporal import cli
from devflow_temporal.activities import run_role
from devflow_temporal.candidate import candidate_for, snapshot
from devflow_temporal.contracts import digest, public_inputs
from devflow_temporal.receipts import ReceiptStore
from devflow_temporal.workflow import IssueWorkflow


def repo_at(path: Path) -> Path:
    path.mkdir()
    (path / "README.md").write_text("Disposable test repository\n", encoding="utf-8")
    for args in (
        ("init", "-q"),
        ("config", "user.name", "Temporal Test"),
        ("config", "user.email", "temporal-test@example.invalid"),
        ("add", "README.md"),
        ("commit", "-qm", "Fixture"),
    ):
        subprocess.run(["git", "-C", str(path), *args], check=True)
    return path


def spec_for(
    tmp_path: Path,
    run_id: str,
    *,
    require_decision: bool = False,
    fake_finding: str | None = None,
    fake_change: str | None = None,
) -> dict:
    repo = repo_at(tmp_path / run_id)
    spec = {
        "version": 1,
        "run_id": run_id,
        "goal": "Add a local demo marker",
        "repo": str(repo),
        "state_dir": str(tmp_path / f"{run_id}-state"),
        "provider": "fake",
        "model": None,
        "effort": None,
        "require_decision": require_decision,
        "fake_finding": fake_finding,
        "fake_change": fake_change,
    }
    spec["input_digest"] = digest(public_inputs(spec))
    spec["initial_candidate"] = candidate_for(repo)
    return spec


async def start_run(client, spec: dict, queue: str):
    return await client.start_workflow(
        IssueWorkflow.run,
        spec,
        id=spec["run_id"],
        task_queue=queue,
        id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
        retry_policy=RetryPolicy(maximum_attempts=1),
        memo={"input_digest": spec["input_digest"]},
    )


@pytest.mark.asyncio
async def test_real_temporal_wait_restart_idempotency_and_role_gates(tmp_path, monkeypatch, capsys):
    async with await WorkflowEnvironment.start_local(
        dev_server_database_filename=str(tmp_path / "temporal.sqlite3")
    ) as env:
        queue = f"devflow-test-{uuid4().hex}"
        spec = spec_for(tmp_path, "restart-run", require_decision=True)

        async def test_client(_args):
            return env.client

        monkeypatch.setattr(cli, "_client", test_client)
        args = Namespace(
            address="unused",
            namespace="default",
            queue=queue,
            id=spec["run_id"],
            goal=spec["goal"],
            repo=spec["repo"],
            state_dir=spec["state_dir"],
            provider="fake",
            model=None,
            effort=None,
            decision=True,
            fake_finding=None,
            fake_change=None,
            disposable=True,
        )
        async with Worker(
            env.client, task_queue=queue, workflows=[IssueWorkflow], activities=[run_role]
        ):
            await cli._start(args)
            first = json.loads(capsys.readouterr().out)
            assert first["phase"] == "started"
            handle = env.client.get_workflow_handle(spec["run_id"])
            waiting = await handle.query("status")
            assert waiting["phase"] == "waiting_decision"
            assert waiting["roles"] == []
            assert not (Path(spec["state_dir"]) / "fake-invocations.jsonl").exists()
            await cli._start(args)
            assert json.loads(capsys.readouterr().out)["existing"] is True
            args.goal = "Different requested change"
            with pytest.raises(ValueError, match="different inputs"):
                await cli._start(args)
            args.goal = spec["goal"]

        # The worker is gone while the decision remains in Temporal history.
        async with Worker(
            env.client, task_queue=queue, workflows=[IssueWorkflow], activities=[run_role]
        ):
            waiting = await handle.query("status")
            assert waiting["decision_id"] == "restart-run:start"
            with pytest.raises(WorkflowUpdateFailedError):
                await handle.execute_update(
                    "decision", {"decision_id": "wrong", "revision": 1, "answer": "proceed"}
                )
            with pytest.raises(WorkflowUpdateFailedError):
                await handle.execute_update(
                    "decision",
                    {"decision_id": "restart-run:start", "revision": 9, "answer": "proceed"},
                )
            assert (await handle.query("status"))["roles"] == []
            await handle.execute_update(
                "decision",
                {"decision_id": "restart-run:start", "revision": 1, "answer": "proceed"},
            )
            final = await asyncio.wait_for(handle.result(), 20)

        assert final["outcome"] == "completed"
        assert [role["role"] for role in final["roles"]] == ["implement", "review", "verify"]
        assert len({role["identity"] for role in final["roles"]}) == 3
        assert all(role["status"] == "pass" for role in final["roles"])
        assert final["roles"][1]["input_candidate_id"] == final["candidate"]["id"]
        assert final["roles"][2]["input_candidate_id"] == final["candidate"]["id"]
        invoked = (Path(spec["state_dir"]) / "fake-invocations.jsonl").read_text().splitlines()
        assert len(invoked) == 3


@pytest.mark.asyncio
async def test_real_temporal_findings_and_candidate_mutation_block(tmp_path):
    async with await WorkflowEnvironment.start_local() as env:
        queue = f"devflow-test-{uuid4().hex}"
        async with Worker(
            env.client, task_queue=queue, workflows=[IssueWorkflow], activities=[run_role]
        ):
            for run_id, setting in (
                ("finding-run", "fake_finding"),
                ("mutation-run", "fake_change"),
            ):
                spec = spec_for(tmp_path, run_id, **{setting: "review"})
                handle = await start_run(env.client, spec, queue)
                final = await asyncio.wait_for(handle.result(), 20)
                assert final["outcome"] == "blocked"
                assert [role["role"] for role in final["roles"]] == ["implement", "review"]
                assert final["roles"][-1]["status"] != "pass"
                assert final["findings"]


@pytest.mark.asyncio
async def test_real_temporal_cancellation_cannot_become_success(tmp_path):
    async with await WorkflowEnvironment.start_local() as env:
        queue = f"devflow-test-{uuid4().hex}"
        entered = asyncio.Event()
        release = asyncio.Event()

        @activity.defn(name="run_role")
        async def delayed_role(request: dict) -> dict:
            entered.set()
            await release.wait()
            return await run_role(request)

        async with Worker(
            env.client, task_queue=queue, workflows=[IssueWorkflow], activities=[delayed_role]
        ):
            spec = spec_for(tmp_path, "cancel-run")
            handle = await start_run(env.client, spec, queue)
            await asyncio.wait_for(entered.wait(), 10)
            accepted = await handle.execute_update("cancel", "no longer needed")
            assert accepted["phase"] == "cancelling"
            release.set()
            final = await asyncio.wait_for(handle.result(), 20)
            assert final["outcome"] == "cancelled"
            assert final["cleanup"] == "unknown_after_activity"
            assert final["outcome"] != "completed"


@pytest.mark.asyncio
async def test_receipts_reuse_finished_and_block_ambiguous_invocation(tmp_path):
    spec = spec_for(tmp_path, "receipt-run")
    request = {"spec": spec, "role": "implement", "candidate": spec["initial_candidate"]}
    first = await run_role(request)
    second = await run_role(request)
    assert first == second
    invocation_file = Path(spec["state_dir"]) / "fake-invocations.jsonl"
    assert len(invocation_file.read_text().splitlines()) == 1

    ambiguous = spec_for(tmp_path, "ambiguous-run")
    store = ReceiptStore(Path(ambiguous["state_dir"]))
    claim = store.claim("ambiguous-run", "implement", 0, ambiguous["initial_candidate"]["id"])
    assert claim.state == "new"
    blocked = await run_role(
        {"spec": ambiguous, "role": "implement", "candidate": ambiguous["initial_candidate"]}
    )
    assert blocked["status"] == "recovery_unknown"
    assert not (Path(ambiguous["state_dir"]) / "fake-invocations.jsonl").exists()


def test_candidate_snapshot_excludes_ignored_credentials_and_rejects_symlinks(tmp_path):
    repo = repo_at(tmp_path / "candidate-repo")
    (repo / ".gitignore").write_text(".env\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", ".gitignore"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "Ignore local env"], check=True)
    secret = repo / ".env"
    secret.write_text("secret-first\n", encoding="utf-8")
    identity = candidate_for(repo)
    secret.write_text("secret-second\n", encoding="utf-8")
    assert candidate_for(repo) == identity
    copied = snapshot(repo, tmp_path / "state", "candidate-run", identity)
    assert not (copied / ".env").exists()

    (repo / "linked.txt").symlink_to(secret)
    with pytest.raises(ValueError, match="symlink"):
        candidate_for(repo)
