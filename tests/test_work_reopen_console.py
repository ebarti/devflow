"""Real subprocess CLI/pin tests with owned Git checkouts and an explicit fake gh transport."""

import json
import os
import re
import shutil
import sys
from copy import deepcopy
from pathlib import Path

import pytest
from domain.helpers import NOW, record
from test_subagent_lifecycle import PARENT
from test_user_request_console import Console, git, initialize_git
from test_user_request_console import installed as installed  # noqa: F401
from test_work_reopen import REPOSITORY, reopen_request

from devflow.installation import apply_install, plan_install
from devflow.validation import digest


@pytest.fixture(scope="module")
def historical_release(tmp_path_factory):
    """Synthetic historical 0.4 pin: current executable contract, old stage-policy metadata."""
    root = Path(__file__).parents[1]
    temporary = tmp_path_factory.mktemp("historical-pr-package")
    source = temporary / "source"
    source.mkdir()
    for name in ("src", "skills"):
        shutil.copytree(root / name, source / name, ignore=shutil.ignore_patterns("__pycache__"))
    for name in ("pyproject.toml", "uv.lock"):
        shutil.copyfile(root / name, source / name)
    for name in ("src/devflow/__init__.py", "pyproject.toml", "uv.lock"):
        path = source / name
        path.write_text(path.read_text().replace('"0.5.2"', '"0.4.0"'))
    initialize_git(source)
    git(source, "add", ".")
    git(source, "commit", "-qm", "test: synthetic historical PR workflow pin")
    target = temporary / "host-skill"
    manifest = plan_install(
        source,
        git(source, "rev-parse", "HEAD"),
        temporary / "managed",
        links={target: "skills/devflow"},
        owned_paths=[target],
    )
    apply_install(
        manifest,
        approved_paths=[target],
        approved_root=temporary / "managed",
        approved_plan_id=manifest["plan_id"],
    )
    return Path(manifest["release_dir"]), manifest["revision"]


class PRConsole(Console):
    def __init__(self, temporary, release):
        super().__init__(temporary, release)
        git(self.root, "remote", "add", "origin", "https://github.com/fixture/repo.git")
        profile = self.root / ".devflow/repository.toml"
        profile.write_text(profile.read_text().replace(self.identity, REPOSITORY))
        self.identity = REPOSITORY
        version = re.search(
            r'__version__ = "([^"]+)"', (self.release / "src/devflow/__init__.py").read_text()
        )[1]
        (self.root / ".devflow/workflow.lock").write_text(
            f'schema_version=1\nversion="{version}"\nrevision="{self.package_revision}"\n'
        )
        checks = self.root / ".devflow/checks.toml"
        # This PR has a required recipe absent from the ordinary main profile.
        checks.write_text(
            checks.read_text()
            + checks.read_text().split("[checks.prose]")[1].join(["\n[checks.pr-specific]", ""])
        )
        git(self.root, "add", ".")
        git(self.root, "commit", "-qm", "test: PR-specific verification profile")
        self.base = git(self.root, "rev-parse", "HEAD")
        self.contract["endpoint"] = {"kind": "pr", "target": "main"}
        self.contract["verification"]["recipes"] = ["prose", "pr-specific"]
        self.remote = temporary / "synthetic-remote.json"
        self.remote.write_text(json.dumps({"head": self.base, "prs": [], "writes": []}))
        self.bin = temporary / "bin"
        self.bin.mkdir()
        executable = self.bin / "gh"
        executable.write_text(
            f"#!{sys.executable}\n"
            + """import json,sys
from pathlib import Path
path = Path("""
            + repr(str(self.remote))
            + """)
s=json.loads(path.read_text()); args=sys.argv
method=args[args.index("--method")+1]; endpoint=args[args.index("--method")+2].removeprefix("repos/fixture/repo/")
payload=json.loads(sys.stdin.read()) if "--input" in args else None
if method=="GET" and endpoint.startswith("pulls?"): result=s["prs"]
elif method=="GET" and endpoint=="pulls/1": result=s["prs"][0]
elif method=="GET" and endpoint.startswith("git/ref/heads/"): result={"object":{"sha":s["head"]}}
elif method=="POST" and endpoint=="pulls":
 result={"number":1,"node_id":"PR-1","html_url":"https://github.com/fixture/repo/pull/1","state":"open","draft":False,"title":payload["title"],"body":payload["body"],"head":{"ref":payload["head"],"sha":s["head"],"repo":{"full_name":"fixture/repo"}},"base":{"ref":payload["base"],"repo":{"full_name":"fixture/repo"}}}
 s["prs"].append(result);s["writes"].append([method,endpoint,payload])
elif method=="PATCH" and endpoint=="pulls/1":
 s["prs"][0].update(payload);result=s["prs"][0];s["writes"].append([method,endpoint,payload])
else: raise AssertionError((method,endpoint))
path.write_text(json.dumps(s));print(json.dumps(result))
"""
        )
        executable.chmod(0o700)
        self.config = temporary / "config.toml"
        self.config.write_text('model="synthetic-worker"\nmodel_reasoning_effort="high"\n')
        self.sessions = temporary / "sessions"
        self.sessions.mkdir()

    def call(self, command, request=None, *, error=None, env=None):
        return super().call(
            command,
            request,
            error=error,
            env={
                "CODEX_THREAD_ID": PARENT,
                "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
                **(env or {}),
            },
        )

    def mutate(self, command, **fields):
        self.show()
        return super().mutate(command, **fields)

    def begin_worker(self):
        a = self.mutate(
            "host.assign",
            assignment_id="worker",
            role="implementation_worker",
            owned_paths=["README.md"],
            workspace_reference=str(self.root),
            brief="Repair only the synthetic invariant in the owned fixture",
            config_path=str(self.config),
        )["assignment"]
        self.mutate("host.prepare", assignment_id="worker")
        self.mutate(
            "host.record",
            assignment_id="worker",
            response={"task_name": a["agent_name"], "nickname": "Synthetic"},
        )
        task = "22222222-2222-4222-8222-222222222222"
        source = self.sessions / f"rollout-synthetic-{task}.jsonl"
        source.write_text(
            "\n".join(
                json.dumps(item)
                for item in [
                    {
                        "type": "session_meta",
                        "payload": {
                            "id": task,
                            "parent_thread_id": PARENT,
                            "agent_path": a["agent_name"],
                            "thread_source": "subagent",
                        },
                    },
                    {
                        "type": "turn_context",
                        "timestamp": NOW,
                        "payload": {
                            "turn_id": "synthetic-turn",
                            "model": a["role_policy"]["model"],
                            "effort": a["role_policy"]["reasoning_effort"],
                        },
                    },
                ]
            )
            + "\n"
        )
        self.mutate(
            "host.startup",
            assignment_id="worker",
            task_id=task,
            session_path=str(source),
            sessions_root=str(self.sessions),
            session_meta_line=1,
            turn_context_line=2,
        )
        self.mutate("host.activate", assignment_id="worker")
        self.mutate("host.prepare", assignment_id="worker")
        self.mutate(
            "host.record",
            assignment_id="worker",
            inventory=[{"agent_name": a["agent_name"], "agent_status": "running"}],
        )
        return task

    def finish(self):
        action = self.start(
            operations=["edit", "check", "create_tasks", "publish_pr"],
            execution_mode="subagent",
            owner_task_id=PARENT,
        )
        observation = self.call(
            "workspace.register",
            {
                "work_id": self.contract["work_id"],
                "action_id": action["action_id"],
                "ownership_token": "synthetic-console-owner",
                "expected_head": self.base,
                "base_ref": self.base,
            },
        )["observation"]
        self.mutate("action.begin", action_id=action["action_id"])
        receipt = record(
            "action_receipt",
            action_id=action["action_id"],
            attempt_id="console-attempt",
            operation=action["operation"],
            payload_hash=action["payload_hash"],
            expected_revision=action["expected_revision"],
            status="confirmed",
            external_id=observation["path"],
            observations=["Owned synthetic checkout"],
            recorded_at=NOW,
        )
        self.mutate("action.record", record=receipt, observation=observation)
        task = self.begin_worker()
        (self.root / "README.md").write_text("Reviewed local handoff.\n")
        git(self.root, "add", "README.md")
        git(self.root, "commit", "-qm", "docs: synthetic candidate")
        self.mutate(
            "candidate.capture",
            candidate_id="original-candidate",
            base_ref=self.base,
            dependency_hash=digest("dependencies"),
            environment_hash=digest("environment"),
            ownership_token="synthetic-console-owner",
            assignment_id="worker",
            producer_task_id=task,
        )
        self.mutate(
            "host.result",
            assignment_id="worker",
            observed_task_id=task,
            result={
                "assignment_id": "worker",
                "candidate_id": None,
                "producer_role": "implementation_worker",
                "status": "completed",
                "output_candidate_id": "original-candidate",
                "evidence_reference": "synthetic:completion",
            },
        )
        for recipe in self.contract["verification"]["recipes"]:
            self.mutate("check.run", recipe_id=recipe, acceptance_ids=["A01"])
        self.mutate(
            "usage.account",
            status="unknown",
            source_reference="synthetic:usage-unavailable",
            limitations=["Synthetic host"],
        )
        remote = json.loads(self.remote.read_text())
        remote["head"] = git(self.root, "rev-parse", "HEAD")
        self.remote.write_text(json.dumps(remote))
        self.deliver("original-delivery")

    def deliver(self, identity, body="Synthetic current proof"):
        action = self.mutate(
            "deliver",
            expected_remote_state={
                "head_ref": "test/request",
                "base_ref": "main",
                "title": "Synthetic PR",
                "body": body,
            },
        )["action"]
        result = self.mutate("action.dispatch", action_id=action["action_id"])
        assert result["status"] == "confirmed", result
        state = self.show()
        delivery = record(
            "delivery",
            delivery_id=identity,
            work_id=state["work_id"],
            attempt_id=state["attempt"]["attempt_id"],
            candidate_id=state["candidate_id"],
            authority_id=state["authority"]["authority_id"],
            endpoint=state["contract"]["endpoint"],
            gate_ids=action["payload"]["gate_ids"],
            action_id=action["action_id"],
            receipt_id=result["receipt_id"],
            observed_result="Synthetic transport independent PR readback",
            delivered_at=NOW,
            merge_binding=None,
            resulting_merge=None,
            status="verified",
        )
        self.mutate("deliver", record=delivery, observation=result["observation"])


def test_old_pin_cli_preserves_custom_recipes_and_admits_conflict_paths(
    tmp_path, installed, historical_release
):
    console = PRConsole(tmp_path, historical_release)
    console.finish()
    before = console.show()
    profile_files = {p: p.read_bytes() for p in (console.root / ".devflow").iterdir()}
    console.release, console.package_revision = installed
    request, _ = reopen_request(before)
    request["record"]["scope_revision"] += 1
    request["record"]["scope"]["paths"] += [
        ".devflow/workflow.lock",
        "AGENTS.md",
        "docs/developer/workflow.md",
    ]
    request["record"]["scope"]["boundaries"] = ["Integrate current main into this same PR"]
    request["record"]["verification"]["documentation_owners"] = ["docs/developer/workflow.md"]
    # A normal capture cannot silently replace the branch pin.
    snapshot_request = {
        "snapshot_id": "continuation-snapshot",
        "effective_settings": {
            "model": "synthetic-worker",
            "reasoning_effort": "high",
            "source_reference": "synthetic:observed-settings",
        },
    }
    console.call("snapshot.capture", snapshot_request, error="release_mismatch")
    snapshot = console.call(
        "snapshot.capture", snapshot_request | {"continuation_work_id": before["work_id"]}
    )
    assert snapshot["continuation_upgrade"]["prior_package_revision"] == historical_release[1]
    request["workflow_snapshot"] = snapshot
    console.call("work.reopen", request | {"user_request": None}, error="invalid_user_request")
    wrong = deepcopy(request)
    wrong["continuation"]["head_ref"] = "other-source"
    console.call("work.reopen", wrong, error="continuation_pr")
    reopened = console.call("work.reopen", request)
    after = console.show()
    assert after["phase"] == "implement" and after["candidate_id"] is None
    assert after["contract"]["verification"]["recipes"] == ["prose", "pr-specific"]
    assert all(p.read_bytes() == data for p, data in profile_files.items())
    assert all(after["records"][k] == v for k, v in before["records"].items())
    assert after["attempt"]["entry_phase"] == before["attempt"]["entry_phase"]
    assert console.call("work.reopen", request) == reopened
    assert (
        console.call("next", {"work_id": before["work_id"]})["actions"][0]["role"]
        == "implementation_worker"
    )
    # This exception cannot authorize an ordinary active amendment.
    amended = deepcopy(after["contract"])
    amended["scope_revision"] += 1
    console.call(
        "work.amend",
        {
            "operation_id": "not-a-reopen",
            "work_id": before["work_id"],
            "expected_revision": after["revision"],
            "record": amended,
            "user_request": request["user_request"],
            "workflow_snapshot": snapshot,
        },
        error="snapshot_mismatch",
    )
    assert console.show() == after
    console.call(
        "work.start",
        console.start_request | {"workflow_snapshot": snapshot},
        error="snapshot_mismatch",
    )
    # Resume the actual worker identity, rather than claiming a routing suggestion proves activation.
    worker = console.mutate(
        "host.assign",
        assignment_id="worker",
        role="implementation_worker",
        owned_paths=request["record"]["scope"]["paths"],
        workspace_reference=str(console.root),
        brief="Integrate the admitted conflict paths; preserve the PR-specific recipes",
        config_path=str(console.config),
    )["assignment"]
    assert worker["task_id"] == before["assignments"]["worker"]["task_id"]
    prepared = console.mutate("host.prepare", assignment_id="worker")
    assert prepared["intent"]["native_tool"] == "followup_task"
    console.mutate(
        "host.record",
        assignment_id="worker",
        inventory=[{"agent_name": worker["agent_name"], "agent_status": "running"}],
    )
    assert console.show()["assignments"]["worker"]["status"] == "running"
    assert all(p.read_bytes() == data for p, data in profile_files.items())


def test_cli_deliver_reuses_exact_proof_and_updates_same_pr(tmp_path, installed):
    console = PRConsole(tmp_path, installed)
    console.finish()
    before = console.show()
    request, _ = reopen_request(before, "deliver")
    request["user_request"]["reference"] = before["authority"]["source_reference"]
    request["user_request"]["allowed_operations"] = ["publish_pr"]
    calls = console.counter.read_text()
    console.call("work.reopen", request)
    state = console.show()
    assert (
        state["check_ids"] == before["check_ids"] and state["assignments"] == before["assignments"]
    )
    console.mutate(
        "usage.account",
        status="unknown",
        source_reference="synthetic:continued-period",
        limitations=["Synthetic host"],
    )
    console.deliver("updated-delivery", body="Updated same-outcome readback")
    after = console.show()
    remote = json.loads(console.remote.read_text())
    assert after["lifecycle"] == "done" and after["delivery_id"] == "updated-delivery"
    assert len(remote["prs"]) == 1 and [w[0] for w in remote["writes"]] == ["POST", "PATCH"]
    assert console.counter.read_text() == calls
    assert (
        after["records"]["delivery:original-delivery"]
        == before["records"]["delivery:original-delivery"]
    )
