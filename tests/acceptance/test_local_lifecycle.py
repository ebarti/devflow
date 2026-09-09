"""Production integration using temporary Git and private state; no real remote writes.

Tier-zero editorial delivery exercises real checkout ownership, profile admission,
subprocess checks and local endpoint readback. The release test alone substitutes
an explicitly simulated remote to probe an unverified external observation.
"""
from __future__ import annotations

import itertools
import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from devflow.adapters.git import GitRepository
from devflow.application.commands import WorkflowService
from devflow.check_execution import run_registered_check
from devflow.domain.rules import scope_hash
from devflow.errors import WorkflowError
from devflow.execution import dispatch_action
from devflow.profiles import load_profile
from devflow.validation import digest


def now():
    return datetime.now(UTC).isoformat()


def record(record_type, **fields):
    return {"schema_version": 1, "record_type": record_type, **fields}


def git(path, *args):
    return subprocess.check_output(["git", "-C", str(path), *args], text=True).strip()


class LocalLifecycle:
    """All identities and authority in this fixture are explicitly synthetic."""

    def __init__(self, temporary, *, endpoint="local", profile_name="prose"):
        self.root = temporary / "repository"
        fixture = Path(__file__).resolve().parents[2] / "fixtures/repositories" / profile_name
        shutil.copytree(fixture, self.root)
        self.fixture_case = json.loads((self.root / "fixture-case.json").read_text())
        self.edit_path = self.fixture_case["edit_path"]
        self.recipe_id = self.fixture_case["recipe_id"]
        git(self.root, "init", "-q", "-b", "main")
        git(self.root, "config", "user.name", "Synthetic Integration")
        git(self.root, "config", "user.email", "synthetic@example.invalid")
        if endpoint == "release":
            # Identity only: no test ever contacts this simulated remote.
            git(self.root, "remote", "add", "origin", "https://github.com/synthetic/devflow-fixture.git")
        self.repository = GitRepository(self.root)
        identity = self.repository.identity()
        directory = self.root / ".devflow"
        (directory / "repository.toml").write_text(
            "schema_version = 1\n[repository]\nid = " + json.dumps(identity)
            + '\ndefault_branch = "main"\n'
        )
        git(self.root, "add", ".")
        git(self.root, "commit", "-qm", "test: initialize synthetic editorial repository")
        self.base = git(self.root, "rev-parse", "HEAD")
        git(self.root, "switch", "-qc", "test/editorial-handoff")
        (self.root / self.edit_path).write_text(self.fixture_case["accepted_text"])
        git(self.root, "add", self.edit_path)
        git(self.root, "commit", "-qm", "docs: clarify local handoff")
        self.profile = load_profile(self.root)
        self.state_dir = temporary / "private-state"
        self.service = WorkflowService(self.state_dir)
        self.sequence = itertools.count()
        self.work_id = "synthetic-local-work"
        self.contract = record(
            "work_contract", work_id=self.work_id, scope_revision=1,
            source={"kind": "local_intake", "reference": "synthetic:explicit-request",
                    "stable_id": self.work_id},
            kind="editorial", title="Synthetic editorial handoff",
            outcome="The accepted editorial text is reviewable in the dedicated checkout",
            acceptance=[{"id": "A01", "expected": "Accepted editorial text and preserved unrelated file",
                         "invariant": "I01"}],
            scope={"paths": [self.edit_path], "boundaries": ["Temporary synthetic repository only"]},
            context=[{"summary": "Synthetic local delivery integration", "reference": "fixture:local"}],
            dependencies=[], risk={"tier": 0, "reason": "Editorial text; release adapter is simulated"},
            verification={"recipes": [self.recipe_id], "scenarios": ["A01"],
                          "documentation_owners": [self.edit_path]},
            endpoint={"kind": endpoint, "target": str(self.root) if endpoint == "local" else "v0-test"},
        )
        authority = record(
            "authority", authority_id="synthetic-authority", source_kind="user_instruction",
            source_reference="synthetic:bounded-test-authorization", work_id=self.work_id,
            scope_hash=scope_hash(self.contract), repository=identity,
            allowed_operations=["edit", "check", "release"], expires_at=None, revoked=False,
        )
        self.call("work.ready", record=self.contract, authority=authority)
        snapshot = record(
            "workflow_snapshot", snapshot_id="synthetic-policy", package_version="0.1.0",
            workflow_hash=digest(self.profile.sources), model_policy_hash=digest("synthetic-user-settings"),
            instruction_sources=list(self.profile.sources),
            repository_profile_reference="sha256:" + self.profile.fingerprint,
            effective_settings_reference="synthetic:explicit-owner-settings", captured_at=now(),
        )
        attempt = record(
            "attempt", attempt_id="synthetic-attempt", work_id=self.work_id,
            scope_hash=scope_hash(self.contract), authority_id=authority["authority_id"],
            host_id="synthetic-host", owner_task_id="synthetic-explicit-owner",
            phase="implement", blocker=None, workflow_snapshot_id=snapshot["snapshot_id"],
            model_policy_snapshot_id=snapshot["snapshot_id"], revision=self.state["revision"],
            started_at=now(), status="active",
        )
        action = self.call("work.start", record=attempt, workflow_snapshot=snapshot)["action"]
        self.call("action.begin", action_id=action["action_id"])
        self.ownership_token = "synthetic-owned-checkout-token"
        reservation = self.repository.register_checkout(
            action_id=action["action_id"], ownership_token=self.ownership_token,
            expected_head=git(self.root, "rev-parse", "HEAD"), base_ref=self.base,
        )
        receipt = record(
            "action_receipt", action_id=action["action_id"], attempt_id=attempt["attempt_id"],
            operation=action["operation"], payload_hash=action["payload_hash"],
            expected_revision=action["expected_revision"], status="confirmed",
            external_id=reservation["path"], observations=["Registered dedicated Git checkout"],
            recorded_at=now(),
        )
        self.call("action.record", record=receipt, observation=reservation)
        self.candidate = self.repository.snapshot(
            candidate_id="synthetic-candidate", attempt_id=attempt["attempt_id"],
            scope_hash=self.state["scope_hash"], base_ref=self.base,
            dependency_hash=digest((directory / "workflow.lock").read_text()),
            environment_hash=digest(sys.version), ownership_token=self.ownership_token,
        )
        self.call("candidate.record", record=self.candidate)
        self.check_request = {
            "operation_id": "synthetic-check-" + str(next(self.sequence)),
            "work_id": self.work_id, "expected_revision": self.state["revision"],
            "recipe_id": self.recipe_id, "acceptance_ids": ["A01"],
        }
        self.check_result = run_registered_check(
            self.service, self.check_request, profile=self.profile, repository=self.root,
        )
        self.evidence = self.check_result["evidence"]

    @property
    def state(self):
        return self.service.snapshot(self.work_id)

    def call(self, command, **fields):
        try:
            revision = self.state["revision"]
        except WorkflowError:
            revision = 0
        return self.service.execute(command, {
            "operation_id": "synthetic-operation-" + str(next(self.sequence)),
            "work_id": self.work_id, "expected_revision": revision, **fields,
        })

    def prepare(self, **fields):
        return self.call("deliver", candidate_id=self.candidate["candidate_id"], **fields)["action"]

    def dispatch(self, action, **kwargs):
        return dispatch_action(self.service, {
            "operation_id": "synthetic-dispatch-" + str(next(self.sequence)),
            "work_id": self.work_id, "expected_revision": self.state["revision"],
            "action_id": action["action_id"],
        }, repository=self.root, **kwargs)

    def delivery(self, action, dispatched, *, status="verified", identity="synthetic-delivery"):
        return record(
            "delivery", delivery_id=identity, work_id=self.work_id,
            attempt_id=self.state["attempt"]["attempt_id"], candidate_id=self.candidate["candidate_id"],
            authority_id=self.state["authority"]["authority_id"], endpoint=self.contract["endpoint"],
            gate_ids=action["payload"]["gate_ids"], action_id=action["action_id"],
            receipt_id=dispatched["receipt_id"], observed_result="Adapter independently read endpoint",
            delivered_at=now(), merge_binding=None, resulting_merge=None, status=status,
        )


@pytest.mark.parametrize("profile_name", ["prose", "release-notes"])
def test_real_local_lifecycle_and_restart_idempotent_dispatch(tmp_path, profile_name):
    scenario = LocalLifecycle(tmp_path, profile_name=profile_name)
    assert scenario.evidence["execution_status"] == "PASS"
    scenario.service.store.require_artifact(scenario.evidence["artifact_hash"])
    artifact_path = scenario.state_dir / "artifacts" / scenario.evidence["artifact_hash"]
    artifact = json.loads(artifact_path.read_text())
    assert artifact_path.stat().st_mode & 0o077 == 0
    assert scenario.fixture_case["output_marker"] in artifact["output"]
    assert scenario.evidence["recipe_id"] == scenario.recipe_id
    assert scenario.evidence["argv"] == scenario.profile.recipe(scenario.recipe_id)["argv"]
    assert scenario.state["gate_ids"] == {}  # Editorial tier never fabricates independent gates.
    action = scenario.prepare()
    dispatched = scenario.dispatch(action)
    assert dispatched["status"] == "confirmed"
    assert dispatched["observation"]["head_sha"] == git(scenario.root, "rev-parse", "HEAD")
    assert dispatched["observation"]["tree_sha"] == git(scenario.root, "rev-parse", "HEAD^{tree}")
    assert dispatched["observation"]["independent_readback"] is True
    revision = scenario.state["revision"]
    scenario.service = WorkflowService(scenario.state_dir)

    def forbidden_reexecution(*args, **kwargs):
        pytest.fail("Confirmed dispatch must return its durable receipt without executing again")

    repeated = scenario.dispatch(action, git_factory=forbidden_reexecution,
                                 github_factory=forbidden_reexecution)
    assert repeated["receipt_id"] == dispatched["receipt_id"]
    assert repeated["revision"] == revision
    assert len(scenario.state["actions"][action["action_id"]]["receipts"]) == 1
    result = scenario.call("deliver", record=scenario.delivery(action, dispatched),
                           observation=dispatched["observation"])
    assert result["lifecycle"] == "done"
    assert scenario.state["delivery_id"] == "synthetic-delivery"
    assert scenario.repository.observe()["clean"] is True
    assert git(scenario.root, "show", f"main:{scenario.edit_path}") == (
        scenario.fixture_case["original_text"].strip())
    assert (scenario.root / "unrelated.txt").read_text() == "preserved unrelated content\n"


def test_real_candidate_change_blocks_prepared_local_delivery(tmp_path):
    scenario = LocalLifecycle(tmp_path)
    action = scenario.prepare()
    (scenario.root / "README.md").write_text("Changed after verification.\n")
    git(scenario.root, "add", "README.md")
    git(scenario.root, "commit", "-qm", "docs: change candidate after preparation")
    dispatched = scenario.dispatch(action)
    assert dispatched["status"] == "ambiguous"
    assert dispatched["observation"]["error_code"] == "candidate_drift"
    assert scenario.state["delivery_id"] is None
    assert scenario.state["lifecycle"] == "active"
    with pytest.raises(WorkflowError, match="confirmed prepared action"):
        scenario.call("deliver", record=scenario.delivery(action, dispatched),
                      observation=dispatched["observation"])


def test_simulated_release_exposed_unverified_cannot_become_verified(tmp_path):
    """This probes adapter result handling, not real GitHub release conformance."""
    scenario = LocalLifecycle(tmp_path, endpoint="release")
    action = scenario.prepare(expected_remote_state={"tag": "v0-test", "title": "Synthetic",
                                                       "notes": "Synthetic release response"})
    calls = []

    class SimulatedUnverifiedRemote:
        def publish_release(self, **kwargs):
            calls.append(kwargs)
            return {"release_id": "synthetic-release", "status": "exposed_unverified"}

    dispatched = scenario.dispatch(action, github_factory=lambda *args: SimulatedUnverifiedRemote())
    assert len(calls) == 1
    assert dispatched["observation"]["status"] == "exposed_unverified"
    assert dispatched["observation"]["verified"] is False
    with pytest.raises(WorkflowError, match="Independent adapter observation"):
        scenario.call("deliver", record=scenario.delivery(action, dispatched),
                      observation=dispatched["observation"])
    scenario.call("deliver", record=scenario.delivery(action, dispatched, status="exposed_unverified"),
                  observation=dispatched["observation"])
    assert scenario.state["lifecycle"] == "active"
    assert scenario.state["delivery_id"] is None
    assert scenario.state["blocker"]["code"] == "exposed_unverified"
    scenario.service = WorkflowService(scenario.state_dir)
    scenario.dispatch(action, github_factory=lambda *args: SimulatedUnverifiedRemote())
    assert len(calls) == 1
