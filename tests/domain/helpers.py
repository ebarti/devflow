"""Synthetic test records; never points at a real repository or host task."""

import itertools
from datetime import datetime, timezone

from devflow.application.commands import WorkflowService
from devflow.domain.rules import input_signature, scope_hash

H = "a" * 64
SHA = "b" * 40
NOW = datetime.now(timezone.utc).isoformat()


def record(record_type, **fields):
    return {"schema_version": 1, "record_type": record_type, **fields}


def contract(work_id="synthetic-work", tier=0, endpoint="local"):
    return record(
        "work_contract",
        work_id=work_id,
        scope_revision=1,
        source={
            "kind": "local_intake",
            "reference": "synthetic:user:instruction",
            "stable_id": work_id,
        },
        kind="editorial" if tier == 0 else "feature",
        title="Synthetic outcome",
        outcome="Synthetic invariant remains true",
        acceptance=[{"id": "A01", "expected": "invariant true", "invariant": "I01"}],
        scope={"paths": ["src"], "boundaries": ["synthetic only"]},
        context=[{"summary": "Synthetic reproducible setup", "reference": "fixture:setup"}],
        dependencies=[],
        risk={"tier": tier, "reason": "Synthetic tier"},
        verification={"recipes": ["unit"], "scenarios": ["A01"], "documentation_owners": []},
        endpoint={
            "kind": endpoint,
            "target": {
                "local": "/synthetic/worktree",
                "pr": "main",
                "merge": "main",
                "release": "v1-approved",
            }[endpoint],
        },
    )


def authority(c, authority_id="auth-1", repository="synthetic/repository"):
    return record(
        "authority",
        authority_id=authority_id,
        source_kind="user_instruction",
        source_reference="synthetic:explicit-user-request",
        work_id=c["work_id"],
        scope_hash=scope_hash(c),
        repository=repository,
        allowed_operations=[
            "edit",
            "check",
            "create_tasks",
            "publish_pr",
            "publish_findings",
            "push",
            "merge",
            "release",
            "publish_status",
        ],
        expires_at=None,
        revoked=False,
    )


def workflow_snapshot():
    return record(
        "workflow_snapshot",
        snapshot_id="snapshot-1",
        package_version="0.1.0",
        package_revision="a" * 40,
        workflow_hash=H,
        model_policy_hash=H,
        instruction_sources=[{"reference": "fixture:instructions", "hash": H}],
        repository_profile_reference="fixture:profile",
        effective_settings_reference="fixture:settings",
        captured_at=NOW,
    )


class Scenario:
    def __init__(
        self,
        path,
        tier=0,
        endpoint="local",
        work_id="synthetic-work",
        start=True,
        scenarios=None,
        repository="synthetic/repository",
    ):
        self.service = WorkflowService(path)
        self.work_id = work_id
        self.repository = repository
        self.sequence = itertools.count()
        self.contract = contract(work_id, tier, endpoint)
        if scenarios is not None:
            self.contract["verification"]["scenarios"] = scenarios
        self.call(
            "work.ready",
            record=self.contract,
            authority=authority(self.contract, repository=repository),
        )
        if start:
            self.start()

    @property
    def state(self):
        return self.service.snapshot(self.work_id)

    def call(self, command, **values):
        try:
            revision = self.state["revision"]
        except Exception:
            revision = 0
        return self.service.execute(
            command,
            {
                "operation_id": f"{self.work_id}-op-{next(self.sequence)}",
                "work_id": self.work_id,
                "expected_revision": revision,
                **values,
            },
        )

    def start(self):
        a = record(
            "attempt",
            attempt_id=f"attempt-{self.work_id}",
            work_id=self.work_id,
            scope_hash=scope_hash(self.contract),
            authority_id="auth-1",
            host_id="synthetic-host",
            owner_task_id="synthetic-owner",
            phase="implement",
            blocker=None,
            workflow_snapshot_id="snapshot-1",
            model_policy_snapshot_id="snapshot-1",
            revision=self.state["revision"],
            started_at=NOW,
            status="active",
        )
        result = self.call("work.start", record=a, workflow_snapshot=workflow_snapshot())
        self.confirm(result["action"], external_id="synthetic-workspace")

    def confirm(
        self, action, status="confirmed", external_id="synthetic-external", observation=None
    ):
        receipt = record(
            "action_receipt",
            action_id=action["action_id"],
            attempt_id=action["attempt_id"],
            operation=action["operation"],
            payload_hash=action["payload_hash"],
            expected_revision=action["expected_revision"],
            status=status,
            external_id=external_id,
            observations=["Synthetic readback"],
            recorded_at=NOW,
        )
        return self.call("action.record", record=receipt, observation=observation or {})

    def candidate(self, identity="candidate-1", tree=SHA):
        c = record(
            "candidate",
            candidate_id=identity,
            attempt_id=self.state["attempt"]["attempt_id"],
            scope_hash=self.state["scope_hash"],
            repository=self.repository,
            base_sha=SHA,
            head_sha=tree,
            tree_sha=tree,
            clean=True,
            dependency_hash=H,
            environment_hash=H,
            created_at=NOW,
        )
        self.call("candidate.record", record=c)
        return c

    def check(
        self,
        identity="check-1",
        candidate=None,
        status="PASS",
        assertions=1,
        observations=None,
        scenario_ids=None,
    ):
        candidate = candidate or self.state["records"]["candidate:" + self.state["candidate_id"]]
        evidence = record(
            "check_evidence",
            evidence_id=identity,
            candidate_id=candidate["candidate_id"],
            input_signature=H,
            recipe_id="unit",
            recipe_version="1",
            acceptance_ids=["A01"],
            argv=["synthetic-test"],
            cwd="/synthetic",
            environment_profile="synthetic",
            started_at=NOW,
            ended_at=NOW,
            process_status="success" if status == "PASS" else "failure",
            execution_status=status,
            executed_assertions=assertions,
            skipped_assertions=0,
            observations=observations or [],
            artifact_hash=self.service.put_artifact(b"Synthetic executed assertion evidence"),
        )
        if scenario_ids is not None:
            evidence["scenario_ids"] = scenario_ids
        evidence["input_signature"] = input_signature(candidate, evidence)
        self.call("check.record", record=evidence)
        return evidence

    def assignment(self, role="review", identity=None):
        identity = identity or f"assignment-{role}"
        result = self.call(
            "action.prepare",
            operation="send_role" if identity in self.state["assignments"] else "launch_role",
            payload={"role": role, "candidate_id": self.state["candidate_id"]},
        )
        action = result["action"]
        self.confirm(action, external_id=f"synthetic-{role}")
        a = record(
            "assignment",
            assignment_id=identity,
            action_id=action["action_id"],
            attempt_id=self.state["attempt"]["attempt_id"],
            role=role,
            owner_task_id="synthetic-owner",
            task_id=f"synthetic-{role}",
            client_id=None,
            candidate_id=self.state["candidate_id"],
            owned_paths=["src"],
            workspace_reference=f"synthetic:{role}:workspace",
            status="running",
        )
        self.call("assignment.record", record=a)
        return a

    def gate(
        self, role="review", identity=None, evidence_ids=None, status="PASS", fix_verifications=None
    ):
        a = self.state["assignments"][f"assignment-{role}"]
        gate = record(
            "gate_result",
            gate_id=identity or f"gate-{role}",
            assignment_id=a["assignment_id"],
            producer_task_id=a["task_id"],
            role=role,
            candidate_id=self.state["candidate_id"],
            scope_hash=self.state["scope_hash"],
            workflow_hash=H,
            status=status,
            evidence_ids=evidence_ids or ["check-1"],
            finding_ids=[],
            blocking_finding_ids=[],
            limitations=[],
            completed_at=NOW,
            fix_verification_ids=[v["verification_id"] for v in (fix_verifications or [])],
        )
        self.call("gate.record", record=gate, fix_verifications=fix_verifications or [])
        return gate

    def finding(self, identity="finding-1", severity="high", evidence_id="check-1"):
        f = record(
            "finding",
            finding_id=identity,
            work_id=self.work_id,
            candidate_id=self.state["candidate_id"],
            invariant_id="I01",
            severity=severity,
            summary="Synthetic violated invariant",
            evidence_ids=[evidence_id],
            detector="review",
            detected_at=NOW,
            disposition="open",
            publication="pending_pr",
            pr_reference=None,
            thread_id=None,
            comment_id=None,
            fix_reference=None,
            fix_evidence_ids=[],
            resolution_readback=False,
            related_work_id=None,
            fix_verification_ids=[],
            closure="not_due",
        )
        self.call("finding.record", record=f)
        return f

    def deliver(self):
        prepared = self.call("deliver", candidate_id=self.state["candidate_id"])
        action = prepared["action"]
        c = self.state["records"]["candidate:" + self.state["candidate_id"]]
        observation = {
            "action_id": action["action_id"],
            "payload_hash": action["payload_hash"],
            "candidate_id": c["candidate_id"],
            "head_sha": c["head_sha"],
            "tree_sha": c["tree_sha"],
            "endpoint": self.contract["endpoint"],
            "path": self.contract["endpoint"]["target"],
            "verified": True,
            "independent_readback": True,
        }
        receipt = self.confirm(action, observation=observation)
        delivery = record(
            "delivery",
            delivery_id="delivery-1",
            work_id=self.work_id,
            attempt_id=self.state["attempt"]["attempt_id"],
            candidate_id=c["candidate_id"],
            authority_id=self.state["authority"]["authority_id"],
            endpoint=self.contract["endpoint"],
            gate_ids=action["payload"]["gate_ids"],
            action_id=action["action_id"],
            receipt_id=receipt["receipt_id"],
            observed_result="Synthetic endpoint independently observed",
            delivered_at=NOW,
            merge_binding=None,
            resulting_merge=None,
            status="verified",
        )
        return self.call("deliver", record=delivery, observation=observation)
