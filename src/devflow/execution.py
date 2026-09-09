"""Dispatch committed intents, then persist independent adapter observations.

Native task operations remain in the owner tool context. This boundary never
turns native tool names into HTTP endpoints or starts an unobservable model.
"""

from __future__ import annotations

import fcntl
import hashlib
import os
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from devflow.adapters.git import GitRepository
from devflow.adapters.github import GitHubRepository
from devflow.domain.endpoints import TARGET_FIELD, validate_action_target
from devflow.domain.rules import (
    PERMISSIONS,
    authority,
    blocking_findings,
    missing_checks,
    missing_scenarios,
    required_roles,
    technically_fixed,
    valid_gates,
)
from devflow.errors import WorkflowError
from devflow.validation import digest


def proof_binding(state: dict) -> str:
    candidate = state["records"]["candidate:" + state["candidate_id"]]
    snapshot = state["records"]["workflow_snapshot:" + state["attempt"]["workflow_snapshot_id"]]
    return digest(
        {
            "candidate": candidate,
            "scope_hash": state["scope_hash"],
            "workflow_hash": snapshot["workflow_hash"],
            "gate_ids": sorted(g["gate_id"] for g in valid_gates(state).values()),
        }
    )


def observe_candidate(repository, candidate, git_factory=GitRepository):
    git = git_factory(repository)
    observed = git.observe()
    if not observed["clean"] or any(observed[k] != candidate[k] for k in ("head_sha", "tree_sha")):
        raise WorkflowError("candidate_drift", "Checkout changed since the candidate was verified")
    if git.identity() != candidate["repository"]:
        raise WorkflowError("repository_mismatch", "Checkout is outside the admitted repository")
    return observed


@contextmanager
def _action_lock(root: Path, identity: str):
    directory = root / "action-locks"
    if directory.is_symlink():
        raise WorkflowError("unsafe_state", "Action lock directory cannot be a symlink")
    directory.mkdir(mode=0o700, exist_ok=True)
    key = hashlib.sha256(identity.encode()).hexdigest()
    fd = os.open(directory / key, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def _call(service, command, state, operation_id, **fields):
    return service.execute(
        command,
        {
            "operation_id": operation_id,
            "work_id": state["work_id"],
            "expected_revision": state["revision"],
            **fields,
        },
    )


def _proof_ready(state):
    if (
        missing_checks(state)
        or missing_scenarios(state)
        or blocking_findings(state, state["candidate_id"])
        or not (set(required_roles(state)) <= set(valid_gates(state)))
    ):
        raise WorkflowError(
            "incomplete_proof", "Required checks, findings, or independent gates remain"
        )


def _remote(state, factory):
    identity = state["authority"]["repository"]
    if not identity.startswith("github:") or len(identity[7:].split("/")) != 2:
        raise WorkflowError(
            "unsupported_remote", "This operation requires an enrolled GitHub repository"
        )
    return factory(*identity[7:].split("/"))


def _endpoint_observation(state, kind, value, verified):
    field = TARGET_FIELD[kind]
    actual = value.get(field)
    verified = bool(verified and actual == state["contract"]["endpoint"]["target"])
    return {
        "endpoint": {"kind": kind, "target": actual},
        field: actual,
        "verified": verified,
        "status": "verified" if verified else "exposed_unverified",
    }


def _perform(state, action, repository, *, github_factory, git_factory, reconcile):
    remote_started = False
    remote = None

    def tracked_remote(*args):
        nonlocal remote_started, remote
        remote_started = True
        remote = github_factory(*args)
        return remote

    try:
        return _perform_operation(
            state, action, repository, github_factory=tracked_remote,
            git_factory=git_factory, reconcile=reconcile,
        )
    except WorkflowError as exc:
        # Checkout checks precede remote construction. Once an adapter exists,
        # only its complete operation history can establish that no write began.
        if not remote_started or getattr(remote, "_mutation_may_have_applied", None) is False:
            exc.details["no_mutation"] = True
        raise


def _perform_operation(state, action, repository, *, github_factory, git_factory, reconcile):
    operation, payload, refs = (
        action["operation"],
        action["payload"],
        action["expected_remote_state"],
    )
    validate_action_target(
        state,
        operation,
        payload,
        refs,
        repository_path=str(Path(repository).resolve()),
        terminal=action.get("terminal_delivery", False),
    )
    candidate = state["records"]["candidate:" + state["candidate_id"]]
    observed_checkout = observe_candidate(repository, candidate, git_factory)
    if action.get("terminal_delivery"):
        _proof_ready(state)
    base = {
        "action_id": action["action_id"],
        "payload_hash": action["payload_hash"],
        "candidate_id": candidate["candidate_id"],
        "head_sha": candidate["head_sha"],
        "tree_sha": candidate["tree_sha"],
        "independent_readback": True,
    }
    if operation == "local_delivery":
        return {
            **base,
            **_endpoint_observation(state, "local", observed_checkout, True),
            "external_id": observed_checkout["path"],
        }
    github = _remote(state, github_factory)
    if operation == "publish_finding":
        finding = state["findings"][payload["finding_id"]]
        method = github.reconcile_finding if reconcile else github.publish_finding
        value = method(
            payload["pr_number"],
            finding_id=finding["finding_id"],
            body=payload["body"],
            path=payload["path"],
            expected_head=candidate["head_sha"],
            line=payload.get("line"),
            side=payload.get("side", "RIGHT"),
        )
        if value is None:
            raise WorkflowError(
                "ambiguous_action", "No unique finding readback; do not repeat publication"
            )
        return {
            **base,
            **value,
            "finding_id": finding["finding_id"],
            "pr_reference": f"https://github.com/{state['authority']['repository'][7:]}/pull/{payload['pr_number']}",
            "comment_id": str(value["comment_id"]),
            "external_id": value["thread_id"],
        }
    if operation == "resolve_thread":
        _proof_ready(state)
        finding = state["findings"][payload["finding_id"]]
        if not technically_fixed(state, finding, candidate["candidate_id"]):
            raise WorkflowError("unverified_fix", "Independent verification must precede closure")
        if finding["publication"] != "published":
            raise WorkflowError("unpublished_finding", "A finding must be published before closure")
        method = github.reconcile_thread_closure if reconcile else github.close_thread
        value = method(
            payload["pr_number"],
            thread_id=finding["thread_id"],
            finding_id=finding["finding_id"],
            proof=payload["proof"],
            expected_head=candidate["head_sha"],
            fix_commit=finding["fix_reference"],
        )
        if value is None:
            raise WorkflowError("ambiguous_action", "Thread closure remains uncertain")
        return {
            **base,
            **value,
            "external_id": finding["thread_id"],
            "finding_id": finding["finding_id"],
            "fix_reference": finding["fix_reference"],
            "resolved": value["status"] == "resolved",
            "fix_contained": True,
            "reply_verified": bool(value["reply_id"]),
        }
    if operation == "publish_status":
        if payload["state"] == "success":
            _proof_ready(state)
        binding = proof_binding(state)
        if payload["binding_hash"] != binding:
            raise WorkflowError(
                "stale_proof_binding", "Status binding does not match current evidence"
            )
        method = github.reconcile_status if reconcile else github.publish_status
        value = method(candidate["head_sha"], binding_hash=binding, state=payload["state"])
        if value is None:
            raise WorkflowError("ambiguous_action", "Proof status publication remains uncertain")
        return {**base, **value, "external_id": str(value.get("id", candidate["head_sha"]))}
    if operation == "merge":
        _proof_ready(state)
        binding = payload["merge_binding"]
        if len(binding["source_heads"]) != 1:
            raise WorkflowError(
                "stack_conformance_required",
                "Atomic stack backend must pass enrollment conformance",
            )
        source = binding["source_heads"][0]
        number = int(source["pr_reference"].rstrip("/").rsplit("/", 1)[-1])
        kwargs = {
            "expected_head": candidate["head_sha"],
            "target_ref": binding["target_ref"],
            "target_sha": binding["target_sha"],
            "expected_tree": binding["expected_integrated_tree"],
            "protection_snapshot_hash": binding["protection_snapshot_hash"],
            "method": binding["merge_method"],
        }
        value = (
            github.reconcile_delivery(number, **kwargs)
            if reconcile
            else github.deliver(number, binding_hash=proof_binding(state), **kwargs)
        )
        if value is None:
            raise WorkflowError(
                "ambiguous_action", "Merge has no confirmed result; do not repeat mutation"
            )
        verified = value["status"] == "verified"
        return {
            **base,
            **_endpoint_observation(state, "merge", value, verified),
            "refs": refs,
            "protection_verified": verified,
            "required_checks_verified": verified,
            "merge_binding": binding,
            "resulting_merge": {
                "commit_sha": value["commit_sha"],
                "tree_sha": value["tree_sha"],
                "target_ancestry_verified": value["target_ancestry_verified"],
            },
            "external_id": value["commit_sha"],
            "remote": value,
        }
    if operation == "publish_pr":
        values = {**refs, **payload}
        publication_id = payload.get("publication_action_id", action["action_id"])
        if publication_id != action["action_id"]:
            publication = state["actions"].get(publication_id)
            if (
                publication is None
                or publication["operation"] != "publish_pr"
                or publication["status"] != "confirmed"
                or not action.get("terminal_delivery")
            ):
                raise WorkflowError(
                    "publication_conflict", "Terminal PR correlation is not a confirmed publication"
                )
            method = github.reconcile_pr
        else:
            method = github.reconcile_pr if reconcile else github.publish_pr
        value = method(
            head_ref=values["head_ref"],
            base_ref=values["base_ref"],
            expected_head=candidate["head_sha"],
            title=values["title"],
            body=values["body"],
            action_id=publication_id,
        )
        if value is None:
            raise WorkflowError("ambiguous_action", "PR publication is not uniquely confirmed")
        return {
            **base,
            **value,
            **base,
            **_endpoint_observation(
                state,
                "pr",
                value,
                value.get("status") == "published"
                and value.get("head_sha") == candidate["head_sha"],
            ),
            "refs": refs,
            "remote": value,
            "publication_action_id": publication_id,
            "external_id": str(value["pr_number"]),
        }
    if operation == "release":
        values = {**refs, **payload}
        method = github.reconcile_release if reconcile else github.publish_release
        value = method(
            tag=values["tag"],
            expected_sha=candidate["head_sha"],
            title=values["title"],
            notes=values["notes"],
            action_id=action["action_id"],
        )
        if value is None:
            raise WorkflowError("ambiguous_action", "Release has no unique confirmed readback")
        return {
            **base,
            **value,
            **base,
            **_endpoint_observation(
                state,
                "release",
                value,
                value.get("status") == "published"
                and value.get("commit_sha") == candidate["head_sha"],
            ),
            "refs": refs,
            "remote": value,
            "external_id": str(value["release_id"]),
        }
    if operation == "sync_projection":
        if payload["content_id"] != state["contract"]["source"]["stable_id"]:
            raise WorkflowError(
                "projection_identity", "Project content must match the work issue identity"
            )
        kwargs = {
            "project_id": payload["project_id"],
            "content_id": payload["content_id"],
            "field_updates": payload["field_updates"],
            "action_id": action["action_id"],
        }
        value = (
            github.reconcile_project_item(**kwargs)
            if reconcile
            else github.sync_project_item(
                **kwargs, expected_owned_fields=payload["expected_owned_fields"]
            )
        )
        if value is None:
            raise WorkflowError("ambiguous_action", "Project synchronization remains uncertain")
        return {**base, **value, "external_id": value["item_id"]}
    raise WorkflowError("unsupported_dispatch", f"No automatic dispatcher for {operation}")


def dispatch_action(
    service, request, *, repository, github_factory=GitHubRepository, git_factory=GitRepository
):
    work_id, action_id = request["work_id"], request["action_id"]
    with _action_lock(service.store.root, action_id):
        state = service.snapshot(work_id)
        action = state["actions"].get(action_id)
        if not action:
            raise WorkflowError("unknown_action", "No committed intent exists")
        if action["status"] == "confirmed":
            return {
                "action_id": action_id,
                "status": "confirmed",
                "observation": action["observation"],
                "revision": state["revision"],
                "receipt_id": action["receipts"][-1],
            }
        if action["operation"] in {"launch_role", "send_role", "prepare_workspace"}:
            return {
                "action": action,
                "requires_native_owner": True,
                "reason": "Use native task tools or workspace register, then record actual receipt",
            }
        if action["status"] == "invalidated":
            raise WorkflowError("stale_action", "Action was invalidated")
        if action["status"] == "failed":
            raise WorkflowError(
                "retry_required", "Use action retry to re-admit a definitely failed action"
            )
        if state["revision"] != request["expected_revision"]:
            raise WorkflowError("stale_revision", "Work changed before action dispatch")
        if action["payload"]["scope_hash"] != state["scope_hash"] or (
            action["payload"]["candidate_id"] != state["candidate_id"]
        ):
            raise WorkflowError("stale_action", "Action scope or candidate changed")
        authority(state, datetime.now(UTC), PERMISSIONS[action["operation"]])
        validate_action_target(
            state,
            action["operation"],
            action["payload"],
            action["expected_remote_state"],
            repository_path=str(Path(repository).resolve()),
            terminal=action.get("terminal_delivery", False),
        )
        if action["operation"] in {"local_delivery", "merge", "release"} and not action.get(
            "terminal_delivery"
        ):
            raise WorkflowError(
                "unprepared_delivery", "Endpoint action lacks its evaluated delivery intent"
            )
        reconcile = action["status"] != "prepared"
        if not reconcile:
            _call(
                service,
                "action.begin",
                state,
                request["operation_id"] + ":begin",
                action_id=action_id,
            )
            state = service.snapshot(work_id)
            action = state["actions"][action_id]
        try:
            observation = _perform(
                state,
                action,
                repository,
                github_factory=github_factory,
                git_factory=git_factory,
                reconcile=reconcile,
            )
            status, external_id = "confirmed", str(observation["external_id"])
        except WorkflowError as exc:
            observation = {
                "error_code": exc.code,
                "message": str(exc),
                "independent_readback": False,
                "no_mutation": exc.details.get("no_mutation") is True,
            }
            # A read-only reconciliation cannot establish that the earlier
            # interrupted invocation did not write, even if this read did not.
            status = "failed" if observation["no_mutation"] and not reconcile else "ambiguous"
            external_id = None
        receipt = {
            "schema_version": 1,
            "record_type": "action_receipt",
            "action_id": action_id,
            "attempt_id": action["attempt_id"],
            "operation": action["operation"],
            "payload_hash": action["payload_hash"],
            "expected_revision": action["expected_revision"],
            "status": status,
            "external_id": external_id,
            "observations": [
                "Independent adapter readback"
                if status == "confirmed"
                else "Action outcome unresolved; reconcile before retry"
            ],
            "recorded_at": datetime.now(UTC).isoformat(),
        }
        result = _call(
            service,
            "action.record",
            state,
            request["operation_id"] + ":receipt",
            record=receipt,
            observation=observation,
        )
        return {**result, "action_id": action_id, "status": status, "observation": observation}
