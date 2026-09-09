"""Durable admission and recovery boundary around expensive check execution."""

from __future__ import annotations

import fcntl
import os
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from devflow.adapters.git import GitRepository
from devflow.checks import run_check
from devflow.domain.rules import input_signature
from devflow.errors import WorkflowError
from devflow.profiles import assert_admitted_profile
from devflow.validation import canonical_json, digest


def _private_directory(root, name):
    path = root / name
    if path.is_symlink():
        raise WorkflowError("unsafe_state", "Check execution directories cannot be symlinks")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


@contextmanager
def _action_lock(root, key):
    directory = _private_directory(root, "check-runs")
    descriptor = os.open(directory / f"{key}.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield directory
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _observe(repository, candidate):
    git = GitRepository(repository)
    observed = git.observe()
    if git.identity() != candidate["repository"]:
        raise WorkflowError("repository_mismatch", "Check checkout belongs to another repository")
    if not observed["clean"] or any(observed[k] != candidate[k] for k in ("head_sha", "tree_sha")):
        raise WorkflowError(
            "candidate_drift", "Check checkout no longer matches its clean candidate"
        )
    return observed


def _write_draft(directory, key, draft):
    destination = directory / f"{key}.result.json"
    temporary = directory / f".pending-{uuid4().hex}"
    try:
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            stream.write(canonical_json(draft))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
        directory_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def run_registered_check(service, request, *, profile, repository, runner=None):
    """Run once, or return the committed result of this exact logical request.

    Interrupted dispatched checks require explicit reconciliation. A recoverable
    result draft is retained privately if completion loses a revision race.
    ``runner`` optionally supplies the subprocess-compatible check runner.
    """
    required = {"operation_id", "work_id", "expected_revision", "recipe_id", "acceptance_ids"}
    if not isinstance(request, dict) or not required <= request.keys():
        raise WorkflowError(
            "invalid_request", "Check run requires operation/work/revision/recipe/acceptance IDs"
        )
    if not isinstance(request["operation_id"], str) or not request["operation_id"]:
        raise WorkflowError("invalid_request", "Check operation_id must be nonempty")
    if type(request["expected_revision"]) is not int or request["expected_revision"] < 0:
        raise WorkflowError(
            "invalid_request", "Check expected_revision must be a nonnegative integer"
        )
    if (
        not isinstance(request["recipe_id"], str)
        or not request["recipe_id"]
        or not isinstance(request["acceptance_ids"], list)
        or not all(isinstance(identity, str) and identity for identity in request["acceptance_ids"])
        or not isinstance(request.get("environment_profile", "local"), str)
        or not request.get("environment_profile", "local")
    ):
        raise WorkflowError(
            "invalid_request", "Check recipe, acceptance and environment values must be strings"
        )
    repository = Path(repository).resolve()
    key = digest(request["operation_id"])
    logical_hash = digest({"request": request, "repository": str(repository)})
    completion_id = f"check-complete-{key}"
    with _action_lock(service.store.root, key) as directory:
        prior = service.store.operation(request["operation_id"])
        state = service.snapshot(request["work_id"])
        if prior:
            prepared = prior["result"].get("action")
            if (
                not prepared
                or prepared["operation"] != "run_check"
                or prepared["payload"].get("request_hash") != logical_hash
            ):
                raise WorkflowError(
                    "operation_conflict", "Operation ID was used with a different check request"
                )
            action = state["actions"].get(prepared["action_id"])
            completed = service.store.operation(completion_id)
            if completed:
                return completed["result"]
            if action is None or action["status"] != "prepared":
                raise WorkflowError(
                    "reconcile_required",
                    "Interrupted check requires reconciliation; execution was not repeated",
                    {
                        "action_id": prepared["action_id"],
                        "result_draft": str(directory / f"{key}.result.json"),
                    },
                )
            expected_revision = prepared["expected_revision"]
        else:
            if state["revision"] != request["expected_revision"]:
                raise WorkflowError("stale_revision", "Work changed before check admission")
            if not state["candidate_id"] or state["attempt"] is None:
                raise WorkflowError(
                    "missing_candidate", "Record an active candidate before running a check"
                )
            candidate = state["records"]["candidate:" + state["candidate_id"]]
            snapshot = state["records"][
                "workflow_snapshot:" + state["attempt"]["workflow_snapshot_id"]
            ]
            assert_admitted_profile(profile, snapshot["repository_profile_reference"])
            if profile.root.resolve() != repository:
                raise WorkflowError(
                    "repository_mismatch", "Check profile belongs to another checkout"
                )
            recipe = profile.recipe(request["recipe_id"])
            if not isinstance(request["acceptance_ids"], list) or not set(
                request["acceptance_ids"]
            ) <= {a["id"] for a in state["contract"]["acceptance"]}:
                raise WorkflowError(
                    "unknown_acceptance", "Check names an unknown acceptance criterion"
                )
            _observe(repository, candidate)
            prepared = service.execute(
                "action.prepare",
                {
                    "operation_id": request["operation_id"],
                    "work_id": request["work_id"],
                    "expected_revision": request["expected_revision"],
                    "operation": "run_check",
                    "payload": {
                        "request_hash": logical_hash,
                        "candidate_id": candidate["candidate_id"],
                        "recipe_id": request["recipe_id"],
                        "recipe_version": digest(recipe),
                        "profile_hash": profile.fingerprint,
                        "acceptance_ids": request["acceptance_ids"],
                        "environment_profile": request.get("environment_profile", "local"),
                        "dependency_hash": candidate["dependency_hash"],
                        "environment_hash": candidate["environment_hash"],
                        "argv": recipe["argv"],
                        "cwd": str((profile.root / recipe.get("cwd", ".")).resolve()),
                    },
                    "expected_remote_state": {
                        k: candidate[k] for k in ("repository", "head_sha", "tree_sha")
                    },
                },
            )["action"]
            action = prepared
            expected_revision = prepared["expected_revision"]
        # Recheck the pinned policy and actual checkout before the dispatch claim.
        candidate = state["records"]["candidate:" + action["payload"]["candidate_id"]]
        if profile.fingerprint != action["payload"]["profile_hash"]:
            raise WorkflowError(
                "profile_drift", "Prepared check policy changed; reconcile before execution"
            )
        _observe(repository, candidate)
        begun = service.execute(
            "action.begin",
            {
                "operation_id": f"check-begin-{key}",
                "work_id": request["work_id"],
                "expected_revision": expected_revision,
                "action_id": action["action_id"],
            },
        )
        evidence = run_check(
            profile,
            request["recipe_id"],
            candidate,
            acceptance_ids=request["acceptance_ids"],
            state_dir=service.store.root,
            put_artifact=service.put_artifact,
            environment_profile=request.get("environment_profile", "local"),
            runner=runner,
        )
        evidence["evidence_id"] = f"check-evidence-{key}"
        checkout_verified = True
        try:
            after = _observe(repository, candidate)
        except WorkflowError as exc:
            checkout_verified = False
            after = {"error": exc.code, **exc.details}
            evidence["execution_status"] = "BLOCKED"
            evidence["scenario_ids"] = []
            evidence["observations"].append(
                f"Post-check candidate verification blocked: {exc.code}"
            )
        evidence["input_signature"] = input_signature(candidate, evidence)
        receipt = {
            "schema_version": 1,
            "record_type": "action_receipt",
            "action_id": action["action_id"],
            "attempt_id": action["attempt_id"],
            "operation": "run_check",
            "payload_hash": action["payload_hash"],
            "expected_revision": action["expected_revision"],
            "status": "confirmed",
            "external_id": evidence["evidence_id"],
            "observations": ["Check process completed; candidate rechecked"],
            "recorded_at": datetime.now(UTC).isoformat(),
        }
        observation = {
            **action["expected_remote_state"],
            "artifact_hash": evidence["artifact_hash"],
            "execution_status": evidence["execution_status"],
            "checkout_verified": checkout_verified,
            "after": after,
        }
        completion = {
            "operation_id": completion_id,
            "work_id": request["work_id"],
            "expected_revision": begun["revision"],
            "record": evidence,
            "receipt": receipt,
            "receipt_id": f"check-receipt-{key}",
            "observation": observation,
        }
        _write_draft(directory, key, completion)
        return service.execute("check.complete", completion)
