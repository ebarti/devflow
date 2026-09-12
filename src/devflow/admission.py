"""Durable execution scope for an agent-recorded conversational user request.

The agent interprets the actual user request. This module enforces its recorded
scope and operations; caller JSON does not authenticate a human. Issue content,
labels and queue selection alone cannot create a user request.
"""

from copy import deepcopy
from datetime import datetime
from typing import Protocol

from jsonschema import Draft202012Validator

from devflow.errors import WorkflowError
from devflow.validation import digest, schema, validate_record

# These commands only retain observations or stop work. Reconciliation which
# resumes execution is intentionally absent.
BOOKKEEPING = frozenset({
    "work.cancel", "work.block", "action.record", "check.complete", "finding.record",
    "outcome.record", "usage.record", "usage.account", "segment.record", "gate.record",
})
READ_ONLY = frozenset({
    "doctor", "work.prepare", "work.show", "work.list", "next", "backlog.list",
    "backlog.show", "host.record", "host.wait", "host.result", "host.reconcile",
    "profile.inspect", "profile.route", "validate", "validate.record", "report",
    "report.usage", "report.metrics", "artifact.put", "snapshot.capture",
    "usage.collect", "usage.import", "install.plan", "install.apply", "install.rollback",
})


class TrustedIntakeVerifier(Protocol):
    def resolve(self, admission_id: str | None) -> dict:
        """Resolve a live authenticated decision, including revocation status.

        Authenticate the human for human_validation decisions; establish complete
        consumed lineage from trusted observations, never from agent origin flags.
        Return the same immutable decision on replay; deny revoked decisions.
        """
        ...


class UnavailableIntakeVerifier:
    def resolve(self, admission_id):
        raise WorkflowError(
            "trusted_intake_unavailable",
            "No authenticated intake or independent human-validation adapter is installed; "
            "execution is blocked. Caller approval references cannot replace this capability.",
        )


def user_request_admission(state, contract, user_request, repository, *, continuation_of=None):
    """Derive a replayable binding, not a proof of human authentication."""
    from devflow.domain.rules import scope_hash

    validator = Draft202012Validator({
        "$ref": "#/$defs/UserRequest", "$defs": schema()["$defs"],
    })
    error = next(validator.iter_errors(user_request), None)
    if error:
        raise WorkflowError("invalid_user_request", f"user_request: {error.message}")
    validate_record(contract, "work_contract")
    if not repository:
        raise WorkflowError("admission_repository_required", "Execution needs a bound repository")
    if contract["work_id"] != state["work_id"]:
        raise WorkflowError("admission_binding", "Request and contract work differ")
    admission = {
        "schema_version": 1, "record_type": "intake_admission",
        "repository": repository, "work_id": state["work_id"],
        "scope_hash": scope_hash(contract), "source": deepcopy(contract["source"]),
        "source_digest": digest(contract["source"]), "decision_kind": "user_request",
        "decision_reference": user_request["reference"], "user_request": deepcopy(user_request),
        "allowed_operations": list(user_request["allowed_operations"]),
        "expires_at": None, "revoked": False,
    }
    if continuation_of is not None:
        admission["continuation_of"] = deepcopy(continuation_of)
    admission["admission_id"] = "request-" + digest(admission)
    return admission


def requested_admission(state, request, verifier, now, *, repository):
    """Ready/amend accepts either a direct request or an explicit embedding port."""
    contract = request.get("record", {})
    if "user_request" in request:
        allowed = {"operation_id", "work_id", "expected_revision", "record", "user_request",
                   "approved_delta", "workflow_snapshot"}
        if set(request) - allowed:
            raise WorkflowError("invalid_request", "Unknown fields in user-request admission")
        admission = user_request_admission(state, contract, request["user_request"], repository)
        return execution_admission(
            state, contract, admission["admission_id"], verifier, now,
            repository=repository, requested=admission,
        )
    if verifier is None or isinstance(verifier, UnavailableIntakeVerifier):
        raise WorkflowError(
            "user_request_required",
            "Record the user's work request with reference, summary and allowed_operations; "
            "issue data, labels and legacy approval fields do not authorize work.",
        )
    return execution_admission(
        state, contract, request.get("admission_id"), verifier, now, repository=repository,
    )


def source_prerequisites(source):
    """Execution prerequisites beyond the legacy-compatible Source schema."""
    lineage = source.get("lineage", [])
    missing = []
    if not lineage:
        missing.append("source.lineage")
    if source.get("consumed_digest") != digest(lineage):
        missing.append("source.consumed_digest")
    return missing


def execution_admission(state, contract, admission_id, verifier, now, *, repository=None,
                        operation=None, requested=None):
    """One predicate for admission, continuation and actual dispatch.

    Direct requests resume from their immutable stored binding. Optional embedding
    decisions still resolve live; historical admissions never become direct requests.
    """
    from devflow.domain.rules import scope_hash

    stored = state["records"].get("intake_admission:" + str(admission_id))
    admission = requested or stored
    if admission is not None and admission.get("decision_kind") == "user_request":
        validate_record(admission, "intake_admission")
        expected = user_request_admission(
            state, contract, admission["user_request"], repository,
            continuation_of=admission.get("continuation_of"),
        )
        if admission != expected:
            raise WorkflowError("admission_binding", "Stored user request differs from its exact binding")
        admission = deepcopy(admission)
    elif verifier is not None and not isinstance(verifier, UnavailableIntakeVerifier):
        admission = deepcopy(verifier.resolve(admission_id))
    else:
        raise WorkflowError(
            "user_request_required",
            "No recorded user-request admission covers this work; record a request with ready or amend.",
        )
    validate_record(admission, "intake_admission")
    if repository is None:
        raise WorkflowError("admission_repository_required", "Execution needs a bound repository")
    source = contract.get("source", {})
    lineage = source.get("lineage", [])
    if (
        source_prerequisites(source)
        or admission["source"] != source
        or admission["source_digest"] != digest(source)
    ):
        raise WorkflowError("admission_source", "Admission does not cover exact consumed source lineage")
    if (
        admission["admission_id"] != admission_id
        or admission["work_id"] != state["work_id"]
        or contract["work_id"] != state["work_id"]
        or admission["scope_hash"] != scope_hash(contract)
        or (repository is not None and admission["repository"] != repository)
    ):
        raise WorkflowError("admission_binding", "Admission repository, work or scope differs")
    if admission["decision_kind"] == "trusted_first_party" and any(
        material["origin"] != "internal" for material in lineage
    ):
        raise WorkflowError("human_validation_required", "External or unknown lineage requires human validation")
    if admission["revoked"]:
        raise WorkflowError("revoked_admission", "Admission was revoked")
    expires = admission["expires_at"]
    if expires is not None and datetime.fromisoformat(expires.replace("Z", "+00:00")) <= now:
        raise WorkflowError("expired_admission", "Admission has expired")
    if operation and operation not in admission["allowed_operations"]:
        raise WorkflowError("admission_operation", "Admission does not allow this operation")
    if state.get("admission_id") == admission_id and state.get("authority") != derived_authority(admission):
        raise WorkflowError("admission_binding", "Stored authority differs from verified admission")
    old = state["records"].get("intake_admission:" + admission_id)
    if old is not None and old != admission:
        raise WorkflowError("immutable_admission", "A decision changed; obtain a new admission")
    return admission


def derived_authority(admission):
    return {
        "schema_version": 1, "record_type": "authority",
        "authority_id": admission["admission_id"], "source_kind": "user_instruction",
        "source_reference": admission["decision_reference"],
        **{key: admission[key] for key in (
            "repository", "work_id", "scope_hash", "allowed_operations", "expires_at", "revoked"
        )},
    }
