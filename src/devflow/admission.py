"""Execution admission through an independently trusted host decision.

The default has no authenticated intake/human channel. Request JSON, GitHub
identity, labels, and local signatures cannot implement this port. An embedding
host must supply a verifier whose decision store the consuming agent cannot mint
or rewrite. The shipped CLI deliberately has no such adapter.
"""

from copy import deepcopy
from datetime import datetime
from typing import Protocol

from devflow.errors import WorkflowError
from devflow.validation import digest, validate_record

# These commands only retain observations or stop work. Reconciliation which
# resumes execution is intentionally absent.
BOOKKEEPING = frozenset({
    "work.cancel", "work.block", "action.record", "check.complete", "finding.record",
    "outcome.record", "usage.record", "segment.record",
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


def execution_admission(state, contract, admission_id, verifier, now, *, repository=None,
                        operation=None):
    """One predicate for admission, continuation and actual dispatch.

    Only the verifier supplies the admission. Stored copies establish immutability,
    not trust: every new execution resolves the live decision again.
    """
    from devflow.domain.rules import scope_hash

    admission = deepcopy((verifier or UnavailableIntakeVerifier()).resolve(admission_id))
    validate_record(admission, "intake_admission")
    if repository is None:
        raise WorkflowError("admission_repository_required", "Trusted execution needs a bound repository")
    source = contract.get("source", {})
    lineage = source.get("lineage", [])
    if (
        not lineage
        or source.get("consumed_digest") != digest(lineage)
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
