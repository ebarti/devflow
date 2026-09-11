"""Bounded read-only follow-up issue resolution before a finding is deferred.

Only canonical github.com issue URLs are supported. The transport response stays
private; the durable record contains identity and state, never issue prose.
"""

from __future__ import annotations

import json
import re
from contextlib import closing
from datetime import UTC, datetime
from decimal import Decimal

from devflow.adapters.github import GitHubRepository
from devflow.errors import WorkflowError
from devflow.validation import digest

_OBSERVATION_FIELDS = {
    "kind", "reference", "repository", "issue_number", "issue_id", "issue_node_id",
    "state", "is_pull_request", "source_reference", "observed_at",
}


def issue_identity(reference):
    match = re.fullmatch(
        r"https://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)/issues/([1-9][0-9]*)",
        reference if isinstance(reference, str) else "",
    )
    if not match:
        raise WorkflowError("followup_required", "Deferral needs a canonical GitHub issue URL or unresolved follow-up work")
    owner, name, number = match.groups()
    return owner, name, int(number)


def validate_issue_observation(reference, observation):
    owner, name, number = issue_identity(reference)
    expected = {
        "kind": "github_issue", "reference": reference, "repository": f"github:{owner}/{name}",
        "issue_number": number, "state": "open", "is_pull_request": False,
        "source_reference": f"https://api.github.com/repos/{owner}/{name}/issues/{number}",
    }
    if (not isinstance(observation, dict) or set(observation) != _OBSERVATION_FIELDS
            or any(observation.get(key) != value for key, value in expected.items())
            or type(observation.get("issue_number")) is not int
            or type(observation.get("issue_id")) is not int or observation["issue_id"] < 1
            or not isinstance(observation.get("issue_node_id"), str)
            or not observation["issue_node_id"].strip()):
        raise WorkflowError("unverified_followup", "Follow-up readback must identify the exact open GitHub issue")
    try:
        observed = datetime.fromisoformat(observation["observed_at"].replace("Z", "+00:00"))
        if observed.tzinfo is None:
            raise ValueError("Observation timestamp needs a timezone")
    except (AttributeError, ValueError, TypeError) as exc:
        raise WorkflowError("unverified_followup", "Follow-up readback needs its observation time") from exc
    return observation


def defer_finding(service, request, *, github_factory=GitHubRepository):
    """The CLI supplies adapter observations through a separate, non-JSON channel."""
    if any(key in request for key in ("observation", "followup_observation", "deferral_observation")):
        raise WorkflowError("untrusted_followup", "Follow-up observations come from the adapter, not request JSON")
    service.preflight("finding.defer", request)
    # A confirmed import replays its original readback even if the issue later closes.
    with closing(service.store.connect()) as db:
        saved = db.execute("SELECT payload_hash,result FROM operations WHERE operation_id=?",
                           (request.get("operation_id"),)).fetchone()
    if saved:
        if saved[0] != digest({"command": "finding.defer", "request": request}):
            raise WorkflowError("operation_conflict", "Operation ID was already used with a different payload")
        return json.loads(saved[1], parse_float=Decimal)
    if request.get("related_work_id"):
        return service.execute("finding.defer", request)
    reference = request.get("followup_reference")
    owner, name, number = issue_identity(reference)
    issue = github_factory(owner, name).issue(number)
    root = f"https://api.github.com/repos/{owner}/{name}"
    if (not isinstance(issue, dict) or "pull_request" in issue or issue.get("state") != "open"
            or type(issue.get("number")) is not int or issue["number"] != number
            or issue.get("html_url") != reference or issue.get("url") != f"{root}/issues/{number}"
            or issue.get("repository_url") != root):
        raise WorkflowError("unverified_followup", "Follow-up must be the exact open GitHub issue, not a pull request")
    observation = validate_issue_observation(reference, {
        "kind": "github_issue", "reference": reference, "repository": f"github:{owner}/{name}",
        "issue_number": number, "issue_id": issue.get("id"), "issue_node_id": issue.get("node_id"),
        "state": "open", "is_pull_request": False, "source_reference": issue["url"],
        "observed_at": datetime.now(UTC).isoformat(),
    })
    return service.execute("finding.defer", request, deferral_observation=observation)
