"""Direct conversational requests bind durable scope without authenticating the caller."""

from copy import deepcopy

import pytest
from domain.helpers import contract
from test_admission import REPOSITORY, assert_code, setup

from devflow.application.commands import WorkflowService
from devflow.validation import digest


def user_request(**overrides):
    return {
        "reference": "conversation:synthetic-request-1",
        "summary": "Fix the selected synthetic issue and verify its acceptance criteria",
        "allowed_operations": ["edit", "check"],
        **overrides,
    }


def ready_request(c, **overrides):
    return {
        "operation_id": "ready-" + c["work_id"], "work_id": c["work_id"],
        "expected_revision": 0, "record": c, "user_request": user_request(), **overrides,
    }


@pytest.mark.parametrize("origin", ["internal", "external", "unknown"])
@pytest.mark.parametrize("kind", ["feature", "bug"])
def test_request_admits_selected_issue_as_data_and_resumes(tmp_path, origin, kind):
    c = contract()
    c["kind"] = kind
    c["source"]["lineage"][0]["origin"] = origin
    c["source"]["consumed_digest"] = digest(c["source"]["lineage"])
    request = ready_request(c)
    service = WorkflowService(tmp_path, repository=REPOSITORY)
    ready = service.execute("work.ready", request)
    assert ready["admission_id"] == ready["authority_id"]
    restarted = WorkflowService(tmp_path, repository=REPOSITORY)
    assert restarted.execute("work.ready", request) == ready
    admission = restarted.require_execution(restarted.snapshot(c["work_id"]), operation="check")
    assert admission["decision_kind"] == "user_request"
    assert admission["user_request"] == request["user_request"]
    assert admission["source"] == c["source"]


@pytest.mark.parametrize("invalid", [
    None, False, "yes", [], {},
    user_request(reference=" "), user_request(summary="\n"),
    user_request(allowed_operations=[]), user_request(allowed_operations=["edit", "edit"]),
    user_request(allowed_operations=["approved"]), user_request(approved=True),
    user_request(reference=1), user_request(summary=None),
])
def test_malformed_request_never_creates_work(tmp_path, invalid):
    service = WorkflowService(tmp_path, repository=REPOSITORY)
    assert_code("invalid_user_request", lambda: service.execute(
        "work.ready", ready_request(contract(), user_request=invalid),
    ))
    assert service.list_works(REPOSITORY) == []


@pytest.mark.parametrize("field", ["authority", "admission", "admission_id", "approved", "unknown"])
def test_legacy_and_unknown_fields_cannot_override_direct_request(tmp_path, field):
    service = WorkflowService(tmp_path, repository=REPOSITORY)
    assert_code("invalid_request", lambda: service.execute(
        "work.ready", ready_request(contract(), **{field: "invented"}),
    ))


@pytest.mark.parametrize("field", ["repository", "work_id", "scope_hash", "allowed_operations"])
def test_stored_admission_fields_cannot_drift(tmp_path, field):
    service = WorkflowService(tmp_path, repository=REPOSITORY)
    ready = service.execute("work.ready", ready_request(contract()))
    state = service.snapshot("synthetic-work")
    admission = state["records"]["intake_admission:" + ready["admission_id"]]
    admission[field] = ["edit", "check", "merge"] if field == "allowed_operations" else (
        "f" * 64 if field == "scope_hash" else "other"
    )
    assert_code("admission_binding", lambda: service.require_execution(state))


def test_repo_work_and_operation_limits_are_enforced(tmp_path):
    c = contract()
    service = WorkflowService(tmp_path, repository=REPOSITORY)
    assert_code("admission_binding", lambda: service.execute(
        "work.ready", ready_request(c, work_id="another-work"),
    ))
    service.execute("work.ready", ready_request(c))
    state = service.snapshot(c["work_id"])
    other = WorkflowService(tmp_path, repository="github:other/repository")
    assert_code("admission_binding", lambda: other.require_execution(state))
    assert_code("admission_operation", lambda: service.require_execution(state, operation="merge"))


@pytest.mark.parametrize("changed", ["scope", "source"])
def test_exact_binding_requires_recorded_amendment(tmp_path, changed):
    service = WorkflowService(tmp_path, repository=REPOSITORY)
    c = contract()
    ready = service.execute("work.ready", ready_request(c))
    updated = deepcopy(c)
    updated["scope_revision"] = 2
    if changed == "scope":
        updated["scope"]["paths"].append("tests")
    else:
        updated["source"]["lineage"][0]["content_digest"] = digest("updated issue body")
        updated["source"]["consumed_digest"] = digest(updated["source"]["lineage"])
    drifted = service.snapshot(c["work_id"])
    drifted["contract"] = updated
    assert_code("admission_binding", lambda: service.require_execution(drifted))
    request = ready_request(updated, operation_id="amend", expected_revision=1)
    missing = {key: value for key, value in request.items() if key != "user_request"}
    assert_code("user_request_required", lambda: service.execute("work.amend", missing))
    amended = service.execute("work.amend", request)
    assert amended["admission_id"] != ready["admission_id"]
    state = service.snapshot(c["work_id"])
    assert "intake_admission:" + ready["admission_id"] in state["records"]
    assert service.require_execution(state)["user_request"]["reference"] == user_request()["reference"]
    assert WorkflowService(tmp_path, repository=REPOSITORY).execute("work.amend", request) == amended


def test_selected_batch_records_each_work_under_one_request(tmp_path):
    service = WorkflowService(tmp_path, repository=REPOSITORY)
    request = user_request(summary="Complete the selected P1 backlog issues: one and two")
    admissions = []
    for work_id in ["one", "two"]:
        result = service.execute("work.ready", ready_request(contract(work_id), user_request=request))
        admissions.append(result["admission_id"])
        assert service.require_execution(service.snapshot(work_id))["user_request"] == request
    assert len(set(admissions)) == 2
    assert_code("user_request_required", lambda: service.execute("work.ready", {
        "operation_id": "unselected", "work_id": "three", "expected_revision": 0,
        "record": contract("three"), "labels": ["P1", "ready"], "queue": "P1",
    }))


def test_new_request_readmits_legacy_work_without_erasing_history(tmp_path):
    legacy, _, c, original = setup(tmp_path)
    legacy.execute("work.ready", original)
    prior = deepcopy(legacy.snapshot(c["work_id"])["records"]["intake_admission:auth-1"])
    service = WorkflowService(tmp_path, repository=REPOSITORY)
    assert_code("user_request_required", lambda: service.require_execution(service.snapshot(c["work_id"])))
    amended = deepcopy(c)
    amended["scope_revision"] = 2
    service.execute("work.amend", ready_request(amended, operation_id="amend", expected_revision=1))
    state = service.snapshot(c["work_id"])
    assert state["records"]["intake_admission:auth-1"] == prior
    assert service.require_execution(state)["decision_kind"] == "user_request"
    with service.store.transaction() as db:
        assert db.execute("SELECT count(*) FROM records WHERE record_key LIKE 'intake_admission:%'").fetchone()[0] == 2
