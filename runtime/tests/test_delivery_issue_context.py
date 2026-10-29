from __future__ import annotations

import base64
import copy
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_store import service as service

from devflow_temporal import delivery_issue_context as context
from devflow_temporal.delivery_github_contract import GitHubContractError, GitHubDelivery
from devflow_temporal.supervisor import DeliverySupervisor


class GitHubFixture(GitHubDelivery):
    def __init__(self):
        self.calls = []
        self.body = ("## Acceptance\nKeep dated official evidence and unknown eligibility.\n"
                     "Prior design: https://github.com/example/fixture/pull/7\n"
                     "Source: https://github.com/example/fixture/blob/" + 'a' * 40
                     + "/docs/design.md#L2-L3\n")
        self.failed = False

    def api(self, endpoint, *, method="GET", body=None):
        assert method == "GET" and body is None
        self.calls.append(endpoint)
        if endpoint == "repos/example/fixture":
            return {"full_name": "example/fixture", "node_id": "repo", "private": False}
        if endpoint == "repos/other/private":
            return {"private": True}
        if endpoint.endswith('/comments?per_page=100'):
            return [{"html_url": "https://github.com/example/fixture/issues/3#issuecomment-1",
                     "body": "Existing design discussion", "updated_at": "then"}]
        if endpoint.endswith('/timeline?per_page=100'):
            return [{"source": {"issue": {"html_url":
                     "https://github.com/example/fixture/pull/7"}}}]
        if '/contents/' in endpoint:
            return {"encoding": "base64", "sha": "blob", "size": 25,
                    "content": base64.b64encode(b'one\ntwo\nthree\nfour\n').decode()}
        if endpoint.endswith('/pulls/7/files?per_page=100'):
            return [{"filename": "docs/prior.md", "patch": "+Retain official provenance"}]
        if endpoint.endswith('/pulls/7'):
            return {"title": "Earlier design", "body": "Prior design acceptance",
                    "state": "closed", "merged_at": None, "head": {"sha": 'b' * 40}}
        if endpoint.endswith('/issues/7'):
            return {"pull_request": {}}
        if '/issues/' in endpoint:
            if self.failed:
                raise GitHubContractError("primary issue unavailable", 403)
            number = int(endpoint.rsplit('/', 1)[-1])
            return {"node_id": "issue", "id": number, "number": number,
                    "html_url": f"https://github.com/example/fixture/issues/{number}",
                    "title": "Official evidence", "body": self.body,
                    "state": "open", "updated_at": "now", "labels": []}
        raise AssertionError(endpoint)


def submitted(service, monkeypatch):
    store, request = service
    store.submit(request)
    github = GitHubFixture()
    monkeypatch.setattr(context, "GitHubDelivery", lambda: github)
    return store, request, github


def test_new_intake_round_acquires_new_context_and_rejects_cross_run_commands(service, monkeypatch):
    store, raw, github = submitted(service, monkeypatch)
    spec = {**store.spec(raw["run_id"]), "provider": "codex"}
    request = {"spec": spec, "role": "intake", "iteration": 0,
               "candidate": {"id": "candidate"}, "workspace": spec["checkout"]}
    first = context.prepare_intake(store, request)
    github.body += "\nNew clarification evidence."
    second = context.prepare_intake(store, {**request, "iteration": 1})
    assert second["issue_context"] != first["issue_context"]
    store.submit({**raw, "run_id": "run-2", "command_id": "submit-2", "work_id": "work-2",
                  "branch": "feat/two", "issue_url": "https://github.com/example/fixture/issues/4"})
    with pytest.raises(ValueError, match="another run"):
        context.refresh(store, "run-2", {"command_id": "intake-context:"
                        + DeliverySupervisor._job_key(request)})


@pytest.mark.asyncio
async def test_real_intake_activity_supplies_context_before_supervisor_launch(service, monkeypatch):
    from devflow_temporal import delivery_activities as activities

    store, raw, github = submitted(service, monkeypatch)
    spec = {**store.spec(raw["run_id"]), "provider": "codex"}
    candidate = {"id": "candidate", "head": "head"}
    seen = []

    async def run(request):
        seen.append(copy.deepcopy(request))
        return {"status": "questions", "questions": [], "cleanup": "confirmed"}

    monkeypatch.setattr(activities, "_context", lambda _: (store, SimpleNamespace(
        checkout=Path(spec["checkout"]), candidate=lambda: candidate)))
    monkeypatch.setattr(activities, "get_supervisor", lambda _: SimpleNamespace(run=run))
    await activities._run_intake({"spec": spec, "candidate": candidate, "iteration": 0})
    assert seen[0]["issue_context"]["issue"]["body"] == github.body
