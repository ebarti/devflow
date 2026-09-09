"""Synthetic Projects server exercises real adapter transport and pagination."""

import copy
import json
import subprocess

import pytest

from devflow.adapters.github import GitHubRepository
from devflow.errors import WorkflowError


class ProjectServer:
    def __init__(self, *, existing=False, later_page=False):
        self.calls, self.writes = [], []
        self.lost = None
        self.field_effect = True
        self.later_page = later_page
        self.items = [self.item()] if existing else []
        self.fields = [
            {
                "id": "status-field",
                "dataType": "SINGLE_SELECT",
                "name": "Status",
                "options": [{"id": "done-option", "name": "Done"}],
            },
            {"id": "notes-field", "dataType": "TEXT", "name": "Private note"},
            {"id": "risk-field", "dataType": "TEXT", "name": "Risk"},
        ]

    @staticmethod
    def item():
        return {
            "id": "item-1",
            "isArchived": False,
            "content": {"id": "issue-1", "__typename": "Issue"},
            "fieldValues": [
                {
                    "__typename": "ProjectV2ItemFieldTextValue",
                    "field": {"id": "notes-field"},
                    "text": "User sentinel must survive",
                }
            ],
        }

    @staticmethod
    def page(nodes, after=None):
        return {
            "nodes": copy.deepcopy(nodes),
            "pageInfo": {"hasNextPage": after is not None, "endCursor": after},
        }

    def runner(self, argv, **kwargs):
        request = json.loads(kwargs["input"])
        self.calls.append(request)
        data = self.route(request["query"], request["variables"])
        return subprocess.CompletedProcess(argv, 0, json.dumps({"data": data}), "")

    def route(self, query, variables):
        if "addProjectV2ItemById" in query:
            self.writes.append(("add", variables))
            assert variables["project"] == "project-1" and variables["content"] == "issue-1"
            self.items.append(self.item())
            self.maybe_lost("add")
            return {"addProjectV2ItemById": {"item": {"id": "item-1"}}}
        if "updateProjectV2ItemFieldValue" in query:
            self.writes.append(("update", variables))
            assert variables["field"] in {"status-field", "risk-field"}
            if self.field_effect:
                field_id, typed = variables["field"], variables["value"]
                values = self.items[0]["fieldValues"]
                values[:] = [value for value in values if value["field"]["id"] != field_id]
                kind = (
                    "ProjectV2ItemFieldSingleSelectValue"
                    if "singleSelectOptionId" in typed
                    else "ProjectV2ItemFieldTextValue"
                )
                data = (
                    {"optionId": typed["singleSelectOptionId"]}
                    if "singleSelectOptionId" in typed
                    else typed
                )
                values.append({"__typename": kind, "field": {"id": field_id}, **data})
            self.maybe_lost("update")
            return {"updateProjectV2ItemFieldValue": {"projectV2Item": {"id": "item-1"}}}
        if "nameWithOwner" in query:
            return {
                "node": {
                    "id": "issue-1",
                    "__typename": "Issue",
                    "repository": {"nameWithOwner": "fixture/repo"},
                }
            }
        if "fields(first" in query:
            return {"node": {"fields": self.page(self.fields)}}
        if "items(first" in query:
            if self.later_page and variables["after"] is None:
                return {"node": {"id": "project-1", "items": self.page([], "items-later")}}
            nodes = [
                item | {"fieldValues": self.page(item["fieldValues"][:1], "fields-later")}
                for item in self.items
            ]
            return {"node": {"id": "project-1", "items": self.page(nodes)}}
        if "fieldValues(first" in query:
            assert variables["after"] == "fields-later"
            return {"node": {"fieldValues": self.page(self.items[0]["fieldValues"][1:])}}
        raise AssertionError((query, variables))

    def maybe_lost(self, operation):
        if self.lost == operation:
            self.lost = None
            raise subprocess.TimeoutExpired(["gh", "api"], 60)


def synchronize(repo, **overrides):
    args = dict(
        project_id="project-1",
        content_id="issue-1",
        action_id="action-1",
        field_updates={"status-field": {"singleSelectOptionId": "done-option"}},
        expected_owned_fields={"status-field": "SINGLE_SELECT"},
    )
    return repo.sync_project_item(**(args | overrides))


@pytest.mark.parametrize("lost", [None, "add", "update"])
def test_project_writes_reconcile_lost_responses_preserve_unowned_fields(lost):
    server = ProjectServer()
    server.lost = lost
    repo = GitHubRepository("fixture", "repo", server.runner)
    result = synchronize(repo)
    assert result["status"] == "synchronized" and result["item_id"] == "item-1"
    assert [kind for kind, _ in server.writes] == ["add", "update"]
    assert server.items[0]["fieldValues"][0]["text"] == "User sentinel must survive"
    assert synchronize(repo) == result
    assert len(server.writes) == 2


def test_later_page_existing_item_is_not_duplicated_and_inner_values_are_read():
    server = ProjectServer(existing=True, later_page=True)
    repo = GitHubRepository("fixture", "repo", server.runner)
    synchronize(repo)
    assert [kind for kind, _ in server.writes] == ["update"]
    assert any(call["variables"].get("after") == "items-later" for call in server.calls)
    assert any(call["variables"].get("after") == "fields-later" for call in server.calls)


def test_project_only_manifest_owned_ids_and_types_can_change():
    server = ProjectServer(existing=True)
    repo = GitHubRepository("fixture", "repo", server.runner)
    with pytest.raises(WorkflowError) as error:
        synchronize(repo, field_updates={"notes-field": {"text": "overwrite"}})
    assert error.value.code == "project_field_not_owned"
    with pytest.raises(WorkflowError) as error:
        synchronize(repo, expected_owned_fields={"status-field": "TEXT"})
    assert error.value.code == "project_binding_changed"
    with pytest.raises(WorkflowError) as error:
        synchronize(repo, field_updates={"status-field": {"singleSelectOptionId": "guessed-name"}})
    assert error.value.code == "project_binding_changed"
    assert not server.writes
    assert server.items[0]["fieldValues"][0]["text"] == "User sentinel must survive"


def test_missing_project_scope_is_a_clean_blocker_not_retry():
    calls = []

    def runner(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "errors": [
                        {
                            "type": "INSUFFICIENT_SCOPES",
                            "message": "Token has not been granted required scopes: secret-sentinel",
                        }
                    ]
                }
            ),
            "",
        )

    with pytest.raises(WorkflowError) as error:
        synchronize(GitHubRepository("fixture", "repo", runner))
    assert error.value.code == "project_permission_missing" and len(calls) == 1
    assert "secret-sentinel" not in str(error.value)


def test_failed_field_readback_does_not_report_projection_success():
    server = ProjectServer(existing=True)
    server.field_effect = False
    with pytest.raises(WorkflowError) as error:
        synchronize(GitHubRepository("fixture", "repo", server.runner))
    assert error.value.code == "ambiguous_github_action"
    assert len(server.writes) == 1


def test_reconcile_partial_project_never_writes():
    server = ProjectServer(existing=True)
    repo = GitHubRepository("fixture", "repo", server.runner)
    assert (
        repo.reconcile_project_item(
            project_id="project-1",
            content_id="issue-1",
            action_id="action-1",
            field_updates={"status-field": {"singleSelectOptionId": "done-option"}},
        )
        is None
    )
    assert not server.writes
