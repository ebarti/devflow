"""Project backlog observation never admits work or rewrites delivery history."""
from __future__ import annotations

import copy
import json
from datetime import UTC, datetime, timedelta

import pytest
from test_delivery_store import service as service

from devflow_temporal.delivery_features import _status
from devflow_temporal.delivery_project_board import desired_status, read
from devflow_temporal.delivery_project_sync import ProjectSynchronizer


@pytest.mark.parametrize(("item", "expected"), [
    ({"status": "Needs validation", "type": "Issue", "state": "OPEN"}, "Backlog"),
    ({"status": "Done", "type": "PullRequest", "state": "CLOSED"}, "Backlog"),
    ({"status": "Done", "type": "PullRequest", "state": "OPEN"}, "Awaiting merge"),
    ({"status": "PR closed", "type": "Issue", "state": "OPEN"}, "Backlog"),
    ({"status": "Merged", "type": "PullRequest", "state": "MERGED"}, "Done"),
    ({"status": "In review", "type": "Issue", "state": "OPEN"}, "Awaiting merge"),
    ({"status": "Awaiting merge", "labels": ["do not merge yet"]}, "Backlog"),
    ({"status": "Backlog", "state": "CLOSED", "state_reason": "NOT_PLANNED"}, "Cancelled"),
])
def test_project_dispositions(item, expected):
    assert desired_status(item) == expected


@pytest.mark.parametrize(("phase", "outcome", "pr_state", "issue", "expected"), [
    ("delivered", "delivered", "CLOSED", {}, "Backlog"),
    ("delivered", "delivered", "OPEN", {}, "Awaiting merge"),
    ("blocked", "blocked", "MERGED", {}, "Done"),
    ("review", None, "OPEN", {}, "Validating"),
    ("tracker", None, "OPEN", {}, "In Progress"),
    ("cancelled", "cancelled", "OPEN", {}, "Backlog"),
    ("cancelled", "cancelled", "OPEN", {"state_reason": "NOT_PLANNED"}, "Cancelled"),
    ("delivered", "delivered", "OPEN", {"labels": ["status: deferred"]}, "Backlog"),
    ("delivered", "delivered", "OPEN", {"state": "CLOSED", "state_reason": "COMPLETED"}, "Backlog"),
])
def test_work_status_is_not_attempt_or_issue_closure(phase, outcome, pr_state, issue, expected):
    receipt = {"url": "https://github.com/example/fixture/pull/7", "head": "a"}
    row = {"phase": phase, "outcome": outcome, 
           "execution_state": "terminal" if outcome else "running",
           "request_json": "{}", "pr_json": json.dumps(receipt)}
    assert _status(row, [{"observation": {"url": receipt["url"], "state": pr_state, "head": "a"}}],
                   receipt, issue) == expected


def test_changed_pr_requires_validation_and_never_becomes_done_from_ci_alone():
    receipt = {"url": "https://github.com/example/fixture/pull/7", "head": "old"}
    row = {"phase": "delivered", "outcome": "delivered", "execution_state": "terminal",
           "request_json": "{}", "pr_json": json.dumps(receipt)}
    observed = {"url": receipt["url"], "state": "OPEN", "head": "new"}
    assert _status(row, [{"observation": observed}]) == "Validating"


class ProjectRemote:
    def __init__(self):
        self.reads = 0
        self.writes = []
        self.failure = False
        self.data = {"id": "P", "url": "https://github.com/users/owner/projects/1",
                     "field": {"id": "F", "options": []}, "items": []}

    def project_board(self, _url):
        self.reads += 1
        if self.failure:
            raise RuntimeError("GitHub unavailable")
        return copy.deepcopy(self.data)

    def project_item_status(self, _project, item, status):
        self.writes.append((item["id"], status))
        next(value for value in self.data["items"] if value["id"] == item["id"])["status"] = status


def project_store(service):
    store, _ = service
    store.config.raw["repositories"]["fixture"].update(
        project_url="https://github.com/users/owner/projects/1", github_repo="example/fixture")
    return store


def test_backlog_survives_restart_and_remote_failure_without_admitting_work(service):
    store = project_store(service)
    remote = ProjectRemote()
    for number, status, kind, state in [(1, "Needs validation", "Issue", "OPEN"),
                                        (2, "Done", "PullRequest", "CLOSED"),
                                        (3, "Merged", "PullRequest", "MERGED")]:
        remote.data["items"].append({"id": str(number), "url": f"https://github.com/example/fixture/issues/{number}",
                                     "repository": "example/fixture", "title": f"Work {number}",
                                     "number": number, "status": status, "type": kind,
                                     "state": state, "labels": []})
    stamp = datetime(2026, 10, 20, tzinfo=UTC)
    sync = ProjectSynchronizer([store], remote, clock=lambda: stamp)
    assert sync.tick() == {}
    first = read(store)
    assert [item["status"] for item in first["items"]] == ["Backlog", "Backlog", "Done"]
    assert first["projects"][0]["state"] == "observed"
    assert remote.reads == 2  # A separate readback confirms the status writes.
    assert sync.next_delay() == 300
    sync.tick()
    assert remote.reads == 2
    stamp += timedelta(seconds=300)
    remote.failure = True
    restarted = ProjectSynchronizer([store], remote, clock=lambda: stamp)
    restarted.tick()
    after = read(store)
    assert after["items"] == first["items"]
    assert after["projects"][0]["state"] == "stale"
    assert after["projects"][0]["error"] == "GitHub unavailable"
    assert restarted.next_delay() == 15
    with store._connect() as db:
        assert db.execute("SELECT count(*) FROM delivery_runs").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM delivery_attempts").fetchone()[0] == 0


def test_project_items_outside_configured_repository_are_never_changed_or_exposed(service):
    store = project_store(service)
    remote = ProjectRemote()
    remote.data["items"] = [{"id": "foreign", "repository": "another/repo",
                              "url": "https://github.com/another/repo/issues/1",
                              "status": "PR closed", "labels": []}]
    ProjectSynchronizer([store], remote).tick()
    assert read(store)["items"] == []
    assert remote.writes == []


@pytest.mark.asyncio
async def test_dashboard_reads_cached_project_without_remote_calls_or_execution(service):
    import httpx

    from devflow_temporal.delivery_api import create_app

    store = project_store(service)
    store.config.path.write_text(json.dumps(store.config.raw))
    remote = ProjectRemote()
    ProjectSynchronizer([store], remote).tick()
    app = create_app(store.config.path)
    before = remote.reads
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url=store.config.dashboard_url) as client:
        result = await client.get('/api/project-board')
        assert result.status_code == 200
        assert result.json()['projects'][0]['state'] == 'observed'
        denied = await client.get('/api/project-board', headers={'host': 'evil.invalid'})
        assert denied.status_code == 403
    assert remote.reads == before
    with store._connect() as db:
        assert db.execute('SELECT count(*) FROM delivery_runs').fetchone()[0] == 0


def test_project_reader_consumes_every_page_and_rejects_stalled_pagination():
    from devflow_temporal.delivery_project_sync import GitHub

    class Pages(GitHub):
        def __init__(self):
            self.cursors = []
            self.stall = False

        def graphql(self, _host, _query, **variables):
            cursor = variables['cursor']
            self.cursors.append(cursor)
            content = {'__typename': 'Issue', 'url': 'https://github.com/example/fixture/issues/1',
                       'title': 'Work', 'number': 1,
                       'repository': {'nameWithOwner': 'example/fixture'},
                       'state': 'OPEN', 'labels': {'nodes': []}}
            return {'user': {'projectV2': {
                'id': 'P', 'closed': False, 'field': {'id': 'F', 'options': []},
                'items': {'nodes': [{'id': str(cursor), 'content': content,
                                    'status': {'name': 'Backlog'}}],
                          'pageInfo': {'hasNextPage': cursor is None or self.stall,
                                       'endCursor': 'next'}},
            }}}

    remote = Pages()
    assert len(remote.project_board('https://github.com/users/owner/projects/1')['items']) == 2
    assert remote.cursors == [None, 'next']
    remote.stall = True
    with pytest.raises(ValueError, match='pagination did not advance'):
        remote.project_board('https://github.com/users/owner/projects/1')


@pytest.mark.parametrize('changed', [False, True])
def test_project_status_write_preserves_a_concurrent_owner_change(changed):
    from devflow_temporal.delivery_project_sync import GitHub, Superseded

    class Remote(GitHub):
        writes = []

        def graphql(self, _host, query, **variables):
            if 'mutation(' in query:
                self.writes.append(variables)
                return {}
            return {'node': {'project': {'id': 'P'}, 'fieldValueByName': {
                'name': 'In Progress' if changed else 'PR closed'}}}

    remote = Remote()
    remote.writes = []
    project = {'id': 'P', 'url': 'https://github.com/users/owner/projects/1',
               'field': {'id': 'F', 'options': [{'id': 'B', 'name': 'Backlog'}]}}
    item = {'id': 'I', 'status': 'PR closed'}
    if changed:
        with pytest.raises(Superseded, match='item changed'):
            remote.project_item_status(project, item, 'Backlog')
        assert remote.writes == []
    else:
        remote.project_item_status(project, item, 'Backlog')
        assert remote.writes == [{'project': 'P', 'item': 'I', 'field': 'F', 'option': 'B'}]
