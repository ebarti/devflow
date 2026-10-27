"""Project schema is independent of an item's durable mirror operation."""
from __future__ import annotations

import copy
import json
import threading
from datetime import UTC, datetime, timedelta

import pytest
from test_delivery_project_sync import published
from test_delivery_store import service as delivery_service

from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_project_sync import DAY, GitHub, ProjectSynchronizer
from devflow_temporal.delivery_store import DeliveryStore

service = delivery_service
PROJECT = "https://github.com/users/example/projects/1"
ISSUE = "https://github.com/example/fixture/issues/3"


class ProjectRemote(GitHub):
    """Execute the real mirror against explicit GraphQL-shaped remote state."""

    def __init__(self, *, desired="Done", configured=True):
        self.desired = desired
        self.options = [
            {"id": "backlog", "name": "Backlog", "color": "BLUE", "description": "keep"},
            {"id": "foreign", "name": "Owner review", "color": "RED",
             "description": "another actor's option"},
        ]
        if configured:
            self.configure_status()
        self.other_status = "foreign"
        self.target_status = "backlog"
        self.mutations = []
        self.after_field_read = None
        self.mirror_reads = 0
        self.lose_item_ack = False

    def configure_status(self):
        self.options.append({"id": "desired", "name": self.desired,
                             "color": "GREEN", "description": "configured by owner"})

    def command(self, *args, **_kwargs):
        if args[:2] == ("pr", "view"):
            return {"url": args[2], "state": "OPEN", "headRefOid": "a" * 40,
                    "baseRefOid": "b" * 40, "mergedAt": None}
        assert args[:2] == ("issue", "view")
        return {"id": "issue", "url": ISSUE, "state": "OPEN", "stateReason": None,
                "closedAt": None, "labels": [], "assignees": [{"login": "owner"}]}

    def graphql(self, host, query, **variables):
        assert host == "github.com"
        if "projectV2(number:" in query:
            snapshot = {"id": "project", "closed": False,
                        "field": {"id": "field", "options": copy.deepcopy(self.options)}}
            if "items(first:" in query:
                snapshot["items"] = {"nodes": [], "pageInfo": {
                    "hasNextPage": False, "endCursor": None}}
            else:
                self.mirror_reads += 1
                if self.after_field_read:
                    action, self.after_field_read = self.after_field_read, None
                    action()
            return {"user": {"projectV2": snapshot}}
        if "updateProjectV2Field(" in query:
            pytest.fail("an item mirror cannot replace Project field options")
        if "projectItems(first:" in query:
            return {"node": {"projectItems": {
                "nodes": [{"id": "item", "project": {"id": "project"}}],
                "pageInfo": {"hasNextPage": False, "endCursor": None}}}}
        if "updateProjectV2ItemFieldValue(" in query:
            option = next((item for item in self.options
                           if item["id"] == variables["option"]), None)
            if not option:
                raise RuntimeError("selected Status option no longer exists")
            self.mutations.append(copy.deepcopy(variables))
            self.target_status = option["id"]
            if self.lose_item_ack:
                self.lose_item_ack = False
                raise RuntimeError("lost response after item-status mutation")
            return {"updateProjectV2ItemFieldValue": {"projectV2Item": {"id": "item"}}}
        if "fieldValueByName" in query:
            option = next((item for item in self.options
                           if item["id"] == self.target_status), None)
            return {"node": {"project": {"id": "project"}, "fieldValueByName": (
                {"optionId": option["id"], "name": option["name"]} if option else None)}}
        pytest.fail(query)


def feature(status="Done"):
    return {"issue": ISSUE, "status": status,
            "binding": {"project": PROJECT, "assignee": "owner"}}


@pytest.mark.parametrize("configured", [False, True])
@pytest.mark.parametrize("edit", ["add", "rename", "recolor", "delete"])
def test_item_mirror_preserves_independent_schema_edits(configured, edit):
    remote = ProjectRemote(configured=configured)
    changed = {}

    def external_edit():
        option = next(item for item in remote.options if item["id"] == "foreign")
        if edit == "add":
            remote.options.append({"id": "foreign-new", "name": "Deferred by owner",
                                   "color": "YELLOW", "description": "concurrent addition"})
            remote.other_status = "foreign-new"
        elif edit == "rename":
            option["name"] = "Needs owner review"
        elif edit == "recolor":
            option.update(color="PURPLE", description="concurrent owner update")
        else:
            remote.options.remove(option)
            remote.other_status = None
        changed.update(options=copy.deepcopy(remote.options), status=remote.other_status)

    def interleave():
        actor = threading.Thread(target=external_edit, name="external Project schema owner")
        actor.start()
        actor.join(2)
        assert not actor.is_alive()

    remote.after_field_read = interleave
    if configured:
        receipt = remote.mirror(feature(), lambda: None)
        assert receipt["option_id"] == "desired" and receipt["status"] == "Done"
        assert remote.mutations == [{"project": "project", "item": "item",
                                     "field": "field", "option": "desired"}]
    else:
        with pytest.raises(ValueError, match="lacks configured Status option 'Done'") as error:
            remote.mirror(feature(), lambda: None)
        assert PROJECT in str(error.value) and "configure that option in GitHub" in str(error.value)
        assert remote.mutations == []
    assert remote.options == changed["options"]
    assert remote.other_status == changed["status"]


def configured_publication(service):
    original, request = service
    raw = copy.deepcopy(original.config.raw)
    raw["repositories"]["fixture"].update(project_url=PROJECT, assignee="owner")
    original.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(original.config.path))
    return published((store, request))[:2]


def test_lost_item_acknowledgement_reconciles_without_schema_or_duplicate_effect(service):
    store, request = configured_publication(service)
    remote = ProjectRemote(desired="Awaiting merge")
    options = copy.deepcopy(remote.options)
    remote.lose_item_ack = True
    stamp = datetime(2026, 10, 26, tzinfo=UTC)
    sync = ProjectSynchronizer([store], remote, clock=lambda: stamp,
                               interval=DAY, project_interval=DAY)
    first = sync.tick()[request["issue_url"]]
    assert first["mirror"]["state"] == "pending"
    assert "lost response" in first["mirror"]["last_error"]
    assert first["project_receipt"] is None and len(remote.mutations) == 1

    restarted = ProjectSynchronizer([DeliveryStore(store.config)], remote, clock=lambda: stamp,
                                    interval=DAY, project_interval=DAY)
    restarted.tick()
    stamp += timedelta(seconds=15)
    assert restarted.tick()[request["issue_url"]]["mirror"]["state"] == "consistent"
    assert len(remote.mutations) == 1  # Exact readback reconciles the first write.
    assert remote.options == options and remote.other_status == "foreign"
