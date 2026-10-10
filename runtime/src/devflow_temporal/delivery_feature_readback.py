"""Settle original remote intents before releasing feature execution ownership."""

from __future__ import annotations

import json
from urllib.parse import quote

from .contracts import digest
from .delivery_execution_registry import OwnershipConflict, UnresolvedEffect
from .delivery_feature_execution import registry
from .delivery_github_contract import GitHubDelivery, decode_manifest


def settle_one(store, spec, entry, gh):
    shared = registry(spec)
    token = spec["feature_delivery"]["owner"]
    issue = spec["feature_delivery"]["snapshot"]["issue"]
    kind, key, request = entry["kind"], entry["effect_key"], json.loads(entry["request_json"])
    if kind == "publish_chunk":
        from .delivery_broker import DeliveryBroker
        from .delivery_feature_publication import publish

        child = store.effective_spec(request["run_id"])
        if child["feature_delivery"]["owner"]["issue_id"] != token["issue_id"]:
            raise OwnershipConflict("publication readback belongs to another feature")
        result = publish(store, DeliveryBroker(store, child), {"spec": child, **request},
                         reconcile=True, settlement=True)
        if result.get("state") == "pending":
            raise UnresolvedEffect("original publication readback is pending")
        return
    if kind in {"github_child_plan", "github_plan_revision"}:
        return gh.settle_plan_effect(issue, entry, shared, token)
    if kind in {"feature_merge", "close_merged_issue"}:
        from .delivery_feature_merge import merge

        if kind == "close_merged_issue":
            parent_key = key.split(":close:", 1)[0]
            original = shared.effect(issue["id"], parent_key)
            if original is None or original["kind"] != "feature_merge":
                raise OwnershipConflict("issue closure lost its original merge authority")
            request = original["request"]
        authority = request["authority"]
        result = merge(store, spec, authority["publication"], authority.get("command"),
                       gh=gh, settlement=True)
        if result["state"] != "confirmed":
            raise UnresolvedEffect("original merge readback is pending")
        return
    with shared.mutation(token):
        prefix = f"repos/{issue['repository']}"
        if kind == "github_plan":
            matches = [item for item in gh.pages(prefix + f"/issues/{issue['number']}/comments")
                       if item.get("body") == request["body"]]
            if len(matches) != 1:
                raise UnresolvedEffect("original plan creation still needs exact readback")
            result = {"id": matches[0]["id"], "node_id": matches[0]["node_id"]}
        elif kind == "github_plan_label":
            label = gh.api(prefix + "/labels/" + quote(request["name"], safe=""))
            if label["name"] != request["name"]:
                raise OwnershipConflict("original plan label identity changed")
            result = {"name": request["name"]}
        elif kind == "github_plan_bind":
            current = gh.issue(issue["repository"], issue["number"], issue["repository_id"])
            if current["id"] != issue["id"] or request["name"] not in current["labels"]:
                raise UnresolvedEffect("original plan binding is not confirmed")
            result = request
        elif kind == "github_workstream":
            matches = [item for item in gh.pages(prefix + "/issues?state=all")
                       if not item.get("pull_request") and item.get("body") == request["body"]
                       and item.get("title") == request["title"]]
            if len(matches) != 1:
                raise UnresolvedEffect("original workstream creation still needs exact readback")
            result = {"number": matches[0]["number"], "id": matches[0]["node_id"]}
        elif kind == "github_subissue":
            parent = gh.api(prefix + f"/issues/{request['child_number']}/parent")
            child = gh.api(prefix + f"/issues/{request['child_number']}")
            if parent["node_id"] != request["parent"] or child["node_id"] != request["child"]:
                raise UnresolvedEffect("original hierarchy linkage is not confirmed")
            result = {"child": request["child"], "parent": request["parent"]}
        elif kind == "github_record":
            raw = gh.api(prefix + f"/issues/comments/{request['comment_id']}")
            if raw["id"] != request["comment_id"]:
                raise OwnershipConflict("original delivery comment identity changed")
            manifest = decode_manifest(raw["body"], issue)
            if digest(manifest) != request["after"]:
                raise UnresolvedEffect("original delivery record update is not confirmed")
            result = {"digest": request["after"]}
        elif kind == "github_stack":
            if request["stack_id"] is not None:
                stack = gh.api(prefix + f"/stacks/{request['stack_id']}")
            else:
                matches = gh.api(prefix + f"/stacks?pull_request={request['members'][0]}")
                if len(matches) != 1:
                    raise UnresolvedEffect("original stack creation needs exact PR readback")
                stack = matches[0]
            if [item["number"] for item in stack["pull_requests"]] != request["members"]:
                raise UnresolvedEffect("original stack membership update is not confirmed")
            result = {"number": stack["number"]}
        else:
            raise OwnershipConflict("unknown pending feature operation: " + kind)
        shared.finish_effect(token, key, result)


def settle(store, spec, *, gh=None):
    shared = registry(spec)
    token = spec["feature_delivery"]["owner"]
    with shared.connect() as db:
        shared.require_settlement(db, token)
        pending = [dict(row) for row in db.execute(
            "SELECT * FROM execution_effects WHERE issue_id=? AND state='pending' ORDER BY rowid",
            (token["issue_id"],),
        )]
    for entry in pending:
        if shared.effect(token["issue_id"], entry["effect_key"])["state"] != "pending":
            continue
        try:
            settle_one(store, spec, entry, gh or GitHubDelivery())
        except Exception as exc:
            # The durable coordinator waits here with its original generation.
            # No new product attempt or blind remote write is authorized.
            return {"state": "pending", "effect_key": entry["effect_key"],
                    "reason": f"{type(exc).__name__}: {str(exc)[:300]}"}
    return {"state": "confirmed"}
