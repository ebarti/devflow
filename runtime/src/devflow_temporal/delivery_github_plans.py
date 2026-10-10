"""Child-owned immutable plans and journaled parent-index commit points.

The parent comment ID is retained across supported v1 migration and revisions.
A child comment is never PATCHed. Its exact body, issue, comment node ID, and
parent hierarchy are authenticated before the compact parent pointer commits.
Unknown POST outcomes are recovered only by their original exact operation body.
"""

from __future__ import annotations

import json
from copy import deepcopy

from .contracts import canonical_json, digest
from .delivery_execution_registry import OwnershipConflict, UnresolvedEffect
from .delivery_plan_model import (
    _text,
    ordered_chunks,
    plan_version,
    resolve_plan_issues,
    validate_plan,
)

CHILD_MARKER = "<!-- devflow-workstream-plan:v2 -->"
CHILD_FIELDS = {"version", "parent_issue_id", "repository_id", "issue_id", "workstream_id",
                "plan_revision", "plan_digest", "creation_key", "workstream"}


def _comment_issue_url(issue):
    return f"https://api.github.com/repos/{issue['repository']}/issues/{issue['number']}"


def authenticate_comment(raw, issue, reference):
    if (not isinstance(raw, dict) or raw.get("id") != reference["comment_id"]
            or raw.get("node_id") != reference["comment_node_id"]
            or not isinstance(raw.get("issue_url"), str)
            or raw["issue_url"].casefold() != _comment_issue_url(issue).casefold()):
        raise OwnershipConflict("recorded delivery comment identity changed")



def verify_parent_binding(gh, issue, reference):
    from .delivery_github_contract import LABEL_PREFIX

    parent = gh.issue(issue["repository"], issue["number"], issue["repository_id"])
    labels = [name for name in parent["labels"] if name.startswith(LABEL_PREFIX)]
    if parent["id"] != issue["id"] or labels != [LABEL_PREFIX + str(reference["comment_id"])]:
        raise OwnershipConflict("parent plan binding changed before revision publication")
    return parent

def child_plan_links(record):
    """Exact published links; legacy records have no detailed child-plan comments."""
    return deepcopy(record["manifest"].get("workstream_plans", {}))


def encode_child_plan(payload, issue, parent):
    stream = payload["workstream"]
    lines = ["## Workstream plan: " + stream["title"], "",
             f"Feature: {parent['url']}", f"Workstream: {issue['url']}",
             f"Plan revision: {payload['plan_revision']}", "", "Acceptance criteria:"]
    lines.extend("- " + text for text in stream["acceptance"])
    for chunk in stream["chunks"]:
        lines.extend(["", f"### {chunk['title']} ({chunk['id']})", "", chunk["scope"], "",
                      "Prerequisites: " + (", ".join(chunk["depends_on"]) or "none"), "",
                      "Implementation steps:"])
        lines.extend(f"{number}. {step}" for number, step in enumerate(chunk["steps"], 1))
        lines.extend(["", "Verification:"])
        lines.extend("- " + text for text in chunk["verification"])
        lines.extend(["", "Chunk acceptance:"])
        lines.extend("- " + text for text in chunk["acceptance"])
        lines.extend(["", "Expected files:"])
        lines.extend("- `" + path + "`" for path in chunk["expected_paths"])
        lines.extend(["", "Admitted gate selections:"])
        for gate in chunk["gates"]:
            selection = ", ".join(gate["selectors"]) or "whole admitted recipe"
            lines.append(f"- {gate['stage']}/{gate['recipe_id']}: {selection}")
            if gate.get("reason"):
                lines.append("  " + gate["reason"])
    lines.extend(["", CHILD_MARKER, "```json", canonical_json(payload), "```", ""])
    body = "\n".join(lines)
    if len(body.encode()) > 60000:
        raise ValueError("child workstream plan exceeds the GitHub comment limit")
    return body


def decode_child_plan(body):
    if (not isinstance(body, str) or body.count(CHILD_MARKER) != 1
            or len(body.encode()) > 60000):
        raise ValueError("child workstream comment lacks its unique plan record")
    encoded = body.split(CHILD_MARKER, 1)[1].strip()
    if not encoded.startswith("```json\n") or not encoded.endswith("\n```"):
        raise ValueError("child workstream plan record is malformed")
    value = json.loads(encoded[8:-4])
    if (not isinstance(value, dict) or set(value) != CHILD_FIELDS
            or type(value["version"]) is not int or value["version"] != 2
            or type(value["plan_revision"]) is not int or value["plan_revision"] < 1):
        raise ValueError("child workstream plan has an unsupported schema")
    return value


def _exact_child(gh, parent, binding):
    child = gh.issue(parent["repository"], binding["number"], parent["repository_id"])
    if any(child.get(field) != binding[field] for field in ("id", "number", "url")):
        raise OwnershipConflict("recorded sub-issue identity changed")
    hierarchy = gh.optional(f"repos/{parent['repository']}/issues/{child['number']}/parent")
    if not hierarchy or hierarchy.get("node_id") != parent["id"]:
        raise OwnershipConflict("workstream belongs to a different parent issue")
    return child


def hydrate_record(gh, issue, record):
    from .delivery_github_contract import validate_manifest, wire_manifest

    index = wire_manifest(validate_manifest(record["manifest"], issue))
    if index["version"] == 1:
        return deepcopy(record)
    plan = deepcopy(index["plan"])
    for stream in plan["workstreams"]:
        binding = index["workstream_issues"][stream["id"]]
        child = _exact_child(gh, issue, binding)
        ref = index["workstream_plans"][stream["id"]]
        raw = gh.api(f"repos/{issue['repository']}/issues/comments/{ref['comment_id']}")
        authenticate_comment(raw, child, ref)
        if digest(raw.get("body")) != ref["digest"]:
            raise OwnershipConflict("immutable child-plan comment digest changed")
        payload = decode_child_plan(raw["body"])
        if (payload["parent_issue_id"] != issue["id"]
                or payload["repository_id"] != issue["repository_id"]
                or payload["issue_id"] != binding["id"]
                or payload["workstream_id"] != stream["id"]
                or payload["plan_revision"] != index["plan_revision"]
                or payload["plan_digest"] != index["plan_digest"]):
            raise OwnershipConflict("child-plan comment belongs to a different feature revision")
        detailed = payload["workstream"]
        if not isinstance(detailed, dict):
            raise ValueError("child plan requires a structured workstream")
        summary = deepcopy(detailed)
        try:
            summary["chunks"] = [{key: chunk[key] for key in ("id", "title", "depends_on")}
                                 for chunk in detailed["chunks"]]
        except (KeyError, TypeError) as exc:
            raise ValueError("child plan lacks its detailed chunk contract") from exc
        if summary != stream:
            raise OwnershipConflict("child workstream plan differs from its parent index")
        stream.update(deepcopy(detailed))
    validate_plan(plan)
    if digest(plan) != index["plan_digest"]:
        raise OwnershipConflict("hydrated child plan differs from the parent plan digest")
    return {**deepcopy(record), "manifest": {**index, "plan": plan}, "wire_index": index}


def load_record(gh, issue, reference):
    from .delivery_github_contract import decode_manifest

    raw = gh.api(f"repos/{issue['repository']}/issues/comments/{reference['comment_id']}")
    authenticate_comment(raw, issue, reference)
    record = {"comment_id": raw["id"], "comment_node_id": raw["node_id"],
              "manifest": decode_manifest(raw["body"], issue)}
    return hydrate_record(gh, issue, record)


def _reference(raw, child, body):
    from .delivery_github_contract import GITHUB_ID_MAX, GITHUB_NODE_MAX

    reference = {"comment_id": raw.get("id"), "comment_node_id": raw.get("node_id"),
                 "digest": digest(body),
                 "url": child["url"] + f"#issuecomment-{raw.get('id')}"}
    if (type(reference["comment_id"]) is not int
            or not 1 <= reference["comment_id"] <= GITHUB_ID_MAX
            or not isinstance(reference["comment_node_id"], str)
            or not reference["comment_node_id"]
            or len(reference["comment_node_id"]) > GITHUB_NODE_MAX):
        raise OwnershipConflict("child-plan creation returned a different comment identity")
    authenticate_comment(raw, child, reference)
    if raw.get("body") != body:
        raise OwnershipConflict("child-plan creation readback differs from its exact operation")
    return reference


def _child_plan_body(parent, child, stream, plan, plan_revision, operation_id):
    key = "github-child-plan:" + digest({"operation_id": operation_id, "stream": stream["id"]})
    payload = {"version": 2, "parent_issue_id": parent["id"],
               "repository_id": parent["repository_id"], "issue_id": child["id"],
               "workstream_id": stream["id"], "plan_revision": plan_revision,
               "plan_digest": digest(plan), "creation_key": key, "workstream": stream}
    body = encode_child_plan(payload, child, parent)
    if decode_child_plan(body) != payload:
        raise ValueError("child plan encoding differs from its exact payload")
    return key, body


def _write_child(gh, parent, child, stream, plan, plan_revision, registry, token, operation_id):
    key, body = _child_plan_body(parent, child, stream, plan, plan_revision, operation_id)
    request = {"operation_id": operation_id, "parent_issue_id": parent["id"],
               "child": {field: child[field] for field in ("id", "number", "url")},
               "plan_revision": plan_revision, "body": body}
    effect = registry.intent(token, key, "github_child_plan", request)
    if effect["state"] == "complete":
        ref = effect["result"]
        raw = gh.api(f"repos/{parent['repository']}/issues/comments/{ref['comment_id']}")
        authenticate_comment(raw, child, ref)
    elif effect["fresh"]:
        raw = gh.api(f"repos/{parent['repository']}/issues/{child['number']}/comments",
                     method="POST", body={"body": body})
        # A create response alone does not prove the exact durable comment.
        ref = _reference(raw, child, body)
        raw = gh.api(f"repos/{parent['repository']}/issues/comments/{ref['comment_id']}")
    else:
        matches = [raw for raw in gh.pages(
            f"repos/{parent['repository']}/issues/{child['number']}/comments")
                   if raw.get("body") == body]
        if len(matches) != 1:
            raise UnresolvedEffect("original child-plan comment creation is unresolved")
        raw = matches[0]
    ref = _reference(raw, child, body)
    registry.finish_effect(token, key, ref)
    return ref


def _ensure_no_competing_effects(registry, token, operation_id):
    with registry.connect() as db:
        pending = list(db.execute("SELECT kind,request_json FROM execution_effects "
                                  "WHERE issue_id=? AND state='pending'", (token["issue_id"],)))
    for stage_key, stage in registry.checkpoints(token["issue_id"]).items():
        if stage_key.startswith("github-plan-stage:") and stage["operation_id"] != operation_id:
            effect = registry.effect(token["issue_id"],
                                     "github-plan-revision:" + digest(stage["operation_id"]))
            if effect is None or effect["state"] != "complete":
                raise OwnershipConflict("another staged plan revision retains publication custody")
    for row in pending:
        request = json.loads(row["request_json"])
        if row["kind"] in {"github_child_plan", "github_plan_revision"}:
            if request["operation_id"] == operation_id:
                continue
        raise UnresolvedEffect("feature has unsettled effects before plan revision publication")


def _preserve_plan(old, new):
    if (old["scope"] != new["scope"] or not set(old["acceptance"]) <= set(new["acceptance"])):
        raise OwnershipConflict("plan revision changed the authorized outcome "
                                "or weakened acceptance")
    final_gates = {(gate["stage"], gate["recipe_id"]): gate for gate in new["final_gates"]}
    for gate in old.get("final_gates", []):
        replacement = final_gates.get((gate["stage"], gate["recipe_id"]))
        if (replacement is None
                or (not gate["selectors"] and replacement["selectors"])
                or not set(gate["selectors"]) <= set(replacement["selectors"])):
            raise OwnershipConflict("plan revision weakened accepted final verification")
    before = {stream["id"]: stream for stream in old["workstreams"]}
    after = {stream["id"]: stream for stream in new["workstreams"]}
    if before.keys() != after.keys():
        raise OwnershipConflict("plan revision changed workstream identities")
    for key, stream in before.items():
        revised = after[key]
        if not set(stream["acceptance"]) <= set(revised["acceptance"]):
            raise OwnershipConflict("plan revision weakened workstream acceptance")
        old_chunks = {chunk["id"]: chunk for chunk in stream["chunks"]}
        new_chunks = {chunk["id"]: chunk for chunk in revised["chunks"]}
        if not old_chunks.keys() <= new_chunks.keys():
            raise OwnershipConflict("plan revision changed stable chunk ownership")
        for chunk_id, chunk in old_chunks.items():
            replacement = new_chunks[chunk_id]
            if (chunk["scope"] != replacement["scope"]
                    or not set(chunk["acceptance"]) <= set(replacement["acceptance"])):
                raise OwnershipConflict("plan revision weakened chunk scope or acceptance")


def _extended_publication(old, plan, bindings):
    publication = deepcopy(old["publication"])
    chunks = ordered_chunks(plan)
    for index, member in enumerate(publication["members"]):
        if index >= len(chunks) or member["chunk_id"] != chunks[index]["id"]:
            raise OwnershipConflict("plan revision reordered an existing publication")
        child = bindings[chunks[index]["workstream_id"]]
        extra = {"workstream_id": chunks[index]["workstream_id"], "issue_id": child["id"],
                 "issue_number": child["number"], "issue_url": child["url"]}
        if any(key in member and member[key] != value for key, value in extra.items()):
            raise OwnershipConflict("plan revision changed an existing PR child binding")
        member.update(extra)
    return publication


def _parent_plan_body(manifest, issue):
    from .delivery_github_contract import (
        decode_manifest,
        encode_manifest,
        validate_manifest,
        wire_manifest,
    )

    validate_manifest(manifest, issue)
    body = encode_manifest(manifest)
    if decode_manifest(body, issue) != wire_manifest(manifest):
        raise ValueError("parent index encoding differs from its exact payload")
    return body


def _preflight_plan_records(issue, manifest, children, operation_id):
    """Admit all child bodies and the bounded parent before initial or revision effects."""
    from .delivery_github_contract import GITHUB_ID_MAX, GITHUB_NODE_MAX

    preview = deepcopy(manifest)
    refs = {}
    for offset, stream in enumerate(preview["plan"]["workstreams"]):
        child = children[stream["id"]]
        _, body = _child_plan_body(issue, child, stream, preview["plan"],
                                   preview["plan_revision"], operation_id)
        comment_id = GITHUB_ID_MAX - offset
        refs[stream["id"]] = {
            "comment_id": comment_id,
            # Control characters maximize canonical JSON escaping per character.
            "comment_node_id": "\x01" * GITHUB_NODE_MAX,
            "digest": digest(body),
            "url": child["url"] + f"#issuecomment-{comment_id}",
        }
    preview["workstream_plans"] = refs
    _parent_plan_body(preview, issue)
    return preview


def _initial_manifest(issue, plan, bindings, key):
    return {"version": 2, "issue_id": issue["id"], "repository_id": issue["repository_id"],
            "revision": 1, "plan_revision": 1, "creation_key": key,
            "plan_digest": digest(plan), "plan": plan, "workstream_issues": bindings,
            "workstream_plans": {}, "publication": {"stack_id": None, "members": []}}


def _preflight_initial_plan(issue, supplied_plan, key):
    """Reserve enough encoded space for nullable child identities before creating them."""
    from .delivery_github_contract import GITHUB_ID_MAX, GITHUB_NODE_MAX

    used = {issue["number"]} | {stream["issue_number"] for stream in supplied_plan["workstreams"]
                               if stream["issue_number"] is not None}
    bindings = {}
    for offset, stream in enumerate(supplied_plan["workstreams"]):
        number = stream["issue_number"]
        if number is None:
            number = GITHUB_ID_MAX - offset
            while number in used:
                number -= 1
            used.add(number)
        bindings[stream["id"]] = {
            "id": "\x01" * (GITHUB_NODE_MAX - 1) + chr(16 + offset),
            "number": number,
            "url": f"https://github.com/{issue['repository']}/issues/{number}",
        }
    plan = resolve_plan_issues(supplied_plan, bindings)
    _preflight_plan_records(issue, _initial_manifest(issue, plan, bindings, key), bindings, key)


def _stage_key(operation_id):
    _text(operation_id, "plan operation ID", 128)
    return "github-plan-stage:" + digest(operation_id)


def _stage_locked(gh, issue, record, supplied_plan, registry, token, operation_id):
    from .delivery_github_contract import validate_manifest, wire_manifest

    validate_plan(supplied_plan)
    if plan_version(supplied_plan) != 2:
        raise ValueError("explicit plan revision must adopt a v2 plan")
    stage_key = _stage_key(operation_id)
    saved = registry.checkpoints(issue["id"]).get(stage_key)
    baseline = saved["before_record"] if saved else record
    old = baseline["manifest"]
    if saved:
        supplied_wire = wire_manifest(record["manifest"])
        parent_effect = registry.effect(issue["id"],
                                        "github-plan-revision:" + digest(operation_id))
        known_after = parent_effect["request"]["after"] if parent_effect else None
        if (record["comment_id"] != baseline["comment_id"]
                or record["comment_node_id"] != baseline["comment_node_id"]
                or (supplied_wire != wire_manifest(old) and digest(supplied_wire) != known_after)):
            raise OwnershipConflict("expected plan revision record changed for the operation")
    bindings = old["workstream_issues"]
    plan = resolve_plan_issues(supplied_plan, bindings)
    _preserve_plan(old["plan"], plan)
    publication = _extended_publication(old, plan, bindings)
    target_revision = old.get("plan_revision", 1) + 1
    proposal = {"operation_id": operation_id, "before_record": baseline,
                "target_plan_revision": target_revision, "plan_digest": digest(plan)}
    if saved and saved != proposal:
        raise OwnershipConflict("plan revision operation identity changed")
    _ensure_no_competing_effects(registry, token, operation_id)
    verify_parent_binding(gh, issue, baseline)
    current = load_record(gh, issue, baseline)
    before = wire_manifest(old)
    parent_effect = registry.effect(issue["id"], "github-plan-revision:" + digest(operation_id))
    if wire_manifest(current["manifest"]) != before:
        if (saved and parent_effect and parent_effect["request"]["after"]
                == digest(wire_manifest(current["manifest"]))
                and current["manifest"].get("plan_revision") == target_revision
                and current["manifest"].get("plan_digest") == digest(plan)):
            return current
        raise OwnershipConflict("GitHub delivery record changed before plan revision")
    children = {stream["id"]: _exact_child(gh, issue, bindings[stream["id"]])
                for stream in plan["workstreams"]}
    manifest = {"version": 2, "issue_id": issue["id"], "repository_id": issue["repository_id"],
                "revision": old["revision"] + 1, "plan_revision": target_revision,
                "creation_key": old["creation_key"], "plan_digest": digest(plan), "plan": plan,
                "workstream_issues": deepcopy(bindings), "workstream_plans": {},
                "publication": publication}
    manifest = _preflight_plan_records(issue, manifest, children, operation_id)
    registry.checkpoint(token, stage_key, proposal)
    refs = {}
    for stream in plan["workstreams"]:
        child = children[stream["id"]]
        refs[stream["id"]] = _write_child(gh, issue, child, stream, plan, target_revision,
                                         registry, token, operation_id)
    manifest["workstream_plans"] = refs
    validate_manifest(manifest, issue)
    staged = {"comment_id": baseline["comment_id"],
              "comment_node_id": baseline["comment_node_id"], "manifest": manifest}
    # Hydration authenticates every child again before the parent commit point.
    return hydrate_record(gh, issue, staged)


def stage_plan_revision(gh, issue, record, plan, registry, token, *, operation_id):
    with registry.mutation(token):
        return _stage_locked(gh, issue, record, plan, registry, token, operation_id)


def publish_plan_revision(gh, issue, record, plan, registry, token, *, operation_id):
    from .delivery_github_contract import encode_manifest, wire_manifest

    with registry.mutation(token):
        staged = _stage_locked(gh, issue, record, plan, registry, token, operation_id)
        saved = registry.checkpoints(issue["id"])[_stage_key(operation_id)]
        baseline = saved["before_record"]
        target = wire_manifest(staged["manifest"])
        body = encode_manifest(target)
        key = "github-plan-revision:" + digest(operation_id)
        request = {"operation_id": operation_id, "comment_id": baseline["comment_id"],
                   "comment_node_id": baseline["comment_node_id"],
                   "before": digest(wire_manifest(baseline["manifest"])), "after": digest(target),
                   "expected_revision": baseline["manifest"]["revision"],
                   "plan_revision": target["plan_revision"], "body": body}
        registry.intent(token, key, "github_plan_revision", request)
        verify_parent_binding(gh, issue, baseline)
        current = load_record(gh, issue, baseline)
        observed = digest(wire_manifest(current["manifest"]))
        if observed != request["after"]:
            if observed != request["before"]:
                raise OwnershipConflict("parent index changed after child-plan staging")
            # PATCH is idempotent; a retry authenticates the same original before
            # state and target. A pending POST is never repeated blindly.
            gh.api(f"repos/{issue['repository']}/issues/comments/{baseline['comment_id']}",
                   method="PATCH", body={"body": body})
            current = load_record(gh, issue, baseline)
        if digest(wire_manifest(current["manifest"])) != request["after"]:
            raise OwnershipConflict("published plan revision differs from exact readback")
        registry.finish_effect(token, key, {"digest": request["after"],
                                           "plan_revision": target["plan_revision"]})
        return current


def initialize_v2(gh, issue, supplied_plan, registry, token):
    from .delivery_github_contract import LABEL_PREFIX, wire_manifest

    validate_plan(supplied_plan)
    key = "github-plan:" + digest({"issue": issue["id"], "plan": supplied_plan})
    with registry.mutation(token):
        parent = gh.issue(issue["repository"], issue["number"], issue["repository_id"])
        existing = gh.bound_record(parent)
        if existing:
            resolved = resolve_plan_issues(supplied_plan, existing["manifest"]["workstream_issues"])
            if existing["manifest"]["version"] != 2 or existing["manifest"]["plan"] != resolved:
                raise OwnershipConflict("feature already has a different accepted delivery plan")
            gh.reconcile_record(issue, existing, registry, token)
            pending_key = "github-plan-bind:" + LABEL_PREFIX + str(existing["comment_id"])
            pending = registry.effect(issue["id"], pending_key)
            if pending and pending["state"] == "pending":
                registry.finish_effect(token, pending_key,
                                       {"issue": issue["id"], "name": LABEL_PREFIX
                                        + str(existing["comment_id"])})
            return existing
        _preflight_initial_plan(issue, supplied_plan, key)
        bindings, children = {}, {}
        for stream in supplied_plan["workstreams"]:
            child = gh._resolve_workstream(issue, stream, None, registry, token)
            children[stream["id"]] = child
            bindings[stream["id"]] = {field: child[field] for field in ("id", "number", "url")}
        plan = resolve_plan_issues(supplied_plan, bindings)
        manifest = _preflight_plan_records(
            issue, _initial_manifest(issue, plan, bindings, key), children, key)
        manifest["workstream_plans"] = {
            stream["id"]: _write_child(gh, issue, children[stream["id"]], stream, plan, 1,
                                       registry, token, key)
            for stream in plan["workstreams"]}
        staged = hydrate_record(gh, issue, {"manifest": manifest})
        body = _parent_plan_body(staged["manifest"], issue)
        intent = registry.intent(token, key, "github_plan", {"body": body})
        if intent["state"] == "complete":
            comment = intent["result"]
        elif intent["fresh"]:
            comment = gh.api(f"repos/{issue['repository']}/issues/{issue['number']}/comments",
                             method="POST", body={"body": body})
        else:
            matches = [item for item in gh.pages(
                f"repos/{issue['repository']}/issues/{issue['number']}/comments")
                       if item.get("body") == body]
            if len(matches) != 1:
                raise UnresolvedEffect("original delivery index creation is unresolved")
            comment = matches[0]
        reference = {"comment_id": comment["id"], "comment_node_id": comment["node_id"]}
        readback = load_record(gh, issue, reference)
        if wire_manifest(readback["manifest"]) != wire_manifest(manifest):
            raise OwnershipConflict("initial parent index differs from exact readback")
        registry.finish_effect(token, key, {"id": comment["id"], "node_id": comment["node_id"]})
        gh._bind_label(issue, LABEL_PREFIX + str(comment["id"]), registry, token)
        refreshed = gh.issue(issue["repository"], issue["number"], issue["repository_id"])
        bound = gh.bound_record(refreshed)
        if bound is None or bound["wire_index"] != wire_manifest(manifest):
            raise OwnershipConflict("initial parent plan pointer did not commit")
        return bound


def settle_plan_effect(gh, issue, entry, registry, token):
    from .delivery_github_contract import wire_manifest

    request = json.loads(entry["request_json"]) if "request_json" in entry else entry["request"]
    key, kind = entry["effect_key"], entry["kind"]
    with registry.mutation(token):
        if kind == "github_child_plan":
            if request["parent_issue_id"] != issue["id"]:
                raise OwnershipConflict("child-plan effect belongs to another parent")
            child = _exact_child(gh, issue, request["child"])
            matches = [raw for raw in gh.pages(
                f"repos/{issue['repository']}/issues/{child['number']}/comments")
                       if raw.get("body") == request["body"]]
            if len(matches) != 1:
                raise UnresolvedEffect("original child-plan creation remains unresolved")
            result = _reference(matches[0], child, request["body"])
        elif kind == "github_plan_revision":
            reference = {field: request[field] for field in ("comment_id", "comment_node_id")}
            current = load_record(gh, issue, reference)
            if (digest(wire_manifest(current["manifest"])) != request["after"]
                    or current["manifest"].get("plan_revision") != request["plan_revision"]):
                raise UnresolvedEffect("original parent plan revision is not confirmed")
            result = {"digest": request["after"], "plan_revision": request["plan_revision"]}
        else:
            raise OwnershipConflict("unsupported plan effect: " + kind)
        registry.finish_effect(token, key, result)
        return result
