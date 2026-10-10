"""Exact GitHub publication custody for a feature's single PR stack."""

from __future__ import annotations

import json
from copy import deepcopy

from .contracts import digest
from .delivery_execution_registry import OwnershipConflict, UnresolvedEffect
from .delivery_feature_execution import registry, require_execution
from .delivery_feature_pass import checkpoints as current_checkpoints
from .delivery_github_contract import GitHubDelivery, ordered_chunks


def current_record(spec, gh=None):
    """Fence the exact adopted revision, then hydrate only its recorded children."""
    from .delivery_feature_revisions import adopted_plan_identity

    gh = gh or GitHubDelivery()
    feature = spec["feature_delivery"]
    issue = feature["snapshot"]["issue"]
    shared = registry(spec)
    identity = adopted_plan_identity(spec, shared)
    explicit = spec.get("feature_plan_revision")
    if explicit is not None and any(explicit.get(key) != identity.get(key)
                                    for key in ("plan_revision", "plan_digest")):
        raise OwnershipConflict("worker or coordinator carries a superseded plan revision")
    if spec.get("feature_worker") and explicit is None:
        original = (feature["snapshot"].get("delivery") or {}).get("manifest", {})
        if original.get("plan_revision", 1) != identity["plan_revision"]:
            raise OwnershipConflict("legacy worker carries a superseded plan revision")
    reference = {key: identity.get(key) for key in ("comment_id", "comment_node_id")}
    if reference["comment_id"] is None:
        reference = shared.checkpoints(issue["id"]).get("github-record")
    if reference is None or reference.get("comment_id") is None:
        reference = feature["snapshot"].get("delivery")
    if reference is None:
        raise OwnershipConflict("feature has no published delivery record")
    record = gh.load_record(issue, reference)
    manifest = record["manifest"]
    if manifest["version"] == 2:
        from .delivery_github_plans import verify_parent_binding

        verify_parent_binding(gh, issue, reference)
    if (manifest.get("plan_revision", 1) != identity["plan_revision"]
            or (identity.get("plan_digest") is not None
                and identity["plan_digest"] != digest(manifest["plan"]))):
        raise OwnershipConflict("GitHub plan changed after execution acceptance")
    bindings = identity.get("workstream_issues")
    if bindings and bindings != manifest["workstream_issues"]:
        raise OwnershipConflict("GitHub workstream bindings changed after execution acceptance")
    token = shared.token(shared.current(issue["id"]))
    gh.reconcile_record(issue, record, shared, token)
    return record


def publication_child_binding(record, chunk_id):
    """Bind one PR to the actual child that owns its stable chunk identity."""
    chunks = ordered_chunks(record["manifest"]["plan"])
    chunk = next((item for item in chunks if item["id"] == chunk_id), None)
    if chunk is None:
        raise OwnershipConflict("publication names an unknown feature chunk")
    child = record["manifest"]["workstream_issues"].get(chunk["workstream_id"])
    if child is None:
        raise OwnershipConflict("publication chunk has no exact child issue binding")
    return {"workstream_id": chunk["workstream_id"], "issue_id": child["id"],
            "issue_number": child["number"], "issue_url": child["url"]}


def publish(store, broker, request, *, reconcile=False, settlement=False):
    """Keep feature custody until both the PR and its canonical binding settle."""
    spec = request["spec"]
    shared = registry(spec)
    token = shared.token(shared.current(spec["feature_delivery"]["owner"]["issue_id"]))
    key = "publication:" + digest(
        {
            "run": spec["run_id"],
            "iteration": request["iteration"],
            "candidate": request["candidate"],
        }
    )
    with shared.mutation(token):
        if spec.get("feature_worker"):
            # Authenticate before any broker push/PR effect, including retries.
            current_record(spec)
        if settlement and shared.effect(token["issue_id"], key) is None:
            raise OwnershipConflict("settlement cannot originate a new publication")
        intent = shared.intent(
            token,
            key,
            "publish_chunk",
            {
                "run_id": spec["run_id"],
                "iteration": request["iteration"],
                "candidate": request["candidate"],
            },
        )
        if intent["state"] == "complete":
            result = intent["result"]
        else:
            try:
                if reconcile or not intent["fresh"]:
                    result = broker.reconcile_publish(
                        request["iteration"],
                        request["candidate"],
                        expected_head=request.get("expected_head"),
                        expected_pr_number=request.get("expected_pr_number"),
                    )
                else:
                    result = broker.publish(request["iteration"], request["candidate"])
            except Exception:
                if intent["fresh"] and broker.publication_may_have_effect is False:
                    shared.finish_effect(
                        token, key, {"rejected_before_effect": True}, no_effect=True
                    )
                raise
    # These operations journal their own intents. The outer publication intent
    # stays pending throughout, including a crash between PR creation and binding.
    if result.get("state") == "pending" or not result.get("number"):
        return result
    result = record_publication(store, spec, result, settlement=settlement)
    shared.finish_effect(token, key, result)
    return result


def live_members(spec, record, gh=None):
    gh = gh or GitHubDelivery()
    publication = record["manifest"]["publication"]
    members = publication["members"]
    if publication["stack_id"] is not None:
        stack = gh.api(f"repos/{spec['github_repo']}/stacks/{publication['stack_id']}")
        if stack["number"] != publication["stack_id"] or [
            item["number"] for item in stack["pull_requests"]
        ] != [item["number"] for item in members]:
            raise OwnershipConflict("recorded stack membership changed")
    results = []
    for member in members:
        raw = gh.api(f"repos/{spec['github_repo']}/pulls/{member['number']}")
        if (
            raw["number"] != member["number"]
            or raw["html_url"] != member["url"]
            or raw["head"]["sha"] != member["head"]
            or raw["head"]["ref"] != member["branch"]
            or raw["draft"]
            or raw["head"]["repo"]["full_name"].casefold() != spec["github_repo"].casefold()
            or raw["base"]["repo"]["full_name"].casefold() != spec["github_repo"].casefold()
            or (not raw["merged"] and raw["base"]["ref"] != member["base_branch"])
        ):
            raise OwnershipConflict("recorded PR changed; its candidate requires validation")
        results.append(raw)
    return results


def owned_pr_number(spec):
    record = current_record(spec)
    found = [
        member
        for member in record["manifest"]["publication"]["members"]
        if member["chunk_id"] == spec["feature_worker"]["chunk_id"]
    ]
    if found and found[0]["branch"] != spec["branch"]:
        raise OwnershipConflict("chunk's publication branch changed")
    return found[0]["number"] if found else None


def verify_retained_publication(broker):
    """An unpushed integration attempt retains its original, explicitly bound PR."""
    from .delivery_broker import _git

    spec = broker.spec
    previous = spec["feature_worker"]["previous_publication"]
    record = current_record(spec)
    if (previous not in record["manifest"]["publication"]["members"]
            or previous["branch"] != spec["branch"]
            or _git(broker.checkout, "branch", "--show-current")
            != spec.get("local_branch", spec["branch"])
            or _git(broker.source, "remote", "get-url", "origin") != spec["origin_url"]
            or _git(broker.checkout, "remote", "get-url", "--push", "origin")
            != spec["origin_url"]):
        raise OwnershipConflict("retained integration publication identity changed")
    raw = next(item for item in live_members(spec, record) if item["number"] == previous["number"])
    remote = _git(broker.source, "ls-remote", "origin", "refs/heads/" + previous["branch"])
    if raw["merged"] or raw["state"] != "open" or remote.split() != [
        previous["head"], "refs/heads/" + previous["branch"],
    ]:
        raise OwnershipConflict("retained integration PR is closed or its branch moved")


def record_publication(store, spec, receipt, gh=None, *, settlement=False):
    if not receipt.get("number"):
        return receipt
    gh = gh or GitHubDelivery()
    if not settlement:
        require_execution(store, spec)
    shared = registry(spec)
    token = shared.token(shared.current(spec["feature_delivery"]["owner"]["issue_id"]))
    issue = spec["feature_delivery"]["snapshot"]["issue"]
    record = current_record(spec, gh)
    manifest = deepcopy(record["manifest"])
    publication = manifest["publication"]
    members = publication["members"]
    chunk_id = spec["feature_worker"]["chunk_id"]
    member = {
        "chunk_id": chunk_id,
        "number": receipt["number"],
        "url": receipt["url"],
        "branch": spec["branch"],
        "head": receipt["head"],
        "base_branch": spec["publication_base_ref"],
    }
    if manifest["version"] == 2:
        binding = publication_child_binding(record, chunk_id)
        if (spec["feature_worker"].get("workstream_id") != binding["workstream_id"]
                or spec["issue_url"] != binding["issue_url"]):
            raise OwnershipConflict("worker publication differs from its actual child issue")
        member.update(binding)
    previous = next((item for item in members if item["chunk_id"] == chunk_id), None)
    if previous == member:
        live_members(spec, record, gh)
        return receipt
    if previous:
        reintegrating = spec.get("feature_worker", {}).get("previous_publication")
        pass_state = current_checkpoints(spec)
        index = members.index(previous)
        preceding = ordered_chunks(manifest["plan"])[:index]
        own_previous = previous == reintegrating
        if reintegrating and not own_previous:
            with store._connect() as db:
                receipts = [json.loads(row[0]) for row in db.execute(
                    "SELECT observed_json FROM delivery_effects WHERE run_id=? "
                    "AND kind='publish' AND state='complete' AND observed_json IS NOT NULL",
                    (spec["run_id"],),
                )]
            own_previous = any(all(saved.get(key) == previous[key]
                                   for key in ("number", "url", "head")) for saved in receipts)
        if reintegrating and (
            not own_previous or any("verified:" + item["id"] not in pass_state
                                    for item in preceding)
        ):
            raise OwnershipConflict("integration differs from the original stack head or order")
        if (
            (not reintegrating and previous != members[-1])
            or previous["number"] != member["number"]
            or previous["branch"] != member["branch"]
        ):
            raise OwnershipConflict("repair would replace a PR or rewrite a lower stack layer")
        members[index] = member
    else:
        expected = ordered_chunks(manifest["plan"])
        if len(members) >= len(expected) or expected[len(members)]["id"] != chunk_id:
            raise OwnershipConflict("publication is out of the feature's integration order")
        members.append(member)
    if len(members) > 1 and not previous:
        numbers = [item["number"] for item in members]
        key = "github-stack:" + ":".join(map(str, numbers))
        with shared.mutation(token):
            intent = shared.intent(
                token,
                key,
                "github_stack",
                {"stack_id": publication["stack_id"], "members": numbers},
            )
            if intent["state"] == "complete":
                stack_id = intent["result"]["number"]
                stack = gh.api(f"repos/{spec['github_repo']}/stacks/{stack_id}")
            elif publication["stack_id"]:
                stack_id = publication["stack_id"]
                stack = gh.api(f"repos/{spec['github_repo']}/stacks/{stack_id}")
                observed = [item["number"] for item in stack["pull_requests"]]
                if observed != numbers:
                    if observed != numbers[:-1] or not intent["fresh"]:
                        raise UnresolvedEffect("recorded stack append needs reconciliation")
                    gh.api(
                        f"repos/{spec['github_repo']}/stacks/{stack_id}/add",
                        method="POST",
                        body={"pull_requests": numbers[-1:]},
                    )
                    stack = gh.api(f"repos/{spec['github_repo']}/stacks/{stack_id}")
            elif intent["fresh"]:
                stack = gh.api(
                    f"repos/{spec['github_repo']}/stacks",
                    method="POST",
                    body={"pull_requests": numbers},
                )
                stack = gh.api(f"repos/{spec['github_repo']}/stacks/{stack['number']}")
            else:
                matches = gh.api(f"repos/{spec['github_repo']}/stacks?pull_request={numbers[0]}")
                if len(matches) != 1:
                    raise UnresolvedEffect("original stack creation needs exact PR readback")
                stack = matches[0]
            if [item["number"] for item in stack["pull_requests"]] != numbers:
                raise OwnershipConflict("GitHub stack readback has different members")
            shared.finish_effect(token, key, {"number": stack["number"]})
            publication["stack_id"] = stack["number"]
    manifest["revision"] += 1
    saved = gh.update(issue, record, manifest, shared, token)
    live_members(spec, saved, gh)
    return receipt
