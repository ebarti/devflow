"""Reconcile an owned, already-reviewed PR with its target without another worker pass."""
from __future__ import annotations

import json
from copy import deepcopy

from .contracts import digest


class MergeChecksPending(ValueError):
    """Required current-target CI has not returned a verdict yet."""


def _shared(broker):
    if broker.spec.get("feature_delivery"):
        from .delivery_feature_execution import registry

        return registry(broker.spec), broker.spec["feature_delivery"]["owner"]
    return None, None


def _effects(broker, pr):
    shared, token = _shared(broker)
    if shared:
        with shared.connect() as db:
            rows = [dict(row) for row in db.execute(
                "SELECT *, result_json AS observed_json FROM execution_effects "
                "WHERE issue_id=? AND kind='merge_reconcile' ORDER BY rowid",
                (token["issue_id"],),
            )]
    else:
        with broker.store._connect() as db:
            rows = [dict(row) for row in db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=? AND kind='merge_reconcile' "
                "ORDER BY rowid", (broker.spec["run_id"],),
            )]
    return [row for row in rows if all(json.loads(row["request_json"])[key] == value
            for key, value in (("number", pr["number"]), ("original_head", pr["head"])))]


def _intent(broker, key, request):
    shared, token = _shared(broker)
    return (shared.intent(token, key, "merge_reconcile", request)["fresh"] if shared
            else broker.intent(key, "merge_reconcile", request))


def _finish(broker, key, result):
    shared, token = _shared(broker)
    if shared:
        shared.finish_effect(token, key, result)
    else:
        broker.finish(key, result)


def _commit(broker, head):
    return broker.api(f"repos/{broker.spec['github_repo']}/git/commits/{head}")


def _observe(broker, request, raw):
    head = raw["head"]["sha"]
    commit = _commit(broker, head)
    if (commit["sha"] != head or commit["tree"]["sha"] != request["tree"]
            or [p["sha"] for p in commit["parents"]]
            != [request["previous_head"], request["target"]]):
        raise ValueError("PR changed outside its exact mechanical target reconciliation")
    return {"head": head, "tree": request["tree"], "base": request["target"]}


def reconcile(broker, pr, raw, *, branch=None, base_ref=None, original_base=None):
    """Return current PR/tree/base, a pending readback, or an explicit conflict decision.

    GitHub's update-branch endpoint merges the base into the expected head. Its
    result must match the exact two parents and tree of the prior GitHub test
    merge. An unknown submission is only observed again, never sent twice.
    Original native role evidence remains bound to the original feature source.
    """
    branch = branch or broker.spec["branch"]
    base_ref = base_ref or broker.spec["publication_base_ref"]
    original_base = original_base or broker.spec["base_sha"]
    if (raw["number"] != pr["number"] or raw["html_url"] != pr["url"]
            or raw["head"]["ref"] != branch or raw["base"]["ref"] != base_ref
            or raw["head"]["repo"]["full_name"] != broker.spec["github_repo"]
            or raw["base"]["repo"]["full_name"] != broker.spec["github_repo"]
            or raw["draft"]):
        raise ValueError("reconciliation PR changed its owned identity")
    expected = pr["head"]
    base = original_base
    bindings = []
    for effect in _effects(broker, pr):
        request = json.loads(effect["request_json"])
        if (request["original_head"] != pr["head"] or request["previous_head"] != expected
                or request["branch"] != branch or request["base_ref"] != base_ref):
            raise ValueError("target reconciliation lost its original publication lineage")
        if effect["state"] == "complete":
            result = json.loads(effect["observed_json"])
            observed = _observe(broker, request, {"head": {"sha": result["head"]}})
            if observed != result:
                raise ValueError("target reconciliation receipt changed")
        else:
            if raw["head"]["sha"] == expected:
                return {"state": "pending", "reason": "original_target_update_readback"}
            result = _observe(broker, request, raw)
            _finish(broker, effect["effect_key"], result)
        expected, base = result["head"], result["base"]
        bindings.append({"previous_head": request["previous_head"], **result})
    if raw["head"]["sha"] != expected:
        raise ValueError("unowned head change cannot use reviewed source evidence")
    repo = f"repos/{broker.spec['github_repo']}"
    target = broker.api(f"{repo}/git/ref/heads/{base_ref}")["object"]["sha"]
    if raw["merged"] or target == base:
        current = {**deepcopy(pr), "head": expected}
        return {"state": "ready", "pr": current, "base": base, "bindings": bindings,
                "tree": _commit(broker, expected)["tree"]["sha"]}
    comparison = broker.api(f"{repo}/compare/{base}...{target}")
    if (comparison.get("status") not in {"ahead", "identical"}
            or comparison.get("merge_base_commit", {}).get("sha") != base):
        return {"state": "decision_required", "reason": "target_history_changed",
                "target": target, "original_head": pr["head"]}
    if raw.get("mergeable") is False:
        return {"state": "decision_required", "reason": "conflicting_upstream_changes",
                "target": target, "original_head": pr["head"],
                "instruction": "Retain the feature and main. Resolve mechanical conflicts; "
                               "ask the user before choosing an opposing product direction."}
    if raw.get("mergeable") is not True or not raw.get("merge_commit_sha"):
        return {"state": "pending", "reason": "current_target_mergeability"}
    hosted = _commit(broker, raw["merge_commit_sha"])
    parents = [p["sha"] for p in hosted["parents"]]
    if (len(parents) != 2 or parents[1] != expected
            or _commit(broker, parents[0])["tree"]["sha"]
            != _commit(broker, target)["tree"]["sha"]):
        return {"state": "pending", "reason": "current_target_test_merge"}
    request = {"number": pr["number"], "original_head": pr["head"],
               "previous_head": expected, "target": target, "tree": hosted["tree"]["sha"],
               "branch": branch, "base_ref": base_ref}
    _, token = _shared(broker)
    owner = token["issue_id"] if token else broker.spec["run_id"]
    key = "merge-reconcile:" + owner + ":" + digest(request)
    if _intent(broker, key, request):
        broker.api(f"{repo}/pulls/{pr['number']}/update-branch", method="PUT",
                   body={"expected_head_sha": expected})
    # Never treat the accepted HTTP response as a completed update or a passed gate.
    return {"state": "pending", "reason": "target_update_submitted", "target": target}


def feature(store, parent, record, live):
    """Qualify the final stack on current trunk without reopening its chunk workers."""
    from .delivery_feature_pass import checkpoints
    from .delivery_merge import MergeBroker

    proofs = checkpoints(parent)
    members, trees, bindings, predecessor = [], [], [], None
    native = record["manifest"]["publication"]["stack_id"] is not None
    for member, raw in zip(record["manifest"]["publication"]["members"], live, strict=True):
        proof = proofs["verified:" + member["chunk_id"]]
        original = store.effective_spec(proof["run_id"])
        authority = {**parent, "branch": member["branch"],
                     "publication_base_ref": member["base_branch"],
                     "base_sha": original["base_sha"],
                     "policy": {**parent["policy"],
                                "required_ci": original["policy"]["required_ci"]}}
        broker = MergeBroker(store, authority)
        pr = {key: member[key] for key in ("number", "url", "head")}
        current = reconcile(broker, pr, raw)
        if current["state"] != "ready":
            return current
        updated = {**member, "head": current["pr"]["head"]}
        if not raw["merged"]:
            verified = MergeBroker(store, {**authority, "base_sha": current["base"]})
            try:
                predecessor = verified.live_ci(
                    current["pr"], raw, current["tree"], native_stack=native,
                    predecessor=predecessor,
                )
            except MergeChecksPending as exc:
                return {"state": "pending", "reason": str(exc), "head": updated["head"]}
        else:
            predecessor = None
        members.append(updated)
        trees.append(current["tree"])
        bindings.append({"number": member["number"], "original_head": member["head"],
                         "base": current["base"], "bindings": current["bindings"]})
    MergeBroker(store, {**parent, "publication_base_ref": members[0]["base_branch"]}
                ).require_base_enforcement()
    return {"state": "ready", "members": members, "trees": trees, "bindings": bindings}


def settle_update(broker, entry, raw):
    """Observe an existing update without creating another remote operation."""
    request = json.loads(entry["request_json"])
    if (raw["number"] != request["number"] or raw["head"]["ref"] != request["branch"]
            or raw["base"]["ref"] != request["base_ref"]
            or raw["head"]["repo"]["full_name"] != broker.spec["github_repo"]
            or raw["base"]["repo"]["full_name"] != broker.spec["github_repo"]):
        raise ValueError("target update lost its owned PR identity")
    if raw["head"]["sha"] == request["previous_head"]:
        return False
    _finish(broker, entry["effect_key"], _observe(broker, request, raw))
    return True
